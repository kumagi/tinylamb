import Std.Tactic

/-!
# ASOF JOIN frontier over a time-ordered snapshot

Right rows arrive ordered by `(time, appearance id)`; left rows arrive
ordered by time.  Each left row needs the *latest visible* right row in its
time window whose key equals the query key (NULL keys never match) and which
is not deleted.  Equal timestamps are resolved by an explicit tie-break on
the appearance id, and the window may be inclusive (`time ≤ left.time`) or
exclusive (`time < left.time`).

Reference computation: `refTop1` is the correlated Top-1 — it scans **all**
right rows for each left row and keeps the last eligible one (under the
sortedness hypothesis this is the maximum of `(time, tie)`).

Optimized computation: `mergeGo` keeps a single frontier while both inputs
are scanned forward.  It advances past right rows inside the current left
row's window, replacing the frontier only when the row is eligible, and
stops advancing at the first right row outside the window.

Unmatched left rows are padded with `none`.  `asof_snapshot_frontier_sound`
proves the merge output (padding included) equals the correlated output.
`asof_merge_linear` proves each left row is read once and each right row at
most once.  Counterexamples cover a deleted newest same-time candidate, a
NULL query key and an exclusive boundary.
-/

namespace TinylambOpt.Research.AsofSnapshotFrontier

/-- A left row: its event time and a stable row id. -/
structure LRow where
  time : Int
  rowId : Nat
  deriving DecidableEq, Repr

/-- A right version: time, tie-breaking appearance id, SQL key (NULL =
    `none`), deletion flag and a payload. -/
structure RRow where
  time : Int
  tie : Nat
  key : Option Int
  deleted : Bool
  payload : Int
  deriving DecidableEq, Repr

/-- Inclusive vs exclusive upper window. -/
def inWindow (incl : Bool) (l : LRow) (r : RRow) : Bool :=
  if incl then decide (r.time ≤ l.time) else decide (r.time < l.time)

/-- Key equality with NULL semantics: a NULL on either side never matches. -/
def qMatch (qk : Option Int) (r : RRow) : Bool :=
  match qk, r.key with
  | some a, some b => decide (a = b)
  | _, _ => false

def visible (r : RRow) : Bool := !r.deleted

/-- A right row is eligible when it is in the window, matches the key and
    is not a deleted version. -/
def eligible (incl : Bool) (l : LRow) (qk : Option Int) (r : RRow) : Bool :=
  inWindow incl l r && qMatch qk r && visible r

/-- Strict order of right versions by `(time, tie)`: larger is later. -/
def SlotLe (a b : RRow) : Prop := a.time < b.time ∨ (a.time = b.time ∧ a.tie < b.tie)

def SortedR : List RRow → Prop := List.Pairwise SlotLe

def SortedL : List LRow → Prop := List.Pairwise (fun a b : LRow => a.time ≤ b.time)

theorem slotLe_time_le {a b : RRow} (h : SlotLe a b) : a.time ≤ b.time := by
  rcases h with hlt | ⟨heq, _⟩
  · omega
  · omega

theorem inWindow_mono (incl : Bool) {lprev l : LRow} (h : lprev.time ≤ l.time)
    {r : RRow} (hp : inWindow incl lprev r = true) : inWindow incl l r = true := by
  cases incl <;> simp [inWindow] at hp ⊢ <;> omega

theorem not_inWindow_of_later (incl : Bool) (l : LRow) {a r : RRow}
    (ha : inWindow incl l a = false) (hle : a.time ≤ r.time) :
    inWindow incl l r = false := by
  cases incl <;> simp [inWindow] at ha ⊢ <;> omega

/- ------------------------------------------------------------------------ -
    `lastSatisfying`: keep the last row satisfying a predicate
    ------------------------------------------------------------------------ -/

/-- Scan a list, replacing the carried option whenever `p r` holds. -/
def lastSatisfying (p : RRow → Bool) (f0 : Option RRow) : List RRow → Option RRow
  | [] => f0
  | r :: rest => lastSatisfying p (if p r then some r else f0) rest

theorem lastSatisfying_append (p : RRow → Bool) (f0 : Option RRow)
    (xs ys : List RRow) :
    lastSatisfying p (lastSatisfying p f0 xs) ys = lastSatisfying p f0 (xs ++ ys) := by
  induction xs generalizing f0 with
  | nil => rfl
  | cons x xs ih =>
      simp [lastSatisfying, ih]

theorem lastSatisfying_all_false (p : RRow → Bool) (f0 : Option RRow) :
    ∀ ys : List RRow, (∀ y ∈ ys, p y = false) → lastSatisfying p f0 ys = f0
  | [], _ => rfl
  | y :: ys, h => by
      have hy : p y = false := h y (by simp)
      dsimp [lastSatisfying]
      rw [hy]
      apply lastSatisfying_all_false p f0 ys
      intro z hz
      exact h z (by simp [hz])

theorem lastSatisfying_append_of_none (p : RRow → Bool) (f0 : Option RRow)
    (xs ys : List RRow) (h : ∀ y ∈ ys, p y = false) :
    lastSatisfying p f0 (xs ++ ys) = lastSatisfying p f0 xs := by
  induction xs generalizing f0 with
  | nil => simpa [lastSatisfying] using lastSatisfying_all_false p f0 ys h
  | cons x xs ih =>
      dsimp [lastSatisfying]
      exact ih (if p x then some x else f0)

theorem lastSatisfying_congr (p q : RRow → Bool) (f0 : Option RRow) :
    ∀ xs : List RRow, (∀ r ∈ xs, p r = q r) →
      lastSatisfying p f0 xs = lastSatisfying q f0 xs
  | [], _ => rfl
  | x :: xs, h => by
      have hx : p x = q x := h x (by simp)
      dsimp [lastSatisfying]
      rw [hx]
      apply lastSatisfying_congr p q (if q x then some x else f0) xs
      intro r hr
      exact h r (by simp [hr])

/-- **Reference**: correlated Top-1 over every right row. -/
def refTop1 (incl : Bool) (l : LRow) (qk : Option Int) (rs : List RRow) : Option RRow :=
  lastSatisfying (eligible incl l qk) none rs

/-- **Reference output**: one (possibly padded) result per left row. -/
def refAsOf (incl : Bool) (qk : Option Int) (ls : List LRow) (rs : List RRow) :
    List (LRow × Option RRow) :=
  ls.map (fun l => (l, refTop1 incl l qk rs))

/- ------------------------------------------------------------------------ -
    Merge frontier
    ------------------------------------------------------------------------ -/

/-- Advance over the prefix of `rs` inside the window.  Returns the consumed
    rows, the untouched suffix and the number of rows read. -/
def advanceGo (incl : Bool) (l : LRow) : List RRow → List RRow × List RRow × Nat
  | [] => ([], [], 0)
  | r :: rest =>
      if inWindow incl l r then
        let (cs, rem, c) := advanceGo incl l rest
        (r :: cs, rem, c + 1)
      else ([], r :: rest, 0)

theorem advanceGo_split (incl : Bool) (l : LRow) (rs : List RRow) :
    let (cs, rem, c) := advanceGo incl l rs
    cs ++ rem = rs ∧ (∀ r ∈ cs, inWindow incl l r = true) ∧ c = cs.length := by
  induction rs with
  | nil => simp [advanceGo]
  | cons r rest ih =>
      rcases hres : advanceGo incl l rest with ⟨cs, rem, c⟩
      rw [hres] at ih
      rcases ih with ⟨happ, hcs, hc⟩
      by_cases hwin : inWindow incl l r = true
      · simp [advanceGo, hwin, hres]
        refine ⟨?_, ?_, ?_⟩
        · simp [happ]
        · intro x hx
          exact hcs x hx
        · omega
      · have hno : inWindow incl l r = false := by
          cases hb : inWindow incl l r with
          | true => exact False.elim (hwin hb)
          | false => rfl
        simp [advanceGo, hwin]

theorem advanceGo_outside (incl : Bool) (l : LRow) (rs : List RRow)
    (hs : SortedR rs) :
    let (cs, rem, c) := advanceGo incl l rs
    ∀ r ∈ rem, inWindow incl l r = false := by
  induction rs with
  | nil => simp [advanceGo]
  | cons r rest ih =>
      rcases hres : advanceGo incl l rest with ⟨cs, rem, c⟩
      rw [hres] at ih
      by_cases hwin : inWindow incl l r = true
      · have htail : SortedR rest := (List.pairwise_cons.mp hs).2
        have ih' := ih htail
        simp [advanceGo, hwin, hres]
        exact ih'
      · have hno : inWindow incl l r = false := by
          cases hb : inWindow incl l r with
          | true => exact False.elim (hwin hb)
          | false => rfl
        simp [advanceGo, hwin]
        intro x hx
        have hle : r.time ≤ x.time := by
          have hh := (List.pairwise_cons.mp hs).1 x hx
          exact slotLe_time_le hh
        exact not_inWindow_of_later incl l hno hle

/-- Result of the merge, including the linear scan counters. -/
structure MergeResult where
  out : List (LRow × Option RRow)
  seen : List RRow
  remaining : List RRow
  frontier : Option RRow
  leftReads : Nat
  rightReads : Nat

def mergeGo (incl : Bool) (qk : Option Int) (seen remaining : List RRow)
    (frontier : Option RRow) : List LRow → MergeResult
  | [] => { out := [], seen, remaining, frontier, leftReads := 0, rightReads := 0 }
  | l :: ls =>
      let (cs, rem, cR) := advanceGo incl l remaining
      let f' := lastSatisfying (eligible incl l qk) frontier cs
      let r := mergeGo incl qk (seen ++ cs) rem f' ls
      { out := (l, f') :: r.out, seen := r.seen, remaining := r.remaining,
        frontier := r.frontier, leftReads := r.leftReads + 1, rightReads := cR + r.rightReads }

def mergeAsOf (incl : Bool) (qk : Option Int) (ls : List LRow) (rs : List RRow) :
    List (LRow × Option RRow) × Nat × Nat :=
  let r := mergeGo incl qk [] rs none ls
  (r.out, r.leftReads, r.rightReads)

/-- The merge invariant after processing a left row `l`. -/
def Inv (incl : Bool) (qk : Option Int) (rs : List RRow) (l : LRow)
    (seen remaining : List RRow) (frontier : Option RRow) : Prop :=
  seen ++ remaining = rs ∧
  (∀ r ∈ seen, inWindow incl l r = true) ∧
  (∀ r ∈ remaining, inWindow incl l r = false) ∧
  frontier = lastSatisfying (eligible incl l qk) none seen

theorem SortedR_suffix {rs seen remaining : List RRow} (hs : SortedR rs)
    (hsplit : seen ++ remaining = rs) : SortedR remaining := by
  rw [← hsplit] at hs
  exact (List.pairwise_append.mp hs).2.1

theorem eligible_mono (incl : Bool) (qk : Option Int) {lprev l : LRow}
    (hmono : lprev.time ≤ l.time) {r : RRow}
    (hp : inWindow incl lprev r = true) :
    eligible incl lprev qk r = eligible incl l qk r := by
  have hl : inWindow incl l r = true := inWindow_mono incl hmono hp
  simp [eligible, hp, hl]

theorem frontier_eq_refTop1 (incl : Bool) (qk : Option Int) (rs : List RRow)
    (l : LRow) (seen remaining : List RRow) (frontier : Option RRow)
    (hInv : Inv incl qk rs l seen remaining frontier) :
    frontier = refTop1 incl l qk rs := by
  rcases hInv with ⟨hsplit, hseen, hrem, hfront⟩
  rw [hfront]
  unfold refTop1
  rw [← hsplit]
  have hnone : ∀ r ∈ remaining, eligible incl l qk r = false := by
    intro r hr
    have hrfalse := hrem r hr
    simp [eligible, hrfalse]
  exact (lastSatisfying_append_of_none (eligible incl l qk) none seen remaining hnone).symm

theorem merge_step_first (incl : Bool) (qk : Option Int) (rs : List RRow)
    (l : LRow) (hRs : SortedR rs) (cs rem : List RRow) (cR : Nat)
    (hadv : advanceGo incl l rs = (cs, rem, cR)) :
    Inv incl qk rs l cs rem (lastSatisfying (eligible incl l qk) none cs) := by
  have hs := advanceGo_split incl l rs
  rw [hadv] at hs
  have ho := advanceGo_outside incl l rs hRs
  rw [hadv] at ho
  rcases hs with ⟨happ, hcs, hc⟩
  exact ⟨happ, hcs, ho, rfl⟩

theorem merge_step (incl : Bool) (qk : Option Int) (rs : List RRow)
    (lprev l : LRow) (seen remaining : List RRow) (frontier : Option RRow)
    (cs rem : List RRow) (cR : Nat)
    (hadv : advanceGo incl l remaining = (cs, rem, cR))
    (hmono : lprev.time ≤ l.time) (hRs : SortedR rs)
    (hInv : Inv incl qk rs lprev seen remaining frontier) :
    Inv incl qk rs l (seen ++ cs) rem
      (lastSatisfying (eligible incl l qk) frontier cs) := by
  rcases hInv with ⟨hsplit, hseen, hrem, hfront⟩
  have hsrem : SortedR remaining := SortedR_suffix hRs hsplit
  have hs := advanceGo_split incl l remaining
  rw [hadv] at hs
  have ho := advanceGo_outside incl l remaining hsrem
  rw [hadv] at ho
  rcases hs with ⟨happ, hcs, hc⟩
  refine ⟨?_, ?_, ho, ?_⟩
  · rw [← hsplit, ← happ]
    simp [List.append_assoc]
  · intro r hr
    rcases List.mem_append.mp hr with hrseen | hrcs
    · exact inWindow_mono incl hmono (hseen r hrseen)
    · exact hcs r hrcs
  · have hseenMono : ∀ r ∈ seen, eligible incl lprev qk r = eligible incl l qk r := by
      intro r hr
      exact eligible_mono incl qk hmono (hseen r hr)
    have hfront' : frontier = lastSatisfying (eligible incl l qk) none seen := by
      rw [hfront]
      exact lastSatisfying_congr (eligible incl lprev qk) (eligible incl l qk) none seen hseenMono
    rw [hfront']
    exact lastSatisfying_append (eligible incl l qk) none seen cs

/-- Once the invariant holds for `lcur`, the rest of the merge is correct. -/
theorem mergeGo_correct_from (incl : Bool) (qk : Option Int) (rs : List RRow)
    (lcur : LRow) (ls : List LRow) (hsorted : SortedL (lcur :: ls))
    (hRs : SortedR rs) (seen remaining : List RRow) (frontier : Option RRow)
    (hInv : Inv incl qk rs lcur seen remaining frontier) :
    (mergeGo incl qk seen remaining frontier ls).out =
      ls.map (fun l => (l, refTop1 incl l qk rs)) := by
  induction ls generalizing lcur seen remaining frontier with
  | nil => rfl
  | cons l ls ih =>
      have hmono : lcur.time ≤ l.time := ((List.pairwise_cons.mp hsorted).1) l (by simp)
      have htail : SortedL (l :: ls) := (List.pairwise_cons.mp hsorted).2
      rcases hadv : advanceGo incl l remaining with ⟨cs, rem, cR⟩
      have hstep := merge_step incl qk rs lcur l seen remaining frontier cs rem cR
        hadv hmono hRs hInv
      dsimp [mergeGo]
      rw [hadv]
      dsimp
      have hf' := frontier_eq_refTop1 incl qk rs l (seen ++ cs) rem _ hstep
      rw [hf']
      have hstep2 : Inv incl qk rs l (seen ++ cs) rem (refTop1 incl l qk rs) := by
        simpa [hf'] using hstep
      rw [ih l htail (seen ++ cs) rem (refTop1 incl l qk rs) hstep2]
      rfl

/- ------------------------------------------------------------------------ -
    Main theorems
    ------------------------------------------------------------------------ -/

/-- **Main theorem.** The forward merge frontier agrees with the correlated
    per-left Top-1, unmatched padding included. -/
theorem asof_snapshot_frontier_sound (incl : Bool) (qk : Option Int)
    (ls : List LRow) (rs : List RRow) (hLs : SortedL ls) (hRs : SortedR rs) :
    (mergeAsOf incl qk ls rs).1 = refAsOf incl qk ls rs := by
  cases ls with
  | nil => simp [mergeAsOf, mergeGo, refAsOf]
  | cons l0 rest =>
      have hfirstPred := (List.pairwise_cons.mp hLs).1
      have htail : SortedL rest := (List.pairwise_cons.mp hLs).2
      have hsorted : SortedL (l0 :: rest) := List.Pairwise.cons hfirstPred htail
      rcases hadv : advanceGo incl l0 rs with ⟨cs, rem, cR⟩
      have hfirst := merge_step_first incl qk rs l0 hRs cs rem cR hadv
      dsimp [mergeAsOf, mergeGo]
      rw [hadv]
      dsimp
      have hf0 := frontier_eq_refTop1 incl qk rs l0 cs rem _ hfirst
      rw [hf0]
      have hfirst' : Inv incl qk rs l0 cs rem (refTop1 incl l0 qk rs) := by
        simpa [hf0] using hfirst
      rw [mergeGo_correct_from incl qk rs l0 rest hsorted hRs cs rem
        (refTop1 incl l0 qk rs) hfirst']
      rfl

theorem mergeGo_counts (incl : Bool) (qk : Option Int) (rs : List RRow)
    (ls : List LRow) (seen remaining : List RRow) (frontier : Option RRow)
    (hsplit : seen ++ remaining = rs) :
    let r := mergeGo incl qk seen remaining frontier ls
    r.leftReads = ls.length ∧ r.rightReads + seen.length = r.seen.length ∧
      r.seen ++ r.remaining = rs := by
  induction ls generalizing seen remaining frontier with
  | nil => simp [mergeGo, hsplit]
  | cons l ls ih =>
      rcases hadv : advanceGo incl l remaining with ⟨cs, rem, cR⟩
      have hs := advanceGo_split incl l remaining
      rw [hadv] at hs
      rcases hs with ⟨happ, hcs, hc⟩
      have hsplit' : (seen ++ cs) ++ rem = rs := by
        rw [← hsplit, ← happ]
        simp [List.append_assoc]
      have ih' := ih (seen ++ cs) rem (lastSatisfying (eligible incl l qk) frontier cs) hsplit'
      dsimp at ih'
      rcases ih' with ⟨hL, hR, hsn⟩
      simp [mergeGo, hadv, hL, hsn]
      simp [List.length_append] at hR
      omega

/-- **Linear scan bound.** Left rows are read exactly once and right rows at
    most once, so total work is at most `|L| + |R|`. -/
theorem asof_merge_linear (incl : Bool) (qk : Option Int)
    (ls : List LRow) (rs : List RRow) :
    let (_, cL, cR) := mergeAsOf incl qk ls rs
    cL = ls.length ∧ cR ≤ rs.length ∧ cL + cR ≤ ls.length + rs.length := by
  have hcnt := mergeGo_counts incl qk rs ls [] rs none rfl
  dsimp [mergeAsOf]
  dsimp at hcnt
  rcases hcnt with ⟨hL, hR, hsn⟩
  have hle : (mergeGo incl qk [] rs none ls).seen.length ≤ rs.length := by
    have := congrArg List.length hsn
    simp at this
    omega
  refine ⟨hL, ?_, ?_⟩
  · omega
  · omega

/- ------------------------------------------------------------------------ -
    Concrete counterexamples
    ------------------------------------------------------------------------ -/

def r1 : RRow := { time := 5, tie := 1, key := some 1, deleted := false, payload := 10 }
def r2 : RRow := { time := 10, tie := 2, key := some 1, deleted := false, payload := 20 }
def r3 : RRow := { time := 10, tie := 3, key := some 1, deleted := true, payload := 30 }
def sampleRights : List RRow := [r1, r2, r3]
def sampleLeft : LRow := { time := 10, rowId := 0 }

/-- **Counterexample:** the newest same-time candidate (`r3`) is deleted;
    the preceding distinct row `r2` is visible.  Taking the last physical
    row instead of the last *eligible* one is wrong. -/
theorem deleted_newest_frontier_counterexample :
    refTop1 true sampleLeft (some 1) sampleRights = some r2 ∧
    sampleRights.getLast? = some r3 ∧ r3.deleted = true := by
  native_decide

/-- **Counterexample:** a NULL key never matches any right row. -/
theorem null_key_never_matches_counterexample :
    refTop1 true sampleLeft none sampleRights = none := by
  native_decide

/-- **Counterexample:** with an exclusive bound the row at time = left.time
    is outside the window; the answer falls back to the earlier row. -/
theorem exclusive_boundary_counterexample :
    refTop1 false sampleLeft (some 1) sampleRights = some r1 ∧
    refTop1 true sampleLeft (some 1) sampleRights = some r2 := by
  native_decide

end TinylambOpt.Research.AsofSnapshotFrontier
