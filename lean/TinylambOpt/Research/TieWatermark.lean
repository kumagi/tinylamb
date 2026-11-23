import Std.Tactic

set_option maxRecDepth 100000

/-!
# Stop watermark for distributed Top-N with ties

Several inputs are already ordered by a composite ORDER BY key (mixed
ASC/DESC, shared NULL placement).  A stable merge of the inputs gives the
global order.  We must emit `OFFSET` then `FETCH FIRST k ROWS WITH TIES`:
appearance ids are *not* part of the order key, so every row tied with the
k-th row must be emitted even when it sits at the very end of an unread
input block.

Reference computation: `refWithTies` filters the whole merged sequence by
`key ≤ boundary` (it scans everything).

Optimized computation: `wmRead` stops at the first row strictly past the
boundary, uses `takeWhile`, and returns an explicit read count.

`tie_watermark_sound` proves the emitted payload sequence, the watermark and
the determinable prefix are exact.  `tie_watermark_read_bound` proves rows
strictly past the boundary are never read, in any input stream.  The
counterexample shows that a local `take k` loses a large tied block.
-/

namespace TinylambOpt.Research.TieWatermark

/- ------------------------------------------------------------------------ -
    Composite order key (mixed ASC/DESC, shared NULL placement)
    ------------------------------------------------------------------------ -/

inductive Dir where
  | ASC | DESC
  deriving DecidableEq, Repr, Inhabited

inductive Nulls where
  | First | Last
  deriving DecidableEq, Repr, Inhabited

abbrev Spec := List (Dir × Nulls)
abbrev Key := List (Option Int)

def optLt (n : Nulls) (a b : Option Int) : Bool :=
  match a, b with
  | none, none => false
  | none, some _ => n == Nulls.First
  | some _, none => n == Nulls.Last
  | some x, some y => x < y

theorem optLt_irrefl (n : Nulls) (a : Option Int) : optLt n a a = false := by
  cases n <;> cases a <;> simp [optLt]

theorem optLt_trans (n : Nulls) {a b c : Option Int}
    (hab : optLt n a b = true) (hbc : optLt n b c = true) : optLt n a c = true := by
  cases n <;> cases a <;> cases b <;> cases c <;> simp [optLt] at * <;> omega

theorem optLt_trichotomy (n : Nulls) (a b : Option Int) :
    optLt n a b = true ∨ a = b ∨ optLt n b a = true := by
  cases n <;> cases a <;> cases b <;> simp [optLt] <;> omega

private def reverseNulls : Nulls → Nulls
  | .First => .Last
  | .Last => .First

def valLt (d : Dir) (n : Nulls) (a b : Option Int) : Bool :=
  match d with
  | Dir.ASC => optLt n a b
  | Dir.DESC => optLt (reverseNulls n) b a

/-- Explicit NULL placement is independent of direction; direction orders values only. -/
theorem sql_order_component (d : Dir) (n : Nulls) (x y : Int) :
    valLt d n none (some y) = (n == .First) ∧
    valLt d n (some x) none = (n == .Last) ∧
    valLt d n none none = false ∧
    valLt d n (some x) (some y) = (if d == .ASC then decide (x < y) else decide (y < x)) := by
  cases d <;> cases n <;> exact ⟨rfl, rfl, rfl, rfl⟩

theorem valLt_irrefl (d : Dir) (n : Nulls) (a : Option Int) : valLt d n a a = false := by
  cases d <;> simp [valLt, optLt_irrefl]

theorem valLt_trichotomy (d : Dir) (n : Nulls) (a b : Option Int) :
    valLt d n a b = true ∨ a = b ∨ valLt d n b a = true := by
  cases d with
  | ASC => exact optLt_trichotomy n a b
  | DESC =>
      rcases optLt_trichotomy (reverseNulls n) b a with hlt | heq | hgt
      · exact Or.inl (by simpa [valLt] using hlt)
      · exact Or.inr (Or.inl heq.symm)
      · exact Or.inr (Or.inr (by simpa [valLt] using hgt))

def keyLe : Spec → Key → Key → Bool
  | [], _, _ => true
  | _ :: _, [], _ => true
  | _ :: _, _ :: _, [] => false
  | (d, n) :: spec, a :: as, b :: bs =>
      valLt d n a b || (decide (a = b) && keyLe spec as bs)

theorem keyLe_refl (spec : Spec) (as : Key) : keyLe spec as as = true := by
  induction spec generalizing as with
  | nil => simp [keyLe]
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      cases as with
      | nil => simp [keyLe]
      | cons a as => simp [keyLe, ih]

theorem keyLe_total (spec : Spec) (as bs : Key) :
    keyLe spec as bs || keyLe spec bs as = true := by
  induction spec generalizing as bs with
  | nil => simp [keyLe]
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      cases as with
      | nil => simp [keyLe]
      | cons a as =>
          cases bs with
          | nil => simp [keyLe]
          | cons b bs =>
              rcases valLt_trichotomy d n a b with hlt | heq | hgt
              · simp [keyLe, hlt]
              · subst b
                simpa [keyLe, valLt_irrefl] using ih as bs
              · simp [keyLe, hgt]

theorem keyLe_trans (spec : Spec) (as bs cs : Key)
    (hab : keyLe spec as bs = true) (hbc : keyLe spec bs cs = true) :
    keyLe spec as cs = true := by
  induction spec generalizing as bs cs with
  | nil => simp [keyLe]
  | cons s spec ih =>
      rcases s with ⟨d, n⟩
      cases as with
      | nil => simp [keyLe]
      | cons a as =>
          cases bs with
          | nil => simp [keyLe] at hab
          | cons b bs =>
              cases cs with
              | nil => simp [keyLe] at hbc
              | cons c cs =>
                  simp [keyLe] at hab hbc ⊢
                  rcases hab with hltab | ⟨heqab, habtail⟩
                  · rcases hbc with hltbc | ⟨heqbc, hbctail⟩
                    · exact Or.inl (by
                        cases d with
                        | ASC => exact optLt_trans n hltab hltbc
                        | DESC => exact optLt_trans (reverseNulls n) hltbc hltab)
                    · subst c
                      exact Or.inl hltab
                  · subst b
                    rcases hbc with hltbc | ⟨heqbc, hbctail⟩
                    · exact Or.inl hltbc
                    · subst c
                      exact Or.inr ⟨rfl, ih as bs cs habtail hbctail⟩

/-- Strict part of the key order.  NULL-vs-NULL and equal keys are never
    "better"; a boundary is never confused with an appearance id. -/
def better (spec : Spec) (a b : Key) : Bool :=
  keyLe spec a b && !keyLe spec b a

theorem not_better_means_ge (spec : Spec) {a b : Key}
    (h : better spec a b = false) : keyLe spec b a = true := by
  cases hle : keyLe spec a b with
  | false =>
      have ht := keyLe_total spec a b
      simpa [hle] using ht
  | true =>
      cases hge : keyLe spec b a with
      | false => simp [better, hle, hge] at h
      | true => rfl

theorem keyLe_of_not_ge (spec : Spec) {a b : Key}
    (h : keyLe spec a b = false) : better spec b a = true := by
  cases hge : keyLe spec b a with
  | false =>
      have ht := keyLe_total spec b a
      simp [hge, h] at ht
  | true => simp [better, h, hge]

/- ------------------------------------------------------------------------ -
    Rows, sorted streams, stable merge
    ------------------------------------------------------------------------ -/

/-- A row carries a key, a payload and a unique appearance id.  The id is
    deliberately *not* part of the key: tied rows keep distinct ids. -/
structure Row where
  key : Key
  payload : Int
  id : Nat
  deriving DecidableEq, Repr

def SortedKey (spec : Spec) : List Row → Prop :=
  List.Pairwise (fun a b : Row => keyLe spec a.key b.key = true)

def SortedStreams (spec : Spec) (streams : List (List Row)) : Prop :=
  ∀ s ∈ streams, SortedKey spec s

/-- Stable insertion: `r` goes after all rows whose key is ≤ its key. -/
def insort (spec : Spec) (r : Row) : List Row → List Row
  | [] => [r]
  | x :: xs => if better spec x.key r.key = true then x :: insort spec r xs else r :: x :: xs

def isort (spec : Spec) : List Row → List Row
  | [] => []
  | r :: rs => insort spec r (isort spec rs)

theorem insort_mem (spec : Spec) (r : Row) :
    ∀ (rs : List Row) (x : Row), x ∈ insort spec r rs → x = r ∨ x ∈ rs := by
  intro rs
  induction rs with
  | nil =>
      intro x hx
      simp [insort] at hx
      exact Or.inl hx
  | cons y ys ih =>
      intro x hx
      by_cases hy : better spec y.key r.key = true
      · simp [insort, hy] at hx
        rcases hx with hxy | hxins
        · exact Or.inr (by simpa [hxy])
        · rcases ih x hxins with hxr | hxys
          · exact Or.inl hxr
          · exact Or.inr (by simpa [hxys])
      · simp [insort, hy] at hx
        rcases hx with hxr | hxy | hxys
        · exact Or.inl hxr
        · exact Or.inr (by simpa [hxy])
        · exact Or.inr (by simpa [hxys])

theorem insort_sorted (spec : Spec) (r : Row) :
    ∀ (rs : List Row), SortedKey spec rs → SortedKey spec (insort spec r rs) := by
  intro rs hs
  induction rs with
  | nil => simp [insort, SortedKey]
  | cons x xs ih =>
      have hx_all : ∀ y ∈ xs, keyLe spec x.key y.key = true := (List.pairwise_cons.mp hs).1
      have htail : SortedKey spec xs := (List.pairwise_cons.mp hs).2
      by_cases hxr : better spec x.key r.key = true
      · have hxr' : (keyLe spec x.key r.key && !keyLe spec r.key x.key) = true := by
          simpa [better] using hxr
        have hx_le_r : keyLe spec x.key r.key = true := (Bool.and_eq_true _ _).mp hxr' |>.1
        simp [insort, hxr]
        apply List.Pairwise.cons
        · intro y hy
          rcases insort_mem spec r xs y hy with rfl | hyxs
          · exact hx_le_r
          · exact hx_all y hyxs
        · exact ih htail
      · have hxrf : better spec x.key r.key = false := by
          cases hb : better spec x.key r.key
          · rfl
          · exact False.elim (hxr hb)
        have hrx : keyLe spec r.key x.key = true := not_better_means_ge spec hxrf
        have hr_all : ∀ y ∈ xs, keyLe spec r.key y.key = true := by
          intro y hy
          exact keyLe_trans spec r.key x.key y.key hrx (hx_all y hy)
        simp [insort, hxr]
        apply List.Pairwise.cons
        · intro y hy
          rcases List.mem_cons.mp hy with rfl | hyxs
          · exact hrx
          · exact hr_all y hyxs
        · exact List.Pairwise.cons hx_all htail

theorem isort_sorted (spec : Spec) (rs : List Row) : SortedKey spec (isort spec rs) := by
  induction rs with
  | nil => simp [isort, SortedKey]
  | cons r rs ih => simpa [isort] using insort_sorted spec r (isort spec rs) ih

/-- The stable global merge of all inputs. -/
def mergeStreams (spec : Spec) (streams : List (List Row)) : List Row :=
  isort spec streams.flatten

theorem mergeStreams_sorted (spec : Spec) (streams : List (List Row)) :
    SortedKey spec (mergeStreams spec streams) := by
  simpa [mergeStreams] using isort_sorted spec streams.flatten

/- ------------------------------------------------------------------------ -
    List helpers used by the stop argument
    ------------------------------------------------------------------------ -/

theorem filter_eq_takeWhile_of_sorted (spec : Spec) {b : Key} {rs : List Row}
    (hs : SortedKey spec rs) :
    rs.filter (fun r => keyLe spec r.key b = true) =
    rs.takeWhile (fun r => keyLe spec r.key b = true) := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      have hr_all : ∀ y ∈ rs, keyLe spec r.key y.key = true := (List.pairwise_cons.mp hs).1
      have htail : SortedKey spec rs := (List.pairwise_cons.mp hs).2
      by_cases hrb : keyLe spec r.key b = true
      · have hcond : decide (keyLe spec r.key b = true) = true := by simp [hrb]
        rw [List.filter_cons, List.takeWhile_cons, if_pos hcond, if_pos hcond]
        exact congrArg (fun xs => r :: xs) (ih htail)
      · have hnone : ∀ y ∈ rs, keyLe spec y.key b = false := by
          intro y hy
          cases hle : keyLe spec y.key b with
          | false => rfl
          | true => exact False.elim (hrb (keyLe_trans spec r.key y.key b (hr_all y hy) hle))
        have hf : rs.filter (fun y => keyLe spec y.key b = true) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro a ha
          have hf' := hnone a ha
          rw [hf']
          decide
        have hcond : ¬ decide (keyLe spec r.key b = true) = true := by simp [hrb]
        rw [List.filter_cons, List.takeWhile_cons, if_neg hcond, if_neg hcond]
        exact hf

theorem takeWhile_eq_take_length (p : Row → Bool) (l : List Row) :
    l.takeWhile p = l.take ((l.takeWhile p).length) := by
  induction l with
  | nil => rfl
  | cons y ys ih =>
      by_cases hp : p y = true
      · rw [List.takeWhile_cons, if_pos hp]
        simp only [List.length_cons]
        rw [List.take_succ_cons]
        exact congrArg (fun xs => y :: xs) ih
      · rw [List.takeWhile_cons, if_neg hp]
        rfl

theorem takeWhile_length_le (p : Row → Bool) (l : List Row) :
    (l.takeWhile p).length ≤ l.length := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      by_cases hp : p y = true <;> simp [hp] at ih ⊢ <;> omega

theorem drop_takeWhile_len (p : Row → Bool) (l : List Row) :
    l.drop ((l.takeWhile p).length) = l.dropWhile p := by
  have h := List.takeWhile_append_dropWhile (p := p) (l := l)
  rw [← h, List.drop_append]
  simp

theorem takeWhile_subset (p : Row → Bool) (l : List Row) (x : Row) :
    x ∈ l.takeWhile p → x ∈ l := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      intro hx
      by_cases hp : p y = true
      · simp [hp] at hx
        rcases hx with rfl | hx
        · simp
        · exact List.mem_cons_of_mem y (ih hx)
      · simp [hp] at hx

theorem mem_drop_subset (l : List Row) (n : Nat) (x : Row) : x ∈ l.drop n → x ∈ l := by
  induction n generalizing l with
  | zero => intro hx; exact hx
  | succ n ih =>
      cases l with
      | nil => simp
      | cons y ys =>
          intro hx
          exact List.mem_cons_of_mem y (ih ys hx)

theorem better_of_dropWhile (spec : Spec) {b : Key} {rs : List Row}
    (hs : SortedKey spec rs) :
    ∀ r ∈ rs.dropWhile (fun r => keyLe spec r.key b = true),
      better spec b r.key = true := by
  induction rs with
  | nil => intro r hr; simp at hr
  | cons y ys ih =>
      have hy_all : ∀ z ∈ ys, keyLe spec y.key z.key = true := (List.pairwise_cons.mp hs).1
      have htail : SortedKey spec ys := (List.pairwise_cons.mp hs).2
      intro r hr
      by_cases hyb : keyLe spec y.key b = true
      · simp [hyb] at hr
        exact ih htail r hr
      · have hybf : keyLe spec y.key b = false := by
          cases hb : keyLe spec y.key b
          · rfl
          · exact False.elim (hyb hb)
        simp [hyb] at hr
        rcases hr with rfl | hry
        · exact keyLe_of_not_ge spec hybf
        · have hnot : keyLe spec r.key b = false := by
            cases hle : keyLe spec r.key b with
            | false => rfl
            | true => exact False.elim (hyb (keyLe_trans spec y.key r.key b (hy_all r hry) hle))
          exact keyLe_of_not_ge spec hnot

/- ------------------------------------------------------------------------ -
    Reference (full scan) vs watermark (early stop)
    ------------------------------------------------------------------------ -/

/-- Key of the k-th row after the offset; the watermark boundary. -/
def boundaryKey (after : List Row) (k : Nat) : Key :=
  match after.take k with
  | [] => []
  | r :: rs => (rs.getLast?.getD r).key

/-- **Reference**: after `offset`, keep every row whose key is no worse than
    the boundary.  `List.filter` scans the whole remainder. -/
def refWithTies (spec : Spec) (offset k : Nat) (sorted : List Row) : List Row × Key :=
  if k = 0 then ([], []) else
  let after := sorted.drop offset
  if after.length ≤ k then (after, [])
  else
    let b := boundaryKey after k
    (after.filter (fun r => keyLe spec r.key b = true), b)

/-- **Optimized watermark**: stop at the first row strictly past the
    boundary, using `takeWhile`; also return the explicit read count. -/
def wmRead (spec : Spec) (offset k : Nat) (sorted : List Row) : List Row × Key × Nat :=
  if k = 0 then ([], [], 0) else
  let after := sorted.drop offset
  if after.length ≤ k then (after, [], sorted.length)
  else
    let b := boundaryKey after k
    let kept := after.takeWhile (fun r => keyLe spec r.key b = true)
    (kept, b, offset + kept.length)

/-- Zero demand has no output or boundary rows and consumes no page rows. -/
theorem fetch_zero (spec : Spec) (offset : Nat) (rows : List Row) :
    (refWithTies spec offset 0 rows).1 = [] ∧
    wmRead spec offset 0 rows = ([], [], 0) := by
  exact ⟨rfl, rfl⟩

/- ------------------------------------------------------------------------ -
    Main theorems
    ------------------------------------------------------------------------ -/

/-- **Main theorem.** The early-stop watermark emits exactly the same
    payload sequence as the full-scan reference, with the same determinable
    watermark; the emitted rows form the determinable prefix of the merged
    order. -/
theorem tie_watermark_sound (spec : Spec) (offset k : Nat)
    (streams : List (List Row)) :
    let sorted := mergeStreams spec streams
    let after := sorted.drop offset
    let (outR, bR) := refWithTies spec offset k sorted
    let (outO, bO, _) := wmRead spec offset k sorted
    outO = outR ∧ bO = bR ∧
    (∀ r ∈ outO, r ∈ sorted) ∧ after.take outO.length = outO := by
  let sorted := mergeStreams spec streams
  let after := sorted.drop offset
  change (let (outR, bR) := refWithTies spec offset k sorted
          let (outO, bO, _) := wmRead spec offset k sorted
          outO = outR ∧ bO = bR ∧ (∀ r ∈ outO, r ∈ sorted) ∧
            after.take outO.length = outO)
  by_cases hz : k = 0
  · subst k
    simp [refWithTies, wmRead]
  dsimp [refWithTies, wmRead]
  simp only [if_neg hz]
  by_cases hsmall : after.length ≤ k
  · rw [if_pos hsmall, if_pos hsmall]
    dsimp
    refine ⟨rfl, rfl, ?_, ?_⟩
    · intro r hr
      exact mem_drop_subset sorted offset r hr
    · dsimp [after]
      exact List.take_length
  · let b := boundaryKey after k
    let kept := after.takeWhile (fun r => keyLe spec r.key b = true)
    have hs : SortedKey spec after := List.Pairwise.drop (mergeStreams_sorted spec streams)
    have hfilter : after.filter (fun r => keyLe spec r.key b = true) = kept := by
      dsimp [kept]
      exact filter_eq_takeWhile_of_sorted spec hs
    have hsub : ∀ r ∈ kept, r ∈ sorted := by
      intro r hr
      exact mem_drop_subset sorted offset r
        (takeWhile_subset (fun r => keyLe spec r.key b = true) after r (by simpa [kept] using hr))
    have hpref : after.take kept.length = kept := by
      dsimp [kept]
      exact (takeWhile_eq_take_length (fun r => keyLe spec r.key b = true) after).symm
    rw [if_neg hsmall, if_neg hsmall]
    change kept = after.filter (fun r => keyLe spec r.key b = true) ∧ b = b ∧
      (∀ r ∈ kept, r ∈ sorted) ∧ after.take kept.length = kept
    exact ⟨hfilter.symm, rfl, hsub, hpref⟩

/-- Rows strictly past the boundary are never read, in any input stream. -/
theorem tie_watermark_read_bound (spec : Spec) (offset k : Nat) (hk : k ≠ 0)
    (streams : List (List Row)) :
    let sorted := mergeStreams spec streams
    let (outO, bO, reads) := wmRead spec offset k sorted
    reads ≤ sorted.length ∧
    (∀ r ∈ sorted.drop reads, better spec bO r.key = true) ∧
    (∀ s ∈ streams, ∀ r ∈ s, r ∈ sorted.drop reads → better spec bO r.key = true) := by
  let sorted := mergeStreams spec streams
  let after := sorted.drop offset
  let b := boundaryKey after k
  let kept := after.takeWhile (fun r => keyLe spec r.key b = true)
  change (let (_, bO, reads) := wmRead spec offset k sorted
          reads ≤ sorted.length ∧
          (∀ r ∈ sorted.drop reads, better spec bO r.key = true) ∧
          (∀ s ∈ streams, ∀ r ∈ s, r ∈ sorted.drop reads → better spec bO r.key = true))
  dsimp [wmRead]
  simp only [if_neg hk]
  by_cases hsmall : after.length ≤ k
  · rw [if_pos hsmall]
    simp
  · rw [if_neg hsmall]
    change (offset + kept.length ≤ sorted.length ∧
      (∀ r ∈ sorted.drop (offset + kept.length), better spec b r.key = true) ∧
      (∀ s ∈ streams, ∀ r ∈ s, r ∈ sorted.drop (offset + kept.length) →
        better spec b r.key = true))
    have hs : SortedKey spec after := List.Pairwise.drop (mergeStreams_sorted spec streams)
    have hlen_kept : kept.length ≤ after.length := by
      dsimp [kept]
      exact takeWhile_length_le (fun r => keyLe spec r.key b = true) after
    have hlen_after : after.length = sorted.length - offset := by
      simp [after]
    have hpos : 0 < after.length := by omega
    have hoff_le : offset ≤ sorted.length := by omega
    have hsum : (sorted.length - offset) + offset = sorted.length := Nat.sub_add_cancel hoff_le
    have hdrop : sorted.drop (offset + kept.length) = after.drop kept.length := by
      dsimp [after]
      rw [List.drop_drop]
    have hdropWhile : after.drop kept.length =
        after.dropWhile (fun r => keyLe spec r.key b = true) := by
      dsimp [kept]
      exact drop_takeWhile_len (fun r => keyLe spec r.key b = true) after
    have hworse : ∀ r ∈ sorted.drop (offset + kept.length), better spec b r.key = true := by
      rw [hdrop, hdropWhile]
      exact better_of_dropWhile spec hs
    constructor
    · omega
    constructor
    · exact hworse
    · intro s hsStream r hrs hrdrop
      exact hworse r hrdrop

/- ------------------------------------------------------------------------ -
    Counterexample: local `take k` loses a large tied block
    ------------------------------------------------------------------------ -/

def kSpec : Spec := [(Dir.ASC, Nulls.Last)]
def kk (n : Int) : Key := [some n]

def tiedStream : List Row :=
  [ { key := kk 1, payload := 0, id := 0 },
    { key := kk 2, payload := 100, id := 1 },
    { key := kk 1, payload := 2, id := 2 },
    { key := kk 1, payload := 3, id := 3 },
    { key := kk 1, payload := 4, id := 4 } ]

def sortedTied : List Row := mergeStreams kSpec [tiedStream]

/-- **Counterexample:** `k = 1` with WITH TIES must emit all four rows tied
    at the boundary key; a local `take 1` keeps only the first one and loses
    the block of three tied rows (including the one at the very end). -/
theorem local_take_k_loses_tied_block_counterexample :
    (refWithTies kSpec 0 1 sortedTied).1 =
      [ { key := kk 1, payload := 0, id := 0 },
        { key := kk 1, payload := 2, id := 2 },
        { key := kk 1, payload := 3, id := 3 },
        { key := kk 1, payload := 4, id := 4 } ] ∧
    (refWithTies kSpec 0 1 sortedTied).1.map Row.payload = [0, 2, 3, 4] ∧
    sortedTied.take 1 = [ { key := kk 1, payload := 0, id := 0 } ] := by
  native_decide

/-- Tied rows are distinguished by appearance id, never merged by the
    watermark's key equality. -/
theorem tied_rows_keep_distinct_ids :
    (refWithTies kSpec 0 1 sortedTied).1.map Row.id = [0, 2, 3, 4] := by
  native_decide

end TinylambOpt.Research.TieWatermark
