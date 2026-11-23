import Std.Tactic

/-!
# Adaptive memoization across a join-order switch (`adaptive-memo`)

The reference plan is a probe-outer inner join over a commutative region:
for every probe occurrence it emits one `(build occurrence id, probe
occurrence id)` pair per matching build occurrence.  The optimized plan
processes a prefix of the probes with that same plan, memoizes the emitted
**combinations**, then switches to a build-outer join for the residual probes
(using the next snapshot).  Because build rows may share a value, the memo
key must be the occurrence id, never the value: keying by value would drop a
duplicate build row.

The model carries the four pieces of state required by the issue: already
emitted combinations (`emitted`), pending probes (`pending`), the next
snapshot (`snapshot`) and the first error observed so far (`firstErr`).

C++ counterpart: `plan/cascades_search.cpp`.
-/

namespace TinylambOpt.Research.AdaptiveMemo

/-- A build row carries an occurrence id and a join value. -/
structure BRow where
  id : Nat
  val : Nat
  deriving DecidableEq, Repr

/-- A probe row carries an occurrence id, a join value and a failure flag. -/
structure PRow where
  id : Nat
  val : Nat
  failing : Bool
  deriving DecidableEq, Repr

/-- An emitted combination identifies both occurrences (not their values). -/
abbrev Combo := Nat × Nat

/-- Predicate used by the commutative inner-join region. -/
def rel (b : BRow) (p : PRow) : Bool := b.val == p.val

def proj (b : BRow) (p : PRow) : Combo := (b.id, p.id)

/-! ## Reference (fixed plan) computation -/

def emit (build : List BRow) (p : PRow) : List Combo :=
  (build.filter (fun b => rel b p)).map (fun b => proj b p)

/-- Fixed plan: probe outer, build inner. -/
def refJoin (build : List BRow) (probes : List PRow) : List Combo :=
  probes.flatMap (emit build)

/-- First failing probe in input order. -/
def firstErr : List PRow → Option Nat
  | [] => none
  | p :: ps => if p.failing then some p.id else firstErr ps

def firstOr (a b : Option Nat) : Option Nat :=
  match a with
  | some _ => a
  | none => b

/-- Reference result: output bag plus first error. -/
def refRun (build : List BRow) (probes : List PRow) : List Combo × Option Nat :=
  (refJoin build probes, firstErr probes)

theorem firstErr_append (l₁ l₂ : List PRow) :
    firstErr (l₁ ++ l₂) = firstOr (firstErr l₁) (firstErr l₂) := by
  induction l₁ with
  | nil => simp [firstErr, firstOr]
  | cons p ps ih =>
      by_cases h : p.failing = true <;> simp [firstErr, firstOr, h, ih]

theorem refJoin_append (build : List BRow) (l₁ l₂ : List PRow) :
    refJoin build (l₁ ++ l₂) = refJoin build l₁ ++ refJoin build l₂ := by
  simp [refJoin, List.flatMap_append]

/-! ## Occurrence-based memo state and the switched (optimized) computation -/

structure MemoState where
  emitted : List Combo
  pending : List PRow
  snapshot : List BRow
  firstErr : Option Nat
  deriving DecidableEq, Repr

def startState (build : List BRow) (probes : List PRow) : MemoState :=
  ⟨[], probes, build, none⟩

/-- Captured state after the fixed-plan prefix. -/
def capture (build : List BRow) (k : Nat) (probes : List PRow) : MemoState :=
  ⟨refJoin build (probes.take k), probes.drop k, build, firstErr (probes.take k)⟩

/-- Switched phase: build outer over the residual probes. -/
def residualBuildOuter (st : MemoState) : List Combo :=
  st.snapshot.flatMap (fun b =>
    st.pending.filterMap (fun p => if rel b p then some (proj b p) else none))

/-- Optimized result: memoized combinations ++ residual combinations, with
    the first error taken in input order (prefix before residual). -/
def switchRun (build : List BRow) (probes : List PRow) (k : Nat) : List Combo × Option Nat :=
  let st := capture build k probes
  (st.emitted ++ residualBuildOuter st,
   firstOr st.firstErr (firstErr st.pending))

/-! ## Bag preservation -/

/-- A single build row emits the same combinations from either side. -/
private def fmapSingle (b : BRow) (p : PRow) : List Combo :=
  if rel b p then [proj b p] else []

private theorem filterMap_eq_flatMap (b : BRow) (ps : List PRow) :
    ps.filterMap (fun p => if rel b p then some (proj b p) else none) =
    ps.flatMap (fun p => fmapSingle b p) := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
      by_cases h : rel b p = true <;> simp [h, fmapSingle, ih]

private theorem perm_flatMap_append_distr {α : Type} (bs : List PRow)
    (g h : PRow → List α) :
    (bs.flatMap (fun b => g b ++ h b)).Perm (bs.flatMap g ++ bs.flatMap h) := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.flatMap_cons]
      let fg := bs.flatMap g
      let fh := bs.flatMap h
      have ih' : (bs.flatMap (fun b => g b ++ h b)).Perm (fg ++ fh) := by
        simpa [fg, fh] using ih
      have pswap : (h b ++ fg ++ fh).Perm (fg ++ h b ++ fh) := by
        simpa [List.append_assoc] using
          (List.Perm.append_right fh (List.perm_append_comm : (h b ++ fg).Perm (fg ++ h b)))
      have hmid : (g b ++ h b ++ (fg ++ fh)).Perm ((g b ++ fg) ++ (h b ++ fh)) := by
        simpa [List.append_assoc] using (List.Perm.append_left (g b) pswap)
      have h1 : (g b ++ h b ++ bs.flatMap (fun b => g b ++ h b)).Perm
          (g b ++ h b ++ (fg ++ fh)) := List.Perm.append_left _ ih'
      exact h1.trans (by simpa [fg, fh] using hmid)

private theorem filter_map_eq_filterMap {α β : Type} (p : α → Bool) (f : α → β) :
    ∀ l : List α, (l.filter p).map f = l.filterMap (fun x => if p x then some (f x) else none)
  | [] => rfl
  | x :: xs => by
      by_cases h : p x = true <;> simp [h, filter_map_eq_filterMap p f xs]

private theorem emit_eq_filterMap (build : List BRow) (p : PRow) :
    emit build p = build.filterMap (fun b => if rel b p then some (proj b p) else none) := by
  simpa [emit, proj] using
    (filter_map_eq_filterMap (fun b : BRow => rel b p) (fun b : BRow => proj b p) build)

/-- Probe-outer and build-outer enumeration are permutations of each other. -/
private theorem perm_transpose_build :
    ∀ (build : List BRow) (probes : List PRow),
      ((build.flatMap (fun b => probes.filterMap
          (fun p => if rel b p then some (proj b p) else none))).Perm
       (probes.flatMap (fun p => build.filterMap
          (fun b => if rel b p then some (proj b p) else none)))) := by
  intro build
  induction build with
  | nil =>
      intro probes
      simp
  | cons b bs ih =>
      intro probes
      have hf : probes.filterMap (fun p => if rel b p then some (proj b p) else none) =
          probes.flatMap (fun p => fmapSingle b p) := filterMap_eq_flatMap b probes
      have hperm := ih probes
      simp only [List.flatMap_cons]
      rw [hf]
      have hsplit := perm_flatMap_append_distr probes
          (fun p => fmapSingle b p)
          (fun p => bs.filterMap (fun b' => if rel b' p then some (proj b' p) else none))
      have hmain : ((probes.flatMap (fun p => fmapSingle b p) ++
          bs.flatMap (fun b' => probes.filterMap (fun p => if rel b' p then some (proj b' p) else none))).Perm
          (probes.flatMap (fun p => fmapSingle b p) ++
          probes.flatMap (fun p => bs.filterMap (fun b' => if rel b' p then some (proj b' p) else none)))) :=
        List.Perm.append_left _ hperm
      have hfcons : ∀ p : PRow,
          (b :: bs).filterMap (fun b' => if rel b' p then some (proj b' p) else none) =
          fmapSingle b p ++ bs.filterMap (fun b' => if rel b' p then some (proj b' p) else none) := by
        intro p
        by_cases h : rel b p = true <;> simp [h, fmapSingle]
      simpa [hfcons, List.append_assoc] using hmain.trans hsplit.symm

theorem perm_build_outer (build : List BRow) (probes : List PRow) :
    (residualBuildOuter ⟨[], probes, build, none⟩).Perm (refJoin build probes) := by
  have href : refJoin build probes =
      probes.flatMap (fun p => build.filterMap (fun b => if rel b p then some (proj b p) else none)) := by
    induction probes with
    | nil => rfl
    | cons p ps ih =>
        simp only [refJoin, List.flatMap_cons] at ih ⊢
        rw [ih, emit_eq_filterMap]
  rw [href]
  exact perm_transpose_build build probes

theorem switch_bag_eq (build : List BRow) (probes : List PRow) (k : Nat) :
    ((switchRun build probes k).1).Perm (refJoin build probes) := by
  let st := capture build k probes
  have hsplit : refJoin build probes = refJoin build (probes.take k) ++ refJoin build (probes.drop k) := by
    have ht := List.take_append_drop k probes
    calc
      refJoin build probes = refJoin build (probes.take k ++ probes.drop k) := by rw [ht]
      _ = refJoin build (probes.take k) ++ refJoin build (probes.drop k) := refJoin_append _ _ _
  have hres : (residualBuildOuter st).Perm (refJoin build (probes.drop k)) := by
    simpa [st, capture, residualBuildOuter] using perm_build_outer build (probes.drop k)
  have happ : (refJoin build (probes.take k) ++ residualBuildOuter st).Perm
      (refJoin build (probes.take k) ++ refJoin build (probes.drop k)) :=
    List.Perm.append_left _ hres
  simpa [switchRun, st, capture, hsplit] using happ

/-! ## First-error preservation -/

theorem switch_first_err_eq (build : List BRow) (probes : List PRow) (k : Nat) :
    (switchRun build probes k).2 = firstErr probes := by
  have h := firstErr_append (probes.take k) (probes.drop k)
  have ht : probes.take k ++ probes.drop k = probes := List.take_append_drop k probes
  rw [ht] at h
  simpa [switchRun, capture, firstOr] using h.symm

/-! ## Cost bound: extra probe events ≤ unprocessed candidates -/

/-- One equality test per (build row, residual probe) pair in the switched phase. -/
def probeEvents : List BRow → List PRow → Nat
  | [], _ => 0
  | _ :: bs, ps => ps.length + probeEvents bs ps

/-- Unprocessed candidate pairs: residual probes × snapshot rows. -/
def candidates (st : MemoState) : Nat := st.snapshot.length * st.pending.length

theorem probeEvents_eq (build : List BRow) (probes : List PRow) :
    probeEvents build probes = build.length * probes.length := by
  induction build with
  | nil => simp [probeEvents]
  | cons b bs ih => simp [probeEvents, ih, Nat.succ_mul, Nat.add_comm]

theorem extra_probes_le_candidates (build : List BRow) (probes : List PRow) (k : Nat) :
    probeEvents build (probes.drop k) ≤ candidates (capture build k probes) := by
  simp [candidates, capture, probeEvents_eq]

/-! ## Main theorem -/

/-- Unique occurrence ids on both sides (the hypothesis that makes occurrence
    memoization well-defined). -/
def IdsUniqueB (build : List BRow) : Prop :=
  build.Pairwise (fun b₁ b₂ => b₁.id ≠ b₂.id)

def IdsUniqueP (probes : List PRow) : Prop :=
  probes.Pairwise (fun p₁ p₂ => p₁.id < p₂.id)

/-- Switching the join order after any prefix preserves the output bag and
    the first error, and the switched phase performs no more probe events
    than there are unprocessed candidates. -/
theorem adaptive_memo_sound
    (build : List BRow) (probes : List PRow) (k : Nat)
    (_hB : IdsUniqueB build) (_hP : IdsUniqueP probes) :
    let r := refRun build probes
    let o := switchRun build probes k
    (o.1).Perm r.1 ∧ o.2 = r.2 ∧
      probeEvents build (probes.drop k) ≤ candidates (capture build k probes) := by
  dsimp [refRun, switchRun]
  exact ⟨switch_bag_eq build probes k, switch_first_err_eq build probes k,
    extra_probes_le_candidates build probes k⟩

/-! ## Counterexample: key-level done set drops a duplicate -/

/-- Naive residual computation keyed by build **value** instead of build
    occurrence id. -/
def keyLevelResidual (doneVals : List Nat) (build : List BRow) (probes : List PRow) : List Combo :=
  probes.flatMap (fun p =>
    (build.filter (fun b => rel b p && b.val ∉ doneVals)).map (fun b => proj b p))

/-- Correct residual computation keyed by build **occurrence id**. -/
def occLevelResidual (doneIds : List Nat) (build : List BRow) (probes : List PRow) : List Combo :=
  probes.flatMap (fun p =>
    (build.filter (fun b => rel b p && b.id ∉ doneIds)).map (fun b => proj b p))

/-- Two build rows share value 7 and one combination `(1,1)` was already
    emitted.  A key-level done set `[7]` drops the still-unemitted occurrence
    2, whereas occurrence-level memoization `[1]` keeps it. -/
theorem key_level_done_set_counterexample :
    keyLevelResidual [7] [⟨1, 7⟩, ⟨2, 7⟩] [⟨1, 7, false⟩] = [] ∧
    occLevelResidual [1] [⟨1, 7⟩, ⟨2, 7⟩] [⟨1, 7, false⟩] = [(2, 1)] ∧
    refJoin [⟨1, 7⟩, ⟨2, 7⟩] [⟨1, 7, false⟩] = [(1, 1), (2, 1)] := by
  decide

end TinylambOpt.Research.AdaptiveMemo
