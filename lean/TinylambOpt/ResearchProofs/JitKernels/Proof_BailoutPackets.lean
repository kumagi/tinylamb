import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

/-! Proofs and counterexamples for strip-mined bailout packets. Positive
results preserve selected ordinals and the successful prefix before the first
fault. Counterexamples show that selection order is not source order, null
outputs do not measure physical progress, lane-major evaluation changes fault
priority, and packet-aligned replay can skip observable output. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Proof_BailoutPackets

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

private theorem findSome?_map_option {α β γ : Type} (f : α → Option β)
    (g : β → γ) (xs : List α) :
    xs.findSome? (fun x => (f x).map g) = (xs.findSome? f).map g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases hx : f x with
      | none => simp [List.findSome?_cons, hx, ih]
      | some y => simp [List.findSome?_cons, hx]

private theorem first_cons (x : Cell) (xs : List Cell) :
    first (x :: xs) = match x with
      | .error e => some (0, e)
      | .ok _ => (first xs).map (fun p => (p.1 + 1, p.2)) := by
  cases x with
  | error e => rfl
  | ok v =>
      simp only [first, List.zipIdx_cons, List.findSome?_cons, bad,
        Bool.false_eq_true, ↓reduceIte]
      rw [List.zipIdx_succ, List.findSome?_map]
      exact findSome?_map_option _ _ _

private theorem first_eq_none_iff (xs : List Cell) :
    first xs = none ↔ ∀ c ∈ xs, !bad c := by
  induction xs with
  | nil => simp [first]
  | cons x xs ih =>
      cases x with
      | error e => simp [first_cons, bad]
      | ok v =>
          simp only [first_cons, Option.map_eq_none_iff, List.mem_cons,
            forall_eq_or_imp, bad, Bool.not_false, true_and]
          exact ih

private theorem prefixOut_append_of_first_none (a b : List Cell)
    (ha : first a = none) : prefixOut (a ++ b) = prefixOut a ++ prefixOut b := by
  induction a with
  | nil => simp [prefixOut]
  | cons x xs ih =>
      have hx : !bad x := (first_eq_none_iff (x :: xs)).mp ha x (by simp)
      have hxs : first xs = none := by
        cases hx' : x with
        | error e => simp [first_cons, hx'] at ha
        | ok v => simpa [first_cons, hx'] using ha
      cases x with
      | error e => simp [bad] at hx
      | ok v =>
          simp only [List.cons_append, prefixOut, List.takeWhile_cons, hx,
            bad, Bool.not_false, ↓reduceIte, List.filterMap_cons]
          rw [show List.takeWhile (fun c => !bad c) (xs ++ b) =
              List.takeWhile (fun c => !bad c) xs ++
                List.takeWhile (fun c => !bad c) b from ?_]
          · simp [List.filterMap_append, ih hxs]
          · exact List.takeWhile_append_of_forall_not (by
              intro c hc
              exact (first_eq_none_iff xs).mp hxs c hc)

private theorem first_gather_inv (xs : List Cell) (sel : List Nat) (j e : Nat)
    (h : first (gather xs sel) = some (j, e)) :
    ∃ i, sel[j]? = some i ∧ xs[i]? = some (.error e) := by
  induction sel generalizing j with
  | nil => simp [gather, first] at h
  | cons i sel ih =>
      cases hx : xs[i]? with
      | none =>
          simp only [gather, List.filterMap_cons, hx, first] at h
          have h' := ih (j - 1) h
          obtain ⟨i', hi', hx'⟩ := h'
          have hj : 0 < j := by
            by_contra hn
            have : j = 0 := by omega
            subst j
            simp at h
          exact ⟨i', by simpa [List.getElem?_cons_succ, Nat.add_sub_cancel] using hi', hx'⟩
      | some c =>
          cases c with
          | error z =>
              simp only [gather, List.filterMap_cons, hx, first_cons,
                Option.some.injEq, Prod.mk.injEq] at h
              obtain ⟨rfl, rfl⟩ := h
              exact ⟨i, by simp, hx⟩
          | ok v =>
              simp only [gather, List.filterMap_cons, hx, first_cons,
                Option.map_eq_some_iff, Prod.mk.injEq] at h
              obtain ⟨⟨j', e'⟩, hrest, heq⟩ := h
              have hj : j = j' + 1 := by omega
              have he : e = e' := by omega
              subst j
              subst e
              have h' := ih j' hrest
              obtain ⟨i', hi', hx'⟩ := h'
              exact ⟨i', by simpa [List.getElem?_cons_succ] using hi', hx'⟩

/-- A compacted packet fault still identifies the selected source row. -/
theorem compactedBailoutOrdinal : CompactedBailoutOrdinal.Goal := by
  intro xs sel j e _ h
  exact first_gather_inv xs sel j e h

/-- Selection order need not be physical source order. -/
theorem lowestSourceOrdinalIsFirstFault_refute :
    ¬ LowestSourceOrdinalIsFirstFault.Goal := by
  intro h
  let xs : List Cell := [.ok (some 0), .error 8, .error 7]
  let sel := [2, 1]
  have hf : first (gather xs sel) = some (0, 7) := by decide
  have hbad := h xs sel 0 7 hf 1 1 8 rfl rfl
  simp [sel] at hbad

/-- Successful packets before any fault contribute an unchanged prefix. -/
theorem wordOfFaultsReplayPrefix : WordOfFaultsReplayPrefix.Goal := by
  intro a b ha
  exact prefixOut_append_of_first_none a b ha

/-- Separate lane-major passes do not preserve scalar first-fault priority. -/
theorem laneMajorSitePriority_refute : ¬ LaneMajorSitePriority.Goal := by
  intro h
  let a : List Cell := [.ok (some 0), .error 1]
  let b : List Cell := [.error 2, .ok (some 0)]
  have heq := h a b rfl
  simp [a, b, first, bad] at heq

/-- Intersecting the fault ballot with selected ordinals preserves fault presence. -/
theorem errorBitsSelectedOnly : ErrorBitsSelectedOnly.Goal := by
  intro xs sel _
  induction sel with
  | nil => simp [gather, first]
  | cons i sel ih =>
      cases hx : xs[i]? with
      | none => simp [gather, hx, first_cons, ih]
      | some c =>
          cases c with
          | error e => simp [gather, hx, first_cons, bad]
          | ok v => simp [gather, hx, first_cons, bad, ih]

/-- NULL-producing successful lanes make output length smaller than row progress. -/
theorem successfulNullLaneProgress_refute :
    ¬ SuccessfulNullLaneProgress.Goal := by
  intro h
  have heq := h [.ok none, .error 5] 1 5 (by decide)
  simp [prefixOut, first] at heq

/-- Replay beginning at the first fault observes that fault in position zero. -/
theorem coldReplayAtFirstFault : ColdReplayAtFirstFault.Goal := by
  intro xs i e h
  induction xs generalizing i with
  | nil => simp [first] at h
  | cons x xs ih =>
      cases hx : x with
      | error z =>
          simp only [first_cons, hx, Option.some.injEq, Prod.mk.injEq] at h
          obtain ⟨rfl, rfl⟩ := h
          simp [hx]
      | ok v =>
          simp only [first_cons, hx, Option.map_eq_some_iff, Prod.mk.injEq] at h
          obtain ⟨⟨j, z⟩, hrest, heq⟩ := h
          have hi : i = j + 1 := by omega
          have he : e = z := by omega
          subst i
          subst e
          have ht := ih j hrest
          simpa [List.drop_succ_cons] using ht

/-- Only materializable values before the first fault are committed. -/
theorem scatterEarlierSuccessesOnly : ScatterEarlierSuccessesOnly.Goal := by
  intro xs i e h
  induction xs generalizing i with
  | nil => simp [first] at h
  | cons x xs ih =>
      cases hx : x with
      | error z =>
          simp only [first_cons, hx, Option.some.injEq, Prod.mk.injEq] at h
          obtain ⟨rfl, rfl⟩ := h
          simp [prefixOut]
      | ok v =>
          simp only [first_cons, hx, Option.map_eq_some_iff, Prod.mk.injEq] at h
          obtain ⟨⟨j, z⟩, hrest, heq⟩ := h
          have hi : i = j + 1 := by omega
          have he : e = z := by omega
          subst i
          subst e
          have ht := ih j hrest
          simp only [prefixOut, List.takeWhile_cons, hx, bad, Bool.not_false,
            ↓reduceIte, List.filterMap_cons, List.take_succ_cons]
          rw [ht]

/-- Aligning replay forward can skip successes after the fault. -/
theorem misalignedPacketResume_refute : ¬ MisalignedPacketResume.Goal := by
  intro h
  let xs : List Cell := [.ok (some 10), .error 5, .ok (some 20), .ok (some 30)]
  have heq := h xs 1 5 2 (by decide) (by decide)
  simp [xs, prefixOut] at heq

end TinylambOpt.ResearchProofs.JitKernels.Proof_BailoutPackets
