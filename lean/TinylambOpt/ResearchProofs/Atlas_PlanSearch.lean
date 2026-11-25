import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas_PlanSearch

namespace PredicateOrder

open TinylambOpt.ResearchGoals.Atlas.PlanSearch.PredicateOrder

private def predAll (ts : List (Nat × (Nat → Bool))) (r : Nat) : Bool :=
  ts.all (fun t => t.2 r)

private def surv (ts : List (Nat × (Nat → Bool))) (rows : List Nat) : List Nat :=
  rows.filter (predAll ts)

private theorem surv_nil (rows : List Nat) : surv [] rows = rows := by
  unfold surv predAll
  simp only [List.all_nil]
  apply List.filter_eq_self.mpr
  intro r hr
  rfl

private theorem surv_cons (c : Nat) (p : Nat → Bool) (ts : List (Nat × (Nat → Bool)))
    (rows : List Nat) : surv ((c, p) :: ts) rows = surv ts (rows.filter p) := by
  unfold surv predAll
  rw [List.filter_filter]
  ext r
  simp [Bool.and_comm]

private theorem actual_append (ts us : List (Nat × (Nat → Bool))) (rows : List Nat) :
    actual (ts ++ us) rows = actual ts rows + actual us (surv ts rows) := by
  induction ts generalizing rows with
  | nil => rw [List.nil_append, surv_nil]; simp [actual]
  | cons t ts ih =>
      obtain ⟨c, p⟩ := t
      rw [List.cons_append]
      simp only [actual]
      rw [ih, surv_cons]
      omega

private theorem surv_length_le (ts : List (Nat × (Nat → Bool))) (rows : List Nat) :
    (surv ts rows).length ≤ rows.length := by
  unfold surv
  induction rows with
  | nil => simp
  | cons r rs ih =>
      by_cases h : predAll ts r = true
      · simp [h, ih]
      · simp [h]
        omega

private theorem countP_neg (p : Nat → Bool) (rows : List Nat) :
    rows.countP (fun x => !p x) + rows.countP p = rows.length := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      cases hpr : p r <;> simp [hpr] <;> omega

theorem ConditionalSwap_sound : ConditionalSwap.Goal := by
  intro ca cb pa pb rest rows
  have hA : actual ((ca, pa) :: (cb, pb) :: rest) rows =
      ca * rows.length + cb * (rows.filter pa).length +
        actual rest ((rows.filter pa).filter pb) := by
    simp [actual]
    omega
  have hB : actual ((cb, pb) :: (ca, pa) :: rest) rows =
      cb * rows.length + ca * (rows.filter pb).length +
        actual rest ((rows.filter pb).filter pa) := by
    simp [actual]
    omega
  have hff : (rows.filter pa).filter pb = (rows.filter pb).filter pa := by
    rw [List.filter_filter, List.filter_filter]
    apply List.filter_congr
    intro x hx
    simp [Bool.and_comm]
  rw [hA, hB, hff]
  have hcpa := countP_neg pa rows
  have hcpb := countP_neg pb rows
  have hnotpa : rows.countP (fun x => !pa x) = rows.length - rows.countP pa := by omega
  have hnotpb : rows.countP (fun x => !pb x) = rows.length - rows.countP pb := by omega
  have hfilterpa : (rows.filter pa).length = rows.countP pa := List.countP_eq_length_filter.symm
  have hfilterpb : (rows.filter pb).length = rows.countP pb := List.countP_eq_length_filter.symm
  rw [hfilterpa, hfilterpb, hnotpa, hnotpb]
  have hlepa : rows.countP pa ≤ rows.length := List.countP_le_length
  have hlepb : rows.countP pb ≤ rows.length := List.countP_le_length
  rw [Int.ofNat_sub hlepa, Int.ofNat_sub hlepb]
  grind

theorem RepeatedGuard_sound : RepeatedGuard.Goal := by
  intro c p middle tail rows
  have hs1 : surv middle (rows.filter p) =
      rows.filter (fun r => p r && middle.all (fun t => t.2 r)) := by
    unfold surv predAll
    rw [List.filter_filter]
    ext r
    simp [Bool.and_comm]
  have hfp : (surv middle (rows.filter p)).filter p = surv middle (rows.filter p) := by
    unfold surv predAll
    apply List.filter_eq_self.mpr
    intro r hr
    exact (List.mem_filter.mp (List.mem_filter.mp hr).1).2
  calc
    actual ((c, p) :: middle ++ (c, p) :: tail) rows
        = c * rows.length + actual (middle ++ (c, p) :: tail) (rows.filter p) := rfl
    _ = c * rows.length +
        (actual middle (rows.filter p) + actual ((c, p) :: tail) (surv middle (rows.filter p))) := by
          rw [actual_append]
    _ = c * rows.length +
        (actual middle (rows.filter p) +
          (c * (surv middle (rows.filter p)).length + actual tail ((surv middle (rows.filter p)).filter p))) := rfl
    _ = c * rows.length +
        (actual middle (rows.filter p) +
          (c * (surv middle (rows.filter p)).length + actual tail (surv middle (rows.filter p)))) := by
            rw [hfp]
    _ = (c * rows.length + (actual middle (rows.filter p) + actual tail (surv middle (rows.filter p)))) +
          c * (surv middle (rows.filter p)).length := by omega
    _ = actual ((c, p) :: middle ++ tail) rows +
          c * (rows.filter (fun r => p r && middle.all (fun t => t.2 r))).length := by
            rw [show actual ((c, p) :: middle ++ tail) rows =
                c * rows.length + (actual middle (rows.filter p) + actual tail (surv middle (rows.filter p))) by
                  rw [List.cons_append, actual, actual_append]]
            rw [hs1]

theorem DeferredPass_sound : DeferredPass.Goal := by
  intro c tests rows
  have hle := surv_length_le tests rows
  have hmul : c * (surv tests rows).length ≤ c * rows.length := Nat.mul_le_mul_left c hle
  calc
    actual (tests ++ [(c, fun _ => true)]) rows
        = actual tests rows + actual [(c, fun _ => true)] (surv tests rows) := by
            rw [actual_append]
    _ = actual tests rows + c * (surv tests rows).length := by rfl
    _ ≤ c * rows.length + actual tests rows := by omega
    _ = actual ((c, fun _ => true) :: tests) rows := by
          have hf : rows.filter (fun _ : Nat => true) = rows := by
            apply List.filter_eq_self.mpr
            intro r hr
            rfl
          simp [actual, hf]

theorem MarginalTie_refutation : ¬ MarginalTie.Goal := by
  intro h
  let rows := [0, 1]
  let gate := fun (x : Nat) => x == 0
  let a := fun (x : Nat) => x == 0
  let b := fun (x : Nat) => x == 1
  have hcount : rows.countP a = rows.countP b := by decide
  have hne : actual [(1, gate), (1, a), (1, b)] rows ≠
      actual [(1, gate), (1, b), (1, a)] rows := by decide
  exact hne (h rows a b gate hcount)

-- TODO(RatioSort): exchange argument for mergeSort optimality not yet formalised.

end PredicateOrder

namespace AlphabeticMerge

open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

-- TODO(OptimalTree): DP optimality not yet formalised.
-- TODO(MonotoneCut): quadrangle/argmin monotonicity not yet formalised.
-- TODO(Quadrangle): not yet formalised.
-- TODO(ReweightBound): not yet formalised.
-- TODO(ScaleReuse): not yet formalised.

end AlphabeticMerge

namespace ParallelCuts

open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

-- TODO(Feasible): greedy optimality not yet formalised.
-- TODO(CapacitySearch): not yet formalised.
-- TODO(ReverseScan): not yet formalised.
-- TODO(BoundarySlack): not yet formalised.
-- TODO(LocalCorrection): not yet formalised.

end ParallelCuts

namespace RiskEnvelope

open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

-- TODO(QuantileReserve): not yet formalised.
-- TODO(ReservationSlope): not yet formalised.
-- TODO(HistogramFold): not yet formalised.
-- TODO(RareWorkload): not yet formalised.
-- TODO(MeanReserve): not yet formalised.

end RiskEnvelope

end TinylambOpt.ResearchProofs.Atlas_PlanSearch
