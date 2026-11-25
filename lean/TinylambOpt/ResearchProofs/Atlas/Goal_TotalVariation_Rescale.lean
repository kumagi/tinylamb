import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_Rescale

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Exhaustive check on small integer pages and scale factors: `false` means
that no counterexample was found. -/
#eval
  let xs : List (List Int) :=
    [[], [0], [3], [0, 1], [1, 1], [-2, 3, 0], [5, -1, 4, 2], [7, 7, -3, 0, 6]]
  let as : List Int := [-3, -1, 0, 1, 2, 4]
  (as.flatMap (fun a => xs.map (fun x =>
    decide (TotalVariation.measure (scaled a x) = (a.natAbs : Int) * TotalVariation.measure x)))).contains false

private theorem natAbs_mul_int (a z : Int) :
    ((a * z).natAbs : Int) = (a.natAbs : Int) * (z.natAbs : Int) := by
  rw [Int.natAbs_mul]
  rfl

private theorem delta_cons_cons (y z : Int) (zs : List Int) :
    delta (y :: z :: zs) = (z - y) :: delta (z :: zs) := by
  unfold delta adj
  have h1 : List.drop 1 (y :: z :: zs) = z :: zs := by rfl
  have h2 : List.drop 1 (z :: zs) = zs := by rfl
  rw [h1, h2]
  rfl

private theorem measure_cons_cons (y z : Int) (zs : List Int) :
    TotalVariation.measure (y :: z :: zs) =
      ((z - y).natAbs : Int) + TotalVariation.measure (z :: zs) := by
  simp only [TotalVariation.measure, delta_cons_cons, List.map_cons, List.sum_cons]

theorem sound_TotalVariation_Rescale : TotalVariation.Rescale.Goal := by
  intro a x
  induction x with
  | nil => simp [scaled, TotalVariation.measure, delta, adj]
  | cons y ys ih =>
      cases ys with
      | nil => simp [scaled, TotalVariation.measure, delta, adj]
      | cons z zs =>
          have hhead : ((a * z - a * y).natAbs : Int) =
              (a.natAbs : Int) * ((z - y).natAbs : Int) := by
            rw [← Int.mul_sub, natAbs_mul_int]
          change TotalVariation.measure ((a * y) :: scaled a (z :: zs)) =
            (a.natAbs : Int) * TotalVariation.measure (y :: z :: zs)
          calc
            TotalVariation.measure ((a * y) :: scaled a (z :: zs))
                = ((a * z - a * y).natAbs : Int) + TotalVariation.measure (scaled a (z :: zs)) := by
                    rw [show scaled a (z :: zs) = (a * z) :: scaled a zs by rfl]
                    exact measure_cons_cons (a * y) (a * z) (scaled a zs)
            _ = (a.natAbs : Int) * ((z - y).natAbs : Int) +
                  (a.natAbs : Int) * TotalVariation.measure (z :: zs) := by
                    rw [hhead, ih]
            _ = (a.natAbs : Int) *
                  (((z - y).natAbs : Int) + TotalVariation.measure (z :: zs)) := by
                    rw [Int.mul_add]
            _ = (a.natAbs : Int) * TotalVariation.measure (y :: z :: zs) := by
                    rw [measure_cons_cons]

end TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_Rescale
