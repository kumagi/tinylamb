import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_Seam

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

local notation "tv" => TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure

private theorem tv_cons₂ (a b : Int) (xs : Series) :
    tv (a :: b :: xs) = distance a b + tv (b :: xs) := by
  simp [TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure,
    delta, adj, distance]

private theorem seam_cons₂ (a b : Int) (bs : Series) (y : Series)
    (f : Int → Int → Int) :
    seam (a :: b :: bs) y f = seam (b :: bs) y f := by
  simp [seam, List.getLast?_cons]

theorem sound_TotalVariation_Seam : TotalVariation.Seam.Goal := by
  intro x y
  induction x with
  | nil =>
      simp [TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure,
        delta, adj, seam]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil =>
              simp [TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure,
                delta, adj, seam]
          | cons b bs =>
              simp only [List.cons_append, List.nil_append]
              rw [tv_cons₂ a b bs]
              simp [TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure,
                delta, adj, seam, distance]
              omega
      | cons b bs =>
          simp only [List.cons_append]
          simp only [List.cons_append] at ih
          rw [tv_cons₂ a b (bs ++ y)]
          rw [show tv (a :: b :: bs) = distance a b + tv (b :: bs) from tv_cons₂ a b bs]
          rw [seam_cons₂ a b bs y distance]
          rw [ih]
          omega

end TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_Seam
