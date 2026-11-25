import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Samples_EmptySample

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

private theorem countP_map_comp {α β : Type} (ss : List α) (f : α → β) (p : β → Bool) :
    (ss.map f).countP p = ss.countP (fun s => p (f s)) := by
  induction ss with
  | nil => simp
  | cons s ss ih =>
      simp only [List.map_cons, List.countP_cons]
      rw [ih]

private theorem countP_const_false {α : Type} (ss : List α) :
    ss.countP (fun _ => false) = 0 := by
  induction ss with
  | nil => rfl
  | cons s ss ih => simp [ih]

private theorem empty_sample_count : ∀ (xs : List Bool) (k : Nat),
    (samples k xs).countP (fun s => !s.contains true) = choose (xs.count false) k
  | [], k => by
      cases k <;> simp [samples, choose]
  | x :: xs, k => by
      cases k with
      | zero => simp [samples, choose]
      | succ k =>
          rw [samples, List.countP_append, countP_map_comp]
          have ih1 := empty_sample_count xs (k + 1)
          have ih0 := empty_sample_count xs k
          cases x with
          | false =>
              have hpred : (fun s : List Bool => !((false :: s).contains true)) =
                  (fun s : List Bool => !(s.contains true)) := by
                funext s
                simp
              rw [hpred, ih1, ih0]
              simp [choose]
              omega
          | true =>
              have hpred : (fun s : List Bool => !((true :: s).contains true)) =
                  (fun _ : List Bool => false) := by
                funext s
                simp
              rw [hpred, countP_const_false, ih1]
              simp

theorem sound_Samples_EmptySample :
    TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples.EmptySample.Goal := by
  intro xs k
  exact empty_sample_count xs k

end TinylambOpt.ResearchProofs.Atlas.Goal_Samples_EmptySample
