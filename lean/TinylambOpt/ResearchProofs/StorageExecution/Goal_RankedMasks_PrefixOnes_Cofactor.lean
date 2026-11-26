import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_Cofactor

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes

private theorem sum_map_zero {α : Type} (xs : List α) : (xs.map (fun _ => (0 : Nat))).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

-- 1 ビット伸ばした prefix の項の総和は、その 1 ビットの遷移で条件付けできる。
private theorem sum_run_cons_getD (m : Machine) (q : Nat) (c : Bool) (n : Nat)
    (f : Nat → Nat) :
    ((words n).map (fun pre => ((run m q (c :: pre)).map f).getD 0)).sum =
      ((m.step q c).map (fun q' =>
        ((words n).map (fun pre => ((run m q' pre).map f).getD 0)).sum)).getD 0 := by
  cases h : m.step q c with
  | none => simp [run, h, sum_map_zero]
  | some q' => simp [run, h]

private theorem getD_map_congr {α β : Type} (o : Option α) (f g : α → β) (d : β)
    (h : ∀ x, f x = g x) : (o.map f).getD d = (o.map g).getD d := by
  cases o with
  | none => simp
  | some x => simp [h]

-- `cofactor` は Machine に依存しない: 長さ a の全 prefix で分割した合計が全体の数え上げになる。
private theorem cofactor_general (m : Machine) : cofactor m := by
  intro q a
  induction a generalizing q with
  | zero =>
      intro b
      simp [words, run]
  | succ n ih =>
      intro b
      rw [show (n + 1) + b = (n + b) + 1 by omega]
      change ((m.step q false).map (count m (n + b))).getD 0 +
          ((m.step q true).map (count m (n + b))).getD 0 =
        ((words (n + 1)).map (fun pre => ((run m q pre).map (count m b)).getD 0)).sum
      rw [words]
      simp only [List.map_append, List.sum_append, List.map_map, Function.comp_def]
      rw [sum_run_cons_getD m q false n (count m b)]
      rw [sum_run_cons_getD m q true n (count m b)]
      congr 1
      · exact getD_map_congr (m.step q false) (count m (n + b))
          (fun q' => ((words n).map (fun pre => ((run m q' pre).map (count m b)).getD 0)).sum)
          0 (fun q' => ih q' b)
      · exact getD_map_congr (m.step q true) (count m (n + b))
          (fun q' => ((words n).map (fun pre => ((run m q' pre).map (count m b)).getD 0)).sum)
          0 (fun q' => ih q' b)

theorem sound_Cofactor : PrefixOnes.Cofactor.Goal := by
  exact cofactor_general model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_Cofactor
