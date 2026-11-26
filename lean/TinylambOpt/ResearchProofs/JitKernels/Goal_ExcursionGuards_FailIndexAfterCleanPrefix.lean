import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_FailIndexAfterCleanPrefix

set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.FailIndexAfterCleanPrefix.Goal := by
  intro lo hi s a b hsafe
  let badA : Int → Bool := fun v => s + v < lo || hi < s + v
  let badB : Int → Bool := fun v => (s + a.sum) + v < lo || hi < (s + a.sum) + v
  have hnoneA : List.findIdx? badA (TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path a) = none := by
    rw [List.findIdx?_eq_none_iff]
    intro v hv
    have hvgood := (List.all_eq_true.mp hsafe) v hv
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hvgood
    simp [badA, hvgood.1, hvgood.2]
  have hscan : ∀ (xs : List Int) (t : Int),
      List.scanl (fun x y : Int => x + y) t xs =
        (List.scanl (fun x y : Int => x + y) 0 xs).map (fun w => t + w) := by
    intro xs
    induction xs with
    | nil => intro t; simp
    | cons x xs ih =>
        intro t
        simp only [List.scanl_cons]
        rw [ih (t + x)]
        simp only [Int.zero_add]
        rw [ih x]
        simp only [List.map_cons, List.map_map]
        congr 1
        · omega
        · congr 1
          funext w
          simp only [Function.comp_apply]
          omega
  have hmemSum : ∀ xs : List Int,
      xs.sum ∈ TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path xs := by
    intro xs
    induction xs with
    | nil => simp [TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path]
    | cons x xs ih =>
        unfold TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path at ih ⊢
        simp only [List.scanl_cons, List.sum_cons]
        simp only [Int.zero_add]
        rw [hscan xs x]
        simp only [List.mem_cons]
        right
        exact List.mem_map.mpr ⟨xs.sum, ih, rfl⟩
  have hgoodSum := (List.all_eq_true.mp hsafe) a.sum
    (hmemSum a)
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hgoodSum
  have hheadB : badB 0 = false := by
    simp [badB, hgoodSum.1, hgoodSum.2]
  have hpath :
      TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path (a ++ b) =
        TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path a ++
          ((TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path b).tail.map (fun w => a.sum + w)) := by
    unfold TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path
    rw [List.scanl_append]
    have hfold : a.foldl (fun x y : Int => x + y) 0 = a.sum := by
      rw [← List.sum_eq_foldl]
    rw [hfold]
    rw [hscan b a.sum]
    rw [← List.map_tail]
  unfold TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.faultAt
  rw [hpath]
  rw [List.findIdx?_append]
  rw [hnoneA]
  simp only [Option.none_or]
  rw [List.findIdx?_map]
  cases b with
  | nil => simp [TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path, hgoodSum.1, hgoodSum.2]
  | cons x xs =>
      simp only [TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards.path, List.scanl_cons] at hheadB ⊢
      rw [List.findIdx?_cons]
      simp
      have hpred :
          ((fun v => s + v < lo || hi < s + v) ∘ fun w : Int => a.sum + w) =
            (fun v => (s + a.sum) + v < lo || hi < (s + a.sum) + v) := by
        funext w
        simp only [Function.comp_apply]
        have h1 : s + (a.sum + w) < lo ↔ (s + a.sum) + w < lo := by omega
        have h2 : hi < s + (a.sum + w) ↔ hi < (s + a.sum) + w := by omega
        simp [h1, h2]
      rw [hpred]
      have hnotLow : ¬ s + a.sum < lo := by omega
      have hnotHigh : ¬ hi < s + a.sum := by omega
      simp [hnotLow, hnotHigh]
      congr 1
      funext i
      simp only [Function.comp_apply]
      omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_FailIndexAfterCleanPrefix
