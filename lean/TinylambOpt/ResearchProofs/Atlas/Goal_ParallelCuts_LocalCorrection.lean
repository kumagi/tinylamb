import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ParallelCuts_LocalCorrection

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

/-! 小さい入力で真偽を総当たり確認する。
    `xs` は長さ 0..3 の Nat 列、`cs` は分割位置列、`i` と `d` も小さく抑える。 -/
def vecs : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (List.range 3).flatMap (fun i => (vecs n).map (fun xs => i :: xs))

def cuts : List (List Nat) :=
  [[]] ++ (List.range 4).map (fun a => [a]) ++
    ((List.range 3).flatMap (fun a => (List.range 3).map (fun b => [a, b])))

#eval! (vecs 3).all (fun xs => cuts.all (fun cs =>
  (List.range 4).all (fun i => (List.range 4).all (fun d =>
    decide (peak (groups (xs.modify i (· + d)) cs) ≤ peak (groups xs cs) + d)))))

/-! `peak` と `List.modify` のための補題。 -/
private theorem peak_cons (b : List Nat) (bs : List (List Nat)) :
    peak (b :: bs) = max b.sum (peak bs) := by
  unfold peak
  simp only [List.map_cons]
  cases h : (bs.map List.sum).max? with
  | none => simp [h, List.max?_cons]
  | some v => simp [h, List.max?_cons]

private theorem sum_modify_le (xs : List Nat) (i d : Nat) :
    (xs.modify i (· + d)).sum ≤ xs.sum + d := by
  induction xs generalizing i d with
  | nil => simp
  | cons a as ih =>
      rw [List.modify_cons]
      by_cases hi : i = 0
      · simp [hi]
        omega
      · simp [hi]
        have ht := ih (i - 1) d
        omega

private theorem drop_modify {α : Type} (f : α → α) (i c : Nat) (l : List α) :
    (l.modify i f).drop c =
      if i < c then l.drop c else (l.drop c).modify (i - c) f := by
  induction l generalizing i c with
  | nil => simp
  | cons a as ih =>
      cases c with
      | zero => simp
      | succ c' =>
          rw [List.modify_cons]
          by_cases hi : i = 0
          · simp [hi]
          · simp [hi]
            by_cases h : i < c' + 1
            · have h' : i - 1 < c' := by omega
              rw [ih]
              simp [h, h']
            · have h' : ¬ i - 1 < c' := by omega
              rw [ih]
              simp [h, h']
              congr 1
              omega

theorem sound_ParallelCuts_LocalCorrection : ParallelCuts.LocalCorrection.Goal := by
  intro xs cs
  induction cs generalizing xs with
  | nil =>
      intro i d
      simpa [groups, peak] using sum_modify_le xs i d
  | cons c cs ih =>
      intro i d
      simp only [groups]
      rw [peak_cons, peak_cons]
      have hhead : ((xs.modify i (· + d)).take c).sum ≤ (xs.take c).sum + d := by
        rw [List.take_modify]
        exact sum_modify_le (xs.take c) i d
      by_cases hic : i < c
      · rw [drop_modify]
        simp [hic]
        have htail : peak (groups (xs.drop c) cs) ≤ peak (groups (xs.drop c) cs) + d := by
          omega
        exact Nat.max_le.mpr
          ⟨Nat.le_trans hhead (Nat.add_le_add_right (Nat.le_max_left _ _) d),
           Nat.le_trans htail (Nat.add_le_add_right (Nat.le_max_right _ _) d)⟩
      · rw [drop_modify]
        simp [hic]
        have htail := ih (xs.drop c) (i - c) d
        exact Nat.max_le.mpr
          ⟨Nat.le_trans hhead (Nat.add_le_add_right (Nat.le_max_left _ _) d),
           Nat.le_trans htail (Nat.add_le_add_right (Nat.le_max_right _ _) d)⟩

end TinylambOpt.ResearchProofs.Atlas.Goal_ParallelCuts_LocalCorrection
