import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_UnpaddedSplitScanCost
open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages UnpaddedSplitScanCost

private theorem candidate_merge_left {lo hi : Nat} {a b : Page}
    (ha : candidate lo hi a = true) : candidate lo hi ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩ = true := by
  unfold candidate at *
  simp only [Bool.and_eq_true, Option.any_eq_true] at ha ⊢
  obtain ⟨⟨mn, hmn, hmnle⟩, ⟨mx, hmx, hlo⟩⟩ := ha
  have hne : a.keys ≠ [] := by intro h; simp [h] at hmn
  have hne' : (a.keys ++ b.keys) ≠ [] := List.append_ne_nil_of_left_ne_nil hne b.keys
  have hmin_some : (a.keys ++ b.keys).min?.isSome = true := by rw [List.isSome_min?_iff]; exact hne'
  obtain ⟨mn', hmn'⟩ := Option.isSome_iff_exists.mp hmin_some
  have hmax_some : (a.keys ++ b.keys).max?.isSome = true := by rw [List.isSome_max?_iff]; exact hne'
  obtain ⟨mx', hmx'⟩ := Option.isSome_iff_exists.mp hmax_some
  refine ⟨⟨mn', hmn', ?_⟩, ⟨mx', hmx', ?_⟩⟩
  · have hle : mn' ≤ mn := (List.min?_eq_some_iff.mp hmn').2 mn (List.mem_append_left b.keys ((List.min?_eq_some_iff.mp hmn).1))
    exact decide_eq_true_iff.mpr (Nat.le_trans hle (decide_eq_true_iff.mp hmnle))
  · have hle : mx ≤ mx' := (List.max?_eq_some_iff.mp hmx').2 mx (List.mem_append_left b.keys ((List.max?_eq_some_iff.mp hmx).1))
    exact decide_eq_true_iff.mpr (Nat.le_trans (decide_eq_true_iff.mp hlo) hle)

private theorem candidate_merge_right {lo hi : Nat} {a b : Page}
    (hb : candidate lo hi b = true) : candidate lo hi ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩ = true := by
  unfold candidate at *
  simp only [Bool.and_eq_true, Option.any_eq_true] at hb ⊢
  obtain ⟨⟨mn, hmn, hmnle⟩, ⟨mx, hmx, hlo⟩⟩ := hb
  have hne : b.keys ≠ [] := by intro h; simp [h] at hmn
  have hne' : (a.keys ++ b.keys) ≠ [] := List.append_ne_nil_of_right_ne_nil a.keys hne
  have hmin_some : (a.keys ++ b.keys).min?.isSome = true := by rw [List.isSome_min?_iff]; exact hne'
  obtain ⟨mn', hmn'⟩ := Option.isSome_iff_exists.mp hmin_some
  have hmax_some : (a.keys ++ b.keys).max?.isSome = true := by rw [List.isSome_max?_iff]; exact hne'
  obtain ⟨mx', hmx'⟩ := Option.isSome_iff_exists.mp hmax_some
  refine ⟨⟨mn', hmn', ?_⟩, ⟨mx', hmx', ?_⟩⟩
  · have hle : mn' ≤ mn := (List.min?_eq_some_iff.mp hmn').2 mn (List.mem_append_right a.keys ((List.min?_eq_some_iff.mp hmn).1))
    exact decide_eq_true_iff.mpr (Nat.le_trans hle (decide_eq_true_iff.mp hmnle))
  · have hle : mx ≤ mx' := (List.max?_eq_some_iff.mp hmx').2 mx (List.mem_append_right a.keys ((List.max?_eq_some_iff.mp hmx).1))
    exact decide_eq_true_iff.mpr (Nat.le_trans (decide_eq_true_iff.mp hlo) hle)

theorem sound : Goal := by
  intro a b lo hi
  unfold readBytes
  simp only [List.filter, List.map, List.sum_cons, List.sum_nil]
  by_cases hca : candidate lo hi a = true
  · have hcm := candidate_merge_left (a:=a) (b:=b) (lo:=lo) (hi:=hi) hca
    by_cases hcb : candidate lo hi b = true <;> simp [hca, hcb, hcm] <;> omega
  · by_cases hcb : candidate lo hi b = true
    · have hcm := candidate_merge_right (a:=a) (b:=b) (lo:=lo) (hi:=hi) hcb
      simp [hca, hcb, hcm] <;> try omega
    · by_cases hcm : candidate lo hi ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩ = true <;>
        simp [hca, hcb, hcm] <;> try omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_UnpaddedSplitScanCost
