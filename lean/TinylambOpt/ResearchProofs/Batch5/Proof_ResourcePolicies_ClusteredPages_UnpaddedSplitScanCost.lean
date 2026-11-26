import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_UnpaddedSplitScanCost. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_UnpaddedSplitScanCost

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

open TinylambOpt.ResearchGoals.Compression.Wire

private theorem min?_le_min?_append_left
    {a b : List Nat} {mn mn' : Nat} (hmn : a.min? = some mn) (hmn' : (a ++ b).min? = some mn') :
    mn' ≤ mn := by
  have hmem : mn ∈ a ++ b := List.mem_append_left b ((List.min?_eq_some_iff.mp hmn).1)
  exact (List.min?_eq_some_iff.mp hmn').2 mn hmem

private theorem max?_le_max?_append_left
    {a b : List Nat} {mx mx' : Nat} (hmx : a.max? = some mx) (hmx' : (a ++ b).max? = some mx') :
    mx ≤ mx' := by
  have hmem : mx ∈ a ++ b := List.mem_append_left b ((List.max?_eq_some_iff.mp hmx).1)
  exact (List.max?_eq_some_iff.mp hmx').2 mx hmem

private theorem min?_le_min?_append_right
    {a b : List Nat} {mn mn' : Nat} (hmn : b.min? = some mn) (hmn' : (a ++ b).min? = some mn') :
    mn' ≤ mn := by
  have hmem : mn ∈ a ++ b := List.mem_append_right a ((List.min?_eq_some_iff.mp hmn).1)
  exact (List.min?_eq_some_iff.mp hmn').2 mn hmem

private theorem max?_le_max?_append_right
    {a b : List Nat} {mx mx' : Nat} (hmx : b.max? = some mx) (hmx' : (a ++ b).max? = some mx') :
    mx ≤ mx' := by
  have hmem : mx ∈ a ++ b := List.mem_append_right a ((List.max?_eq_some_iff.mp hmx).1)
  exact (List.max?_eq_some_iff.mp hmx').2 mx hmem

private theorem candidate_merge_left {lo hi : Nat} {a b : Page}
    (ha : candidate lo hi a = true) : candidate lo hi ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩ = true := by
  unfold candidate at *
  simp only [Bool.and_eq_true, Option.any_eq_true] at ha ⊢
  obtain ⟨⟨mn, hmn, hmnle⟩, ⟨mx, hmx, hlo⟩⟩ := ha
  have hne : a.keys ≠ [] := by intro h; simp [h] at hmn
  have hne' : (a.keys ++ b.keys) ≠ [] := List.append_ne_nil_of_left_ne_nil hne b.keys
  have hsome : (a.keys ++ b.keys).min?.isSome = true := (List.isSome_min?_iff).mpr hne'
  obtain ⟨mn', hmn'⟩ := Option.isSome_iff_exists.mp hsome
  have hsome2 : (a.keys ++ b.keys).max?.isSome = true := (List.isSome_max?_iff).mpr hne'
  obtain ⟨mx', hmx'⟩ := Option.isSome_iff_exists.mp hsome2
  refine ⟨⟨mn', hmn', ?_⟩, ⟨mx', hmx', ?_⟩⟩
  · exact decide_eq_true_iff.mpr (Nat.le_trans (min?_le_min?_append_left hmn hmn') (decide_eq_true_iff.mp hmnle))
  · exact decide_eq_true_iff.mpr (Nat.le_trans (decide_eq_true_iff.mp hlo) (max?_le_max?_append_left hmx hmx'))

private theorem candidate_merge_right {lo hi : Nat} {a b : Page}
    (hb : candidate lo hi b = true) : candidate lo hi ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩ = true := by
  unfold candidate at *
  simp only [Bool.and_eq_true, Option.any_eq_true] at hb ⊢
  obtain ⟨⟨mn, hmn, hmnle⟩, ⟨mx, hmx, hlo⟩⟩ := hb
  have hne : b.keys ≠ [] := by intro h; simp [h] at hmn
  have hne' : (a.keys ++ b.keys) ≠ [] := List.append_ne_nil_of_right_ne_nil a.keys hne
  have hsome : (a.keys ++ b.keys).min?.isSome = true := (List.isSome_min?_iff).mpr hne'
  obtain ⟨mn', hmn'⟩ := Option.isSome_iff_exists.mp hsome
  have hsome2 : (a.keys ++ b.keys).max?.isSome = true := (List.isSome_max?_iff).mpr hne'
  obtain ⟨mx', hmx'⟩ := Option.isSome_iff_exists.mp hsome2
  refine ⟨⟨mn', hmn', ?_⟩, ⟨mx', hmx', ?_⟩⟩
  · exact decide_eq_true_iff.mpr (Nat.le_trans (min?_le_min?_append_right hmn hmn') (decide_eq_true_iff.mp hmnle))
  · exact decide_eq_true_iff.mpr (Nat.le_trans (decide_eq_true_iff.mp hlo) (max?_le_max?_append_right hmx hmx'))

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.UnpaddedSplitScanCost.Goal := by
  intro a b lo hi
  unfold readBytes
  simp only [List.filter_cons, List.filter_nil, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  by_cases hca : candidate lo hi a = true
  · have hcm := candidate_merge_left (a := a) (b := b) (lo := lo) (hi := hi) hca
    by_cases hcb : candidate lo hi b = true <;> simp [hca, hcb, hcm]
  · by_cases hcb : candidate lo hi b = true
    · have hcm := candidate_merge_right (a := a) (b := b) (lo := lo) (hi := hi) hcb
      simp [hca, hcb, hcm]
    · by_cases hcm : candidate lo hi ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩ = true <;> simp [hca, hcb, hcm]

end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_UnpaddedSplitScanCost
