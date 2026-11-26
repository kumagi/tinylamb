import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Dense key pages make the min/max zone test exact for a nonempty range: a
range overlapping the key interval always contains a real key. A SQL range scan
therefore loses no qualifying rows when a dense page is admitted by its zone. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_DensePageNoFalseRead

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

private theorem candidate_of_range {lo hi k : Nat} {p : Page}
    (hk : k ∈ p.keys) (hlo : lo ≤ k) (hhi : k ≤ hi) : candidate lo hi p = true := by
  unfold candidate
  simp only [Bool.and_eq_true, Option.any_eq_true]
  have hne : p.keys ≠ [] := List.ne_nil_of_mem hk
  have hmin_some : p.keys.min?.isSome = true := by rw [List.isSome_min?_iff]; exact hne
  have hmax_some : p.keys.max?.isSome = true := by rw [List.isSome_max?_iff]; exact hne
  obtain ⟨mn, hmn⟩ := Option.isSome_iff_exists.mp hmin_some
  obtain ⟨mx, hmx⟩ := Option.isSome_iff_exists.mp hmax_some
  refine ⟨⟨mn, hmn, ?_⟩, ⟨mx, hmx, ?_⟩⟩
  · exact decide_eq_true_iff.mpr (Nat.le_trans ((List.min?_eq_some_iff.mp hmn).2 k hk) hhi)
  · exact decide_eq_true_iff.mpr (Nat.le_trans hlo ((List.max?_eq_some_iff.mp hmx).2 k hk))

theorem sound : DensePageNoFalseRead.Goal := by
  intro p lo hi hlohi hdense
  constructor
  · intro hcand
    unfold candidate at hcand
    simp only [Bool.and_eq_true, Option.any_eq_true] at hcand
    obtain ⟨⟨mn, hmn, hmnhi⟩, ⟨mx, hmx, hlomx⟩⟩ := hcand
    have hmnmem : mn ∈ p.keys := (List.min?_eq_some_iff.mp hmn).1
    have hmxmem : mx ∈ p.keys := (List.max?_eq_some_iff.mp hmx).1
    have hmnle : mn ≤ mx := (List.max?_eq_some_iff.mp hmx).2 mn hmnmem
    let x := max lo mn
    have hxlo : lo ≤ x := Nat.le_max_left _ _
    have hxmn : mn ≤ x := Nat.le_max_right _ _
    have hxhi : x ≤ hi := Nat.max_le.mpr ⟨hlohi, decide_eq_true_iff.mp hmnhi⟩
    have hxmx : x ≤ mx := Nat.max_le.mpr ⟨decide_eq_true_iff.mp hlomx, hmnle⟩
    have hxmem : x ∈ p.keys := hdense mn hmnmem mx hmxmem x hxmn hxmx
    unfold rangeRows
    exact List.ne_nil_of_mem (show x ∈ (List.filter (fun k => lo ≤ k && k ≤ hi) p.keys) by simp [hxmem, hxlo, hxhi])
  · intro hrows
    unfold rangeRows at hrows
    obtain ⟨k, hk⟩ := List.exists_mem_of_ne_nil _ hrows
    simp at hk
    exact candidate_of_range hk.1 hk.2.1 hk.2.2
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_DensePageNoFalseRead
