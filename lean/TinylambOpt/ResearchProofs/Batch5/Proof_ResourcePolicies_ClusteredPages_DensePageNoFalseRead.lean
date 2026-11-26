import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_DensePageNoFalseRead. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_DensePageNoFalseRead

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

open TinylambOpt.ResearchGoals.Compression.Wire

private theorem candidate_of_range {lo hi k : Nat} {p : Page}
    (hk : k ∈ p.keys) (hlo : lo ≤ k) (hhi : k ≤ hi) : candidate lo hi p = true := by
  unfold candidate
  simp only [Bool.and_eq_true, Option.any_eq_true]
  have hne : p.keys ≠ [] := List.ne_nil_of_mem hk
  have hmin_some : p.keys.min?.isSome = true := (List.isSome_min?_iff).mpr hne
  have hmax_some : p.keys.max?.isSome = true := (List.isSome_max?_iff).mpr hne
  obtain ⟨mn, hmn⟩ := Option.isSome_iff_exists.mp hmin_some
  obtain ⟨mx, hmx⟩ := Option.isSome_iff_exists.mp hmax_some
  refine ⟨⟨mn, hmn, ?_⟩, ⟨mx, hmx, ?_⟩⟩
  · have hle : mn ≤ k := (List.min?_eq_some_iff.mp hmn).2 k hk
    exact decide_eq_true_iff.mpr (by omega)
  · have hle : k ≤ mx := (List.max?_eq_some_iff.mp hmx).2 k hk
    exact decide_eq_true_iff.mpr (by omega)

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.DensePageNoFalseRead.Goal := by
  intro p lo hi hlohi hdense
  constructor
  · intro hc
    unfold candidate at hc
    simp only [Bool.and_eq_true, Option.any_eq_true] at hc
    obtain ⟨⟨mn, hmn, hmnhi⟩, ⟨mx, hmx, hlomx⟩⟩ := hc
    have hmnhi' : mn ≤ hi := decide_eq_true_iff.mp hmnhi
    have hlomx' : lo ≤ mx := decide_eq_true_iff.mp hlomx
    have hmnmem : mn ∈ p.keys := (List.min?_eq_some_iff.mp hmn).1
    have hmxmem : mx ∈ p.keys := (List.max?_eq_some_iff.mp hmx).1
    have hmn_le_mx : mn ≤ mx := (List.max?_eq_some_iff.mp hmx).2 mn hmnmem
    let x := max lo mn
    have hmn_le_x : mn ≤ x := Nat.le_max_right lo mn
    have hlo_le_x : lo ≤ x := Nat.le_max_left lo mn
    have hx_le_hi : x ≤ hi := (Nat.max_le).mpr ⟨hlohi, hmnhi'⟩
    have hx_le_mx : x ≤ mx := (Nat.max_le).mpr ⟨hlomx', hmn_le_mx⟩
    have hxmem : x ∈ p.keys := hdense mn hmnmem mx hmxmem x hmn_le_x hx_le_mx
    unfold rangeRows
    intro hempty
    have hxfilter : x ∈ List.filter (fun k => lo ≤ k && k ≤ hi) p.keys := by
      simp [hxmem, hlo_le_x, hx_le_hi]
    rw [hempty] at hxfilter
    exact List.not_mem_nil hxfilter
  · intro hne
    unfold rangeRows at hne
    obtain ⟨k, hk⟩ := List.exists_mem_of_ne_nil _ hne
    simp only [List.mem_filter, Bool.and_eq_true, decide_eq_true_iff] at hk
    exact candidate_of_range hk.1 hk.2.1 hk.2.2

end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ClusteredPages.Goal_DensePageNoFalseRead
