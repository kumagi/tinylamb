import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

/-!
Proof contribution for ResourcePolicies_ClusteredPages_SplitPhysicalReadEnvelope. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_SplitPhysicalReadEnvelope

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

private theorem align_le_self (u n : Nat) (hu : 0 < u) : n ≤ align u n := by
  unfold align
  have h := (Nat.div_le_iff_le_mul hu).1 (Nat.le_refl ((n + u - 1) / u))
  omega

private theorem align_mono (u a b : Nat) (h : a ≤ b) : align u a ≤ align u b := by
  unfold align
  exact Nat.mul_le_mul_right u (Nat.div_le_div_right (by omega))

private theorem align_eq_of_dvd (u n : Nat) (hu : 0 < u) (h : u ∣ n) : align u n = n := by
  rcases h with ⟨k, hk⟩
  subst n
  unfold align
  have heq : u * k + u - 1 = u * k + (u - 1) := by omega
  rw [heq, Nat.mul_add_div hu]
  have hzero : (u - 1) / u = 0 := Nat.div_eq_of_lt (by omega)
  rw [hzero]
  simp [Nat.mul_comm]

private theorem align_le_of_le_of_dvd (u x A : Nat) (hu : 0 < u) (hx : x ≤ A) (hd : u ∣ A) :
    align u x ≤ A := by
  rw [← align_eq_of_dvd u A hu hd]
  exact align_mono u x A hx

private theorem align_add_le (u x y : Nat) (hu : 0 < u) :
    align u (x + y) ≤ align u x + align u y := by
  apply align_le_of_le_of_dvd u (x+y) (align u x + align u y) hu
  · have hx := align_le_self u x hu
    have hy := align_le_self u y hu
    omega
  · exact Nat.dvd_add (Nat.dvd_mul_left u ((x + u - 1) / u)) (Nat.dvd_mul_left u ((y + u - 1) / u))

private theorem align_add (u x y : Nat) (hu : 0 < u) :
    align u x + align u y ≤ align u (x + y) + u := by
  let A := x + u - 1
  let B := y + u - 1
  let C := x + y + u - 1
  have hsum : A + B = C + (u - 1) := by dsimp [A, B, C]; omega
  have hAB : (A + B) / u = A / u + B / u + (if u ≤ A % u + B % u then 1 else 0) := Nat.add_div hu
  have hle1 : A / u + B / u ≤ (A + B) / u := by rw [hAB]; split <;> omega
  have hC : (C + (u - 1)) / u = C / u + (u - 1) / u + (if u ≤ C % u + (u - 1) % u then 1 else 0) := Nat.add_div hu
  have hum1 : (u - 1) / u = 0 := Nat.div_eq_of_lt (by omega)
  have hle2 : (A + B) / u ≤ C / u + 1 := by
    rw [hsum, hC, hum1]
    split <;> omega
  have hdiv_le : A / u + B / u ≤ C / u + 1 := Nat.le_trans hle1 hle2
  have hmul := Nat.mul_le_mul_left u hdiv_le
  simp only [Nat.mul_add, Nat.mul_one] at hmul
  simpa [align, A, B, C, Nat.mul_comm] using hmul

private theorem align_split_envelope (h u x y : Nat) (hu : 0 < u) :
    align u (h + x) + align u (h + y) ≤ align u (h + x + y) + align u h + u := by
  calc
    align u (h + x) + align u (h + y) ≤ align u ((h + x) + (h + y)) + u := align_add u (h+x) (h+y) hu
    _ = align u ((h + x + y) + h) + u := by congr 2 <;> omega
    _ ≤ align u (h + x + y) + align u h + u := by
      have hh := align_add_le u (h+x+y) h hu
      omega

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages.SplitPhysicalReadEnvelope.Goal := by
  intro a b h u lo hi hu
  unfold physicalCost physical
  simp only [List.filter, List.map, List.sum_cons, List.sum_nil]
  by_cases hca : candidate lo hi a = true
  · have hcm := candidate_merge_left (a:=a) (b:=b) (lo:=lo) (hi:=hi) hca
    by_cases hcb : candidate lo hi b = true
    · simp [hca, hcb, hcm]
      rw [← Nat.add_assoc h a.bytes b.bytes]
      have he := align_split_envelope h u a.bytes b.bytes hu
      omega
    · simp [hca, hcb, hcm]
      rw [← Nat.add_assoc h a.bytes b.bytes]
      have he := align_mono u (h + a.bytes) (h + a.bytes + b.bytes) (by omega)
      omega
  · by_cases hcb : candidate lo hi b = true
    · have hcm := candidate_merge_right (a:=a) (b:=b) (lo:=lo) (hi:=hi) hcb
      simp [hca, hcb, hcm]
      rw [← Nat.add_assoc h a.bytes b.bytes]
      have he := align_mono u (h + b.bytes) (h + a.bytes + b.bytes) (by omega)
      omega
    · by_cases hcm : candidate lo hi ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩ = true <;>
        simp [hca, hcb, hcm]

end TinylambOpt.ResearchProofs.ResourcePolicies.ClusteredPages.Goal_SplitPhysicalReadEnvelope
