import TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! With headers and alignment charged, splitting a page can add at most one
additional header rounding plus one alignment step of worst-case scanning work.
The bound lets an executor reject a split whose extra physical reads outweigh
the saved payload bytes. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_SplitPhysicalReadEnvelope

open TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem le_align (u n : Nat) (hu : 0 < u) : n ≤ align u n := by
  unfold align
  let q := (n + u - 1) / u
  by_cases hle : n ≤ q * u
  · exact hle
  · have hlt : q * u < n := Nat.lt_of_not_ge hle
    have hge : (q+1)*u ≤ n+u-1 := by
      rw [Nat.add_mul, Nat.one_mul]
      omega
    have hqge : q+1 ≤ (n+u-1)/u := (Nat.le_div_iff_mul_le hu).mpr hge
    omega

private theorem align_mono (u x y : Nat) (hxy : x ≤ y) : align u x ≤ align u y := by
  by_cases hu : u = 0
  · subst u
    simp [align]
  · unfold align
    have hxy' : x+u-1 ≤ y+u-1 := by omega
    exact Nat.mul_le_mul_right u (Nat.div_le_div_right hxy')

private theorem align_subadd (u x y : Nat) (hu : 0 < u) : align u (x+y) ≤ align u x + align u y := by
  unfold align
  let a := (x+u-1)/u
  let b := (y+u-1)/u
  let q := (x+y+u-1)/u
  have hxle : x ≤ a*u := le_align u x hu
  have hyle : y ≤ b*u := le_align u y hu
  have hsum : a*u + b*u = (a+b)*u := (Nat.add_mul a b u).symm
  have hqle : q ≤ a+b := by
    dsimp [q]
    apply (Nat.div_le_iff_le_mul hu).mpr
    omega
  change q*u ≤ a*u+b*u
  have hmul := Nat.mul_le_mul_right u hqle
  rw [← hsum] at hmul
  exact hmul

private theorem align_add (u x y : Nat) (hu : 0 < u) : align u x + align u y ≤ align u (x+y) + u := by
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

theorem sound : SplitPhysicalReadEnvelope.Goal := by
  intro a b h u lo hi hu
  unfold physicalCost
  simp only [List.filter_cons, List.map_cons, List.sum_cons, List.sum_nil]
  let merged : Page := ⟨a.keys ++ b.keys, a.bytes + b.bytes⟩
  have heq : h + (a.bytes + b.bytes) = h + a.bytes + b.bytes := by omega
  have hmono_a : align u (h+a.bytes) ≤ align u (h+a.bytes+b.bytes) :=
    align_mono u (h+a.bytes) (h+a.bytes+b.bytes) (by omega)
  have hmono_b : align u (h+b.bytes) ≤ align u (h+a.bytes+b.bytes) := by
    have hbmono := align_mono u (h+b.bytes) (h+b.bytes+a.bytes) (by omega)
    have heq : h+b.bytes+a.bytes = h+a.bytes+b.bytes := by omega
    simpa [heq] using hbmono
  by_cases hca : candidate lo hi a = true
  · have hcm : candidate lo hi merged = true := by
      dsimp [merged]
      exact candidate_merge_left hca
    by_cases hcb : candidate lo hi b = true
    · simp [physical, merged, hca, hcb, hcm]
      have hs := align_subadd u h b.bytes hu
      have hadd := align_add u (h+a.bytes) b.bytes hu
      rw [heq]
      omega
    · simp [physical, merged, hca, hcb, hcm]
      have hm : align u (h+a.bytes) ≤ align u (h+(a.bytes+b.bytes)) := by
        have hm := hmono_a
        rw [← heq] at hm
        exact hm
      omega
  · by_cases hcb : candidate lo hi b = true
    · have hcm : candidate lo hi merged = true := by
        dsimp [merged]
        exact candidate_merge_right hcb
      simp [physical, merged, hca, hcb, hcm]
      have hs := align_subadd u h a.bytes hu
      have hadd := align_add u (h+b.bytes) a.bytes hu
      have heq2 : h + (a.bytes + b.bytes) = h+b.bytes+a.bytes := by omega
      rw [heq2]
      have hm : align u (h+b.bytes) ≤ align u (h+b.bytes+a.bytes) := by
        have hb := hmono_b
        rw [show h+a.bytes+b.bytes = h+b.bytes+a.bytes by omega] at hb
        exact hb
      omega
    · simp [physical, merged, hca, hcb]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ClusteredPages_SplitPhysicalReadEnvelope
