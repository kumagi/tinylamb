import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

/-- The quadratic-gap score of two points in independent intervals lies inside
the certified interval. -/
private theorem soundInterval_kernel : soundInterval kernel := by
  intro t a b x y hax hby
  rcases a with ⟨alo, ahi⟩
  rcases b with ⟨blo, bhi⟩
  simp only [inside] at hax hby
  simp only [kernel, gapMin, gapMax, gap]
  have hgapMin : max (alo - bhi) (blo - ahi) ≤ (x - y) + (y - x) := by
    apply Nat.max_le.mpr
    constructor <;> omega
  have hgapMax : (x - y) + (y - x) ≤ max (ahi - blo) (bhi - alo) := by
    by_cases hyx : y ≤ x
    · have hzero : y - x = 0 := Nat.sub_eq_zero_of_le hyx
      rw [hzero, Nat.add_zero]
      exact Nat.le_trans (by omega : x - y ≤ ahi - blo) (Nat.le_max_left _ _)
    · have hxy : x ≤ y := by omega
      have hzero : x - y = 0 := Nat.sub_eq_zero_of_le hxy
      rw [hzero, Nat.zero_add]
      exact Nat.le_trans (by omega : y - x ≤ bhi - alo) (Nat.le_max_right _ _)
  constructor
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hgapMin 2) t
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hgapMax 2) t

/-- Every score on a covered page belongs to the certified interval. -/
private theorem inside_scores {t p a b} (hc : covers p a b) :
    ∀ v ∈ scores kernel t p, inside (kernel.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hr, hm⟩ : ∃ r ∈ p, r.map (kernel.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ kernel.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
      cases xy with
      | mk x y => exact ⟨x, y, hr, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact soundInterval_kernel t a b x y hax hby

private theorem filter_congr_of_mem {l : List Nat} {p q : Nat → Bool}
    (h : ∀ v ∈ l, p v = q v) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons a t ih =>
    by_cases hp : (p a) = true
    · have hq : q a = true := by rw [← h a (List.mem_cons_self (l := t))]; exact hp
      rw [List.filter_cons_of_pos hp, List.filter_cons_of_pos hq]
      rw [ih (fun v hv => h v (List.mem_cons_of_mem a hv))]
    · by_cases hq : (q a) = true
      · exfalso
        have hp' : p a = true := by rw [h a (List.mem_cons_self (l := t))]; exact hq
        contradiction
      · rw [List.filter_cons_of_neg hp, List.filter_cons_of_neg hq]
        rw [ih (fun v hv => h v (List.mem_cons_of_mem a hv))]

/-- A key that itself lies in the zone survives the zone filter. -/
private theorem contains_filter_zone {ks : List Nat} {v lo hi : Nat}
    (h : lo ≤ v ∧ v ≤ hi) :
    ks.contains v = (ks.filter (fun k => lo ≤ k && k ≤ hi)).contains v := by
  induction ks with
  | nil => rfl
  | cons k t ih =>
    by_cases hk : k = v
    · subst hk
      have hw : ((fun z : Nat => lo ≤ z && z ≤ hi) k) = true := by simpa using h
      rw [List.filter_cons_of_pos (p := fun z : Nat => lo ≤ z && z ≤ hi) hw,
        List.contains_cons, List.contains_cons]
      simp
    · have hkv : (v == k) = false := by
        simpa using (show ¬ (v = k) from fun e => hk e.symm)
      by_cases hw : (lo ≤ k && k ≤ hi)
      · have hw' : ((fun z : Nat => lo ≤ z && z ≤ hi) k) = true := hw
        rw [List.filter_cons_of_pos (p := fun z : Nat => lo ≤ z && z ≤ hi) hw',
          List.contains_cons, List.contains_cons, hkv,
          Bool.false_or, Bool.false_or, ih]
      · have hn : ¬ ((fun z : Nat => lo ≤ z && z ≤ hi) k) := hw
        rw [List.filter_cons_of_neg (p := fun z : Nat => lo ≤ z && z ≤ hi) hn,
          List.contains_cons, hkv, Bool.false_or, ih]

/-- Filtering the build keys by the page's derived zone does not change the
semijoin result on that page: every score is already inside the zone. -/
theorem sound_SemijoinFilter : QuadraticGap.SemijoinFilter.Goal := by
  intro t p a b ks hc
  refine filter_congr_of_mem (fun v hv => ?_)
  exact contains_filter_zone (inside_scores (t := t) (p := p) (a := a) (b := b) hc v hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SemijoinFilter
