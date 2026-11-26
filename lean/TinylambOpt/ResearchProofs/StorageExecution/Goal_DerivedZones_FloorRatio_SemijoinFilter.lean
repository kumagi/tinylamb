import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `FloorRatio.SemijoinFilter` — Apply the footer to the build-key set before probing;
  retain left bag order and duplicates.
- `FloorRatio.SoundInterval` — Bound a computed group or predicate key from independent
  column min/max.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

/-- The certified interval lemma for this kernel (division is monotone in the
numerator and antitone in the positive denominator). -/
private theorem soundInterval_floorRatio : FloorRatio.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  constructor
  · exact Nat.div_le_div (Nat.mul_le_mul ha1 (Nat.le_refl t)) (by omega) (by omega)
  · exact Nat.div_le_div (Nat.mul_le_mul ha2 (Nat.le_refl t)) (by omega) (by omega)

/-- Every non-NULL score of a covered page lies in the certified interval. -/
private theorem inside_scores {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) :
    ∀ v ∈ scores FloorRatio.kernel t p, inside (FloorRatio.kernel.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hrp, hm⟩ : ∃ r ∈ p, r.map (FloorRatio.kernel.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ :
      ∃ x y, some (x, y) ∈ p ∧ FloorRatio.kernel.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
        cases xy with
        | mk x y => exact ⟨x, y, hrp, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact soundInterval_floorRatio t a b x y hax hby

/-- A key in the interval survives the zone filter; a key outside it cannot
equal an in-interval score. -/
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

/-- Restricting the build-key set to the page's certified score interval does
not change which probe scores are retained. -/
theorem sound_SemijoinFilter : FloorRatio.SemijoinFilter.Goal := by
  intro t p a b ks hc
  refine filter_congr_of_mem (fun v hv => ?_)
  exact contains_filter_zone (inside_scores hc v hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SemijoinFilter
