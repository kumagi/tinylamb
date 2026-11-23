import TinylambOpt.ResearchGoals.FloatFubiniHavingBand

namespace TinylambOpt.ResearchProofs.FloatFubiniHavingBand

open TinylambOpt.ResearchGoals.FloatFubiniHavingBand

/-!
# Fubini sum identity and HAVING error-band pruning

`keysOf` is a left fold that inserts each encountered key into a list, keeping
the first-occurrence order. We prove two properties of this fold (no
duplicates, and coverage of all row keys) and then show that summing the group
sums over such a key list reconstructs the global total.

For the band part, write `D = den`, `A = num`, and let
`δG = gExact - gHat`, `δT = tExact - tHat`. The `natAbs` hypotheses give
`|δG| ≤ epsG` and `|δT| ≤ epsT`, hence
`|D * δG - A * δT| ≤ D * epsG + A * epsT = margin`. Since
`D * gExact - A * tExact = (D * gHat - A * tHat) + (D * δG - A * δT)`,
the two out-of-band verdicts decide the exact comparison.
-/

-------------------------------------------------------------------------------
-- 1. The key-accumulating fold
-------------------------------------------------------------------------------

private def insertKey (acc : List Key) (k : Key) : List Key :=
  if acc.contains k then acc else acc ++ [k]

private theorem keysOf_eq_foldl (rows : List Row) :
    keysOf rows = (rows.map Prod.fst).foldl insertKey [] := by
  rfl

private theorem insertKey_self_mem (acc : List Key) (k : Key) : k ∈ insertKey acc k := by
  by_cases h : k ∈ acc <;> simp [insertKey, h]

private theorem insertKey_preserves_mem {acc : List Key} {k x : Key} (h : x ∈ acc) :
    x ∈ insertKey acc k := by
  by_cases hk : k ∈ acc
  · simpa [insertKey, hk] using h
  · simp [insertKey, hk, h]

private theorem nodup_append_singleton {l : List Key} {k : Key} (h : l.Nodup)
    (hk : k ∉ l) : (l ++ [k]).Nodup := by
  rw [List.nodup_append]
  refine ⟨h, by simp, ?_⟩
  intro x hx y hy
  simp only [List.mem_singleton] at hy
  subst y
  intro hxk
  exact hk (by simpa [hxk] using hx)

private theorem insertKey_nodup {acc : List Key} {k : Key} (h : acc.Nodup) :
    (insertKey acc k).Nodup := by
  by_cases hk : k ∈ acc
  · simpa [insertKey, hk] using h
  · simpa [insertKey, hk] using (nodup_append_singleton h hk)

private theorem foldl_insertKey_nodup (ks : List Key) (acc : List Key) (h : acc.Nodup) :
    (ks.foldl insertKey acc).Nodup := by
  induction ks generalizing acc with
  | nil => exact h
  | cons k ks ih => exact ih (insertKey acc k) (insertKey_nodup h)

private theorem foldl_insertKey_mem (ks : List Key) (acc : List Key) {k : Key}
    (h : k ∈ ks ∨ k ∈ acc) : k ∈ ks.foldl insertKey acc := by
  induction ks generalizing acc with
  | nil => simpa using h
  | cons x xs ih =>
      apply ih (insertKey acc x)
      rcases h with h | h
      · simp only [List.mem_cons] at h
        rcases h with h | h
        · subst x
          exact Or.inr (insertKey_self_mem acc k)
        · exact Or.inl h
      · exact Or.inr (insertKey_preserves_mem h)

private theorem keysOf_nodup (rows : List Row) : (keysOf rows).Nodup := by
  rw [keysOf_eq_foldl]
  exact foldl_insertKey_nodup _ _ (by simp)

private theorem mem_keysOf_of_mem (rows : List Row) {r : Row} (h : r ∈ rows) :
    r.1 ∈ keysOf rows := by
  rw [keysOf_eq_foldl]
  apply foldl_insertKey_mem
  left
  exact List.mem_map.mpr ⟨r, h, rfl⟩

-------------------------------------------------------------------------------
-- 2. Summing group sums over a nodup covering key list
-------------------------------------------------------------------------------

private theorem sum_map_zero (ks : List Key) : (ks.map fun _ : Key => (0 : Int)).sum = 0 := by
  induction ks with
  | nil => simp
  | cons k ks ih => simp [ih]

private theorem sum_map_add (ks : List Key) (f g : Key → Int) :
    (ks.map fun k => f k + g k).sum = (ks.map f).sum + (ks.map g).sum := by
  induction ks with
  | nil => simp
  | cons k ks ih =>
      simp [ih]
      omega

private theorem sum_map_eq_zero_of_not_mem (ks : List Key) (k0 : Key) (v : Int)
    (hnot : k0 ∉ ks) : (ks.map fun k => if k == k0 then v else 0).sum = 0 := by
  induction ks with
  | nil => simp
  | cons k ks ih =>
      have hk : k ≠ k0 := by
        intro h
        apply hnot
        simp [h]
      have hnot' : k0 ∉ ks := by
        intro h
        apply hnot
        simp [h]
      rw [List.map_cons, List.sum_cons]
      have hhead : (if k == k0 then v else 0) = 0 := by simp [hk]
      rw [hhead, ih hnot']
      simp

private theorem sum_map_ite_eq (ks : List Key) (k0 : Key) (v : Int)
    (hnd : ks.Nodup) (hin : k0 ∈ ks) :
    (ks.map fun k => if k == k0 then v else 0).sum = v := by
  induction ks with
  | nil => simp at hin
  | cons k ks ih =>
      rw [List.nodup_cons] at hnd
      rcases hnd with ⟨hnot, hksnd⟩
      simp only [List.mem_cons] at hin
      rcases hin with hin | hin
      · subst k
        have hzero : (ks.map fun k' => if k' == k0 then v else 0).sum = 0 :=
          sum_map_eq_zero_of_not_mem ks k0 v hnot
        rw [List.map_cons, List.sum_cons]
        have hhead : (if k0 == k0 then v else 0) = v := by simp
        rw [hhead, hzero]
        simp
      · have hk : k ≠ k0 := by
          intro h
          exact hnot (by simpa [h] using hin)
        rw [List.map_cons, List.sum_cons]
        have hhead : (if k == k0 then v else 0) = 0 := by simp [hk]
        rw [hhead, ih hksnd hin]
        simp

private theorem groupSum_cons (r : Row) (rs : List Row) (k : Key) :
    groupSum (r :: rs) k = groupSum rs k + (if k == r.1 then r.2 else 0) := by
  by_cases h : k = r.1
  · subst k
    simp [groupSum]
    omega
  · have h' : r.1 ≠ k := by
      intro h'
      exact h h'.symm
    simp [groupSum, h, h']

private theorem map_groupSum_cons (r : Row) (rs : List Row) (ks : List Key) :
    ks.map (groupSum (r :: rs)) =
      ks.map (fun k => groupSum rs k + (if k == r.1 then r.2 else 0)) := by
  induction ks with
  | nil => simp
  | cons k ks ih =>
      rw [List.map_cons, List.map_cons, groupSum_cons, ih]

private theorem sum_group_over_keys (rows : List Row) (ks : List Key)
    (hnd : ks.Nodup) (hcov : ∀ r ∈ rows, r.1 ∈ ks) :
    (ks.map (groupSum rows)).sum = totalSum rows := by
  induction rows with
  | nil =>
      have hfun : groupSum [] = fun _ : Key => (0 : Int) := by
        funext k
        simp [groupSum]
      rw [hfun]
      simp [totalSum, sum_map_zero]
  | cons r rs ih =>
      have hcov_rs : ∀ r' ∈ rs, r'.1 ∈ ks := by
        intro r' hr'
        exact hcov r' (by simp [hr'])
      have hih : (ks.map (groupSum rs)).sum = totalSum rs := ih hcov_rs
      have hk : r.1 ∈ ks := hcov r (by simp)
      have hite : (ks.map fun k => if k == r.1 then r.2 else 0).sum = r.2 :=
        sum_map_ite_eq ks r.1 r.2 hnd hk
      have hadd :
          (ks.map fun k => groupSum rs k + (if k == r.1 then r.2 else 0)).sum =
            (ks.map (groupSum rs)).sum +
              (ks.map fun k => if k == r.1 then r.2 else 0).sum :=
        sum_map_add ks (fun k => groupSum rs k) (fun k => if k == r.1 then r.2 else 0)
      rw [map_groupSum_cons, hadd, hih, hite]
      simp [totalSum]
      omega

private theorem fubini (rows : List Row) :
    ((keysOf rows).map (groupSum rows)).sum = totalSum rows := by
  exact sum_group_over_keys rows (keysOf rows) (keysOf_nodup rows) (by
    intro r hr
    exact mem_keysOf_of_mem rows hr)

-------------------------------------------------------------------------------
-- 3. The error band
-------------------------------------------------------------------------------

private theorem bounds_of_natAbs_le {x : Int} {e : Nat} (h : x.natAbs ≤ e) :
    -(e : Int) ≤ x ∧ x ≤ (e : Int) := by
  rcases Int.natAbs_eq x with hx | hx
  · rw [hx]
    have he : ((x.natAbs : Nat) : Int) ≤ (e : Int) := Int.ofNat_le.mpr h
    omega
  · rw [hx]
    have he : ((x.natAbs : Nat) : Int) ≤ (e : Int) := Int.ofNat_le.mpr h
    omega

private theorem band_correct (num den epsG epsT : Nat) (gHat gExact tHat tExact : Int)
    (hg : (gHat - gExact).natAbs ≤ epsG)
    (ht : (tHat - tExact).natAbs ≤ epsT) :
    let D : Int := Int.ofNat den
    let A : Int := Int.ofNat num
    let M : Int := D * Int.ofNat epsG + A * Int.ofNat epsT
    let df : Int := D * gHat - A * tHat
    (M < df → A * tExact < D * gExact) ∧
    (M ≤ -df → ¬ A * tExact < D * gExact) := by
  dsimp only
  let D : Int := Int.ofNat den
  let A : Int := Int.ofNat num
  let EG : Int := Int.ofNat epsG
  let ET : Int := Int.ofNat epsT
  let M : Int := D * EG + A * ET
  let df : Int := D * gHat - A * tHat
  let E : Int := D * gExact - A * tExact
  change (M < df → A * tExact < D * gExact) ∧
    (M ≤ -df → ¬ A * tExact < D * gExact)
  have hD : 0 ≤ D := by dsimp [D]; exact Int.natCast_nonneg den
  have hA : 0 ≤ A := by dsimp [A]; exact Int.natCast_nonneg num
  have hDGabs : (gExact - gHat).natAbs ≤ epsG := by
    have hneg : gExact - gHat = -(gHat - gExact) := by omega
    rw [hneg, Int.natAbs_neg]
    exact hg
  have hDTabs : (tExact - tHat).natAbs ≤ epsT := by
    have hneg : tExact - tHat = -(tHat - tExact) := by omega
    rw [hneg, Int.natAbs_neg]
    exact ht
  have hG := bounds_of_natAbs_le hDGabs
  have hT := bounds_of_natAbs_le hDTabs
  have hDG_up : D * (gExact - gHat) ≤ D * EG := by
    simpa [EG] using (Int.mul_le_mul_of_nonneg_left hG.2 hD)
  have hDG_lo : -(D * EG) ≤ D * (gExact - gHat) := by
    simpa [EG, Int.mul_neg] using (Int.mul_le_mul_of_nonneg_left hG.1 hD)
  have hAT_up : A * (tExact - tHat) ≤ A * ET := by
    simpa [ET] using (Int.mul_le_mul_of_nonneg_left hT.2 hA)
  have hAT_lo : -(A * ET) ≤ A * (tExact - tHat) := by
    simpa [ET, Int.mul_neg] using (Int.mul_le_mul_of_nonneg_left hT.1 hA)
  have herr_up : D * (gExact - gHat) - A * (tExact - tHat) ≤ M := by
    dsimp [M]
    omega
  have herr_lo : -M ≤ D * (gExact - gHat) - A * (tExact - tHat) := by
    dsimp [M]
    omega
  have hident : E = df + (D * (gExact - gHat) - A * (tExact - tHat)) := by
    dsimp [E, df]
    rw [Int.mul_sub, Int.mul_sub]
    omega
  constructor
  · intro hp
    have hpos : 0 < E := by omega
    dsimp [E] at hpos
    omega
  · intro hf
    have hnonpos : E ≤ 0 := by omega
    dsimp [E] at hnonpos
    omega

theorem sound : TinylambOpt.ResearchGoals.FloatFubiniHavingBand.Goal := by
  constructor
  · exact fubini
  · intro num den epsG epsT gHat gExact tHat tExact hden hg ht
    have hband := band_correct num den epsG epsT gHat gExact tHat tExact hg ht
    dsimp only at hband
    unfold classifyBand exactPasses
    by_cases hp : (Int.ofNat den * Int.ofNat epsG + Int.ofNat num * Int.ofNat epsT) <
        (Int.ofNat den * gHat - Int.ofNat num * tHat)
    · rw [ite_eq_left hp]
      exact hband.1 hp
    · by_cases hf : (Int.ofNat den * Int.ofNat epsG + Int.ofNat num * Int.ofNat epsT) ≤
          -(Int.ofNat den * gHat - Int.ofNat num * tHat)
      · rw [ite_eq_right hp, ite_eq_left hf]
        exact hband.2 hf
      · rw [ite_eq_right hp, ite_eq_right hf]
        trivial

end TinylambOpt.ResearchProofs.FloatFubiniHavingBand
