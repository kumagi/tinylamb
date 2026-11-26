import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ScaledFloor.PartitionDomain` — `∀ u x ys, valid u ys → ((List.range (u/(x+1)+1)).map
  (fun t => fan search u t x ys)).sum=ys.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_PartitionDomain

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ScaledFloor

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, hi]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, hi]

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) := (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      rw [Nat.pow_succ]
      omega
    rw [hpow]
    omega

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by omega
      subst hi
      simp only [searchBound]
      exact ⟨⟨hbelow, habove⟩, Nat.le_refl lo, Nat.le_refl lo⟩
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp only [searchBound, hge, ↓reduceIte]
        exact ⟨⟨hbelow, habove⟩, trivial, trivial⟩
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        have hmidlen : mid < xs.toList.length := by simpa using hmid
        have hval : xs[mid]?.getD 0 = xs[mid] := by
          rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hlistval : xs.toList.getD mid 0 = xs[mid] := by
          rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
        have hpow : (2 : Nat) ^ (fuel + 1) = 2 * 2 ^ fuel := by
          simp [Nat.pow_succ, Nat.mul_comm]
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        change Cut xs.toList v
            (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
              else searchBound xs v fuel lo mid) ∧
          lo ≤ (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
            else searchBound xs v fuel lo mid) ∧
          (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
            else searchBound xs v fuel lo mid) ≤ hi
        rw [hval]
        by_cases hv : xs[mid] < v
        · rw [ite_eq_left hv]
          have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
            intro j hj
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              exact hv
            · have hjm : j < mid := by omega
              have hjlen : j < xs.toList.length := by
                simpa using (by omega : j < xs.size)
              by_cases hjl : j < lo
              · exact hbelow j hjl
              · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                  rw [getD_eq_getElem hjlen, getD_eq_getElem hmidlen]
                  exact hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [hlistval] at hrel
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          exact ⟨hc, Nat.le_trans (by omega : lo ≤ mid + 1) hlb, hub⟩
        · rw [ite_eq_right hv]
          have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
            intro j hj hjlen
            by_cases heq : j = mid
            · subst j
              rw [hlistval]
              omega
            · have hmj : mid < j := by omega
              by_cases hjh : hi ≤ j
              · exact habove j hjh hjlen
              · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                  rw [hlistval, getD_eq_getElem hjlen]
                  exact hs.rel_getElem_of_lt hmidlen hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            rw [hpow] at hf
            omega
          obtain ⟨hc, hlb, hub⟩ := ih lo mid hml (by omega) hf' hbelow habove'
          exact ⟨hc, hlb, Nat.le_trans hub (Nat.le_of_lt hmh)⟩

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by
      intro j hj hjlen
      have hjl : j < ys.length := by simpa using hjlen
      omega)
    (by simpa using hs)
  simpa using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem lower_zero {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) : lower ys 0 = 0 := by
  apply Nat.eq_zero_of_not_pos
  intro hp
  have hlt : ys.getD 0 0 < 0 := lower_lt_of_lt hs 0 hp
  omega

private theorem lower_eq_length_of_all_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {u : Nat} (hu : ∀ y ∈ ys, y < u) : lower ys u = ys.length := by
  have hle : lower ys u ≤ ys.length := lower_le_length hs u
  apply Nat.le_antisymm hle
  apply Nat.le_of_not_gt
  intro hnot
  have hi : lower ys u < ys.length := by omega
  have hge := lower_le_of_ge hs u (Nat.le_refl _) hi
  have hmem : ys.getD (lower ys u) 0 ∈ ys := by
    rw [getD_eq_getElem hi]
    exact List.getElem_mem hi
  have hylt := hu (ys.getD (lower ys u) 0) hmem
  omega

private theorem fan_eq_lower_sub {u t x : Nat} {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) :
    fan search u t x ys =
      lower ys (min u ((t + 1) * (x + 1))) - lower ys (t * (x + 1)) := by
  unfold fan
  change ((TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional
      (t * (x + 1)) (min u ((t + 1) * (x + 1)))).map
      (fun (a, b) => lower ys b - lower ys a)).sum =
    lower ys (min u ((t + 1) * (x + 1))) - lower ys (t * (x + 1))
  by_cases hab : t * (x + 1) < min u ((t + 1) * (x + 1))
  · simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
  · have hmono := lower_mono hs (Nat.le_of_not_gt hab)
    simp [TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hab]
    omega

private theorem sum_range_telescope (h : Nat → Nat)
    (hm : ∀ {i j : Nat}, i ≤ j → h i ≤ h j) (n : Nat) :
    ((List.range n).map (fun i => h (i + 1) - h i)).sum = h n - h 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp only [List.map_singleton, List.sum_singleton]
      have hmn : h n ≤ h (n + 1) := hm (Nat.le_succ n)
      have h0n : h 0 ≤ h n := hm (Nat.zero_le n)
      omega

theorem sound_PartitionDomain : ScaledFloor.PartitionDomain.Goal := by
  intro u x ys hv
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  let w := x + 1
  have hw : 0 < w := by omega
  let q := u / w
  have hqmul : q * w ≤ u := by
    simpa [q] using Nat.div_mul_le_self u w
  have hmod : u % w < w := Nat.mod_lt u hw
  have hdecomp : q * w + u % w = u := by
    simpa [q, Nat.mul_comm] using Nat.div_add_mod u w
  have hqbound : u < (q + 1) * w := by
    rw [Nat.succ_mul]
    omega
  let h : Nat → Nat := fun i => lower ys (i * w)
  have hhmono : ∀ {i j : Nat}, i ≤ j → h i ≤ h j := by
    intro i j hij
    unfold h
    apply lower_mono hs
    exact Nat.mul_le_mul_right w hij
  have hfan_head : ∀ t, t < q → fan search u t x ys = h (t + 1) - h t := by
    intro t ht
    rw [fan_eq_lower_sub hs]
    have htq : t + 1 ≤ q := by omega
    have htmul : (t + 1) * w ≤ q * w := Nat.mul_le_mul_right w htq
    have htu : (t + 1) * w ≤ u := by omega
    rw [Nat.min_eq_right htu]
  have hfan_last : fan search u q x ys = lower ys u - h q := by
    rw [fan_eq_lower_sub hs]
    have hmin : min u ((q + 1) * w) = u := Nat.min_eq_left (Nat.le_of_lt hqbound)
    rw [hmin]
  have hrange : List.range (u / (x + 1) + 1) = List.range q ++ [q] := by
    simp [q, w, List.range_succ]
  rw [hrange, List.map_append, List.sum_append]
  simp only [List.map_singleton, List.sum_singleton]
  have hsumhead : ((List.range q).map (fun t => fan search u t x ys)).sum = h q - h 0 := by
    rw [show ((List.range q).map (fun t => fan search u t x ys)) =
        ((List.range q).map (fun t => h (t + 1) - h t)) by
      apply List.map_congr_left
      intro t ht
      exact hfan_head t (List.mem_range.mp ht)]
    exact sum_range_telescope h hhmono q
  rw [hsumhead, hfan_last]
  have hzero : h 0 = 0 := by
    unfold h
    simpa using lower_zero hs
  have hlast : lower ys u = ys.length := lower_eq_length_of_all_lt hs hv.2
  have hqle : h q ≤ ys.length := by
    unfold h
    exact lower_le_length hs (q * w)
  rw [hzero, hlast]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ScaledFloor_PartitionDomain
