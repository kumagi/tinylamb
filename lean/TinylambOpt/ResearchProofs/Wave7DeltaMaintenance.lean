import TinylambOpt.ResearchGoals.ExecutionStrategies.DeltaMaintenance

namespace TinylambOpt.ResearchProofs.Wave7DeltaMaintenance

open TinylambOpt.ResearchGoals.DeltaMaintenance

private theorem count_filter_eq (b : Bag) (x k : Int) :
    (b.filter fun y => y == x).count k = if x == k then b.count k else 0 := by
  by_cases h : x == k
  · have hk : k == x := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
    rw [if_pos h]
    exact List.count_filter (a := k) (p := fun y : Int => y == x) (l := b) hk
  · have hk : ¬ k == x := by
      intro hk'
      exact h (beq_iff_eq.mpr (beq_iff_eq.mp hk').symm)
    rw [if_neg h]
    apply List.count_eq_zero_of_not_mem
    intro hm
    simp only [List.mem_filter] at hm
    exact hk hm.2

private theorem count_joinKeys (a b : Bag) (k : Int) :
    (joinKeys a b).count k = a.count k * b.count k := by
  unfold joinKeys
  rw [List.count_flatMap]
  induction a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, Function.comp_apply]
      rw [count_filter_eq b x k, ih, List.count_cons]
      by_cases h : x == k
      · simp [h, Nat.succ_mul, Nat.add_comm]
      · simp [h]

private theorem count_chainKeys (a b c : Bag) (k : Int) :
    (joinKeys (joinKeys a b) c).count k = a.count k * b.count k * c.count k := by
  rw [count_joinKeys, count_joinKeys]

theorem JoinBatch_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.JoinBatch.Goal := by
  intro a b a' b' k
  unfold delta freq
  repeat rw [count_joinKeys]
  push_cast
  simp only [Int.sub_eq_add_neg, Int.mul_add, Int.add_mul, Int.neg_mul, Int.mul_neg]
  omega

theorem ChainBatch_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.ChainBatch.Goal := by
  intro a b c a' b' c' k
  unfold delta freq
  repeat rw [count_chainKeys]
  push_cast
  simp only [Int.sub_eq_add_neg, Int.mul_add, Int.add_mul, Int.neg_mul, Int.mul_neg]
  omega

private theorem count_filter_ne (b : Bag) (x k : Int) (h : ¬ x == k) :
    (b.filter fun y => ! y == x).count k = b.count k := by
  apply List.count_filter
  have hk : ¬ k == x := by
    intro hk'
    exact h (beq_iff_eq.mpr (beq_iff_eq.mp hk').symm)
  simp [hk]

private theorem count_filter_eq_zero (b : Bag) (x k : Int) (h : x == k) :
    (b.filter fun y => ! y == x).count k = 0 := by
  apply List.count_eq_zero_of_not_mem
  intro hm
  simp only [List.mem_filter] at hm
  have hk : k == x := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
  simp [hk] at hm

private theorem count_eraseDups (xs : Bag) (k : Int) :
    (xs.eraseDups).count k = if xs.count k == 0 then 0 else 1 := by
  match xs with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons, List.count_cons]
      rw [count_eraseDups (xs.filter fun b => ! b == x) k]
      by_cases h : x == k
      · rw [count_filter_eq_zero xs x k h]
        have hpos : 0 < List.count k (x :: xs) := by
          rw [List.count_cons]
          simp [h]
        have hnzero : ¬ List.count k (x :: xs) = 0 := by omega
        simp [h, hnzero]
      · rw [count_filter_ne xs x k h]
        rw [List.count_cons]
        simp [h]
termination_by xs.length
decreasing_by
  apply Nat.lt_succ_of_le
  exact List.length_filter_le _ _

private theorem thresholdChange_count (xs ys : Bag) (k : Int) :
    thresholdChange (freq xs k) (delta xs ys k) =
      ((if ys.count k == 0 then 0 else 1 : Nat) : Int) -
        ((if xs.count k == 0 then 0 else 1 : Nat) : Int) := by
  have hnd : (xs.count k : Int) + ((ys.count k : Int) - (xs.count k : Int)) = (ys.count k : Int) := by omega
  by_cases hx : xs.count k == 0
  · have hx0 : xs.count k = 0 := beq_iff_eq.mp hx
    by_cases hy : ys.count k == 0
    · have hy0 : ys.count k = 0 := beq_iff_eq.mp hy
      simp [thresholdChange, freq, delta, hx0, hy0, hnd]
    · have hyne : ys.count k ≠ 0 := by intro h0; apply hy; simp [h0]
      have hypos : 0 < ys.count k := by omega
      have hymem : k ∈ ys := List.count_pos_iff.mp hypos
      have hyposI : 0 < (ys.count k : Int) := by omega
      simp [thresholdChange, freq, delta, hx0, hyne, hymem, hyposI, hnd]
  · have hxne : xs.count k ≠ 0 := by intro h0; apply hx; simp [h0]
    have hxpos : 0 < xs.count k := by omega
    have hxmem : k ∈ xs := List.count_pos_iff.mp hxpos
    by_cases hy : ys.count k == 0
    · have hy0 : ys.count k = 0 := beq_iff_eq.mp hy
      simp [thresholdChange, freq, delta, hxne, hy0, hxmem, hnd]
      omega
    · have hyne : ys.count k ≠ 0 := by intro h0; apply hy; simp [h0]
      have hypos : 0 < ys.count k := by omega
      have hymem : k ∈ ys := List.count_pos_iff.mp hypos
      have hyposI : 0 < (ys.count k : Int) := by omega
      simp [thresholdChange, freq, delta, hxne, hyne, hxmem, hymem, hyposI, hnd]

theorem DistinctCrossing_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.DistinctCrossing.Goal := by
  intro xs ys k
  unfold delta freq
  repeat rw [count_eraseDups]
  have hT := thresholdChange_count xs ys k
  simp only [freq, delta] at hT
  change thresholdChange (↑(List.count k xs)) (↑(List.count k ys) - ↑(List.count k xs)) = _ at hT
  rw [hT]

private theorem count_semi (a b : Bag) (k : Int) :
    (semi a b).count k = if b.contains k = true then a.count k else 0 := by
  unfold semi
  by_cases h : b.contains k = true
  · rw [List.count_filter (a := k) (p := fun x => b.contains x) h]
    rw [if_pos h]
  · have hnot : ¬ k ∈ a.filter (fun x => b.contains x) := by
      intro hm
      simp only [List.mem_filter] at hm
      exact h hm.2
    rw [List.count_eq_zero_of_not_mem hnot]
    rw [if_neg h]

private theorem threshold_eq_indicator (n d : Int) (hnnonneg : 0 ≤ n) (hsum_nonneg : 0 ≤ n + d) :
    thresholdChange n d = (if 0 < n + d then (1 : Int) else 0) - (if 0 < n then (1 : Int) else 0) := by
  by_cases hn0 : n == 0
  · have hn : n = 0 := beq_iff_eq.mp hn0
    subst n
    by_cases hspos : 0 < d
    · simp [thresholdChange, hspos]
    · have hs0 : d == 0 := by
        apply beq_iff_eq.mpr
        omega
      have hsposF : ¬ 0 < d := hspos
      simp [thresholdChange, hs0, hsposF]
  · have hnne : n ≠ 0 := by
      intro h
      apply hn0
      exact beq_iff_eq.mpr h
    have hnpos : 0 < n := by omega
    by_cases hs0 : n + d == 0
    · have hs : n + d = 0 := beq_iff_eq.mp hs0
      have hsum_ne : ¬ 0 < n + d := by omega
      simp [thresholdChange, hn0, hnpos, hs, hsum_ne]
    · have hsne : n + d ≠ 0 := by
        intro h
        apply hs0
        exact beq_iff_eq.mpr h
      have hsumpos : 0 < n + d := by omega
      simp [thresholdChange, hn0, hnpos, hsumpos, hsne]

theorem SemiWitness_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.SemiWitness.Goal := by
  intro a b a' b' k
  unfold delta freq
  repeat rw [count_semi]
  have hnd' : 0 ≤ (b.count k : Int) + ((b'.count k : Int) - (b.count k : Int)) := by omega
  have hT := threshold_eq_indicator (freq b k) (delta b b' k) (by simp [freq]) (by simpa [freq, delta] using hnd')
  simp only [freq, delta] at hT
  change thresholdChange (↑(List.count k b)) (↑(List.count k b') - ↑(List.count k b)) = _ at hT
  rw [hT]
  by_cases hb' : b'.contains k = true
  · have hb'mem : k ∈ b' := List.contains_iff_mem.mp hb'
    have hb'pos : 0 < b'.count k := List.count_pos_iff.mpr hb'mem
    have hb'posI : 0 < (b'.count k : Int) := by omega
    by_cases hb : b.contains k = true
    · have hbmem : k ∈ b := List.contains_iff_mem.mp hb
      have hbpos : 0 < b.count k := List.count_pos_iff.mpr hbmem
      have hbposI : 0 < (b.count k : Int) := by omega
      have hsumposI : 0 < (b.count k : Int) + ((b'.count k : Int) - (b.count k : Int)) := by omega
      simp [hb'mem, hbmem, hbposI, hsumposI]
    · have hbmem : ¬ k ∈ b := by intro hm; apply hb; exact List.contains_iff_mem.mpr hm
      have hb0 : b.count k = 0 := List.count_eq_zero_of_not_mem hbmem
      simp [hb'mem, hbmem, hb0]
  · have hb'mem : ¬ k ∈ b' := by intro hm; apply hb'; exact List.contains_iff_mem.mpr hm
    have hb'0 : b'.count k = 0 := List.count_eq_zero_of_not_mem hb'mem
    by_cases hb : b.contains k = true
    · have hbmem : k ∈ b := List.contains_iff_mem.mp hb
      have hbpos : 0 < b.count k := List.count_pos_iff.mpr hbmem
      have hbposI : 0 < (b.count k : Int) := by omega
      have hz : (b.count k : Int) + -(b.count k : Int) = 0 := by omega
      simp [hb'mem, hbmem, hb'0, hz]
    · have hbmem : ¬ k ∈ b := by intro hm; apply hb; exact List.contains_iff_mem.mpr hm
      have hb0 : b.count k = 0 := List.count_eq_zero_of_not_mem hbmem
      simp [hb'mem, hbmem, hb'0, hb0]

private theorem length_filter_keyEq_some (b : TinylambOpt.ResearchGoals.OuterJoinCounts.Rows)
    (v : Int) :
    (b.filter (TinylambOpt.ResearchGoals.OuterJoinCounts.keyEq (some v))).length =
      b.count (some v) := by
  rw [← List.countP_eq_length_filter, List.count_eq_countP]
  congr 1
  funext y
  cases y with
  | none => simp [TinylambOpt.ResearchGoals.OuterJoinCounts.keyEq]
  | some w =>
      by_cases h : v == w
      · have h' : w == v := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
        simp [TinylambOpt.ResearchGoals.OuterJoinCounts.keyEq, h, h']
      · have h' : ¬ w == v := by
          intro hk
          exact h (beq_iff_eq.mpr (beq_iff_eq.mp hk).symm)
        simp [TinylambOpt.ResearchGoals.OuterJoinCounts.keyEq, h, h']

private theorem length_filter_keyEq_none (b : TinylambOpt.ResearchGoals.OuterJoinCounts.Rows) :
    (b.filter (TinylambOpt.ResearchGoals.OuterJoinCounts.keyEq none)).length = 0 := by
  simp [TinylambOpt.ResearchGoals.OuterJoinCounts.keyEq]

private theorem count_leftKeys_none (a b : TinylambOpt.ResearchGoals.OuterJoinCounts.Rows) :
    (leftKeys a b).count none = a.count none := by
  unfold leftKeys
  induction a with
  | nil => simp
  | cons x xs ih =>
      rw [List.flatMap_cons, List.count_append, List.count_replicate, ih]
      by_cases h : x == none
      · have hx : x = none := beq_iff_eq.mp h
        subst x
        rw [length_filter_keyEq_none]
        simp [h, Nat.add_comm]
      · rw [List.count_cons]
        simp [h]

private theorem count_leftKeys_some (a b : TinylambOpt.ResearchGoals.OuterJoinCounts.Rows) (v : Int) :
    (leftKeys a b).count (some v) = a.count (some v) * max 1 (b.count (some v)) := by
  unfold leftKeys
  induction a with
  | nil => simp
  | cons x xs ih =>
      rw [List.flatMap_cons, List.count_append, List.count_replicate, ih, List.count_cons]
      by_cases h : x == some v
      · have hx : x = some v := beq_iff_eq.mp h
        subst x
        rw [length_filter_keyEq_some]
        rw [Nat.add_mul]
        simp [h, Nat.add_comm]
      · simp [h]

private theorem cast_max_one (n : Nat) : ((max 1 n : Nat) : Int) = max 1 (n : Int) := by
  by_cases h : 1 ≤ n
  · rw [Nat.max_eq_right h]
    have hi : (1 : Int) ≤ (n : Int) := by omega
    rw [Int.max_eq_right hi]
  · have h' : n ≤ 1 := by omega
    rw [Nat.max_eq_left h']
    have hi : (n : Int) ≤ 1 := by omega
    rw [Int.max_eq_left hi]
    simp

theorem OuterPadding_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.OuterPadding.Goal := by
  intro a b b' k
  cases k with
  | none =>
      rw [count_leftKeys_none, count_leftKeys_none]
      simp
  | some v =>
      rw [count_leftKeys_some, count_leftKeys_some]
      push_cast
      simp [cast_max_one, Int.mul_sub]

private theorem common_count (a b : Bag) (k : Int) :
    (common a b).count k = min (a.count k) (b.count k) := by
  induction a generalizing b with
  | nil => simp [common]
  | cons x xs ih =>
      by_cases hmem : x ∈ b
      · have hmemC : b.contains x = true := List.contains_iff_mem.mpr hmem
        rw [common, if_pos hmemC, List.count_cons, ih, List.count_erase]
        by_cases hxk : x == k
        · have hpos : 0 < b.count k := by
            apply List.count_pos_iff.mpr
            have : k ∈ b := by
              have hxk' : x = k := beq_iff_eq.mp hxk
              simpa [hxk'] using hmem
            exact this
          simp [hxk, hpos]
          have hxcount : List.count k (x :: xs) = xs.count k + 1 := by
            rw [List.count_cons]
            simp [hxk]
          rw [hxcount]
          by_cases hlt : xs.count k < b.count k
          · have h1 : min (xs.count k) (b.count k - 1) = xs.count k := by
              apply Nat.min_eq_left
              omega
            have h2 : min (xs.count k + 1) (b.count k) = xs.count k + 1 := by
              apply Nat.min_eq_left
              omega
            rw [h1, h2]
          · have h1 : min (xs.count k) (b.count k - 1) = b.count k - 1 := by
              apply Nat.min_eq_right
              omega
            have h2 : min (xs.count k + 1) (b.count k) = b.count k := by
              apply Nat.min_eq_right
              omega
            rw [h1, h2]
            omega
        · have hkx : ¬ k == x := by
            intro hk'
            exact hxk (beq_iff_eq.mpr (beq_iff_eq.mp hk').symm)
          simp [List.count_cons, hxk, hkx]
      · have hmemC : ¬ b.contains x = true := by
          intro hc
          apply hmem
          exact List.contains_iff_mem.mp hc
        rw [common, if_neg hmemC, ih]
        by_cases hxk : x == k
        · have hb0 : b.count k = 0 := by
            apply List.count_eq_zero_of_not_mem
            intro hk
            apply hmem
            have hxk' : x = k := beq_iff_eq.mp hxk
            simpa [hxk'] using hk
          rw [List.count_cons]
          simp [hxk, hb0]
        · have hkx : ¬ k == x := by
            intro hk'
            exact hxk (beq_iff_eq.mpr (beq_iff_eq.mp hk').symm)
          rw [List.count_cons]
          simp [hxk, hkx]

private theorem count_foldl_erase_le (xs b : Bag) (k : Int) :
    (xs.foldl List.erase b).count k ≤ b.count k := by
  induction xs generalizing b with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans (ih (b.erase x)) (by rw [List.count_erase]; omega)

theorem IntersectionDelete_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.IntersectionDelete.Goal := by
  intro a b removed k
  dsimp only
  unfold delta freq
  rw [common_count a (removed.foldl List.erase b) k, common_count a b k]
  have hle : (removed.foldl List.erase b).count k ≤ b.count k := count_foldl_erase_le removed b k
  by_cases hle' : (removed.foldl List.erase b).count k ≤ a.count k
  · have hmin1 : min (a.count k) ((removed.foldl List.erase b).count k) =
        (removed.foldl List.erase b).count k := Nat.min_eq_right hle'
    have hmin2 : min (a.count k) (b.count k) ≥ (removed.foldl List.erase b).count k := by
      by_cases h : a.count k ≤ b.count k
      · rw [Nat.min_eq_left h]
        omega
      · rw [Nat.min_eq_right (by omega)]
        omega
    rw [hmin1]
    omega
  · have hlt : a.count k < (removed.foldl List.erase b).count k := by omega
    have hmin1 : min (a.count k) ((removed.foldl List.erase b).count k) = a.count k :=
      Nat.min_eq_left (by omega)
    have hmin2 : min (a.count k) (b.count k) = a.count k := by
      apply Nat.min_eq_left
      omega
    rw [hmin1, hmin2]
    have hz : (a.count k - (removed.foldl List.erase b).count k : Nat) = 0 := by omega
    simp [hz]

private theorem removeBatch_nil (dels : Bag) : removeBatch [] dels = [] := by
  induction dels with
  | nil => simp [removeBatch]
  | cons d ds ih => simpa [removeBatch] using ih

private theorem count_removeBatch (xs dels : Bag) (k : Int) :
    (removeBatch xs dels).count k = xs.count k - dels.count k := by
  induction dels generalizing xs with
  | nil => simp [removeBatch]
  | cons d ds ih =>
      change (removeBatch (xs.erase d) ds).count k = xs.count k - (d :: ds).count k
      rw [ih, List.count_erase, List.count_cons]
      by_cases h : d == k <;> simp [h, Nat.sub_sub, Nat.add_comm, Nat.add_assoc]

private theorem removeBatch_mem_of_count (xs dels : Bag) (k : Int)
    (h : dels.count k < xs.count k) : k ∈ removeBatch xs dels := by
  apply List.count_pos_iff.mp
  rw [count_removeBatch]
  omega

private theorem removeBatch_mem_lower (xs dels : Bag) (v : Int)
    (hv : ∀ z ∈ xs, v ≤ z) : ∀ z ∈ removeBatch xs dels, v ≤ z := by
  intro z hz
  induction dels generalizing xs with
  | nil => exact hv z hz
  | cons d ds ih =>
      simp only [removeBatch, List.foldl_cons] at hz
      apply ih (xs.erase d)
      · intro w hw
        exact hv w (List.Sublist.mem hw List.erase_sublist)
      · exact hz

private theorem min_append_of_min (L A : List Int) (v : Int)
    (hv : v ∈ L) (hlo : ∀ z ∈ L, v ≤ z) :
    (v :: A).min? = (L ++ A).min? := by
  have hL : L.min? = some v := List.min?_eq_some_iff.mpr ⟨hv, hlo⟩
  cases hAm : A.min? with
  | none =>
      have hA : A = [] := List.min?_eq_none_iff.mp hAm
      subst A
      simp [hL, List.min?_cons]
  | some a =>
      have ha : a ∈ A ∧ ∀ z ∈ A, a ≤ z := List.min?_eq_some_iff.mp hAm
      have hR : (L ++ A).min? = some (min v a) := by
        apply List.min?_eq_some_iff.mpr
        constructor
        · by_cases hva : v ≤ a
          · rw [Int.min_eq_left hva]
            exact List.mem_append.mpr (Or.inl hv)
          · rw [Int.min_eq_right (by omega)]
            exact List.mem_append.mpr (Or.inr ha.1)
        · intro z hz
          rcases List.mem_append.mp hz with hzL | hzA
          · have hvz : v ≤ z := hlo z hzL
            by_cases hva : v ≤ a
            · rw [Int.min_eq_left hva]
              exact hvz
            · rw [Int.min_eq_right (by omega)]
              omega
          · have haz : a ≤ z := ha.2 z hzA
            by_cases hva : v ≤ a
            · rw [Int.min_eq_left hva]
              omega
            · rw [Int.min_eq_right (by omega)]
              exact haz
      have hLa : (v :: A).min? = some (min v a) := by
        rw [List.min?_cons, hAm]
        simp
      rw [hLa, hR]

private theorem repairMin_some_case (xs dels adds : Bag) (v : Int)
    (hmin : xs.min? = some v) :
    repairMin xs dels adds = (removeBatch xs dels ++ adds).min? := by
  have hv : v ∈ xs ∧ ∀ z ∈ xs, v ≤ z := List.min?_eq_some_iff.mp hmin
  simp only [repairMin]
  rw [hmin]
  simp only
  by_cases h : dels.count v < xs.count v
  · rw [if_pos h]
    have hmem : v ∈ removeBatch xs dels := removeBatch_mem_of_count xs dels v h
    have hlo : ∀ z ∈ removeBatch xs dels, v ≤ z := removeBatch_mem_lower xs dels v hv.2
    exact min_append_of_min (removeBatch xs dels) adds v hmem hlo
  · rw [if_neg h]

theorem MinimumRepair_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.MinimumRepair.Goal := by
  intro xs dels adds
  cases xs with
  | nil => simp [repairMin, removeBatch_nil]
  | cons x xs =>
      cases hxs : xs.min? with
      | none =>
          have hnil : xs = [] := List.min?_eq_none_iff.mp hxs
          have hmin : (x :: xs).min? = some x := by simp [hnil]
          exact repairMin_some_case (x :: xs) dels adds x hmin
      | some m =>
          have hm : m ∈ xs ∧ ∀ z ∈ xs, m ≤ z := List.min?_eq_some_iff.mp hxs
          by_cases hxm : x ≤ m
          · have hmin : (x :: xs).min? = some x := by
              apply List.min?_eq_some_iff.mpr
              constructor
              · simp
              · intro z hz
                simp only [List.mem_cons] at hz
                rcases hz with rfl | hz
                · omega
                · have hmz : m ≤ z := hm.2 z hz
                  omega
            exact repairMin_some_case (x :: xs) dels adds x hmin
          · have hmin : (x :: xs).min? = some m := by
              apply List.min?_eq_some_iff.mpr
              constructor
              · exact List.mem_cons.mpr (Or.inr hm.1)
              · intro z hz
                simp only [List.mem_cons] at hz
                rcases hz with rfl | hz
                · omega
                · exact hm.2 z hz
            exact repairMin_some_case (x :: xs) dels adds m hmin


private theorem sorted_mergeSort (xs : List Int) : xs.mergeSort.Pairwise (fun a b => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Int => a ≤ b)
    (by intro a b c hab hbc
        have hab' : a ≤ b := of_decide_eq_true hab
        have hbc' : b ≤ c := of_decide_eq_true hbc
        exact decide_eq_true (Int.le_trans hab' hbc'))
    (by intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have h' : b ≤ a := by omega
          simp [h, h']) xs
  exact h.imp (by intro a b hab; exact of_decide_eq_true hab)

private theorem mergeSort_congr_perm {l1 l2 : List Int} (p : l1.Perm l2) : l1.mergeSort = l2.mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b => a ≤ b)
  · intro a b ha hb hab hba
    omega
  · exact sorted_mergeSort l1
  · exact sorted_mergeSort l2
  · exact (List.mergeSort_perm l1 (fun a b => a ≤ b)).trans
      (p.trans (List.mergeSort_perm l2 (fun a b => a ≤ b)).symm)

private theorem countLE_ge_of_getElem (L : List Int) (hs : L.Pairwise (fun a b => a ≤ b))
    (i : Nat) (hi : i < L.length) (t : Int) (hit : L[i] ≤ t) :
    i + 1 ≤ (L.filter fun z => z ≤ t).length := by
  let T := L.take (i + 1)
  have hTlen : T.length = i + 1 := by
    dsimp [T]
    rw [List.length_take]
    exact Nat.min_eq_left (by omega)
  have hTall : ∀ z ∈ T, z ≤ t := by
    intro z hz
    rw [List.mem_iff_getElem] at hz
    rcases hz with ⟨j, hj, hzEq⟩
    have hjT : j < T.length := hj
    have hjL : j < L.length := by
      dsimp [T] at hjT
      rw [List.length_take] at hjT
      omega
    have hzL : L[j] = z := by
      rw [← hzEq]
      dsimp [T]
      exact (List.getElem_take (xs := L) (j := i + 1) (i := j)).symm
    rw [← hzL]
    by_cases hji : j = i
    · subst j
      exact hit
    · have hji' : j < i := by omega
      have hrel := hs.rel_getElem_of_lt hjL hi hji'
      exact Int.le_trans hrel hit
  have hTfilter : T.filter (fun z => z ≤ t) = T := by
    apply List.filter_eq_self.mpr
    intro z hz
    exact decide_eq_true (hTall z hz)
  have hsub : (T.filter fun z => z ≤ t).Sublist (L.filter fun z => z ≤ t) := by
    exact List.Sublist.filter _ (List.take_sublist (i+1) L)
  have hlen := hsub.length_le
  rw [hTfilter, hTlen] at hlen
  exact hlen

private theorem countLE_le_of_getElem_gt (L : List Int) (hs : L.Pairwise (fun a b => a ≤ b))
    (i : Nat) (hi : i < L.length) (t : Int) (ht : t < L[i]) :
    (L.filter fun z => z ≤ t).length ≤ i := by
  have hdropnil : (L.drop i).filter (fun z => z ≤ t) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro b hb
    rw [List.mem_iff_getElem] at hb
    rcases hb with ⟨j, hj, hbEq⟩
    have hidx : i + j < L.length := by
      have hlen := List.length_drop (l := L) (i := i)
      have : j < (L.drop i).length := hj
      omega
    have hbL : L[i + j] = b := by
      rw [← hbEq]
      exact (List.getElem_drop (xs := L) (i := i) (j := j)).symm
    by_cases hj0 : j = 0
    · subst j
      rw [← hbL]
      intro hle
      exact (Int.not_le_of_gt ht) (of_decide_eq_true hle)
    · have hrel : L[i] ≤ L[i + j] := hs.rel_getElem_of_lt hi hidx (by omega)
      rw [← hbL]
      intro hle
      exact (Int.not_le_of_gt (Int.lt_of_lt_of_le ht hrel)) (of_decide_eq_true hle)
  have hsplit : L.filter (fun z => z ≤ t) =
      (L.take i).filter (fun z => z ≤ t) ++ (L.drop i).filter (fun z => z ≤ t) := by
    conv => lhs; rw [← List.take_append_drop i L]
    rw [List.filter_append]
  rw [hsplit, hdropnil]
  simp
  have hlen : ((L.take i).filter fun z => z ≤ t).length ≤ (L.take i).length :=
    List.length_filter_le _ _
  rw [List.length_take] at hlen
  omega

private theorem sorted_take_eq_of_countLE_min (L1 L2 : List Int) (k : Nat)
    (h1 : L1.Pairwise (fun a b => a ≤ b)) (h2 : L2.Pairwise (fun a b => a ≤ b))
    (hmin : ∀ t : Int, min k ((L1.filter fun z => z ≤ t).length) =
        min k ((L2.filter fun z => z ≤ t).length)) :
    L1.take k = L2.take k := by
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_take, List.getElem?_take]
  by_cases hik : i < k
  · simp only [if_pos hik]
    by_cases hL1 : i < L1.length
    · by_cases hL2 : i < L2.length
      · have hle : L1[i] ≤ L2[i] := by
          by_cases hle' : L1[i] ≤ L2[i]
          · exact hle'
          · have hlt : L2[i] < L1[i] := by omega
            have hge2 : i + 1 ≤ (L2.filter fun z => z ≤ L2[i]).length :=
              countLE_ge_of_getElem L2 h2 i hL2 (L2[i]) (by omega)
            have hle1 : (L1.filter fun z => z ≤ L2[i]).length ≤ i :=
              countLE_le_of_getElem_gt L1 h1 i hL1 (L2[i]) hlt
            have hm := hmin (L2[i])
            have hmin1ge : i + 1 ≤ min k ((L1.filter fun z => z ≤ L2[i]).length) := by
              rw [hm]
              exact Nat.le_min.mpr ⟨by omega, hge2⟩
            have hmin1le : min k ((L1.filter fun z => z ≤ L2[i]).length) ≤ i :=
              Nat.le_trans (Nat.min_le_right _ _) hle1
            omega
        have hge : L2[i] ≤ L1[i] := by
          by_cases hge' : L2[i] ≤ L1[i]
          · exact hge'
          · have hlt : L1[i] < L2[i] := by omega
            have hge1 : i + 1 ≤ (L1.filter fun z => z ≤ L1[i]).length :=
              countLE_ge_of_getElem L1 h1 i hL1 (L1[i]) (by omega)
            have hle2 : (L2.filter fun z => z ≤ L1[i]).length ≤ i :=
              countLE_le_of_getElem_gt L2 h2 i hL2 (L1[i]) hlt
            have hm := hmin (L1[i])
            have hmin2le : min k ((L2.filter fun z => z ≤ L1[i]).length) ≤ i :=
              Nat.le_trans (Nat.min_le_right _ _) hle2
            have hmin2ge : i + 1 ≤ min k ((L2.filter fun z => z ≤ L1[i]).length) := by
              rw [← hm]
              exact Nat.le_min.mpr ⟨by omega, hge1⟩
            omega
        have heq : L1[i] = L2[i] := by omega
        rw [List.getElem?_eq_getElem hL1, List.getElem?_eq_getElem hL2, heq]
      · have hge1 : i + 1 ≤ (L1.filter fun z => z ≤ L1[i]).length :=
          countLE_ge_of_getElem L1 h1 i hL1 (L1[i]) (by omega)
        have hle2 : (L2.filter fun z => z ≤ L1[i]).length ≤ i := by
          have hlen : L2.length ≤ i := by omega
          have hsub : (L2.filter fun z => z ≤ L1[i]).Sublist L2 := List.filter_sublist
          have hs := hsub.length_le
          omega
        have hm := hmin (L1[i])
        have hmin1ge : i + 1 ≤ min k ((L1.filter fun z => z ≤ L1[i]).length) :=
          Nat.le_min.mpr ⟨by omega, hge1⟩
        have hmin2le : min k ((L2.filter fun z => z ≤ L1[i]).length) ≤ i :=
          Nat.le_trans (Nat.min_le_right _ _) hle2
        rw [hm] at hmin1ge
        omega
    · by_cases hL2 : i < L2.length
      · have hge2 : i + 1 ≤ (L2.filter fun z => z ≤ L2[i]).length :=
          countLE_ge_of_getElem L2 h2 i hL2 (L2[i]) (by omega)
        have hle1 : (L1.filter fun z => z ≤ L2[i]).length ≤ i := by
          have hlen : L1.length ≤ i := by omega
          have hsub : (L1.filter fun z => z ≤ L2[i]).Sublist L1 := List.filter_sublist
          have hs := hsub.length_le
          omega
        have hm := hmin (L2[i])
        have hmin2ge : i + 1 ≤ min k ((L2.filter fun z => z ≤ L2[i]).length) :=
          Nat.le_min.mpr ⟨by omega, hge2⟩
        have hmin1le : min k ((L1.filter fun z => z ≤ L2[i]).length) ≤ i :=
          Nat.le_trans (Nat.min_le_right _ _) hle1
        rw [← hm] at hmin2ge
        omega
      · rw [List.getElem?_eq_none_iff.mpr (by omega), List.getElem?_eq_none_iff.mpr (by omega)]
  · simp only [if_neg hik]
private theorem exists_decomp_of_mem {α : Type} (b : α) :
    ∀ {C : List α}, b ∈ C → ∃ t r : List α, C = t ++ b :: r := by
  intro C h
  induction C with
  | nil => exact False.elim (List.not_mem_nil h)
  | cons x xs ih =>
      rcases List.mem_cons.mp h with rfl | h
      · exact ⟨[], xs, rfl⟩
      · rcases ih h with ⟨t, r, heq⟩
        exact ⟨x :: t, r, by rw [heq, ← List.cons_append]⟩

private theorem filter_length_le_of_count_le (L1 L2 : List Int) (p : Int → Bool) :
    (∀ v, L1.count v ≤ L2.count v) → (L1.filter p).length ≤ (L2.filter p).length := by
  induction L1 generalizing L2 with
  | nil => intro h; simp
  | cons a l ih =>
      intro h
      have hpos : 0 < L2.count a := by
        have ha := h a
        rw [List.count_cons] at ha
        simp at ha
        omega
      have hamem : a ∈ L2 := List.count_pos_iff.mp hpos
      rcases exists_decomp_of_mem a hamem with ⟨t, r, hL2⟩
      have hsub : ∀ v, l.count v ≤ (t ++ r).count v := by
        intro v
        have hv := h v
        rw [List.count_cons, hL2, List.count_append, List.count_cons] at hv
        rw [List.count_append]
        omega
      have hi := ih (t ++ r) hsub
      rw [List.filter_append, List.length_append] at hi
      rw [hL2]
      by_cases hpa : p a = true
      · simp [hpa]
        omega
      · simp [hpa]
        omega

private theorem length_removeBatch_ge (P D : List Int) : P.length - D.length ≤ (D.foldl List.erase P).length := by
  induction D generalizing P with
  | nil => simp
  | cons d ds ih =>
      change P.length - (ds.length + 1) ≤ (ds.foldl List.erase (P.erase d)).length
      have hlen : P.length - 1 ≤ (P.erase d).length := by
        rw [List.length_erase]
        split <;> omega
      have hi := ih (P.erase d)
      omega
private theorem count_foldl_erase (xs dels : Bag) (k : Int) :
    (dels.foldl List.erase xs).count k = xs.count k - dels.count k := by
  simpa [removeBatch] using count_removeBatch xs dels k

private theorem take_eq_self_of_length_le (L : List Int) {n : Nat} (h : L.length ≤ n) : L.take n = L := by
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_take]
  by_cases hi : i < n
  · simp [hi]
  · have hnil : L[i]? = none := List.getElem?_eq_none_iff.mpr (by omega)
    simp [hi, hnil]

private theorem foldl_erase_sublist (dels P : List Int) : (dels.foldl List.erase P).Sublist P := by
  induction dels generalizing P with
  | nil => exact List.Sublist.refl P
  | cons d ds ih =>
      simp only [List.foldl_cons]
      exact (ih (P.erase d)).trans List.erase_sublist

private theorem removeBatch_perm (dels : List Int) {l1 l2 : List Int} (p : l1.Perm l2) :
    (dels.foldl List.erase l1).Perm (dels.foldl List.erase l2) := by
  induction dels generalizing l1 l2 with
  | nil => exact p
  | cons d ds ih =>
      simp only [List.foldl_cons]
      exact ih (List.Perm.erase d p)

theorem TopKRepair_sound : TinylambOpt.ResearchGoals.DeltaMaintenance.TopKRepair.Goal := by
  intro xs dels adds k
  change ((dels.foldl List.erase (xs.mergeSort.take (k + dels.length)) ++ adds).mergeSort.take k) =
    ((dels.foldl List.erase xs ++ adds).mergeSort.take k)
  let n := k + dels.length
  let S := xs.mergeSort
  let P := S.take n
  let Q := S.drop n
  have hSl : S.length = xs.length := by
    dsimp [S]
    rw [List.length_mergeSort]
  have hpermS : S.Perm xs := by
    dsimp [S]
    exact List.mergeSort_perm xs (fun a b => a ≤ b)
  by_cases hsmall : xs.length ≤ n
  · have hPeq : P = S := by
      dsimp [P]
      apply take_eq_self_of_length_le
      rw [hSl]
      exact hsmall
    have hpermR : (dels.foldl List.erase S).Perm (dels.foldl List.erase xs) := removeBatch_perm dels hpermS
    have hpermAll : (dels.foldl List.erase S ++ adds).Perm (dels.foldl List.erase xs ++ adds) :=
      List.Perm.append_right adds hpermR
    have hsort := mergeSort_congr_perm hpermAll
    change (dels.foldl List.erase P ++ adds).mergeSort.take k =
      (dels.foldl List.erase xs ++ adds).mergeSort.take k
    rw [hPeq, hsort]
  · have hbig : n < xs.length := by omega
    have hPlen : P.length = n := by
      dsimp [P]
      rw [List.length_take, Nat.min_eq_left]
      rw [hSl]
      omega
    have hPQ : P ++ Q = S := by
      dsimp [P, Q]
      exact List.take_append_drop n S
    have hpermXs : xs.Perm (P ++ Q) := by
      rw [hPQ]
      exact hpermS.symm
    have hpermR : (dels.foldl List.erase xs).Perm (dels.foldl List.erase (P ++ Q)) :=
      removeBatch_perm dels hpermXs
    have hpermAll : (dels.foldl List.erase xs ++ adds).Perm (dels.foldl List.erase (P ++ Q) ++ adds) :=
      List.Perm.append_right adds hpermR
    have hsort := mergeSort_congr_perm hpermAll
    change (dels.foldl List.erase P ++ adds).mergeSort.take k =
      (dels.foldl List.erase xs ++ adds).mergeSort.take k
    rw [hsort]
    let M1 := (dels.foldl List.erase P ++ adds).mergeSort
    let M2 := (dels.foldl List.erase (P ++ Q) ++ adds).mergeSort
    change M1.take k = M2.take k
    apply sorted_take_eq_of_countLE_min M1 M2 k
    · dsimp [M1]
      exact sorted_mergeSort _
    · dsimp [M2]
      exact sorted_mergeSort _
    · intro t
      dsimp [M1, M2]
      have hp1 := List.mergeSort_perm (dels.foldl List.erase P ++ adds) (fun a b => a ≤ b)
      have hp2 := List.mergeSort_perm (dels.foldl List.erase (P ++ Q) ++ adds) (fun a b => a ≤ b)
      have hcP1 := List.Perm.countP_eq (fun z : Int => z ≤ t) hp1
      have hcP2 := List.Perm.countP_eq (fun z : Int => z ≤ t) hp2
      rw [List.countP_eq_length_filter, List.countP_eq_length_filter] at hcP1 hcP2
      rw [hcP1, hcP2]
      let c1 := ((dels.foldl List.erase P).filter fun z => z ≤ t).length
      let c2 := ((dels.foldl List.erase (P ++ Q)).filter fun z => z ≤ t).length
      let cQ := ((Q.filter fun z => z ≤ t).length)
      let cA := ((adds.filter fun z => z ≤ t).length)
      have hc12 : c1 ≤ c2 := by
        dsimp [c1, c2]
        apply filter_length_le_of_count_le
        intro v
        rw [count_foldl_erase, count_foldl_erase, List.count_append]
        omega
      have hc2Q : c2 ≤ c1 + cQ := by
        dsimp [c1, c2, cQ]
        have hh := filter_length_le_of_count_le (dels.foldl List.erase (P ++ Q))
          (dels.foldl List.erase P ++ Q) (fun z => z ≤ t) (by
            intro v
            rw [count_foldl_erase (xs := P ++ Q) (dels := dels)]
            simp only [List.count_append]
            rw [count_foldl_erase (xs := P) (dels := dels)]
            omega)
        simpa [List.filter_append] using hh
      have hQdom : 0 < cQ → k ≤ c1 := by
        intro hcQpos
        dsimp [cQ] at hcQpos
        have hqmem : ∃ q, q ∈ Q ∧ q ≤ t := by
          have hne : Q.filter (fun z => z ≤ t) ≠ [] := by
            intro hnil
            simp [hnil] at hcQpos
          rcases List.exists_mem_of_ne_nil _ hne with ⟨q, hq⟩
          have hq' := List.mem_filter.mp hq
          exact ⟨q, hq'.1, of_decide_eq_true hq'.2⟩
        rcases hqmem with ⟨q, hqQ, hqt⟩
        have hpairPQ : (P ++ Q).Pairwise (fun a b => a ≤ b) := by
          rw [hPQ]
          dsimp [S]
          exact sorted_mergeSort xs
        have hcross := (List.pairwise_append.mp hpairPQ).2.2
        have hallP : ∀ z ∈ dels.foldl List.erase P, z ≤ t := by
          intro z hz
          have hzP : z ∈ P := List.Sublist.mem hz (foldl_erase_sublist dels P)
          have hzq : z ≤ q := hcross z hzP q hqQ
          exact Int.le_trans hzq hqt
        have hself : (dels.foldl List.erase P).filter (fun z => z ≤ t) = dels.foldl List.erase P := by
          apply List.filter_eq_self.mpr
          intro z hz
          exact decide_eq_true (hallP z hz)
        dsimp [c1]
        rw [hself]
        have hlen := length_removeBatch_ge P dels
        rw [hPlen] at hlen
        omega
      dsimp [c1, c2, cQ, cA] at hc12 hc2Q hQdom
      rw [List.filter_append, List.filter_append, List.length_append, List.length_append]
      change min k (c1 + cA) = min k (c2 + cA)
      by_cases hcz : cQ = 0
      · have heq : c1 + cA = c2 + cA := by omega
        rw [heq]
      · have hk1 : k ≤ c1 := hQdom (by omega)
        have hm1 : min k (c1 + cA) = k := by
          apply Nat.min_eq_left
          omega
        have hm2 : min k (c2 + cA) = k := by
          apply Nat.min_eq_left
          omega
        rw [hm1, hm2]

end TinylambOpt.ResearchProofs.Wave7DeltaMaintenance
