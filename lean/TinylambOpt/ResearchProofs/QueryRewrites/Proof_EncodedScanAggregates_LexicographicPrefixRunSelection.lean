import TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampTieBoundaryForSecondarySort
import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_LexicographicPrefixRunSelection

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
A lexicographic top-ten cut splits cleanly at a certified timestamp boundary:
all earlier timestamps precede the tie group, which in turn precedes all later
timestamps. Sorting the three blocks separately and taking the prefix therefore
only needs the remaining quota of secondary keys from the tie group.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking

namespace EncodedScanAggregateLexicographicRuns

private theorem count_filter_of_pred_true {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (p : α → Bool) (x : α) (h : p x = true) :
    (xs.filter p).count x = xs.count x := by
  induction xs with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : y == x
      · have heq : y = x := beq_iff_eq.mp hyx
        have hpy : p y = true := by simp [heq, h]
        simp [List.count_cons, hpy, hyx, ih]
      · by_cases hpy : p y
        · simp [List.count_cons, hpy, hyx, ih]
        · simp [List.count_cons, hpy, hyx, ih]

private theorem count_filter_of_pred_false {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (p : α → Bool) (x : α) (h : p x = false) :
    (xs.filter p).count x = 0 := by
  rw [List.count_eq_zero.mpr]
  intro hx
  have hx' := (List.mem_filter.mp hx).2
  simpa [h] using hx'

private theorem partition_perm (rs : List (Nat × Nat)) time :
    rs.Perm ((rs.filter (fun r => r.1 < time)) ++
      (rs.filter (fun r => r.1 == time)) ++
      (rs.filter (fun r => time < r.1))) := by
  apply List.perm_iff_count.mpr
  intro x
  by_cases hlt : x.1 < time
  · have hltt : (decide (x.1 < time)) = true := by simpa using hlt
    have heq : (x.1 == time) = false := by
      by_cases h : x.1 == time
      · have h' : x.1 = time := beq_iff_eq.mp h
        omega
      · simpa using h
    have hgt : (decide (time < x.1)) = false := by simpa using (show ¬ time < x.1 by omega)
    simp [count_filter_of_pred_true, count_filter_of_pred_false, hltt, heq, hgt]
  · by_cases heqb : x.1 == time
    · have heq : x.1 = time := beq_iff_eq.mp heqb
      have hltt : (decide (x.1 < time)) = false := by simpa using hlt
      have hgt : (decide (time < x.1)) = false := by simpa using (show ¬ time < x.1 by omega)
      simp [count_filter_of_pred_true, count_filter_of_pred_false, hltt, heqb, hgt]
    · have hne : x.1 ≠ time := by
        intro h
        exact heqb (by simp [h])
      have hgtp : time < x.1 := by omega
      have hltt : (decide (x.1 < time)) = false := by simpa using hlt
      have hgt : (decide (time < x.1)) = true := by simpa using hgtp
      simp [count_filter_of_pred_true, count_filter_of_pred_false, hltt, heqb, hgt]

private theorem map_eq_self_of_fst_eq : ∀ (l : List (Nat × Nat)) time,
    (∀ r ∈ l, r.1 == time) → l.map (fun r => (time, r.2)) = l
  | [], time, h => rfl
  | a :: l, time, h => by
      have ha : a.1 == time := h a (by simp)
      have hrest : ∀ r ∈ l, r.1 == time := fun r hr => h r (by simp [hr])
      have heq : a = (time, a.2) := by
        have h' : a.1 = time := beq_iff_eq.mp ha
        cases a
        simp_all
      have hrec := map_eq_self_of_fst_eq l time hrest
      rw [List.map_cons, hrec, heq]

private theorem tiedRows_perm (rs : List (Nat × Nat)) time :
    (rs.filter (fun r => r.1 == time)).Perm
      ((forKey rs time).map (fun k => (time, k))) := by
  unfold forKey
  simp only [List.map_map, Function.comp_apply]
  change (rs.filter (fun r => r.1 == time)).Perm
    ((rs.filter (fun r => r.1 == time)).map (fun r => (time, r.2)))
  rw [map_eq_self_of_fst_eq (rs.filter (fun r => r.1 == time)) time (by
    intro r hr
    exact (List.mem_filter.mp hr).2)]

private theorem cmpTrans : ∀ a b c : Nat × Nat,
    decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)) = true →
    decide (b.1 < c.1 ∨ (b.1 = c.1 ∧ b.2 ≤ c.2)) = true →
    decide (a.1 < c.1 ∨ (a.1 = c.1 ∧ a.2 ≤ c.2)) = true := by
  intro a b c hab hbc
  simp only [decide_eq_true_eq] at hab hbc ⊢
  rcases hab with hab | ⟨hab, hab'⟩
  · rcases hbc with hbc | ⟨hbc, hbc'⟩
    · left; omega
    · left; omega
  · rcases hbc with hbc | ⟨hbc, hbc'⟩
    · left; omega
    · right; constructor <;> omega

private theorem cmpTotal : ∀ a b : Nat × Nat,
    (decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)) ||
      decide (b.1 < a.1 ∨ (b.1 = a.1 ∧ b.2 ≤ a.2))) = true := by
  intro a b
  by_cases h : a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)
  · simp [h]
  · have h' : b.1 < a.1 ∨ (b.1 = a.1 ∧ b.2 ≤ a.2) := by
      by_cases hba : b.1 < a.1
      · exact Or.inl hba
      · right
        constructor
        · omega
        · by_cases hle : b.1 ≤ a.1
          · have : a.1 = b.1 := by omega
            omega
          · omega
    simp [h, h']

private theorem cmpAntisym : ∀ a b : Nat × Nat,
    decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)) = true →
    decide (b.1 < a.1 ∨ (b.1 = a.1 ∧ b.2 ≤ a.2)) = true → a = b := by
  intro a b hab hba
  simp only [decide_eq_true_eq] at hab hba
  rcases hab with hab | ⟨hab, hab'⟩
  · rcases hba with hba | ⟨hbaba, hleba⟩ <;> omega
  · rcases hba with hba | ⟨hbaba, hleba⟩
    · omega
    · cases a
      cases b
      simp_all
      omega

end EncodedScanAggregateLexicographicRuns

open EncodedScanAggregateLexicographicRuns
open TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampTieBoundaryForSecondarySort

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.LexicographicPrefixRunSelection.Goal := by
  intro rs time
  dsimp only
  intro hearly htotal
  let cmp : Nat × Nat → Nat × Nat → Bool := fun a b =>
    decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2))
  let earlier := rs.filter (fun r => r.1 < time)
  let tiedRows := rs.filter (fun r => r.1 == time)
  let later := rs.filter (fun r => time < r.1)
  let tied := forKey rs time
  let pack : Nat → Nat × Nat := fun k => (time, k)
  have hcmp : ∀ a b, cmp a b = decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)) := by
    intro a b
    rfl
  have htrans : ∀ a b c, cmp a b = true → cmp b c = true → cmp a c = true := by
    intro a b c hab hbc
    simpa [hcmp] using cmpTrans a b c hab hbc
  have htotalcmp : ∀ a b, (cmp a b || cmp b a) = true := by
    intro a b
    simpa [hcmp] using cmpTotal a b
  have hantisym : ∀ a b, cmp a b = true → cmp b a = true → a = b := by
    intro a b hab hba
    exact cmpAntisym a b (by simpa [hcmp] using hab) (by simpa [hcmp] using hba)
  have hdom_early_tied : ∀ a ∈ earlier, ∀ b ∈ tiedRows, cmp a b = true := by
    intro a ha b hb
    have halt : a.1 < time := by simpa using (List.mem_filter.mp ha).2
    have hbeq : b.1 = time := beq_iff_eq.mp (List.mem_filter.mp hb).2
    simp [cmp, hbeq, halt]
  have hdom_early_later : ∀ a ∈ earlier, ∀ b ∈ later, cmp a b = true := by
    intro a ha b hb
    have halt : a.1 < time := by simpa using (List.mem_filter.mp ha).2
    have hbgt : time < b.1 := by simpa using (List.mem_filter.mp hb).2
    simp [cmp]
    left
    omega
  have hdom_tied_later : ∀ a ∈ tiedRows, ∀ b ∈ later, cmp a b = true := by
    intro a ha b hb
    have haeq : a.1 = time := beq_iff_eq.mp (List.mem_filter.mp ha).2
    have hbgt : time < b.1 := by simpa using (List.mem_filter.mp hb).2
    simp [cmp, haeq, hbgt]
  have hpartition := partition_perm rs time
  have hrsSort0 : rs.mergeSort cmp = (earlier ++ tiedRows ++ later).mergeSort cmp := by
    exact mergeSort_of_perm cmp htrans htotalcmp hantisym (by simpa [earlier, tiedRows, later] using hpartition)
  have hsortETL : (earlier ++ tiedRows ++ later).mergeSort cmp =
      (earlier ++ tiedRows).mergeSort cmp ++ later.mergeSort cmp := by
    apply mergeSort_append_of_dominates cmp htrans htotalcmp hantisym (earlier ++ tiedRows) later
    intro a ha b hb
    rcases List.mem_append.mp ha with ha | ha
    · exact hdom_early_later a ha b hb
    · exact hdom_tied_later a ha b hb
  have hsortET : (earlier ++ tiedRows).mergeSort cmp =
      earlier.mergeSort cmp ++ tiedRows.mergeSort cmp := by
    exact mergeSort_append_of_dominates cmp htrans htotalcmp hantisym earlier tiedRows hdom_early_tied
  have htiedPerm := tiedRows_perm rs time
  have htiedSort0 : tiedRows.mergeSort cmp = (tied.map pack).mergeSort cmp := by
    exact mergeSort_of_perm cmp htrans htotalcmp hantisym (by simpa [tiedRows, tied, pack] using htiedPerm)
  have hmapSort : (tied.mergeSort).map pack = (tied.map pack).mergeSort cmp := by
    apply List.map_mergeSort
    intro a ha b hb
    simp [pack, cmp]
  have htiedSort : tiedRows.mergeSort cmp = (tied.mergeSort).map pack := by
    rw [htiedSort0, ← hmapSort]
  have hrsSort : rs.mergeSort cmp =
      earlier.mergeSort cmp ++ (tied.mergeSort).map pack ++ later.mergeSort cmp := by
    rw [hrsSort0, hsortETL, hsortET, htiedSort]
  have hlenE : (earlier.mergeSort cmp).length = earlier.length :=
    (List.mergeSort_perm earlier cmp).length_eq
  have hlenT : (tied.mergeSort).length = tied.length :=
    (List.mergeSort_perm tied (fun a b : Nat => decide (a ≤ b))).length_eq
  have hlenTpack : ((tied.mergeSort).map pack).length = tied.length := by
    simpa using hlenT
  have htakeE : (earlier.mergeSort cmp).take 10 = earlier.mergeSort cmp := by
    apply List.take_of_length_le
    simpa [hlenE] using hearly
  have htotal' : 10 ≤ earlier.length + tied.length := by simpa [earlier, tied] using htotal
  have hnT : 10 - earlier.length ≤ tied.length := by omega
  change earlier.mergeSort cmp ++ (tied.mergeSort.take (10 - earlier.length)).map pack =
    (rs.mergeSort cmp).take 10
  rw [hrsSort]
  rw [List.append_assoc]
  have hnT' : 10 - earlier.length ≤ ((tied.mergeSort).map pack).length := by
    rw [hlenTpack]
    exact hnT
  have hnTpack : 10 - (earlier.mergeSort cmp).length ≤ ((tied.mergeSort).map pack).length := by
    rw [hlenE]
    exact hnT'
  rw [List.take_append]
  rw [htakeE]
  rw [List.take_append_of_le_length hnTpack]
  rw [List.map_take]
  rw [← hlenE]
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_LexicographicPrefixRunSelection
