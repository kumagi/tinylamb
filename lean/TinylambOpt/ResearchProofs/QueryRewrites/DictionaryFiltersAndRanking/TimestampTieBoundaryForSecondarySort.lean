import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# TimestampTieBoundaryForSecondarySort

The primary ordering determines a prefix through the last row tied at the cutoff, and
the secondary order resolves positions within that prefix.

A time-ordered scan can stop after the cutoff timestamp's entire tie group and sort the
necessary secondary keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampTieBoundaryForSecondarySort
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem takeWhile_all_sat {α : Type} (p : α → Bool) : ∀ l : List α,
    ∀ x ∈ l.takeWhile p, p x = true
  | [], x, hx => by simp at hx
  | a :: l, x, hx => by
      by_cases h : p a = true
      · simp [List.takeWhile_cons, h] at hx
        rcases hx with rfl | hx
        · exact h
        · exact takeWhile_all_sat p l x hx
      · simp [List.takeWhile_cons, h] at hx

theorem dropWhile_all_gt : ∀ (rs : List (Nat × Nat)) t,
    (rs.map Prod.fst).Pairwise (fun a b => a ≤ b) →
    ∀ r ∈ rs.dropWhile (fun r : Nat × Nat => decide (r.1 ≤ t)), t < r.1
  | [], t, hs, r, hr => by simp at hr
  | a :: rs, t, hs, r, hr => by
      have hs' := List.pairwise_cons.mp hs
      by_cases ha : a.1 ≤ t
      · simp [List.dropWhile_cons, ha] at hr
        exact dropWhile_all_gt rs t hs'.2 r hr
      · simp [List.dropWhile_cons, ha] at hr
        rcases hr with rfl | hr
        · omega
        · have hmem : r.1 ∈ rs.map Prod.fst := List.mem_map.mpr ⟨r, hr, rfl⟩
          have hge : a.1 ≤ r.1 := hs'.1 r.1 hmem
          omega

theorem filter_eq_takeWhile_of_sorted : ∀ (rs : List (Nat × Nat)) t,
    (rs.map Prod.fst).Pairwise (fun a b => a ≤ b) →
    rs.filter (fun r => decide (r.1 ≤ t)) = rs.takeWhile (fun r => decide (r.1 ≤ t))
  | [], t, hs => by simp
  | a :: rs, t, hs => by
      have hs' := List.pairwise_cons.mp hs
      by_cases ha : a.1 ≤ t
      · simp [List.filter_cons, List.takeWhile_cons, ha,
          filter_eq_takeWhile_of_sorted rs t hs'.2]
      · have htail : rs.filter (fun r => decide (r.1 ≤ t)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro r hr
          have hmem : r.1 ∈ rs.map Prod.fst := List.mem_map.mpr ⟨r, hr, rfl⟩
          have hge : a.1 ≤ r.1 := hs'.1 r.1 hmem
          simp
          omega
        simp [List.filter_cons, List.takeWhile_cons, ha, htail]

theorem mergeSort_of_perm {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b, (le a b || le b a) = true)
    (antisym : ∀ a b, le a b = true → le b a = true → a = b) :
    ∀ {l₁ l₂ : List α}, l₁.Perm l₂ → l₁.mergeSort le = l₂.mergeSort le := by
  intro l₁ l₂ h
  apply List.Perm.eq_of_pairwise (fun a b _ _ h₁ h₂ => antisym a b h₁ h₂)
  · exact List.pairwise_mergeSort trans total l₁
  · exact List.pairwise_mergeSort trans total l₂
  · exact (List.mergeSort_perm l₁ le).trans (h.trans (List.mergeSort_perm l₂ le).symm)

theorem mergeSort_append_of_dominates {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b, (le a b || le b a) = true)
    (antisym : ∀ a b, le a b = true → le b a = true → a = b)
    (A B : List α) (hdom : ∀ a ∈ A, ∀ b ∈ B, le a b = true) :
    (A ++ B).mergeSort le = A.mergeSort le ++ B.mergeSort le := by
  apply List.Perm.eq_of_pairwise (fun a b _ _ h₁ h₂ => antisym a b h₁ h₂)
  · exact List.pairwise_mergeSort trans total (A ++ B)
  · rw [List.pairwise_append]
    refine ⟨List.pairwise_mergeSort trans total A, List.pairwise_mergeSort trans total B, ?_⟩
    intro a ha b hb
    exact hdom a ((List.mergeSort_perm A le).mem_iff.mp ha)
      b ((List.mergeSort_perm B le).mem_iff.mp hb)
  · exact (List.mergeSort_perm (A ++ B) le).trans
      (((List.mergeSort_perm A le).append (List.mergeSort_perm B le)).symm)

theorem take_topn_of_dominates {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b, (le a b || le b a) = true)
    (antisym : ∀ a b, le a b = true → le b a = true → a = b)
    (A B : List α) (hdom : ∀ a ∈ A, ∀ b ∈ B, le a b = true) n (hn : n ≤ A.length) :
    ((A ++ B).mergeSort le).take n = (A.mergeSort le).take n := by
  rw [mergeSort_append_of_dominates le trans total antisym A B hdom]
  exact List.take_append_of_le_length (by simpa using hn)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.TimestampTieBoundaryForSecondarySort.Goal := by
  intro rs t hsorted hten cmp
  let P := rs.takeWhile (fun r : Nat × Nat => decide (r.1 ≤ t))
  let S := rs.dropWhile (fun r : Nat × Nat => decide (r.1 ≤ t))
  have htrans : ∀ a b c : Nat × Nat, cmp a b = true → cmp b c = true → cmp a c = true := by
    intro a b c hab hbc
    simp [cmp] at hab hbc ⊢
    rcases hab with hab | ⟨hab, hab'⟩
    · rcases hbc with hbc | ⟨hbc, hbc'⟩
      · left; omega
      · left; omega
    · rcases hbc with hbc | ⟨hbc, hbc'⟩
      · left; omega
      · right; constructor <;> omega
  have htotal : ∀ a b : Nat × Nat, (cmp a b || cmp b a) = true := by
    intro a b
    by_cases h : cmp a b = true
    · simp [h]
    · have h' : cmp b a = true := by
        simp [cmp] at h ⊢
        by_cases hlt : b.1 < a.1
        · exact Or.inl hlt
        · right
          constructor
          · have hge : a.1 ≤ b.1 := by omega
            omega
          · by_cases hle : b.1 ≤ a.1
            · have heq : a.1 = b.1 := by omega
              omega
            · omega
      have hf : cmp a b = false := by simpa using h
      simp [hf, h']
  have hantisym : ∀ a b : Nat × Nat, cmp a b = true → cmp b a = true → a = b := by
    intro a b hab hba
    simp [cmp] at hab hba
    rcases hab with hab | ⟨hab, hleab⟩
    · rcases hba with hba | ⟨hbaba, hleba⟩ <;> omega
    · rcases hba with hba | ⟨hbaba, hleba⟩
      · omega
      · cases a
        cases b
        simp_all
        omega
  have hP_filter : rs.filter (fun r => decide (r.1 ≤ t)) = P := by
    simpa [P] using filter_eq_takeWhile_of_sorted rs t hsorted
  have hPlen : 10 ≤ P.length := by
    rw [← hP_filter]
    simpa [countWhere] using hten
  have hdom : ∀ a ∈ P, ∀ b ∈ S, cmp a b = true := by
    intro a ha b hb
    have hale : a.1 ≤ t := by
      simpa using takeWhile_all_sat (fun r : Nat × Nat => decide (r.1 ≤ t)) rs a (by simpa [P] using ha)
    have hbgt : t < b.1 := dropWhile_all_gt rs t hsorted b (by simpa [S] using hb)
    simp [cmp]
    left
    omega
  have hrs_perm : rs.Perm (P ++ S) := by
    rw [← List.takeWhile_append_dropWhile (p := fun r : Nat × Nat => decide (r.1 ≤ t)) (l := rs)]
  have hrs_sort : rs.mergeSort cmp = (P ++ S).mergeSort cmp :=
    mergeSort_of_perm cmp htrans htotal hantisym hrs_perm
  change ((rs.takeWhile (fun r : Nat × Nat => decide (r.1 ≤ t))).mergeSort cmp).take 10 =
    (rs.mergeSort cmp).take 10
  rw [hrs_sort]
  change (P.mergeSort cmp).take 10 = ((P ++ S).mergeSort cmp).take 10
  rw [take_topn_of_dominates cmp htrans htotal hantisym P S hdom 10 hPlen]
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.TimestampTieBoundaryForSecondarySort
