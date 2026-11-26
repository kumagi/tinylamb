import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_RetiredReportReclamation

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
Discarding reports below the retired frontier does not change future output: lookups for ids at or above the frontier are unaffected by filtering lower ids.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem readyIds_cons (rs : List Report) (start count : Nat) (h : (lookup rs start).isSome = true) :
    readyIds rs start (count + 1) = start :: readyIds rs (start + 1) count := by
  unfold readyIds
  rw [← List.range'_eq_map_range (s := start) (n := count + 1)]
  rw [← List.range'_eq_map_range (s := start + 1) (n := count)]
  rw [List.range'_succ]
  simp [h]

private theorem readyIds_nil_of_none (rs : List Report) (start count : Nat)
    (h : (lookup rs start).isSome = false) : readyIds rs start count = [] := by
  unfold readyIds
  cases count with
  | zero => simp
  | succ n =>
      rw [List.range_succ_eq_map]
      simp [h]

private theorem readyIds_mem_ge (rs : List Report) (start count i : Nat)
    (hi : i ∈ readyIds rs start count) : start ≤ i := by
  unfold readyIds at hi
  have hsub : ((List.range count).map (start + ·)).takeWhile (fun i => (lookup rs i).isSome) ⊆
      ((List.range count).map (start + ·)) := List.takeWhile_subset _
  have hm := hsub hi
  simp only [List.mem_map] at hm
  obtain ⟨j, hj, rfl⟩ := hm
  simp at hj
  omega

private theorem lookup_isSome_of_readyIds_mem (rs : List Report) (start count i : Nat)
    (hi : i ∈ readyIds rs start count) : (lookup rs i).isSome = true := by
  unfold readyIds at hi
  let P := fun i => (lookup rs i).isSome
  have hall : (((List.range count).map (start + ·)).takeWhile P).all P = true := List.all_takeWhile
  exact (List.all_eq_true.mp hall) i hi

private theorem flatMap_lookup_eq_on (L : List Nat) (f g : Nat → Option (List Nat))
    (h : ∀ i ∈ L, f i = g i) :
    L.flatMap (fun i => (f i).getD []) = L.flatMap (fun i => (g i).getD []) := by
  induction L with
  | nil => simp
  | cons i is ih =>
      simp [h i (by simp), ih (fun j hj => h j (by simp [hj]))]

private theorem lookup_filter_eq (rs : List Report) (start i : Nat) (hi : start ≤ i) :
    lookup (rs.filter (fun r => start ≤ r.1)) i = lookup rs i := by
  unfold lookup
  rw [List.find?_filter]
  have hfun : (fun a : Report => decide (decide (start ≤ a.1) = true ∧ (a.1 == i) = true)) =
      (fun a : Report => a.1 == i) := by
    funext a
    by_cases ha : start ≤ a.1
    · cases heq : (a.1 == i) <;> simp [heq, ha]
    · have hne : (a.1 == i) = false := by
        simp only [Bool.eq_false_iff]
        intro hqi
        apply ha
        have hai : a.1 = i := by simpa using hqi
        omega
      simp [ha, hne]
  rw [hfun]

private theorem readyIds_filter_eq_of_le (rs : List Report) (t start count : Nat) (ht : t ≤ start) :
    readyIds (rs.filter (fun r => t ≤ r.1)) start count = readyIds rs start count := by
  induction count generalizing start with
  | zero => simp [readyIds]
  | succ c ih =>
      by_cases h : (lookup (rs.filter (fun r => t ≤ r.1)) start).isSome = true
      · have hl := lookup_filter_eq rs t start ht
        rw [readyIds_cons _ start c h]
        have h' : (lookup rs start).isSome = true := by rw [← hl]; exact h
        rw [readyIds_cons rs start c h']
        rw [ih (start + 1) (by omega)]
      · have hl := lookup_filter_eq rs t start ht
        have hfalse : (lookup (rs.filter (fun r => t ≤ r.1)) start).isSome = false := by
          cases hb : (lookup (rs.filter (fun r => t ≤ r.1)) start).isSome <;> simp_all
        have hnilf := readyIds_nil_of_none _ start (c + 1) hfalse
        have h' : (lookup rs start).isSome = false := by rw [← hl]; exact hfalse
        have hnils := readyIds_nil_of_none rs start (c + 1) h'
        rw [hnilf, hnils]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.RetiredReportReclamation.Goal := by
  intro rs start count
  unfold emit
  rw [readyIds_filter_eq_of_le rs start start count (by omega)]
  apply flatMap_lookup_eq_on
  intro i hi
  apply lookup_filter_eq
  apply readyIds_mem_ge
  exact hi
end TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_RetiredReportReclamation
