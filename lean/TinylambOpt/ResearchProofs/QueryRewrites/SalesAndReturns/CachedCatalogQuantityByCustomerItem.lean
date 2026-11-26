import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# CachedCatalogQuantityByCustomerItem

Grouping repeated customer-item keys and weighting each nullable catalog subtotal by its
frequency preserves the total.

After ticket matching, the executor can reuse the catalog quantity aggregate for
repeated customer-item pairs.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CachedCatalogQuantityByCustomerItem
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_perm nsum_map_nsum nsum_flatMap_const)
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unnecessarySimpa false

theorem sum_if_eq_of_mem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α)
    (c : α → Nat) (hnd : l.Nodup) :
    (l.map (fun k => if k == a then c k else 0)).sum = if a ∈ l then c a else 0 := by
  induction l with
  | nil => rfl
  | cons k ks ih =>
      have hnd' : ks.Nodup := (List.nodup_cons.mp hnd).2
      have hknot : k ∉ ks := (List.nodup_cons.mp hnd).1
      by_cases hka : k == a
      · have hkeq : k = a := by simpa using hka
        have hnot : a ∉ ks := by
          intro h
          exact hknot (hkeq ▸ h)
        have hih := ih hnd'
        simp only [List.map_cons, List.sum_cons]
        rw [hih]
        simp [hkeq, hnot]
      · have hka' : (k == a) = false := by simpa using hka
        by_cases hmem : a ∈ ks
        · have hmcons : a ∈ k :: ks := by simp [hmem]
          simp [ih hnd', hmem, hmcons, hka']
        · have hmcons : a ∉ k :: ks := by
            intro hm
            simp only [List.mem_cons] at hm
            rcases hm with heq | h
            · have hne : a ≠ k := by intro h'; exact hka (by simpa [h'])
              exact hne heq
            · exact hmem h
          simp [ih hnd', hmem, hmcons, hka']

theorem sum_if_eq_zero_of_not_mem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α)
    (c : α → Nat) (hnot : a ∉ l) :
    (l.map (fun k => if k == a then c k else 0)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons k ks ih =>
      have hnot' : a ∉ ks := by intro h; exact hnot (by simp [h])
      have hkne : k ≠ a := by intro h; exact hnot (by simp [h])
      have hka : (k == a) = false := by simpa [hkne]
      simp [hka, ih hnot']

theorem count_group {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (a : α) :
    (xs.eraseDups.flatMap (fun k => List.replicate (xs.count k) k)).count a = xs.count a := by
  rw [List.count_flatMap]
  rw [show (xs.eraseDups.map (List.count a ∘ fun k => List.replicate (xs.count k) k)) =
      xs.eraseDups.map (fun k => if k == a then xs.count k else 0) by
    apply List.map_congr_left
    intro k hk
    change (List.replicate (xs.count k) k).count a = if k == a then xs.count k else 0
    rw [List.count_replicate]]
  by_cases ha : a ∈ xs
  · rw [sum_if_eq_of_mem xs.eraseDups a (fun k => xs.count k) (KeyGrouping.nodup_eraseDups xs)]
    simp [List.mem_eraseDups.mpr ha]
  · rw [sum_if_eq_zero_of_not_mem xs.eraseDups a (fun k => xs.count k) (by simpa using ha)]
    exact (List.count_eq_zero.mpr ha).symm

theorem group_perm {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.Perm (xs.eraseDups.flatMap (fun k => List.replicate (xs.count k) k)) := by
  rw [List.perm_iff_count]
  intro a
  exact (count_group xs a).symm

theorem nsum_group_general {α : Type} [BEq α] [LawfulBEq α] (xs : List α)
    (cat : α → List (Option Int)) :
    nsum (xs.flatMap cat) =
      nsum ((frequency xs).map (fun (k, n) => (nsum (cat k)).map (fun v => v * (n : Int)))) := by
  rw [nsum_perm ((group_perm xs).flatMap_right cat), List.flatMap_assoc]
  change nsum ((xs.eraseDups.map (fun k => (List.replicate (xs.count k) k).flatMap cat)).flatten) = _
  rw [← nsum_map_nsum]
  simp only [frequency, List.map_map]
  apply congrArg nsum
  apply List.map_congr_left
  intro k hk
  have hcount : xs.count k ≠ 0 :=
    Nat.ne_of_gt (List.count_pos_iff.mpr (List.mem_eraseDups.mp hk))
  simpa [List.flatMap_replicate, hcount, Int.mul_comm] using
    nsum_flatMap_const (List.replicate (xs.count k) k) (cat k)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.CachedCatalogQuantityByCustomerItem.Goal := by
  intro matchedTickets catalog
  exact nsum_group_general matchedTickets catalog
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CachedCatalogQuantityByCustomerItem
