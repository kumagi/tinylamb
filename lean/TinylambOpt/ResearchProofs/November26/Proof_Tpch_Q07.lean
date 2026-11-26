import TinylambOpt.ResearchGoals.November26.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q07
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpch
open TinylambOpt.ResearchGoals.BenchmarkRematch


theorem count_map_fst (l : List (Nat × Bool)) (k : Nat) :
    (l.map Prod.fst).count k = (l.filter (fun r => r.1 == k)).length := by
  rw [List.count_eq_length_filter, List.filter_map]
  simp only [List.length_map]
  apply congrArg List.length
  apply List.filter_congr
  intro r hr
  rfl

theorem matching_count (orders : List (Nat × Bool)) (k : Nat) (supplier : Bool) :
    ((orders.filter (fun r => r.2 != supplier)).map Prod.fst).count k =
      (orders.filter (fun r => r.1 == k && r.2 != supplier)).length := by
  rw [count_map_fst, List.filter_filter]

theorem length_filter_forKey (orders : List (Nat × Bool)) (k : Nat) (s supplier : Bool)
    (hs : (s == supplier) = true) :
    ((forKey orders k).filter (fun c => s == supplier && s != c)).length =
      (orders.filter (fun r => r.1 == k && r.2 != supplier)).length := by
  have hss : s = supplier := by simpa using hs
  subst s
  cases supplier <;>
    simp [forKey, List.filter_map] <;>
    apply congrArg List.length <;>
    apply List.filter_congr <;>
    intro r hr <;>
    simp [Bool.and_comm]

theorem length_filter_forKey_zero (orders : List (Nat × Bool)) (k : Nat) (s supplier : Bool)
    (hs : (s == supplier) = false) :
    ((forKey orders k).filter (fun c => s == supplier && s != c)).length = 0 := by
  simp [hs]

theorem sum_filter_drop_zero {α : Type} (xs : List α) (p : α → Bool) (f : α → Nat)
    (h : ∀ x ∈ xs, p x = false → f x = 0) :
    (xs.map f).sum = ((xs.filter p).map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hi : ∀ y ∈ xs, p y = false → f y = 0 := by
        intro y hy hpy
        exact h y (by simp [hy]) hpy
      by_cases hp : p x = true
      · simpa [hp] using ih hi
      · have hpfalse : p x = false := by cases hpx : p x <;> simp [hpx] at hp ⊢
        simpa [hpfalse, h x (by simp) hpfalse] using ih hi


theorem sound : TinylambOpt.ResearchGoals.November26.Tpch.Q07.Goal := by
  intro orders lines supplier
  let matchingOrders := (orders.filter (fun r => r.2 != supplier)).map Prod.fst
  let matchingLines := (lines.filter (fun r => r.2 == supplier)).map Prod.fst
  let f : Nat × Bool → Nat := fun (k, s) =>
    ((forKey orders k).filter (fun c => s == supplier && s != c)).length
  have hflat : (lines.flatMap (fun (k, s) =>
      (forKey orders k).filter (fun c => s == supplier && s != c))).length =
      (lines.map f).sum := by
    induction lines with
    | nil => simp [f]
    | cons line lines ih =>
        rcases line with ⟨k, s⟩
        simp [f, ih]
  have hdrop : (lines.map f).sum = ((lines.filter (fun r => r.2 == supplier)).map f).sum := by
    apply TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q07.sum_filter_drop_zero
    intro r hr hfalse
    unfold f
    have hsf : (r.2 == supplier) = false := hfalse
    exact TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q07.length_filter_forKey_zero orders r.1 r.2 supplier hsf
  have hpoint : ((lines.filter (fun r => r.2 == supplier)).map f).sum =
      ((lines.filter (fun r => r.2 == supplier)).map (fun r => matchingOrders.count r.1)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro r hr
    rcases r with ⟨k, s⟩
    have hs : (s == supplier) = true := by simpa using (List.mem_filter.mp hr).2
    dsimp [f]
    rw [TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q07.length_filter_forKey orders k s supplier hs]
    exact (TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q07.matching_count orders k supplier).symm
  have hkeys : ((lines.filter (fun r => r.2 == supplier)).map (fun r => matchingOrders.count r.1)).sum =
      (matchingLines.map matchingOrders.count).sum := by
    dsimp [matchingLines]
    rw [List.map_map]
    rfl
  rw [hflat, hdrop, hpoint, hkeys]
end TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q07
