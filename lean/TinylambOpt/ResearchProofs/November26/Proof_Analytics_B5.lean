import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_B5
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.BenchmarkRematch

abbrev Item := Int × Nat

private theorem dropWhile_eq_self {α : Type} (p : α → Bool) :
    ∀ xs : List α, (∀ x ∈ xs, p x = false) → xs.dropWhile p = xs
  | [], h => rfl
  | x :: xs, h => by
      simp [h x (by simp), dropWhile_eq_self p xs (fun y hy => h y (by simp [hy]))]

private theorem map_snd_filter_count (k : Nat) : ∀ (items : List Item),
    (∀ q ∈ items, 100 < q.1) →
    ((items.map Prod.snd).filter (fun v => v == k)).length =
      (items.filter (fun r : Item => decide (100 < r.1) && r.2 == k)).length
  | [], h => rfl
  | (x, v) :: xs, h => by
      have hxgt : 100 < x := h (x, v) (by simp)
      have ht : ∀ q ∈ xs, 100 < q.1 := fun q hq => h q (by simp [hq])
      rw [List.map_cons, List.filter_cons, List.filter_cons]
      by_cases hv : v == k <;> simp [hxgt, hv, map_snd_filter_count k xs ht]

private theorem main : ∀ (items : List Item) (k : Nat),
    (items.map Prod.fst).Pairwise (· ≤ ·) →
    ((items.dropWhile (fun r => r.1 ≤ 100)).map Prod.snd).count k =
      countWhere items (fun r => 100 < r.1 && r.2 == k)
  | [], k, h => by simp [countWhere]
  | (x, v) :: xs, k, h => by
      rw [List.map_cons, List.pairwise_cons] at h
      obtain ⟨hhead, htail⟩ := h
      by_cases hx : x ≤ 100
      · have hpred : (decide (100 < x) && v == k) = false := by
          simp [show ¬ 100 < x by omega]
        simp [hx, countWhere, hpred, main xs k htail]
      · have hxgt : 100 < x := by omega
        have hgtAll : ∀ q ∈ (x, v) :: xs, 100 < q.1 := by
          intro q hq
          rcases List.mem_cons.mp hq with rfl | hq'
          · exact hxgt
          · have hqmem : q.1 ∈ xs.map Prod.fst := List.mem_map.mpr ⟨q, hq', rfl⟩
            have hle := hhead q.1 hqmem
            change x ≤ q.1 at hle
            omega
        have hdropAll : ((x, v) :: xs).dropWhile (fun r : Item => r.1 ≤ 100) =
            (x, v) :: xs :=
          dropWhile_eq_self (fun r : Item => r.1 ≤ 100) ((x, v) :: xs)
            (by
              intro q hq
              have hgtq := hgtAll q hq
              simp [show ¬ q.1 ≤ 100 by omega])
        rw [hdropAll, List.count_eq_length_filter]
        change ((List.filter (fun v => v == k) (((x, v) :: xs).map Prod.snd)).length =
          countWhere ((x, v) :: xs) (fun r : Item => decide (100 < r.1) && r.2 == k))
        simpa [countWhere] using map_snd_filter_count k ((x, v) :: xs) hgtAll

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.B5.Goal := by
  intro items k hsorted
  exact main items k hsorted


#print axioms TinylambOpt.ResearchProofs.November26.Analytics.Goal_B5.sound
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_B5
