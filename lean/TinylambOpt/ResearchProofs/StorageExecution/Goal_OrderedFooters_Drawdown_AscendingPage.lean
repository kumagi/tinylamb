import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_AscendingPage

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem le_min?_getD (a : Nat) : ∀ l : List Nat, (∀ y ∈ l, a ≤ y) → a ≤ l.min?.getD a
  | [], _ => by simp
  | b :: t, h => by
      rw [List.min?_cons]
      have hab : a ≤ b := h b (by simp)
      have ht : ∀ y ∈ t, a ≤ y := by
        intro y hy
        exact h y (by simp [hy])
      cases hm : t.min? with
      | none =>
          have ht_nil : t = [] := List.min?_eq_none_iff.mp hm
          subst t
          simp [hab]
      | some m =>
          have ham : a ≤ m := by
            have hrec := le_min?_getD a t ht
            simpa [hm] using hrec
          simpa [hm] using (Nat.le_min.2 ⟨hab, ham⟩)

private theorem min?_cons_getD (a : Nat) (t : List Nat) :
    (a :: t).min?.getD a = min a (t.min?.getD a) := by
  rw [List.min?_cons]
  cases t.min? <;> simp

private theorem drop_min_eq_head : ∀ (xs : List Nat), xs.Pairwise (· ≤ ·) →
    ∀ i (hi : i < xs.length), (xs.drop i).min?.getD xs[i] = xs[i]
  | [], _, i, hi => by simp at hi
  | a :: t, h, 0, hi => by
      have hle : ∀ y ∈ t, a ≤ y := (List.pairwise_cons.mp h).1
      have hge : a ≤ t.min?.getD a := le_min?_getD a t hle
      change (a :: t).min?.getD a = a
      rw [min?_cons_getD a t]
      exact Nat.min_eq_left hge
  | a :: t, h, i + 1, hi => by
      have ht : t.Pairwise (· ≤ ·) := (List.pairwise_cons.mp h).2
      have hit : i < t.length := by simpa using hi
      simpa [List.drop_succ_cons] using drop_min_eq_head t ht i hit

private theorem peak_zero_of_all_zero : ∀ ys : List Nat, (∀ y ∈ ys, y = 0) → peak ys = 0
  | [], _ => by simp [peak]
  | y :: ys, h => by
      have hy : y = 0 := h y (by simp)
      have hys : ∀ z ∈ ys, z = 0 := by
        intro z hz
        exact h z (by simp [hz])
      unfold peak
      rw [List.max?_cons]
      cases hm : ys.max? with
      | none =>
          have hynil : ys = [] := List.max?_eq_none_iff.mp hm
          subst ys
          simp [hy]
      | some m =>
          have hm_mem : m ∈ ys := List.max?_mem hm
          have hm0 : m = 0 := hys m hm_mem
          simp [hy, hm0]

private theorem drawdown_zero_of_pairwise (xs : List Nat) (hxs : xs.Pairwise (· ≤ ·)) :
    peak (xs.zipIdx.map (fun (x, i) => x - ((xs.drop i).min?.getD x))) = 0 := by
  apply peak_zero_of_all_zero
  intro y hy
  rcases List.mem_map.mp hy with ⟨⟨x, i⟩, hmem, rfl⟩
  have hzip := List.mem_zipIdx (k := 0) hmem
  rcases hzip with ⟨_, hi, hx⟩
  have hxi : x = xs[i] := by simpa using hx
  rw [hxi]
  have hi' : i < xs.length := by simpa using hi
  change xs[i] - (xs.drop i).min?.getD xs[i] = 0
  rw [drop_min_eq_head xs hxs i hi']
  simp

theorem sound_AscendingPage : Drawdown.AscendingPage.Goal := by
  intro xs hxs
  cases xs with
  | nil => simp [agg]
  | cons a t =>
      have hdd := drawdown_zero_of_pairwise (a :: t) hxs
      change ((peak ((a :: t).zipIdx.map (fun (x, i) => x - ((List.drop i (a :: t)).min?.getD x))) == 0) = true)
      rw [hdd]
      rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_AscendingPage
