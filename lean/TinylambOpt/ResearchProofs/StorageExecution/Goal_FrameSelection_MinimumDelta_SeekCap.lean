import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_SeekCap

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.MinimumDelta

-- The claim is independent of the writer: relaxing the cap only enlarges the
-- set of admissible partitions, so the minimum charge cannot increase.

private theorem foldl_min_le_start (ys : List Nat) (y : Nat) : ys.foldl min y ≤ y := by
  induction ys generalizing y with
  | nil => simp
  | cons z zs ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans (ih (min y z)) (Nat.min_le_left y z)

private theorem foldl_min_le_mem (ys : List Nat) (y z : Nat) (hz : z ∈ ys) :
    ys.foldl min y ≤ z := by
  induction ys generalizing y with
  | nil => simp at hz
  | cons w ws ih =>
      simp only [List.foldl_cons]
      simp only [List.mem_cons] at hz
      rcases hz with rfl | hz
      · exact Nat.le_trans (foldl_min_le_start ws (min y z)) (Nat.min_le_right y z)
      · exact ih (min y w) hz

private theorem min?_le_of_mem {l : List Nat} {x : Nat} (hx : x ∈ l) :
    ∃ m, l.min? = some m ∧ m ≤ x := by
  cases l with
  | nil => simp at hx
  | cons y ys =>
      refine ⟨ys.foldl min y, rfl, ?_⟩
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hx
      · exact foldl_min_le_start ys x
      · exact foldl_min_le_mem ys y x hx

private theorem foldl_min_mem (ys : List Nat) (y : Nat) : ys.foldl min y ∈ y :: ys := by
  induction ys generalizing y with
  | nil => simp
  | cons z zs ih =>
      simp only [List.foldl_cons]
      have h := ih (min y z)
      by_cases hyz : y ≤ z
      · have hmin : min y z = y := Nat.min_eq_left hyz
        rw [hmin] at h
        rw [hmin]
        simp only [List.mem_cons] at h ⊢
        rcases h with heq | htail
        · exact Or.inl heq
        · exact Or.inr (Or.inr htail)
      · have hzy : z ≤ y := by omega
        have hmin : min y z = z := Nat.min_eq_right hzy
        rw [hmin] at h
        rw [hmin]
        simp only [List.mem_cons] at h ⊢
        rcases h with heq | htail
        · exact Or.inr (Or.inl heq)
        · exact Or.inr (Or.inr htail)

private theorem min?_mem_of_some {l : List Nat} {m : Nat} (hm : l.min? = some m) : m ∈ l := by
  cases l with
  | nil => simp at hm
  | cons y ys =>
      change some (ys.foldl min y) = some m at hm
      cases hm
      exact foldl_min_mem ys y

theorem sound_SeekCap : SeekCap.Goal := by
  intro xs a b s hab hsmall
  let la : List (List (List Nat)) :=
    (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))
  let lb : List (List (List Nat)) :=
    (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))
  let va : List Nat := la.map (charge write 0)
  let vb : List Nat := lb.map (charge write 0)
  have hva : va.min? = some s := by
    simpa [va, la, budget] using hsmall
  have hs_mem : s ∈ va := min?_mem_of_some hva
  have hs_mem_vb : s ∈ vb := by
    simp only [va, List.mem_map] at hs_mem
    rcases hs_mem with ⟨p, hp_la, hp_eq⟩
    simp only [vb, List.mem_map]
    refine ⟨p, ?_, hp_eq⟩
    simp only [la, List.mem_filter] at hp_la
    rcases hp_la with ⟨hp_parts, hp_pred_a⟩
    have hqa : ∀ q ∈ p, q.length ≤ a := by
      simpa using hp_pred_a
    have hp_pred_b : p.all (fun q => q.length ≤ b) = true := by
      simpa using (fun q hq => by
        have h := hqa q hq
        omega)
    simp only [lb, List.mem_filter]
    exact ⟨hp_parts, hp_pred_b⟩
  obtain ⟨t, htmin, ht_le⟩ := min?_le_of_mem hs_mem_vb
  refine ⟨t, ht_le, ?_⟩
  simpa [vb, lb, budget] using htmin

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_SeekCap
