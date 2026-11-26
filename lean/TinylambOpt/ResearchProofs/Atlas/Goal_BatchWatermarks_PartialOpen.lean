import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `BatchWatermarks.PartialOpen` — `∀ runs t, score t runs.flatten = score t (summaries
  runs) + ((runs.filter (fun r => t < stamp r)).map (score t)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_PartialOpen

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

/-! Exhaustive small-input sanity check: pairs of sub-lists of a small log,
checked at every threshold. -/

def smallRows : List Row := [(0, 3), (1, -2), (2, 5), (3, -1)]

def sublists : List Row → List (List Row)
  | [] => [[]]
  | r :: rs => (sublists rs).flatMap (fun s => [s, r :: s])

#eval (sublists smallRows).all (fun a =>
  (sublists smallRows).all (fun b =>
    (List.range 6).all (fun t =>
      score t ([a, b].flatten) ==
        score t (summaries [a, b]) +
          (([a, b].filter (fun r => t < stamp r)).map (score t)).sum)))

private theorem score_append (t : Nat) (xs ys : List Row) :
    score t (xs ++ ys) = score t xs + score t ys := by
  simp [score, visible]

private theorem score_cons (t a : Nat) (w : Int) (rs : List Row) :
    score t ((a, w) :: rs) = (if a ≤ t then w else 0) + score t rs := by
  by_cases h : a ≤ t <;> simp [score, visible, h]

private theorem score_eq_sum_of_all_le (t : Nat) (r : List Row)
    (h : ∀ (a : Nat) (w : Int), (a, w) ∈ r → a ≤ t) :
    score t r = (r.map Prod.snd).sum := by
  induction r with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rw [score_cons]
      have hr : r.1 ≤ t := h r.1 r.2 (by simp)
      have hrs : ∀ (a : Nat) (w : Int), (a, w) ∈ rs → a ≤ t := by
        intro a w haw
        exact h a w (by simp [haw])
      rw [ih hrs]
      simp [hr]

private theorem stamp_upper : ∀ (r : List Row) (a : Nat) (w : Int),
    (a, w) ∈ r → a ≤ stamp r := by
  intro r
  induction r with
  | nil => simp
  | cons r rs ih =>
      intro a w h
      rcases r with ⟨a0, w0⟩
      simp only [List.mem_cons] at h
      rcases h with h | h
      · injection h with ha hw
        subst a
        subst w
        simp [stamp, List.max?_cons]
        cases hm : (rs.map Prod.fst).max? with
        | none => simp
        | some b => exact Nat.le_max_left a0 b
      · have hih := ih a w h
        simp [stamp, List.max?_cons]
        cases hm : (rs.map Prod.fst).max? with
        | none =>
            simp [stamp, hm] at hih ⊢
            omega
        | some b =>
            simp [stamp, hm] at hih ⊢
            have hb : b ≤ max a0 b := Nat.le_max_right a0 b
            omega

theorem sound_BatchWatermarks_PartialOpen :
    BatchWatermarks.PartialOpen.Goal := by
  intro runs t
  induction runs with
  | nil => simp [score, visible, summaries]
  | cons r rs ih =>
      rw [show (r :: rs).flatten = r ++ rs.flatten by rfl]
      rw [score_append, ih]
      by_cases h : t < stamp r
      · have hn : ¬ stamp r ≤ t := by omega
        simp only [summaries, List.map_cons]
        rw [score_cons]
        simp [h, hn]
        omega
      · have hs : stamp r ≤ t := by omega
        have hscore : score t r = (r.map Prod.snd).sum := by
          apply score_eq_sum_of_all_le
          intro a w haw
          exact Nat.le_trans (stamp_upper r a w haw) hs
        simp only [summaries, List.map_cons]
        rw [score_cons]
        simp [h, hs, hscore]
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_PartialOpen
