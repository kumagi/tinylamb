import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HistoricalBand

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

private theorem score_cons (t : Nat) (a : Nat) (w : Int) (rs : List Row) :
    score t ((a, w) :: rs) = (if a ≤ t then w else 0) + score t rs := by
  by_cases h : a ≤ t <;> simp [score, visible, h]

private theorem score_append (t : Nat) (xs ys : List Row) :
    score t (xs ++ ys) = score t xs + score t ys := by
  simp [score, visible, List.filter_append]

private theorem band_score (lo hi : Nat) (hlohi : lo ≤ hi) (xs : List Row) :
    score hi xs - score lo xs =
      ((xs.filter (fun r => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons, score_cons]
      have hsplit : (if a ≤ hi then w else 0) + score hi rs -
          ((if a ≤ lo then w else 0) + score lo rs) =
          (score hi rs - score lo rs) + ((if a ≤ hi then w else 0) - (if a ≤ lo then w else 0)) := by
        omega
      rw [hsplit, ih]
      by_cases hhi : a ≤ hi <;> by_cases hltlo : lo < a <;>
        simp [hhi, hltlo] <;> omega

theorem sound_HistoricalBand :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.HistoricalBand.Goal := by
  intro xs late lo hi hlohi
  rw [score_append, score_append]
  have hb := band_score lo hi hlohi late
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_HistoricalBand
