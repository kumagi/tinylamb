import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `BatchWatermarks.HistoricalBand` — `∀ xs late lo hi, lo ≤ hi → (score hi
  (xs++late)-score lo (xs++late)) - (score hi xs-score lo xs) = ((late.filter (fun r
  => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_HistoricalBand

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

/-! Exhaustive small-input sanity check: enumerate sub-lists of a small log and
check that the correction to the score difference equals the net contribution
of the late rows whose timestamp lies in the (lo, hi] band. -/

def smallRows : List Row := [(0, 3), (1, -2), (2, 5), (3, -1)]

def sublists : List Row → List (List Row)
  | [] => [[]]
  | r :: rs => (sublists rs).flatMap (fun s => [s, r :: s])

def band (late : List Row) (lo hi : Nat) : Int :=
  ((late.filter (fun r => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum

#eval (sublists smallRows).all (fun xs =>
  (sublists smallRows).all (fun late =>
    (List.range 5).all (fun lo =>
      (List.range 5).all (fun hi =>
        if lo ≤ hi then
          (score hi (xs ++ late) - score lo (xs ++ late)) - (score hi xs - score lo xs) == band late lo hi
        else true))))

private theorem score_append (t : Nat) (xs ys : List Row) :
    score t (xs ++ ys) = score t xs + score t ys := by
  simp [score, visible]

private theorem score_cons (t a : Nat) (w : Int) (rs : List Row) :
    score t ((a, w) :: rs) = (if a ≤ t then w else 0) + score t rs := by
  by_cases h : a ≤ t <;> simp [score, visible, h]

private theorem band_diff (late : List Row) (lo hi : Nat) (hle : lo ≤ hi) :
    score hi late - score lo late =
      ((late.filter (fun r => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum := by
  induction late with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons, score_cons]
      by_cases hhi : a ≤ hi
      · by_cases hlo : a ≤ lo
        · have hnlt : ¬ lo < a := by omega
          simp [hhi, hlo, hnlt]
          omega
        · have hnlo : ¬ a ≤ lo := hlo
          have hlt : lo < a := by omega
          simp [hhi, hnlo, hlt]
          omega
      · have hnlo : ¬ a ≤ lo := by omega
        simp [hhi, hnlo]
        omega

theorem sound_BatchWatermarks_HistoricalBand :
    BatchWatermarks.HistoricalBand.Goal := by
  intro xs late lo hi hle
  rw [score_append, score_append]
  have hband := band_diff late lo hi hle
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_HistoricalBand
