import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_EquivalentHistory

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

/-! Exhaustive small-input sanity check: enumerate all sub-lists of a small
log and compare `score` with the per-timestamp net contribution. -/

def baseRows : List Row := [(0, 4), (0, -1), (1, -4), (2, 3)]

def sublists : List Row → List (List Row)
  | [] => [[]]
  | r :: rs => (sublists rs).flatMap (fun s => [s, r :: s])

def netAt (xs : List Row) (t : Nat) : Int :=
  ((xs.filter (fun r => r.1 == t)).map Prod.snd).sum

#eval (sublists baseRows).all (fun a =>
  (sublists baseRows).all (fun b =>
    ((List.range 4).all (fun t => score t a == score t b)) ==
      ((List.range 4).all (fun t => netAt a t == netAt b t))))

private theorem beq_eq_decide (a b : Nat) : (a == b) = decide (a = b) := by
  by_cases h : a == b
  · have hab : a = b := beq_iff_eq.mp h
    simp [hab]
  · have hab : a ≠ b := by intro he; exact h (by simp [he])
    simp [hab]

private theorem score_cons (t : Nat) (a : Nat) (w : Int) (rs : List Row) :
    score t ((a, w) :: rs) = (if a ≤ t then w else 0) + score t rs := by
  by_cases h : a ≤ t <;> simp [score, visible, h]

private theorem score_zero (xs : List Row) :
    score 0 xs = ((xs.filter (fun r => r.1 == 0)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons]
      have ih' : score 0 rs = ((rs.filter (fun r => r.1 = 0)).map Prod.snd).sum := by
        simpa [beq_eq_decide] using ih
      rw [ih']
      by_cases ha : a = 0 <;> simp [ha, beq_eq_decide] <;> omega

private theorem score_succ (xs : List Row) (n : Nat) :
    score (n + 1) xs = score n xs + ((xs.filter (fun r => r.1 == n + 1)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons, score_cons]
      have ih' : score (n + 1) rs =
          score n rs + ((rs.filter (fun r => r.1 = n + 1)).map Prod.snd).sum := by
        simpa [beq_eq_decide] using ih
      rw [ih']
      by_cases h1 : a ≤ n + 1 <;> by_cases h0 : a ≤ n <;>
        by_cases ha : a = n + 1 <;> simp [h1, h0, ha, beq_eq_decide] <;> omega

theorem sound_BatchWatermarks_EquivalentHistory :
    BatchWatermarks.EquivalentHistory.Goal := by
  intro a b
  constructor
  · intro h t
    cases t with
    | zero =>
        have ha := score_zero a
        have hb := score_zero b
        have ht := h 0
        omega
    | succ n =>
        have ha := score_succ a n
        have hb := score_succ b n
        have ht := h (n + 1)
        have hp := h n
        omega
  · intro h t
    induction t with
    | zero =>
        have ha := score_zero a
        have hb := score_zero b
        have ht := h 0
        omega
    | succ n ih =>
        have ha := score_succ a n
        have hb := score_succ b n
        have ht := h (n + 1)
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_BatchWatermarks_EquivalentHistory
