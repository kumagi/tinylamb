import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `IntervalDeletes.SignedEndpoints` — `∀ rs i, blocked rs i = decide (0 < depth rs i)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_SignedEndpoints

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.IntervalDeletes

/-! `blocked rs i` is the disjunction of the interval hits.  `depth rs i` is the
    sum of signed endpoint contributions, and one span's two contributions add
    to exactly its hit indicator (empty or reversed spans contribute nothing).
    Hence `depth rs i` is the *number* of spans hitting `i`, so it is positive
    exactly when some span hits `i`.  Verified exhaustively on all lists of at
    most two spans with endpoints in `0..3` and probes `0..4` before proving. -/

private theorem span_contrib (a b i : Nat) :
    ((if a < b && a ≤ i then 1 else 0 : Nat) : Int) -
        ((if a < b && b ≤ i then 1 else 0 : Nat) : Int) =
      ((if hit (a, b) i then 1 else 0 : Nat) : Int) := by
  by_cases hab : a < b <;> by_cases hai : a ≤ i <;>
    by_cases hbi : b ≤ i <;> by_cases hii : i < b <;>
      simp [hit, hab, hai, hbi, hii] <;> omega

private theorem depth_eq_count_hits (rs : List Span) (i : Nat) :
    depth rs i = (rs.countP (fun r => hit r i) : Int) := by
  induction rs with
  | nil => simp [depth]
  | cons r rs ih =>
      rcases r with ⟨a, b⟩
      simp only [depth, List.countP_cons]
      have hcast (p : Bool) (c : Nat) :
          ((c + (if p then 1 else 0) : Nat) : Int) =
            ((if p then 1 else 0 : Nat) : Int) + (c : Int) := by
        cases p <;> omega
      rw [hcast (a < b && a ≤ i) (rs.countP (fun r : Span => r.1 < r.2 && r.1 ≤ i))]
      rw [hcast (a < b && b ≤ i) (rs.countP (fun r : Span => r.1 < r.2 && r.2 ≤ i))]
      rw [hcast (hit (a, b) i) (rs.countP (fun r : Span => hit r i))]
      rw [show ∀ A B C D : Int, (A + B) - (C + D) = (A - C) + (B - D) by
        intro A B C D; omega]
      rw [span_contrib a b i, ← ih]
      simp only [depth]

private theorem decide_pos_nat (n : Nat) :
    decide (0 < (n : Int)) = decide (0 < n) := by
  by_cases h : 0 < n <;> simp [h] <;> omega

private theorem decide_pos_add (n : Nat) (p : Bool) :
    decide (0 < n + (if p then 1 else 0 : Nat)) = (decide (0 < n) || p) := by
  cases p <;> simp <;> omega

private theorem blocked_eq_count_hits (rs : List Span) (i : Nat) :
    blocked rs i = decide (0 < rs.countP (fun r => hit r i)) := by
  induction rs with
  | nil => simp [blocked]
  | cons r rs ih =>
      simp only [blocked, List.any_cons, List.countP_cons] at *
      rw [decide_pos_add (rs.countP (fun r => hit r i)) (hit r i)]
      rw [← ih]
      exact Bool.or_comm _ _

theorem sound_IntervalDeletes_SignedEndpoints : SignedEndpoints.Goal := by
  intro rs i
  rw [depth_eq_count_hits]
  change blocked rs i = decide (0 < (rs.countP (fun r => hit r i) : Int))
  rw [decide_pos_nat]
  exact blocked_eq_count_hits rs i

end TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_SignedEndpoints
