import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_RebaseMask

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.IntervalDeletes

/-! Truth checked exhaustively first: every list of at most two spans built from
    nine representative endpoints (including empty, reversed and clamped ones),
    every `base` and every `n` in `0..4`, satisfies the goal. -/
private def spansUpTo2 : List (List Span) :=
  let atoms : List Span := [(0,0),(0,1),(0,3),(1,2),(1,4),(2,5),(3,3),(4,2),(5,1)]
  [] :: (atoms.map (fun r => [r])) ++ (atoms.flatMap (fun a => atoms.map (fun b => [a,b])))

#guard spansUpTo2.all (fun rs => (List.range 5).all (fun base =>
  (List.range 5).all (fun n =>
    ((List.range n).filter (fun i => !blocked rs (base+i))) ==
      live n (rs.map (fun (a,b) => (a-base,b-base))))))

private theorem bool_eq_of_iff {u v : Bool} (h : (u = true) ↔ (v = true)) : u = v := by
  cases u <;> cases v <;> simp_all

/-- Saturating subtraction commutes with translation by `base` in both endpoints:
    `a - base ≤ i ↔ a ≤ base + i` and `i < b - base ↔ base + i < b`. -/
private theorem hit_rebase (base i a b : Nat) :
    hit (a - base, b - base) i = hit (a, b) (base + i) := by
  apply bool_eq_of_iff
  simp [hit]
  constructor <;> intro h <;> rcases h with ⟨h1, h2⟩ <;> constructor
  · by_cases ha : a ≤ base <;> omega
  · by_cases hb : b ≤ base <;> omega
  · by_cases ha : a ≤ base <;> omega
  · by_cases hb : b ≤ base <;> omega

private theorem blocked_rebase (base i : Nat) (rs : List Span) :
    blocked (rs.map (fun p : Span => (p.1 - base, p.2 - base))) i = blocked rs (base + i) := by
  induction rs with
  | nil => simp [blocked]
  | cons r rs ih =>
      rcases r with ⟨a, b⟩
      simp only [blocked, List.map_cons, List.any_cons]
      rw [hit_rebase base i a b]
      have ih' : (List.map (fun p : Span => (p.1 - base, p.2 - base)) rs).any
          (fun x => hit x i) = rs.any (fun x => hit x (base + i)) := by
        simpa [blocked] using ih
      rw [ih']

theorem sound_IntervalDeletes_RebaseMask : RebaseMask.Goal := by
  intro base n rs
  unfold live
  apply List.filter_congr
  intro i hi
  rw [blocked_rebase base i rs]

end TinylambOpt.ResearchProofs.Atlas.Goal_IntervalDeletes_RebaseMask
