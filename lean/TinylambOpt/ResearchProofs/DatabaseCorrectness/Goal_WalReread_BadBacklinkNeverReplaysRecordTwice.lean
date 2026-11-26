import TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

/-! A broken backlink never replays the same WAL record twice.

The backward walk only continues when the previous LSN is strictly smaller
than the current one, so every visited record has a smaller start than its
predecessor in the walk. SQL execution may fall back to WAL re-read with a
corrupt backlink without applying an edit twice. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_BadBacklinkNeverReplaysRecordTwice

open TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

private theorem find_start (wal : List Record) (owner cur : Nat) (r : Record)
    (h : wal.find? (fun x => x.start == cur && x.owner == owner) = some r) : r.start = cur := by
  have hp := List.find?_some h
  simp only [Bool.and_eq_true] at hp
  exact beq_iff_eq.mp hp.1

private theorem backward_start_le (wal : List Record) (owner first fuel cur : Nat) :
    ∀ r ∈ backward wal owner first fuel cur, r.start ≤ cur := by
  induction fuel generalizing cur with
  | zero =>
      intro r hr
      simp [backward] at hr
  | succ fuel ih =>
      dsimp [backward]
      cases hfind : wal.find? (fun x => x.start == cur && x.owner == owner) with
      | none => simp
      | some r =>
          have hs : r.start = cur := find_start wal owner cur r hfind
          by_cases hcond : (cur == first || cur ≤ r.prev || r.prev < first)
          · simp [hcond]
            omega
          · simp [hcond]
            constructor
            · omega
            · intro x hx
              have hle := ih (cur := r.prev) x hx
              have hprevlt : r.prev < cur := by
                have hn : ¬ cur ≤ r.prev := by
                  intro hle'
                  apply hcond
                  simp [hle']
                omega
              omega

theorem sound : BadBacklinkNeverReplaysRecordTwice.Goal := by
  intro log owner first fuel cur
  induction fuel generalizing cur with
  | zero => simp [backward]
  | succ fuel ih =>
      dsimp [backward]
      cases hfind : log.find? (fun x => x.start == cur && x.owner == owner) with
      | none => simp
      | some r =>
          have hs : r.start = cur := find_start log owner cur r hfind
          by_cases hcond : (cur == first || cur ≤ r.prev || r.prev < first)
          · simp [hcond]
          · simp [hcond]
            constructor
            · intro x hmem
              have hle := backward_start_le log owner first fuel r.prev x hmem
              have hprevlt : r.prev < cur := by
                have hn : ¬ cur ≤ r.prev := by
                  intro hle'
                  apply hcond
                  simp [hle']
                omega
              omega
            · exact ih (cur := r.prev)
end TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_BadBacklinkNeverReplaysRecordTwice
