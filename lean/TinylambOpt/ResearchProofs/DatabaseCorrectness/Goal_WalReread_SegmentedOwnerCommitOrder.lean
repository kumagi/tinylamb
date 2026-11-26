import TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

/-! Segmented WAL scans preserve owner-local commit order.

For an increasing WAL, the records with start below the segment boundary form
a prefix and the rest form a suffix. Splitting the window at that boundary
therefore concatenates exactly the same owner edits as one continuous scan.
SQL execution may scan consecutive LSN ranges separately and still apply each
transaction's writes in physical order. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_SegmentedOwnerCommitOrder

open TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

theorem sound : SegmentedOwnerCommitOrder.Goal := by
  intro log owner lo mid hi hlo hmid hpair
  induction log with
  | nil => simp [ownEdits]
  | cons r tl ih =>
      rw [List.pairwise_cons] at hpair
      rcases hpair with ⟨hall, htlpair⟩
      have ihtl := ih htlpair
      unfold ownEdits at ihtl ⊢
      by_cases hrmid : r.start < mid
      · have hrhi : r.start < hi := by omega
        by_cases hrlo : lo ≤ r.start
        · by_cases hown : r.owner == owner
          · cases hedit : r.edit <;> simp [hedit, hown, hrlo, hrmid, hrhi, ihtl]
          · simp [hown, ihtl]
        · have hnmid : ¬ mid ≤ r.start := Nat.not_le_of_lt hrmid
          simp [hrlo, hnmid, ihtl]
      · have hrmidge : mid ≤ r.start := Nat.le_of_not_gt hrmid
        have htailgt : ∀ a ∈ tl, mid ≤ a.start := by
          intro a ha
          exact Nat.le_of_lt (Nat.lt_of_le_of_lt hrmidge (hall a ha))
        have hf : tl.filter (fun x => x.owner == owner && lo ≤ x.start && x.start < mid) = [] := by
          rw [List.eq_nil_iff_forall_not_mem]
          intro a ha
          rw [List.mem_filter] at ha
          simp only [Bool.and_eq_true] at ha
          have hlt : a.start < mid := by simpa using ha.2.2
          exact (Nat.not_le_of_lt hlt) (htailgt a ha.1)
        have hfirst : (tl.filter (fun x => x.owner == owner && lo ≤ x.start && x.start < mid)).filterMap Record.edit = [] := by
          rw [hf]
          rfl
        rw [hfirst] at ihtl
        simp only [List.filter_cons] at ihtl ⊢
        simp [hrmid] at ⊢
        rw [hfirst] at ⊢
        simp only [List.nil_append, List.filterMap_cons] at ihtl ⊢
        by_cases hrhi : r.start < hi
        · by_cases hrlo : lo ≤ r.start
          · by_cases hown : r.owner == owner
            · have hownP : r.owner = owner := beq_iff_eq.mp hown
              cases hedit : r.edit <;> simp [hedit, hownP, hrlo, hrhi, hrmidge, ihtl]
            · have hownP : r.owner ≠ owner := by intro he; exact hown (by simp [he])
              simp [hownP, ihtl]
          · have hnmid : ¬ mid ≤ r.start := by omega
            simpa [hnmid, hrlo] using ihtl
        · simpa [hrhi] using ihtl
end TinylambOpt.ResearchProofs.DatabaseCorrectness.WalReread_SegmentedOwnerCommitOrder
