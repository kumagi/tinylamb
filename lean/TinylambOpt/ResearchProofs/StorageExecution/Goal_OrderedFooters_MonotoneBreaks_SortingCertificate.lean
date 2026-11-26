import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_SortingCertificate

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

private def breaks (xs : List Nat) : Nat :=
  (edges xs).countP (fun (x, y) => y < x)

private theorem edges_cons_cons (a b : Nat) (t : List Nat) :
    edges (a :: b :: t) = (a, b) :: edges (b :: t) := by
  simp [edges]

-- A list of naturals is non-decreasing exactly when it has no descending
-- adjacent edge.  This is the statement checked by the sorting certificate.
private theorem count_zero_iff_pairwise : ∀ xs : List Nat,
    breaks xs = 0 ↔ xs.Pairwise (· ≤ ·)
  | [] => by simp [breaks, edges]
  | [a] => by simp [breaks, edges]
  | a :: b :: t => by
      have ih := count_zero_iff_pairwise (b :: t)
      unfold breaks
      rw [edges_cons_cons]
      simp only [List.countP_cons]
      by_cases hba : b < a
      · have hnot : ¬ (a :: b :: t).Pairwise (· ≤ ·) := by
          intro h
          have hab : a ≤ b := (List.pairwise_cons.mp h).1 b (by simp)
          omega
        simp [hba, hnot]
      · have hab : a ≤ b := Nat.le_of_not_gt hba
        have hforall : (b :: t).Pairwise (· ≤ ·) → ∀ z ∈ b :: t, a ≤ z := by
          intro h z hz
          simp only [List.mem_cons] at hz
          rcases hz with rfl | hz
          · exact hab
          · have hbz : b ≤ z := (List.pairwise_cons.mp h).1 z hz
            omega
        simp only [hba]
        constructor
        · intro hcnt
          have hbt : (b :: t).Pairwise (· ≤ ·) := ih.mp hcnt
          exact List.pairwise_cons.mpr ⟨hforall hbt, hbt⟩
        · intro h
          have hbt : (b :: t).Pairwise (· ≤ ·) := (List.pairwise_cons.mp h).2
          exact ih.mpr hbt

theorem sound_SortingCertificate : MonotoneBreaks.SortingCertificate.Goal := by
  intro xs
  cases xs with
  | nil => simp [agg, edges]
  | cons a t =>
      simpa [agg, breaks] using (count_zero_iff_pairwise (a :: t))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_SortingCertificate
