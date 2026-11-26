import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_DuplicateSourceIdentityConflict

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity


theorem beq_comm_key (a b : Key) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor <;> intro h
  · exact beq_iff_eq.mpr (beq_iff_eq.mp h).symm
  · exact beq_iff_eq.mpr (beq_iff_eq.mp h).symm

theorem eqOn_count (src : List Key) (k : Key) (hk : k.isSome) :
    (src.filter (fun a => eqOn k a)).length = src.count k := by
  rw [← List.countP_eq_length_filter, List.count_eq_countP]
  induction src with
  | nil => rfl
  | cons a src ih =>
      rw [List.countP_cons, List.countP_cons, ih]
      simp [eqOn, hk, beq_comm_key]

theorem zipIdx_filter_length (src : List Key) (k : Key) (n : Nat) :
    ((src.zipIdx n).filter (fun s => eqOn k s.1)).length =
      (src.filter (fun a => eqOn k a)).length := by
  rw [← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
  induction src generalizing n with
  | nil => rfl
  | cons a src ih =>
      simp only [List.zipIdx]
      rw [List.countP_cons, List.countP_cons, ih]

theorem sourceIds_length (src : List Key) (k : Key) (hk : k.isSome) :
    (sourceIds src k).length = src.count k := by
  rw [sourceIds, List.length_map]
  exact (zipIdx_filter_length src k 0).trans (eqOn_count src k hk)

theorem sound : DuplicateSourceIdentityConflict.Goal := by
  intro src k hk hc
  have hlen := sourceIds_length src k hk
  have hgt : 1 < (sourceIds src k).length := by omega
  have hne : sourceIds src k ≠ [] := by
    intro h
    rw [h] at hgt
    simp at hgt
  simp [decideMatched, hne, hgt]
  rfl

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_DuplicateSourceIdentityConflict
