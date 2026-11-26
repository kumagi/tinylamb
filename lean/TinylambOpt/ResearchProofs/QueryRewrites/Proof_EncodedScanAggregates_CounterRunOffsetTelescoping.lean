import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

/-!
Counter-run offset telescoping: positive endpoint distances and repeated
endpoints recover a monotone run's total offset and span count. This allows
encoded counter runs to be aggregated without reconstructing every row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_CounterRunOffsetTelescoping

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


private theorem telescope_aux : ∀ (ends : List Nat) (start : Nat),
    (start :: ends).Pairwise (· ≤ ·) →
      let spans := (start :: ends).zip ends
      let lengths := spans.map (fun (a, b) => b - a)
      ((lengths.filter (· > 0)).sum = (ends.getLast?).getD start - start ∧
       (lengths.filter (· > 0)).length =
         ends.length - TinylambOpt.ResearchGoals.QueryRewrites.countWhere spans (fun r => r.1 == r.2)) ∧
      start ≤ (ends.getLast?).getD start := by
  intro ends
  induction ends with
  | nil =>
      intro start h
      simp
  | cons e rest ih =>
      intro start h
      have hse : start ≤ e := (List.pairwise_cons.mp h).1 e (by simp)
      have hrest : (e :: rest).Pairwise (· ≤ ·) := (List.pairwise_cons.mp h).2
      have ih' := ih e hrest
      simp only [List.zip_cons_cons, List.map_cons, List.filter_cons,
        List.length_cons, TinylambOpt.ResearchGoals.QueryRewrites.countWhere,
        List.filter_cons] at ih' ⊢
      by_cases heq : start = e
      · subst e
        simp [List.getLast?_cons] at ih' ⊢
        exact ih'
      · have hlt : start < e := Nat.lt_of_le_of_ne hse heq
        have hpos : 0 < e - start := Nat.sub_pos_of_lt hlt
        have hnebool : (start == e) = false := by simp [heq]
        simp [List.getLast?_cons, hpos, hnebool] at ih' ⊢
        have hsum := ih'.1.1
        have hcnt := ih'.1.2
        have hle := ih'.2
        let z := (List.filter (fun r => r.fst == r.snd) ((e :: rest).zip rest)).length
        have hz : z ≤ rest.length := by
          have hz' := List.length_filter_le (fun r => r.fst == r.snd) ((e :: rest).zip rest)
          simp [z, List.length_zip] at hz' ⊢
          exact hz'
        constructor
        · constructor
          · rw [hsum]
            rw [Nat.add_comm]
            exact Nat.sub_add_sub_cancel hle hse
          · rw [hcnt]
            change _ + 1 = rest.length + 1 - z
            rw [Nat.add_comm rest.length 1, Nat.add_sub_assoc hz]
            omega
        · omega

/-- On a monotone counter run the positive endpoint distances telescope to
the final offset, and the positive count is the span count minus zero spans. -/
theorem sound : CounterRunOffsetTelescoping.Goal := by
  intro start ends hpair
  have h := telescope_aux ends start hpair
  exact Prod.ext h.1.1 h.1.2

end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_CounterRunOffsetTelescoping
