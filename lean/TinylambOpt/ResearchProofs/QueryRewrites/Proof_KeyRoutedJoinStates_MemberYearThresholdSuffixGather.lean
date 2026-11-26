import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_MemberYearThresholdSuffixGather. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_MemberYearThresholdSuffixGather

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# MemberYearThresholdSuffixGather

Memberships sorted by joining year have all sub-threshold years in a leading
prefix. Dropping that prefix therefore keeps exactly the memberships with year
at least 2010, so summing the person weights over the suffix equals summing the
guarded weights over the whole list, including duplicate persons and years.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.MemberYearThresholdSuffixGather.Goal := by
  intro memberships personMatches hsorted
  induction memberships with
  | nil => simp
  | cons r rs ih =>
      have hpair := List.pairwise_cons.mp hsorted
      have ih' := ih hpair.2
      have hge : ∀ q ∈ rs, r.1 ≤ q.1 := by
        intro q hq
        exact hpair.1 q.1 (List.mem_map.mpr ⟨q, hq, rfl⟩)
      by_cases hr : r.1 < 2010
      · have hrnot : ¬ 2010 ≤ r.1 := by omega
        simpa [hr, hrnot] using ih'
      · have hrge : 2010 ≤ r.1 := by omega
        have htailge : ∀ q ∈ rs, 2010 ≤ q.1 := by
          intro q hq
          have h := hge q hq
          omega
        have htailif :
            (rs.map (fun (yr, p) => if 2010 ≤ yr then personMatches p else 0)).sum =
              (rs.map (fun q => personMatches q.2)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro q hq
          simp [show 2010 ≤ q.1 from htailge q hq]
        simp [hr, hrge, htailif]
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_MemberYearThresholdSuffixGather
