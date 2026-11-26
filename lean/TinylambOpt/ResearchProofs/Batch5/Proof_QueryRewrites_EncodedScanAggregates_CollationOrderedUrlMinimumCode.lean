import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_CollationOrderedUrlMinimumCode

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Order-preserving code-to-URL decoding: the minimum accepted code yields the
minimum URL, including empty groups. This lets a SQL scan keep the encoded
minimum and decode it once, preserving the observable MIN result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- An order-preserving dictionary decodes the minimum code to the minimum
URL: monotone maps commute with taking a minimum, including the empty case. -/
theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.CollationOrderedUrlMinimumCode.Goal := by
  intro ids url hmono
  cases ids with
  | nil => simp
  | cons a as =>
      let l : List Nat := a :: as
      let m := l.min (by simp [l])
      have hm : l.min? = some m := by
        exact List.min?_eq_some_min (by simp [l])
      have hmem : m ∈ l := List.min_mem (by simp [l])
      have hle : ∀ b ∈ l, m ≤ b := (List.min?_eq_some_iff.mp hm).2
      have hminurl : (l.map url).min? = some (url m) := by
        apply (List.min?_eq_some_iff).2
        constructor
        · exact List.mem_map.mpr ⟨m, hmem, rfl⟩
        · intro v hv
          rcases List.mem_map.mp hv with ⟨b, hb, hv⟩
          subst v
          exact hmono m b (hle b hb)
      change Option.map url l.min? = (l.map url).min?
      rw [hm, hminurl]
      rfl
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_CollationOrderedUrlMinimumCode
