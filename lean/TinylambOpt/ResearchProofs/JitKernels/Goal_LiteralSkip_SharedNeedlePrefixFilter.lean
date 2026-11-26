import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
A match of a concatenated needle is exactly a match of its prefix followed by a
match of its suffix at the advanced offset. Reusing the prefix hit positions as
candidates for a longer literal therefore preserves the set of starts.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_SharedNeedlePrefixFilter

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem prefix_iff (p t : B) : p.isPrefixOf t = true ↔ ∃ u, p ++ u = t := by
  exact List.isPrefixOf_iff_prefix

theorem prefix_append_iff (a b t : B) :
    (a ++ b).isPrefixOf t = true ↔
      a.isPrefixOf t = true ∧ b.isPrefixOf (t.drop a.length) = true := by
  rw [prefix_iff, prefix_iff, prefix_iff]
  constructor
  · intro h
    rcases h with ⟨u, hu⟩
    constructor
    · exact ⟨b ++ u, by simpa [List.append_assoc] using hu⟩
    · have hdrop : b ++ u = t.drop a.length := by
        rw [← hu]
        simp
      exact ⟨u, hdrop⟩
  · intro h
    rcases h with ⟨ha, hb⟩
    rcases ha with ⟨u, hu⟩
    rcases hb with ⟨v, hv⟩
    have hdrop : u = b ++ v := by
      rw [← hu] at hv
      simp at hv
      exact hv.symm
    exact ⟨v, by simpa [List.append_assoc, hdrop] using hu⟩

theorem sound : TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.SharedNeedlePrefixFilter.Goal := by
  intro a b s
  have hpred : (fun i : Nat =>
      b.isPrefixOf (s.drop (i + a.length)) && a.isPrefixOf (s.drop i)) =
      (fun i : Nat => (a ++ b).isPrefixOf (s.drop i)) := by
    funext i
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      exact (prefix_append_iff a b (s.drop i)).mpr
        ⟨(Bool.and_eq_true_iff.mp h).2, by simpa [List.drop_drop] using (Bool.and_eq_true_iff.mp h).1⟩
    · intro h
      have hp := (prefix_append_iff a b (s.drop i)).mp h
      exact Bool.and_eq_true_iff.mpr ⟨by simpa [List.drop_drop] using hp.2, hp.1⟩
  simp only [starts, List.filter_filter]
  rw [hpred]

end TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_SharedNeedlePrefixFilter
