import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
An unbordered needle cannot match at two overlapping offsets: a second hit
inside the first would make a proper prefix equal to a proper suffix. SQL
execution may therefore advance by the full needle length after a hit when the
needle has no nonempty proper border.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_UnborderedHitAdvance

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem prefix_iff (p t : B) : p.isPrefixOf t = true ↔ ∃ u, p ++ u = t := by
  exact List.isPrefixOf_iff_prefix

theorem overlap_eq (p u v : B) (d : Nat)
    (h : (p.drop d) ++ u = p ++ v) : p.drop d = p.take (p.length - d) := by
  have htake := congrArg (fun l : B => l.take (p.length - d)) h
  have hlen : (p.drop d).length = p.length - d := by simp [List.length_drop]
  rw [← hlen] at htake
  rw [List.take_left] at htake
  rw [hlen] at htake
  rw [List.take_append_of_le_length (Nat.sub_le _ _)] at htake
  exact htake

theorem sound : UnborderedHitAdvance.Goal := by
  intro p s i j hun hi hj hij
  by_cases hle : i + p.length ≤ j
  · exact hle
  · have hjlt : j < i + p.length := by omega
    let d := j - i
    have hdpos : 0 < d := by dsimp [d]; omega
    have hdlt : d < p.length := by dsimp [d]; omega
    have hifilter : i ∈ (List.range (s.length + 1)).filter
        (fun t => p.isPrefixOf (s.drop t)) := by
      simpa [starts] using hi
    have hjfilter : j ∈ (List.range (s.length + 1)).filter
        (fun t => p.isPrefixOf (s.drop t)) := by
      simpa [starts] using hj
    rcases (prefix_iff p (s.drop i)).mp (List.mem_filter.mp hifilter).2 with ⟨u, hiu⟩
    rcases (prefix_iff p (s.drop j)).mp (List.mem_filter.mp hjfilter).2 with ⟨v, hjv⟩
    have hdrop : (p.drop d) ++ u = p ++ v := by
      calc
        (p.drop d) ++ u = (p ++ u).drop d := by
          rw [List.drop_append_of_le_length (by omega : d ≤ p.length)]
        _ = (s.drop i).drop d := by rw [hiu]
        _ = s.drop (i + d) := by rw [List.drop_drop]
        _ = s.drop j := by
          congr 1
          dsimp [d]
          omega
        _ = p ++ v := hjv.symm
    have hov := overlap_eq p u v d hdrop
    have hkpos : 0 < p.length - d := by omega
    have hklt : p.length - d < p.length := by omega
    have hne := hun (p.length - d) hkpos hklt
    exfalso
    apply hne
    have hkd : p.length - (p.length - d) = d := by omega
    rw [hkd]
    exact hov.symm

end TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_UnborderedHitAdvance
