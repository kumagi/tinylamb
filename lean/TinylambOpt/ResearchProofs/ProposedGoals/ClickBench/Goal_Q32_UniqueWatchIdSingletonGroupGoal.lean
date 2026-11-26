import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q32_UniqueWatchIdSingletonGroupGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem filter_fst_eq_singleton {rows : List (Nat × Int)} {w : Nat} {v : Int}
    (hnodup : (rows.map Prod.fst).Nodup) (hmem : (w, v) ∈ rows) :
    rows.filter (fun r => r.1 == w) = [(w, v)] := by
  induction rows with
  | nil => simp at hmem
  | cons r rs ih =>
      rcases r with ⟨w', v'⟩
      simp only [List.map_cons] at hnodup
      have hnd := List.nodup_cons.mp hnodup
      have hne : w' ∉ rs.map Prod.fst := hnd.1
      have htailnodup : (rs.map Prod.fst).Nodup := hnd.2
      simp only [List.mem_cons] at hmem
      rcases hmem with hhead | htail
      · have heq : (w', v') = (w, v) := hhead.symm
        have hw : w = w' := by simpa using congrArg Prod.fst heq.symm
        have hv : v = v' := by simpa using congrArg Prod.snd heq.symm
        subst w
        subst v
        have htailnil : rs.filter (fun r => r.1 == w') = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro x hx hxw
          have hxw' : x.1 = w' := by simpa using hxw
          exact hne (List.mem_map.mpr ⟨x, hx, hxw'⟩)
        simp [htailnil]
      · have hwne : w' ≠ w := by
          intro hweq
          subst w'
          exact hne (List.mem_map.mpr ⟨(w, v), htail, rfl⟩)
        have ihres := ih htailnodup htail
        simp [hwne, ihres]

theorem sound : Q32_UniqueWatchIdSingletonGroupGoal := by
  intro rows w v hnodup hmem
  have hsingle := filter_fst_eq_singleton hnodup hmem
  simp [total, hsingle]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q32_UniqueWatchIdSingletonGroupGoal
