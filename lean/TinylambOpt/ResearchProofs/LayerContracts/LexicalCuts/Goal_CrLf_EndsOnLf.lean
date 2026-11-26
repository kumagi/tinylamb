import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `CrLf.EndsOnLf` — `∀ q xs i, i∈(scan model q xs).2 → xs[i]?=some .lf`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_EndsOnLf

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

private theorem hit_is_lf (q : Nat) (c : Sym) (h : (CrLf.model.step q c).2 = true) : c = Sym.lf := by
  cases c <;> simp [CrLf.model, sym_beq_true] at h ⊢

theorem sound : CrLf.EndsOnLf.Goal := by
  intro q xs
  induction xs generalizing q with
  | nil =>
      intro i h
      simp [scan] at h
  | cons c xs ih =>
      intro i h
      rcases hstep : CrLf.model.step q c with ⟨q1,hit⟩
      rw [Common.scan_cons] at h
      simp [hstep] at h
      by_cases hh : hit = true
      · have hc : c = Sym.lf := hit_is_lf q c (by rw [hstep]; exact hh)
        simp [hh] at h
        rcases h with hi0 | ⟨j,hj,hji⟩
        · subst i
          subst c
          rfl
        · rw [← hji]
          have hjl := ih q1 j hj
          rw [show 1 + j = j + 1 by omega]
          simpa using hjl
      · simp [hh] at h
        rcases h with ⟨j,hj,hji⟩
        rw [← hji]
        have hjl := ih q1 j hj
        rw [show 1 + j = j + 1 by omega]
        simpa using hjl

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_EndsOnLf
