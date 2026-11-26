import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `NestedComment.Balanced` — `∀ n, (scan model 0 (List.replicate n .openComment ++
  List.replicate n .closeComment ++ [.semi])).2=[2*n]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_Balanced

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : NestedComment.Balanced.Goal := by
  have hsopen : ∀ q, NestedComment.model.step q Sym.openComment = (q+1,false) := by
    intro q
    simp [NestedComment.model, sym_beq_true]
  have hsclose : ∀ q, NestedComment.model.step q Sym.closeComment = (q-1,false) := by
    intro q
    simp [NestedComment.model, sym_beq_true]
  have hop : ∀ q n, scan NestedComment.model q (List.replicate n Sym.openComment) = (q+n,[]) := by
    intro q n
    induction n generalizing q with
    | zero => simp [scan]
    | succ n ih =>
        rw [List.replicate_succ, Common.scan_cons]
        simp only [hsopen]
        rw [ih (q+1)]
        congr 1 <;> omega
  have hcl : ∀ q n, scan NestedComment.model q (List.replicate n Sym.closeComment) = (q-n,[]) := by
    intro q n
    induction n generalizing q with
    | zero => simp [scan]
    | succ n ih =>
        rw [List.replicate_succ, Common.scan_cons]
        simp only [hsclose]
        rw [ih (q-1)]
        congr 1 <;> omega
  have hsemi : scan NestedComment.model 0 [Sym.semi] = (0,[0]) := by native_decide
  intro n
  have hc := Common.carry_general NestedComment.model 0 (List.replicate n Sym.openComment)
      (List.replicate n Sym.closeComment ++ [Sym.semi])
  rw [hop] at hc
  have hc2 : scan NestedComment.model n (List.replicate n Sym.closeComment ++ [Sym.semi]) =
      (0, [n]) := by
    have h2 := Common.carry_general NestedComment.model n (List.replicate n Sym.closeComment) [Sym.semi]
    rw [hcl n n] at h2
    simp [joinAt, hsemi] at h2
    exact h2
  have hsnd := congrArg Prod.snd hc
  simp [joinAt, hc2] at hsnd
  have hnn : n + n = 2 * n := by omega
  rw [hnn] at hsnd
  simpa [List.append_assoc] using hsnd

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_Balanced
