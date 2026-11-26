import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `CsvRecord.NeutralSignature` — Two chunks equivalent from state zero can substitute
  at any scanner entry.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_NeutralSignature

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ CsvRecord.NeutralSignature.Goal := by
  intro h
  have hlen : [Sym.dq, Sym.lf, Sym.dq].length = [Sym.dq, Sym.atom, Sym.dq].length := by decide
  have hscan : scan CsvRecord.model 0 [Sym.dq, Sym.lf, Sym.dq] = scan CsvRecord.model 0 [Sym.dq, Sym.atom, Sym.dq] := by native_decide
  have hr : reachable CsvRecord.model 1 := ⟨[Sym.dq], by native_decide⟩
  have hh := h [Sym.dq, Sym.lf, Sym.dq] [Sym.dq, Sym.atom, Sym.dq] hlen hscan 1 hr
  have hneq : scan CsvRecord.model 1 [Sym.dq, Sym.lf, Sym.dq] ≠ scan CsvRecord.model 1 [Sym.dq, Sym.atom, Sym.dq] := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CsvRecord_NeutralSignature
