import TinylambOpt.ResearchProofs.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Refuted by counterexample

- `OverlappingSavings` — `∀ pat s, 1 < pat.length → let allHits := (List.range
  s.length).countP (fun i => pat.isPrefixOf (s.drop i)) (symbols pat s.length
  s).length + allHits*(pat.length-1) = s.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_OverlappingSavings

/-- Counterexample: `pat = [0,0]`, `s = [0,0,0]` — the two overlapping hits
are double-counted while the greedy parse consumes the middle byte. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.Compression.Text.OverlappingSavings.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.OverlappingSavings_disproof

end TinylambOpt.ResearchProofs.Compression.Goal_Text_OverlappingSavings
