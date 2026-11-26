import TinylambOpt.ResearchProofs.Compression.Text

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Goal_Text_OverlappingSavings

/-- Counterexample: `pat = [0,0]`, `s = [0,0,0]` — the two overlapping hits
are double-counted while the greedy parse consumes the middle byte. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.Compression.Text.OverlappingSavings.Goal :=
  TinylambOpt.ResearchProofs.Compression.Text.OverlappingSavings_disproof

end TinylambOpt.ResearchProofs.Compression.Goal_Text_OverlappingSavings
