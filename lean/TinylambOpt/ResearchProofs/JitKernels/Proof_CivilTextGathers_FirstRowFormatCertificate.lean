import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_FirstRowFormatCertificate
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/--
`FirstRowFormatCertificate.Goal` claims validating the first row's hour digits
plus equal row lengths licenses eliding per-row digit checks. This is false:
equal lengths constrain no byte values of the second row.
Witness: `a` all '1' (digit bytes), `b` all 'A' (non-digit), both of length 19.
-/
theorem refute : ¬ FirstRowFormatCertificate.Goal := by
  intro h
  exact absurd
    (h (List.replicate 19 49) (List.replicate 19 65) (by decide) (by decide) (by decide))
    (by decide)

end TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_FirstRowFormatCertificate
