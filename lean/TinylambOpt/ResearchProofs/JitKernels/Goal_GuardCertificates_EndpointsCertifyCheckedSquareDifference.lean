import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_EndpointsCertifyCheckedSquareDifference

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

theorem refute : ¬ EndpointsCertifyCheckedSquareDifference.Goal := by
  intro h
  let d : Domain := { lo := 0, hi := 10, modulus := 1, residue := 0 }
  have hd : inside d (5 : Int) := by unfold inside d; native_decide
  have hf : fits 4 (first d * (10 - first d)) := by unfold fits first d; native_decide
  have hl : fits 4 (last d * (10 - last d)) := by unfold fits last d; native_decide
  have h' := h d 10 4 (5 : Int) (by native_decide) hd hf hl
  have hbad : ¬ fits 4 ((5 : Int) * (10 - 5)) := by unfold fits; native_decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_EndpointsCertifyCheckedSquareDifference
