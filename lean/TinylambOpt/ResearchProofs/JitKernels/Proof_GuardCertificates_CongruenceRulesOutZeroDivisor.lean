import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-! SOUND: If a denominator's congruence class `d` has nonzero residue modulo its (positive)
stride, then no value inside the class is zero — even when the interval bounds cross zero —
so the JIT may hoist the divide-by-zero guard out of the loop. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_CongruenceRulesOutZeroDivisor
open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-- A nonzero residue class (mod a positive stride) excludes zero. -/
theorem sound : CongruenceRulesOutZeroDivisor.Goal := by
  intro d x hm hr hx hx0
  apply hr
  have hc : (x - d.residue) % (d.modulus:Int) = 0 := hx.2.2
  rw [hx0] at hc
  rw [show (0:Int) - d.residue = -d.residue by omega] at hc
  have hd : (d.modulus:Int) ∣ d.residue := Int.dvd_neg.mp (Int.dvd_of_emod_eq_zero hc)
  exact Int.emod_eq_zero_of_dvd hd

end TinylambOpt.ResearchProofs.JitKernels.GuardCertificates_CongruenceRulesOutZeroDivisor
