import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-! Independent proof record for the InductionLowering.ResiduePeriodLoopPeeling research goal: the proposed rewrite is sound and preserves the modeled result. -/

/-!
Stepping by `step` modulo `slots` visits `slots / gcd(slots, step)` distinct
residues and returns to the starting residue after that period. Writing
`slots = g*s` and `step = g*t` with coprime `s` and `t` reduces the claim to
cancellation of the coprime factor `t` modulo `s`; SQL execution may peel one
period of a modular stride and specialize the phase without losing slots.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false
set_option maxRecDepth 10000

namespace Nov26.Proof_InductionLowering_ResiduePeriodLoopPeeling
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering.ResiduePeriodLoopPeeling

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem sound : Goal := by
  intro slots step start hslots
  let g := Nat.gcd slots step
  let s := slots / g
  let t := step / g
  have hgpos : 0 < g := Nat.pos_of_dvd_of_pos (Nat.gcd_dvd_left slots step) hslots
  have hsg : slots = g * s := by
    dsimp [s]
    rw [Nat.mul_comm]
    exact (Nat.div_mul_cancel (Nat.gcd_dvd_left slots step)).symm
  have htg : step = g * t := by
    dsimp [t]
    rw [Nat.mul_comm]
    exact (Nat.div_mul_cancel (Nat.gcd_dvd_right slots step)).symm
  have hcop : s.Coprime t := by
    dsimp [s, t]
    exact Nat.coprime_div_gcd_div_gcd hgpos
  have hinj : ∀ i j : Nat, i < s → j < s →
      (start + i * step) % slots = (start + j * step) % slots → i = j := by
    intro i j hi hj hmod
    rcases Nat.mod_eq_mod_iff.mp hmod with ⟨k1, k2, heq⟩
    have heq' : i * step + k1 * slots = j * step + k2 * slots := by omega
    rw [hsg, htg] at heq'
    have heq'' : g * (i * t + k1 * s) = g * (j * t + k2 * s) := by
      calc
        g * (i * t + k1 * s) = i * (g * t) + k1 * (g * s) := by
          simp [Nat.mul_add, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
        _ = j * (g * t) + k2 * (g * s) := heq'
        _ = g * (j * t + k2 * s) := by
          simp [Nat.mul_add, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
    have hcan : i * t + k1 * s = j * t + k2 * s :=
      Nat.eq_of_mul_eq_mul_left hgpos heq''
    by_cases hij : i ≤ j
    · let d := j - i
      have hjdecomp : j = i + d := by dsimp [d]; omega
      have hdlt : d < s := by dsimp [d]; omega
      rw [hjdecomp, Nat.add_mul] at hcan
      have heqd : k1 * s = k2 * s + d * t := by omega
      have hdvd : s ∣ d * t := by
        rw [Nat.dvd_add_iff_right (Nat.dvd_mul_left s k2)]
        rw [← heqd]
        exact Nat.dvd_mul_left s k1
      have hsdvd : s ∣ d := hcop.dvd_of_dvd_mul_right hdvd
      have hd0 : d = 0 := Nat.eq_zero_of_dvd_of_lt hsdvd hdlt
      omega
    · have hji : j ≤ i := by omega
      let d := i - j
      have hidecomp : i = j + d := by dsimp [d]; omega
      have hdlt : d < s := by dsimp [d]; omega
      rw [hidecomp, Nat.add_mul] at hcan
      have heqd : k2 * s = k1 * s + d * t := by omega
      have hdvd : s ∣ d * t := by
        rw [Nat.dvd_add_iff_right (Nat.dvd_mul_left s k1)]
        rw [← heqd]
        exact Nat.dvd_mul_left s k2
      have hsdvd : s ∣ d := hcop.dvd_of_dvd_mul_right hdvd
      have hd0 : d = 0 := Nat.eq_zero_of_dvd_of_lt hsdvd hdlt
      omega
  have hpair : (List.range s).Pairwise
      (fun a b : Nat => (start + a * step) % slots ≠ (start + b * step) % slots) := by
    apply List.Pairwise.imp_of_mem (l := List.range s)
      (R := fun a b : Nat => a ≠ b)
      (S := fun a b : Nat => (start + a * step) % slots ≠ (start + b * step) % slots)
    · intro a b ha hb hne heq
      exact hne (hinj a b (List.mem_range.mp ha) (List.mem_range.mp hb) heq)
    · exact List.nodup_range
  have hnodup : ((List.range s).map (fun i => (start + i * step) % slots)).Nodup := by
    exact List.Pairwise.map (fun i : Nat => (start + i * step) % slots)
      (fun a b h => h) hpair
  have hslotsdvd : slots ∣ s * step := by
    rw [hsg, htg]
    have hprod : s * (g * t) = (g * s) * t := by ac_rfl
    rw [hprod]
    exact Nat.dvd_mul_right (g * s) t
  change ((List.range s).map (fun i => (start + i * step) % slots)).Nodup ∧
    (start + s * step) % slots = start % slots
  constructor
  · exact hnodup
  · rw [Nat.add_mod]
    rw [Nat.mod_eq_zero_of_dvd hslotsdvd]
    simp

end Nov26.Proof_InductionLowering_ResiduePeriodLoopPeeling
