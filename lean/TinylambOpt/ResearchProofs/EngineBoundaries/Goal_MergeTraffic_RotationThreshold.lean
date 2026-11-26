import TinylambOpt.ResearchGoals.EngineBoundaries.MergeTraffic

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_MergeTraffic_RotationThreshold

open TinylambOpt.ResearchGoals.EngineBoundaries.MergeTraffic

private theorem depthBytes_succ (d : Nat) (t : Tree) : depthBytes (d+1) t = depthBytes d t + mass t := by
  induction t generalizing d with
  | leaf n => simp [depthBytes, mass, Nat.succ_mul]
  | merge a b iha ihb =>
      simp [depthBytes, mass, iha, ihb]
      omega

private theorem peak_le_twice_mass (t : Tree) : peak t ≤ 2 * mass t := by
  induction t with
  | leaf n => simp [peak, mass]; omega
  | merge a b iha ihb =>
      simp only [peak, mass]
      omega

private theorem pages_mono (p x y : Nat) (h : x ≤ y) : pages p x ≤ pages p y := by
  unfold pages
  apply Nat.div_le_div_right
  omega

private theorem pages_mul (p n : Nat) (hp : 0 < p) (hd : p ∣ n) : pages p n * p = n := by
  rcases hd with ⟨k, rfl⟩
  unfold pages
  have hquot : (p * k + p - 1) / p = k := by
    have h1 : k*p ≤ p*k + p - 1 := by
      rw [Nat.mul_comm k p]
      omega
    have h2 : p*k + p - 1 < (k+1)*p := by
      rw [Nat.add_mul, Nat.one_mul]
      rw [Nat.mul_comm k p]
      omega
    exact Nat.div_eq_of_lt_le h1 h2
  rw [hquot]
  rw [Nat.mul_comm k p]

private def alignedTree (p : Nat) : Tree → Prop
  | .leaf n => p ∣ n
  | .merge a b => alignedTree p a ∧ alignedTree p b

private theorem mass_dvd (p : Nat) (t : Tree) (h : alignedTree p t) : p ∣ mass t := by
  induction t with
  | leaf n => exact h
  | merge a b iha ihb =>
      simp [mass]
      exact Nat.dvd_add (iha h.1) (ihb h.2)

theorem sound : RotationThreshold.Goal := by
  intro a b c
  simp [io, mass]
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_MergeTraffic_RotationThreshold
