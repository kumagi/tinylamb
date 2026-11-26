import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `GrayPair.AdjacentGray` — `∀ x y, ∃ b, (key (x,y)).1 ^^^ (key (x+1,y)).1 = 2^b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_GrayPair_AdjacentGray

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.GrayPair

-- Truth checks on small inputs (consecutive Gray codes differ in exactly one bit).
#eval (List.range 2000).all (fun x =>
  let d := (x ^^^ x / 2) ^^^ ((x + 1) ^^^ (x + 1) / 2)
  (List.range 32).any (fun b => d = 2 ^ b))

private def g (n : Nat) : Nat := n ^^^ (n / 2)

private theorem xor_swap (a b c d : Nat) : (a ^^^ b) ^^^ (c ^^^ d) = (a ^^^ c) ^^^ (b ^^^ d) := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp only [Nat.testBit_xor]
  cases ha : a.testBit i <;> cases hb : b.testBit i <;> cases hc : c.testBit i <;> cases hd : d.testBit i <;> simp [ha, hb, hc, hd]

private theorem xor_cancel_right (a b c : Nat) : (a ^^^ b) ^^^ (c ^^^ b) = a ^^^ c := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp only [Nat.testBit_xor]
  cases ha : a.testBit i <;> cases hb : b.testBit i <;> cases hc : c.testBit i <;> simp [ha, hb, hc]

private theorem xor_div_two (a b : Nat) : (a / 2) ^^^ (b / 2) = (a ^^^ b) / 2 := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_xor, Nat.testBit_div_two a i, Nat.testBit_div_two b i,
    Nat.testBit_div_two (a ^^^ b) i, Nat.testBit_xor]

private theorem xor_even_odd (a b : Nat) : (2 * a) ^^^ (2 * b + 1) = 2 * (a ^^^ b) + 1 := by
  apply Nat.eq_of_testBit_eq
  intro i
  cases i with
  | zero =>
      simp [Nat.testBit_zero]
  | succ i =>
      rw [Nat.testBit_xor]
      rw [Nat.testBit_add (2 * a) i 1, Nat.testBit_add (2 * b + 1) i 1,
        Nat.testBit_add (2 * (a ^^^ b) + 1) i 1]
      simp only [Nat.pow_one]
      have h1 : (2 * a) / 2 = a := by omega
      have h2 : (2 * b + 1) / 2 = b := by omega
      have h3 : (2 * (a ^^^ b) + 1) / 2 = a ^^^ b := by omega
      rw [h1, h2, h3, Nat.testBit_xor]

private theorem xor_odd_even (a b : Nat) : (2 * a + 1) ^^^ (2 * b) = 2 * (a ^^^ b) + 1 := by
  calc
    (2 * a + 1) ^^^ (2 * b) = (2 * b) ^^^ (2 * a + 1) := Nat.xor_comm _ _
    _ = 2 * (b ^^^ a) + 1 := xor_even_odd b a
    _ = 2 * (a ^^^ b) + 1 := by rw [Nat.xor_comm b a]

private theorem xor_consecutive_odd (n : Nat) : (n ^^^ (n + 1)) % 2 = 1 := by
  have hm : n % 2 < 2 := Nat.mod_lt n (by decide)
  have hcases : n % 2 = 0 ∨ n % 2 = 1 := by omega
  rcases hcases with h0 | h1
  · have hn : n = 2 * (n / 2) := by
      have := Nat.div_add_mod n 2
      omega
    rw [hn]
    rw [xor_even_odd]
    simp [Nat.xor_self]
  · have hn : n = 2 * (n / 2) + 1 := by
      have := Nat.div_add_mod n 2
      omega
    rw [hn]
    have hn2 : 2 * (n / 2) + 1 + 1 = 2 * ((n / 2) + 1) := by omega
    rw [hn2]
    rw [xor_odd_even]
    simp

private theorem odd_xor_shift (s : Nat) (hs : s % 2 = 1) :
    (2 * s + 1) ^^^ s = 2 * (s ^^^ (s / 2)) := by
  apply Nat.eq_of_testBit_eq
  intro i
  cases i with
  | zero =>
      rw [Nat.testBit_xor, Nat.testBit_zero, Nat.testBit_zero, Nat.testBit_zero]
      have htwo : (2 * s + 1) % 2 = 1 := by omega
      have hrhs : (2 * (s ^^^ (s / 2))) % 2 = 0 := by omega
      rw [htwo, hrhs, hs]
      simp
  | succ i =>
      rw [Nat.testBit_xor]
      rw [Nat.testBit_add (2 * s + 1) i 1, Nat.testBit_add s i 1,
        Nat.testBit_add (2 * (s ^^^ (s / 2))) i 1]
      simp only [Nat.pow_one]
      have h1 : (2 * s + 1) / 2 = s := by omega
      have h2 : (2 * (s ^^^ (s / 2))) / 2 = s ^^^ (s / 2) := by omega
      rw [h1, h2, Nat.testBit_xor]

theorem sound_AdjacentGray : GrayPair.AdjacentGray.Goal := by
  intro x y
  change ∃ b, g x ^^^ g (x + 1) = 2 ^ b
  exact Nat.strongRecOn x (motive := fun n => ∃ b, g n ^^^ g (n + 1) = 2 ^ b) (fun n ih => by
    have hm : n % 2 < 2 := Nat.mod_lt n (by decide)
    have hcases : n % 2 = 0 ∨ n % 2 = 1 := by omega
    rcases hcases with h0 | h1
    · have hn : n = 2 * (n / 2) := by
        have := Nat.div_add_mod n 2
        omega
      refine ⟨0, ?_⟩
      rw [hn]
      unfold g
      have hdiv1 : (2 * (n / 2)) / 2 = n / 2 := by omega
      have hdiv2 : (2 * (n / 2) + 1) / 2 = n / 2 := by omega
      simp only [hdiv1, hdiv2]
      calc
        (2 * (n / 2) ^^^ n / 2) ^^^ ((2 * (n / 2) + 1) ^^^ n / 2)
            = (2 * (n / 2)) ^^^ (2 * (n / 2) + 1) := xor_cancel_right _ _ _
        _ = (2 * (n / 2) + 1) ^^^ (2 * (n / 2)) := Nat.xor_comm _ _
        _ = 2 * ((n / 2) ^^^ (n / 2)) + 1 := xor_odd_even _ _
        _ = 2 ^ 0 := by simp [Nat.xor_self]
    · let q := n / 2
      have hn : n = 2 * q + 1 := by
        dsimp [q]
        have := Nat.div_add_mod n 2
        omega
      have hq_lt : q < n := by omega
      obtain ⟨b, hb⟩ := ih q hq_lt
      refine ⟨b + 1, ?_⟩
      rw [hn]
      have hn2 : 2 * q + 1 + 1 = 2 * (q + 1) := by omega
      rw [hn2]
      unfold g
      have hdiv1 : (2 * q + 1) / 2 = q := by omega
      have hdiv2 : (2 * (q + 1)) / 2 = q + 1 := by omega
      simp only [hdiv1, hdiv2]
      calc
        ((2 * q + 1) ^^^ q) ^^^ ((2 * (q + 1)) ^^^ (q + 1))
            = ((2 * q + 1) ^^^ (2 * (q + 1))) ^^^ (q ^^^ (q + 1)) := xor_swap _ _ _ _
        _ = (2 * (q ^^^ (q + 1)) + 1) ^^^ (q ^^^ (q + 1)) := by rw [xor_odd_even]
        _ = 2 * ((q ^^^ (q + 1)) ^^^ ((q ^^^ (q + 1)) / 2)) :=
              odd_xor_shift _ (xor_consecutive_odd q)
        _ = 2 * (g q ^^^ g (q + 1)) := by
              apply congrArg (fun t => 2 * t)
              rw [← xor_div_two q (q + 1)]
              rw [← xor_swap q (q / 2) (q + 1) ((q + 1) / 2)]
              rfl
        _ = 2 * 2 ^ b := by rw [hb]
        _ = 2 ^ (b + 1) := by rw [Nat.pow_succ, Nat.mul_comm])

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_GrayPair_AdjacentGray
