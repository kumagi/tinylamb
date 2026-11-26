import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `Contiguous.ReadAmplification` — `∀ (seekFee : Nat), readAmplification (price
  seekFee)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_ReadAmplification

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Contiguous

-- brute force truth probe on small inputs
def probe : Bool :=
  (List.range 5).all (fun fee =>
  (List.range 6).all (fun a =>
  (List.range 6).all (fun b =>
    a < b →
  ([[0], [1], [0,1], [0,4,4,20], [3,1,7,2,9], [0,0,0], [5,2,5,2,5], [9,1,3,7,2,8,4], [0,2], [0,1,2], [0,1,2,3], [2,0,2,0]] : List (List Nat)).all (fun xs =>
    used (choose (fun l r => b*span l r + price fee l r) xs) ≤
      used (choose (fun l r => a*span l r + price fee l r) xs)))))

-- `covers` is never empty: every point set admits at least one covering plan.
private theorem covers_nonempty : ∀ xs : List Nat, covers xs ≠ [] := by
  intro xs
  induction xs with
  | nil => simp [covers]
  | cons x xs ih =>
      rw [covers]
      cases hcov : covers xs with
      | nil => contradiction
      | cons p ps =>
          cases p with
          | nil => simp
          | cons a tl =>
              rcases a with ⟨_, _⟩
              simp

-- For the contiguous byte model an extent costs a fixed seek fee plus a
-- per-byte coefficient times its span, so the total cost is linear in `used`.
private theorem cost_linear (fee c : Nat) (ps : List Extent) :
    cost (fun l r => c * span l r + price fee l r) ps =
      ps.length * fee + (c + 1) * used ps := by
  induction ps with
  | nil => simp [cost, used]
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      have hstep : c * span a b + price fee a b = fee + (c + 1) * span a b := by
        rw [price, ← Nat.succ_eq_add_one c, Nat.succ_mul]
        omega
      calc
        cost (fun l r => c * span l r + price fee l r) ((a, b) :: ps)
            = (fee + (c + 1) * span a b) +
                cost (fun l r => c * span l r + price fee l r) ps := by
                simp [cost, hstep]
        _ = (fee + (c + 1) * span a b) +
              (ps.length * fee + (c + 1) * used ps) := by rw [ih]
        _ = ((a, b) :: ps).length * fee + (c + 1) * used ((a, b) :: ps) := by
              simp [used, Nat.add_mul, Nat.mul_add]
              omega

-- `choose` returns a member of `covers (points xs)`.
private theorem choose_mem (f : Price) (xs : List Nat) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  let r : List Extent → List Extent → Bool := fun p q => decide (cost f p ≤ cost f q)
  change ((covers (points xs)).mergeSort r).head?.getD [] ∈ covers (points xs)
  have hperm := List.mergeSort_perm (covers (points xs)) r
  have hne : (covers (points xs)).mergeSort r ≠ [] := by
    intro hnil
    have hlen := hperm.length_eq
    simp [hnil] at hlen
    exact covers_nonempty (points xs) (List.length_eq_zero_iff.mp hlen.symm)
  cases hms : (covers (points xs)).mergeSort r with
  | nil => contradiction
  | cons p tl =>
      have hp : p ∈ covers (points xs) := hperm.mem_iff.mp (by simp [hms])
      simpa [hms] using hp

-- The plan selected by `choose` has minimum cost among all covering plans.
private theorem choose_min (f : Price) (xs : List Nat) (q : List Extent)
    (hq : q ∈ covers (points xs)) : cost f (choose f xs) ≤ cost f q := by
  unfold choose
  let r : List Extent → List Extent → Bool := fun p q => decide (cost f p ≤ cost f q)
  change cost f (((covers (points xs)).mergeSort r).head?.getD []) ≤ cost f q
  have htrans : ∀ p q s : List Extent, r p q = true → r q s = true → r p s = true := by
    intro p q s hpq hqs
    simp [r] at hpq hqs ⊢
    exact Nat.le_trans hpq hqs
  have htotal : ∀ p q : List Extent, (r p q || r q p) = true := by
    intro p q
    by_cases h : cost f p ≤ cost f q
    · simp [r, h]
    · have h' : cost f q ≤ cost f p := Nat.le_of_not_ge h
      simp [r, h, h']
  have hpair := List.pairwise_mergeSort htrans htotal (covers (points xs))
  have hperm := List.mergeSort_perm (covers (points xs)) r
  have hne : (covers (points xs)).mergeSort r ≠ [] := by
    intro hnil
    have hlen := hperm.length_eq
    simp [hnil] at hlen
    exact covers_nonempty (points xs) (List.length_eq_zero_iff.mp hlen.symm)
  cases hms : (covers (points xs)).mergeSort r with
  | nil => contradiction
  | cons p tl =>
      rw [hms] at hpair
      have hqsorted : q ∈ p :: tl := by
        simpa [hms] using (hperm.mem_iff.mpr hq)
      simp only [List.mem_cons] at hqsorted
      rcases hqsorted with hpq | hqtl
      · subst q
        simp
      · have hp_le_q := List.rel_of_pairwise_cons hpair hqtl
        simp [r] at hp_le_q
        simpa [hms] using hp_le_q

-- A higher per-byte coefficient cannot increase the total number of bytes
-- read by the optimizer's plan: both plans minimize their own cost, and
-- adding the two optimality inequalities isolates the coefficient difference.
theorem sound_ReadAmplification : ReadAmplification.Goal := by
  intro fee xs a b hab
  let fa : Price := fun l r => a * span l r + price fee l r
  let fb : Price := fun l r => b * span l r + price fee l r
  let Pa := choose fa xs
  let Pb := choose fb xs
  have hPa : Pa ∈ covers (points xs) := by simpa [Pa, fa] using choose_mem fa xs
  have hPb : Pb ∈ covers (points xs) := by simpa [Pb, fb] using choose_mem fb xs
  have h1 : cost fa Pa ≤ cost fa Pb := by
    dsimp [Pa, Pb]
    exact choose_min fa xs Pb hPb
  have h2 : cost fb Pb ≤ cost fb Pa := by
    dsimp [Pa, Pb]
    exact choose_min fb xs Pa hPa
  simp only [fa, fb] at h1 h2
  rw [cost_linear fee a Pa, cost_linear fee a Pb] at h1
  rw [cost_linear fee b Pb, cost_linear fee b Pa] at h2
  have h3 : (a + 1) * used Pa + (b + 1) * used Pb ≤
      (a + 1) * used Pb + (b + 1) * used Pa := by
    omega
  have hb : b + 1 = (a + 1) + (b - a) := by omega
  rw [hb] at h3
  simp only [Nat.add_mul] at h3
  have h4 : (b - a) * used Pb ≤ (b - a) * used Pa := by
    omega
  have hd : 0 < b - a := Nat.sub_pos_of_lt hab
  exact Nat.le_of_mul_le_mul_left h4 hd

#eval probe

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_ReadAmplification
