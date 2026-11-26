import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `DictionaryFault.ReadAmplification` — `∀ (group fault : Nat), readAmplification
  (price group fault)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_ReadAmplification

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.DictionaryFault

-- brute force truth probe on small inputs
def probe : Bool :=
  (List.range 4).all (fun group =>
  (List.range 4).all (fun fault =>
  (List.range 6).all (fun a =>
  (List.range 6).all (fun b =>
    a < b →
  ([[0], [1], [0,1], [0,4,4,20], [3,1,7,2,9], [0,0,0], [5,2,5,2,5], [9,1,3,7,2,8,4], [0,2], [0,1,2], [0,1,2,3], [2,0,2,0]] : List (List Nat)).all (fun xs =>
    used (choose (fun l r => b*span l r + price group fault l r) xs) ≤
      used (choose (fun l r => a*span l r + price group fault l r) xs))))))

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

-- The part of a plan's cost that does not depend on the per-cell coefficient.
private def baseCost (f : Price) (ps : List Extent) : Nat :=
  (ps.map (fun (a, b) => f a b)).sum

private theorem sum_pair_map (c : Nat) (f : Price) (ps : List Extent) :
    (ps.map (fun (a, b) => f a b + c * span a b)).sum =
      (ps.map (fun (a, b) => f a b)).sum +
        c * (ps.map (fun (a, b) => span a b)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp [Nat.mul_add, Nat.add_assoc, Nat.add_left_comm]

-- For any base price, the cost under `c * span + f` is affine in `c` with
-- slope `used ps`.  The dictionary bucket charge only enters via `baseCost`.
private theorem cost_decomp (c : Nat) (f : Price) (ps : List Extent) :
    cost (fun l r => c * span l r + f l r) ps = baseCost f ps + c * used ps := by
  unfold cost baseCost used
  simpa [Nat.add_comm] using sum_pair_map c f ps

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

-- A higher per-cell coefficient cannot increase the total number of cells
-- read by the optimizer's plan: both plans minimize their own affine cost,
-- and adding the two optimality inequalities isolates the coefficient difference.
theorem sound_ReadAmplification : ReadAmplification.Goal := by
  intro group fault xs a b hab
  let f : Price := price group fault
  let fa : Price := fun l r => a * span l r + f l r
  let fb : Price := fun l r => b * span l r + f l r
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
  rw [cost_decomp a f Pa, cost_decomp a f Pb] at h1
  rw [cost_decomp b f Pb, cost_decomp b f Pa] at h2
  have h3 : a * used Pa + b * used Pb ≤ a * used Pb + b * used Pa := by
    omega
  have hb : b = a + (b - a) := by omega
  rw [hb] at h3
  simp only [Nat.add_mul] at h3
  have h4 : (b - a) * used Pb ≤ (b - a) * used Pa := by
    omega
  have hd : 0 < b - a := Nat.sub_pos_of_lt hab
  exact Nat.le_of_mul_le_mul_left h4 hd

#eval probe

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_DictionaryFault_ReadAmplification
