import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_StripedWorkers_ReadAmplification

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.StripedWorkers

-- Brute-force truth probe on small inputs (evaluates to `true`).
def probe : Bool :=
  (List.range 4).all (fun stripe =>
  (List.range 4).all (fun workers =>
  (List.range 7).all (fun a =>
  (List.range 7).all (fun b =>
    a < b →
  ([[0], [1], [0,1], [0,4,4,20], [3,1,7,2,9], [0,0,0], [5,2,5,2,5],
    [9,1,3,7,2,8,4], [0,2], [0,1,2], [0,1,2,3], [2,0,2,0], [0,5],
    [1,6], [2,7], [1,1,2,2], [8,3,8,3]] : List (List Nat)).all (fun xs =>
    used (choose (fun l r => b * span l r + price stripe workers l r) xs) ≤
      used (choose (fun l r => a * span l r + price stripe workers l r) xs))))))

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

-- For the striped-worker model an extent costs its span plus a bounded
-- activation charge, so a weighted plan's cost splits into a term linear in
-- `used` and a plan-dependent activation total.
private theorem cost_decomp (c stripe workers : Nat) (ps : List Extent) :
    cost (fun l r => c * span l r + price stripe workers l r) ps =
      (c + 1) * used ps +
        (ps.map (fun p => min (workers + 1) (pages (stripe + 1) p.1 p.2))).sum := by
  induction ps with
  | nil => simp [cost, used]
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      have hstep : c * span a b + price stripe workers a b =
          (c + 1) * span a b + min (workers + 1) (pages (stripe + 1) a b) := by
        unfold price
        rw [Nat.add_mul]
        simp only [Nat.one_mul]
        ac_rfl
      change (c * span a b + price stripe workers a b) +
          cost (fun l r => c * span l r + price stripe workers l r) ps =
        (c + 1) * (span a b + used ps) +
          (min (workers + 1) (pages (stripe + 1) a b) +
            (ps.map (fun p => min (workers + 1) (pages (stripe + 1) p.1 p.2))).sum)
      rw [hstep, ih, Nat.mul_add]
      ac_rfl

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

-- Raising the per-cell charge cannot increase the optimizer's total overread:
-- add the two optimality inequalities; the activation totals cancel out.
theorem sound_ReadAmplification : ReadAmplification.Goal := by
  intro stripe workers xs a b hab
  let fa : Price := fun l r => a * span l r + price stripe workers l r
  let fb : Price := fun l r => b * span l r + price stripe workers l r
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
  rw [cost_decomp a stripe workers Pa, cost_decomp a stripe workers Pb] at h1
  rw [cost_decomp b stripe workers Pb, cost_decomp b stripe workers Pa] at h2
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_StripedWorkers_ReadAmplification
