import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `Sector.OneSector` — `∀ k fee xs, xs≠[] → (∀ x∈xs, x/2^k=(xs.head?.getD 0)/2^k) →
  cost (price k fee) (choose (price k fee) xs) = fee+1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_OneSector

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Sector

private theorem points_mem (xs : List Nat) (x : Nat) : x ∈ points xs ↔ x ∈ xs := by
  simp [points]

private theorem points_nonempty {xs : List Nat} (h : xs ≠ []) : points xs ≠ [] := by
  intro hp
  obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil xs h
  have hx' : x ∈ points xs := (points_mem xs x).mpr hx
  simp [hp] at hx'

private theorem head_mem_nat : ∀ (xs : List Nat), xs ≠ [] → xs.head?.getD 0 ∈ xs
  | [], h => absurd rfl h
  | x :: xs, _ => by simp

private theorem getLast_mem_nat : ∀ (xs : List Nat), xs ≠ [] → xs.getLast?.getD 0 ∈ xs
  | [], h => absurd rfl h
  | x :: xs, h => by
      cases xs with
      | nil => simp
      | cons y ys =>
          have ih := getLast_mem_nat (y :: ys) (by simp)
          simpa using List.mem_cons_of_mem x ih

private theorem covers_one_extent : ∀ (xs : List Nat), xs ≠ [] →
    [(xs.head?.getD 0, xs.getLast?.getD 0)] ∈ covers xs
  | [], h => absurd rfl h
  | x :: xs, h => by
      cases xs with
      | nil => simp [covers]
      | cons y ys =>
          have hmem : [((y :: ys).head?.getD 0, (y :: ys).getLast?.getD 0)] ∈
              covers (y :: ys) :=
            covers_one_extent (y :: ys) (by simp)
          simp only [List.head?_cons, Option.getD_some] at hmem ⊢
          have hlast_eq : (x :: y :: ys).getLast?.getD 0 =
              (y :: ys).getLast?.getD 0 := by
            simp
          rw [hlast_eq]
          rw [covers]
          apply List.mem_flatMap.mpr
          refine ⟨[(y, (y :: ys).getLast?.getD 0)], hmem, ?_⟩
          simp

private theorem covers_cost_ge (k fee : Nat) : ∀ (ys : List Nat) (ps : List Extent),
    ps ∈ covers ys → ys ≠ [] → fee + 1 ≤ cost (price k fee) ps
  | [], ps, hps, hne => absurd rfl hne
  | x :: xs, ps, hps, hne => by
      change ps ∈ (covers xs).flatMap (fun q => match q with
        | [] => [[(x, x)]]
        | (_, b) :: rest => [(x, x) :: q, (x, b) :: rest]) at hps
      rw [List.mem_flatMap] at hps
      obtain ⟨q, hq, hps⟩ := hps
      cases q with
      | nil =>
          simp at hps
          subst ps
          simp [cost, price, pages]
      | cons e rest =>
          rcases e with ⟨a, b⟩
          simp at hps
          rcases hps with hps | hps
          · subst ps
            simp [cost, price, pages]
          · subst ps
            simp [cost, price, pages]
            omega

private theorem choose_mem_covers (f : Price) (xs : List Nat)
    (hne : covers (points xs) ≠ []) : choose f xs ∈ covers (points xs) := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hperm := List.mergeSort_perm (covers (points xs)) le
  have hne' : (covers (points xs)).mergeSort le ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact hne this
  cases h : (covers (points xs)).mergeSort le with
  | nil => exact absurd h hne'
  | cons hd tl =>
      have hmem : hd ∈ (covers (points xs)).mergeSort le := by simp [h]
      rw [List.mem_mergeSort] at hmem
      simpa [le] using hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) : cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil => simp [h] at hmem'
  | cons hd tl =>
      simp only [List.head?_cons, Option.getD_some]
      have hle : le hd ps = true := by
        have hpw := hpair
        rw [h, List.pairwise_cons] at hpw
        have hmem'' : ps ∈ hd :: tl := by simpa [h] using hmem'
        rcases List.mem_cons.mp hmem'' with rfl | htail
        · simp [le]
        · exact hpw.1 ps htail
      simpa only [le, decide_eq_true_eq] using hle

theorem sound_OneSector : Sector.OneSector.Goal := by
  intro k fee xs hxs hsec
  let f := price k fee
  let ks := points xs
  have hks : ks ≠ [] := points_nonempty hxs
  have hnon : covers ks ≠ [] := by
    have hps : [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks :=
      covers_one_extent ks hks
    intro h
    simp [h] at hps
  have hlower : fee + 1 ≤ cost f (choose f xs) := by
    exact covers_cost_ge k fee ks (choose f xs)
      (choose_mem_covers f xs (by simpa [ks] using hnon)) (by simpa [ks] using hks)
  let ps1 := [(ks.head?.getD 0, ks.getLast?.getD 0)]
  have hmem1 : ps1 ∈ covers ks := by
    exact covers_one_extent ks hks
  have hheadmem : ks.head?.getD 0 ∈ xs :=
    (points_mem xs (ks.head?.getD 0)).mp (by simpa [ks] using head_mem_nat ks hks)
  have hlastmem : ks.getLast?.getD 0 ∈ xs :=
    (points_mem xs (ks.getLast?.getD 0)).mp (by simpa [ks] using getLast_mem_nat ks hks)
  have hhead : ks.head?.getD 0 / 2 ^ k = xs.head?.getD 0 / 2 ^ k := hsec _ hheadmem
  have hlast : ks.getLast?.getD 0 / 2 ^ k = xs.head?.getD 0 / 2 ^ k := hsec _ hlastmem
  have hp : pages (2 ^ k) (ks.head?.getD 0) (ks.getLast?.getD 0) = 1 := by
    unfold pages
    rw [hlast, hhead]
    omega
  have hcost1 : cost f ps1 = fee + 1 := by
    simp [ps1, cost, f, price, hp]
  have hupper : cost f (choose f xs) ≤ fee + 1 := by
    rw [← hcost1]
    exact choose_le f xs ps1 (by simpa [ks] using hmem1)
  exact Nat.le_antisymm hupper hlower

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_OneSector
