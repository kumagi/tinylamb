import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_InverseLookup

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.filterMap_append]
      rw [ih (fun i hi => h i (by omega))]
      simp [h n (by omega)]

private theorem rows_map (c : Config) :
    rows layout c = (List.range c.n).map (fun (i : Nat) => (i, c.base + c.step * (i : Int))) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp [layout]

private theorem filter_map_pair_snd {α : Type} [BEq α] (xs : List Nat) (f : Nat → α) (v : α) :
    ((xs.map (fun i => (i, f i))).filter (fun r => r.2 == v)).map Prod.fst =
      xs.filter (fun i => f i == v) := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : f x == v <;> simp [h, ih]

private theorem filter_congr_bool {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem filter_range_eq (n k : Nat) :
    (List.range n).filter (fun i => i == k) = if k < n then [k] else [] := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      by_cases h : k == n
      · have h' : k = n := beq_iff_eq.mp h
        subst k
        simp
      · have h' : k ≠ n := by
          intro heq
          exact h (by rw [heq]; simp)
        have hk : k < n + 1 ↔ k < n := by omega
        have hn' : n ≠ k := by omega
        simp [hk, hn']

private theorem filter_range_eq_int (n : Nat) (j : Int) :
    (List.range n).filter (fun (i : Nat) => (i : Int) == j) =
      if 0 ≤ j && j < (n : Int) then [j.toNat] else [] := by
  by_cases hj : 0 ≤ j
  · have heq : (List.range n).filter (fun (i : Nat) => (i : Int) == j) =
        (List.range n).filter (fun (i : Nat) => i == j.toNat) := by
      apply filter_congr_bool
      intro i hi
      by_cases heq : (i : Int) = j
      · have heq' : i = j.toNat := by omega
        rw [beq_iff_eq.mpr heq, beq_iff_eq.mpr heq']
      · have heq' : i ≠ j.toNat := by
          intro h
          apply heq
          omega
        rw [beq_eq_false_iff_ne.mpr heq, beq_eq_false_iff_ne.mpr heq']
    rw [heq, filter_range_eq]
    have hlt : j.toNat < n ↔ j < (n : Int) := by
      omega
    simp [hj, hlt]
  · have hjneg : j < 0 := by omega
    induction n with
    | zero => simp [hj]
    | succ n ih =>
        rw [List.range_succ, List.filter_append, ih]
        have hn' : (n : Int) ≠ j := by omega
        have hn : ((n : Int) == j) = false := beq_eq_false_iff_ne.mpr hn'
        simp [hn, hj]

private theorem value_eq_iff (c : Config) (i : Nat) (v : Int) (hstep : c.step ≠ 0)
    (hmod : (v - c.base) % c.step = 0) :
    (c.base + c.step * (i : Int) == v) = ((i : Int) == (v - c.base) / c.step) := by
  have hjmul : c.step * ((v - c.base) / c.step) = v - c.base := by
    simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod
  by_cases hp : c.base + c.step * (i : Int) = v
  · have hmul : c.step * (i : Int) = v - c.base := by omega
    have hamul : c.step * (i : Int) = c.step * ((v - c.base) / c.step) := by omega
    have hq : (i : Int) = (v - c.base) / c.step :=
      Int.eq_of_mul_eq_mul_left hstep hamul
    rw [beq_iff_eq.mpr hp, beq_iff_eq.mpr hq]
  · have hq : (i : Int) ≠ (v - c.base) / c.step := by
      intro heq
      apply hp
      rw [heq]
      omega
    rw [beq_eq_false_iff_ne.mpr hp, beq_eq_false_iff_ne.mpr hq]

theorem sound_LinearOrdinal_InverseLookup : LinearOrdinal.InverseLookup.Goal := by
  intro c v hstep
  rw [rows_map]
  rw [filter_map_pair_snd]
  let d : Int := v - c.base
  let j : Int := d / c.step
  by_cases hmodB : d % c.step == 0
  · have hmod : d % c.step = 0 := beq_iff_eq.mp hmodB
    have hfilter : (List.range c.n).filter (fun (i : Nat) => c.base + c.step * (i : Int) == v) =
        (List.range c.n).filter (fun (i : Nat) => (i : Int) == j) := by
      apply filter_congr_bool
      intro i hi
      dsimp [j, d]
      exact value_eq_iff c i v hstep hmod
    rw [hfilter, filter_range_eq_int]
    simp [d, j, hmodB]
  · have hmodne : d % c.step ≠ 0 := by
      intro h
      exact hmodB (by rw [h]; simp)
    have hfalse : ∀ i : Nat, ¬ (c.base + c.step * (i : Int) == v) := by
      intro i hi
      have hi' : c.base + c.step * (i : Int) = v := beq_iff_eq.mp hi
      have hdvd : c.step ∣ d := by
        refine ⟨(i : Int), ?_⟩
        dsimp [d]
        omega
      exact hmodne (Int.emod_eq_zero_of_dvd hdvd)
    have hfilter : (List.range c.n).filter (fun (i : Nat) => c.base + c.step * (i : Int) == v) = [] := by
      induction c.n with
      | zero => simp
      | succ n ih =>
          rw [List.range_succ, List.filter_append, ih]
          simp [hfalse n]
    rw [hfilter]
    simp [d, hmodB]

end TinylambOpt.ResearchProofs.Atlas.Goal_LinearOrdinal_InverseLookup
