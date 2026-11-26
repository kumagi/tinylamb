import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `DyadicTile.TileBijection` — `∀ x y a b, key (x,y)=key (a,b) ↔ x=a ∧ y=b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_TileBijection

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.DyadicTile
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · unfold width
    simp [hn]
    exact Nat.lt_log2_self

private theorem testBit_add_of_and_eq_zero {x y : Nat} (h : x &&& y = 0) (j : Nat) :
    (x + y).testBit j = (x.testBit j || y.testBit j) := by
  induction j generalizing x y with
  | zero =>
      rw [Nat.testBit_zero, Nat.testBit_zero, Nat.testBit_zero]
      have hxmod : x % 2 < 2 := Nat.mod_lt _ (by omega)
      have hymod : y % 2 < 2 := Nat.mod_lt _ (by omega)
      have hnot : ¬ (x % 2 = 1 ∧ y % 2 = 1) := by
        intro hp
        have handmod : (x &&& y) % 2 = 1 := by
          rw [Nat.and_mod_two_eq_one]
          exact hp
        rw [h] at handmod
        simp at handmod
      have hsummod : (x + y) % 2 = x % 2 + y % 2 := by
        have hxdm := Nat.div_add_mod x 2
        have hydm := Nat.div_add_mod y 2
        omega
      rw [hsummod]
      by_cases hx1 : x % 2 = 1 <;> by_cases hy1 : y % 2 = 1 <;> simp [hx1, hy1] <;> omega
  | succ j ih =>
      rw [Nat.testBit_succ, Nat.testBit_succ, Nat.testBit_succ]
      have hxmod : x % 2 < 2 := Nat.mod_lt _ (by omega)
      have hymod : y % 2 < 2 := Nat.mod_lt _ (by omega)
      have hnot : ¬ (x % 2 = 1 ∧ y % 2 = 1) := by
        intro hp
        have handmod : (x &&& y) % 2 = 1 := by
          rw [Nat.and_mod_two_eq_one]
          exact hp
        rw [h] at handmod
        simp at handmod
      have hdiv : (x + y) / 2 = x / 2 + y / 2 := by
        have hxdm := Nat.div_add_mod x 2
        have hydm := Nat.div_add_mod y 2
        omega
      rw [hdiv]
      have hand : (x / 2) &&& (y / 2) = 0 := by
        rw [← Nat.and_div_two, h]
      exact ih hand

private theorem or_eq_add_of_and_eq_zero {x y : Nat} (h : x &&& y = 0) : x ||| y = x + y := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_or]
  exact (testBit_add_of_and_eq_zero h i).symm

private theorem hterm (x y j i : Nat) :
    ((if x.testBit i then 2^(2*i) else 0) + (if y.testBit i then 2^(2*i+1) else 0)).testBit j =
      (((j == 2*i) && x.testBit i) || ((j == 2*i+1) && y.testBit i)) := by
  by_cases hx : x.testBit i <;> by_cases hy : y.testBit i <;> simp [hx, hy]
  · have hd : (2^(2*i)) &&& (2^(2*i+1)) = 0 := by
      apply Nat.eq_of_testBit_eq
      intro k
      rw [Nat.testBit_and]
      simp only [Nat.testBit_two_pow]
      by_cases h1 : 2*i = k <;> by_cases h2 : 2*i+1 = k <;> simp [h1, h2] <;> omega
    have hw : 2^(2*i) + 2^(2*i+1) = 2^(2*i) ||| 2^(2*i+1) := (or_eq_add_of_and_eq_zero hd).symm
    rw [hw, Nat.testBit_or, Nat.testBit_two_pow, Nat.testBit_two_pow]
    by_cases h1 : 2*i = j <;> by_cases h2 : 2*i+1 = j
    · subst j; simp
    · have hj2 : j ≠ 2*i+1 := by intro hj; exact h2 hj.symm
      simp [h1, h2, hj2]
    · have hj1 : j ≠ 2*i := by intro hj; exact h1 hj.symm
      simp [h1, h2, hj1]
    · have hj1 : j ≠ 2*i := by intro hj; exact h1 hj.symm
      have hj2 : j ≠ 2*i+1 := by intro hj; exact h2 hj.symm
      simp [h1, h2, hj1, hj2]
  · rw [Nat.testBit_two_pow]
    by_cases h1 : 2*i = j
    · subst j; simp
    · have hj : j ≠ 2*i := by omega
      simp [h1, hj]
  · rw [Nat.testBit_two_pow]
    by_cases h1 : 2*i+1 = j
    · subst j; simp
    · have hj : j ≠ 2*i+1 := by omega
      simp [h1, hj]

private theorem morton_testBit (x y j : Nat) :
    (morton x y).testBit j = (if j % 2 == 0 then x.testBit (j / 2) else y.testBit (j / 2)) := by
  unfold morton
  let m := max (width x) (width y)
  let f := fun i => (if x.testBit i then 2^(2*i) else 0) + (if y.testBit i then 2^(2*i+1) else 0)
  have hpair : ∀ a b : Nat, a ≠ b → f a &&& f b = 0 := by
    intro a b hne
    apply Nat.eq_of_testBit_eq
    intro k
    dsimp [f]
    rw [Nat.testBit_and, hterm x y k a, hterm x y k b]
    simp only [Nat.zero_testBit]
    have hab : a < b ∨ b < a := by omega
    rcases hab with hab | hab <;>
      by_cases h1 : k = 2*a <;> by_cases h2 : k = 2*a+1 <;>
        by_cases h3 : k = 2*b <;> by_cases h4 : k = 2*b+1 <;>
          simp [h1, h2, h3, h4] <;> omega
  have hgen : ∀ (j : Nat) (l : List Nat), l.Nodup →
        ((l.map f).sum).testBit j = (l.any (fun i => (f i).testBit j)) := by
    intro j l hnd
    induction l generalizing j with
    | nil => simp
    | cons a as ih =>
        cases hnd with
        | cons hnot htail =>
            rw [List.map_cons, List.sum_cons, List.any_cons]
            have hdisj : f a &&& ((as.map f).sum) = 0 := by
              apply Nat.eq_of_testBit_eq
              intro k
              rw [Nat.testBit_and, Nat.zero_testBit]
              rw [Bool.and_eq_false_imp]
              intro hfa
              have hsum_as : ((as.map f).sum).testBit k = (as.any (fun i => (f i).testBit k)) := ih k htail
              rw [hsum_as, ← Bool.not_eq_true, List.any_eq_true]
              simp only [not_exists, not_and]
              intro b hb hfb
              have hne : a ≠ b := hnot b hb
              have hdab := hpair a b hne
              have ht := congrArg (fun n => n.testBit k) hdab
              rw [Nat.testBit_and] at ht
              simp at ht
              have hbfalse := ht hfa
              rw [hbfalse] at hfb
              cases hfb
            rw [← or_eq_add_of_and_eq_zero hdisj, Nat.testBit_or]
            congr 1
            exact ih j htail
  have hsum : ((List.range m).map f).sum.testBit j = (List.range m).any (fun i => (f i).testBit j) :=
    hgen j (List.range m) List.nodup_range
  rw [hsum]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hany
    rw [List.any_eq_true] at hany
    have hjmod : j % 2 < 2 := Nat.mod_lt _ (by omega)
    have hjdm := Nat.div_add_mod j 2
    by_cases hj : j % 2 == 0
    · have hj0 : j % 2 = 0 := by simpa using hj
      have hje : j = 2 * (j / 2) := by omega
      simp [hj]
      obtain ⟨i, hi, ht⟩ := hany
      dsimp [f] at ht
      rw [hterm x y j i] at ht
      by_cases h1 : j = 2*i
      · have h2 : j ≠ 2*i+1 := by omega
        have hti : x.testBit i = true := by simpa [h1, h2] using ht
        have hi' : i = j/2 := by
          rw [h1]
          exact (Nat.mul_div_right i (by omega : 0 < 2)).symm
        subst i
        exact hti
      · by_cases h2 : j = 2*i+1
        · omega
        · simp [h1, h2] at ht
    · have hjne : j % 2 ≠ 0 := by simpa using hj
      have hj1mod : j % 2 = 1 := by omega
      have hjo : j = 2 * (j / 2) + 1 := by omega
      simp [hj]
      obtain ⟨i, hi, ht⟩ := hany
      dsimp [f] at ht
      rw [hterm x y j i] at ht
      by_cases h1 : j = 2*i
        -- impossible when j is odd
      · omega
      · by_cases h2 : j = 2*i+1
        · have hti : y.testBit i = true := by simpa [h1, h2] using ht
          have hi' : i = j/2 := by
            rw [h2]
            exact (Nat.div_eq_of_lt_le (by omega) (by omega)).symm
          subst i
          exact hti
        · simp [h1, h2] at ht
  · intro hif
    rw [List.any_eq_true]
    have hjmod : j % 2 < 2 := Nat.mod_lt _ (by omega)
    have hjdm := Nat.div_add_mod j 2
    by_cases hj : j % 2 == 0
    · have hj0 : j % 2 = 0 := by simpa using hj
      have hje : j = 2 * (j / 2) := by omega
      simp [hj] at hif
      refine ⟨j/2, ?_, ?_⟩
      · by_cases hlt : j/2 < m
        · simpa using hlt
        · have hge' : m ≤ j/2 := by omega
          have hx0 : x.testBit (j/2) = false := Nat.testBit_lt_two_pow (by
            calc x < 2 ^ width x := lt_two_pow_width x
              _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_left _ _)
              _ ≤ 2 ^ (j/2) := Nat.pow_le_pow_right (by omega) hge')
          simp [hx0] at hif
      · dsimp [f]
        rw [hterm x y j (j/2)]
        have heq : (j == 2*(j/2)) = true := by simpa using hje
        have hne : (j == 2*(j/2)+1) = false := by simpa using (show j ≠ 2*(j/2)+1 by omega)
        simp [heq, hne, hif]
    · have hjne : j % 2 ≠ 0 := by simpa using hj
      have hj1mod : j % 2 = 1 := by omega
      have hjo : j = 2 * (j / 2) + 1 := by omega
      simp [hj] at hif
      refine ⟨j/2, ?_, ?_⟩
      · by_cases hlt : j/2 < m
        · simpa using hlt
        · have hge' : m ≤ j/2 := by omega
          have hy0 : y.testBit (j/2) = false := Nat.testBit_lt_two_pow (by
            calc y < 2 ^ width y := lt_two_pow_width y
              _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_right _ _)
              _ ≤ 2 ^ (j/2) := Nat.pow_le_pow_right (by omega) hge')
          simp [hy0] at hif
      · dsimp [f]
        rw [hterm x y j (j/2)]
        have hne1 : (j == 2*(j/2)) = false := by simpa using (show j ≠ 2*(j/2) by omega)
        have heq : (j == 2*(j/2)+1) = true := by simpa using hjo
        simp [hne1, heq, hif]

private theorem morton_inj : Function.Injective (fun (p : Nat × Nat) => morton p.1 p.2) := by
  intro p q h
  ext
  · apply Nat.eq_of_testBit_eq
    intro i
    have ht := congrArg (fun n => n.testBit (2*i)) h
    rw [morton_testBit, morton_testBit] at ht
    simpa using ht
  · apply Nat.eq_of_testBit_eq
    intro i
    have ht := congrArg (fun n => n.testBit (2*i+1)) h
    rw [morton_testBit, morton_testBit] at ht
    have hb : (((2*i+1) % 2 == 0) = false) := by
      have hm : (2*i+1) % 2 = 1 := by omega
      simp [hm]
    have hd : (2*i+1)/2 = i := by
      exact Nat.div_eq_of_lt_le (by omega) (by omega)
    simpa [hb, hd] using ht

theorem sound_TileBijection : DyadicTile.TileBijection.Goal := by
  intro x y a b
  simp only [DyadicTile.key]
  constructor
  · intro h
    obtain ⟨h1, h2⟩ := Prod.mk.inj h
    have hy : y % 16 = b % 16 := by omega
    have hx16 : x % 16 = a % 16 := by omega
    have hinj := @morton_inj (x/16, y/16) (a/16, b/16) h1
    simp at hinj
    obtain ⟨hxq, hyq⟩ := hinj
    have hx : x = a := by have := Nat.div_add_mod x 16; have := Nat.div_add_mod a 16; omega
    have hy' : y = b := by have := Nat.div_add_mod y 16; have := Nat.div_add_mod b 16; omega
    exact ⟨hx, hy'⟩
  · intro ⟨h1, h2⟩
    subst h1
    subst h2
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_DyadicTile_TileBijection
