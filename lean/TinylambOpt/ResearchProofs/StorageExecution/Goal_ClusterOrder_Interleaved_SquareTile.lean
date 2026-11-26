import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_SquareTile

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Interleaved
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem testBit_sum_two_pow {a b j : Nat} (hab : a < b) :
    (2^a + 2^b).testBit j = (decide (a=j) || decide (b=j)) := by
  have hpow : 2^a < 2^b := Nat.pow_lt_pow_right (by omega) hab
  rw [show 2^a + 2^b = 2^b * 1 + 2^a by omega]
  rw [Nat.testBit_two_pow_mul_add 1 hpow j]
  by_cases hj : j < b
  · simp [hj, Nat.testBit_two_pow]
    have hbj : ¬ b = j := by omega
    simp [hbj]
  · have hge : b ≤ j := by omega
    simp [hj]
    rw [show (1:Nat).testBit (j-b) = decide (0 = j-b) by simpa using (Nat.testBit_two_pow (n:=0) (m:=j-b))]
    by_cases hjb : j = b
    · subst hjb; simp
    · have hgt : b + 1 ≤ j := by omega
      have haj : a ≠ j := by omega
      have h0 : 0 ≠ j-b := by omega
      have hbj : ¬ b = j := by omega
      simp [haj, h0, hbj]

private theorem testBit_add_of_and_eq_zero : ∀ (x y i : Nat), x &&& y = 0 →
    (x + y).testBit i = (x.testBit i || y.testBit i) := by
  intro x y i hxy
  induction i generalizing x y with
  | zero =>
      have hb := congrArg (fun n : Nat => n.testBit 0) hxy
      simp [Nat.testBit_zero] at hb
      have hx01 : x % 2 = 0 ∨ x % 2 = 1 := by have := Nat.mod_lt x (by omega : 0 < 2); omega
      have hy01 : y % 2 = 0 ∨ y % 2 = 1 := by have := Nat.mod_lt y (by omega : 0 < 2); omega
      rcases hx01 with hx0 | hx1 <;> rcases hy01 with hy0 | hy1 <;>
        simp_all [Nat.testBit_zero, Nat.add_mod] <;> omega
  | succ i ih =>
      have hhalf : x/2 &&& y/2 = 0 := by
        apply Nat.eq_of_testBit_eq
        intro k
        rw [Nat.testBit_and]
        have hb := congrArg (fun n : Nat => n.testBit (k+1)) hxy
        rw [Nat.testBit_and] at hb
        simpa [Nat.testBit_add_one] using hb
      have hbit0 : ¬ (2 ≤ x % 2 + y % 2) := by
        have hb := congrArg (fun n : Nat => n.testBit 0) hxy
        simp [Nat.testBit_zero] at hb
        have hx01 : x % 2 = 0 ∨ x % 2 = 1 := by have := Nat.mod_lt x (by omega : 0 < 2); omega
        have hy01 : y % 2 = 0 ∨ y % 2 = 1 := by have := Nat.mod_lt y (by omega : 0 < 2); omega
        rcases hx01 with hx0 | hx1 <;> rcases hy01 with hy0 | hy1 <;> simp_all
      have hdiv2 : (x + y) / 2 = x / 2 + y / 2 := by
        rw [Nat.add_div (by omega : 0 < 2)]
        simp [hbit0]
      rw [Nat.testBit_add_one, hdiv2, Nat.testBit_add_one, Nat.testBit_add_one]
      exact ih (x/2) (y/2) hhalf

private theorem or_eq_add_of_and_eq_zero {x y : Nat} (h : x &&& y = 0) : x ||| y = x + y := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_or]
  exact (testBit_add_of_and_eq_zero x y i h).symm

private theorem sum_testBit_of_disjoint (f : Nat → Nat) : ∀ (l : List Nat), l.Nodup →
    (∀ a ∈ l, ∀ b ∈ l, a ≠ b → f a &&& f b = 0) → ∀ j,
    ((l.map f).sum).testBit j = (l.any (fun i => (f i).testBit j)) := by
  intro l
  induction l with
  | nil => simp
  | cons a as ih =>
      intro hnd hd j
      have hnot : a ∉ as := (List.nodup_cons.mp hnd).1
      have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
      rw [List.map_cons, List.sum_cons, List.any_cons]
      have hdisj : f a &&& ((as.map f).sum) = 0 := by
        apply Nat.eq_of_testBit_eq
        intro k
        rw [Nat.testBit_and]
        have hsum_as : ((as.map f).sum).testBit k = (as.any (fun i => (f i).testBit k)) :=
          ih hndas (fun x hx y hy hne => hd x (by simp [hx]) y (by simp [hy]) hne) k
        rw [hsum_as]
        by_cases hfa : (f a).testBit k
        · have hanyfalse : (as.any (fun i => (f i).testBit k)) = false := by
            rw [List.any_eq_false]
            intro b hb hfb
            have hdab := hd a (by simp) b (by simp [hb]) (by intro heq; subst b; exact hnot hb)
            have hb0 := congrArg (fun n : Nat => n.testBit k) hdab
            rw [Nat.testBit_and] at hb0
            simp [hfa] at hb0
            simpa [hb0] using hfb
          simp [hfa, hanyfalse]
        · simp [hfa]
      rw [← or_eq_add_of_and_eq_zero hdisj, Nat.testBit_or]
      congr 1
      exact ih hndas (fun x hx y hy hne => hd x (by simp [hx]) y (by simp [hy]) hne) j

private theorem term_testBit (x y i j : Nat) :
    ((if x.testBit i then 2^(2*i) else 0) + (if y.testBit i then 2^(2*i+1) else 0)).testBit j =
      (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false) := by
  have hxf : x.testBit i = false ∨ x.testBit i = true := by cases h : x.testBit i <;> simp [h]
  have hyf : y.testBit i = false ∨ y.testBit i = true := by cases h : y.testBit i <;> simp [h]
  rcases hxf with hx | hx <;> rcases hyf with hy | hy
  · simp [hx, hy]
  · simp [hx, hy, Nat.testBit_two_pow] <;> omega
  · simp [hx, hy, Nat.testBit_two_pow]
  · have ht := testBit_sum_two_pow (a:=2*i) (b:=2*i+1) (j:=j) (by omega)
    simp [hx, hy, ht]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  unfold width
  by_cases h : n == 0
  · simp at h; subst h; decide
  · simpa [h] using (Nat.lt_log2_self : n < 2 ^ (n.log2 + 1))

private theorem morton_testBit (x y j : Nat) :
    (morton x y).testBit j = (if j % 2 == 0 then x.testBit (j / 2) else y.testBit (j / 2)) := by
  unfold morton
  let m := max (width x) (width y)
  let f := fun i => (if x.testBit i then 2^(2*i) else 0) + (if y.testBit i then 2^(2*i+1) else 0)
  have hd : ∀ a ∈ List.range m, ∀ b ∈ List.range m, a ≠ b → f a &&& f b = 0 := by
    intro a ha b hb hab
    apply Nat.eq_of_testBit_eq
    intro k
    rw [Nat.testBit_and]
    have hta := term_testBit x y a k
    have htb := term_testBit x y b k
    dsimp [f] at hta htb ⊢
    rw [hta, htb]
    by_cases h2a : 2*a = k
    · have h2a1 : ¬ 2*a+1 = k := by omega
      have h2b : ¬ 2*b = k := by omega
      have h2b1 : ¬ 2*b+1 = k := by omega
      simp [h2a, h2a1, h2b, h2b1]
    · by_cases h2a1 : 2*a+1 = k
      · have h2b : ¬ 2*b = k := by omega
        have h2b1 : ¬ 2*b+1 = k := by omega
        simp [h2a, h2a1, h2b, h2b1]
      · simp [h2a, h2a1]
  have hsum : ((List.range m).map f).sum.testBit j = (List.range m).any (fun i => (f i).testBit j) :=
    sum_testBit_of_disjoint f (List.range m) List.nodup_range hd j
  rw [hsum]
  have htermeach : ∀ i, (f i).testBit j =
      (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false) := by
    intro i
    dsimp [f]
    exact term_testBit x y i j
  simp only [htermeach]
  by_cases hj : j % 2 == 0
  · have hjmod : j % 2 = 0 := by simpa using hj
    have hje : j = 2*(j/2) := by omega
    by_cases hlt : j/2 < m
    · apply Bool.eq_iff_iff.mpr
      rw [List.any_eq_true]
      constructor
      · intro hex
        rcases hex with ⟨i, hi, hval⟩
        have hi_lt : i < m := by simpa using hi
        by_cases h2i : 2*i = j
        · have hi_eq : i = j/2 := by omega
          subst hi_eq
          simpa [hjmod, h2i] using hval
        · have h2i1 : ¬ 2*i+1 = j := by omega
          have hfalse : (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false) = false := by
            simp [h2i, h2i1]
          rw [hfalse] at hval
          contradiction
      · intro hxtrue
        have hxtrue' : x.testBit (j/2) = true := by simpa [hjmod] using hxtrue
        refine ⟨j/2, ?_, ?_⟩
        · simpa using hlt
        · have h2odd : ¬ 2*(j/2)+1 = j := by omega
          have h2even : 2*(j/2) = j := by omega
          simp [h2even, h2odd, hxtrue']
    · have hx0 : x.testBit (j/2) = false := Nat.testBit_lt_two_pow (by
        calc x < 2 ^ width x := lt_two_pow_width x
          _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_left _ _)
          _ ≤ 2 ^ (j/2) := Nat.pow_le_pow_right (by omega) (by omega))
      have hanyfalse : (List.range m).any (fun i =>
          (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false)) = false := by
        rw [List.any_eq_false]
        intro i hi hval
        have hi' : i < m := by simpa using hi
        have h2i : ¬ 2*i = j := by omega
        have h2i1 : ¬ 2*i+1 = j := by omega
        have hfalse : (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false) = false := by
          simp [h2i, h2i1]
        rw [hfalse] at hval
        contradiction
      rw [hanyfalse, hx0]
      simp [hj]
  · have hjmod : j % 2 ≠ 0 := by
      intro h; apply hj; simp [h]
    have hjo : j = 2*(j/2)+1 := by omega
    by_cases hlt : j/2 < m
    · apply Bool.eq_iff_iff.mpr
      rw [List.any_eq_true]
      constructor
      · intro hex
        rcases hex with ⟨i, hi, hval⟩
        have hi_lt : i < m := by simpa using hi
        by_cases h2i1 : 2*i+1 = j
        · have hi_eq : i = j/2 := by omega
          subst hi_eq
          have h2even : ¬ 2*(j/2) = j := by omega
          simpa [hjmod, h2i1, h2even] using hval
        · have h2i : ¬ 2*i = j := by omega
          have hfalse : (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false) = false := by
            simp [h2i, h2i1]
          rw [hfalse] at hval
          contradiction
      · intro hytrue
        have hytrue' : y.testBit (j/2) = true := by simpa [hjmod] using hytrue
        refine ⟨j/2, ?_, ?_⟩
        · simpa using hlt
        · have h2even : ¬ 2*(j/2) = j := by omega
          have h2oddEq : 2*(j/2)+1 = j := by omega
          simp [h2even, h2oddEq, hytrue']
    · have hy0 : y.testBit (j/2) = false := Nat.testBit_lt_two_pow (by
        calc y < 2 ^ width y := lt_two_pow_width y
          _ ≤ 2 ^ m := Nat.pow_le_pow_right (by omega) (Nat.le_max_right _ _)
          _ ≤ 2 ^ (j/2) := Nat.pow_le_pow_right (by omega) (by omega))
      have hanyfalse : (List.range m).any (fun i =>
          (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false)) = false := by
        rw [List.any_eq_false]
        intro i hi hval
        have hi' : i < m := by simpa using hi
        have h2i : ¬ 2*i = j := by omega
        have h2i1 : ¬ 2*i+1 = j := by omega
        have hfalse : (if 2*i == j then x.testBit i else if 2*i+1 == j then y.testBit i else false) = false := by
          simp [h2i, h2i1]
        rw [hfalse] at hval
        contradiction
      rw [hanyfalse, hy0]
      simp [hj]

private theorem morton_div_pow (x y h : Nat) : morton x y / 4 ^ h = morton (x / 2 ^ h) (y / 2 ^ h) := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [show 4 ^ h = 2 ^ (2*h) by rw [show (4:Nat) = 2^2 by rfl, Nat.pow_mul]]
  rw [Nat.testBit_div_two_pow]
  rw [morton_testBit, morton_testBit]
  simp only [Nat.testBit_div_two_pow]
  by_cases hi : i % 2 == 0
  · have himod : i % 2 = 0 := by simpa using hi
    have hpar0 : (i + 2*h) % 2 = 0 := by omega
    have hpar : (i + 2*h) % 2 == 0 := by simpa using hpar0
    have hdiv1 : (i + 2*h) / 2 = i/2 + h := by omega
    simp [hi, hpar, hdiv1]
  · have himod : i % 2 ≠ 0 := by intro hz; apply hi; simpa [hz]
    have hpar0 : (i + 2*h) % 2 ≠ 0 := by omega
    have hpar : ¬ (i + 2*h) % 2 == 0 := by intro hz; apply hpar0; simpa using hz
    have hdiv1 : (i + 2*h) / 2 = i/2 + h := by omega
    simp [hi, hpar, hdiv1]

private theorem morton_inj : Function.Injective (fun (p : Nat × Nat) => morton p.1 p.2) := by
  intro p q heq
  ext
  · apply Nat.eq_of_testBit_eq
    intro i
    have hb := congrArg (fun n : Nat => n.testBit (2*i)) heq
    rw [morton_testBit, morton_testBit] at hb
    have hpar0 : (2*i) % 2 = 0 := by omega
    have hpar : (2*i) % 2 == 0 := by simpa using hpar0
    have hdiv : (2*i)/2 = i := by omega
    simpa [hpar, hdiv] using hb
  · apply Nat.eq_of_testBit_eq
    intro i
    have hb := congrArg (fun n : Nat => n.testBit (2*i+1)) heq
    rw [morton_testBit, morton_testBit] at hb
    have hpar0 : (2*i+1) % 2 = 1 := by omega
    have hpar : ¬ (2*i+1) % 2 == 0 := by
      intro hz
      have hz0 : (2*i+1) % 2 = 0 := by simpa using hz
      omega
    have hdiv : (2*i+1)/2 = i := by omega
    simpa [hpar, hdiv] using hb

theorem sound_SquareTile : Interleaved.SquareTile.Goal := by
  intro h x y bx by_
  simp only [Interleaved.key]
  rw [morton_div_pow]
  constructor
  · intro heq
    have heq' : (fun p : Nat × Nat => morton p.1 p.2) (x / 2 ^ h, y / 2 ^ h) =
        (fun p : Nat × Nat => morton p.1 p.2) (bx, by_) := by simpa using heq
    have hinj := morton_inj heq'
    simpa using hinj
  · intro hpair
    rcases hpair with ⟨h1, h2⟩
    rw [h1, h2]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_SquareTile
