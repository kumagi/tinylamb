import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `Interleaved.MergeShards` — A stable merge of independently clustered shards recovers
  the global physical order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_MergeShards

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Interleaved
open TinylambOpt.ResearchGoals.Compression.Wire

private abbrev Rw := Nat × Nat

--------------------------------------------------------------------------------
-- Morton encoding is injective (the key-dependent part of the argument).
-- The bit lemmas below mirror the established `Interleaved.SquareTile` proof.

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

--------------------------------------------------------------------------------
-- The remainder is key-independent: total-order sortedness + permutation.
-- (Same argument as the Lexicographic MergeShards proof.)

-- The merge comparator of `mergeShards`, specialized to this key:
-- `key (x,y) = (morton x y, 0)`, so the secondary component always ties.
private def leKey (x y : Rw) : Bool :=
  morton x.1 x.2 < morton y.1 y.2 || (morton x.1 x.2 == morton y.1 y.2)

-- The comparator used by `perm` for this key (ties by original ordinal).
private def cmpKey (ri rj : Rw × Nat) : Bool :=
  morton ri.1.1 ri.1.2 < morton rj.1.1 rj.1.2 ||
    (morton ri.1.1 ri.1.2 == morton rj.1.1 rj.1.2 && ri.2 ≤ rj.2)

private theorem leKey_trans : ∀ a b c : Rw, leKey a b = true → leKey b c = true → leKey a c = true := by
  intro a b c hab hbc
  simp [leKey] at hab hbc ⊢
  omega

private theorem leKey_total : ∀ a b : Rw, (leKey a b || leKey b a) = true := by
  intro a b
  simp [leKey]
  omega

private theorem leKey_antisymm {a b : Rw} (hab : leKey a b = true) (hba : leKey b a = true) : a = b := by
  simp [leKey] at hab hba
  have hm : morton a.1 a.2 = morton b.1 b.2 := by omega
  exact morton_inj hm

private theorem cmpKey_leKey {a b : Rw × Nat} (h : cmpKey a b = true) : leKey a.1 b.1 = true := by
  simp [cmpKey, leKey] at h ⊢
  omega

private theorem cmpKey_trans (a b c : Rw × Nat) (hab : cmpKey a b = true)
    (hbc : cmpKey b c = true) : cmpKey a c = true := by
  simp [cmpKey] at hab hbc ⊢
  omega

private theorem cmpKey_total (a b : Rw × Nat) : (cmpKey a b || cmpKey b a) = true := by
  simp [cmpKey]
  omega

private theorem getD_zipIdx (rs : List Rw) (ri : Rw × Nat) (h : ri ∈ rs.zipIdx) :
    rs[ri.2]?.getD (0, 0) = ri.1 := by
  rcases ri with ⟨r, i⟩
  have hm := List.mem_zipIdx h
  simp at hm
  have hg : rs[i]? = some rs[i] := List.getElem?_eq_getElem hm.1
  rw [hg]
  simpa using hm.2.symm

private theorem map_fst_zipIdx (rs : List Rw) : (rs.zipIdx.map Prod.fst) = rs := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [List.zipIdx_cons]

private theorem physical_eq_map_fst (rs : List Rw) :
    physical key rs = (rs.zipIdx.mergeSort cmpKey).map Prod.fst := by
  unfold physical perm
  have hC : (fun (ri rj : Rw × Nat) =>
      let a := key ri.1
      let b := key rj.1
      a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && ri.2 ≤ rj.2)))) = cmpKey := by
    funext ri rj
    rcases ri with ⟨⟨x1, x2⟩, i⟩
    rcases rj with ⟨⟨y1, y2⟩, j⟩
    by_cases hlt : morton x1 x2 < morton y1 y2 <;>
      by_cases heq : morton x1 x2 = morton y1 y2 <;>
      by_cases hij : i ≤ j <;>
      simp [cmpKey, key, hlt, heq, hij, beq_iff_eq]
  rw [hC]
  simp only [List.map_map]
  apply List.map_congr_left
  intro ri hri
  exact getD_zipIdx rs ri ((List.mem_mergeSort (a := ri) (l := rs.zipIdx) (le := cmpKey)).1 hri)

private theorem physical_perm (rs : List Rw) : (physical key rs).Perm rs := by
  rw [physical_eq_map_fst]
  have h := (List.mergeSort_perm (rs.zipIdx) cmpKey).map Prod.fst
  simpa [map_fst_zipIdx] using h

private theorem physical_sorted (rs : List Rw) :
    (physical key rs).Pairwise (fun x y => leKey x y = true) := by
  rw [physical_eq_map_fst]
  have hs : (rs.zipIdx.mergeSort cmpKey).Pairwise (fun a b => leKey a.1 b.1 = true) := by
    apply (List.pairwise_mergeSort cmpKey_trans cmpKey_total (rs.zipIdx)).imp
    intro a b h
    exact cmpKey_leKey h
  exact hs.map Prod.fst (by intro a b h; exact h)

private theorem cons_append_perm (y : Rw) (xs ys : List Rw) :
    (y :: (xs ++ ys)).Perm (xs ++ y :: ys) := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      exact (List.Perm.swap a y (xs ++ ys)).trans (List.Perm.cons a ih)

private theorem merge_perm_append : ∀ (l1 l2 : List Rw), (l1.merge l2 leKey).Perm (l1 ++ l2)
  | [], l2 => by simp
  | x :: xs, [] => by simp
  | x :: xs, y :: ys => by
      by_cases h : leKey x y = true
      · simp [h]
        exact merge_perm_append xs (y :: ys)
      · simp [h]
        exact (List.Perm.cons y (merge_perm_append (x :: xs) ys)).trans
          ((List.Perm.swap x y (xs ++ ys)).trans
            (List.Perm.cons x (cons_append_perm y xs ys)))

private theorem sorted_eq_of_perm : ∀ (l1 l2 : List Rw),
    l1.Pairwise (fun x y => leKey x y = true) →
    l2.Pairwise (fun x y => leKey x y = true) →
    l1.Perm l2 → l1 = l2
  | [], [], _, _, _ => rfl
  | [], _ :: _, _, _, hp => by simp at hp
  | _ :: _, [], _, _, hp => by simp at hp
  | x :: xs, z :: zs, h1, h2, hp => by
      have hpair1 : (∀ y ∈ xs, leKey x y = true) ∧ xs.Pairwise (fun a b => leKey a b = true) := by
        simpa using h1
      have hpair2 : (∀ y ∈ zs, leKey z y = true) ∧ zs.Pairwise (fun a b => leKey a b = true) := by
        simpa using h2
      have hxmem : x ∈ z :: zs := hp.mem_iff.mp (by simp)
      have hzmem : z ∈ x :: xs := hp.symm.mem_iff.mp (by simp)
      have hzx : leKey z x = true := by
        by_cases hz : z = x
        · simp [hz, leKey]
        · have hz' : x ≠ z := fun h => hz h.symm
          exact hpair2.1 x (by simpa [hz'] using hxmem)
      have hxz : leKey x z = true := by
        by_cases hz : z = x
        · simp [hz, leKey]
        · exact hpair1.1 z (by simpa [hz] using hzmem)
      have hzx' : z = x := leKey_antisymm hzx hxz
      subst z
      exact congrArg (List.cons x) (sorted_eq_of_perm xs zs hpair1.2 hpair2.2 (List.Perm.cons_inv hp))

-- Truth check on small inputs (all `true`).
private def checkOne (a b : List Rw) : Bool :=
  List.merge (physical key a) (physical key b) leKey == physical key (a ++ b)

#eval checkOne [(2, 1), (1, 1), (2, 0)] [(1, 1), (0, 5), (2, 0)]
#eval checkOne [(0, 0), (1, 1), (1, 2)] [(1, 1), (0, 0), (2, 3)]
#eval checkOne [] [(0, 0), (0, 0)]
#eval checkOne [(5, 1), (3, 1), (9, 1)] [(4, 1), (1, 1)]
#eval checkOne [(0, 3), (4, 0), (2, 2)] [(3, 3), (1, 0), (7, 7)]

private theorem goalComparator_eq (x y : Rw) :
    (let u := key x
     let v := key y
     u.1 < v.1 || (u.1 == v.1 && u.2 ≤ v.2)) = leKey x y := by
  rcases x with ⟨x1, x2⟩
  rcases y with ⟨y1, y2⟩
  by_cases hlt : morton x1 x2 < morton y1 y2 <;>
    by_cases heq : morton x1 x2 = morton y1 y2 <;>
    simp [leKey, key, hlt, heq, beq_iff_eq]

theorem sound_MergeShards : Interleaved.MergeShards.Goal := by
  intro a b
  have hmain : List.merge (physical key a) (physical key b) leKey = physical key (a ++ b) := by
    apply sorted_eq_of_perm
    · exact List.pairwise_merge leKey_trans leKey_total (physical key a) (physical key b)
        (physical_sorted a) (physical_sorted b)
    · exact physical_sorted (a ++ b)
    · exact (merge_perm_append (physical key a) (physical key b)).trans
        ((List.Perm.append (physical_perm a) (physical_perm b)).trans (physical_perm (a ++ b)).symm)
  change List.merge (physical key a) (physical key b)
    (fun x y => let u := key x; let v := key y; u.1 < v.1 || (u.1 == v.1 && u.2 ≤ v.2)) =
    physical key (a ++ b)
  simp only [goalComparator_eq]
  exact hmain

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_MergeShards
