import TinylambOpt.ResearchGoals.Frontiers.SparseRecovery

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A04

open TinylambOpt.ResearchGoals.Frontiers.SparseRecovery

/-!
Per-goal notes:

* `UniformBucket`: true. The identity `Σ_x (n*x - Σxs)^2 = n*(n*Σx² - (Σx)²)`
  reduces the hypothesis to a sum of nonnegative integer squares equal to zero,
  hence every element equals `Σxs / n`.
* `ThreeFromTwo`: false. `[0,4,5]` and `[1,2,6]` have equal sums (9) and equal
  sums of squares (41), but only the first contains 0.
* `IsolatedHash`: true. The isolating hash's bucket is exactly `xs.count key`;
  every other hash's bucket contains all copies of `key`, so it is at least that
  large. Thus the minimum is attained at the isolating hash.
* `PeelingVariance`: true. The peeled-out moments are exactly the moments of
  `live`, so the same zero-variance lemma applies to `live`.
* `DisjointResultant`: true. An integral product vanishes iff one factor does;
  `poly a z = 0` iff `a` contains `z`.
-/

private theorem int_sum_map (xs : List Nat) (f : Nat → Nat) :
    ((xs.map f).sum : Int) = (xs.map (fun x => (f x : Int))).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem cast_sum_id (xs : List Nat) :
    (xs.map (fun (x : Nat) => (x : Int))).sum = (xs.sum : Int) := by
  simpa using (int_sum_map xs id).symm

private theorem scaled_sq_identity (xs : List Nat) (N M : Int) :
    (xs.map (fun (x : Nat) => (N * (x : Int) - M) * (N * (x : Int) - M))).sum =
      N * N * (xs.map (fun (x : Nat) => (x : Int) * (x : Int))).sum -
        N * M * (xs.map (fun (x : Nat) => (x : Int))).sum -
        M * N * (xs.map (fun (x : Nat) => (x : Int))).sum + (xs.length : Int) * M * M := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih]
      simp only [Int.mul_add, Int.sub_eq_add_neg, Int.mul_neg, Int.add_assoc,
        Int.add_left_comm, Int.add_comm, Int.mul_comm, Int.mul_left_comm, Int.mul_assoc,
        Int.mul_one]
      omega

private theorem variance_identity (xs : List Nat) :
    (xs.map (fun (x : Nat) =>
       ((xs.length : Int) * (x : Int) - (xs.sum : Int)) *
       ((xs.length : Int) * (x : Int) - (xs.sum : Int)))).sum =
      (xs.length : Int) *
        ((xs.length : Int) * (xs.map (fun (x : Nat) => (x : Int) * (x : Int))).sum -
          (xs.sum : Int) * (xs.sum : Int)) := by
  rw [scaled_sq_identity]
  rw [cast_sum_id]
  rw [Int.mul_sub]
  simp only [Int.sub_eq_add_neg, Int.add_comm,
    Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]
  omega

private theorem sq_nonneg (y : Int) : 0 ≤ y * y := by
  rcases Int.le_total 0 y with h | h
  · exact Int.mul_nonneg h h
  · exact Int.mul_nonneg_of_nonpos_of_nonpos h h

private theorem sum_sq_nonneg_nat (xs : List Nat) (f : Nat → Int) :
    0 ≤ (xs.map (fun x => f x * f x)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp
      exact Int.add_nonneg (sq_nonneg (f x)) ih

private theorem eq_zero_of_sqsum_eq_zero (xs : List Nat) (f : Nat → Int)
    (h : (xs.map (fun x => f x * f x)).sum = 0) : ∀ x ∈ xs, f x = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp at h
      intro y hymem
      simp at hymem
      rcases hymem with hyx | hymem
      · subst y
        have hs : 0 ≤ (xs.map (fun y => f y * f y)).sum := sum_sq_nonneg_nat xs f
        have hn : 0 ≤ f x * f x := sq_nonneg (f x)
        have hle : f x * f x ≤ 0 := by omega
        have hzero : f x * f x = 0 := by omega
        exact (Int.mul_eq_zero.mp hzero).elim id id
      · have hsum : (xs.map (fun y => f y * f y)).sum = 0 := by
          have hs : 0 ≤ (xs.map (fun y => f y * f y)).sum := sum_sq_nonneg_nat xs f
          have hn : 0 ≤ f x * f x := sq_nonneg (f x)
          omega
        exact ih hsum y hymem

private theorem uniform_of_zero_variance (xs : List Nat) (hne : xs ≠ [])
    (h : xs.length * moment xs 2 = moment xs 1 ^ 2) :
    xs = List.replicate xs.length (moment xs 1 / xs.length) := by
  have hlenpos : 0 < xs.length := by
    cases xs with
    | nil => contradiction
    | cons x xs => simp
  have h' : xs.length * moment xs 2 = moment xs 1 * moment xs 1 := by
    simpa [Nat.pow_two] using h
  have hcast : (xs.length : Int) * (moment xs 2 : Int) =
      (moment xs 1 : Int) * (moment xs 1 : Int) := by
    exact_mod_cast h'
  have hm1 : (moment xs 1 : Int) = (xs.sum : Int) := by
    simp [moment]
  have hm2 : (moment xs 2 : Int) =
      (xs.map (fun (x : Nat) => (x : Int) * (x : Int))).sum := by
    simpa [moment, Nat.pow_two] using int_sum_map xs (fun (x : Nat) => x * x)
  rw [hm1, hm2] at hcast
  have hv : (xs.length : Int) *
      ((xs.length : Int) * (xs.map (fun (x : Nat) => (x : Int) * (x : Int))).sum -
        (xs.sum : Int) * (xs.sum : Int)) = 0 := by
    rw [Int.mul_sub]
    rw [← hcast]
    omega
  have hdevsum : (xs.map (fun (x : Nat) =>
       ((xs.length : Int) * (x : Int) - (xs.sum : Int)) *
       ((xs.length : Int) * (x : Int) - (xs.sum : Int)))).sum = 0 := by
    rw [variance_identity]
    exact hv
  apply List.eq_replicate_of_mem
  intro x hxmem
  have hdev := eq_zero_of_sqsum_eq_zero xs
      (fun (x : Nat) => (xs.length : Int) * (x : Int) - (xs.sum : Int)) hdevsum x hxmem
  have hnat : xs.length * x = xs.sum := by
    exact_mod_cast (show (xs.length : Int) * (x : Int) = (xs.sum : Int) by omega)
  simp [moment]
  rw [← hnat]
  exact (Nat.mul_div_right x hlenpos).symm

theorem UniformBucket_sound : UniformBucket.Goal := by
  intro xs hne h
  exact uniform_of_zero_variance xs hne h

theorem ThreeFromTwo_disproof : ¬ ThreeFromTwo.Goal := by
  intro h
  have hc := h [0, 4, 5] [1, 2, 6] (by decide) (by decide) (by decide) (by decide) 0
  simp at hc

private theorem count_le_bucket (xs : List Nat) (h' : Nat → Nat) (key : Nat) :
    xs.count key ≤ buckets xs h' key := by
  unfold buckets
  rw [List.count_eq_countP]
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.countP_cons, List.countP_cons]
      by_cases hx : x = key
      · have hh : (h' x == h' key) = true := by simp [hx]
        simp [hx]
        omega
      · by_cases hh : (h' x == h' key) = true
        · simp [hx, hh]
          omega
        · simp [hx, hh]
          omega

private theorem isolated_bucket_eq (xs : List Nat) (h : Nat → Nat) (key : Nat)
    (hiso : ∀ x ∈ xs, h x = h key → x = key) :
    buckets xs h key = xs.count key := by
  unfold buckets
  rw [List.count_eq_countP]
  apply List.countP_congr
  intro x hxmem
  constructor
  · intro hb
    have heq : h x = h key := by simpa using hb
    have hxkey : x = key := hiso x hxmem heq
    simp [hxkey]
  · intro hb
    have hxkey : x = key := by simpa using hb
    simp [hxkey]

theorem IsolatedHash_sound : IsolatedHash.Goal := by
  intro xs hashes key hex
  rcases hex with ⟨h, hmem, hiso⟩
  rw [List.min?_eq_some_iff]
  constructor
  · apply List.mem_map.mpr
    exact ⟨h, hmem, isolated_bucket_eq xs h key hiso⟩
  · intro b hbmem
    rcases List.mem_map.mp hbmem with ⟨h', h'mem, rfl⟩
    exact count_le_bucket xs h' key

theorem PeelingVariance_sound : PeelingVariance.Goal := by
  intro known live hlive
  dsimp
  intro hvar
  have hlen : (known ++ live).length - known.length = live.length := by simp
  have hm1 : moment (known ++ live) 1 - moment known 1 = moment live 1 := by
    simp [moment]
  have hm2 : moment (known ++ live) 2 - moment known 2 = moment live 2 := by
    simp [moment, Nat.pow_two]
  have hv : live.length * moment live 2 = moment live 1 ^ 2 := by
    simpa [hlen, hm1, hm2, Nat.pow_two] using hvar
  have hrep := uniform_of_zero_variance live hlive hv
  intro x hx
  rw [hrep] at hx
  have hxeq := (List.mem_replicate.mp hx).2
  simpa [hlen, hm1] using hxeq

private theorem list_prod_eq_zero_iff (xs : List Int) : xs.prod = 0 ↔ ∃ x ∈ xs, x = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.prod_cons, Int.mul_eq_zero, ih]
      constructor
      · intro h
        rcases h with hx | ⟨y, hymem, hy⟩
        · exact ⟨x, by simp, hx⟩
        · exact ⟨y, by simp [hymem], hy⟩
      · rintro ⟨y, hymem, hy⟩
        simp at hymem
        rcases hymem with rfl | hymem
        · exact Or.inl hy
        · exact Or.inr ⟨y, hymem, hy⟩

private theorem poly_eq_zero_iff (a : List Nat) (z : Nat) :
    poly a z = 0 ↔ ∃ x ∈ a, x = z := by
  induction a with
  | nil => simp [poly]
  | cons x xs ih =>
      change ((z : Int) - (x : Int)) * poly xs z = 0 ↔ ∃ y ∈ x :: xs, y = z
      rw [Int.mul_eq_zero, ih]
      constructor
      · intro h
        rcases h with hz | ⟨y, hymem, hy⟩
        · exact ⟨x, by simp, by omega⟩
        · exact ⟨y, by simp [hymem], hy⟩
      · rintro ⟨y, hymem, hy⟩
        simp at hymem
        rcases hymem with rfl | hymem
        · exact Or.inl (by omega)
        · exact Or.inr ⟨y, hymem, hy⟩

theorem DisjointResultant_sound : DisjointResultant.Goal := by
  intro a b
  constructor
  · intro hprod
    rcases (list_prod_eq_zero_iff (b.map (fun (x : Nat) => poly a x))).mp hprod with
      ⟨p, hpmem, hpzero⟩
    rcases List.mem_map.mp hpmem with ⟨z, hzb, hzp⟩
    have hzpoly : poly a z = 0 := by omega
    rcases (poly_eq_zero_iff a z).mp hzpoly with ⟨x, hxa, hxz⟩
    exact ⟨x, hxa, by simpa [hxz] using hzb⟩
  · rintro ⟨x, hxa, hxb⟩
    apply (list_prod_eq_zero_iff (b.map (fun (y : Nat) => poly a y))).mpr
    exact ⟨poly a x, List.mem_map_of_mem hxb, (poly_eq_zero_iff a x).mpr ⟨x, hxa, rfl⟩⟩

end TinylambOpt.ResearchProofs.NewGoalsBatch.A04
