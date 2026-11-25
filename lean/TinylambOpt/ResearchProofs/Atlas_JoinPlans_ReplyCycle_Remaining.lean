import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReplyCycle.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle

/-! Finite-range sum helpers over `Int` (Std only, no Mathlib). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      simp [rsum]
      exact (rsum_const_zero m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem list_sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

/-! Indicator view of the support of an edge relation. -/

private def ind (db : DB) (r x y : Nat) : Int := if db r x y == 0 then 0 else 1

private theorem ind_nonneg (db : DB) (r x y : Nat) : 0 ≤ ind db r x y := by
  by_cases h : db r x y == 0 <;> simp [ind, h]

private theorem ind_le_one (db : DB) (r x y : Nat) : ind db r x y ≤ 1 := by
  by_cases h : db r x y == 0 <;> simp [ind, h]

private theorem countP_eq_rsum (n : Nat) (p : Nat → Bool) :
    ((List.range n).countP p : Int) = rsum n (fun x => if p x then 1 else 0) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      simp [List.range_succ, List.countP_cons, rsum_succ, ih]
      by_cases h : p n <;> simp [h]

private theorem list_cast_sum (l : List Nat) (f : Nat → Nat) :
    ((l.map f).sum : Int) = (l.map (fun x => (f x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem support_supp (n : Nat) (db : DB) (r : Nat) (b : Bind) :
    rsum n (fun x => rsum n (fun y => ind db r x y)) =
      (support n db ⟨r, 0, 1⟩ pattern b : Int) := by
  simp [support, pattern]
  rw [list_cast_sum]
  rw [show (fun x : Nat => ((List.range n).countP (fun y => db r x y != 0) : Int)) =
      (fun x : Nat => rsum n (fun y => ind db r x y)) by
        funext x
        rw [countP_eq_rsum]
        apply rsum_congr
        intro y hy
        by_cases h : db r x y = 0 <;> simp [ind, h]]
  rfl

private theorem reference_reply (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (ind db) b =
      rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x0 =>
        ind db 0 x0 x1 * ind db 0 x1 x2 * ind db 0 x2 x3 * ind db 0 x3 x0)))) := by
  simp [reference, integrate, pattern, value, put, rsum, Function.comp_def, Int.mul_assoc]

/-! Cardinality bound for the four-cycle. -/

private theorem rsum4_swap34 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun c => f a b c d)))) := by
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro b hb
  exact rsum_comm n n (fun c d => f a b c d)

private theorem rsum4_swap23 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun b => rsum n (fun d => f a b c d)))) := by
  apply rsum_congr
  intro a ha
  exact rsum_comm n n (fun b c => rsum n (fun d => f a b c d))

private theorem rsum4_swap12 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d)))) =
      rsum n (fun b => rsum n (fun a => rsum n (fun c => rsum n (fun d => f a b c d)))) := by
  exact rsum_comm n n (fun a b => rsum n (fun c => rsum n (fun d => f a b c d)))

private theorem ref_nonneg (n : Nat) (db : DB) (b : Bind) :
    0 ≤ reference pattern n (ind db) b := by
  rw [reference_reply]
  apply rsum_nonneg
  intro x1 hx1
  apply rsum_nonneg
  intro x2 hx2
  apply rsum_nonneg
  intro x3 hx3
  apply rsum_nonneg
  intro x0 hx0
  exact Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg
    (ind_nonneg db 0 x0 x1) (ind_nonneg db 0 x1 x2))
    (ind_nonneg db 0 x2 x3)) (ind_nonneg db 0 x3 x0)

private theorem support_any (n : Nat) (db : DB) (b : Bind) (src dst : Nat)
    (hs : src ∈ pattern.freeVars) (hd : dst ∈ pattern.freeVars) :
    support n db ⟨0, src, dst⟩ pattern b = support n db ⟨0, 0, 1⟩ pattern b := by
  have hsc : src = 1 ∨ src = 2 ∨ src = 3 ∨ src = 0 := by simpa [pattern] using hs
  have hdc : dst = 1 ∨ dst = 2 ∨ dst = 3 ∨ dst = 0 := by simpa [pattern] using hd
  simp [support, pattern, hsc, hdc]

private theorem support_supp_atom (n : Nat) (db : DB) (b : Bind) (src dst : Nat)
    (hs : src ∈ pattern.freeVars) (hd : dst ∈ pattern.freeVars) :
    rsum n (fun x => rsum n (fun y => ind db 0 x y)) =
      (support n db ⟨0, src, dst⟩ pattern b : Int) := by
  rw [support_any n db b src dst hs hd]
  exact support_supp n db 0 b

private theorem ref_le_supp02 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (ind db) b ≤
      (support n db ⟨0, 0, 1⟩ pattern b : Int) * (support n db ⟨0, 2, 3⟩ pattern b : Int) := by
  rw [reference_reply]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x0 =>
        ind db 0 x0 x1 * ind db 0 x1 x2 * ind db 0 x2 x3 * ind db 0 x3 x0))))
      ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x0 =>
          ind db 0 x0 x1 * ind db 0 x2 x3)))) := by
            apply rsum_le
            intro x1 hx1
            apply rsum_le
            intro x2 hx2
            apply rsum_le
            intro x3 hx3
            apply rsum_le
            intro x0 hx0
            let A := ind db 0 x0 x1
            let B := ind db 0 x1 x2
            let C := ind db 0 x2 x3
            let D := ind db 0 x3 x0
            have hA : 0 ≤ A := by dsimp only [A]; exact ind_nonneg db 0 x0 x1
            have hB : 0 ≤ B := by dsimp only [B]; exact ind_nonneg db 0 x1 x2
            have hC : 0 ≤ C := by dsimp only [C]; exact ind_nonneg db 0 x2 x3
            have hD : 0 ≤ D := by dsimp only [D]; exact ind_nonneg db 0 x3 x0
            have hB1 : B ≤ 1 := by dsimp only [B]; exact ind_le_one db 0 x1 x2
            have hD1 : D ≤ 1 := by dsimp only [D]; exact ind_le_one db 0 x3 x0
            have hAB : A * B ≤ A * 1 := Int.mul_le_mul (by omega) hB1 hB hA
            have hCD : C * D ≤ C * 1 := Int.mul_le_mul (by omega) hD1 hD hC
            have hm : A * B * (C * D) ≤ A * 1 * (C * 1) :=
              Int.mul_le_mul hAB hCD (Int.mul_nonneg hC hD) (Int.mul_nonneg hA (by decide))
            simpa [Int.mul_assoc] using hm
    _ = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
          ind db 0 x0 x1 * ind db 0 x2 x3)))) := by
            rw [rsum4_swap34]
            rw [rsum4_swap23]
            rw [rsum4_swap12]
    _ = (rsum n (fun x0 => rsum n (fun x1 => ind db 0 x0 x1))) *
        (rsum n (fun x2 => rsum n (fun x3 => ind db 0 x2 x3))) := by
            let K := rsum n (fun x2 => rsum n (fun x3 => ind db 0 x2 x3))
            calc
              rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
                  ind db 0 x0 x1 * ind db 0 x2 x3))))
                  = rsum n (fun x0 => rsum n (fun x1 => ind db 0 x0 x1 * K)) := by
                      apply rsum_congr
                      intro x0 hx0
                      apply rsum_congr
                      intro x1 hx1
                      dsimp only [K]
                      rw [show (fun x2 => rsum n (fun x3 => ind db 0 x0 x1 * ind db 0 x2 x3)) =
                          (fun x2 => ind db 0 x0 x1 * rsum n (fun x3 => ind db 0 x2 x3)) by
                            funext x2
                            exact rsum_const_mul n (ind db 0 x0 x1) (fun x3 => ind db 0 x2 x3)]
                      exact rsum_const_mul n (ind db 0 x0 x1) (fun x2 => rsum n (fun x3 => ind db 0 x2 x3))
              _ = (rsum n (fun x0 => rsum n (fun x1 => ind db 0 x0 x1))) * K := by
                      calc
                        rsum n (fun x0 => rsum n (fun x1 => ind db 0 x0 x1 * K))
                            = rsum n (fun x0 => (rsum n (fun x1 => ind db 0 x0 x1)) * K) := by
                                apply rsum_congr
                                intro x0 hx0
                                exact rsum_mul_const n (fun x1 => ind db 0 x0 x1) K
                        _ = (rsum n (fun x0 => rsum n (fun x1 => ind db 0 x0 x1))) * K := by
                                exact rsum_mul_const n
                                  (fun x0 => rsum n (fun x1 => ind db 0 x0 x1)) K
    _ = (support n db ⟨0, 0, 1⟩ pattern b : Int) * (support n db ⟨0, 2, 3⟩ pattern b : Int) := by
            rw [← support_supp_atom n db b 0 1 (by simp [pattern]) (by simp [pattern]),
            ← support_supp_atom n db b 2 3 (by simp [pattern]) (by simp [pattern])]

private theorem ref_le_supp13 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (ind db) b ≤
      (support n db ⟨0, 1, 2⟩ pattern b : Int) * (support n db ⟨0, 3, 0⟩ pattern b : Int) := by
  rw [reference_reply]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x0 =>
        ind db 0 x0 x1 * ind db 0 x1 x2 * ind db 0 x2 x3 * ind db 0 x3 x0))))
      ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x0 =>
          ind db 0 x1 x2 * ind db 0 x3 x0)))) := by
            apply rsum_le
            intro x1 hx1
            apply rsum_le
            intro x2 hx2
            apply rsum_le
            intro x3 hx3
            apply rsum_le
            intro x0 hx0
            let A := ind db 0 x0 x1
            let B := ind db 0 x1 x2
            let C := ind db 0 x2 x3
            let D := ind db 0 x3 x0
            have hA : 0 ≤ A := by dsimp only [A]; exact ind_nonneg db 0 x0 x1
            have hB : 0 ≤ B := by dsimp only [B]; exact ind_nonneg db 0 x1 x2
            have hC : 0 ≤ C := by dsimp only [C]; exact ind_nonneg db 0 x2 x3
            have hD : 0 ≤ D := by dsimp only [D]; exact ind_nonneg db 0 x3 x0
            have hA1 : A ≤ 1 := by dsimp only [A]; exact ind_le_one db 0 x0 x1
            have hC1 : C ≤ 1 := by dsimp only [C]; exact ind_le_one db 0 x2 x3
            have hAB : A * B ≤ 1 * B := Int.mul_le_mul hA1 (by omega) hB (by decide)
            have hCD : C * D ≤ 1 * D := Int.mul_le_mul hC1 (by omega) hD (by decide)
            have hm : A * B * (C * D) ≤ 1 * B * (1 * D) :=
              Int.mul_le_mul hAB hCD (Int.mul_nonneg hC hD) (Int.mul_nonneg (by decide) hB)
            simpa [Int.mul_assoc] using hm
    _ = (rsum n (fun x1 => rsum n (fun x2 => ind db 0 x1 x2))) *
        (rsum n (fun x3 => rsum n (fun x0 => ind db 0 x3 x0))) := by
            let K := rsum n (fun x3 => rsum n (fun x0 => ind db 0 x3 x0))
            calc
              rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x0 =>
                  ind db 0 x1 x2 * ind db 0 x3 x0))))
                  = rsum n (fun x1 => rsum n (fun x2 => ind db 0 x1 x2 * K)) := by
                      apply rsum_congr
                      intro x1 hx1
                      apply rsum_congr
                      intro x2 hx2
                      dsimp only [K]
                      rw [show (fun x3 => rsum n (fun x0 => ind db 0 x1 x2 * ind db 0 x3 x0)) =
                          (fun x3 => ind db 0 x1 x2 * rsum n (fun x0 => ind db 0 x3 x0)) by
                            funext x3
                            exact rsum_const_mul n (ind db 0 x1 x2) (fun x0 => ind db 0 x3 x0)]
                      exact rsum_const_mul n (ind db 0 x1 x2) (fun x3 => rsum n (fun x0 => ind db 0 x3 x0))
              _ = (rsum n (fun x1 => rsum n (fun x2 => ind db 0 x1 x2))) * K := by
                      calc
                        rsum n (fun x1 => rsum n (fun x2 => ind db 0 x1 x2 * K))
                            = rsum n (fun x1 => (rsum n (fun x2 => ind db 0 x1 x2)) * K) := by
                                apply rsum_congr
                                intro x1 hx1
                                exact rsum_mul_const n (fun x2 => ind db 0 x1 x2) K
                        _ = (rsum n (fun x1 => rsum n (fun x2 => ind db 0 x1 x2))) * K := by
                                exact rsum_mul_const n
                                  (fun x1 => rsum n (fun x2 => ind db 0 x1 x2)) K
    _ = (support n db ⟨0, 1, 2⟩ pattern b : Int) * (support n db ⟨0, 3, 0⟩ pattern b : Int) := by
            rw [← support_supp_atom n db b 1 2 (by simp [pattern]) (by simp [pattern]),
            ← support_supp_atom n db b 3 0 (by simp [pattern]) (by simp [pattern])]

private theorem rsum_zero_of_all_zero (n : Nat) (f : Nat → Int)
    (h : ∀ i, i < n → f i = 0) : rsum n f = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih (fun i hi => h i (Nat.lt_trans hi (Nat.lt_succ_self n))),
        h n (Nat.lt_succ_self n)]
      simp

private theorem rsum_eq_zero_of_nonneg (n : Nat) (f : Nat → Int)
    (h : ∀ i, i < n → 0 ≤ f i) (hz : rsum n f = 0) : ∀ i, i < n → f i = 0 := by
  induction n with
  | zero => intro i hi; omega
  | succ n ih =>
      rw [rsum_succ] at hz
      have hn := h n (Nat.lt_succ_self n)
      have hi : 0 ≤ rsum n f := rsum_nonneg n f (fun i hi => h i (Nat.lt_trans hi (Nat.lt_succ_self n)))
      have hzn : f n = 0 := by omega
      have hzr : rsum n f = 0 := by omega
      intro i hlt
      by_cases hin : i = n
      · subst i; exact hzn
      · have hil : i < n := by omega
        exact ih (fun j hj => h j (Nat.lt_trans hj (Nat.lt_succ_self n))) hzr i hil

private theorem ind_zero_of_support_zero (n : Nat) (db : DB) (b : Bind) (src dst : Nat)
    (hs : src ∈ pattern.freeVars) (hd : dst ∈ pattern.freeVars)
    (hz : support n db ⟨0, src, dst⟩ pattern b = 0) :
    ∀ x y, x < n → y < n → ind db 0 x y = 0 := by
  have hz' : support n db ⟨0, 0, 1⟩ pattern b = 0 := by
    rw [← support_any n db b src dst hs hd]
    exact hz
  have hs0 : rsum n (fun u => rsum n (fun v => ind db 0 u v)) = 0 := by
    rw [support_supp n db 0 b]
    simp [hz']
  intro x y hx hy
  have hout := rsum_eq_zero_of_nonneg n (fun u => rsum n (fun v => ind db 0 u v))
    (fun u hu => rsum_nonneg n (fun v => ind db 0 u v) (fun v hv => ind_nonneg db 0 u v)) hs0 x hx
  exact rsum_eq_zero_of_nonneg n (fun v => ind db 0 x v)
    (fun v hv => ind_nonneg db 0 x v) hout y hy

private theorem ref_zero_of_edge_zero (n : Nat) (db : DB) (b : Bind) (src dst : Nat)
    (hs : src ∈ pattern.freeVars) (hd : dst ∈ pattern.freeVars)
    (hz : support n db ⟨0, src, dst⟩ pattern b = 0) : reference pattern n (ind db) b = 0 := by
  have hall := ind_zero_of_support_zero n db b src dst hs hd hz
  rw [reference_reply]
  apply rsum_zero_of_all_zero
  intro x1 hx1
  apply rsum_zero_of_all_zero
  intro x2 hx2
  apply rsum_zero_of_all_zero
  intro x3 hx3
  apply rsum_zero_of_all_zero
  intro x0 hx0
  simp [hall x0 x1 hx0 hx1]

theorem sound_FractionalCap : TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle.FractionalCap.Goal := by
  intro n db b weights den hcover
  rcases hcover with ⟨hden, hwlen, hcov⟩
  have hwlen4 : weights.length = 4 := by simpa [pattern] using hwlen
  cases weights with
  | nil => simp at hwlen4
  | cons w0 ws =>
    cases ws with
    | nil => simp at hwlen4
    | cons w1 ws =>
      cases ws with
      | nil => simp at hwlen4
      | cons w2 ws =>
        cases ws with
        | nil => simp at hwlen4
        | cons w3 ws =>
          cases ws with
          | nil =>
            have h1 : den ≤ w0 + w1 := by
              have hh := hcov 1 (by simp [pattern])
              simpa [pattern] using hh
            have h2 : den ≤ w1 + w2 := by
              have hh := hcov 2 (by simp [pattern])
              simpa [pattern] using hh
            have h3 : den ≤ w2 + w3 := by
              have hh := hcov 3 (by simp [pattern])
              simpa [pattern] using hh
            have h4 : den ≤ w3 + w0 := by
              have hh := hcov 0 (by simp [pattern])
              have hh' : den ≤ w0 + w3 := by simpa [pattern] using hh
              omega
            let E0 := support n db ⟨0, 0, 1⟩ pattern b
            let E1 := support n db ⟨0, 1, 2⟩ pattern b
            let E2 := support n db ⟨0, 2, 3⟩ pattern b
            let E3 := support n db ⟨0, 3, 0⟩ pattern b
            let RHS := ((pattern.atoms.zip [w0, w1, w2, w3]).map
              (fun (a, w) => support n db a pattern b ^ w)).prod
            by_cases hE0 : E0 = 0
            · have hr := ref_zero_of_edge_zero n db b 0 1 (by simp [pattern]) (by simp [pattern])
                (by simpa [E0] using hE0)
              change (reference pattern n (ind db) b).toNat ^ den ≤ RHS
              simp [hr, hden]
            by_cases hE1 : E1 = 0
            · have hr := ref_zero_of_edge_zero n db b 1 2 (by simp [pattern]) (by simp [pattern])
                (by simpa [E1] using hE1)
              change (reference pattern n (ind db) b).toNat ^ den ≤ RHS
              simp [hr, hden]
            by_cases hE2 : E2 = 0
            · have hr := ref_zero_of_edge_zero n db b 2 3 (by simp [pattern]) (by simp [pattern])
                (by simpa [E2] using hE2)
              change (reference pattern n (ind db) b).toNat ^ den ≤ RHS
              simp [hr, hden]
            by_cases hE3 : E3 = 0
            · have hr := ref_zero_of_edge_zero n db b 3 0 (by simp [pattern]) (by simp [pattern])
                (by simpa [E3] using hE3)
              change (reference pattern n (ind db) b).toNat ^ den ≤ RHS
              simp [hr, hden]
            have hE0p : 0 < E0 := by omega
            have hE1p : 0 < E1 := by omega
            have hE2p : 0 < E2 := by omega
            have hE3p : 0 < E3 := by omega
            let N := (reference pattern n (ind db) b).toNat
            have hNnon : 0 ≤ reference pattern n (ind db) b := ref_nonneg n db b
            have hN02 : N ≤ E0 * E2 := by
              have hz : ((reference pattern n (ind db) b).toNat : Int) ≤ (E0 * E2 : Nat) := by
                rw [Int.toNat_of_nonneg hNnon]
                simpa [E0, E2] using ref_le_supp02 n db b
              exact Int.ofNat_le.mp hz
            have hN13 : N ≤ E1 * E3 := by
              have hz : ((reference pattern n (ind db) b).toNat : Int) ≤ (E1 * E3 : Nat) := by
                rw [Int.toNat_of_nonneg hNnon]
                simpa [E1, E3] using ref_le_supp13 n db b
              exact Int.ofNat_le.mp hz
            let c := max (den - w1) (den - w3)
            have hc0w0 : c ≤ w0 := by dsimp only [c]; omega
            have hc0w2 : c ≤ w2 := by dsimp only [c]; omega
            have hcden : c ≤ den := by dsimp only [c]; omega
            have hdcw1 : den - c ≤ w1 := by omega
            have hdcw3 : den - c ≤ w3 := by omega
            have hp02 : N ^ c ≤ (E0 * E2) ^ c := Nat.pow_le_pow_left hN02 c
            have hp13 : N ^ (den - c) ≤ (E1 * E3) ^ (den - c) := Nat.pow_le_pow_left hN13 _
            have hpowE0 : E0 ^ c ≤ E0 ^ w0 := Nat.pow_le_pow_right hE0p hc0w0
            have hpowE2 : E2 ^ c ≤ E2 ^ w2 := Nat.pow_le_pow_right hE2p hc0w2
            have hpowE1 : E1 ^ (den - c) ≤ E1 ^ w1 := Nat.pow_le_pow_right hE1p hdcw1
            have hpowE3 : E3 ^ (den - c) ≤ E3 ^ w3 := Nat.pow_le_pow_right hE3p hdcw3
            change (reference pattern n (ind db) b).toNat ^ den ≤ RHS
            calc
              (reference pattern n (ind db) b).toNat ^ den
                  = N ^ den := by rfl
              _ = N ^ c * N ^ (den - c) := by
                    rw [← Nat.pow_add]
                    congr 1
                    omega
              _ ≤ (E0 * E2) ^ c * (E1 * E3) ^ (den - c) := Nat.mul_le_mul hp02 hp13
              _ = (E0 ^ c * E2 ^ c) * (E1 ^ (den - c) * E3 ^ (den - c)) := by
                    rw [Nat.mul_pow, Nat.mul_pow]
              _ ≤ (E0 ^ w0 * E2 ^ w2) * (E1 ^ w1 * E3 ^ w3) :=
                    Nat.mul_le_mul (Nat.mul_le_mul hpowE0 hpowE2) (Nat.mul_le_mul hpowE1 hpowE3)
              _ = RHS := by
                    dsimp only [RHS, E0, E1, E2, E3]
                    simp [pattern]
                    ac_rfl
          | cons _ _ => simp at hwlen4



/-! Collision correction (temporary exploration). -/

private def bind4 (b : Bind) (a c d e : Nat) : Bind := put (put (put (put b 1 a) 2 c) 3 d) 0 e

private def F4 (db : DB) (b : Bind) (a c d e : Nat) : Int :=
  let x := bind4 b a c d e
  db 0 (x 0) (x 1) * (db 0 (x 1) (x 2) * (db 0 (x 2) (x 3) * db 0 (x 3) (x 0)))

private def constOnB (bs : List (List Nat)) (a c d e : Nat) : Bool :=
  let x := bind4 (fun _ => 0) a c d e
  pattern.freeVars.all (fun v =>
    x v == x (((bs.find? (fun w => decide (v ∈ w))).bind List.head?).getD v))

private theorem rsum_single (n u : Nat) (hu : u < n) (g : Nat → Int) :
    rsum n (fun t => if t == u then g t else 0) = g u := by
  induction n with
  | zero =>
      simp [rsum] at hu
  | succ n ih =>
      rw [show rsum (n+1) (fun t => if t == u then g t else 0) =
          rsum n (fun t => if t == u then g t else 0) + (if n == u then g n else 0) by
            simp [rsum, List.range_succ, List.sum_append]]
      by_cases hnu : n == u
      · have hnu' : n = u := Nat.eq_of_beq_eq_true hnu
        subst u
        have hz : rsum n (fun t => if t = n then g t else 0) = 0 := by
          apply rsum_zero_of_all_zero
          intro t ht
          have htn : t ≠ n := by omega
          simp [htn]
        simpa [hz]
      · have hne : n ≠ u := by
          intro h
          subst u
          simp at hnu
        have hu' : u < n := by omega
        rw [ih hu']
        simp [hnu]


private theorem ite_and_comm (P Q : Prop) [Decidable P] [Decidable Q] (t : Int) :
    (if P ∧ Q then t else 0) = if Q then (if P then t else 0) else 0 := by
  by_cases hP : P <;> by_cases hQ : Q <;> simp [hP, hQ]

private theorem rsum_chain1 (n u : Nat) (hu : u < n) (g : Nat → Int) :
    rsum n (fun x => if x = u then g x else 0) = g u := by
  simpa using rsum_single n u hu g

private theorem rsum_chain2 (n ux uy : Nat) (hux : ux < n) (huy : uy < n)
    (g : Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y =>
      if y = uy then (if x = ux then g x y else 0) else 0)) = g ux uy := by
  rw [show (fun x => rsum n (fun y =>
        if y = uy then (if x = ux then g x y else 0) else 0)) =
      (fun x => if x = ux then g x uy else 0) by
        funext x
        simpa using (rsum_single n uy huy (fun t => if x = ux then g x t else 0))]
  exact rsum_chain1 n ux hux (fun x => g x uy)

private theorem rsum_chain3 (n ux uy uz : Nat) (hux : ux < n) (huy : uy < n) (huz : uz < n)
    (g : Nat → Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      if z = uz then (if y = uy then (if x = ux then g x y z else 0) else 0) else 0))) =
      g ux uy uz := by
  rw [show (fun x => rsum n (fun y => rsum n (fun z =>
        if z = uz then (if y = uy then (if x = ux then g x y z else 0) else 0) else 0))) =
      (fun x => rsum n (fun y =>
        if y = uy then (if x = ux then g x y uz else 0) else 0)) by
        funext x
        rw [show (fun y => rsum n (fun z =>
              if z = uz then (if y = uy then (if x = ux then g x y z else 0) else 0) else 0)) =
            (fun y => if y = uy then (if x = ux then g x y uz else 0) else 0) by
              funext y
              simpa using (rsum_single n uz huz (fun t =>
                if y = uy then (if x = ux then g x y t else 0) else 0))]]
  exact rsum_chain2 n ux uy hux huy (fun x y => g x y uz)

private theorem collapsed_eq_full (bs : List (List Nat)) (hmem : bs ∈ partitions pattern.freeVars)
    (n : Nat) (db : DB) (b : Bind) :
  collapsed pattern n db b bs =
  rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
    if constOnB bs a c d e then F4 db b a c d e else 0)))) := by
  simp [partitions, pattern] at hmem
  rcases hmem with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => F4 db b a c d e)))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => F4 db b a c d e))))
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro e he
    rfl
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun d => rsum n (fun e => F4 db b a a d e))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a then F4 db b a c d e else 0))))
    rw [rsum4_swap23, rsum4_swap34]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro e he
    simpa using (rsum_chain1 n a ha (fun c => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun a => rsum n (fun e => F4 db b a c a e))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12, rsum4_swap34]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro e he
    simpa using (rsum_chain1 n a ha (fun d => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun d => rsum n (fun a => F4 db b a c d a))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12, rsum4_swap23]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain1 n a ha (fun e => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => rsum n (fun e => F4 db b a c c e))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = c then F4 db b a c d e else 0))))
    rw [rsum4_swap34]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro e he
    simpa using (rsum_chain1 n c hc (fun d => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun e => F4 db b a a a e)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ d = a then F4 db b a c d e else 0))))
    rw [rsum4_swap34, rsum4_swap23]
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro e he
    simpa using (rsum_chain2 n a a ha ha (fun c => (fun d => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun a => F4 db b a c c a)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = c ∧ e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12]
    simp only [ite_and_comm]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain2 n c a hc ha (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun d => rsum n (fun c => F4 db b a c d c))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if e = c then F4 db b a c d e else 0))))
    rw [rsum4_swap23]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro c hc
    simpa using (rsum_chain1 n c hc (fun e => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => F4 db b a c a c)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = a ∧ e = c then F4 db b a c d e else 0))))
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    simpa using (rsum_chain2 n a c ha hc (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun d => rsum n (fun a => F4 db b a a d a)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap23, rsum4_swap12]
    simp only [ite_and_comm]
    apply rsum_congr
    intro d hd
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain2 n a a ha ha (fun c => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => rsum n (fun d => F4 db b a c d d))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if e = d then F4 db b a c d e else 0))))
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro d hd
    simpa using (rsum_chain1 n d hd (fun e => F4 db b a c d e)).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun d => F4 db b a a d d)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ e = d then F4 db b a c d e else 0))))
    rw [rsum4_swap23]
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro d hd
    simpa using (rsum_chain2 n a d ha hd (fun c => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun c => rsum n (fun a => F4 db b a c a a)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = a ∧ e = a then F4 db b a c d e else 0))))
    rw [rsum4_swap12]
    simp only [ite_and_comm]
    apply rsum_congr
    intro c hc
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain2 n a a ha ha (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => rsum n (fun c => F4 db b a c c c)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if d = c ∧ e = c then F4 db b a c d e else 0))))
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    apply rsum_congr
    intro c hc
    simpa using (rsum_chain2 n c c hc hc (fun d => (fun e => F4 db b a c d e))).symm
  · simp [collapsed, integrate, pattern, value, bind4, put, constOnB]
    change rsum n (fun a => F4 db b a a a a) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if c = a ∧ d = a ∧ e = a then F4 db b a c d e else 0))))
    simp only [ite_and_comm]
    apply rsum_congr
    intro a ha
    simpa using (rsum_chain3 n a a a ha ha ha (fun c => (fun d => (fun e => F4 db b a c d e)))).symm

private theorem coeff_id (a c d e : Nat) :
    ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * (if constOnB bs a c d e then (1 : Int) else 0))).sum =
    (if [a, c, d, e].Nodup then (1 : Int) else 0) := by
  by_cases hac : c = a <;> by_cases had : d = a <;> by_cases hae : e = a <;>
    by_cases hcd : d = c <;> by_cases hce : e = c <;> by_cases hde : e = d
  all_goals
    simp [*, constOnB, bind4, pattern, put, partitions, mobius, factorial] <;> omega

private theorem injective_eq_full (n : Nat) (db : DB) (b : Bind) :
    injective pattern n db b =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
        if [a, c, d, e].Nodup then F4 db b a c d e else 0)))) := by
  simp [injective, integrate, pattern, value, put, F4, bind4, rsum]

private theorem list_sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons x l ih => simp [ih, Int.add_mul]

private theorem rsum4_zero (n : Nat) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => (0 : Int))))) = 0 := by
  rw [show (fun c => rsum n (fun d => (0 : Int))) = (fun _ => 0) by
        funext c; exact rsum_const_zero n]
  rw [show (fun b => rsum n (fun c => (0 : Int))) = (fun _ => 0) by
        funext b; exact rsum_const_zero n]
  rw [show (fun a => rsum n (fun b => (0 : Int))) = (fun _ => 0) by
        funext a; exact rsum_const_zero n]
  exact rsum_const_zero n

private theorem sum_rsum4 {α} (n : Nat) (l : List α) (f : α → Nat → Nat → Nat → Nat → Int) :
    (l.map (fun x => rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
      f x a c d e)))))).sum =
    rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
      (l.map (fun x => f x a c d e)).sum)))) := by
  induction l with
  | nil => simpa using (rsum4_zero n).symm
  | cons x l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [← rsum_add]
      apply rsum_congr
      intro a ha
      rw [← rsum_add]
      apply rsum_congr
      intro c hc
      rw [← rsum_add]
      apply rsum_congr
      intro d hd
      rw [← rsum_add]

private theorem rsum4_ext (n : Nat) (f g : Nat → Nat → Nat → Nat → Int)
    (h : ∀ a c d e, a < n → c < n → d < n → e < n → f a c d e = g a c d e) :
    rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => f a c d e)))) =
    rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => g a c d e)))) := by
  apply rsum_congr
  intro a ha
  apply rsum_congr
  intro c hc
  apply rsum_congr
  intro d hd
  apply rsum_congr
  intro e he
  exact h a c d e ha hc hd he

private theorem const_mul_rsum4 (n : Nat) (c : Int) (g : Nat → Nat → Nat → Nat → Int) :
    c * rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e => g a b d e)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun d => rsum n (fun e =>
        c * g a b d e)))) := by
  rw [← rsum_const_mul]
  apply rsum_congr
  intro a ha
  rw [← rsum_const_mul]
  apply rsum_congr
  intro b hb
  rw [← rsum_const_mul]
  apply rsum_congr
  intro d hd
  rw [← rsum_const_mul]

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle.CollisionCorrection.Goal := by
  intro n db b
  have hR : ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * collapsed pattern n db b bs)).sum =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
        ((partitions pattern.freeVars).map (fun bs =>
          mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))).sum)))) := by
    calc
      ((partitions pattern.freeVars).map (fun bs =>
          mobius bs * collapsed pattern n db b bs)).sum =
          ((partitions pattern.freeVars).map (fun bs =>
            rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
              mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))))))).sum := by
                apply list_sum_congr
                intro bs hbs
                rw [collapsed_eq_full bs hbs n db b]
                exact const_mul_rsum4 n (mobius bs) (fun a c d e =>
                  if constOnB bs a c d e then F4 db b a c d e else 0)
      _ = rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
            ((partitions pattern.freeVars).map (fun bs =>
              mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))).sum)))) := by
                exact sum_rsum4 n (partitions pattern.freeVars) (fun bs a c d e =>
                  mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))
  rw [injective_eq_full, hR]
  apply rsum4_ext
  intro a c d e ha hc hd he
  rw [show ((partitions pattern.freeVars).map (fun bs =>
        mobius bs * (if constOnB bs a c d e then F4 db b a c d e else 0))).sum =
      ((partitions pattern.freeVars).map (fun bs =>
        mobius bs * (if constOnB bs a c d e then (1 : Int) else 0))).sum * F4 db b a c d e by
      rw [← list_sum_mul_right]
      apply list_sum_congr
      intro bs hbs
      by_cases h : constOnB bs a c d e <;> simp [h]]
  rw [coeff_id]
  by_cases h : [a, c, d, e].Nodup <;> simp [h]
