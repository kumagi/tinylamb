import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `MovieCredits.CollisionCorrection` — `∀ n db b, injective pattern n db b =
  ((partitions pattern.freeVars).map (fun bs => mobius bs * collapsed pattern n db b
  bs)).sum`
- `MovieCredits.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCreditsCapsAndCollisionCorrection

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.MovieCredits

/--! Finite-range sums (Std only). -/

def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum
def nsum (n : Nat) (f : Nat → Nat) : Nat := ((List.range n).map f).sum

private theorem list_sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp only [List.map_cons, List.sum_cons, h a (by simp), ih (fun x hx => h x (by simp [hx]))]
private theorem nat_list_sum_congr {α} (l : List α) (f g : α → Nat) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp only [List.map_cons, List.sum_cons, h a (by simp), ih (fun x hx => h x (by simp [hx]))]
private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]
private theorem nsum_zero (f : Nat → Nat) : nsum 0 f = 0 := by simp [nsum]
private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]
private theorem nsum_succ (n : Nat) (f : Nat → Nat) : nsum (n+1) f = nsum n f + f n := by
  simp [nsum, List.range_succ, List.sum_append]
private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) : rsum n f = rsum n g := by
  apply list_sum_congr
  intro i hi
  exact h i (List.mem_range.mp hi)
private theorem nsum_congr (n : Nat) {f g : Nat → Nat} (h : ∀ i, i < n → f i = g i) : nsum n f = nsum n g := by
  apply nat_list_sum_congr
  intro i hi
  exact h i (List.mem_range.mp hi)
private theorem rsum_add (n : Nat) (f g : Nat → Int) : rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega
private theorem rsum_sub (n : Nat) (f g : Nat → Int) : rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega
private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) : rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]
private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) : rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]
private theorem rsum_const_zero (n : Nat) : rsum n (fun _ => (0 : Int)) = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, ih]; simp
private theorem nsum_const_zero (n : Nat) : nsum n (fun _ => (0 : Nat)) = 0 := by
  induction n with
  | zero => simp [nsum]
  | succ n ih => simp [nsum_succ, ih]
private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr; intro j hj; rw [rsum_zero]]
      exact (rsum_const_zero m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) = rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
      rw [rsum_add]
private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) : 0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega
private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) : rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega
private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr; intro i hi; rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]
private theorem rsum3_add (n : Nat) (f g : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c + g a b c))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c))) +
      rsum n (fun a => rsum n (fun b => rsum n (fun c => g a b c))) := by
  rw [show (fun a => rsum n (fun b => rsum n (fun c => f a b c + g a b c))) =
      (fun a => rsum n (fun b => rsum n (fun c => f a b c)) + rsum n (fun b => rsum n (fun c => g a b c))) by
    funext a
    rw [show (fun b => rsum n (fun c => f a b c + g a b c)) =
        (fun b => rsum n (fun c => f a b c) + rsum n (fun c => g a b c)) by
      funext b
      rw [rsum_add]]
    rw [rsum_add]]
  rw [rsum_add]
private theorem rsum3_sub (n : Nat) (f g : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c - g a b c))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c))) -
      rsum n (fun a => rsum n (fun b => rsum n (fun c => g a b c))) := by
  rw [show (fun a => rsum n (fun b => rsum n (fun c => f a b c - g a b c))) =
      (fun a => rsum n (fun b => rsum n (fun c => f a b c)) - rsum n (fun b => rsum n (fun c => g a b c))) by
    funext a
    rw [show (fun b => rsum n (fun c => f a b c - g a b c)) =
        (fun b => rsum n (fun c => f a b c) - rsum n (fun c => g a b c)) by
      funext b
      rw [rsum_sub]]
    rw [rsum_sub]]
  rw [rsum_sub]
private theorem rsum3_const_mul (n : Nat) (k : Int) (f : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => k * f a b c))) =
      k * rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c))) := by
  rw [show (fun a => rsum n (fun b => rsum n (fun c => k * f a b c))) =
      (fun a => k * rsum n (fun b => rsum n (fun c => f a b c))) by
    funext a
    rw [show (fun b => rsum n (fun c => k * f a b c)) =
        (fun b => k * rsum n (fun c => f a b c)) by
      funext b
      rw [rsum_const_mul]]
    rw [rsum_const_mul]]
  rw [rsum_const_mul]
private theorem rsum_eq_zero_of_false (n a : Nat) (g : Nat → Int)
    (h : ∀ x, x < n → (x == a) = false) : rsum n (fun x => if x == a then g x else 0) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [rsum_succ]
      have hn : (n == a) = false := h n (Nat.lt_succ_self n)
      rw [ih (fun x hx => h x (Nat.lt_succ_of_lt hx)), hn]
      simp
private theorem rsum_eq_single (n a : Nat) (ha : a < n) (g : Nat → Int) :
    rsum n (fun x => if x == a then g x else 0) = g a := by
  induction n with
  | zero => simp at ha
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : n == a
      · have hna : n = a := Nat.eq_of_beq_eq_true h
        subst n
        have hprev := rsum_eq_zero_of_false a a g (by
          intro x hx
          have hxa : x ≠ a := by omega
          cases hx' : (x == a) <;> simp_all)
        rw [hprev]
        simp
      · have hne : n ≠ a := by intro hn; subst n; simp at h
        have ha' : a < n := by omega
        rw [ih ha']
        simp [h]
private theorem rsum_eq_single_pair (n a : Nat) (ha : a < n) (g : Nat → Nat → Int) :
    rsum n (fun b => rsum n (fun d => if b == a && d == a then g b d else 0)) = g a a := by
  calc
    rsum n (fun b => rsum n (fun d => if b == a && d == a then g b d else 0))
        = rsum n (fun b => if b == a then rsum n (fun d => if d == a then g b d else 0) else 0) := by
            apply rsum_congr; intro b hb
            by_cases hba : b == a
            · simp [hba]
            · have hz : rsum n (fun d => if b == a && d == a then g b d else 0) = 0 := by
                rw [show (fun d => if b == a && d == a then g b d else 0) = (fun _ => (0 : Int)) by
                  funext d
                  simp [hba]]
                exact rsum_const_zero n
              rw [hz]
              simp [hba]
    _ = g a a := by
        rw [rsum_eq_single n a ha (fun b => rsum n (fun d => if d == a then g b d else 0))]
        rw [rsum_eq_single n a ha (fun d => g a d)]

/--! Möbius inversion on the three free variables of the MovieCredits pattern. -/

private def F (db : DB) (a b c : Nat) : Int := db 0 c a * (db 1 a b * db 2 b c)

private theorem inclusion_exclusion (n : Nat) (F : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      if [a, b, c].Nodup then F a b c else 0))) =
    rsum n (fun a => rsum n (fun b => rsum n (fun c => F a b c))) -
    rsum n (fun a => rsum n (fun c => F a a c)) -
    rsum n (fun a => rsum n (fun b => F a b a)) -
    rsum n (fun a => rsum n (fun b => F a b b)) +
    2 * rsum n (fun a => F a a a) := by
  let ind1 := fun a b c : Nat => if b == a then (1 : Int) else 0
  let ind2 := fun a b c : Nat => if c == a then (1 : Int) else 0
  let ind3 := fun a b c : Nat => if c == b then (1 : Int) else 0
  let ind4 := fun a b c : Nat => if b == a && c == a then (1 : Int) else 0
  have hT1 : rsum n (fun a => rsum n (fun b => rsum n (fun c => ind1 a b c * F a b c))) =
      rsum n (fun a => rsum n (fun c => F a a c)) := by
    apply rsum_congr; intro a ha
    calc
      rsum n (fun b => rsum n (fun c => ind1 a b c * F a b c))
          = rsum n (fun b => rsum n (fun c => if b == a then F a b c else 0)) := by
        apply rsum_congr; intro b hb
        apply rsum_congr; intro c hc
        dsimp only [ind1]
        by_cases h : b == a <;> simp [h]
      _ = rsum n (fun b => if b == a then rsum n (fun c => F a b c) else 0) := by
        apply rsum_congr; intro b hb
        by_cases hba : b == a
        · simp [hba]
        · have hz : rsum n (fun c => if b == a then F a b c else 0) = 0 := by
            rw [show (fun c => if b == a then F a b c else 0) = (fun _ => (0 : Int)) by
              funext c; simp [hba]]
            exact rsum_const_zero n
          rw [hz]
          simp [hba]
      _ = rsum n (fun c => F a a c) := by
        rw [rsum_eq_single n a ha (fun b => rsum n (fun c => F a b c))]
  have hT2 : rsum n (fun a => rsum n (fun b => rsum n (fun c => ind2 a b c * F a b c))) =
      rsum n (fun a => rsum n (fun b => F a b a)) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    symm
    calc
      F a b a = rsum n (fun c => if c == a then F a b c else 0) := (rsum_eq_single n a ha (fun c => F a b c)).symm
      _ = rsum n (fun c => ind2 a b c * F a b c) := by
        apply rsum_congr; intro c hc
        dsimp only [ind2]
        by_cases h : c == a <;> simp [h]
  have hT3 : rsum n (fun a => rsum n (fun b => rsum n (fun c => ind3 a b c * F a b c))) =
      rsum n (fun a => rsum n (fun b => F a b b)) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    symm
    calc
      F a b b = rsum n (fun c => if c == b then F a b c else 0) := (rsum_eq_single n b hb (fun c => F a b c)).symm
      _ = rsum n (fun c => ind3 a b c * F a b c) := by
        apply rsum_congr; intro c hc
        dsimp only [ind3]
        by_cases h : c == b <;> simp [h]
  have hT4 : rsum n (fun a => rsum n (fun b => rsum n (fun c => ind4 a b c * F a b c))) =
      rsum n (fun a => F a a a) := by
    apply rsum_congr; intro a ha
    symm
    calc
      F a a a = rsum n (fun b => rsum n (fun c => if b == a && c == a then F a b c else 0)) :=
        (rsum_eq_single_pair n a ha (fun b c => F a b c)).symm
      _ = rsum n (fun b => rsum n (fun c => ind4 a b c * F a b c)) := by
        apply rsum_congr; intro b hb
        apply rsum_congr; intro c hc
        dsimp only [ind4]
        by_cases h : b == a && c == a <;> simp [h]
  have hpoint : ∀ a b c : Nat,
      (if [a, b, c].Nodup then F a b c else 0) =
        F a b c - ind1 a b c * F a b c - ind2 a b c * F a b c -
          ind3 a b c * F a b c + 2 * (ind4 a b c * F a b c) := by
    intro a b c
    dsimp only [ind1, ind2, ind3, ind4]
    simp only [beq_iff_eq]
    by_cases hba : b = a
    · by_cases hca : c = a
      · have hcb : c = b := by omega
        simp [hba, hca] <;> omega
      · have hca' : a ≠ c := fun h => hca h.symm
        by_cases hcb : c = b
        · simp [hba, hcb] <;> omega
        · have hcb' : b ≠ c := fun h => hcb h.symm
          simp [hba, hca, hca'] <;> omega
    · have hba' : a ≠ b := fun h => hba h.symm
      by_cases hca : c = a
      · have hcb : c ≠ b := by intro h; exact hba (h.symm.trans hca)
        simp [hba, hba', hca] <;> omega
      · have hca' : a ≠ c := fun h => hca h.symm
        by_cases hcb : c = b
        · simp [hba, hba', hcb] <;> omega
        · have hcb' : b ≠ c := fun h => hcb h.symm
          simp [hba, hba', hca, hca', hcb, hcb'] <;> omega
  calc
    rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      if [a, b, c].Nodup then F a b c else 0)))
        = rsum n (fun a => rsum n (fun b => rsum n (fun c =>
            F a b c - ind1 a b c * F a b c - ind2 a b c * F a b c -
              ind3 a b c * F a b c + 2 * (ind4 a b c * F a b c)))) := by
            apply rsum_congr; intro a ha
            apply rsum_congr; intro b hb
            apply rsum_congr; intro c hc
            exact hpoint a b c
    _ = (rsum n (fun a => rsum n (fun b => rsum n (fun c => F a b c))) -
          rsum n (fun a => rsum n (fun b => rsum n (fun c => ind1 a b c * F a b c))) -
          rsum n (fun a => rsum n (fun b => rsum n (fun c => ind2 a b c * F a b c))) -
          rsum n (fun a => rsum n (fun b => rsum n (fun c => ind3 a b c * F a b c)))) +
          2 * rsum n (fun a => rsum n (fun b => rsum n (fun c => ind4 a b c * F a b c))) := by
            rw [rsum3_add, rsum3_sub, rsum3_sub, rsum3_sub, rsum3_const_mul]
    _ = rsum n (fun a => rsum n (fun b => rsum n (fun c => F a b c))) -
        rsum n (fun a => rsum n (fun c => F a a c)) -
        rsum n (fun a => rsum n (fun b => F a b a)) -
        rsum n (fun a => rsum n (fun b => F a b b)) +
        2 * rsum n (fun a => F a a a) := by
            rw [hT1, hT2, hT3, hT4]

private theorem parts_eq : partitions pattern.freeVars =
    ([[[2], [3], [1]], [[2, 3], [1]], [[3], [2, 1]], [[2], [3, 1]], [[2, 3, 1]]] : List (List (List Nat))) := by
  native_decide

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  have hinj : injective pattern n db b =
      rsum n (fun a => rsum n (fun c => rsum n (fun d =>
        if [a, c, d].Nodup then F db a c d else 0))) := by
    simp [injective, pattern, integrate, value, put, rsum, F]
  have hc1 : collapsed pattern n db b ([[2], [3], [1]] : List (List Nat)) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => F db a c d))) := by
    simp [collapsed, pattern, integrate, value, put, rsum, F]
  have hc2 : collapsed pattern n db b ([[2, 3], [1]] : List (List Nat)) =
      rsum n (fun a => rsum n (fun d => F db a a d)) := by
    simp [collapsed, pattern, integrate, value, put, rsum, F]
  have hc3 : collapsed pattern n db b ([[3], [2, 1]] : List (List Nat)) =
      rsum n (fun a => rsum n (fun c => F db a c a)) := by
    rw [show collapsed pattern n db b ([[3], [2, 1]] : List (List Nat)) =
        rsum n (fun c => rsum n (fun a => F db a c a)) by
          simp [collapsed, pattern, integrate, value, put, rsum, F]]
    rw [rsum_comm]
  have hc4 : collapsed pattern n db b ([[2], [3, 1]] : List (List Nat)) =
      rsum n (fun a => rsum n (fun c => F db a c c)) := by
    simp [collapsed, pattern, integrate, value, put, rsum, F]
  have hc5 : collapsed pattern n db b ([[2, 3, 1]] : List (List Nat)) =
      rsum n (fun a => F db a a a) := by
    simp [collapsed, pattern, integrate, value, put, rsum, F]
  rw [hinj, parts_eq]
  simp only [List.map_cons, List.sum_cons, List.map_nil, List.sum_nil]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5]
  have hie := inclusion_exclusion n (F db)
  have htarget : rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      if [a, c, d].Nodup then F db a c d else 0))) =
    (rsum n (fun a => rsum n (fun c => rsum n (fun d => F db a c d))) -
      rsum n (fun a => rsum n (fun d => F db a a d)) -
      rsum n (fun a => rsum n (fun c => F db a c a)) -
      rsum n (fun a => rsum n (fun c => F db a c c)) +
      2 * rsum n (fun a => F db a a a)) := by
    simpa only [] using hie
  have htarget2 : (rsum n (fun a => rsum n (fun c => rsum n (fun d =>
      if ((¬ a = c ∧ ¬ a = d) ∧ ¬ c = d) then F db a c d else 0))) =
    (rsum n (fun a => rsum n (fun c => rsum n (fun d => F db a c d))) -
      rsum n (fun a => rsum n (fun d => F db a a d)) -
      rsum n (fun a => rsum n (fun c => F db a c a)) -
      rsum n (fun a => rsum n (fun c => F db a c c)) +
      2 * rsum n (fun a => F db a a a))) := by
    simpa using htarget
  omega

/--! Fractional edge-cover bound. -/

private def bin (db : DB) : DB := fun r x y => if db r x y == 0 then 0 else 1

private theorem bin_nonneg (db : DB) (r x y : Nat) : 0 ≤ bin db r x y := by
  simp [bin]
  split <;> omega
private theorem bin_le_one (db : DB) (r x y : Nat) : bin db r x y ≤ 1 := by
  simp [bin]
  split <;> omega
private theorem bin_sq (db : DB) (r x y : Nat) : bin db r x y * bin db r x y = bin db r x y := by
  by_cases h : db r x y == 0 <;> simp [bin, h]

private theorem list_sum_int {α} (l : List α) (f : α → Nat) :
    ((l.map f).sum : Int) = (l.map (fun x => (f x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem countP_int (n : Nat) (p : Nat → Bool) :
    ((List.range n).countP p : Int) = ((List.range n).map (fun x => if p x then (1 : Int) else 0)).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      rw [show (((List.range n).countP p + [n].countP p : Nat) : Int) =
          (((List.range n).countP p : Int) + ([n].countP p : Int)) by omega]
      rw [ih]
      simp [List.map_append, List.sum_append]
      by_cases hp : p n = true <;> simp [hp]

private theorem support_cast (n : Nat) (db : DB) (b : Bind) (r src dst : Nat)
    (hsrc : pattern.freeVars.contains src = true) (hdst : pattern.freeVars.contains dst = true) :
    (support n db ⟨r, src, dst⟩ pattern b : Int) =
      rsum n (fun x => rsum n (fun y => bin db r x y)) := by
  have hsrc' : src = 2 ∨ src = 3 ∨ src = 1 := by simpa [pattern] using hsrc
  have hdst' : dst = 2 ∨ dst = 3 ∨ dst = 1 := by simpa [pattern] using hdst
  rw [show support n db ⟨r, src, dst⟩ pattern b =
      nsum n (fun x => (List.range n).countP (fun y => db r x y != 0)) by
    simp [support, pattern, nsum, hsrc', hdst']]
  dsimp only [nsum, rsum]
  rw [list_sum_int]
  apply list_sum_congr
  intro x hx
  rw [countP_int]
  apply list_sum_congr
  intro y hy
  simp [bin, beq_iff_eq]

private theorem reference_bin (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (bin db) b =
      rsum n (fun a => rsum n (fun c => rsum n (fun d =>
        bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by
  simp [reference, pattern, integrate, value, put, rsum, bin]

private theorem rsum_mul_le_mul_rsum (n : Nat) (f g : Nat → Int)
    (hf : ∀ i, i < n → 0 ≤ f i) (hg : ∀ i, i < n → 0 ≤ g i) :
    rsum n (fun i => f i * g i) ≤ rsum n f * rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ]
      have hfn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
      have hgn : 0 ≤ g n := hg n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => hf i (Nat.lt_succ_of_lt hi)) (fun i hi => hg i (Nat.lt_succ_of_lt hi))
      have hsf : 0 ≤ rsum n f := rsum_nonneg n f (fun i hi => hf i (Nat.lt_succ_of_lt hi))
      have hsg : 0 ≤ rsum n g := rsum_nonneg n g (fun i hi => hg i (Nat.lt_succ_of_lt hi))
      calc
        rsum n (fun i => f i * g i) + f n * g n ≤ rsum n f * rsum n g + f n * g n :=
          Int.add_le_add hi (by omega)
        _ ≤ (rsum n f * rsum n g + rsum n f * g n) + (f n * rsum n g + f n * g n) := by
            have hcross1 : 0 ≤ rsum n f * g n := Int.mul_nonneg hsf hgn
            have hcross2 : 0 ≤ f n * rsum n g := Int.mul_nonneg hfn hsg
            have hnon : 0 ≤ rsum n f * g n + f n * rsum n g := by omega
            omega
        _ = (rsum n f + f n) * (rsum n g + g n) := by
            simp [Int.add_mul, Int.mul_add, Int.add_assoc, Int.add_comm, Int.add_left_comm]

private theorem list_pairs_sum (l : List Nat) (n : Nat) (f : Nat → Nat → Int) :
    (l.map (fun i => rsum n (fun j => f i j))).sum =
      ((l.flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
        (fun p => f p.1 p.2)).sum := by
  induction l with
  | nil => simp
  | cons i l ih =>
      simp only [List.map_cons, List.sum_cons, List.flatMap_cons]
      rw [ih]
      simp [rsum, List.map_append, List.sum_append, Function.comp_def]

private theorem rsum_pairs (n : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => f i j)) =
      (((List.range n).flatMap (fun i => (List.range n).map (fun j => (i,j)))).map
        (fun p => f p.1 p.2)).sum := by
  exact list_pairs_sum (List.range n) n f

private theorem sq_nonneg_int (z : Int) : 0 ≤ z ^ 2 := Int.sq_nonneg z

private theorem cs_aux (l : List (Int × Int)) (a b : Int) :
  (l.map (fun p => p.1*p.1)).sum * (b*b) + (l.map (fun p => p.2*p.2)).sum * (a*a) -
    2*(l.map (fun p => p.1*p.2)).sum*a*b =
  (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      grind

private theorem cs_sum_sq_nonneg (l : List (Int × Int)) (a b : Int) :
    0 ≤ (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hp : 0 ≤ (a*p.2 - p.1*b)^2 := Int.sq_nonneg _
      omega

private theorem cs_list : ∀ l : List (Int × Int),
    (l.map (fun p => p.1 * p.2)).sum ^ 2 ≤
      (l.map (fun p => p.1 * p.1)).sum * (l.map (fun p => p.2 * p.2)).sum
  | [] => by simp
  | (a,b)::l => by
      simp only [List.map_cons, List.sum_cons]
      have ih := cs_list l
      have hdiff : 0 ≤ (a*a + (l.map (fun p => p.1*p.1)).sum) *
              (b*b + (l.map (fun p => p.2*p.2)).sum) -
            (a*b + (l.map (fun p => p.1*p.2)).sum)^2 := by
        rw [show (a*a + (l.map (fun p => p.1*p.1)).sum) *
              (b*b + (l.map (fun p => p.2*p.2)).sum) -
              (a*b + (l.map (fun p => p.1*p.2)).sum)^2 =
            ((l.map (fun p => p.1*p.1)).sum * (l.map (fun p => p.2*p.2)).sum -
              (l.map (fun p => p.1*p.2)).sum^2) +
            ((l.map (fun p => p.1*p.1)).sum * (b*b) + (l.map (fun p => p.2*p.2)).sum * (a*a) -
              2*(l.map (fun p => p.1*p.2)).sum*a*b) by grind]
        rw [cs_aux]
        have h1 : 0 ≤ (l.map (fun p => p.1*p.1)).sum * (l.map (fun p => p.2*p.2)).sum -
              (l.map (fun p => p.1*p.2)).sum^2 := by omega
        have h2 : 0 ≤ (l.map (fun p => (a*p.2 - p.1*b)^2)).sum := cs_sum_sq_nonneg l a b
        omega
      omega

private theorem weighted_bound (T E0 E1 E2 : Nat)
    (hE0 : 1 ≤ E0) (hE1 : 1 ≤ E1) (hE2 : 1 ≤ E2)
    (h1 : T ≤ E0 * E1) (h2 : T ≤ E1 * E2) (h3 : T ≤ E0 * E2)
    (h4 : T ^ 2 ≤ E0 * E1 * E2) :
    ∀ den : Nat, 0 < den → ∀ w0 w1 w2 : Nat,
      den ≤ w0 + w1 → den ≤ w1 + w2 → den ≤ w0 + w2 →
      T ^ den ≤ E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by
  intro den
  induction den using Nat.strongRecOn with
  | ind den ih =>
      intro hden w0 w1 w2 hc2 hc3 hc1
      by_cases h0z : w0 = 0
      · have hw1 : den ≤ w1 := by omega
        have hw2 : den ≤ w2 := by omega
        have hp : T ^ den ≤ (E1 * E2) ^ den := Nat.pow_le_pow_left h2 den
        rw [Nat.mul_pow] at hp
        calc
          T ^ den ≤ E1 ^ den * E2 ^ den := hp
          _ ≤ E1 ^ w1 * E2 ^ w2 := Nat.mul_le_mul (Nat.pow_le_pow_right hE1 hw1)
            (Nat.pow_le_pow_right hE2 hw2)
          _ = E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by simp [h0z]
      by_cases h1z : w1 = 0
      · have hw0 : den ≤ w0 := by omega
        have hw2 : den ≤ w2 := by omega
        have hp : T ^ den ≤ (E0 * E2) ^ den := Nat.pow_le_pow_left h3 den
        rw [Nat.mul_pow] at hp
        calc
          T ^ den ≤ E0 ^ den * E2 ^ den := hp
          _ ≤ E0 ^ w0 * E2 ^ w2 := Nat.mul_le_mul (Nat.pow_le_pow_right hE0 hw0)
            (Nat.pow_le_pow_right hE2 hw2)
          _ = E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by simp [h1z]
      by_cases h2z : w2 = 0
      · have hw0 : den ≤ w0 := by omega
        have hw1 : den ≤ w1 := by omega
        have hp : T ^ den ≤ (E0 * E1) ^ den := Nat.pow_le_pow_left h1 den
        rw [Nat.mul_pow] at hp
        calc
          T ^ den ≤ E0 ^ den * E1 ^ den := hp
          _ ≤ E0 ^ w0 * E1 ^ w1 := Nat.mul_le_mul (Nat.pow_le_pow_right hE0 hw0)
            (Nat.pow_le_pow_right hE1 hw1)
          _ = E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by simp [h2z]
      have hw0p : 0 < w0 := Nat.pos_of_ne_zero h0z
      have hw1p : 0 < w1 := Nat.pos_of_ne_zero h1z
      have hw2p : 0 < w2 := Nat.pos_of_ne_zero h2z
      by_cases hden1 : den = 1
      · subst den
        have hw0 : 1 ≤ w0 := by omega
        have hw1 : 1 ≤ w1 := by omega
        have hp : T ≤ E0 ^ w0 * E1 ^ w1 := by
          calc
            T ≤ E0 * E1 := h1
            _ ≤ E0 ^ w0 * E1 ^ w1 := Nat.mul_le_mul
              (by simpa using Nat.pow_le_pow_right hE0 hw0)
              (by simpa using Nat.pow_le_pow_right hE1 hw1)
        have hE2pow : 1 ≤ E2 ^ w2 := Nat.one_le_pow w2 E2 hE2
        have hmul : E0 ^ w0 * E1 ^ w1 ≤ E0 ^ w0 * E1 ^ w1 * E2 ^ w2 :=
          Nat.le_mul_of_pos_right _ (Nat.pow_pos hE2)
        simpa using (Nat.le_trans hp hmul)
      by_cases hden2 : den = 2
      · subst den
        have hw0 : 1 ≤ w0 := by omega
        have hw1 : 1 ≤ w1 := by omega
        have hw2 : 1 ≤ w2 := by omega
        calc
          T ^ 2 ≤ E0 * E1 * E2 := h4
          _ ≤ E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by
            exact Nat.mul_le_mul (Nat.mul_le_mul
              (by simpa using Nat.pow_le_pow_right hE0 hw0)
              (by simpa using Nat.pow_le_pow_right hE1 hw1))
              (by simpa using Nat.pow_le_pow_right hE2 hw2)
      have hdge3 : 3 ≤ den := by omega
      let d : Nat := den - 2
      have hdlt : d < den := by dsimp only [d]; omega
      have hdpos : 0 < d := by dsimp only [d]; omega
      have hc2' : d ≤ (w0 - 1) + (w1 - 1) := by dsimp only [d]; omega
      have hc3' : d ≤ (w1 - 1) + (w2 - 1) := by dsimp only [d]; omega
      have hc1' : d ≤ (w0 - 1) + (w2 - 1) := by dsimp only [d]; omega
      have hih := ih d hdlt hdpos (w0 - 1) (w1 - 1) (w2 - 1) hc2' hc3' hc1'
      have hp : T ^ den = T ^ 2 * T ^ d := by
        rw [show den = 2 + d by dsimp only [d]; omega, Nat.pow_add]
      rw [hp]
      have hm := Nat.mul_le_mul h4 hih
      have hp0 : E0 ^ w0 = E0 ^ (w0 - 1) * E0 := by
        calc
          E0 ^ w0 = E0 ^ ((w0 - 1) + 1) :=
            congrArg (fun k : Nat => E0 ^ k) (show w0 = (w0 - 1) + 1 by omega)
          _ = E0 ^ (w0 - 1) * E0 := by rw [Nat.pow_succ]
      have hp1 : E1 ^ w1 = E1 ^ (w1 - 1) * E1 := by
        calc
          E1 ^ w1 = E1 ^ ((w1 - 1) + 1) :=
            congrArg (fun k : Nat => E1 ^ k) (show w1 = (w1 - 1) + 1 by omega)
          _ = E1 ^ (w1 - 1) * E1 := by rw [Nat.pow_succ]
      have hp2 : E2 ^ w2 = E2 ^ (w2 - 1) * E2 := by
        calc
          E2 ^ w2 = E2 ^ ((w2 - 1) + 1) :=
            congrArg (fun k : Nat => E2 ^ k) (show w2 = (w2 - 1) + 1 by omega)
          _ = E2 ^ (w2 - 1) * E2 := by rw [Nat.pow_succ]
      calc
        T ^ 2 * T ^ d ≤ (E0 * E1 * E2) *
            (E0 ^ (w0 - 1) * E1 ^ (w1 - 1) * E2 ^ (w2 - 1)) := hm
        _ = E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by
            rw [hp0, hp1, hp2]
            ac_rfl

theorem sound_FractionalCap : FractionalCap.Goal := by
  intro n db b weights den hcover
  have hden : 0 < den := hcover.1
  have hwlen : weights.length = 3 := by simpa [pattern] using hcover.2.1
  obtain ⟨w0, w1, w2, hw⟩ : ∃ w0 w1 w2, weights = [w0, w1, w2] := by
    cases weights with
    | nil => simp at hwlen
    | cons a as =>
        cases as with
        | nil => simp at hwlen
        | cons c cs =>
            cases cs with
            | nil => simp at hwlen
            | cons d ds =>
                cases ds with
                | nil => exact ⟨a, c, d, rfl⟩
                | cons e es => simp at hwlen
  subst weights
  have hc2 : den ≤ w0 + w1 := by
    have h := hcover.2.2 2 (by simp [pattern])
    simp [pattern] at h
    omega
  have hc3 : den ≤ w1 + w2 := by
    have h := hcover.2.2 3 (by simp [pattern])
    simp [pattern] at h
    omega
  have hc1 : den ≤ w0 + w2 := by
    have h := hcover.2.2 1 (by simp [pattern])
    simp [pattern] at h
    omega
  let e0 : Int := rsum n (fun x => rsum n (fun y => bin db 0 x y))
  let e1 : Int := rsum n (fun x => rsum n (fun y => bin db 1 x y))
  let e2 : Int := rsum n (fun x => rsum n (fun y => bin db 2 x y))
  let t : Int := rsum n (fun a => rsum n (fun c => rsum n (fun d =>
    bin db 0 d a * (bin db 1 a c * bin db 2 c d))))
  have hs0 : (support n db (⟨0, 1, 2⟩ : Atom) pattern b : Int) = e0 := by
    dsimp only [e0]
    exact support_cast n db b 0 1 2 (by simp [pattern]) (by simp [pattern])
  have hs1 : (support n db (⟨1, 2, 3⟩ : Atom) pattern b : Int) = e1 := by
    dsimp only [e1]
    exact support_cast n db b 1 2 3 (by simp [pattern]) (by simp [pattern])
  have hs2 : (support n db (⟨2, 3, 1⟩ : Atom) pattern b : Int) = e2 := by
    dsimp only [e2]
    exact support_cast n db b 2 3 1 (by simp [pattern]) (by simp [pattern])
  have he0 : 0 ≤ e0 := by
    dsimp only [e0]
    apply rsum_nonneg; intro x hx
    apply rsum_nonneg; intro y hy
    exact bin_nonneg db 0 x y
  have he1 : 0 ≤ e1 := by
    dsimp only [e1]
    apply rsum_nonneg; intro x hx
    apply rsum_nonneg; intro y hy
    exact bin_nonneg db 1 x y
  have he2 : 0 ≤ e2 := by
    dsimp only [e2]
    apply rsum_nonneg; intro x hx
    apply rsum_nonneg; intro y hy
    exact bin_nonneg db 2 x y
  have ht0 : 0 ≤ t := by
    dsimp only [t]
    apply rsum_nonneg; intro a ha
    apply rsum_nonneg; intro c hc
    apply rsum_nonneg; intro d hd
    exact Int.mul_nonneg (bin_nonneg db 0 d a)
      (Int.mul_nonneg (bin_nonneg db 1 a c) (bin_nonneg db 2 c d))
  have hterm1 : ∀ a c d : Nat,
      bin db 0 d a * (bin db 1 a c * bin db 2 c d) ≤ bin db 0 d a * bin db 1 a c := by
    intro a c d
    have h0 : 0 ≤ bin db 0 d a := bin_nonneg db 0 d a
    have h1n : 0 ≤ bin db 1 a c := bin_nonneg db 1 a c
    have h2n : 0 ≤ bin db 2 c d := bin_nonneg db 2 c d
    have h21 : bin db 2 c d ≤ 1 := bin_le_one db 2 c d
    have hh := Int.mul_le_mul (Int.le_refl (bin db 0 d a * bin db 1 a c)) h21 h2n
      (Int.mul_nonneg h0 h1n)
    calc
      bin db 0 d a * (bin db 1 a c * bin db 2 c d)
          = (bin db 0 d a * bin db 1 a c) * bin db 2 c d := by ac_rfl
      _ ≤ (bin db 0 d a * bin db 1 a c) * 1 := hh
      _ = bin db 0 d a * bin db 1 a c := by rw [Int.mul_one]
  have h1I : t ≤ e0 * e1 := by
    have hsum : t ≤ rsum n (fun a => rsum n (fun c => rsum n (fun d =>
        bin db 0 d a * bin db 1 a c))) := by
      dsimp only [t]
      apply rsum_le; intro a ha
      apply rsum_le; intro c hc
      apply rsum_le; intro d hd
      exact hterm1 a c d
    calc
      t ≤ rsum n (fun a => rsum n (fun c => rsum n (fun d =>
          bin db 0 d a * bin db 1 a c))) := hsum
      _ = rsum n (fun a => (rsum n (fun d => bin db 0 d a)) *
          (rsum n (fun c => bin db 1 a c))) := by
            apply rsum_congr; intro a ha
            calc
              rsum n (fun c => rsum n (fun d => bin db 0 d a * bin db 1 a c))
                  = rsum n (fun d => rsum n (fun c => bin db 0 d a * bin db 1 a c)) := by
                      rw [rsum_comm]
              _ = rsum n (fun d => bin db 0 d a * rsum n (fun c => bin db 1 a c)) := by
                      apply rsum_congr; intro d hd
                      rw [rsum_const_mul]
              _ = (rsum n (fun d => bin db 0 d a)) * (rsum n (fun c => bin db 1 a c)) := by
                      rw [rsum_mul_const]
      _ ≤ (rsum n (fun a => rsum n (fun d => bin db 0 d a))) *
          (rsum n (fun a => rsum n (fun c => bin db 1 a c))) := by
            apply rsum_mul_le_mul_rsum
            · intro a ha
              apply rsum_nonneg; intro d hd
              exact bin_nonneg db 0 d a
            · intro a ha
              apply rsum_nonneg; intro c hc
              exact bin_nonneg db 1 a c
      _ = e0 * e1 := by
            dsimp only [e0, e1]
            rw [show rsum n (fun x => rsum n (fun y => bin db 0 x y)) =
                rsum n (fun a => rsum n (fun d => bin db 0 d a)) by rw [rsum_comm]]
  have hterm2 : ∀ a c d : Nat,
      bin db 0 d a * (bin db 1 a c * bin db 2 c d) ≤ bin db 1 a c * bin db 2 c d := by
    intro a c d
    have h0 : 0 ≤ bin db 0 d a := bin_nonneg db 0 d a
    have h01 : bin db 0 d a ≤ 1 := bin_le_one db 0 d a
    have h12 : 0 ≤ bin db 1 a c * bin db 2 c d :=
      Int.mul_nonneg (bin_nonneg db 1 a c) (bin_nonneg db 2 c d)
    have hh := Int.mul_le_mul h01 (Int.le_refl (bin db 1 a c * bin db 2 c d)) h12 (by omega)
    calc
      bin db 0 d a * (bin db 1 a c * bin db 2 c d)
          = bin db 0 d a * (bin db 1 a c * bin db 2 c d) := rfl
      _ ≤ 1 * (bin db 1 a c * bin db 2 c d) := hh
      _ = bin db 1 a c * bin db 2 c d := by rw [Int.one_mul]
  have h2I : t ≤ e1 * e2 := by
    have hsum : t ≤ rsum n (fun c => rsum n (fun a => rsum n (fun d =>
        bin db 1 a c * bin db 2 c d))) := by
      dsimp only [t]
      calc
        rsum n (fun a => rsum n (fun c => rsum n (fun d =>
            bin db 0 d a * (bin db 1 a c * bin db 2 c d))))
            = rsum n (fun c => rsum n (fun a => rsum n (fun d =>
                bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by rw [rsum_comm]
        _ ≤ rsum n (fun c => rsum n (fun a => rsum n (fun d =>
                bin db 1 a c * bin db 2 c d))) := by
                apply rsum_le; intro c hc
                apply rsum_le; intro a ha
                apply rsum_le; intro d hd
                exact hterm2 a c d
    calc
      t ≤ rsum n (fun c => rsum n (fun a => rsum n (fun d =>
          bin db 1 a c * bin db 2 c d))) := hsum
      _ = rsum n (fun c => rsum n (fun a =>
          bin db 1 a c * rsum n (fun d => bin db 2 c d))) := by
            apply rsum_congr; intro c hc
            apply rsum_congr; intro a ha
            rw [rsum_const_mul]
      _ = rsum n (fun c => (rsum n (fun a => bin db 1 a c)) *
          (rsum n (fun d => bin db 2 c d))) := by
            apply rsum_congr; intro c hc
            rw [rsum_mul_const]
      _ ≤ (rsum n (fun c => rsum n (fun a => bin db 1 a c))) *
          (rsum n (fun c => rsum n (fun d => bin db 2 c d))) := by
            apply rsum_mul_le_mul_rsum
            · intro c hc
              apply rsum_nonneg; intro a ha
              exact bin_nonneg db 1 a c
            · intro c hc
              apply rsum_nonneg; intro d hd
              exact bin_nonneg db 2 c d
      _ = e1 * e2 := by
            dsimp only [e1, e2]
            rw [show rsum n (fun x => rsum n (fun y => bin db 1 x y)) =
                rsum n (fun c => rsum n (fun a => bin db 1 a c)) by rw [rsum_comm]]
  have hterm3 : ∀ a c d : Nat,
      bin db 0 d a * (bin db 1 a c * bin db 2 c d) ≤ bin db 0 d a * bin db 2 c d := by
    intro a c d
    have h0 : 0 ≤ bin db 0 d a := bin_nonneg db 0 d a
    have h1n : 0 ≤ bin db 1 a c := bin_nonneg db 1 a c
    have h11 : bin db 1 a c ≤ 1 := bin_le_one db 1 a c
    have h02 : 0 ≤ bin db 0 d a * bin db 2 c d :=
      Int.mul_nonneg h0 (bin_nonneg db 2 c d)
    have hh := Int.mul_le_mul (Int.le_refl (bin db 0 d a * bin db 2 c d)) h11 h1n h02
    calc
      bin db 0 d a * (bin db 1 a c * bin db 2 c d)
          = (bin db 0 d a * bin db 2 c d) * bin db 1 a c := by ac_rfl
      _ ≤ (bin db 0 d a * bin db 2 c d) * 1 := hh
      _ = bin db 0 d a * bin db 2 c d := by rw [Int.mul_one]
  have h3I : t ≤ e0 * e2 := by
    have hsum : t ≤ rsum n (fun d => rsum n (fun c => rsum n (fun a =>
        bin db 0 d a * bin db 2 c d))) := by
      dsimp only [t]
      calc
        rsum n (fun a => rsum n (fun c => rsum n (fun d =>
            bin db 0 d a * (bin db 1 a c * bin db 2 c d))))
            = rsum n (fun c => rsum n (fun a => rsum n (fun d =>
                bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by rw [rsum_comm]
        _ = rsum n (fun c => rsum n (fun d => rsum n (fun a =>
                bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by
                apply rsum_congr; intro c hc
                rw [rsum_comm]
        _ = rsum n (fun d => rsum n (fun c => rsum n (fun a =>
                bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by rw [rsum_comm]
        _ ≤ rsum n (fun d => rsum n (fun c => rsum n (fun a =>
                bin db 0 d a * bin db 2 c d))) := by
                apply rsum_le; intro d hd
                apply rsum_le; intro c hc
                apply rsum_le; intro a ha
                exact hterm3 a c d
    calc
      t ≤ rsum n (fun d => rsum n (fun c => rsum n (fun a =>
          bin db 0 d a * bin db 2 c d))) := hsum
      _ = rsum n (fun d => (rsum n (fun a => bin db 0 d a)) *
          (rsum n (fun c => bin db 2 c d))) := by
            apply rsum_congr; intro d hd
            calc
              rsum n (fun c => rsum n (fun a => bin db 0 d a * bin db 2 c d))
                  = rsum n (fun c => (rsum n (fun a => bin db 0 d a)) * bin db 2 c d) := by
                      apply rsum_congr; intro c hc
                      rw [rsum_mul_const]
              _ = (rsum n (fun a => bin db 0 d a)) * (rsum n (fun c => bin db 2 c d)) := by
                      rw [rsum_const_mul]
      _ ≤ (rsum n (fun d => rsum n (fun a => bin db 0 d a))) *
          (rsum n (fun d => rsum n (fun c => bin db 2 c d))) := by
            apply rsum_mul_le_mul_rsum
            · intro d hd
              apply rsum_nonneg; intro a ha
              exact bin_nonneg db 0 d a
            · intro d hd
              apply rsum_nonneg; intro c hc
              exact bin_nonneg db 2 c d
      _ = e0 * e2 := by
            dsimp only [e0, e2]
            rw [show rsum n (fun d => rsum n (fun c => bin db 2 c d)) =
                rsum n (fun x => rsum n (fun y => bin db 2 x y)) by rw [rsum_comm]]
  let P : Nat → Nat → Int := fun d c => rsum n (fun a => bin db 0 d a * bin db 1 a c)
  have ht_pairs : t = rsum n (fun d => rsum n (fun c => P d c * bin db 2 c d)) := by
    dsimp only [t, P]
    calc
      rsum n (fun a => rsum n (fun c => rsum n (fun d =>
          bin db 0 d a * (bin db 1 a c * bin db 2 c d))))
          = rsum n (fun c => rsum n (fun a => rsum n (fun d =>
              bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by rw [rsum_comm]
      _ = rsum n (fun c => rsum n (fun d => rsum n (fun a =>
              bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by
              apply rsum_congr; intro c hc
              rw [rsum_comm]
      _ = rsum n (fun d => rsum n (fun c => rsum n (fun a =>
              bin db 0 d a * (bin db 1 a c * bin db 2 c d)))) := by rw [rsum_comm]
      _ = rsum n (fun d => rsum n (fun c =>
              (rsum n (fun a => bin db 0 d a * bin db 1 a c)) * bin db 2 c d)) := by
              apply rsum_congr; intro d hd
              apply rsum_congr; intro c hc
              rw [show (fun a => bin db 0 d a * (bin db 1 a c * bin db 2 c d)) =
                  (fun a => (bin db 0 d a * bin db 1 a c) * bin db 2 c d) by
                    funext a
                    ac_rfl]
              rw [rsum_mul_const]
  let pairs : List (Nat × Nat) :=
    (List.range n).flatMap (fun d => (List.range n).map (fun c => (d, c)))
  have hcs := cs_list (pairs.map (fun p => (P p.1 p.2, bin db 2 p.2 p.1)))
  have hmap1 : (pairs.map (fun p => (P p.1 p.2, bin db 2 p.2 p.1))).map
        (fun q : Int × Int => q.1 * q.2) =
      pairs.map (fun p => P p.1 p.2 * bin db 2 p.2 p.1) := by
    simp [List.map_map]
  have hmap2 : (pairs.map (fun p => (P p.1 p.2, bin db 2 p.2 p.1))).map
        (fun q : Int × Int => q.1 * q.1) =
      pairs.map (fun p => P p.1 p.2 * P p.1 p.2) := by
    simp [List.map_map]
  have hmap3 : (pairs.map (fun p => (P p.1 p.2, bin db 2 p.2 p.1))).map
        (fun q : Int × Int => q.2 * q.2) =
      pairs.map (fun p => bin db 2 p.2 p.1 * bin db 2 p.2 p.1) := by
    simp [List.map_map]
  rw [hmap1, hmap2, hmap3] at hcs
  have hsumP : (pairs.map (fun p => P p.1 p.2 * bin db 2 p.2 p.1)).sum = t := by
    rw [← rsum_pairs n (fun d c => P d c * bin db 2 c d)]
    exact ht_pairs.symm
  have hPpoint : ∀ d c a a' : Nat,
      (bin db 0 d a * bin db 1 a c) * (bin db 0 d a' * bin db 1 a' c) ≤
        bin db 0 d a * bin db 1 a' c := by
    intro d c a a'
    have hA0 : 0 ≤ bin db 0 d a := bin_nonneg db 0 d a
    have hB0 : 0 ≤ bin db 1 a c := bin_nonneg db 1 a c
    have hB1 : bin db 1 a c ≤ 1 := bin_le_one db 1 a c
    have hA0' : 0 ≤ bin db 0 d a' := bin_nonneg db 0 d a'
    have hA1' : bin db 0 d a' ≤ 1 := bin_le_one db 0 d a'
    have hB0' : 0 ≤ bin db 1 a' c := bin_nonneg db 1 a' c
    have hprod : bin db 1 a c * bin db 0 d a' ≤ 1 := by
      have hh := Int.mul_le_mul hB1 hA1' hA0' (by omega : (0 : Int) ≤ 1)
      simpa using hh
    have hprod0 : 0 ≤ bin db 1 a c * bin db 0 d a' := Int.mul_nonneg hB0 hA0'
    have hbase0 : 0 ≤ bin db 0 d a * bin db 1 a' c := Int.mul_nonneg hA0 hB0'
    have hh := Int.mul_le_mul (Int.le_refl (bin db 0 d a * bin db 1 a' c)) hprod
      hprod0 hbase0
    calc
      (bin db 0 d a * bin db 1 a c) * (bin db 0 d a' * bin db 1 a' c)
          = (bin db 0 d a * bin db 1 a' c) * (bin db 1 a c * bin db 0 d a') := by ac_rfl
      _ ≤ (bin db 0 d a * bin db 1 a' c) * 1 := hh
      _ = bin db 0 d a * bin db 1 a' c := by rw [Int.mul_one]
  have hsumP2le : (pairs.map (fun p => P p.1 p.2 * P p.1 p.2)).sum ≤ e0 * e1 := by
    rw [← rsum_pairs n (fun d c => P d c * P d c)]
    dsimp only [P]
    calc
      rsum n (fun d => rsum n (fun c =>
          (rsum n (fun a => bin db 0 d a * bin db 1 a c)) *
          (rsum n (fun a' => bin db 0 d a' * bin db 1 a' c))))
          = rsum n (fun d => rsum n (fun c =>
              rsum n (fun a => rsum n (fun a' =>
                (bin db 0 d a * bin db 1 a c) * (bin db 0 d a' * bin db 1 a' c))))) := by
              apply rsum_congr; intro d hd
              apply rsum_congr; intro c hc
              rw [rsum_product]
      _ ≤ rsum n (fun d => rsum n (fun c =>
              rsum n (fun a => rsum n (fun a' => bin db 0 d a * bin db 1 a' c)))) := by
              apply rsum_le; intro d hd
              apply rsum_le; intro c hc
              apply rsum_le; intro a ha
              apply rsum_le; intro a' ha'
              exact hPpoint d c a a'
      _ = rsum n (fun d => rsum n (fun c =>
              (rsum n (fun a => bin db 0 d a)) * (rsum n (fun a' => bin db 1 a' c)))) := by
              apply rsum_congr; intro d hd
              apply rsum_congr; intro c hc
              rw [rsum_product]
      _ = rsum n (fun d => (rsum n (fun a => bin db 0 d a)) *
            (rsum n (fun c => rsum n (fun a' => bin db 1 a' c)))) := by
            apply rsum_congr; intro d hd
            rw [rsum_const_mul]
      _ = (rsum n (fun d => rsum n (fun a => bin db 0 d a))) *
          (rsum n (fun c => rsum n (fun a' => bin db 1 a' c))) := by
            rw [rsum_mul_const]
      _ = e0 * e1 := by
            dsimp only [e0, e1]
            rw [show rsum n (fun x => rsum n (fun y => bin db 1 x y)) =
                rsum n (fun c => rsum n (fun a' => bin db 1 a' c)) by rw [rsum_comm]]
  have hsumC2 : (pairs.map (fun p => bin db 2 p.2 p.1 * bin db 2 p.2 p.1)).sum = e2 := by
    rw [← rsum_pairs n (fun d c => bin db 2 c d * bin db 2 c d)]
    calc
      rsum n (fun d => rsum n (fun c => bin db 2 c d * bin db 2 c d))
          = rsum n (fun d => rsum n (fun c => bin db 2 c d)) := by
              apply rsum_congr; intro d hd
              apply rsum_congr; intro c hc
              exact bin_sq db 2 c d
      _ = e2 := by
              dsimp only [e2]
              rw [rsum_comm]
  have h4I : t ^ 2 ≤ e0 * e1 * e2 := by
    rw [← hsumP]
    calc
      ((pairs.map (fun p => P p.1 p.2 * bin db 2 p.2 p.1)).sum) ^ 2
          ≤ (pairs.map (fun p => P p.1 p.2 * P p.1 p.2)).sum *
            (pairs.map (fun p => bin db 2 p.2 p.1 * bin db 2 p.2 p.1)).sum := hcs
      _ ≤ (e0 * e1) * e2 := by
            have hCnon : 0 ≤ (pairs.map (fun p => bin db 2 p.2 p.1 * bin db 2 p.2 p.1)).sum := by
              rw [← rsum_pairs n (fun d c => bin db 2 c d * bin db 2 c d)]
              apply rsum_nonneg; intro d hd
              apply rsum_nonneg; intro c hc
              exact Int.mul_nonneg (bin_nonneg db 2 c d) (bin_nonneg db 2 c d)
            exact Int.mul_le_mul hsumP2le (by rw [hsumC2]; omega) hCnon (Int.mul_nonneg he0 he1)
      _ = e0 * e1 * e2 := rfl
  let E0 : Nat := support n db (⟨0, 1, 2⟩ : Atom) pattern b
  let E1 : Nat := support n db (⟨1, 2, 3⟩ : Atom) pattern b
  let E2 : Nat := support n db (⟨2, 3, 1⟩ : Atom) pattern b
  let T : Nat := (reference pattern n (bin db) b).toNat
  have hbin : bin db = (fun r x y => if db r x y = 0 then 0 else 1) := by
    funext r x y
    simp [bin, beq_iff_eq]
  have hTgoal : T = (reference pattern n (fun r x y => if db r x y = 0 then 0 else 1) b).toNat := by
    dsimp only [T]
    rw [hbin]
  have hTcast : (T : Int) = t := by
    dsimp only [T]
    rw [reference_bin n db b]
    exact Int.toNat_of_nonneg ht0
  have hE0cast : (E0 : Int) = e0 := by dsimp only [E0]; exact hs0
  have hE1cast : (E1 : Int) = e1 := by dsimp only [E1]; exact hs1
  have hE2cast : (E2 : Int) = e2 := by dsimp only [E2]; exact hs2
  rw [show ((pattern.atoms.zip [w0, w1, w2]).map
        (fun (a, w) => support n db a pattern b ^ w)).prod =
      E0 ^ w0 * E1 ^ w1 * E2 ^ w2 by
        simp [pattern, E0, E1, E2, Nat.mul_assoc]]
  have h1n : T ≤ E0 * E1 := by
    apply Int.ofNat_le.mp
    rw [hTcast]
    rw [show ((E0 * E1 : Nat) : Int) = (E0 : Int) * (E1 : Int) by rw [Int.natCast_mul]]
    rw [hE0cast, hE1cast]
    exact h1I
  have h2n : T ≤ E1 * E2 := by
    apply Int.ofNat_le.mp
    rw [hTcast]
    rw [show ((E1 * E2 : Nat) : Int) = (E1 : Int) * (E2 : Int) by rw [Int.natCast_mul]]
    rw [hE1cast, hE2cast]
    exact h2I
  have h3n : T ≤ E0 * E2 := by
    apply Int.ofNat_le.mp
    rw [hTcast]
    rw [show ((E0 * E2 : Nat) : Int) = (E0 : Int) * (E2 : Int) by rw [Int.natCast_mul]]
    rw [hE0cast, hE2cast]
    exact h3I
  have h4n : T ^ 2 ≤ E0 * E1 * E2 := by
    apply Int.ofNat_le.mp
    rw [show ((T ^ 2 : Nat) : Int) = (T : Int) ^ 2 by simp]
    rw [show ((E0 * E1 * E2 : Nat) : Int) = (E0 : Int) * (E1 : Int) * (E2 : Int) by
      rw [Int.natCast_mul, Int.natCast_mul]]
    rw [hTcast, hE0cast, hE1cast, hE2cast]
    exact h4I
  by_cases hz0 : E0 = 0
  · have hTz : T = 0 := Nat.eq_zero_of_le_zero (by simpa [hz0] using h1n)
    have hz : T ^ den ≤ E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by
      simp [hTz, Nat.zero_pow hden]
    rw [hTgoal] at hz
    simpa [T, E0, E1, E2, pattern] using hz
  by_cases hz1 : E1 = 0
  · have hTz : T = 0 := Nat.eq_zero_of_le_zero (by simpa [hz1] using h2n)
    have hz : T ^ den ≤ E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by
      simp [hTz, Nat.zero_pow hden]
    rw [hTgoal] at hz
    simpa [T, E0, E1, E2, pattern] using hz
  by_cases hz2 : E2 = 0
  · have hTz : T = 0 := Nat.eq_zero_of_le_zero (by simpa [hz2] using h3n)
    have hz : T ^ den ≤ E0 ^ w0 * E1 ^ w1 * E2 ^ w2 := by
      simp [hTz, Nat.zero_pow hden]
    rw [hTgoal] at hz
    simpa [T, E0, E1, E2, pattern] using hz
  have hE0p : 1 ≤ E0 := by omega
  have hE1p : 1 ≤ E1 := by omega
  have hE2p : 1 ≤ E2 := by omega
  have hfinal := weighted_bound T E0 E1 E2 hE0p hE1p hE2p h1n h2n h3n h4n
    den hden w0 w1 w2 hc2 hc3 hc1
  rw [hTgoal] at hfinal
  simpa [T, E0, E1, E2, pattern] using hfinal

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.MovieCreditsCapsAndCollisionCorrection
