import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans4

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

/-! Small list and finite-sum toolkit. -/

private theorem sum_map_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (fun b => F a b)).sum)).sum =
      (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      exact (sum_zero l').symm
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, sum_add]

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Int) :
    rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int}
    (h : ∀ i, i < n → f i = g i) : rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  exact sum_add (List.range n) f g

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  exact sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  exact sum_mul_right (List.range n) f c

private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) =
      rsum m (fun j => rsum n (fun i => f i j)) := by
  exact sum_swap (List.range n) (List.range m) f

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := rsum_mul_const n f (rsum m g)

private theorem rsum_swap23 (n : Nat) (f : Nat → Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => rsum n (fun k => f i j k))) =
      rsum n (fun i => rsum n (fun k => rsum n (fun j => f i j k))) := by
  apply rsum_congr
  intro i hi
  exact rsum_comm n n (fun j k => f i j k)

private theorem rsum_swap34 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => rsum n (fun k => rsum n (fun l => f i j k l)))) =
      rsum n (fun i => rsum n (fun j => rsum n (fun l => rsum n (fun k => f i j k l)))) := by
  apply rsum_congr
  intro i hi
  apply rsum_congr
  intro j hj
  exact rsum_comm n n (fun k l => f i j k l)

private theorem rsum_prod3 (n m k : Nat) (f g h : Nat → Int) :
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l))) =
      rsum n f * rsum m g * rsum k h := by
  calc
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l)))
        = rsum n (fun i => rsum m (fun j => (f i * g j) * rsum k h)) := by
            apply rsum_congr; intro i hi
            apply rsum_congr; intro j hj
            rw [rsum_const_mul]
    _ = rsum n (fun i => (rsum m (fun j => f i * g j)) * rsum k h) := by
            apply rsum_congr; intro i hi
            rw [rsum_mul_const]
    _ = rsum n (fun i => (f i * rsum m g) * rsum k h) := by
            apply rsum_congr; intro i hi
            rw [rsum_const_mul]
    _ = rsum n (fun i => f i * rsum m g) * rsum k h := rsum_mul_const n _ _
    _ = (rsum n f * rsum m g) * rsum k h := by rw [rsum_mul_const]

private theorem rsum_factor_two_pairs (n : Nat) (P Q R S T U V : Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      rsum n (fun e => P x a * (Q x b * (R x c * (S x d * (T x e * (U a b * V c d))))))))))) =
    rsum n (fun x => (rsum n (fun a => rsum n (fun b => P x a * (Q x b * U a b)))) *
      (rsum n (fun c => rsum n (fun d => R x c * (S x d * V c d)))) * rsum n (T x)) := by
  apply rsum_congr
  intro x hx
  have h1 : ∀ a b c d,
      rsum n (fun e => P x a * (Q x b * (R x c * (S x d * (T x e * (U a b * V c d)))))) =
        (P x a * (Q x b * (R x c * (S x d * (U a b * V c d))))) * rsum n (T x) := by
    intro a b c d
    rw [show rsum n (fun e => P x a * (Q x b * (R x c * (S x d * (T x e * (U a b * V c d)))))) =
        rsum n (fun e => (P x a * (Q x b * (R x c * (S x d * (U a b * V c d))))) * T x e) by
          apply rsum_congr; intro e he; ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h1]
  have h2 : ∀ a b c,
      rsum n (fun d => (P x a * (Q x b * (R x c * (S x d * (U a b * V c d))))) * rsum n (T x)) =
        (P x a * (Q x b * (R x c * U a b)) * rsum n (T x)) *
          rsum n (fun d => S x d * V c d) := by
    intro a b c
    rw [show rsum n (fun d => (P x a * (Q x b * (R x c * (S x d * (U a b * V c d))))) * rsum n (T x)) =
        rsum n (fun d => (P x a * (Q x b * (R x c * U a b)) * rsum n (T x)) *
          (S x d * V c d)) by
          apply rsum_congr; intro d hd; ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h2]
  have h3 : ∀ a b,
      rsum n (fun c => (P x a * (Q x b * (R x c * U a b)) * rsum n (T x)) *
          rsum n (fun d => S x d * V c d)) =
        (P x a * Q x b * rsum n (T x)) *
          (U a b * rsum n (fun c => R x c * rsum n (fun d => S x d * V c d))) := by
    intro a b
    rw [show rsum n (fun c => (P x a * (Q x b * (R x c * U a b)) * rsum n (T x)) *
          rsum n (fun d => S x d * V c d)) =
        rsum n (fun c => (P x a * Q x b * rsum n (T x)) *
          (U a b * (R x c * rsum n (fun d => S x d * V c d)))) by
          apply rsum_congr; intro c hc; ac_rfl]
    simpa only [Int.mul_assoc] using rsum_const_mul n
      (P x a * Q x b * rsum n (T x) * U a b)
      (fun c => R x c * rsum n (fun d => S x d * V c d))
  simp only [h3]
  have h4 : ∀ a,
      rsum n (fun b => (P x a * Q x b * rsum n (T x)) *
          (U a b * rsum n (fun c => R x c * rsum n (fun d => S x d * V c d)))) =
        (P x a * rsum n (T x) *
          rsum n (fun c => R x c * rsum n (fun d => S x d * V c d))) *
            rsum n (fun b => Q x b * U a b) := by
    intro a
    rw [show rsum n (fun b => (P x a * Q x b * rsum n (T x)) *
          (U a b * rsum n (fun c => R x c * rsum n (fun d => S x d * V c d)))) =
        rsum n (fun b => (P x a * rsum n (T x) *
          rsum n (fun c => R x c * rsum n (fun d => S x d * V c d))) * (Q x b * U a b)) by
          apply rsum_congr; intro b hb; ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h4]
  rw [show rsum n (fun a => (P x a * rsum n (T x) *
      rsum n (fun c => R x c * rsum n (fun d => S x d * V c d))) *
        rsum n (fun b => Q x b * U a b)) =
      (rsum n (T x) * rsum n (fun c => R x c * rsum n (fun d => S x d * V c d))) *
        rsum n (fun a => P x a * rsum n (fun b => Q x b * U a b)) by
      rw [show rsum n (fun a => (P x a * rsum n (T x) *
          rsum n (fun c => R x c * rsum n (fun d => S x d * V c d))) *
            rsum n (fun b => Q x b * U a b)) =
          rsum n (fun a => (rsum n (T x) *
            rsum n (fun c => R x c * rsum n (fun d => S x d * V c d))) *
              (P x a * rsum n (fun b => Q x b * U a b))) by
            apply rsum_congr; intro a ha; ac_rfl]
      rw [rsum_const_mul]]
  have hR : rsum n (fun c => R x c * rsum n (fun d => S x d * V c d)) =
      rsum n (fun c => rsum n (fun d => R x c * (S x d * V c d))) := by
    apply rsum_congr; intro c hc
    exact (rsum_const_mul n (R x c) (fun d => S x d * V c d)).symm
  rw [hR]
  have hL : rsum n (fun a => P x a * rsum n (fun b => Q x b * U a b)) =
      rsum n (fun a => rsum n (fun b => P x a * (Q x b * U a b))) := by
    apply rsum_congr; intro a ha
    exact (rsum_const_mul n (P x a) (fun b => Q x b * U a b)).symm
  rw [hL]
  ac_rfl

private theorem rsum_factor_paths (n : Nat) (P A B C D E F : Nat → Nat → Int) :
    rsum n (fun y => rsum n (fun z => rsum n (fun u => rsum n (fun w => rsum n (fun v =>
      rsum n (fun q => P y z * (A z w * (B w u * (C u y * (D y v * (E v q * F q z))))))))))) =
    rsum n (fun y => rsum n (fun z => P y z *
      rsum n (fun u => rsum n (fun w => A z w * (B w u * C u y))) *
        rsum n (fun v => rsum n (fun q => D y v * (E v q * F q z))))) := by
  apply rsum_congr
  intro y hy
  apply rsum_congr
  intro z hz
  have h1 : ∀ u w v,
      rsum n (fun q => P y z * (A z w * (B w u * (C u y * (D y v * (E v q * F q z)))))) =
        (P y z * (A z w * (B w u * (C u y * D y v)))) * rsum n (fun q => E v q * F q z) := by
    intro u w v
    rw [show rsum n (fun q => P y z * (A z w * (B w u * (C u y * (D y v * (E v q * F q z)))))) =
        rsum n (fun q => (P y z * (A z w * (B w u * (C u y * D y v)))) * (E v q * F q z)) by
          apply rsum_congr; intro q hq; ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h1]
  have h2 : ∀ u w,
      rsum n (fun v => (P y z * (A z w * (B w u * (C u y * D y v)))) *
          rsum n (fun q => E v q * F q z)) =
        (P y z * (A z w * (B w u * C u y))) * rsum n (fun v => D y v * rsum n (fun q => E v q * F q z)) := by
    intro u w
    rw [show rsum n (fun v => (P y z * (A z w * (B w u * (C u y * D y v)))) *
          rsum n (fun q => E v q * F q z)) =
        rsum n (fun v => (P y z * (A z w * (B w u * C u y))) *
          (D y v * rsum n (fun q => E v q * F q z))) by
          apply rsum_congr; intro v hv; ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h2]
  have h3 : ∀ u,
      rsum n (fun w => (P y z * (A z w * (B w u * C u y))) *
          rsum n (fun v => D y v * rsum n (fun q => E v q * F q z))) =
        (P y z * rsum n (fun v => D y v * rsum n (fun q => E v q * F q z))) *
          rsum n (fun w => A z w * (B w u * C u y)) := by
    intro u
    rw [show rsum n (fun w => (P y z * (A z w * (B w u * C u y))) *
          rsum n (fun v => D y v * rsum n (fun q => E v q * F q z))) =
        rsum n (fun w => (P y z * rsum n (fun v => D y v * rsum n (fun q => E v q * F q z))) *
          (A z w * (B w u * C u y))) by
          apply rsum_congr; intro w hw; ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h3]
  rw [rsum_const_mul]
  have hR : rsum n (fun v => D y v * rsum n (fun q => E v q * F q z)) =
      rsum n (fun v => rsum n (fun q => D y v * (E v q * F q z))) := by
    apply rsum_congr; intro v hv
    exact (rsum_const_mul n (D y v) (fun q => E v q * F q z)).symm
  rw [hR]
  ac_rfl

private theorem rsum_nonneg (n : Nat) (f : Nat → Int)
    (h : ∀ i, i < n → 0 ≤ f i) : 0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_pos_iff (n : Nat) (f : Nat → Int)
    (h : ∀ i, i < n → 0 ≤ f i) :
    0 < rsum n f ↔ ∃ i, i < n ∧ 0 < f i := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have htail := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      constructor
      · intro hp
        have hnon := h n (Nat.lt_succ_self n)
        by_cases hn : 0 < f n
        · exact ⟨n, Nat.lt_succ_self n, hn⟩
        · have hz : f n = 0 := by omega
          have hp' : 0 < rsum n f := by simpa [hz] using hp
          rcases htail.mp hp' with ⟨i, hi, hfi⟩
          exact ⟨i, Nat.lt_succ_of_lt hi, hfi⟩
      · rintro ⟨i, hi, hfi⟩
        have hlast := h n (Nat.lt_succ_self n)
        have hr := rsum_nonneg n f (fun j hj => h j (Nat.lt_succ_of_lt hj))
        rcases Nat.lt_succ_iff_lt_or_eq.mp hi with hi | rfl
        · have hp := htail.mpr ⟨i, hi, hfi⟩
          omega
        · omega

private theorem int_mul_pos_of_nonneg {a b : Int} (ha : 0 ≤ a) (hb : 0 ≤ b) :
    0 < a * b ↔ 0 < a ∧ 0 < b := by
  constructor
  · intro h
    by_cases hza : a = 0
    · simp [hza] at h
    · by_cases hzb : b = 0
      · simp [hzb] at h
      · exact ⟨by omega, by omega⟩
  · rintro ⟨ha', hb'⟩
    by_cases hza : a = 0
    · simp [hza] at ha'
    · by_cases hzb : b = 0
      · simp [hzb] at hb'
      · exact Int.mul_pos ha' hb'

/-! Binding updates and integration. -/

private theorem put_pinned (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i = v
  · subst i; simp [put]
  · simp [put, h]

private theorem put_get_other (b : Bind) (v w x : Nat) (h : w ≠ v) :
    put b v x w = b w := by
  have hb : (w == v) = false := by
    cases h' : (w == v) <;> simp_all
  simp [put, hb]

private theorem put_src_dst (b : Bind) (src dst u v : Nat) (h : src ≠ dst) :
    put (put b src u) dst v src = u := by
  rw [put_get_other (put b src u) dst src v h]
  simp [put]

private theorem put_keep_other (b : Bind) (v w x : Nat) (hvw : v ≠ w) :
    put (put b v x) w (b w) = put b v x := by
  funext i
  by_cases hi : i = w
  · subst i
    have hwv : (w == v) = false := by cases h' : (w == v) <;> simp_all
    simp [put, hwv]
  · by_cases hv : i = v
    · subst i
      have hvw' : (v == w) = false := by cases h' : (v == w) <;> simp_all
      simp [put, hvw']
    · simp [put, hi, hv]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : v ≠ w) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i = v
  · subst i
    have hw : (v == w) = false := by cases h' : (v == w) <;> simp_all
    simp [put, hw]
  · by_cases hw : i = w
    · subst i
      have hv' : (w == v) = false := by cases h' : (w == v) <;> simp_all
      simp [put, hv']
    · simp [put, hv, hw]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind)
    (f g : Bind → Int) (h : ∀ x, f x = g x) :
    integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      exact sum_map_congr _ _ _ (fun x _ => ih (put b v x))

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v = w
  · subst w; rfl
  · change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    rw [sum_swap]
    apply sum_map_congr
    intro x hx
    apply sum_map_congr
    intro y hy
    rw [put_comm_ne b v w y x h]

private theorem integrate_perm (n : Nat) {vs vs' : List Nat} (h : vs.Perm vs') :
    ∀ (b : Bind) (f : Bind → Int), integrate n vs b f = integrate n vs' b f := by
  induction h with
  | nil => intro b f; rfl
  | cons x h ih =>
      intro b f
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      apply sum_map_congr
      intro t ht
      exact ih (put b x t) f
  | swap x y l =>
      intro b f
      exact integrate_swap n y x l b f
  | trans h1 h2 ih1 ih2 =>
      intro b f
      rw [ih1 b f, ih2 b f]

private theorem integrate_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [integrate_swap]
      change ((List.range n).map (fun x => integrate n (v :: ws) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum
      exact sum_map_congr _ _ _ (fun x _ => ih (put b w x))

private theorem integrate_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind)
    (f : Bind → Int) :
    integrate n (vs ++ [v]) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      change ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n ws (put b w x)
          (fun b' => integrate n [v] b' f))).sum
      exact sum_map_congr _ _ _ (fun x _ => ih (put b w x))

private theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind)
    (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) =
      integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add]

private theorem integrate_neg (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b (fun x => -f x) = -integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x)
        (fun y => -f y))).sum = -((List.range n).map (fun x =>
          integrate n vs (put b v x) f)).sum
      rw [sum_map_congr _ (fun x => integrate n vs (put b v x) (fun y => -f y))
        (fun x => -integrate n vs (put b v x) f) (by
          intro x hx
          exact ih (put b v x))]
      simpa using sum_mul_left (List.range n) (-1) (fun x => integrate n vs (put b v x) f)

private theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) =
      integrate n vs b f - integrate n vs b g := by
  have heq : (fun x => f x - g x) = (fun x => f x + -g x) := by
    funext x; rw [Int.sub_eq_add_neg]
  rw [heq, integrate_add, integrate_neg, Int.sub_eq_add_neg]

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind)
    (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_left]

private theorem integrate_mul_right (n : Nat) (vs : List Nat) (b : Bind)
    (f : Bind → Int) (c : Int) :
    integrate n vs b (fun x => f x * c) = integrate n vs b f * c := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_right]

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x)
        (fun _ => (0 : Int)))).sum = 0
      rw [sum_map_congr _ (fun _ => integrate n vs _ (fun _ => (0 : Int)))
        (fun _ => 0) (fun x _ => ih (put b v x)), sum_zero]

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons, integrate_add, ih]

private theorem integrate_fixed (n : Nat) (vs : List Nat) (b : Bind) (v : Nat)
    (hv : v ∉ vs) (F : Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) x) =
      integrate n vs b (fun x => F (b v) x) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      have hvw : v ≠ w := by intro h; subst w; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      change ((List.range n).map (fun t => integrate n ws (put b w t)
        (fun x => F (x v) x))).sum =
        ((List.range n).map (fun t => integrate n ws (put b w t)
          (fun x => F (b v) x))).sum
      apply sum_map_congr
      intro t ht
      rw [ih (put b w t) hvws, put_get_other b w v t hvw]

private theorem integrate_fixed2 (n : Nat) (vs : List Nat) (b : Bind) (v w : Nat)
    (hv : v ∉ vs) (hw : w ∉ vs) (F : Nat → Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) (x w) x) =
      integrate n vs b (fun x => F (b v) (b w) x) := by
  calc
    integrate n vs b (fun x => F (x v) (x w) x)
        = integrate n vs b (fun x => F (b v) (x w) x) :=
          integrate_fixed n vs b v hv (fun u x => F u (x w) x)
    _ = integrate n vs b (fun x => F (b v) (b w) x) :=
          integrate_fixed n vs b w hw (fun u x => F (b v) u x)

/-! Variable elimination is integration in a different order. -/

private def Respects (f : Factor) : Prop :=
  ∀ b b', (∀ v, f.scope.contains v = true → b v = b' v) → f.get b = f.get b'

private theorem respects_value (db : DB) (a : Atom) :
    Respects ⟨[a.src, a.dst], value db a⟩ := by
  intro b b' h
  simp only [value]
  rw [h a.src (by simp), h a.dst (by simp)]

private theorem product_respects_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    {b b' : Bind} (heq : ∀ f ∈ fs, ∀ v, f.scope.contains v = true → b v = b' v) :
    product fs b = product fs b' := by
  induction fs with
  | nil => rfl
  | cons f fs ih =>
      simp only [product, List.map_cons, List.prod_cons]
      rw [h f (by simp) b b' (heq f (by simp))]
      change f.get b' * product fs b = f.get b' * product fs b'
      rw [ih (fun g hg => h g (by simp [hg])) (fun g hg => heq g (by simp [hg]))]

private theorem product_put_eq {fs : List Factor} (h : ∀ f ∈ fs, Respects f)
    (hnot : ∀ f ∈ fs, f.scope.contains v = false) (b : Bind) (x : Nat) :
    product fs (put b v x) = product fs b := by
  apply product_respects_eq h
  intro f hf w hw
  by_cases hwv : w = v
  · subst w; rw [hnot f hf] at hw; simp at hw
  · simp [put, hwv]

private theorem partition_loop_spec (p : α → Bool) : ∀ (l a b : List α),
    (List.partition.loop p l (a, b)).fst = a.reverse ++ l.filter p ∧
    (List.partition.loop p l (a, b)).snd = b.reverse ++ l.filter (fun x => !p x)
  | [], a, b => by simp [List.partition.loop]
  | x :: xs, a, b => by
      by_cases h : p x = true
      · simp [List.partition.loop, h, partition_loop_spec p xs]
      · simp [List.partition.loop, h, partition_loop_spec p xs]

private theorem partition_filters (p : α → Bool) (l : List α) :
    (List.partition p l).fst = l.filter p ∧
    (List.partition p l).snd = l.filter (fun x => !p x) := by
  simpa [List.partition] using partition_loop_spec p l [] []

private theorem mem_partition_p (p : α → Bool) {l : List α} {a : α} :
    a ∈ (List.partition p l).fst → p a = true := by
  rw [(partition_filters p l).1]
  intro h
  exact (List.mem_filter.mp h).2

private theorem mem_partition_notp (p : α → Bool) {l : List α} {a : α} :
    a ∈ (List.partition p l).snd → p a = false := by
  rw [(partition_filters p l).2]
  intro h
  have hn := (List.mem_filter.mp h).2
  by_cases hp : p a = true
  · simp [hp] at hn
  · simpa using hp

private theorem product_filter_mul (p : α → Bool) (l : List α) (g : α → Int) :
    ((l.filter p).map g).prod * ((l.filter (fun x => !p x)).map g).prod =
      (l.map g).prod := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a = true <;> simp [h, ih, Int.mul_assoc, Int.mul_left_comm]

private theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    ((List.partition p l).fst.map (fun f => f.get b)).prod *
      ((List.partition p l).snd.map (fun f => f.get b)).prod =
    (l.map (fun f => f.get b)).prod := by
  rw [(partition_filters p l).1, (partition_filters p l).2]
  exact product_filter_mul p l (fun f => f.get b)

private theorem mem_of_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l := by
  exact (List.mem_partition (p := fun g : Factor => g.scope.contains v)
    (l := l) (a := f)).mpr (Or.inl hf)

private theorem mem_of_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l := by
  exact (List.mem_partition (p := fun g : Factor => g.scope.contains v)
    (l := l) (a := f)).mpr (Or.inr hf)

private theorem respects_msg (n v : Nat) (inc : List Factor)
    (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  change ((List.range n).map (fun x => product inc (put b v x))).sum =
    ((List.range n).map (fun x => product inc (put b' v x))).sum
  apply sum_map_congr
  intro x hx
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w = v
  · subst w; simp [put]
  · simp [put, hwv]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwErase : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    have hmem : w ∈ (inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v) :=
      List.mem_filter.mpr ⟨hwErase, by simp [hwv]⟩
    simpa using hmem

private theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (eliminate n v fs) b = integrate n [v] b (product fs) := by
  dsimp only [eliminate]
  let inc := (fs.partition (fun f : Factor => f.scope.contains v)).fst
  let rest := (fs.partition (fun f : Factor => f.scope.contains v)).snd
  have hinc_res : ∀ f ∈ inc, Respects f := by
    intro f hf; exact hres f (mem_of_partition_fst hf)
  have hrest_res : ∀ f ∈ rest, Respects f := by
    intro f hf; exact hres f (mem_of_partition_snd hf)
  have hrest_not : ∀ f ∈ rest, f.scope.contains v = false := by
    intro f hf; exact mem_partition_notp (fun g : Factor => g.scope.contains v) hf
  change (integrate n [v] b (fun x => product inc x)) * product rest b =
    integrate n [v] b (product fs)
  change ((List.range n).map (fun x => product inc (put b v x))).sum * product rest b =
    ((List.range n).map (fun x => product fs (put b v x))).sum
  rw [← sum_mul_right]
  apply sum_map_congr
  intro x hx
  rw [← product_put_eq hrest_res hrest_not b x]
  change product inc (put b v x) * product rest (put b v x) = product fs (put b v x)
  unfold product
  rw [← product_partition_get (fun f : Factor => f.scope.contains v) fs (put b v x)]

private theorem respects_eliminate (n v : Nat) (fs : List Factor)
    (hres : ∀ f ∈ fs, Respects f) : ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  dsimp only [eliminate] at hf
  simp only [List.mem_cons] at hf
  rcases hf with rfl | hf
  · exact respects_msg n v (fs.partition (fun g : Factor => g.scope.contains v)).fst
      (by intro g hg; exact hres g (mem_of_partition_fst hg))
  · exact hres f (mem_of_partition_snd hf)

private theorem product_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor)
    (b : Bind) (hres : ∀ f ∈ fs, Respects f) :
    product (vs.foldl (fun fs v => eliminate n v fs) fs) b =
      integrate n vs b (product fs) := by
  induction vs generalizing fs with
  | nil => rfl
  | cons v vs ih =>
      change product (vs.foldl (fun fs v => eliminate n v fs) (eliminate n v fs)) b =
        integrate n (v :: vs) b (product fs)
      rw [ih (eliminate n v fs) (respects_eliminate n v fs hres),
        integrate_cons_singleton]
      exact integrate_congr n vs b _ _ (fun b' => product_eliminate n v fs b' hres)

private theorem elimination_eq_reference (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    elimination p n db b = reference p n db b := by
  unfold elimination reference
  rw [product_fold_eliminate]
  · exact integrate_congr n p.freeVars b _ _ (fun x => by simp [factors, product, Function.comp_def])
  · intro f hf
    rcases List.mem_map.mp hf with ⟨a, ha, rfl⟩
    exact respects_value db a

/-! Telescoping product differences and anchored repair. -/

private def cpExcl (atoms : List Atom) (old new : DB) (i : Nat) (b : Bind) : Int :=
  ((atoms.take i).map (fun a => value new a b)).prod *
    ((atoms.drop (i+1)).map (fun a => value old a b)).prod

private theorem cpExcl_zero (a : Atom) (as : List Atom) (old new : DB) (b : Bind) :
    cpExcl (a::as) old new 0 b = (as.map (fun t => value old t b)).prod := by
  simp [cpExcl]

private theorem cpExcl_succ (a : Atom) (as : List Atom) (old new : DB) (i : Nat) (b : Bind) :
    cpExcl (a::as) old new (i+1) b = value new a b * cpExcl as old new i b := by
  simp [cpExcl, Int.mul_assoc]

private theorem zipIdx_succ_map {α} (l : List α) (n : Nat) :
    l.zipIdx (n+1) = (l.zipIdx n).map (fun x => (x.1, x.2+1)) := by
  induction l generalizing n with
  | nil => rfl
  | cons a l ih => rw [List.zipIdx_cons, List.zipIdx_cons, ih]; rfl

private theorem prod_diff_telescope (atoms : List Atom) (old new : DB) (b : Bind) :
    (atoms.map (fun a => value new a b)).prod - (atoms.map (fun a => value old a b)).prod =
      ((atoms.zipIdx.map (fun (a, i) =>
        (value new a b - value old a b) * cpExcl atoms old new i b)).sum) := by
  induction atoms with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.prod_cons, List.zipIdx_cons]
      rw [cpExcl_zero a as old new b]
      have hzip : (as.zipIdx (0+1)).map (fun (x : Atom × Nat) =>
          (value new x.1 b - value old x.1 b) * cpExcl (a :: as) old new x.2 b) =
          (as.zipIdx.map (fun (t, i) =>
            (value new t b - value old t b) * (value new a b * cpExcl as old new i b))) := by
        rw [zipIdx_succ_map as 0]
        simp only [List.map_map, Function.comp_def, cpExcl_succ]
      rw [hzip]
      have hcomm : (as.zipIdx.map (fun (t, i) =>
            (value new t b - value old t b) * (value new a b * cpExcl as old new i b))).sum =
          value new a b * (as.zipIdx.map (fun (t, i) =>
            (value new t b - value old t b) * cpExcl as old new i b)).sum := by
        rw [← sum_mul_left]
        apply sum_map_congr
        intro x hx
        rcases x with ⟨t, i⟩
        ac_rfl
      rw [List.sum_cons, hcomm, ← ih, Int.sub_mul, Int.mul_sub]
      omega

private theorem pivot_point (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hsd : a.src ≠ a.dst) (u w : Nat) :
    let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
    let b' := put (put b a.src u) a.dst w
    integrate n vars b' (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      (new a.rel u w - old a.rel u w) *
        integrate n vars b' (fun x => cpExcl p.atoms old new j x) := by
  dsimp only
  let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
  let b' := put (put b a.src u) a.dst w
  have hsrc_not : a.src ∉ vars := by
    dsimp only [vars]; intro h; have hm := (List.mem_filter.mp h).2; simp at hm
  have hdst_not : a.dst ∉ vars := by
    dsimp only [vars]; intro h; have hm := (List.mem_filter.mp h).2; simp at hm
  have hsrc_b : b' a.src = u := put_src_dst b a.src a.dst u w hsd
  have hdst_b : b' a.dst = w := by simp [b', put]
  change integrate n vars b'
      (fun x => (new a.rel (x a.src) (x a.dst) - old a.rel (x a.src) (x a.dst)) *
        cpExcl p.atoms old new j x) =
    (new a.rel u w - old a.rel u w) * integrate n vars b' (fun x => cpExcl p.atoms old new j x)
  rw [integrate_fixed2 n vars b' a.src a.dst hsrc_not hdst_not
    (fun u v x => (new a.rel u v - old a.rel u v) * cpExcl p.atoms old new j x), hsrc_b, hdst_b,
    integrate_mul_left]

private theorem filter_ne_eq_erase (vs : List Nat) (a : Nat) (hnd : vs.Nodup) :
    vs.filter (fun v => v != a) = vs.erase a := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      by_cases hxa : x == a
      · have hx : x = a := Nat.eq_of_beq_eq_true hxa
        subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        simp [List.erase_cons_head, hna, ih hp.2]
      · have hxa' : (x != a) = true := by simpa using hxa
        simp [List.erase_cons_tail hxa, hxa', ih hp.2]

private theorem erase_erase_filter (vs : List Nat) (a b : Nat) (hnd : vs.Nodup) :
    (vs.erase a).erase b = vs.filter (fun v => v != a && v != b) := by
  rw [← filter_ne_eq_erase vs a hnd]
  have hnd' : (vs.filter (fun v : Nat => v != a)).Nodup := hnd.filter _
  rw [← filter_ne_eq_erase (vs.filter (fun v : Nat => v != a)) b hnd', List.filter_filter]
  apply List.filter_congr
  intro x hx
  cases hv : (x == a) <;> cases hw : (x == b) <;> simp_all [Bool.and_comm]

private theorem integrate_extract (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (hnd : vs.Nodup) (hv : v ∈ vs) :
    integrate n vs b f = integrate n [v] b (fun b' => integrate n (vs.erase v) b' f) := by
  induction vs generalizing b f with
  | nil => simp at hv
  | cons w ws ih =>
      by_cases hwv : w == v
      · have hwv' : w = v := Nat.eq_of_beq_eq_true hwv
        subst w
        rw [List.erase_cons_head]
        rfl
      · have hvw : v ∈ ws := by
          simp only [List.mem_cons] at hv
          rcases hv with rfl | hv
          · simp at hwv
          · exact hv
        have hndws : ws.Nodup := (List.nodup_cons.mp hnd).2
        calc
          integrate n (w :: ws) b f
              = integrate n ws b (fun b' => integrate n [w] b' f) :=
                integrate_cons_singleton n w ws b f
          _ = integrate n [v] b (fun b' => integrate n (ws.erase v) b'
                (fun b'' => integrate n [w] b'' f)) :=
                ih b (fun b' => integrate n [w] b' f) hndws hvw
          _ = integrate n [v] b (fun b' => integrate n (w :: ws.erase v) b' f) := by
                apply integrate_congr; intro b'
                exact (integrate_cons_singleton n w (ws.erase v) b' f).symm
          _ = integrate n [v] b (fun b' => integrate n ((w :: ws).erase v) b' f) := by
                rw [List.erase_cons_tail hwv]

private theorem sum_filter_eq {α} (l : List α) (p : α → Bool) (g : α → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : p x = true
      · simp [hx, ih (fun y hy hp => h y (by simp [hy]) hp)]
      · have hx0 : g x = 0 := h x (by simp) (by cases h' : p x <;> simp_all)
        simp [hx, hx0, ih (fun y hy hp => h y (by simp [hy]) hp)]

private theorem pivot_integral_eq (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) :
    integrate n p.freeVars b
        (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      pivotTerm p n old new b j a := by
  let vs := p.freeVars
  let vars := vs.filter (fun v => v != a.src && v != a.dst)
  let f : Bind → Int := fun x =>
    (value new a x - value old a x) * cpExcl p.atoms old new j x
  have hsrc_not : a.src ∉ vars := by simp [vars]
  have hdst_not : a.dst ∉ vars := by simp [vars]
  by_cases hs : a.src ∈ vs
  · by_cases hd : a.dst ∈ vs
    · have hd_erase : a.dst ∈ vs.erase a.src := by
        rw [← filter_ne_eq_erase vs a.src hnd]
        exact List.mem_filter.mpr ⟨hd, by simp [hsd.symm]⟩
      calc
        integrate n vs b f
            = integrate n [a.src] b (fun b' => integrate n (vs.erase a.src) b' f) :=
              integrate_extract n a.src vs b f hnd hs
        _ = integrate n [a.src] b (fun b' => integrate n [a.dst] b'
              (fun b'' => integrate n ((vs.erase a.src).erase a.dst) b'' f)) := by
              apply integrate_congr; intro b'
              exact integrate_extract n a.dst (vs.erase a.src) b' f (hnd.erase _) hd_erase
        _ = integrate n [a.src] b (fun b' => integrate n [a.dst] b'
              (fun b'' => integrate n vars b'' f)) := by
              apply integrate_congr; intro b'
              apply integrate_congr; intro b''
              rw [show (vs.erase a.src).erase a.dst = vars from
                erase_erase_filter vs a.src a.dst hnd]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              have hsc : p.freeVars.contains a.src = true := List.contains_iff_mem.mpr hs
              have hdc : p.freeVars.contains a.dst = true := List.contains_iff_mem.mpr hd
              rw [hsc, hdc]
              change ((List.range n).map (fun u => ((List.range n).map (fun v =>
                integrate n vars (put (put b a.src u) a.dst v) f)).sum)).sum =
                ((List.range n).map (fun u => (((List.range n).filter
                  (fun v => old a.rel u v != new a.rel u v)).map
                    (fun v => (new a.rel u v - old a.rel u v) *
                      integrate n vars (put (put b a.src u) a.dst v)
                        (fun x => cpExcl p.atoms old new j x))).sum)).sum
              apply congrArg List.sum
              apply List.map_congr_left
              intro u hu
              have hfac : (fun v => integrate n vars (put (put b a.src u) a.dst v) f) =
                  (fun v => (new a.rel u v - old a.rel u v) *
                    integrate n vars (put (put b a.src u) a.dst v)
                      (fun x => cpExcl p.atoms old new j x)) := by
                funext v
                exact pivot_point p n old new b j a hsd u v
              rw [hfac]
              exact (sum_filter_eq _ _ _ (by
                intro v hv hp
                have heq : old a.rel u v = new a.rel u v := by
                  cases h' : (old a.rel u v == new a.rel u v) <;> simp_all
                rw [heq]; simp)).symm
    · have hvars : vs.erase a.src = vars := by
        rw [← filter_ne_eq_erase vs a.src hnd]
        apply List.filter_congr
        intro x hx
        by_cases hxd : x = a.dst
        · subst x; exact False.elim (hd hx)
        · simp [hxd]
      calc
        integrate n vs b f
            = integrate n [a.src] b (fun b' => integrate n (vs.erase a.src) b' f) :=
              integrate_extract n a.src vs b f hnd hs
        _ = integrate n [a.src] b (fun b' => integrate n vars b' f) := by
              apply integrate_congr; intro b'; rw [hvars]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              have hsc : p.freeVars.contains a.src = true := List.contains_iff_mem.mpr hs
              have hdc : p.freeVars.contains a.dst = false := by simpa using hd
              rw [hsc, hdc]
              simp only [Bool.false_eq_true, ite_false, ite_true, List.map_singleton,
                List.sum_singleton, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil,
                Int.add_zero]
              change ((List.range n).map (fun u => integrate n vars (put b a.src u) f)).sum =
                ((List.range n).map (fun u => ((([b a.dst].filter
                  (fun v => old a.rel u v != new a.rel u v)).map
                    (fun v => (new a.rel u v - old a.rel u v) *
                      integrate n vars (put (put b a.src u) a.dst v)
                        (fun x => cpExcl p.atoms old new j x))).sum))).sum
              apply sum_map_congr
              intro u hu
              let G : Nat → Int := fun v => (new a.rel u v - old a.rel u v) *
                integrate n vars (put (put b a.src u) a.dst v)
                  (fun x => cpExcl p.atoms old new j x)
              change integrate n vars (put b a.src u) f =
                ((([b a.dst].filter (fun v => old a.rel u v != new a.rel u v)).map G).sum)
              rw [sum_filter_eq [b a.dst] (fun v => old a.rel u v != new a.rel u v) G (by
                intro v hv hp
                have heq : old a.rel u v = new a.rel u v := by
                  cases h' : (old a.rel u v == new a.rel u v) <;> simp_all
                dsimp only [G]
                rw [heq]; simp)]
              simp only [List.map_singleton, List.sum_singleton]
              dsimp only [G]
              rw [put_keep_other b a.src a.dst u hsd]
              simpa only [f, put_keep_other b a.src a.dst u hsd] using
                pivot_point p n old new b j a hsd u (b a.dst)
  · by_cases hd : a.dst ∈ vs
    · have hvars : vs.erase a.dst = vars := by
        rw [← filter_ne_eq_erase vs a.dst hnd]
        apply List.filter_congr
        intro x hx
        by_cases hxs : x = a.src
        · subst x; exact False.elim (hs hx)
        · simp [hxs]
      calc
        integrate n vs b f
            = integrate n [a.dst] b (fun b' => integrate n (vs.erase a.dst) b' f) :=
              integrate_extract n a.dst vs b f hnd hd
        _ = integrate n [a.dst] b (fun b' => integrate n vars b' f) := by
              apply integrate_congr; intro b'; rw [hvars]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              have hsc : p.freeVars.contains a.src = false := by simpa using hs
              have hdc : p.freeVars.contains a.dst = true := List.contains_iff_mem.mpr hd
              rw [hsc, hdc]
              simp only [Bool.false_eq_true, ite_false, ite_true, List.filter_cons,
                beq_self_eq_true, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil,
                Int.add_zero]
              change ((List.range n).map (fun w => integrate n vars (put b a.dst w) f)).sum =
                (((List.range n).filter (fun v =>
                    old a.rel (b a.src) v != new a.rel (b a.src) v)).map
                    (fun v => (new a.rel (b a.src) v - old a.rel (b a.src) v) *
                      integrate n vars (put (put b a.src (b a.src)) a.dst v)
                        (fun x => cpExcl p.atoms old new j x))).sum
              let G : Nat → Int := fun v =>
                (new a.rel (b a.src) v - old a.rel (b a.src) v) *
                  integrate n vars (put (put b a.src (b a.src)) a.dst v)
                    (fun x => cpExcl p.atoms old new j x)
              change ((List.range n).map (fun w => integrate n vars (put b a.dst w) f)).sum =
                (((List.range n).filter
                  (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v)).map G).sum
              rw [sum_filter_eq (List.range n)
                (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v) G (by
                  intro v hv hp
                  have heq : old a.rel (b a.src) v = new a.rel (b a.src) v := by
                    cases h' : (old a.rel (b a.src) v == new a.rel (b a.src) v) <;> simp_all
                  dsimp only [G]
                  rw [heq]; simp)]
              apply sum_map_congr
              intro w hw
              dsimp only [G]
              rw [put_pinned b a.src]
              simpa only [f, put_pinned b a.src, put_keep_other b a.src a.dst w hsd] using
                pivot_point p n old new b j a hsd (b a.src) w
    · have hvars : vs = vars := by
        have hf : vs.filter (fun v => v != a.src && v != a.dst) = vs := by
          apply List.filter_eq_self.mpr
          intro x hx
          have hxs : x ≠ a.src := by intro h; subst x; exact hs hx
          have hxd : x ≠ a.dst := by intro h; subst x; exact hd hx
          simp [hxs, hxd]
        exact hf.symm
      calc
        integrate n vs b f = integrate n vars b f := by rw [hvars]
        _ = pivotTerm p n old new b j a := by
              dsimp only [f, pivotTerm]
              have hsc : p.freeVars.contains a.src = false := by simpa using hs
              have hdc : p.freeVars.contains a.dst = false := by simpa using hd
              rw [hsc, hdc]
              simp only [Bool.false_eq_true, ite_false, List.map_singleton, List.sum_singleton]
              let G : Nat → Int := fun v =>
                (new a.rel (b a.src) v - old a.rel (b a.src) v) *
                  integrate n vars (put (put b a.src (b a.src)) a.dst v)
                    (fun x => cpExcl p.atoms old new j x)
              change integrate n vars b f =
                ((([b a.dst].filter
                  (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v)).map G).sum)
              rw [sum_filter_eq [b a.dst]
                (fun v => old a.rel (b a.src) v != new a.rel (b a.src) v) G (by
                  intro v hv hp
                  have heq : old a.rel (b a.src) v = new a.rel (b a.src) v := by
                    cases h' : (old a.rel (b a.src) v == new a.rel (b a.src) v) <;> simp_all
                  dsimp only [G]
                  rw [heq]; simp)]
              simp only [List.map_singleton, List.sum_singleton]
              dsimp only [G]
              rw [put_pinned b a.src, put_pinned b a.dst]
              simpa only [f, put_pinned b a.src, put_pinned b a.dst] using
                pivot_point p n old new b j a hsd (b a.src) (b a.dst)

private theorem anchored_repair_generic (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (hnd : p.freeVars.Nodup) (hsd : ∀ a ∈ p.atoms, a.src ≠ a.dst) :
    reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
  have htel : ∀ x : Bind,
      (p.atoms.map (fun a => value new a x)).prod - (p.atoms.map (fun a => value old a x)).prod =
        ((p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) :=
    fun x => prod_diff_telescope p.atoms old new x
  calc
    reference p n new b - reference p n old b
        = integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value new a x)).prod) -
            integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value old a x)).prod) := rfl
    _ = integrate n p.freeVars b (fun x =>
          (p.atoms.map (fun a => value new a x)).prod -
          (p.atoms.map (fun a => value old a x)).prod) := by rw [integrate_sub]
    _ = integrate n p.freeVars b (fun x => (p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) := by
          exact integrate_congr n p.freeVars b _ _ htel
    _ = (p.atoms.zipIdx.map (fun (a, i) => integrate n p.freeVars b (fun x =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x))).sum :=
          integrate_list_sum n p.freeVars b _ _
    _ = (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
          apply sum_map_congr
          intro ai hai
          rcases ai with ⟨a, i⟩
          have hmem : a ∈ p.atoms := by
            have hz := List.mem_zipIdx' hai
            rw [hz.2]
            exact List.get_mem p.atoms ⟨i, hz.1⟩
          exact pivot_integral_eq p n old new b i a hnd (hsd a hmem)

private theorem bridge_pos_iff (n : Nat) (db : DB) (r s x y : Nat)
    (h : ∀ r i j, 0 ≤ db r i j) :
    0 < bridge n db r s x y ↔ ∃ k, k < n ∧ 0 < db r x k ∧ 0 < db s k y := by
  change 0 < rsum n (fun k => db r x k * db s k y) ↔
    ∃ k, k < n ∧ 0 < db r x k ∧ 0 < db s k y
  rw [rsum_pos_iff]
  · constructor
    · rintro ⟨k, hk, hp⟩
      exact ⟨k, hk, (int_mul_pos_of_nonneg (h r x k) (h s k y)).mp hp⟩
    · rintro ⟨k, hk, h1, h2⟩
      exact ⟨k, hk, (int_mul_pos_of_nonneg (h r x k) (h s k y)).mpr ⟨h1, h2⟩⟩
  · intro i hi
    exact Int.mul_nonneg (h r x i) (h s i y)

private theorem twohop_reference_factor (n : Nat) (db : DB) (b : Bind) :
    reference TwoHopAntiWitness.pattern n db b =
      rsum n (fun z => bridge n db 0 1 (b 0) z * bridge n db 2 1 (b 0) z *
        total n (db 0 z)) := by
  simp [reference, integrate, TwoHopAntiWitness.pattern, value, put, bridge, total]
  change rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x2 =>
    db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) =
    rsum n (fun z => (rsum n (fun k => db 0 (b 0) k * db 1 k z)) *
      (rsum n (fun k => db 2 (b 0) k * db 1 k z)) * rsum n (db 0 z))
  calc
    rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x2 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4)))))))
        = rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x4 =>
            db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) :=
          rsum_swap34 n _
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) := by
          apply rsum_congr; intro x1 hx1
          exact rsum_comm n n (fun x3 x2 => rsum n (fun x4 =>
            db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4)))))
    _ = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) :=
          rsum_comm n n _
    _ = rsum n (fun z => (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 z)) *
          (rsum n (fun x3 => db 2 (b 0) x3 * db 1 x3 z)) * rsum n (db 0 z)) := by
          apply rsum_congr
          intro z hz
          simpa only [Int.mul_assoc] using rsum_prod3 n n n
            (fun x1 => db 0 (b 0) x1 * db 1 x1 z)
            (fun x3 => db 2 (b 0) x3 * db 1 x3 z) (db 0 z)

theorem sound_TwoHopAntiWitness_IndependentWitness : TwoHopAntiWitness.IndependentWitness.Goal := by
  intro n db b hnonneg
  rw [twohop_reference_factor]
  change 0 < rsum n (fun z =>
    (rsum n (fun k => db 0 (b 0) k * db 1 k z)) *
      (rsum n (fun k => db 2 (b 0) k * db 1 k z)) * rsum n (db 0 z)) ↔
    ∃ z, z < n ∧ 0 < rsum n (fun k => db 0 (b 0) k * db 1 k z) ∧
      0 < rsum n (fun k => db 2 (b 0) k * db 1 k z) ∧ 0 < rsum n (db 0 z)
  rw [rsum_pos_iff]
  · constructor
    · rintro ⟨z, hz, hp⟩
      have hpos := int_mul_pos_of_nonneg
        (a := rsum n (fun i => db 0 (b 0) i * db 1 i z) *
          rsum n (fun i => db 2 (b 0) i * db 1 i z))
        (b := total n (db 0 z))
        (Int.mul_nonneg (rsum_nonneg n _ (fun i hi =>
          Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
          (rsum_nonneg n _ (fun i hi =>
            Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z))))
        (rsum_nonneg n (db 0 z) (fun i hi => hnonneg 0 z i))
      have h1 := hpos.mp hp
      have hb := int_mul_pos_of_nonneg
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z)))
      have hbridges := hb.mp (And.left h1)
      exact ⟨z, hz, And.left hbridges, And.right hbridges, And.right h1⟩
    · rintro ⟨z, hz, h1, h2, h3⟩
      have hb := int_mul_pos_of_nonneg
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z)))
      have hpos := int_mul_pos_of_nonneg
        (a := rsum n (fun i => db 0 (b 0) i * db 1 i z) *
          rsum n (fun i => db 2 (b 0) i * db 1 i z))
        (b := total n (db 0 z))
        (Int.mul_nonneg (rsum_nonneg n _ (fun i hi =>
          Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
          (rsum_nonneg n _ (fun i hi =>
            Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z))))
        (rsum_nonneg n (db 0 z) (fun i hi => hnonneg 0 z i))
      exact ⟨z, hz, hpos.mpr ⟨hb.mpr ⟨h1, h2⟩, h3⟩⟩
  · intro i hi
    exact Int.mul_nonneg
      (Int.mul_nonneg (rsum_nonneg n _ (fun j hj =>
        Int.mul_nonneg (hnonneg 0 (b 0) j) (hnonneg 1 j i)))
        (rsum_nonneg n _ (fun j hj =>
          Int.mul_nonneg (hnonneg 2 (b 0) j) (hnonneg 1 j i))))
      (rsum_nonneg n (db 0 i) (fun j hj => hnonneg 0 i j))

theorem sound_HubWithChords_ChordComponents : HubWithChords.ChordComponents.Goal := by
  intro n db b
  unfold reference
  have hperm : HubWithChords.pattern.freeVars.Perm [0,1,2,3,4,5] := by decide
  rw [integrate_perm n hperm b]
  simp [integrate, HubWithChords.pattern, value, put, total, dot]
  change rsum n (fun x => rsum n (fun a => rsum n (fun b' => rsum n (fun c =>
    rsum n (fun d => rsum n (fun e =>
      db 0 x a * (db 1 x b' * (db 2 x c * (db 3 x d * (db 4 x e * (db 5 a b' * db 6 c d))))))))))) =
    rsum n (fun x => (rsum n (fun a => db 0 x a * rsum n (fun b' => db 5 a b' * db 1 x b'))) *
      (rsum n (fun c => db 2 x c * rsum n (fun d => db 6 c d * db 3 x d))) * rsum n (db 4 x))
  have hEq :
      rsum n (fun x =>
        (rsum n (fun a => db 0 x a * rsum n (fun b' => db 5 a b' * db 1 x b'))) *
        (rsum n (fun c => db 2 x c * rsum n (fun d => db 6 c d * db 3 x d))) *
        rsum n (db 4 x)) =
      rsum n (fun x =>
        (rsum n (fun a => rsum n (fun b' => db 0 x a * (db 1 x b' * db 5 a b')))) *
        (rsum n (fun c => rsum n (fun d => db 2 x c * (db 3 x d * db 6 c d)))) *
        rsum n (db 4 x)) := by
    apply rsum_congr
    intro x hx
    rw [show rsum n (fun a => db 0 x a * rsum n (fun b' => db 5 a b' * db 1 x b')) =
        rsum n (fun a => rsum n (fun b' => db 0 x a * (db 1 x b' * db 5 a b'))) by
          apply rsum_congr; intro a ha
          rw [rsum_const_mul]
          have hb : rsum n (fun b' => db 5 a b' * db 1 x b') =
              rsum n (fun b' => db 1 x b' * db 5 a b') := by
            apply rsum_congr
            intro b' hb'
            ac_rfl
          rw [hb]]
    rw [show rsum n (fun c => db 2 x c * rsum n (fun d => db 6 c d * db 3 x d)) =
        rsum n (fun c => rsum n (fun d => db 2 x c * (db 3 x d * db 6 c d))) by
          apply rsum_congr; intro c hc
          rw [rsum_const_mul]
          have hd : rsum n (fun d => db 6 c d * db 3 x d) =
              rsum n (fun d => db 3 x d * db 6 c d) := by
            apply rsum_congr
            intro d hd
            ac_rfl
          rw [hd]]
  rw [hEq, rsum_factor_two_pairs]

theorem sound_IntersectingCycles_TwoSeparators : IntersectingCycles.TwoSeparators.Goal := by
  intro n db b
  unfold reference
  have hperm : IntersectingCycles.pattern.freeVars.Perm [2,3,1,0,4,5] := by decide
  rw [integrate_perm n hperm b]
  simp [integrate, IntersectingCycles.pattern, value, put, bridge, total]
  have hfold : ∀ f : Nat → Int, ((List.range n).map f).sum = rsum n f := fun _ => rfl
  simp only [hfold]
  have hreorder :
      rsum n (fun x => rsum n (fun x_1 => rsum n (fun x_2 => rsum n (fun x_3 => rsum n (fun x_4 => rsum n (fun x_5 =>
        db 0 x_3 x_2 * (db 0 x_2 x * (db 0 x x_1 * (db 0 x_1 x_3 * (db 1 x x_4 * (db 2 x_4 x_5 * db 2 x_5 x_1))))))))))) =
      rsum n (fun y => rsum n (fun z => rsum n (fun u => rsum n (fun w => rsum n (fun v => rsum n (fun q =>
        db 0 y z * (db 0 z w * (db 0 w u * (db 0 u y * (db 1 y v * (db 2 v q * db 2 q z))))))))))) := by
    apply rsum_congr; intro x hx
    apply rsum_congr; intro x1 hx1
    apply rsum_congr; intro x2 hx2
    apply rsum_congr; intro x3 hx3
    apply rsum_congr; intro x4 hx4
    apply rsum_congr; intro x5 hx5
    ac_rfl
  rw [hreorder, rsum_factor_paths]
  apply rsum_congr
  intro y hy
  apply rsum_congr
  intro z hz
  have hFirst : rsum n (fun u => rsum n (fun w => db 0 z w * (db 0 w u * db 0 u y))) =
      rsum n (fun u => (rsum n (fun w => db 0 z w * db 0 w u)) * db 0 u y) := by
    apply rsum_congr
    intro u hu
    have : rsum n (fun w => db 0 z w * (db 0 w u * db 0 u y)) =
        rsum n (fun w => (db 0 z w * db 0 w u) * db 0 u y) := by
      apply rsum_congr; intro w hw; ac_rfl
    rw [this, rsum_mul_const n (fun w => db 0 z w * db 0 w u) (db 0 u y)]
  have hSecond : rsum n (fun v => rsum n (fun q => db 1 y v * (db 2 v q * db 2 q z))) =
      rsum n (fun v => (rsum n (fun q => db 1 y q * db 2 q v)) * db 2 v z) := by
    rw [rsum_comm n n (fun v q => db 1 y v * (db 2 v q * db 2 q z))]
    change rsum n (fun v => rsum n (fun q => db 1 y q * (db 2 q v * db 2 v z))) = rsum n (fun v => (rsum n (fun q => db 1 y q * db 2 q v)) * db 2 v z)
    apply rsum_congr
    intro v hv
    have : rsum n (fun q => db 1 y q * (db 2 q v * db 2 v z)) =
        rsum n (fun q => (db 1 y q * db 2 q v) * db 2 v z) := by
      apply rsum_congr; intro q hq; ac_rfl
    rw [this, rsum_mul_const n (fun q => db 1 y q * db 2 q v) (db 2 v z)]
  rw [hFirst, hSecond]

/-! The five boundary-message goals. -/

theorem sound_TwoHopAntiWitness_BoundaryMessages : TwoHopAntiWitness.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference TwoHopAntiWitness.pattern n db b

theorem sound_HubWithChords_BoundaryMessages : HubWithChords.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference HubWithChords.pattern n db b

theorem sound_SupplierClique_BoundaryMessages : SupplierClique.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference SupplierClique.pattern n db b

theorem sound_IntersectingCycles_BoundaryMessages : IntersectingCycles.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference IntersectingCycles.pattern n db b

theorem sound_ParallelLineage_BoundaryMessages : ParallelLineage.BoundaryMessages.Goal := by
  intro n db b
  exact elimination_eq_reference ParallelLineage.pattern n db b

/-! The five anchored-repair goals. -/

theorem sound_TwoHopAntiWitness_AnchoredRepair : TwoHopAntiWitness.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic TwoHopAntiWitness.pattern n old new b (by decide) (by decide)

theorem sound_HubWithChords_AnchoredRepair : HubWithChords.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic HubWithChords.pattern n old new b (by decide) (by decide)

theorem sound_SupplierClique_AnchoredRepair : SupplierClique.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic SupplierClique.pattern n old new b (by decide) (by decide)

theorem sound_IntersectingCycles_AnchoredRepair : IntersectingCycles.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic IntersectingCycles.pattern n old new b (by decide) (by decide)

theorem sound_ParallelLineage_AnchoredRepair : ParallelLineage.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic ParallelLineage.pattern n old new b (by decide) (by decide)

end TinylambOpt.ResearchProofs.Atlas.JoinPlans4
