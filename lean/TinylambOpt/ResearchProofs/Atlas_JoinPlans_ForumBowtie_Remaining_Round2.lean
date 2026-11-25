import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Round2.Task71

open TinylambOpt.ResearchGoals.Atlas.JoinPlans

private theorem cover_decomp : ∀ den w0 w1 w2 w3 w4 w5 : Nat,
    den ≤ w0 + w1 → den ≤ w1 + w2 → den ≤ w3 + w4 → den ≤ w4 + w5 →
    den ≤ w0 + w2 + w3 + w5 →
    ∃ t1 t2 t3 t4 t5 t6 t7 s1 s4 : Nat,
      t1 + t2 + t3 + t4 + t5 + t6 + t7 + 2 * (s1 + s4) = den ∧
      (t1 + t6 + t7) + s4 ≤ w0 ∧
      (t1 + t2 + t3 + t4 + t5) + 2*s1 + s4 ≤ w1 ∧
      (t2 + t6 + t7) + s4 ≤ w2 ∧
      (t3 + t5 + t7) + s1 ≤ w3 ∧
      (t1 + t2 + t3 + t4 + t6) + s1 + 2*s4 ≤ w4 ∧
      (t4 + t5 + t7) + s1 ≤ w5 := by
  intro den
  induction den using Nat.strongRecOn with
  | ind den ih =>
      intro w0 w1 w2 w3 w4 w5 h0 h1 h3 h4 h5
      by_cases hden : den = 0
      · subst den
        exact ⟨0,0,0,0,0,0,0,0,0, by simp⟩
      by_cases hC1 : 0 < w0 ∧ 0 < w1 ∧ 0 < w4 ∧ den + 1 ≤ w0 + w1
      · rcases ih (den-1) (by omega) (w0-1) (w1-1) w2 w3 (w4-1) w5
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1+1,t2,t3,t4,t5,t6,t7,s1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hC2 : 0 < w1 ∧ 0 < w4 ∧ 0 < w2 ∧ den + 1 ≤ w1 + w2
      · rcases ih (den-1) (by omega) w0 (w1-1) (w2-1) w3 (w4-1) w5
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2+1,t3,t4,t5,t6,t7,s1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hC3 : 0 < w1 ∧ 0 < w4 ∧ 0 < w3 ∧ den + 1 ≤ w3 + w4
      · rcases ih (den-1) (by omega) w0 (w1-1) w2 (w3-1) (w4-1) w5
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2,t3+1,t4,t5,t6,t7,s1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hC4 : 0 < w1 ∧ 0 < w4 ∧ 0 < w5 ∧ den + 1 ≤ w4 + w5
      · rcases ih (den-1) (by omega) w0 (w1-1) w2 w3 (w4-1) (w5-1)
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2,t3,t4+1,t5,t6,t7,s1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hC5 : 0 < w1 ∧ 0 < w3 ∧ 0 < w5 ∧ den + 1 ≤ w0 + w2 + w3 + w5
      · rcases ih (den-1) (by omega) w0 (w1-1) w2 (w3-1) w4 (w5-1)
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2,t3,t4,t5+1,t6,t7,s1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hC6 : 0 < w0 ∧ 0 < w2 ∧ 0 < w4 ∧ den + 1 ≤ w0 + w2 + w3 + w5
      · rcases ih (den-1) (by omega) (w0-1) w1 (w2-1) w3 (w4-1) w5
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2,t3,t4,t5,t6+1,t7,s1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hC7 : 0 < w0 ∧ 0 < w2 ∧ 0 < w3 ∧ 0 < w5 ∧ den + 3 ≤ w0 + w2 + w3 + w5
      · rcases ih (den-1) (by omega) (w0-1) w1 (w2-1) (w3-1) w4 (w5-1)
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2,t3,t4,t5,t6,t7+1,s1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hS1 : 2 ≤ den ∧ 2 ≤ w1 ∧ 0 < w3 ∧ 0 < w4 ∧ 0 < w5
      · rcases ih (den-2) (by omega) w0 (w1-2) w2 (w3-1) (w4-1) (w5-1)
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2,t3,t4,t5,t6,t7,s1+1,s4,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hS4 : 2 ≤ den ∧ 2 ≤ w4 ∧ 0 < w0 ∧ 0 < w1 ∧ 0 < w2
      · rcases ih (den-2) (by omega) (w0-1) (w1-1) (w2-1) w3 (w4-2) w5
          (by omega) (by omega) (by omega) (by omega) (by omega)
          with ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4,hs,hu0,hu1,hu2,hu3,hu4,hu5⟩
        refine ⟨t1,t2,t3,t4,t5,t6,t7,s1,s4+1,?_,?_,?_,?_,?_,?_,?_⟩ <;> omega
      by_cases hp0 : 0 < w0 <;>
      by_cases hp1 : 0 < w1 <;>
      by_cases hp2 : 0 < w2 <;>
      by_cases hp3 : 0 < w3 <;>
      by_cases hp4 : 0 < w4 <;>
      by_cases hp5 : 0 < w5 <;>
      simp [hp0, hp1, hp2, hp3, hp4, hp5] at hC1 hC2 hC3 hC4 hC5 hC6 hC7 hS1 hS4 <;> omega

/-! ---------- finite sums ---------- -/

private abbrev rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
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

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
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
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        simp [rsum]]
      exact (rsum_zero m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

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

private theorem rsum_product (n m : Nat) (f g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr; intro i hi; exact rsum_const_mul m (f i) g
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

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

/-! ---------- Cauchy-Schwarz for lists ---------- -/

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

/-! ---------- finite-support injection machinery ---------- -/

private theorem sum_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_get_other (b : Bind) (v w x : Nat) (h : w ≠ v) :
    put b v x w = b w := by
  have h' : (w == v) = false := by
    cases hw : w == v <;> simp_all
  simp [put, h']

private theorem nodup_map {α β} (l : List α) (f : α → β)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hnd : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [List.nodup_cons] at hnd
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, hfb⟩
        have hab : a = b := hinj a (by simp) b (by simp [hb]) hfb.symm
        subst b
        exact hnd.1 hb
      · exact ih (fun x hx y hy => hinj x (by simp [hx]) y (by simp [hy])) hnd.2

private theorem nodup_filter {α} (p : α → Bool) (l : List α) (hnd : l.Nodup) :
    (l.filter p).Nodup := hnd.filter p

private theorem nodup_flatMap {α β} (l : List α) (f : α → List β)
    (hnd : l.Nodup) (hf : ∀ a ∈ l, (f a).Nodup)
    (hd : ∀ a ∈ l, ∀ b ∈ l, a ≠ b → ∀ x ∈ f a, ∀ y ∈ f b, x ≠ y) :
    (l.flatMap f).Nodup := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [List.flatMap_cons, List.nodup_append]
      refine ⟨hf a (by simp), ih (List.nodup_cons.mp hnd).2
        (fun b hb => hf b (by simp [hb]))
        (fun b hb c hc hbc x hx y hy => hd b (by simp [hb]) c (by simp [hc]) hbc x hx y hy), ?_⟩
      intro x hx y hy hxy
      rcases List.mem_flatMap.mp hy with ⟨c, hc, hyc⟩
      have hac : a ≠ c := by
        intro h
        subst c
        exact (List.nodup_cons.mp hnd).1 hc
      exact hd a (by simp) c (by simp [hc]) hac x hx y hyc hxy

private def bindings (n : Nat) : List Nat → Bind → List Bind
  | [], b => [b]
  | v :: vs, b => (List.range n).flatMap (fun x => bindings n vs (put b v x))

private theorem bindings_fixed (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∉ vs, y v = b v := by
  induction vs generalizing b with
  | nil =>
      intro y hy v hv
      simp [bindings] at hy
      subst y
      rfl
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      have hvw : v ≠ w := by intro h; subst v; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      rw [ih (put b w x) y hy v hvws, put_get_other b w v x hvw]

private theorem bindings_nodup (n : Nat) (vs : List Nat) (b : Bind) (hnd : vs.Nodup) :
    (bindings n vs b).Nodup := by
  induction vs generalizing b with
  | nil => simp [bindings]
  | cons v vs ih =>
      have hv : v ∉ vs := (List.nodup_cons.mp hnd).1
      apply nodup_flatMap
      · exact List.nodup_range
      · intro x hx
        exact ih (put b v x) (List.nodup_cons.mp hnd).2
      · intro x hx y hy hxy u hu z hz heq
        have haa := bindings_fixed n vs (put b v x) u hu v hv
        have hbb := bindings_fixed n vs (put b v y) z hz v hv
        simp only [put_self] at haa hbb
        subst z
        exact hxy (haa.symm.trans hbb)

private theorem sum_flatMap {α β} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons,
      List.sum_cons, ih]

private theorem bindings_sum (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = ((bindings n vs b).map f).sum := by
  induction vs generalizing b with
  | nil => simp [integrate, bindings]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        (((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f).sum
      rw [sum_flatMap]
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem nat_sum_const {α} (l : List α) (c : Nat) :
    (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_mul, Nat.add_comm]

private theorem nat_sum_congr {α} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private def domain (p : Pattern) (n : Nat) (b : Bind) (v : Nat) : List Nat :=
  if p.freeVars.contains v then List.range n else [b v]

private theorem domain_nodup (p : Pattern) (n : Nat) (b : Bind) (v : Nat) :
    (domain p n b v).Nodup := by
  by_cases h : p.freeVars.contains v = true
  · rw [domain, if_pos h]
    exact List.nodup_range
  · have hf : p.freeVars.contains v = false := by cases h' : p.freeVars.contains v <;> simp_all
    rw [domain, if_neg h]
    simp

private def supportList (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom) : List (Nat × Nat) :=
  (domain p n b a.src).flatMap (fun x =>
    ((domain p n b a.dst).filter (fun y => db a.rel x y != 0)).map (fun y => (x, y)))

private theorem supportList_length (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom) :
    (supportList p n db b a).length = support n db a p b := by
  unfold supportList support domain
  rw [List.length_flatMap]
  apply nat_sum_congr
  intro x hx
  rw [List.length_map, ← List.countP_eq_length_filter]

private theorem supportList_nodup (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom) :
    (supportList p n db b a).Nodup := by
  apply nodup_flatMap
  · exact domain_nodup p n b a.src
  · intro x hx
    apply nodup_map
    · intro y hy z hz h
      exact Prod.mk.inj h |>.2
    · exact nodup_filter _ _ (domain_nodup p n b a.dst)
  · intro x hx y hy hxy u hu v hv huv
    rcases List.mem_map.mp hu with ⟨uy, _, huy⟩
    rcases List.mem_map.mp hv with ⟨vy, _, hvy⟩
    rw [← huy, ← hvy] at huv
    exact hxy (Prod.mk.inj huv).1

private def carts {α} : List (List α) → List (List α)
  | [] => [[]]
  | l :: ls => l.flatMap (fun x => (carts ls).map (fun xs => x :: xs))

private theorem carts_length {α} (ls : List (List α)) :
    (carts ls).length = (ls.map List.length).prod := by
  induction ls with
  | nil => simp [carts]
  | cons l ls ih =>
      rw [carts, List.length_flatMap]
      rw [show l.map (fun a => ((carts ls).map (fun xs => a :: xs)).length) =
          l.map (fun _ => (carts ls).length) by
        apply List.map_congr_left
        intro a ha
        exact List.length_map _]
      rw [nat_sum_const, ih, List.map_cons, List.prod_cons]

private theorem carts_mem_cons {α} (l : List α) (ls : List (List α)) (x : α) (xs : List α)
    (hx : x ∈ l) (hxs : xs ∈ carts ls) : x :: xs ∈ carts (l :: ls) := by
  rw [carts]
  exact List.mem_flatMap.mpr ⟨x, hx, List.mem_map.mpr ⟨xs, hxs, rfl⟩⟩

private theorem bindings_range (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∈ vs, y v ∈ List.range n := by
  induction vs generalizing b with
  | nil => simp
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      by_cases hvw : w = v
      · subst w
        by_cases htail : v ∈ ws
        · exact ih (put b v x) y hy v htail
        · have heq := bindings_fixed n ws (put b v x) y hy v htail
          have hx' : y v ∈ List.range n := by
            rw [heq]
            simpa [put] using hx
          exact hx'
      · have htail : v ∈ ws := by
          rcases List.mem_cons.mp hv with h | h
          · exact (hvw h.symm).elim
          · exact h
        exact ih (put b w x) y hy v htail

private theorem binding_domain_mem (p : Pattern) (n : Nat) (b : Bind) :
    ∀ y ∈ bindings n p.freeVars b, ∀ v, y v ∈ domain p n b v := by
  intro y hy v
  by_cases h : p.freeVars.contains v = true
  · have hv : v ∈ p.freeVars := by simpa using h
    rw [domain, if_pos h]
    exact bindings_range n p.freeVars b y hy v hv
  · have hf : p.freeVars.contains v = false := by cases h' : p.freeVars.contains v <;> simp_all
    have hv : v ∉ p.freeVars := by simpa using hf
    rw [domain, if_neg h]
    simp [bindings_fixed n p.freeVars b y hy v hv]

private def bin (db : DB) : DB := fun r x y => if db r x y == 0 then 0 else 1

private def satisfies (p : Pattern) (db : DB) (x : Bind) : Bool :=
  p.atoms.all (fun a => value db a x != 0)

private theorem prod_indicator {α} (l : List α) (q : α → Bool) :
    (l.map (fun a => if q a then (1 : Int) else 0)).prod = if l.all q then 1 else 0 := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : q a = true <;> simp [ha, ih]

private theorem sum_filter_indicator {α} (l : List α) (q : α → Bool) :
    ((l.filter q).map (fun _ => (1 : Int))).sum = (l.filter q).length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : q a = true <;> simp [ha, ih] <;> omega

private def satBindings (p : Pattern) (n : Nat) (db : DB) (b : Bind) : List Bind :=
  (bindings n p.freeVars b).filter (satisfies p (bin db))

private theorem reference_bin_count (p : Pattern) (n : Nat) (db : DB) (b : Bind) :
    reference p n (bin db) b = (satBindings p n db b).length := by
  rw [reference, bindings_sum]
  change ((bindings n p.freeVars b).map
    (fun x => (p.atoms.map (fun a => value (bin db) a x)).prod)).sum = _
  rw [show (bindings n p.freeVars b).map
      (fun x => (p.atoms.map (fun a => value (bin db) a x)).prod) =
      (bindings n p.freeVars b).map (fun x => if satisfies p (bin db) x then 1 else 0) by
    apply List.map_congr_left
    intro x hx
    simpa [value, bin, satisfies, beq_iff_eq] using
      prod_indicator p.atoms (fun a => value (bin db) a x != 0)]
  rw [show ((bindings n p.freeVars b).map
      (fun x => if satisfies p (bin db) x then 1 else 0)).sum =
      ((satBindings p n db b).map (fun _ => (1 : Int))).sum by
    unfold satBindings
    induction bindings n p.freeVars b with
    | nil => simp
    | cons a l ih => by_cases ha : satisfies p (bin db) a = true <;> simp [ha, ih]]
  exact sum_filter_indicator _ _

private theorem supportList_mem (p : Pattern) (n : Nat) (db : DB) (b : Bind) (a : Atom)
    (x : Bind) (hx : x ∈ bindings n p.freeVars b) (hnz : db a.rel (x a.src) (x a.dst) != 0) :
    (x a.src, x a.dst) ∈ supportList p n db b a := by
  unfold supportList
  apply List.mem_flatMap.mpr
  refine ⟨x a.src, binding_domain_mem p n b x hx a.src, ?_⟩
  apply List.mem_map.mpr
  refine ⟨x a.dst, ?_, rfl⟩
  exact List.mem_filter.mpr ⟨binding_domain_mem p n b x hx a.dst, hnz⟩

private def encode (C : List Atom) (x : Bind) : List (Nat × Nat) :=
  C.map (fun a => (x a.src, x a.dst))

private theorem encode_mem_carts (p : Pattern) (n : Nat) (db : DB) (b : Bind)
    (C : List Atom) :
    (∀ a ∈ C, a ∈ p.atoms) → ∀ x, x ∈ satBindings p n db b →
      encode C x ∈ carts (C.map (supportList p n db b)) := by
  induction C with
  | nil => intro hCp x hx; simp [encode, carts]
  | cons a C ih =>
      intro hCp x hx
      have hsat : satisfies p (bin db) x = true := (List.mem_filter.mp hx).2
      have hb : x ∈ bindings n p.freeVars b := (List.mem_filter.mp hx).1
      have hsat' : p.atoms.all (fun t => value (bin db) t x != 0) = true := hsat
      have ha : value (bin db) a x != 0 :=
        List.all_eq_true.mp hsat' a (hCp a (by simp))
      have hnz : db a.rel (x a.src) (x a.dst) != 0 := by
        simpa [value, bin, beq_iff_eq] using ha
      rw [encode, List.map_cons, carts]
      exact carts_mem_cons _ _ _ _ (supportList_mem p n db b a x hb hnz)
        (ih (fun t ht => hCp t (by simp [ht])) x hx)

private theorem map_eq_on_mem {α β} {C : List α} {f g : α → β}
    (h : C.map f = C.map g) : ∀ a ∈ C, f a = g a := by
  induction C with
  | nil => simp
  | cons a C ih =>
      simp only [List.map_cons, List.cons.injEq] at h
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx
      · exact h.1
      · exact ih h.2 x hx

private theorem encode_injective (p : Pattern) (n : Nat) (b : Bind) (C : List Atom)
    (hcov : ∀ v ∈ p.freeVars, ∃ a ∈ C, a.src = v ∨ a.dst = v) :
    ∀ x ∈ bindings n p.freeVars b, ∀ y ∈ bindings n p.freeVars b,
      encode C x = encode C y → x = y := by
  intro x hx y hy heq
  funext v
  by_cases hv : v ∈ p.freeVars
  · rcases hcov v hv with ⟨a, ha, hend⟩
    have hp := map_eq_on_mem heq a ha
    rcases hend with rfl | rfl
    · exact Prod.mk.inj hp |>.1
    · exact Prod.mk.inj hp |>.2
  · rw [bindings_fixed n p.freeVars b x hx v hv, bindings_fixed n p.freeVars b y hy v hv]

private theorem ordinary_cover_bound (p : Pattern) (n : Nat) (db : DB) (b : Bind)
    (C : List Atom) (hcov : ∀ v ∈ p.freeVars, ∃ a ∈ C, a.src = v ∨ a.dst = v)
    (hCp : ∀ a ∈ C, a ∈ p.atoms) (hnd : p.freeVars.Nodup) :
    (satBindings p n db b).length ≤ (C.map (fun a => support n db a p b)).prod := by
  have hsnd : (satBindings p n db b).Nodup :=
    nodup_filter _ _ (bindings_nodup n p.freeVars b hnd)
  have hemb : (satBindings p n db b).map (encode C) ⊆
      carts (C.map (supportList p n db b)) := by
    intro z hz
    rcases List.mem_map.mp hz with ⟨x, hx, rfl⟩
    exact encode_mem_carts p n db b C hCp x hx
  have hmapnd : ((satBindings p n db b).map (encode C)).Nodup := by
    apply nodup_map
    · intro x hx y hy heq
      exact encode_injective p n b C hcov x (List.mem_filter.mp hx).1 y
        (List.mem_filter.mp hy).1 heq
    · exact hsnd
  have hle := hmapnd.length_le_of_subset hemb
  rw [List.length_map, carts_length, List.map_map] at hle
  rw [show C.map (List.length ∘ supportList p n db b) =
      C.map (fun a => (supportList p n db b a).length) by
    apply List.map_congr_left
    intro a ha
    rfl] at hle
  rw [show (C.map (fun a => (supportList p n db b a).length)) =
      C.map (fun a => support n db a p b) by
    apply List.map_congr_left
    intro a ha
    exact supportList_length p n db b a] at hle
  exact hle

/-! ---------- triangle Cauchy-Schwarz bound ---------- -/

private theorem bin_nonneg (db : DB) (r x y : Nat) : 0 ≤ bin db r x y := by
  simp [bin]
  split <;> omega

private theorem bin_le_one (db : DB) (r x y : Nat) : bin db r x y ≤ 1 := by
  simp [bin]
  split <;> omega

private theorem bin_sq (db : DB) (r x y : Nat) : bin db r x y * bin db r x y = bin db r x y := by
  by_cases h : db r x y == 0 <;> simp [bin, h]

private def triE (n : Nat) (db : DB) (r : Nat) : Int :=
  rsum n (fun x => rsum n (fun y => bin db r x y))

private def triT (n : Nat) (db : DB) (r s t : Nat) : Int :=
  rsum n (fun x => rsum n (fun y => rsum n (fun z =>
    bin db t z x * (bin db r x y * bin db s y z))))

private theorem triangle_sq (n : Nat) (db : DB) (r s t : Nat) :
    (triT n db r s t)^2 ≤ triE n db r * triE n db s * triE n db t := by
  let P : Nat → Nat → Int := fun x z => rsum n (fun y => bin db r x y * bin db s y z)
  have hT : triT n db r s t = rsum n (fun x => rsum n (fun z => P x z * bin db t z x)) := by
    dsimp only [triT, P]
    calc
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
          bin db t z x * (bin db r x y * bin db s y z))))
          = rsum n (fun x => rsum n (fun z => rsum n (fun y =>
              bin db t z x * (bin db r x y * bin db s y z)))) := by
              apply rsum_congr; intro x hx; rw [rsum_comm]
      _ = rsum n (fun x => rsum n (fun z =>
              (rsum n (fun y => bin db r x y * bin db s y z)) * bin db t z x)) := by
              apply rsum_congr; intro x hx
              apply rsum_congr; intro z hz
              rw [show (fun y => bin db t z x * (bin db r x y * bin db s y z)) =
                  (fun y => (bin db r x y * bin db s y z) * bin db t z x) by
                funext y
                ac_rfl]
              rw [rsum_mul_const]
  let pairs : List (Nat × Nat) :=
    (List.range n).flatMap (fun x => (List.range n).map (fun z => (x, z)))
  have hcs := cs_list (pairs.map (fun p => (P p.1 p.2, bin db t p.2 p.1)))
  have hmap1 : (pairs.map (fun p => (P p.1 p.2, bin db t p.2 p.1))).map
        (fun q : Int × Int => q.1 * q.2) =
      pairs.map (fun p => P p.1 p.2 * bin db t p.2 p.1) := by
    simp [List.map_map]
  have hmap2 : (pairs.map (fun p => (P p.1 p.2, bin db t p.2 p.1))).map
        (fun q : Int × Int => q.1 * q.1) =
      pairs.map (fun p => P p.1 p.2 * P p.1 p.2) := by
    simp [List.map_map]
  have hmap3 : (pairs.map (fun p => (P p.1 p.2, bin db t p.2 p.1))).map
        (fun q : Int × Int => q.2 * q.2) =
      pairs.map (fun p => bin db t p.2 p.1 * bin db t p.2 p.1) := by
    simp [List.map_map]
  rw [hmap1, hmap2, hmap3] at hcs
  have hsumP : (pairs.map (fun p => P p.1 p.2 * bin db t p.2 p.1)).sum =
      rsum n (fun x => rsum n (fun z => P x z * bin db t z x)) := by
    rw [← rsum_pairs n (fun x z => P x z * bin db t z x)]
  have hPpoint : ∀ x z y y' : Nat,
      (bin db r x y * bin db s y z) * (bin db r x y' * bin db s y' z) ≤
        bin db r x y * bin db s y' z := by
    intro x z y y'
    have hr0 : 0 ≤ bin db r x y := bin_nonneg db r x y
    have hs0 : 0 ≤ bin db s y z := bin_nonneg db s y z
    have hs1 : bin db s y z ≤ 1 := bin_le_one db s y z
    have hr0' : 0 ≤ bin db r x y' := bin_nonneg db r x y'
    have hr1' : bin db r x y' ≤ 1 := bin_le_one db r x y'
    have hs0' : 0 ≤ bin db s y' z := bin_nonneg db s y' z
    have hprod : bin db s y z * bin db r x y' ≤ 1 := by
      have hh := Int.mul_le_mul hs1 hr1' hr0' (by omega : (0 : Int) ≤ 1)
      simpa using hh
    have hprod0 : 0 ≤ bin db s y z * bin db r x y' := Int.mul_nonneg hs0 hr0'
    have hbase0 : 0 ≤ bin db r x y * bin db s y' z := Int.mul_nonneg hr0 hs0'
    have hh := Int.mul_le_mul (Int.le_refl (bin db r x y * bin db s y' z)) hprod
      hprod0 hbase0
    calc
      (bin db r x y * bin db s y z) * (bin db r x y' * bin db s y' z)
          = (bin db r x y * bin db s y' z) * (bin db s y z * bin db r x y') := by ac_rfl
      _ ≤ (bin db r x y * bin db s y' z) * 1 := hh
      _ = bin db r x y * bin db s y' z := by rw [Int.mul_one]
  have hsumP2le : (pairs.map (fun p => P p.1 p.2 * P p.1 p.2)).sum ≤
      triE n db r * triE n db s := by
    rw [← rsum_pairs n (fun x z => P x z * P x z)]
    dsimp only [P]
    calc
      rsum n (fun x => rsum n (fun z =>
          (rsum n (fun y => bin db r x y * bin db s y z)) *
          (rsum n (fun y' => bin db r x y' * bin db s y' z))))
          = rsum n (fun x => rsum n (fun z =>
              rsum n (fun y => rsum n (fun y' =>
                (bin db r x y * bin db s y z) * (bin db r x y' * bin db s y' z))))) := by
              apply rsum_congr; intro x hx
              apply rsum_congr; intro z hz
              rw [rsum_product]
      _ ≤ rsum n (fun x => rsum n (fun z =>
              rsum n (fun y => rsum n (fun y' => bin db r x y * bin db s y' z)))) := by
              apply rsum_le; intro x hx
              apply rsum_le; intro z hz
              apply rsum_le; intro y hy
              apply rsum_le; intro y' hy'
              exact hPpoint x z y y'
      _ = rsum n (fun x => rsum n (fun z =>
              (rsum n (fun y => bin db r x y)) * (rsum n (fun y' => bin db s y' z)))) := by
              apply rsum_congr; intro x hx
              apply rsum_congr; intro z hz
              rw [rsum_product]
      _ = rsum n (fun x => (rsum n (fun y => bin db r x y)) *
            (rsum n (fun z => rsum n (fun y' => bin db s y' z)))) := by
            apply rsum_congr; intro x hx
            rw [rsum_const_mul]
      _ = (rsum n (fun x => rsum n (fun y => bin db r x y))) *
          (rsum n (fun z => rsum n (fun y' => bin db s y' z))) := by
            rw [rsum_mul_const]
      _ = triE n db r * triE n db s := by
            dsimp only [triE]
            rw [show rsum n (fun x => rsum n (fun y => bin db s x y)) =
                rsum n (fun z => rsum n (fun y' => bin db s y' z)) by rw [rsum_comm]]
  have hsumC2 : (pairs.map (fun p => bin db t p.2 p.1 * bin db t p.2 p.1)).sum = triE n db t := by
    rw [← rsum_pairs n (fun x z => bin db t z x * bin db t z x)]
    calc
      rsum n (fun x => rsum n (fun z => bin db t z x * bin db t z x))
          = rsum n (fun x => rsum n (fun z => bin db t z x)) := by
              apply rsum_congr; intro x hx
              apply rsum_congr; intro z hz
              exact bin_sq db t z x
      _ = triE n db t := by
              dsimp only [triE]
              rw [rsum_comm]
  rw [hT, ← hsumP]
  calc
    ((pairs.map (fun p => P p.1 p.2 * bin db t p.2 p.1)).sum) ^ 2
        ≤ (pairs.map (fun p => P p.1 p.2 * P p.1 p.2)).sum *
          (pairs.map (fun p => bin db t p.2 p.1 * bin db t p.2 p.1)).sum := hcs
    _ ≤ (triE n db r * triE n db s) * triE n db t := by
          have hCnon : 0 ≤ (pairs.map (fun p => bin db t p.2 p.1 * bin db t p.2 p.1)).sum := by
            rw [← rsum_pairs n (fun x z => bin db t z x * bin db t z x)]
            apply rsum_nonneg; intro x hx
            apply rsum_nonneg; intro z hz
            exact Int.mul_nonneg (bin_nonneg db t z x) (bin_nonneg db t z x)
          have hE : 0 ≤ triE n db t := by
            dsimp only [triE]
            apply rsum_nonneg; intro x hx
            apply rsum_nonneg; intro y hy
            exact bin_nonneg db t x y
          have hErEs : 0 ≤ triE n db r * triE n db s := by
            apply Int.mul_nonneg
            · dsimp only [triE]
              apply rsum_nonneg; intro x hx
              apply rsum_nonneg; intro y hy
              exact bin_nonneg db r x y
            · dsimp only [triE]
              apply rsum_nonneg; intro x hx
              apply rsum_nonneg; intro y hy
              exact bin_nonneg db s x y
          exact Int.mul_le_mul hsumP2le (by rw [hsumC2]; omega) hCnon hErEs
    _ = triE n db r * triE n db s * triE n db t := rfl

open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie

private theorem reference_shape (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (bin db) b =
      rsum n (fun v1 => rsum n (fun v2 => rsum n (fun v4 => rsum n (fun v3 => rsum n (fun v0 =>
        bin db 0 v0 v1 * (bin db 1 v1 v2 * (bin db 2 v2 v0 *
        (bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0))))))))) := by
  simp [reference, pattern, integrate, value, put, rsum, bin]

private def T1 (n : Nat) (db : DB) (u : Nat) : Int :=
  rsum n (fun x => rsum n (fun y => bin db 0 u x * (bin db 1 x y * bin db 2 y u)))

private def T2 (n : Nat) (db : DB) (u : Nat) : Int :=
  rsum n (fun v => rsum n (fun w => bin db 0 u v * (bin db 1 v w * bin db 2 w u)))

private def Nbow (n : Nat) (db : DB) : Int := rsum n (fun u => T1 n db u * T2 n db u)

private theorem factor_four (n : Nat) (A B : Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => A a b * B c d)))) =
      (rsum n (fun a => rsum n (fun b => A a b))) *
      (rsum n (fun c => rsum n (fun d => B c d))) := by
  let K : Int := rsum n (fun c => rsum n (fun d => B c d))
  have hinner : ∀ a b, rsum n (fun c => rsum n (fun d => A a b * B c d)) = A a b * K := by
    intro a b
    dsimp only [K]
    calc
      rsum n (fun c => rsum n (fun d => A a b * B c d))
          = rsum n (fun c => A a b * rsum n (fun d => B c d)) := by
              apply rsum_congr; intro c hc
              rw [rsum_const_mul]
      _ = A a b * rsum n (fun c => rsum n (fun d => B c d)) := by
              rw [rsum_const_mul]
  have h1 : rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => A a b * B c d)))) =
      rsum n (fun a => rsum n (fun b => A a b * K)) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    exact hinner a b
  rw [h1]
  have h2 : rsum n (fun a => rsum n (fun b => A a b * K)) =
      rsum n (fun a => (rsum n (fun b => A a b)) * K) := by
    apply rsum_congr; intro a ha
    rw [rsum_mul_const]
  rw [h2]
  rw [rsum_mul_const]

private theorem sum_T2_eq_triT (n : Nat) (db : DB) :
    rsum n (fun u => T2 n db u) = triT n db 0 1 2 := by
  dsimp only [T2, triT]
  apply rsum_congr
  intro u hu
  apply rsum_congr
  intro v hv
  apply rsum_congr
  intro w hw
  ac_rfl

private theorem reference_bin_bow (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (bin db) b = Nbow n db := by
  rw [reference_shape n db b]
  dsimp only [Nbow, T1, T2]
  calc
    rsum n (fun v1 => rsum n (fun v2 => rsum n (fun v4 => rsum n (fun v3 => rsum n (fun v0 =>
        bin db 0 v0 v1 * (bin db 1 v1 v2 * (bin db 2 v2 v0 *
        (bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0)))))))))
      = rsum n (fun v1 => rsum n (fun v2 => rsum n (fun v4 => rsum n (fun v0 => rsum n (fun v3 =>
          bin db 0 v0 v1 * (bin db 1 v1 v2 * (bin db 2 v2 v0 *
          (bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0))))))))) := by
          apply rsum_congr; intro v1 h1
          apply rsum_congr; intro v2 h2
          apply rsum_congr; intro v4 h4
          rw [rsum_comm]
    _ = rsum n (fun v1 => rsum n (fun v2 => rsum n (fun v0 => rsum n (fun v4 => rsum n (fun v3 =>
          bin db 0 v0 v1 * (bin db 1 v1 v2 * (bin db 2 v2 v0 *
          (bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0))))))))) := by
          apply rsum_congr; intro v1 h1
          apply rsum_congr; intro v2 h2
          rw [rsum_comm]
    _ = rsum n (fun v1 => rsum n (fun v0 => rsum n (fun v2 => rsum n (fun v4 => rsum n (fun v3 =>
          bin db 0 v0 v1 * (bin db 1 v1 v2 * (bin db 2 v2 v0 *
          (bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0))))))))) := by
          apply rsum_congr; intro v1 h1
          rw [rsum_comm]
    _ = rsum n (fun v0 => rsum n (fun v1 => rsum n (fun v2 => rsum n (fun v4 => rsum n (fun v3 =>
          bin db 0 v0 v1 * (bin db 1 v1 v2 * (bin db 2 v2 v0 *
          (bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0))))))))) := by
          rw [rsum_comm]
    _ = rsum n (fun v0 => rsum n (fun v1 => rsum n (fun v2 => rsum n (fun v4 => rsum n (fun v3 =>
          (bin db 0 v0 v1 * (bin db 1 v1 v2 * bin db 2 v2 v0)) *
          (bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0))))))) := by
          apply rsum_congr; intro v0 h0
          apply rsum_congr; intro v1 h1
          apply rsum_congr; intro v2 h2
          apply rsum_congr; intro v4 h4
          apply rsum_congr; intro v3 h3
          ac_rfl
    _ = rsum n (fun v0 =>
          (rsum n (fun v1 => rsum n (fun v2 => bin db 0 v0 v1 * (bin db 1 v1 v2 * bin db 2 v2 v0)))) *
          (rsum n (fun v4 => rsum n (fun v3 => bin db 0 v0 v3 * (bin db 1 v3 v4 * bin db 2 v4 v0))))) := by
          apply rsum_congr; intro v0 h0
          rw [factor_four]
    _ = rsum n (fun v0 => T1 n db v0 * T2 n db v0) := by
          apply rsum_congr; intro v0 h0
          congr 1
          dsimp only [T2]
          rw [rsum_comm]

private theorem T1_nonneg (n : Nat) (db : DB) (u : Nat) : 0 ≤ T1 n db u := by
  dsimp only [T1]
  apply rsum_nonneg; intro x hx
  apply rsum_nonneg; intro y hy
  exact Int.mul_nonneg (bin_nonneg db 0 u x)
    (Int.mul_nonneg (bin_nonneg db 1 x y) (bin_nonneg db 2 y u))

private theorem T2_nonneg (n : Nat) (db : DB) (u : Nat) : 0 ≤ T2 n db u := by
  dsimp only [T2]
  apply rsum_nonneg; intro v hv
  apply rsum_nonneg; intro w hw
  exact Int.mul_nonneg (bin_nonneg db 0 u v)
    (Int.mul_nonneg (bin_nonneg db 1 v w) (bin_nonneg db 2 w u))

private theorem Nbow_nonneg (n : Nat) (db : DB) : 0 ≤ Nbow n db := by
  dsimp only [Nbow]
  apply rsum_nonneg; intro u hu
  exact Int.mul_nonneg (T1_nonneg n db u) (T2_nonneg n db u)

private theorem T1_le_triE1 (n : Nat) (db : DB) (u : Nat) : T1 n db u ≤ triE n db 1 := by
  have hpt : ∀ x y, bin db 0 u x * (bin db 1 x y * bin db 2 y u) ≤ bin db 1 x y := by
    intro x y
    have h0 : 0 ≤ bin db 0 u x := bin_nonneg db 0 u x
    have h0le : bin db 0 u x ≤ 1 := bin_le_one db 0 u x
    have h2 : 0 ≤ bin db 2 y u := bin_nonneg db 2 y u
    have h2le : bin db 2 y u ≤ 1 := bin_le_one db 2 y u
    have h1 : 0 ≤ bin db 1 x y := bin_nonneg db 1 x y
    have hprod : bin db 0 u x * bin db 2 y u ≤ 1 := by
      have hh := Int.mul_le_mul h0le h2le h2 (by omega : (0 : Int) ≤ 1)
      simpa using hh
    have hprod0 : 0 ≤ bin db 0 u x * bin db 2 y u := Int.mul_nonneg h0 h2
    have hh := Int.mul_le_mul (Int.le_refl (bin db 1 x y)) hprod hprod0 h1
    calc
      bin db 0 u x * (bin db 1 x y * bin db 2 y u)
          = bin db 1 x y * (bin db 0 u x * bin db 2 y u) := by ac_rfl
      _ ≤ bin db 1 x y * 1 := hh
      _ = bin db 1 x y := by rw [Int.mul_one]
  dsimp only [T1, triE]
  calc
    rsum n (fun x => rsum n (fun y => bin db 0 u x * (bin db 1 x y * bin db 2 y u)))
        ≤ rsum n (fun x => rsum n (fun y => bin db 1 x y)) := by
            apply rsum_le; intro x hx
            apply rsum_le; intro y hy
            exact hpt x y
    _ = rsum n (fun x => rsum n (fun y => bin db 1 x y)) := rfl

private theorem T2_le_triE1 (n : Nat) (db : DB) (u : Nat) : T2 n db u ≤ triE n db 1 := by
  have hpt : ∀ v w, bin db 0 u v * (bin db 1 v w * bin db 2 w u) ≤ bin db 1 v w := by
    intro v w
    have h0 : 0 ≤ bin db 0 u v := bin_nonneg db 0 u v
    have h0le : bin db 0 u v ≤ 1 := bin_le_one db 0 u v
    have h2 : 0 ≤ bin db 2 w u := bin_nonneg db 2 w u
    have h2le : bin db 2 w u ≤ 1 := bin_le_one db 2 w u
    have h1 : 0 ≤ bin db 1 v w := bin_nonneg db 1 v w
    have hprod : bin db 0 u v * bin db 2 w u ≤ 1 := by
      have hh := Int.mul_le_mul h0le h2le h2 (by omega : (0 : Int) ≤ 1)
      simpa using hh
    have hprod0 : 0 ≤ bin db 0 u v * bin db 2 w u := Int.mul_nonneg h0 h2
    have hh := Int.mul_le_mul (Int.le_refl (bin db 1 v w)) hprod hprod0 h1
    calc
      bin db 0 u v * (bin db 1 v w * bin db 2 w u)
          = bin db 1 v w * (bin db 0 u v * bin db 2 w u) := by ac_rfl
      _ ≤ bin db 1 v w * 1 := hh
      _ = bin db 1 v w := by rw [Int.mul_one]
  dsimp only [T2, triE]
  apply rsum_le; intro v hv
  apply rsum_le; intro w hw
  exact hpt v w

private theorem triE_nonneg (n : Nat) (db : DB) (r : Nat) : 0 ≤ triE n db r := by
  dsimp only [triE]
  apply rsum_nonneg; intro x hx
  apply rsum_nonneg; intro y hy
  exact bin_nonneg db r x y

private theorem bow_sq (n : Nat) (db : DB) :
    Nbow n db ^ 2 ≤ triE n db 0 * triE n db 1 ^ 3 * triE n db 2 := by
  have htri := triangle_sq n db 0 1 2
  let Tri : Int := triT n db 0 1 2
  have hTri_nonneg : 0 ≤ Tri := by
    dsimp only [Tri]
    rw [← sum_T2_eq_triT n db]
    apply rsum_nonneg; intro u hu
    exact T2_nonneg n db u
  have hNle : Nbow n db ≤ triE n db 1 * Tri := by
    calc
      Nbow n db = rsum n (fun u => T1 n db u * T2 n db u) := rfl
      _ ≤ rsum n (fun u => triE n db 1 * T2 n db u) := by
          apply rsum_le; intro u hu
          exact Int.mul_le_mul (T1_le_triE1 n db u) (Int.le_refl (T2 n db u))
            (T2_nonneg n db u) (triE_nonneg n db 1)
      _ = triE n db 1 * rsum n (fun u => T2 n db u) := by
          rw [rsum_const_mul]
      _ = triE n db 1 * Tri := by
          dsimp only [Tri]
          rw [sum_T2_eq_triT]
  have hNle2 : Nbow n db ^ 2 ≤ (triE n db 1 * Tri) ^ 2 := by
    have hn := Nbow_nonneg n db
    have hr : 0 ≤ triE n db 1 * Tri := Int.mul_nonneg (triE_nonneg n db 1) hTri_nonneg
    have h := Int.mul_le_mul hNle hNle hn hr
    have hp1 : Nbow n db ^ 2 = Nbow n db * Nbow n db := by
      rw [Int.pow_succ, Int.pow_one]
    have hp2 : (triE n db 1 * Tri) ^ 2 = (triE n db 1 * Tri) * (triE n db 1 * Tri) := by
      rw [Int.pow_succ, Int.pow_one]
    rw [hp1, hp2]
    exact h
  have htri' : Tri ^ 2 ≤ triE n db 0 * triE n db 1 * triE n db 2 := by
    dsimp only [Tri]
    exact htri
  have hE1sq_nonneg : 0 ≤ triE n db 1 ^ 2 := Int.sq_nonneg _
  have hmul := Int.mul_le_mul (Int.le_refl (triE n db 1 ^ 2)) htri'
    (Int.sq_nonneg Tri) hE1sq_nonneg
  calc
    Nbow n db ^ 2 ≤ (triE n db 1 * Tri) ^ 2 := hNle2
    _ = (triE n db 1 ^ 2) * (Tri ^ 2) := by grind
    _ ≤ (triE n db 1 ^ 2) * (triE n db 0 * triE n db 1 * triE n db 2) := hmul
    _ = triE n db 0 * triE n db 1 ^ 3 * triE n db 2 := by grind

/-! ---------- support = edge count ---------- -/

private theorem countP_eq_rsum (n : Nat) (p : Nat → Bool) :
    ((List.range n).countP p : Int) = rsum n (fun x => if p x then (1 : Int) else 0) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [List.range_succ, List.countP_append, List.countP_singleton]
      rw [Int.natCast_add, ih, rsum_succ]
      by_cases h : p n = true <;> simp [h]

private theorem nat_sum_cast {α} (l : List α) (f : α → Nat) :
    ((l.map f).sum : Int) = (l.map (fun x => (f x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem support_eq_triE (n : Nat) (db : DB) (b : Bind) (r src dst : Nat)
    (hs : pattern.freeVars.contains src = true) (hd : pattern.freeVars.contains dst = true) :
    (support n db ⟨r, src, dst⟩ pattern b : Int) = triE n db r := by
  have hsm : src ∈ pattern.freeVars := by simpa using hs
  have hdm : dst ∈ pattern.freeVars := by simpa using hd
  rw [show support n db ⟨r, src, dst⟩ pattern b =
      ((List.range n).map (fun x => (List.range n).countP (fun y => db r x y != 0))).sum by
    simp [support, domain, hsm, hdm]]
  rw [nat_sum_cast]
  rw [show ((List.range n).map
      (fun x => ((List.range n).countP (fun y => db r x y != 0) : Int))).sum =
      rsum n (fun x => rsum n (fun y => if db r x y == 0 then (0 : Int) else 1)) by
    apply sum_congr
    intro x hx
    rw [countP_eq_rsum]
    apply rsum_congr
    intro y hy
    by_cases h : db r x y = 0 <;> simp [h, beq_iff_eq]]
  rfl

private def a0 : Atom := ⟨0, 0, 1⟩
private def a1 : Atom := ⟨1, 1, 2⟩
private def a2 : Atom := ⟨2, 2, 0⟩
private def a3 : Atom := ⟨0, 0, 3⟩
private def a4 : Atom := ⟨1, 3, 4⟩
private def a5 : Atom := ⟨2, 4, 0⟩

private def C1 : List Atom := [a0, a1, a4]
private def C2 : List Atom := [a1, a4, a2]
private def C3 : List Atom := [a1, a4, a3]
private def C4 : List Atom := [a1, a4, a5]
private def C5 : List Atom := [a1, a3, a5]
private def C6 : List Atom := [a0, a2, a4]
private def C7 : List Atom := [a0, a2, a3, a5]

private theorem cover_C1 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C1, a.src = v ∨ a.dst = v := by
  native_decide
private theorem cover_C2 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C2, a.src = v ∨ a.dst = v := by
  native_decide
private theorem cover_C3 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C3, a.src = v ∨ a.dst = v := by
  native_decide
private theorem cover_C4 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C4, a.src = v ∨ a.dst = v := by
  native_decide
private theorem cover_C5 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C5, a.src = v ∨ a.dst = v := by
  native_decide
private theorem cover_C6 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C6, a.src = v ∨ a.dst = v := by
  native_decide
private theorem cover_C7 : ∀ v ∈ pattern.freeVars, ∃ a ∈ C7, a.src = v ∨ a.dst = v := by
  native_decide
private theorem atoms_C1 : ∀ a ∈ C1, a ∈ pattern.atoms := by
  intro a ha; simp [C1, a0, a1, a4] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp [pattern]
private theorem atoms_C2 : ∀ a ∈ C2, a ∈ pattern.atoms := by
  intro a ha; simp [C2, a1, a4, a2] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp [pattern]
private theorem atoms_C3 : ∀ a ∈ C3, a ∈ pattern.atoms := by
  intro a ha; simp [C3, a1, a4, a3] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp [pattern]
private theorem atoms_C4 : ∀ a ∈ C4, a ∈ pattern.atoms := by
  intro a ha; simp [C4, a1, a4, a5] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp [pattern]
private theorem atoms_C5 : ∀ a ∈ C5, a ∈ pattern.atoms := by
  intro a ha; simp [C5, a1, a3, a5] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp [pattern]
private theorem atoms_C6 : ∀ a ∈ C6, a ∈ pattern.atoms := by
  intro a ha; simp [C6, a0, a2, a4] at ha ⊢
  rcases ha with rfl | rfl | rfl <;> simp [pattern]
private theorem atoms_C7 : ∀ a ∈ C7, a ∈ pattern.atoms := by
  intro a ha; simp [C7, a0, a2, a3, a5] at ha ⊢
  rcases ha with rfl | rfl | rfl | rfl <;> simp [pattern]
private theorem fv_nodup : pattern.freeVars.Nodup := by native_decide

private theorem list6_of_length6 {α : Type} (l : List α) (h : l.length = 6) :
    ∃ a b c d e f, l = [a, b, c, d, e, f] := by
  cases l with
  | nil => simp at h
  | cons a l =>
    cases l with
    | nil => simp at h
    | cons b l =>
      cases l with
      | nil => simp at h
      | cons c l =>
        cases l with
        | nil => simp at h
        | cons d l =>
          cases l with
          | nil => simp at h
          | cons e l =>
            cases l with
            | nil => simp at h
            | cons f l =>
              cases l with
              | nil => exact ⟨a, b, c, d, e, f, rfl⟩
              | cons g l => simp at h

private theorem pow_mono_of_pos {x a b : Nat} (hx : 0 < x) (hab : a ≤ b) : x^a ≤ x^b := by
  have hs : b = a + (b - a) := by omega
  rw [hs, Nat.pow_add]
  exact Nat.le_mul_of_pos_right (x^a) (show 0 < x ^ (b - a) from Nat.pow_pos hx)

private theorem bow_sq_nat (n : Nat) (db : DB) (b : Bind) :
    let L : Nat := (satBindings pattern n db b).length
    let S0 := support n db a0 pattern b
    let S1 := support n db a1 pattern b
    let S2 := support n db a2 pattern b
    let S3 := support n db a3 pattern b
    let S4 := support n db a4 pattern b
    let S5 := support n db a5 pattern b
    L ^ 2 ≤ S1 ^ 2 * S3 * S4 * S5 ∧ L ^ 2 ≤ S0 * S1 * S2 * S4 ^ 2 := by
  dsimp only
  let L : Nat := (satBindings pattern n db b).length
  let S0 := support n db a0 pattern b
  let S1 := support n db a1 pattern b
  let S2 := support n db a2 pattern b
  let S3 := support n db a3 pattern b
  let S4 := support n db a4 pattern b
  let S5 := support n db a5 pattern b
  change L ^ 2 ≤ S1 ^ 2 * S3 * S4 * S5 ∧ L ^ 2 ≤ S0 * S1 * S2 * S4 ^ 2
  have hL : (L : Int) = Nbow n db := by
    dsimp only [L]
    rw [← reference_bin_count pattern n db b, reference_bin_bow]
  have hS0 : (S0 : Int) = triE n db 0 := by
    dsimp only [S0, a0]
    exact support_eq_triE n db b 0 0 1 (by simp [pattern]) (by simp [pattern])
  have hS1 : (S1 : Int) = triE n db 1 := by
    dsimp only [S1, a1]
    exact support_eq_triE n db b 1 1 2 (by simp [pattern]) (by simp [pattern])
  have hS2 : (S2 : Int) = triE n db 2 := by
    dsimp only [S2, a2]
    exact support_eq_triE n db b 2 2 0 (by simp [pattern]) (by simp [pattern])
  have hS3 : (S3 : Int) = triE n db 0 := by
    dsimp only [S3, a3]
    exact support_eq_triE n db b 0 0 3 (by simp [pattern]) (by simp [pattern])
  have hS4 : (S4 : Int) = triE n db 1 := by
    dsimp only [S4, a4]
    exact support_eq_triE n db b 1 3 4 (by simp [pattern]) (by simp [pattern])
  have hS5 : (S5 : Int) = triE n db 2 := by
    dsimp only [S5, a5]
    exact support_eq_triE n db b 2 4 0 (by simp [pattern]) (by simp [pattern])
  have hbsq := bow_sq n db
  rw [← hL] at hbsq
  have hnat : L ^ 2 ≤ S0 * S1 * S2 * S4 ^ 2 := by
    apply Int.ofNat_le.mp
    simp only [Int.natCast_pow, Int.natCast_mul]
    rw [hS0, hS1, hS2, hS4]
    grind
  have hnat' : L ^ 2 ≤ S1 ^ 2 * S3 * S4 * S5 := by
    apply Int.ofNat_le.mp
    simp only [Int.natCast_pow, Int.natCast_mul]
    rw [hS1, hS3, hS4, hS5]
    grind
  exact ⟨hnat', hnat⟩

private def tuple6 (a0 a1 a2 a3 a4 a5 : Nat) : Nat := a0 * a1 * a2 * a3 * a4 * a5

private theorem tuple6_mul (a0 a1 a2 a3 a4 a5 b0 b1 b2 b3 b4 b5 : Nat) :
    tuple6 a0 a1 a2 a3 a4 a5 * tuple6 b0 b1 b2 b3 b4 b5 =
      tuple6 (a0*b0) (a1*b1) (a2*b2) (a3*b3) (a4*b4) (a5*b5) := by
  dsimp [tuple6]
  ac_rfl

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.FractionalCap.Goal := by
  intro n db b weights den hcov
  rcases hcov with ⟨hdenpos, hlen, hc⟩
  rcases list6_of_length6 weights (by simpa [pattern] using hlen) with
    ⟨w0, w1, w2, w3, w4, w5, rfl⟩
  have h0 : den ≤ w0 + w2 + w3 + w5 := by
    have hh := hc 0 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h1 : den ≤ w0 + w1 := by
    have hh := hc 1 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h2 : den ≤ w1 + w2 := by
    have hh := hc 2 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h3 : den ≤ w3 + w4 := by
    have hh := hc 3 (by simp [pattern])
    simp [pattern] at hh
    omega
  have h4 : den ≤ w4 + w5 := by
    have hh := hc 4 (by simp [pattern])
    simp [pattern] at hh
    omega
  rcases cover_decomp den w0 w1 w2 w3 w4 w5 h1 h2 h3 h4 h0 with
    ⟨t1, t2, t3, t4, t5, t6, t7, s1, s4, hs, hu0, hu1, hu2, hu3, hu4, hu5⟩
  let S0 := support n db a0 pattern b
  let S1 := support n db a1 pattern b
  let S2 := support n db a2 pattern b
  let S3 := support n db a3 pattern b
  let S4 := support n db a4 pattern b
  let S5 := support n db a5 pattern b
  let L := (satBindings pattern n db b).length
  have hb1 : L ≤ S0 * S1 * S4 := by
    dsimp only [L, S0, S1, S4]
    simpa [C1, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C1 cover_C1 atoms_C1 fv_nodup)
  have hb2 : L ≤ S1 * S4 * S2 := by
    dsimp only [L, S1, S4, S2]
    simpa [C2, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C2 cover_C2 atoms_C2 fv_nodup)
  have hb3 : L ≤ S1 * S4 * S3 := by
    dsimp only [L, S1, S4, S3]
    simpa [C3, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C3 cover_C3 atoms_C3 fv_nodup)
  have hb4 : L ≤ S1 * S4 * S5 := by
    dsimp only [L, S1, S4, S5]
    simpa [C4, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C4 cover_C4 atoms_C4 fv_nodup)
  have hb5 : L ≤ S1 * S3 * S5 := by
    dsimp only [L, S1, S3, S5]
    simpa [C5, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C5 cover_C5 atoms_C5 fv_nodup)
  have hb6 : L ≤ S0 * S2 * S4 := by
    dsimp only [L, S0, S2, S4]
    simpa [C6, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C6 cover_C6 atoms_C6 fv_nodup)
  have hb7 : L ≤ S0 * S2 * S3 * S5 := by
    dsimp only [L, S0, S2, S3, S5]
    simpa [C7, Nat.mul_assoc] using
      (ordinary_cover_bound pattern n db b C7 cover_C7 atoms_C7 fv_nodup)
  have hsq := bow_sq_nat n db b
  dsimp only at hsq
  have hsq1 : L ^ 2 ≤ S1 ^ 2 * S3 * S4 * S5 := by
    simpa [L, S1, S3, S4, S5] using hsq.1
  have hsq4 : L ^ 2 ≤ S0 * S1 * S2 * S4 ^ 2 := by
    simpa [L, S0, S1, S2, S4] using hsq.2
  have href : (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat = L := by
    have hbin : (fun r x y : Nat => if db r x y == 0 then (0 : Int) else 1) = bin db := by
      funext r x y
      simp [bin, beq_iff_eq]
    rw [hbin]
    dsimp only [L]
    exact congrArg Int.toNat (reference_bin_count pattern n db b)
  have hRHS :
      ((pattern.atoms.zip [w0, w1, w2, w3, w4, w5]).map
        (fun (a, w) => support n db a pattern b ^ w)).prod =
      S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 * S4 ^ w4 * S5 ^ w5 := by
    simp [pattern, S0, S1, S2, S3, S4, S5, a0, a1, a2, a3, a4, a5, Nat.mul_assoc]
  rw [href, hRHS]
  by_cases hL0 : L = 0
  · rw [hL0]
    simp [Nat.zero_pow hdenpos]
  · have hLpos : 0 < L := Nat.pos_of_ne_zero hL0
    have hSpos : ∀ a ∈ pattern.atoms, 0 < support n db a pattern b := by
      intro a ha
      have hlenpos : 0 < (satBindings pattern n db b).length := by simpa [L] using hLpos
      obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil _ ((List.length_pos_iff).mp hlenpos)
      have hxbind : x ∈ bindings n pattern.freeVars b := (List.mem_filter.mp hx).1
      have hsat : satisfies pattern (bin db) x = true := (List.mem_filter.mp hx).2
      have hnz : db a.rel (x a.src) (x a.dst) != 0 := by
        have hall : pattern.atoms.all (fun t => value (bin db) t x != 0) = true := hsat
        simpa [value, bin, beq_iff_eq] using (List.all_eq_true.mp hall a ha)
      have hmemsl := supportList_mem pattern n db b a x hxbind hnz
      have hslne : supportList pattern n db b a ≠ [] := by
        intro hnil
        have hnot : (x a.src, x a.dst) ∉ supportList pattern n db b a := by simp [hnil]
        exact hnot hmemsl
      have hslpos : 0 < (supportList pattern n db b a).length :=
        (List.length_pos_iff).mpr hslne
      rw [supportList_length] at hslpos
      exact hslpos
    have hS0p : 0 < S0 := by dsimp only [S0]; exact hSpos a0 (by simp [a0, pattern])
    have hS1p : 0 < S1 := by dsimp only [S1]; exact hSpos a1 (by simp [a1, pattern])
    have hS2p : 0 < S2 := by dsimp only [S2]; exact hSpos a2 (by simp [a2, pattern])
    have hS3p : 0 < S3 := by dsimp only [S3]; exact hSpos a3 (by simp [a3, pattern])
    have hS4p : 0 < S4 := by dsimp only [S4]; exact hSpos a4 (by simp [a4, pattern])
    have hS5p : 0 < S5 := by dsimp only [S5]; exact hSpos a5 (by simp [a5, pattern])
    let U0 := (t1 + t6 + t7) + s4
    let U1 := (t1 + t2 + t3 + t4 + t5) + 2*s1 + s4
    let U2 := (t2 + t6 + t7) + s4
    let U3 := (t3 + t5 + t7) + s1
    let U4 := (t1 + t2 + t3 + t4 + t6) + s1 + 2*s4
    let U5 := (t4 + t5 + t7) + s1
    have hU0 : U0 ≤ w0 := by dsimp only [U0]; exact hu0
    have hU1 : U1 ≤ w1 := by dsimp only [U1]; exact hu1
    have hU2 : U2 ≤ w2 := by dsimp only [U2]; exact hu2
    have hU3 : U3 ≤ w3 := by dsimp only [U3]; exact hu3
    have hU4 : U4 ≤ w4 := by dsimp only [U4]; exact hu4
    have hU5 : U5 ≤ w5 := by dsimp only [U5]; exact hu5
    have hbig : L ^ den ≤ S0 ^ U0 * S1 ^ U1 * S2 ^ U2 * S3 ^ U3 * S4 ^ U4 * S5 ^ U5 := by
      have h1p := Nat.pow_le_pow_left hb1 t1
      have h2p := Nat.pow_le_pow_left hb2 t2
      have h3p := Nat.pow_le_pow_left hb3 t3
      have h4p := Nat.pow_le_pow_left hb4 t4
      have h5p := Nat.pow_le_pow_left hb5 t5
      have h6p := Nat.pow_le_pow_left hb6 t6
      have h7p := Nat.pow_le_pow_left hb7 t7
      have hsp1 := Nat.pow_le_pow_left hsq1 s1
      have hsp4 := Nat.pow_le_pow_left hsq4 s4
      have hpoweq : L ^ den = L ^ t1 * L ^ t2 * L ^ t3 * L ^ t4 * L ^ t5 * L ^ t6 * L ^ t7 *
          (L ^ 2) ^ s1 * (L ^ 2) ^ s4 := by
        rw [show den = (t1 + t2 + t3 + t4 + t5 + t6 + t7) + 2*s1 + 2*s4 by omega]
        simp only [Nat.pow_add]
        rw [Nat.pow_mul, Nat.pow_mul]
      rw [hpoweq]
      have hm1 := Nat.mul_le_mul h1p h2p
      have hm2 := Nat.mul_le_mul hm1 h3p
      have hm3 := Nat.mul_le_mul hm2 h4p
      have hm4 := Nat.mul_le_mul hm3 h5p
      have hm5 := Nat.mul_le_mul hm4 h6p
      have hm6 := Nat.mul_le_mul hm5 h7p
      have hm7 := Nat.mul_le_mul hm6 hsp1
      have hm8 := Nat.mul_le_mul hm7 hsp4
      have hR : (S0 * S1 * S4) ^ t1 * (S1 * S4 * S2) ^ t2 * (S1 * S4 * S3) ^ t3 *
          (S1 * S4 * S5) ^ t4 * (S1 * S3 * S5) ^ t5 * (S0 * S2 * S4) ^ t6 *
          (S0 * S2 * S3 * S5) ^ t7 * (S1 ^ 2 * S3 * S4 * S5) ^ s1 *
          (S0 * S1 * S2 * S4 ^ 2) ^ s4 =
        S0 ^ U0 * S1 ^ U1 * S2 ^ U2 * S3 ^ U3 * S4 ^ U4 * S5 ^ U5 := by
        dsimp only [U0, U1, U2, U3, U4, U5]
        let B1 := tuple6 (S0 ^ t1) (S1 ^ t1) 1 1 (S4 ^ t1) 1
        let B2 := tuple6 1 (S1 ^ t2) (S2 ^ t2) 1 (S4 ^ t2) 1
        let B3 := tuple6 1 (S1 ^ t3) 1 (S3 ^ t3) (S4 ^ t3) 1
        let B4 := tuple6 1 (S1 ^ t4) 1 1 (S4 ^ t4) (S5 ^ t4)
        let B5 := tuple6 1 (S1 ^ t5) 1 (S3 ^ t5) 1 (S5 ^ t5)
        let B6 := tuple6 (S0 ^ t6) 1 (S2 ^ t6) 1 (S4 ^ t6) 1
        let B7 := tuple6 (S0 ^ t7) 1 (S2 ^ t7) (S3 ^ t7) 1 (S5 ^ t7)
        let B8 := tuple6 1 ((S1 ^ 2) ^ s1) 1 (S3 ^ s1) (S4 ^ s1) (S5 ^ s1)
        let B9 := tuple6 (S0 ^ s4) (S1 ^ s4) (S2 ^ s4) 1 ((S4 ^ 2) ^ s4) 1
        have e0 : (S0 * S1 * S4) ^ t1 = B1 := by
          dsimp only [B1, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
        have e1 : (S1 * S4 * S2) ^ t2 = B2 := by
          dsimp only [B2, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
          ac_rfl
        have e2 : (S1 * S4 * S3) ^ t3 = B3 := by
          dsimp only [B3, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
          ac_rfl
        have e3 : (S1 * S4 * S5) ^ t4 = B4 := by
          dsimp only [B4, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
          ac_rfl
        have e4 : (S1 * S3 * S5) ^ t5 = B5 := by
          dsimp only [B5, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
          ac_rfl
        have e5 : (S0 * S2 * S4) ^ t6 = B6 := by
          dsimp only [B6, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
        have e6 : (S0 * S2 * S3 * S5) ^ t7 = B7 := by
          dsimp only [B7, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
        have e7 : (S1 ^ 2 * S3 * S4 * S5) ^ s1 = B8 := by
          dsimp only [B8, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
          ac_rfl
        have e8 : (S0 * S1 * S2 * S4 ^ 2) ^ s4 = B9 := by
          dsimp only [B9, tuple6]
          simp only [Nat.mul_pow, Nat.mul_one]
        rw [e0, e1, e2, e3, e4, e5, e6, e7, e8]
        have c1 : B1 * B2 = tuple6 ((S0 ^ t1 * 1)) ((S1 ^ t1 * S1 ^ t2)) ((1 * S2 ^ t2)) ((1 * 1)) ((S4 ^ t1 * S4 ^ t2)) ((1 * 1)) := by
          dsimp only [B1, B2]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c1]
        have c2 : tuple6 ((S0 ^ t1 * 1)) ((S1 ^ t1 * S1 ^ t2)) ((1 * S2 ^ t2)) ((1 * 1)) ((S4 ^ t1 * S4 ^ t2)) ((1 * 1)) * B3 = tuple6 (((S0 ^ t1 * 1) * 1)) (((S1 ^ t1 * S1 ^ t2) * S1 ^ t3)) (((1 * S2 ^ t2) * 1)) (((1 * 1) * S3 ^ t3)) (((S4 ^ t1 * S4 ^ t2) * S4 ^ t3)) (((1 * 1) * 1)) := by
          dsimp only [B3]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c2]
        have c3 : tuple6 (((S0 ^ t1 * 1) * 1)) (((S1 ^ t1 * S1 ^ t2) * S1 ^ t3)) (((1 * S2 ^ t2) * 1)) (((1 * 1) * S3 ^ t3)) (((S4 ^ t1 * S4 ^ t2) * S4 ^ t3)) (((1 * 1) * 1)) * B4 = tuple6 ((((S0 ^ t1 * 1) * 1) * 1)) ((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4)) ((((1 * S2 ^ t2) * 1) * 1)) ((((1 * 1) * S3 ^ t3) * 1)) ((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4)) ((((1 * 1) * 1) * S5 ^ t4)) := by
          dsimp only [B4]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c3]
        have c4 : tuple6 ((((S0 ^ t1 * 1) * 1) * 1)) ((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4)) ((((1 * S2 ^ t2) * 1) * 1)) ((((1 * 1) * S3 ^ t3) * 1)) ((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4)) ((((1 * 1) * 1) * S5 ^ t4)) * B5 = tuple6 (((((S0 ^ t1 * 1) * 1) * 1) * 1)) (((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5)) (((((1 * S2 ^ t2) * 1) * 1) * 1)) (((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5)) (((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1)) (((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5)) := by
          dsimp only [B5]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c4]
        have c5 : tuple6 (((((S0 ^ t1 * 1) * 1) * 1) * 1)) (((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5)) (((((1 * S2 ^ t2) * 1) * 1) * 1)) (((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5)) (((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1)) (((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5)) * B6 = tuple6 ((((((S0 ^ t1 * 1) * 1) * 1) * 1) * S0 ^ t6)) ((((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5) * 1)) ((((((1 * S2 ^ t2) * 1) * 1) * 1) * S2 ^ t6)) ((((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5) * 1)) ((((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1) * S4 ^ t6)) ((((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5) * 1)) := by
          dsimp only [B6]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c5]
        have c6 : tuple6 ((((((S0 ^ t1 * 1) * 1) * 1) * 1) * S0 ^ t6)) ((((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5) * 1)) ((((((1 * S2 ^ t2) * 1) * 1) * 1) * S2 ^ t6)) ((((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5) * 1)) ((((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1) * S4 ^ t6)) ((((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5) * 1)) * B7 = tuple6 (((((((S0 ^ t1 * 1) * 1) * 1) * 1) * S0 ^ t6) * S0 ^ t7)) (((((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5) * 1) * 1)) (((((((1 * S2 ^ t2) * 1) * 1) * 1) * S2 ^ t6) * S2 ^ t7)) (((((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5) * 1) * S3 ^ t7)) (((((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1) * S4 ^ t6) * 1)) (((((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5) * 1) * S5 ^ t7)) := by
          dsimp only [B7]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c6]
        have c7 : tuple6 (((((((S0 ^ t1 * 1) * 1) * 1) * 1) * S0 ^ t6) * S0 ^ t7)) (((((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5) * 1) * 1)) (((((((1 * S2 ^ t2) * 1) * 1) * 1) * S2 ^ t6) * S2 ^ t7)) (((((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5) * 1) * S3 ^ t7)) (((((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1) * S4 ^ t6) * 1)) (((((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5) * 1) * S5 ^ t7)) * B8 = tuple6 ((((((((S0 ^ t1 * 1) * 1) * 1) * 1) * S0 ^ t6) * S0 ^ t7) * 1)) ((((((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5) * 1) * 1) * (S1 ^ 2) ^ s1)) ((((((((1 * S2 ^ t2) * 1) * 1) * 1) * S2 ^ t6) * S2 ^ t7) * 1)) ((((((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5) * 1) * S3 ^ t7) * S3 ^ s1)) ((((((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1) * S4 ^ t6) * 1) * S4 ^ s1)) ((((((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5) * 1) * S5 ^ t7) * S5 ^ s1)) := by
          dsimp only [B8]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c7]
        have c8 : tuple6 ((((((((S0 ^ t1 * 1) * 1) * 1) * 1) * S0 ^ t6) * S0 ^ t7) * 1)) ((((((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5) * 1) * 1) * (S1 ^ 2) ^ s1)) ((((((((1 * S2 ^ t2) * 1) * 1) * 1) * S2 ^ t6) * S2 ^ t7) * 1)) ((((((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5) * 1) * S3 ^ t7) * S3 ^ s1)) ((((((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1) * S4 ^ t6) * 1) * S4 ^ s1)) ((((((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5) * 1) * S5 ^ t7) * S5 ^ s1)) * B9 = tuple6 (((((((((S0 ^ t1 * 1) * 1) * 1) * 1) * S0 ^ t6) * S0 ^ t7) * 1) * S0 ^ s4)) (((((((((S1 ^ t1 * S1 ^ t2) * S1 ^ t3) * S1 ^ t4) * S1 ^ t5) * 1) * 1) * (S1 ^ 2) ^ s1) * S1 ^ s4)) (((((((((1 * S2 ^ t2) * 1) * 1) * 1) * S2 ^ t6) * S2 ^ t7) * 1) * S2 ^ s4)) (((((((((1 * 1) * S3 ^ t3) * 1) * S3 ^ t5) * 1) * S3 ^ t7) * S3 ^ s1) * 1)) (((((((((S4 ^ t1 * S4 ^ t2) * S4 ^ t3) * S4 ^ t4) * 1) * S4 ^ t6) * 1) * S4 ^ s1) * (S4 ^ 2) ^ s4)) (((((((((1 * 1) * 1) * S5 ^ t4) * S5 ^ t5) * 1) * S5 ^ t7) * S5 ^ s1) * 1)) := by
          dsimp only [B9]
          exact tuple6_mul _ _ _ _ _ _ _ _ _ _ _ _
        rw [c8]
        congr 1 <;> simp only [U0, U1, U2, U3, U4, U5, Nat.mul_one, Nat.one_mul, Nat.pow_add, Nat.pow_mul, Nat.mul_assoc] <;> ac_rfl
      rw [hR] at hm8
      exact hm8
    have hfinal : S0 ^ U0 * S1 ^ U1 * S2 ^ U2 * S3 ^ U3 * S4 ^ U4 * S5 ^ U5 ≤
        S0 ^ w0 * S1 ^ w1 * S2 ^ w2 * S3 ^ w3 * S4 ^ w4 * S5 ^ w5 := by
      have hp0 := pow_mono_of_pos hS0p hU0
      have hp1 := pow_mono_of_pos hS1p hU1
      have hp2 := pow_mono_of_pos hS2p hU2
      have hp3 := pow_mono_of_pos hS3p hU3
      have hp4 := pow_mono_of_pos hS4p hU4
      have hp5 := pow_mono_of_pos hS5p hU5
      have hm1 := Nat.mul_le_mul hp0 hp1
      have hm2 := Nat.mul_le_mul hm1 hp2
      have hm3 := Nat.mul_le_mul hm2 hp3
      have hm4 := Nat.mul_le_mul hm3 hp4
      exact Nat.mul_le_mul hm4 hp5
    exact Nat.le_trans hbig hfinal


/-! ---------- CollisionCorrection: Möbius inversion over equality partitions ---------- -/

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_zero_int {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add]

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      simp only [integrate] at ih ⊢
      calc
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun _ => (0 : Int)))).sum =
            ((List.range n).map (fun _ : Nat => (0 : Int))).sum := by
              apply sum_congr
              intro x hx
              exact ih (put b v x)
        _ = 0 := by exact sum_zero_int (List.range n)

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp only [integrate] at ih ⊢
      rw [← sum_mul_left]
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons]; rw [integrate_add, ih]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private def heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private def rootOf (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun B => B.contains v)).bind List.head?).getD v

private def compatProp (vs : List Nat) (bs : List (List Nat)) (x : Bind) : Prop :=
  ∀ v ∈ vs, ∀ w ∈ vs, (bs.any (fun B => B.contains v && B.contains w)) = true → x v = x w

private noncomputable instance (vs : List Nat) (bs : List (List Nat)) (x : Bind) :
    Decidable (compatProp vs bs x) := Classical.propDecidable _

private theorem heads_subset (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ h ∈ heads bs, h ∈ pattern.freeVars := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem root_mem_heads (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ v ∈ pattern.freeVars, rootOf bs v ∈ heads bs := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem blocks_subset (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ B ∈ bs, ∀ v ∈ B, v ∈ pattern.freeVars := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem root_notfree (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ v, v ∉ pattern.freeVars → rootOf bs v = v := by
  intro v hv
  unfold rootOf
  have hnone : bs.find? (fun B => B.contains v) = none := by
    rw [List.find?_eq_none]
    intro B hB hc
    have hvB : v ∈ B := by simpa using hc
    exact hv (blocks_subset bs hb B hB v hvB)
  have hnone2 : bs.find? (fun B => decide (v ∈ B)) = none := by simpa using hnone
  simp [hnone2]

private theorem head_root (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ h ∈ heads bs, rootOf bs h = h := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem heads_nodup (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    (heads bs).Nodup := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem same_block_root (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ v ∈ pattern.freeVars, ∀ w ∈ pattern.freeVars,
      (bs.any (fun B => B.contains v && B.contains w)) = true → rootOf bs v = rootOf bs w := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem root_same_block (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ v ∈ pattern.freeVars,
      (bs.any (fun B => B.contains v && B.contains (rootOf bs v))) = true := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem mem_bindings_iff (n : Nat) (vs : List Nat) (b : Bind) (hnd : vs.Nodup) (y : Bind) :
    y ∈ bindings n vs b ↔
      (∀ v ∈ vs, y v ∈ List.range n) ∧ (∀ v ∉ vs, y v = b v) := by
  induction vs generalizing b with
  | nil =>
      constructor
      · intro hy
        constructor
        · intro v hv
          simp at hv
        · intro v hv
          have hyb : y = b := by simpa [bindings] using hy
          rw [hyb]
      · intro h
        have hyb : y = b := by
          funext v
          exact h.2 v (by simp)
        simp [bindings, hyb]
  | cons v vs ih =>
      have hnot : v ∉ vs := (List.nodup_cons.mp hnd).1
      have hnd' : vs.Nodup := (List.nodup_cons.mp hnd).2
      constructor
      · intro hy
        rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
        have hy' := (ih (put b v x) hnd').mp hy
        have hyv : y v = x := by
          have := hy'.2 v hnot
          simpa [put] using this
        constructor
        · intro w hw
          rcases List.mem_cons.mp hw with rfl | hw
          · simpa [hyv] using hx
          · exact hy'.1 w hw
        · intro w hw
          have hwv : w ≠ v := by intro h; subst w; exact hw (by simp)
          have hwvs : w ∉ vs := by intro h; exact hw (by simp [h])
          rw [hy'.2 w hwvs]
          simp [put, hwv]
      · intro h
        have hyv_mem : y v ∈ List.range n := h.1 v (by simp)
        apply List.mem_flatMap.mpr
        refine ⟨y v, hyv_mem, ?_⟩
        apply (ih (put b v (y v)) hnd').mpr
        constructor
        · intro w hw
          have hwv : w ≠ v := by intro he; subst w; exact hnot hw
          simpa [put, hwv] using h.1 w (by simp [hw])
        · intro w hw
          by_cases hwv : w = v
          · subst w
            simp [put]
          · have hwfull : w ∉ v :: vs := by
              intro hm
              rcases List.mem_cons.mp hm with he | he
              · exact hwv he
              · exact hw he
            rw [h.2 w hwfull]
            simp [put, hwv]

private theorem sum_perm_int {α} {l₁ l₂ : List α} (h : l₁.Perm l₂) (f : α → Int) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simp [ih]
  | swap x y l => simp [Int.add_assoc, Int.add_comm, Int.add_left_comm]
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem not_mem_erase_self_of_nodup {α} [BEq α] [LawfulBEq α] {a : α} {l : List α}
    (hnd : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hbnd : b ∉ bs := (List.nodup_cons.mp hnd).1
      have hndbs : bs.Nodup := (List.nodup_cons.mp hnd).2
      by_cases h : a == b
      · have hab : a = b := beq_iff_eq.mp h
        subst b
        simpa using hbnd
      · have hne : a ≠ b := by intro he; subst b; simp at h
        have hba : (b == a) = false := by
          cases hba' : b == a
          · rfl
          · have : b = a := beq_iff_eq.mp hba'
            exact False.elim (hne this.symm)
        simp [List.erase, hba, hne, ih hndbs]

private theorem perm_of_nodup_mem {α} [DecidableEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hl2 : l₂ = [] := by
        cases l₂ with
        | nil => rfl
        | cons a as =>
            have ha2 : a ∈ a :: as := by simp
            have ha1 := (hm a).mpr ha2
            simp at ha1
      subst l₂
      exact List.Perm.nil
  | cons a l₁ ih =>
      have ha2 : a ∈ l₂ := (hm a).mp (by simp)
      have hnot1 : a ∉ l₁ := (List.nodup_cons.mp h₁).1
      have hnd1 : l₁.Nodup := (List.nodup_cons.mp h₁).2
      have hnd2e : (l₂.erase a).Nodup := h₂.erase a
      have hm' : ∀ x, x ∈ l₁ ↔ x ∈ l₂.erase a := by
        intro x
        constructor
        · intro hx
          have hx2 : x ∈ l₂ := (hm x).mp (by simp [hx])
          have hxa : x ≠ a := by intro he; subst x; exact hnot1 hx
          exact (List.mem_erase_of_ne hxa).mpr hx2
        · intro hx
          have hx2 : x ∈ l₂ := List.mem_of_mem_erase hx
          have hxcons : x ∈ a :: l₁ := (hm x).mpr hx2
          rcases List.mem_cons.mp hxcons with heq | hxl
          · subst x
            exact False.elim (not_mem_erase_self_of_nodup h₂ hx)
          · exact hxl
      exact (List.Perm.cons a (ih hnd1 hnd2e hm')).trans (List.perm_cons_erase ha2).symm

private theorem sum_if_prop {α} (l : List α) (p : α → Prop) [DecidablePred p] (f : α → Int) :
    (l.map (fun x => if p x then f x else 0)).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases hp : p a <;> simp [hp, ih]

private theorem collapsed_eq_integrate (n : Nat) (db : DB) (b : Bind)
    (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    collapsed pattern n db b bs =
      integrate n pattern.freeVars b (fun x =>
        if compatProp pattern.freeVars bs x then
          (pattern.atoms.map (fun a => value db a x)).prod else 0) := by
  classical
  let ext : Bind → Bind := fun y v => y (rootOf bs v)
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hsub := heads_subset bs hb
  have hrootmem := root_mem_heads bs hb
  have hrootnot := root_notfree bs hb
  have hheadroot := head_root bs hb
  have hndheads := heads_nodup bs hb
  have hsameroot := same_block_root bs hb
  have hndvs : pattern.freeVars.Nodup := by native_decide
  have hperm : (bindings n (heads bs) b).map ext |>.Perm
      ((bindings n pattern.freeVars b).filter (compatProp pattern.freeVars bs)) := by
    apply perm_of_nodup_mem
    · apply nodup_map
      · intro y hy z hz heq
        funext h
        by_cases hh : h ∈ heads bs
        · have hr : rootOf bs h = h := hheadroot h hh
          have he := congrFun heq h
          simpa [ext, hr] using he
        · have hyb := bindings_fixed n (heads bs) b y hy h hh
          have hzb := bindings_fixed n (heads bs) b z hz h hh
          rw [hyb, hzb]
      · exact bindings_nodup n (heads bs) b hndheads
    · exact (bindings_nodup n pattern.freeVars b hndvs).filter _
    · intro z
      constructor
      · intro hzmap
        rcases List.mem_map.mp hzmap with ⟨y, hy, rfl⟩
        simp only [List.mem_filter]
        constructor
        · apply (mem_bindings_iff n pattern.freeVars b hndvs (ext y)).mpr
          constructor
          · intro v hv
            exact bindings_range n (heads bs) b y hy (rootOf bs v) (hrootmem v hv)
          · intro v hv
            have hr : rootOf bs v = v := hrootnot v hv
            have hvh : v ∉ heads bs := by intro hh; exact hv (hsub v hh)
            have hyb := bindings_fixed n (heads bs) b y hy v hvh
            simpa [ext, hr] using hyb
        · rw [decide_eq_true_eq]
          intro v hv w hw hsame
          have hr := hsameroot v hv w hw hsame
          change y (rootOf bs v) = y (rootOf bs w)
          rw [hr]
      · intro hz
        have hzbind : z ∈ bindings n pattern.freeVars b := (List.mem_filter.mp hz).1
        have hzcompat : compatProp pattern.freeVars bs z :=
          of_decide_eq_true (List.mem_filter.mp hz).2
        let restr : Bind := fun (u : Nat) => if u ∈ heads bs then z u else b u
        apply List.mem_map.mpr
        refine ⟨restr, ?_, ?_⟩
        · apply (mem_bindings_iff n (heads bs) b hndheads restr).mpr
          constructor
          · intro h hh
            simpa [restr, hh] using bindings_range n pattern.freeVars b z hzbind h (hsub h hh)
          · intro h hh
            simp [restr, hh]
        · funext v
          by_cases hv : v ∈ pattern.freeVars
          · have hr := hrootmem v hv
            have hrb := root_same_block bs hb v hv
            have hzeq := hzcompat v hv (rootOf bs v) (hsub (rootOf bs v) hr) hrb
            simpa [ext, restr, hr] using hzeq.symm
          · have hr : rootOf bs v = v := hrootnot v hv
            have hvh : v ∉ heads bs := by intro hh; exact hv (hsub v hh)
            have hzb := bindings_fixed n pattern.freeVars b z hzbind v hv
            simpa [ext, restr, hr, hvh] using hzb.symm
  rw [collapsed, bindings_sum, bindings_sum]
  change ((bindings n (heads bs) b).map (fun y => F (ext y))).sum =
    ((bindings n pattern.freeVars b).map (fun x => if compatProp pattern.freeVars bs x then F x else 0)).sum
  have hp := sum_perm_int hperm F
  rw [List.map_map] at hp
  rw [← sum_if_prop (bindings n pattern.freeVars b) (compatProp pattern.freeVars bs) F] at hp
  exact hp

private def headOfB : List Nat → Nat
  | h :: _ => h
  | [] => 0

private def constOnB (bs : List (List Nat)) (x : Bind) : Bool :=
  bs.all (fun bl => bl.all (fun v => x v == x (headOfB bl)))

private theorem blocks_nonempty (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) :
    ∀ B ∈ bs, B ≠ [] := by
  simp [partitions, pattern] at hb
  rcases hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl
  <;> native_decide

private theorem constOnB_eq_compat (bs : List (List Nat)) (hb : bs ∈ partitions pattern.freeVars) (x : Bind) :
    (constOnB bs x = true) ↔ compatProp pattern.freeVars bs x := by
  constructor
  · intro h v hv w hw hsame
    rcases List.any_eq_true.mp hsame with ⟨B, hBmem, hBand⟩
    have hBv : B.contains v = true := (Bool.and_eq_true_iff.mp hBand).1
    have hBw : B.contains w = true := (Bool.and_eq_true_iff.mp hBand).2
    have hnon := blocks_nonempty bs hb B hBmem
    have hhead : headOfB B ∈ B := by
      cases B with
      | nil => contradiction
      | cons c cs => simp [headOfB]
    have hBall : B.all (fun u => x u == x (headOfB B)) = true :=
      List.all_eq_true.mp h B hBmem
    have hvMem : v ∈ B := by simpa using hBv
    have hwMem : w ∈ B := by simpa using hBw
    have hvEq : x v = x (headOfB B) := by
      have := List.all_eq_true.mp hBall v hvMem
      exact beq_iff_eq.mp this
    have hwEq : x w = x (headOfB B) := by
      have := List.all_eq_true.mp hBall w hwMem
      exact beq_iff_eq.mp this
    exact hvEq.trans hwEq.symm
  · intro h
    apply List.all_eq_true.mpr
    intro B hBmem
    apply List.all_eq_true.mpr
    intro v hvB
    have hnon := blocks_nonempty bs hb B hBmem
    have hhead : headOfB B ∈ B := by
      cases B with
      | nil => contradiction
      | cons c cs => simp [headOfB]
    have hs : (bs.any (fun B' => B'.contains v && B'.contains (headOfB B))) = true := by
      apply List.any_eq_true.mpr
      refine ⟨B, hBmem, ?_⟩
      exact Bool.and_eq_true_iff.mpr ⟨by simpa using hvB, by simpa using hhead⟩
    have hsub := blocks_subset bs hb
    exact beq_iff_eq.mpr (h v (hsub B hBmem v hvB) (headOfB B) (hsub B hBmem (headOfB B) hhead) hs)

private theorem mobius_sum_bool (x : Bind) :
  ((partitions pattern.freeVars).map (fun bs => if constOnB bs x then mobius bs else 0)).sum =
    if (pattern.freeVars.map x).Nodup then 1 else 0 := by
  have e10 : (x 1 = x 0) = (x 0 = x 1) := by exact propext (eq_comm : (x 1 = x 0) ↔ (x 0 = x 1))
  have e20 : (x 2 = x 0) = (x 0 = x 2) := by exact propext (eq_comm : (x 2 = x 0) ↔ (x 0 = x 2))
  have e30 : (x 3 = x 0) = (x 0 = x 3) := by exact propext (eq_comm : (x 3 = x 0) ↔ (x 0 = x 3))
  have e40 : (x 4 = x 0) = (x 0 = x 4) := by exact propext (eq_comm : (x 4 = x 0) ↔ (x 0 = x 4))
  have e21 : (x 2 = x 1) = (x 1 = x 2) := by exact propext (eq_comm : (x 2 = x 1) ↔ (x 1 = x 2))
  have e31 : (x 3 = x 1) = (x 1 = x 3) := by exact propext (eq_comm : (x 3 = x 1) ↔ (x 1 = x 3))
  have e41 : (x 4 = x 1) = (x 1 = x 4) := by exact propext (eq_comm : (x 4 = x 1) ↔ (x 1 = x 4))
  have e32 : (x 3 = x 2) = (x 2 = x 3) := by exact propext (eq_comm : (x 3 = x 2) ↔ (x 2 = x 3))
  have e42 : (x 4 = x 2) = (x 2 = x 4) := by exact propext (eq_comm : (x 4 = x 2) ↔ (x 2 = x 4))
  have e43 : (x 4 = x 3) = (x 3 = x 4) := by exact propext (eq_comm : (x 4 = x 3) ↔ (x 3 = x 4))
  by_cases h01 : x 0 = x 1 <;> by_cases h02 : x 0 = x 2 <;> by_cases h03 : x 0 = x 3 <;> by_cases h04 : x 0 = x 4 <;> by_cases h12 : x 1 = x 2 <;> by_cases h13 : x 1 = x 3 <;> by_cases h14 : x 1 = x 4 <;> by_cases h23 : x 2 = x 3 <;> by_cases h24 : x 2 = x 4 <;> by_cases h34 : x 3 = x 4 <;>
  simp [pattern, partitions, mobius, factorial, constOnB, headOfB, e10, e20, e30, e40, e21, e31, e41, e32, e42, e43, h01, h02, h03, h04, h12, h13, h14, h23, h24, h34]

private theorem mobius_sum (x : Bind) :
  ((partitions pattern.freeVars).map (fun bs => if compatProp pattern.freeVars bs x then mobius bs else 0)).sum =
    if (pattern.freeVars.map x).Nodup then 1 else 0 := by
  have hfun : (partitions pattern.freeVars).map
      (fun bs => if compatProp pattern.freeVars bs x then mobius bs else 0) =
      (partitions pattern.freeVars).map
      (fun bs => if constOnB bs x then mobius bs else 0) := by
    apply List.map_congr_left
    intro bs hb
    by_cases hc : compatProp pattern.freeVars bs x
    · have ht : constOnB bs x = true := (constOnB_eq_compat bs hb x).mpr hc
      simp [hc, ht]
    · have ht : constOnB bs x = false := by
        by_cases hh : constOnB bs x = true
        · exact False.elim (hc ((constOnB_eq_compat bs hb x).mp hh))
        · exact Bool.eq_false_of_ne_true hh
      simp [hc, ht]
  rw [hfun]
  exact mobius_sum_bool x

private theorem sum_mul_ite_const {α} (l : List α) (p : α → Prop) [DecidablePred p]
    (c : α → Int) (K : Int) :
    (l.map (fun a => c a * if p a then K else 0)).sum =
      K * (l.map (fun a => if p a then c a else 0)).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a
      · simp [hp, ih, Int.mul_add, Int.add_mul, Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]
      · simp [hp, ih]

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.CollisionCorrection.Goal := by
  intro n db b
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  unfold injective
  have hR : ((partitions pattern.freeVars).map
      (fun bs => mobius bs * collapsed pattern n db b bs)).sum =
      integrate n pattern.freeVars b (fun x =>
        ((partitions pattern.freeVars).map
          (fun bs => mobius bs * if compatProp pattern.freeVars bs x then F x else 0)).sum) := by
    rw [integrate_list_sum]
    apply sum_congr
    intro bs hb
    rw [integrate_mul_left]
    rw [collapsed_eq_integrate n db b bs hb]
  rw [hR]
  apply integrate_congr
  intro x
  have hm := mobius_sum x
  rw [sum_mul_ite_const, hm]
  by_cases hnd : (pattern.freeVars.map x).Nodup
  · simp [hnd, F]
  · simp [hnd]

end TinylambOpt.ResearchProofs.Round2.Task71
