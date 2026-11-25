import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_CoLikes_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_CountryReturn_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_CountryReturn_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_OrderSupplier_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_OrderSupplier_Remaining_Round2

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J3

namespace ForumWork

open TinylambOpt.ResearchGoals.Atlas.JoinPlans

private theorem cover_decomp : ∀ den w0 w1 w2 w3 w4 w5 : Nat,
    den ≤ w0 + w1 → den ≤ w1 + w2 → den ≤ w3 + w4 → den ≤ w4 + w5 →
    den ≤ w0 + w2 + w3 + w5 →
    ∃ t1 t2 t3 t4 t5 t6 t7 s1 s4 : Nat,
      t1 + t2 + t3 + t4 + t5 + t6 + t7 + 2 * (s1 + s4) = den ∧
      (t1 + t6 + t7) + s4 ≤ w0 ∧
      (t1 + t2 + t3 + t4 + t5) + 2*s1 ≤ w1 ∧
      (t2 + t6 + t7) + s4 ≤ w2 ∧
      (t3 + t5 + t7) + s1 ≤ w3 ∧
      (t1 + t2 + t3 + t4 + t6) + 2*s4 ≤ w4 ∧
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
    let U1 := (t1 + t2 + t3 + t4 + t5) + 2*s1
    let U2 := (t2 + t6 + t7) + s4
    let U3 := (t3 + t5 + t7) + s1
    let U4 := (t1 + t2 + t3 + t4 + t6) + 2*s4
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
        simp only [Nat.mul_pow, Nat.pow_add]
        ac_rfl
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

end ForumWork

namespace CollisionWork

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
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
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      exact (rsum_zero_const m).symm
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

/-- Partition-helper definitions mirroring the `collapsed` definition. -/
private def headOf : List Nat → Nat
  | h :: _ => h
  | [] => 0

private def Heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private def Root (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun bl => bl.contains v)).bind List.head?).getD v

private def constOn (bs : List (List Nat)) (x : Bind) : Bool :=
  bs.all (fun bl => bl.all (fun v => x v == x (headOf bl)))

/-- Boolean structural spec of a partition of the fixed variable list. -/
private def PSb (V : List Nat) (bs : List (List Nat)) : Bool :=
  decide bs.Nodup &&
  bs.all (fun bl => decide bl.Nodup && bl.all (fun v => V.contains v)) &&
  bs.all (fun bl1 => bs.all (fun bl2 =>
    bl1.all (fun v => if bl2.contains v then bl1 == bl2 else true))) &&
  V.all (fun v => bs.any (fun bl => bl.contains v))

private theorem ps_all : (partitions pattern.freeVars).all (fun bs => PSb pattern.freeVars bs) = true := by
  native_decide

private theorem ps_of_mem {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    PSb pattern.freeVars bs = true :=
  List.all_eq_true.mp ps_all bs hbs

private theorem find_eq_some_unique {α} (p : α → Bool) (l : List α) (a : α)
    (ha : a ∈ l) (hp : p a = true) (huniq : ∀ b ∈ l, p b = true → b = a) :
    l.find? p = some a := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x
        simp [hp]
      · have hax : a ≠ x := by intro h; exact hxa h.symm
        have haxs : a ∈ xs := by simpa [hax] using ha
        have hpx : p x = false := by
          cases hpx' : p x with
          | true =>
              have hxeq : x = a := huniq x (by simp) hpx'
              exact False.elim (hxa hxeq)
          | false => rfl
        simp [hpx, ih haxs (fun b hb hpb => huniq b (by simp [hb]) hpb)]

private theorem root_eq_head (bs : List (List Nat)) (hPS : PSb pattern.freeVars bs = true) :
    ∀ bl ∈ bs, ∀ v ∈ bl, Root bs v = headOf bl := by
  simp [PSb] at hPS
  rcases hPS with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro bl hbl v hv
  have hpbl : bl.contains v = true := by simpa using hv
  have huniq : ∀ b ∈ bs, b.contains v = true → b = bl := by
    intro b hb hbv
    have hdeq := hdisj bl hbl b hb v hv
    have hbmem : v ∈ b := by simpa using hbv
    rcases hdeq with hnot | heq
    · exact False.elim (hnot hbmem)
    · exact heq.symm
  have hfind : bs.find? (fun b : List Nat => b.contains v) = some bl :=
    find_eq_some_unique (fun b : List Nat => b.contains v) bs bl hbl hpbl huniq
  unfold Root
  rw [hfind]
  cases bl with
  | nil => simp at hv
  | cons h tl => rfl

private theorem head_mem_heads_cons (bl : List Nat) (bs : List (List Nat))
    (hs : bl.head?.isSome = true) : headOf bl ∈ Heads (bl :: bs) := by
  cases bl with
  | nil => simp at hs
  | cons h tl => simp [Heads, headOf]

private theorem head_mem_heads {bl : List Nat} {bs : List (List Nat)}
    (hbl : bl ∈ bs) : bl.head?.isSome = true → headOf bl ∈ Heads bs := by
  induction bs with
  | nil => simp at hbl
  | cons b bs ih =>
      intro hs
      rcases List.mem_cons.mp hbl with hbeq | hbl
      · subst bl
        exact head_mem_heads_cons b bs hs
      · have htail := ih hbl hs
        cases hb : b.head? with
        | none => simpa [Heads, hb] using htail
        | some h =>
            simp [Heads, hb]
            apply Or.inr
            exact List.mem_filterMap.mp htail

private def HPb (bs : List (List Nat)) : Bool :=
  decide (Heads bs).Nodup && (Heads bs).all (fun h => pattern.freeVars.contains h)

private theorem hp_all : (partitions pattern.freeVars).all HPb = true := by
  native_decide

private theorem heads_spec {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    (Heads bs).Nodup ∧ (∀ h ∈ Heads bs, h ∈ pattern.freeVars) := by
  have h := List.all_eq_true.mp hp_all bs hbs
  simp [HPb] at h
  exact h

private theorem find_none_of_all_false {α} (p : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = false) : l.find? p = none := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.find?, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem root_out {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v, v ∉ pattern.freeVars → Root bs v = v := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  have hfalse : ∀ bl ∈ bs, bl.contains v = false := by
    intro bl hbl
    have hbsub := hsub bl hbl
    by_cases hne : bl.contains v = true
    · have hvbl : v ∈ bl := by simpa using hne
      exact False.elim (hv (hbsub.2 v hvbl))
    · cases hne' : bl.contains v <;> simp_all
  unfold Root
  rw [find_none_of_all_false (fun bl : List Nat => bl.contains v) bs hfalse]
  simp

private theorem root_head {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ h ∈ Heads bs, Root bs h = h := by
  intro h hh
  rcases List.mem_filterMap.mp hh with ⟨bl, hbl, hhbl⟩
  have hhroot : headOf bl = h := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simpa [headOf] using hhbl
  have hvbl : headOf bl ∈ bl := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simp [headOf]
  rw [← hhroot]
  exact root_eq_head bs hPS bl hbl (headOf bl) hvbl

private theorem root_mem_heads {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v ∈ pattern.freeVars, Root bs v ∈ Heads bs := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  rcases hcov v hv with ⟨bl, hbl, hvbl⟩
  rw [root_eq_head bs hPS bl hbl v hvbl]
  exact head_mem_heads hbl (by cases bl with | nil => simp at hvbl | cons h t => simp)

private theorem constOn_iff {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) (x : Bind) :
    constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  constructor
  · intro hc v hv
    rcases hcov v hv with ⟨bl, hbl, hvbl⟩
    rw [root_eq_head bs hPS bl hbl v hvbl]
    have hbeq := List.all_eq_true.mp hc bl hbl
    have hveq := List.all_eq_true.mp hbeq v hvbl
    simpa using hveq
  · intro hx
    apply List.all_eq_true.mpr
    intro bl hbl
    apply List.all_eq_true.mpr
    intro v hvbl
    have hvV : v ∈ pattern.freeVars := (hsub bl hbl).2 v hvbl
    have hrv := hx v hvV
    rw [root_eq_head bs hPS bl hbl v hvbl] at hrv
    simpa using hrv

/-- Integration helpers (Std-only). -/
private theorem sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_eq_zero {α} (l : List α) (f : α → Int) (h : ∀ x ∈ l, f x = 0) :
    (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]
      simp

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i == v
  · by_cases hw : i == w
    · have hiv : i = v := Nat.eq_of_beq_eq_true hv
      have hiw : i = w := Nat.eq_of_beq_eq_true hw
      have hvw : v = w := by omega
      simp [hvw] at h
    · simp [put, hv, hw]
  · by_cases hw : i == w <;> simp [put, hv, hw]

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by cases h' : (v == w) <;> simp_all
    change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    change rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f))
    rw [rsum_comm]
    apply sum_congr
    intro x hx
    apply sum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem integrate_append (n : Nat) (xs ys : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (xs ++ ys) b f = integrate n xs b (fun b' => integrate n ys b' f) := by
  induction xs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n (vs ++ ys) (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun b' => integrate n ys b' f))).sum
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys)
    (b : Bind) (f : Bind → Int) : integrate n xs b f = integrate n ys b f := by
  induction h generalizing b with
  | nil => rfl
  | cons x h ih =>
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      apply sum_congr
      intro t ht
      exact ih (put b x t)
  | swap x y l => exact (integrate_swap n x y l b f).symm
  | trans h1 h2 ih1 ih2 => exact (ih1 b).trans (ih2 b)

private def bindings (n : Nat) : List Nat → Bind → List Bind
  | [], b => [b]
  | v :: vs, b => (List.range n).flatMap (fun x => bindings n vs (put b v x))

private theorem bindings_sum (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = ((bindings n vs b).map f).sum := by
  induction vs generalizing b with
  | nil => simp [integrate, bindings]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        (((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f).sum
      rw [show ((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f =
          (List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f) by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.map_append, ih2]]
      rw [show ((List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f)).sum =
          ((List.range n).map (fun x => ((bindings n vs (put b v x)).map f).sum)).sum by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.sum_append, ih2]]
      apply sum_congr
      intro x hx
      exact ih (put b v x)

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
      rw [ih (put b w x) y hy v hvws]
      simp [put, hvw]

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
          rw [heq]
          simpa [put] using hx
      · have htail : v ∈ ws := by
          rcases List.mem_cons.mp hv with h | h
          · exact (hvw h.symm).elim
          · exact h
        exact ih (put b w x) y hy v htail

private theorem integrate_congr_bindings (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ y, y ∈ bindings n vs b → f y = g y) :
    integrate n vs b f = integrate n vs b g := by
  rw [bindings_sum, bindings_sum]
  apply sum_congr
  intro y hy
  exact h y hy

private theorem rsum_ite_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : a = n
      · subst a
        have hzero : rsum n (fun x => if x = n then f x else 0) = 0 := by
          rw [show rsum n (fun x => if x = n then f x else 0) = rsum n (fun _ => (0 : Int)) by
            apply rsum_congr
            intro x hx
            have hxn : x ≠ n := by omega
            simp [hxn]]
          exact rsum_zero_const n
        rw [hzero]
        simp
      · have han : a < n := by omega
        rw [ih han]
        have hne : ¬ n = a := by omega
        simp [hne]

private theorem integrate_zero_of_false (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (h : ∀ y ∈ bindings n vs b, f y = 0) : integrate n vs b f = 0 := by
  rw [bindings_sum]
  apply sum_eq_zero
  intro y hy
  exact h y hy

private theorem inner_collapse (n : Nat) (M : List Nat) (Root : Nat → Nat) (y : Bind) (C : Int)
    (hM : M.Nodup) (hRnot : ∀ m ∈ M, Root m ∉ M)
    (hy : ∀ m ∈ M, y (Root m) ∈ List.range n) :
    integrate n M y (fun x => if M.all (fun m => x m == y (Root m)) then C else 0) = C := by
  induction M generalizing y with
  | nil => simp [integrate]
  | cons m Ms ih =>
      have hmMs : m ∉ Ms := (List.nodup_cons.mp hM).1
      have hMnd : Ms.Nodup := (List.nodup_cons.mp hM).2
      have hRnot' : ∀ v ∈ Ms, Root v ∉ Ms := by
        intro v hv hmem
        exact hRnot v (by simp [hv]) (by simp [hmem])
      have hrootne : ∀ v ∈ Ms, Root v ≠ m := by
        intro v hv h_eq
        exact hRnot v (by simp [hv]) (by simp [h_eq])
      have hpoint : ∀ t < n,
          integrate n Ms (put y m t)
            (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) =
          if t = y (Root m) then C else 0 := by
        intro t ht
        by_cases htm : t = y (Root m)
        · subst t
          have hyrange' : ∀ v ∈ Ms, (put y m (y (Root m))) (Root v) ∈ List.range n := by
            intro v hv
            have hne := hrootne v hv
            simpa [put, hne] using hy v (by simp [hv])
          calc
            integrate n Ms (put y m (y (Root m)))
                (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)
                = integrate n Ms (put y m (y (Root m)))
                    (fun x => if Ms.all (fun v => x v == (put y m (y (Root m))) (Root v)) then C else 0) := by
                    apply integrate_congr_bindings
                    intro x hx
                    have hxm : x m = y (Root m) := by
                      rw [bindings_fixed n Ms (put y m (y (Root m))) x hx m hmMs]
                      simp [put]
                    simp [hxm]
                    by_cases hleft : ∀ v ∈ Ms, x v = y (Root v)
                    · have hright : ∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v) := by
                        intro v hv
                        have hv' := hleft v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_left hleft, ite_eq_left hright]
                    · have hrightnot : ¬ (∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v)) := by
                        intro hr
                        apply hleft
                        intro v hv
                        have hv' := hr v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_right hleft, ite_eq_right hrightnot]
            _ = C := ih (put y m (y (Root m))) hMnd hRnot' hyrange'
            _ = (if y (Root m) = y (Root m) then C else 0) := by simp
        · have hzero : integrate n Ms (put y m t)
              (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) = 0 := by
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htm]
          simpa [htm] using hzero
      change rsum n (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) = C
      rw [show (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) =
        (fun t => if t = y (Root m) then C else 0) by
          funext t
          by_cases ht : t < n
          · exact hpoint t ht
          · have htn : y (Root m) < n := by
              have := hy m (by simp)
              simpa using this
            have htne : t ≠ y (Root m) := by omega
            simp [htne]
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htne]]
      rw [rsum_ite_eq_single n (y (Root m)) (by
        have := hy m (by simp)
        simpa using this)]

private theorem perm_append_left {α} {l1 l2 : List α} (h : l1.Perm l2) (l3 : List α) :
    (l1 ++ l3).Perm (l2 ++ l3) := by
  induction h with
  | nil => rfl
  | cons x h ih => exact .cons x ih
  | swap x y l => exact .swap x y (l ++ l3)
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_append_right {α} (l1 : List α) {l3 l4 : List α} (h : l3.Perm l4) :
    (l1 ++ l3).Perm (l1 ++ l4) := by
  induction l1 with
  | nil => simpa using h
  | cons a l1 ih => exact .cons a ih

private theorem perm_append {α} {l1 l2 l3 l4 : List α} (h1 : l1.Perm l2) (h2 : l3.Perm l4) :
    (l1 ++ l3).Perm (l2 ++ l4) := by
  exact (perm_append_left h1 l3).trans (perm_append_right l2 h2)

private theorem cons_append_perm (a : Nat) (l : List Nat) : (a :: l).Perm (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons x l ih => exact (List.Perm.swap x a l).trans (.cons x ih)

private theorem filter_append_compl (p : Nat → Bool) (l : List Nat) :
    l.Perm ((l.filter p) ++ (l.filter (fun x => !p x))) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : p a = true
      · simpa [ha] using (List.Perm.cons a ih)
      · have ha' : p a = false := by cases h : p a <;> simp_all
        have hstep := (List.Perm.cons a ih).trans
          (perm_append (cons_append_perm a (l.filter p))
            (List.Perm.refl (l.filter (fun x => !p x))))
        simpa [ha'] using hstep

private theorem perm_filter_of_nodup {l l' : List Nat} (hnd : l.Nodup) (hnd' : l'.Nodup)
    (hsub : ∀ x ∈ l, x ∈ l') : l.Perm (l'.filter (fun x => x ∈ l)) := by
  induction l' generalizing l with
  | nil =>
      have hl : l = [] := by
        cases l with
        | nil => rfl
        | cons a as => exact False.elim (by simpa using hsub a (by simp))
      subst l
      simp
  | cons a l' ih =>
      have hnot : a ∉ l' := (List.nodup_cons.mp hnd').1
      by_cases ha : a ∈ l
      · have htail := ih (hnd.erase a) (List.nodup_cons.mp hnd').2 (by
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnd.not_mem_erase hx
          have hxl := (List.mem_erase_of_ne hxa).mp hx
          simpa [hxa] using hsub x hxl)
        have hab : decide (a ∈ l) = true := by simp [ha]
        have hfilter : (a :: l').filter (fun x => x ∈ l) =
            a :: l'.filter (fun x : Nat => x ∈ l.erase a) := by
          rw [List.filter_cons, ite_eq_left hab]
          congr 1
          apply List.filter_congr
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnot hx
          simp [List.mem_erase_of_ne hxa]
        rw [hfilter]
        exact (List.perm_cons_erase ha).trans (.cons a htail)
      · rw [List.filter_cons]
        simp [ha]
        apply ih hnd (List.nodup_cons.mp hnd').2
        intro x hx
        have hxa : x ≠ a := by intro h; subst x; exact ha hx
        simpa [hxa] using hsub x hx

private theorem collapse_sum (n : Nat) (b : Bind) (F : Bind → Int)
    (bs : List (List Nat)) (hbs : bs ∈ partitions pattern.freeVars)
    (hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → F x = F y) :
    integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) =
      integrate n pattern.freeVars b (fun x => if constOn bs x then F x else 0) := by
  have hPS := ps_of_mem hbs
  have hspec := heads_spec hbs
  have hVnd : pattern.freeVars.Nodup := by native_decide
  let H := Heads bs
  let M := pattern.freeVars.filter (fun v => !H.contains v)
  have hHsub : ∀ v ∈ H, v ∈ pattern.freeVars := hspec.2
  have hrootV : ∀ v ∈ pattern.freeVars, Root bs v ∈ H := by
    intro v hv
    exact root_mem_heads hPS v hv
  have hrootH : ∀ h ∈ H, Root bs h = h := by
    intro h hh
    exact root_head hPS h hh
  have hrootOut : ∀ v, v ∉ pattern.freeVars → Root bs v = v := root_out hPS
  have hconst : ∀ x, constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := constOn_iff hPS
  have hfilt : (pattern.freeVars.filter (fun v => H.contains v)).Perm H := by
    have hp := perm_filter_of_nodup hspec.1 hVnd hHsub
    simpa using hp.symm
  have hpermV : pattern.freeVars.Perm (H ++ M) := by
    have h1 := filter_append_compl H.contains pattern.freeVars
    have h2 : (pattern.freeVars.filter (fun v => H.contains v) ++
        pattern.freeVars.filter (fun v => !H.contains v)).Perm
        (H ++ M) := perm_append hfilt (by rfl)
    exact h1.trans h2
  rw [integrate_perm n hpermV b (fun x => if constOn bs x then F x else 0)]
  rw [integrate_append]
  apply integrate_congr_bindings
  intro y hy
  have hyH : ∀ h ∈ H, y h ∈ List.range n := by
    intro h hh
    exact bindings_range n H b y hy h hh
  have hyfixed : ∀ v ∉ H, y v = b v := by
    intro v hv
    exact bindings_fixed n H b y hy v hv
  let x0 : Bind := fun v => y (Root bs v)
  have hx0V : ∀ v ∈ pattern.freeVars, x0 v ∈ List.range n := by
    intro v hv
    have hr := hrootV v hv
    simpa [x0] using hyH (Root bs v) hr
  have hMsub : ∀ v ∈ M, v ∈ pattern.freeVars := by
    intro v hv
    exact (List.mem_filter.mp hv).1
  have hMnotH : ∀ v ∈ M, v ∉ H := by
    intro v hv
    have hnot := (List.mem_filter.mp hv).2
    simpa using hnot
  have hMnd : M.Nodup := hVnd.filter _
  have hRnot : ∀ m ∈ M, Root bs m ∉ M := by
    intro m hm hmem
    have hr := hrootV m (hMsub m hm)
    exact hMnotH (Root bs m) hmem hr
  have hyrange : ∀ m ∈ M, y (Root bs m) ∈ List.range n := by
    intro m hm
    exact hyH (Root bs m) (hrootV m (hMsub m hm))
  have hinner : integrate n M y (fun x => if constOn bs x then F x else 0) = F x0 := by
    calc
      integrate n M y (fun x => if constOn bs x then F x else 0)
          = integrate n M y (fun x => if M.all (fun m => x m == y (Root bs m)) then F x0 else 0) := by
            apply integrate_congr_bindings
            intro x hx
            have hxfixedH : ∀ h ∈ H, x h = y h := by
              intro h hh
              have hnot : h ∉ M := by
                intro hm
                exact hMnotH h hm hh
              rw [bindings_fixed n M y x hx h hnot]
            by_cases hall : M.all (fun m => x m == y (Root bs m)) = true
            · rw [ite_eq_left hall]
              have hxFx0 : ∀ v ∈ pattern.freeVars, x v = x0 v := by
                intro v hv
                by_cases hvH : v ∈ H
                · have hrv := hrootH v hvH
                  change x v = y (Root bs v)
                  rw [hxfixedH v hvH, hrv]
                · have hvM : v ∈ M := by
                    simp [M, hv, hvH]
                  have hvEq := List.all_eq_true.mp hall v hvM
                  change x v = y (Root bs v)
                  simpa using hvEq
              have hcond : ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
                intro v hv
                have hxv := hxFx0 v hv
                have hxroot : x (Root bs v) = y (Root bs v) :=
                  hxfixedH (Root bs v) (hrootV v hv)
                exact hxv.trans (by simpa [x0] using hxroot.symm)
              rw [ite_eq_left ((hconst x).2 hcond)]
              have hx0eq : x 0 = x0 0 := by
                by_cases h0H : 0 ∈ H
                · have hxfixed0 : x 0 = y 0 := hxfixedH 0 h0H
                  have h0root : Root bs 0 = 0 := hrootH 0 h0H
                  change x 0 = y (Root bs 0)
                  rw [hxfixed0, h0root]
                · have h0M : 0 ∈ M := by simp [M, h0H, pattern]
                  have h0eq := List.all_eq_true.mp hall 0 h0M
                  simpa [x0] using h0eq
              exact hF x x0 hxFx0
            · have hallnot : ¬ M.all (fun m => x m == y (Root bs m)) = true := by
                intro h
                exact hall h
              rw [ite_eq_right hallnot]
              have hcondnot : ¬ (∀ v ∈ pattern.freeVars, x v = x (Root bs v)) := by
                intro hc
                apply hall
                apply List.all_eq_true.mpr
                intro m hm
                have hcv := hc m (hMsub m hm)
                have hrv := hrootV m (hMsub m hm)
                have hxrv : x (Root bs m) = y (Root bs m) := hxfixedH (Root bs m) hrv
                simp [hcv, hxrv]
              have hconstnot : ¬ constOn bs x = true := by
                intro hx'
                exact hcondnot ((hconst x).1 hx')
              rw [ite_eq_right hconstnot]
      _ = F x0 := inner_collapse n M (fun v => Root bs v) y (F x0) hMnd hRnot hyrange
  simpa [x0] using hinner.symm

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

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

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum = (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l' with
  | nil =>
      change (l.map (fun _ => (0 : Int))).sum = 0
      exact sum_zero l
  | cons b l' ih =>
      rw [show (fun a => (List.map (F a) (b :: l')).sum) =
          (fun a => F a b + (l'.map (F a)).sum) by funext a; rfl]
      rw [sum_add, ih]
      simp


private theorem mobius_point (x : Bind) :
    ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
    (if (pattern.freeVars.map x).Nodup then (1 : Int) else 0) := by
  have e10 : (x 1 = x 0) = (x 0 = x 1) := propext eq_comm
  have e20 : (x 2 = x 0) = (x 0 = x 2) := propext eq_comm
  have e30 : (x 3 = x 0) = (x 0 = x 3) := propext eq_comm
  have e40 : (x 4 = x 0) = (x 0 = x 4) := propext eq_comm
  have e21 : (x 2 = x 1) = (x 1 = x 2) := propext eq_comm
  have e31 : (x 3 = x 1) = (x 1 = x 3) := propext eq_comm
  have e41 : (x 4 = x 1) = (x 1 = x 4) := propext eq_comm
  have e32 : (x 3 = x 2) = (x 2 = x 3) := propext eq_comm
  have e42 : (x 4 = x 2) = (x 2 = x 4) := propext eq_comm
  have e43 : (x 4 = x 3) = (x 3 = x 4) := propext eq_comm
  by_cases h01 : x 0 = x 1 <;>
  by_cases h02 : x 0 = x 2 <;>
  by_cases h03 : x 0 = x 3 <;>
  by_cases h04 : x 0 = x 4 <;>
  by_cases h12 : x 1 = x 2 <;>
  by_cases h13 : x 1 = x 3 <;>
  by_cases h14 : x 1 = x 4 <;>
  by_cases h23 : x 2 = x 3 <;>
  by_cases h24 : x 2 = x 4 <;>
  by_cases h34 : x 3 = x 4 <;>
  simp [pattern, partitions, mobius, factorial, constOn, headOf,
    e10, e20, e30, e40, e21, e31, e41, e32, e42, e43,
    h01, h02, h03, h04, h12, h13, h14, h23, h24, h34]

theorem sound_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.CollisionCorrection.Goal := by
  intro n db b
  let V := pattern.freeVars
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → F x = F y := by
    intro x y hxy
    dsimp only [F]
    simp [pattern, value, hxy 0 (by native_decide), hxy 1 (by native_decide),
      hxy 2 (by native_decide), hxy 3 (by native_decide), hxy 4 (by native_decide)]
  have hcoll : ∀ bs ∈ partitions V,
      collapsed pattern n db b bs =
        integrate n V b (fun x => if constOn bs x then F x else 0) := by
    intro bs hbs
    unfold collapsed
    change integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) = _
    exact collapse_sum n b F bs hbs hF
  change integrate n V b (fun x => if (V.map x).Nodup then F x else 0) =
    ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum
  calc
    integrate n V b (fun x => if (V.map x).Nodup then F x else 0)
        = ((bindings n V b).map (fun x => if (V.map x).Nodup then F x else 0)).sum :=
            bindings_sum n V b _
    _ = ((bindings n V b).map (fun x => F x *
          ((partitions V).map (fun bs => mobius bs *
            (if constOn bs x then (1 : Int) else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [mobius_point x]
          by_cases hN : (V.map x).Nodup <;> simp [hN, V]
    _ = ((bindings n V b).map (fun x => ((partitions V).map (fun bs =>
          mobius bs * (if constOn bs x then F x else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [show (fun bs => mobius bs * (if constOn bs x then F x else 0)) =
              (fun bs => (mobius bs * (if constOn bs x then (1 : Int) else 0)) * F x) by
            funext bs
            by_cases h : constOn bs x <;> simp [h]]
          rw [sum_mul_right]
          ac_rfl
    _ = ((partitions V).map (fun bs => mobius bs *
          ((bindings n V b).map (fun x => if constOn bs x then F x else 0)).sum)).sum := by
          rw [show (fun x => ((partitions V).map (fun bs =>
              mobius bs * (if constOn bs x then F x else 0))).sum) =
              (fun x => ((partitions V).map (fun bs =>
                (fun x => mobius bs * (if constOn bs x then F x else 0)) x)).sum) by rfl]
          rw [sum_swap]
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [sum_mul_left]
    _ = ((partitions V).map (fun bs => mobius bs *
          integrate n V b (fun x => if constOn bs x then F x else 0))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [bindings_sum]
    _ = ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [hcoll bs hbs]


end CollisionWork

/-! ## Assigned goals -/

theorem CoLikes_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikes.Remaining.sound_FractionalCap

theorem CoLikes_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CoLikes.Remaining.sound_CollisionCorrection

theorem ForumBowtie_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.FractionalCap.Goal := by
  exact ForumWork.sound_FractionalCap

theorem ForumBowtie_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie.CollisionCorrection.Goal := by
  exact CollisionWork.sound_CollisionCorrection

theorem CountryReturn_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task72.sound_FractionalCap

theorem CountryReturn_DeltaForecast :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.DeltaForecast.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturn.Remaining.sound_DeltaForecast

theorem CountryReturn_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task72.sound_CollisionCorrection

theorem OrderSupplier_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier.FractionalCap.Goal := by
  exact TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplier.Remaining.sound_FractionalCap

theorem OrderSupplier_CollisionCorrection :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier.CollisionCorrection.Goal := by
  exact TinylambOpt.ResearchProofs.Round2.Task73.sound_CollisionCorrection

end TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J3
