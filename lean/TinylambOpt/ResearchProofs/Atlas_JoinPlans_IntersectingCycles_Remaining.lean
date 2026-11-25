import TinylambOpt.ResearchGoals.Atlas.JoinPlans

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.IntersectingCycles.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private abbrev fv : List Nat := IntersectingCycles.pattern.freeVars

private theorem fv_eq : fv = [4, 5, 1, 2, 3, 0] := rfl

private def repOf (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun b => b.contains v)).bind List.head?).getD v

private def heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?


private def collapseF (n : Nat) (b : Bind) (bs : List (List Nat)) (F : Bind → Int) : Int :=
  integrate n (heads bs) b (fun x => F (fun v => x (repOf bs v)))

private theorem collapsed_eq_collapseF (n : Nat) (db : DB) (b : Bind) (bs : List (List Nat)) :
    collapsed IntersectingCycles.pattern n db b bs =
      collapseF n b bs (fun x => (IntersectingCycles.pattern.atoms.map (fun a => value db a x)).prod) := by
  rfl

-- finite facts about the concrete partition list, discharged once by evaluation
private theorem parts_heads_perm :
    (partitions fv).all (fun bs => decide (fv.Perm (heads bs ++ fv.filter (fun v => !(heads bs).contains v)))) = true := by
  native_decide

private theorem parts_heads_mem :
    (partitions fv).all (fun bs => (heads bs).all (fun v => decide (v ∈ fv))) = true := by
  native_decide

private theorem parts_heads_nodup :
    (partitions fv).all (fun bs => decide ((heads bs).Nodup)) = true := by
  native_decide

private theorem parts_nonempty :
    (partitions fv).all (fun bs => bs.all (fun blk => decide (blk ≠ []))) = true := by
  native_decide

private theorem parts_nodup_blocks :
    (partitions fv).all (fun bs => bs.all (fun blk => decide (blk.Nodup))) = true := by
  native_decide

private theorem parts_disjoint :
    (partitions fv).all (fun bs => bs.all (fun b1 => bs.all (fun b2 =>
      decide (b1 ≠ b2 → ∀ v, v ∈ b1 → v ∈ b2 → False)))) = true := by
  native_decide

private theorem parts_cover :
    (partitions fv).all (fun bs => fv.all (fun v => decide (v ∈ bs.flatten))) = true := by
  native_decide

private theorem parts_rep_of_mem :
    (partitions fv).all (fun bs => bs.all (fun blk => blk.all (fun v =>
      decide (repOf bs v == blk.head?.getD 0)))) = true := by
  native_decide

private def extra (bs : List (List Nat)) : List Nat := fv.filter (fun v => !(heads bs).contains v)

private def constOnB (bs : List (List Nat)) (x : Bind) : Bool :=
  decide ((extra bs).all (fun u => x u == x (repOf bs u)))

private theorem parts_extra_rep :
    (partitions fv).all (fun bs => (extra bs).all (fun u => decide (repOf bs u ∈ heads bs))) = true := by
  native_decide

private theorem parts_heads_not_extra :
    (partitions fv).all (fun bs => (heads bs).all (fun u => decide (u ∉ extra bs))) = true := by
  native_decide

private theorem parts_head_rep :
    (partitions fv).all (fun bs => (heads bs).all (fun v => decide (repOf bs v == v))) = true := by
  native_decide

private theorem parts_blocks_nonhead :
    (partitions fv).all (fun bs => bs.all (fun blk =>
      (blk.filter (fun u => u != blk.head?.getD 0)).all (fun u => decide (u ∈ extra bs)))) = true := by
  native_decide

private theorem get_from_bool {p : Prop} [Decidable p] (h : decide p = true) : p := of_decide_eq_true h

private theorem partition_property {p : List (List Nat) → Prop} [DecidablePred p]
    (hall : (partitions fv).all (fun bs => decide (p bs)) = true)
    {bs : List (List Nat)} (hbs : bs ∈ partitions fv) : p bs := by
  exact get_from_bool ((List.all_eq_true.mp hall) bs hbs)

private theorem allall {α β : Type} {l : List α} {f : α → List β} {p : β → Bool}
    (h : l.all (fun a => (f a).all p) = true) {a : α} (ha : a ∈ l) :
    ∀ b ∈ f a, p b = true := by
  exact List.all_eq_true.mp ((List.all_eq_true.mp h) a ha)

-- basic integrate algebra
private theorem integrate_congr_fun {n : Nat} {vs : List Nat} {b : Bind} {f g : Bind → Int}
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate, h]
  | cons v vs ih =>
      simp only [integrate]
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      exact ih

private theorem integrate_append (n : Nat) (vs ws : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ ws) b f = integrate n vs b (fun x => integrate n ws x f) := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp only [List.cons_append, integrate]
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      rw [ih]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem sum_const_zero (m : Nat) : ((List.range m).map (fun _ => (0 : Int))).sum = 0 := by
  induction m with
  | zero => simp
  | succ m ih => rw [List.range_succ, List.map_append, List.sum_append, ih]; simp

private theorem sum_map_sum_list {α : Type} (l : List α) (m : Nat) (F : α → Nat → Int) :
    (l.map (fun a => ((List.range m).map (fun c => F a c)).sum)).sum =
      ((List.range m).map (fun c => (l.map (fun a => F a c)).sum)).sum := by
  induction l with
  | nil =>
      simp
      exact (sum_const_zero m).symm
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [show ((List.range m).map (fun c => F a c + (l.map (fun a => F a c)).sum)).sum =
          ((List.range m).map (fun c => F a c)).sum +
          ((List.range m).map (fun c => (l.map (fun a => F a c)).sum)).sum by
        rw [sum_map_add]]

private theorem list_sum_comm (n m : Nat) (F : Nat → Nat → Int) :
    ((List.range n).map (fun a => ((List.range m).map (fun c => F a c)).sum)).sum =
      ((List.range m).map (fun c => ((List.range n).map (fun a => F a c)).sum)).sum := by
  exact sum_map_sum_list (List.range n) m F

private theorem put_put_comm {b : Bind} {v w a c : Nat} (h : v ≠ w) :
    put (put b v a) w c = put (put b w c) v a := by
  funext i
  by_cases hiv : i == v <;> by_cases hiw : i == w <;> simp [put, hiv, hiw]
  have hiv' : i = v := beq_iff_eq.mp hiv
  have hiw' : i = w := beq_iff_eq.mp hiw
  exact False.elim (h (hiv'.symm.trans hiw'))

private theorem integrate_swap (n : Nat) (v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases hb : v == w
  · have hvw : v = w := beq_iff_eq.mp hb
    subst w
    rfl
  · have hvw : v ≠ w := by intro he; exact hb (by simp [he])
    simp only [integrate, List.cons_append, integrate_append]
    change ((List.range n).map (fun a => ((List.range n).map (fun c =>
      integrate n vs (put (put b v a) w c) f)).sum)).sum =
      ((List.range n).map (fun c => ((List.range n).map (fun a =>
      integrate n vs (put (put b w c) v a) f)).sum)).sum
    rw [list_sum_comm]
    apply congrArg List.sum
    apply List.map_congr_left
    intro c hc
    apply congrArg List.sum
    apply List.map_congr_left
    intro a ha
    rw [put_put_comm hvw]

private theorem integrate_perm {n : Nat} {vs ws : List Nat} (h : vs.Perm ws) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = integrate n ws b f := by
  induction h generalizing b with
  | nil => rfl
  | cons v h ih =>
      simp only [integrate]
      apply congrArg List.sum
      apply List.map_congr_left
      intro a ha
      rw [ih]
  | swap v w vs =>
      exact integrate_swap n w v vs b f
  | trans h1 h2 ih1 ih2 =>
      rw [ih1, ih2]

private theorem rsum_zero_of_gt (n k : Nat) (h : ∀ t, t < n → t ≠ k) (f : Nat → Int) :
    ((List.range n).map (fun t => if t == k then f t else 0)).sum = 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      rw [ih (fun t ht => h t (Nat.lt_succ_of_lt ht))]
      have hnk : ¬ n == k := by intro he; exact h n (Nat.lt_succ_self n) (beq_iff_eq.mp he)
      have hneq : n ≠ k := by intro he; exact h n (Nat.lt_succ_self n) he
      simp [hnk, hneq]

private theorem rsum_eq_single (n k : Nat) (hk : k < n) (f : Nat → Int) :
    ((List.range n).map (fun t => if t == k then f t else 0)).sum = f k := by
  induction n generalizing k with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      by_cases hkn : k = n
      · subst k
        rw [rsum_zero_of_gt n n (by intro t ht htn; omega) f]
        simp
      · have hklt : k < n := by omega
        rw [ih k hklt]
        have hne : n ≠ k := by omega
        simp [hne]


private theorem constOnB_iff {bs : List (List Nat)} {x : Bind} :
    constOnB bs x = true ↔ ∀ u ∈ extra bs, x u == x (repOf bs u) := by
  unfold constOnB
  rw [decide_eq_true_eq]
  exact List.all_eq_true

private theorem rep_not_mem_extra {bs : List (List Nat)} (hbs : bs ∈ partitions fv) :
    ∀ u ∈ extra bs, repOf bs u ∉ extra bs := by
  intro u hu
  have hrepall : (extra bs).all (fun u => decide (repOf bs u ∈ heads bs)) = true :=
    (List.all_eq_true.mp parts_extra_rep) bs hbs
  have hrepval : repOf bs u ∈ heads bs :=
    get_from_bool ((List.all_eq_true.mp hrepall) u hu)
  have hnotall : (heads bs).all (fun v => decide (v ∉ extra bs)) = true :=
    (List.all_eq_true.mp parts_heads_not_extra) bs hbs
  exact get_from_bool ((List.all_eq_true.mp hnotall) (repOf bs u) hrepval)

private theorem head_rep_of_head {bs : List (List Nat)} (hbs : bs ∈ partitions fv) :
    ∀ v ∈ heads bs, repOf bs v = v := by
  intro v hv
  have hallall : (heads bs).all (fun v => decide (repOf bs v == v)) = true :=
    (List.all_eq_true.mp parts_head_rep) bs hbs
  exact beq_iff_eq.mp (get_from_bool ((List.all_eq_true.mp hallall) v hv))

private theorem head_bound_of_assign {n : Nat} {bs : List (List Nat)} {y : Bind}
    (hbs : bs ∈ partitions fv) (hy : ∀ v ∈ heads bs, y v < n)
    {u : Nat} (hu : u ∈ heads bs) : y (repOf bs u) < n := by
  rw [head_rep_of_head hbs u hu]
  exact hy u hu

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.IntersectingCycles.Remaining
