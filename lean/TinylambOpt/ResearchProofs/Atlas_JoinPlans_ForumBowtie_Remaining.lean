import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.ForumBowtie.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-! Basic list-sum helpers -/

private theorem sum_map_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
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

private theorem sum_const {α} (l : List α) (c : Int) : (l.map (fun _ => c)).sum = (l.length : Int) * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.natCast_succ, Int.add_mul, Int.add_comm]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (fun b => F a b)).sum)).sum =
    (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      rw [sum_const]
      simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp [sum_add, Int.add_assoc, Int.add_comm]

/-! `put` lemmas -/

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_put (b : Bind) (v x y : Nat) : put (put b v x) v y = put b v y := by
  funext i
  by_cases h : i == v <;> simp [put, h]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i == v
  · by_cases hw : i == w
    · have hi_v : i = v := Nat.eq_of_beq_eq_true hv
      have hi_w : i = w := Nat.eq_of_beq_eq_true hw
      have hvw : v = w := by omega
      simp [hvw] at h
    · simp [put, hv, hw]
  · by_cases hw : i == w <;> simp [put, hv, hw]

private theorem put_keep_other (b : Bind) (v w x : Nat) (hvw : v ≠ w) :
    put (put b v x) w (b w) = put b v x := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    have hvw' : (v == w) = false := by cases h' : (v == w) <;> simp_all
    simp [put, hvw']
  · by_cases hj : i == w
    · have hiw : i = w := Nat.eq_of_beq_eq_true hj
      subst i
      have hwv' : (w == v) = false := by cases h' : (w == v) <;> simp_all
      simp [put, hwv']
    · simp [put, hi, hj]

/-! `integrate` lemmas -/

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      apply sum_map_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by simpa using h
    change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    rw [sum_swap]
    apply sum_map_congr
    intro x hx
    apply sum_map_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem integrate_rotate (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n (vs ++ [v]) b f := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      rw [integrate_swap]
      change ((List.range n).map (fun x => integrate n (v :: ws) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum
      exact integrate_congr n [w] b _ _ ih

private theorem integrate_snoc (n : Nat) (vs : List Nat) (v : Nat) (b : Bind) (f : Bind → Int) :
    integrate n (vs ++ [v]) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      change ((List.range n).map (fun x => integrate n (ws ++ [v]) (put b w x) f)).sum =
        ((List.range n).map (fun x => integrate n ws (put b w x) (fun b' => integrate n [v] b' f))).sum
      apply sum_map_congr
      intro x hx
      exact ih (put b w x)

private theorem integrate_cons_singleton (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: vs) b f = integrate n vs b (fun b' => integrate n [v] b' f) := by
  rw [integrate_rotate, integrate_snoc]

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun y => f y + g y))).sum = _
      simp only [integrate]
      rw [← sum_add]
      apply sum_map_congr
      intro x hx
      rw [ih]

private theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  have h : (fun x => f x - g x) = (fun x => f x + - g x) := by funext x; omega
  rw [h, integrate_add]
  -- need integrate_neg
  have hneg : integrate n vs b (fun x => - g x) = - integrate n vs b g := by
    induction vs generalizing b with
    | nil => simp [integrate]
    | cons v vs ih =>
        simp only [integrate] at ih ⊢
        calc
          ((List.range n).map (fun x => integrate n vs (put b v x) (fun y => - g y))).sum =
              ((List.range n).map (fun x => - integrate n vs (put b v x) g)).sum := by
                apply sum_map_congr
                intro x hx
                exact ih (put b v x)
          _ = ((List.range n).map (fun x => (-1 : Int) * integrate n vs (put b v x) g)).sum := by
                apply sum_map_congr
                intro x hx
                omega
          _ = (-1 : Int) * ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum := by
                rw [sum_mul_left]
          _ = - ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum := by omega
  rw [hneg]
  omega

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      simp only [integrate] at ih ⊢
      rw [← sum_mul_left]
      apply sum_map_congr
      intro x hx
      exact ih (put b v x)

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

private theorem list_sum_single {α} [DecidableEq α] (l : List α) (hnd : l.Nodup) (t : α)
    (hmem : t ∈ l) (G : α → Int) :
    (l.map (fun x => if x == t then G x else 0)).sum = G t := by
  induction l with
  | nil => simp at hmem
  | cons a l ih =>
      have hnd' : l.Nodup := (List.nodup_cons.mp hnd).2
      have hane : a ∉ l := (List.nodup_cons.mp hnd).1
      simp only [List.map_cons, List.sum_cons]
      by_cases hat : a == t
      · have ha : a = t := by simpa using hat
        subst a
        have htnot : t ∉ l := hane
        have hz : (l.map (fun x => if x == t then G x else 0)).sum = 0 := by
          rw [show (l.map (fun x => if x == t then G x else 0)) = l.map (fun _ : α => (0 : Int)) by
            apply List.map_congr_left
            intro x hx
            have hxne : (x == t) = false := by
              by_cases h : x == t
              · have hxt : x = t := by simpa using h
                subst x
                exact False.elim (htnot hx)
              · exact Bool.eq_false_of_ne_true h
            simp [hxne]]
          simpa using (sum_const l (0 : Int))
        rw [show (if t == t then G t else 0) = G t by simp]
        rw [hz]
        omega
      · have htmem : t ∈ l := by
          simp only [List.mem_cons] at hmem
          rcases hmem with rfl | hmem
          · simpa using hat
          · exact hmem
        rw [ih hnd' htmem]
        simp [hat]

private theorem range_sum_single (n t : Nat) (ht : t < n) (G : Nat → Int) :
    ((List.range n).map (fun x => if x == t then G x else 0)).sum = G t := by
  exact list_sum_single (List.range n) (List.nodup_range (n := n)) t (by simp [ht]) G

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      simp only [integrate]
      calc
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun _ => (0 : Int)))).sum =
            ((List.range n).map (fun _ : Nat => (0 : Int))).sum := by
              apply sum_map_congr
              intro x hx
              exact ih (put b v x)
        _ = 0 := by simpa using (sum_const (List.range n) (0 : Int))

private theorem integrate_congr_on (n : Nat) (vs : List Nat) (v : Nat) (hvn : v ∉ vs)
    (b : Bind) (x : Nat) (G H : Bind → Int) (h : ∀ z, z v = x → G z = H z) :
    integrate n vs (put b v x) G = integrate n vs (put b v x) H := by
  induction vs generalizing b G H with
  | nil =>
      apply h
      simp [put_self]
  | cons w ws ih =>
      have hvnws : v ∉ ws := by intro he; exact hvn (by simp [he])
      have hvwbool : (v != w) = true := by cases h' : (v == w) <;> simp_all
      simp only [integrate]
      apply sum_map_congr
      intro y hy
      rw [put_comm_ne b v w x y hvwbool]
      exact ih hvnws (put b w y) G H h

private theorem integrate_eq_const (n : Nat) (vs : List Nat) (hnd : vs.Nodup) (b : Bind) (t : Nat)
    (ht : t < n) (F : Bind → Int) :
    integrate n vs b (fun x => if vs.all (fun v => x v == t) then F x else 0) =
      F (fun v => if vs.contains v then t else b v) := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      have hnd' : vs.Nodup := (List.nodup_cons.mp hnd).2
      have hvn : v ∉ vs := (List.nodup_cons.mp hnd).1
      simp only [integrate]
      rw [show (fun x => integrate n vs (put b v x)
          (fun z => if (v :: vs).all (fun w => z w == t) then F z else 0)) =
          (fun x => if x == t then integrate n vs (put b v x)
          (fun z => if vs.all (fun w => z w == t) then F z else 0) else 0) by
            funext x
            by_cases hxt : x == t
            · simp [hxt]
              apply integrate_congr_on n vs v hvn b x
              intro z hzv
              have hxeq : x = t := Nat.eq_of_beq_eq_true hxt
              simp [List.all_cons, hzv, hxeq]
            · simp [hxt]
              calc
                integrate n vs (put b v x) (fun z => if z v = t ∧ ∀ (w : Nat), w ∈ vs → z w = t then F z else 0)
                    = integrate n vs (put b v x) (fun _ => (0 : Int)) := by
                        apply integrate_congr_on n vs v hvn b x
                        intro z hzv
                        simp only [hzv]
                        have hxne : x ≠ t := by intro hxet; exact hxt (by simp [hxet])
                        simp [hxne]
                _ = 0 := integrate_zero n vs (put b v x)]
      rw [show ((List.range n).map (fun x => if x == t then integrate n vs (put b v x)
          (fun z => if vs.all (fun w => z w == t) then F z else 0) else 0)).sum =
          integrate n vs (put b v t)
          (fun z => if vs.all (fun w => z w == t) then F z else 0) by
            exact range_sum_single n t ht
              (fun x => integrate n vs (put b v x)
                (fun z => if vs.all (fun w => z w == t) then F z else 0))]
      rw [ih hnd' (put b v t)]
      congr 1
      funext w
      by_cases hw : w == v
      · have hwv : w = v := Nat.eq_of_beq_eq_true hw
        subst w
        simp [put_self]
      · have hwne : w ≠ v := by intro he; subst w; simp at hw
        by_cases hwvs : w ∈ vs
        · simp [put, hw, hwne, hwvs]
        · simp [put, hw, hwne, hwvs]


end TinylambOpt.ResearchProofs.Atlas.JoinPlans.ForumBowtie.Remaining
