import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas_JoinPlans_G2

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)

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

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_mul_left {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_const {α} (l : List α) (c : Int) :
    (l.map (fun _ => c)).sum = (l.length : Int) * c := by
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

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by
  simp [put]

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

private def Respects (f : Factor) : Prop :=
  ∀ b b', (∀ v, f.scope.contains v = true → b v = b' v) → f.get b = f.get b'

private theorem respects_value (db : DB) (a : Atom) :
    Respects ⟨[a.src, a.dst], value db a⟩ := by
  intro b b' h
  simp only [value]
  rw [h a.src (by simp), h a.dst (by simp)]

private theorem respects_put (f : Factor) (hf : Respects f) (v x : Nat) (b : Bind)
    (hnot : f.scope.contains v = false) : f.get (put b v x) = f.get b := by
  apply hf
  intro w hw
  by_cases h : w == v
  · have hwv : w = v := Nat.eq_of_beq_eq_true h
    subst w
    rw [hnot] at hw
    simp at hw
  · simp [put, h]

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
  by_cases h' : w == v
  · have hwv : w = v := Nat.eq_of_beq_eq_true h'
    subst w
    rw [hnot f hf] at hw
    simp at hw
  · simp [put, h']

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
    ((l.filter p).map g).prod * ((l.filter (fun x => !p x)).map g).prod = (l.map g).prod := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true <;>
        simp [h, ih, Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]

private theorem product_partition_get (p : Factor → Bool) (l : List Factor) (b : Bind) :
    product (List.partition p l).fst b * product (List.partition p l).snd b = product l b := by
  dsimp only [product]
  rw [(partition_filters p l).1, (partition_filters p l).2]
  exact product_filter_mul p l (fun f => f.get b)

private theorem respects_msg (n v : Nat) (inc : List Factor) (hres : ∀ f ∈ inc, Respects f) :
    Respects ⟨(inc.flatMap Factor.scope).eraseDups.filter (fun w => w != v),
      fun b => integrate n [v] b (product inc)⟩ := by
  intro b b' hb
  change ((List.range n).map (fun x => product inc (put b v x))).sum =
    ((List.range n).map (fun x => product inc (put b' v x))).sum
  apply sum_map_congr
  intro x hx
  apply product_respects_eq hres
  intro f hf w hw
  by_cases hwv : w == v
  · have hwv' : w = v := Nat.eq_of_beq_eq_true hwv
    subst w
    simp [put_self]
  · simp [put, hwv]
    apply hb
    have hwmem : w ∈ f.scope := by simpa using hw
    have hwflat : w ∈ inc.flatMap Factor.scope := List.mem_flatMap.mpr ⟨f, hf, hwmem⟩
    have hwErase : w ∈ (inc.flatMap Factor.scope).eraseDups := List.mem_eraseDups.mpr hwflat
    have hwne : (w != v) = true := by simpa using hwv
    simpa using (List.mem_filter (p := fun w : Nat => w != v)).mpr ⟨hwErase, hwne⟩

private theorem mem_of_partition_fst {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).fst) : f ∈ l := by
  rw [(partition_filters (fun g : Factor => g.scope.contains v) l).1] at hf
  exact (List.mem_filter.mp hf).1

private theorem mem_of_partition_snd {l : List Factor} {v : Nat} {f : Factor}
    (hf : f ∈ (List.partition (fun g : Factor => g.scope.contains v) l).snd) : f ∈ l := by
  rw [(partition_filters (fun g : Factor => g.scope.contains v) l).2] at hf
  exact (List.mem_filter.mp hf).1

private theorem product_eliminate (n v : Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (eliminate n v fs) b = integrate n [v] b (product fs) := by
  dsimp only [eliminate]
  let inc := (fs.partition (fun f : Factor => f.scope.contains v)).fst
  let rest := (fs.partition (fun f : Factor => f.scope.contains v)).snd
  have hinc_res : ∀ f ∈ inc, Respects f := by
    intro f hf
    exact hres f (mem_of_partition_fst hf)
  have hrest_res : ∀ f ∈ rest, Respects f := by
    intro f hf
    exact hres f (mem_of_partition_snd hf)
  have hrest_not : ∀ f ∈ rest, f.scope.contains v = false := by
    intro f hf
    exact mem_partition_notp (fun g : Factor => g.scope.contains v) hf
  change (integrate n [v] b (fun x => product inc x)) * product rest b =
    integrate n [v] b (product fs)
  change ((List.range n).map (fun x => product inc (put b v x))).sum * product rest b =
    ((List.range n).map (fun x => product fs (put b v x))).sum
  rw [← sum_mul_right]
  apply sum_map_congr
  intro x hx
  rw [← product_partition_get (fun f : Factor => f.scope.contains v) fs (put b v x)]
  rw [product_put_eq hrest_res hrest_not b x]

private theorem respects_eliminate (n v : Nat) (fs : List Factor) (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ eliminate n v fs, Respects f := by
  intro f hf
  dsimp only [eliminate] at hf
  simp only [List.mem_cons] at hf
  rcases hf with rfl | hf
  · exact respects_msg n v (fs.partition (fun g : Factor => g.scope.contains v)).fst (by
      intro g hg
      exact hres g (mem_of_partition_fst hg))
  · exact hres f (mem_of_partition_snd hf)

private theorem respects_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor)
    (hres : ∀ f ∈ fs, Respects f) :
    ∀ f ∈ vs.foldl (fun fs v => eliminate n v fs) fs, Respects f := by
  induction vs generalizing fs with
  | nil => exact hres
  | cons v vs ih =>
      exact ih (eliminate n v fs) (respects_eliminate n v fs hres)

private theorem product_fold_eliminate (n : Nat) (vs : List Nat) (fs : List Factor) (b : Bind)
    (hres : ∀ f ∈ fs, Respects f) :
    product (vs.foldl (fun fs v => eliminate n v fs) fs) b = integrate n vs b (product fs) := by
  induction vs generalizing fs with
  | nil => rfl
  | cons v vs ih =>
      change product (vs.foldl (fun fs v => eliminate n v fs) (eliminate n v fs)) b =
        integrate n (v :: vs) b (product fs)
      rw [ih (eliminate n v fs) (respects_eliminate n v fs hres)]
      rw [integrate_cons_singleton n v vs b (product fs)]
      apply integrate_congr
      intro b'
      exact product_eliminate n v fs b' hres

/-! Additional finite-sum and integration helpers. -/

private theorem sum_neg {α} (l : List α) (f : α → Int) :
    (l.map (fun x => - f x)).sum = -(l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; omega

private theorem sum_sub {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; omega

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun y => f y + g y))).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum +
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      rw [← sum_add]
      apply sum_map_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun y => f y - g y))).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum -
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      rw [← sum_sub]
      apply sum_map_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun y => c * f y))).sum =
        c * ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum
      rw [← sum_mul_left]
      apply sum_map_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_mul_right (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) (c : Int) :
    integrate n vs b (fun x => f x * c) = integrate n vs b f * c := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun y => f y * c))).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum * c
      rw [← sum_mul_right]
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

private theorem integrate_front (n v : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) (hv : v ∈ vs) :
    integrate n vs b f = integrate n (v :: vs.erase v) b f :=
  integrate_perm n (List.perm_cons_erase hv) b f

private theorem put_pinned (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i == v
  · have hi : i = v := Nat.eq_of_beq_eq_true h
    subst i
    simp [put]
  · simp [put, h]

private theorem put_dst_pinned (b : Bind) (src dst u : Nat) (hne : src ≠ dst) :
    put (put b src u) dst (b dst) = put b src u := by
  funext i
  by_cases hid : i == dst
  · have hi : i = dst := Nat.eq_of_beq_eq_true hid
    subst i
    have hds : (dst == src) = false := by
      by_cases h : dst == src
      · have : dst = src := Nat.eq_of_beq_eq_true h
        omega
      · simpa using h
    simp [put, hds]
  · simp [put, hid]

private theorem filter_ne_eq_erase (vs : List Nat) (a : Nat) (hnd : vs.Nodup) :
    vs.filter (fun v => v != a) = vs.erase a := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      have hnd_tail : xs.Nodup := hp.2
      by_cases hxa : x == a
      · have hx : x = a := Nat.eq_of_beq_eq_true hxa
        subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        simp [List.erase_cons_head, hna, ih hnd_tail]
      · have hxa' : (x != a) = true := by simpa using hxa
        simp [List.erase_cons_tail hxa, hxa', ih hnd_tail]

private theorem filter_and_ne_of_not_mem (l : List Nat) (a b : Nat) (ha : a ∉ l) :
    l.filter (fun v => v != a && v != b) = l.filter (fun v => v != b) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxna : x ≠ a := by intro h; subst h; exact ha (by simp)
      have hxne : (x != a) = true := by simpa using hxna
      have haxs : a ∉ xs := by intro h; exact ha (by simp [h])
      by_cases hxb : x == b
      · have hx : x = b := Nat.eq_of_beq_eq_true hxb
        subst x
        simp [ih haxs]
      · have hxb' : (x != b) = true := by simpa using hxb
        simp [hxne, hxb', ih haxs]

private theorem filter_and_ne_of_not_mem2 (l : List Nat) (a b : Nat) (hb : b ∉ l) :
    l.filter (fun v => v != a && v != b) = l.filter (fun v => v != a) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hxb : x ≠ b := by intro h; subst h; exact hb (by simp)
      have hxb' : (x != b) = true := by simpa using hxb
      have hbxs : b ∉ xs := by intro h; exact hb (by simp [h])
      by_cases hxa : x == a
      · have hx : x = a := Nat.eq_of_beq_eq_true hxa
        subst x
        simp [ih hbxs]
      · have hxa' : (x != a) = true := by simpa using hxa
        simp [hxa', hxb', ih hbxs]

private theorem erase_erase_filter (vs : List Nat) (a b : Nat) (hnd : vs.Nodup) :
    (vs.erase a).erase b = vs.filter (fun v => v != a && v != b) := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      have hnd' : xs.Nodup := hp.2
      by_cases hxa : x == a
      · have hx : x = a := Nat.eq_of_beq_eq_true hxa
        subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        rw [List.erase_cons_head]
        change xs.erase b = (a :: xs).filter (fun v => v != a && v != b)
        simp only [List.filter_cons]
        rw [if_neg (by simp)]
        rw [filter_and_ne_of_not_mem xs a b hna]
        exact (filter_ne_eq_erase xs b hnd').symm
      · by_cases hxb : x == b
        · have hx : x = b := Nat.eq_of_beq_eq_true hxb
          subst x
          have hnb : b ∉ xs := by intro h; exact hp.1 b h rfl
          have hbne : b ≠ a := by simpa using hxa
          have hnb' : b ∉ xs.erase a := by
            intro hb
            exact hnb ((List.mem_erase_of_ne (a := b) (b := a) hbne).mp hb)
          have hih := ih hnd'
          rw [List.erase_of_not_mem hnb'] at hih
          rw [List.erase_cons_tail hxa, List.erase_cons_head]
          change xs.erase a = (b :: xs).filter (fun v => v != a && v != b)
          simp only [List.filter_cons]
          rw [if_neg (by simp)]
          simpa using hih
        · have hxa' : (x != a) = true := by simpa using hxa
          have hxb' : (x != b) = true := by simpa using hxb
          rw [List.erase_cons_tail hxa, List.erase_cons_tail hxb]
          simp [hxa', hxb', ih hnd']

private theorem integrate_pivot (n : Nat) (vs : List Nat) (src dst : Nat) (b : Bind) (f : Bind → Int)
    (hnd : vs.Nodup) (hne : src ≠ dst) :
    integrate n vs b f =
      let cand := fun v => if vs.contains v then List.range n else [b v]
      let vars := vs.filter (fun v => v != src && v != dst)
      ((cand src).map (fun u => ((cand dst).map (fun w =>
        integrate n vars (put (put b src u) dst w) f)).sum)).sum := by
  dsimp only
  by_cases hsrc : vs.contains src = true
  case pos =>
    have hsrcm : src ∈ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    case pos =>
      have hdstm : dst ∈ vs := by simpa using hdst
      have hvars : (vs.erase src).erase dst = vs.filter (fun v => v != src && v != dst) :=
        erase_erase_filter vs src dst hnd
      have hdstm_erase : dst ∈ vs.erase src :=
        (List.mem_erase_of_ne (a := dst) (b := src) hne.symm).mpr hdstm
      have h1 : integrate n vs b f = integrate n (src :: vs.erase src) b f :=
        integrate_perm n (List.perm_cons_erase hsrcm) b f
      rw [h1, if_pos hsrc, if_pos hdst]
      change ((List.range n).map (fun u => integrate n (vs.erase src) (put b src u) f)).sum =
        ((List.range n).map (fun u =>
          ((List.range n).map (fun w =>
            integrate n (vs.filter (fun v => v != src && v != dst)) (put (put b src u) dst w) f)).sum)).sum
      apply sum_map_congr
      intro u hu
      have h2 : integrate n (vs.erase src) (put b src u) f =
          integrate n (dst :: (vs.erase src).erase dst) (put b src u) f :=
        integrate_perm n (List.perm_cons_erase hdstm_erase) (put b src u) f
      rw [h2]
      change ((List.range n).map (fun w => integrate n ((vs.erase src).erase dst) (put (put b src u) dst w) f)).sum =
        ((List.range n).map (fun w =>
          integrate n (vs.filter (fun v => v != src && v != dst)) (put (put b src u) dst w) f)).sum
      rw [hvars]
    case neg =>
      have hdstm : dst ∉ vs := by simpa using hdst
      have hvars : vs.filter (fun v => v != src && v != dst) = vs.erase src := by
        rw [filter_and_ne_of_not_mem2 vs src dst hdstm]
        exact filter_ne_eq_erase vs src hnd
      have h1 : integrate n vs b f = integrate n (src :: vs.erase src) b f :=
        integrate_perm n (List.perm_cons_erase hsrcm) b f
      rw [h1, if_pos hsrc, if_neg hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      change ((List.range n).map (fun u => integrate n (vs.erase src) (put b src u) f)).sum = _
      apply sum_map_congr
      intro u hu
      rw [hvars, put_dst_pinned b src dst u hne]
  case neg =>
    have hsrcm : src ∉ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    case pos =>
      have hdstm : dst ∈ vs := by simpa using hdst
      have hvars : vs.filter (fun v => v != src && v != dst) = vs.erase dst := by
        rw [filter_and_ne_of_not_mem vs src dst hsrcm]
        exact filter_ne_eq_erase vs dst hnd
      have h1 : integrate n vs b f = integrate n (dst :: vs.erase dst) b f :=
        integrate_perm n (List.perm_cons_erase hdstm) b f
      rw [h1, if_neg hsrc, if_pos hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      change ((List.range n).map (fun w => integrate n (vs.erase dst) (put b dst w) f)).sum = _
      apply sum_map_congr
      intro w hw
      rw [hvars, put_pinned b src]
    case neg =>
      have hdstm : dst ∉ vs := by simpa using hdst
      rw [if_neg hsrc, if_neg hdst]
      have hvars : vs.filter (fun v => v != src && v != dst) = vs := by
        apply List.filter_eq_self.mpr
        intro v hv
        have hvs : v ≠ src := by intro h; subst h; exact hsrcm hv
        have hvd : v ≠ dst := by intro h; subst h; exact hdstm hv
        simp [hvs, hvd]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      rw [hvars]
      rw [put_pinned b src, put_pinned b dst]

/-! Finite range-sum helpers. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

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

private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        simp [rsum]]
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

private theorem rsum_product (n m : Nat) (f : Nat → Int) (g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            dsimp only [rsum]
            exact sum_mul_left (List.range m) g (f i)
    _ = rsum n f * rsum m g := by
            dsimp only [rsum]
            rw [sum_mul_right]

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  exact sum_mul_left (List.range n) f c

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  exact sum_mul_right (List.range n) f c

private theorem value_put2 (db : DB) (a : Atom) (b : Bind) (u v : Nat) (hne : a.src ≠ a.dst) :
    value db a (put (put b a.src u) a.dst v) = db a.rel u v := by
  unfold value
  have hsrc : (put (put b a.src u) a.dst v) a.src = u := by
    by_cases h : a.src == a.dst
    · have h' : a.src = a.dst := Nat.eq_of_beq_eq_true h
      exact False.elim (hne h')
    · simp [put, h]
  have hdst : (put (put b a.src u) a.dst v) a.dst = v := by
    by_cases h : a.dst == a.dst <;> simp [put]
  rw [hsrc, hdst]

private theorem take_succ_of_lt {α} (l : List α) (j : Nat) (h : j < l.length) :
    l.take (j+1) = l.take j ++ [l.get ⟨j,h⟩] := by
  induction l generalizing j with
  | nil => simp at h
  | cons x xs ih =>
      cases j with
      | zero => simp
      | succ j =>
          simp only [List.take_succ_cons]
          have hj : j < xs.length := by
            simp only [List.length_cons, Nat.succ_eq_add_one] at h
            omega
          rw [ih j hj]
          rfl

private theorem drop_succ_of_lt {α} (l : List α) (j : Nat) (h : j < l.length) :
    l.drop j = l.get ⟨j,h⟩ :: l.drop (j+1) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x xs ih =>
      cases j with
      | zero => simp
      | succ j =>
          simp only [List.drop_succ_cons]
          have hj : j < xs.length := by
            simp only [List.length_cons, Nat.succ_eq_add_one] at h
            omega
          rw [ih j hj]
          rfl

private theorem F_step (n : Nat) (p : Pattern) (old new : DB) (b : Bind) (j : Nat)
    (hj : j < p.atoms.length) :
    let F : Nat → Int := fun j => integrate n p.freeVars b (fun x =>
      ((p.atoms.take j).map (fun a => value new a x)).prod *
      ((p.atoms.drop j).map (fun a => value old a x)).prod)
    let a := p.atoms.get ⟨j, hj⟩
    F (j+1) - F j = integrate n p.freeVars b (fun x =>
      (value new a x - value old a x) *
        (((p.atoms.take j).map (fun b => value new b x)).prod *
         ((p.atoms.drop (j+1)).map (fun b => value old b x)).prod)) := by
  dsimp only
  change integrate n p.freeVars b (fun x =>
      ((p.atoms.take (j+1)).map (fun a => value new a x)).prod *
      ((p.atoms.drop (j+1)).map (fun a => value old a x)).prod) -
    integrate n p.freeVars b (fun x =>
      ((p.atoms.take j).map (fun a => value new a x)).prod *
      ((p.atoms.drop j).map (fun a => value old a x)).prod) = _
  rw [take_succ_of_lt p.atoms j hj, drop_succ_of_lt p.atoms j hj]
  simp only [List.map_append, List.map_cons, List.prod_append, List.prod_cons]
  rw [← integrate_sub]
  apply integrate_congr
  intro x
  rw [Int.sub_mul]
  ac_rfl

private theorem sum_filter_delta (l : List Nat) (oldv newv : Nat → Int) (I : Nat → Int) :
    ((l.filter (fun v => oldv v != newv v)).map (fun v => (newv v - oldv v) * I v)).sum =
    ((l.map (fun v => (newv v - oldv v) * I v)).sum) := by
  induction l with
  | nil => simp
  | cons v l ih =>
      by_cases h : oldv v != newv v
      · simp [h, ih]
      · have heq : oldv v = newv v := by
          by_cases hb : oldv v == newv v
          · exact beq_iff_eq.mp hb
          · have hne : (oldv v != newv v) = true := by simpa using hb
            rw [hne] at h
            simp at h
        simp [h, heq, ih]

private theorem telescope_zip (l : List Atom) (F : Nat → Int) (k : Nat) :
    ((l.zipIdx k).map (fun (a,i) => F (i+1) - F i)).sum = F (k + l.length) - F k := by
  induction l generalizing k with
  | nil => simp
  | cons a l ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp only [List.length_cons]
      have hidx : k + (l.length + 1) = k + 1 + l.length := by omega
      rw [hidx]
      omega

private theorem rsum_ite_eq (n k : Nat) (f : Nat → Int) :
    rsum n (fun l => if l == k then f l else 0) = if k < n then f k else 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      by_cases hkn : k < n
      · have hne : (n == k) = false := by
          by_cases h : n == k
          · have : n = k := Nat.eq_of_beq_eq_true h
            omega
          · simpa using h
        simp [hkn, Nat.lt_succ_of_lt hkn, hne]
      · by_cases hkeq : k == n
        · have hk : k = n := Nat.eq_of_beq_eq_true hkeq
          subst k
          simp [hkn]
        · have hkne : k ≠ n := by
            intro h
            subst k
            simp at hkeq
          have hkgt : n < k := by omega
          have hnlt : ¬ k < n + 1 := by omega
          simp [hkn, hnlt]
          intro h
          omega

private theorem offdiag_square (n : Nat) (A : Nat → Int) :
    rsum n (fun k => rsum n (fun l => if k = l then 0 else A k * A l)) =
    (rsum n A)^2 - rsum n (fun j => (A j)^2) := by
  have hsq : (rsum n A)^2 = rsum n (fun k => rsum n (fun l => A k * A l)) := by
    have h : (rsum n A)^2 = (rsum n A) * (rsum n A) := by grind
    rw [h, rsum_product]
  have hdiag : rsum n (fun k => rsum n (fun l => if k = l then A k * A l else 0)) =
      rsum n (fun j => A j * A j) := by
    apply rsum_congr
    intro k hk
    rw [show rsum n (fun l => if k = l then A k * A l else 0) =
        rsum n (fun l => if l == k then A l * A l else 0) by
      apply rsum_congr
      intro l hl
      by_cases h : k = l
      · subst l
        simp
      · have hlk : (l == k) = false := by
          by_cases h' : l == k
          · have : l = k := Nat.eq_of_beq_eq_true h'
            exact False.elim (h this.symm)
          · simpa using h'
        simp [h, hlk]]
    simpa [hk] using rsum_ite_eq n k (fun l => A l * A l)
  have hsum : rsum n (fun k => rsum n (fun l => if k = l then 0 else A k * A l)) +
      rsum n (fun k => rsum n (fun l => if k = l then A k * A l else 0)) =
      rsum n (fun k => rsum n (fun l => A k * A l)) := by
    rw [← rsum_add]
    apply rsum_congr
    intro k hk
    rw [← rsum_add]
    apply rsum_congr
    intro l hl
    by_cases h : k = l <;> simp [h]
  have hsq2 : rsum n (fun j => (A j)^2) = rsum n (fun j => A j * A j) := by
    apply rsum_congr
    intro j hj
    have h : (A j)^2 = A j * A j := by grind
    rw [h]
  omega

private theorem factor5 (n : Nat) (A D : Nat → Int) (B C E : Nat → Nat → Int) :
    rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      A u * (B u x * (C u y * (D v * E v w)))))))) =
    (rsum n (fun u => A u * rsum n (B u) * rsum n (C u))) *
    (rsum n (fun v => D v * rsum n (E v))) := by
  let K : Int := rsum n (fun v => D v * rsum n (E v))
  have h1 : ∀ u x y v,
      rsum n (fun w => A u * (B u x * (C u y * (D v * E v w)))) =
        (A u * (B u x * (C u y * D v))) * rsum n (E v) := by
    intro u x y v
    rw [show rsum n (fun w => A u * (B u x * (C u y * (D v * E v w)))) =
        rsum n (fun w => (A u * (B u x * (C u y * D v))) * E v w) by
      apply rsum_congr
      intro w hw
      ac_rfl]
    exact rsum_const_mul n _ _
  simp only [h1]
  have h2 : ∀ u x y,
      rsum n (fun v => (A u * (B u x * (C u y * D v))) * rsum n (E v)) =
        (A u * (B u x * C u y)) * K := by
    intro u x y
    rw [show rsum n (fun v => (A u * (B u x * (C u y * D v))) * rsum n (E v)) =
        rsum n (fun v => (A u * (B u x * C u y)) * (D v * rsum n (E v))) by
      apply rsum_congr
      intro v hv
      ac_rfl]
    dsimp only [K]
    rw [rsum_const_mul]
  simp only [h2]
  have h3 : ∀ u x,
      rsum n (fun y => (A u * (B u x * C u y)) * K) =
        (A u * B u x * K) * rsum n (C u) := by
    intro u x
    rw [show rsum n (fun y => (A u * (B u x * C u y)) * K) =
        rsum n (fun y => (A u * B u x * K) * C u y) by
      apply rsum_congr
      intro y hy
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h3]
  have h4 : ∀ u,
      rsum n (fun x => (A u * B u x * K) * rsum n (C u)) =
        (A u * rsum n (C u) * K) * rsum n (B u) := by
    intro u
    rw [show rsum n (fun x => (A u * B u x * K) * rsum n (C u)) =
        rsum n (fun x => (A u * rsum n (C u) * K) * B u x) by
      apply rsum_congr
      intro x hx
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h4]
  rw [show rsum n (fun u => (A u * rsum n (C u) * K) * rsum n (B u)) =
      rsum n (fun u => K * (A u * rsum n (B u) * rsum n (C u))) by
    apply rsum_congr
    intro u hu
    ac_rfl]
  rw [rsum_const_mul]
  dsimp only [K]
  ac_rfl

private theorem factor_bowtie (n : Nat) (A D B C E F : Nat → Nat → Int) :
    rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      A u x * (B x y * (C y u * (D u v * (E v w * F w u))))))))) =
    rsum n (fun u =>
      (rsum n (fun x => rsum n (fun y => A u x * (B x y * C y u)))) *
      (rsum n (fun v => rsum n (fun w => D u v * (E v w * F w u))))) := by
  have h1 : ∀ u x y v,
      rsum n (fun w => A u x * (B x y * (C y u * (D u v * (E v w * F w u))))) =
        (A u x * (B x y * (C y u * D u v))) * rsum n (fun w => E v w * F w u) := by
    intro u x y v
    rw [show rsum n (fun w => A u x * (B x y * (C y u * (D u v * (E v w * F w u))))) =
        rsum n (fun w => (A u x * (B x y * (C y u * D u v))) * (E v w * F w u)) by
      apply rsum_congr
      intro w hw
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h1]
  have h2 : ∀ u x y,
      rsum n (fun v => (A u x * (B x y * (C y u * D u v))) * rsum n (fun w => E v w * F w u)) =
        (A u x * (B x y * C y u)) * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) := by
    intro u x y
    rw [show rsum n (fun v => (A u x * (B x y * (C y u * D u v))) * rsum n (fun w => E v w * F w u)) =
        rsum n (fun v => (A u x * (B x y * C y u)) * (D u v * rsum n (fun w => E v w * F w u))) by
      apply rsum_congr
      intro v hv
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h2]
  have h3 : ∀ u x,
      rsum n (fun y => (A u x * (B x y * C y u)) * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) =
        (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) *
          rsum n (fun y => B x y * C y u) := by
    intro u x
    rw [show rsum n (fun y => (A u x * (B x y * C y u)) * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) =
        rsum n (fun y => (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) * (B x y * C y u)) by
      apply rsum_congr
      intro y hy
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h3]
  have h4 : ∀ u,
      rsum n (fun x => (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) * rsum n (fun y => B x y * C y u)) =
        rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) *
          rsum n (fun x => A u x * rsum n (fun y => B x y * C y u)) := by
    intro u
    rw [show rsum n (fun x => (A u x * rsum n (fun v => D u v * rsum n (fun w => E v w * F w u))) * rsum n (fun y => B x y * C y u)) =
        rsum n (fun x => rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) *
          (A u x * rsum n (fun y => B x y * C y u))) by
      apply rsum_congr
      intro x hx
      ac_rfl]
    rw [rsum_const_mul]
  simp only [h4]
  have hL : ∀ u, rsum n (fun x => A u x * rsum n (fun y => B x y * C y u)) =
      rsum n (fun x => rsum n (fun y => A u x * (B x y * C y u))) := by
    intro u
    apply rsum_congr
    intro x hx
    exact (rsum_const_mul n (A u x) (fun y => B x y * C y u)).symm
  simp only [hL]
  have hR : ∀ u, rsum n (fun v => rsum n (fun w => D u v * (E v w * F w u))) =
      rsum n (fun v => D u v * rsum n (fun w => E v w * F w u)) := by
    intro u
    apply rsum_congr
    intro v hv
    exact rsum_const_mul n (D u v) (fun w => E v w * F w u)
  simp only [hR]
  apply rsum_congr
  intro u hu
  ac_rfl

private theorem boundary_messages (p : Pattern) :
    ∀ n db b, elimination p n db b = reference p n db b := by
  intro n db b
  unfold elimination reference
  rw [product_fold_eliminate]
  · unfold product factors
    simp only [List.map_map]
    rfl
  · intro f hf
    simp only [factors, List.mem_map] at hf
    rcases hf with ⟨a, ha, rfl⟩
    exact respects_value db a

private theorem integrate_value_mul (n : Nat) (vars : List Nat) (b' : Bind) (a : Atom)
    (db : DB) (R : Bind → Int) (hsrc : a.src ∉ vars) (hdst : a.dst ∉ vars) :
    integrate n vars b' (fun x => value db a x * R x) =
      db a.rel (b' a.src) (b' a.dst) * integrate n vars b' R := by
  induction vars generalizing b' with
  | nil =>
      simp [integrate, value]
  | cons w ws ih =>
      have hws : a.src ∉ ws := by intro h; exact hsrc (by simp [h])
      have hws' : a.dst ∉ ws := by intro h; exact hdst (by simp [h])
      have hw_src : (w == a.src) = false := by
        by_cases h : w == a.src
        · have : w = a.src := Nat.eq_of_beq_eq_true h
          exact False.elim (hsrc (by simp [this]))
        · simpa using h
      have hw_dst : (w == a.dst) = false := by
        by_cases h : w == a.dst
        · have : w = a.dst := Nat.eq_of_beq_eq_true h
          exact False.elim (hdst (by simp [this]))
        · simpa using h
      have hsrc_ne : a.src ≠ w := by intro h; exact hsrc (by simp [h])
      have hdst_ne : a.dst ≠ w := by intro h; exact hdst (by simp [h])
      change ((List.range n).map (fun t =>
        integrate n ws (put b' w t) (fun x => value db a x * R x))).sum =
        db a.rel (b' a.src) (b' a.dst) *
          ((List.range n).map (fun t => integrate n ws (put b' w t) R)).sum
      rw [show ((List.range n).map (fun t =>
        integrate n ws (put b' w t) (fun x => value db a x * R x))).sum =
        ((List.range n).map (fun t =>
          db a.rel ((put b' w t) a.src) ((put b' w t) a.dst) *
            integrate n ws (put b' w t) R)).sum by
        apply sum_map_congr
        intro t ht
        exact ih (put b' w t) hws hws']
      rw [← sum_mul_left]
      apply sum_map_congr
      intro t ht
      simp [put, hw_src, hw_dst, hsrc_ne, hdst_ne]

private theorem anchored_repair (p : Pattern) (hnd : p.freeVars.Nodup)
    (hne : ∀ a ∈ p.atoms, a.src ≠ a.dst) :
    ∀ n old new b, reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a,i) => pivotTerm p n old new b i a)).sum := by
  intro n old new b
  let F : Nat → Int := fun j => integrate n p.freeVars b (fun x =>
      ((p.atoms.take j).map (fun a => value new a x)).prod *
      ((p.atoms.drop j).map (fun a => value old a x)).prod)
  have hF0 : F 0 = reference p n old b := by
    dsimp only [F, reference]
    simp
  have hFL : F p.atoms.length = reference p n new b := by
    dsimp only [F, reference]
    simp
  rw [← hFL, ← hF0]
  change F p.atoms.length - F 0 = _
  rw [show F p.atoms.length - F 0 =
      ((p.atoms.zipIdx 0).map (fun (a,i) => F (i+1) - F i)).sum by
    simpa using (telescope_zip p.atoms F 0).symm]
  apply sum_map_congr
  intro pair hpair
  rcases pair with ⟨a, j⟩
  rcases List.mem_zipIdx hpair with ⟨hj0, hjlen, ha⟩
  simp only [Nat.zero_add] at hjlen
  have hj : j < p.atoms.length := hjlen
  have ha' : a = p.atoms.get ⟨j, hj⟩ := by
    simpa using ha
  subst a
  change F (j+1) - F j = pivotTerm p n old new b j (p.atoms.get ⟨j, hj⟩)
  have hne_a : (p.atoms.get ⟨j, hj⟩).src ≠ (p.atoms.get ⟨j, hj⟩).dst :=
    hne _ (List.get_mem _ _)
  have hstep : F (j+1) - F j = integrate n p.freeVars b (fun x =>
      (value new (p.atoms.get ⟨j, hj⟩) x - value old (p.atoms.get ⟨j, hj⟩) x) *
        (((p.atoms.take j).map (fun c => value new c x)).prod *
         ((p.atoms.drop (j+1)).map (fun c => value old c x)).prod)) := by
    dsimp only [F]
    exact F_step n p old new b j hj
  rw [hstep]
  let a := p.atoms.get ⟨j, hj⟩
  let R : Bind → Int := fun x =>
      ((p.atoms.take j).map (fun c => value new c x)).prod *
      ((p.atoms.drop (j+1)).map (fun c => value old c x)).prod
  change integrate n p.freeVars b (fun x => (value new a x - value old a x) * R x) = _
  rw [integrate_pivot n p.freeVars a.src a.dst b (fun x => (value new a x - value old a x) * R x)
    hnd hne_a]
  dsimp only
  -- pointwise rewrite of the inner integrals
  let vars := p.freeVars.filter (fun w => w != a.src && w != a.dst)
  have hsrc_not : a.src ∉ vars := by
    intro h
    dsimp only [vars] at h
    have hf := (List.mem_filter.mp h).2
    simpa using hf
  have hdst_not : a.dst ∉ vars := by
    intro h
    dsimp only [vars] at h
    have hf := (List.mem_filter.mp h).2
    simpa using hf
  have hbind_src : ∀ u v, (put (put b a.src u) a.dst v) a.src = u := by
    intro u v
    by_cases h : a.src == a.dst
    · have h' : a.src = a.dst := Nat.eq_of_beq_eq_true h
      exact False.elim (hne_a h')
    · simp [put, h]
  have hbind_dst : ∀ u v, (put (put b a.src u) a.dst v) a.dst = v := by
    intro u v
    simp [put]
  have hinner : ∀ u v,
      integrate n (p.freeVars.filter (fun w => w != a.src && w != a.dst))
        (put (put b a.src u) a.dst v)
        (fun x => (value new a x - value old a x) * R x) =
      (new a.rel u v - old a.rel u v) *
        integrate n (p.freeVars.filter (fun w => w != a.src && w != a.dst))
          (put (put b a.src u) a.dst v) R := by
    intro u v
    change integrate n vars (put (put b a.src u) a.dst v)
        (fun x => (value new a x - value old a x) * R x) =
      (new a.rel u v - old a.rel u v) * integrate n vars
        (put (put b a.src u) a.dst v) R
    rw [show (fun x => (value new a x - value old a x) * R x) =
        (fun x => value new a x * R x - value old a x * R x) from by
      funext x
      rw [Int.sub_mul]]
    rw [integrate_sub]
    rw [integrate_value_mul n vars (put (put b a.src u) a.dst v) a new R hsrc_not hdst_not,
        integrate_value_mul n vars (put (put b a.src u) a.dst v) a old R hsrc_not hdst_not]
    rw [hbind_src u v, hbind_dst u v]
    rw [Int.sub_mul]
  simp only [hinner]
  -- unfold pivotTerm and drop the filter
  dsimp only [pivotTerm]
  have hfilter : ∀ u,
      (((if p.freeVars.contains a.dst then List.range n else [b a.dst]).filter
        (fun w => old a.rel u w != new a.rel u w)).map (fun v =>
          let delta := new a.rel u v - old a.rel u v
          delta * integrate n (p.freeVars.filter (fun w => w != a.src && w != a.dst))
            (put (put b a.src u) a.dst v) R)).sum =
      ((if p.freeVars.contains a.dst then List.range n else [b a.dst]).map (fun v =>
          (new a.rel u v - old a.rel u v) *
            integrate n vars (put (put b a.src u) a.dst v) R)).sum := by
    intro u
    dsimp only [vars]
    exact sum_filter_delta _ (fun v => old a.rel u v) (fun v => new a.rel u v)
      (fun v => integrate n (p.freeVars.filter (fun w => w != a.src && w != a.dst))
        (put (put b a.src u) a.dst v) R)
  dsimp only [a, R] at hfilter
  simp only [hfilter]
  dsimp only [a, R, vars]

/-! Namespace goals. -/

namespace VoteBadge
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  exact anchored_repair pattern (by native_decide) (by native_decide)

theorem IndependentArms_sound : IndependentArms.Goal := by
  intro n db b
  unfold reference
  have hperm : pattern.freeVars.Perm [1,2,3,4,5] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, pattern, value, put, total]
  change rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      db 0 (b 0) u * (db 1 u x * (db 2 u y * (db 3 (b 0) v * db 3 v w)))))))) =
    rsum n (fun u => db 0 (b 0) u * rsum n (db 1 u) * rsum n (db 2 u)) *
      rsum n (fun v => db 3 (b 0) v * rsum n (db 3 v))
  exact factor5 n (fun u => db 0 (b 0) u) (fun v => db 3 (b 0) v)
    (fun u x => db 1 u x) (fun u y => db 2 u y) (fun v w => db 3 v w)

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end VoteBadge

namespace ReplyCycle
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ReplyCycle

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  exact anchored_repair pattern (by native_decide) (by native_decide)

theorem UndirectedTrace_sound : UndirectedTrace.Goal := by
  intro n db b hsym
  unfold reference
  have hperm : pattern.freeVars.Perm [0,1,2,3] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, pattern, value, put, bridge, total]
  change rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      db 0 x0 x1 * (db 0 x1 x2 * (db 0 x2 x3 * db 0 x3 x0)))))) =
    rsum n (fun i => rsum n (fun j => (rsum n (fun k => db 0 i k * db 0 k j))^2))
  have hsq : ∀ X : Int, X ^ 2 = X * X := by intro X; grind
  simp only [hsq]
  have hexp : ∀ i j,
      (rsum n (fun k => db 0 i k * db 0 k j)) * (rsum n (fun l => db 0 i l * db 0 l j)) =
      rsum n (fun k => rsum n (fun l => (db 0 i k * db 0 k j) * (db 0 i l * db 0 l j))) := by
    intro i j
    rw [rsum_product]
  simp only [hexp]
  apply rsum_congr
  intro i hi
  rw [rsum_comm]
  apply rsum_congr
  intro k hk
  apply rsum_congr
  intro j hj
  apply rsum_congr
  intro l hl
  rw [hsym i hi l hl, hsym l hl k hk]
  ac_rfl

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end ReplyCycle

namespace FriendBridge
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.FriendBridge

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  exact anchored_repair pattern (by native_decide) (by native_decide)

theorem BidirectionalMeet_sound : BidirectionalMeet.Goal := by
  intro n db b
  simp [reference, integrate, pattern, value, put, bridge, dot, total]
  change rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 x2 x3 * db 3 x3 (b 4)))))) =
    rsum n (fun u => (rsum n (fun y => db 0 (b 0) y * db 1 y u)) *
      (rsum n (fun z => db 2 u z * db 3 z (b 4))))
  rw [rsum_comm]
  apply rsum_congr
  intro u hu
  have hreg : rsum n (fun x1 => rsum n (fun x3 =>
        db 0 (b 0) x1 * (db 1 x1 u * (db 2 u x3 * db 3 x3 (b 4))))) =
      rsum n (fun x1 => rsum n (fun x3 =>
        (db 0 (b 0) x1 * db 1 x1 u) * (db 2 u x3 * db 3 x3 (b 4)))) := by
    apply rsum_congr
    intro x1 h1
    apply rsum_congr
    intro x3 h3
    ac_rfl
  rw [hreg]
  rw [rsum_product]

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end FriendBridge

namespace CoLikes
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CoLikes

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  exact anchored_repair pattern (by native_decide) (by native_decide)

theorem DistinctMessages_sound : DistinctMessages.Goal := by
  intro n db b
  have hperm : pattern.freeVars.Perm [0,2,3] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, pattern, value, put, total, overlap, dot, factors, product]
  change rsum n (fun u => rsum n (fun x => rsum n (fun y =>
      if x = y then 0 else
        db 0 u x * (db 0 (b 1) x * (db 0 (b 1) y * (db 0 u y * db 2 u (b 1))))))) =
    rsum n (fun u => db 2 u (b 1) *
      ((rsum n (fun k => db 0 u k * db 0 (b 1) k))^2 -
       rsum n (fun j => (db 0 u j * db 0 (b 1) j)^2)))
  have hbracket : ∀ u,
      (rsum n (fun k => db 0 u k * db 0 (b 1) k))^2 -
        rsum n (fun j => (db 0 u j * db 0 (b 1) j)^2) =
      rsum n (fun x => rsum n (fun y => if x = y then 0 else
        (db 0 u x * db 0 (b 1) x) * (db 0 u y * db 0 (b 1) y))) := by
    intro u
    exact (offdiag_square n (fun k => db 0 u k * db 0 (b 1) k)).symm
  simp only [hbracket]
  apply rsum_congr
  intro u hu
  rw [show rsum n (fun x => rsum n (fun y =>
      if x = y then 0 else
        db 0 u x * (db 0 (b 1) x * (db 0 (b 1) y * (db 0 u y * db 2 u (b 1)))))) =
      rsum n (fun x => rsum n (fun y => db 2 u (b 1) *
        (if x = y then 0 else
          (db 0 u x * db 0 (b 1) x) * (db 0 u y * db 0 (b 1) y)))) by
    apply rsum_congr
    intro x hx
    apply rsum_congr
    intro y hy
    by_cases h : x = y
    · simp [h]
    · simp [h]
      ac_rfl]
  rw [show rsum n (fun x => rsum n (fun y => db 2 u (b 1) *
        (if x = y then 0 else
          (db 0 u x * db 0 (b 1) x) * (db 0 u y * db 0 (b 1) y)))) =
      rsum n (fun x => db 2 u (b 1) * rsum n (fun y =>
        (if x = y then 0 else
          (db 0 u x * db 0 (b 1) x) * (db 0 u y * db 0 (b 1) y)))) by
    apply rsum_congr
    intro x hx
    rw [rsum_const_mul]]
  rw [rsum_const_mul]

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end CoLikes

namespace ForumBowtie
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ForumBowtie

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  exact anchored_repair pattern (by native_decide) (by native_decide)

theorem ArticulationSquare_sound : ArticulationSquare.Goal := by
  intro n db b
  unfold reference
  have hperm : pattern.freeVars.Perm [0,1,2,3,4] := by native_decide
  rw [integrate_perm n hperm b]
  simp [integrate, pattern, value, put, bridge, loop3, total]
  change rsum n (fun u => rsum n (fun x => rsum n (fun y => rsum n (fun v => rsum n (fun w =>
      db 0 u x * (db 1 x y * (db 2 y u * (db 0 u v * (db 1 v w * db 2 w u))))))))) =
    rsum n (fun x => (rsum n (fun y => (rsum n (fun k => db 0 x k * db 1 k y)) * db 2 y x))^2)
  rw [factor_bowtie n (fun u x => db 0 u x) (fun u v => db 0 u v)
    (fun x y => db 1 x y) (fun y u => db 2 y u)
    (fun v w => db 1 v w) (fun w u => db 2 w u)]
  apply rsum_congr
  intro u hu
  have hloop : rsum n (fun x => rsum n (fun y => db 0 u x * (db 1 x y * db 2 y u))) =
      rsum n (fun y => (rsum n (fun k => db 0 u k * db 1 k y)) * db 2 y u) := by
    rw [rsum_comm]
    apply rsum_congr
    intro y hy
    rw [show rsum n (fun x => db 0 u x * (db 1 x y * db 2 y u)) =
        rsum n (fun x => (db 0 u x * db 1 x y) * db 2 y u) by
      apply rsum_congr
      intro x hx
      ac_rfl]
    rw [rsum_mul_const]
  rw [hloop]
  let X : Int := rsum n (fun y => (rsum n (fun k => db 0 u k * db 1 k y)) * db 2 y u)
  change X * X = X ^ 2
  have hs : X * X = X ^ 2 := by grind
  rw [hs]

-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end ForumBowtie

end TinylambOpt.ResearchProofs.Atlas_JoinPlans_G2
