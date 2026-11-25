import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas_JoinPlans_G4

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


private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) (fun _ => (0 : Int)))).sum = 0
      rw [sum_map_congr (List.range n) (fun _ => integrate n vs _ (fun _ => (0 : Int))) (fun _ => 0) (by intro x hx; exact ih (put b v x))]
      simp [sum_const]

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [integrate_add, ih]

/-! Namespace goals: BoundaryMessages -/

namespace TwoHopAntiWitness
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

end TwoHopAntiWitness

namespace HubWithChords
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

end HubWithChords

namespace SupplierClique
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

end SupplierClique

private theorem rsum_prod2 (n : Nat) (A B : Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      A a b * B c d)))) =
    (rsum n (fun a => rsum n (fun b => A a b))) *
      (rsum n (fun c => rsum n (fun d => B c d))) := by
  have h1 : ∀ a b c, rsum n (fun d => A a b * B c d) = A a b * rsum n (fun d => B c d) := by
    intro a b c
    exact rsum_const_mul n (A a b) (fun d => B c d)
  have hL1 : rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      A a b * B c d)))) = rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      A a b * rsum n (fun d => B c d)))) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    apply rsum_congr; intro c hc
    exact h1 a b c
  rw [hL1]
  let K := rsum n (fun c => rsum n (fun d => B c d))
  have h2 : ∀ a b, rsum n (fun c => A a b * rsum n (fun d => B c d)) = A a b * K := by
    intro a b
    have h' : (fun c => A a b * rsum n (fun d => B c d)) =
        (fun c => A a b * (rsum n (fun d => B c d))) := rfl
    rw [h']
    rw [rsum_const_mul]
  have hL2 : rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      A a b * rsum n (fun d => B c d)))) = rsum n (fun a => rsum n (fun b => A a b * K)) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    exact h2 a b
  rw [hL2]
  have h3 : ∀ a, rsum n (fun b => A a b * K) = K * rsum n (fun b => A a b) := by
    intro a
    rw [show (fun b => A a b * K) = (fun b => K * A a b) by funext b; ac_rfl]
    exact rsum_const_mul n K (fun b => A a b)
  have hL3 : rsum n (fun a => rsum n (fun b => A a b * K)) =
      rsum n (fun a => K * rsum n (fun b => A a b)) := by
    apply rsum_congr; intro a ha
    exact h3 a
  rw [hL3]
  rw [rsum_const_mul]
  dsimp only [K]
  ac_rfl

private theorem rsum_prod2E (n : Nat) (C : Int) (A B : Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      C * (A a b * B c d))))) =
    C * (rsum n (fun a => rsum n (fun b => A a b))) *
      (rsum n (fun c => rsum n (fun d => B c d))) := by
  have hconv : (rsum n fun a => rsum n fun b => rsum n fun c => rsum n fun d =>
      C * (A a b * B c d)) = (rsum n fun a => rsum n fun b => rsum n fun c => rsum n fun d =>
      (C * A a b) * B c d) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    apply rsum_congr; intro c hc
    apply rsum_congr; intro d hd
    ac_rfl
  rw [hconv]
  rw [rsum_prod2]
  have hC : (rsum n fun a => rsum n fun b => C * A a b) =
      C * (rsum n fun a => rsum n fun b => A a b) := by
    rw [show (fun a => rsum n (fun b => C * A a b)) = (fun a => C * rsum n (fun b => A a b)) by
      funext a
      rw [rsum_const_mul]]
    rw [rsum_const_mul]
  rw [hC]


namespace IntersectingCycles
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles


theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

theorem TwoSeparators_sound : TwoSeparators.Goal := by
  intro n db b
  simp [reference, pattern, value, bridge, dot, total, rsum]
  rw [integrate_perm n (by native_decide : [4, 5, 1, 2, 3, 0].Perm [2, 3, 0, 1, 4, 5])]
  let U := fun z u => rsum n (fun k => db 0 z k * db 0 k u)
  let V := fun y v => rsum n (fun k => db 1 y k * db 2 k v)
  change rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x0 => rsum n (fun x1 =>
      rsum n (fun x4 => rsum n (fun x5 =>
        db 0 x0 x1 * (db 0 x1 x2 * (db 0 x2 x3 * (db 0 x3 x0 *
          (db 1 x2 x4 * (db 2 x4 x5 * db 2 x5 x3))))))))))) =
    rsum n (fun y => rsum n (fun z => db 0 y z * rsum n (fun u => U z u * db 0 u y) *
      rsum n (fun v => V y v * db 2 v z)))
  apply rsum_congr
  intro y hy
  apply rsum_congr
  intro z hz
  have hraw : rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x4 => rsum n (fun x5 =>
      db 0 x0 x1 * (db 0 x1 y * (db 0 y z * (db 0 z x0 *
        (db 1 y x4 * (db 2 x4 x5 * db 2 x5 z))))))))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
        db 0 y z * ((db 0 z a * db 0 a c * db 0 c y) *
          (db 1 y d * db 2 d e * db 2 e z)))))) := by
    apply rsum_congr; intro x0 h0
    apply rsum_congr; intro x1 h1
    apply rsum_congr; intro x4 h4
    apply rsum_congr; intro x5 h5
    ac_rfl
  rw [hraw]
  have hshape : (rsum n fun a => rsum n fun c => rsum n fun d => rsum n fun e =>
      db 0 y z * (db 0 z a * db 0 a c * db 0 c y * (db 1 y d * db 2 d e * db 2 e z))) =
      (rsum n fun a => rsum n fun c => rsum n fun d => rsum n fun e =>
        db 0 y z * ((db 0 z a * db 0 a c * db 0 c y) *
          (db 1 y d * db 2 d e * db 2 e z))) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro c hc
    apply rsum_congr; intro d hd
    apply rsum_congr; intro e he
    ac_rfl
  rw [hshape]
  rw [rsum_prod2E]
  have hA : (rsum n fun a => rsum n fun c => db 0 z a * db 0 a c * db 0 c y) =
      rsum n (fun u => U z u * db 0 u y) := by
    rw [rsum_comm]
    apply rsum_congr
    intro u hu
    rw [show (fun a => db 0 z a * db 0 a u * db 0 u y) =
        (fun a => (db 0 z a * db 0 a u) * db 0 u y) by funext a; ac_rfl]
    rw [rsum_mul_const]

  have hB : (rsum n fun d => rsum n fun e => db 1 y d * db 2 d e * db 2 e z) =
      rsum n (fun v => V y v * db 2 v z) := by
    rw [rsum_comm]
    apply rsum_congr
    intro v hv
    rw [show (fun d => db 1 y d * db 2 d v * db 2 v z) =
        (fun d => (db 1 y d * db 2 d v) * db 2 v z) by funext d; ac_rfl]
    rw [rsum_mul_const]

  rw [hA, hB]

end IntersectingCycles

namespace ParallelLineage
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage

theorem BoundaryMessages_sound : BoundaryMessages.Goal := by
  exact boundary_messages pattern

end ParallelLineage

/-! Anchored repair helpers. -/

private theorem put_get_other (b : Bind) (v w x : Nat) (h : w ≠ v) : put b v x w = b w := by
  by_cases h' : w == v
  · have : w = v := Nat.eq_of_beq_eq_true h'
    exact (h this).elim
  · simp [put, show (w == v) = false by cases h'' : w == v <;> simp_all]

private theorem put_src_dst_value (b : Bind) (src dst u v : Nat) (hne : src ≠ dst) :
    put (put b src u) dst v src = u := by
  rw [put_get_other (put b src u) dst src v hne]
  simp [put]

private theorem put_dst_dst_value (b : Bind) (src dst u v : Nat) :
    put (put b src u) dst v dst = v := by
  simp [put]

private theorem integrate_fixed (n : Nat) (vs : List Nat) (b : Bind) (v : Nat)
    (hv : v ∉ vs) (F : Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) x) = integrate n vs b (fun x => F (b v) x) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      have hvw : v ≠ w := by intro h; subst h; exact hv (by simp)
      have hvw' : v ∉ ws := by intro h; exact hv (by simp [h])
      change ((List.range n).map (fun t => integrate n ws (put b w t) (fun x => F (x v) x))).sum =
        ((List.range n).map (fun t => integrate n ws (put b w t) (fun x => F (b v) x))).sum
      apply sum_map_congr
      intro t ht
      rw [ih (put b w t) hvw']
      rw [show (put b w t) v = b v by exact put_get_other b w v t hvw]

private theorem integrate_fixed2 (n : Nat) (vs : List Nat) (b : Bind) (v w : Nat)
    (hv : v ∉ vs) (hw : w ∉ vs) (F : Nat → Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) (x w) x) = integrate n vs b (fun x => F (b v) (b w) x) := by
  calc
    integrate n vs b (fun x => F (x v) (x w) x)
        = integrate n vs b (fun x => F (b v) (x w) x) :=
          integrate_fixed n vs b v hv (fun u x => F u (x w) x)
    _ = integrate n vs b (fun x => F (b v) (b w) x) :=
          integrate_fixed n vs b w hw (fun u x => F (b v) u x)

private theorem sum_filter_eq {α} (l : List α) (p : α → Bool) (g : α → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : p x = true
      · simp [hx, ih (fun y hy hp => h y (by simp [hy]) hp)]
      · have hxf : p x = false := by cases hpx : p x <;> simp_all
        have hx0 : g x = 0 := h x (by simp) hxf
        simp [hxf, hx0, ih (fun y hy hp => h y (by simp [hy]) hp)]

private def cpExcl (atoms : List Atom) (old new : DB) (i : Nat) (b : Bind) : Int :=
  ((atoms.take i).map (fun a => value new a b)).prod *
    ((atoms.drop (i+1)).map (fun a => value old a b)).prod

private theorem cpExcl_zero (a : Atom) (as : List Atom) (old new : DB) (b : Bind) :
    cpExcl (a::as) old new 0 b = (as.map (fun t => value old t b)).prod := by
  simp [cpExcl]

private theorem cpExcl_succ (a : Atom) (as : List Atom) (old new : DB) (i : Nat) (b : Bind) :
    cpExcl (a::as) old new (i+1) b = value new a b * cpExcl as old new i b := by
  simp [cpExcl, Int.mul_assoc]

private theorem list_sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem prod_diff_telescope (atoms : List Atom) (old new : DB) (b : Bind) :
    (atoms.map (fun a => value new a b)).prod - (atoms.map (fun a => value old a b)).prod =
      ((atoms.zipIdx.map (fun (a, i) =>
        (value new a b - value old a b) * cpExcl atoms old new i b)).sum) := by
  induction atoms with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.prod_cons, List.zipIdx_cons, List.zipIdx_succ,
        List.sum_cons, List.map_map, Function.comp_apply]
      rw [cpExcl_zero a as old new b]
      rw [show (List.map ((fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * cpExcl (a::as) old new x.2 b) ∘
            (fun x : Atom × Nat => (x.1, x.2 + 1))) as.zipIdx) =
          (as.zipIdx.map (fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * cpExcl (a::as) old new (x.2+1) b)) by rfl]
      rw [show (as.zipIdx.map (fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * cpExcl (a::as) old new (x.2+1) b)) =
          (as.zipIdx.map (fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * (value new a b * cpExcl as old new x.2 b))) by
            apply List.map_congr_left
            intro x hx
            rw [cpExcl_succ]]
      rw [show (List.map (fun x : Atom × Nat =>
            (value new x.1 b - value old x.1 b) * (value new a b * cpExcl as old new x.2 b))
            (as.zipIdx)) =
          (as.zipIdx.map (fun (t, i) =>
            value new a b * ((value new t b - value old t b) * cpExcl as old new i b))) by
            apply List.map_congr_left
            intro x hx
            ac_rfl]
      rw [list_sum_mul_left]
      rw [← ih]
      rw [Int.sub_mul, Int.mul_sub]
      omega

private theorem pivot_point (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst)
    (u w : Nat) :
    let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
    let b' := put (put b a.src u) a.dst w
    integrate n vars b' (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      (new a.rel u w - old a.rel u w) *
        integrate n vars b' (fun x => cpExcl p.atoms old new j x) := by
  dsimp only
  let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
  let b' := put (put b a.src u) a.dst w
  have hsrc_not : a.src ∉ vars := by
    dsimp only [vars]
    intro h
    have hm := (List.mem_filter.mp h).2
    simp at hm
  have hdst_not : a.dst ∉ vars := by
    dsimp only [vars]
    intro h
    have hm := (List.mem_filter.mp h).2
    simp at hm
  have hsrc_b : b' a.src = u := by
    dsimp only [b']
    exact put_src_dst_value b a.src a.dst u w hsd
  have hdst_b : b' a.dst = w := by
    dsimp only [b']
    exact put_dst_dst_value b a.src a.dst u w
  change integrate n vars b'
      (fun x => (new a.rel (x a.src) (x a.dst) - old a.rel (x a.src) (x a.dst)) *
        cpExcl p.atoms old new j x) =
    (new a.rel u w - old a.rel u w) *
      integrate n vars b' (fun x => cpExcl p.atoms old new j x)
  rw [integrate_fixed2 n vars b' a.src a.dst hsrc_not hdst_not
    (fun p1 q1 x => (new a.rel p1 q1 - old a.rel p1 q1) * cpExcl p.atoms old new j x)]
  rw [hsrc_b, hdst_b]
  rw [integrate_mul_left]

private theorem pivot_integral_eq (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) :
    integrate n p.freeVars b (fun x =>
      (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      pivotTerm p n old new b j a := by
  let src := a.src
  let dst := a.dst
  let vars := p.freeVars.filter (fun v => v != src && v != dst)
  let cand := fun v => if p.freeVars.contains v then List.range n else [b v]
  have hpiv := integrate_pivot n p.freeVars src dst b
    (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) hnd hsd
  dsimp only at hpiv
  rw [hpiv]
  apply sum_map_congr
  intro u hu
  -- first drop the filter on the destination candidates
  rw [← sum_filter_eq (if p.freeVars.contains dst then List.range n else [b dst])
        (fun w => old a.rel u w != new a.rel u w)
        (fun w => integrate n vars (put (put b src u) dst w)
          (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x))
        (by
          intro w hw hp
          have heq : old a.rel u w = new a.rel u w := by
            simpa using hp
          have hz := pivot_point p n old new b j a hnd hsd u w
          dsimp only [vars, src, dst] at hz
          rw [hz]
          simp [heq, integrate_zero])]
  apply sum_map_congr
  intro w hw
  have hz := pivot_point p n old new b j a hnd hsd u w
  dsimp only [vars, src, dst] at hz
  rw [hz]
  rfl

private theorem anchored_repair_generic (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (hnd : p.freeVars.Nodup) (hsd : ∀ a ∈ p.atoms, a.src ≠ a.dst) :
    reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
  have htel : ∀ x : Bind,
      (p.atoms.map (fun a => value new a x)).prod - (p.atoms.map (fun a => value old a x)).prod =
        ((p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) := by
    intro x
    exact prod_diff_telescope p.atoms old new x
  calc
    reference p n new b - reference p n old b
        = integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value new a x)).prod) -
          integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value old a x)).prod) := rfl
    _ = integrate n p.freeVars b (fun x =>
          (p.atoms.map (fun a => value new a x)).prod -
          (p.atoms.map (fun a => value old a x)).prod) := by
          rw [integrate_sub]
    _ = integrate n p.freeVars b (fun x => (p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) := by
          apply integrate_congr
          intro x
          exact htel x
    _ = (p.atoms.zipIdx.map (fun (a, i) => integrate n p.freeVars b (fun x =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x))).sum := by
          rw [integrate_list_sum]
    _ = (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
          apply sum_map_congr
          intro ai hai
          rcases ai with ⟨a, i⟩
          have hmem : a ∈ p.atoms := by
            have hz := List.mem_zipIdx' hai
            rw [hz.2]
            exact List.get_mem p.atoms ⟨i, hz.1⟩
          exact pivot_integral_eq p n old new b i a hnd (hsd a hmem)

/-! Anchored repair goals. -/

namespace TwoHopAntiWitness
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end TwoHopAntiWitness

namespace HubWithChords
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end HubWithChords

namespace SupplierClique
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end SupplierClique

namespace IntersectingCycles
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end IntersectingCycles

namespace ParallelLineage
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage

theorem AnchoredRepair_sound : AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end ParallelLineage

/-! Shared helpers for the remaining special goals. -/

private theorem prod_pos_iff_of_nonneg {a b : Int} (ha : 0 ≤ a) (hb : 0 ≤ b) :
    0 < a * b ↔ 0 < a ∧ 0 < b := by
  constructor
  · intro h
    have ha0 : a ≠ 0 := by intro hz; subst a; simp at h
    have hb0 : b ≠ 0 := by intro hz; subst b; simp at h
    constructor <;> omega
  · rintro ⟨ha', hb'⟩
    exact Int.mul_pos ha' hb'

private theorem rsum_nonneg {n : Nat} {f : Nat → Int} (hf : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have h1 := ih (fun i hi => hf i (Nat.lt_trans hi (Nat.lt_succ_self n)))
      have h2 := hf n (Nat.lt_succ_self n)
      omega

private theorem rsum_pos_iff {n : Nat} {f : Nat → Int} (hf : ∀ i, i < n → 0 ≤ f i) :
    0 < rsum n f ↔ ∃ i, i < n ∧ 0 < f i := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      constructor
      · intro h
        by_cases hn : 0 < f n
        · exact ⟨n, Nat.lt_succ_self n, hn⟩
        · have hfn : f n = 0 := by
            have hge := hf n (Nat.lt_succ_self n)
            omega
          have hprev : 0 < rsum n f := by omega
          rcases (ih (fun i hi => hf i (Nat.lt_trans hi (Nat.lt_succ_self n)))).mp hprev with ⟨i, hi, hp⟩
          exact ⟨i, Nat.lt_succ_of_lt hi, hp⟩
      · rintro ⟨i, hi, hp⟩
        by_cases hin : i = n
        · subst i
          have hsum : 0 ≤ rsum n f := rsum_nonneg (fun i hi => hf i (Nat.lt_trans hi (Nat.lt_succ_self n)))
          omega
        · have hi' : i < n := by omega
          have hge := hf n (Nat.lt_succ_self n)
          have hprev : 0 < rsum n f := (ih (fun j hj => hf j (Nat.lt_trans hj (Nat.lt_succ_self n)))).mpr ⟨i, hi', hp⟩
          omega

/-! IndependentWitness -/

namespace TwoHopAntiWitness
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness

private theorem reference_factor (n : Nat) (db : DB) (b : Bind) :
    reference pattern n db b =
      total n (fun z => bridge n db 0 1 (b 0) z * bridge n db 2 1 (b 0) z * total n (db 0 z)) := by
  simp [reference, pattern, value, put, bridge, dot, total, rsum]
  rw [integrate_perm n (by native_decide : [1, 3, 4, 2].Perm [2, 1, 3, 4])]
  change rsum n (fun z => rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
      db 0 (b 0) x1 * (db 1 x1 z * (db 2 (b 0) x3 * (db 1 x3 z * db 0 z x4))))))) =
    rsum n (fun z => (rsum n (fun k => db 0 (b 0) k * db 1 k z)) *
      (rsum n (fun k => db 2 (b 0) k * db 1 k z)) * (rsum n (fun k => db 0 z k)))
  apply rsum_congr
  intro z hz
  let A := fun x => db 0 (b 0) x * db 1 x z
  let C := fun x => db 2 (b 0) x * db 1 x z
  let E := fun x => db 0 z x
  have hraw : rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
      db 0 (b 0) x1 * (db 1 x1 z * (db 2 (b 0) x3 * (db 1 x3 z * db 0 z x4)))))) =
      rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
        A x1 * (C x3 * E x4)))) := by
    apply rsum_congr
    intro x1 hx1
    apply rsum_congr
    intro x3 hx3
    apply rsum_congr
    intro x4 hx4
    dsimp only [A, C, E]
    ac_rfl
  rw [hraw]
  change rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
      A x1 * (C x3 * E x4)))) = (rsum n A) * (rsum n C) * rsum n E
  have h1 : ∀ x1 x3, rsum n (fun x4 => A x1 * (C x3 * E x4)) = (A x1 * C x3) * rsum n E := by
    intro x1 x3
    rw [show (fun x4 => A x1 * (C x3 * E x4)) = (fun x4 => (A x1 * C x3) * E x4) by
      funext x4; ac_rfl]
    exact rsum_const_mul n (A x1 * C x3) E
  have hL : rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
      A x1 * (C x3 * E x4)))) = rsum n (fun x1 => rsum n (fun x3 =>
      (A x1 * C x3) * rsum n E)) := by
    apply rsum_congr
    intro x1 hx1
    apply rsum_congr
    intro x3 hx3
    exact h1 x1 x3
  rw [hL]
  have h2 : ∀ x1, rsum n (fun x3 => (A x1 * C x3) * rsum n E) = (A x1 * rsum n E) * rsum n C := by
    intro x1
    rw [show (fun x3 => (A x1 * C x3) * rsum n E) = (fun x3 => (A x1 * rsum n E) * C x3) by
      funext x3; ac_rfl]
    exact rsum_const_mul n (A x1 * rsum n E) C
  have hL2 : rsum n (fun x1 => rsum n (fun x3 =>
      (A x1 * C x3) * rsum n E)) = rsum n (fun x1 => (A x1 * rsum n E) * rsum n C) := by
    apply rsum_congr
    intro x1 hx1
    exact h2 x1
  rw [hL2]
  have h3 : rsum n (fun x1 => (A x1 * rsum n E) * rsum n C) = (rsum n A) * (rsum n C) * rsum n E := by
    rw [show (fun x1 => (A x1 * rsum n E) * rsum n C) = (fun x1 => (rsum n C * rsum n E) * A x1) by
      funext x1; ac_rfl]
    rw [rsum_const_mul]
    ac_rfl
  rw [h3]

theorem IndependentWitness_sound : IndependentWitness.Goal := by
  intro n db b hnonneg
  rw [reference_factor]
  let F := fun z => bridge n db 0 1 (b 0) z * bridge n db 2 1 (b 0) z * total n (db 0 z)
  have hF : ∀ z, z < n → 0 ≤ F z := by
    intro z hz
    dsimp only [F]
    have hb01 : 0 ≤ bridge n db 0 1 (b 0) z := by
      dsimp only [bridge]
      apply rsum_nonneg
      intro k hk
      exact Int.mul_nonneg (hnonneg 0 (b 0) k) (hnonneg 1 k z)
    have hb21 : 0 ≤ bridge n db 2 1 (b 0) z := by
      dsimp only [bridge]
      apply rsum_nonneg
      intro k hk
      exact Int.mul_nonneg (hnonneg 2 (b 0) k) (hnonneg 1 k z)
    have ht : 0 ≤ total n (db 0 z) := by
      dsimp only [total]
      apply rsum_nonneg
      intro k hk
      exact hnonneg 0 z k
    exact Int.mul_nonneg (Int.mul_nonneg hb01 hb21) ht
  change 0 < rsum n F ↔ _
  rw [rsum_pos_iff hF]
  apply exists_congr
  intro z
  constructor
  · rintro ⟨hz, hp⟩
    refine ⟨hz, ?_, ?_, ?_⟩
    · have hb01 : 0 ≤ bridge n db 0 1 (b 0) z := by
        dsimp only [bridge]
        apply rsum_nonneg
        intro k hk
        exact Int.mul_nonneg (hnonneg 0 (b 0) k) (hnonneg 1 k z)
      have hb21 : 0 ≤ bridge n db 2 1 (b 0) z := by
        dsimp only [bridge]
        apply rsum_nonneg
        intro k hk
        exact Int.mul_nonneg (hnonneg 2 (b 0) k) (hnonneg 1 k z)
      have ht : 0 ≤ total n (db 0 z) := by
        dsimp only [total]
        apply rsum_nonneg
        intro k hk
        exact hnonneg 0 z k
      have h12 := (prod_pos_iff_of_nonneg (Int.mul_nonneg hb01 hb21) ht).mp hp
      exact (prod_pos_iff_of_nonneg hb01 hb21).mp h12.1 |>.1
    · -- second bridge
      have hb01 : 0 ≤ bridge n db 0 1 (b 0) z := by
        dsimp only [bridge]
        apply rsum_nonneg
        intro k hk
        exact Int.mul_nonneg (hnonneg 0 (b 0) k) (hnonneg 1 k z)
      have hb21 : 0 ≤ bridge n db 2 1 (b 0) z := by
        dsimp only [bridge]
        apply rsum_nonneg
        intro k hk
        exact Int.mul_nonneg (hnonneg 2 (b 0) k) (hnonneg 1 k z)
      have ht : 0 ≤ total n (db 0 z) := by
        dsimp only [total]
        apply rsum_nonneg
        intro k hk
        exact hnonneg 0 z k
      have h12 := (prod_pos_iff_of_nonneg (Int.mul_nonneg hb01 hb21) ht).mp hp
      exact (prod_pos_iff_of_nonneg hb01 hb21).mp h12.1 |>.2
    · have hb01 : 0 ≤ bridge n db 0 1 (b 0) z := by
        dsimp only [bridge]
        apply rsum_nonneg
        intro k hk
        exact Int.mul_nonneg (hnonneg 0 (b 0) k) (hnonneg 1 k z)
      have hb21 : 0 ≤ bridge n db 2 1 (b 0) z := by
        dsimp only [bridge]
        apply rsum_nonneg
        intro k hk
        exact Int.mul_nonneg (hnonneg 2 (b 0) k) (hnonneg 1 k z)
      have ht : 0 ≤ total n (db 0 z) := by
        dsimp only [total]
        apply rsum_nonneg
        intro k hk
        exact hnonneg 0 z k
      exact (prod_pos_iff_of_nonneg (Int.mul_nonneg hb01 hb21) ht).mp hp |>.2
  · rintro ⟨hz, h1, h2, h3⟩
    exact ⟨hz, Int.mul_pos (Int.mul_pos h1 h2) h3⟩

end TwoHopAntiWitness

/-! ChordComponents -/

private theorem rsum_prod_blocks (n : Nat) (A B : Nat → Nat → Int) (E : Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => rsum n (fun e =>
      A a b * B c d * E e))))) =
    (rsum n (fun a => rsum n (fun b => A a b))) *
      (rsum n (fun c => rsum n (fun d => B c d))) * rsum n E := by
  have h1 : ∀ a b c, rsum n (fun d => rsum n (fun e => A a b * B c d * E e)) =
      (A a b) * rsum n (fun d => B c d) * rsum n E := by
    intro a b c
    have h1a : ∀ d, rsum n (fun e => A a b * B c d * E e) = (A a b * B c d) * rsum n E := by
      intro d
      rw [show (fun e => A a b * B c d * E e) = (fun e => (A a b * B c d) * E e) by
        funext e; ac_rfl]
      exact rsum_const_mul n _ E
    rw [show (fun d => rsum n (fun e => A a b * B c d * E e)) =
        (fun d => (A a b * B c d) * rsum n E) by funext d; rw [h1a d]]
    rw [show (fun d => (A a b * B c d) * rsum n E) =
        (fun d => (A a b * rsum n E) * B c d) by funext d; ac_rfl]
    rw [rsum_const_mul]
    ac_rfl
  have hL1 : rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      rsum n (fun d => rsum n (fun e => A a b * B c d * E e))))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun c =>
        (A a b) * rsum n (fun d => B c d) * rsum n E))) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    apply rsum_congr; intro c hc
    exact h1 a b c
  rw [hL1]
  let K := (rsum n (fun c => rsum n (fun d => B c d))) * rsum n E
  have h2 : ∀ a b, rsum n (fun c => A a b * rsum n (fun d => B c d) * rsum n E) = A a b * K := by
    intro a b
    rw [show (fun c => A a b * rsum n (fun d => B c d) * rsum n E) =
        (fun c => A a b * (rsum n (fun d => B c d) * rsum n E)) by funext c; ac_rfl]
    rw [rsum_const_mul]
    rw [show (rsum n (fun c => rsum n (fun d => B c d) * rsum n E)) =
        rsum n (fun c => (rsum n E) * rsum n (fun d => B c d)) by
        apply rsum_congr; intro c hc; ac_rfl]
    rw [rsum_const_mul]
    dsimp only [K]
    ac_rfl
  have hL2 : rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      A a b * rsum n (fun d => B c d) * rsum n E))) =
      rsum n (fun a => rsum n (fun b => A a b * K)) := by
    apply rsum_congr; intro a ha
    apply rsum_congr; intro b hb
    exact h2 a b
  rw [hL2]
  have hmul : ∀ a, rsum n (fun b => A a b * K) = K * rsum n (fun b => A a b) := by
    intro a
    rw [show (fun b => A a b * K) = (fun b => K * A a b) by funext b; ac_rfl]
    exact rsum_const_mul n K (fun b => A a b)
  have hL3 : rsum n (fun a => rsum n (fun b => A a b * K)) =
      rsum n (fun a => K * rsum n (fun b => A a b)) := by
    apply rsum_congr
    intro a ha
    exact hmul a
  rw [hL3]
  rw [rsum_const_mul]
  dsimp only [K]
  ac_rfl

namespace HubWithChords
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

theorem ChordComponents_sound : ChordComponents.Goal := by
  intro n db b
  dsimp only
  let arm := fun (x a d e : Nat) => total n (fun i => db a x i * dot n (db e i) (db d x))
  change reference pattern n db b = total n (fun x => arm x 0 1 5 * arm x 2 3 6 * total n (db 4 x))
  simp [reference, pattern, value, bridge, dot, total, rsum, arm]
  rw [integrate_perm n (by native_decide : [1, 2, 5, 4, 3, 0].Perm [0, 1, 2, 3, 4, 5])]
  change rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => rsum n (fun x5 =>
        db 0 x0 x1 * (db 1 x0 x2 * (db 2 x0 x3 * (db 3 x0 x4 * (db 4 x0 x5 *
          (db 5 x1 x2 * db 6 x3 x4))))))))))) =
    rsum n (fun x0 =>
      (rsum n (fun i => db 0 x0 i * rsum n (fun j => db 5 i j * db 1 x0 j))) *
      (rsum n (fun i => db 2 x0 i * rsum n (fun j => db 6 i j * db 3 x0 j))) *
      rsum n (fun i => db 4 x0 i))
  apply rsum_congr
  intro x0 hx0
  have hraw : rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => rsum n (fun x5 =>
        db 0 x0 x1 * (db 1 x0 x2 * (db 2 x0 x3 * (db 3 x0 x4 * (db 4 x0 x5 *
          (db 5 x1 x2 * db 6 x3 x4)))))))))) =
      rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x5 =>
        (db 0 x0 x1 * db 5 x1 x2 * db 1 x0 x2) *
          (db 2 x0 x3 * db 6 x3 x4 * db 3 x0 x4) * db 4 x0 x5))))) := by
    apply rsum_congr; intro x1 h1
    apply rsum_congr; intro x2 h2
    apply rsum_congr; intro x3 h3
    apply rsum_congr; intro x4 h4
    apply rsum_congr; intro x5 h5
    ac_rfl
  rw [hraw]
  rw [rsum_prod_blocks]
  have hA : (rsum n fun a => rsum n fun b => db 0 x0 a * db 5 a b * db 1 x0 b) =
      (rsum n fun i => db 0 x0 i * rsum n fun j => db 5 i j * db 1 x0 j) := by
    apply rsum_congr
    intro a ha
    rw [show (fun b => db 0 x0 a * db 5 a b * db 1 x0 b) =
        (fun b => db 0 x0 a * (db 5 a b * db 1 x0 b)) by funext b; ac_rfl]
    rw [rsum_const_mul]
  have hB : (rsum n fun c => rsum n fun d => db 2 x0 c * db 6 c d * db 3 x0 d) =
      (rsum n fun i => db 2 x0 i * rsum n fun j => db 6 i j * db 3 x0 j) := by
    apply rsum_congr
    intro c hc
    rw [show (fun d => db 2 x0 c * db 6 c d * db 3 x0 d) =
        (fun d => db 2 x0 c * (db 6 c d * db 3 x0 d)) by funext d; ac_rfl]
    rw [rsum_const_mul]
  rw [hA, hB]

end HubWithChords

/-! TwoSeparators -/


/-! Unresolved goals (require substantially heavier combinatorial machinery). -/

namespace TwoHopAntiWitness
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end TwoHopAntiWitness

namespace HubWithChords
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end HubWithChords

namespace SupplierClique
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique
-- TODO(OrbitRepresentative)
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end SupplierClique

namespace IntersectingCycles
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end IntersectingCycles

namespace ParallelLineage
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage
-- TODO(DeltaForecast)
-- TODO(FractionalCap)
-- TODO(CollisionCorrection)
end ParallelLineage

end TinylambOpt.ResearchProofs.Atlas_JoinPlans_G4
