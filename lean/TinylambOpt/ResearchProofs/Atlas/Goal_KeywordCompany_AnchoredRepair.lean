import TinylambOpt.ResearchGoals.Atlas.JoinPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_KeywordCompany_AnchoredRepair

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.KeywordCompany

/-! Finite-sum and integration toolkit, used to prove the anchored repair identity
for this pattern without assuming anything about the databases. -/

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

private theorem sum_sub {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x - g x)).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]; omega

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

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum =
      (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l with
  | nil => simp [sum_zero]
  | cons a l ih => simp [ih, sum_add]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases h : i = v
  · subst i; simp [put]
  · simp [put, show (i == v) = false by simpa using h]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : v ≠ w) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hi : i = v
  · subst i
    simp [put, show (v == w) = false by simpa using h]
  · by_cases hj : i = w
    · subst i; simp [put, hi]
    · simp [put, show (i == v) = false by simpa using hi,
        show (i == w) = false by simpa using hj]

private theorem integrate_congr (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ x, f x = g x) : integrate n vs b f = integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) g)).sum
      exact sum_map_congr _ _ _ (fun x hx => ih (put b v x))

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases hvw : v = w
  · subst w; rfl
  change ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
    ((List.range n).map (fun x =>
      ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
  rw [sum_swap]
  apply sum_map_congr
  intro x hx
  apply sum_map_congr
  intro y hy
  rw [put_comm_ne b w v x y (fun h' => hvw h'.symm)]

private theorem integrate_perm (n : Nat) {vs vs' : List Nat} (h : vs.Perm vs') :
    ∀ (b : Bind) (f : Bind → Int), integrate n vs b f = integrate n vs' b f := by
  induction h with
  | nil => intro b f; rfl
  | cons x h ih =>
      intro b f
      exact sum_map_congr _ _ _ (fun t ht => ih (put b x t) f)
  | swap x y l => intro b f; exact integrate_swap n y x l b f
  | trans h1 h2 ih1 ih2 => intro b f; rw [ih1 b f, ih2 b f]

private theorem integrate_add (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x + g x) = integrate n vs b f + integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_add]

private theorem integrate_sub (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int) :
    integrate n vs b (fun x => f x - g x) = integrate n vs b f - integrate n vs b g := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_sub]

private theorem integrate_mul_left (n : Nat) (vs : List Nat) (b : Bind) (c : Int) (f : Bind → Int) :
    integrate n vs b (fun x => c * f x) = c * integrate n vs b f := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih => simp [integrate, ih, sum_mul_left]

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => rfl
  | cons v vs ih => simp [integrate, ih, sum_zero]

private theorem integrate_list_sum {α} (n : Nat) (vs : List Nat) (b : Bind)
    (l : List α) (F : α → Bind → Int) :
    integrate n vs b (fun x => (l.map (fun a => F a x)).sum) =
      (l.map (fun a => integrate n vs b (F a))).sum := by
  induction l with
  | nil => simp [integrate_zero]
  | cons a l ih => simp only [List.map_cons, List.sum_cons, integrate_add, ih]

private theorem integrate_fixed (n : Nat) (vs : List Nat) (b : Bind) (v : Nat)
    (hv : v ∉ vs) (F : Nat → Bind → Int) :
    integrate n vs b (fun x => F (x v) x) = integrate n vs b (fun x => F (b v) x) := by
  induction vs generalizing b with
  | nil => rfl
  | cons w ws ih =>
      have hvw : v ≠ w := by intro h; subst w; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      change ((List.range n).map (fun t => integrate n ws (put b w t) (fun x => F (x v) x))).sum =
        ((List.range n).map (fun t => integrate n ws (put b w t) (fun x => F (b v) x))).sum
      apply sum_map_congr
      intro t ht
      rw [ih (put b w t) hvws]
      rw [show (put b w t) v = b v by simp [put, show (v == w) = false by simpa using hvw]]

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

/-! Telescoping the product difference, then anchoring each changed atom. -/

private def cpExcl (atoms : List Atom) (old new : DB) (i : Nat) (b : Bind) : Int :=
  ((atoms.take i).map (fun a => value new a b)).prod *
    ((atoms.drop (i+1)).map (fun a => value old a b)).prod

private theorem cpExcl_zero (a : Atom) (as : List Atom) (old new : DB) (b : Bind) :
    cpExcl (a::as) old new 0 b = (as.map (fun t => value old t b)).prod := by simp [cpExcl]

private theorem cpExcl_succ (a : Atom) (as : List Atom) (old new : DB) (i : Nat) (b : Bind) :
    cpExcl (a::as) old new (i+1) b = value new a b * cpExcl as old new i b := by
  simp [cpExcl, Int.mul_assoc]

private theorem sum_zipIdx_succ {α} (as : List α) (f : α → Nat → Int) : ∀ k : Nat,
    ((as.zipIdx (k+1)).map (fun p => f p.1 p.2)).sum =
      ((as.zipIdx k).map (fun p => f p.1 (p.2+1))).sum := by
  induction as with
  | nil => intro k; rfl
  | cons a as ih =>
      intro k
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
      rw [ih (k+1)]

private theorem prod_diff_telescope (atoms : List Atom) (old new : DB) (b : Bind) :
    (atoms.map (fun a => value new a b)).prod - (atoms.map (fun a => value old a b)).prod =
      ((atoms.zipIdx.map (fun (a, i) =>
        (value new a b - value old a b) * cpExcl atoms old new i b)).sum) := by
  induction atoms with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.prod_cons, List.zipIdx_cons, Nat.zero_add]
      rw [cpExcl_zero a as old new b]
      change value new a b * (as.map (fun t => value new t b)).prod -
          value old a b * (as.map (fun t => value old t b)).prod =
        (value new a b - value old a b) * (as.map (fun t => value old t b)).prod +
          ((as.zipIdx 1).map (fun p =>
            (value new p.1 b - value old p.1 b) * cpExcl (a::as) old new p.2 b)).sum
      have hs := sum_zipIdx_succ as (fun t i =>
        (value new t b - value old t b) * cpExcl (a::as) old new i b) 0
      simp only [Nat.zero_add] at hs
      rw [hs]
      rw [show (as.zipIdx.map (fun p =>
            (value new p.1 b - value old p.1 b) * cpExcl (a::as) old new (p.2+1) b)) =
            (as.zipIdx.map (fun (t, i) =>
              value new a b * ((value new t b - value old t b) * cpExcl as old new i b))) by
        apply List.map_congr_left
        intro x hx
        rcases x with ⟨t, i⟩
        dsimp only
        rw [cpExcl_succ]
        ac_rfl]
      rw [sum_mul_left, ← ih, Int.sub_mul, Int.mul_sub]
      omega

private theorem filter_ne_eq_erase (vs : List Nat) (a : Nat) (hnd : vs.Nodup) :
    vs.filter (fun v => v != a) = vs.erase a := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      by_cases hxa : x = a
      · subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        simp [List.erase_cons_head, hna, ih hp.2]
      · rw [List.erase_cons_tail (a := a) (b := x) (by simp [hxa])]
        rw [List.filter_cons, if_pos (by simp [hxa]), ih hp.2]

private theorem erase_erase_filter (vs : List Nat) (a b : Nat) (hnd : vs.Nodup) :
    (vs.erase a).erase b = vs.filter (fun v => v != a && v != b) := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      have hp := List.pairwise_cons.mp hnd
      by_cases hxa : x = a
      · subst x
        have hna : a ∉ xs := by intro h; exact hp.1 a h rfl
        rw [List.erase_cons_head]
        change xs.erase b = (a::xs).filter (fun v => v != a && v != b)
        rw [List.filter_cons, if_neg (by simp [show (a != a) = false by simp])]
        have hf : xs.filter (fun v => v != a && v != b) = xs.filter (fun v => v != b) := by
          apply List.filter_congr
          intro v hv
          have hva : v ≠ a := by intro h; subst v; exact hna hv
          simp [hva]
        rw [hf, ← filter_ne_eq_erase xs b hp.2]
      · by_cases hxb : x = b
        · subst x
          have hnb : b ∉ xs := by intro h; exact hp.1 b h rfl
          have hbne : b ≠ a := by simpa using hxa
          have hnb' : b ∉ xs.erase a := fun h =>
            hnb ((List.mem_erase_of_ne (a := b) (b := a) hbne).mp h)
          have hih := ih hp.2
          rw [List.erase_of_not_mem hnb'] at hih
          rw [List.erase_cons_tail (a := a) (b := b) (by simp [hxa]),
            List.erase_cons_head (a := b)]
          change xs.erase a = (b::xs).filter (fun v => v != a && v != b)
          rw [List.filter_cons, if_neg (by simp [show (b != b) = false by simp])]
          exact hih
        · rw [List.erase_cons_tail (a := a) (b := x) (by simp [hxa]),
            List.erase_cons_tail (a := b) (b := x) (by simp [hxb])]
          simp [show (x != a) = true by simpa using hxa,
            show (x != b) = true by simpa using hxb, ih hp.2]

private theorem filter_and_ne (vs : List Nat) (a b : Nat) :
    (vs.filter (fun v => v != a)).filter (fun v => v != b) =
      vs.filter (fun v => v != a && v != b) := by
  induction vs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x; simp [ih]
      · by_cases hxb : x = b
        · subst x; simp [ih, Bool.and_comm]
        · simp [show (x != a) = true by simpa using hxa,
            show (x != b) = true by simpa using hxb, ih]

private theorem integrate_pivot (n : Nat) (vs : List Nat) (src dst : Nat) (b : Bind) (f : Bind → Int)
    (hnd : vs.Nodup) (hne : src ≠ dst) :
    integrate n vs b f =
      let cand := fun v => if vs.contains v then List.range n else [b v]
      let vars := vs.filter (fun v => v != src && v != dst)
      ((cand src).map (fun u => ((cand dst).map (fun w =>
        integrate n vars (put (put b src u) dst w) f)).sum)).sum := by
  dsimp only
  by_cases hsrc : vs.contains src = true
  · have hsm : src ∈ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    · have hdm : dst ∈ vs := by simpa using hdst
      have hde : dst ∈ vs.erase src :=
        (List.mem_erase_of_ne (a := dst) (b := src) hne.symm).mpr hdm
      rw [if_pos hsrc, if_pos hdst]
      change integrate n vs b f =
        ((List.range n).map (fun u => ((List.range n).map (fun w =>
          integrate n (vs.filter (fun v => v != src && v != dst))
            (put (put b src u) dst w) f)).sum)).sum
      rw [integrate_perm n (List.perm_cons_erase hsm) b f]
      apply sum_map_congr
      intro u hu
      rw [integrate_perm n (List.perm_cons_erase hde) (put b src u) f]
      change ((List.range n).map (fun w => integrate n ((vs.erase src).erase dst)
          (put (put b src u) dst w) f)).sum = _
      rw [erase_erase_filter vs src dst hnd]
    · have hdm : dst ∉ vs := by simpa using hdst
      rw [if_pos hsrc, if_neg hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      rw [integrate_perm n (List.perm_cons_erase hsm) b f]
      apply sum_map_congr
      intro u hu
      have hf : vs.filter (fun v => v != src && v != dst) = vs.erase src := by
        rw [← filter_and_ne, filter_ne_eq_erase vs src hnd]
        apply List.filter_eq_self.mpr
        intro v hv
        have hv' := List.mem_of_mem_erase hv
        have hvd : v ≠ dst := by intro h; subst v; exact hdm hv'
        simp [hvd]
      rw [hf]
      change integrate n (vs.erase src) (put b src u) f =
        integrate n (vs.erase src) (put (put b src u) dst (b dst)) f
      rw [show put (put b src u) dst (b dst) = put b src u by
        funext i; by_cases hi : i = dst <;> simp [put, hi, hne.symm]]
  · have hsm : src ∉ vs := by simpa using hsrc
    by_cases hdst : vs.contains dst = true
    · have hdm : dst ∈ vs := by simpa using hdst
      rw [if_neg hsrc, if_pos hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      rw [integrate_perm n (List.perm_cons_erase hdm) b f]
      apply sum_map_congr
      intro w hw
      have hf : vs.filter (fun v => v != src && v != dst) = vs.erase dst := by
        have hfs : vs.filter (fun v => v != src) = vs := by
          apply List.filter_eq_self.mpr
          intro v hv
          have hvs : v ≠ src := by intro h; subst v; exact hsm hv
          simp [hvs]
        rw [← filter_and_ne, hfs, filter_ne_eq_erase vs dst hnd]
      rw [hf]
      change integrate n (vs.erase dst) (put b dst w) f =
        integrate n (vs.erase dst) (put (put b src (b src)) dst w) f
      rw [put_same]
    · have hdm : dst ∉ vs := by simpa using hdst
      rw [if_neg hsrc, if_neg hdst]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
      have hf : vs.filter (fun v => v != src && v != dst) = vs := by
        apply List.filter_eq_self.mpr
        intro v hv
        have hvs : v ≠ src := by intro h; subst v; exact hsm hv
        have hvd : v ≠ dst := by intro h; subst v; exact hdm hv
        simp [hvs, hvd]
      rw [hf, put_same, put_same]

private theorem pivot_point (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hsd : a.src ≠ a.dst) (u w : Nat) :
    let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
    let b' := put (put b a.src u) a.dst w
    integrate n vars b' (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x) =
      (new a.rel u w - old a.rel u w) *
        integrate n vars b' (fun x => cpExcl p.atoms old new j x) := by
  dsimp only
  have hs : a.src ∉ p.freeVars.filter (fun v => v != a.src && v != a.dst) := by simp
  have hd : a.dst ∉ p.freeVars.filter (fun v => v != a.src && v != a.dst) := by simp
  have hsb : put (put b a.src u) a.dst w a.src = u := by
    simp [put, show (a.src == a.dst) = false by simpa using hsd]
  have hdb : put (put b a.src u) a.dst w a.dst = w := by simp [put]
  change integrate n (p.freeVars.filter (fun v => v != a.src && v != a.dst))
      (put (put b a.src u) a.dst w)
      (fun x => (new a.rel (x a.src) (x a.dst) - old a.rel (x a.src) (x a.dst)) *
        cpExcl p.atoms old new j x) = _
  rw [integrate_fixed2 n _ _ a.src a.dst hs hd
    (fun u v x => (new a.rel u v - old a.rel u v) * cpExcl p.atoms old new j x), hsb, hdb]
  exact integrate_mul_left _ _ _ _ _

private theorem sum_filter_eq {α} (l : List α) (p : α → Bool) (g : α → Int)
    (h : ∀ x ∈ l, p x = false → g x = 0) :
    ((l.filter p).map g).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : p x = true
      · simp [hx, ih (fun y hy hp => h y (by simp [hy]) hp)]
      · have hf : p x = false := by cases hp : p x <;> simp_all
        have hz : g x = 0 := h x (by simp) hf
        simp [hx, hz, ih (fun y hy hp => h y (by simp [hy]) hp)]

private theorem pivot_integral_eq (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (j : Nat) (a : Atom) (hnd : p.freeVars.Nodup) (hsd : a.src ≠ a.dst) :
    integrate n p.freeVars b (fun x =>
      (value new a x - value old a x) * cpExcl p.atoms old new j x) = pivotTerm p n old new b j a := by
  rw [integrate_pivot n p.freeVars a.src a.dst b _ hnd hsd]
  dsimp only
  apply sum_map_congr
  intro u hu
  rw [← sum_filter_eq (if p.freeVars.contains a.dst then List.range n else [b a.dst])
    (fun w => old a.rel u w != new a.rel u w)
    (fun w => integrate n (p.freeVars.filter (fun v => v != a.src && v != a.dst))
      (put (put b a.src u) a.dst w)
      (fun x => (value new a x - value old a x) * cpExcl p.atoms old new j x))
    (by
      intro w hw hne
      have heq : old a.rel u w = new a.rel u w := by
        by_cases h : old a.rel u w == new a.rel u w
        · exact beq_iff_eq.mp h
        · simpa [h] using hne
      rw [pivot_point p n old new b j a hsd u w]
      simp [heq])]
  apply sum_map_congr
  intro w hw
  exact pivot_point p n old new b j a hsd u w

private theorem anchored_repair_generic (p : Pattern) (n : Nat) (old new : DB) (b : Bind)
    (hnd : p.freeVars.Nodup) (hsd : ∀ a ∈ p.atoms, a.src ≠ a.dst) :
    reference p n new b - reference p n old b =
      (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
  calc
    reference p n new b - reference p n old b
        = integrate n p.freeVars b (fun x =>
            (p.atoms.map (fun a => value new a x)).prod -
              (p.atoms.map (fun a => value old a x)).prod) := by
              rw [integrate_sub]; rfl
    _ = integrate n p.freeVars b (fun x => (p.atoms.zipIdx.map (fun (a, i) =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x)).sum) := by
            apply integrate_congr
            intro x
            exact prod_diff_telescope p.atoms old new x
    _ = (p.atoms.zipIdx.map (fun (a, i) => integrate n p.freeVars b (fun x =>
          (value new a x - value old a x) * cpExcl p.atoms old new i x))).sum := by
            rw [integrate_list_sum]
    _ = (p.atoms.zipIdx.map (fun (a, i) => pivotTerm p n old new b i a)).sum := by
          apply sum_map_congr
          intro ai hai
          rcases ai with ⟨a, i⟩
          have hz := List.mem_zipIdx' hai
          have ham : a ∈ p.atoms := by
            rw [hz.2]
            exact List.getElem_mem hz.1
          exact pivot_integral_eq p n old new b i a hnd (hsd a ham)

theorem sound_KeywordCompany_AnchoredRepair : KeywordCompany.AnchoredRepair.Goal := by
  intro n old new b
  exact anchored_repair_generic pattern n old new b (by native_decide) (by native_decide)

end TinylambOpt.ResearchProofs.Atlas.Goal_KeywordCompany_AnchoredRepair
