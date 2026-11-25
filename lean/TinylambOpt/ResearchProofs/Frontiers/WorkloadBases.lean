import TinylambOpt.ResearchGoals.Frontiers.WorkloadBases

namespace TinylambOpt.ResearchProofs.Frontiers.WorkloadBases

noncomputable section

open TinylambOpt.ResearchGoals.Frontiers.Contractions (Vec Mat total dot)
open TinylambOpt.ResearchGoals.Frontiers.WorkloadBases

set_option maxHeartbeats 0
local instance propDecidable (p : Prop) : Decidable p := Classical.propDecidable p

/-! ### Shared list-sum algebra -/

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun x => f x + g x).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons]; rw [ih]; omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun x => f x - g x).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons]; rw [ih]; omega

private theorem sum_map_mul_left {α : Type} (xs : List α) (c : Int) (f : α → Int) :
    (xs.map fun x => c * f x).sum = c * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      grind

private theorem sum_map_mul_right {α : Type} (xs : List α) (f : α → Int) (c : Int) :
    (xs.map fun x => f x * c).sum = (xs.map f).sum * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      grind

private theorem sum_map_neg {α : Type} (xs : List α) (f : α → Int) :
    (xs.map fun x => - f x).sum = - (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; omega

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

private theorem sum_map_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp)]
      rw [ih (by intro y hy; exact h y (by simp [hy]))]

private theorem sum_map_zero {α : Type} (xs : List α) : (xs.map fun _ => (0 : Int)).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem sum_map_eq_zero {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  rw [sum_map_congr xs f (fun _ => 0) h]
  exact sum_map_zero xs

private theorem sum_swap {α β : Type} (xs : List α) (ys : List β) (f : α → β → Int) :
    (xs.map fun x => (ys.map fun y => f x y).sum).sum =
      (ys.map fun y => (xs.map fun x => f x y).sum).sum := by
  induction xs with
  | nil => simp [sum_map_zero]
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [sum_map_add, ih]

private theorem total_succ (n : Nat) (v : Vec) : total (n+1) v = total n v + v n := by
  simp [total, List.range_succ]

private theorem total_add (n : Nat) (v w : Vec) :
    total n (fun j => v j + w j) = total n v + total n w := by
  exact sum_map_add (List.range n) v w

private theorem total_sub (n : Nat) (v w : Vec) :
    total n (fun j => v j - w j) = total n v - total n w := by
  exact sum_map_sub (List.range n) v w

private theorem total_mul_left (n : Nat) (c : Int) (v : Vec) :
    total n (fun j => c * v j) = c * total n v := by
  exact sum_map_mul_left (List.range n) c v

private theorem total_mul_right (n : Nat) (v : Vec) (c : Int) :
    total n (fun j => v j * c) = total n v * c := by
  exact sum_map_mul_right (List.range n) v c

private theorem total_congr (n : Nat) (v w : Vec) (h : ∀ j < n, v j = w j) :
    total n v = total n w := by
  apply sum_map_congr (List.range n)
  intro j hj
  exact h j (List.mem_range.mp hj)

private theorem dot_congr (n : Nat) (a v w : Vec) (h : ∀ j < n, v j = w j) :
    dot n a v = dot n a w := by
  apply total_congr
  intro j hj
  rw [h j hj]

private theorem dot_expand (n : Nat) (a b : Vec) :
    dot n a b = total n (fun j => a j * b j) := rfl

private theorem dot_eq_sum_swap (n k : Nat) (row coeff : Vec) (basis : Mat) :
    dot n row (fun j => dot k coeff (fun h => basis h j)) =
      dot k coeff (fun h => dot n row (basis h)) := by
  unfold dot total
  change ((List.range n).map fun j =>
      row j * ((List.range k).map fun h => coeff h * basis h j).sum).sum =
    ((List.range k).map fun h =>
      coeff h * ((List.range n).map fun j => row j * basis h j).sum).sum
  rw [show ((List.range n).map fun j =>
      row j * ((List.range k).map fun h => coeff h * basis h j).sum).sum =
      ((List.range n).map fun j =>
      ((List.range k).map fun h => row j * (coeff h * basis h j)).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro j hj
        rw [sum_map_mul_left]]
  rw [sum_swap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro h hh
  rw [show (List.map (fun x => row x * (coeff h * basis h x)) (List.range n)).sum =
      (List.map (fun x => coeff h * (row x * basis h x)) (List.range n)).sum by
        apply sum_map_congr
        intro j hj
        grind]
  rw [sum_map_mul_left]

/-! ### FanoutOnly: false -/

theorem FanoutOnly_disproof :
    ¬ TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.FanoutOnly.Goal := by
  intro h
  have hh := h 2 (fun _ => 1) (fun j => if j = 0 then (5 : Int) else 8)
      (fun _ => (1 : Int)) (fun _ => 0) (by
        intro j hj
        constructor
        · omega
        · rfl)
  have hleft : dot 2 (fun _ => (1 : Int)) (fun j => if j = 0 then (5 : Int) else 8) = 13 := by
    native_decide
  have hright : dot 2 (fun _ => (1 : Int)) (fun _ => if (0 : Nat) = 0 then (5 : Int) else 8) = 10 := by
    native_decide
  rw [hleft, hright] at hh
  omega

/-! ### QueryBasis -/

theorem QueryBasis_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.QueryBasis.Goal := by
  intro n k row coeff basis
  exact dot_eq_sum_swap n k row coeff basis

/-! ### Indistinguishable -/

private theorem dot_indicator (k h : Nat) (v : Vec) (hh : h < k) :
    dot k (fun h' => if h' == h then (1 : Int) else 0) v = v h := by
  unfold dot total
  induction k generalizing h with
  | zero => omega
  | succ k ih =>
      rw [List.range_succ]
      simp only [List.map_append, List.sum_append, List.map_singleton, List.sum_singleton]
      by_cases hk : h == k
      · have heq : h = k := beq_iff_eq.mp hk
        subst k
        have hne : ∀ j ∈ List.range h, (j == h) = false := by
          intro j hj
          apply Bool.eq_false_of_ne_true
          intro htrue
          have hjeq : j = h := beq_iff_eq.mp htrue
          have := List.mem_range.mp hj
          omega
        have hz : ((List.range h).map fun j => (if j == h then (1 : Int) else 0) * v j).sum = 0 := by
          apply sum_map_eq_zero
          intro j hj
          simp [hne j hj]
        rw [hz]
        simp
      · have hne : h ≠ k := by
          intro heq
          subst k
          exact hk (by simp)
        have hlt : h < k := by omega
        have hk' : (k == h) = false := by
          rw [beq_comm]
          exact Bool.eq_false_of_ne_true hk
        rw [ih h hlt]
        simp [hk']

private theorem sum_map_erase (dirty : List Nat) (d : Nat) (f : Nat → Int)
    (hd : d ∈ dirty) (hnd : dirty.Nodup) :
    (dirty.map f).sum = f d + ((dirty.erase d).map f).sum := by
  induction dirty with
  | nil => simp at hd
  | cons x xs ih =>
      by_cases hxd : x = d
      · subst d
        rw [List.erase_cons_head]
        simp
      · have htail : d ∈ xs := by
          rw [List.mem_cons] at hd
          rcases hd with rfl | h
          · exact (hxd rfl).elim
          · exact h
        have hndtail : xs.Nodup := (List.nodup_cons.mp hnd).2
        have hxne : ¬(x == d) = true := by
          intro hb
          exact hxd (beq_iff_eq.mp hb)
        rw [List.erase_cons_tail hxne]
        simp only [List.map_cons, List.sum_cons]
        rw [ih htail hndtail]
        omega

private theorem not_mem_erase_self (dirty : List Nat) (d : Nat) (hnd : dirty.Nodup) :
    d ∉ dirty.erase d := by
  induction dirty with
  | nil => simp
  | cons x xs ih =>
      by_cases hxd : x = d
      · subst x
        rw [List.erase_cons_head]
        exact (List.nodup_cons.mp hnd).1
      · have hxne : ¬(x == d) = true := by
          intro hb
          exact hxd (beq_iff_eq.mp hb)
        rw [List.erase_cons_tail hxne]
        intro hmem
        rw [List.mem_cons] at hmem
        rcases hmem with h | h
        · have hd_eq_x : d = x := beq_iff_eq.mp (by simpa using h)
          exact hxd hd_eq_x.symm
        · exact ih ((List.nodup_cons.mp hnd).2) h

private theorem total_eq_sum_nodup (n : Nat) (f : Nat → Int) (dirty : List Nat)
    (hnd : dirty.Nodup) (hbd : ∀ j ∈ dirty, j < n)
    (hzero : ∀ j < n, j ∉ dirty → f j = 0) :
    total n f = (dirty.map f).sum := by
  induction n generalizing dirty with
  | zero =>
      have hdempty : dirty = [] := by
        cases dirty with
        | nil => rfl
        | cons d ds =>
            have := hbd d (by simp)
            omega
      subst dirty
      simp [total]
  | succ n ih =>
      by_cases hn : n ∈ dirty
      · let ds := dirty.erase n
        have hnds : ds.Nodup := (hnd.erase n)
        have hbds : ∀ j ∈ ds, j < n := by
          intro j hj
          have hjd : j ∈ dirty := List.mem_of_mem_erase hj
          have hjn : j < n + 1 := hbd j hjd
          have hjne : j ≠ n := by
            intro h
            subst j
            exact not_mem_erase_self dirty n hnd hj
          omega
        have hzero' : ∀ j < n, j ∉ ds → f j = 0 := by
          intro j hj hjnot
          apply hzero j (by omega)
          intro hjd
          apply hjnot
          have hjne : j ≠ n := by omega
          exact (List.mem_erase_of_ne hjne).2 hjd
        rw [total_succ, ih ds hnds hbds hzero', sum_map_erase dirty n f hn hnd]
        dsimp [ds]
        omega
      · have hfn : f n = 0 := hzero n (by omega) hn
        have hbd' : ∀ j ∈ dirty, j < n := by
          intro j hj
          have : j < n + 1 := hbd j hj
          have hjne : j ≠ n := by
            intro h
            subst j
            exact hn hj
          omega
        have hzero' : ∀ j < n, j ∉ dirty → f j = 0 := by
          intro j hj hjnot
          exact hzero j (by omega) hjnot
        rw [total_succ, ih dirty hnd hbd' hzero', hfn]
        omega

private theorem sum_filter_partition_int {α : Type} (xs : List α) (p : α → Bool) (f : α → Int) :
    ((xs.filter p).map f).sum + ((xs.filter fun x => !p x).map f).sum = (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · simp [hp]
        omega
      · simp [hp]
        omega

private theorem map_filter_map {α β : Type} (xs : List α) (f : α → β) (p : β → Bool) :
    (xs.map f).filter p = (xs.filter fun x => p (f x)).map f := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hp : p (f x) = true
      · simp [hp, ih]
      · simp [hp, ih]

private theorem filter_filter_comm {α : Type} (p q : α → Bool) (xs : List α) :
    (xs.filter p).filter q = (xs.filter q).filter p := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true <;> by_cases hq : q x = true <;> simp [hp, hq, ih]

private theorem filter_filter_eq_filter {α : Type} (p q : α → Bool) (xs : List α)
    (himp : ∀ x ∈ xs, p x = true → q x = true) :
    (xs.filter p).filter q = xs.filter p := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hq : q x = true := himp x (by simp) hp
        simp [hp, hq, ih (by intro y hy; exact himp y (by simp [hy]))]
      · simp [hp, ih (by intro y hy; exact himp y (by simp [hy]))]

private theorem filter_filter_ne (rest : List Nat) (weight : Nat → Int) (w c : Int)
    (hcne : c ≠ w) :
    ((rest.filter fun x => !(weight x == w)).filter fun x => weight x == c) =
      rest.filter (fun x => weight x == c) := by
  rw [filter_filter_comm]
  apply filter_filter_eq_filter
  intro x hx hc
  have hxc : weight x = c := beq_iff_eq.mp hc
  have hwfalse : (weight x == w) = false := Bool.eq_false_of_ne_true (by
    intro hw
    have hxw : weight x = w := beq_iff_eq.mp hw
    exact hcne (by rw [← hxc, hxw]))
  simp [hwfalse]

private def classSumOf (keys : List Nat) (weight : Nat → Int) (value : Nat → Int) (c : Int) : Int :=
  ((keys.filter fun j => weight j == c).map value).sum

private theorem classDot_eq_sum (keys : List Nat) (weight : Nat → Int) (value : Nat → Int) :
    ((keys.map weight).eraseDups.map fun c => c * classSumOf keys weight value c).sum =
      (keys.map fun j => weight j * value j).sum := by
  let motive : Nat → Prop := fun n => ∀ (keys : List Nat), keys.length = n →
    ((keys.map weight).eraseDups.map fun c => c * classSumOf keys weight value c).sum =
      (keys.map fun j => weight j * value j).sum
  have hmain : ∀ n, motive n := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro keys hlen
        cases keys with
      | nil => simp [classSumOf]
      | cons j rest =>
          let w := weight j
          let restNo := rest.filter fun x => weight x == w
          let restYes := rest.filter fun x => !(weight x == w)
          let C0 := ((rest.map weight).filter fun b => !b == w).eraseDups
          have hC0mem : ∀ c ∈ C0, c ≠ w := by
            intro c hc
            dsimp [C0] at hc
            have hcf : c ∈ (rest.map weight).filter fun b => !b == w := (List.mem_eraseDups.mp hc)
            have hp := (List.mem_filter.mp hcf).2
            intro hcw
            subst c
            simp at hp
          have hsumC0 : (C0.map fun c => c * classSumOf rest weight value c).sum =
              (restYes.map fun x => weight x * value x).sum := by
            have hih := ih restYes.length (by
              dsimp [restYes]
              have hle := List.length_filter_le (p := fun x => !(weight x == w)) rest
              have hlen' : rest.length + 1 = n := by simpa using hlen
              omega) restYes rfl
            have hC0eq : C0 = (restYes.map weight).eraseDups := by
              dsimp [C0, restYes]
              rw [map_filter_map]
            rw [hC0eq]
            rw [← hih]
            apply sum_map_congr (restYes.map weight).eraseDups
              (fun c => c * classSumOf rest weight value c)
              (fun c => c * classSumOf restYes weight value c)
            intro c hc
            apply congrArg (fun z => c * z)
            dsimp [classSumOf]
            have hcne : c ≠ w := hC0mem c (by simpa [hC0eq] using hc)
            have hfilt : (restYes.filter fun x => weight x == c) =
                (rest.filter fun x => weight x == c) := by
              dsimp [restYes]
              exact filter_filter_ne rest weight w c hcne
            rw [hfilt]
          have hsumW : (w * classSumOf rest weight value w) =
              (restNo.map fun x => weight x * value x).sum := by
            dsimp [classSumOf]
            rw [← sum_map_mul_left]
            apply sum_map_congr (rest.filter fun x => weight x == w)
              (fun x => w * value x) (fun x => weight x * value x)
            intro x hx
            have hxw : weight x = w := beq_iff_eq.mp (List.mem_filter.mp hx).2
            rw [hxw]
          have hpart := sum_filter_partition_int rest (fun x => weight x == w)
            (fun x => weight x * value x)
          rw [show ((j :: rest).map weight).eraseDups =
              w :: C0 by
            dsimp [w, C0]
            simp only [List.map_cons]
            rw [List.eraseDups_cons]]
          simp only [List.map_cons, List.sum_cons]
          rw [show classSumOf (j :: rest) weight value w = value j + classSumOf rest weight value w by
            dsimp [classSumOf]
            simp [w]]
          rw [sum_map_congr C0 (fun c => c * classSumOf (j :: rest) weight value c)
            (fun c => c * classSumOf rest weight value c) (by
              intro c hc
              rw [show classSumOf (j :: rest) weight value c = classSumOf rest weight value c by
                dsimp [classSumOf, w]
                have hcne : c ≠ w := hC0mem c hc
                have hwc : (weight j == c) = false :=
                  Bool.eq_false_of_ne_true (by
                    intro hb
                    have hcw : c = w := beq_iff_eq.mp (by rw [beq_comm]; exact hb)
                    exact hcne hcw)
                simp [hwc]])]
          rw [Int.mul_add, hsumC0, hsumW]
          dsimp [restNo, restYes]
          rw [← hpart]
          dsimp [w]
          omega
  exact hmain keys.length keys rfl

theorem SeparateMeasures_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.SeparateMeasures.Goal := by
  intro keys lc ls rc rs
  apply Prod.ext
  · have h := classDot_eq_sum keys rc ls
    dsimp [classDot, classSumOf] at h ⊢
    rw [h]
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hj
    grind
  · have h := classDot_eq_sum keys rs lc
    dsimp [classDot, classSumOf] at h ⊢
    rw [h]
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hj
    grind

theorem DirtyWeights_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.DirtyWeights.Goal := by
  intro n row old new dirty hnd hbd hchange
  let f : Nat → Int := fun j => row j * (new j - old j)
  have hsum : total n f = (dirty.map f).sum := by
    apply total_eq_sum_nodup n f dirty hnd hbd
    intro j hj hjnot
    have holdnew : old j = new j := by
      by_cases heq : old j = new j
      · exact heq
      · exact False.elim (hjnot (hchange j hj heq))
    change row j * (new j - old j) = 0
    rw [← holdnew]
    simp
  unfold dot
  change total n (fun j => row j * new j) = total n (fun j => row j * old j) + (dirty.map f).sum
  rw [← hsum]
  rw [← total_add]
  apply congrArg List.sum
  apply List.map_congr_left
  intro j hj
  dsimp [f]
  grind

theorem Indistinguishable_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.Indistinguishable.Goal := by
  intro n k a b basis
  constructor
  · intro hbasis c
    rw [dot_eq_sum_swap n k a c basis, dot_eq_sum_swap n k b c basis]
    apply total_congr
    intro hidx hh
    change c hidx * dot n a (basis hidx) = c hidx * dot n b (basis hidx)
    rw [hbasis hidx hh]
  · intro hall hidx hh
    let c : Vec := fun h' => if h' == hidx then (1 : Int) else 0
    have hc := hall c
    rw [dot_eq_sum_swap n k a c basis, dot_eq_sum_swap n k b c basis] at hc
    change (dot k c (fun h => dot n a (basis h))) = dot k c (fun h => dot n b (basis h)) at hc
    change (dot k (fun h' => if h' == hidx then (1 : Int) else 0) (fun h => dot n a (basis h))) =
      dot k (fun h' => if h' == hidx then (1 : Int) else 0) (fun h => dot n b (basis h)) at hc
    rw [dot_indicator k hidx (fun h => dot n a (basis h)) hh,
      dot_indicator k hidx (fun h => dot n b (basis h)) hh] at hc
    exact hc

private theorem int_pow_two_eq (bits : Nat) : ((2 : Int) ^ bits) = ((2 ^ bits : Nat) : Int) := by
  induction bits with
  | zero => simp
  | succ bits ih =>
      rw [Int.pow_succ, Nat.pow_succ, ih]
      grind

private theorem bits_sum_int (x bits : Nat) (hx : x < 2 ^ bits) :
    ((List.range bits).map fun b => if x.testBit b then ((2 ^ b : Nat) : Int) else 0).sum = (x : Int) := by
  induction bits generalizing x with
  | zero =>
      have hx0 : x = 0 := by omega
      subst x
      simp
  | succ bits ih =>
      have hxhalf : x / 2 < 2 ^ bits := by
        rw [Nat.div_lt_iff_lt_mul (by omega : 0 < 2)]
        simpa [Nat.pow_succ] using hx
      rw [List.range_succ_eq_map]
      simp only [List.map_cons, List.map_map, List.sum_cons]
      have hfirst : (if x.testBit 0 then ((2 ^ 0 : Nat) : Int) else 0) = ((x % 2 : Nat) : Int) := by
        rw [Nat.testBit_zero]
        have hm : x % 2 < 2 := Nat.mod_lt x (by omega)
        by_cases h : x % 2 = 1
        · simp [h]
        · have h0 : x % 2 = 0 := by omega
          simp [h0]
      rw [hfirst]
      have htail : ((List.range bits).map fun b =>
          if x.testBit (b + 1) then ((2 ^ (b + 1) : Nat) : Int) else 0).sum =
          2 * ((List.range bits).map fun b =>
            if (x / 2).testBit b then ((2 ^ b : Nat) : Int) else 0).sum := by
        rw [← sum_map_mul_left]
        apply sum_map_congr
        intro b hb
        rw [Nat.testBit_add_one]
        by_cases hb2 : (x / 2).testBit b
        · simp [hb2, Nat.pow_succ]
          omega
        · simp [hb2]
      change ((x % 2 : Nat) : Int) +
          ((List.range bits).map fun b =>
            if x.testBit (b + 1) then ((2 ^ (b + 1) : Nat) : Int) else 0).sum = (x : Int)
      rw [htail, ih (x / 2) hxhalf]
      have hmod := Nat.mod_add_div x 2
      omega

theorem NarrowWeights_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.NarrowWeights.Goal := by
  intro n bits lo row weight hrange
  let x : Nat → Nat := fun j => (weight j - lo).toNat
  have hxdef : ∀ j < n, ((x j : Nat) : Int) = weight j - lo := by
    intro j hj
    dsimp [x]
    exact Int.toNat_of_nonneg (by have := hrange j hj; omega)
  have hxlt : ∀ j < n, x j < 2 ^ bits := by
    intro j hj
    have h1 : ((x j : Nat) : Int) < ((2 ^ bits : Nat) : Int) := by
      rw [hxdef j hj]
      have hlt := (hrange j hj).2
      rw [int_pow_two_eq bits] at hlt
      omega
    omega
  have hweq : ∀ j < n, weight j = lo + (x j : Int) := by
    intro j hj
    rw [hxdef j hj]
    omega
  unfold dot
  change ((List.range n).map fun j => row j * weight j).sum =
    lo * ((List.range n).map row).sum +
      ((List.range bits).map fun b => ((2 ^ b : Nat) : Int) *
        ((List.range n).map fun j => if (x j).testBit b then row j else 0).sum).sum
  rw [show ((List.range n).map fun j => row j * weight j).sum =
      ((List.range n).map fun j => row j * (lo + (x j : Int))).sum by
        apply sum_map_congr
        intro j hj
        rw [hweq j (List.mem_range.mp hj)]]
  rw [show ((List.range n).map fun j => row j * (lo + (x j : Int))).sum =
      ((List.range n).map fun j => lo * row j + row j * (x j : Int)).sum by
        apply sum_map_congr
        intro j hj
        grind]
  rw [sum_map_add, sum_map_mul_left]
  congr 1
  rw [show ((List.range n).map fun j => row j * (x j : Int)).sum =
      ((List.range n).map fun j =>
        ((List.range bits).map fun b => row j *
          (if (x j).testBit b then ((2 ^ b : Nat) : Int) else 0)).sum).sum by
        apply sum_map_congr
        intro j hj
        rw [sum_map_mul_left]
        rw [bits_sum_int (x j) bits (hxlt j (List.mem_range.mp hj))]]
  rw [sum_swap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro b hb
  rw [← sum_map_mul_left]
  apply congrArg List.sum
  apply List.map_congr_left
  intro j hj
  by_cases hb2 : (x j).testBit b <;> simp [hb2] <;> grind

private theorem sum_mul_sum {α β : Type} (xs : List α) (ys : List β)
    (f : α → Int) (g : β → Int) :
    (xs.map f).sum * (ys.map g).sum =
      (xs.map fun x => (ys.map fun y => f x * g y).sum).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, Int.add_mul, ih]
      rw [← sum_map_mul_left]

private theorem sq_nonneg_int (a : Int) : 0 ≤ a * a := by
  have h := Int.natAbs_mul_self (a := a)
  have h0 : (0 : Int) ≤ ((a.natAbs * a.natAbs : Nat) : Int) := by omega
  rw [h] at h0
  exact h0

private theorem sum_map_nonneg {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ x ∈ xs, 0 ≤ f x) : 0 ≤ (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht := ih (by intro y hy; exact h y (by simp [hy]))
      omega

private theorem cauchy_double (n : Nat) (r d : Vec) :
    2 * (dot n r r * total n (fun j => d j * d j) - (dot n r d) * (dot n r d)) =
      ((List.range n).map fun i =>
        ((List.range n).map fun j => (r i * d j - r j * d i) * (r i * d j - r j * d i)).sum).sum := by
  have hexpand : ∀ i j : Nat,
      (r i * d j - r j * d i) * (r i * d j - r j * d i) =
        (r i * r i) * (d j * d j) + (r j * r j) * (d i * d i) -
          2 * ((r i * d i) * (r j * d j)) := by
    intro i j
    grind
  rw [show ((List.range n).map fun i =>
        ((List.range n).map fun j => (r i * d j - r j * d i) * (r i * d j - r j * d i)).sum).sum =
      ((List.range n).map fun i =>
        ((List.range n).map fun j =>
          (r i * r i) * (d j * d j) + (r j * r j) * (d i * d i) -
            2 * ((r i * d i) * (r j * d j))).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        apply sum_map_congr
        intro j hj
        exact hexpand i j]
  rw [show ((List.range n).map fun i =>
        ((List.range n).map fun j =>
          (r i * r i) * (d j * d j) + (r j * r j) * (d i * d i) -
            2 * ((r i * d i) * (r j * d j))).sum).sum =
      ((List.range n).map fun i =>
        ((List.range n).map fun j => (r i * r i) * (d j * d j)).sum +
        ((List.range n).map fun j => (r j * r j) * (d i * d i)).sum -
        ((List.range n).map fun j => 2 * ((r i * d i) * (r j * d j))).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        rw [sum_map_sub, sum_map_add]]
  rw [show ((List.range n).map fun i =>
        ((List.range n).map fun j => (r i * r i) * (d j * d j)).sum +
        ((List.range n).map fun j => (r j * r j) * (d i * d i)).sum -
        ((List.range n).map fun j => 2 * ((r i * d i) * (r j * d j))).sum).sum =
      ((List.range n).map fun i =>
        ((List.range n).map fun j => (r i * r i) * (d j * d j)).sum).sum +
      ((List.range n).map fun i =>
        ((List.range n).map fun j => (r j * r j) * (d i * d i)).sum).sum -
      ((List.range n).map fun i =>
        ((List.range n).map fun j => 2 * ((r i * d i) * (r j * d j))).sum).sum by
        rw [sum_map_sub, sum_map_add]]
  rw [← sum_mul_sum]
  rw [show ((List.range n).map fun i =>
        ((List.range n).map fun j => (r j * r j) * (d i * d i)).sum).sum =
      ((List.range n).map fun i =>
        ((List.range n).map fun j => (d i * d i) * (r j * r j)).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        apply sum_map_congr
        intro j hj
        grind]
  rw [sum_swap]
  rw [show ((List.range n).map fun y =>
        ((List.range n).map fun x => (d x * d x) * (r y * r y)).sum).sum =
      ((List.range n).map fun y =>
        ((List.range n).map fun x => (r y * r y) * (d x * d x)).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro y hy
        apply sum_map_congr
        intro x hx
        grind]
  rw [← sum_mul_sum]
  rw [show ((List.range n).map fun i =>
        ((List.range n).map fun j => 2 * ((r i * d i) * (r j * d j))).sum).sum =
      ((List.range n).map fun i =>
        2 * ((List.range n).map fun j => (r i * d i) * (r j * d j)).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        rw [sum_map_mul_left]]
  rw [sum_map_mul_left]
  rw [← sum_mul_sum]
  unfold dot total
  grind

theorem EnergyBound_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.EnergyBound.Goal := by
  intro n row actual code
  let d : Vec := fun j => actual j - code j
  let B : Int := dot n row actual - dot n row code
  have hB : dot n row d = B := by
    dsimp [d, B]
    unfold dot
    change ((List.range n).map fun j => row j * (actual j - code j)).sum =
      ((List.range n).map fun j => row j * actual j).sum -
        ((List.range n).map fun j => row j * code j).sum
    rw [show ((List.range n).map fun j => row j * (actual j - code j)).sum =
        ((List.range n).map fun j => row j * actual j - row j * code j).sum by
          apply sum_map_congr
          intro j hj
          grind]
    rw [sum_map_sub]
  have hnonneg : 0 ≤
      ((List.range n).map fun i =>
        ((List.range n).map fun j => (row i * d j - row j * d i) * (row i * d j - row j * d i)).sum).sum := by
    apply sum_map_nonneg
    intro i hi
    apply sum_map_nonneg
    intro j hj
    exact sq_nonneg_int (row i * d j - row j * d i)
  rw [← cauchy_double n row d] at hnonneg
  rw [hB] at hnonneg
  dsimp [d, B] at hnonneg ⊢
  have hp1 : (dot n row actual - dot n row code) ^ 2 =
      (dot n row actual - dot n row code) * (dot n row actual - dot n row code) := by grind
  have hp2 : (total n fun j => (actual j - code j) ^ 2) =
      total n (fun j => (actual j - code j) * (actual j - code j)) := by
    apply sum_map_congr
    intro j hj
    grind
  rw [hp1, hp2]
  omega

private theorem cast_sum_natAbs (s : List Int) (m : Int) :
    ((s.map fun x => (x - m).natAbs).sum : Int) =
      (s.map fun x => ((x - m).natAbs : Int)).sum := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      omega

private theorem perm_sum_int {α : Type} (g : α → Int) {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    (l₁.map g).sum = (l₂.map g).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem getElem?_some_bound {s : List Int} {j : Nat} {v : Int}
    (hv : s[j]? = some v) : j < s.length := by
  by_cases hj : j < s.length
  · exact hj
  · have hnone := List.getElem?_eq_none (by omega : s.length ≤ j)
    rw [hnone] at hv
    cases hv

private theorem getElem?_some_eq {s : List Int} {j : Nat} {v : Int}
    (hv : s[j]? = some v) (hj : j < s.length) : s[j] = v := by
  have hg := List.getElem?_eq_getElem hj
  rw [hg] at hv
  cases hv
  rfl

private theorem pairwise_getElem_le {s : List Int} (hs : s.Pairwise (fun a b : Int => a ≤ b))
    {i j : Nat} (hi : i < s.length) (hj : j < s.length) (hij : i < j) : s[i] ≤ s[j] := by
  induction s generalizing i j with
  | nil => simp at hi
  | cons x xs ih =>
      cases i with
      | zero =>
          cases j with
          | zero => omega
          | succ j =>
              have hpair := (List.pairwise_cons.mp hs).1
              have hjxs : j < xs.length := by simpa using hj
              exact hpair (xs[j]) (List.getElem_mem hjxs)
      | succ i =>
          cases j with
          | zero => omega
          | succ j =>
              exact ih (List.pairwise_cons.mp hs).2 (by simpa using hi) (by simpa using hj) (by omega)

private theorem count_le_take (s : List Int) (hs : s.Pairwise (fun a b : Int => a ≤ b))
    (i : Nat) (ci : Int)
    (hc : ∀ j, i ≤ j → ∀ v, s[j]? = some v → ci < v) :
    (s.filter fun x => x ≤ ci).length ≤ i := by
  induction s generalizing i with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero =>
          have h0 := hc 0 (by omega) x (by simp)
          have hx : ¬ x ≤ ci := by
            intro hle
            omega
          have hc' : ∀ j, 0 ≤ j → ∀ v, xs[j]? = some v → ci < v := by
            intro j hj0 v hv
            exact hc (j+1) (by omega) v (by simpa using hv)
          have ht := ih (List.pairwise_cons.mp hs).2 0 hc'
          simpa [hx] using ht
      | succ i =>
          by_cases hx : x ≤ ci
          · have hc' : ∀ j, i ≤ j → ∀ v, xs[j]? = some v → ci < v := by
              intro j hji v hv
              exact hc (j+1) (by omega) v (by simpa using hv)
            have ht := ih (List.pairwise_cons.mp hs).2 i hc'
            simp [hx]
            omega
          · have hc' : ∀ j, i+1 ≤ j → ∀ v, xs[j]? = some v → ci < v := by
              intro j hji v hv
              exact hc (j+1) (by omega) v (by simpa using hv)
            have ht := ih (List.pairwise_cons.mp hs).2 (i+1) hc'
            simp [hx]
            omega

private theorem count_ge_drop (s : List Int) (hs : s.Pairwise (fun a b : Int => a ≤ b))
    (i : Nat) (ci : Int)
    (hc : ∀ j, i ≤ j → ∀ v, s[j]? = some v → ci < v) :
    (s.drop i).length ≤ (s.filter fun x => ci < x).length := by
  induction s generalizing i with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero =>
          have hx : ci < x := hc 0 (by omega) x (by simp)
          have hc' : ∀ j, 0 ≤ j → ∀ v, xs[j]? = some v → ci < v := by
            intro j hj0 v hv
            exact hc (j+1) (by omega) v (by simpa using hv)
          have ht := ih (List.pairwise_cons.mp hs).2 0 hc'
          rw [show (x :: xs).drop 0 = x :: xs by rfl] 
          have ht' := ht
          rw [show xs.drop 0 = xs by rfl] at ht'
          simp [hx]
          omega
      | succ i =>
          have hc' : ∀ j, i ≤ j → ∀ v, xs[j]? = some v → ci < v := by
            intro j hji v hv
            exact hc (j+1) (by omega) v (by simpa using hv)
          have ht := ih (List.pairwise_cons.mp hs).2 i hc'
          simp only [List.drop_succ_cons]
          by_cases hcx : ci < x
          · have hgo : (xs.drop i).length ≤ (xs.filter fun y => ci < y).length + 1 := by omega
            simpa [hcx] using hgo
          · simpa [hcx] using ht

private theorem count_ge_le_drop (s : List Int) (hs : s.Pairwise (fun a b : Int => a ≤ b))
    (i : Nat) (ci : Int)
    (hc : ∀ j, j ≤ i → ∀ v, s[j]? = some v → v ≤ ci) :
    (s.filter fun x => ci < x).length ≤ (s.drop (i+1)).length := by
  induction s generalizing i with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero =>
          have hx : x ≤ ci := hc 0 (by omega) x (by simp)
          have hle : (xs.filter fun y => ci < y).length ≤ xs.length := List.length_filter_le ..
          simp [hx]
          omega
      | succ i =>
          have hx : x ≤ ci := hc 0 (by omega) x (by simp)
          have hc' : ∀ j, j ≤ i → ∀ v, xs[j]? = some v → v ≤ ci := by
            intro j hji v hv
            exact hc (j+1) (by omega) v (by simpa using hv)
          have ht := ih (List.pairwise_cons.mp hs).2 i hc'
          have hnot : ¬ ci < x := by omega
          simpa [hnot] using ht

private theorem count_le_ge_take (s : List Int) (hs : s.Pairwise (fun a b : Int => a ≤ b))
    (i : Nat) (ci : Int) (hi : i < s.length)
    (hc : ∀ j, j ≤ i → ∀ v, s[j]? = some v → v ≤ ci) :
    i + 1 ≤ (s.filter fun x => x ≤ ci).length := by
  induction s generalizing i with
  | nil =>
      simp at hi
  | cons x xs ih =>
      cases i with
      | zero =>
          have hx : x ≤ ci := hc 0 (by omega) x (by simp)
          simp [hx]
      | succ i =>
          have hx : x ≤ ci := hc 0 (by omega) x (by simp)
          have hc' : ∀ j, j ≤ i → ∀ v, xs[j]? = some v → v ≤ ci := by
            intro j hji v hv
            exact hc (j+1) (by omega) v (by simpa using hv)
          have hiTail : i < xs.length := by simpa using hi
          have ht := ih (List.pairwise_cons.mp hs).2 i hiTail hc'
          simp [hx]
          omega

private theorem natAbs_step (x c : Int) :
    ((x - (c + 1)).natAbs : Int) = (x - c).natAbs + (if x ≤ c then 1 else -1) := by
  by_cases h : x ≤ c
  · rw [← Int.natAbs_neg (x - (c+1)), Int.natAbs_of_nonneg (by omega : 0 ≤ -(x - (c+1)))]
    rw [← Int.natAbs_neg (x - c), Int.natAbs_of_nonneg (by omega : 0 ≤ -(x - c))]
    simp [h]
    omega
  · rw [Int.natAbs_of_nonneg (by omega : 0 ≤ x - (c+1))]
    rw [Int.natAbs_of_nonneg (by omega : 0 ≤ x - c)]
    simp [h]
    omega

private theorem l1_step (s : List Int) (c : Int) :
    ((s.map fun x => (x - (c+1)).natAbs).sum : Int) =
      ((s.map fun x => (x - c).natAbs).sum : Int) +
        (s.map fun x => if x ≤ c then (1 : Int) else -1).sum := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hstep := natAbs_step x c
      omega

private theorem sign_sum (s : List Int) (c : Int) :
    (s.map fun x => if x ≤ c then (1 : Int) else -1).sum =
      ((s.filter fun x => x ≤ c).length : Int) -
        ((s.filter fun x => c < x).length : Int) := by
  induction s with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x ≤ c
      · have hnx : ¬ c < x := by omega
        simp [hx, hnx, ih]
        omega
      · have hnx : c < x := by omega
        simp [hx, hnx, ih]
        omega

private theorem median_min_sorted (s : List Int) (hs : s.Pairwise (fun a b : Int => a ≤ b))
    (hne : s ≠ []) :
    ∀ c : Int, ((s.map fun x => (x - s[s.length / 2]?.getD 0).natAbs).sum : Int) ≤
      ((s.map fun x => (x - c).natAbs).sum : Int) := by
  let i := s.length / 2
  have hlen : 0 < s.length := by
    cases s with
    | nil => exfalso; exact hne rfl
    | cons x xs => simp
  have hi : i < s.length := by
    dsimp [i]
    omega
  let m : Int := s[i]'hi
  have hm : s[s.length / 2]?.getD 0 = m := by
    change s[i]?.getD 0 = m
    rw [List.getElem?_eq_getElem hi]
    rfl
  let f : Int → Int := fun c => ((s.map fun x => (x - c).natAbs).sum : Int)
  have hc_i_le : ∀ {j}, j < s.length → j ≤ i → s[j]?.getD 0 ≤ m := by
    intro j hjlen hj
    have hval : s[j]?.getD 0 = s[j] := by
      rw [List.getElem?_eq_getElem hjlen]
      rfl
    rw [hval]
    by_cases hji : j = i
    · subst j
      omega
    · exact pairwise_getElem_le hs hjlen hi (by omega)
  have hi_le_sj : ∀ {j}, j < s.length → i ≤ j → m ≤ s[j]?.getD 0 := by
    intro j hjlen hji
    have hval : s[j]?.getD 0 = s[j] := by
      rw [List.getElem?_eq_getElem hjlen]
      rfl
    rw [hval]
    by_cases hji' : j = i
    · subst j
      omega
    · exact pairwise_getElem_le hs hi hjlen (by omega)
  have step_down : ∀ c : Int, c < m → f (c+1) ≤ f c := by
    intro c hc
    have hcount : (s.filter fun x => x ≤ c).length ≤ (s.filter fun x => c < x).length := by
      have hle := count_le_take s hs i c (by
        intro j hji v hv
        have hjlen := getElem?_some_bound hv
        have h := hi_le_sj hjlen hji
        have hvget : s[j]?.getD 0 = v := by rw [hv]; rfl
        omega)
      have hge := count_ge_drop s hs i c (by
        intro j hji v hv
        have hjlen := getElem?_some_bound hv
        have h := hi_le_sj hjlen hji
        have hvget : s[j]?.getD 0 = v := by rw [hv]; rfl
        omega)
      have hlen_drop : (s.drop i).length = s.length - i := List.length_drop ..
      rw [hlen_drop] at hge
      have hi_le : i ≤ s.length - i := by
        dsimp [i]
        omega
      omega
    have hsign : (s.map fun x => if x ≤ c then (1 : Int) else -1).sum ≤ 0 := by
      rw [sign_sum]
      omega
    change ((s.map fun x => (x - (c+1)).natAbs).sum : Int) ≤
      ((s.map fun x => (x - c).natAbs).sum : Int)
    rw [l1_step]
    omega
  have step_up : ∀ c : Int, m ≤ c → f c ≤ f (c+1) := by
    intro c hc
    have hcount : (s.filter fun x => c < x).length ≤ (s.filter fun x => x ≤ c).length := by
      have hle := count_ge_le_drop s hs i c (by
        intro j hji v hv
        have hjlen := getElem?_some_bound hv
        have h := hc_i_le hjlen hji
        have hvget : s[j]?.getD 0 = v := by rw [hv]; rfl
        omega)
      have hge := count_le_ge_take s hs i c hi (by
        intro j hji v hv
        have hjlen := getElem?_some_bound hv
        have h := hc_i_le hjlen hji
        have hvget : s[j]?.getD 0 = v := by rw [hv]; rfl
        omega)
      have htail : (s.drop (i+1)).length = s.length - (i+1) := List.length_drop ..
      have hineq : s.length - (i+1) ≤ i+1 := by
        dsimp [i]
        omega
      omega
    have hsign : 0 ≤ (s.map fun x => if x ≤ c then (1 : Int) else -1).sum := by
      rw [sign_sum]
      omega
    change ((s.map fun x => (x - c).natAbs).sum : Int) ≤
      ((s.map fun x => (x - (c+1)).natAbs).sum : Int)
    rw [l1_step]
    omega
  rw [hm]
  intro c
  by_cases hmc : m ≤ c
  · let k := (c - m).toNat
    have hk : (k : Int) = c - m := Int.toNat_of_nonneg (by omega)
    have hmain : ∀ t : Nat, f m ≤ f (m + (t : Int)) := by
      intro t
      induction t with
      | zero => simp
      | succ t ih =>
          have hstep := step_up (m + (t : Int)) (by omega)
          have heq : m + (t : Int) + 1 = m + ((t + 1 : Nat) : Int) := by omega
          rw [heq] at hstep
          omega
    have hc_eq : m + (k : Int) = c := by omega
    rw [← hc_eq]
    exact hmain k
  · have hcm : c ≤ m := by omega
    let k := (m - c).toNat
    have hk : (k : Int) = m - c := Int.toNat_of_nonneg (by omega)
    have hmain : ∀ t : Nat, f m ≤ f (m - (t : Int)) := by
      intro t
      induction t with
      | zero => simp
      | succ t ih =>
          have hstep := step_down (m - ((t : Int) + 1)) (by omega)
          have heq : (m - (t : Int)) = (m - ((t : Int) + 1)) + 1 := by omega
          rw [← heq] at hstep
          change f m ≤ f (m - ((t : Int) + 1))
          omega
    have hc_eq : m - (k : Int) = c := by omega
    rw [← hc_eq]
    exact hmain k

theorem MedianCode_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.MedianCode.Goal := by
  intro ws center hne
  let s := ws.mergeSort
  have hperm : s.Perm ws := by
    dsimp [s]
    exact List.mergeSort_perm ws (fun a b : Int => a ≤ b)
  have hsorted : s.Pairwise (fun a b : Int => a ≤ b) := by
    dsimp [s]
    have htmp := List.pairwise_mergeSort
      (le := fun a b : Int => a ≤ b)
      (by
        intro a b c hab hbc
        have hab' : a ≤ b := by simpa using hab
        have hbc' : b ≤ c := by simpa using hbc
        simpa using (show a ≤ c from by omega))
      (by intro a b; by_cases h : a ≤ b <;> simp [h] <;> omega) ws
    simpa using htmp
  have hlen : 0 < ws.length := by
    cases ws with
    | nil => exfalso; exact hne rfl
    | cons x xs => simp
  have hi : ws.length / 2 < s.length := by
    dsimp [s]
    simp only [List.length_mergeSort]
    omega
  have hget : ws.mergeSort[ws.length / 2]?.getD 0 = (ws.mergeSort)[ws.length / 2] := by
    rw [List.getElem?_eq_getElem (by simpa [s] using hi)]
    rfl
  rw [hget]
  have hlenEq : s.length = ws.length := hperm.length_eq
  have hsne : s ≠ [] := by
    intro hs
    have hlen0 : s.length = 0 := by simp [hs]
    have hlenws : ws.length = 0 := by simpa [hlenEq] using hlen0
    cases ws with
    | nil => exact hne rfl
    | cons x xs => simp at hlenws
  have hmed : s[s.length / 2]?.getD 0 = s[ws.length / 2] := by
    rw [hlenEq]
    have hi' : ws.length / 2 < s.length := by simpa [s] using hi
    rw [List.getElem?_eq_getElem hi']
    rfl
  have hmin : ((s.map fun x => (x - s[ws.length / 2]).natAbs).sum : Int) ≤
      ((s.map fun x => (x - center).natAbs).sum : Int) := by
    have hh := median_min_sorted s hsorted hsne center
    rwa [hmed] at hh
  have hsumws : ((s.map fun x => (x - s[ws.length / 2]).natAbs).sum : Int) =
      ((ws.map fun w => (w - s[ws.length / 2]).natAbs).sum : Int) := by
    rw [cast_sum_natAbs s (s[ws.length / 2]), cast_sum_natAbs ws (s[ws.length / 2])]
    exact perm_sum_int (fun w : Int => ((w - s[ws.length / 2]).natAbs : Int)) hperm
  have hsumws2 : ((s.map fun x => (x - center).natAbs).sum : Int) =
      ((ws.map fun w => (w - center).natAbs).sum : Int) := by
    rw [cast_sum_natAbs s center, cast_sum_natAbs ws center]
    exact perm_sum_int (fun w : Int => ((w - center).natAbs : Int)) hperm
  have hif : ((ws.map fun w => (w - s[ws.length / 2]).natAbs).sum : Int) ≤
      ((ws.map fun w => (w - center).natAbs).sum : Int) := by omega
  change (ws.map fun w => (w - s[ws.length / 2]).natAbs).sum ≤
    (ws.map fun w => (w - center).natAbs).sum
  omega

private def wcmp (score : Nat → Int) (a b : Nat) : Bool :=
  score a > score b || (score a == score b && a ≤ b)

private theorem wcmp_total (score : Nat → Int) (a b : Nat) :
    (wcmp score a b || wcmp score b a) = true := by
  dsimp [wcmp]
  by_cases h : score a > score b
  · simp [h]
  · by_cases h' : score b > score a
    · simp [h, h']
    · have heq : score a = score b := by omega
      simp [heq]
      omega

private theorem wcmp_of_lt (score : Nat → Int) {a b : Nat} (h : score b < score a) :
    wcmp score a b = true := by
  dsimp [wcmp]
  have hgt : score a > score b := by omega
  simp [hgt]

private theorem wcmp_not_of_lt (score : Nat → Int) {a b : Nat} (h : score b < score a) :
    wcmp score b a = false := by
  dsimp [wcmp]
  have hgt : score a > score b := by omega
  have hnot : ¬ score b > score a := by omega
  have hne : score b ≠ score a := by omega
  have hbeq : (score b == score a) = false :=
    Bool.eq_false_of_ne_true (by intro hh; exact hne (beq_iff_eq.mp hh))
  simp [hnot, hbeq]

private theorem wcmp_trans (score : Nat → Int) (a b c : Nat)
    (hab : wcmp score a b = true) (hbc : wcmp score b c = true) :
    wcmp score a c = true := by
  dsimp [wcmp] at hab hbc ⊢
  by_cases habgt : score a > score b
  · by_cases hbcgt : score b > score c
    · simp [show score a > score c by omega]
    · have hbeq : score b = score c := by
        by_cases hbe : score b == score c
        · exact beq_iff_eq.mp hbe
        · simp [hbcgt, hbe] at hbc
      simp [show score a > score c by omega]
  · have hbeqab : score a = score b := by
      by_cases hbe : score a == score b
      · exact beq_iff_eq.mp hbe
      · simp [habgt, hbe] at hab
    have hable : a ≤ b := by
      have hbe : score a == score b := by simp [hbeqab]
      simp [habgt, hbe] at hab
      omega
    by_cases hbcgt : score b > score c
    · simp [show score a > score c by omega]
    · have hbeqbc : score b = score c := by
        by_cases hbe : score b == score c
        · exact beq_iff_eq.mp hbe
        · simp [hbcgt, hbe] at hbc
      have hble : b ≤ c := by
        have hbe : score b == score c := by simp [hbeqbc]
        simp [hbcgt, hbe] at hbc
        omega
      have haeqc : score a == score c := by simp [hbeqab, hbeqbc]
      simp [show ¬ score a > score c by omega, haeqc]
      omega

private theorem wcmp_antisymm (score : Nat → Int) {a b : Nat}
    (hab : wcmp score a b = true) (hba : wcmp score b a = true) : a = b := by
  by_cases hgt : score b < score a
  · have hf := wcmp_not_of_lt score hgt
    rw [hf] at hba
    cases hba
  · by_cases hlt : score a < score b
    · have hf := wcmp_not_of_lt score hlt
      rw [hf] at hab
      cases hab
    · have heq : score a = score b := by omega
      dsimp [wcmp] at hab hba
      have hbeq : (score a == score b) = true := by simp [heq]
      simp [show ¬ score a > score b by omega, show ¬ score b > score a by omega, hbeq] at hab hba
      omega

private theorem pairwise_getElem_rel {α : Type} {R : α → α → Prop}
    {L : List α} (h : L.Pairwise R) {i j : Nat} (hi : i < L.length) (hj : j < L.length)
    (hij : i < j) : R L[i] L[j] := by
  induction L generalizing i j with
  | nil => simp at hi
  | cons x xs ih =>
      cases i with
      | zero =>
          cases j with
          | zero => omega
          | succ j =>
              have ht := (List.pairwise_cons.mp h).1
              have hjxs : j < xs.length := by simpa using hj
              exact ht (xs[j]) (List.getElem_mem hjxs)
      | succ i =>
          cases j with
          | zero => omega
          | succ j =>
              exact ih (List.pairwise_cons.mp h).2 (by simpa using hi) (by simpa using hj) (by omega)

private theorem rel_take_drop {α : Type} {R : α → α → Prop} (L : List α) (h : L.Pairwise R) :
    ∀ k x y, x ∈ L.take k → y ∈ L.drop k → R x y := by
  induction L with
  | nil => simp
  | cons a L ih =>
      intro k x y hx hy
      cases k with
      | zero => cases hx
      | succ k =>
          change x ∈ a :: L.take k at hx
          change y ∈ L.drop k at hy
          rw [List.mem_cons] at hx
          rcases hx with rfl | hx
          · have hyL : y ∈ L := List.mem_of_mem_drop hy
            exact (List.pairwise_cons.mp h).1 y hyL
          · exact ih (List.pairwise_cons.mp h).2 k x y hx hy

private theorem sorted_eq_of_mem_iff (score : Nat → Int) {l₁ l₂ : List Nat}
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hmem : ∀ a, a ∈ l₁ ↔ a ∈ l₂)
    (hs₁ : l₁.Pairwise fun a b => wcmp score a b = true)
    (hs₂ : l₂.Pairwise fun a b => wcmp score a b = true) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => rfl
      | cons b bs =>
          have : b ∈ ([] : List Nat) := (hmem b).mpr (by simp)
          simp at this
  | cons a xs ih =>
      cases l₂ with
      | nil =>
          have : a ∈ ([] : List Nat) := (hmem a).mp (by simp)
          simp at this
      | cons b ys =>
          have hbL : b ∈ a :: xs := (hmem b).mpr (by simp)
          have hba_eq : b = a := by
            by_cases hba : b = a
            · exact hba
            · have hbtail : b ∈ xs := by simpa [hba] using hbL
              have hab : wcmp score a b = true := (List.pairwise_cons.mp hs₁).1 b hbtail
              have haL2 : a ∈ b :: ys := (hmem a).mp (by simp)
              have hab' : a ≠ b := by intro heq; exact hba heq.symm
              have hatail : a ∈ ys := by simpa [hba, hab'] using haL2
              have hbaC : wcmp score b a = true := (List.pairwise_cons.mp hs₂).1 a hatail
              have heq := wcmp_antisymm score hab hbaC
              exact (hba heq.symm).elim
          subst b
          have hndxs : xs.Nodup := (List.nodup_cons.mp hnd₁).2
          have hndys : ys.Nodup := (List.nodup_cons.mp hnd₂).2
          have hmem' : ∀ c, c ∈ xs ↔ c ∈ ys := by
            intro c
            constructor
            · intro hc
              have hcne : c ≠ a := by
                intro hca
                subst c
                exact (List.nodup_cons.mp hnd₁).1 hc
              have : c ∈ a :: ys := (hmem c).mp (by simp [hc])
              simpa [hcne] using this
            · intro hc
              have hcne : c ≠ a := by
                intro hca
                subst c
                exact (List.nodup_cons.mp hnd₂).1 hc
              have : c ∈ a :: xs := (hmem c).mpr (by simp [hc])
              simpa [hcne] using this
          have hs₁' : xs.Pairwise fun c d => wcmp score c d = true := (List.pairwise_cons.mp hs₁).2
          have hs₂' : ys.Pairwise fun c d => wcmp score c d = true := (List.pairwise_cons.mp hs₂).2
          exact congrArg (fun l : List Nat => a :: l) (ih hndxs hndys hmem' hs₁' hs₂')

private theorem natAbs_le_bounds {a : Int} {e : Nat} (h : a.natAbs ≤ e) :
    -((e : Nat) : Int) ≤ a ∧ a ≤ (e : Int) := by
  rcases Int.natAbs_eq a with ha | ha
  · rw [ha]
    omega
  · rw [ha]
    omega

private theorem mem_of_forall_mem_eq_length {l₁ l₂ : List Nat}
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hlen : l₁.length = l₂.length)
    (hall : ∀ y, y ∈ l₁ → y ∈ l₂) : ∀ a, a ∈ l₂ → a ∈ l₁ := by
  induction l₁ generalizing l₂ with
  | nil =>
      intro a ha
      have hlen0 : l₂.length = 0 := by simpa using hlen.symm
      have hnil : l₂ = [] := List.eq_nil_of_length_eq_zero hlen0
      simp [hnil] at ha
  | cons x xs ih =>
      intro a ha
      have hx2 : x ∈ l₂ := hall x (by simp)
      by_cases hax : a = x
      · subst a
        simp
      · have ha2' : a ∈ l₂.erase x := (List.mem_erase_of_ne hax).2 ha
        have hndxs : xs.Nodup := (List.nodup_cons.mp hnd₁).2
        have hnd2' : (l₂.erase x).Nodup := hnd₂.erase x
        have hlen' : xs.length = (l₂.erase x).length := by
          rw [List.length_erase_of_mem hx2]
          have hlen1 : xs.length + 1 = l₂.length := by simpa using hlen
          omega
        have hall' : ∀ y, y ∈ xs → y ∈ l₂.erase x := by
          intro y hy
          have hy2 : y ∈ l₂ := hall y (by simp [hy])
          have hyx : y ≠ x := by
            intro heq
            subst y
            exact (List.nodup_cons.mp hnd₁).1 hy
          exact (List.mem_erase_of_ne hyx).2 hy2
        have hyxs : a ∈ xs := ih hndxs hnd2' hlen' hall' a ha2'
        simp [hax, hyxs]

private theorem exists_mem_not_mem_of_nodup_eq_length {l₁ l₂ : List Nat}
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hlen : l₁.length = l₂.length)
    {a : Nat} (ha2 : a ∈ l₂) (ha1 : a ∉ l₁) : ∃ y, y ∈ l₁ ∧ y ∉ l₂ := by
  by_cases hex : ∃ y, y ∈ l₁ ∧ y ∉ l₂
  · exact hex
  · exfalso
    have hall : ∀ y, y ∈ l₁ → y ∈ l₂ := by
      intro y hy
      by_cases hy2 : y ∈ l₂
      · exact hy2
      · exact False.elim (hex ⟨y, hy, hy2⟩)
    exact ha1 (mem_of_forall_mem_eq_length hnd₁ hnd₂ hlen hall a ha2)

theorem LateSecondMeasure_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.LateSecondMeasure.Goal := by
  intro gs k approx exact payload err hclose kept hgap hnodup
  let sa := gs.mergeSort (wcmp approx)
  let se := gs.mergeSort (wcmp exact)
  have hkeptEq : kept = sa.take k := by
    change winners gs approx k = sa.take k
    rfl
  let exactKept := se.take k
  have hsaSorted : sa.Pairwise fun a b => wcmp approx a b = true := by
    dsimp [sa]
    exact List.pairwise_mergeSort (wcmp_trans approx) (wcmp_total approx) gs
  have hseSorted : se.Pairwise fun a b => wcmp exact a b = true := by
    dsimp [se]
    exact List.pairwise_mergeSort (wcmp_trans exact) (wcmp_total exact) gs
  have hsaPerm : sa.Perm gs := by
    dsimp [sa]
    exact List.mergeSort_perm gs (wcmp approx)
  have hsePerm : se.Perm gs := by
    dsimp [se]
    exact List.mergeSort_perm gs (wcmp exact)
  have hkeptLen : kept.length = min k gs.length := by
    rw [hkeptEq]
    dsimp [sa]
    simp only [List.length_take, List.length_mergeSort]
  have hexactKeptLen : exactKept.length = min k gs.length := by
    dsimp [exactKept, se]
    simp only [List.length_take, List.length_mergeSort]
  have hlenEq : kept.length = exactKept.length := by omega
  have hndKept : kept.Nodup := by
    rw [hkeptEq]
    have hsaNodup : sa.Nodup := hsaPerm.symm.nodup hnodup
    exact List.Pairwise.take (i := k) hsaNodup
  have hndExactKept : exactKept.Nodup := by
    dsimp [exactKept, se]
    have hseNodup : se.Nodup := hsePerm.symm.nodup hnodup
    exact List.Pairwise.take (i := k) hseNodup
  have mem_kept_sa : ∀ a, a ∈ kept → a ∈ sa := by
    intro a ha
    rw [hkeptEq] at ha
    exact List.mem_of_mem_take ha
  have mem_kept_gs : ∀ a, a ∈ kept → a ∈ gs := by
    intro a ha
    exact (hsaPerm.mem_iff).mp (mem_kept_sa a ha)
  have mem_exactKept_se : ∀ a, a ∈ exactKept → a ∈ se := by
    intro a ha
    dsimp [exactKept] at ha
    exact List.mem_of_mem_take ha
  have mem_exactKept_gs : ∀ a, a ∈ exactKept → a ∈ gs := by
    intro a ha
    exact (hsePerm.mem_iff).mp (mem_exactKept_se a ha)
  have hsep : ∀ x, x ∈ kept → ∀ y, y ∈ gs → y ∉ kept → exact y < exact x := by
    intro x hx y hy hyNot
    have hrel := hgap x hx y hy hyNot
    have hxGs : x ∈ gs := mem_kept_gs x hx
    have hcx := natAbs_le_bounds (hclose x hxGs)
    have hcy := natAbs_le_bounds (hclose y hy)
    omega
  have hmemEquiv : ∀ a, a ∈ exactKept ↔ a ∈ kept := by
    intro a
    constructor
    · intro haE
      by_cases haK : a ∈ kept
      · exact haK
      · have haGs : a ∈ gs := mem_exactKept_gs a haE
        obtain ⟨y, hyK, hyNotE⟩ := exists_mem_not_mem_of_nodup_eq_length hndKept hndExactKept
          hlenEq haE haK
        have hyGs : y ∈ gs := mem_kept_gs y hyK
        have hlt : exact a < exact y := hsep y hyK a haGs haK
        have hcmp : wcmp exact a y = true := by
          dsimp [exactKept] at haE
          have hyDrop : y ∈ se.drop k := by
            have hySE : y ∈ se := (hsePerm.mem_iff).mpr hyGs
            have hpart := List.take_append_drop k se
            rw [← hpart] at hySE
            rw [List.mem_append] at hySE
            rcases hySE with h | h
            · exact False.elim (by
                dsimp [exactKept] at hyNotE
                exact hyNotE h)
            · exact h
          exact rel_take_drop se hseSorted k a y haE hyDrop
        have hnot := wcmp_not_of_lt exact hlt
        rw [hnot] at hcmp
        cases hcmp
    · intro haK
      by_cases haE : a ∈ exactKept
      · exact haE
      · have haGs : a ∈ gs := mem_kept_gs a haK
        obtain ⟨y, hyE, hyNotK⟩ := exists_mem_not_mem_of_nodup_eq_length hndExactKept hndKept
          hlenEq.symm haK haE
        have hyGs : y ∈ gs := mem_exactKept_gs y hyE
        have hlt : exact y < exact a := hsep a haK y hyGs hyNotK
        have hcmp : wcmp exact y a = true := by
          -- compare in se: y is in take k, a is in drop k
          have haDropSE : a ∈ se.drop k := by
            have haSE : a ∈ se := (hsePerm.mem_iff).mpr haGs
            have hpart := List.take_append_drop k se
            rw [← hpart] at haSE
            rw [List.mem_append] at haSE
            rcases haSE with h | h
            · exact False.elim (by
                dsimp [exactKept] at haE
                exact haE h)
            · exact h
          exact rel_take_drop se hseSorted k y a (by dsimp [exactKept] at hyE; exact hyE) haDropSE
        have hnot := wcmp_not_of_lt exact hlt
        rw [hnot] at hcmp
        cases hcmp
  have hsortExKept : exactKept.Pairwise fun a b => wcmp exact a b = true := by
    dsimp [exactKept]
    exact hseSorted.take
  have hsortSortedKept : ((sa.take k).mergeSort (wcmp exact)).Pairwise
      fun a b => wcmp exact a b = true := by
    exact List.pairwise_mergeSort (wcmp_trans exact) (wcmp_total exact) (sa.take k)
  have heqLists : exactKept = (sa.take k).mergeSort (wcmp exact) := by
    apply sorted_eq_of_mem_iff exact hndExactKept
      ((List.mergeSort_perm (sa.take k) (wcmp exact)).symm.nodup (by rw [← hkeptEq]; exact hndKept))
    · intro a
      rw [List.mem_mergeSort, ← hkeptEq]
      exact hmemEquiv a
    · exact hsortExKept
    · exact hsortSortedKept
  have hlenSortKept : ((sa.take k).mergeSort (wcmp exact)).length ≤ k := by
    simp only [List.length_mergeSort, List.length_take]
    omega
  have htake : ((sa.take k).mergeSort (wcmp exact)).take k =
      (sa.take k).mergeSort (wcmp exact) := List.take_of_length_le hlenSortKept
  change (se.take k).map (fun g => (g, payload g)) =
    (((sa.take k).mergeSort (wcmp exact)).take k).map (fun g => (g, payload g))
  rw [htake, ← heqLists]

private def xorSpan (S : List Nat) (x : Nat) : Prop :=
  match S with
  | [] => x = 0
  | b::bs => xorSpan bs x ∨ xorSpan bs (x ^^^ b)

private theorem xorSpan_zero (S : List Nat) : xorSpan S 0 := by
  induction S with
  | nil => rfl
  | cons b bs ih => exact Or.inl ih

private theorem xor_cancel_xor (x y b : Nat) : (x ^^^ b) ^^^ (y ^^^ b) = x ^^^ y := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp [Nat.testBit_xor, Bool.xor_comm, Bool.xor_left_comm]

private theorem xorSpan_xor {S : List Nat} {x y : Nat}
    (hx : xorSpan S x) (hy : xorSpan S y) : xorSpan S (x ^^^ y) := by
  induction S generalizing x y with
  | nil => simp [xorSpan] at hx hy ⊢; rw [hx, hy]; simp
  | cons b bs ih =>
      dsimp [xorSpan] at hx hy ⊢
      rcases hx with hx | hx <;> rcases hy with hy | hy
      · exact Or.inl (ih hx hy)
      · have hz := ih hx hy
        rw [← Nat.xor_assoc] at hz
        exact Or.inr hz
      · have hxy : (x ^^^ b) ^^^ y = (x ^^^ y) ^^^ b := by
          apply Nat.eq_of_testBit_eq
          intro i
          simp [Nat.testBit_xor, Bool.xor_comm]
        exact Or.inr (by rw [← hxy]; exact ih hx hy)
      · have hz := ih hx hy
        rw [xor_cancel_xor] at hz
        exact Or.inl hz

private theorem xorSpan_insert_self (S : List Nat) (x : Nat) : xorSpan (insertBits S x) x := by
  induction S generalizing x with
  | nil =>
      by_cases hx : x = 0
      · simp [insertBits, hx, xorSpan]
      · simp [insertBits, hx, xorSpan, Nat.xor_self]
  | cons b bs ih =>
      by_cases hx0 : x = 0
      · simpa [insertBits, hx0] using xorSpan_zero (b::bs)
      · by_cases hgt : b.log2 < x.log2
        · have hins : insertBits (b::bs) x = x :: b :: bs := by simp [insertBits, hx0, hgt]
          rw [hins]
          change xorSpan (b::bs) x ∨ xorSpan (b::bs) (x ^^^ x)
          exact Or.inr (by simpa using xorSpan_zero (b::bs))
        · by_cases heq : x.log2 == b.log2
          · have hins : insertBits (b::bs) x = b :: insertBits bs (x ^^^ b) := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            change xorSpan (insertBits bs (x ^^^ b)) x ∨
              xorSpan (insertBits bs (x ^^^ b)) (x ^^^ b)
            exact Or.inr (ih (x ^^^ b))
          · have hins : insertBits (b::bs) x = b :: insertBits bs x := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            change xorSpan (insertBits bs x) x ∨ xorSpan (insertBits bs x) (x ^^^ b)
            exact Or.inl (ih x)

private theorem xorSpan_mono {S : List Nat} {x : Nat} (h : xorSpan S x) (y : Nat) :
    xorSpan (insertBits S y) x := by
  induction S generalizing x y with
  | nil =>
      by_cases hy : y = 0
      · simpa [insertBits, hy] using h
      · simp [insertBits, hy, xorSpan]
        rw [h]
        simp
  | cons b bs ih =>
      by_cases hy0 : y = 0
      · simpa [insertBits, hy0] using h
      · by_cases hgt : b.log2 < y.log2
        · have hins : insertBits (b::bs) y = y :: b :: bs := by simp [insertBits, hy0, hgt]
          rw [hins]
          change xorSpan (b::bs) x ∨ xorSpan (b::bs) (x ^^^ y)
          exact Or.inl h
        · by_cases heq : y.log2 == b.log2
          · have hins : insertBits (b::bs) y = b :: insertBits bs (y ^^^ b) := by
              simp [insertBits, hy0, hgt, heq]
            rw [hins]
            change xorSpan (insertBits bs (y ^^^ b)) x ∨
              xorSpan (insertBits bs (y ^^^ b)) (x ^^^ b)
            dsimp [xorSpan] at h
            rcases h with h | h
            · exact Or.inl (ih h (y ^^^ b))
            · exact Or.inr (ih h (y ^^^ b))
          · have hins : insertBits (b::bs) y = b :: insertBits bs y := by
              simp [insertBits, hy0, hgt, heq]
            rw [hins]
            change xorSpan (insertBits bs y) x ∨ xorSpan (insertBits bs y) (x ^^^ b)
            dsimp [xorSpan] at h
            rcases h with h | h
            · exact Or.inl (ih h y)
            · exact Or.inr (ih h y)

private theorem log2_lt_two_pow {x k : Nat} (hx : x ≠ 0) (h : x < 2^k) : x.log2 < k :=
  (Nat.log2_lt hx).mpr h

private theorem xor_same_log2_lt (x b : Nat) (hx : x ≠ 0) (hb : b ≠ 0)
    (heq : x.log2 = b.log2) : x ^^^ b < 2 ^ b.log2 := by
  apply Nat.lt_pow_two_of_testBit
  intro i hi
  by_cases hib : i = b.log2
  · subst i
    have hxbit : x.testBit b.log2 = true := by rw [← heq]; exact Nat.testBit_log2 hx
    rw [Nat.testBit_xor, hxbit, Nat.testBit_log2 hb]
    simp
  · have hib' : b.log2 < i := by omega
    have hxlt : x < 2^i := (Nat.log2_lt hx).mp (by omega)
    have hblt : b < 2^i := (Nat.log2_lt hb).mp hib'
    simp [Nat.testBit_xor, Nat.testBit_lt_two_pow hxlt, Nat.testBit_lt_two_pow hblt]

private theorem xor_lt_two_pow {a b k : Nat} (ha : a < 2^k) (hb : b < 2^k) : a ^^^ b < 2^k := by
  apply Nat.lt_pow_two_of_testBit
  intro i hi
  have hai : a < 2^i := by
    have hp : 2^k ≤ 2^i := Nat.pow_le_pow_right (by omega) hi
    omega
  have hbi : b < 2^i := by
    have hp : 2^k ≤ 2^i := Nat.pow_le_pow_right (by omega) hi
    omega
  simp [Nat.testBit_xor, Nat.testBit_lt_two_pow hai, Nat.testBit_lt_two_pow hbi]

private theorem xor_cancel_right (x b : Nat) : (x ^^^ b) ^^^ b = x := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp [Nat.testBit_xor]

private theorem xorSpan_lt_two_pow {S : List Nat} {k x : Nat}
    (hall : ∀ s ∈ S, s < 2^k) (hx : xorSpan S x) : x < 2^k := by
  induction S generalizing x with
  | nil =>
      simp [xorSpan] at hx
      rw [hx]
      exact Nat.two_pow_pos k
  | cons b bs ih =>
      dsimp [xorSpan] at hx
      have hb : b < 2^k := hall b (by simp)
      have htail : ∀ s ∈ bs, s < 2^k := by intro s hs; exact hall s (by simp [hs])
      rcases hx with hx | hx
      · exact ih htail hx
      · have hz := ih htail hx
        have hz2 : (x ^^^ b) ^^^ b < 2^k := xor_lt_two_pow hz hb
        rw [xor_cancel_right] at hz2
        exact hz2

private theorem insertBits_below (S : List Nat) (x k : Nat)
    (hS : ∀ s ∈ S, s.log2 < k) (hx : x = 0 ∨ x.log2 < k) :
    ∀ s ∈ insertBits S x, s.log2 < k := by
  induction S generalizing x with
  | nil =>
      by_cases hx0 : x = 0
      · simp [insertBits, hx0]
      · have hxlt : x.log2 < k := by
          rcases hx with h | h
          · exact False.elim (hx0 h)
          · exact h
        simp [insertBits, hx0, hxlt]
  | cons b bs ih =>
      by_cases hx0 : x = 0
      · simpa [insertBits, hx0] using hS
      · have hxlt : x.log2 < k := by
          rcases hx with h | h
          · exact False.elim (hx0 h)
          · exact h
        have hb : b.log2 < k := hS b (by simp)
        by_cases hgt : b.log2 < x.log2
        · have hins : insertBits (b::bs) x = x :: b :: bs := by simp [insertBits, hx0, hgt]
          rw [hins]
          intro s hs
          rw [List.mem_cons] at hs
          rcases hs with rfl | hs
          · exact hxlt
          · rw [List.mem_cons] at hs
            rcases hs with rfl | hs
            · exact hb
            · exact hS s (by simp [hs])
        · by_cases heq : x.log2 == b.log2
          · have hins : insertBits (b::bs) x = b :: insertBits bs (x ^^^ b) := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            have hxb0 : x ^^^ b = 0 ∨ (x ^^^ b).log2 < k := by
              by_cases hz : x ^^^ b = 0
              · exact Or.inl hz
              · by_cases hb0 : b = 0
                · subst b
                  simpa using (Or.inr hxlt : x = 0 ∨ x.log2 < k)
                · have heq' : x.log2 = b.log2 := beq_iff_eq.mp heq
                  have hlt : x ^^^ b < 2 ^ b.log2 := xor_same_log2_lt x b hx0 hb0 heq'
                  exact Or.inr (log2_lt_two_pow hz (by
                    have hbk : b.log2 < k := hb
                    have hp : 2 ^ b.log2 ≤ 2 ^ k := Nat.pow_le_pow_right (by omega) (Nat.le_of_lt hbk)
                    omega))
            have htail : ∀ s ∈ insertBits bs (x ^^^ b), s.log2 < k :=
              ih (x ^^^ b) (by intro s hs; exact hS s (by simp [hs])) hxb0
            intro s hs
            rw [List.mem_cons] at hs
            rcases hs with rfl | hs
            · exact hb
            · exact htail s hs
          · have hins : insertBits (b::bs) x = b :: insertBits bs x := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            have htail : ∀ s ∈ insertBits bs x, s.log2 < k :=
              ih x (by intro s hs; exact hS s (by simp [hs])) (Or.inr hxlt)
            intro s hs
            rw [List.mem_cons] at hs
            rcases hs with rfl | hs
            · exact hb
            · exact htail s hs
private def canonical (S : List Nat) : Prop :=
  S.Pairwise (fun a b => a.log2 > b.log2) ∧ ∀ s ∈ S, s ≠ 0

private theorem insertBits_canonical (S : List Nat) (x : Nat) (hc : canonical S) :
    canonical (insertBits S x) := by
  rcases hc with ⟨hdec, hnz⟩
  induction S generalizing x with
  | nil =>
      constructor
      · by_cases hx : x = 0 <;> simp [insertBits, hx]
      · intro s hs
        by_cases hx : x = 0
        · simp [insertBits, hx] at hs
        · simp [insertBits, hx] at hs
          rcases hs with rfl
          exact hx
  | cons b bs ih =>
      have hbsCan : canonical bs :=
        ⟨(List.pairwise_cons.mp hdec).2, by intro s hs; exact hnz s (by simp [hs])⟩
      have hbhead : ∀ s ∈ bs, b.log2 > s.log2 := (List.pairwise_cons.mp hdec).1
      have hbne : b ≠ 0 := hnz b (by simp)
      by_cases hx0 : x = 0
      · have hc' : canonical (b::bs) := ⟨hdec, hnz⟩
        simpa [insertBits, hx0] using hc'
      · by_cases hgt : b.log2 < x.log2
        · have hins : insertBits (b::bs) x = x :: b :: bs := by simp [insertBits, hx0, hgt]
          rw [hins]
          constructor
          · simp only [List.pairwise_cons]
            constructor
            · intro s hs
              rw [List.mem_cons] at hs
              rcases hs with rfl | hs
              · exact hgt
              · have := hbhead s hs
                omega
            · exact List.pairwise_cons.mp hdec
          · intro s hs
            rw [List.mem_cons] at hs
            rcases hs with rfl | hs
            · exact hx0
            · exact hnz s (by simp [hs])
        · by_cases heqT : x.log2 == b.log2
          · have heq : x.log2 = b.log2 := beq_iff_eq.mp heqT
            have hins : insertBits (b::bs) x = b :: insertBits bs (x ^^^ b) := by
              simp [insertBits, hx0, hgt, heqT]
            rw [hins]
            have hxb : x ^^^ b = 0 ∨ (x ^^^ b).log2 < b.log2 := by
              by_cases hz : x ^^^ b = 0
              · exact Or.inl hz
              · have hlt := xor_same_log2_lt x b hx0 hbne heq
                exact Or.inr (log2_lt_two_pow hz hlt)
            have hbelow : ∀ s ∈ insertBits bs (x ^^^ b), s.log2 < b.log2 :=
              insertBits_below bs (x ^^^ b) b.log2 hbhead hxb
            have htailCan := ih (x ^^^ b) (List.pairwise_cons.mp hdec).2
              (by intro s hs; exact hnz s (by simp [hs]))
            constructor
            · simp only [List.pairwise_cons]
              constructor
              · intro s hs
                exact hbelow s hs
              · exact htailCan.1
            · intro s hs
              rw [List.mem_cons] at hs
              rcases hs with rfl | hs
              · exact hbne
              · exact htailCan.2 s hs
          · have hnelog : x.log2 ≠ b.log2 := by
              intro hh
              exact heqT (by simp [hh])
            have hxlt : x.log2 < b.log2 := by omega
            have hins : insertBits (b::bs) x = b :: insertBits bs x := by
              simp [insertBits, hx0, hgt, heqT]
            rw [hins]
            have hbelow : ∀ s ∈ insertBits bs x, s.log2 < b.log2 :=
              insertBits_below bs x b.log2 hbhead (Or.inr hxlt)
            have htailCan := ih x (List.pairwise_cons.mp hdec).2
              (by intro s hs; exact hnz s (by simp [hs]))
            constructor
            · simp only [List.pairwise_cons]
              constructor
              · intro s hs
                exact hbelow s hs
              · exact htailCan.1
            · intro s hs
              rw [List.mem_cons] at hs
              rcases hs with rfl | hs
              · exact hbne
              · exact htailCan.2 s hs

private theorem foldl_canonical (V S : List Nat) (hS : canonical S) :
    canonical (V.foldl insertBits S) := by
  induction V generalizing S with
  | nil => simpa using hS
  | cons x xs ih =>
      exact ih (insertBits S x) (insertBits_canonical S x hS)

private theorem fold_canonical (V : List Nat) : canonical (V.foldl insertBits []) := by
  apply foldl_canonical V []
  constructor
  · simp
  · intro s hs
    simp at hs
private theorem insertBits_length (S : List Nat) (x : Nat) (hc : canonical S) :
    (insertBits S x).length = S.length + (if xorSpan S x then 0 else 1) := by
  rcases hc with ⟨hdec, hnz⟩
  induction S generalizing x with
  | nil =>
      by_cases hx : x = 0
      · simp [insertBits, xorSpan, hx]
      · simp [insertBits, xorSpan, hx]
  | cons b bs ih =>
      have hbsCan : canonical bs :=
        ⟨(List.pairwise_cons.mp hdec).2, by intro s hs; exact hnz s (by simp [hs])⟩
      have hbhead : ∀ s ∈ bs, b.log2 > s.log2 := (List.pairwise_cons.mp hdec).1
      have hbne : b ≠ 0 := hnz b (by simp)
      by_cases hx0 : x = 0
      · subst x
        have hs0 := xorSpan_zero (b::bs)
        simp [insertBits, hs0]
      · by_cases hgt : b.log2 < x.log2
        · have hnot : ¬ xorSpan (b::bs) x := by
            intro hsx
            have hp : 2 ^ x.log2 ≤ x := Nat.ge_two_pow_of_testBit (Nat.testBit_log2 hx0)
            have hbval : b < 2 ^ x.log2 := (Nat.log2_lt hbne).mp hgt
            have htail : ∀ s ∈ bs, s < 2 ^ x.log2 := by
              intro s hs
              have hslog := hbhead s hs
              exact (Nat.log2_lt (hnz s (by simp [hs]))).mp (by omega)
            have hall : ∀ s ∈ b::bs, s < 2 ^ x.log2 := by
              intro s hs
              rw [List.mem_cons] at hs
              rcases hs with rfl | hs
              · exact hbval
              · exact htail s hs
            have hspan := xorSpan_lt_two_pow hall hsx
            omega
          have hins : insertBits (b::bs) x = x :: b :: bs := by simp [insertBits, hx0, hgt]
          rw [hins]
          simp only [List.length_cons]
          simp [hnot]
        · by_cases heqT : x.log2 == b.log2
          · have heq : x.log2 = b.log2 := beq_iff_eq.mp heqT
            have hiff : xorSpan (b::bs) x ↔ xorSpan bs (x ^^^ b) := by
              constructor
              · intro hs
                dsimp [xorSpan] at hs
                rcases hs with hs | hs
                · have htail : ∀ s ∈ bs, s < 2 ^ b.log2 := by
                    intro s hmem
                    have hsl := hbhead s hmem
                    exact (Nat.log2_lt (by have := hnz s (by simp [hmem]); omega)).mp hsl
                  have hlt := xorSpan_lt_two_pow htail hs
                  have hxge : 2 ^ b.log2 ≤ x := by
                    rw [← heq]
                    exact Nat.ge_two_pow_of_testBit (Nat.testBit_log2 hx0)
                  omega
                · exact hs
              · intro hs
                exact Or.inr hs
            have hins : insertBits (b::bs) x = b :: insertBits bs (x ^^^ b) := by
              simp [insertBits, hx0, hgt, heqT]
            rw [hins]
            simp only [List.length_cons]
            rw [ih (x ^^^ b) (List.pairwise_cons.mp hdec).2
              (by intro s hs; exact hnz s (by simp [hs]))]
            rw [show (if xorSpan (b :: bs) x then 0 else 1) =
                (if xorSpan bs (x ^^^ b) then 0 else 1) by rw [hiff]]
            omega
          · have hnelog : x.log2 ≠ b.log2 := by intro hh; exact heqT (by simp [hh])
            have hxlt : x.log2 < b.log2 := by omega
            have hiff : xorSpan (b::bs) x ↔ xorSpan bs x := by
              constructor
              · intro hs
                dsimp [xorSpan] at hs
                rcases hs with hs | hs
                · exact hs
                · have htail : ∀ s ∈ bs, s < 2 ^ b.log2 := by
                    intro s hmem
                    have hsl := hbhead s hmem
                    exact (Nat.log2_lt (by have := hnz s (by simp [hmem]); omega)).mp hsl
                  have hlt := xorSpan_lt_two_pow htail hs
                  have hbitx : x.testBit b.log2 = false := by
                    exact Nat.testBit_lt_two_pow ((Nat.log2_lt hx0).mp hxlt)
                  have hbitb : b.testBit b.log2 = true := Nat.testBit_log2 hbne
                  have hbit : (x ^^^ b).testBit b.log2 = true := by
                    rw [Nat.testBit_xor, hbitx, hbitb]
                    simp
                  have hge : 2 ^ b.log2 ≤ x ^^^ b := Nat.ge_two_pow_of_testBit hbit
                  omega
              · intro hs
                exact Or.inl hs
            have hins : insertBits (b::bs) x = b :: insertBits bs x := by
              simp [insertBits, hx0, hgt, heqT]
            rw [hins]
            simp only [List.length_cons]
            rw [ih x (List.pairwise_cons.mp hdec).2
              (by intro s hs; exact hnz s (by simp [hs]))]
            rw [show (if xorSpan (b :: bs) x then 0 else 1) =
                (if xorSpan bs x then 0 else 1) by rw [hiff]]
            omega

private theorem foldl_len_le (V S : List Nat) (hc : canonical S) :
    (V.foldl insertBits S).length ≤ S.length + V.length := by
  induction V generalizing S with
  | nil => simp
  | cons x xs ih =>
      have hc' : canonical (insertBits S x) := insertBits_canonical S x hc
      simp only [List.foldl_cons]
      have hstep := insertBits_length S x hc
      have hdelta : (insertBits S x).length ≤ S.length + 1 := by
        by_cases hsx : xorSpan S x
        · rw [hstep]
          simp [hsx]
        · rw [hstep]
          simp [hsx]
      have ih' := ih (insertBits S x) hc'
      simp only [List.length_cons]
      omega

private theorem fold_len_le (V : List Nat) : (V.foldl insertBits []).length ≤ V.length := by
  have h := foldl_len_le V [] (by
    constructor
    · simp
    · intro s hs
      simp at hs)
  simpa using h

private theorem canonical_len_le {S : List Nat} {k : Nat} (hc : canonical S)
    (hlt : ∀ s ∈ S, s < 2^k) : S.length ≤ k := by
  induction S generalizing k with
  | nil => simp
  | cons b bs ih =>
      have hbne : b ≠ 0 := hc.2 b (by simp)
      have hblog : b.log2 < k := log2_lt_two_pow hbne (hlt b (by simp))
      have hbsCan : canonical bs :=
        ⟨(List.pairwise_cons.mp hc.1).2, by intro s hs; exact hc.2 s (by simp [hs])⟩
      have hbslt : ∀ s ∈ bs, s < 2 ^ b.log2 := by
        intro s hs
        have hsl := (List.pairwise_cons.mp hc.1).1 s hs
        exact (Nat.log2_lt (hc.2 s (by simp [hs]))).mp hsl
      have ih' := ih hbsCan hbslt
      simp only [List.length_cons]
      omega

private theorem insertBits_lt (S : List Nat) (x k : Nat)
    (hS : ∀ s ∈ S, s < 2^k) (hx : x < 2^k) :
    ∀ s ∈ insertBits S x, s < 2^k := by
  induction S generalizing x with
  | nil =>
      by_cases hx0 : x = 0
      · simp [insertBits, hx0]
      · simp [insertBits, hx0, hx]
  | cons b bs ih =>
      by_cases hx0 : x = 0
      · simpa [insertBits, hx0] using hS
      · have hb : b < 2^k := hS b (by simp)
        by_cases hgt : b.log2 < x.log2
        · have hins : insertBits (b::bs) x = x :: b :: bs := by simp [insertBits, hx0, hgt]
          rw [hins]
          intro s hs
          rw [List.mem_cons] at hs
          rcases hs with rfl | hs
          · exact hx
          · rw [List.mem_cons] at hs
            rcases hs with rfl | hs
            · exact hb
            · exact hS s (by simp [hs])
        · by_cases heq : x.log2 == b.log2
          · have hins : insertBits (b::bs) x = b :: insertBits bs (x ^^^ b) := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            have hxb : x ^^^ b < 2^k := xor_lt_two_pow hx hb
            have htail := ih (x ^^^ b) (by intro s hs; exact hS s (by simp [hs])) hxb
            intro s hs
            rw [List.mem_cons] at hs
            rcases hs with rfl | hs
            · exact hb
            · exact htail s hs
          · have hins : insertBits (b::bs) x = b :: insertBits bs x := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            have htail := ih x (by intro s hs; exact hS s (by simp [hs])) hx
            intro s hs
            rw [List.mem_cons] at hs
            rcases hs with rfl | hs
            · exact hb
            · exact htail s hs

private theorem foldl_lt (V S : List Nat) (k : Nat)
    (hS : ∀ s ∈ S, s < 2^k) (hV : ∀ v ∈ V, v < 2^k) :
    ∀ s ∈ V.foldl insertBits S, s < 2^k := by
  induction V generalizing S with
  | nil => simpa using hS
  | cons x xs ih =>
      simp only [List.foldl_cons]
      apply ih
      · exact insertBits_lt S x k hS (hV x (by simp))
      · intro v hv
        exact hV v (by simp [hv])

private theorem column_bound (V : List Nat) (k : Nat) (hV : ∀ v ∈ V, v < 2^k) :
    (V.foldl insertBits []).length ≤ k := by
  have hc := fold_canonical V
  have hlt := foldl_lt V [] k (by intro s hs; simp at hs) hV
  exact canonical_len_le hc hlt

private def decode : List Nat → Nat → Nat
  | [], _ => 0
  | g :: gs, y => (if y.testBit 0 then g else 0) ^^^ decode gs (y / 2)

private theorem decode_zero (G : List Nat) : decode G 0 = 0 := by
  induction G with
  | nil => rfl
  | cons g gs ih => simp [decode, ih]

private theorem decode_xor (G : List Nat) (y z : Nat) :
    decode G (y ^^^ z) = decode G y ^^^ decode G z := by
  induction G generalizing y z with
  | nil => simp [decode]
  | cons g gs ih =>
      simp only [decode]
      rw [Nat.testBit_xor, Nat.xor_div_two]
      by_cases hy : y.testBit 0 = true <;> by_cases hz : z.testBit 0 = true <;>
        simp [hy, hz, ih] <;>
        apply Nat.eq_of_testBit_eq <;> intro i <;>
        simp [Nat.testBit_xor, Bool.xor_assoc, Bool.xor_comm, Bool.xor_left_comm]
private theorem span_rep (G : List Nat) {x : Nat} (h : xorSpan G x) :
    ∃ y, y < 2 ^ G.length ∧ decode G y = x := by
  induction G generalizing x with
  | nil =>
      simp [xorSpan] at h
      subst x
      exact ⟨0, by simp [decode]⟩
  | cons g gs ih =>
      dsimp [xorSpan] at h
      rcases h with h | h
      · rcases ih h with ⟨y, hy, hdec⟩
        refine ⟨2 * y, ?_, ?_⟩
        · simp only [List.length_cons, Nat.pow_succ]
          exact show 2 * y < 2 ^ gs.length * 2 by omega
        · have hb0 : (2 * y).testBit 0 = false := by
            rw [Nat.testBit_zero]
            simp
          have hdiv : (2 * y) / 2 = y := by omega
          simp [decode, hb0, hdiv, hdec]
      · rcases ih h with ⟨y, hy, hdec⟩
        refine ⟨2 * y + 1, ?_, ?_⟩
        · simp only [List.length_cons, Nat.pow_succ]
          exact show 2 * y + 1 < 2 ^ gs.length * 2 by omega
        · have hb0 : (2 * y + 1).testBit 0 = true := by
            rw [Nat.testBit_zero]
            simp
          have hdiv : (2 * y + 1) / 2 = y := by omega
          simp [decode, hb0, hdiv, hdec]
          rw [Nat.xor_comm g (x ^^^ g)]
          exact xor_cancel_right x g

private theorem decode_bound {G : List Nat} {k : Nat} (hG : ∀ s ∈ G, s < 2^k) (y : Nat) :
    decode G y < 2^k := by
  induction G generalizing y with
  | nil => simp [decode]; exact Nat.two_pow_pos k
  | cons g gs ih =>
      have hg : g < 2^k := hG g (by simp)
      have htail : ∀ s ∈ gs, s < 2^k := by intro s hs; exact hG s (by simp [hs])
      by_cases hb : y.testBit 0 = true
      · simp [decode, hb]
        exact xor_lt_two_pow hg (ih htail (y/2))
      · simp [decode, hb]
        exact ih htail (y/2)

private theorem canonical_indep {B : List Nat} {y : Nat} (hc : canonical B)
    (hy : y < 2^B.length) (hdec : decode B y = 0) : y = 0 := by
  induction B generalizing y with
  | nil =>
      simp at hy
      omega
  | cons b bs ih =>
      have hbne : b ≠ 0 := hc.2 b (by simp)
      have hbsCan : canonical bs :=
        ⟨(List.pairwise_cons.mp hc.1).2, by intro s hs; exact hc.2 s (by simp [hs])⟩
      have hbslt : ∀ s ∈ bs, s < 2 ^ b.log2 := by
        intro s hs
        have hsl := (List.pairwise_cons.mp hc.1).1 s hs
        exact (Nat.log2_lt (hc.2 s (by simp [hs]))).mp hsl
      have htaildec := decode_bound hbslt (y/2)
      have hbslen : bs.length ≤ b.log2 := canonical_len_le hbsCan hbslt
      have hp : 2 ^ bs.length ≤ 2 ^ b.log2 := Nat.pow_le_pow_right (by omega) hbslen
      have hy' : y < 2 ^ b.log2 * 2 := by
        have hy0 : y < 2 ^ bs.length * 2 := by
          simpa [List.length_cons, Nat.pow_succ] using hy
        omega
      by_cases hbit : y.testBit 0 = true
      · have hdec' : b ^^^ decode bs (y/2) = 0 := by simpa [decode, hbit] using hdec
        have heq : b = decode bs (y/2) := by
          have hh := congrArg (fun z : Nat => z ^^^ decode bs (y/2)) hdec'
          rw [xor_cancel_right] at hh
          simpa using hh
        have hbge : 2 ^ b.log2 ≤ b := Nat.ge_two_pow_of_testBit (Nat.testBit_log2 hbne)
        omega
      · have hdec' : decode bs (y/2) = 0 := by simpa [decode, hbit] using hdec
        have hne1 : ¬ y % 2 = 1 := by
          intro hh
          apply hbit
          rw [Nat.testBit_zero]
          simp [hh]
        have hmod : y % 2 = 0 := by
          have hm : y % 2 < 2 := Nat.mod_lt y (by omega)
          omega
        have hdecomp : 2 * (y / 2) = y := by
          have hmad := Nat.mod_add_div y 2
          omega
        have hyhalf : y / 2 < 2 ^ bs.length := by
          have hy'' : y < 2 ^ bs.length * 2 := by
            simpa [List.length_cons, Nat.pow_succ] using hy
          omega
        have hhalf0 : y / 2 = 0 := ih hbsCan hyhalf hdec'
        rw [← hdecomp, hhalf0]

private theorem decode_ite (G : List Nat) (z y : Nat) :
    decode G (if y.testBit 0 then z else 0) = if y.testBit 0 then decode G z else 0 := by
  by_cases hb : y.testBit 0 = true <;> simp [hb, decode_zero]

private theorem decode_decode (G Y : List Nat) (y : Nat) :
    decode G (decode Y y) = decode (Y.map fun z => decode G z) y := by
  induction Y generalizing y with
  | nil =>
      simp only [decode]
      exact decode_zero G
  | cons z zs ih =>
      change decode G ((if y.testBit 0 then z else 0) ^^^ decode zs (y / 2)) =
        (if y.testBit 0 then decode G z else 0) ^^^
          decode (zs.map fun z => decode G z) (y / 2)
      rw [decode_xor, decode_ite, ih]

private theorem xorSpan_append (S T : List Nat) {x : Nat} (h : xorSpan S x) :
    xorSpan (S ++ T) x := by
  induction S generalizing x with
  | nil =>
      cases h
      exact xorSpan_zero T
  | cons b bs ih =>
      dsimp [xorSpan] at h ⊢
      rcases h with h | h
      · exact Or.inl (ih h)
      · exact Or.inr (ih (x := x ^^^ b) h)

private theorem xorSpan_mem {S : List Nat} {x : Nat} (h : x ∈ S) : xorSpan S x := by
  induction S with
  | nil => simp at h
  | cons b bs ih =>
      rw [List.mem_cons] at h
      rcases h with rfl | h
      · exact Or.inr (by simpa using xorSpan_zero bs)
      · exact Or.inl (ih h)

private theorem insertBits_span_mem {G S : List Nat} {x : Nat}
    (hS : ∀ s ∈ S, xorSpan G s) (hx : xorSpan G x) :
    ∀ s ∈ insertBits S x, xorSpan G s := by
  induction S generalizing x with
  | nil =>
      by_cases hx0 : x = 0
      · simp [insertBits, hx0]
      · simp [insertBits, hx0]
        exact hx
  | cons hd bs ih =>
      by_cases hx0 : x = 0
      · simpa [insertBits, hx0] using hS
      · by_cases hgt : hd.log2 < x.log2
        · have hins : insertBits (hd::bs) x = x :: hd :: bs := by simp [insertBits, hx0, hgt]
          rw [hins]
          intro s hs
          rw [List.mem_cons] at hs
          rcases hs with hsd | hs
          · subst s
            exact hx
          · rw [List.mem_cons] at hs
            rcases hs with hsd | hs
            · subst s
              exact hS hd (by simp)
            · exact hS s (by simp [hs])
        · by_cases heq : x.log2 == hd.log2
          · have hins : insertBits (hd::bs) x = hd :: insertBits bs (x ^^^ hd) := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            have hxb : xorSpan G (x ^^^ hd) := xorSpan_xor hx (hS hd (by simp))
            have htail := ih (by intro s hs; exact hS s (by simp [hs])) hxb
            intro s hs
            rw [List.mem_cons] at hs
            rcases hs with hsd | hs
            · subst s
              exact hS hd (by simp)
            · exact htail s hs
          · have hins : insertBits (hd::bs) x = hd :: insertBits bs x := by
              simp [insertBits, hx0, hgt, heq]
            rw [hins]
            have htail := ih (by intro s hs; exact hS s (by simp [hs])) hx
            intro s hs
            rw [List.mem_cons] at hs
            rcases hs with hsd | hs
            · subst s
              exact hS hd (by simp)
            · exact htail s hs

private theorem span_fold (V G S : List Nat) (hS : ∀ s ∈ S, xorSpan G s)
    (hV : ∀ v ∈ V, xorSpan G v) :
    ∀ s ∈ V.foldl insertBits S, xorSpan G s := by
  induction V generalizing S with
  | nil => simpa using hS
  | cons v vs ih =>
      simp only [List.foldl_cons]
      apply ih
      · exact insertBits_span_mem hS (hV v (by simp))
      · intro w hw
        exact hV w (by simp [hw])

private theorem rowBits_succ (m : Nat) (v : Vec) :
    rowBits (m+1) v = rowBits m v + (if v m % 2 == 1 then 2^m else 0) := by
  simp [rowBits, List.range_succ]

private theorem rowBits_lt (m : Nat) (v : Vec) : rowBits m v < 2^m := by
  induction m with
  | zero => simp [rowBits]
  | succ m ih =>
      rw [rowBits_succ]
      rw [Nat.pow_succ]
      by_cases h : v m % 2 == 1 <;> simp [h] <;> omega

private theorem rowBits_testBit (m j : Nat) (v : Vec) :
    (rowBits m v).testBit j = decide (j < m && v j % 2 == 1) := by
  induction m generalizing j with
  | zero => simp [rowBits]
  | succ m ih =>
      rw [rowBits_succ]
      by_cases hp : v m % 2 == 1
      · rw [if_pos hp]
        by_cases hjm : j < m
        · rw [Nat.add_comm, Nat.testBit_two_pow_add_gt hjm, ih]
          by_cases hv : v j % 2 == 1 <;> simp [hjm, hv] <;> omega
        · by_cases hjeq : j = m
          · subst j
            have hbitfalse := Nat.testBit_lt_two_pow (rowBits_lt m v)
            rw [Nat.add_comm, Nat.testBit_two_pow_add_eq]
            simp [hbitfalse, hp]
          · have hval : rowBits m v + 2 ^ m < 2 ^ j := by
              have h1 : rowBits m v + 2 ^ m < 2 ^ m + 2 ^ m := by
                have := rowBits_lt m v
                omega
              have h2 : 2 ^ m + 2 ^ m = 2 ^ (m + 1) := by rw [Nat.two_pow_succ]
              have h3 : 2 ^ (m + 1) ≤ 2 ^ j := Nat.pow_le_pow_right (by omega) (by omega)
              omega
            rw [Nat.testBit_lt_two_pow hval]
            simp [show ¬ j < m + 1 by omega]
      · rw [if_neg hp]
        simp only [Nat.add_zero]
        rw [ih]
        by_cases hjeq : j = m
        · subst j
          simp [hp]
        · by_cases hjm : j < m
          · simp [hjm, show j < m + 1 by omega]
          · simp [show ¬ j < m by omega, show ¬ j < m + 1 by omega]

private theorem parity_mul (a b : Int) : ((a * b) % 2 == 1) = ((a % 2 == 1) && (b % 2 == 1)) := by
  rw [Int.mul_emod]
  have ha0 := Int.emod_nonneg a (by omega : (2:Int) ≠ 0)
  have ha1 := Int.emod_lt a (by omega : (2:Int) ≠ 0)
  have hb0 := Int.emod_nonneg b (by omega : (2:Int) ≠ 0)
  have hb1 := Int.emod_lt b (by omega : (2:Int) ≠ 0)
  have haca : a % 2 = 0 ∨ a % 2 = 1 := by omega
  have hbcb : b % 2 = 0 ∨ b % 2 = 1 := by omega
  rcases haca with ha0' | ha1'
  · rcases hbcb with hb0' | hb1'
    · simp [ha0', hb0']
    · simp [ha0', hb1']
  · rcases hbcb with hb0' | hb1'
    · simp [ha1', hb0']
    · simp [ha1', hb1']

private theorem parity_add (a b : Int) : ((a + b) % 2 == 1) = ((a % 2 == 1) ^^ (b % 2 == 1)) := by
  rw [Int.add_emod]
  have ha0 := Int.emod_nonneg a (by omega : (2:Int) ≠ 0)
  have ha1 := Int.emod_lt a (by omega : (2:Int) ≠ 0)
  have hb0 := Int.emod_nonneg b (by omega : (2:Int) ≠ 0)
  have hb1 := Int.emod_lt b (by omega : (2:Int) ≠ 0)
  have haca : a % 2 = 0 ∨ a % 2 = 1 := by omega
  have hbcb : b % 2 = 0 ∨ b % 2 = 1 := by omega
  rcases haca with ha0' | ha1'
  · rcases hbcb with hb0' | hb1'
    · simp [ha0', hb0']
    · simp [ha0', hb1']
  · rcases hbcb with hb0' | hb1'
    · simp [ha1', hb0']
    · simp [ha1', hb1']

private theorem rowBits_add (m : Nat) (u v : Vec) :
    rowBits m (fun j => u j + v j) = rowBits m u ^^^ rowBits m v := by
  apply Nat.eq_of_testBit_eq
  intro j
  rw [Nat.testBit_xor]
  rw [rowBits_testBit, rowBits_testBit, rowBits_testBit, parity_add]
  by_cases hj : j < m
  · by_cases hu : u j % 2 == 1 <;> by_cases hv : v j % 2 == 1 <;> simp [hj, hu, hv]
  · simp [hj]

private theorem rowBits_mul (m : Nat) (c : Int) (v : Vec) :
    rowBits m (fun j => c * v j) = if c % 2 == 1 then rowBits m v else 0 := by
  apply Nat.eq_of_testBit_eq
  intro j
  by_cases hc : c % 2 == 1
  · rw [if_pos hc]
    rw [rowBits_testBit, rowBits_testBit, parity_mul]
    simp [hc]
  · rw [if_neg hc]
    have hne : c % 2 ≠ 1 := by intro heq; exact hc (by simp [heq])
    have hc0 : c % 2 = 0 := by
      have h0 := Int.emod_nonneg c (by omega : (2:Int) ≠ 0)
      have h1 := Int.emod_lt c (by omega : (2:Int) ≠ 0)
      omega
    have hx : ((c * v j) % 2 == 1) = false := by
      rw [parity_mul]
      simp [hc]
    rw [rowBits_testBit, hx]
    simp

private theorem rowBits_zero (m : Nat) : rowBits m (fun _ => (0 : Int)) = 0 := by
  apply Nat.eq_of_testBit_eq
  intro j
  rw [rowBits_testBit]
  simp

private theorem rowBits_total (n m : Nat) (w : Nat → Vec) :
    rowBits m (fun j => total n (fun h => w h j)) =
      (List.range n).foldl (fun acc h => acc ^^^ rowBits m (w h)) 0 := by
  induction n with
  | zero => simp [total, rowBits_zero]
  | succ n ih =>
      rw [show (fun j => total (n+1) (fun h => w h j)) =
          (fun j => total n (fun h => w h j) + w n j) by
            funext j
            exact total_succ n (fun h => w h j)]
      rw [rowBits_add, ih]
      simp [List.range_succ, List.foldl_append]

private theorem rowA_span (n m r : Nat) (l t : Mat) (i : Nat) :
    xorSpan ((List.range r).map fun h => rowBits m (t h))
      (rowBits m (fun j => dot r (l i) (fun h => t h j))) := by
  induction r with
  | zero =>
      change xorSpan [] (rowBits m (fun _ => (0 : Int)))
      rw [rowBits_zero]
      exact xorSpan_zero []
  | succ r ih =>
      have hdot : (fun j => dot (r+1) (l i) (fun h => t h j)) =
          (fun j => total r (fun h => l i h * t h j) + l i r * t r j) := by
        funext j
        rw [dot, total_succ]
      rw [hdot]
      let tailRow := rowBits m (fun j => total r (fun h => l i h * t h j))
      let rowR := rowBits m (t r)
      have htail : xorSpan ((List.range r).map fun h => rowBits m (t h)) tailRow := by
        dsimp [tailRow]
        exact ih
      have htailFull : xorSpan (((List.range r).map fun h => rowBits m (t h)) ++ [rowR]) tailRow :=
        xorSpan_append _ _ htail
      have hrowFull : xorSpan (((List.range r).map fun h => rowBits m (t h)) ++ [rowR]) rowR :=
        xorSpan_mem (show rowR ∈ ((List.range r).map fun h => rowBits m (t h)) ++ [rowR] by simp [rowR])
      by_cases hp : l i r % 2 == 1
      · have hsum : rowBits m (fun j => total r (fun h => l i h * t h j) + l i r * t r j) =
            tailRow ^^^ rowR := by
          dsimp [tailRow, rowR]
          rw [rowBits_add, rowBits_mul]
          simp [hp]
        rw [hsum]
        have hlist : (List.range (r+1)).map (fun h => rowBits m (t h)) =
            ((List.range r).map fun h => rowBits m (t h)) ++ [rowR] := by
          simp [List.range_succ, rowR]
        rw [hlist]
        exact xorSpan_xor htailFull hrowFull
      · have hsum : rowBits m (fun j => total r (fun h => l i h * t h j) + l i r * t r j) =
            tailRow := by
          dsimp [tailRow, rowR]
          rw [rowBits_add, rowBits_mul]
          simp [hp]
        rw [hsum]
        have hlist : (List.range (r+1)).map (fun h => rowBits m (t h)) =
            ((List.range r).map fun h => rowBits m (t h)) ++ [rowR] := by
          simp [List.range_succ, rowR]
        rw [hlist]
        exact htailFull

private def rfold (V : List Nat) : List Nat :=
  V.foldr (fun x acc => insertBits acc x) []

private theorem rfold_canonical (V : List Nat) : canonical (rfold V) := by
  induction V with
  | nil =>
      constructor
      · simp [rfold]
      · intro s hs; simp [rfold] at hs
  | cons x xs ih =>
      dsimp [rfold] at ih ⊢
      exact insertBits_canonical (xs.foldr (fun x acc => insertBits acc x) []) x ih

private theorem rfold_lt (V : List Nat) (k : Nat) (hV : ∀ v ∈ V, v < 2^k) :
    ∀ s ∈ rfold V, s < 2^k := by
  induction V with
  | nil => simp [rfold]
  | cons x xs ih =>
      dsimp [rfold] at ih ⊢
      apply insertBits_lt
      · exact ih (by intro v hv; exact hV v (by simp [hv]))
      · exact hV x (by simp)

private theorem rfold_len_le (V : List Nat) (k : Nat) (hV : ∀ v ∈ V, v < 2^k) :
    (rfold V).length ≤ k := by
  have hc := rfold_canonical V
  have hlt := rfold_lt V k hV
  exact canonical_len_le hc hlt

private theorem xorSpan_of_span {G C : List Nat} {x : Nat}
    (hG : ∀ g ∈ G, xorSpan C g) (hx : xorSpan G x) : xorSpan C x := by
  induction G generalizing x with
  | nil =>
      cases hx
      exact xorSpan_zero C
  | cons g gs ih =>
      dsimp [xorSpan] at hx
      rcases hx with hx | hx
      · exact ih (by intro s hs; exact hG s (by simp [hs])) hx
      · have hres := xorSpan_xor (hG g (by simp))
            (ih (by intro s hs; exact hG s (by simp [hs])) hx)
        rw [show g ^^^ (x ^^^ g) = x by
          rw [Nat.xor_comm g (x ^^^ g)]
          exact xor_cancel_right x g] at hres
        exact hres

private theorem rfold_span (V : List Nat) : ∀ s ∈ rfold V, xorSpan V s := by
  induction V with
  | nil => intro s hs; simp [rfold] at hs
  | cons z zs ih =>
      dsimp [rfold]
      apply insertBits_span_mem
      · intro s hs
        dsimp [xorSpan]
        exact Or.inl (ih s hs)
      · exact xorSpan_mem (by simp)
private theorem rfold_len_eq_of_indep (V : List Nat)
    (hind : ∀ y, y < 2^V.length → decode V y = 0 → y = 0) :
    (rfold V).length = V.length := by
  induction V with
  | nil => simp [rfold]
  | cons z zs ih =>
      have htailIndep : ∀ y, y < 2^zs.length → decode zs y = 0 → y = 0 := by
        intro y hy hdec
        have hyFull : 2*y < 2^(z::zs).length := by
          simp only [List.length_cons, Nat.pow_succ]
          exact show 2*y < 2^zs.length*2 by omega
        have hdecFull : decode (z::zs) (2*y) = 0 := by
          have hb0 : (2*y).testBit 0 = false := by rw [Nat.testBit_zero]; simp
          have hdiv : (2*y)/2 = y := by omega
          rw [decode, hb0, hdiv, hdec]
          simp
        have hz0 := hind (2*y) hyFull hdecFull
        omega
      have ih' := ih htailIndep
      have hlenRF : (rfold zs).length = zs.length := ih'
      have hnot : ¬ xorSpan (rfold zs) z := by
        intro hs
        have hsZs : xorSpan zs z := xorSpan_of_span (rfold_span zs) hs
        rcases span_rep zs hsZs with ⟨y, hy, hdec⟩
        have hyFull : 2*y+1 < 2^(z::zs).length := by
          simp only [List.length_cons, Nat.pow_succ]
          exact show 2*y+1 < 2^zs.length*2 by omega
        have hdecFull : decode (z::zs) (2*y+1) = 0 := by
          have hb0 : (2*y+1).testBit 0 = true := by rw [Nat.testBit_zero]; simp
          have hdiv : (2*y+1)/2 = y := by omega
          rw [decode, hb0, hdiv]
          have hzEq : z = decode zs y := hdec.symm
          simpa [hzEq]
        have hz0 := hind (2*y+1) hyFull hdecFull
        omega
      have hc := rfold_canonical zs
      dsimp [rfold] at ih' ⊢
      change (insertBits (rfold zs) z).length = zs.length + 1
      rw [insertBits_length (rfold zs) z hc]
      rw [if_neg hnot]
      omega
private theorem span_mono_fold (V S : List Nat) {x : Nat} (hx : xorSpan S x) :
    xorSpan (V.foldl insertBits S) x := by
  induction V generalizing S with
  | nil => simpa using hx
  | cons v vs ih =>
      simp only [List.foldl_cons]
      exact ih (insertBits S v) (xorSpan_mono hx v)

private theorem span_fold_acc (V S : List Nat) :
    ∀ v ∈ V, xorSpan (V.foldl insertBits S) v := by
  induction V generalizing S with
  | nil => intro v hv; cases hv
  | cons x xs ih =>
      intro v hv
      rw [List.mem_cons] at hv
      rw [List.foldl_cons]
      rcases hv with h | hv
      · subst v
        exact span_mono_fold xs (insertBits S x) (xorSpan_insert_self S x)
      · exact ih (insertBits S x) v hv


theorem RankRejection_sound :
    TinylambOpt.ResearchGoals.Frontiers.WorkloadBases.RankRejection.Goal := by
  intro n m r l t
  let G : List Nat := (List.range r).map fun h => rowBits m (t h)
  let C : List Nat := G.foldl insertBits []
  let V : List Nat := (List.range n).map fun i =>
    rowBits m (fun j => dot r (l i) (fun h => t h j))
  have hcC : canonical C := by dsimp [C]; exact fold_canonical G
  have hlenC : C.length ≤ r := by
    dsimp [C, G]
    have h := fold_len_le ((List.range r).map fun h => rowBits m (t h))
    simpa using h
  have hGspan : ∀ s ∈ G, xorSpan C s := by
    dsimp [C]
    exact span_fold_acc G []
  have hVspan : ∀ v ∈ V, xorSpan C v := by
    intro v hv
    obtain ⟨i, hi, hvi⟩ := List.getElem_of_mem hv
    have hspanI : xorSpan C (rowBits m (fun j => dot r (l i) (fun h => t h j))) := by
      have hspanG := rowA_span n m r l t i
      change xorSpan G (rowBits m (fun j => dot r (l i) (fun h => t h j))) at hspanG
      exact xorSpan_of_span hGspan hspanG
    rw [← hvi]
    simpa [V] using hspanI
  let B : List Nat := V.foldl insertBits []
  have hcB : canonical B := by dsimp [B]; exact fold_canonical V
  have hBspan : ∀ b ∈ B, xorSpan C b := by
    dsimp [B]
    exact span_fold V C [] (by intro s hs; cases hs) hVspan
  let yOf : Nat → Nat := fun b =>
    if hb : b ∈ B then Classical.choose (span_rep C (hBspan b hb)) else 0
  let Y : List Nat := B.map yOf
  have hYdecode : (Y.map fun y => decode C y) = B := by
    dsimp [Y]
    rw [List.map_map]
    change (B.map fun b => decode C (yOf b)) = B
    rw [show B = B.map (fun b : Nat => b) by simp]
    simp only [List.map_map]
    apply List.map_congr_left
    intro b hb
    dsimp [yOf]
    rw [dif_pos hb]
    exact (Classical.choose_spec (span_rep C (hBspan b hb))).2
  have hYlt : ∀ y ∈ Y, y < 2 ^ C.length := by
    intro y hy
    obtain ⟨b, hb, rfl⟩ := List.mem_map.mp hy
    dsimp [Y, yOf] at *
    rw [dif_pos hb]
    exact (Classical.choose_spec (span_rep C (hBspan b hb))).1
  have hYindep : ∀ y, y < 2^Y.length → decode Y y = 0 → y = 0 := by
    intro y hy hdec
    have hyB : y < 2^B.length := by simpa [Y] using hy
    have hdecB : decode B y = 0 := by
      have hdd := decode_decode C Y y
      rw [hYdecode] at hdd
      rw [hdec, decode_zero] at hdd
      exact hdd.symm
    exact canonical_indep hcB hyB hdecB
  have hYlenEq : (rfold Y).length = Y.length := rfold_len_eq_of_indep Y hYindep
  have hYlenB : Y.length = B.length := by simp [Y]
  have hYle : Y.length ≤ C.length := by
    rw [← hYlenEq]
    exact rfold_len_le Y C.length hYlt
  have hBfinal : B.length ≤ r := by omega
  dsimp [binaryRank]
  rw [← List.foldl_map]
  simpa [B, V] using hBfinal

end

end TinylambOpt.ResearchProofs.Frontiers.WorkloadBases
