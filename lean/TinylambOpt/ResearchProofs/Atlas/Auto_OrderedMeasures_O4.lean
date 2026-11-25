import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O4

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Generic list and arithmetic helpers -/

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using eq_comm

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [List.countP_cons, h, ih]

private theorem countP_beq_comm (a : Int) (xs : List Int) :
    xs.countP (fun s => a == s) = xs.countP (fun s => s == a) := by
  apply List.countP_congr
  intro s hs
  simp only [beq_iff_eq]
  exact eq_comm

private theorem range_map_succ {α : Type} (n : Nat) (f : Nat → α) :
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = (n + 1).succ by omega, List.range_succ, List.map_append, ih]
      simp [List.range_succ]

private theorem flatMap_range_succ {α : Type} (f : Nat → List α) (n : Nat) :
    (List.range (n + 1)).flatMap f = f 0 ++ (List.range n).flatMap (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = n.succ.succ by omega, List.range_succ, List.flatMap_append]
      rw [ih]
      rw [List.range_succ, List.flatMap_append]
      simp [List.append_assoc]

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun v => a + v) := by
  unfold prefs
  simp only [List.length_cons]
  rw [range_map_succ]
  simp [List.take_succ_cons, List.map_map]

private theorem sum_map_add_const (xs : List Int) (f : Int → Int) (c : Int) :
    (xs.map (fun x => f x + c)).sum = (xs.map f).sum + (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem sum_nonneg_int (xs : List Int) (h : ∀ v ∈ xs, 0 ≤ v) : 0 ≤ xs.sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons]
      have ha := h a (by simp)
      have hi := ih (fun v hv => h v (by simp [hv]))
      omega

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => simp [choose2]
  | succ n => unfold choose2; grind

private theorem max0_add_le (x y : Int) : max 0 (x + y) ≤ max 0 x + max 0 y := by
  by_cases hx : 0 ≤ x
  · by_cases hy : 0 ≤ y
    · rw [Int.max_eq_right (by omega : 0 ≤ x + y), Int.max_eq_right hx, Int.max_eq_right hy]
      omega
    · have hy' : y ≤ 0 := by omega
      have hxy : x + y ≤ x := by omega
      rw [Int.max_eq_right hx, Int.max_eq_left hy']
      exact Int.max_le.mpr ⟨by simpa using hx, by simpa using hxy⟩
  · have hx' : x ≤ 0 := by omega
    by_cases hy : 0 ≤ y
    · have hxy : x + y ≤ y := by omega
      rw [Int.max_eq_left hx', Int.max_eq_right hy]
      exact Int.max_le.mpr ⟨by simpa using hy, by simpa using hxy⟩
    · have hy' : y ≤ 0 := by omega
      rw [Int.max_eq_left (by omega : x + y ≤ 0), Int.max_eq_left hx', Int.max_eq_left hy']
      omega

private theorem max0_sub_identity (a b : Int) : max 0 (b - a) - max 0 (a - b) = b - a := by
  by_cases h : a ≤ b
  · have h1 : 0 ≤ b - a := by omega
    have h2 : a - b ≤ 0 := by omega
    rw [Int.max_eq_right h1, Int.max_eq_left h2]
    omega
  · have h1 : b - a ≤ 0 := by omega
    have h2 : 0 ≤ a - b := by omega
    rw [Int.max_eq_left h1, Int.max_eq_right h2]
    omega

private theorem max0_neg_identity (v : Int) : 2 * max 0 v = (v.natAbs : Int) + v := by
  by_cases h : 0 ≤ v
  · rw [Int.max_eq_right h, Int.natAbs_of_nonneg h]
    omega
  · have hv : v ≤ 0 := by omega
    have hneg : 0 ≤ -v := by omega
    have hn : v.natAbs = (-v).natAbs := by simpa using (Int.natAbs_neg (-v))
    have hc : ((-v).natAbs : Int) = -v := Int.natAbs_of_nonneg hneg
    rw [Int.max_eq_left hv]
    have hc' : (v.natAbs : Int) = -v := by rw [hn, hc]
    rw [hc']
    omega

/-! ### Adjacent-edge algebra -/

private def edgeBridge (x y : Series) : List (Int × Int) :=
  (x.getLast?.bind (fun a => y.head?.map (fun b => (a, b)))).toList

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem adj_append (x y : Series) :
    adj x ++ edgeBridge x y ++ adj y = adj (x ++ y) := by
  induction x with
  | nil => simp [adj, edgeBridge]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [adj, edgeBridge]
          | cons b ys =>
              simp only [List.cons_append, List.nil_append, adj_cons₂]
              simp [adj, edgeBridge, List.getLast?_cons, List.head?_cons,
                List.append_assoc]
      | cons b bs =>
          simp only [List.cons_append]
          have hbridge : edgeBridge (a :: b :: bs) y = edgeBridge (b :: bs) y := by
            simp [edgeBridge, List.getLast?_cons]
          have hi : adj (b :: bs) ++ edgeBridge (b :: bs) y ++ adj y =
              adj (b :: (bs ++ y)) := by
            simpa only [List.cons_append] using ih
          rw [adj_cons₂, hbridge, adj_cons₂]
          simpa only [List.append_assoc, List.cons_append] using
            congrArg (fun l => (a, b) :: l) hi

private theorem seam_eq_bridge_sum (x y : Series) (f : Int → Int → Int) :
    seam x y f = ((edgeBridge x y).map (fun p : Int × Int => f p.1 p.2)).sum := by
  cases x <;> cases y <;> simp [seam, edgeBridge, List.getLast?_cons, List.head?_cons]

private theorem sum_delta (x : Series) :
    (delta x).sum = x.getLast?.getD 0 - x.head?.getD 0 := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b bs =>
          simp only [delta, adj_cons₂, List.map_cons, List.sum_cons,
            List.head?_cons, List.getLast?_cons, Option.getD_some] at ih ⊢
          omega

/-! ### PositiveJumps -/

private def pj (x : Series) : Int := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.measure x
private def tv (x : Series) : Int := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure x

private theorem pj_cons₂ (a b : Int) (xs : Series) :
    pj (a :: b :: xs) = max 0 (b - a) + pj (b :: xs) := by
  simp [pj, PositiveJumps.measure, delta, adj_cons₂]

private theorem pj_nil : pj [] = 0 := by rfl

private theorem pj_singleton (a : Int) : pj [a] = 0 := by
  simp [pj, PositiveJumps.measure, delta, adj]

private theorem pj_append (x y : Series) :
    pj (x ++ y) = pj x + pj y + seam x y (fun a b => max 0 (b - a)) := by
  unfold pj PositiveJumps.measure delta
  rw [← adj_append]
  simp only [List.map_append, List.map_map, Function.comp_def, List.sum_append]
  rw [seam_eq_bridge_sum]
  omega

private theorem pj_append_singleton (u : Series) (a : Int) :
    pj (u ++ [a]) = pj u +
      (match u.getLast? with | none => 0 | some b => max 0 (a - b)) := by
  rw [pj_append, pj_singleton]
  cases u with
  | nil => simp [seam, pj_nil]
  | cons b bs => simp [seam, List.getLast?_cons]

private theorem tv_cons₂ (a b : Int) (xs : Series) :
    tv (a :: b :: xs) = ((b - a).natAbs : Int) + tv (b :: xs) := by
  simp [tv, TotalVariation.measure, delta, adj_cons₂]

private def pjFrom (prev : Option Int) (xs : Series) : Int :=
  pj xs + match prev, xs.head? with
  | some a, some b => max 0 (b - a)
  | _, _ => 0

private theorem pjFrom_none (xs : Series) : pjFrom none xs = pj xs := by
  cases xs <;> simp [pjFrom]

private theorem pjFrom_none_cons (a : Int) (xs : Series) :
    pjFrom none (a :: xs) = pjFrom (some a) xs := by
  cases xs <;> simp [pjFrom, pj_cons₂, pj_nil, pj_singleton] <;> omega

private theorem pj_cons_eq_from (a : Int) (xs : Series) :
    pj (a :: xs) = pjFrom (some a) xs := by
  rw [← pjFrom_none_cons, pjFrom_none]

private theorem pjFrom_cons_prev (prev : Option Int) (a : Int) (xs : Series) :
    pjFrom prev (a :: xs) =
      (match prev with | none => 0 | some p => max 0 (a - p)) + pjFrom (some a) xs := by
  cases prev <;> cases xs <;> simp [pjFrom, pj_cons₂, pj_nil, pj_singleton] <;> omega

private theorem pjFrom_cons_ge (prev : Option Int) (a : Int) (xs : Series) :
    pjFrom prev xs ≤ pjFrom prev (a :: xs) := by
  cases prev with
  | none =>
      rw [pjFrom_none, pjFrom_none]
      cases xs with
      | nil => simp [pjFrom, pj_nil, pj_singleton]
      | cons b bs =>
          rw [pj_cons₂]
          have hm : 0 ≤ max 0 (b - a) := by omega
          omega
  | some p =>
      cases xs with
      | nil =>
          simp [pjFrom, pj_nil, pj_singleton]
          omega
      | cons b bs =>
          simp only [pjFrom, List.head?_cons]
          rw [pj_cons₂]
          have htri := max0_add_le (b - a) (a - p)
          have heq : (b - a) + (a - p) = b - p := by omega
          rw [heq] at htri
          omega

theorem sound_PositiveJumps_BoundaryJump :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.BoundaryJump.Goal := by
  intro x y
  change pj (x ++ y) = pj x + pj y + seam x y (fun a b => max 0 (b - a))
  exact pj_append x y

theorem sound_PositiveJumps_ReverseBalance :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.ReverseBalance.Goal := by
  intro x
  change pj x - pj x.reverse = x.getLast?.getD 0 - x.head?.getD 0
  induction x with
  | nil => simp [pj_nil]
  | cons a xs ih =>
      cases xs with
      | nil => simp [pj_singleton]
      | cons b bs =>
          rw [List.reverse_cons, pj_append_singleton, pj_cons₂]
          simp only [List.getLast?_reverse, List.head?_cons, Option.getD_some] at ih ⊢
          rw [show (a :: b :: bs).getLast?.getD 0 = (b :: bs).getLast?.getD 0 by simp]
          have hmax := max0_sub_identity a b
          omega

theorem sound_PositiveJumps_SharedMagnitude :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.SharedMagnitude.Goal := by
  intro x
  change 2 * pj x = tv x + x.getLast?.getD 0 - x.head?.getD 0
  have tv_nil : tv [] = 0 := by simp [tv, TotalVariation.measure, delta, adj]
  have tv_singleton : ∀ a : Int, tv [a] = 0 := by
    intro a; simp [tv, TotalVariation.measure, delta, adj]
  induction x with
  | nil => simp [pj_nil, tv_nil]
  | cons a xs ih =>
      cases xs with
      | nil => simp [pj_singleton, tv_singleton]
      | cons b bs =>
          rw [pj_cons₂, tv_cons₂]
          rw [show (a :: b :: bs).getLast?.getD 0 = (b :: bs).getLast?.getD 0 by simp]
          simp only [List.head?_cons, Option.getD_some]
          simp only [List.head?_cons, Option.getD_some] at ih
          have hm := max0_neg_identity (b - a)
          omega

theorem sound_PositiveJumps_NonincreasingSkip :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.NonincreasingSkip.Goal := by
  intro x hx
  change pj x = 0
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          have hab : b ≤ a := (List.pairwise_cons.mp hx).1 b (by simp)
          have htail : (b :: bs).Pairwise (fun u v : Int => u ≥ v) :=
            (List.pairwise_cons.mp hx).2
          rw [pj_cons₂, ih htail]
          rw [Int.max_eq_left (by omega : b - a ≤ 0)]
          simp

theorem sound_PositiveJumps_FilterBudget :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.FilterBudget.Goal := by
  intro x keep
  change pj (x.filter keep) ≤ pj x
  have haux : ∀ (prev : Option Int) (xs : Series), pjFrom prev (xs.filter keep) ≤ pjFrom prev xs := by
    intro prev xs
    induction xs generalizing prev with
    | nil => simp [pjFrom]
    | cons a xs ih =>
        cases hk : keep a
        · rw [List.filter_cons, if_neg (by simp [hk])]
          exact Int.le_trans (ih prev) (pjFrom_cons_ge prev a xs)
        · rw [List.filter_cons, if_pos hk, pjFrom_cons_prev, pjFrom_cons_prev]
          exact Int.add_le_add_left (ih (some a)) _
  simpa [pjFrom_none] using haux none x

/-! ### ZeroFrames: pair-count and prefix helpers -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem pairs_countP_append (P : Int × Int → Bool) (x y : Series) :
    (pairs (x ++ y)).countP P =
      (pairs x).countP P + (pairs y).countP P + (cart x y).countP P := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.countP_append]
      rw [countP_map_comp, countP_map_comp, List.countP_map]
      simp only [Function.comp_def]
      rw [List.countP_append, ih]
      omega

private def zf (x : Series) : Nat := (windows x).countP (fun w => w.sum == 0)
private def tc (x : Series) : Nat := (pairs x).countP (fun p => p.1 == p.2)

private theorem tc_cons (a : Int) (x : Series) :
    tc (a :: x) = x.countP (fun b => a == b) + tc x := by
  unfold tc
  rw [pairs_cons, List.countP_append, countP_map_comp]

private theorem tc_singleton (a : Int) : tc [a] = 0 := by
  simp [tc, pairs]

private theorem tc_map_add (a : Int) (x : Series) :
    tc (x.map (fun s => a + s)) = tc x := by
  induction x with
  | nil => rfl
  | cons b bs ih =>
      simp only [List.map_cons]
      rw [tc_cons, tc_cons, ih]
      rw [countP_map_comp]
      apply congrArg (fun n => n + tc bs)
      apply List.countP_congr
      intro s hs
      simp only [beq_iff_eq]
      omega

private theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) =
      (List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1)) ++ windows xs := by
  unfold windows
  simp only [List.length_cons]
  rw [flatMap_range_succ]
  have hsub : ∀ i : Nat, xs.length + 1 - (i + 1) = xs.length - i := by intro i; omega
  simp [List.drop_succ_cons, hsub]

private theorem zf_cons (a : Int) (xs : Series) :
    zf (a :: xs) = zf xs +
      ((List.range (xs.length + 1)).map (fun j => a + (xs.take j).sum)).countP (fun s => s == 0) := by
  unfold zf
  rw [windows_cons, List.countP_append]
  have hcongr :
      ((List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1))).countP (fun w => w.sum == 0) =
      ((List.range (xs.length + 1)).map (fun j => a + (xs.take j).sum)).countP (fun s => s == 0) := by
    rw [countP_map_comp, countP_map_comp]
    apply List.countP_congr
    intro j hj
    simp only [beq_iff_eq]
    simp [List.take_succ_cons]
  rw [hcongr]
  omega

private theorem prefix_zero_count (a : Int) (xs : Series) :
    ((List.range (xs.length + 1)).map (fun j => a + (xs.take j).sum)).countP (fun s => s == 0) =
      (if a == 0 then 1 else 0) + (prefs xs).countP (fun s => s == -a) := by
  rw [range_map_succ]
  simp only [List.countP_cons, List.take_zero, List.sum_nil]
  have hfirst : (if (a + 0 == 0) = true then 1 else 0) = (if a == 0 then 1 else 0) := by simp
  rw [hfirst]
  have hmap : (List.range xs.length).map (fun i => a + (xs.take (i + 1)).sum) =
      (prefs xs).map (fun s => a + s) := by
    unfold prefs
    rw [List.map_map]
    rfl
  rw [hmap, countP_map_comp]
  have hc : (prefs xs).countP (fun x => a + x == 0) =
      (prefs xs).countP (fun s => s == -a) := by
    apply List.countP_congr
    intro s hs
    simp only [beq_iff_eq]
    omega
  rw [hc]
  omega

private theorem tc_zero_cons_shift (a : Int) (ys : Series) :
    tc (0 :: a :: (ys.map (fun s => a + s))) =
      (if a == 0 then 1 else 0) + ys.countP (fun s => s == -a) +
        ys.countP (fun s => s == 0) + tc ys := by
  simp only [tc_cons, List.countP_cons]
  rw [tc_map_add]
  have hleft : (ys.map (fun s => a + s)).countP (fun z => 0 == z) =
      ys.countP (fun s => s == -a) := by
    rw [countP_map_comp]
    apply List.countP_congr
    intro s hs
    simp only [beq_iff_eq]
    omega
  have hright : (ys.map (fun s => a + s)).countP (fun z => a == z) =
      ys.countP (fun s => s == 0) := by
    rw [countP_map_comp]
    apply List.countP_congr
    intro s hs
    simp only [beq_iff_eq]
    omega
  rw [hleft, hright]
  have hbeq : (if (0 == a) = true then 1 else 0) = (if a == 0 then 1 else 0) := by
    rw [beq_comm_int 0 a]
  rw [hbeq]
  omega

private theorem zf_eq_tc (x : Series) : zf x = tc (0 :: prefs x) := by
  induction x with
  | nil => simp [zf, tc, windows, prefs, pairs]
  | cons a xs ih =>
      rw [zf_cons, prefix_zero_count, prefs_cons]
      rw [tc_zero_cons_shift]
      rw [tc_cons] at ih
      have ih' : zf xs = (prefs xs).countP (fun s => s == 0) + tc (prefs xs) := by
        rw [ih, countP_beq_comm 0 (prefs xs)]
      rw [ih']
      omega

private theorem tc_eq_zero_iff_nodup (x : Series) : tc x = 0 ↔ x.Nodup := by
  induction x with
  | nil => simp [tc, pairs]
  | cons a xs ih =>
      rw [tc_cons, List.nodup_cons]
      constructor
      · intro h
        have hc : xs.countP (fun b => a == b) = 0 := by omega
        have ht : tc xs = 0 := by omega
        constructor
        · intro hmem
          have hn := (List.countP_eq_zero.mp hc) a hmem
          simp at hn
        · exact ih.mp ht
      · intro h
        rcases h with ⟨hnmem, hnd⟩
        have hc : xs.countP (fun b => a == b) = 0 := by
          apply List.countP_eq_zero.mpr
          intro b hb
          simp only [beq_iff_eq]
          intro heq
          subst b
          exact hnmem hb
        have ht : tc xs = 0 := ih.mpr hnd
        omega

theorem sound_ZeroFrames_PrefixCollision :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames.PrefixCollision.Goal := by
  intro x
  unfold ZeroFrames.measure TiePairs.measure
  change (zf x : Int) = (tc (0 :: prefs x) : Int)
  exact_mod_cast zf_eq_tc x

theorem sound_ZeroFrames_EarliestPrune :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames.EarliestPrune.Goal := by
  intro x
  change (zf x : Int) = 0 ↔ (0 :: prefs x).Nodup
  have hz := zf_eq_tc x
  have hn := tc_eq_zero_iff_nodup (0 :: prefs x)
  constructor
  · intro h
    have h' : zf x = 0 := by exact_mod_cast h
    exact hn.mp (by rw [← hz]; exact h')
  · intro h
    have h' : zf x = 0 := by rw [hz]; exact hn.mpr h
    exact_mod_cast h'

/-! ### ZeroFrames: boundary balances -/

private theorem prefs_nil : prefs [] = [] := rfl

private theorem prefs_length (x : Series) : (prefs x).length = x.length := by
  simp [prefs]

private theorem prefs_append (xs ys : Series) :
    prefs (xs ++ ys) = prefs xs ++ (prefs ys).map (fun v => xs.sum + v) := by
  induction xs with
  | nil => simp [prefs_nil]
  | cons a xs ih =>
      rw [List.cons_append, prefs_cons, prefs_cons, ih]
      simp only [List.map_append, List.map_cons, List.cons_append, List.map_map,
        Function.comp_def, List.sum_cons]
      rw [show (prefs ys).map (fun x => a + (xs.sum + x)) =
          (prefs ys).map (fun v => a + xs.sum + v) by
        apply List.map_congr_left
        intro v hv
        grind]

private theorem suffs_cons (a : Int) (xs : Series) :
    suffs (a :: xs) = (a + xs.sum) :: suffs xs := by
  unfold suffs
  simp only [List.length_cons]
  rw [range_map_succ]
  simp [List.drop_succ_cons, List.sum_cons]

private theorem tc_append (x y : Series) :
    tc (x ++ y) = tc x + tc y + (cart x y).countP (fun p => p.1 == p.2) := by
  unfold tc
  rw [pairs_countP_append]

private theorem countP_map_add_left (a b : Int) (xs : Series) :
    (xs.map (fun s => a + s)).countP (fun s => s == b) = xs.countP (fun s => s == b - a) := by
  rw [countP_map_comp]
  apply List.countP_congr
  intro s hs
  simp only [beq_iff_eq]
  omega

private theorem countP_map_add_right (a b : Int) (xs : Series) :
    (xs.map (fun s => a + s)).countP (fun s => b == s) = xs.countP (fun s => b - a == s) := by
  rw [countP_map_comp]
  apply List.countP_congr
  intro s hs
  simp only [beq_iff_eq]
  omega

private theorem countP_shift_zero (S : Int) (v : List Int) :
    (v.map (fun q => S + q)).countP (fun z => z == 0) = v.countP (fun q => q == -S) := by
  rw [countP_map_comp]
  apply List.countP_congr
  intro q hq
  simp only [beq_iff_eq]
  omega

private theorem cart_pred_shift_cancel (c d : Int) (u v : List Int) :
    (cart u v).countP (fun p => c + p.1 == c + d + p.2) =
      (cart u (v.map (fun q => d + q))).countP (fun p => p.1 == p.2) := by
  induction u with
  | nil => simp [cart]
  | cons a as ih =>
      simp only [cart_cons, List.countP_append, ih]
      have hL : (v.map (fun b => (a, b))).countP
          (fun p => c + p.1 == c + d + p.2) = v.countP (fun q => c + a == c + d + q) := by
        rw [countP_map_comp]
      have hR : ((v.map (fun q => d + q)).map (fun b => (a, b))).countP
          (fun p => p.1 == p.2) = v.countP (fun q => a == d + q) := by
        rw [countP_map_comp, countP_map_comp]
      rw [hL, hR]
      congr 1
      apply List.countP_congr
      intro q hq
      simp only [beq_iff_eq]
      omega

private theorem cross_pair_shift (c d : Int) (u v : List Int) :
    (cart (u.map (fun p => c + p)) (v.map (fun q => d + q))).countP (fun p => p.1 == p.2) =
      (cart u v).countP (fun p => c + p.1 == d + p.2) := by
  induction u with
  | nil => simp [cart]
  | cons a as ih =>
      simp only [List.map_cons, cart_cons, List.countP_append]
      rw [ih]
      have hL : ((v.map (fun q => d + q)).map (fun b => (c + a, b))).countP
          (fun p => p.1 == p.2) = v.countP (fun q => c + a == d + q) := by
        rw [countP_map_comp, countP_map_comp]
      have hR : (v.map (fun b => (a, b))).countP
          (fun p => c + p.1 == d + p.2) = v.countP (fun q => c + a == d + q) := by
        rw [countP_map_comp]
      rw [hL, hR]

private theorem cart_cons_count (a : Int) (u v : Series) :
    (cart (a :: u) v).countP (fun p => p.1 == p.2) =
      v.countP (fun b => a == b) + (cart u v).countP (fun p => p.1 == p.2) := by
  rw [cart_cons, List.countP_append, countP_map_comp]

private theorem cross_count (x y : Series) :
    (cart (0 :: prefs x) ((prefs y).map (fun v => x.sum + v))).countP (fun p => p.1 == p.2) =
      (prefs y).countP (fun v => v == 0) +
        ((suffs x).map (fun s => (prefs y).countP (fun v => v == -s))).sum := by
  induction x with
  | nil =>
      simp only [prefs, suffs, List.length_nil, List.range_zero, List.map_nil,
        List.sum_nil, Int.zero_add]
      rw [cart_cons_count]
      simp only [cart, List.flatMap_nil, List.countP_nil, Nat.add_zero, List.map_id']
      apply List.countP_congr
      intro i hi
      simp only [beq_iff_eq]
      exact eq_comm
  | cons a xs ih =>
      rw [prefs_cons, List.sum_cons, suffs_cons]
      let v : Series := (prefs y).map (fun q => a + xs.sum + q)
      have hv0 : v.countP (fun z => 0 == z) =
          (prefs y).countP (fun q => q == -(a + xs.sum)) := by
        dsimp only [v]
        rw [countP_beq_comm 0 (((prefs y).map (fun q => a + xs.sum + q)))]
        simpa [Int.add_assoc] using countP_shift_zero (a + xs.sum) (prefs y)
      have hv1 : v.countP (fun z => a == z) =
          (prefs y).countP (fun q => q == -xs.sum) := by
        dsimp only [v]
        rw [countP_map_comp]
        apply List.countP_congr
        intro q hq
        simp only [beq_iff_eq]
        omega
      have hthird :
          (cart ((prefs xs).map (fun q => a + q)) v).countP (fun p => p.1 == p.2) =
            (cart (prefs xs) ((prefs y).map (fun q => xs.sum + q))).countP (fun p => p.1 == p.2) := by
        dsimp only [v]
        calc
          _ = (cart (prefs xs) (prefs y)).countP (fun p => a + p.1 == a + xs.sum + p.2) := by
                simpa [Int.add_assoc] using
                  cross_pair_shift a (a + xs.sum) (prefs xs) (prefs y)
          _ = _ := by
                simpa [Int.add_assoc] using cart_pred_shift_cancel a xs.sum (prefs xs) (prefs y)
      rw [cart_cons_count 0 (a :: (prefs xs).map (fun q => a + q)) v,
        cart_cons_count a ((prefs xs).map (fun q => a + q)) v, hv0, hv1, hthird]
      rw [cart_cons_count 0 (prefs xs) ((prefs y).map (fun q => xs.sum + q))] at ih
      have hv0' : ((prefs y).map (fun q => xs.sum + q)).countP (fun z => 0 == z) =
          (prefs y).countP (fun q => q == -xs.sum) := by
        rw [countP_beq_comm 0 ((prefs y).map (fun q => xs.sum + q))]
        exact countP_shift_zero xs.sum (prefs y)
      rw [hv0'] at ih
      rw [List.map_cons, List.sum_cons]
      omega

private theorem zf_append (x y : Series) :
    zf (x ++ y) = zf x + zf y + ((suffs x).map (fun s => (prefs y).count (-s))).sum := by
  have hpref := prefs_append x y
  have hcomb : 0 :: prefs (x ++ y) =
      (0 :: prefs x) ++ ((prefs y).map (fun v => x.sum + v)) := by
    rw [hpref]
    rfl
  rw [zf_eq_tc, hcomb, tc_append, tc_map_add, cross_count]
  rw [← zf_eq_tc x]
  have hy : zf y = (prefs y).countP (fun v => v == 0) + tc (prefs y) := by
    rw [zf_eq_tc, tc_cons, countP_beq_comm 0 (prefs y)]
  have hcnt :
      ((suffs x).map (fun s => (prefs y).countP (fun v => v == -s))).sum =
        ((suffs x).map (fun s => (prefs y).count (-s))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro s hs
    rw [List.count_eq_countP]
  rw [hcnt]
  omega

theorem sound_ZeroFrames_BoundaryBalances :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames.BoundaryBalances.Goal := by
  intro x y
  change (zf (x ++ y) : Int) = (zf x : Int) + (zf y : Int) +
    (((suffs x).map (fun s => (prefs y).count (-s))).sum : Int)
  have h := zf_append x y
  exact_mod_cast h

/-! ### ZeroFrames: nonnegative pages and uniform pages -/

private theorem mem_window_of_mem {x w : Series} (hw : w ∈ windows x) :
    ∀ v ∈ w, v ∈ x := by
  unfold windows at hw
  rcases List.mem_flatMap.mp hw with ⟨i, hi, hwmem⟩
  rcases List.mem_map.mp hwmem with ⟨j, hj, rfl⟩
  intro v hv
  exact List.mem_of_mem_drop (List.mem_of_mem_take hv)

private theorem sum_eq_zero_of_all_zero {w : List Int} (h : ∀ v ∈ w, v = 0) : w.sum = 0 := by
  induction w with
  | nil => rfl
  | cons a as ih =>
      rw [List.sum_cons, h a (by simp), ih (fun v hv => h v (by simp [hv]))]
      simp

private theorem all_zero_of_nonneg_sum_zero {w : List Int} (hv : ∀ v ∈ w, 0 ≤ v)
    (hs : w.sum = 0) : ∀ v ∈ w, v = 0 := by
  induction w with
  | nil => simp
  | cons a as ih =>
      have ha : 0 ≤ a := hv a (by simp)
      have has : ∀ v ∈ as, 0 ≤ v := by intro v h; exact hv v (by simp [h])
      have hsumas : 0 ≤ as.sum := sum_nonneg_int as has
      simp only [List.sum_cons] at hs
      intro v hv'
      rcases List.mem_cons.mp hv' with rfl | hv'
      · omega
      · exact ih has (by omega) v hv'

theorem sound_ZeroFrames_Nonnegative :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames.Nonnegative.Goal := by
  intro x hx
  change (zf x : Int) = ((windows x).countP (fun w => List.all w (fun a => a == 0)) : Int)
  have hnat : zf x = (windows x).countP (fun w => List.all w (fun a => a == 0)) := by
    unfold zf
    apply List.countP_congr
    intro w hw
    rw [List.all_eq_true]
    simp only [beq_iff_eq]
    constructor
    · intro hs
      intro v hv
      exact all_zero_of_nonneg_sum_zero
        (by intro z hz; exact hx z (mem_window_of_mem hw z hz)) (by simpa using hs) v hv
    · intro hall
      exact sum_eq_zero_of_all_zero (by intro v hv; exact hall v hv)
  exact_mod_cast hnat

private theorem tc_append_singleton (x : Series) (a : Int) :
    tc (x ++ [a]) = tc x + x.countP (fun v => v == a) := by
  rw [tc_append, tc_singleton]
  have hcart : (cart x [a]).countP (fun p => p.1 == p.2) = x.countP (fun v => v == a) := by
    induction x with
    | nil => simp [cart]
    | cons v vs ih =>
        rw [cart_cons, List.countP_append, ih]
        simp only [List.countP_cons]
        by_cases h : v = a
        · subst v
          simp [Nat.add_comm]
        · simp [h, beq_iff_eq, Nat.add_comm]
  rw [hcart]
  omega

private theorem prefs_repeated (n : Nat) (a : Int) :
    prefs (repeated n a) = (List.range n).map (fun i => a * ((i + 1 : Nat) : Int)) := by
  induction n with
  | zero => simp [repeated, prefs]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by simp [repeated, List.replicate_succ]]
      rw [prefs_cons, ih]
      rw [List.range_succ_eq_map]
      simp only [List.map_cons, List.map_map, Function.comp_def]
      simp [Nat.zero_add]
      congr 1
      intro i hi
      have hd := Int.mul_add a (↑i + 1) 1
      rw [show a * (↑i + 1 + 1) = a * ((↑i + 1) + 1) by omega]
      rw [hd]
      simp
      omega

private theorem mem_prefs_repeated {n : Nat} {a v : Int}
    (hv : v ∈ prefs (repeated n a)) : ∃ k : Nat, k < n ∧ v = a * ((k + 1 : Nat) : Int) := by
  rw [prefs_repeated] at hv
  rcases List.mem_map.mp hv with ⟨i, hi, rfl⟩
  exact ⟨i, List.mem_range.mp hi, rfl⟩


private theorem tc_mul_progression (n : Nat) (a : Int) :
    tc (0 :: prefs (repeated n a)) = if a == 0 then choose2 (n + 1) else 0 := by
  induction n with
  | zero => simp [prefs, repeated, tc, pairs, choose2]
  | succ n ih =>
      have happ : 0 :: prefs (repeated (n + 1) a) =
          (0 :: prefs (repeated n a)) ++ [a * ((n + 1 : Nat) : Int)] := by
        rw [prefs_repeated (n + 1) a, prefs_repeated n a]
        simpa [List.range_succ, List.map_append]
      rw [happ, tc_append_singleton, ih]
      by_cases ha : a = 0
      · subst a
        simp at ih ⊢
        have htail0 : (prefs (repeated n 0)).countP (fun v => v == 0) =
            (prefs (repeated n 0)).length := by
          apply List.countP_eq_length.mpr
          intro v hv
          rcases mem_prefs_repeated hv with ⟨k, hk, rfl⟩
          simp
        have hlen0 : (prefs (repeated n 0)).length = n := by simp [prefs, repeated]
        have htail : (prefs (repeated n 0)).countP (fun v => v == 0) = n := by
          rw [htail0, hlen0]
        rw [htail]
        rw [choose2_succ (n + 1)]
      · have hc : (0 :: prefs (repeated n a)).countP
            (fun v => v == a * ((n + 1 : Nat) : Int)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro v hv
          rcases List.mem_cons.mp hv with rfl | hv
          · simp only [beq_iff_eq]
            intro hz
            have hz' : a * ((n + 1 : Nat) : Int) = 0 := by omega
            rcases (Int.mul_eq_zero.mp hz') with ha0 | hn0
            · exact ha ha0
            · have : ((n + 1 : Nat) : Int) ≠ 0 := by exact_mod_cast (Nat.succ_ne_zero n)
              exact this hn0
          · rcases mem_prefs_repeated hv with ⟨k, hk, rfl⟩
            simp only [beq_iff_eq]
            intro heq
            have hprod : a * (((k + 1 : Nat) : Int) - ((n + 1 : Nat) : Int)) = 0 := by
              grind
            rcases (Int.mul_eq_zero.mp hprod) with ha0 | hd
            · exact ha ha0
            · have hkeq : k + 1 = n + 1 := by exact_mod_cast (show ((k + 1 : Nat) : Int) = ((n + 1 : Nat) : Int) by omega)
              omega
        rw [hc]
        simp [ha]

theorem sound_ZeroFrames_Uniform :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.ZeroFrames.Uniform.Goal := by
  intro n a
  change (zf (repeated n a) : Int) = if a == 0 then (choose2 (n + 1) : Int) else 0
  rw [show (zf (repeated n a) : Int) = (tc (0 :: prefs (repeated n a)) : Int) by
    exact_mod_cast zf_eq_tc (repeated n a)]
  have hp := tc_mul_progression n a
  by_cases ha : (a == 0) = true
  · simp [ha] at hp ⊢
    exact_mod_cast hp
  · have haf : (a == 0) = false := by cases h : a == 0 <;> simp_all
    simp [haf] at hp ⊢
    exact_mod_cast hp

end TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O4
