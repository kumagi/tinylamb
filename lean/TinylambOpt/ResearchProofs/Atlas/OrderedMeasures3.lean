import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.OrderedMeasures_WorkA

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures3

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Generic adjacent-list algebra -/

private theorem adj_nil : adj ([] : Series) = [] := rfl

private theorem adj_single (a : Int) : adj [a] = [] := rfl

private theorem adj_cons_cons (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := rfl

private def edgeSum (f : Int → Int → Int) (x : Series) : Int :=
  ((adj x).map (fun p => f p.1 p.2)).sum

private theorem edgeSum_nil (f : Int → Int → Int) : edgeSum f [] = 0 := rfl

private theorem edgeSum_single (f : Int → Int → Int) (a : Int) :
    edgeSum f [a] = 0 := rfl

private theorem edgeSum_cons (f : Int → Int → Int) (a : Int) (xs : Series) :
    edgeSum f (a :: xs) = (xs.head?.map (f a)).getD 0 + edgeSum f xs := by
  unfold edgeSum
  cases xs with
  | nil => simp [adj_single, adj_nil]
  | cons b bs =>
      rw [adj_cons_cons]
      simp only [List.map_cons, List.sum_cons, List.head?_cons,
        Option.map_some, Option.getD_some]

private theorem edgeSum_cons_cons (f : Int → Int → Int) (a b : Int) (xs : Series) :
    edgeSum f (a :: b :: xs) = f a b + edgeSum f (b :: xs) := by
  change ((adj (a :: b :: xs)).map (fun p => f p.1 p.2)).sum =
    f a b + ((adj (b :: xs)).map (fun p => f p.1 p.2)).sum
  rw [adj_cons_cons]
  ac_rfl

private theorem seam_left_nil (f : Int → Int → Int) (y : Series) :
    seam [] y f = 0 := rfl

private theorem seam_right_nil (f : Int → Int → Int) (x : Series) :
    seam x [] f = 0 := by
  cases x <;> rfl

private theorem seam_cons_cons (f : Int → Int → Int) (a b : Int) (xs ys : Series) :
    seam (a :: b :: xs) ys f = seam (b :: xs) ys f := rfl

private theorem seam_singleton_left (f : Int → Int → Int) (x y : Series) (a : Int) :
    seam x (a :: y) f = seam x [a] f := by
  cases x <;> simp [seam]

private theorem edgeSum_append (f : Int → Int → Int) (x y : Series) :
    edgeSum f (x ++ y) = edgeSum f x + edgeSum f y + seam x y f := by
  induction x generalizing y with
  | nil => simp [edgeSum, adj_nil, seam]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [edgeSum, adj_nil, adj_single, seam]
          | cons b bs =>
              change edgeSum f (a :: b :: bs) =
                edgeSum f [a] + edgeSum f (b :: bs) + seam [a] (b :: bs) f
              rw [edgeSum_cons_cons, edgeSum_single]
              simp [seam]
              ac_rfl
      | cons b bs =>
          change edgeSum f (a :: b :: (bs ++ y)) =
            edgeSum f (a :: b :: bs) + edgeSum f y + seam (a :: b :: bs) y f
          rw [edgeSum_cons_cons, edgeSum_cons_cons, seam_cons_cons]
          change f a b + edgeSum f ((b :: bs) ++ y) =
            f a b + edgeSum f (b :: bs) + edgeSum f y + seam (b :: bs) y f
          rw [ih]
          ac_rfl

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x - g x)).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sum_map_mul_left {α : Type} (xs : List α) (a : Int) (f : α → Int) :
    (xs.map (fun x => a * f x)).sum = a * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; grind

private theorem sq_eq_mul (a : Int) : a ^ 2 = a * a := by grind

private theorem cast_nat_sub_one (n : Nat) (hn : 0 < n) :
    ((n - 1 : Nat) : Int) = (n : Int) - 1 := by
  cases n with
  | zero => omega
  | succ n => simp

private theorem sum_map_const {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem adj_map (f : Int → Int) (x : Series) :
    adj (x.map f) = (adj x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          simp only [List.map_cons, adj_cons_cons]
          change (f a, f b) :: adj (List.map f (b :: bs)) =
            (f a, f b) :: (adj (b :: bs)).map (fun p => (f p.1, f p.2))
          rw [ih]

private theorem adj_length (x : Series) : (adj x).length = x.length - 1 := by
  simp [adj, List.length_zip]

private theorem adj_pair_sum (x : Series) :
    ((adj x).map (fun p : Int × Int => p.1 + p.2)).sum =
      2 * x.sum - x.head?.getD 0 - x.getLast?.getD 0 := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => simp [adj_single]; grind
      | cons b bs =>
          rw [adj_cons_cons]
          simp only [List.map_cons, List.sum_cons, ih, List.sum_cons,
            List.head?_cons, List.getLast?_cons_cons]
          grind

/-! ### LagProduct -/

theorem sound_LagProduct_BoundaryProduct : LagProduct.BoundaryProduct.Goal := by
  intro x y
  simpa [LagProduct.measure, edgeSum] using edgeSum_append (· * ·) x y

theorem sound_LagProduct_CenterShift : LagProduct.CenterShift.Goal := by
  intro x b hx
  have hshift : LagProduct.measure (shifted b x) =
      ((adj x).map (fun p : Int × Int => (p.1 + b) * (p.2 + b))).sum := by
    simp only [LagProduct.measure, shifted, adj_map, List.map_map, Function.comp_def]
  rw [hshift]
  have hterms : (adj x).map (fun p : Int × Int => (p.1 + b) * (p.2 + b)) =
      (adj x).map (fun p : Int × Int =>
        p.1 * p.2 + (b * (p.1 + p.2) + b ^ 2)) := by
    apply List.map_congr_left
    intro p hp
    grind
  rw [hterms, sum_map_add, sum_map_add, sum_map_mul_left, sum_map_const, adj_pair_sum]
  have hlen : ((adj x).length : Int) = ((x.length - 1 : Nat) : Int) := by
    rw [adj_length]
  simp only [LagProduct.measure]
  rw [hlen]
  grind

private theorem square_energy_identity (x : Series) :
    2 * LagProduct.measure x =
      2 * (x.map (· ^ 2)).sum - x.head?.getD 0 ^ 2 - x.getLast?.getD 0 ^ 2 -
        QuadraticVariation.measure x := by
  induction x with
  | nil => simp [LagProduct.measure, QuadraticVariation.measure, adj, delta]
  | cons a xs ih =>
      cases xs with
      | nil =>
          simp [LagProduct.measure, QuadraticVariation.measure, adj, delta]
          grind
      | cons b bs =>
          have hm : LagProduct.measure (a :: b :: bs) =
              a * b + LagProduct.measure (b :: bs) := by
            simp only [LagProduct.measure, adj_cons_cons, List.map_cons, List.sum_cons]
          have hq : QuadraticVariation.measure (a :: b :: bs) =
              (b - a) ^ 2 + QuadraticVariation.measure (b :: bs) := by
            simp only [QuadraticVariation.measure, delta, adj_cons_cons,
              List.map_cons, List.sum_cons]
          rw [hm, hq]
          simp only [List.map_cons, List.sum_cons, List.head?_cons,
            List.getLast?_cons_cons] at ih ⊢
          grind

theorem sound_LagProduct_EnergyIdentity : LagProduct.EnergyIdentity.Goal := by
  intro x
  exact square_energy_identity x

private theorem edgeHead_replicate (a : Int) (n : Nat) :
    ((repeated n a).head?.map (a * ·)).getD 0 = if n = 0 then 0 else a * a := by
  cases n <;> simp [repeated, List.replicate_succ]

private theorem edgeSum_replicate (a : Int) (n : Nat) :
    edgeSum (· * ·) (repeated n a) = ((n - 1 : Nat) : Int) * a ^ 2 := by
  induction n with
  | zero => simp [edgeSum, repeated, adj]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by rfl, edgeSum_cons, ih,
        edgeHead_replicate]
      cases n with
      | zero => simp [sq_eq_mul]
      | succ m =>
          simp only [ite_eq_right (Nat.succ_ne_zero m), Nat.add_sub_cancel,
            Int.natCast_add, Int.natCast_one, sq_eq_mul]
          rw [Int.add_mul, Int.one_mul]
          ac_rfl

private theorem seam_replicate_append (a b : Int) (zs : Series) (n : Nat) (hn : 0 < n) :
    seam (repeated n a) (repeated n b ++ zs) (· * ·) = a * b := by
  cases n with
  | zero => omega
  | succ n =>
      change seam (List.replicate (n + 1) a)
        (List.replicate (n + 1) b ++ zs) (· * ·) = a * b
      unfold seam
      have hlast : (List.replicate (n + 1) a).getLast? = some a := by
        rw [List.getLast?_replicate]
        simp
      rw [hlast]
      simp only [Option.bind_some, Option.map_some, Option.getD_some,
        List.replicate_succ, List.cons_append, List.head?_cons]

private theorem edgeSum_expand_runs (x : Series) (n : Nat) (hn : 0 < n) :
    edgeSum (· * ·) (x.flatMap (repeated n)) =
      edgeSum (· * ·) x + ((n - 1 : Nat) : Int) * (x.map (· ^ 2)).sum := by
  induction x with
  | nil => simp [edgeSum_nil]
  | cons a xs ih =>
      simp only [List.flatMap_cons]
      rw [edgeSum_append, edgeSum_replicate, ih]
      cases xs with
      | nil => simp [edgeSum_single, seam_right_nil, edgeSum_nil]
      | cons b bs =>
          rw [show (b :: bs).flatMap (repeated n) =
            repeated n b ++ bs.flatMap (repeated n) by rfl]
          rw [seam_replicate_append a b (bs.flatMap (repeated n)) n hn,
            edgeSum_cons_cons, edgeSum_cons]
          simp only [List.map_cons, List.sum_cons]
          have hn' := cast_nat_sub_one n hn
          rw [hn']
          simp only [sq_eq_mul, Int.sub_mul, Int.mul_add,
            Int.mul_one, Int.one_mul]
          ac_rfl

theorem sound_LagProduct_RunStretch : LagProduct.RunStretch.Goal := by
  intro x n hn
  simpa [LagProduct.measure, edgeSum] using edgeSum_expand_runs x n hn

theorem sound_LagProduct_RemoveInterior : LagProduct.RemoveInterior.Goal := by
  intro x y a
  change edgeSum (· * ·) (x ++ y) - edgeSum (· * ·) (x ++ a :: y) = _
  rw [edgeSum_append, edgeSum_append, seam_singleton_left]
  have hsplit : edgeSum (· * ·) (a :: y) =
      edgeSum (· * ·) y + seam [a] y (· * ·) := by
    simpa [edgeSum_single] using edgeSum_append (· * ·) [a] y
  rw [hsplit]
  omega

/-! ### Indexed-list and prefix/suffix helpers -/

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem zipIdx_append {α : Type} (xs ys : List α) (n : Nat) :
    (xs ++ ys).zipIdx n = xs.zipIdx n ++ ys.zipIdx (n + xs.length) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.cons_append, List.zipIdx_cons, List.length_cons, ih]
      congr 1
      ac_rfl

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (a + ·) := by
  unfold prefs
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.take_succ_cons, List.sum_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  simp only [List.sum_nil, Int.add_zero]

private theorem suffs_cons (a : Int) (xs : Series) :
    suffs (a :: xs) = (a + xs.sum) :: suffs xs := by
  unfold suffs
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.drop_succ_cons, List.drop_zero, List.sum_cons,
    Nat.add_zero, Nat.succ_eq_add_one]

private theorem prefixExposure_cons (a : Int) (xs : Series) :
    PrefixExposure.measure (a :: xs) =
      PrefixExposure.measure xs + ((xs.length + 1 : Nat) : Int) * a := by
  unfold PrefixExposure.measure
  rw [prefs_cons]
  simp only [List.map_cons, List.sum_cons]
  rw [sum_map_add, sum_map_const, List.map_id']
  change a + ((prefs xs).length * a + (prefs xs).sum) =
    (prefs xs).sum + ((xs.length + 1 : Nat) : Int) * a
  have hlen : (prefs xs).length = xs.length := by simp [prefs]
  rw [hlen]
  simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
    Int.mul_one, Int.one_mul]
  ac_rfl

private theorem prefixExposure_multiplicities (x : Series) :
    PrefixExposure.measure x =
      (x.zipIdx.map (fun (v, i) => v * ((x.length - i : Nat) : Int))).sum := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [prefixExposure_cons, ih]
      simp only [List.zipIdx_cons, List.map_cons, List.length_cons, List.sum_cons,
        Nat.zero_add, Nat.add_sub_cancel]
      have hmap :
          (xs.zipIdx (0 + 1)).map
              (fun (v, i) => v * ((xs.length + 1 - i : Nat) : Int)) =
            xs.zipIdx.map (fun (v, i) => v * ((xs.length - i : Nat) : Int)) := by
        rw [zipIdx_add, List.map_map]
        apply List.map_congr_left
        intro p hp
        change p.1 * ((xs.length + 1 - (p.2 + 1) : Nat) : Int) =
          p.1 * ((xs.length - p.2 : Nat) : Int)
        have hidx : xs.length + 1 - (p.2 + 1) = xs.length - p.2 := by omega
        rw [hidx]
      rw [hmap]
      simp only [Int.natCast_add, Int.natCast_one, Int.mul_one]
      ac_rfl

private theorem windows_cons_prefix (a : Int) (xs : Series) :
    windows (a :: xs) =
      [[a]] ++ ((List.range xs.length).map (fun i => a :: xs.take (i + 1))) ++ windows xs := by
  unfold windows
  rw [List.length_cons, List.range_succ_eq_map]
  simp only [List.flatMap_cons, List.map_cons, List.flatMap_map, List.map_map,
    Function.comp_def, List.flatMap_append, List.take_succ_cons, List.take_zero,
    Nat.add_zero, Nat.succ_eq_add_one]
  congr 2
  · simp [List.range_succ_eq_map, List.drop_zero, List.take_succ_cons,
      List.take_zero, Nat.add_assoc]
  · funext i
    simp [List.drop_drop, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem frameTotals_cons (a : Int) (xs : Series) :
    FrameTotals.measure (a :: xs) =
      FrameTotals.measure xs + PrefixExposure.measure xs +
        ((xs.length + 1 : Nat) : Int) * a := by
  unfold FrameTotals.measure
  rw [windows_cons_prefix]
  simp only [List.map_append, List.sum_append, List.map_cons, List.sum_cons,
    List.map_map, Function.comp_def]
  rw [sum_map_add, sum_map_const]
  simp only [List.length_map, List.length_range, List.sum_nil, Int.add_zero]
  unfold PrefixExposure.measure prefs
  simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
    Int.mul_one, Int.one_mul]
  ac_rfl

/-! ### FrameTotals -/

theorem sound_FrameTotals_RowIncidence : FrameTotals.RowIncidence.Goal := by
  intro x
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [frameTotals_cons, prefixExposure_multiplicities, ih]
      simp only [List.zipIdx_cons, List.map_cons, List.length_cons, List.sum_cons,
        Nat.zero_add, Nat.add_sub_cancel]
      have hsum :
          (xs.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int) *
            ((xs.length - i : Nat) : Int))).sum +
          (xs.zipIdx.map (fun (v, i) => v * ((xs.length - i : Nat) : Int))).sum =
          ((xs.zipIdx (0 + 1)).map (fun (v, i) =>
            v * ((i + 1 : Nat) : Int) * ((xs.length + 1 - i : Nat) : Int))).sum := by
        rw [zipIdx_add, List.map_map, ← sum_map_add]
        congr 1
        apply List.map_congr_left
        intro p hp
        simp only [Function.comp_def]
        change p.1 * ((p.2 + 1 : Nat) : Int) * ((xs.length - p.2 : Nat) : Int) +
            p.1 * ((xs.length - p.2 : Nat) : Int) =
          p.1 * (((p.2 + 1) + 1 : Nat) : Int) *
            ((xs.length + 1 - (p.2 + 1) : Nat) : Int)
        have hidx : xs.length + 1 - (p.2 + 1) = xs.length - p.2 := by omega
        rw [hidx]
        push_cast
        simp only [Int.mul_add, Int.add_mul, Int.mul_one]
      rw [hsum]
      simp only [Int.natCast_add, Int.natCast_one, Int.mul_one]
      ac_rfl

private theorem zipIdx_values {α : Type} (xs : List α) (n : Nat) :
    (xs.zipIdx n).map Prod.fst = xs := by
  induction xs generalizing n with
  | nil => rfl
  | cons a as ih => simp only [List.zipIdx_cons, List.map_cons, ih]

private theorem sum_eq_zipIdx_values (x : Series) :
    x.sum = (x.zipIdx.map (fun (v, _) => v)).sum := by
  rw [show (x.zipIdx.map (fun (v, _) => v)) = x from zipIdx_values x 0]

private theorem suffs_sum_weights (x : Series) :
    (suffs x).sum =
      (x.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int))).sum := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [suffs_cons]
      simp only [List.sum_cons]
      rw [ih, sum_eq_zipIdx_values]
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons, Int.natCast_add,
        Int.natCast_one, Int.mul_one, Int.mul_add, Nat.zero_add]
      rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add, List.map_map]
      simp only [Function.comp_def, Nat.zero_add, Nat.add_zero, Int.natCast_add,
        Int.natCast_one, Int.mul_one, Int.mul_add, Int.add_mul, Int.mul_zero,
        Int.add_zero]
      rw [Int.add_assoc, ← sum_map_add]
      simp
      apply congrArg List.sum
      apply List.map_congr_left
      intro p hp
      ac_rfl

private theorem choose2_step (n : Nat) :
    choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  simp only [show n + 2 - 1 = n + 1 by omega, show n + 1 - 1 = n by omega]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind,
    Nat.mul_comm 2 (n + 1), Nat.add_mul_div_right _ _ (by decide)]

private theorem choose3_step (n : Nat) :
    choose3 (n + 3) = choose3 (n + 2) + choose2 (n + 2) := by
  unfold choose3 choose2
  simp only [show n + 3 - 1 = n + 2 by omega,
    show n + 3 - 2 = n + 1 by omega, show n + 2 - 1 = n + 1 by omega,
    show n + 2 - 2 = n by omega]
  have hb : 2 ∣ (n + 2) * (n + 1) := by
    rcases Nat.mod_two_eq_zero_or_one n with h | h
    · have hd : 2 ∣ n + 2 := by omega
      exact Nat.dvd_mul_right_of_dvd hd (n + 1)
    · have hd : 2 ∣ n + 1 := by omega
      exact Nat.dvd_mul_left_of_dvd hd (n + 2)
  have h2 : 2 ∣ (n + 2) * (n + 1) * n := Nat.dvd_mul_right_of_dvd hb n
  have h3 : 3 ∣ (n + 2) * (n + 1) * n := by
    have hm := Nat.mod_lt n (by decide : 0 < 3)
    rcases (show n % 3 = 0 ∨ n % 3 = 1 ∨ n % 3 = 2 by omega) with h | h | h
    · have hd : 3 ∣ n := Nat.dvd_of_mod_eq_zero h
      simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using
        Nat.dvd_mul_right_of_dvd hd ((n + 2) * (n + 1))
    · have hd : 3 ∣ n + 2 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using
        Nat.dvd_mul_right_of_dvd hd ((n + 1) * n)
    · have hd : 3 ∣ n + 1 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using
        Nat.dvd_mul_left_of_dvd hd ((n + 2) * n)
  have hc : 6 ∣ (n + 2) * (n + 1) * n :=
    (by decide : Nat.Coprime 2 3).mul_dvd_of_dvd_of_dvd h2 h3
  have h3B : 6 ∣ 3 * ((n + 2) * (n + 1)) := by
    rcases hb with ⟨k, hk⟩
    refine ⟨k, ?_⟩
    rw [hk]
    simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  have hAC : (n + 3) * (n + 2) * (n + 1) =
      (n + 2) * (n + 1) * n + 3 * ((n + 2) * (n + 1)) := by grind
  rw [hAC, Nat.add_div (by decide),
    Nat.mod_eq_zero_of_dvd hc, Nat.mod_eq_zero_of_dvd h3B]
  simp only [Nat.add_zero]
  rcases hb with ⟨k, hk⟩
  rw [hk]
  simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]

private theorem prefixExposure_deposit (b : Int) (x : Series) :
    PrefixExposure.measure (shifted b x) =
      PrefixExposure.measure x + b * (choose2 (x.length + 1) : Int) := by
  induction x with
  | nil => simp [PrefixExposure.measure, shifted, prefs, choose2]
  | cons a xs ih =>
      change PrefixExposure.measure ((a + b) :: shifted b xs) =
        PrefixExposure.measure (a :: xs) + b * (choose2 ((a :: xs).length + 1) : Int)
      rw [prefixExposure_cons, prefixExposure_cons, ih]
      simp only [List.length_cons]
      have hs := choose2_step xs.length
      have hc : (choose2 (xs.length + 2) : Int) =
          (choose2 (xs.length + 1) : Int) + (xs.length + 1 : Nat) := by
        exact_mod_cast hs
      rw [hc]
      have hlen : (shifted b xs).length = xs.length := by simp [shifted]
      rw [hlen]
      simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
        Int.mul_one, Int.one_mul]
      ac_rfl

theorem sound_FrameTotals_PrefixSuffix : FrameTotals.PrefixSuffix.Goal := by
  intro x y
  rw [sound_FrameTotals_RowIncidence, sound_FrameTotals_RowIncidence,
    sound_FrameTotals_RowIncidence, suffs_sum_weights]
  have hy : (prefs y).sum =
      (y.zipIdx.map (fun (v, i) => v * ((y.length - i : Nat) : Int))).sum := by
    simpa [PrefixExposure.measure] using prefixExposure_multiplicities y
  rw [hy]
  simp only [List.length_append, zipIdx_append, List.map_append, List.sum_append,
    Nat.zero_add]
  have hsplit :
      ((y.zipIdx x.length).map (fun (v, i) =>
        v * ((i + 1 : Nat) : Int) * ((x.length + y.length - i : Nat) : Int))).sum =
      (y.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int) *
        ((y.length - i : Nat) : Int))).sum +
      (x.length : Int) * (y.zipIdx.map (fun (v, i) =>
        v * ((y.length - i : Nat) : Int))).sum := by
    have hz := zipIdx_add y 0 x.length
    simp only [Nat.zero_add] at hz
    rw [hz, List.map_map]
    simp only [Function.comp_def, Nat.add_zero, Nat.zero_add]
    rw [← sum_map_mul_left, ← sum_map_add]
    congr 1
    apply List.map_congr_left
    intro p hp
    have hidx : x.length + y.length - (p.2 + x.length) = y.length - p.2 := by omega
    change p.1 * ((p.2 + x.length + 1 : Nat) : Int) *
        ((x.length + y.length - (p.2 + x.length) : Nat) : Int) =
      p.1 * ((p.2 + 1 : Nat) : Int) * ((y.length - p.2 : Nat) : Int) +
        (x.length : Int) * (p.1 * ((y.length - p.2 : Nat) : Int))
    rw [hidx]
    push_cast
    simp only [Int.mul_add, Int.add_mul, Int.mul_one]
    ac_rfl
  rw [hsplit]
  have hx :
      (x.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int) *
        ((x.length + y.length - i : Nat) : Int))).sum =
      (x.zipIdx.map (fun (v, i) => v * ((i + 1 : Nat) : Int) *
        ((x.length - i : Nat) : Int))).sum +
      (y.length : Int) * (x.zipIdx.map (fun (v, i) =>
        v * ((i + 1 : Nat) : Int))).sum := by
    rw [← sum_map_mul_left, ← sum_map_add]
    congr 1
    apply List.map_congr_left
    intro p hp
    cases p with
    | mk v i =>
        have hi : i < x.length := List.snd_lt_of_mem_zipIdx hp
        have hidx : x.length + y.length - i = (x.length - i) + y.length := by omega
        change v * ((i + 1 : Nat) : Int) * ((x.length + y.length - i : Nat) : Int) =
          v * ((i + 1 : Nat) : Int) * ((x.length - i : Nat) : Int) +
            (y.length : Int) * (v * ((i + 1 : Nat) : Int))
        rw [hidx]
        push_cast
        simp only [Int.mul_add, Int.add_mul, Int.mul_one]
        ac_rfl
  rw [hx]
  simp only [size]
  ac_rfl

theorem sound_FrameTotals_LocalCorrection : FrameTotals.LocalCorrection.Goal := by
  intro x y a b
  rw [sound_FrameTotals_RowIncidence, sound_FrameTotals_RowIncidence]
  simp only [List.length_append, List.length_cons, zipIdx_append, List.zipIdx_cons,
    List.map_append, List.map_cons, List.sum_append, List.sum_cons, Nat.zero_add]
  have hidx : x.length + (y.length + 1) - x.length = y.length + 1 := by omega
  rw [hidx]
  simp only [size, Int.natCast_add, Int.natCast_one, Int.mul_add, Int.add_mul,
    Int.sub_mul, Int.sub_eq_add_neg, Int.neg_add, Int.neg_mul, Int.mul_one, Int.one_mul]
  omega

theorem sound_FrameTotals_UniformShift : FrameTotals.UniformShift.Goal := by
  intro x b
  induction x with
  | nil => simp [FrameTotals.measure, shifted, windows, choose3]
  | cons a xs ih =>
      change FrameTotals.measure ((a + b) :: shifted b xs) =
        FrameTotals.measure (a :: xs) + b * (choose3 ((a :: xs).length + 2) : Int)
      rw [frameTotals_cons, frameTotals_cons, prefixExposure_deposit, ih]
      simp only [List.length_cons]
      have hs := choose3_step xs.length
      have hc : (choose3 (xs.length + 3) : Int) =
          (choose3 (xs.length + 2) : Int) + (choose2 (xs.length + 2) : Int) := by
        exact_mod_cast hs
      have hs2 := choose2_step xs.length
      have hc2 : (choose2 (xs.length + 2) : Int) =
          (choose2 (xs.length + 1) : Int) + (xs.length + 1 : Nat) := by
        exact_mod_cast hs2
      rw [hc, hc2]
      have hlen : (shifted b xs).length = xs.length := by simp [shifted]
      rw [hlen]
      simp only [List.length_cons, Int.natCast_add, Int.natCast_one,
        Int.add_mul, Int.mul_add, Int.mul_one, Int.one_mul]
      ac_rfl

private theorem zipIdx_reverse (x : Series) :
    x.reverse.zipIdx = (x.zipIdx.reverse).map (fun (v, i) => (v, x.length - 1 - i)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [List.reverse_cons, zipIdx_append, ih]
      simp only [List.length_reverse, List.zipIdx_cons, List.map_cons, List.map_nil,
        List.reverse_cons, List.reverse_nil, List.nil_append, List.map_append,
        List.map_reverse, List.map_map, Function.comp_def, List.length_cons]
      · simp [Nat.zero_add]
        have hz := zipIdx_add xs 0 1
        simp only [Nat.zero_add] at hz
        rw [hz, List.map_map]
        apply List.map_congr_left
        intro p hp
        cases p with
        | mk v i =>
            simp only [Function.comp_def]
            have h2 : xs.length - (i + 1) = xs.length - 1 - i := by omega
            rw [h2]

theorem sound_FrameTotals_EvenSplitter : FrameTotals.EvenSplitter.Goal := by
  intro x
  rw [sound_FrameTotals_RowIncidence, sound_FrameTotals_RowIncidence, zipIdx_reverse,
    List.map_map, List.map_reverse, List.sum_reverse]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  cases p with
  | mk v i =>
      have hi : i < x.length := List.snd_lt_of_mem_zipIdx hp
      have hidx : x.length + 1 - 1 - i = x.length - i := by omega
      have h1 : x.length - 1 - i + 1 = x.length - i := by omega
      have h2 : x.length - (x.length - 1 - i) = i + 1 := by omega
      simp only [Function.comp_def, List.length_reverse, h1, h2, Int.mul_comm]
      ac_rfl

/-! ### Pair/triple enumeration helpers -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => rfl
  | succ n => exact choose2_step n

private theorem pairs_length (x : Series) : (pairs x).length = choose2 x.length := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [pairs_cons, List.length_append, List.length_map, List.length_cons, ih, choose2_succ]
      simp [Nat.add_comm]

private theorem triples_cons (a : Int) (xs : Series) :
    triples (a :: xs) = (pairs xs).map (fun (b, c) => (a, b, c)) ++ triples xs := by
  unfold triples
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem sum_map_ite_filter {α : Type} (l : List α) (p : α → Bool) (f : α → Nat) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

private theorem sum_map_ite_filter_prop {α : Type} (l : List α) (p : α → Prop) [DecidablePred p] (f : α → Nat) :
    ((l.filter (fun x => decide (p x))).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
      · simp [h, ih]

private theorem zipIdx_map_ignore_index {β : Type} (xs : Series) (n : Nat) (g : Int → β) :
    (xs.zipIdx n).map (fun (v, _) => g v) = xs.map g := by
  induction xs generalizing n with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem middle_cross_left (x y : Series) :
    (x.zipIdx.map (fun (v, i) =>
      (x.take i).countP (fun a => a < v) * y.countP (fun c => v < c))).sum =
    ((pairs x).filter rising |>.map (fun (a, b) => y.countP (b < ·))).sum := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      have h0 : (List.take 0 (a :: xs)).countP (fun t => t < a) = 0 := by simp
      simp [h0]
      rw [zipIdx_add xs 0 1]
      have hlist :
        ((xs.zipIdx.map (fun (v, i) => (v, i + 1))).map
            (fun (v, i) =>
              (List.take i (a :: xs)).countP (fun t => t < v) *
                y.countP (fun c => v < c))) =
        (xs.zipIdx.map (fun (v, i) =>
          (List.take (i + 1) (a :: xs)).countP (fun t => t < v) *
            y.countP (fun c => v < c))) := by
        rw [List.map_map]
        apply List.map_congr_left
        intro p hp
        rfl
      rw [hlist]
      have hrest :
        (xs.zipIdx.map (fun (v, i) =>
          (List.take (i + 1) (a :: xs)).countP (fun t => t < v) *
            y.countP (fun c => v < c))).sum =
        (xs.zipIdx.map (fun (v, j) =>
          (if a < v then 1 else 0) * y.countP (fun c => v < c) +
          (xs.take j).countP (fun t => t < v) * y.countP (fun c => v < c))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro p hp
        rcases p with ⟨v, i⟩
        have ht : List.take (i + 1) (a :: xs) = a :: List.take i xs := by simp
        simp only [ht]
        simp only [List.countP_cons]
        by_cases h : a < v
        · simp [h, Nat.add_mul]; ac_rfl
        · simp [h, Nat.add_mul]
      rw [hrest]
      have hsplit :
        (xs.zipIdx.map (fun (v, j) =>
          (if a < v then 1 else 0) * y.countP (fun c => v < c) +
          (xs.take j).countP (fun t => t < v) * y.countP (fun c => v < c))).sum =
        (xs.zipIdx.map (fun (v, j) =>
          (if a < v then 1 else 0) * y.countP (fun c => v < c))).sum +
        (xs.zipIdx.map (fun (v, j) =>
          (xs.take j).countP (fun t => t < v) * y.countP (fun c => v < c))).sum := by
        have hcong :
          (xs.zipIdx.map (fun (v, j) =>
            (if a < v then 1 else 0) * y.countP (fun c => v < c) +
            (xs.take j).countP (fun t => t < v) * y.countP (fun c => v < c))) =
          (xs.zipIdx.map (fun p =>
            (fun (v, j) => (if a < v then 1 else 0) * y.countP (fun c => v < c)) p +
            (fun (v, j) => (xs.take j).countP (fun t => t < v) * y.countP (fun c => v < c)) p)) := by
          apply List.map_congr_left
          intro p hp
          rcases p with ⟨v, j⟩
          rfl
        rw [hcong]
        exact sum_map_add_nat xs.zipIdx
          (fun (v, j) => (if a < v then 1 else 0) * y.countP (fun c => v < c))
          (fun (v, j) => (xs.take j).countP (fun t => t < v) * y.countP (fun c => v < c))
      rw [hsplit]
      have hcontrib :
        (xs.zipIdx.map (fun (v, j) =>
          (if a < v then 1 else 0) * y.countP (fun c => v < c))).sum =
        (xs.map (fun v => if a < v then y.countP (fun c => v < c) else 0)).sum := by
        have hmap : (xs.zipIdx.map (fun (v, j) => (if a < v then 1 else 0) * y.countP (fun c => v < c))) =
            (xs.map (fun v => (if a < v then 1 else 0) * y.countP (fun c => v < c))) := by
          simpa using zipIdx_map_ignore_index xs 0 (fun v => (if a < v then 1 else 0) * y.countP (fun c => v < c))
        rw [hmap]
        apply congrArg List.sum
        apply List.map_congr_left
        intro v hv
        by_cases h : a < v
        · simp [h]
        · simp [h]
      rw [hcontrib]
      have hfilter :
        (xs.map (fun v => if a < v then y.countP (fun c => v < c) else 0)).sum =
        ((xs.map (fun v => (a, v))).filter rising |>.map (fun (u, v) => y.countP (v < ·))).sum := by
        have hfm :
          ((xs.map (fun v => (a, v))).filter rising |>.map (fun (u, v) => y.countP (v < ·))) =
          (xs.filter (fun v => a < v)).map (fun v => y.countP (v < ·)) := by
          rw [List.filter_map]
          simp only [Function.comp_def, rising]
          rw [List.map_map]
          rfl
        rw [hfm]
        exact (sum_map_ite_filter_prop xs (fun v => a < v) (fun v => y.countP (fun c => v < c))).symm
      rw [hfilter]
      rw [ih]
      have hpairs :
        ((pairs (a :: xs)).filter rising |>.map (fun (u, v) => y.countP (v < ·))).sum =
          ((xs.map (fun v => (a, v))).filter rising |>.map (fun (u, v) => y.countP (v < ·))).sum +
          ((pairs xs).filter rising |>.map (fun (u, v) => y.countP (v < ·))).sum := by
        rw [pairs_cons]
        rw [List.filter_append, List.map_append, List.sum_append]
      rw [hpairs]

private theorem sum_if_const_nat (ys : Series) (b : Int) (k : Nat) :
    (ys.map (fun c => if b < c then k else 0)).sum = (ys.countP (fun c => b < c)) * k := by
  induction ys with
  | nil => simp
  | cons c cs ih =>
      by_cases h : b < c
      · simp [h, ih, Nat.add_mul, Nat.add_comm]
      · simp [h, ih]

private theorem middle_cross_right (x y : Series) :
    (y.zipIdx.map (fun (v, i) =>
      x.countP (fun a => a < v) * (y.drop (i + 1)).countP (fun c => v < c))).sum =
    ((pairs y).filter rising |>.map (fun (b, c) => x.countP (· < b))).sum := by
  induction y with
  | nil => simp [pairs]
  | cons b ys ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      have hd : List.drop 1 (b :: ys) = ys := by simp
      simp [hd]
      rw [zipIdx_add ys 0 1]
      rw [List.map_map]
      have hshift :
        (ys.zipIdx.map ((fun p =>
          List.countP (fun a => decide (a < p.fst)) x *
            List.countP (fun c => decide (p.fst < c)) (List.drop p.snd ys)) ∘
          (fun (v, i) => (v, i + 1)))).sum =
        (ys.zipIdx.map (fun (v, i) =>
          List.countP (fun a => decide (a < v)) x *
            List.countP (fun c => decide (v < c)) (List.drop (i + 1) ys))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro p hp
        rfl
      rw [hshift]
      rw [ih]
      have hpairs :
        ((pairs (b :: ys)).filter rising |>.map (fun (u, v) => x.countP (· < u))).sum =
          (ys.countP (fun c => b < c)) * x.countP (· < b) +
          ((pairs ys).filter rising |>.map (fun (u, v) => x.countP (· < u))).sum := by
        rw [pairs_cons]
        rw [List.filter_append, List.map_append, List.sum_append]
        have hconv :
          ((ys.map (fun c => (b, c))).filter rising |>.map (fun (u, v) => x.countP (· < u))).sum =
            (ys.countP (fun c => b < c)) * x.countP (· < b) := by
          have hfm :
            ((ys.map (fun c => (b, c))).filter rising |>.map (fun (u, v) => x.countP (· < u))) =
            (ys.filter (fun c => b < c)).map (fun c => x.countP (· < b)) := by
            rw [List.filter_map]
            simp only [Function.comp_def, rising]
            rw [List.map_map]
            rfl
          rw [hfm]
          rw [sum_map_ite_filter_prop ys (fun c => b < c) (fun c => x.countP (· < b))]
          exact sum_if_const_nat ys b (x.countP (· < b))
        rw [hconv]
      rw [hpairs]
      ac_rfl

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

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

private theorem cart_countP (x y : Series) (P : Int × Int → Bool) :
    (cart x y).countP P = (x.map (fun a => y.countP (fun b => P (a, b)))).sum := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [show cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y by simp [cart]]
      rw [List.countP_append, countP_map_comp, ih]
      simp

private theorem sum_if_one_eq_countP {α : Type} (l : List α) (q : α → Bool) :
    (l.map (fun x => if q x then 1 else 0)).sum = l.countP q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : q x = true
      · simp [h, ih]; ac_rfl
      · simp [h, ih]

theorem sound_TripleRise_Concatenate : TripleRise.Concatenate.Goal := by
  intro x y
  induction x with
  | nil =>
      have hb : ((pairs []).filter rising |>.map (fun t => y.countP (t.2 < ·))).sum = 0 := by
        simp [pairs]
      have hc : ((pairs y).filter rising |>.map (fun t => (0 : Nat))).sum = 0 := by
        induction (pairs y).filter rising with
        | nil => rfl
        | cons x xs ih => simp [ih]
      simp [TripleRise.measure, triples, List.nil_append, hb, hc]
  | cons a xs ih =>
      have hl : TripleRise.measure (a :: (xs ++ y)) =
          (pairs (xs ++ y)).countP (fun (b, c) => strict3 (a, b, c)) +
            TripleRise.measure (xs ++ y) := by
        unfold TripleRise.measure
        rw [triples_cons, List.countP_append, countP_map_comp]
        rfl
      change (TripleRise.measure (a :: (xs ++ y))) = _
      rw [hl]
      rw [pairs_countP_append (fun (b, c) => strict3 (a, b, c)) xs y]
      -- measure (xs ++ y) by induction
      have hxs : (TripleRise.measure (xs ++ y) : Int) =
          TripleRise.measure xs + TripleRise.measure y +
            ((pairs xs).filter rising |>.map (fun t => y.countP (t.2 < ·))).sum +
            ((pairs y).filter rising |>.map (fun t => xs.countP (· < t.1))).sum := by
        simpa using ih
      -- cart identity
      have hcart : (cart xs y).countP (fun (b, c) => strict3 (a, b, c)) =
          ((xs.map (fun b => (a, b))).filter rising |>.map (fun (u, v) => y.countP (v < ·))).sum := by
        rw [cart_countP]
        have hpoint : (xs.map (fun b => y.countP (fun c => strict3 (a, b, c)))) =
            (xs.map (fun b => if a < b then y.countP (fun c => b < c) else 0)) := by
          apply List.map_congr_left
          intro b hb
          by_cases h : a < b
          · simp [strict3, h]
          · simp [strict3, h]
        rw [hpoint]
        have hfm : ((xs.map (fun b => (a, b))).filter rising |>.map (fun (u, v) => y.countP (v < ·))) =
            (xs.filter (fun b => a < b)).map (fun b => y.countP (b < ·)) := by
          rw [List.filter_map]
          simp only [Function.comp_def, rising]
          rw [List.map_map]
          rfl
        rw [(sum_map_ite_filter_prop xs (fun b => a < b) (fun b => y.countP (fun c => b < c))).symm]
        rw [hfm.symm]
      -- A_y identity
      have hay : (pairs y).countP (fun (b, c) => strict3 (a, b, c)) =
          ((pairs y).filter rising |>.map (fun (b, c) => if a < b then 1 else 0)).sum := by
        have hq : (pairs y).countP (fun (b, c) => strict3 (a, b, c)) =
            (pairs y).countP (fun (b, c) => rising (b, c) && decide (a < b)) := by
          apply List.countP_congr
          intro p hp
          cases p with
          | mk b c =>
              by_cases h : b < c
              · simp [strict3, rising, h]
              · simp [strict3, rising, h]
        rw [hq]
        have hfilter : (pairs y).countP (fun (b, c) => rising (b, c) && decide (a < b)) =
            ((pairs y).filter rising).countP (fun (b, c) => decide (a < b)) := by
          rw [List.countP_filter]
          apply List.countP_congr
          intro p hp
          cases p with
          | mk b c => simp [Bool.and_comm]
        rw [hfilter]
        exact (sum_if_one_eq_countP ((pairs y).filter rising) (fun (b, c) => decide (a < b))).symm
      -- Assemble
      -- LHS = A_xs + A_y + A_cart + measure (xs ++ y)
      -- RHS = (A_xs + measure xs) + measure y + (cross_left_xs + cross_left_a) + (cross_right_xs + cross_right_a)
      -- where cross_left_a = (xs.map (a,b)).filter rising map y.countP sum = A_cart
      -- cross_right_a = (pairs y).filter rising map (if a<b then 1) sum = A_y
      -- measure (xs++y) = measure xs + measure y + cross_left_xs + cross_right_xs
      simp only [TripleRise.measure]
      rw [hxs]
      rw [hcart]
      rw [hay]
      ac_rfl
private theorem pairs_fst_mem (xs : Series) {p : Int × Int} (h : p ∈ pairs xs) : p.1 ∈ xs := by
  induction xs with
  | nil => simp [pairs] at h
  | cons a ys ih =>
      rw [pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨b, hb, hp⟩
        rw [← hp]; simp
      · exact List.mem_cons_of_mem a (ih h)

private theorem pairs_snd_mem (xs : Series) {p : Int × Int} (h : p ∈ pairs xs) : p.2 ∈ xs := by
  induction xs with
  | nil => simp [pairs] at h
  | cons a ys ih =>
      rw [pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨b, hb, hp⟩
        rw [← hp]; exact List.mem_cons_of_mem a hb
      · exact List.mem_cons_of_mem a (ih h)

theorem sound_TripleRise_DisjointRanges : TripleRise.DisjointRanges.Goal := by
  intro x y h
  rw [sound_TripleRise_Concatenate]
  have hleft : ((pairs x).filter rising |>.map (fun t => y.countP (t.2 < ·))).sum =
      (pairs x).filter rising |>.length * y.length := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro (t : Int × Int) ht
    have hmem := (List.mem_filter.mp ht).1
    have hy : y.countP (fun c => t.2 < c) = y.length := by
      apply List.countP_eq_length_filter.mpr
      apply List.filter_eq_self.mpr
      intro c hc
      exact decide_eq_true (h (pairs_snd_mem x hmem) hc)
    exact hy
  have hright : ((pairs y).filter rising |>.map (fun t => x.countP (· < t.1))).sum =
      (pairs y).filter rising |>.length * x.length := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro (t : Int × Int) ht
    have hmem := (List.mem_filter.mp ht).1
    have hx : x.countP (fun a => a < t.1) = x.length := by
      apply List.countP_eq_length_filter.mpr
      apply List.filter_eq_self.mpr
      intro a ha
      exact decide_eq_true (h ha (pairs_fst_mem y hmem))
    exact hx
  rw [hleft, hright]
  simp only [List.length_filter, ← List.countP_eq_length_filter]
  unfold RisePairs.measure size
  push_cast
  simp only [Int.mul_add, Int.add_mul, Int.mul_one]
  ac_rfl

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures3
