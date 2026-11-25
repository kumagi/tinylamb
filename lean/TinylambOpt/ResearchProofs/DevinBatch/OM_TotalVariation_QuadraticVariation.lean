import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.OM_TotalVariation_QuadraticVariation

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ### Shared list infrastructure -/

/-- The (at most one) adjacent pair crossing the boundary of `x ++ y`. -/
private def crossPair (x y : Series) : List (Int × Int) :=
  match x.getLast?, y.head? with
  | some a, some b => [(a, b)]
  | _, _ => []

/-- The delta contributed by the boundary pair of `x ++ y`. -/
private def crossDelta (x y : Series) : List Int :=
  (crossPair x y).map (fun (a, b) => b - a)

private theorem adj_cons (a : Int) (z : Series) :
    adj (a :: z) = (z.head?.map (fun b => (a, b))).toList ++ adj z := by
  cases z with
  | nil => rfl
  | cons b zs => simp [adj]

private theorem delta_cons (a : Int) (z : Series) :
    delta (a :: z) = (z.head?.map (fun b => b - a)).toList ++ delta z := by
  cases z with
  | nil => rfl
  | cons b zs => simp [delta, adj_cons]

private theorem delta_cons_cons (a b : Int) (cs : Series) :
    delta (a :: b :: cs) = [b - a] ++ delta (b :: cs) := by
  rw [delta_cons]
  have hb : (b :: cs).head? = some b := by simp
  rw [hb]
  simp

private theorem getLast?_cons_of_ne_nil (a : Int) {xs : Series} (h : xs ≠ []) :
    (a :: xs).getLast? = xs.getLast? := by
  cases xs with
  | nil => exact absurd rfl h
  | cons b bs =>
      simp [List.getLast?_cons]

private theorem crossPair_cons_of_ne_nil (a : Int) {xs : Series} (h : xs ≠ []) (y : Series) :
    crossPair (a :: xs) y = crossPair xs y := by
  unfold crossPair
  rw [getLast?_cons_of_ne_nil a h]

private theorem crossPair_singleton (a : Int) (y : Series) :
    crossPair [a] y = (y.head?.map (fun b => (a, b))).toList := by
  unfold crossPair
  cases y with
  | nil => rfl
  | cons b ys => rfl

private theorem adj_append (x y : Series) :
    adj (x ++ y) = adj x ++ crossPair x y ++ adj y := by
  induction x with
  | nil => simp [adj, crossPair]
  | cons a xs ih =>
      cases xs with
      | nil =>
          rw [List.cons_append]
          show adj (a :: ([] ++ y)) = _
          rw [List.nil_append, adj_cons, crossPair_singleton]
          have hadj : adj [a] = ([] : List (Int × Int)) := rfl
          rw [hadj]
          simp
      | cons c cs =>
          have hne : c :: cs ≠ [] := by simp
          have hhead : ((c :: cs) ++ y).head? = some c := by
            rw [List.head?_append]
            simp [List.head?_cons]
          have hhead2 : (c :: cs).head? = some c := by simp [List.head?_cons]
          rw [List.cons_append, adj_cons, ih]
          conv => rhs; rw [adj_cons]
          rw [hhead, hhead2, crossPair_cons_of_ne_nil a hne]
          simp [List.append_assoc]

private theorem delta_append (x y : Series) :
    delta (x ++ y) = delta x ++ crossDelta x y ++ delta y := by
  simp only [delta, adj_append, crossDelta, List.map_append]

private theorem crossDelta_TV_sum (x y : Series) :
    ((crossDelta x y).map (fun v => (v.natAbs : Int))).sum = seam x y distance := by
  unfold crossDelta crossPair seam distance
  cases hx : x.getLast? <;> cases hy : y.head? <;> simp_all

private theorem crossDelta_QV_sum (x y : Series) :
    ((crossDelta x y).map (fun v => v ^ 2)).sum = seam x y (fun a b => (b - a) ^ 2) := by
  unfold crossDelta crossPair seam
  cases hx : x.getLast? <;> cases hy : y.head? <;> simp_all

private theorem crossDelta_head_cons (x : Series) (m : Int) (y : Series) :
    crossDelta x (m :: y) = crossDelta x [m] := by
  unfold crossDelta crossPair
  have hh : (m :: y).head? = some m := by simp
  have hh2 : [m].head? = some m := by simp
  rw [hh, hh2]

private theorem crossDelta_singleton (x : Series) (m : Int) :
    crossDelta x [m] = (x.getLast?.map (fun a => m - a)).toList := by
  unfold crossDelta crossPair
  cases hx : x.getLast? <;> simp

private theorem sum_map_const_mul' (c : Int) (g : Int → Int) (l : List Int) :
    (l.map (fun d => c * g d)).sum = c * (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, Int.mul_add]

private theorem delta_map (g : Int → Int) (x : Series) :
    delta (x.map g) = (adj x).map (fun (u, v) => g v - g u) := by
  unfold delta adj
  rw [← List.map_drop, List.zip_map, List.map_map]
  congr 1

private theorem delta_map_eq (x : Series) (g k : Int → Int)
    (h : ∀ u v, g v - g u = k (v - u)) :
    delta (x.map g) = (delta x).map k := by
  rw [delta_map]
  unfold delta
  rw [List.map_map]
  have hfg : (fun (u, v) => g v - g u) = k ∘ (fun (a, b) => b - a) := by
    funext p
    obtain ⟨u, v⟩ := p
    exact h u v
  rw [hfg]

private theorem delta_scaled (a : Int) (x : Series) :
    delta (scaled a x) = (delta x).map (fun d => a * d) := by
  unfold scaled
  apply delta_map_eq
  intro u v
  exact (Int.mul_sub a v u).symm

private theorem shifted_scaled_map (a b : Int) (x : Series) :
    shifted b (scaled a x) = x.map (fun v => a * v + b) := by
  unfold shifted scaled
  rw [List.map_map]
  rfl

private theorem delta_affine (a b : Int) (x : Series) :
    delta (shifted b (scaled a x)) = (delta x).map (fun d => a * d) := by
  rw [shifted_scaled_map]
  apply delta_map_eq
  intro u v
  grind

private theorem delta_sum (x : Series) :
    (delta x).sum = x.getLast?.getD 0 - x.head?.getD 0 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b cs =>
          rw [delta_cons_cons]
          simp only [List.sum_append, List.sum_singleton]
          rw [ih]
          have hgl : (a :: b :: cs).getLast? = (b :: cs).getLast? :=
            getLast?_cons_of_ne_nil a (by simp : b :: cs ≠ [])
          have hh : (a :: b :: cs).head? = some a := by simp [List.head?_cons]
          have hb2 : (b :: cs).head? = some b := by simp
          rw [hgl, hh, hb2]
          simp only [Option.getD_some]
          omega

private theorem delta_length (x : Series) : (delta x).length = x.length - 1 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b cs =>
          rw [delta_cons_cons]
          simp only [List.length_append, List.length_cons, List.length_nil, ih]
          omega

/-! ### Quadratic-variation helpers (pairs / Cauchy–Schwarz identity) -/

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

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  have h : a ^ 2 = a * a := by grind
  rw [h, ← Int.natAbs_mul_self (a := a)]
  exact Int.natCast_nonneg _

private theorem sum_of_sq_nonneg (xs : List Int) : 0 ≤ (xs.map (fun v => v ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := sq_nonneg a
      omega

private theorem sum_map_sub_sq (a : Int) (xs : Series) :
    (xs.map (fun b => (a - b) ^ 2)).sum =
      (xs.length : Int) * a ^ 2 - 2 * a * xs.sum + (xs.map (fun b => b ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

/-- Cauchy–Schwarz / variance identity:
`n·Σx² − (Σx)² = Σ_{i<j} (x_i − x_j)²`. -/
private theorem cs_identity (l : List Int) :
    (l.length : Int) * (l.map (fun v => v ^ 2)).sum - l.sum ^ 2 =
      ((pairs l).map (fun p => (p.1 - p.2) ^ 2)).sum := by
  induction l with
  | nil => simp [pairs]
  | cons d rest ih =>
      rw [pairs_cons]
      simp only [List.map_append, List.map_map, List.sum_append, List.length_cons,
        List.sum_cons, List.map_cons, Int.natCast_add_one]
      have hcomp : ((fun p => (p.1 - p.2) ^ 2) ∘ fun b => (d, b)) = fun b => (d - b) ^ 2 := by
        funext b
        rfl
      have hfirst : (rest.map ((fun p => (p.1 - p.2) ^ 2) ∘ fun b => (d, b))).sum =
          (rest.map (fun b => (d - b) ^ 2)).sum := by
        rw [hcomp]
      rw [hfirst, sum_map_sub_sq, ← ih]
      grind

/-! ### TotalVariation goals -/

theorem Seam_sound : TotalVariation.Seam.Goal := by
  intro x y
  unfold TotalVariation.measure
  rw [delta_append]
  simp only [List.map_append, List.sum_append]
  rw [crossDelta_TV_sum]
  omega

theorem Rescale_sound : TotalVariation.Rescale.Goal := by
  intro a x
  unfold TotalVariation.measure
  rw [delta_scaled]
  rw [List.map_map]
  show ((delta x).map (fun d => ((a * d).natAbs : Int))).sum = _
  have hfun : (fun d => ((a * d).natAbs : Int)) =
      (fun d => (a.natAbs : Int) * (d.natAbs : Int)) := by
    funext d
    rw [Int.natAbs_mul]
    exact Int.natCast_mul _ _
  rw [hfun]
  exact sum_map_const_mul' (a.natAbs : Int) (fun v => (v.natAbs : Int)) (delta x)

private theorem delta_nonneg_of_pairwise :
    ∀ x : Series, x.Pairwise (· ≤ ·) → ∀ d ∈ delta x, 0 ≤ d := by
  intro x h
  induction x with
  | nil => intro d hd; simp [delta, adj] at hd
  | cons a as ih =>
      cases as with
      | nil => intro d hd; simp [delta, adj] at hd
      | cons b bs =>
          rw [List.pairwise_cons] at h
          obtain ⟨hhead, htail⟩ := h
          have hab : a ≤ b := hhead b List.mem_cons_self
          intro d hd
          rw [delta_cons_cons] at hd
          simp only [List.mem_append, List.mem_singleton] at hd
          cases hd with
          | inl hd1 =>
              rw [hd1]
              omega
          | inr hd2 =>
              exact ih htail d hd2

private theorem measure_eq_delta_sum_pairwise (x : Series) (h : x.Pairwise (· ≤ ·)) :
    TotalVariation.measure x = (delta x).sum := by
  unfold TotalVariation.measure
  have hnn := delta_nonneg_of_pairwise x h
  have hmap : (delta x).map (fun v => (v.natAbs : Int)) = (delta x).map id := by
    apply List.map_congr_left
    intro d hd
    exact Int.natAbs_of_nonneg (hnn d hd)
  rw [hmap, List.map_id]

private theorem sorted_min?_eq_head? (x : Series) (h : x.Pairwise (· ≤ ·)) :
    x.min? = x.head? := by
  apply List.min?_eq_head?
  exact List.Pairwise.imp (fun hab => Int.min_eq_left hab) h

private theorem sorted_max?_eq_getLast? : ∀ x : Series, x.Pairwise (· ≤ ·) →
    x.max? = x.getLast? := by
  intro x h
  induction x with
  | nil => rfl
  | cons a as ih =>
      cases as with
      | nil => rfl
      | cons b bs =>
          rw [List.pairwise_cons] at h
          obtain ⟨hhead, htail⟩ := h
          rw [List.max?_cons, ih htail]
          have hgl : (b :: bs).getLast? = some ((b :: bs).getLast (by simp)) :=
            List.getLast?_eq_some_getLast (by simp)
          rw [hgl]
          have hgl2 : (a :: b :: bs).getLast? =
              some (((b :: bs).getLast? ).getD a) := List.getLast?_cons
          rw [hgl2, hgl]
          simp only [Option.getD_some]
          -- goal: some ((some L).elim a (max a)) = some L  →  max a L = L
          have hmem : (b :: bs).getLast (by simp) ∈ b :: bs := List.getLast_mem (by simp)
          have haL : a ≤ (b :: bs).getLast (by simp) := hhead _ hmem
          simp only [Option.elim_some]
          congr 1
          exact Int.max_eq_right haL

theorem Monotone_sound : TotalVariation.Monotone.Goal := by
  intro x h
  unfold spread
  rw [measure_eq_delta_sum_pairwise x h]
  rw [delta_sum]
  have hmin := sorted_min?_eq_head? x h
  have hmax := sorted_max?_eq_getLast? x h
  rw [hmax, hmin]

private theorem TV_measure_cons (a : Int) (xs : Series) :
    TotalVariation.measure (a :: xs) = TotalVariation.measure xs +
        (xs.head?.map (fun b => distance a b)).getD 0 := by
  have h := Seam_sound [a] xs
  have hcons : [a] ++ xs = a :: xs := rfl
  rw [hcons] at h
  have hm : TotalVariation.measure [a] = 0 := by simp [TotalVariation.measure, delta, adj]
  rw [hm, Int.zero_add] at h
  have hseam : seam [a] xs distance = (xs.head?.map (fun b => distance a b)).getD 0 := by
    unfold seam
    simp [List.getLast?_cons]
  rw [hseam] at h
  exact h

private theorem measure_replicate_TV_list (n : Nat) (a : Int) :
    TotalVariation.measure (List.replicate n a) = 0 := by
  induction n with
  | zero => simp [TotalVariation.measure, delta, adj]
  | succ n ih =>
      rw [List.replicate_succ]
      by_cases hn : n = 0
      · subst hn
        simp [TotalVariation.measure, delta, adj]
      · rw [TV_measure_cons a (List.replicate n a)]
        rw [ih]
        have hhd : (List.replicate n a).head? = some a := by
          rw [List.head?_replicate]
          simp [hn]
        rw [hhd]
        simp [distance]

private theorem measure_replicate_TV (n : Nat) (a : Int) :
    TotalVariation.measure (repeated n a) = 0 :=
  measure_replicate_TV_list n a

theorem DropDuplicates_sound : TotalVariation.DropDuplicates.Goal := by
  intro x n hn
  induction x with
  | nil => simp [TotalVariation.measure]
  | cons a xs ih =>
      rw [List.flatMap_cons]
      rw [Seam_sound]
      rw [measure_replicate_TV n a]
      rw [ih]
      cases xs with
      | nil =>
          simp [TotalVariation.measure, delta, adj, seam]
      | cons b ys =>
          rw [TV_measure_cons a (b :: ys)]
          simp only [List.head?_cons, Option.map_some, Option.getD_some]
          have hseam : seam (repeated n a) ((b :: ys).flatMap (repeated n)) distance =
              distance a b := by
            unfold seam
            have hgl : (repeated n a).getLast? = some a := by
              show (List.replicate n a).getLast? = some a
              rw [List.getLast?_replicate]
              simp [Nat.ne_of_gt hn]
            have hhd : ((b :: ys).flatMap (repeated n)).head? = some b := by
              rw [List.flatMap_cons, List.head?_append]
              have hb : (repeated n b).head? = some b := by
                show (List.replicate n b).head? = some b
                rw [List.head?_replicate]
                simp [Nat.ne_of_gt hn]
              rw [hb]
              simp
            rw [hgl, hhd]
            simp [distance]
          rw [hseam]
          omega

private theorem natAbs_sum_le_sum_map (l : List Int) :
    ((l.sum).natAbs : Int) ≤ (l.map (fun v => (v.natAbs : Int))).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.sum_cons, List.map_cons, List.sum_cons]
      have h1 : Int.natAbs (a + as.sum) ≤ Int.natAbs a + Int.natAbs as.sum :=
        Int.natAbs_add_le a as.sum
      have h2 : ((a + as.sum).natAbs : Int) ≤ (a.natAbs : Int) + (as.sum.natAbs : Int) := by
        rw [← Int.natCast_add]
        exact Int.ofNat_le.mpr (Int.natAbs_add_le a as.sum)
      omega

theorem EndpointFence_sound : TotalVariation.EndpointFence.Goal := by
  intro x
  unfold TotalVariation.measure distance
  rw [← delta_sum x]
  exact natAbs_sum_le_sum_map (delta x)

/-! ### QuadraticVariation goals -/

theorem SeamEnergy_sound : QuadraticVariation.SeamEnergy.Goal := by
  intro x y
  unfold QuadraticVariation.measure
  rw [delta_append]
  simp only [List.map_append, List.sum_append]
  rw [crossDelta_QV_sum]
  omega

theorem Affine_sound : QuadraticVariation.Affine.Goal := by
  intro a b x
  unfold QuadraticVariation.measure
  rw [delta_affine]
  have hsq : ∀ l : List Int, ((l.map (fun d => a * d)).map (fun v => v ^ 2)).sum =
      a ^ 2 * ((l.map (fun v => v ^ 2)).sum) := by
    intro l
    induction l with
    | nil => simp
    | cons d rest ih =>
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        have hsq2 : (a * d) ^ 2 = a ^ 2 * d ^ 2 := by grind
        rw [hsq2, Int.mul_add]
  rw [hsq]

theorem EndpointEnergy_sound : QuadraticVariation.EndpointEnergy.Goal := by
  intro x
  unfold QuadraticVariation.measure
  rw [← delta_sum x]
  have hcs := cs_identity (delta x)
  have hnn : 0 ≤ ((pairs (delta x)).map (fun p => (p.1 - p.2) ^ 2)).sum := by
    have h0 := sum_of_sq_nonneg ((pairs (delta x)).map (fun p => p.1 - p.2))
    rw [List.map_map] at h0
    exact h0
  have hle : (delta x).sum ^ 2 ≤ (↑(delta x).length : Int) *
      ((delta x).map (fun v => v ^ 2)).sum := by omega
  have hcoeff : (↑(delta x).length : Int) * ((delta x).map (fun v => v ^ 2)).sum =
      (↑x.length - 1) * ((delta x).map (fun v => v ^ 2)).sum := by
    by_cases hx : x = []
    · subst hx
      simp [delta, adj]
    · have h1 : 1 ≤ x.length := List.length_pos_iff.mpr hx
      have hd : (delta x).length = x.length - 1 := delta_length x
      rw [hd, Int.natCast_sub h1]
      simp
  rw [hcoeff] at hle
  exact hle

private theorem delta_linearRamp (n : Nat) (a d : Int) :
    delta ((List.range n).map (fun (i : Nat) => a + d * (i : Int))) =
      List.replicate (n - 1) d := by
  induction n with
  | zero => simp [delta, adj]
  | succ n ih =>
      cases n with
      | zero =>
          simp [List.range_succ, delta, adj]
      | succ m =>
          have hstep : m + 1 + 1 = (m + 1) + 1 := rfl
          rw [hstep, List.range_succ, List.map_append]
          rw [show (List.map (fun i : Nat => a + d * (i : Int)) [m + 1]) =
              [a + d * ((m + 1 : Nat) : Int)] from rfl]
          rw [delta_append]
          have hdv : delta [a + d * ((m + 1 : Nat) : Int)] = [] := by simp [delta, adj]
          rw [hdv]
          have hX : (List.map (fun i : Nat => a + d * (i : Int)) (List.range (m + 1))).getLast? =
              some (a + d * ((m : Nat) : Int)) := by
            rw [List.getLast?_map, List.getLast?_range]
            simp
          have hdiff : a + d * ((m + 1 : Nat) : Int) - (a + d * ((m : Nat) : Int)) = d := by
            have hc : ((m + 1 : Nat) : Int) = (m : Int) + 1 := by
              rw [Int.natCast_add, Int.natCast_one]
            rw [hc, Int.mul_add, Int.mul_one]
            omega
          rw [crossDelta_singleton, hX]
          simp only [Option.map_some, Option.toList_some]
          rw [hdiff]
          rw [ih]
          simp only [List.append_nil, Nat.add_sub_cancel]
          rw [← List.replicate_succ']

theorem LinearRamp_sound : QuadraticVariation.LinearRamp.Goal := by
  intro n a d
  unfold QuadraticVariation.measure
  rw [delta_linearRamp]
  rw [List.map_replicate]
  rw [List.sum_replicate_int]

private theorem option_toList_map_sq_sum (o : Option Int) (f : Int → Int) :
    (((o.map f).toList).map (fun v => v ^ 2)).sum = (o.map (fun v => (f v) ^ 2)).getD 0 := by
  cases o <;> simp

private theorem crossDelta_singleton_QV_sum (x : Series) (m : Int) :
    ((crossDelta x [m]).map (fun v => v ^ 2)).sum = seam x [m] (fun u v => (v - u) ^ 2) := by
  unfold crossDelta crossPair seam
  cases hx : x.getLast? <;> simp

private theorem QV_measure_insert (x : Series) (m : Int) (y : Series) :
    QuadraticVariation.measure (x ++ m :: y) =
      QuadraticVariation.measure x + QuadraticVariation.measure y +
        seam x [m] (fun u v => (v - u) ^ 2) + seam [m] y (fun u v => (v - u) ^ 2) := by
  unfold QuadraticVariation.measure
  rw [delta_append, delta_cons, crossDelta_head_cons]
  simp only [List.map_append, List.sum_append]
  rw [crossDelta_singleton_QV_sum x m]
  have hopt : (((y.head?.map (fun b => b - m)).toList).map (fun v => v ^ 2)).sum =
      (y.head?.map (fun v => (v - m) ^ 2)).getD 0 :=
    option_toList_map_sq_sum _ _
  rw [hopt]
  have hseam2 : (y.head?.map (fun v => (v - m) ^ 2)).getD 0 =
      seam [m] y (fun u v => (v - u) ^ 2) := by
    unfold seam
    simp
  rw [hseam2]
  omega

theorem LocalEdit_sound : QuadraticVariation.LocalEdit.Goal := by
  intro x y a b
  rw [QV_measure_insert x b y, QV_measure_insert x a y]
  omega

end TinylambOpt.ResearchProofs.DevinBatch.OM_TotalVariation_QuadraticVariation
