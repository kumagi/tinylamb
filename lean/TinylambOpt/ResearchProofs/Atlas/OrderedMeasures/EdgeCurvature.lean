import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `EdgeCurvature.AffineTrend` — `∀ x (a b : Int), measure (x.zipIdx.map (fun (v,i) =>
  v+a*i+b)) = measure x`
- `EdgeCurvature.Backward` — `∀ x, measure x.reverse = measure x`
- `EdgeCurvature.EndpointRepair` — `∀ x y, measure (x++y) = measure x + measure y +
  measure ((x.reverse.take 2).reverse ++ y.take 2)`
- `EdgeCurvature.FlatAcceleration` — `∀ x a, (∀ v ∈ delta (delta x), v = a) → measure x
  = ((x.length-2:Nat):Int)*a^2`
- `EdgeCurvature.TrendCertificate` — `∀ x, measure x = 0 ↔ ∃ a d : Int, x = (List.range
  x.length).map (fun (i : Nat) => a+d*i)`
-/

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.EdgeCurvature

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj_cons₂]

private def d2 (x : Series) : Series := delta (delta x)

private theorem d2_nil : d2 [] = [] := by simp [d2, delta, adj]
private theorem d2_singleton (_a : Int) : d2 [_a] = [] := by simp [d2, delta, adj]
private theorem d2_pair (a b : Int) : d2 [a, b] = [] := by simp [d2, delta, adj]
private theorem d2_take2 (t : Series) : d2 (t.take 2) = [] := by
  cases t with
  | nil => simp [d2_nil]
  | cons a xs =>
      cases xs with
      | nil => simp [d2_singleton]
      | cons b ys => simp [d2_pair]

private theorem d2_cons₃ (a b c : Int) (xs : Series) :
    d2 (a :: b :: c :: xs) = (c - 2*b + a) :: d2 (b :: c :: xs) := by
  unfold d2
  rw [delta_cons₂ a b (c :: xs)]
  rw [delta_cons₂ b c xs]
  rw [delta_cons₂ (b - a) (c - b) (delta (c :: xs))]
  simp
  omega

private theorem d2_triple (a b c : Int) : d2 [a, b, c] = [c - 2*b + a] := by
  rw [d2_cons₃ a b c []]
  simp [d2_pair]

private theorem delta_length (x : Series) : (delta x).length = x.length - 1 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b bs =>
          rw [delta_cons₂]
          simp only [List.length_cons]
          have hlen : (b :: bs).length = bs.length + 1 := by simp
          rw [hlen] at ih
          simp only [Nat.add_sub_cancel] at ih
          omega

private theorem d2_length (x : Series) : (d2 x).length = x.length - 2 := by
  unfold d2
  rw [delta_length]
  rw [delta_length]
  omega

private theorem zipIdx_add (xs : Series) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private def shiftAffine (a c : Int) (x : Series) : Series :=
  x.zipIdx.map (fun (v, i) => v + a * (i : Int) + c)

private theorem shiftAffine_cons (a c : Int) (v : Int) (xs : Series) :
    shiftAffine a c (v :: xs) = (v + c) :: shiftAffine a (c + a) xs := by
  unfold shiftAffine
  simp only [List.zipIdx_cons, List.map_cons]
  have hzip := zipIdx_add xs 0 1
  simp only [Nat.zero_add] at hzip
  rw [hzip]
  simp only [List.map_map]
  congr 1
  · simp
  · apply List.map_congr_left
    intro p hp
    rcases p with ⟨w, i⟩
    dsimp
    rw [Int.mul_add]
    simp
    omega

private theorem d2_shiftAffine (a c : Int) (x : Series) :
    d2 (shiftAffine a c x) = d2 x := by
  induction x generalizing c with
  | nil => simp [d2, shiftAffine, delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [d2, shiftAffine, delta, adj]
      | cons w xs =>
          cases xs with
          | nil => simp [d2, shiftAffine, delta, adj]
          | cons u xs =>
              rw [shiftAffine_cons a c v (w :: u :: xs)]
              rw [shiftAffine_cons a (c + a) w (u :: xs)]
              rw [shiftAffine_cons a (c + a + a) u xs]
              rw [d2_cons₃]
              have htail := ih (c + a)
              rw [shiftAffine_cons a (c + a) w (u :: xs)] at htail
              rw [shiftAffine_cons a (c + a + a) u xs] at htail
              rw [htail]
              congr 1
              dsimp
              omega

private def dBridge (x y : Series) : Series :=
  (x.getLast?.bind (fun a => y.head?.map (fun b => b - a))).toList

private theorem delta_append (x y : Series) :
    delta (x ++ y) = delta x ++ dBridge x y ++ delta y := by
  induction x with
  | nil => simp [delta, adj, dBridge]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [delta, adj, dBridge]
          | cons b ys => simp [delta, adj, dBridge]
      | cons b bs =>
          simp only [List.cons_append]
          rw [delta_cons₂ a b (bs ++ y)]
          simp only [List.cons_append] at ih
          rw [ih]
          rw [delta_cons₂ a b bs]
          simp [dBridge]

private theorem delta_reverse (x : Series) :
    delta x.reverse = (delta x).reverse.map (fun v => -v) := by
  induction x with
  | nil => simp [delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons w ws =>
          rw [List.reverse_cons]
          rw [delta_append]
          simp [dBridge]
          rw [List.reverse_cons] at ih
          rw [ih]
          rw [delta_cons₂ v w ws]
          simp [delta, adj]
          omega

private theorem delta_map_neg (x : Series) :
    delta (x.map (fun v => -v)) = (delta x).map (fun v => -v) := by
  induction x with
  | nil => simp [delta, adj]
  | cons v xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons w ws =>
          simp only [List.map_cons] at ih ⊢
          rw [delta_cons₂ (-v) (-w) (ws.map (fun v => -v))]
          rw [delta_cons₂ v w ws]
          rw [ih]
          simp
          omega

private theorem rev_neg_rev_neg (l : Series) :
    (l.reverse.map (fun v => -v)).reverse.map (fun v => -v) = l := by
  rw [← List.map_reverse, List.reverse_reverse]
  simp [List.map_map, Function.comp_def]

private theorem d2_reverse (x : Series) : d2 x.reverse = (d2 x).reverse := by
  unfold d2
  rw [delta_reverse]
  rw [delta_map_neg]
  have h := delta_reverse (delta x).reverse
  simp only [List.reverse_reverse] at h
  have h' := congrArg (fun l : Series => l.reverse.map (fun v => -v)) h.symm
  have hA : delta ((delta x).reverse) = (delta (delta x)).reverse.map (fun v => -v) := by
    simpa [List.map_reverse, List.map_map, Function.comp_def] using h'
  rw [hA]
  simp [List.map_map, Function.comp_def]

private def suffix2 : Series → Series
  | [] => []
  | [a] => [a]
  | a :: b :: [] => [a, b]
  | _a :: b :: c :: xs => suffix2 (b :: c :: xs)

private theorem suffix2_eq : ∀ x : Series, (x.reverse.take 2).reverse = suffix2 x
  | [] => by simp [suffix2]
  | [a] => by simp [suffix2]
  | a :: b :: [] => by simp [suffix2]
  | a :: b :: c :: xs => by
      have ih := suffix2_eq (b :: c :: xs)
      simp only [suffix2]
      rw [List.reverse_cons]
      have hlen : 2 ≤ (b :: c :: xs).reverse.length := by simp
      rw [List.take_append_of_le_length hlen]
      exact ih

private theorem d2_append_take2 : ∀ (p t : Series),
    d2 (p ++ t) = d2 (p ++ t.take 2) ++ d2 t
  | [], t => by simp [d2_take2]
  | [a], t => by
      cases t with
      | nil => simp [d2_nil]
      | cons b bs =>
          cases bs with
          | nil => simp [d2_singleton]
          | cons c cs =>
              change d2 (a :: b :: c :: cs) = d2 [a, b, c] ++ d2 (b :: c :: cs)
              rw [d2_cons₃, d2_triple]
              simp
  | a :: b :: [], t => by
      cases t with
      | nil => simp [d2_nil, d2_pair]
      | cons c cs =>
          cases cs with
          | nil => simp [d2_triple, d2_singleton]
          | cons d ds =>
              change d2 (a :: b :: c :: d :: ds) = d2 [a, b, c, d] ++ d2 (c :: d :: ds)
              rw [d2_cons₃ a b c (d :: ds), d2_cons₃ a b c [d],
                d2_cons₃ b c d ds, d2_cons₃ b c d []]
              simp [d2_pair]
  | a :: b :: c :: xs, t => by
      have ih := d2_append_take2 (b :: c :: xs) t
      change d2 (b :: c :: (xs ++ t)) = d2 (b :: c :: (xs ++ t.take 2)) ++ d2 t at ih
      change d2 (a :: b :: c :: (xs ++ t)) =
        d2 (a :: b :: c :: (xs ++ t.take 2)) ++ d2 t
      rw [d2_cons₃, d2_cons₃, ih]
      simp

private theorem d2_append_suffix2 : ∀ (x t : Series), t.length ≤ 2 →
    d2 (x ++ t) = d2 x ++ d2 (suffix2 x ++ t)
  | [], t, ht => by simp [suffix2, d2_nil]
  | [a], t, ht => by simp [suffix2, d2_singleton]
  | a :: b :: [], t, ht => by simp [suffix2, d2_pair]
  | a :: b :: c :: xs, t, ht => by
      cases xs with
      | nil =>
          change d2 (a :: b :: c :: t) = d2 [a, b, c] ++ d2 ([b, c] ++ t)
          rw [d2_cons₃, d2_triple]
          simp
      | cons d ys =>
          have ih := d2_append_suffix2 (b :: c :: d :: ys) t ht
          change d2 (b :: c :: d :: (ys ++ t)) =
            d2 (b :: c :: d :: ys) ++ d2 (suffix2 (b :: c :: d :: ys) ++ t) at ih
          change d2 (a :: b :: c :: d :: (ys ++ t)) =
            d2 (a :: b :: c :: d :: ys) ++ d2 (suffix2 (b :: c :: d :: ys) ++ t)
          rw [d2_cons₃ a b c (d :: (ys ++ t)), d2_cons₃ a b c (d :: ys), ih]
          simp

private theorem d2_append4 (x y : Series) :
    d2 (x ++ y) = d2 x ++ d2 (suffix2 x ++ y.take 2) ++ d2 y := by
  rw [d2_append_take2]
  rw [d2_append_suffix2 x (y.take 2) (by simpa using (Nat.min_le_left 2 y.length))]

private theorem sum_sq_eq_of_all_eq (l : Series) (a : Int)
    (h : ∀ v ∈ l, v = a) :
    (l.map (fun v => v ^ 2)).sum = (l.length : Int) * a ^ 2 := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      have hv : v = a := h v (by simp)
      have ht : ∀ w ∈ vs, w = a := by intro w hw; exact h w (by simp [hw])
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [hv, ih ht]
      have hcast : ((vs.length + 1 : Nat) : Int) = (vs.length : Int) + 1 := by omega
      rw [hcast, Int.add_mul]
      simp
      omega

private theorem pow_two_int (a : Int) : a ^ 2 = a * a := by
  rw [Int.pow_succ, Int.pow_one]

private theorem int_mul_self_nonneg (a : Int) : 0 ≤ a * a := by
  rw [← Int.natAbs_mul_self (a := a)]
  exact Int.natCast_nonneg _

private theorem sq_nonneg_int (a : Int) : 0 ≤ a ^ 2 := by
  rw [pow_two_int]
  exact int_mul_self_nonneg a

private theorem sq_eq_zero_int (a : Int) (h : a ^ 2 = 0) : a = 0 := by
  have hmul : a * a = 0 := by simpa [pow_two_int] using h
  rcases (Int.mul_eq_zero.mp hmul) with hz | hz <;> exact hz

private theorem sum_sq_nonneg (l : Series) : 0 ≤ (l.map (fun v => v ^ 2)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := sq_nonneg_int a
      omega

private theorem sq_le_sum_sq {l : Series} {v : Int} (hv : v ∈ l) :
    v ^ 2 ≤ (l.map (fun w => w ^ 2)).sum := by
  induction l with
  | nil => simp at hv
  | cons a as ih =>
      simp only [List.mem_cons] at hv
      simp only [List.map_cons, List.sum_cons]
      rcases hv with rfl | hv
      · have ht := sum_sq_nonneg as
        omega
      · have ht := ih hv
        have ha := sq_nonneg_int a
        omega

private theorem sum_sq_eq_zero_iff_all_zero (l : Series) :
    (l.map (fun v => v ^ 2)).sum = 0 ↔ ∀ v ∈ l, v = 0 := by
  constructor
  · intro hs v hv
    have hle := sq_le_sum_sq hv
    have hvn := sq_nonneg_int v
    have hsq : v ^ 2 = 0 := by omega
    exact sq_eq_zero_int v hsq
  · intro h
    induction l with
    | nil => simp
    | cons a as ih =>
        simp only [List.map_cons, List.sum_cons]
        have ha : a = 0 := h a (by simp)
        have ht : ∀ w ∈ as, w = 0 := by intro w hw; exact h w (by simp [hw])
        rw [ha, ih ht]
        simp

private def arith (a d : Int) (n : Nat) : Series :=
  (List.range n).map (fun i : Nat => a + d * (i : Int))

private theorem arith_length (a d : Int) (n : Nat) : (arith a d n).length = n := by
  simp [arith]

private theorem arith_succ (a d : Int) (n : Nat) :
    arith a d (n + 1) = a :: arith (a + d) d n := by
  unfold arith
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      rw [List.range_succ, List.map_append]
      simp only [List.cons_append]
      congr 1
      simp only [List.map_cons, List.map_nil]
      have hcast : ((n + 1 : Nat) : Int) = (n : Int) + 1 := by omega
      rw [hcast, Int.mul_add]
      simp
      omega

private theorem d2_zero_arith_canon : ∀ (a b : Int) (xs : Series),
    (∀ v ∈ d2 (a :: b :: xs), v = 0) →
    a :: b :: xs = arith a (b - a) (a :: b :: xs).length
  | a, b, [], h => by
      change [a, b] = [a + (b - a) * (0 : Int), a + (b - a) * (1 : Int)]
      simp
      omega
  | a, b, c :: ys, h => by
      have hhead : c - 2*b + a = 0 := by
        apply h
        rw [d2_cons₃]
        simp
      have htail : ∀ v ∈ d2 (b :: c :: ys), v = 0 := by
        intro v hv
        apply h
        rw [d2_cons₃]
        simp [hv]
      have ihres := d2_zero_arith_canon b c ys htail
      rw [ihres]
      have hd : c - b = b - a := by omega
      rw [hd]
      simp only [List.length_cons]
      rw [arith_length]
      have hadd : a + (b - a) = b := by omega
      change a :: arith b (b - a) (ys.length + 2) = arith a (b - a) (ys.length + 3)
      rw [show ys.length + 3 = (ys.length + 2) + 1 by omega]
      rw [arith_succ a (b - a) (ys.length + 2)]
      rw [hadd]

private theorem d2_zero_arith (x : Series) (h : ∀ v ∈ d2 x, v = 0) :
    ∃ a d : Int, x = arith a d x.length := by
  cases x with
  | nil => exact ⟨0, 0, by simp [arith]⟩
  | cons a xs =>
      cases xs with
      | nil => exact ⟨a, 0, by simp [arith]⟩
      | cons b ys =>
          exact ⟨a, b - a, d2_zero_arith_canon a b ys h⟩

private theorem d2_replicate (a : Int) : ∀ n : Nat,
    d2 (List.replicate n a) = List.replicate (n - 2) 0
  | 0 => by simp [d2, delta, adj]
  | 1 => by simp [d2, delta, adj]
  | 2 => by simp [d2, delta, adj]
  | n + 3 => by
      change d2 (a :: a :: a :: List.replicate n a) = List.replicate (n + 1) 0
      rw [d2_cons₃]
      have ih := d2_replicate a (n + 2)
      change d2 (a :: a :: List.replicate n a) = List.replicate n 0 at ih
      rw [ih, List.replicate_succ]
      congr 1
      omega

private theorem range_map_succ (f : Nat → Int × Nat) (n : Nat) :
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, ih]
      rw [List.range_succ, List.map_append]
      simp

private theorem zipIdx_replicate (n : Nat) :
    (List.replicate n (0 : Int)).zipIdx = (List.range n).map (fun i : Nat => (0, i)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.zipIdx_cons]
      rw [show (List.replicate n (0 : Int)).zipIdx 1 =
          ((List.replicate n (0 : Int)).zipIdx).map (fun p => (p.1, p.2 + 1)) by
            simpa using zipIdx_add (List.replicate n (0 : Int)) 0 1]
      rw [ih, List.map_map]
      change (0, 0) :: (List.range n).map (fun i : Nat => (0, i + 1)) =
        (List.range (n + 1)).map (fun i : Nat => (0, i))
      rw [← range_map_succ (fun i : Nat => (0, i)) n]

private theorem arith_eq_shift (a d : Int) (n : Nat) :
    arith a d n = shiftAffine d a (List.replicate n 0) := by
  unfold arith shiftAffine
  rw [zipIdx_replicate, List.map_map]
  apply List.map_congr_left
  intro i hi
  dsimp
  change a + d * (i : Int) = (0 : Int) + d * (i : Int) + a
  omega

private theorem d2_arith (a d : Int) (n : Nat) :
    d2 (arith a d n) = List.replicate (n - 2) 0 := by
  rw [arith_eq_shift]
  rw [d2_shiftAffine]
  exact d2_replicate 0 n

theorem sound_AffineTrend :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.AffineTrend.Goal := by
  intro x a b
  change ((d2 (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b))).map (fun v => v ^ 2)).sum =
    ((d2 x).map (fun v => v ^ 2)).sum
  rw [show (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b)) = shiftAffine a b x by rfl]
  rw [d2_shiftAffine]

theorem sound_Backward :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.Backward.Goal := by
  intro x
  change ((d2 x.reverse).map (fun v => v ^ 2)).sum = ((d2 x).map (fun v => v ^ 2)).sum
  rw [d2_reverse]
  simp

theorem sound_EndpointRepair :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.EndpointRepair.Goal := by
  intro x y
  change ((d2 (x ++ y)).map (fun v => v ^ 2)).sum =
    ((d2 x).map (fun v => v ^ 2)).sum + ((d2 y).map (fun v => v ^ 2)).sum +
      (((d2 ((x.reverse.take 2).reverse ++ y.take 2)).map (fun v => v ^ 2)).sum)
  rw [d2_append4]
  rw [suffix2_eq]
  simp
  omega

theorem sound_FlatAcceleration :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.FlatAcceleration.Goal := by
  intro x a h
  change ((d2 x).map (fun v => v ^ 2)).sum = ((x.length - 2 : Nat) : Int) * a ^ 2
  rw [sum_sq_eq_of_all_eq (d2 x) a h]
  rw [d2_length]

theorem sound_TrendCertificate :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.TrendCertificate.Goal := by
  intro x
  constructor
  · intro h
    change ((d2 x).map (fun v => v ^ 2)).sum = 0 at h
    have hall : ∀ v ∈ d2 x, v = 0 := (sum_sq_eq_zero_iff_all_zero (d2 x)).mp h
    rcases d2_zero_arith x hall with ⟨a, d, hx⟩
    exact ⟨a, d, by simpa [arith] using hx⟩
  · rintro ⟨a, d, hx⟩
    rw [hx]
    change ((d2 (arith a d x.length)).map (fun v => v ^ 2)).sum = 0
    rw [d2_arith]
    simp

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.EdgeCurvature
