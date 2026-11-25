import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O5

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! ## Generic helpers for adjacent differences and curvature -/

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj_cons₂]

private def d2 (x : Series) : Series := delta (delta x)

private theorem d2_cons₃ (a b c : Int) (xs : Series) :
    d2 (a :: b :: c :: xs) = (c - 2 * b + a) :: d2 (b :: c :: xs) := by
  unfold d2
  rw [delta_cons₂ a b (c :: xs)]
  rw [delta_cons₂ b c xs]
  rw [delta_cons₂ (b - a) (c - b) (delta (c :: xs))]
  simp
  omega

private def E (x : Series) : Int := ((d2 x).map (fun v => v ^ 2)).sum

private theorem E_cons3 (a b c : Int) (xs : Series) :
    E (a :: b :: c :: xs) = (c - 2 * b + a) ^ 2 + E (b :: c :: xs) := by
  unfold E
  rw [d2_cons₃]
  rfl

private theorem adj_length (x : Series) : (adj x).length = x.length - 1 := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => simp [adj]
      | cons b bs =>
          simp only [adj_cons₂, List.length_cons] at ih ⊢
          omega

private theorem delta_length (x : Series) : (delta x).length = x.length - 1 := by
  simp [delta, adj_length]

private theorem d2_length (x : Series) : (d2 x).length = x.length - 2 := by
  unfold d2
  rw [delta_length, delta_length]
  omega

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  have h : a ^ 2 = a * a := by grind
  rw [h, ← Int.natAbs_mul_self (a := a)]
  exact Int.natCast_nonneg _

private theorem sum_map_nonneg_sq (xs : Series) : 0 ≤ (xs.map (fun v => v ^ 2)).sum := by
  induction xs with
  | nil => exact Int.le_refl 0
  | cons v vs ih =>
      change 0 ≤ v ^ 2 + (vs.map (fun w => w ^ 2)).sum
      have hv := sq_nonneg v
      omega

private theorem all_zero_of_sum_sq_eq_zero : ∀ ys : Series,
    (ys.map (fun v => v ^ 2)).sum = 0 → ∀ v ∈ ys, v = 0 := by
  intro ys hsum
  induction ys with
  | nil => simp
  | cons v vs ih =>
      change v ^ 2 + (vs.map (fun w => w ^ 2)).sum = 0 at hsum
      have hv2 := sq_nonneg v
      have htail := sum_map_nonneg_sq vs
      have hvzero : v ^ 2 = 0 := by omega
      have hpow : v ^ 2 = v * v := by grind
      have hmul : v * v = 0 := by rwa [← hpow]
      have hv : v = 0 := (Int.mul_eq_zero.mp hmul).elim id id
      subst v
      simp at hsum
      have hi := ih hsum
      intro w hw
      rcases List.mem_cons.mp hw with rfl | hw
      · rfl
      · exact hi w hw

private theorem sum_sq_eq_zero_of_all_zero (ys : Series) (h : ∀ v ∈ ys, v = 0) :
    (ys.map (fun v => v ^ 2)).sum = 0 := by
  induction ys with
  | nil => rfl
  | cons v vs ih =>
      have hv : v = 0 := h v (by simp)
      have ht : ∀ w ∈ vs, w = 0 := by intro w hw; exact h w (by simp [hw])
      change v ^ 2 + (vs.map (fun w => w ^ 2)).sum = 0
      rw [ih ht]
      simp [hv]

private theorem sum_map_const_int {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

private theorem E_of_length_le_two (l : Series) (h : l.length ≤ 2) : E l = 0 := by
  cases l with
  | nil => rfl
  | cons a l1 =>
      cases l1 with
      | nil => rfl
      | cons b l2 =>
          cases l2 with
          | nil => rfl
          | cons c l3 => simp at h

private theorem getLastD_reverse (x : Series) : x.reverse.getLast?.getD 0 = x.head?.getD 0 := by
  rw [List.getLast?_reverse]

private theorem delta_append_singleton (x : Series) (a : Int) (hx : x ≠ []) :
    delta (x ++ [a]) = delta x ++ [a - x.getLast?.getD 0] := by
  induction x with
  | nil => contradiction
  | cons b bs ih =>
      cases bs with
      | nil => simp [delta, adj]
      | cons c cs =>
          have hne : (c :: cs) ≠ [] := by simp
          have hi := ih hne
          change (c - b) :: delta ((c :: cs) ++ [a]) =
            (c - b) :: (delta (c :: cs) ++ [a - (b :: c :: cs).getLast?.getD 0])
          rw [hi]
          simp [List.getLast?_cons]

private theorem delta_reverse (x : Series) :
    delta x.reverse = (delta x).reverse.map (fun v => -v) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          rw [List.reverse_cons]
          rw [delta_append_singleton ((b :: bs).reverse) a (by simp)]
          rw [ih]
          rw [getLastD_reverse (b :: bs)]
          simp only [List.head?_cons, Option.getD_some]
          rw [delta_cons₂]
          simp only [List.map_cons, List.reverse_cons, List.map_append]
          have hneg : -(b - a) = a - b := by omega
          rw [hneg]
          simp

private theorem delta_map_neg (x : Series) :
    delta (x.map (fun v => -v)) = (delta x).map (fun v => -v) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          simp only [List.map_cons] at ih ⊢
          rw [delta_cons₂, ih]
          change (-b - -a) :: (delta (b :: bs)).map (fun v => -v) =
            (-(b - a)) :: (delta (b :: bs)).map (fun v => -v)
          congr 1
          omega

private theorem d2_reverse (x : Series) : d2 x.reverse = (d2 x).reverse := by
  unfold d2
  rw [delta_reverse]
  rw [delta_map_neg]
  rw [delta_reverse]
  simp [List.map_map, Function.comp_def]

/-! ## EdgeCurvature -/

-- AffineTrend: reuse the proven pattern from Goal_EdgeCurvature_AffineTrend.lean.
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

private theorem measure_of_all_d2_eq (x : Series) (a : Int)
    (h : ∀ v ∈ d2 x, v = a) :
    E x = ((x.length - 2 : Nat) : Int) * a ^ 2 := by
  unfold E
  have hm : (d2 x).map (fun v => v ^ 2) = (d2 x).map (fun _ => a ^ 2) := by
    apply List.map_congr_left
    intro v hv
    rw [h v hv]
  rw [hm, sum_map_const_int]
  rw [show ((d2 x).length : Int) = ((x.length - 2 : Nat) : Int) by
    rw [d2_length]]

private theorem E_cons_one (a : Int) (y : Series) :
    E (a :: y) = E y + E ([a] ++ y.take 2) := by
  cases y with
  | nil => rfl
  | cons r rs =>
      cases rs with
      | nil => rfl
      | cons s ss =>
          rw [E_cons3]
          simp [E_cons3, E_of_length_le_two]
          omega

private theorem E_cons_two (a b : Int) (y : Series) :
    E (a :: b :: y) = E y + E ([a, b] ++ y.take 2) := by
  cases y with
  | nil => rfl
  | cons r rs =>
      cases rs with
      | nil =>
          rw [E_cons3]
          simp [E_cons3, E_of_length_le_two]
      | cons s ss =>
          rw [E_cons3, E_cons3]
          simp [E_cons3, E_of_length_le_two]
          omega

private theorem take_two_append_of_length (l u v : Series) (h : 2 ≤ l.length) :
    (l ++ u).take 2 = (l ++ v).take 2 := by
  cases l with
  | nil => simp at h
  | cons a l' =>
      cases l' with
      | nil => simp at h
      | cons b l'' => simp

private theorem last2_cons3 (a b c : Int) (xs : Series) :
    ((a :: b :: c :: xs).reverse.take 2).reverse = ((b :: c :: xs).reverse.take 2).reverse := by
  cases xs with
  | nil => simp [List.reverse_cons]
  | cons d ds =>
      cases ds with
      | nil => simp [List.reverse_cons]
      | cons e es =>
          cases es with
          | nil => simp [List.reverse_cons]
          | cons f fs => simpa [List.reverse_cons] using
              (take_two_append_of_length (fs.reverse ++ [f, e])
                [d, c, b, a] [d, c, b] (by simp))

theorem sound_EdgeCurvature_AffineTrend_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.AffineTrend.Goal := by
  intro x a b
  change E (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b)) = E x
  rw [show (x.zipIdx.map (fun (v, i) => v + a * (i : Int) + b)) = shiftAffine a b x by rfl]
  unfold E at ⊢
  rw [d2_shiftAffine]

theorem sound_EdgeCurvature_Backward_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.Backward.Goal := by
  intro x
  change E x.reverse = E x
  unfold E at ⊢
  rw [d2_reverse]
  rw [List.map_reverse]
  exact List.sum_reverse _

theorem sound_EdgeCurvature_EndpointRepair_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.EndpointRepair.Goal := by
  intro x y
  induction x with
  | nil =>
      have hy : E (y.take 2) = 0 := E_of_length_le_two _ (by simp [Nat.min_le_left])
      have hnil : E ([] : Series) = 0 := rfl
      change E y = E ([] : Series) + E y + E (y.take 2)
      rw [hnil, hy]
      omega
  | cons a xs ih =>
      cases xs with
      | nil =>
          change E (a :: y) = E [a] + E y + E ([a] ++ y.take 2)
          have hEa : E [a] = 0 := rfl
          simpa [hEa] using E_cons_one a y
      | cons b ys =>
          cases ys with
          | nil =>
              change E (a :: b :: y) = E [a, b] + E y + E ([a, b] ++ y.take 2)
              have hEab : E [a, b] = 0 := rfl
              simpa [hEab] using E_cons_two a b y
          | cons c zs =>
              simp only [List.cons_append] at ih ⊢
              change E (b :: c :: (zs ++ y)) =
                E (b :: c :: zs) + E y +
                  E (((b :: c :: zs).reverse.take 2).reverse ++ y.take 2) at ih
              change E (a :: b :: c :: (zs ++ y)) =
                E (a :: b :: c :: zs) + E y +
                  E (((a :: b :: c :: zs).reverse.take 2).reverse ++ y.take 2)
              rw [E_cons3 a b c (zs ++ y)]
              rw [E_cons3 a b c zs]
              rw [ih]
              rw [last2_cons3 a b c zs]
              omega

theorem sound_EdgeCurvature_FlatAcceleration_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.FlatAcceleration.Goal := by
  intro x a h
  exact measure_of_all_d2_eq x a h

private def ramp (n : Nat) (a d : Int) : Series :=
  (List.range n).map (fun i : Nat => a + d * (i : Int))

private theorem ramp_succ (n : Nat) (a d : Int) :
    ramp (n + 1) a d = a :: ramp n (a + d) d := by
  simp only [ramp, List.range_succ_eq_map, List.map_cons, List.map_map, Function.comp_def]
  simp only [Int.natCast_zero, Int.mul_zero, Int.add_zero]
  congr 1
  apply List.map_congr_left
  intro i hi
  grind

private theorem ramp_succ₂ (n : Nat) (a d : Int) :
    ramp (n + 2) a d = a :: (a + d) :: ramp n (a + 2 * d) d := by
  rw [show n + 2 = (n + 1) + 1 by omega, ramp_succ, ramp_succ]
  congr 2
  grind

private theorem E_ramp_zero : ∀ (n : Nat) (a d : Int), E (ramp n a d) = 0 := by
  intro n
  induction n with
  | zero => intro a d; rfl
  | succ n ih =>
      intro a d
      cases n with
      | zero => rfl
      | succ m =>
          cases m with
          | zero => rfl
          | succ k =>
              rw [show k + 1 + 1 + 1 = (k + 1) + 2 by omega]
              rw [ramp_succ₂ (k + 1) a d]
              rw [show ramp (k + 1) (a + 2 * d) d = (a + 2 * d) :: ramp k (a + 3 * d) d by
                rw [ramp_succ]
                congr 1
                grind]
              rw [E_cons3]
              have hz : ((a + 2 * d) - 2 * (a + d) + a) ^ 2 = 0 := by grind
              rw [hz]
              simp
              have hr : (a + d) :: (a + 2 * d) :: ramp k (a + 3 * d) d = ramp (k + 2) (a + d) d := by
                rw [ramp_succ₂ k (a + d) d]
                rw [show (a + d) + d = a + 2 * d by omega]
                rw [show (a + d) + 2 * d = a + 3 * d by omega]
              rw [hr]
              exact ih (a + d) d

private theorem d2_zero_arith : ∀ x : Series, (∀ v ∈ d2 x, v = 0) →
    ∃ a d : Int, x = ramp x.length a d := by
  intro x
  induction x with
  | nil =>
      intro h
      exact ⟨0, 0, by simp [ramp]⟩
  | cons a xs ih =>
      cases xs with
      | nil =>
          intro h
          exact ⟨a, 0, by simp [ramp]⟩
      | cons b ys =>
          cases ys with
          | nil =>
              intro h
              refine ⟨a, b - a, ?_⟩
              change [a, b] = [a + (b - a) * (0 : Int), a + (b - a) * (1 : Int)]
              simp
              omega
          | cons c zs =>
              intro h
              have hd2 := d2_cons₃ a b c zs
              have hzero : c - 2 * b + a = 0 := by
                exact h (c - 2 * b + a) (by rw [hd2]; simp)
              have htail : ∀ v ∈ d2 (b :: c :: zs), v = 0 := by
                intro v hv
                exact h v (by rw [hd2]; simp [hv])
              rcases ih htail with ⟨a', d', hrepr⟩
              change b :: c :: zs = ramp (zs.length + 2) a' d' at hrepr
              rw [ramp_succ₂ zs.length a' d'] at hrepr
              simp only [List.cons.injEq] at hrepr
              have hb : b = a' := hrepr.1
              subst a'
              have hd' : d' = c - b := by omega
              subst d'
              have ha : a = 2 * b - c := by omega
              subst a
              refine ⟨2 * b - c, c - b, ?_⟩
              change (2 * b - c) :: b :: c :: zs = ramp (zs.length + 3) (2 * b - c) (c - b)
              rw [show zs.length + 3 = (zs.length + 1) + 2 by omega]
              rw [ramp_succ₂ (zs.length + 1) (2 * b - c) (c - b)]
              rw [show (2 * b - c) + (c - b) = b by omega]
              rw [show (2 * b - c) + 2 * (c - b) = c by omega]
              rw [ramp_succ zs.length c (c - b)]
              rw [show c + (c - b) = 2 * c - b by omega]
              have htail2 : zs = ramp zs.length (2 * c - b) (c - b) := by
                have he : b + 2 * (c - b) = 2 * c - b := by omega
                simpa [he] using hrepr.2.2
              rw [← htail2]

theorem sound_EdgeCurvature_TrendCertificate_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.EdgeCurvature.TrendCertificate.Goal := by
  intro x
  constructor
  · intro hzero
    have hz : (List.map (fun v => v ^ 2) (d2 x)).sum = 0 := by
      simpa [EdgeCurvature.measure, QuadraticVariation.measure, d2] using hzero
    have hall : ∀ v ∈ d2 x, v = 0 := all_zero_of_sum_sq_eq_zero (d2 x) hz
    exact d2_zero_arith x hall
  · intro hx
    rcases hx with ⟨a, d, hx⟩
    rw [hx]
    exact E_ramp_zero x.length a d

/-! ## PeakCount -/

private def pc (l : Series) (p : Int × Nat) : Bool :=
  decide (0 < p.2) && (l[p.2 - 1]?.any (fun u => decide (u < p.1))) &&
    (l[p.2 + 1]?.any (fun u => decide (u < p.1)))

private abbrev M := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.measure

private theorem m_eq_count (l : Series) : M l = (l.zipIdx.countP (pc l) : Int) := by
  rfl

private theorem zipIdx_succ (l : Series) (n : Nat) :
    l.zipIdx (n + 1) = (l.zipIdx n).map (fun p => (p.1, p.2 + 1)) := by
  induction l generalizing n with
  | nil => simp
  | cons v vs ih => simp [List.zipIdx_cons, ih, Nat.add_assoc]

private theorem pc_shift (a b c v : Int) (l : Series) (k : Nat) :
    pc (a :: b :: c :: l) (v, k + 2) = pc (b :: c :: l) (v, k + 1) := by
  simp [pc]

private theorem count_pc_shift (a b c : Int) (l : Series) (k : Nat) :
    (l.zipIdx (k + 2)).countP (pc (a :: b :: c :: l)) =
      (l.zipIdx (k + 1)).countP (pc (b :: c :: l)) := by
  rw [show k + 2 = (k + 1) + 1 by omega, zipIdx_succ, List.countP_map]
  apply List.countP_congr
  intro p hp
  rcases p with ⟨v, i⟩
  have hmem := List.mem_zipIdx hp
  have hi : 1 ≤ i := by omega
  have hshift := pc_shift a b c v l (i - 1)
  have h1 : i - 1 + 2 = i + 1 := by omega
  have h2 : i - 1 + 1 = i := by omega
  rw [h1, h2] at hshift
  simp only [Function.comp_apply]
  rw [hshift]

private theorem measure_cons3 (a b c : Int) (xs : Series) :
    M (a :: b :: c :: xs) = (if a < b && c < b then 1 else 0) + M (b :: c :: xs) := by
  rw [m_eq_count, m_eq_count]
  simp [List.zipIdx, List.countP_cons, pc, count_pc_shift a b c xs 1]
  split <;> split <;> omega

private theorem M_nil : M ([] : Series) = 0 := by
  simp [m_eq_count]

private theorem M_one (a : Int) : M [a] = 0 := by
  simp [m_eq_count, pc]

private theorem M_two (a b : Int) : M [a, b] = 0 := by
  simp [m_eq_count, pc]

private theorem M_cons_one (a : Int) (y : Series) : M (a :: y) = M y + M ([a] ++ y.take 2) := by
  cases y with
  | nil => simp [M_nil, M_one]
  | cons v vs =>
      cases vs with
      | nil => simp [M_one, M_two]
      | cons w ws =>
          rw [measure_cons3]
          simp [measure_cons3, M_two]
          omega

private theorem M_cons_two (a b : Int) (y : Series) :
    M (a :: b :: y) = M y + M ([a, b] ++ y.take 2) := by
  cases y with
  | nil => simp [M_nil, M_two]
  | cons v vs =>
      cases vs with
      | nil =>
          rw [measure_cons3]
          simp only [List.take]
          change (if a < b && v < b then 1 else 0) + M [b, v] = M [v] + M [a, b, v]
          rw [measure_cons3 a b v []]
          simp [M_one, M_two]
      | cons w ws =>
          rw [measure_cons3]
          simp [measure_cons3, M_two]
          omega

private theorem M_of_length_le_two (l : Series) (h : l.length ≤ 2) : M l = 0 := by
  cases l with
  | nil => simp [M_nil]
  | cons a l' =>
      cases l' with
      | nil => simp [M_one]
      | cons b l'' =>
          cases l'' with
          | nil => simp [M_two]
          | cons c l''' => simp at h

private theorem M_length_le_three_le_one (l : Series) (h : l.length ≤ 3) : M l ≤ 1 := by
  cases l with
  | nil => simp [M_nil]
  | cons a l1 =>
      cases l1 with
      | nil => simp [M_one]
      | cons b l2 =>
          cases l2 with
          | nil => simp [M_two]
          | cons c l3 =>
              cases l3 with
              | nil =>
                  rw [measure_cons3]
                  simp [M_two]
                  split <;> omega
              | cons d l4 => simp at h

theorem sound_PeakCount_BoundaryPeaks_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.BoundaryPeaks.Goal := by
  intro x y
  induction x with
  | nil =>
      have hy : M (y.take 2) = 0 := M_of_length_le_two _ (by simp [Nat.min_le_left])
      simp [M_nil, hy]
  | cons a xs ih =>
      cases xs with
      | nil => simpa [M_one] using M_cons_one a y
      | cons b ys =>
          cases ys with
          | nil => simpa [M_two] using M_cons_two a b y
          | cons c zs =>
              simp only [List.cons_append] at ih ⊢
              change M (b :: c :: (zs ++ y)) =
                M (b :: c :: zs) + M y +
                  M (((b :: c :: zs).reverse.take 2).reverse ++ y.take 2) at ih
              change M (a :: b :: c :: (zs ++ y)) =
                M (a :: b :: c :: zs) + M y +
                  M (((a :: b :: c :: zs).reverse.take 2).reverse ++ y.take 2)
              rw [measure_cons3 a b c (zs ++ y)]
              rw [measure_cons3 a b c zs]
              rw [ih]
              rw [last2_cons3 a b c zs]
              omega

theorem sound_PeakCount_OrderDictionary_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.OrderDictionary.Goal := by
  intro x f hf
  induction x with
  | nil => simp [M_nil]
  | cons a xs ih =>
      cases xs with
      | nil => simp [M_one]
      | cons b ys =>
          cases ys with
          | nil => simp [M_two]
          | cons c zs =>
              have htail : ∀ u ∈ (b :: c :: zs), ∀ v ∈ (b :: c :: zs), u < v ↔ f u < f v := by
                intro u hu v hv
                exact hf u (by simp [hu]) v (by simp [hv])
              have hih := ih htail
              simp only [List.map_cons] at hih
              change M (f b :: f c :: List.map f zs) = M (b :: c :: zs) at hih
              change M (f a :: f b :: f c :: zs.map f) = M (a :: b :: c :: zs)
              rw [measure_cons3 (f a) (f b) (f c) (zs.map f)]
              rw [measure_cons3 a b c zs]
              rw [hih]
              have hab : a < b ↔ f a < f b := hf a (by simp) b (by simp)
              have hcb : c < b ↔ f c < f b := hf c (by simp) b (by simp)
              by_cases h1 : a < b <;> by_cases h2 : c < b <;>
                simp [h1, h2, hab, hcb] <;> omega

private theorem measure_cons_two_le (a b : Int) (xs : Series) : M (a :: b :: xs) ≤ M xs + 1 := by
  cases xs with
  | nil => simp [M_two, M_nil]
  | cons c cs =>
      cases cs with
      | nil =>
          rw [measure_cons3]
          simp [M_one, M_two]
          omega
      | cons d ds =>
          rw [measure_cons3]
          rw [M_cons_one b (c :: d :: ds)]
          simp only [List.take]
          change (if a < b && c < b then 1 else 0) + (M (c :: d :: ds) + M [b, c, d]) ≤
            M (c :: d :: ds) + 1
          rw [measure_cons3 b c d []]
          simp [M_two]
          split <;> split <;> omega

private theorem adjacent_cap_aux : ∀ (n : Nat) (x : Series), x.length ≤ n →
    M x ≤ (((x.length - 1) / 2 : Nat) : Int) := by
  intro n
  induction n with
  | zero =>
      intro x h
      have hx : x = [] := by cases x <;> simp_all
      subst x
      simp [M_nil]
  | succ n ih =>
      intro x hlen
      cases x with
      | nil => simp [M_nil]
      | cons a xs =>
          cases xs with
          | nil => simp [M_one]
          | cons b ys =>
              cases ys with
              | nil => simp [M_two]
              | cons c zs =>
                  cases zs with
                  | nil =>
                      rw [measure_cons3]
                      simp [M_two]
                      omega
                  | cons d ds =>
                      have hle := measure_cons_two_le a b (c :: d :: ds)
                      have hcap := ih (c :: d :: ds) (by simp only [List.length_cons] at hlen ⊢; omega)
                      have harith : ((((c :: d :: ds).length - 1) / 2 : Nat) : Int) + 1 ≤
                          ((((a :: b :: c :: d :: ds).length - 1) / 2 : Nat) : Int) := by
                        simp
                        omega
                      omega

theorem sound_PeakCount_AdjacentCap_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.AdjacentCap.Goal := by
  intro x
  exact adjacent_cap_aux x.length x (Nat.le_refl _)

private theorem take_length_le (n : Nat) (l : Series) : (l.take n).length ≤ n := by
  simpa using Nat.min_le_left n l.length

private theorem M_swap_head_bound (y : Series) (a b : Int) :
    (M (a :: y) - M (b :: y)).natAbs ≤ 1 := by
  rw [M_cons_one a y, M_cons_one b y]
  have ht : (y.take 2).length ≤ 2 := take_length_le 2 y
  have h1 := M_length_le_three_le_one ([a] ++ y.take 2) (by
    simp only [List.length_append, List.length_cons, List.length_nil]
    omega)
  have h2 := M_length_le_three_le_one ([b] ++ y.take 2) (by
    simp only [List.length_append, List.length_cons, List.length_nil]
    omega)
  have h10 : 0 ≤ M ([a] ++ y.take 2) := by exact Int.natCast_nonneg _
  have h20 : 0 ≤ M ([b] ++ y.take 2) := by exact Int.natCast_nonneg _
  omega

private theorem M_prefix2_le_one (p a : Int) (t : Series) (ht : t.length ≤ 2) :
    M ([p, a] ++ t) ≤ 1 := by
  cases t with
  | nil => simp [M_two]
  | cons r t1 =>
      cases t1 with
      | nil =>
          rw [show [p, a] ++ [r] = p :: a :: r :: ([] : Series) by rfl]
          rw [measure_cons3]
          simp [M_two]
          split <;> omega
      | cons s t2 =>
          cases t2 with
          | nil =>
              rw [show [p, a] ++ [r, s] = p :: a :: r :: [s] by rfl]
              rw [measure_cons3 p a r [s], measure_cons3 a r s []]
              simp [M_two]
              split <;> split <;> omega
          | cons u t3 => simp at ht

private theorem M_prefix3_le_one (p q a : Int) (t : Series) (ht : t.length ≤ 1) :
    M ([p, q, a] ++ t) ≤ 1 := by
  cases t with
  | nil =>
      rw [show [p, q, a] ++ ([] : Series) = p :: q :: a :: [] by rfl]
      rw [measure_cons3]
      simp [M_two]
      split <;> omega
  | cons r t1 =>
      cases t1 with
      | nil =>
          rw [show [p, q, a] ++ [r] = p :: q :: a :: [r] by rfl]
          rw [measure_cons3 p q a [r], measure_cons3 q a r []]
          simp [M_two]
          split <;> split <;> omega
      | cons s t2 => simp at ht

private theorem M_edit_prefix_le (u : Series) (a b : Int) (y : Series) (hu : u.length ≤ 2) :
    (M (u ++ a :: y) - M (u ++ b :: y)).natAbs ≤ 2 := by
  cases u with
  | nil =>
      simp only [List.nil_append]
      have h := M_swap_head_bound y a b
      omega
  | cons p us =>
      cases us with
      | nil =>
          simp only [List.cons_append, List.nil_append]
          rw [M_cons_two p a y, M_cons_two p b y]
          have hhead := M_swap_head_bound y a b
          have hpa := M_prefix2_le_one p a (y.take 2) (by simpa using take_length_le 2 y)
          have hpb := M_prefix2_le_one p b (y.take 2) (by simpa using take_length_le 2 y)
          have h0a : 0 ≤ M ([p, a] ++ y.take 2) := by exact Int.natCast_nonneg _
          have h0b : 0 ≤ M ([p, b] ++ y.take 2) := by exact Int.natCast_nonneg _
          omega
      | cons q us2 =>
          cases us2 with
          | nil =>
              simp only [List.cons_append, List.nil_append]
              rw [M_cons_two p q (a :: y), M_cons_two p q (b :: y)]
              simp only [List.take]
              have hhead := M_swap_head_bound y a b
              have hpa := M_prefix3_le_one p q a (y.take 1) (by simpa using take_length_le 1 y)
              have hpb := M_prefix3_le_one p q b (y.take 1) (by simpa using take_length_le 1 y)
              have h0a : 0 ≤ M ([p, q, a] ++ y.take 1) := by exact Int.natCast_nonneg _
              have h0b : 0 ≤ M ([p, q, b] ++ y.take 1) := by exact Int.natCast_nonneg _
              have h0ma : 0 ≤ M (a :: y) := by exact Int.natCast_nonneg _
              have h0mb : 0 ≤ M (b :: y) := by exact Int.natCast_nonneg _
              change (M (a :: y) + M ([p, q, a] ++ y.take 1) -
                (M (b :: y) + M ([p, q, b] ++ y.take 1))).natAbs ≤ 2
              omega
          | cons r rest => simp at hu

theorem sound_PeakCount_BoundedRepair_O5 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.BoundedRepair.Goal := by
  intro x
  induction x with
  | nil =>
      intro y a b
      exact M_edit_prefix_le [] a b y (by simp)
  | cons p xs ih =>
      cases xs with
      | nil =>
          intro y a b
          exact M_edit_prefix_le [p] a b y (by simp)
      | cons q ys =>
          cases ys with
          | nil =>
              intro y a b
              exact M_edit_prefix_le [p, q] a b y (by simp)
          | cons r zs =>
              intro y a b
              have hi := ih y a b
              simp only [List.cons_append] at hi ⊢
              change (M (q :: r :: (zs ++ a :: y)) - M (q :: r :: (zs ++ b :: y))).natAbs ≤ 2 at hi
              change (M (p :: q :: r :: (zs ++ a :: y)) - M (p :: q :: r :: (zs ++ b :: y))).natAbs ≤ 2
              rw [measure_cons3 p q r (zs ++ a :: y), measure_cons3 p q r (zs ++ b :: y)]
              omega

theorem disproof_PeakCount_DuplicateLoss_O5 :
    ¬ TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.DuplicateLoss.Goal := by
  intro h
  have hbad : ¬ M ([0, 1, 0].flatMap (TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.repeated 2)) = M [0, 1, 0] := by
    native_decide
  exact hbad (h [0, 1, 0])

end TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O5
