import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PositiveJumps.Remaining

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps

private abbrev up (a b : Int) : Int := max 0 (b - a)

/-- Induction from the right end of a list (no Mathlib `reverseRecOn` here). -/
private theorem snoc_induction {motive : Series → Prop}
    (h_nil : motive []) (h_snoc : ∀ xs a, motive xs → motive (xs ++ [a])) :
    ∀ xs, motive xs := by
  have hrev : ∀ ys : Series, motive ys.reverse := by
    intro ys
    induction ys with
    | nil => simpa using h_nil
    | cons a ys ih =>
        rw [List.reverse_cons]
        exact h_snoc ys.reverse a ih
  intro xs
  simpa using hrev xs.reverse

private theorem delta_cons_cons (a b : Int) (bs : Series) :
    delta (a :: b :: bs) = (b - a) :: delta (b :: bs) := by
  simp [delta, adj]

private theorem getLast_snoc_getD (xs : Series) (a d : Int) :
    (xs ++ [a]).getLast?.getD d = a := by
  rw [List.getLast?_concat]
  simp

private theorem sum_map_max_nonneg (ds : List Int) : 0 ≤ (ds.map (max 0)).sum := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons]
      have hmax : 0 ≤ max 0 d := Int.le_max_left 0 d
      omega

private theorem measure_nonneg (x : Series) : 0 ≤ PositiveJumps.measure x := by
  simpa [PositiveJumps.measure, delta] using sum_map_max_nonneg (delta x)

private theorem measure_cons (b : Int) (l : Series) :
    PositiveJumps.measure (b :: l) = PositiveJumps.measure l + up b (l.head?.getD b) := by
  cases l with
  | nil => simp [PositiveJumps.measure, delta, adj, up]
  | cons c cs =>
      simp [PositiveJumps.measure, delta, adj, up]
      omega

private theorem measure_append (x : Series) : ∀ y : Series,
    PositiveJumps.measure (x ++ y) = PositiveJumps.measure x + PositiveJumps.measure y +
      seam x y up := by
  induction x with
  | nil =>
      intro y
      simp [PositiveJumps.measure, delta, adj, seam]
  | cons a xs ih =>
      intro y
      cases y with
      | nil => simp [PositiveJumps.measure, delta, adj, seam]
      | cons b ys =>
          rw [List.cons_append, measure_cons]
          rw [ih (b :: ys)]
          cases xs with
          | nil =>
              simp [seam, measure_cons, up]
          | cons c cs =>
              simp [seam, measure_cons, up] at ih ⊢
              omega

private theorem measure_snoc (xs : Series) (a : Int) :
    PositiveJumps.measure (xs ++ [a]) = PositiveJumps.measure xs + up (xs.getLast?.getD a) a := by
  have h := measure_append xs [a]
  cases hx : xs.getLast? with
  | none =>
      have hxnone : xs = [] := by
        cases xs with
        | nil => rfl
        | cons b bs =>
            simp at hx
      subst xs
      simp [PositiveJumps.measure, delta, adj, up]
  | some l =>
      have hseam : seam xs [a] up = up l a := by simp [seam, hx]
      rw [hseam] at h
      simpa [PositiveJumps.measure, delta, adj, hx, up] using h

private theorem delta_sum (x : Series) : (delta x).sum = x.getLast?.getD 0 - x.head?.getD 0 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b bs =>
          simp [delta, adj] at ih ⊢
          omega

private theorem pos_neg_diff (d : Int) : max 0 d - max 0 (-d) = d := by
  by_cases h : 0 ≤ d
  · have h1 : max 0 d = d := Int.max_eq_right h
    have h2 : max 0 (-d) = 0 := Int.max_eq_left (by omega)
    rw [h1, h2]
    omega
  · have h1 : max 0 d = 0 := Int.max_eq_left (by omega)
    have h2 : max 0 (-d) = -d := Int.max_eq_right (by omega)
    rw [h1, h2]
    omega

private theorem twice_pos (d : Int) : 2 * max 0 d = (d.natAbs : Int) + d := by
  by_cases h : 0 ≤ d
  · rw [Int.max_eq_right h, Int.natAbs_of_nonneg h]
    omega
  · have hd : d ≤ 0 := by omega
    have hn : 0 ≤ -d := by omega
    have hnegabs : (-d).natAbs = d.natAbs := Int.natAbs_neg d
    have hnabs : ((-d).natAbs : Int) = -d := Int.natAbs_of_nonneg hn
    rw [Int.max_eq_left hd, ← hnegabs, hnabs]
    omega

private theorem twice_pos_sum (ds : List Int) :
    2 * (ds.map (max 0)).sum = (ds.map (fun d => (d.natAbs : Int))).sum + ds.sum := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [Int.mul_add, ih, twice_pos]
      omega

private theorem delta_nonpos : ∀ x : Series, x.Pairwise (· ≥ ·) →
    ∀ v ∈ delta x, v ≤ 0
  | [], hx => by simp [delta, adj]
  | [a], hx => by simp [delta, adj]
  | a :: b :: bs, hx => by
      simp only [List.pairwise_cons] at hx
      have hba : b ≤ a := hx.1 b (by simp)
      intro v hv
      rw [delta_cons_cons] at hv
      simp only [List.mem_cons] at hv
      rcases hv with hv | hv
      · subst v
        omega
      · have htail : (b :: bs).Pairwise (· ≥ ·) :=
          (List.pairwise_cons).mpr ⟨hx.2.1, hx.2.2⟩
        exact delta_nonpos (b :: bs) htail v hv

private theorem sum_max_zero_of_nonpos (ds : List Int) (h : ∀ v ∈ ds, v ≤ 0) :
    (ds.map (max 0)).sum = 0 := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      have hd : d ≤ 0 := h d (by simp)
      have ht : ∀ v ∈ ds, v ≤ 0 := by intro v hv; exact h v (by simp [hv])
      simp only [List.map_cons, List.sum_cons]
      rw [Int.max_eq_left hd, ih ht]
      simp

private theorem up_triangle (a b c : Int) : up a c ≤ up a b + up b c := by
  dsimp [up]
  have hsum : c - a = (b - a) + (c - b) := by omega
  rw [hsum]
  apply (Int.max_le).mpr
  constructor
  · have h1 : 0 ≤ max 0 (b - a) := Int.le_max_left 0 (b - a)
    have h2 : 0 ≤ max 0 (c - b) := Int.le_max_left 0 (c - b)
    omega
  · have h1 : b - a ≤ max 0 (b - a) := Int.le_max_right 0 (b - a)
    have h2 : c - b ≤ max 0 (c - b) := Int.le_max_right 0 (c - b)
    omega

private theorem head_triangle (a b : Int) (xs : Series) :
    up a (xs.head?.getD a) ≤ up b (xs.head?.getD b) + up a b := by
  cases xs with
  | nil =>
      dsimp [up]
      have h : 0 ≤ max 0 (b - a) := Int.le_max_left 0 (b - a)
      omega
  | cons c cs =>
      simp only [List.head?_cons, Option.getD_some]
      have htri := up_triangle a b c
      omega

private theorem filter_up_bound (xs : Series) (keep : Int → Bool) : ∀ a : Int,
    PositiveJumps.measure (xs.filter keep) + up a ((xs.filter keep).head?.getD a) ≤
      PositiveJumps.measure xs + up a (xs.head?.getD a) := by
  induction xs with
  | nil =>
      intro a
      simp [PositiveJumps.measure, delta, adj, up]
  | cons b bs ih =>
      intro a
      by_cases hb : keep b = true
      · have hih := ih b
        calc
          PositiveJumps.measure ((b :: bs).filter keep) + up a (((b :: bs).filter keep).head?.getD a)
              = PositiveJumps.measure (b :: bs.filter keep) + up a b := by simp [hb]
          _ = (PositiveJumps.measure (bs.filter keep) + up b ((bs.filter keep).head?.getD b)) + up a b := by
                rw [measure_cons]
          _ ≤ (PositiveJumps.measure bs + up b (bs.head?.getD b)) + up a b := by omega
          _ = PositiveJumps.measure (b :: bs) + up a ((b :: bs).head?.getD a) := by
                rw [measure_cons]
                simp
      · have hih := ih a
        have htri := head_triangle a b bs
        calc
          PositiveJumps.measure ((b :: bs).filter keep) + up a (((b :: bs).filter keep).head?.getD a)
              = PositiveJumps.measure (bs.filter keep) + up a ((bs.filter keep).head?.getD a) := by simp [hb]
          _ ≤ PositiveJumps.measure bs + up a (bs.head?.getD a) := hih
          _ ≤ PositiveJumps.measure bs + (up b (bs.head?.getD b) + up a b) := by omega
          _ = PositiveJumps.measure (b :: bs) + up a ((b :: bs).head?.getD a) := by
                rw [measure_cons]
                simp
                omega

theorem sound_BoundaryJump :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.BoundaryJump.Goal := by
  intro x y
  exact measure_append x y

theorem sound_ReverseBalance :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.ReverseBalance.Goal := by
  intro x
  have hmain : PositiveJumps.measure x - PositiveJumps.measure x.reverse = (delta x).sum := by
    refine snoc_induction
      (motive := fun z => PositiveJumps.measure z - PositiveJumps.measure z.reverse = (delta z).sum)
      ?_ ?_ x
    · simp [PositiveJumps.measure, delta, adj]
    · intro xs a ih
      rw [delta_sum]
      rw [getLast_snoc_getD]
      rw [measure_snoc]
      rw [show (xs ++ [a]).reverse = a :: xs.reverse by simp]
      rw [measure_cons]
      have hih := ih
      rw [delta_sum] at hih
      cases xs with
      | nil =>
          simp [PositiveJumps.measure, delta, adj, up]
      | cons b bs =>
          have hlast : (b :: bs).getLast?.getD a = (b :: bs).getLast?.getD 0 := by
            simp [List.getLast?_cons]
          have hrevhead : (b :: bs).reverse.head?.getD a = (b :: bs).getLast?.getD 0 := by
            rw [List.head?_reverse]
            simp [List.getLast?_cons]
          rw [hlast, hrevhead]
          have hhead : (b :: bs ++ [a]).head?.getD 0 = b := rfl
          rw [hhead]
          have hhead0 : (b :: bs).head?.getD 0 = b := rfl
          rw [hhead0] at hih
          have hposneg := pos_neg_diff (a - (b :: bs).getLast?.getD 0)
          dsimp [up] at hposneg ⊢
          omega
  rw [hmain, delta_sum]

theorem sound_SharedMagnitude :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.SharedMagnitude.Goal := by
  intro x
  have h := twice_pos_sum (delta x)
  rw [delta_sum] at h
  dsimp [PositiveJumps.measure, TotalVariation.measure] at h ⊢
  omega

theorem sound_NonincreasingSkip :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.NonincreasingSkip.Goal := by
  intro x hx
  have hd := delta_nonpos x hx
  simpa [PositiveJumps.measure] using sum_max_zero_of_nonpos (delta x) hd

theorem sound_FilterBudget :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PositiveJumps.FilterBudget.Goal := by
  intro x keep
  let a : Int := x.head?.getD 0
  have h := filter_up_bound x keep a
  have hzero : up a (x.head?.getD a) = 0 := by
    cases x with
    | nil => simp [a, up]
    | cons b bs => simp [a, up]
  rw [hzero] at h
  have hnonneg : 0 ≤ up a ((x.filter keep).head?.getD a) := by
    dsimp [up]
    exact Int.le_max_left 0 _
  omega

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures.PositiveJumps.Remaining
