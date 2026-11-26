import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PeakCount.BoundaryPeaks` — `∀ x y, measure (x++y) = measure x + measure y + measure
  ((x.reverse.take 2).reverse ++ y.take 2)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_BoundaryPeaks

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

abbrev M := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.measure

def vecs : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (List.range 5).flatMap (fun i => (vecs n).map (fun xs => (Int.ofNat i - 2) :: xs))

-- Exhaustive sanity check over all lists of length <= 4 with entries in [-2, 2].
#eval! (vecs 4).all (fun x => (vecs 4).all (fun y =>
  decide (M (x ++ y) = M x + M y + M (((x.reverse.take 2).reverse) ++ y.take 2))))

def pc (l : List Int) (p : Int × Nat) : Bool :=
  decide (0 < p.2) && (l[p.2 - 1]?.any (fun u => decide (u < p.1))) &&
    (l[p.2 + 1]?.any (fun u => decide (u < p.1)))

theorem m_eq_count (l : List Int) : M l = (l.zipIdx.countP (pc l) : Int) := by
  rfl

theorem zipIdx_succ (l : List Int) (n : Nat) :
    l.zipIdx (n + 1) = (l.zipIdx n).map (fun p => (p.1, p.2 + 1)) := by
  induction l generalizing n with
  | nil => simp
  | cons v vs ih => simp [List.zipIdx_cons, ih, Nat.add_assoc]

theorem pc_shift (a b c v : Int) (l : List Int) (k : Nat) :
    pc (a :: b :: c :: l) (v, k + 2) = pc (b :: c :: l) (v, k + 1) := by
  simp [pc]

theorem count_pc_shift (a b c : Int) (l : List Int) (k : Nat) :
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

theorem measure_cons3 (a b c : Int) (xs : List Int) :
    M (a :: b :: c :: xs) = (if a < b && c < b then 1 else 0) + M (b :: c :: xs) := by
  rw [m_eq_count, m_eq_count]
  simp [List.zipIdx, List.countP_cons, pc, count_pc_shift a b c xs 1]
  split <;> split <;> omega

theorem M_nil : M ([] : List Int) = 0 := by
  simp [m_eq_count]

theorem M_one (a : Int) : M [a] = 0 := by
  simp [m_eq_count, pc]

theorem M_two (a b : Int) : M [a, b] = 0 := by
  simp [m_eq_count, pc]

theorem M_cons_one (a : Int) (y : List Int) : M (a :: y) = M y + M ([a] ++ y.take 2) := by
  cases y with
  | nil => simp [M_nil, M_one]
  | cons v vs =>
      cases vs with
      | nil => simp [M_one, M_two]
      | cons w ws =>
          rw [measure_cons3]
          simp [measure_cons3, M_two]
          omega

theorem M_cons_two (a b : Int) (y : List Int) :
    M (a :: b :: y) = M y + M ([a, b] ++ y.take 2) := by
  cases y with
  | nil => simp [M_nil, M_two]
  | cons v vs =>
      cases vs with
      | nil =>
          rw [measure_cons3]
          simp [M_one, measure_cons3, M_two]
      | cons w ws =>
          rw [measure_cons3]
          simp [measure_cons3, M_two]
          omega

theorem take_two_append_of_length (l u v : List Int) (h : 2 ≤ l.length) :
    (l ++ u).take 2 = (l ++ v).take 2 := by
  cases l with
  | nil => simp at h
  | cons a l' =>
      cases l' with
      | nil => simp at h
      | cons b l'' => simp

theorem last2_cons3 (a b c : Int) (xs : List Int) :
    ((a :: b :: c :: xs).reverse.take 2).reverse = ((b :: c :: xs).reverse.take 2).reverse := by
  cases xs with
  | nil => simp [List.reverse_cons]
  | cons d ds =>
      cases ds with
      | nil => simp [List.reverse_cons]
      | cons e es =>
          cases es with
          | nil => simp [List.reverse_cons]
          | cons f fs =>
              cases fs with
              | nil => simp [List.reverse_cons]
              | cons g gs =>
                  cases gs with
                  | nil => simp [List.reverse_cons]
                  | cons h hs =>
                      cases hs with
                      | nil => simp [List.reverse_cons]
                      | cons i is =>
                          simpa [List.reverse_cons] using
                            (take_two_append_of_length (is.reverse ++ [i, h])
                              [g, f, e, d, c, b, a] [g, f, e, d, c, b]
                              (by simp))

theorem M_of_length_le_two (l : List Int) (h : l.length ≤ 2) : M l = 0 := by
  cases l with
  | nil => simp [M_nil]
  | cons a l' =>
      cases l' with
      | nil => simp [M_one]
      | cons b l'' =>
          cases l'' with
          | nil => simp [M_two]
          | cons c l''' => simp at h

theorem sound_PeakCount_BoundaryPeaks : TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.PeakCount.BoundaryPeaks.Goal := by
  intro x y
  induction x with
  | nil =>
      have hy : M (y.take 2) = 0 := M_of_length_le_two _ (by simp [Nat.min_le_left])
      simp [M_nil, hy]
  | cons a xs ih =>
      cases xs with
      | nil =>
          simpa [M_one] using M_cons_one a y
      | cons b ys =>
          cases ys with
          | nil =>
              simpa [M_two] using M_cons_two a b y
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

end TinylambOpt.ResearchProofs.Atlas.Goal_PeakCount_BoundaryPeaks
