import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_RowIncidence

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_LocalCorrection

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.FrameTotals

/-! 小さい入力で真偽を総当たり確認する。長さ 0..2、各要素 -2..2 の列 x, y について、
    中央の一行を a から b に差し替えたときの FrameTotals.measure の差が
    (b-a)*(size x+1)*(size y+1) に等しいことを確かめる。 -/
def vecs : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (List.range 5).flatMap (fun i =>
      (vecs n).map (fun xs => (Int.ofNat i - 2) :: xs))

#eval! (vecs 2).all (fun x => (vecs 2).all (fun y =>
  decide (measure (x ++ (-3 :: y)) - measure (x ++ (4 :: y)) =
    ((-3 : Int) - 4) * (size x + 1) * (size y + 1))))

/-- zipIdx の append に対する挙動。 -/
private theorem zipIdx_append {α : Type} (xs ys : List α) (n : Nat) :
    (xs ++ ys).zipIdx n = xs.zipIdx n ++ ys.zipIdx (n + xs.length) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.cons_append, List.zipIdx_cons, List.length_cons, ih]
      congr 1
      ac_rfl

theorem sound_FrameTotals_LocalCorrection : FrameTotals.LocalCorrection.Goal := by
  intro x y a b
  rw [TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_RowIncidence.sound_FrameTotals_RowIncidence,
      TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_RowIncidence.sound_FrameTotals_RowIncidence]
  simp only [List.length_append, List.length_cons, zipIdx_append, List.zipIdx_cons,
    List.map_append, List.map_cons, List.sum_append, List.sum_cons, Nat.zero_add]
  have hidx : x.length + (y.length + 1) - x.length = y.length + 1 := by omega
  rw [hidx]
  simp only [size, Int.natCast_add, Int.natCast_one, Int.mul_add, Int.add_mul,
    Int.sub_eq_add_neg, Int.neg_add, Int.neg_mul, Int.mul_one]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_FrameTotals_LocalCorrection
