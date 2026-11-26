import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `ReversedRank.InversionPairs` — `∀ (ps : List Point), ps.Pairwise (fun a b => a.x <
  b.x) → (ps.map (fun p => ps.countP (shape.qualifies p))).sum = 2*(ps.zipIdx.map
  (fun (p,i) => (ps.drop (i+1)).countP (fun q => q.y < p.y))).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReversedRank_InversionPairs

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ReversedRank

/-- Number of index-ordered pairs whose second coordinate is inverted. -/
private def invPairs (ps : List Point) : Nat :=
  (ps.zipIdx.map (fun (p, i) => (ps.drop (i + 1)).countP (fun q => q.y < p.y))).sum

private theorem zipIdx_add {α : Type} (xs : List α) (n : Nat) :
    xs.zipIdx (n + 1) = (xs.zipIdx n).map (fun z => (z.1, z.2 + 1)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp [List.zipIdx_cons, ih, Nat.add_assoc]

private theorem invPairs_cons (p : Point) (xs : List Point) :
    invPairs (p :: xs) = invPairs xs + xs.countP (fun q => q.y < p.y) := by
  unfold invPairs
  rw [List.zipIdx_cons (i := 0)]
  simp only [List.map_cons, List.sum_cons]
  have hdrop0 : (p :: xs).drop (0 + 1) = xs := by simp
  have htail :
      ((xs.zipIdx 1).map (fun z =>
        ((p :: xs).drop (z.2 + 1)).countP (fun r : Point => r.y < z.1.y))).sum =
      (xs.zipIdx.map (fun z =>
        (xs.drop (z.2 + 1)).countP (fun r : Point => r.y < z.1.y))).sum := by
    rw [zipIdx_add xs 0]
    simp only [List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro z hz
    simp only [Function.comp_apply]
    have hdrop : (p :: xs).drop ((z.2 + 1) + 1) = xs.drop (z.2 + 1) := by
      rw [show (z.2 + 1) + 1 = Nat.succ (z.2 + 1) by omega, List.drop_succ_cons]
    rw [hdrop]
  rw [hdrop0, htail]
  omega

private theorem sum_boole {α : Type} (xs : List α) (f : α → Bool) :
    (xs.map (fun x => if f x then 1 else 0)).sum = xs.countP f := by
  induction xs with
  | nil => simp
  | cons x xs ih => by_cases h : f x = true <;> simp [h, ih] <;> omega

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem qualifies_lt (p q : Point) (hx : p.x < q.x) :
    shape.qualifies p q = (q.y < p.y) := by
  have hx' : ¬ q.x < p.x := by omega
  simp [shape, hx, hx']

private theorem qualifies_gt (p q : Point) (hx : p.x < q.x) :
    shape.qualifies q p = (q.y < p.y) := by
  have hx' : ¬ q.x < p.x := by omega
  simp [shape, hx, hx']

private theorem qualCount_cons (p : Point) (xs : List Point) :
    ((p :: xs).map (fun r => (p :: xs).countP (shape.qualifies r))).sum =
      xs.countP (shape.qualifies p) +
        (xs.map (fun r => xs.countP (shape.qualifies r))).sum +
        xs.countP (fun r => shape.qualifies r p) := by
  simp only [List.map_cons, List.sum_cons]
  have hself : shape.qualifies p p = false := by simp [shape]
  have hmap : (xs.map (fun r => (p :: xs).countP (shape.qualifies r))) =
      xs.map (fun r => xs.countP (shape.qualifies r) + (if shape.qualifies r p then 1 else 0)) := by
    apply List.map_congr_left
    intro r hr
    rw [List.countP_cons]
  rw [hmap, sum_map_add, sum_boole]
  rw [List.countP_cons, hself]
  simp
  omega

theorem sound_ReversedRank_InversionPairs : ReversedRank.InversionPairs.Goal := by
  intro ps hps
  have hmain : (ps.map (fun p => ps.countP (shape.qualifies p))).sum = 2 * invPairs ps := by
    induction ps with
    | nil => simp [invPairs]
    | cons p xs ih =>
        have hpair := List.pairwise_cons.mp hps
        have htail : xs.Pairwise (fun a b => a.x < b.x) := hpair.2
        have hhead : ∀ q ∈ xs, p.x < q.x := hpair.1
        rw [qualCount_cons, ih htail]
        have hfw : xs.countP (shape.qualifies p) = xs.countP (fun q => q.y < p.y) := by
          apply List.countP_congr
          intro q hq
          rw [qualifies_lt p q (hhead q hq)]
          simp
        have hbw : xs.countP (fun r => shape.qualifies r p) = xs.countP (fun q => q.y < p.y) := by
          apply List.countP_congr
          intro q hq
          rw [qualifies_gt p q (hhead q hq)]
          simp
        rw [hfw, hbw, invPairs_cons]
        omega
  rw [hmain]
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_ReversedRank_InversionPairs
