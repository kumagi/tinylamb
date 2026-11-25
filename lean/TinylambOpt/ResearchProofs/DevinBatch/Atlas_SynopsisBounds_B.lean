import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.Atlas_SynopsisBounds_B

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.HeavyBounds

/-!
# Atlas.SynopsisBounds — batch B

Assigned goals:
* `Couplings.TransportLower.Goal`
* `Couplings.PrefixTransport.Goal`
* `HeavyBounds.TailFanout.Goal`      — already proved in
  `TinylambOpt.ResearchProofs.Atlas.SynopsisBounds`
  (`sound_HeavyBounds_TailFanout`).
* `HeavyBounds.MonotoneRefine.Goal`  — already proved there
  (`sound_HeavyBounds_MonotoneRefine`).
* `HeavyBounds.AliasError.Goal`      — already proved there
  (`sound_HeavyBounds_AliasError`).

The two `Couplings` goals are the one-dimensional optimal-transport facts:
the monotone (sorted) coupling minimizes `Σ |x - y|`, and its cost equals the
integral of the marginal-CDF gap.  We prove them through the "layer cake"
decomposition `|x - y| = #{t : exactly one of x ≤ t, y ≤ t}`.
-/

namespace CouplingsAux

/-- Exactly one of `x ≤ t`, `y ≤ t` holds (XOR of the two order tests). -/
private def crossB (x y t : Int) : Bool :=
  !(decide (x ≤ t) == decide (y ≤ t))

private theorem crossB_eq_true {x y t : Int} :
    crossB x y t = true ↔ (min x y ≤ t ∧ t < max x y) := by
  unfold crossB
  by_cases hx : x ≤ t <;> by_cases hy : y ≤ t <;>
    simp [hx, hy] <;> omega

/-- Number of `i ∈ [0, N)` with `lo ≤ i < hi`, for arbitrary `lo hi`. -/
private theorem range_countP_Ico : ∀ (N lo hi : Nat),
    (List.range N).countP (fun i => lo ≤ i && i < hi) = min hi N - min lo N
  | 0, _, _ => by simp
  | N + 1, lo, hi => by
      rw [List.range_succ, List.countP_append, List.countP_singleton,
        range_countP_Ico N lo hi]
      by_cases h1 : lo ≤ N <;> by_cases h2 : N < hi <;>
        simp [h1, h2] <;> omega

/-- Thresholds `{-B, -B+1, ..., B-1}` as integers. -/
private def thr (B : Nat) : List Int :=
  (List.range (2 * B)).map fun i : Nat => (i : Int) - (B : Int)

/-- Layer cake: `(x - y).natAbs` equals the number of thresholds `t ∈ [-B, B)`
where exactly one of `x ≤ t`, `y ≤ t` holds, provided `|x|, |y| < B`. -/
private theorem pair_cost (B : Nat) (x y : Int)
    (hx : -(B : Int) < x) (hxb : x < (B : Int))
    (hy : -(B : Int) < y) (hyb : y < (B : Int)) :
    (x - y).natAbs = (thr B).countP (fun t => crossB x y t) := by
  rcases Int.le_total x y with hxy | hyx
  · unfold thr
    rw [List.countP_map]
    show (x - y).natAbs = (List.range (2 * B)).countP
      (fun i : Nat => crossB x y ((i : Int) - (B : Int)))
    have hcongr : (List.range (2 * B)).countP
          (fun i : Nat => crossB x y ((i : Int) - (B : Int))) =
        (List.range (2 * B)).countP
          (fun i : Nat => ((min x y + (B : Int)).toNat ≤ i) &&
            (i < (max x y + (B : Int)).toNat)) := by
      apply List.countP_congr
      intro i _
      rw [crossB_eq_true, Int.min_eq_left hxy, Int.max_eq_right hxy]
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      constructor <;> rintro ⟨h1, h2⟩ <;> refine ⟨?_, ?_⟩ <;> omega
    rw [hcongr, range_countP_Ico]
    omega
  · unfold thr
    rw [List.countP_map]
    show (x - y).natAbs = (List.range (2 * B)).countP
      (fun i : Nat => crossB x y ((i : Int) - (B : Int)))
    have hcongr : (List.range (2 * B)).countP
          (fun i : Nat => crossB x y ((i : Int) - (B : Int))) =
        (List.range (2 * B)).countP
          (fun i : Nat => ((min x y + (B : Int)).toNat ≤ i) &&
            (i < (max x y + (B : Int)).toNat)) := by
      apply List.countP_congr
      intro i _
      rw [crossB_eq_true, Int.min_eq_right hyx, Int.max_eq_left hyx]
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      constructor <;> rintro ⟨h1, h2⟩ <;> refine ⟨?_, ?_⟩ <;> omega
    rw [hcongr, range_countP_Ico]
    omega

/-- `countP` as a sum of `0/1` indicators. -/
private theorem countP_eq_sum_indicator {α : Type} (p : α → Bool) :
    ∀ l : List α, l.countP p = (l.map fun a => if p a then (1 : Nat) else 0).sum
  | [] => rfl
  | a :: l => by
      rw [List.countP_cons, countP_eq_sum_indicator p l, List.map_cons,
        List.sum_cons]
      cases h : p a <;> simp [h] <;> omega

/-- `(l.map (f + g)).sum = (l.map f).sum + (l.map g).sum` for `ℕ`-valued maps. -/
private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    (l.map fun a => f a + g a).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [List.map_cons, List.sum_cons, ih, Nat.add_assoc,
      Nat.add_comm, Nat.add_left_comm]

/-- Swap the order of a double `List.sum`. -/
private theorem sum_sum_swap {α β : Type} (f : α → β → Nat)
    (l : List α) (m : List β) :
    (l.map fun a => (m.map fun b => f a b).sum).sum =
      (m.map fun b => (l.map fun a => f a b).sum).sum := by
  induction l with
  | nil => simp [List.sum_eq_zero_iff]
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      simp only [List.map_cons, List.sum_cons, sum_map_add_nat]

end CouplingsAux

end TinylambOpt.ResearchProofs.DevinBatch.Atlas_SynopsisBounds_B
