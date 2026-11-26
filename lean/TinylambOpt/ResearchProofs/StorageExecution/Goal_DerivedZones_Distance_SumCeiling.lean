import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Distance.SumCeiling` — A footer gives a deterministic interval for the SUM
  accumulator over non-NULL scores.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

-- 真偽確認: lo <= hi な小さい区間と小さいページを全網羅し、covers を満たす
-- 入力について和が len*gapMin .. len*gapMax に収まることを確かめる。
private def smallBounds : List Bounds :=
  (List.range 5).flatMap fun lo =>
    ((List.range 5).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def coversB (p : List (Option Row)) (a b : Bounds) : Bool :=
  p.all fun r =>
    match r with
    | none => true
    | some (x, y) =>
        decide (a.lo ≤ x) && decide (x ≤ a.hi) &&
          decide (b.lo ≤ y) && decide (y ≤ b.hi)

private def smallPages : List (List (Option Row)) :=
  [[]] ++
    ([none, some (0, 0), some (3, 1), some (1, 3), some (2, 2), some (4, 4)].map fun r => [r]) ++
    [[some (3, 1), none, some (1, 2)]]

private def counterexamples : List (Nat × List (Option Row) × Bounds × Bounds) :=
  (List.range 4).flatMap fun t =>
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  smallPages.filterMap fun p =>
    let vs := scores kernel t p
    if coversB p a b &&
        decide ¬ (vs.length * (kernel.bounds t a b).lo ≤ vs.sum ∧
          vs.sum ≤ vs.length * (kernel.bounds t a b).hi) then
      some (t, p, a, b)
    else none

#eval counterexamples.length

-- 独立な列の min/max から得られる区間が、各スコア（切断絶対差）を挟む。
-- Distance の soundInterval と同じ gap 評価を流用する。
private theorem gap_bounds {a b : Bounds} {x y : Nat}
    (hx : inside a x) (hy : inside b y) :
    gapMin a b ≤ gap x y ∧ gap x y ≤ gapMax a b := by
  simp only [inside] at hx hy
  simp only [gapMin, gapMax, gap]
  constructor <;> omega

/-- Every non-NULL score of a covered page lies in the derived interval. -/
private theorem score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    (kernel.bounds t a b).lo ≤ v ∧ v ≤ (kernel.bounds t a b).hi := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcovxy := hc r hrp x y hrxy
  rw [← hscore]
  dsimp [kernel]
  exact gap_bounds hcovxy.1 hcovxy.2

/-- A list of n values, each between lo and hi, has its sum between n*lo
    and n*hi. -/
private theorem sum_bounds {lo hi : Nat} {vs : List Nat}
    (hv : ∀ v ∈ vs, lo ≤ v ∧ v ≤ hi) :
    vs.length * lo ≤ vs.sum ∧ vs.sum ≤ vs.length * hi := by
  induction vs with
  | nil => simp
  | cons v vs ih =>
    have hhead := hv v (by simp)
    have htail := ih (fun x hx => hv x (by simp [hx]))
    simp only [List.length_cons, List.sum_cons, Nat.add_mul]
    omega

theorem sound_SumCeiling : Distance.SumCeiling.Goal := by
  intro t p a b hc
  exact sum_bounds (fun v hv => score_bounds (t := t) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SumCeiling
