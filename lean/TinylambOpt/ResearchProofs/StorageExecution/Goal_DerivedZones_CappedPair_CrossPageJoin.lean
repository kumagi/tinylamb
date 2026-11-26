import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_CrossPageJoin

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

-- 真偽確認: t, u と各 Bounds の lo/hi を 0..3、ページを空/NULL/少数行に限定して
-- 全組み合わせを走査する。仮説 (bounds が分離) を満たしてスコアが一致する
-- 組は存在しない (#eval の結果は [])。
private def smallBounds : List Bounds :=
  (List.range 4).flatMap fun lo =>
    ((List.range 4).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def singletonPages : List (List (Option Row)) :=
  [none, some (0, 0), some (3, 1), some (1, 3), some (2, 2)].map fun r => [r]

private def smallPages : List (List (Option Row)) :=
  [[]] ++ singletonPages ++ [[some (3, 1), none, some (1, 2)]]

/-- Decidable boolean form of `covers`, used only by the brute-force scan. -/
private def coversB (p : List (Option Row)) (a b : Bounds) : Bool :=
  p.all fun r =>
    match r with
    | none => true
    | some (x, y) =>
        decide (a.lo ≤ x) && decide (x ≤ a.hi) &&
          decide (b.lo ≤ y) && decide (y ≤ b.hi)

private def counterexamples :
    List (Nat × Nat × List (Option Row) × List (Option Row) × Bounds × Bounds × Bounds × Bounds) :=
  (List.range 4).flatMap fun t =>
  (List.range 4).flatMap fun u =>
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  smallBounds.flatMap fun c =>
  smallBounds.flatMap fun d =>
  smallPages.flatMap fun p =>
  smallPages.filterMap fun q =>
    if coversB p a b && coversB q c d &&
        decide ((kernel.bounds t a b).hi < (kernel.bounds u c d).lo) &&
          (scores kernel t p).any (fun x => (scores kernel u q).contains x) then
      some (t, u, p, q, a, b, c, d)
    else none

#eval counterexamples

private theorem score_mem {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores kernel t p) :
    ∃ r ∈ p, ∃ x y, r = some (x, y) ∧ v = kernel.score t (x, y) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  exact ⟨r, hr, x, y, hrxy, hscore.symm⟩

/-- `min` is monotone in both arguments, so a score of a covered page lies in
    the certified interval: min lo lo ≤ min x y ≤ min hi hi, and the outer
    cap preserves both inequalities. -/
private theorem score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    inside (kernel.bounds t a b) v := by
  rcases score_mem hv with ⟨r, hr, x, y, hrxy, rfl⟩
  have hcov := hc r hr x y hrxy
  simp [inside, kernel] at hcov ⊢
  omega

theorem sound_CrossPageJoin : CappedPair.CrossPageJoin.Goal := by
  intro t u p q a b c d hcp hcq hsep x hx hxq
  have hb1 := score_bounds (t := t) (p := p) (a := a) (b := b) hcp hx
  have hb2 := score_bounds (t := u) (p := q) (a := c) (b := d) hcq hxq
  simp only [inside, kernel] at hb1 hb2 hsep
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_CrossPageJoin
