import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `WrappedSum.CrossPageJoin` — Disjoint derived score intervals imply an empty equijoin
  between pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_CrossPageJoin

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

-- 真偽確認: t, u と各端点・行を小さい grid で全網羅し、仮説を満たして
-- スコアが衝突する例が無い（真）ことを確認する。
private def smallBounds : List Bounds :=
  (List.range 4).flatMap fun lo =>
    ((List.range 4).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def smallRows : List (Option Row) :=
  [none, some (0, 0), some (0, 1), some (1, 0), some (1, 1),
    some (2, 1), some (1, 2), some (3, 2), some (2, 3), some (3, 3)]

private def smallPages : List (List (Option Row)) :=
  [[]] ++ (smallRows.map fun r => [r]) ++
    [[some (0, 1), none, some (2, 2)], [some (1, 3), some (3, 1)]]

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

private theorem mod_le_mod_of_div_eq {d s₁ s₂ : Nat} (hd : 0 < d)
    (hdiv : s₁ / d = s₂ / d) (hle : s₁ ≤ s₂) : s₁ % d ≤ s₂ % d := by
  have h1 := (Nat.div_mod_unique hd).1
    (And.intro rfl rfl : s₁ / d = s₁ / d ∧ s₁ % d = s₁ % d)
  have h2 := (Nat.div_mod_unique hd).1
    (And.intro hdiv.symm rfl : s₂ / d = s₁ / d ∧ s₂ % d = s₂ % d)
  omega

-- WrappedSum.soundInterval と同じ論法（周期内は剰余が単調、またぎなら [0,t]）。
private theorem sound : soundInterval kernel := by
  intro t a b x y hx hy
  rcases hx with ⟨ha_lo, ha_hi⟩
  rcases hy with ⟨hb_lo, hb_hi⟩
  by_cases hq : (a.lo + b.lo) / (t + 1) = (a.hi + b.hi) / (t + 1)
  · simp only [kernel, hq, ↓reduceIte, inside]
    have hsum_lo : a.lo + b.lo ≤ x + y := by omega
    have hsum_hi : x + y ≤ a.hi + b.hi := by omega
    have hq_lo : (a.lo + b.lo) / (t + 1) = (x + y) / (t + 1) := by
      have h := Nat.div_le_div_right (c := t + 1) hsum_lo
      have h' := Nat.div_le_div_right (c := t + 1) hsum_hi
      omega
    have hq_hi : (x + y) / (t + 1) = (a.hi + b.hi) / (t + 1) := by
      have h := Nat.div_le_div_right (c := t + 1) hsum_lo
      have h' := Nat.div_le_div_right (c := t + 1) hsum_hi
      omega
    constructor
    · exact mod_le_mod_of_div_eq (by omega) hq_lo hsum_lo
    · exact mod_le_mod_of_div_eq (by omega) hq_hi hsum_hi
  · simp only [kernel, hq, ↓reduceIte, inside]
    constructor
    · exact Nat.zero_le _
    · exact Nat.le_of_lt_succ (Nat.mod_lt (x + y) (by omega))

private theorem score_mem {t : Nat} {p : List (Option Row)} {v : Nat}
    (hv : v ∈ scores kernel t p) :
    ∃ r ∈ p, ∃ x y, r = some (x, y) ∧ v = kernel.score t (x, y) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  exact ⟨r, hr, x, y, hrxy, hscore.symm⟩

private theorem score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    inside (kernel.bounds t a b) v := by
  rcases score_mem hv with ⟨r, hr, x, y, hrxy, rfl⟩
  have hcov := hc r hr x y hrxy
  exact sound t a b x y hcov.1 hcov.2

theorem sound_CrossPageJoin : WrappedSum.CrossPageJoin.Goal := by
  intro t u p q a b c d hcp hcq hsep x hx hxq
  have hb1 := score_bounds (t := t) (p := p) (a := a) (b := b) hcp hx
  have hb2 := score_bounds (t := u) (p := q) (a := c) (b := d) hcq hxq
  simp only [inside] at hb1 hb2
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_CrossPageJoin
