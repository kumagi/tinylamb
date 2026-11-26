import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `WrappedSum.SoundInterval` — Bound a computed group or predicate key from independent
  column min/max.
- `WrappedSum.SumCeiling` — A footer gives a deterministic interval for the SUM
  accumulator over non-NULL scores.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

-- Executable version of `covers` for the small-input truth check below.
private def coversBool (p : List (Option Row)) (a b : Bounds) : Bool :=
  p.all (fun r => match r with
    | none => true
    | some (x, y) => (a.lo <= x && x <= a.hi) && (b.lo <= y && y <= b.hi))

-- Small-input truth check: exhaustively enumerate all pages over a fixed pool
-- of rows and all bounds endpoints in 0..3, checking every covered page.
-- Result: true, so the goal is expected to be provable.
#eval let rows : List (Option Row) := [none, some (0, 0), some (1, 1), some (2, 3), some (3, 0)]
      let indexed := (List.range rows.length).zip rows
      let pages : List (List (Option Row)) :=
        (List.range 32).map (fun k =>
          (indexed.filter (fun ir => (k / 2 ^ ir.1) % 2 == 1)).map Prod.snd)
      let ns := List.range 4
      pages.all (fun p => ns.all (fun t => ns.all (fun al => ns.all (fun ah =>
        ns.all (fun bl => ns.all (fun bh =>
          let a : Bounds := ⟨al, ah⟩
          let b : Bounds := ⟨bl, bh⟩
          if coversBool p a b then
            let vs := scores kernel t p
            decide (vs.length * (kernel.bounds t a b).lo ≤ vs.sum ∧
              vs.sum ≤ vs.length * (kernel.bounds t a b).hi)
          else true))))))

-- Within one period, equal quotients make the remainder monotone; across a
-- period boundary, [0,t] is a conservative interval. This is the existing
-- WrappedSum soundInterval argument, repeated locally for self-containment.
private theorem mod_le_mod_of_div_eq {d s₁ s₂ : Nat} (hd : 0 < d)
    (hdiv : s₁ / d = s₂ / d) (hle : s₁ ≤ s₂) : s₁ % d ≤ s₂ % d := by
  have h1 := (Nat.div_mod_unique hd).1
    (And.intro rfl rfl : s₁ / d = s₁ / d ∧ s₁ % d = s₁ % d)
  have h2 := (Nat.div_mod_unique hd).1
    (And.intro hdiv.symm rfl : s₂ / d = s₁ / d ∧ s₂ % d = s₂ % d)
  omega

private theorem wrapped_score_bounds : WrappedSum.SoundInterval.Goal := by
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

/-- Every non-NULL score of a covered page lies in the certified interval. -/
private theorem inside_scores {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    inside (kernel.bounds t a b) v := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hxy := hc r hrp x y hrxy
  rw [← hscore]
  exact wrapped_score_bounds t a b x y hxy.1 hxy.2

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

theorem sound_SumCeiling : WrappedSum.SumCeiling.Goal := by
  intro t p a b hc
  exact sum_bounds (fun v hv => inside_scores (t := t) (p := p) (a := a) (b := b) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SumCeiling
