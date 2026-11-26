import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ReciprocalPrice.MonotoneProbe` — `∀ u t x z ys, valid u ys → x≤z → fan search u t z
  ys ≤ fan search u t x ys`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_MonotoneProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice

/-! ## Monotonicity of the binary lower-bound offset

The upper end of the postings interval only shrinks as the left key grows, and
`lower` is monotone in its threshold (no sortedness is needed for that). -/

private theorem searchBound_bounds (xs : Array Nat) (v fuel lo hi : Nat) (hlo : lo ≤ hi) :
    lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero => simp [searchBound, hlo]
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have : lo = hi := by omega
        subst hi
        simp [searchBound]
      · simp only [searchBound, hge, ↓reduceIte]
        by_cases hv : xs[(lo + hi) / 2]?.getD 0 < v
        · simp [hv]
          have h := ih ((lo + hi) / 2 + 1) hi (by omega)
          omega
        · simp [hv]
          have h := ih lo ((lo + hi) / 2) (by omega)
          omega

private theorem searchBound_mono {xs : Array Nat} {v v' : Nat} (hvv : v ≤ v') :
    ∀ fuel lo hi, searchBound xs v fuel lo hi ≤ searchBound xs v' fuel lo hi := by
  intro fuel
  induction fuel with
  | zero => intro lo hi; simp [searchBound]
  | succ fuel ih =>
      intro lo hi
      by_cases hge : lo ≥ hi
      · simp [searchBound, hge]
      · simp only [searchBound, hge, ↓reduceIte]
        by_cases hv : xs[(lo + hi) / 2]?.getD 0 < v
        · have hv' : xs[(lo + hi) / 2]?.getD 0 < v' := Nat.lt_of_lt_of_le hv hvv
          simp [hv, hv']
          exact ih ((lo + hi) / 2 + 1) hi
        · by_cases hv' : xs[(lo + hi) / 2]?.getD 0 < v'
          · simp [hv, hv']
            have h₁ := (searchBound_bounds xs v fuel lo ((lo + hi) / 2) (by omega)).2
            have h₂ := (searchBound_bounds xs v' fuel ((lo + hi) / 2 + 1) hi (by omega)).1
            omega
          · simp [hv, hv']
            exact ih lo ((lo + hi) / 2)

private theorem searchBound_zero (xs : Array Nat) :
    ∀ fuel hi, searchBound xs 0 fuel 0 hi = 0 := by
  intro fuel
  induction fuel with
  | zero => intro hi; simp [searchBound]
  | succ fuel ih =>
      intro hi
      by_cases hge : 0 ≥ hi
      · have : hi = 0 := by omega
        subst hi
        simp [searchBound]
      · simp [searchBound, hge, Nat.not_lt_zero, ih]

private theorem lower_zero (ys : List Nat) : lower ys 0 = 0 := by
  unfold lower
  simpa using searchBound_zero ys.toArray (width ys.length + 1) ys.length

private theorem lower_mono {ys : List Nat} {a b : Nat} (hab : a ≤ b) :
    lower ys a ≤ lower ys b := by
  unfold lower
  exact searchBound_mono hab (width ys.length + 1) 0 ys.length

private abbrev opt := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional

private theorem opt_eq (a b : Nat) : opt a b = if a < b then [(a, b)] else [] := rfl

private theorem fan_eq_lower (u t v : Nat) (ys : List Nat) :
    fan ReciprocalPrice.search u t v ys =
      lower ys (if v == 0 then u else min u (t / v + 1)) := by
  let U := if v == 0 then u else min u (t / v + 1)
  change ((opt 0 U).map (fun p : Nat × Nat => lower ys p.2 - lower ys p.1)).sum =
    lower ys U
  by_cases h : 0 < U
  · simp [opt_eq, h, lower_zero]
  · have hz : U = 0 := by omega
    simp [opt_eq, lower_zero, hz]

theorem sound_MonotoneProbe : ReciprocalPrice.MonotoneProbe.Goal := by
  intro u t x z ys hv hxz
  rw [fan_eq_lower u t z ys, fan_eq_lower u t x ys]
  apply lower_mono
  by_cases hx : x = 0
  · subst x
    by_cases hz : z = 0
    · simp [hz]
    · simp [hz]
      omega
  · have hxpos : 0 < x := Nat.pos_of_ne_zero hx
    have hzpos : 0 < z := Nat.lt_of_lt_of_le hxpos hxz
    have hz0 : z ≠ 0 := Nat.ne_of_gt hzpos
    have hdiv : t / z ≤ t / x := Nat.div_le_div_left hxz hxpos
    simp [beq_iff_eq, hx, hz0]
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_MonotoneProbe
