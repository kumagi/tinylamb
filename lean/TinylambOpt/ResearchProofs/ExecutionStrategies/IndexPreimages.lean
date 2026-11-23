import TinylambOpt.ResearchGoals.ExecutionStrategies.IndexPreimages

/-!
# Proofs for `IndexPreimages` research goals

Strategy per goal:

* `TruncBucket`: reduce `x.tdiv d = q` to the sign-split quotient/remainder
  characterizations `Int.tdiv_tmod_unique` / `Int.tdiv_tmod_unique'`, then match
  the three `truncBounds` branches (`q < 0`, `q = 0`, `q > 0`) against the sign
  of `x`.
* `DescendingBand`: rewrite both sides into linear inequalities in the atom
  `d * x` using `Int.le_ediv_iff_mul_le`, `Int.ediv_lt_iff_lt_mul` and
  `Int.add_one_le_iff`, then finish with `omega`.
* `DistanceRing`: the negative and positive seeks are disjoint, so their
  concatenation is a permutation of the filter by the disjunction of the two
  range predicates (`filter_or_perm`); that disjunction is pointwise equal to
  the `|x - o| ∈ [l, h]` predicate (`ring_union`), again by `omega` after
  splitting on the sign of `x - o` for `Int.natAbs`.
-/

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.IndexPreimages

open TinylambOpt.ResearchGoals.IndexPreimages

/-! ### TruncBucket -/

private theorem tdiv_eq_iff_nonneg {x d q : Int} (hd : 0 < d) (hx : 0 ≤ x) :
    x.tdiv d = q ↔ 0 ≤ q ∧ q * d ≤ x ∧ x ≤ q * d + d - 1 := by
  have hb : d ≠ 0 := Int.ne_of_gt hd
  have hna : (d.natAbs : Int) = d := Int.natAbs_of_nonneg (Int.le_of_lt hd)
  constructor
  · intro h
    have h' := (Int.tdiv_tmod_unique hx hb).mp ⟨h, rfl⟩
    obtain ⟨hrx, hr0, hrd⟩ := h'
    rw [hna] at hrd
    rw [Int.mul_comm d q] at hrx
    refine ⟨?_, ?_, ?_⟩
    · by_cases hq : 0 ≤ q
      · exact hq
      · have hdq : q * d ≤ -d := by
          have h2 := Int.mul_le_mul_of_nonneg_right (show q ≤ -1 by omega) (show 0 ≤ d by omega)
          rwa [Int.neg_one_mul] at h2
        have hq' : q < 0 := Int.not_le.mp hq
        omega
    · omega
    · omega
  · rintro ⟨hq0, h1, h2⟩
    have hr : x - q * d + d * q = x := by rw [Int.mul_comm d q]; omega
    have h0 : 0 ≤ x - q * d := by omega
    have hlt : x - q * d < (d.natAbs : Int) := by rw [hna]; omega
    exact ((Int.tdiv_tmod_unique hx hb).mpr ⟨hr, h0, hlt⟩).1

private theorem tdiv_eq_iff_nonpos {x d q : Int} (hd : 0 < d) (hx : x ≤ 0) :
    x.tdiv d = q ↔ q ≤ 0 ∧ q * d - d + 1 ≤ x ∧ x ≤ q * d := by
  have hb : d ≠ 0 := Int.ne_of_gt hd
  have hna : (d.natAbs : Int) = d := Int.natAbs_of_nonneg (Int.le_of_lt hd)
  constructor
  · intro h
    have h' := (Int.tdiv_tmod_unique' hx hb).mp ⟨h, rfl⟩
    obtain ⟨hrx, hr0, hrd⟩ := h'
    rw [hna] at hr0
    rw [Int.mul_comm d q] at hrx
    refine ⟨?_, ?_, ?_⟩
    · by_cases hq : q ≤ 0
      · exact hq
      · have hdq : d ≤ q * d := by
          have h2 := Int.mul_le_mul_of_nonneg_right (show 1 ≤ q by omega) (show 0 ≤ d by omega)
          rwa [Int.one_mul] at h2
        have hq' : 0 < q := Int.not_le.mp hq
        omega
    · omega
    · omega
  · rintro ⟨hq0, h1, h2⟩
    have hr : x - q * d + d * q = x := by rw [Int.mul_comm d q]; omega
    have h0 : -(d.natAbs : Int) < x - q * d := by rw [hna]; omega
    have h1' : x - q * d ≤ 0 := by omega
    exact ((Int.tdiv_tmod_unique' hx hb).mpr ⟨hr, h0, h1'⟩).1

/-- `x.tdiv d = q` iff `x` lies in the interval picked by `truncBounds d q`.
The interval depends on the sign of `q`; the proof splits on the sign of `x`
(nonnegative dividends use `tdiv_tmod_unique`, nonpositive ones use
`tdiv_tmod_unique'`). -/
theorem sound_TruncBucket : TruncBucket.Goal := by
  intro x d q hd
  simp only [truncBounds]
  by_cases hx : 0 ≤ x
  · have hA := tdiv_eq_iff_nonneg (x := x) (d := d) (q := q) hd hx
    by_cases hq : q < 0
    · rw [ite_eq_left hq]
      dsimp only
      rw [hA]
      have hdq : q * d ≤ -d := by
        have h2 := Int.mul_le_mul_of_nonneg_right (show q ≤ -1 by omega) (show 0 ≤ d by omega)
        rwa [Int.neg_one_mul] at h2
      apply iff_of_false
      · rintro ⟨h1, -, h3⟩; omega
      · rintro ⟨-, h2⟩; omega
    · by_cases hq0 : q = 0
      · have hq0' : (q == 0) = true := beq_iff_eq.mpr hq0
        rw [ite_eq_right hq, ite_eq_left hq0']
        dsimp only
        subst hq0
        simp only [Int.zero_mul, Int.zero_add] at hA
        rw [hA]
        constructor
        · rintro ⟨-, h1, h2⟩; exact ⟨by omega, h2⟩
        · rintro ⟨h1, h2⟩; exact ⟨by omega, by omega, h2⟩
      · have hq0' : ¬ ((q == 0) = true) := fun hh => hq0 (beq_iff_eq.mp hh)
        rw [ite_eq_right hq, ite_eq_right hq0']
        dsimp only
        rw [hA]
        have hqpos : 0 < q := by omega
        constructor
        · rintro ⟨-, h1, h2⟩; exact ⟨h1, h2⟩
        · rintro ⟨h1, h2⟩; exact ⟨by omega, h1, h2⟩
  · have hxle : x ≤ 0 := Int.le_of_lt (Int.not_le.mp hx)
    have hB := tdiv_eq_iff_nonpos (x := x) (d := d) (q := q) hd hxle
    by_cases hq : q < 0
    · rw [ite_eq_left hq]
      dsimp only
      rw [hB]
      have hqle : q ≤ 0 := by omega
      constructor
      · rintro ⟨-, h1, h2⟩; exact ⟨h1, h2⟩
      · rintro ⟨h1, h2⟩; exact ⟨by omega, h1, h2⟩
    · by_cases hq0 : q = 0
      · have hq0' : (q == 0) = true := beq_iff_eq.mpr hq0
        rw [ite_eq_right hq, ite_eq_left hq0']
        dsimp only
        subst hq0
        simp only [Int.zero_mul] at hB
        rw [hB]
        constructor
        · rintro ⟨-, h1, h2⟩; exact ⟨by omega, by omega⟩
        · rintro ⟨h1, h2⟩; exact ⟨by omega, by omega, by omega⟩
      · have hq0' : ¬ ((q == 0) = true) := fun hh => hq0 (beq_iff_eq.mp hh)
        rw [ite_eq_right hq, ite_eq_right hq0']
        dsimp only
        rw [hB]
        have hdq : d ≤ q * d := by
          have h2 := Int.mul_le_mul_of_nonneg_right (show 1 ≤ q by omega) (show 0 ≤ d by omega)
          rwa [Int.one_mul] at h2
        apply iff_of_false
        · rintro ⟨h1, -, -⟩; omega
        · rintro ⟨h1, -⟩; omega

/-! ### DescendingBand -/

/-- Inverting `lo ≤ bias - d*x < hi` for `0 < d`: both bounds become floor-division
inequalities in `x`, which `Int.le_ediv_iff_mul_le` and
`Int.ediv_lt_iff_lt_mul` turn back into linear facts about `d * x`. -/
theorem sound_DescendingBand : DescendingBand.Goal := by
  intro x lo hi bias d hd
  constructor
  · rintro ⟨h1, h2⟩
    have hx1 : x ≤ (bias - lo) / d := by
      rw [Int.le_ediv_iff_mul_le hd, Int.mul_comm x d]
      omega
    have hx2 : (bias - hi) / d + 1 ≤ x := by
      rw [Int.add_one_le_iff, Int.ediv_lt_iff_lt_mul hd, Int.mul_comm x d]
      omega
    exact ⟨hx2, hx1⟩
  · rintro ⟨h1, h2⟩
    have hB' : bias - hi < d * x := by
      have t := h1
      rw [Int.add_one_le_iff, Int.ediv_lt_iff_lt_mul hd, Int.mul_comm x d] at t
      exact t
    have hA' : d * x ≤ bias - lo := by
      have t := h2
      rw [Int.le_ediv_iff_mul_le hd, Int.mul_comm x d] at t
      exact t
    constructor <;> omega

/-! ### DistanceRing -/

/-- Concatenating the filters of two pointwise-disjoint Boolean predicates is a
permutation of the filter by their disjunction. Proved by induction on the list;
the only subtlety is that `||`/`&&` bind looser than `=`, so the `= true`
hypotheses need parentheses, and `filter_cons_of_pos/neg` need the combined
predicate passed explicitly to avoid higher-order unification picking
`Bool.or (p a)` as the predicate. -/
private theorem filter_or_perm {α : Type} (p q : α → Bool) (l : List α)
    (hdj : ∀ a ∈ l, ¬ (p a = true ∧ q a = true)) :
    (l.filter p ++ l.filter q).Perm (l.filter fun a => p a || q a) := by
  induction l with
  | nil => exact List.Perm.refl _
  | cons a l ih =>
    have hdj' : ∀ b ∈ l, ¬ (p b = true ∧ q b = true) :=
      fun b hb => hdj b (List.mem_cons_of_mem a hb)
    by_cases hpa : p a = true
    · by_cases hqa : q a = true
      · exact absurd ⟨hpa, hqa⟩ (hdj a List.mem_cons_self)
      · have hq : ¬ q a = true := hqa
        have h_or : (p a || q a) = true := by rw [hpa]; rfl
        rw [List.filter_cons_of_pos hpa, List.filter_cons_of_neg hq,
            List.filter_cons_of_pos (p := fun a => p a || q a) h_or]
        rw [List.cons_append]
        exact List.Perm.cons a (ih hdj')
    · have hp : ¬ p a = true := hpa
      by_cases hqa : q a = true
      · have hpf : p a = false := by
          have h := Bool.not_eq_true (p a)
          exact Eq.mp h hpa
        have h_or : (p a || q a) = true := by rw [hpf, hqa]; rfl
        rw [List.filter_cons_of_neg hp, List.filter_cons_of_pos hqa,
            List.filter_cons_of_pos (p := fun a => p a || q a) h_or]
        exact (List.perm_middle).trans (List.Perm.cons a (ih hdj'))
      · have hpf : p a = false := by
          have h := Bool.not_eq_true (p a)
          exact Eq.mp h hpa
        have hqf : q a = false := by
          have h := Bool.not_eq_true (q a)
          exact Eq.mp h hqa
        have h_or : ¬ ((p a || q a) = true) := by rw [hpf, hqf]; exact Bool.false_ne_true
        rw [List.filter_cons_of_neg hp, List.filter_cons_of_neg hqa,
            List.filter_cons_of_neg (p := fun a => p a || q a) h_or]
        exact ih hdj'

/-- Pointwise, the disjunction of the negative and positive annulus seeks equals
the absolute-distance predicate. Both sides are Boolean combinations of linear
inequalities; `Bool.eq_iff_iff` reduces the goal to an iff of `= true`
propositions, `Int.natAbs_of_nonneg` / `Int.ofNat_natAbs_of_nonpos` evaluate
`(x - o).natAbs` by the sign of `x - o`, and `omega` (which understands `max`)
discharges the resulting linear arithmetic. -/
private theorem ring_union {o l h x : Int} (hl : 0 ≤ l) :
    ((decide (o - h ≤ x) && decide (x ≤ o - max 1 l)) ||
     (decide (o + l ≤ x) && decide (x ≤ o + h))) =
    (decide (l ≤ ((x - o).natAbs : Int)) && decide (((x - o).natAbs : Int) ≤ h)) := by
  rw [Bool.eq_iff_iff]
  constructor
  · intro h
    rw [Bool.or_eq_true] at h
    rcases h with hp | hp
    · simp only [Bool.and_eq_true, decide_eq_true_eq] at hp
      obtain ⟨h1, h2⟩ := hp
      by_cases hxo : 0 ≤ x - o
      · have hna : ((x - o).natAbs : Int) = x - o := Int.natAbs_of_nonneg hxo
        rw [hna]
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        exact ⟨by omega, by omega⟩
      · have hna : ((x - o).natAbs : Int) = -(x - o) := by
          have hxo' : x - o ≤ 0 := by omega
          exact Int.ofNat_natAbs_of_nonpos hxo'
        rw [hna]
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        exact ⟨by omega, by omega⟩
    · simp only [Bool.and_eq_true, decide_eq_true_eq] at hp
      obtain ⟨h1, h2⟩ := hp
      by_cases hxo : 0 ≤ x - o
      · have hna : ((x - o).natAbs : Int) = x - o := Int.natAbs_of_nonneg hxo
        rw [hna]
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        exact ⟨by omega, by omega⟩
      · have hna : ((x - o).natAbs : Int) = -(x - o) := by
          have hxo' : x - o ≤ 0 := by omega
          exact Int.ofNat_natAbs_of_nonpos hxo'
        rw [hna]
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        exact ⟨by omega, by omega⟩
  · intro h
    simp only [Bool.and_eq_true, decide_eq_true_eq] at h
    obtain ⟨h1, h2⟩ := h
    rw [Bool.or_eq_true]
    by_cases hxo : 0 ≤ x - o
    · have hna : ((x - o).natAbs : Int) = x - o := Int.natAbs_of_nonneg hxo
      rw [hna] at h1 h2
      right
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact ⟨by omega, by omega⟩
    · have hna : ((x - o).natAbs : Int) = -(x - o) := by
        have hxo' : x - o ≤ 0 := by omega
        exact Int.ofNat_natAbs_of_nonpos hxo'
      rw [hna] at h1 h2
      left
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      exact ⟨by omega, by omega⟩

/-- The two seeks are disjoint: `x ≤ o - max 1 l ≤ o - 1` contradicts
`o + l ≤ x` since `0 ≤ l`. Hence `filter_or_perm` applies, and `ring_union`
identifies the combined predicate with the annulus predicate. -/
theorem sound_DistanceRing : DistanceRing.Goal := by
  intro rows o l h hl neg pos
  have hdisj : ∀ a ∈ rows,
      ¬ ((fun x => decide (o - h ≤ x) && decide (x ≤ o - max 1 l)) a = true ∧
         (fun x => decide (o + l ≤ x) && decide (x ≤ o + h)) a = true) := by
    intro a _
    rintro ⟨h1, h2⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at h1 h2
    obtain ⟨-, h1b⟩ := h1
    obtain ⟨h2a, -⟩ := h2
    omega
  have hperm := filter_or_perm
    (p := fun x => decide (o - h ≤ x) && decide (x ≤ o - max 1 l))
    (q := fun x => decide (o + l ≤ x) && decide (x ≤ o + h))
    (l := rows) hdisj
  refine hperm.trans ?_
  have heq : rows.filter (fun a =>
        (fun x => decide (o - h ≤ x) && decide (x ≤ o - max 1 l)) a ||
        (fun x => decide (o + l ≤ x) && decide (x ≤ o + h)) a) =
      rows.filter (fun x => decide (l ≤ ((x - o).natAbs : Int)) &&
        decide (((x - o).natAbs : Int) ≤ h)) := by
    apply List.filter_congr
    intro x _
    exact ring_union (x := x) hl
  rw [heq]

end TinylambOpt.ResearchProofs.ExecutionStrategies.IndexPreimages
