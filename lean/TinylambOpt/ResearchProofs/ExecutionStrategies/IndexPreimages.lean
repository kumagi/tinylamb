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
* `ModularLookup`: both sides are residues in `[0, m)`, hence equivalent to
  divisibility of `a*x+bias-target` and `x-inv*(target-bias)`; the validated
  inverse `a*inv ≡ 1` bridges them (`modlookup_dvd`, `emod_eq_iff_dvd_sub`).
* `CheckedDistanceBand`: the block hypotheses put every row inside the
  `checkedDistance` guard, so `mapM` reduces pointwise to
  `decide (center-rad ≤ x ∧ x ≤ center+rad)` via a `natAbs` sign split.
* `RoundedBucket`: `2n - 2kd = 2(n%d) + 2d(n/d - k)` shows only `k = n/d` and
  `k = n/d + 1` can be within `d`, matching the `nearestEven` tie-break.
* `CursorRanges`: `cmpKey cur r == .lt` is pointwise "peer or later"; the
  pairwise `≠ .gt` hypothesis gives disjointness and separation, and induction
  on `rows` yields `peers ++ later`.
* `PeriodicSeek`: the normalized residue enumeration
  `filter (· % p == r) (map Nat→Int (range k))` is proved by induction on `k`
  (splitting on `k % p = r`), then bridged to the `[lo, hi)` window via
  Euclidean division (`ps_enum_nat`, `ps_bridge`, `ps_rhs`).
* `MortonSquare`: `morton_split'` splits the interleaved-bit sum at depth `b`
  into low bits `< 4^b` plus `4^b *` high bits; `morton_inj` (induction on bit
  depth, peeling the lowest interleaved pair) shows the high Morton code
  determines `(x/2^b, y/2^b)` uniquely, turning the interval membership into
  the quotient-pair equality.
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

/-! ### ModularLookup -/

/-- Both sides are residues in `[0, m)`, so each equation is equivalent to a
divisibility statement; clearing `a * inv ≡ 1 (mod m)` turns one divisibility
into the other by explicit `Int.mul_*` rewrites and `omega`.
-/
private theorem emod_eq_iff_dvd_sub {u v m : Int} (hm : 0 < m)
    (hu : 0 ≤ u) (hu' : u < m) :
    u = v % m ↔ m ∣ (u - v) := by
  have hvm : 0 ≤ v % m := Int.emod_nonneg v (Int.ne_of_gt hm)
  have hv'm : v % m < m := Int.emod_lt_of_pos v hm
  constructor
  · intro h
    have h2 : u % m = (v % m) % m := congrArg (· % m) h
    have h3 : (u - v % m) % m = 0 :=
      (Int.emod_eq_emod_iff_emod_sub_eq_zero).mp h2
    have h4 : (u - v) % m = (u - v % m) % m := (Int.sub_emod_emod u v m).symm
    exact (Int.dvd_iff_emod_eq_zero).mpr (h4.trans h3)
  · intro h
    have h0 : (u - v) % m = 0 := (Int.dvd_iff_emod_eq_zero).mp h
    have h1 : (u - v % m) % m = 0 := (Int.sub_emod_emod u v m).trans h0
    have h2 : u % m = (v % m) % m :=
      (Int.emod_eq_emod_iff_emod_sub_eq_zero).mpr h1
    rw [Int.emod_eq_of_lt hu hu', Int.emod_eq_of_lt hvm hv'm] at h2
    exact h2

private theorem modlookup_dvd {m a inv bias x target : Int}
    (hm : 1 < m) (hinv : (a * inv) % m = 1) :
    m ∣ (a * x + bias - target) ↔ m ∣ (x - inv * (target - bias)) := by
  have hm0 : 0 < m := by omega
  have h1m : (1 : Int) % m = 1 := Int.emod_eq_of_lt (by omega) (by omega)
  have hdiff : (a * inv - 1) % m = 0 := by
    have hh : (a * inv) % m = (1 : Int) % m := by rw [hinv, h1m]
    exact (Int.emod_eq_emod_iff_emod_sub_eq_zero).mp hh
  have hAinv1 : m ∣ (a * inv - 1) := (Int.dvd_iff_emod_eq_zero).mpr hdiff
  constructor
  · intro hA
    have hinvA : m ∣ inv * (a * x + bias - target) := Int.dvd_mul_of_dvd_right hA
    have hId2 : inv * (a * x + bias - target) - (x - inv * (target - bias)) =
        (inv * a - 1) * x := by
      rw [Int.mul_sub, Int.mul_add, ← Int.mul_assoc, Int.mul_sub, Int.sub_mul, Int.one_mul]
      omega
    have hInvA1 : m ∣ (inv * a - 1) := by
      have he : inv * a = a * inv := Int.mul_comm _ _
      rw [he]; exact hAinv1
    have hmul : m ∣ (inv * a - 1) * x := Int.dvd_mul_of_dvd_left hInvA1
    have hdiff2 : m ∣ (inv * (a * x + bias - target) - (x - inv * (target - bias))) := by
      rw [hId2]; exact hmul
    have hZeq : x - inv * (target - bias) =
        inv * (a * x + bias - target) -
          (inv * (a * x + bias - target) - (x - inv * (target - bias))) := by omega
    rw [hZeq]
    exact Int.dvd_sub hinvA hdiff2
  · intro hZ
    have haZ : m ∣ a * (x - inv * (target - bias)) := Int.dvd_mul_of_dvd_right hZ
    have hId1 : a * (x - inv * (target - bias)) - (a * x + bias - target) =
        (target - bias) - a * inv * (target - bias) := by
      rw [Int.mul_sub, ← Int.mul_assoc]; omega
    have hfac : (target - bias) - a * inv * (target - bias) =
        (1 - a * inv) * (target - bias) := by
      rw [Int.sub_mul, Int.one_mul]
    have h1ma : m ∣ (1 - a * inv) := by
      have he : 1 - a * inv = -(a * inv - 1) := (Int.neg_sub _ _).symm
      rw [he]; exact Int.dvd_neg.mpr hAinv1
    have hmul2 : m ∣ (1 - a * inv) * (target - bias) :=
      Int.dvd_mul_of_dvd_left h1ma
    have hdiff1 : m ∣ (a * (x - inv * (target - bias)) - (a * x + bias - target)) := by
      rw [hId1, hfac]; exact hmul2
    have hAeq : a * x + bias - target =
        a * (x - inv * (target - bias)) -
          (a * (x - inv * (target - bias)) - (a * x + bias - target)) := by omega
    rw [hAeq]
    exact Int.dvd_sub haZ hdiff1

theorem sound_ModularLookup : ModularLookup.Goal := by
  intro m a inv bias x target hm hinv hx0 hxm ht0 htm
  have hm0 : 0 < m := by omega
  have htarget : target % m = target := Int.emod_eq_of_lt ht0 htm
  have hL : ((a * x + bias) % m = target) ↔ m ∣ (a * x + bias - target) := by
    constructor
    · intro h
      have hh : (a * x + bias) % m = target % m := by rw [h, htarget]
      exact (Int.dvd_iff_emod_eq_zero).mpr
        ((Int.emod_eq_emod_iff_emod_sub_eq_zero).mp hh)
    · intro h
      have h0 : (a * x + bias - target) % m = 0 := (Int.dvd_iff_emod_eq_zero).mp h
      have h1 : (a * x + bias) % m = target % m :=
        (Int.emod_eq_emod_iff_emod_sub_eq_zero).mpr h0
      rwa [htarget] at h1
  have hR : (x = (inv * (target - bias)) % m) ↔ m ∣ (x - inv * (target - bias)) :=
    emod_eq_iff_dvd_sub hm0 hx0 hxm
  rw [hL, hR]
  exact modlookup_dvd hm hinv

/-! ### CheckedDistanceBand -/

/-- Under the block bounds every row satisfies the `checkedDistance` guard, so
`mapM` never yields `none`; pointwise `|x - center| ≤ rad` is the interval
`center - rad ≤ x ≤ center + rad` after a `natAbs` sign split.
-/
private theorem checkedDistance_eq_some {m x center : Int}
    (h1 : -m ≤ x - center) (h2 : x - center < m) (h3 : x - center ≠ -m) :
    checkedDistance m x center = some ((x - center).natAbs : Int) := by
  have hcond : (decide (-m ≤ x - center) && decide (x - center < m) &&
      (x - center) != -m) = true := by
    simp only [Bool.and_eq_true, decide_eq_true_eq, bne_iff_ne]
    exact ⟨⟨h1, h2⟩, h3⟩
  unfold checkedDistance
  dsimp only
  rw [ite_eq_left hcond]

private theorem checkedDistance_decide {x center rad : Int} :
    decide (((x - center).natAbs : Int) ≤ rad) =
      decide (center - rad ≤ x ∧ x ≤ center + rad) := by
  rw [Bool.eq_iff_iff]
  simp only [decide_eq_true_eq]
  by_cases hxo : 0 ≤ x - center
  · have hna : ((x - center).natAbs : Int) = x - center := Int.natAbs_of_nonneg hxo
    rw [hna]
    constructor
    · intro h; exact ⟨by omega, by omega⟩
    · intro h; omega
  · have hna : ((x - center).natAbs : Int) = -(x - center) := by
      have hxo' : x - center ≤ 0 := by omega
      exact Int.ofNat_natAbs_of_nonpos hxo'
    rw [hna]
    constructor
    · intro h; exact ⟨by omega, by omega⟩
    · intro h; omega

private theorem mapM_checkedDistance {m center rad : Int} :
    ∀ (rows : List Int),
      (∀ x ∈ rows, -m ≤ x - center ∧ x - center < m ∧ x - center ≠ -m) →
      rows.mapM (fun x => (checkedDistance m x center).map (fun n => decide (n ≤ rad))) =
        some (rows.map fun x => decide (center - rad ≤ x ∧ x ≤ center + rad)) := by
  intro rows
  induction rows with
  | nil => intro _; rfl
  | cons x xs ih =>
    intro h
    have hx := h x List.mem_cons_self
    have hxs : ∀ y ∈ xs, -m ≤ y - center ∧ y - center < m ∧ y - center ≠ -m :=
      fun y hy => h y (List.mem_cons_of_mem x hy)
    obtain ⟨h1, h2, h3⟩ := hx
    have hcd : checkedDistance m x center = some ((x - center).natAbs : Int) :=
      checkedDistance_eq_some h1 h2 h3
    rw [List.mapM_cons, hcd, Option.map_some, ih hxs]
    simp
    rw [← Bool.decide_and]
    exact checkedDistance_decide

theorem sound_CheckedDistanceBand : CheckedDistanceBand.Goal := by
  intro rows m l h center rad hrow hl hh
  have hguard : ∀ x ∈ rows, -m ≤ x - center ∧ x - center < m ∧ x - center ≠ -m := by
    intro x hx
    obtain ⟨hxl, hxh⟩ := hrow x hx
    exact ⟨by omega, by omega, by omega⟩
  exact mapM_checkedDistance rows hguard

/-! ### RoundedBucket -/

/-- Write `2n - 2kd = 2(n%d) + 2d(n/d - k)`: the nearest bucket `k = n/d` or
`k = n/d + 1` gives distance `< d` or `= d`, every other `k` is too far, and
the exact-half tie is decided by `k % 2 = 0` exactly as `nearestEven` does.
-/
private theorem rb_dist_eq {n d k : Int} :
    2 * n - 2 * k * d = 2 * (n % d) + 2 * (d * (n / d) - d * k) := by
  generalize hA : d * (n / d) = A
  generalize hB : d * k = B
  have hdec : n = A + n % d := by
    have h := Int.mul_ediv_add_emod n d
    rw [hA] at h
    exact h.symm
  have hTB : 2 * k * d = 2 * B := by rw [Int.mul_assoc, Int.mul_comm k d, hB]
  rw [hTB]
  omega

private theorem rb_far {n d k : Int} (hd : 0 < d)
    (hR0 : 0 ≤ n % d) (hRd : n % d < d)
    (hk : k ≤ n / d - 1 ∨ k ≥ n / d + 2)
    (hP : (↑(2 * n - 2 * k * d).natAbs < d) ∨
          (↑(2 * n - 2 * k * d).natAbs = d ∧ k % 2 = 0)) : False := by
  have hEk := rb_dist_eq (n := n) (d := d) (k := k)
  rcases hk with hk | hk
  · have hBk : d * k ≤ d * (n / d) - d := by
      have h1 : k * d ≤ (n / d - 1) * d :=
        Int.mul_le_mul_of_nonneg_right hk (by omega)
      have h2 : (n / d - 1) * d = d * (n / d) - d := by
        rw [Int.mul_comm (n / d - 1) d, Int.mul_sub, Int.mul_one]
      have h3 : d * k = k * d := Int.mul_comm _ _
      omega
    have hsign : 0 ≤ 2 * n - 2 * k * d := by omega
    rw [Int.natAbs_of_nonneg hsign] at hP
    omega
  · have hBk : d * (n / d) + 2 * d ≤ d * k := by
      have h1 : (n / d + 2) * d ≤ k * d :=
        Int.mul_le_mul_of_nonneg_right (by omega) (by omega)
      have h2 : (n / d + 2) * d = d * (n / d) + 2 * d := by
        rw [Int.mul_comm (n / d + 2) d, Int.mul_add, Int.mul_comm d 2]
      have h3 : d * k = k * d := Int.mul_comm _ _
      omega
    have hsign : 2 * n - 2 * k * d ≤ 0 := by omega
    rw [Int.ofNat_natAbs_of_nonpos hsign] at hP
    omega

theorem sound_RoundedBucket : RoundedBucket.Goal := by
  intro n d k hd
  unfold nearestEven
  dsimp only
  have hR0 : 0 ≤ n % d := Int.emod_nonneg n (Int.ne_of_gt hd)
  have hRd : n % d < d := Int.emod_lt_of_pos n hd
  split
  · rename_i hC
    simp only [Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at hC
    constructor
    · intro h
      subst h
      have hEk := rb_dist_eq (n := n) (d := d) (k := n / d)
      have hX : 2 * n - 2 * (n / d) * d = 2 * (n % d) := by omega
      have hsign : 0 ≤ 2 * n - 2 * (n / d) * d := by omega
      have hna : ((2 * n - 2 * (n / d) * d).natAbs : Int) = 2 * (n % d) :=
        (Int.natAbs_of_nonneg hsign).trans hX
      rw [hna]
      exact hC
    · intro hP
      by_cases hkq : k = n / d
      · subst k; rfl
      · by_cases hkq1 : k = n / d + 1
        · subst k
          exfalso
          have hEk := rb_dist_eq (n := n) (d := d) (k := n / d + 1)
          have hB1 : d * (n / d + 1) = d * (n / d) + d := by
            rw [Int.mul_add, Int.mul_one]
          have hsign : 2 * n - 2 * (n / d + 1) * d ≤ 0 := by omega
          rw [Int.ofNat_natAbs_of_nonpos hsign] at hP
          omega
        · have hkle : k ≤ n / d - 1 ∨ k ≥ n / d + 2 := by omega
          exact (rb_far hd hR0 hRd hkle hP).elim
  · rename_i hC
    simp only [Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at hC
    constructor
    · intro h
      subst h
      have hEk := rb_dist_eq (n := n) (d := d) (k := n / d + 1)
      have hB1 : d * (n / d + 1) = d * (n / d) + d := by
        rw [Int.mul_add, Int.mul_one]
      have hsign : 2 * n - 2 * (n / d + 1) * d ≤ 0 := by omega
      have hX : -(2 * n - 2 * (n / d + 1) * d) = 2 * d - 2 * (n % d) := by omega
      have hna : ((2 * n - 2 * (n / d + 1) * d).natAbs : Int) = 2 * d - 2 * (n % d) := by
        have h1 := Int.ofNat_natAbs_of_nonpos hsign
        exact h1.trans hX
      rw [hna]
      omega
    · intro hP
      by_cases hkq1 : k = n / d + 1
      · subst k; rfl
      · by_cases hkq : k = n / d
        · subst k
          exfalso
          have hEk := rb_dist_eq (n := n) (d := d) (k := n / d)
          have hsign : 0 ≤ 2 * n - 2 * (n / d) * d := by omega
          rw [Int.natAbs_of_nonneg hsign] at hP
          omega
        · have hkle : k ≤ n / d - 1 ∨ k ≥ n / d + 2 := by omega
          exact (rb_far hd hR0 hRd hkle hP).elim

/-! ### CursorRanges -/

/-- `cmpKey cur r == .lt` unfolds (via `cursor_key_lt`) to "same first key and
smaller second key, or a strictly later first key". Pairwise `≠ .gt` ordering
makes a peer and a later row incompatible (`cursor_disj`) and forces all rows
after a later row to be later too (`cursor_sep`), so a single list induction
rebuilds the filter as `peers ++ later`.
-/
private theorem cursor_key_lt (cur r : Key) :
    (cmpKey cur r == Ordering.lt) =
      ((r.1 == cur.1 && decide (r.2 < cur.2)) ||
       (match cur.1, r.1 with
        | some x, some y => decide (x < y)
        | some _, none => true
        | _, _ => false)) := by
  obtain ⟨c1, c2⟩ := cur
  obtain ⟨r1, r2⟩ := r
  unfold cmpKey
  dsimp only
  cases c1 <;> cases r1
  · simp [Lean.Grind.beq_eq_decide_eq, Int.compare_eq_lt]
  · simp
  · simp
  · rename_i cx ry
    by_cases h : cx = ry
    · subst h
      simp [Lean.Grind.beq_eq_decide_eq, Int.compare_eq_lt]
    · simp [h, Ne.symm h, Lean.Grind.beq_eq_decide_eq, Int.compare_eq_lt]

private theorem cursor_sep (cur a b : Key)
    (hla : (match cur.1, a.1 with
        | some x, some y => decide (x < y)
        | some _, none => true
        | _, _ => false) = true)
    (hpb : (b.1 == cur.1 && decide (b.2 < cur.2)) = true) :
    cmpKey a b = Ordering.gt := by
  obtain ⟨a1, a2⟩ := a
  obtain ⟨b1, b2⟩ := b
  obtain ⟨c1, c2⟩ := cur
  dsimp only at hla hpb ⊢
  rw [Bool.and_eq_true] at hpb
  obtain ⟨hbeq, hlt⟩ := hpb
  rw [beq_iff_eq] at hbeq
  subst b1
  cases c1 with
  | none =>
    cases a1 <;> simp at hla
  | some cx =>
    cases a1 with
    | none =>
      unfold cmpKey; dsimp only
      simp
    | some ay =>
      have hay : cx < ay := of_decide_eq_true hla
      have hayne : ay ≠ cx := by omega
      unfold cmpKey; dsimp only
      simp only [beq_iff_eq, Int.compare_eq_eq, hayne]
      exact Int.compare_eq_gt.mpr hay

private theorem cursor_disj (cur r : Key)
    (hpb : (r.1 == cur.1 && decide (r.2 < cur.2)) = true)
    (hla : (match cur.1, r.1 with
        | some x, some y => decide (x < y)
        | some _, none => true
        | _, _ => false) = true) : False := by
  obtain ⟨r1, r2⟩ := r
  obtain ⟨c1, c2⟩ := cur
  dsimp only at hpb hla ⊢
  rw [Bool.and_eq_true] at hpb
  obtain ⟨hbeq, hlt⟩ := hpb
  rw [beq_iff_eq] at hbeq
  subst r1
  cases c1 with
  | none => simp at hla
  | some cx =>
    have : cx < cx := of_decide_eq_true hla
    omega

private theorem cursor_aux (cur : Key) :
    ∀ rows : List Key, rows.Pairwise (fun a b => cmpKey a b ≠ Ordering.gt) →
      (rows.filter fun r => r.1 == cur.1 && decide (r.2 < cur.2)) ++
      (rows.filter fun r => match cur.1, r.1 with
          | some x, some y => decide (x < y)
          | some _, none => true
          | _, _ => false) =
      rows.filter (fun r => cmpKey cur r == Ordering.lt) := by
  intro rows
  induction rows with
  | nil => intro _; rfl
  | cons a rows ih =>
    intro hsort
    rw [List.pairwise_cons] at hsort
    obtain ⟨hhead, htail⟩ := hsort
    have hIH := ih htail
    by_cases hpa : (a.1 == cur.1 && decide (a.2 < cur.2)) = true
    · by_cases hla : (match cur.1, a.1 with
          | some x, some y => decide (x < y)
          | some _, none => true
          | _, _ => false) = true
      · exact (cursor_disj cur a hpa hla).elim
      · have htarg : (cmpKey cur a == Ordering.lt) = true := by
          rw [cursor_key_lt]; simp [hpa, hla]
        simp [hpa, hla, htarg]
        exact hIH
    · by_cases hla : (match cur.1, a.1 with
          | some x, some y => decide (x < y)
          | some _, none => true
          | _, _ => false) = true
      · have hfp : rows.filter
            (fun r => r.1 == cur.1 && decide (r.2 < cur.2)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy hpy
          exact hhead y hy (cursor_sep cur a y hla hpy)
        rw [hfp] at hIH
        have htarg : (cmpKey cur a == Ordering.lt) = true := by
          rw [cursor_key_lt]; simp [hpa, hla]
        simp [hpa, hla, htarg, hfp]
        exact hIH
      · have htarg : (cmpKey cur a == Ordering.lt) = false := by
          rw [cursor_key_lt]; simp [hpa, hla]
        simp [hpa, hla, htarg]
        exact hIH

theorem sound_CursorRanges : CursorRanges.Goal := by
  intro rows cur later peers hsort
  exact cursor_aux cur rows hsort

private theorem ps_emod_zero_iff {k r p : Int} (hr0 : 0 ≤ r) (hrp : r < p) :
    (k - r) % p = 0 ↔ k % p = r := by
  have hrp' : r % p = r := Int.emod_eq_of_lt hr0 hrp
  constructor
  · intro h
    have h2 : k % p = r % p := (Int.emod_eq_emod_iff_emod_sub_eq_zero).mpr h
    rw [hrp'] at h2
    exact h2
  · intro h
    have h2 : k % p = r % p := by rw [h, hrp']
    exact (Int.emod_eq_emod_iff_emod_sub_eq_zero).mp h2

private theorem ps_ediv_hit {k r p : Int} (hp : 0 < p)
    (h : k % p = r) :
    (k - 1 - r) / p = (k - r) / p - 1 ∧ (k - r) / p = k / p := by
  have hp0 : p ≠ 0 := Int.ne_of_gt hp
  have hdec : p * (k / p) + k % p = k := Int.mul_ediv_add_emod k p
  have hkr : k - r = p * (k / p) := by omega
  have hQ1 : (k - r) / p = k / p := Int.ediv_eq_of_eq_mul_right hp0 hkr
  have hQ0 : (k - 1 - r) / p = k / p - 1 := by
    have hdecomp : (p - 1) + p * (k / p - 1) = k - 1 - r := by
      have h1 : p * (k / p - 1) = p * (k / p) - p := by
        rw [Int.mul_sub, Int.mul_one]
      omega
    have hge : 0 ≤ p - 1 := by omega
    have hlt : p - 1 < p := by omega
    have hpair := (Int.ediv_emod_unique hp).mpr ⟨hdecomp, hge, hlt⟩
    exact hpair.1
  exact ⟨by omega, hQ1⟩

private theorem ps_ediv_miss {k r p : Int} (hp : 0 < p) (hr0 : 0 ≤ r) (hrp : r < p)
    (h : k % p ≠ r) :
    (k - 1 - r) / p = (k - r) / p := by
  have hp0 : p ≠ 0 := Int.ne_of_gt hp
  have hs0 : (k - r) % p ≠ 0 := by
    intro hs
    exact h ((ps_emod_zero_iff hr0 hrp).mp hs)
  have hsge : 0 ≤ (k - r) % p := Int.emod_nonneg _ hp0
  have hslt : (k - r) % p < p := Int.emod_lt_of_pos _ hp
  have hs1 : 1 ≤ (k - r) % p := by omega
  have hdec : p * ((k - r) / p) + (k - r) % p = k - r := Int.mul_ediv_add_emod (k - r) p
  have hdecomp : ((k - r) % p - 1) + p * ((k - r) / p) = k - 1 - r := by omega
  have hge : 0 ≤ (k - r) % p - 1 := by omega
  have hlt : (k - r) % p - 1 < p := by omega
  have hpair := (Int.ediv_emod_unique hp).mpr ⟨hdecomp, hge, hlt⟩
  exact hpair.1

private theorem ps_enum_nat (k : Nat) (r p : Int) (hp : 0 < p)
    (hr0 : 0 ≤ r) (hrp : r < p) :
    ((List.range k).map (fun (i : Nat) => (i : Int))).filter (fun i => i % p == r) =
      (List.range ((((k : Int) - 1 - r) / p + 1).toNat)).map
        (fun (j : Nat) => r + (j : Int) * p) := by
  induction k with
  | zero =>
    have hJ : ((((0 : Nat) : Int) - 1 - r) / p + 1).toNat = 0 := by
      have hneg : ((0 : Nat) : Int) - 1 - r < 0 := by omega
      have hq : (((0 : Nat) : Int) - 1 - r) / p < 0 := Int.ediv_neg_of_neg_of_pos hneg hp
      have hle : (((0 : Nat) : Int) - 1 - r) / p + 1 ≤ 0 := by omega
      exact Int.toNat_of_nonpos hle
    rw [hJ]
    simp
  | succ k ih =>
    have hk0 : 0 ≤ (k : Int) := Int.natCast_nonneg k
    have hp0 : 0 ≤ p := by omega
    by_cases hhit : (k : Int) % p = r
    · -- ===== hit =====
      have hdec := ps_ediv_hit hp hhit
      have hJ : ((((k + 1 : Nat) : Int) - 1 - r) / p + 1).toNat =
          ((((k : Nat) : Int) - 1 - r) / p + 1).toNat + 1 := by
        rw [Int.natCast_add_one]
        have harg : (k : Int) + 1 - 1 - r = (k : Int) - r := by omega
        rw [harg, hdec.1]
        have hsub : ((k : Int) - r) / p - 1 + 1 = ((k : Int) - r) / p := by omega
        rw [hsub, hdec.2]
        have hq0 : 0 ≤ (k : Int) / p := Int.ediv_nonneg hk0 hp0
        rw [Int.toNat_add hq0 (by omega : (0 : Int) ≤ 1), Int.toNat_one]
      have hJk : ((((k : Nat) : Int) - 1 - r) / p + 1).toNat = ((k : Int) / p).toNat := by
        rw [hdec.1]
        have hsub : ((k : Int) - r) / p - 1 + 1 = ((k : Int) - r) / p := by omega
        rw [hsub, hdec.2]
      have hgJ : r + ((((k : Nat) : Int) - 1 - r) / p + 1).toNat * p = (k : Int) := by
        rw [hJk]
        have hq0 : 0 ≤ (k : Int) / p := Int.ediv_nonneg hk0 hp0
        rw [Int.toNat_of_nonneg hq0]
        have h2 := Int.mul_ediv_add_emod (k : Int) p
        rw [hhit] at h2
        have h3 : ((k : Int) / p) * p = p * ((k : Int) / p) := Int.mul_comm _ _
        rw [h3]
        omega
      rw [hJ]
      simp only [Nat.add_one, List.range_succ, List.map_append, List.filter_append,
        List.map_cons, List.map_nil, List.filter_cons, List.filter_nil]
      rw [ih]
      rw [ite_eq_left (beq_iff_eq.mpr hhit)]
      rw [hgJ]
    · -- ===== miss =====
      have hdec := ps_ediv_miss hp hr0 hrp hhit
      have hJ : ((((k + 1 : Nat) : Int) - 1 - r) / p + 1).toNat =
          ((((k : Nat) : Int) - 1 - r) / p + 1).toNat := by
        rw [Int.natCast_add_one]
        have harg : (k : Int) + 1 - 1 - r = (k : Int) - r := by omega
        rw [harg, hdec]
      rw [hJ]
      simp only [Nat.add_one, List.range_succ, List.map_append, List.filter_append,
        List.map_cons, List.map_nil, List.filter_cons, List.filter_nil]
      rw [ite_eq_right (mt beq_iff_eq.mp hhit)]
      simp only [List.append_nil]
      exact ih

private theorem ps_bridge {lo i phase r p : Int} (hp : 0 < p)
    (hphase0 : 0 ≤ phase) (hphasep : phase < p)
    (hr : r = (phase - lo) % p) :
    (lo + i) % p = phase ↔ i % p = r := by
  have hph : phase % p = phase := Int.emod_eq_of_lt hphase0 hphasep
  have hr0 : 0 ≤ r := by rw [hr]; exact Int.emod_nonneg _ (Int.ne_of_gt hp)
  have hrp : r < p := by rw [hr]; exact Int.emod_lt_of_pos _ hp
  have hrr : r % p = r := Int.emod_eq_of_lt hr0 hrp
  have hdiff : p ∣ ((lo + i) - phase - (i - r)) := by
    have harg : (lo + i) - phase - (i - r) = lo + r - phase := by omega
    rw [harg]
    have h1 : ((phase - lo) - r) % p = 0 := by
      have h2 : (phase - lo) % p = r % p := hr.symm.trans hrr.symm
      exact (Int.emod_eq_emod_iff_emod_sub_eq_zero).mp h2
    have h3 : p ∣ (phase - lo) - r := (Int.dvd_iff_emod_eq_zero).mpr h1
    have h4 : lo + r - phase = -((phase - lo) - r) := by omega
    rw [h4]
    exact Int.dvd_neg.mpr h3
  constructor
  · intro h
    have hdvd : p ∣ (lo + i) - phase := by
      have h2 : (lo + i) % p = phase % p := by rw [h, hph]
      exact (Int.dvd_iff_emod_eq_zero).mpr ((Int.emod_eq_emod_iff_emod_sub_eq_zero).mp h2)
    have h3 : p ∣ ((lo + i) - phase) - ((lo + i) - phase - (i - r)) := Int.dvd_sub hdvd hdiff
    have heq : (lo + i) - phase - ((lo + i) - phase - (i - r)) = i - r := by omega
    rw [heq] at h3
    have h4 : (i - r) % p = 0 := (Int.dvd_iff_emod_eq_zero).mp h3
    have h5 : i % p = r % p := (Int.emod_eq_emod_iff_emod_sub_eq_zero).mpr h4
    rw [hrr] at h5
    exact h5
  · intro h
    have hdvd : p ∣ i - r := by
      have h2 : i % p = r % p := by rw [h, hrr]
      exact (Int.dvd_iff_emod_eq_zero).mpr ((Int.emod_eq_emod_iff_emod_sub_eq_zero).mp h2)
    have h3 : p ∣ (i - r) + ((lo + i) - phase - (i - r)) := Int.dvd_add hdvd hdiff
    have heq : (i - r) + ((lo + i) - phase - (i - r)) = (lo + i) - phase := by omega
    rw [heq] at h3
    have h4 : ((lo + i) - phase) % p = 0 := (Int.dvd_iff_emod_eq_zero).mp h3
    have h5 : (lo + i) % p = phase % p := (Int.emod_eq_emod_iff_emod_sub_eq_zero).mpr h4
    rw [hph] at h5
    exact h5

private theorem ps_map_cast (l : List Nat) (c : Int) :
    List.map (fun (i : Nat) => c + i) l =
      List.map (fun (j : Int) => c + j)
        (List.map (fun (i : Nat) => (i : Int)) l) := by
  induction l with
  | nil => rfl
  | cons a s ih =>
    simp only [List.map_cons, ih]

private theorem ps_filter_cast (T : Nat) (r p : Int) :
    List.map (fun (i : Nat) => (i : Int))
        (List.filter (fun (i : Nat) => (i : Int) % p == r) (List.range T)) =
      List.filter (fun (j : Int) => j % p == r)
        (List.map (fun (i : Nat) => (i : Int)) (List.range T)) := by
  rw [List.filter_map]
  congr 1

private theorem ps_rhs (lo hi period phase r : Int) (hp : 0 < period)
    (hphase0 : 0 ≤ phase) (hphasep : phase < period)
    (hr : r = (phase - lo) % period) :
    List.filter (fun x => x % period == phase)
        (List.map (fun (i : Nat) => lo + i) (List.range (hi - lo).toNat)) =
      List.map (fun (j : Nat) => lo + r + (j : Int) * period)
        (List.range ((((hi - 1 - (lo + r)) / period + 1).toNat))) := by
  have hr0 : 0 ≤ r := by rw [hr]; exact Int.emod_nonneg _ (Int.ne_of_gt hp)
  have hrp : r < period := by rw [hr]; exact Int.emod_lt_of_pos _ hp
  have hbridge : ∀ i : Nat,
      ((lo + (i : Int)) % period == phase) = ((i : Int) % period == r) := by
    intro i
    rw [Lean.Grind.beq_eq_decide_eq, Lean.Grind.beq_eq_decide_eq]
    exact decide_eq_decide.mpr (ps_bridge hp hphase0 hphasep hr)
  rw [List.filter_map]
  have hfil : List.filter
        ((fun x => x % period == phase) ∘ (fun (i : Nat) => lo + i))
        (List.range (hi - lo).toNat) =
      List.filter (fun (i : Nat) => (i : Int) % period == r)
        (List.range (hi - lo).toNat) := by
    apply List.filter_congr
    intro i _
    exact hbridge i
  rw [hfil]
  -- goal: map (fun i => lo+i) (filter Q (range T)) = map AP-target (range n')
  -- Step A: ps_map_cast turns LHS into map (fun j => lo+j) (map (·:Int) (filter Q (range T)))
  have hA := ps_map_cast
    (List.filter (fun (i : Nat) => (i : Int) % period == r) (List.range (hi - lo).toNat)) lo
  -- hA : map (fun i => lo+i) l' = map (fun j:Int => lo+j) (map (·:Int) l')
  rw [hA]
  -- Step B: swap inner map/filter via ps_filter_cast
  have hB := ps_filter_cast (hi - lo).toNat r period
  -- hB : map (·:Int) (filter Q (range T)) = filter R (map (·:Int) (range T))
  rw [hB]
  -- goal: map (fun j:Int => lo+j) (filter R (map (·:Int) (range T))) = map target (range n')
  -- Step C: ps_enum_nat on filter R (map (·:Int) (range T))
  have hC := ps_enum_nat (hi - lo).toNat r period hp hr0 hrp
  -- hC : filter (fun i:Int => i%p==r) (map (·:Int)(range T)) = map AP (range J)
  rw [hC]
  -- goal: map (fun j:Int => lo+j) (map AP (range J)) = map target (range n')
  rw [List.map_map]
  -- goal: map ((fun j:Int=>lo+j) ∘ AP) (range J) = map target (range n')
  -- relate J and n'
  have hJeq : ((((hi - lo).toNat : Int) - 1 - r) / period + 1).toNat =
      (((hi - 1 - (lo + r)) / period + 1).toNat) := by
    by_cases h : 0 ≤ hi - lo
    · have hT : ((hi - lo).toNat : Int) = hi - lo := Int.toNat_of_nonneg h
      have harg : ((hi - lo).toNat : Int) - 1 - r = hi - 1 - (lo + r) := by
        rw [hT]; omega
      rw [harg]
    · have hT0 : (hi - lo).toNat = 0 := Int.toNat_of_nonpos (by omega)
      have harg1 : ((hi - lo).toNat : Int) - 1 - r < 0 := by
        rw [hT0]; simp; omega
      have hq1 : (((hi - lo).toNat : Int) - 1 - r) / period < 0 :=
        Int.ediv_neg_of_neg_of_pos harg1 hp
      have hle1 : (((hi - lo).toNat : Int) - 1 - r) / period + 1 ≤ 0 := by omega
      have harg2 : hi - 1 - (lo + r) < 0 := by omega
      have hq2 : (hi - 1 - (lo + r)) / period < 0 :=
        Int.ediv_neg_of_neg_of_pos harg2 hp
      have hle2 : (hi - 1 - (lo + r)) / period + 1 ≤ 0 := by omega
      rw [Int.toNat_of_nonpos hle1, Int.toNat_of_nonpos hle2]
  rw [hJeq]
  -- goal: map (comp) (range n') = map target (range n') — functions pointwise equal
  congr 1
  funext j
  simp only [Function.comp_apply]
  show lo + (r + (j : Int) * period) = lo + r + (j : Int) * period
  omega

theorem sound_PeriodicSeek : PeriodicSeek.Goal := by
  intro lo hi period phase hp hphase0 hphasep
  dsimp only
  generalize hr : (phase - lo) % period = r
  exact (ps_rhs lo hi period phase r hp hphase0 hphasep hr.symm).symm

private theorem list_sum_const_mul (l : List Nat) (c : Nat) (f : Nat → Nat) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => rfl
  | cons a s ih =>
    simp only [List.map_cons, List.sum_cons, ih]
    rw [Nat.mul_add]

private theorem list_sum_map_congr (l : List Nat) (f g : Nat → Nat) :
    (∀ i ∈ l, f i = g i) → (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => intro _; rfl
  | cons a s ih =>
    intro h
    simp only [List.map_cons, List.sum_cons]
    rw [h a (by simp), ih (fun i hi => h i (by simp [hi]))]

private theorem morton_split' (b e x y : Nat) :
    morton (b + e) x y = morton b x y + 4^b * morton e (x / 2^b) (y / 2^b) := by
  have hL : morton (b + e) x y =
      ((List.range (b + e)).map
        (fun i => ((x.testBit i).toNat + 2 * (y.testBit i).toNat) * 4 ^ i)).sum := rfl
  rw [hL, List.range_add, List.map_append, List.sum_append, List.map_map]
  have h1 : ((List.range b).map
      (fun i => ((x.testBit i).toNat + 2 * (y.testBit i).toNat) * 4 ^ i)).sum =
      morton b x y := rfl
  rw [h1]
  congr 1
  have hfun : ((fun i => ((x.testBit i).toNat + 2 * (y.testBit i).toNat) * 4 ^ i) ∘
        (fun x => b + x)) =
      (fun i => 4 ^ b *
        ((((x / 2 ^ b).testBit i).toNat + 2 * ((y / 2 ^ b).testBit i).toNat) * 4 ^ i)) := by
    funext i
    simp only [Function.comp_apply]
    show (((x.testBit (b + i)).toNat + 2 * (y.testBit (b + i)).toNat) * 4 ^ (b + i)) =
      4 ^ b * ((((x / 2 ^ b).testBit i).toNat + 2 * ((y / 2 ^ b).testBit i).toNat) * 4 ^ i)
    rw [Nat.add_comm b i, Nat.testBit_add, Nat.testBit_add, Nat.pow_add]
    ac_rfl
  rw [hfun, list_sum_const_mul]
  congr 1

private theorem morton_succ (d x y : Nat) :
    morton (d + 1) x y = morton d x y +
      ((x.testBit d).toNat + 2 * (y.testBit d).toNat) * 4 ^ d := by
  have hL : morton (d + 1) x y =
      ((List.range (d + 1)).map
        (fun i => ((x.testBit i).toNat + 2 * (y.testBit i).toNat) * 4 ^ i)).sum := rfl
  rw [hL, Nat.add_one, List.range_succ, List.map_append, List.sum_append]
  rw [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
  simp only [Nat.add_zero]
  have h1 : ((List.range d).map
      (fun i => ((x.testBit i).toNat + 2 * (y.testBit i).toNat) * 4 ^ i)).sum =
      morton d x y := rfl
  rw [h1]

private theorem morton_one (a c : Nat) :
    morton 1 a c = (a.testBit 0).toNat + 2 * (c.testBit 0).toNat := by
  have hL : morton 1 a c =
      ((List.range 1).map
        (fun i => ((a.testBit i).toNat + 2 * (c.testBit i).toNat) * 4 ^ i)).sum := rfl
  rw [hL, List.range_one]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.pow_zero,
    Nat.mul_one, Nat.add_zero]

private theorem morton_peel (e a c : Nat) :
    morton (e + 1) a c =
      (a.testBit 0).toNat + 2 * (c.testBit 0).toNat + 4 * morton e (a / 2) (c / 2) := by
  have h := morton_split' 1 e a c
  rw [Nat.add_comm 1 e, morton_one a c] at h
  simp only [Nat.pow_one] at h
  exact h

private theorem morton_bound (d x y : Nat) : morton d x y < 4 ^ d := by
  induction d with
  | zero =>
    have hL : morton 0 x y = 0 := by
      have h2 : morton 0 x y = ((List.range 0).map
          (fun i => ((x.testBit i).toNat + 2 * (y.testBit i).toNat) * 4 ^ i)).sum := rfl
      rw [h2, List.range_zero]
      simp
    rw [hL]
    simp
  | succ d ih =>
    rw [morton_succ]
    have ht : ((x.testBit d).toNat + 2 * (y.testBit d).toNat) * 4 ^ d ≤ 3 * 4 ^ d := by
      have h1 : (x.testBit d).toNat ≤ 1 := Bool.toNat_le _
      have h2 : (y.testBit d).toNat ≤ 1 := Bool.toNat_le _
      have h3 : (x.testBit d).toNat + 2 * (y.testBit d).toNat ≤ 3 := by omega
      exact Nat.mul_le_mul_right _ h3
    have h4 : 4 ^ (d + 1) = 4 ^ d * 4 := by
      rw [Nat.pow_add, Nat.pow_one]
    rw [h4]
    omega

private theorem nat_mod_two_testBit (a : Nat) : a % 2 = (a.testBit 0).toNat := by
  rw [Nat.testBit_zero]
  have hlt : a % 2 < 2 := Nat.mod_lt a (by omega)
  have hcases : a % 2 = 0 ∨ a % 2 = 1 := by omega
  rcases hcases with h0 | h1
  · rw [h0]; rfl
  · rw [h1]; rfl

private theorem morton_inj (d : Nat) :
    ∀ a c e f : Nat, a < 2 ^ d → c < 2 ^ d → e < 2 ^ d → f < 2 ^ d →
      morton d a c = morton d e f → a = e ∧ c = f := by
  induction d with
  | zero =>
    intro a c e f ha hc he hf h
    have ha0 : a = 0 := by omega
    have hc0 : c = 0 := by omega
    have he0 : e = 0 := by omega
    have hf0 : f = 0 := by omega
    exact ⟨ha0.trans he0.symm, hc0.trans hf0.symm⟩
  | succ d ih =>
    intro a c e f ha hc he hf h
    rw [morton_peel, morton_peel] at h
    have hmod : ((a.testBit 0).toNat + 2 * (c.testBit 0).toNat +
          4 * morton d (a / 2) (c / 2)) % 4 =
        ((e.testBit 0).toNat + 2 * (f.testBit 0).toNat +
          4 * morton d (e / 2) (f / 2)) % 4 := by rw [h]
    rw [Nat.add_mul_mod_self_left, Nat.add_mul_mod_self_left] at hmod
    have ht0ac : (a.testBit 0).toNat + 2 * (c.testBit 0).toNat < 4 := by
      have h1 := Bool.toNat_le (a.testBit 0)
      have h2 := Bool.toNat_le (c.testBit 0)
      omega
    have ht0ef : (e.testBit 0).toNat + 2 * (f.testBit 0).toNat < 4 := by
      have h1 := Bool.toNat_le (e.testBit 0)
      have h2 := Bool.toNat_le (f.testBit 0)
      omega
    rw [Nat.mod_eq_of_lt ht0ac, Nat.mod_eq_of_lt ht0ef] at hmod
    have ha01 : (a.testBit 0).toNat ≤ 1 := Bool.toNat_le _
    have hc01 : (c.testBit 0).toNat ≤ 1 := Bool.toNat_le _
    have he01 : (e.testBit 0).toNat ≤ 1 := Bool.toNat_le _
    have hf01 : (f.testBit 0).toNat ≤ 1 := Bool.toNat_le _
    have hbits : (a.testBit 0).toNat = (e.testBit 0).toNat ∧
        (c.testBit 0).toNat = (f.testBit 0).toNat := by omega
    have hbit0 : a.testBit 0 = e.testBit 0 := by
      cases ha2 : a.testBit 0 <;> cases he2 : e.testBit 0 <;> simp_all
    have hbit1 : c.testBit 0 = f.testBit 0 := by
      cases hc2 : c.testBit 0 <;> cases hf2 : f.testBit 0 <;> simp_all
    have h4 : 4 * morton d (a / 2) (c / 2) = 4 * morton d (e / 2) (f / 2) := by omega
    have hMac : morton d (a / 2) (c / 2) = morton d (e / 2) (f / 2) :=
      Nat.mul_left_cancel (by omega : 0 < 4) h4
    have hpow : 2 ^ (d + 1) = 2 ^ d * 2 := by rw [Nat.pow_add, Nat.pow_one]
    rw [hpow] at ha hc he hf
    have ha2 : a / 2 < 2 ^ d := (Nat.div_lt_iff_lt_mul (by omega : 0 < 2)).mpr ha
    have hc2 : c / 2 < 2 ^ d := (Nat.div_lt_iff_lt_mul (by omega : 0 < 2)).mpr hc
    have he2 : e / 2 < 2 ^ d := (Nat.div_lt_iff_lt_mul (by omega : 0 < 2)).mpr he
    have hf2 : f / 2 < 2 ^ d := (Nat.div_lt_iff_lt_mul (by omega : 0 < 2)).mpr hf
    obtain ⟨hae, hcf⟩ := ih (a / 2) (c / 2) (e / 2) (f / 2) ha2 hc2 he2 hf2 hMac
    have hamod : a % 2 = (a.testBit 0).toNat := nat_mod_two_testBit a
    have hemod : e % 2 = (e.testBit 0).toNat := nat_mod_two_testBit e
    have hcfmod : c % 2 = (c.testBit 0).toNat := nat_mod_two_testBit c
    have hfmod : f % 2 = (f.testBit 0).toNat := nat_mod_two_testBit f
    have hamodeq : a % 2 = e % 2 := by rw [hamod, hemod, hbit0]
    have hcfmodeq : c % 2 = f % 2 := by rw [hcfmod, hfmod, hbit1]
    have hae' : a = e := by
      have h1 := Nat.div_add_mod a 2
      have h2 := Nat.div_add_mod e 2
      omega
    have hcf' : c = f := by
      have h1 := Nat.div_add_mod c 2
      have h2 := Nat.div_add_mod f 2
      omega
    exact ⟨hae', hcf'⟩

theorem sound_MortonSquare : MortonSquare.Goal := by
  intro d b x y u v hb hx hy hu hv
  dsimp only
  have hsplit : morton d x y = morton b x y +
      4 ^ b * morton (d - b) (x / 2 ^ b) (y / 2 ^ b) := by
    have h := morton_split' b (d - b) x y
    rw [Nat.add_sub_of_le hb] at h
    exact h
  have hsplit2 : morton d x y = morton b x y +
      morton (d - b) (x / 2 ^ b) (y / 2 ^ b) * 4 ^ b := by
    rw [hsplit]
    congr 1
    exact Nat.mul_comm _ _
  have hLb : morton b x y < 4 ^ b := morton_bound b x y
  have hP2 : 0 < 2 ^ b := Nat.pow_pos (by omega : (0 : Nat) < 2)
  have hP4 : 0 < 4 ^ b := Nat.pow_pos (by omega : (0 : Nat) < 4)
  have hxp : x / 2 ^ b < 2 ^ (d - b) := by
    apply (Nat.div_lt_iff_lt_mul hP2).mpr
    have hpow : 2 ^ (d - b) * 2 ^ b = 2 ^ d := by
      rw [← Nat.pow_add, Nat.sub_add_cancel hb]
    rw [hpow]
    exact hx
  have hyp : y / 2 ^ b < 2 ^ (d - b) := by
    apply (Nat.div_lt_iff_lt_mul hP2).mpr
    have hpow : 2 ^ (d - b) * 2 ^ b = 2 ^ d := by
      rw [← Nat.pow_add, Nat.sub_add_cancel hb]
    rw [hpow]
    exact hy
  constructor
  · -- forward: high parts equal → code in interval
    intro hxy
    have hH : morton (d - b) (x / 2 ^ b) (y / 2 ^ b) = morton (d - b) u v := by
      rw [hxy.1, hxy.2]
    rw [hsplit2, hH]
    refine ⟨?_, ?_⟩ <;> omega
  · -- backward: code in interval → high parts equal
    intro hint
    have hmdiv : morton d x y / 4 ^ b = morton (d - b) (x / 2 ^ b) (y / 2 ^ b) := by
      rw [hsplit2, Nat.add_mul_div_right _ _ hP4, Nat.div_eq_of_lt hLb, Nat.zero_add]
    have hsdiv : morton d x y / 4 ^ b = morton (d - b) u v := by
      have h1 : morton (d - b) u v ≤ morton d x y / 4 ^ b :=
        (Nat.le_div_iff_mul_le hP4).mpr hint.1
      have h2 : morton d x y / 4 ^ b < morton (d - b) u v + 1 := by
        apply (Nat.div_lt_iff_lt_mul hP4).mpr
        have h3 : morton (d - b) u v * 4 ^ b + 4 ^ b =
            (morton (d - b) u v + 1) * 4 ^ b := by
          rw [Nat.add_mul, Nat.one_mul]
        omega
      have h4 : morton d x y / 4 ^ b ≤ morton (d - b) u v := by omega
      exact Nat.le_antisymm h4 h1
    have hH : morton (d - b) (x / 2 ^ b) (y / 2 ^ b) = morton (d - b) u v :=
      hmdiv.symm.trans hsdiv
    exact morton_inj (d - b) (x / 2 ^ b) (y / 2 ^ b) u v hxp hyp hu hv hH

end TinylambOpt.ResearchProofs.ExecutionStrategies.IndexPreimages
