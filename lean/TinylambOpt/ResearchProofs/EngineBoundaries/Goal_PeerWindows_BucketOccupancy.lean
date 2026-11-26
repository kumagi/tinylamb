import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Proved

- `BucketOccupancy` — Exact bucket occupancy also handles more buckets than rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_BucketOccupancy

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-- `1 + x ≤ k` is the same as `x < k` on naturals. -/
theorem one_add_le_iff (x k : Nat) : 1 + x ≤ k ↔ x < k := by
  constructor <;> intro h <;> omega

/-- The frontier plus the whole-bucket stretch re-aligns with `k * small + extra`. -/
theorem front_region_id (k small extra : Nat) (h : extra ≤ k) :
    extra * (small + 1) + (k - extra) * small = k * small + extra := by
  have h1 : extra * (small + 1) = extra * small + extra := by
    rw [Nat.mul_add, Nat.mul_one]
  have h2 : extra * small + (k - extra) * small = k * small := by
    rw [← Nat.add_mul, Nat.add_sub_of_le h]
  omega

/-- Front-region buckets: `1 + i / (small+1) ≤ k` exactly while `i < k * (small+1)`. -/
theorem front_le (i k small : Nat) :
    1 + i / (small + 1) ≤ k ↔ i < k * (small + 1) := by
  rw [one_add_le_iff, Nat.div_lt_iff_lt_mul (k := small + 1) (Nat.succ_pos small)]

/-- A tail-region ordinal never lands in a bucket at or below the big-bucket count. -/
theorem tail_gt (k small extra i : Nat) (h : k ≤ extra) :
    ¬ (1 + extra + (i - extra * (small + 1)) / small ≤ k) := by
  have hz : 0 ≤ (i - extra * (small + 1)) / small := Nat.zero_le _
  omega

/-- Tail-region buckets: with `extra ≤ k` and `0 < small`, a tail ordinal sits in a bucket
`≤ k` exactly while it is below `frontier + (k - extra) * small`. -/
theorem tail_le (i k small extra : Nat) (hfront : extra * (small + 1) ≤ i)
    (hs : 0 < small) (hek : extra ≤ k) :
    1 + extra + (i - extra * (small + 1)) / small ≤ k
      ↔ i < extra * (small + 1) + (k - extra) * small := by
  have hstep : 1 + extra + (i - extra * (small + 1)) / small ≤ k
      ↔ (i - extra * (small + 1)) / small < k - extra := by
    constructor <;> intro h <;> omega
  have hkey : (i - extra * (small + 1)) / small < k - extra
      ↔ i - extra * (small + 1) < (k - extra) * small :=
    Nat.div_lt_iff_lt_mul (k := small) (x := i - extra * (small + 1)) (y := k - extra) hs
  have hfinal : i < extra * (small + 1) + (k - extra) * small
      ↔ i - extra * (small + 1) < (k - extra) * small :=
    (Nat.sub_lt_iff_lt_add' hfront).symm
  rw [hstep, hkey, hfinal]

/-- The NTILE prefix bound: ordinal `i` lands in bucket `≤ k` exactly below the frontier
`k * (n / b) + min k (n % b)`. -/
theorem tile_le_iff (n b k i : Nat) (hi : i < n) (_hb : 0 < b) :
    tile n b i ≤ k ↔ i < k * (n / b) + min k (n % b) := by
  let small := n / b
  let extra := n % b
  show (tile n b i ≤ k ↔ i < k * small + min k extra)
  have hdec : b * small + extra = n := Nat.div_add_mod n b
  have ht : tile n b i = if i < extra * (small + 1) then 1 + i / (small + 1)
      else 1 + extra + (i - extra * (small + 1)) / small := rfl
  rcases Nat.eq_zero_or_pos small with hsmall | hs
  · -- `small = 0`: then `extra = n`, the frontier is `n`, and every bucket has size one
    have h2 := hdec
    rw [hsmall] at h2
    have hex : extra = n := by omega
    rw [ht, hsmall]
    simp only [Nat.zero_add, Nat.mul_one, Nat.div_one, Nat.mul_zero]
    rw [ite_eq_left (by omega : i < extra)]
    constructor <;> intro h <;> omega
  · rcases Nat.lt_or_ge k (extra + 1) with hkle | hkg
    · -- `k ≤ extra`: the cutoff lands inside the big-bucket front region
      have hkex : k ≤ extra := by omega
      have hle : k * small ≤ extra * small := Nat.mul_le_mul_right small hkex
      rw [ht]
      rw [Nat.min_eq_left hkex]
      by_cases hlt : i < extra * (small + 1)
      · rw [ite_eq_left hlt, front_le, Nat.mul_add, Nat.mul_one]
      · rw [ite_eq_right hlt]
        have hge : extra * small + extra = extra * (small + 1) := by
          rw [Nat.mul_add, Nat.mul_one]
        have hr : ¬ (i < k * small + k) := by
          have h1 : extra * (small + 1) ≤ i := by omega
          omega
        exact ⟨fun h2 => (tail_gt k small extra i hkex h2).elim, fun h2 => absurd h2 hr⟩
    · -- `extra < k`: every big bucket plus `k - extra` small ones lie below the cutoff
      have hexk : extra ≤ k := Nat.le_of_succ_le hkg
      have hlt : extra * small < k * small :=
        Nat.mul_lt_mul_of_pos_right (Nat.lt_of_succ_le hkg) hs
      have hge : extra * small + extra = extra * (small + 1) := by
        rw [Nat.mul_add, Nat.mul_one]
      have hfid : extra * (small + 1) + (k - extra) * small = k * small + extra :=
        front_region_id k small extra hexk
      rw [ht]
      rw [Nat.min_eq_right hexk]
      by_cases hfront : i < extra * (small + 1)
      · -- front region: every ordinal here is in a bucket `≤ k` and below the bound
        have hp : i / (small + 1) < extra + 1 := by
          refine Nat.div_lt_of_lt_mul ?_
          have h1 : i < (small + 1) * extra := by rw [Nat.mul_comm]; exact hfront
          have h2 : (small + 1) * extra ≤ (small + 1) * (extra + 1) :=
            Nat.mul_le_mul_left _ (Nat.le_succ extra)
          omega
        have hl : 1 + i / (small + 1) ≤ k := by omega
        have hr : i < k * small + extra := by omega
        rw [ite_eq_left hfront]
        exact ⟨fun _ => hr, fun _ => hl⟩
      · -- tail region
        rw [ite_eq_right hfront,
          tail_le i k small extra (by omega) hs hexk, hfid]

/-- Logical equivalence of decidable propositions transfers through `decide`. -/
theorem decide_congr {p q : Prop} [Decidable p] [Decidable q] (h : p ↔ q) :
    decide p = decide q := by
  by_cases hp : p
  · have hq : q := h.mp hp
    rw [decide_eq_true hp, decide_eq_true hq]
  · have hnq : ¬ q := fun hq => hp (h.mpr hq)
    rw [decide_eq_false hp, decide_eq_false hnq]

/-- A peer value is at most `k` iff it is at most `k-1` or equal to `k`. -/
theorem decide_le_pred_or_eq (a k : Nat) (hk : 0 < k) :
    (decide (a ≤ k - 1) || (a == k)) = decide (a ≤ k) := by
  rw [Bool.beq_eq_decide_eq]
  by_cases h1 : a ≤ k - 1
  · by_cases h2 : a = k
    · omega
    · rw [decide_eq_true h1, decide_eq_false h2, Bool.true_or, decide_eq_true (by omega)]
  · by_cases h2 : a = k
    · rw [decide_eq_false h1, decide_eq_true h2, Bool.false_or, decide_eq_true (by omega)]
    · rw [decide_eq_false h1, decide_eq_false h2, Bool.or_false, decide_eq_false (by omega)]

/-- Two pointwise-disjoint predicates distribute over `filter` additively. -/
theorem filter_or_of_disjoint {α : Type} (xs : List α) (p q : α → Bool)
    (h : ∀ x ∈ xs, ¬ (p x = true ∧ q x = true)) :
    (xs.filter fun x => p x || q x).length
      = (xs.filter p).length + (xs.filter q).length := by
  induction xs with
  | nil => rfl
  | cons y ys ih =>
    have ihh := ih (fun x hx => h x (List.mem_cons_of_mem y hx))
    rw [List.filter_cons, List.filter_cons, List.filter_cons]
    cases hp : p y with
    | true =>
      cases hq : q y with
      | true => exact absurd ⟨hp, hq⟩ (h y List.mem_cons_self)
      | false =>
        simp
        rw [ihh]
        omega
    | false =>
      cases hq : q y with
      | true =>
        simp
        rw [ihh]
        omega
      | false =>
        simp
        rw [ihh]

/-- Counting a range below a bound is a `min`. -/
theorem range_filter_lt_length (n m : Nat) :
    ((List.range n).filter fun i => decide (i < m)).length = min n m := by
  induction n with
  | zero => simp
  | succ t ih =>
    rw [List.range_succ, List.filter_append, List.length_append, ih]
    rcases Nat.lt_or_ge t m with htm | htm
    · have h1 : (([t] : List Nat).filter fun i => decide (i < m)).length = 1 := by
        rw [List.filter_cons_of_pos (p := fun i => decide (i < m)) (decide_eq_true htm),
          List.length_cons, List.filter_nil, List.length_nil]
      rw [h1]
      omega
    · have h1 : (([t] : List Nat).filter fun i => decide (i < m)).length = 0 := by
        rw [List.filter_cons_of_neg (p := fun i => decide (i < m)) (by simpa using htm),
          List.filter_nil, List.length_nil]
      rw [h1]
      omega

/-- The number of ordinals in bucket `k` or below is the NTILE prefix bound. -/
theorem count_le (n b k : Nat) (hb : 0 < b) (hkb : k ≤ b) :
    ((List.range n).filter fun i => decide (tile n b i ≤ k)).length
      = k * (n / b) + min k (n % b) := by
  have hfilter : (List.range n).filter (fun i => decide (tile n b i ≤ k))
      = (List.range n).filter (fun i => decide (i < k * (n / b) + min k (n % b))) := by
    refine List.filter_congr (fun i hi => ?_)
    exact decide_congr (tile_le_iff n b k i (List.mem_range.mp hi) hb)
  have hlen : ((List.range n).filter fun i => decide (i < k * (n / b) + min k (n % b))).length
      = min n (k * (n / b) + min k (n % b)) := range_filter_lt_length n _
  have hle : k * (n / b) + min k (n % b) ≤ n := by
    have h1 : n % b < b := Nat.mod_lt n hb
    have h2 : b * (n / b) + n % b = n := Nat.div_add_mod n b
    have h3 : k * (n / b) ≤ b * (n / b) := Nat.mul_le_mul_right _ hkb
    have h4 : min k (n % b) ≤ n % b := Nat.min_le_right _ _
    omega
  rw [hfilter, hlen, Nat.min_eq_right hle]

theorem bucket_size_arith (n b k q r : Nat) (hk : 0 < k) (hkb : k ≤ b)
    (hfront : ((List.range n).filter fun i => decide (tile n b i ≤ k)).length
      = k * q + min k r)
    (hprev : ((List.range n).filter fun i => decide (tile n b i ≤ k - 1)).length
      = (k - 1) * q + min (k - 1) r)
    (hsplit : ((List.range n).filter fun i => decide (tile n b i ≤ k)).length
      = ((List.range n).filter fun i => decide (tile n b i ≤ k - 1)).length
        + tileSize n b k) :
    tileSize n b k = q + (decide (k ≤ r)).toNat := by
  rw [hfront, hprev] at hsplit
  have hmul : (k - 1) * q + q = k * q := by
    cases k with
    | zero => exact absurd hk (by omega)
    | succ t =>
      show t * q + q = (t + 1) * q
      rw [Nat.succ_mul]
  rcases Nat.lt_or_ge k (r + 1) with hkle | hkg
  · -- `k ≤ r`: bucket `k` is one of the big buckets
    have h1 : min k r = k := Nat.min_eq_left (by omega)
    have h2 : min (k - 1) r = k - 1 := Nat.min_eq_left (by omega)
    have h3 : (decide (k ≤ r)).toNat = 1 := by
      cases h : decide (k ≤ r) with
      | true => rfl
      | false =>
        simp only [decide_eq_false_iff_not] at h
        omega
    rw [h1, h2] at hsplit
    rw [h3]
    rw [← hmul] at hsplit
    omega
  · -- `r < k`: bucket `k` is one of the small buckets
    have h1 : min k r = r := Nat.min_eq_right (by omega)
    have h2 : min (k - 1) r = r := Nat.min_eq_right (by omega)
    have h3 : (decide (k ≤ r)).toNat = 0 := by
      cases h : decide (k ≤ r) with
      | false => rfl
      | true =>
        have := of_decide_eq_true h
        omega
    rw [h1, h2] at hsplit
    rw [h3, Nat.add_zero]
    rw [← hmul] at hsplit
    omega

theorem sound : BucketOccupancy.Goal := by
  intro n b k hk hkb
  have hb : 0 < b := Nat.lt_of_lt_of_le hk hkb
  have hsplit : ((List.range n).filter fun i => decide (tile n b i ≤ k)).length
      = ((List.range n).filter fun i => decide (tile n b i ≤ k - 1)).length
        + tileSize n b k := by
    have h1 : (List.range n).filter (fun i => decide (tile n b i ≤ k))
        = (List.range n).filter (fun i => decide (tile n b i ≤ k - 1) || tile n b i == k) := by
      refine List.filter_congr (fun i hi => ?_)
      exact (decide_le_pred_or_eq (tile n b i) k hk).symm
    have hdisj : ∀ i ∈ List.range n,
        ¬ ((fun j => decide (tile n b j ≤ k - 1)) i = true
          ∧ (fun j => tile n b j == k) i = true) := by
      intro i _ ⟨h2, h3⟩
      have hle := of_decide_eq_true h2
      have heq : tile n b i = k := LawfulBEq.eq_of_beq h3
      omega
    rw [h1, filter_or_of_disjoint (List.range n) (fun i => decide (tile n b i ≤ k - 1))
      (fun i => tile n b i == k) hdisj]
    rfl
  exact bucket_size_arith n b k (n / b) (n % b) hk hkb
    (count_le n b k hb hkb) (count_le n b (k - 1) hb (by omega)) hsplit

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_BucketOccupancy
