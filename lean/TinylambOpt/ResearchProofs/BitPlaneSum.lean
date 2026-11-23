import TinylambOpt.ResearchGoals.BitPlaneSum

/-!
# Soundness of bit-plane SUM without decoding

`weighted` sums per-plane population counts against the weights
`2^0 .. 2^6` and `-2^7` (two's-complement sign plane), guarded by the
selection `&&&` validity mask; `scalar` decodes lane-by-lane and sums the
selected non-`none` values. `sound` proves both agree, including the
all-`none` (`none` output) edge case.

Structure:
* `pack_getLsbD`: bit `i` of `pack f` is `f i` (fold induction).
* `lane`: `v : BitVec 8` equals `Σ k < 8, bit k * wt k` (kernel `decide`).
* `foldsum`: induction over lane prefixes, swapping "sum of per-plane
  counts" with "sum of per-lane decoded values".
* `bridge` / `empty_iff`: relate the `filterMap`-based `scalar` list to the
  selected-index list and to `mask &&& pack isSome = 0`.
-/

namespace TinylambOpt.ResearchProofs.BitPlaneSum

open TinylambOpt.ResearchGoals.BitPlaneSum

/-- Plane weights: plane 7 counts negatively (sign plane). -/
private def wt (k : Nat) : Int := if k = 7 then -128 else (2:Int)^k

/-! ### `pack`: bit `i` of `pack f` is `f i` for `i < 64` -/

private def pstep (f : Nat → Bool) (w : W) (i : Nat) : W :=
  w ||| ((BitVec.ofNat 64 (f i).toNat) <<< i)

private theorem foldl_pstep (f : Nat → Bool) :
    ∀ (n : Nat) (acc : W) (i : Nat), i < 64 →
      ((List.range n).foldl (pstep f) acc).getLsbD i = (acc.getLsbD i || (decide (i < n) && f i)) := by
  intro n acc
  induction n with
  | zero =>
      intro i _
      simp
  | succ n ih =>
      intro j hj
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [pstep, BitVec.getLsbD_or, ih j hj]
      have hsh : ((BitVec.ofNat 64 (f n).toNat) <<< n).getLsbD j = (decide (j = n) && f n) := by
        rw [BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ofNat]
        cases h : f n with
        | false =>
            have h0 : (0 : Nat).testBit (j - n) = false :=
              Nat.testBit_lt_two_pow (Nat.two_pow_pos _)
            rw [Bool.toNat_false, h0]
            simp
        | true =>
            have h1 : (1 : Nat).testBit (j - n) = decide (j - n = 0) := by
              show (2^0 : Nat).testBit (j - n) = decide (j - n = 0)
              rw [Nat.testBit_two_pow]
              by_cases hz : j - n = 0
              · rw [hz]
              · rw [decide_eq_false hz, decide_eq_false (fun h2 => hz h2.symm)]
            rw [Bool.toNat_true, h1, decide_eq_true hj, Bool.true_and]
            rcases Nat.lt_trichotomy j n with hlt | heq | hgt
            · rw [decide_eq_true hlt,
                decide_eq_true (Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt)),
                decide_eq_false (by omega : ¬ (j = n))]
              simp
            · subst heq
              rw [decide_eq_false (Nat.lt_irrefl j), decide_eq_true (Nat.sub_self j),
                decide_eq_true rfl]
              simp
            · rw [decide_eq_false (by omega : ¬ (j < n)),
                decide_eq_false (by omega : ¬ (j - n = 0)),
                decide_eq_false (by omega : ¬ (j = n))]
              simp
      rw [hsh]
      have hA : decide (j < n+1) = (decide (j < n) || decide (j = n)) := by
        rcases Nat.lt_trichotomy j n with hlt | heq | hgt
        · rw [decide_eq_true (Nat.lt_succ_of_lt hlt), decide_eq_true hlt,
            decide_eq_false (by omega : ¬ (j = n))]
          simp
        · subst heq
          rw [decide_eq_true (Nat.lt_succ_self j), decide_eq_false (Nat.lt_irrefl j),
            decide_eq_true rfl]
          simp
        · rw [decide_eq_false (by omega : ¬ (j < n)), decide_eq_false (by omega : ¬ (j = n)),
            decide_eq_false (by omega : ¬ (j < n+1))]
          simp
      rw [hA]
      rcases Nat.lt_trichotomy j n with hlt | heq | hgt
      · rw [decide_eq_true hlt, decide_eq_false (by omega : ¬ (j = n))]
        simp
      · subst heq
        rw [decide_eq_false (Nat.lt_irrefl j), decide_eq_true rfl]
        simp
      · rw [decide_eq_false (by omega : ¬ (j < n)), decide_eq_false (by omega : ¬ (j = n))]
        simp

private theorem pack_getLsbD (f : Nat → Bool) (i : Nat) (hi : i < 64) :
    (pack f).getLsbD i = f i := by
  show ((List.range 64).foldl (pstep f) 0).getLsbD i = f i
  rw [foldl_pstep f 64 0 i hi]
  simp [hi]

/-! ### Bit decomposition of a lane value (`BitVec 8`, two's complement) -/

private theorem lane (v : BitVec 8) :
    ((List.range 8).map fun k => (if v.getLsbD k then wt k else 0)).sum = v.toInt := by
  revert v
  decide

private theorem sum_wt_zero : ((List.range 8).map fun k => (0 : Int) * wt k).sum = 0 := by
  decide

/-! ### Double-sum swap: per-plane counts vs. per-lane decoded values -/

private theorem map_add_sum (l : List Nat) (a b : Nat → Int) :
    (l.map fun k => a k + b k).sum = (l.map a).sum + (l.map b).sum := by
  induction l with
  | nil => rfl
  | cons k t ih => simp only [List.map_cons, List.sum_cons, ih]; omega

private theorem len_range_succ_filter (p : Nat → Bool) (n : Nat) :
    ((List.range (n+1)).filter p).length =
      ((List.range n).filter p).length + (if p n then 1 else 0) := by
  rw [List.range_succ, List.filter_append, List.length_append]
  cases h : p n with
  | true =>
      rw [List.filter_cons_of_pos h]
      simp [List.length_cons, List.length_nil, List.filter_nil]
  | false =>
      rw [List.filter_cons_of_neg (by simp [h])]
      simp [List.length_nil, List.filter_nil]

private theorem foldsum (sel : Nat → Bool) (val : Nat → BitVec 8) :
    ∀ n : Nat,
      (((List.range n).filter sel).map fun i => (val i).toInt).sum =
        ((List.range 8).map fun k =>
            ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length * wt k).sum := by
  intro n
  induction n with
  | zero =>
      rw [List.range_zero, List.filter_nil, List.map_nil, List.sum_nil]
      exact sum_wt_zero.symm
  | succ n ih =>
      by_cases hs : sel n
      · -- lane `n` is selected: it adds `wt k` to plane `k` iff bit `k` is set
        have hT : (((List.range (n+1)).filter sel).map fun i => (val i).toInt).sum =
            (((List.range n).filter sel).map fun i => (val i).toInt).sum + (val n).toInt := by
          rw [List.range_succ, List.filter_append, List.filter_cons_of_pos hs]
          simp [List.map_append, List.sum_append]
        have hstepf (k : Nat) :
            ((List.range (n+1)).filter (fun i => sel i && (val i).getLsbD k)).length =
              ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length +
                (if (val n).getLsbD k then 1 else 0) := by
          rw [len_range_succ_filter (fun i => sel i && (val i).getLsbD k) n]
          cases h : (val n).getLsbD k <;> simp [hs]
        have helem (k : Nat) :
            ((List.range (n+1)).filter (fun i => sel i && (val i).getLsbD k)).length * wt k =
              ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length * wt k +
                (if (val n).getLsbD k then wt k else 0) := by
          rw [hstepf k, Int.natCast_add]
          cases h : (val n).getLsbD k <;> simp [Int.add_mul]
        have hU : ((List.range 8).map fun k =>
              ((List.range (n+1)).filter (fun i => sel i && (val i).getLsbD k)).length * wt k).sum =
            ((List.range 8).map fun k =>
                ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length * wt k).sum +
              ((List.range 8).map fun k => (if (val n).getLsbD k then wt k else 0)).sum := by
          rw [show (List.range 8).map (fun k =>
                ((List.range (n+1)).filter (fun i => sel i && (val i).getLsbD k)).length * wt k) =
              (List.range 8).map (fun k =>
                  ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length * wt k +
                    (if (val n).getLsbD k then wt k else 0))
                from List.map_congr_left (fun a _ => helem a)]
          exact map_add_sum (List.range 8)
            (fun k => ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length * wt k)
            (fun k => if (val n).getLsbD k then wt k else 0)
        rw [hT, hU, ih, ← lane (val n)]
      · -- lane `n` is not selected: it contributes nothing on either side
        have hsf : sel n = false := by
          cases h : sel n <;> simp_all
        have hT : (((List.range (n+1)).filter sel).map fun i => (val i).toInt).sum =
            (((List.range n).filter sel).map fun i => (val i).toInt).sum := by
          rw [List.range_succ, List.filter_append, List.filter_cons_of_neg (by simp [hsf])]
          simp
        have helem (k : Nat) :
            ((List.range (n+1)).filter (fun i => sel i && (val i).getLsbD k)).length * wt k =
              ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length * wt k := by
          rw [len_range_succ_filter (fun i => sel i && (val i).getLsbD k) n]
          cases h : (val n).getLsbD k <;> simp [hsf]
        rw [hT]
        rw [show (List.range 8).map (fun k =>
              ((List.range (n+1)).filter (fun i => sel i && (val i).getLsbD k)).length * wt k) =
            (List.range 8).map (fun k =>
                ((List.range n).filter (fun i => sel i && (val i).getLsbD k)).length * wt k)
              from List.map_congr_left (fun a _ => helem a)]
        exact ih

/-! ### Relating the `filterMap`-based `scalar` list to the selected-index list -/

private theorem bridge (mask : W) (xs : Nat → Option B) :
    ∀ (l : List Nat),
      (((l.filter mask.getLsbD).filterMap xs).map BitVec.toInt).sum =
        ((l.filter (fun i => mask.getLsbD i && (xs i).isSome)).map
          fun i => ((xs i).getD 0).toInt).sum := by
  intro l
  induction l with
  | nil => rfl
  | cons i t ih =>
      by_cases h1 : mask.getLsbD i
      · cases h2 : xs i with
        | none =>
            rw [List.filter_cons_of_pos h1, List.filterMap_cons_none h2, ih,
              List.filter_cons_of_neg (by simp [h2])]
        | some v =>
            rw [List.filter_cons_of_pos h1, List.filterMap_cons_some h2,
              List.map_cons, List.sum_cons, ih,
              List.filter_cons_of_pos (by simp [h1, h2])]
            simp [h2]
      · rw [List.filter_cons_of_neg h1,
          List.filter_cons_of_neg (by simp [h1]), ih]

private theorem empty_iff (mask : W) (xs : Nat → Option B) (m : W)
    (hm : ∀ i, i < 64 → m.getLsbD i = (mask.getLsbD i && (xs i).isSome)) :
    m = 0 ↔ ((List.range 64).filter mask.getLsbD).filterMap xs = [] := by
  have hzero : m = 0 ↔ ∀ i, i < 64 → m.getLsbD i = false := by
    constructor
    · intro h i _
      rw [h]
      simp
    · intro h
      apply BitVec.eq_of_getLsbD_eq
      intro i hi
      rcases Nat.lt_or_ge i 64 with hlt | hge
      · have := h i hlt
        rw [this]
        simp
      · have h1 : m.getLsbD i = false := by
          change m.toNat.testBit i = false
          exact Nat.testBit_lt_two_pow (Nat.lt_of_lt_of_le m.isLt
            (Nat.pow_le_pow_right (by decide : (2:Nat) > 0) hge))
        rw [h1]
        simp
  have hkey : ((List.range 64).filter mask.getLsbD).filterMap xs = [] ↔
      ∀ i, i < 64 → ¬ (mask.getLsbD i && (xs i).isSome) := by
    rw [List.filterMap_eq_nil_iff]
    constructor
    · intro h i hi hh
      rw [Bool.and_eq_true] at hh
      cases hx : xs i with
      | none =>
          simp [hx] at hh
      | some v =>
          have := h i (List.mem_filter.mpr ⟨List.mem_range.mpr hi, hh.1⟩)
          rw [hx] at this
          simp at this
    · intro h a ha
      rw [List.mem_filter] at ha
      cases hx : xs a with
      | none => simp
      | some v =>
          have hc := h a (List.mem_range.mp ha.1)
          have hp : (mask.getLsbD a && (xs a).isSome) = true := by simp [ha.2, hx]
          exact absurd hp hc
  rw [hzero, hkey]
  constructor
  · intro h i hi hh
    have hthis := h i hi
    rw [hm i hi] at hthis
    rw [hthis] at hh
    exact absurd hh Bool.false_ne_true
  · intro h i hi
    cases hbit : m.getLsbD i with
    | false => simp
    | true =>
        have hand : (mask.getLsbD i && (xs i).isSome) = true := by
          rw [← hm i hi]
          exact hbit
        exact absurd hand (h i hi)

theorem sound : TinylambOpt.ResearchGoals.BitPlaneSum.Goal := by
  intro mask xs
  have hpack1 : ∀ i, i < 64 → (pack fun i => (xs i).isSome).getLsbD i = (xs i).isSome :=
    pack_getLsbD _
  have hpack2 : ∀ k i, i < 64 →
      (pack fun i => ((xs i).getD 0).getLsbD k).getLsbD i = ((xs i).getD 0).getLsbD k := by
    intro k
    exact pack_getLsbD _
  have hmbit : ∀ i, i < 64 →
      (mask &&& pack (fun i => (xs i).isSome)).getLsbD i = (mask.getLsbD i && (xs i).isSome) := by
    intro i hi
    rw [BitVec.getLsbD_and, hpack1 i hi]
  have hmbit2 : ∀ (i k : Nat), i < 64 →
      ((mask &&& pack (fun i => (xs i).isSome)) &&&
          pack (fun i => ((xs i).getD 0).getLsbD k)).getLsbD i =
        ((mask.getLsbD i && (xs i).isSome) && ((xs i).getD 0).getLsbD k) := by
    intro i k hi
    rw [BitVec.getLsbD_and, hmbit i hi, hpack2 k i hi]
  -- per-plane population counts as selected-lane bit counts
  have hcount : ∀ k,
      count ((mask &&& pack (fun i => (xs i).isSome)) &&& pack (fun i => ((xs i).getD 0).getLsbD k)) =
        ((List.range 64).filter (fun i =>
            (mask.getLsbD i && (xs i).isSome) && ((xs i).getD 0).getLsbD k)).length := by
    intro k
    show ((List.range 64).filter (fun i =>
        ((mask &&& pack (fun i => (xs i).isSome)) &&&
            pack (fun i => ((xs i).getD 0).getLsbD k)).getLsbD i)).length = _
    rw [List.filter_congr (fun i hi => hmbit2 i k (List.mem_range.mp hi))]
  -- emptiness of the selection mask vs. of the scalar lane list
  have hniliff : ∀ (l : List B), (¬ List.isEmpty l) ↔ l ≠ [] := by
    intro l
    cases l with
    | nil => simp
    | cons a as => simp
  -- the double-sum swap, specialized to this mask/xs
  have hsum := foldsum (fun i => mask.getLsbD i && (xs i).isSome) (fun i => (xs i).getD 0) 64
  rw [show ((List.range 8).map fun k =>
        ((List.range 64).filter (fun i =>
            (mask.getLsbD i && (xs i).isSome) && ((xs i).getD 0).getLsbD k)).length * wt k).sum =
      ((List.range 8).map fun k =>
          (count ((mask &&& pack (fun i => (xs i).isSome)) &&&
            pack (fun i => ((xs i).getD 0).getLsbD k)) : Int) * wt k).sum
      from congrArg List.sum (List.map_congr_left
        (fun k _ => congrArg (fun x : Nat => (x : Int) * wt k) (hcount k).symm))] at hsum
  rw [← bridge mask xs (List.range 64)] at hsum
  -- assemble the `none`/`some` cases
  by_cases h0 : (mask &&& pack (fun i => (xs i).isSome)) = 0
  · have hve : List.isEmpty (((List.range 64).filter mask.getLsbD).filterMap xs) := by
      rw [(empty_iff mask xs _ hmbit).mp h0]
      simp
    simp only [weighted, scalar, ite_eq_left h0, ite_eq_left hve]
  · have hve : ¬ List.isEmpty (((List.range 64).filter mask.getLsbD).filterMap xs) := by
      refine (hniliff _).mpr ?_
      intro hnil
      exact h0 ((empty_iff mask xs _ hmbit).mpr hnil)
    simp only [weighted, scalar, ite_eq_right h0, ite_eq_right hve]
    exact (congrArg Option.some hsum).symm

end TinylambOpt.ResearchProofs.BitPlaneSum
