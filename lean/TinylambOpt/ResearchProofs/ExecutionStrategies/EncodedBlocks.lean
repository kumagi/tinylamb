import TinylambOpt.ResearchGoals.ExecutionStrategies.EncodedBlocks

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.EncodedBlocks

open TinylambOpt.ResearchGoals.EncodedBlocks

/-!
# Encoded-block execution strategies

Proofs for the eight `EncodedBlocks` research goals: patched delta frames,
sparse gathers via validity-bitmap ranks, run-pair overlap aggregation,
arithmetic-progression slices, difference-array range edits, DISTINCT seam
correction across pages, binary-search selection directories, and
second-difference block integration.
-/

/-- Patched frame of reference: an exception lane replaces its base code, so
the two filters agree pointwise. For `patch i = some x` both sides compare
`x` directly; for `none` the delta-domain bounds `lo-base ≤ d < hi-base` are
exactly `lo ≤ base+d < hi` over `Int` (linear arithmetic, `omega`). -/
theorem sound_PatchedRange : PatchedRange.Goal := by
  intro base lo hi deltas patch live
  simp only []
  apply List.filter_congr
  intro i _
  rw [Bool.eq_iff_iff]
  cases hpi : patch i with
  | none =>
      simp only [Option.getD_none, Bool.and_eq_true, decide_eq_true_eq, and_assoc]
      constructor
      · rintro ⟨hl, h1, h2⟩
        exact ⟨hl, by omega, by omega⟩
      · rintro ⟨hl, h1, h2⟩
        exact ⟨hl, by omega, by omega⟩
  | some x =>
      simp only [Option.getD_some, Bool.and_eq_true, decide_eq_true_eq, and_assoc]

/-- Sparse gather: the `filterMap` payload at validity-rank `k` is the `k`-th
non-`none` element. Proved by induction on `xs`: the head contributes rank 0
when it is `some`, and a `succ` index shifts both the validity prefix count
and the dense index by exactly the head's contribution (`0` for `none`,
`1` for `some`), reducing to the induction hypothesis. -/
private theorem sparseGather_aux (xs : List (Option Int)) : ∀ i,
    (if (xs.map Option.isSome)[i]?.getD false
        then (xs.filterMap id)[((xs.map Option.isSome).take i).count true]?
        else none) = xs[i]?.join := by
  induction xs with
  | nil =>
      intro i
      rfl
  | cons x xs' ih =>
      intro i
      cases i with
      | zero =>
          cases x with
          | none => rfl
          | some v =>
              simp only [List.map_cons, List.getElem?_cons_zero, Option.isSome, Option.getD,
                List.take_zero, List.count_nil, List.filterMap_cons, id_eq, List.getElem?_cons_zero]
              rfl
      | succ j =>
          cases x with
          | none =>
              simp only [List.map_cons, List.getElem?_cons_succ, List.take_succ_cons, List.count_cons,
                Option.isSome, List.filterMap_cons, id_eq]
              simp
              rw [← List.getElem?_map]
              exact ih j
          | some v =>
              simp only [List.map_cons, List.getElem?_cons_succ, List.take_succ_cons, List.count_cons,
                Option.isSome, List.filterMap_cons, id_eq]
              simp
              rw [← List.getElem?_map]
              exact ih j

theorem sound_SparseGather : SparseGather.Goal := by
  intro xs i
  exact sparseGather_aux xs i

/-- `zipIdx 1` vs `zipIdx` (0-based): shifting every index by `1` while
strengthening the weight constant by `1` leaves the weighted sum unchanged.
Proved via `List.zipIdx_eq_map_add` and pointwise `Int` linear arithmetic. -/
private theorem sd_tail (ds' : List Int) :
    ((ds'.zipIdx 1).map (fun x => (((ds'.length + 1 : Nat) : Int) - (x.snd : Int)) * x.fst)).sum =
    ((ds'.zipIdx).map (fun x => ((ds'.length : Int) - (x.snd : Int)) * x.fst)).sum := by
  rw [List.zipIdx_eq_map_add, List.map_map]
  congr 1
  apply List.map_congr_left
  intro p hp
  obtain ⟨d, j⟩ := p
  simp only [Function.comp]
  have h1 : ((ds'.length + 1 : Nat) : Int) = (ds'.length : Int) + 1 := by
    rw [Int.natCast_add, Int.natCast_one]
  have h2 : ((1 + j : Nat) : Int) = 1 + (j : Int) := by
    rw [Int.natCast_add, Int.natCast_one]
  rw [h1, h2]
  have : (ds'.length : Int) + 1 - (1 + (j : Int)) = (ds'.length : Int) - (j : Int) := by omega
  rw [this]

/-- Second differences: `integrate` accumulates `(pos, vel)` where each second
difference `d` bumps `vel` by `d` and `pos` by the *current* `vel + d`. After
the fold, `vel = v + Σd` and `pos = p + len·v + Σ (len-i)·d_i` — each `d_i`
contributes to `pos` once per remaining step. Induction on `ds`; the
`zipIdx 1` tail is re-based by `sd_tail`, and the remaining identity is
linear arithmetic over the shared monomials (`omega` after distributing the
`(len+1)·x` products). -/
theorem sound_SecondDifferences : SecondDifferences.Goal := by
  intro p v ds
  induction ds generalizing p v with
  | nil =>
      simp [integrate]
  | cons d ds' ih =>
      have step : integrate p v (d :: ds') = integrate (p + v + d) (v + d) ds' := rfl
      rw [step, ih]
      simp only [List.length_cons, List.zipIdx_cons, List.map_cons, List.sum_cons,
        Nat.zero_add, Int.natCast_zero, Int.sub_zero]
      rw [sd_tail ds']
      simp only [Prod.mk.injEq]
      constructor
      · simp only [Int.natCast_add, Int.natCast_one, Int.mul_add, Int.add_mul,
          Int.one_mul]
        omega
      · omega

/-- `drop lo` of `range m` is `range (m-lo)` shifted by `lo` (Nat subtraction
makes the `lo > m` case collapse to `[]` on both sides). Induction on `lo`
via `range_succ_eq_map` + `map_drop`. -/
private theorem drop_range (m lo : Nat) :
    (List.range m).drop lo = (List.range (m - lo)).map (· + lo) := by
  induction lo generalizing m with
  | zero =>
      simp only [Nat.sub_zero, List.drop_zero]
      exact (List.map_id'' (fun x => by omega) _).symm
  | succ lo ih =>
      cases m with
      | zero => simp [List.range_zero]
      | succ m =>
          rw [List.range_succ_eq_map, List.drop_succ_cons, ← List.map_drop, ih]
          rw [List.map_map]
          have hlen : m + 1 - (lo + 1) = m - lo := by omega
          rw [hlen]
          apply List.map_congr_left
          intro j hj
          show Nat.succ (j + lo) = j + (lo + 1)
          rw [Nat.succ_eq_add_one, Nat.add_assoc]

/-- Doubled sum of an arithmetic progression `a + (k+s)·d` over `k < len`:
`2·Σ = len·(2·first + (len-1)·d)` where `first = a + s·d`. Induction on `len`;
the step adds the last term `a + (len+s)·d` and the remaining identity is
verified by distributing all products (`Int.add_mul`/`mul_add`/`mul_sub`/
`sub_mul`) and closing with `omega` over the shared monomials. -/
private theorem strideSlice_aux (a d : Int) (s : Nat) : ∀ (len : Nat),
    2 * (((List.range len).map (fun k => a + ((k + s : Nat) : Int) * d)).sum) =
    (len : Int) * (2 * (a + (s : Int) * d) + ((len : Int) - 1) * d) := by
  intro len
  induction len with
  | zero => simp [List.range_zero]
  | succ len ih =>
      rw [List.range_succ, List.map_append]
      have hmap : (([len] : List Nat).map (fun k => a + ((k + s : Nat) : Int) * d)) =
          [a + ((len + s : Nat) : Int) * d] := by simp
      rw [hmap, List.sum_append]
      simp only [List.sum_cons, List.sum_nil]
      rw [Int.mul_add, ih]
      simp only [Int.natCast_add, Int.natCast_one, Int.add_mul, Int.mul_add,
        Int.mul_sub, Int.sub_mul, Int.add_sub_cancel, Int.one_mul]
      omega

/-- Stride slice: `take hi` of `range n` is `range (min hi n)`; `drop lo`
re-bases it at `lo` via `drop_range`; `min_comm` aligns with the goal's
`min n hi`; `map_map` composes the `+lo` shift into the stride term, and
`strideSlice_aux` finishes. -/
theorem sound_StrideSlice : StrideSlice.Goal := by
  intro a d n lo hi
  simp only []
  rw [List.take_range, drop_range]
  rw [Nat.min_comm]
  rw [List.map_map]
  exact strideSlice_aux a d lo _

/-- One edit's contribution to the impulse map at boundary index `i`: a live
edit `(l,r,d)` with `l < r` deposits `+d` at `l` and `-d` at `r`; degenerate
edits (`l ≥ r`) and all other indices contribute `0`. -/
private def impulseAt (i : Nat) : Edit → Int
  | (l, r, d) => if l < r then (if l = i then d else 0) - (if r = i then d else 0) else 0

/-- Inserting one edit's two boundary impulses updates any lookup by exactly
`impulseAt`. The `l < r` guard makes `l ≠ r`, so the two inserts never alias;
the four `l = i`/`r = i` cases reduce to linear integer arithmetic (`omega`). -/
private theorem step_getD (h : Std.HashMap Nat Int) (l r : Nat) (d : Int) (i : Nat) :
    ((if l < r then
        (h.insert l (h.getD l 0 + d)).insert r
          ((h.insert l (h.getD l 0 + d)).getD r 0 - d)
      else h)).getD i 0 =
    h.getD i 0 + impulseAt i (l, r, d) := by
  simp only [impulseAt]
  by_cases hlr : l < r
  · rw [ite_eq_left hlr, ite_eq_left hlr]
    simp only [Std.HashMap.getD_insert, beq_iff_eq]
    have hne : l ≠ r := Nat.ne_of_lt hlr
    by_cases hli : l = i <;> by_cases hri : r = i <;>
      simp_all <;> omega
  · rw [ite_eq_right hlr, ite_eq_right hlr]
    omega

/-- The impulse map's value at `i` equals the base map's value plus the sum of
per-edit contributions. Induction over `es` via the `foldl` step lemma. -/
private theorem impulses_foldl_getD (h : Std.HashMap Nat Int) :
    ∀ (es : List Edit), ∀ (i : Nat),
      (es.foldl (fun h (l,r,d) =>
        if l < r then
          let h1 := h.insert l (h.getD l 0 + d)
          h1.insert r (h1.getD r 0 - d)
        else h) h).getD i 0 =
      h.getD i 0 + (es.map fun e => impulseAt i e).sum := by
  intro es
  induction es generalizing h with
  | nil => intro i; simp
  | cons e es ih =>
      intro i
      cases e with
      | mk l p =>
        cases p with
        | mk r d =>
          simp only [List.foldl_cons, List.map_cons, List.sum_cons, impulseAt]
          rw [ih]
          rw [step_getD]
          simp only [impulseAt]
          omega

private theorem impulses_getD (es : List Edit) (i : Nat) :
    (impulses es).getD i 0 = (es.map fun e => impulseAt i e).sum := by
  unfold impulses
  rw [impulses_foldl_getD]
  simp [Std.HashMap.getD_empty]

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    (l.map fun x => f x + g x).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem sum_map_sub_int {α : Type} (l : List α) (f g : α → Int) :
    (l.map fun x => f x - g x).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem sum_const_zero {α : Type} (l : List α) :
    (l.map fun _ => (0 : Int)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [List.map_cons, List.sum_cons, ih]

/-- Sum over positions of per-edit sums equals sum over edits of per-position
sums (Fubini for finite lists). Induction on the position list; each step
splits the edit-map sum via `sum_map_add_int`. -/
private theorem sum_sum_swap (l₁ : List Nat) (l₂ : List Edit) (f : Nat → Edit → Int) :
    ((l₁.map fun j => (l₂.map fun e => f j e).sum).sum) =
    ((l₂.map fun e => (l₁.map fun j => f j e).sum).sum) := by
  induction l₁ with
  | nil => simp [sum_const_zero]
  | cons j js ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [sum_map_add_int]

/-- A single boundary `c` contributes `d` to the prefix sum over `j ≤ i`
exactly when `c ≤ i`. Induction on `i`; the `i+1` term is the new boundary
hit. -/
private theorem sum_range_succ_ite_eq (i c : Nat) (d : Int) :
    ((List.range (i+1)).map fun j => if c = j then d else 0).sum =
    if c ≤ i then d else 0 := by
  induction i with
  | zero =>
      simp [List.range_succ]
  | succ i ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases h1 : c ≤ i <;> by_cases h2 : c = i + 1 <;>
        simp_all <;> omega

/-- Prefix-sum characterization of one edit: the impulses of `(l,r,d)`
accumulate to `d` over positions `j ≤ i` iff `l ≤ i < r` (live range), else
`0`. Splits the `impulseAt` difference into two boundary sums via
`sum_map_sub_int`, then applies `sum_range_succ_ite_eq` to each. -/
private theorem sum_impulseAt_prefix (i l r : Nat) (d : Int) :
    ((List.range (i+1)).map fun j => impulseAt j (l,r,d)).sum =
    if l ≤ i && i < r then d else 0 := by
  by_cases hlr : l < r
  · have hfun : (fun j => impulseAt j (l,r,d)) =
        (fun j => (if l = j then d else 0) - (if r = j then d else 0)) := by
      funext j
      simp only [impulseAt, ite_eq_left hlr]
    rw [hfun]
    rw [show ((List.range (i+1)).map fun j => (if l = j then d else 0) - (if r = j then d else 0)).sum =
        ((List.range (i+1)).map fun j => (if l = j then d else 0)).sum -
        ((List.range (i+1)).map fun j => (if r = j then d else 0)).sum from
      sum_map_sub_int _ _ _]
    rw [sum_range_succ_ite_eq, sum_range_succ_ite_eq]
    by_cases h1 : l ≤ i <;> by_cases h2 : r ≤ i <;>
      simp_all <;> omega
  · have hzero : ∀ j, impulseAt j (l,r,d) = 0 := by
      intro j; simp only [impulseAt, ite_eq_right hlr]
    rw [List.map_congr_left (fun j _ => hzero j)]
    rw [sum_const_zero]
    have hcond : ¬ (l ≤ i && i < r) := by
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      intro ⟨h1, h2⟩; omega
    rw [ite_eq_right hcond]

/-- `foldl` of `s ↦ s + f x` equals `b + Σ f` over the list. Induction on the
list; the step is `b' + f x + Σ = (b' + Σ) + f x` by commutativity/associativity
(`omega` over `Int`). -/
private theorem foldl_add_int (l : List Nat) (f : Nat → Int) :
    ∀ (b : Int), (l.foldl (fun s x => s + f x) b) = b + (l.map f).sum := by
  induction l with
  | nil => intro b; simp
  | cons x xs ih =>
      intro b
      simp only [List.foldl_cons, List.map_cons, List.sum_cons]
      rw [ih]
      omega

/-- Range edits: the difference-array scan (`scanl` of impulse lookups,
`drop 1`) produces per-position adjustments equal to the direct per-edit
membership sum. Pointwise via `List.ext_getElem?`: for `i < xs.length` the
scan prefix `Σ_{j≤i} impulses[j]` equals `Σ_e Σ_{j≤i} impulseAt j e` (by
`impulses_getD` + `sum_sum_swap`), which per edit is `d` iff `l ≤ i < r` —
exactly the direct sum's condition. Out-of-range indices give `none` on both
sides. -/
theorem sound_RangeEdits : RangeEdits.Goal := by
  intro xs es
  simp only []
  apply List.ext_getElem?
  intro i
  by_cases hi : i < xs.length
  · have hx : xs[i]? = some xs[i] := List.getElem?_eq_getElem hi
    have hadj : (List.drop 1 (List.scanl (fun s j => s + (impulses es).getD j 0) 0
        (List.range xs.length)))[i]? =
        some (((List.range (i+1)).map fun j => (impulses es).getD j 0).sum) := by
      rw [List.getElem?_drop, List.getElem?_scanl]
      rw [ite_eq_left (by rw [List.length_range]; omega : 1 + i ≤ (List.range xs.length).length)]
      rw [List.take_range, Nat.min_eq_left (by omega : 1 + i ≤ xs.length)]
      rw [show 1 + i = i + 1 from by omega]
      rw [foldl_add_int]
      simp
    have hL : (List.zipWith (fun x1 x2 => x1 + x2) xs
        (List.drop 1 (List.scanl (fun s j => s + (impulses es).getD j 0) 0
          (List.range xs.length))))[i]? =
        some (xs[i] + ((List.range (i+1)).map fun j => (impulses es).getD j 0).sum) := by
      rw [List.getElem?_zipWith, hx, hadj]
    have hR : (List.map (fun x => x.fst + (List.map (fun x_1 =>
        if (decide (x_1.fst ≤ x.snd) && decide (x.snd < x_1.snd.fst)) = true then x_1.snd.snd else 0)
          es).sum) xs.zipIdx)[i]? =
        some (xs[i] + (es.map fun x_1 =>
          if (decide (x_1.fst ≤ i) && decide (i < x_1.snd.fst)) = true then x_1.snd.snd else 0).sum) := by
      rw [List.getElem?_map, List.getElem?_zipIdx, hx]
      simp only [Option.map_some]
      rw [Nat.zero_add]
    have hadjval : ((List.range (i+1)).map fun j => (impulses es).getD j 0).sum =
        (es.map fun x_1 =>
          if (decide (x_1.fst ≤ i) && decide (i < x_1.snd.fst)) = true then x_1.snd.snd else 0).sum := by
      simp only [impulses_getD]
      rw [sum_sum_swap]
      apply congrArg List.sum
      apply List.map_congr_left
      intro e he
      cases e with
      | mk l p =>
        cases p with
        | mk r d =>
          exact sum_impulseAt_prefix i l r d
    rw [hL, hR, hadjval]
  · have hx : xs[i]? = none := by
      rw [List.getElem?_eq_none_iff]; omega
    rw [List.getElem?_zipWith, hx, List.getElem?_map, List.getElem?_zipIdx, hx]
    simp

end TinylambOpt.ResearchProofs.ExecutionStrategies.EncodedBlocks
