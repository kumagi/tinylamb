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



private def idxs (mask : List Bool) : List Nat :=
  (mask.zipIdx.filter Prod.fst).map Prod.snd

private theorem zipIdx_add {α : Type} (l : List α) (i c : Nat) :
    l.zipIdx (i + c) = (l.zipIdx i).map (fun p => (p.1, p.2 + c)) := by
  induction l generalizing i with
  | nil => simp [List.zipIdx_nil]
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, List.map_cons]
      rw [show i + c + 1 = (i + 1) + c from by omega]
      rw [ih]

private theorem zipIdx_one (l : List Bool) :
    l.zipIdx 1 = (l.zipIdx 0).map (fun p => (p.1, p.2 + 1)) := by
  have h := zipIdx_add l 0 1
  simpa using h

private theorem shift_filter_snd (bs : List Bool) :
    ((bs.zipIdx 1).filter Prod.fst).map Prod.snd =
      (((bs.zipIdx 0).filter Prod.fst).map Prod.snd).map (· + 1) := by
  have h1 : bs.zipIdx 1 = (bs.zipIdx 0).map (fun p => (p.1, p.2 + 1)) := zipIdx_one bs
  rw [h1, List.filter_map]
  have hcomp : (Prod.fst ∘ (fun p : Bool × Nat => (p.1, p.2 + 1))) = Prod.fst := rfl
  rw [hcomp, List.map_map, List.map_map]
  rfl

private theorem idxs_cons (b : Bool) (bs : List Bool) :
    idxs (b :: bs) = (if b then [0] else []) ++ (idxs bs).map (· + 1) := by
  unfold idxs
  rw [List.zipIdx_cons]
  simp only [Nat.zero_add]
  rw [List.filter_cons]
  have hfst : Prod.fst (b, 0) = b := rfl
  rw [hfst]
  by_cases hb : b = true
  · subst hb
    rw [ite_eq_left rfl, List.map_cons]
    have h0 : Prod.snd (true, 0) = 0 := rfl
    rw [h0]
    show (0 : Nat) :: ((bs.zipIdx 1).filter Prod.fst).map Prod.snd =
      [0] ++ List.map (· + 1) (((bs.zipIdx 0).filter Prod.fst).map Prod.snd)
    rw [shift_filter_snd]
    rfl
  · have hb' : b = false := by cases b <;> simp_all
    subst hb'
    rw [ite_eq_right (by simp : ¬ (false = true))]
    show ((bs.zipIdx 1).filter Prod.fst).map Prod.snd =
      ([] : List Nat) ++ List.map (· + 1) (((bs.zipIdx 0).filter Prod.fst).map Prod.snd)
    rw [shift_filter_snd]
    rfl

-- ===== NEW: zipIdx index bound =====
private theorem zipIdx_snd_lt {α : Type} (l : List α) (i : Nat) :
    ∀ p ∈ l.zipIdx i, p.2 < l.length + i := by
  induction l generalizing i with
  | nil => intro p hp; simp [List.zipIdx_nil] at hp
  | cons a as ih =>
      intro p hp
      rw [List.zipIdx_cons, List.mem_cons] at hp
      cases hp with
      | inl h =>
          subst h
          simp only [Prod.snd, List.length_cons]
          omega
      | inr h =>
          have h2 := ih (i + 1) p h
          simp only [List.length_cons] at h2 ⊢
          omega

private theorem idxs_mem_lt {mask : List Bool} {x : Nat} (h : x ∈ idxs mask) :
    x < mask.length := by
  unfold idxs at h
  rw [List.mem_map] at h
  obtain ⟨p, hp, rfl⟩ := h
  rw [List.mem_filter] at hp
  have h2 := zipIdx_snd_lt mask 0 p hp.1
  simp only [Nat.add_zero] at h2
  exact h2

-- ===== NEW: sortedness =====
private theorem pairwise_map_succ {l : List Nat} (h : l.Pairwise (· < ·)) :
    (l.map (· + 1)).Pairwise (· < ·) := by
  rw [List.pairwise_map]
  exact List.Pairwise.imp (fun {a b} hab => Nat.add_lt_add_iff_right.mpr hab) h

private theorem idxs_pairwise (mask : List Bool) : (idxs mask).Pairwise (· < ·) := by
  induction mask with
  | nil => simp [idxs]
  | cons b bs ih =>
      rw [idxs_cons]
      by_cases hb : b = true
      · subst hb
        rw [show (if (true = true) then ([0] : List Nat) else []) = [0] from rfl]
        rw [List.pairwise_append]
        refine ⟨List.pairwise_singleton _ _, pairwise_map_succ ih, ?_⟩
        intro a ha c hc
        rw [List.mem_singleton] at ha
        subst ha
        rw [List.mem_map] at hc
        obtain ⟨d, hd, rfl⟩ := hc
        omega
      · have hb' : b = false := by cases b <;> simp_all
        subst hb'
        rw [show (if (false = true) then ([0] : List Nat) else []) = [] from rfl]
        rw [List.nil_append]
        exact pairwise_map_succ ih


-- countP predicate shift (composed form as produced by countP_map)
private theorem countP_succ_lt (l : List Nat) (j : Nat) :
    l.countP ((fun y => y < j + 1) ∘ fun x => x + 1) = l.countP (· < j) := by
  apply List.countP_congr
  intro x hx
  show (decide (x + 1 < j + 1) = true ↔ decide (x < j) = true)
  rw [decide_eq_true_eq, decide_eq_true_eq]
  exact Nat.add_lt_add_iff_right

private theorem countP_lt_succ_zero (j : Nat) :
    ([0] : List Nat).countP (· < j + 1) = 1 := by
  simp [List.countP_cons]

private theorem rank_idx (mask : List Bool) :
    ∀ i, i ≤ mask.length →
      (mask.take i).count true = (idxs mask).countP (· < i) := by
  induction mask with
  | nil =>
      intro i hi
      have hi0 : i = 0 := by simp at hi; omega
      subst hi0
      simp [idxs]
  | cons b bs ih =>
      intro i hi
      cases i with
      | zero =>
          simp [idxs]
      | succ j =>
          have hj : j ≤ bs.length := by
            simp only [List.length_cons] at hi
            omega
          rw [List.take_succ_cons, List.count_cons, ih j hj]
          rw [idxs_cons, List.countP_append, List.countP_map]
          by_cases hb : b = true
          · subst hb
            rw [show (if (true = true) then ([0] : List Nat) else []) = [0] from rfl]
            rw [countP_lt_succ_zero]
            have h1 : (if ((true == true) = true) then (1 : Nat) else 0) = 1 := by decide
            rw [h1, countP_succ_lt]
            omega
          · have hb' : b = false := by cases b <;> simp_all
            subst hb'
            rw [show (if (false = true) then ([0] : List Nat) else []) = [] from rfl]
            rw [List.countP_nil]
            have h0 : (if ((false == true) = true) then (1 : Nat) else 0) = 0 := by decide
            rw [h0, countP_succ_lt]
            omega


-- ===== NEW countP lemmas =====
private theorem sorted_le_getElem {l : List Nat} (h : l.Pairwise (· < ·))
    {i j : Nat} (hi : i < l.length) (hij : i ≤ j) (hj : j < l.length) : l[i] ≤ l[j] := by
  rw [List.pairwise_iff_getElem] at h
  by_cases hlt : i < j
  · exact Nat.le_of_lt (h i j hi hj hlt)
  · have heq : i = j := by omega
    subst heq
    exact Nat.le_refl _

private theorem pairwise_countP_lt (l : List Nat) :
    l.Pairwise (· < ·) → ∀ {k : Nat} (hk : k < l.length), l.countP (· < l[k]) = k := by
  induction l with
  | nil => intro h k hk; simp at hk
  | cons x xs ih =>
      intro h k hk
      rw [List.pairwise_cons] at h
      obtain ⟨hhead, htail⟩ := h
      rcases Nat.eq_zero_or_pos k with rfl | hkpos
      · have h0 : (x :: xs)[0] = x := List.getElem_cons_zero x xs (by simp)
        rw [h0]
        apply List.countP_eq_zero.mpr
        intro a ha
        rw [List.mem_cons] at ha
        rcases ha with hax | hax
        · intro hlt
          rw [hax] at hlt
          rw [decide_eq_true_eq] at hlt
          exact Nat.lt_irrefl x hlt
        · intro hlt
          rw [decide_eq_true_eq] at hlt
          exact Nat.lt_asymm (hhead a hax) hlt
      · obtain ⟨j, rfl⟩ : ∃ j, k = j + 1 := ⟨k - 1, by omega⟩
        have hj : j < xs.length := by
          simp only [List.length_cons] at hk; omega
        have hget : (x :: xs)[j + 1] = xs[j] :=
          List.getElem_cons_succ x xs j (by simp only [List.length_cons]; omega)
        rw [hget]
        rw [List.countP_cons, ih htail hj]
        have hxj : x < xs[j] := hhead _ (List.getElem_mem hj)
        have hcond : decide (x < xs[j]) = true := by
          rw [decide_eq_true_eq]; exact hxj
        rw [ite_eq_left hcond]

private theorem countP_lt_ge {l : List Nat} (h : l.Pairwise (· < ·))
    {k j : Nat} (hk : k < l.length) (hkj : l[k] < j) :
    k + 1 ≤ l.countP (· < j) := by
  have hsub : (l.take (k + 1)).Sublist l := List.take_sublist (k + 1) l
  have hle : (l.take (k + 1)).countP (· < j) ≤ l.countP (· < j) :=
    List.Sublist.countP_le hsub
  have hall : ∀ a ∈ l.take (k + 1), decide (a < j) = true := by
    intro a ha
    obtain ⟨m, hm, hma⟩ := List.getElem_of_mem ha
    have hmkl : m < k + 1 := by
      have h1 := hm
      rw [List.length_take] at h1
      have hmin := Nat.min_le_left (k + 1) l.length
      omega
    have hmkl2 : m < l.length := by
      have h1 := hm
      rw [List.length_take] at h1
      have hmin := Nat.min_le_right (k + 1) l.length
      omega
    have hml : (l.take (k + 1))[m] = l[m] := List.getElem_take (h := hm)
    have ham : a = l[m] := hma.symm.trans hml
    subst ham
    rw [decide_eq_true_eq]
    have hmk : m ≤ k := by omega
    have hle2 : l[m] ≤ l[k] := sorted_le_getElem h hmkl2 hmk hk
    omega
  have heq : (l.take (k + 1)).countP (· < j) = k + 1 := by
    have h1 : (l.take (k + 1)).countP (· < j) = (l.take (k + 1)).length :=
      List.countP_eq_length.mpr hall
    rw [h1, List.length_take]
    have hkl : k + 1 ≤ l.length := by omega
    simp [Nat.min_eq_left hkl]
  omega

private theorem countP_all_lt {l : List Nat} {n : Nat} (h : ∀ a ∈ l, a < n) :
    l.countP (· < n) = l.length := by
  apply List.countP_eq_length.mpr
  intro a ha
  rw [decide_eq_true_eq]
  exact h a ha


private def rsOf (mask : List Bool) : List Nat :=
  mask.scanl (fun n bit => n + bit.toNat) 0

private theorem tonat_eq_count_ite (b : Bool) :
    b.toNat = (if (b == true) = true then 1 else 0) := by
  cases b <;> rfl

private theorem foldl_tonat (c : Nat) (xs : List Bool) :
    List.foldl (fun n bit => n + bit.toNat) c xs = c + xs.count true := by
  induction xs generalizing c with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.count_cons]
      rw [ih]
      have h : b.toNat = (if (b == true) = true then 1 else 0) := tonat_eq_count_ite b
      rw [h]
      omega

private theorem rs_get (mask : List Bool) (i : Nat) (hi : i ≤ mask.length) :
    (rsOf mask)[i]? = some (List.foldl (fun n bit => n + bit.toNat) 0 (mask.take i)) := by
  rw [rsOf, List.getElem?_scanl]
  rw [ite_eq_left hi]

private theorem rs_eq_count (mask : List Bool) (i : Nat) (hi : i ≤ mask.length) :
    (rsOf mask)[i]? = some ((mask.take i).count true) := by
  rw [rs_get mask i hi, foldl_tonat, Nat.zero_add]

private theorem rs_mono (mask : List Bool) {i j : Nat} (hij : i ≤ j) (hj : j ≤ mask.length) :
    (mask.take i).count true ≤ (mask.take j).count true := by
  obtain ⟨d, rfl⟩ : ∃ d, j = i + d := ⟨j - i, by omega⟩
  rw [List.take_add, List.count_append]
  omega

private theorem rankAt (mask : List Bool) (j : Nat) (hj : j ≤ mask.length) :
    ((rsOf mask).toArray[j]?.getD 0) = (mask.take j).count true := by
  rw [List.getElem?_toArray, rs_eq_count mask j hj]
  rfl

private theorem locate_succ' (r : Array Nat) (w fuel lo hi : Nat) :
    locate r w (fuel+1) lo hi =
      if lo ≥ hi then lo
      else if r[(lo+hi)/2+1]?.getD 0 ≤ w then locate r w fuel ((lo+hi)/2+1) hi
           else locate r w fuel lo ((lo+hi)/2) := by
  rw [locate.eq_2]

private theorem locate_correct (mask : List Bool) (wanted answer : Nat)
    (hans_le : answer ≤ mask.length)
    (hans_max : ∀ i, i ≤ mask.length → (mask.take i).count true ≤ wanted → i ≤ answer)
    (hans_good : (mask.take answer).count true ≤ wanted) :
    ∀ (fuel lo hi : Nat), hi ≤ mask.length → lo ≤ answer → answer ≤ hi →
      2 ^ fuel > hi - lo →
      locate (rsOf mask).toArray wanted fuel lo hi = answer := by
  intro fuel
  induction fuel with
  | zero =>
      intro lo hi hbound hlo hhi hfuel
      rw [locate.eq_1]
      have h1 : (1 : Nat) > hi - lo := hfuel
      have hdz : hi ≤ lo := by omega
      have : answer = lo := by omega
      exact this.symm
  | succ fuel ih =>
      intro lo hi hbound hlo hhi hfuel
      rw [locate_succ']
      by_cases hlohi : lo ≥ hi
      · rw [ite_eq_left hlohi]
        have : answer = lo := by omega
        exact this.symm
      · rw [ite_eq_right (by omega : ¬ lo ≥ hi)]
        have hlt : lo < hi := by omega
        have hmid_lt : (lo+hi)/2 < hi := by omega
        have hmid1_le : (lo+hi)/2 + 1 ≤ mask.length := by omega
        rw [rankAt mask ((lo+hi)/2 + 1) hmid1_le]
        by_cases hcond : (mask.take ((lo+hi)/2 + 1)).count true ≤ wanted
        · rw [ite_eq_left hcond]
          have hlo' : (lo+hi)/2 + 1 ≤ answer := hans_max _ hmid1_le hcond
          have hsize : hi - ((lo+hi)/2 + 1) ≤ (hi - lo) / 2 := by omega
          have hfuel' : 2 ^ fuel > hi - ((lo+hi)/2 + 1) := by
            have h2f : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by rw [Nat.pow_succ']
            omega
          exact ih ((lo+hi)/2 + 1) hi hbound hlo' hhi hfuel'
        · rw [ite_eq_right hcond]
          have hgt : wanted < (mask.take ((lo+hi)/2 + 1)).count true := by omega
          have hsize : (lo+hi)/2 - lo ≤ (hi - lo) / 2 := by omega
          have hfuel' : 2 ^ fuel > (lo+hi)/2 - lo := by
            have h2f : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by rw [Nat.pow_succ']
            omega
          have hhi' : answer ≤ (lo+hi)/2 := by
            by_cases hle : answer ≤ (lo+hi)/2
            · exact hle
            · exfalso
              have hmid_ans : (lo+hi)/2 < answer := Nat.lt_of_not_ge hle
              have h1 : (lo+hi)/2 + 1 ≤ answer := by omega
              have hmono : (mask.take ((lo+hi)/2 + 1)).count true ≤
                  (mask.take answer).count true :=
                rs_mono mask (by omega) hans_le
              omega
          exact ih lo ((lo+hi)/2) (by omega) hlo hhi' hfuel'


private theorem pow_log2 (n : Nat) : n < 2 ^ (n.log2 + 2) := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
    by_cases h2 : 2 ≤ n
    · have hlog : n.log2 = (n / 2).log2 + 1 := by
        rw [Nat.log2_def]; simp [h2]
      rw [hlog]
      have hn2 : n / 2 < n := by omega
      have ih' := ih (n / 2) hn2
      have hexp : (n / 2).log2 + 1 + 2 = ((n / 2).log2 + 2) + 1 := by omega
      rw [hexp, Nat.pow_succ]
      omega
    · have hlog : n.log2 = 0 := by
        rw [Nat.log2_def]; simp [h2]
      rw [hlog]
      have hn2 : n < 2 := by omega
      have h4 : (2 : Nat) ^ (0 + 2) = 4 := by decide
      rw [h4]
      omega

theorem sound_SelectDirectory : SelectDirectory.Goal := by
  intro mask k
  dsimp only
  show (if locate (rsOf mask).toArray k (mask.length.log2 + 2) 0 mask.length < mask.length
        then some (locate (rsOf mask).toArray k (mask.length.log2 + 2) 0 mask.length)
        else none) = (idxs mask)[k]?
  have key : locate (rsOf mask).toArray k (mask.length.log2 + 2) 0 mask.length =
      (idxs mask)[k]?.getD mask.length := by
    cases hidx : (idxs mask)[k]? with
    | some v =>
        rw [Option.getD_some]
        have hklen : k < (idxs mask).length := by
          by_cases hk : k < (idxs mask).length
          · exact hk
          · have hle : (idxs mask).length ≤ k := by omega
            have hnone := (List.getElem?_eq_none_iff).mpr hle
            rw [hidx] at hnone
            simp at hnone
        have hvk : v = (idxs mask)[k] := by
          have hget := List.getElem?_eq_getElem hklen
          rw [hidx] at hget
          exact Option.some.inj hget
        have hvmem : v ∈ idxs mask := by
          rw [hvk]
          exact List.getElem_mem hklen
        have hvlt : v < mask.length := idxs_mem_lt hvmem
        refine locate_correct mask k _ ?_ ?_ ?_ _ _ _ ?_ ?_ ?_ ?_
        · omega
        · intro i hi hcnt
          by_cases hiv : i ≤ v
          · exact hiv
          · exfalso
            have hvi : v < i := by omega
            have hkj : (idxs mask)[k] < i := by rw [← hvk]; exact hvi
            have hge : k + 1 ≤ (idxs mask).countP (· < i) :=
              countP_lt_ge (idxs_pairwise mask) hklen hkj
            have heq := rank_idx mask i hi
            rw [heq] at hcnt
            omega
        · have hvle : v ≤ mask.length := by omega
          have hcnt : (mask.take v).count true = k := by
            rw [rank_idx mask v hvle, hvk]
            exact pairwise_countP_lt (idxs mask) (idxs_pairwise mask) hklen
          omega
        · exact Nat.le_refl _
        · exact Nat.zero_le _
        · omega
        · have hp := pow_log2 mask.length
          exact hp
    | none =>
        rw [Option.getD_none]
        have hlen : (idxs mask).length ≤ k := (List.getElem?_eq_none_iff).mp hidx
        refine locate_correct mask k _ ?_ ?_ ?_ _ _ _ ?_ ?_ ?_ ?_
        · exact Nat.le_refl _
        · intro i hi hcnt; exact hi
        · have hall : ∀ a ∈ idxs mask, a < mask.length := fun a ha => idxs_mem_lt ha
          have hcnt : (mask.take mask.length).count true = (idxs mask).length := by
            rw [rank_idx mask mask.length (Nat.le_refl _)]
            exact countP_all_lt hall
          rw [hcnt]; exact hlen
        · exact Nat.le_refl _
        · exact Nat.zero_le _
        · exact Nat.le_refl _
        · have hp := pow_log2 mask.length
          exact hp
  rw [key]
  cases hidx : (idxs mask)[k]? with
  | some v =>
      rw [Option.getD_some]
      have hklen : k < (idxs mask).length := by
        by_cases hk : k < (idxs mask).length
        · exact hk
        · have hle : (idxs mask).length ≤ k := by omega
          have hnone := (List.getElem?_eq_none_iff).mpr hle
          rw [hidx] at hnone
          simp at hnone
      have hvk : v = (idxs mask)[k] := by
        have hget := List.getElem?_eq_getElem hklen
        rw [hidx] at hget
        exact Option.some.inj hget
      have hvmem : v ∈ idxs mask := by
        rw [hvk]
        exact List.getElem_mem hklen
      have hvlt : v < mask.length := idxs_mem_lt hvmem
      exact ite_eq_left hvlt
  | none =>
      rw [Option.getD_none]
      exact ite_eq_right (Nat.lt_irrefl mask.length)

private def spansFrom (off : Nat) : List Run → List (Nat × Nat × Int)
  | [] => []
  | (n,x)::rs => (off, off+n, x) :: spansFrom (off+n) rs

private def spansAcc (rs : List Run) (off : Nat) (acc : List (Nat × Nat × Int)) :
    Nat × List (Nat × Nat × Int) :=
  rs.foldl (fun (offset,acc) (n,x) => (offset+n,(offset,offset+n,x)::acc)) (off,acc)

private theorem spansAcc_append (rs : List Run) (off : Nat) (acc : List (Nat × Nat × Int)) :
    (spansAcc rs off acc).2 = (spansAcc rs off []).2 ++ acc := by
  induction rs generalizing off acc with
  | nil => rfl
  | cons q rs ih =>
      rcases q with ⟨n,x⟩
      simp only [spansAcc, List.foldl_cons]
      change (spansAcc rs (off+n) ((off,off+n,x)::acc)).2 =
        (spansAcc rs (off+n) [(off,off+n,x)]).2 ++ acc
      rw [ih, ih]
      rw [ih (off+n) [(off,off+n,x)]]
      simp


private theorem spans_eq_spansFrom (rs : List Run) : spans rs = spansFrom 0 rs := by
  unfold spans
  change (spansAcc rs 0 []).2.reverse = spansFrom 0 rs
  induction rs with
  | nil => rfl
  | cons q rs ih =>
      rcases q with ⟨n,x⟩
      simp only [spansAcc, List.foldl_cons]
      change ((spansAcc rs (0+n) [(0,0+n,x)]).2).reverse = _
      rw [spansAcc_append]
      simp only [List.reverse_append, List.reverse_cons, List.reverse_nil, List.nil_append]
      -- need spans from off n
      have hgen : ∀ (rss : List Run) (off : Nat), (spansAcc rss off []).2.reverse = spansFrom off rss := by
        intro rss off
        induction rss generalizing off with
        | nil => rfl
        | cons q rs ih2 =>
            rcases q with ⟨m,y⟩
            simp only [spansAcc, List.foldl_cons]
            change ((spansAcc rs (off+m) [(off,off+m,y)]).2).reverse = _
            rw [spansAcc_append]
            simp only [List.reverse_append, List.reverse_cons, List.reverse_nil, List.nil_append]
            rw [ih2 (off+m)]
            rfl
      rw [hgen rs (0+n)]
      rfl

private theorem spansFrom_add (m : Nat) (rs : List Run) :
    spansFrom m rs = (spansFrom 0 rs).map (fun (l,r,x) => (l+m, r+m, x)) := by
  induction rs generalizing m with
  | nil => rfl
  | cons q rs ih =>
      rcases q with ⟨n,x⟩
      simp only [spansFrom]
      simp only [Nat.zero_add]
      rw [ih (m+n), ih n]
      simp only [List.map_cons, List.map_map]
      congr 1
      · simp [Nat.add_comm, Nat.add_left_comm]
      · apply congrArg (fun f => List.map f (spansFrom 0 rs))
        funext t
        rcases t with ⟨u,v,z⟩
        simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem int_sum_sub {α : Type} (l : List α) (f g : α → Int) :
    (l.map fun x => f x - g x).sum = (l.map f).sum - (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [ih]
      omega

private theorem int_sum_mul_left (l : List Int) (c : Int) :
    (l.map fun x => c * x).sum = c * l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem prefix_replicate (m i : Nat) (y : Int) :
    ((List.replicate m y).take i).sum = ((min i m : Nat) : Int) * y := by
  rw [List.take_replicate]
  induction (min i m) with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih]
      simp [Int.natCast_add, Int.add_mul]
      omega

private theorem prefix_append (xs ys : List Int) (i : Nat) :
    ((xs ++ ys).take i).sum = (xs.take i).sum + (ys.take (i - xs.length)).sum := by
  rw [List.take_append, List.sum_append]

private theorem take_drop_slice (xs : List Int) (l r : Nat) (hlr : l ≤ r) (hr : r ≤ xs.length) :
    ((xs.take r).drop l).sum = (xs.take r).sum - (xs.take l).sum := by
  have ht : (xs.take r).take l = xs.take l := by
    rw [List.take_take, Nat.min_eq_left hlr]
  have happ : xs.take r = (xs.take l) ++ ((xs.take r).drop l) := by
    conv => lhs; rw [← List.take_append_drop l (xs.take r)]
    rw [ht]
  have hsum := congrArg List.sum happ
  rw [List.sum_append] at hsum
  omega

private theorem prefix_unpack_spans (b : List Run) : ∀ i : Nat,
    ((unpack b).take i).sum =
      ((spansFrom 0 b).map fun (u,v,y) => ((min i v - u : Nat) : Int) * y).sum := by
  induction b with
  | nil => simp [unpack, spansFrom]
  | cons q b ih =>
      intro i
      rcases q with ⟨m,y⟩
      simp only [unpack, List.flatMap_cons, spansFrom]
      rw [prefix_append]
      rw [prefix_replicate]
      rw [show (List.replicate m y).length = m by simp]
      change ((min i m : Nat) : Int) * y + ((unpack b).take (i - m)).sum = _
      rw [ih (i - m)]
      simp only [Nat.zero_add]
      rw [spansFrom_add m b]
      simp only [List.map_cons, List.sum_cons, Nat.sub_zero]
      have hshift : ∀ (u v : Nat), min (i - m) v - u = min i (m+v) - (m+u) := by
        intro u v
        by_cases him : m ≤ i
        · omega
        · have : i - m = 0 := by omega
          rw [this]
          have hmin : min i (m+v) = i := by omega
          rw [hmin]
          omega
      congr 1
      rw [List.map_map]
      apply congrArg List.sum
      apply congrArg (fun f => List.map f (spansFrom 0 b))
      funext t
      rcases t with ⟨u,v,z⟩
      simp [hshift u v, Nat.add_comm, Nat.add_assoc, Nat.add_left_comm]

private theorem slice_eq_prefix (xs : List Int) (l r : Nat) (hlr : l ≤ r) :
    ((xs.take r).drop l).sum = (xs.take r).sum - (xs.take l).sum := by
  induction xs generalizing l r with
  | nil => simp
  | cons x xs ih =>
      cases l with
      | zero =>
          simp
      | succ l' =>
          cases r with
          | zero => omega
          | succ r' =>
              have hlr' : l' ≤ r' := by omega
              simp [ih l' r' hlr']
              omega

private theorem span_overlap_eq (l r u v : Nat) :
    (min r v - max l u : Nat) = (min r v - u) - (min l v - u) := by
  omega

private theorem slice_sum_spans (b : List Run) (l r : Nat) (hlr : l ≤ r) :
    (((unpack b).take r).drop l).sum =
      ((spansFrom 0 b).map fun (u,v,y) => ((min r v - max l u : Nat) : Int) * y).sum := by
  rw [slice_eq_prefix (unpack b) l r hlr]
  rw [prefix_unpack_spans b r, prefix_unpack_spans b l]
  rw [← int_sum_sub]
  apply congrArg List.sum
  apply congrArg (fun f => List.map f (spansFrom 0 b))
  funext t
  rcases t with ⟨u,v,y⟩
  simp only
  have hnat := span_overlap_eq l r u v
  have hle : min l v - u ≤ min r v - u := by omega
  rw [hnat, Int.ofNat_sub hle, Int.sub_mul]

private theorem zip_append_drop {α β : Type} (xs ys : List α) (zs : List β) :
    (xs ++ ys).zip zs = xs.zip zs ++ ys.zip (zs.drop xs.length) := by
  induction xs generalizing zs with
  | nil => simp
  | cons x xs ih =>
      cases zs with
      | nil => simp
      | cons z zs =>
          simp [List.zip_cons_cons, ih]


private theorem dot_replicate (n : Nat) (x : Int) (ys : List Int) :
    (((List.replicate n x).zip ys).map fun (p,q) => p*q).sum = x * (ys.take n).sum := by
  induction n generalizing ys with
  | zero => simp
  | succ n ih =>
      cases ys with
      | nil => simp
      | cons y ys =>
          rw [List.replicate_succ, List.zip_cons_cons]
          simp only [List.map_cons, List.sum_cons]
          rw [ih]
          rw [List.take_succ_cons]
          simp only [List.sum_cons]
          rw [Int.mul_add]

private theorem take_drop_shift (ys : List Int) (l u n : Nat) :
    ((ys.take (u+n)).drop (l+n)).sum = (((ys.drop n).take u).drop l).sum := by
  rw [List.drop_take]
  have hsub : u + n - (l + n) = u - l := by omega
  rw [hsub]
  rw [List.drop_take]
  rw [List.drop_drop]
  rw [Nat.add_comm l n]


private theorem dot_unpack_spans (a : List Run) (ys : List Int) :
    (((unpack a).zip ys).map fun (p,q) => p*q).sum =
      ((spansFrom 0 a).map fun (l,r,x) => x * (((ys.take r).drop l).sum)).sum := by
  induction a generalizing ys with
  | nil => simp [unpack, spansFrom]
  | cons q a ih =>
      rcases q with ⟨n,x⟩
      simp only [unpack, List.flatMap_cons, spansFrom]
      rw [zip_append_drop]
      rw [List.map_append, List.sum_append]
      rw [dot_replicate]
      rw [show (List.replicate n x).length = n by simp]
      change x * (ys.take n).sum +
          ((((unpack a).zip (ys.drop n)).map fun (p,q) => p*q).sum) = _
      rw [ih (ys.drop n)]
      simp only [List.map_cons, List.sum_cons, Nat.zero_add, Nat.sub_zero]
      rw [spansFrom_add n a]
      rw [List.map_map]
      congr 1
      apply congrArg List.sum
      apply congrArg (fun f => List.map f (spansFrom 0 a))
      funext t
      rcases t with ⟨l,r,z⟩
      simp only [Function.comp_apply]
      rw [take_drop_shift]

private theorem spansFrom_le {a : List Run} {l r : Nat} {x : Int}
    (h : (l,r,x) ∈ spansFrom 0 a) : l ≤ r := by
  induction a generalizing l r x with
  | nil => simp [spansFrom] at h
  | cons q a ih =>
      rcases q with ⟨n,z⟩
      simp only [spansFrom, Nat.zero_add, List.mem_cons] at h
      rcases h with heq | h
      · cases heq
        omega
      · have hadd := spansFrom_add n a
        rw [hadd] at h
        rcases List.mem_map.mp h with ⟨p, hp, heq⟩
        rcases p with ⟨l',r',x'⟩
        have hle := ih hp
        simp at heq
        omega

theorem sound_OverlapDot : TinylambOpt.ResearchGoals.EncodedBlocks.OverlapDot.Goal := by
  intro a b
  dsimp only
  rw [spans_eq_spansFrom a, spans_eq_spansFrom b]
  let inner := fun (l r : Nat) (x : Int) =>
    ((spansFrom 0 b).map fun (u,v,y) => ((min r v - max l u : Nat) : Int) * x * y).sum
  have hinner : ∀ (l r : Nat) (x : Int), l ≤ r →
      inner l r x = x * (((unpack b).take r).drop l).sum := by
    intro l r x hlr
    dsimp [inner]
    have hs := slice_sum_spans b l r hlr
    rw [hs, ← int_sum_mul_left]
    rw [List.map_map]
    apply congrArg List.sum
    apply congrArg (fun f => List.map f (spansFrom 0 b))
    funext t
    rcases t with ⟨u,v,y⟩
    simp only
    simp [Int.mul_assoc, Int.mul_comm, Int.mul_left_comm]
  change ((spansFrom 0 a).map fun (l,r,x) => inner l r x).sum =
    (((unpack a).zip (unpack b)).map fun (x,y) => x*y).sum
  have hL : ((spansFrom 0 a).map fun (l,r,x) => inner l r x).sum =
      ((spansFrom 0 a).map fun (l,r,x) => x * (((unpack b).take r).drop l).sum).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro t ht
    rcases t with ⟨l,r,x⟩
    simp only
    exact hinner l r x (spansFrom_le ht)
  rw [hL]
  exact (dot_unpack_spans a (unpack b)).symm

private theorem nodup_eraseDups (l : List Int) : l.eraseDups.Nodup := by
  let P : Nat → Prop := fun n => ∀ l : List Int, l.length = n → l.eraseDups.Nodup
  have h : ∀ n, P n := fun n => Nat.strongRecOn n (fun n ih l hl => by
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        refine List.nodup_cons.mpr ⟨?_, ?_⟩
        · intro hm
          have hma : a ∈ as.filter (fun b => ! b == a) := (List.mem_eraseDups).mp hm
          have hf := (List.mem_filter.mp hma).2
          simp at hf
        · apply ih (as.filter (fun b => ! b == a)).length
          · have hl' : as.length + 1 = n := by simpa using hl
            have : as.length < n := by omega
            exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) this
          · rfl)
  exact h l.length l rfl

private theorem nodup_length_eq_of_mem_iff {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l₁ : List α}, l₁.Nodup → ∀ l₂ : List α, l₂.Nodup →
      (∀ a, a ∈ l₁ ↔ a ∈ l₂) → l₁.length = l₂.length := by
  intro l₁ d₁
  induction l₁ with
  | nil =>
      intro l₂ d₂ h
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ d₂ h
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp (@List.mem_cons_self _ a r)
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact absurd rfl hca
          · exact hr
      have hpos : 0 < l₂.length := by
        cases l₂ with
        | nil => exact absurd ha List.not_mem_nil
        | cons b bs => exact Nat.succ_pos _
      have hlen : (l₂.erase a).length + 1 = l₂.length := by
        rw [List.length_erase_of_mem ha]
        exact Nat.sub_add_cancel (Nat.succ_le_of_lt hpos)
      rw [List.length_cons, ih hnd (l₂.erase a) hd hiff, hlen]

private theorem increasing_eq_of_mem_iff {l₁ l₂ : List Nat}
    (h₁ : l₁.Pairwise (· < ·)) (h₂ : l₂.Pairwise (· < ·))
    (hmem : ∀ q, q ∈ l₁ ↔ q ∈ l₂) : l₁ = l₂ := by
  revert l₂
  induction l₁ with
  | nil =>
      intro l₂ h₂ hmem
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((hmem b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ h₂ hmem
      obtain ⟨haft, hrinc⟩ := List.pairwise_cons.mp h₁
      cases l₂ with
      | nil => exact absurd ((hmem a).mp List.mem_cons_self) List.not_mem_nil
      | cons b bs =>
          obtain ⟨hbft, hbinc⟩ := List.pairwise_cons.mp h₂
          have hba : b = a := by
            by_cases hba' : b = a
            · exact hba'
            · exfalso
              rcases Nat.lt_trichotomy b a with hlt | heq | hgt
              · have hbmem : b ∈ a :: r := (hmem b).mpr List.mem_cons_self
                rcases List.mem_cons.mp hbmem with hba'' | hbr
                · exact hba' hba''
                · have hab := haft b hbr
                  omega
              · exact hba' heq
              · have hamem : a ∈ b :: bs := (hmem a).mp List.mem_cons_self
                rcases List.mem_cons.mp hamem with hab' | hbs
                · exact hba' hab'.symm
                · have hbb := hbft a hbs
                  omega
          subst b
          have hmem' : ∀ q, q ∈ r ↔ q ∈ bs := by
            intro q
            have hq : (q = a ∨ q ∈ r) ↔ (q = a ∨ q ∈ bs) := by
              simpa [List.mem_cons] using hmem q
            constructor
            · intro hqr
              have hne : q ≠ a := by
                intro hqa
                subst q
                exact Nat.lt_irrefl a (haft a hqr)
              rcases (hq.mp (Or.inr hqr)) with hqa | hqs
              · exact absurd hqa hne
              · exact hqs
            · intro hqs
              have hne : q ≠ a := by
                intro hqa
                subst q
                exact Nat.lt_irrefl a (hbft a hqs)
              rcases (hq.mpr (Or.inr hqs)) with hqa | hqr
              · exact absurd hqa hne
              · exact hqr
          have htail : r = bs := ih hrinc hbinc hmem'
          rw [htail]

private theorem eraseDups_cons_length (ys : List Int) (x : Int) :
    (x :: ys).eraseDups.length = ys.eraseDups.length + (if x ∈ ys then 0 else 1) := by
  by_cases hxy : x ∈ ys
  · have hlen := nodup_length_eq_of_mem_iff (nodup_eraseDups (x :: ys)) ys.eraseDups
      (nodup_eraseDups ys) (fun z => by
        constructor
        · intro hz
          rcases List.mem_cons.mp ((List.mem_eraseDups).mp hz) with rfl | hzys
          · exact (List.mem_eraseDups).mpr hxy
          · exact (List.mem_eraseDups).mpr hzys
        · intro hz
          apply (List.mem_eraseDups).mpr
          exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hz)))
    rw [hlen, if_pos hxy, Nat.add_zero]
  · have hnody : (x :: ys.eraseDups).Nodup := by
      refine List.nodup_cons.mpr ⟨?_, nodup_eraseDups ys⟩
      intro hm
      exact hxy ((List.mem_eraseDups).mp hm)
    have hlen := nodup_length_eq_of_mem_iff (nodup_eraseDups (x :: ys)) (x :: ys.eraseDups) hnody
      (fun z => by
        constructor
        · intro hz
          have hz' : z = x ∨ z ∈ ys := List.mem_cons.mp ((List.mem_eraseDups).mp hz)
          rcases hz' with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mpr hzys))
        · intro hz
          apply (List.mem_eraseDups).mpr
          rcases List.mem_cons.mp hz with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hzys)))
    rw [hlen, List.length_cons, if_neg hxy]


private def adjEq (l : List Int) : Nat := (l.zip (l.drop 1)).countP (fun p => p.1 == p.2)

private theorem adjEq_cons (x : Int) (ys : List Int) :
    adjEq (x :: ys) = adjEq ys + (if ys.head? == some x then 1 else 0) := by
  cases ys with
  | nil => simp [adjEq]
  | cons y zs =>
      by_cases hxy : x = y
      · subst y
        simp [adjEq, List.countP_cons]
      · have h1 : (x == y) = false := by simp [hxy]
        have hyx : y ≠ x := by intro h; exact hxy h.symm
        have h2 : (y == x) = false := by simpa using hyx
        simp [adjEq, List.countP_cons, h1, h2]

private theorem adjEq_le_length (l : List Int) : adjEq l ≤ l.length := by
  unfold adjEq
  have h := List.countP_le_length (p := fun p : Int × Int => p.1 == p.2)
    (l := l.zip (l.drop 1))
  simp only [List.length_zip, List.length_drop] at h
  omega

private theorem head_le_of_mem_sorted {x y : Int} {ys : List Int}
    (h : (x :: ys).Pairwise (· ≤ ·)) (hy : y ∈ x :: ys) : x ≤ y := by
  obtain ⟨hall, _⟩ := List.pairwise_cons.mp h
  rcases List.mem_cons.mp hy with rfl | hy
  · omega
  · exact hall y hy

private theorem eraseDups_length_sorted (l : List Int) (h : l.Pairwise (· ≤ ·)) :
    l.eraseDups.length = l.length - adjEq l := by
  induction l with
  | nil => simp [adjEq]
  | cons x ys ih =>
      have hys : ys.Pairwise (· ≤ ·) := h.of_cons
      have hxle : ∀ y ∈ ys, x ≤ y := (List.pairwise_cons.mp h).1
      rw [eraseDups_cons_length]
      rw [ih hys]
      rw [adjEq_cons]
      by_cases hxmem : x ∈ ys
      · have hind : (if ys.head? == some x then 1 else 0) = 1 := by
          cases ys with
          | nil => simp at hxmem
          | cons y zs =>
              have hyx : y ≤ x := head_le_of_mem_sorted h.of_cons hxmem
              have hxy : x ≤ y := hxle y (by simp)
              have hyeq : y = x := by omega
              simp [hyeq]
        rw [if_pos hxmem, hind]
        have hb := adjEq_le_length ys
        simp only [List.length_cons]
        omega
      · have hind : (if ys.head? == some x then 1 else 0) = 0 := by
          cases ys with
          | nil => simp
          | cons y zs =>
              by_cases hyx : y == x
              · have hyeq : y = x := by simpa using hyx
                have : x ∈ y :: zs := by simp [hyeq]
                exact absurd this hxmem
              · simp [hyx]
        rw [if_neg hxmem, hind]
        have hb := adjEq_le_length ys
        simp only [List.length_cons]
        omega

private def seam (l1 l2 : List Int) : Nat :=
  match l1.getLast?, l2.head? with
  | some a, some b => if a == b then 1 else 0
  | _, _ => 0

private theorem adjEq_append (l1 l2 : List Int) :
    adjEq (l1 ++ l2) = adjEq l1 + adjEq l2 + seam l1 l2 := by
  induction l1 with
  | nil =>
      cases l2 with
      | nil => simp [adjEq, seam]
      | cons z zs => simp [adjEq, seam]
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases l2 with
          | nil => simp [adjEq, adjEq_cons, seam]
          | cons z zs =>
              rw [show (x :: []) ++ (z :: zs) = x :: z :: zs by rfl]
              rw [adjEq_cons]
              by_cases h : x = z
              · subst z
                simp [adjEq, seam]
              · have hz : z ≠ x := by intro hz; exact h hz.symm
                simp [adjEq, seam, h, hz]
      | cons y ys =>
          rw [show (x :: y :: ys) ++ l2 = x :: ((y :: ys) ++ l2) by rfl]
          rw [adjEq_cons x ((y :: ys) ++ l2)]
          rw [ih]
          rw [adjEq_cons x (y :: ys)]
          have hhead : (((y :: ys) ++ l2).head?) = (y :: ys).head? := by
            simp
          rw [hhead]
          have hlast : (x :: y :: ys).getLast? = (y :: ys).getLast? := by
            rw [List.getLast?_cons]
            cases h : (y :: ys).getLast? with
            | none =>
                exfalso
                exact absurd (List.getLast?_eq_none_iff.mp h) (by simp)
            | some z => simp [h]
          simp [seam, hlast]
          omega

private theorem sublist_flatten_of_mem {p : List Int} {pages : List (List Int)}
    (hp : p ∈ pages) : p.Sublist pages.flatten := by
  induction pages with
  | nil => simp at hp
  | cons q qs ih =>
      simp at hp
      rcases hp with rfl | hp
      · simpa [List.flatten] using (List.Sublist.refl p).append_right qs.flatten
      · simpa [List.flatten] using (List.Sublist.append (List.nil_sublist q) (ih hp))

private theorem cross_le_of_flatten_pairwise {p q : List Int} {qs : List (List Int)}
    (h : (p :: q :: qs).flatten.Pairwise (· ≤ ·)) :
    ∀ a ∈ p, ∀ b ∈ q, a ≤ b := by
  have hpq : List.Pairwise (· ≤ ·) (p ++ (q :: qs).flatten) := by
    simpa [List.flatten] using h
  have hparts := (List.pairwise_append.mp hpq).2.2
  intro a ha b hb
  exact hparts a ha b (by simp [hb])

private theorem seam_flatten {p q : List Int} {ps : List (List Int)}
    (hp : p ≠ []) (hq : q ≠ []) :
    seam p (q :: ps).flatten = if p.getLast? == q.head? then 1 else 0 := by
  cases q with
  | nil => contradiction
  | cons z zs =>
      cases hlast : p.getLast? <;> simp [seam, hlast]

private theorem adjEq_flatten_pages (ps : List (List Int))
    (hall : ∀ p ∈ ps, p ≠ [])
    (hsorted : ps.flatten.Pairwise (· ≤ ·)) :
    adjEq ps.flatten = (ps.map adjEq).sum +
      (ps.zip (ps.drop 1)).countP (fun (a,b) => a.getLast? == b.head?) := by
  induction ps with
  | nil => simp [adjEq]
  | cons p ps ih =>
      cases ps with
      | nil => simp [adjEq, seam]
      | cons q ps =>
          have hparts := List.pairwise_append.mp (by
            simpa [List.flatten] using hsorted :
              List.Pairwise (fun a b : Int => a ≤ b) (p ++ (q :: ps).flatten))
          have hpp : p.Pairwise (· ≤ ·) := hparts.1
          have hrest : (q :: ps).flatten.Pairwise (· ≤ ·) := hparts.2.1
          have ih' := ih (by intro r hr; exact hall r (by simp [hr])) hrest
          have hcr := cross_le_of_flatten_pairwise hsorted
          have hnonp : p ≠ [] := hall p (by simp)
          have hnonq : q ≠ [] := hall q (by simp)
          rw [show (p :: q :: ps).flatten = p ++ (q :: ps).flatten by rfl]
          rw [adjEq_append]
          rw [ih']
          rw [seam_flatten hnonp hnonq]
          simp only [List.drop_cons, List.zip_cons_cons, List.countP_cons]
          by_cases hs : p.getLast? == q.head? <;> simp [hs] <;> omega

private theorem flatten_filter_nonempty (pages : List (List Int)) :
    (pages.filter (fun p => !p.isEmpty)).flatten = pages.flatten := by
  induction pages with
  | nil => rfl
  | cons p ps ih =>
      cases p <;> simp [ih]

private theorem sum_erase_filter_nonempty (pages : List (List Int)) :
    ((pages.filter (fun p => !p.isEmpty)).map fun p => p.eraseDups.length).sum =
      (pages.map fun p => p.eraseDups.length).sum := by
  induction pages with
  | nil => rfl
  | cons p ps ih =>
      cases p <;> simp [ih]

private theorem length_sum_pages (ps : List (List Int)) :
    ps.flatten.length = ((ps.map fun p => p.length).sum) := by
  induction ps with
  | nil => rfl
  | cons p ps ih => simp [ih]

private theorem erase_sum_pages (ps : List (List Int))
    (hall : ∀ p ∈ ps, p ≠ []) (hsorted : ps.flatten.Pairwise (· ≤ ·)) :
    ((ps.map fun p => p.length).sum) =
      ((ps.map fun p => p.eraseDups.length).sum) + (ps.map adjEq).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      have hpne : p ≠ [] := hall p (by simp)
      have hparts := List.pairwise_append.mp (by
        simpa [List.flatten] using hsorted :
          List.Pairwise (fun a b : Int => a ≤ b) (p ++ ps.flatten))
      have hpp : p.Pairwise (· ≤ ·) := hparts.1
      have hrest : ps.flatten.Pairwise (· ≤ ·) := hparts.2.1
      have ih' := ih (by intro r hr; exact hall r (by simp [hr])) hrest
      have hpErase := eraseDups_length_sorted p hpp
      have hb := adjEq_le_length p
      simp only [List.map_cons, List.sum_cons]
      omega

theorem sound_DistinctSeams : TinylambOpt.ResearchGoals.EncodedBlocks.DistinctSeams.Goal := by
  intro pages hpair
  dsimp only
  let ps := pages.filter fun p => !p.isEmpty
  have hfl : ps.flatten = pages.flatten := by
    simpa [ps] using flatten_filter_nonempty pages
  have hsum : (ps.map fun p => p.eraseDups.length).sum =
      (pages.map fun p => p.eraseDups.length).sum := by
    simpa [ps] using sum_erase_filter_nonempty pages
  have hpsSorted : ps.flatten.Pairwise (· ≤ ·) := by
    rw [hfl]
    exact hpair
  have hnon : ∀ p ∈ ps, p ≠ [] := by
    intro p hp
    have hp' := (List.mem_filter.mp hp).2
    intro hnil
    subst p
    simp at hp'
  rw [← hsum, ← hfl]
  change ((ps.map fun p => p.eraseDups.length).sum -
      (ps.zip (ps.drop 1)).countP (fun (a,b) => a.getLast? == b.head?)) =
    ps.flatten.eraseDups.length
  have hAdj := adjEq_flatten_pages ps hnon hpsSorted
  have hGlob := eraseDups_length_sorted ps.flatten hpsSorted
  have hLen := length_sum_pages ps
  have hErase := erase_sum_pages ps hnon hpsSorted
  rw [hGlob, hLen, hErase, hAdj]
  have hb := adjEq_le_length ps.flatten
  omega

end TinylambOpt.ResearchProofs.ExecutionStrategies.EncodedBlocks
