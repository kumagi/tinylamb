import TinylambOpt.ResearchGoals.ExecutionStrategies.PlaneKernels

/-!
  Soundness proofs for the bit-slice kernels of `PlaneKernels`.

  Every word operation here is lane-wise, so it suffices to prove a one-lane
  statement.  The eight-bit lane kernels are verified by `native_decide` and
  lifted to 64-lane words using `pack`'s bit-extraction lemma.
-/

namespace TinylambOpt.ResearchProofs.Wave7PlaneKernels

open TinylambOpt.ResearchGoals.BitPlaneSum (W B pack count)
open TinylambOpt.ResearchGoals.PlaneKernels

/-! ### Extracting bit `i` of `pack f` -/

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

/-! ### ParallelLess -/

private def ltStep (xb yb : B) (lt : Bool) (k : Nat) : Bool :=
  (!xb.getLsbD k && yb.getLsbD k) || ((xb.getLsbD k == yb.getLsbD k) && lt)

private theorem ltStep_fold_eq (xb yb : B) :
    (List.range 8).foldl (ltStep xb yb) false = decide (xb < yb) := by
  revert xb yb
  native_decide

private theorem lessMask_bit (x y : Nat → B) (i : Nat) (hi : i < 64) :
    (lessMask (planes x) (planes y)).getLsbD i = decide (x i < y i) := by
  unfold lessMask
  let stepW : W → Nat → W := fun lt k =>
    (~~~(planes x k) &&& planes y k) ||| (~~~(planes x k ^^^ planes y k) &&& lt)
  have hstep : ∀ (acc : W) (k : Nat),
      (stepW acc k).getLsbD i = ltStep (x i) (y i) (acc.getLsbD i) k := by
    intro acc k
    dsimp [stepW, ltStep]
    rw [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_and,
      BitVec.getLsbD_not, BitVec.getLsbD_not, BitVec.getLsbD_xor]
    dsimp [planes]
    rw [pack_getLsbD (fun j => (x j).getLsbD k) i hi,
      pack_getLsbD (fun j => (y j).getLsbD k) i hi]
    have hbeq : Bool.not (((x i).getLsbD k) ^^ ((y i).getLsbD k)) =
        (((x i).getLsbD k) == ((y i).getLsbD k)) := by
      cases (x i).getLsbD k <;> cases (y i).getLsbD k <;> rfl
    have hnotxor : (decide (i < 64) && Bool.not (((x i).getLsbD k) ^^ ((y i).getLsbD k))) =
        (((x i).getLsbD k) == ((y i).getLsbD k)) := by
      rw [decide_eq_true hi, Bool.true_and, hbeq]
    rw [hnotxor, decide_eq_true hi]
    cases (x i).getLsbD k <;> cases (y i).getLsbD k <;> cases acc.getLsbD i <;> rfl
  have hfold : ∀ (n : Nat) (acc : W),
      ((List.range n).foldl stepW acc).getLsbD i =
        (List.range n).foldl (ltStep (x i) (y i)) (acc.getLsbD i) := by
    intro n
    induction n with
    | zero => intro acc; simp
    | succ n ih =>
        intro acc
        rw [List.range_succ]
        simp only [List.foldl_append, List.foldl_cons, List.foldl_nil]
        rw [hstep, ih acc]
  change (((List.range 8).foldl stepW 0).getLsbD i) = decide (x i < y i)
  rw [hfold 8 0]
  simpa using ltStep_fold_eq (x i) (y i)

theorem ParallelLess_sound : TinylambOpt.ResearchGoals.PlaneKernels.ParallelLess.Goal := by
  intro x y m
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  rw [BitVec.getLsbD_and, BitVec.getLsbD_and, lessMask_bit x y i hi,
    pack_getLsbD (fun i => decide (x i < y i)) i hi]

/-! ### PlaneAdd -/

private def addStepBool (xb yb : B) (st : List Bool × Bool) (k : Nat) : List Bool × Bool :=
  let p := (xb.getLsbD k ^^ yb.getLsbD k)
  (((p ^^ st.2) :: st.1), (xb.getLsbD k && yb.getLsbD k) || (p && st.2))

private theorem addStepBool_fold_eq (xb yb : B) :
    let (rev, c) := (List.range 8).foldl (addStepBool xb yb) ([], false)
    rev.reverse = ((List.range 8).map fun k => (xb + yb).getLsbD k) ∧
    c = decide (256 ≤ xb.toNat + yb.toNat) := by
  revert xb yb
  native_decide

private def addStepW (a b : Planes) (st : List W × W) (k : Nat) : List W × W :=
  let p := a k ^^^ b k
  (((p ^^^ st.2) :: st.1), (a k &&& b k) ||| (p &&& st.2))

private theorem addFold_bit (x y : Nat → B) (i : Nat) (hi : i < 64) :
    ∀ (n : Nat) (out : List W) (c : W),
      let (rev, carry) := (List.range n).foldl (addStepW (planes x) (planes y)) (out, c)
      (rev.map fun w => w.getLsbD i, carry.getLsbD i) =
        (List.range n).foldl (addStepBool (x i) (y i))
          (out.map fun w => w.getLsbD i, c.getLsbD i) := by
  intro n
  induction n with
  | zero => intro out c; rfl
  | succ n ih =>
      intro out c
      rw [List.range_succ]
      simp only [List.foldl_append, List.foldl_cons, List.foldl_nil]
      have ih' : (((List.range n).foldl (addStepW (planes x) (planes y)) (out, c)).1.map
              (fun w => w.getLsbD i),
            ((List.range n).foldl (addStepW (planes x) (planes y)) (out, c)).2.getLsbD i) =
          (List.range n).foldl (addStepBool (x i) (y i))
            (out.map fun w => w.getLsbD i, c.getLsbD i) := by
        simpa using ih out c
      dsimp [addStepW, addStepBool]
      simp only [List.map_cons]
      apply Prod.ext
      · simp only
        have hfst' : ((List.range n).foldl (addStepW (planes x) (planes y)) (out, c)).1.map
              (fun w => w.getLsbD i) =
            ((List.range n).foldl (addStepBool (x i) (y i))
              (out.map fun w => w.getLsbD i, c.getLsbD i)).1 := by
          simpa using congrArg Prod.fst ih'
        have hsnd' : ((List.range n).foldl (addStepW (planes x) (planes y)) (out, c)).2.getLsbD i =
            ((List.range n).foldl (addStepBool (x i) (y i))
              (out.map fun w => w.getLsbD i, c.getLsbD i)).2 := by
          simpa using congrArg Prod.snd ih'
        rw [hfst']
        congr 1
        rw [BitVec.getLsbD_xor, BitVec.getLsbD_xor, hsnd']
        dsimp [planes]
        rw [pack_getLsbD (fun j => (x j).getLsbD n) i hi,
          pack_getLsbD (fun j => (y j).getLsbD n) i hi]
      · simp only
        have hsnd' : ((List.range n).foldl (addStepW (planes x) (planes y)) (out, c)).2.getLsbD i =
            ((List.range n).foldl (addStepBool (x i) (y i))
              (out.map fun w => w.getLsbD i, c.getLsbD i)).2 := by
          simpa using congrArg Prod.snd ih'
        rw [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_and, hsnd']
        dsimp [planes]
        rw [BitVec.getLsbD_xor,
          pack_getLsbD (fun j => (x j).getLsbD n) i hi,
          pack_getLsbD (fun j => (y j).getLsbD n) i hi]

private theorem add_fold_length (a b : Planes) : ∀ (n : Nat) (out : List W) (c : W),
    ((List.range n).foldl (addStepW a b) (out, c)).1.length = out.length + n := by
  intro n
  induction n with
  | zero => intro out c; simp
  | succ n ih =>
      intro out c
      rw [List.range_succ]
      simp only [List.foldl_append, List.foldl_cons, List.foldl_nil]
      dsimp [addStepW]
      rw [ih]
      omega

theorem PlaneAdd_sound : TinylambOpt.ResearchGoals.PlaneKernels.PlaneAdd.Goal := by
  intro x y
  let st := (List.range 8).foldl (addStepW (planes x) (planes y)) ([], 0)
  let bits := st.1.reverse
  let carry := st.2
  have hlenst : st.1.length = 8 := by
    simpa [st] using (add_fold_length (planes x) (planes y) 8 [] 0)
  have hbitslen : bits.length = 8 := by simpa [bits, hlenst]
  have hbitlane : ∀ (i : Nat), i < 64 →
      bits.map (fun w => w.getLsbD i) =
        (List.range 8).map fun k => ((x i) + (y i)).getLsbD k := by
    intro i hi
    have h0 := addFold_bit x y i hi 8 [] 0
    have h : (st.1.map (fun w => w.getLsbD i), carry.getLsbD i) =
        (List.range 8).foldl (addStepBool (x i) (y i)) ([], false) := by
      simpa [st, carry] using h0
    have hfst : st.1.map (fun w => w.getLsbD i) =
        ((List.range 8).foldl (addStepBool (x i) (y i)) ([], false)).1 := by
      exact congrArg Prod.fst h
    have hb : ((List.range 8).foldl (addStepBool (x i) (y i)) ([], false)).1.reverse =
        ((List.range 8).map fun k => ((x i) + (y i)).getLsbD k) ∧
        ((List.range 8).foldl (addStepBool (x i) (y i)) ([], false)).2 =
          decide (256 ≤ (x i).toNat + (y i).toNat) := by
      simpa using (addStepBool_fold_eq (x i) (y i))
    simpa only [bits, List.map_reverse, hfst] using hb.1
  have hcarrylane : ∀ (i : Nat), i < 64 →
      carry.getLsbD i = decide (256 ≤ (x i).toNat + (y i).toNat) := by
    intro i hi
    have h0 := addFold_bit x y i hi 8 [] 0
    have h : (st.1.map (fun w => w.getLsbD i), carry.getLsbD i) =
        (List.range 8).foldl (addStepBool (x i) (y i)) ([], false) := by
      simpa [st, carry] using h0
    have hb : ((List.range 8).foldl (addStepBool (x i) (y i)) ([], false)).1.reverse =
        ((List.range 8).map fun k => ((x i) + (y i)).getLsbD k) ∧
        ((List.range 8).foldl (addStepBool (x i) (y i)) ([], false)).2 =
          decide (256 ≤ (x i).toNat + (y i).toNat) := by
      simpa using (addStepBool_fold_eq (x i) (y i))
    exact (congrArg Prod.snd h).trans hb.2
  constructor
  · change bits = (List.range 8).map fun k => planes (fun i => x i + y i) k
    apply List.ext_getElem?
    intro n
    by_cases hn : n < 8
    · have hnlen : n < bits.length := by simpa [hbitslen] using hn
      have hnlen' : n < ((List.range 8).map fun k => planes (fun i => x i + y i) k).length := by
        simp [hn]
      rw [List.getElem?_eq_getElem hnlen, List.getElem?_eq_getElem hnlen']
      simp only [Option.some.injEq]
      apply BitVec.eq_of_getLsbD_eq
      intro i hi
      have hget := congrArg (fun l : List Bool => l[n]?) (hbitlane i hi)
      have hm1 : n < (bits.map (fun w => w.getLsbD i)).length := by simpa [hbitslen]
      have hm2 : n < ((List.range 8).map fun k => ((x i + y i).getLsbD k)).length := by
        simp [hn]
      rw [List.getElem?_eq_getElem hm1, List.getElem?_eq_getElem hm2] at hget
      simp only [Option.some.injEq] at hget
      have hbit : (bits[n]).getLsbD i = ((x i + y i).getLsbD n) := by
        rw [List.getElem_map, List.getElem_map] at hget
        rw [show (List.range 8)[n] = n from List.getElem_range (by simpa using hn)] at hget
        exact hget
      rw [List.getElem_map]
      simp [hn]
      dsimp [planes]
      rw [pack_getLsbD (fun j => (x j + y j).getLsbD n) i hi]
      exact hbit
    · have hnlen : bits.length ≤ n := by simpa [hbitslen] using hn
      have hnlen' : ((List.range 8).map fun k => planes (fun i => x i + y i) k).length ≤ n := by
        simpa using hn
      rw [List.getElem?_eq_none hnlen, List.getElem?_eq_none hnlen']
  · change carry = pack (fun i => decide (256 ≤ (x i).toNat + (y i).toNat))
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    rw [hcarrylane i hi, pack_getLsbD _ i hi]

/-! ### PlaneDot -/

private theorem sum_map_zero (l : List Nat) : (l.map fun _ => 0).sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih => simpa using ih

private theorem sum_map_add_nat (l : List Nat) (a b : Nat → Nat) :
    (l.map fun k => a k + b k).sum = (l.map a).sum + (l.map b).sum := by
  induction l with
  | nil => simp
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

private theorem prod_expand (xb yb : B) :
    xb.toNat * yb.toNat =
      ((List.range 8).map fun j =>
        ((List.range 8).map fun k =>
          if xb.getLsbD j && yb.getLsbD k then 2^(j+k) else 0).sum).sum := by
  revert xb yb
  native_decide

private theorem dotFold (sel : Nat → Bool) (valX valY : Nat → B) : ∀ n : Nat,
    (((List.range n).filter sel).map fun i => (valX i).toNat * (valY i).toNat).sum =
      ((List.range 8).map fun j =>
        ((List.range 8).map fun k =>
          2^(j+k) * ((List.range n).filter fun i =>
            sel i && (valX i).getLsbD j && (valY i).getLsbD k).length).sum).sum := by
  intro n
  induction n with
  | zero =>
      simp only [List.range_zero, List.filter_nil, List.map_nil, List.sum_nil,
        List.length_nil, Nat.mul_zero]
      rw [sum_map_zero (List.range 8), sum_map_zero (List.range 8)]
  | succ n ih =>
      by_cases hsel : sel n
      · have hT : (((List.range (n+1)).filter sel).map fun i =>
            (valX i).toNat * (valY i).toNat).sum =
            (((List.range n).filter sel).map fun i =>
              (valX i).toNat * (valY i).toNat).sum +
              (valX n).toNat * (valY n).toNat := by
          rw [List.range_succ, List.filter_append, List.filter_cons_of_pos hsel]
          simp [List.map_append, List.sum_append]
        have helem (j k : Nat) :
            2^(j+k) * ((List.range (n+1)).filter fun i =>
                sel i && (valX i).getLsbD j && (valY i).getLsbD k).length =
              2^(j+k) * ((List.range n).filter fun i =>
                sel i && (valX i).getLsbD j && (valY i).getLsbD k).length +
              (if (valX n).getLsbD j && (valY n).getLsbD k then 2^(j+k) else 0) := by
          rw [len_range_succ_filter (fun i =>
            sel i && (valX i).getLsbD j && (valY i).getLsbD k) n]
          by_cases hcond : (valX n).getLsbD j && (valY n).getLsbD k <;>
            simp [hsel, hcond, Nat.mul_add]
        have hU :
            ((List.range 8).map fun j =>
              ((List.range 8).map fun k =>
                2^(j+k) * ((List.range (n+1)).filter fun i =>
                  sel i && (valX i).getLsbD j && (valY i).getLsbD k).length).sum).sum =
            ((List.range 8).map fun j =>
              ((List.range 8).map fun k =>
                2^(j+k) * ((List.range n).filter fun i =>
                  sel i && (valX i).getLsbD j && (valY i).getLsbD k).length).sum).sum +
            ((List.range 8).map fun j =>
              ((List.range 8).map fun k =>
                if (valX n).getLsbD j && (valY n).getLsbD k then 2^(j+k) else 0).sum).sum := by
          rw [show (List.range 8).map (fun j =>
                ((List.range 8).map fun k =>
                  2^(j+k) * ((List.range (n+1)).filter fun i =>
                    sel i && (valX i).getLsbD j && (valY i).getLsbD k).length).sum) =
              (List.range 8).map (fun j =>
                ((List.range 8).map fun k =>
                  2^(j+k) * ((List.range n).filter fun i =>
                    sel i && (valX i).getLsbD j && (valY i).getLsbD k).length +
                  (if (valX n).getLsbD j && (valY n).getLsbD k then 2^(j+k) else 0)).sum) from
                List.map_congr_left (fun j _ =>
                  congrArg List.sum (List.map_congr_left (fun k _ => helem j k)))]
          rw [show (List.range 8).map (fun j =>
                ((List.range 8).map fun k =>
                  2^(j+k) * ((List.range n).filter fun i =>
                    sel i && (valX i).getLsbD j && (valY i).getLsbD k).length +
                  (if (valX n).getLsbD j && (valY n).getLsbD k then 2^(j+k) else 0)).sum) =
              (List.range 8).map (fun j =>
                ((List.range 8).map fun k =>
                  2^(j+k) * ((List.range n).filter fun i =>
                    sel i && (valX i).getLsbD j && (valY i).getLsbD k).length).sum +
                ((List.range 8).map fun k =>
                  if (valX n).getLsbD j && (valY n).getLsbD k then 2^(j+k) else 0).sum) from
                List.map_congr_left (fun j _ => sum_map_add_nat (List.range 8) _ _)]
          exact sum_map_add_nat (List.range 8) _ _
        rw [hT, hU, ih, prod_expand (valX n) (valY n)]
      · have hsf : sel n = false := by
          cases h : sel n <;> simp_all
        have hT : (((List.range (n+1)).filter sel).map fun i =>
            (valX i).toNat * (valY i).toNat).sum =
            (((List.range n).filter sel).map fun i =>
              (valX i).toNat * (valY i).toNat).sum := by
          rw [List.range_succ, List.filter_append, List.filter_cons_of_neg (by simp [hsf])]
          simp
        have helem (j k : Nat) :
            2^(j+k) * ((List.range (n+1)).filter fun i =>
                sel i && (valX i).getLsbD j && (valY i).getLsbD k).length =
              2^(j+k) * ((List.range n).filter fun i =>
                sel i && (valX i).getLsbD j && (valY i).getLsbD k).length := by
          rw [len_range_succ_filter (fun i =>
            sel i && (valX i).getLsbD j && (valY i).getLsbD k) n]
          simp [hsf]
        rw [hT]
        rw [show (List.range 8).map (fun j =>
              ((List.range 8).map fun k =>
                2^(j+k) * ((List.range (n+1)).filter fun i =>
                  sel i && (valX i).getLsbD j && (valY i).getLsbD k).length).sum) =
            (List.range 8).map (fun j =>
              ((List.range 8).map fun k =>
                2^(j+k) * ((List.range n).filter fun i =>
                  sel i && (valX i).getLsbD j && (valY i).getLsbD k).length).sum) from
              List.map_congr_left (fun j _ =>
                congrArg List.sum (List.map_congr_left (fun k _ => helem j k)))]
        exact ih

private theorem count_inter (m : W) (x y : Nat → B) (j k : Nat) :
    count (m &&& planes x j &&& planes y k) =
      ((List.range 64).filter fun i =>
        m.getLsbD i && (x i).getLsbD j && (y i).getLsbD k).length := by
  show ((List.range 64).filter (fun i =>
    (m &&& planes x j &&& planes y k).getLsbD i)).length = _
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  have hi64 : i < 64 := List.mem_range.mp hi
  rw [BitVec.getLsbD_and, BitVec.getLsbD_and]
  dsimp [planes]
  rw [pack_getLsbD (fun t => (x t).getLsbD j) i hi64,
    pack_getLsbD (fun t => (y t).getLsbD k) i hi64]

theorem PlaneDot_sound : TinylambOpt.ResearchGoals.PlaneKernels.PlaneDot.Goal := by
  intro x y m
  have h := dotFold m.getLsbD x y 64
  rw [show ((List.range 8).map fun j =>
        ((List.range 8).map fun k =>
          2^(j+k) * ((List.range 64).filter fun i =>
            m.getLsbD i && (x i).getLsbD j && (y i).getLsbD k).length).sum).sum =
      ((List.range 8).map fun j =>
        ((List.range 8).map fun k =>
          2^(j+k) * count (m &&& planes x j &&& planes y k)).sum).sum from
      congrArg List.sum (List.map_congr_left (fun j _ =>
        congrArg List.sum (List.map_congr_left (fun k _ =>
          congrArg (fun t : Nat => 2^(j+k) * t) (count_inter m x y j k).symm))))] at h
  simpa [lanes] using h.symm

/-! ### MedianOfThree -/

private theorem getLsbD_ge_width {w : Nat} (v : BitVec w) {i : Nat} (h : w ≤ i) :
    v.getLsbD i = false := by
  change v.toNat.testBit i = false
  exact Nat.testBit_lt_two_pow (Nat.lt_of_lt_of_le v.isLt
    (Nat.pow_le_pow_right (by decide : (2 : Nat) > 0) h))

private theorem median3_bool (xb yb zb : B) (ki : Fin 8) :
    let ab := decide (xb < yb)
    let ac := decide (xb < zb)
    let bc := decide (yb < zb)
    let midA := ab ^^ ac
    let midB := (Bool.not ab) ^^ bc
    ((midA && xb.getLsbD ki) || ((Bool.not midA) && midB && yb.getLsbD ki) ||
      ((Bool.not midA) && (Bool.not midB) && zb.getLsbD ki)) =
      (max (min xb yb) (min (max xb yb) zb)).getLsbD ki := by
  revert xb yb zb ki
  native_decide

private theorem median3_bit (x y z : Nat → B) (i k : Nat) (hi : i < 64) :
    (median3 (planes x) (planes y) (planes z) k).getLsbD i =
      (max (min (x i) (y i)) (min (max (x i) (y i)) (z i))).getLsbD k := by
  by_cases hk : k < 8
  · let ki : Fin 8 := ⟨k, hk⟩
    dsimp [median3]
    simp only [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_xor, BitVec.getLsbD_not]
    rw [lessMask_bit x y i hi, lessMask_bit x z i hi, lessMask_bit y z i hi]
    dsimp [planes]
    rw [pack_getLsbD (fun t => (x t).getLsbD k) i hi,
      pack_getLsbD (fun t => (y t).getLsbD k) i hi,
      pack_getLsbD (fun t => (z t).getLsbD k) i hi]
    simp only [decide_eq_true hi, Bool.true_and]
    simpa [ki] using median3_bool (x i) (y i) (z i) ki
  · have hk8 : 8 ≤ k := by omega
    dsimp [median3, planes]
    simp only [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_xor, BitVec.getLsbD_not]
    rw [pack_getLsbD (fun t => (x t).getLsbD k) i hi,
      pack_getLsbD (fun t => (y t).getLsbD k) i hi,
      pack_getLsbD (fun t => (z t).getLsbD k) i hi]
    rw [show (x i).getLsbD k = false from getLsbD_ge_width _ hk8,
      show (y i).getLsbD k = false from getLsbD_ge_width _ hk8,
      show (z i).getLsbD k = false from getLsbD_ge_width _ hk8]
    simp only [decide_eq_true hi, Bool.true_and, Bool.false_and, Bool.and_false,
      Bool.false_or, Bool.or_false]
    exact (getLsbD_ge_width _ hk8).symm

theorem MedianOfThree_sound : TinylambOpt.ResearchGoals.PlaneKernels.MedianOfThree.Goal := by
  intro x y z k
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  rw [median3_bit x y z i k hi]
  dsimp [planes]
  rw [pack_getLsbD (fun t =>
    (max (min (x t) (y t)) (min (max (x t) (y t)) (z t))).getLsbD k) i hi]

/-! ### RunBoundary -/

private def changedBit (vb xb : B) (k : Nat) : Bool :=
  if vb.getLsbD k then Bool.not (xb.getLsbD k) else xb.getLsbD k

private theorem changed_bit (x : Nat → B) (v : B) (i : Nat) (hi : i < 64) :
    (changed (planes x) v).getLsbD i = decide (x i != v) := by
  unfold changed
  have hstep : ∀ (n : Nat) (acc : W),
      ((List.range n).foldl (fun w k =>
        w ||| (if v.getLsbD k then ~~~(planes x k) else planes x k)) acc).getLsbD i =
        (List.range n).foldl (fun w k => w || changedBit v (x i) k) (acc.getLsbD i) := by
    intro n
    induction n with
    | zero => intro acc; rfl
    | succ n ih =>
        intro acc
        rw [List.range_succ]
        simp only [List.foldl_append, List.foldl_cons, List.foldl_nil]
        rw [BitVec.getLsbD_or, ih]
        dsimp [changedBit]
        by_cases hv : v.getLsbD n
        · rw [ite_eq_left hv]
          rw [BitVec.getLsbD_not]
          dsimp [planes]
          rw [pack_getLsbD (fun t => (x t).getLsbD n) i hi]
          simp [hi, hv]
        · rw [ite_eq_right hv]
          dsimp [planes]
          rw [pack_getLsbD (fun t => (x t).getLsbD n) i hi]
          simp [hi, hv]
  rw [hstep 8 0]
  have hfinite : ∀ (xb vb : B),
      (List.range 8).foldl (fun w k => w || changedBit vb xb k) false = decide (xb != vb) := by
    native_decide
  simpa using hfinite (x i) v

private theorem ctz_find_getD (w : W) :
    ((List.range 64).find? w.getLsbD).getD 64 = w.ctz.toNat := by
  by_cases hz : w = 0
  · have hfind : (List.range 64).find? w.getLsbD = none := by
      apply List.find?_range_eq_none.mpr
      intro i hi
      rw [hz]
      simp
    rw [hfind]
    subst w
    native_decide
  · have hset := BitVec.getLsbD_true_ctz_of_ne_zero hz
    have hfind : (List.range 64).find? w.getLsbD = some w.ctz.toNat := by
      apply List.find?_range_eq_some.mpr
      refine ⟨hset, List.mem_range.mpr (BitVec.lt_of_getLsbD hset), ?_⟩
      intro j hj
      rw [Bool.not_eq_eq_eq_not]
      exact (show w.getLsbD j = false from BitVec.getLsbD_false_of_lt_ctz hj)
    rw [hfind]
    rfl

private theorem find?_congr {α : Type} (p q : α → Bool) : ∀ l : List α,
    (∀ x ∈ l, p x = q x) → l.find? p = l.find? q := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a t ih =>
      intro h
      have ha := h a (by simp)
      have hq : q a = p a := ha.symm
      by_cases hp : p a = true
      · rw [List.find?_cons, hp]
        rw [List.find?_cons, hq, hp]
      · have hp' : p a = false := by
          cases hpa : p a with
          | false => rfl
          | true => simp [hpa] at hp
        rw [List.find?_cons, hp']
        rw [List.find?_cons, hq, hp']
        apply ih
        intro x hx
        exact h x (by simp [hx])

theorem RunBoundary_sound : TinylambOpt.ResearchGoals.PlaneKernels.RunBoundary.Goal := by
  intro x v m
  rw [← ctz_find_getD (m &&& changed (planes x) v)]
  have hfind : (List.range 64).find? (m &&& changed (planes x) v).getLsbD =
      (lanes m).find? (fun i => x i != v) := by
    rw [lanes, List.find?_filter]
    apply find?_congr
    intro i hi
    have hi64 : i < 64 := List.mem_range.mp hi
    rw [BitVec.getLsbD_and, changed_bit x v i hi64]
    rfl
  rw [hfind]

/-! ### ArgMinMask -/

private def pref (v : B) : Nat → Nat
  | 0 => 0
  | d+1 => 2 * pref v d + (if v.getLsbD (7-d) then 1 else 0)

private theorem pref_lt_pow (v : B) : ∀ d, pref v d < 2^d := by
  intro d
  induction d with
  | zero => simp [pref]
  | succ d ih =>
      rw [pref, Nat.pow_succ]
      have hb : (if v.getLsbD (7-d) then 1 else 0) < 2 := by
        cases v.getLsbD (7-d) <;> simp
      omega

private theorem pref_8 (v : B) : pref v 8 = v.toNat := by
  revert v
  native_decide

private theorem pref_le_of_succ_le (v w : B) (d : Nat) (h : pref v (d+1) ≤ pref w (d+1)) :
    pref v d ≤ pref w d := by
  rw [pref, pref] at h
  have hbV : (if v.getLsbD (7 - d) then 1 else 0) < 2 := by
    cases v.getLsbD (7-d) <;> simp
  have hbW : (if w.getLsbD (7 - d) then 1 else 0) < 2 := by
    cases w.getLsbD (7-d) <;> simp
  omega

private theorem pref_succ_step (v w : B) (d : Nat)
    (hp : pref v d < pref w d) : pref v (d+1) < pref w (d+1) := by
  rw [pref, pref]
  have hb : (if v.getLsbD (7 - d) then 1 else 0) < 2 := by
    cases v.getLsbD (7-d) <;> simp
  have hle : pref v d + 1 ≤ pref w d := by omega
  omega

private def refLive (x : Nat → B) (m : W) : Nat → W
  | 0 => m
  | d+1 =>
      let prev := refLive x m d
      let k := 7 - d
      let zeros := prev &&& ~~~(planes x k)
      if zeros == 0 then prev else zeros

private theorem minMask_eq_refLive (x : Nat → B) (m : W) :
    minMask m (planes x) = refLive x m 8 := by
  unfold minMask
  have hlist : (List.range 8).reverse = [7,6,5,4,3,2,1,0] := by native_decide
  rw [hlist]
  rfl

private theorem refLive_char (x : Nat → B) (m : W) : ∀ d, d ≤ 8 → ∀ i, i < 64 →
    ((refLive x m d).getLsbD i = true ↔
      m.getLsbD i = true ∧ ∀ j, m.getLsbD j = true → pref (x i) d ≤ pref (x j) d) := by
  intro d hd
  induction d with
  | zero =>
      intro i hi
      simp [refLive, pref]
  | succ d ih =>
      intro i hi
      have hd' : d ≤ 8 := by omega
      have hchar := ih hd'
      let prev := refLive x m d
      let zeros := prev &&& ~~~(planes x (7-d))
      have hzbit : ∀ t, t < 64 → prev.getLsbD t = true →
          zeros.getLsbD t = Bool.not ((x t).getLsbD (7-d)) := by
        intro t ht hpt
        dsimp [zeros, planes]
        rw [BitVec.getLsbD_and, BitVec.getLsbD_not]
        rw [pack_getLsbD (fun t => (x t).getLsbD (7-d)) t ht]
        simp [hpt, ht]
      have hzexpand : ∀ t, t < 64 → zeros.getLsbD t =
          (prev.getLsbD t && Bool.not ((x t).getLsbD (7-d))) := by
        intro t ht
        dsimp [zeros, planes]
        rw [BitVec.getLsbD_and, BitVec.getLsbD_not]
        rw [pack_getLsbD (fun t => (x t).getLsbD (7-d)) t ht]
        simp [ht]
      change (if zeros == 0 then prev else zeros).getLsbD i = true ↔
        m.getLsbD i = true ∧ ∀ j, m.getLsbD j = true → pref (x i) (d+1) ≤ pref (x j) (d+1)
      by_cases hz : zeros == 0
      · have hz0 : zeros = 0 := by simpa using hz
        have hzfalse : ∀ t, zeros.getLsbD t = false := by
          intro t
          rw [hz0]
          simp
        rw [if_pos hz]
        constructor
        · intro hl
          have hci := (hchar i hi).mp hl
          refine ⟨hci.1, ?_⟩
          intro j hj
          have hjlt : j < 64 := BitVec.lt_of_getLsbD hj
          have hpij := hci.2 j hj
          by_cases hlt : pref (x i) d < pref (x j) d
          · exact Nat.le_of_lt (pref_succ_step (x i) (x j) d hlt)
          · have hpeq : pref (x i) d = pref (x j) d := by omega
            have hbi : (x i).getLsbD (7-d) = true := by
              have hzcalc := hzbit i hi hl
              rw [hzfalse i] at hzcalc
              cases hb : (x i).getLsbD (7-d) <;> simp [hb] at hzcalc ⊢
            have hjmin : ∀ t, m.getLsbD t = true → pref (x j) d ≤ pref (x t) d := by
              intro t ht
              rw [← hpeq]
              exact hci.2 t ht
            have hjl : prev.getLsbD j = true := (hchar j hjlt).mpr ⟨hj, hjmin⟩
            have hbj : (x j).getLsbD (7-d) = true := by
              have hzcalc := hzbit j hjlt hjl
              rw [hzfalse j] at hzcalc
              cases hb : (x j).getLsbD (7-d) <;> simp [hb] at hzcalc ⊢
            simp [pref, hpeq, hbi, hbj]
        · intro hm
          apply (hchar i hi).mpr
          refine ⟨hm.1, ?_⟩
          intro j hj
          exact pref_le_of_succ_le (x i) (x j) d (hm.2 j hj)
      · have hnz : zeros ≠ 0 := by
          intro h0
          exact hz (by simpa [h0])
        have hex : ∃ t, t < 64 ∧ zeros.getLsbD t = true := by
          have hset := BitVec.getLsbD_true_ctz_of_ne_zero hnz
          exact ⟨zeros.ctz.toNat, BitVec.lt_of_getLsbD hset, hset⟩
        rw [if_neg hz]
        constructor
        · intro hl
          have hzi0' : (prev.getLsbD i && Bool.not ((x i).getLsbD (7-d))) = true := by
            rw [← hzexpand i hi]
            exact hl
          rcases Bool.and_eq_true_iff.mp hzi0' with ⟨hpl, hnbi⟩
          have hbi : (x i).getLsbD (7-d) = false := by
            cases hb : (x i).getLsbD (7-d) <;> simp [hb] at hnbi ⊢
          have hci := (hchar i hi).mp hpl
          refine ⟨hci.1, ?_⟩
          intro j hj
          have hpij := hci.2 j hj
          have hbj : 0 ≤ (if (x j).getLsbD (7-d) then 1 else 0) := Nat.zero_le _
          simp [pref, hbi]
          omega
        · intro hm
          have hmin_d : ∀ j, m.getLsbD j = true → pref (x i) d ≤ pref (x j) d := by
            intro j hj
            exact pref_le_of_succ_le (x i) (x j) d (hm.2 j hj)
          have hpl : prev.getLsbD i = true := (hchar i hi).mpr ⟨hm.1, hmin_d⟩
          rcases hex with ⟨i0, hi0, hz0⟩
          have hzi0' : (prev.getLsbD i0 && Bool.not ((x i0).getLsbD (7-d))) = true := by
            rw [← hzexpand i0 hi0]
            exact hz0
          rcases Bool.and_eq_true_iff.mp hzi0' with ⟨hpl0, hnbi0⟩
          have hbi0 : (x i0).getLsbD (7-d) = false := by
            cases hb : (x i0).getLsbD (7-d) <;> simp [hb] at hnbi0 ⊢
          have hci0 := (hchar i0 hi0).mp hpl0
          have hp0i : pref (x i0) d ≤ pref (x i) d := hci0.2 i hm.1
          have hpi0 : pref (x i) d ≤ pref (x i0) d := hmin_d i0 hci0.1
          have hpeq : pref (x i) d = pref (x i0) d := by omega
          have hbi : (x i).getLsbD (7-d) = false := by
            cases hb : (x i).getLsbD (7-d)
            · rfl
            · have hle := hm.2 i0 hci0.1
              simp [pref, hpeq, hb, hbi0] at hle
              omega
          rw [hzexpand i hi, hpl, hbi]
          rfl

theorem ArgMinMask_sound : TinylambOpt.ResearchGoals.PlaneKernels.ArgMinMask.Goal := by
  intro x m
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  rw [minMask_eq_refLive]
  rw [pack_getLsbD (fun i => m.getLsbD i && (lanes m).all (fun j => x i ≤ x j)) i hi]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hleft
    have hc := (refLive_char x m 8 (by omega) i hi).mp hleft
    apply Bool.and_eq_true_iff.mpr
    refine ⟨hc.1, List.all_eq_true.mpr ?_⟩
    intro j hj
    rw [lanes, List.mem_filter] at hj
    have hle := hc.2 j hj.2
    simpa [pref_8, BitVec.le_def] using hle
  · intro hright
    have hr := Bool.and_eq_true_iff.mp hright
    apply (refLive_char x m 8 (by omega) i hi).mpr
    refine ⟨hr.1, ?_⟩
    intro j hj
    have hjlt : j < 64 := BitVec.lt_of_getLsbD hj
    have hjmem : j ∈ lanes m := by
      rw [lanes, List.mem_filter]
      exact ⟨List.mem_range.mpr hjlt, hj⟩
    have h := (List.all_eq_true.mp hr.2) j hjmem
    simpa [pref_8, BitVec.le_def] using h

/-! ### RadixSelect -/

private def radixStep (a : Planes) (k : Nat) (st : W × Nat × Nat) : W × Nat × Nat :=
  let zeros := st.1 &&& ~~~(a k)
  let z := count zeros
  if st.2.1 < z then (zeros, st.2.1, st.2.2)
  else (st.1 &&& a k, st.2.1 - z, st.2.2 + 2^k)

private def radixFold (a : Planes) (st : W × Nat × Nat) (ks : List Nat) : W × Nat × Nat :=
  ks.foldl (fun st k => radixStep a k st) st

private def radixN (a : Planes) : Nat → W × Nat × Nat → W × Nat × Nat
  | 0, st => st
  | d+1, st => radixStep a (7-d) (radixN a d st)

private theorem radixFold_reverse_eq_radixN (a : Planes) (st : W × Nat × Nat) :
    radixFold a st (List.range 8).reverse = radixN a 8 st := by
  have hlist : (List.range 8).reverse = [7,6,5,4,3,2,1,0] := by native_decide
  rw [hlist]
  simp [radixFold, radixN]

private theorem forIn_radix_body (a : Planes) : ∀ (ks : List Nat) (st : W × Nat × Nat),
    Id.run (forIn ks st (fun k s =>
      if s.2.1 < count (s.1 &&& ~~~(a k)) then
        pure (ForInStep.yield (s.1 &&& ~~~(a k), s.2.1, s.2.2))
      else pure (ForInStep.yield (s.1 &&& a k, s.2.1 - count (s.1 &&& ~~~(a k)), s.2.2 + 2^k)))) =
    radixFold a st ks := by
  intro ks
  induction ks with
  | nil => intro st; simp [radixFold]
  | cons k rest ih =>
      intro st
      rw [List.forIn_cons]
      by_cases hcond : st.2.1 < count (st.1 &&& ~~~(a k)) <;> simp [hcond, radixStep, radixFold, ih]

private theorem kth_unfold (m : W) (a : Planes) (rank : Nat) (hk : rank < count m) :
    kth m a rank = some (radixN a 8 (m, rank, 0)).2.2 := by
  unfold kth
  simp [Nat.not_le_of_gt hk]
  rw [forIn_radix_body]
  rw [radixFold_reverse_eq_radixN]

private def cnt (p : Nat → Bool) : Nat := ((List.range 64).filter p).length

private theorem filter_or_length_add {α} (p q : α → Bool) (l : List α) :
    (l.filter fun a => p a || q a).length + (l.filter fun a => p a && q a).length =
      (l.filter p).length + (l.filter q).length := by
  induction l with
  | nil => simp
  | cons a t ih =>
      cases hp : p a <;> cases hq : q a <;> simp [hp, hq] at ih ⊢ <;> omega

private theorem cnt_congr {p q : Nat → Bool} (h : ∀ i, i < 64 → p i = q i) : cnt p = cnt q := by
  unfold cnt
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  exact h i (List.mem_range.mp hi)

private theorem cnt_or_add (p q : Nat → Bool) (hdisj : ∀ i, i < 64 → ¬ (p i && q i)) :
    cnt (fun i => p i || q i) = cnt p + cnt q := by
  have h := filter_or_length_add p q (List.range 64)
  have hz : ((List.range 64).filter fun i => p i && q i).length = 0 := by
    apply List.length_eq_zero_iff.mpr
    apply List.filter_eq_nil_iff.mpr
    intro i hi
    exact hdisj i (List.mem_range.mp hi)
  unfold cnt at h ⊢
  omega

private theorem count_word_split (w p : W) :
    count w = count (w &&& p) + count (w &&& ~~~p) := by
  unfold count
  have h := filter_or_length_add (fun i : Nat => w.getLsbD i && p.getLsbD i)
                                  (fun i : Nat => w.getLsbD i && ! p.getLsbD i) (List.range 64)
  have hdisj : ((List.range 64).filter fun i => (w.getLsbD i && p.getLsbD i) && (w.getLsbD i && !p.getLsbD i)) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro i hi
    cases p.getLsbD i <;> simp
  have hleft : (List.range 64).filter (fun i => (w.getLsbD i && p.getLsbD i) || (w.getLsbD i && !p.getLsbD i)) =
      (List.range 64).filter w.getLsbD := by
    apply List.filter_congr
    intro i hi
    cases hp : p.getLsbD i <;> simp [hp]
  have hwp : ((List.range 64).filter fun i => w.getLsbD i && p.getLsbD i).length =
      ((List.range 64).filter fun i => (w &&& p).getLsbD i).length := by
    apply congrArg List.length
    apply List.filter_congr
    intro i hi
    rw [BitVec.getLsbD_and]
  have hwnp : ((List.range 64).filter fun i => w.getLsbD i && ! p.getLsbD i).length =
      ((List.range 64).filter fun i => (w &&& ~~~p).getLsbD i).length := by
    apply congrArg List.length
    apply List.filter_congr
    intro i hi
    have hi64 := List.mem_range.mp hi
    rw [BitVec.getLsbD_and, BitVec.getLsbD_not]
    simp [hi64]
  rw [hleft, hdisj] at h
  simp at h
  rw [hwp, hwnp] at h
  omega

private theorem zeros_count (x : Nat → B) (live m : W) (d p : Nat)
    (hchar : ∀ i, i < 64 → (live.getLsbD i = true ↔ m.getLsbD i = true ∧ pref (x i) d = p)) :
    count (live &&& ~~~(planes x (7-d))) =
      cnt (fun i => m.getLsbD i && decide (pref (x i) d = p) && ! (x i).getLsbD (7-d)) := by
  unfold count cnt
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  have hi64 := List.mem_range.mp hi
  rw [BitVec.getLsbD_and, BitVec.getLsbD_not]
  dsimp [planes]
  rw [pack_getLsbD (fun t => (x t).getLsbD (7-d)) i hi64]
  simp only [decide_eq_true hi64, Bool.true_and]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro ht
    have ht' := Bool.and_eq_true_iff.mp ht
    have hl : live.getLsbD i = true := ht'.1
    have hnb : Bool.not ((x i).getLsbD (7-d)) = true := ht'.2
    have hc := (hchar i hi64).mp hl
    apply Bool.and_eq_true_iff.mpr
    refine ⟨?_, hnb⟩
    apply Bool.and_eq_true_iff.mpr
    exact ⟨hc.1, decide_eq_true hc.2⟩
  · intro ht
    have ht' := Bool.and_eq_true_iff.mp ht
    have ht'' := Bool.and_eq_true_iff.mp ht'.1
    have hm : m.getLsbD i = true := ht''.1
    have hp : decide (pref (x i) d = p) = true := ht''.2
    have hnb : Bool.not ((x i).getLsbD (7-d)) = true := ht'.2
    apply Bool.and_eq_true_iff.mpr
    exact ⟨(hchar i hi64).mpr ⟨hm, of_decide_eq_true hp⟩, hnb⟩

private theorem pow_split (d : Nat) (hd : d < 8) : 2^(8-d) = 2 * 2^(7-d) := by
  have h : 8 - d = (7 - d) + 1 := by omega
  rw [h, Nat.pow_succ]
  omega

private theorem radixN_inv (x : Nat → B) (m : W) (k : Nat) (hk : k < count m) :
    ∀ d, d ≤ 8 →
      ∃ p, let st := radixN (planes x) d (m, k, 0)
        st.2.2 = p * 2^(8-d) ∧
        (∀ i, i < 64 → (st.1.getLsbD i = true ↔ m.getLsbD i = true ∧ pref (x i) d = p)) ∧
        st.2.1 < count st.1 ∧
        k = st.2.1 + cnt (fun i => m.getLsbD i && decide (pref (x i) d < p)) := by
  intro d hd
  induction d with
  | zero =>
      refine ⟨0, ?_⟩
      simp [radixN, cnt, pref]
      exact hk
  | succ d ih =>
      have hd8 : d ≤ 8 := by omega
      have hdlt : d < 8 := by omega
      rcases ih hd8 with ⟨p, hv, hchar, hn, hrank⟩
      let st0 := radixN (planes x) d (m, k, 0)
      let b := 7 - d
      let zeros := st0.1 &&& ~~~(planes x b)
      let z := count zeros
      have hpow := pow_split d hdlt
      have hz_eq : z = cnt (fun i => m.getLsbD i && decide (pref (x i) d = p) && ! (x i).getLsbD b) := by
        dsimp [z, zeros, b, st0]
        simpa using zeros_count x st0.1 m d p hchar
      have hsplit : count st0.1 = z + count (st0.1 &&& planes x b) := by
        rw [count_word_split st0.1 (planes x b)]
        dsimp [z, zeros]
        omega
      change ∃ p, let st := radixStep (planes x) b st0
        st.2.2 = p * 2^(8-(d+1)) ∧
        (∀ i, i < 64 → (st.1.getLsbD i = true ↔ m.getLsbD i = true ∧ pref (x i) (d+1) = p)) ∧
        st.2.1 < count st.1 ∧
        k = st.2.1 + cnt (fun i => m.getLsbD i && decide (pref (x i) (d+1) < p))
      by_cases hbr : st0.2.1 < z
      · simp [radixStep, st0, b, zeros, z, hbr]
        refine ⟨2*p, ?_, ?_, ?_⟩
        · have hv' : (radixN (planes x) d (m, k, 0)).2.2 = (2*p) * 2^(7-d) := by
            rw [hv, hpow]
            simp [Nat.mul_assoc, Nat.mul_left_comm, Nat.mul_comm]
          exact hv'
        · intro i hi
          constructor
          · intro ht
            rcases ht with ⟨hl, _, hnb⟩
            have hc := (hchar i hi).mp hl
            refine ⟨hc.1, ?_⟩
            rw [pref]
            have hnb' : (x i).getLsbD (7-d) = false := by
              dsimp [planes] at hnb
              rw [pack_getLsbD (fun t => (x t).getLsbD (7-d)) i hi] at hnb
              exact hnb
            have hb0 : (x i).getLsbD b = false := by simpa [b] using hnb'
            simp [b, hb0, hc.2]
          · intro ht
            rcases ht with ⟨hm, hp2⟩
            have hpf : 2 * pref (x i) d + (if (x i).getLsbD b then 1 else 0) = 2*p := by
              simpa [pref] using hp2
            have hb0 : (x i).getLsbD b = false := by
              cases hb : (x i).getLsbD b <;> simp [hb] at hpf ⊢ <;> omega
            have hpe : pref (x i) d = p := by
              cases hb : (x i).getLsbD b <;> simp [hb] at hpf ⊢ <;> omega
            have hplanes : (planes x (7-d)).getLsbD i = false := by
              dsimp [planes]
              rw [pack_getLsbD (fun t => (x t).getLsbD (7-d)) i hi]
              simpa [b] using hb0
            exact ⟨(hchar i hi).mpr ⟨hm, hpe⟩, hi, hplanes⟩
        · have hcnt : cnt (fun i => m.getLsbD i && decide (pref (x i) (d+1) < 2*p)) =
              cnt (fun i => m.getLsbD i && decide (pref (x i) d < p)) := by
            apply cnt_congr
            intro i hi
            rw [pref]
            have hpred : (2 * pref (x i) d + (if (x i).getLsbD b then 1 else 0) < 2*p) ↔
                pref (x i) d < p := by
              have hbnd : (if (x i).getLsbD b then 1 else 0) ≤ 1 := by
                cases (x i).getLsbD b <;> simp
              omega
            rw [show decide (2 * pref (x i) d + (if (x i).getLsbD b then 1 else 0) < 2*p) =
                decide (pref (x i) d < p) from Bool.eq_iff_iff.mpr (by simpa using hpred)]
          omega
      · simp [radixStep, st0, b, zeros, z, hbr]
        refine ⟨2*p+1, ?_, ?_, ?_, ?_⟩
        · have hv' : (radixN (planes x) d (m, k, 0)).2.2 + 2^(7-d) = (2*p+1) * 2^(7-d) := by
            rw [hv, hpow]
            simp [Nat.add_mul, Nat.mul_assoc, Nat.mul_left_comm, Nat.mul_comm]
          exact hv'
        · intro i hi
          constructor
          · intro ht
            have hl : (radixN (planes x) d (m, k, 0)).1.getLsbD i = true := ht.1
            have hb1 : (planes x (7-d)).getLsbD i = true := ht.2
            have hc := (hchar i hi).mp hl
            refine ⟨hc.1, ?_⟩
            rw [pref]
            dsimp [planes] at hb1
            rw [pack_getLsbD (fun t => (x t).getLsbD (7-d)) i hi] at hb1
            simp [hb1, hc.2]
          · intro ht
            have hm : m.getLsbD i = true := ht.1
            have hp2 : pref (x i) (d+1) = 2*p+1 := ht.2
            have hpf : 2 * pref (x i) d + (if (x i).getLsbD (7-d) then 1 else 0) = 2*p+1 := by
              simpa [pref] using hp2
            have hb1 : (x i).getLsbD (7-d) = true := by
              cases hb : (x i).getLsbD (7-d) <;> simp [hb] at hpf ⊢ <;> omega
            have hpe : pref (x i) d = p := by
              cases hb : (x i).getLsbD (7-d) <;> simp [hb] at hpf ⊢ <;> omega
            have hl : (radixN (planes x) d (m, k, 0)).1.getLsbD i = true :=
              (hchar i hi).mpr ⟨hm, hpe⟩
            constructor
            · exact hl
            · dsimp [planes]
              rw [pack_getLsbD (fun t => (x t).getLsbD (7-d)) i hi]
              exact hb1
        · have hn' : st0.2.1 < count st0.1 := by simpa [st0] using hn
          have hzle : z ≤ st0.2.1 := by omega
          have hzdef : z = count (st0.1 &&& ~~~(planes x (7-d))) := by rfl
          change st0.2.1 - count (st0.1 &&& ~~~(planes x (7-d))) < count (st0.1 &&& planes x (7-d))
          dsimp [b] at hsplit
          omega
        · have hdisj : ∀ i, i < 64 → ¬ (m.getLsbD i && decide (pref (x i) d < p) && (m.getLsbD i && decide (pref (x i) d = p) && ! (x i).getLsbD b)) := by
            intro i hi ht
            have ht' := Bool.and_eq_true_iff.mp ht
            have ht'' := Bool.and_eq_true_iff.mp ht'.1
            exact (Nat.lt_irrefl p) (by
              have hp_lt : pref (x i) d < p := of_decide_eq_true ht''.2
              have hp_eq : pref (x i) d = p := by
                have ht'' := (Bool.and_eq_true_iff.mp ht'.2).1
                exact of_decide_eq_true (Bool.and_eq_true_iff.mp ht'').2
              omega)
          have hcntor := cnt_or_add
            (fun i => m.getLsbD i && decide (pref (x i) d < p))
            (fun i => m.getLsbD i && decide (pref (x i) d = p) && ! (x i).getLsbD b) hdisj
          have hcntnew : cnt (fun i => m.getLsbD i && decide (pref (x i) (d+1) < 2*p+1)) =
              cnt (fun i => (m.getLsbD i && decide (pref (x i) d < p)) ||
                (m.getLsbD i && decide (pref (x i) d = p) && ! (x i).getLsbD b)) := by
            apply cnt_congr
            intro i hi
            have hmcase : m.getLsbD i = true ∨ m.getLsbD i = false := by cases m.getLsbD i <;> simp
            rcases hmcase with hm | hm
            · simp [hm]
              apply Bool.eq_iff_iff.mpr
              rw [pref]
              by_cases hb : (x i).getLsbD b <;> simp [b, hb] <;> omega
            · simp [hm]
          have hcntsum : cnt (fun i => m.getLsbD i && decide (pref (x i) (d+1) < 2*p+1)) =
              cnt (fun i => m.getLsbD i && decide (pref (x i) d < p)) + z := by
            rw [hcntnew, hcntor, ← hz_eq]
          have hrank' : k = st0.2.1 + cnt (fun i => m.getLsbD i && decide (pref (x i) d < p)) := by
            simpa [st0] using hrank
          have hzdef : z = count (st0.1 &&& ~~~(planes x (7-d))) := by rfl
          change k = st0.2.1 - z + cnt (fun i => m.getLsbD i && decide (pref (x i) (d+1) < 2*p+1))
          omega

private theorem count_char (live m : W) (pred : Nat → Bool)
    (hchar : ∀ i, i < 64 → (live.getLsbD i = true ↔ m.getLsbD i = true ∧ pred i = true)) :
    count live = cnt (fun i => m.getLsbD i && pred i) := by
  unfold count cnt
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  have hi64 := List.mem_range.mp hi
  apply Bool.eq_iff_iff.mpr
  rw [hchar i hi64]
  simp [Bool.and_eq_true_iff]

private theorem cnt_of_lanes (x : Nat → B) (m : W) (pred : Nat → Bool) :
    cnt (fun i => m.getLsbD i && pred ((x i).toNat)) =
      (((lanes m).map fun i => (x i).toNat).filter pred).length := by
  unfold cnt
  rw [List.filter_map]
  simp [lanes, List.filter_filter, Function.comp_def, Bool.and_comm]

private theorem sorted_kth_eq_of_counts : ∀ (ys : List Nat),
    ys.Pairwise (fun a b : Nat => decide (a ≤ b)) →
    ∀ (k p : Nat), k < ys.length →
      (ys.filter fun a => decide (a < p)).length ≤ k →
      k < (ys.filter fun a => decide (a ≤ p)).length →
      ys[k]? = some p := by
  intro ys
  induction ys with
  | nil =>
      intro _ k p hk
      simp at hk
  | cons a t ih =>
      intro hs k p hk hlo hhi
      have hs' := List.pairwise_cons.mp hs
      have hge : ∀ b ∈ t, a ≤ b := by
        intro b hb
        exact of_decide_eq_true (hs'.1 b hb)
      cases k with
      | zero =>
          simp only [List.getElem?_cons_zero, Option.some.injEq]
          have hp_le_a : p ≤ a := by
            by_cases h : p ≤ a
            · exact h
            · have hap : a < p := by omega
              simp [hap] at hlo
          have ha_le_p : a ≤ p := by
            by_cases h : a ≤ p
            · exact h
            · have hpa : p < a := by omega
              have hf : ((a :: t).filter fun b => decide (b ≤ p)) = [] := by
                apply List.filter_eq_nil_iff.mpr
                intro b hb
                have hab : a ≤ b := by
                  cases hb with
                  | head => exact Nat.le_refl a
                  | tail _ hbt => exact hge b hbt
                simp [show ¬ b ≤ p from by omega]
              rw [hf] at hhi
              simp at hhi
          omega
      | succ k =>
          simp only [List.getElem?_cons_succ]
          simp at hk
          have hkt : k < t.length := by omega
          by_cases hap : a < p
          · have hale : a ≤ p := Nat.le_of_lt hap
            have hlot : ((t.filter fun b => decide (b < p)).length) ≤ k := by
              simp [hap] at hlo
              omega
            have hhit : k < (t.filter fun b => decide (b ≤ p)).length := by
              simp [hale] at hhi
              omega
            exact ih hs'.2 k p hkt hlot hhit
          · have haleq : p ≤ a := by omega
            have hlot : ((t.filter fun b => decide (b < p)).length) ≤ k := by
              have hplow : ∀ b ∈ t, ¬ b < p := by
                intro b hb
                have hab := hge b hb
                omega
              have hf : (t.filter fun b => decide (b < p)) = [] := by
                apply List.filter_eq_nil_iff.mpr
                intro b hb
                simp [hplow b hb]
              rw [hf]
              exact Nat.zero_le k
            have hhit : k < (t.filter fun b => decide (b ≤ p)).length := by
              by_cases hpa : p < a
              · have hf : ((a :: t).filter fun b => decide (b ≤ p)) = [] := by
                  apply List.filter_eq_nil_iff.mpr
                  intro b hb
                  have hab : a ≤ b := by
                    cases hb with
                    | head => exact Nat.le_refl a
                    | tail _ hbt => exact hge b hbt
                  simp [show ¬ b ≤ p from by omega]
                rw [hf] at hhi
                simp at hhi
              · have haeq : a = p := by omega
                simp [haeq] at hhi
                omega
            exact ih hs'.2 k p hkt hlot hhit

theorem RadixSelect_sound : TinylambOpt.ResearchGoals.PlaneKernels.RadixSelect.Goal := by
  intro x m k
  by_cases hk : k < count m
  · rcases radixN_inv x m k hk 8 (by omega) with ⟨p, hv, hchar, hn, hrank⟩
    simp at hv
    rw [kth_unfold m (planes x) k hk, hv]
    let ys := (lanes m).map fun i => (x i).toNat
    have hyslen : ys.length = count m := by simp [ys, lanes, count]
    have hkys : k < ys.length := by omega
    have hsorted : (ys.mergeSort).Pairwise (fun a b : Nat => decide (a ≤ b)) := by
      apply List.pairwise_mergeSort
      · intro a b c hab hbc
        have hab' : a ≤ b := of_decide_eq_true hab
        have hbc' : b ≤ c := of_decide_eq_true hbc
        exact decide_eq_true (by omega)
      · intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have hba : b ≤ a := by omega
          simp [h, hba]
    have hrank' : k = (radixN (planes x) 8 (m, k, 0)).2.1 +
        cnt (fun i => m.getLsbD i && decide ((x i).toNat < p)) := by
      simpa [pref_8] using hrank
    have hn' : (radixN (planes x) 8 (m, k, 0)).2.1 < count (radixN (planes x) 8 (m, k, 0)).1 := hn
    have hcntlive : count (radixN (planes x) 8 (m, k, 0)).1 =
        cnt (fun i => m.getLsbD i && decide ((x i).toNat = p)) := by
      apply count_char
      intro i hi
      simpa [pref_8] using hchar i hi
    have hdisj : ∀ i, i < 64 → ¬ (m.getLsbD i && decide ((x i).toNat < p) &&
        (m.getLsbD i && decide ((x i).toNat = p))) := by
      intro i hi ht
      have ht' := Bool.and_eq_true_iff.mp ht
      have ht'' := Bool.and_eq_true_iff.mp ht'.1
      exact (Nat.lt_irrefl p) (by
        have hlt : (x i).toNat < p := of_decide_eq_true ht''.2
        have heq : (x i).toNat = p := of_decide_eq_true (Bool.and_eq_true_iff.mp ht'.2).2
        omega)
    have horadd := cnt_or_add
      (fun i => m.getLsbD i && decide ((x i).toNat < p))
      (fun i => m.getLsbD i && decide ((x i).toNat = p)) hdisj
    have hlecnt : cnt (fun i => m.getLsbD i && decide ((x i).toNat ≤ p)) =
        cnt (fun i => m.getLsbD i && decide ((x i).toNat < p)) +
        cnt (fun i => m.getLsbD i && decide ((x i).toNat = p)) := by
      have hpoint : cnt (fun i => m.getLsbD i && decide ((x i).toNat ≤ p)) =
          cnt (fun i => (m.getLsbD i && decide ((x i).toNat < p)) ||
            (m.getLsbD i && decide ((x i).toNat = p))) := by
        apply cnt_congr
        intro i hi
        by_cases hlt : (x i).toNat < p <;> by_cases heq : (x i).toNat = p <;> simp [hlt, heq] <;> omega
      rw [hpoint, horadd]
    have hlo : (ys.filter fun a => decide (a < p)).length ≤ k := by
      rw [← cnt_of_lanes x m (fun a => decide (a < p))]
      omega
    have hhi_cnt : k < cnt (fun i => m.getLsbD i && decide ((x i).toNat < p)) +
        cnt (fun i => m.getLsbD i && decide ((x i).toNat = p)) := by
      omega
    have hhi : k < (ys.filter fun a => decide (a ≤ p)).length := by
      rw [← cnt_of_lanes x m (fun a => decide (a ≤ p))]
      rw [hlecnt]
      exact hhi_cnt
    have hperm : List.Perm (ys.mergeSort) ys := List.mergeSort_perm ys _
    have hlo_sort : ((ys.mergeSort).filter fun a => decide (a < p)).length ≤ k := by
      have hp := (hperm.filter (fun a => decide (a < p))).length_eq
      omega
    have hhi_sort : k < ((ys.mergeSort).filter fun a => decide (a ≤ p)).length := by
      have hp := (hperm.filter (fun a => decide (a ≤ p))).length_eq
      omega
    have hkval := sorted_kth_eq_of_counts (ys.mergeSort) hsorted k p
      (by simpa using hkys) hlo_sort hhi_sort
    simpa [ys] using hkval.symm
  · have hkge : count m ≤ k := by omega
    unfold kth
    rw [ite_eq_left hkge]
    have hyslen : ((lanes m).map fun i => (x i).toNat).length = count m := by simp [lanes, count]
    have hnone : (((lanes m).map fun i => (x i).toNat).mergeSort)[k]? = none := by
      apply List.getElem?_eq_none
      rw [List.length_mergeSort, hyslen]
      exact hkge
    rw [hnone]
    rfl

/-! ### OccupiedCodes -/

private def prefLow (v : B) : Nat → Nat
  | 0 => 0
  | d+1 => prefLow v d + (if v.getLsbD d then 2^d else 0)

private theorem prefLow_lt_pow (v : B) : ∀ d, d ≤ 8 → prefLow v d < 2^d := by
  intro d hd
  induction d with
  | zero => simp [prefLow]
  | succ d ih =>
      have hd' : d ≤ 8 := by omega
      rw [prefLow, Nat.pow_succ]
      have hbit : (if v.getLsbD d then 2^d else 0) ≤ 2^d := by
        cases v.getLsbD d <;> simp
      have ih' := ih hd'
      omega

private theorem prefLow_8 (v : B) : prefLow v 8 = v.toNat := by
  revert v
  native_decide

private theorem nodup_eraseDups [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: t => by
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · simp
      · exact nodup_eraseDups (t.filter fun b => ! b == a)
termination_by l => l.length
decreasing_by
  simp
  have hle := List.length_filter_le (fun b => ! b == a) t
  omega

private theorem nodup_len_eq_of_mem_iff [BEq α] [LawfulBEq α] : ∀ (s t : List α),
    s.Nodup → t.Nodup → (∀ a, a ∈ s ↔ a ∈ t) → s.length = t.length := by
  intro s
  induction s with
  | nil =>
      intro t hs ht hmem
      have ht0 : t.length = 0 := by
        by_cases h0 : t.length = 0
        · exact h0
        · exfalso
          have hpos : 0 < t.length := Nat.pos_of_ne_zero h0
          have hmemin : t[0] ∈ ([] : List α) := (hmem t[0]).mpr (List.getElem_mem hpos)
          simp at hmemin
      simp [ht0]
  | cons a s' ih =>
      intro t hs ht hmem
      have hsTail := List.nodup_cons.mp hs
      have hnotmem : a ∉ s' := hsTail.1
      have hsnodup : s'.Nodup := hsTail.2
      have hain : a ∈ t := (hmem a).mp (by simp)
      have htperm : List.Perm t (a :: t.erase a) := List.perm_cons_erase hain
      have htlen : t.length = (a :: t.erase a).length := htperm.length_eq
      have htnodup : (t.erase a).Nodup := ht.erase a
      have hmem' : ∀ b, b ∈ s' ↔ b ∈ t.erase a := by
        intro b
        constructor
        · intro hbS
          have hbne : b ≠ a := by
            intro h
            subst b
            exact hnotmem hbS
          have hbT : b ∈ t := (hmem b).mp (by simp [hbS])
          exact (ht.mem_erase_iff (b := a) (a := b)).mpr ⟨hbne, hbT⟩
        · intro hbEr
          have hbneT := (ht.mem_erase_iff (b := a) (a := b)).mp hbEr
          have hbne : b ≠ a := hbneT.1
          have hbT : b ∈ t := hbneT.2
          have hbCons : b ∈ a :: s' := (hmem b).mpr hbT
          rcases List.mem_cons.mp hbCons with hba | hbs
          · exact False.elim (hbne hba)
          · exact hbs
      have ih' := ih (t.erase a) hsnodup htnodup hmem'
      rw [htlen]
      simp [ih']

private theorem eraseDups_length_perm [BEq α] [LawfulBEq α] {l l' : List α}
    (h : List.Perm l l') : l.eraseDups.length = l'.eraseDups.length := by
  apply nodup_len_eq_of_mem_iff _ _ (nodup_eraseDups l) (nodup_eraseDups l')
  intro a
  simp [List.mem_eraseDups, List.Perm.mem_iff h]

private theorem filter_map_of_inj {α β} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β) (hf : ∀ a b, f a = f b ↔ a = b) (a : α) : ∀ l : List α,
    (l.map f).filter (fun b => ! b == f a) = (l.filter fun b => ! b == a).map f := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x
        simpa using ih
      · have hfxa : f x != f a := by
          apply bne_iff_ne.mpr
          intro h
          exact hxa ((hf x a).mp h)
        have hfxane : f x ≠ f a := bne_iff_ne.mp hfxa
        simp only [List.map_cons, List.filter_cons]
        simp [hxa, hfxane, ih]

private theorem eraseDups_length_map_of_inj {α β} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β) (hf : ∀ a b, f a = f b ↔ a = b) : ∀ l : List α,
    (l.map f).eraseDups.length = l.eraseDups.length
  | [] => by simp
  | a :: as => by
      rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons]
      simp only [List.length_cons]
      congr 1
      rw [filter_map_of_inj f hf a as]
      exact eraseDups_length_map_of_inj f hf (as.filter fun b => ! b == a)
termination_by l => l.length
decreasing_by
  simp
  have hle := List.length_filter_le (fun b => ! b == a) as
  omega

private theorem removeAll_eq_self_of_not_mem [BEq α] [LawfulBEq α] {xs ys : List α}
    (h : ∀ x ∈ xs, x ∉ ys) : xs.removeAll ys = xs := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      rw [List.removeAll_cons]
      have hf : (xs.filter fun x => ! x == y) = xs := by
        apply List.filter_eq_self.mpr
        intro x hx
        have hxny : x ≠ y := by
          intro hxy
          subst y
          exact h x hx (by simp)
        simp [hxny]
      rw [hf]
      apply ih
      intro x hx hy
      exact h x hx (by simp [hy])

private theorem eraseDups_append_disjoint (A B : List Nat)
    (h : ∀ a ∈ A, ∀ b ∈ B, a != b) :
    (A ++ B).eraseDups.length = A.eraseDups.length + B.eraseDups.length := by
  rw [List.eraseDups_append]
  have hrm : B.removeAll A = B := by
    apply removeAll_eq_self_of_not_mem
    intro b hb hbA
    have hbb : b != b := h b hbA b hb
    exact (bne_iff_ne.mp hbb) rfl
  rw [hrm]
  simp

private theorem filter_or_append_perm {α} (p q : α → Bool) : ∀ l : List α,
    (∀ a ∈ l, ¬ (p a && q a)) →
    List.Perm (l.filter fun a => p a || q a) (l.filter p ++ l.filter q) := by
  intro l
  induction l with
  | nil => simp
  | cons a t ih =>
      intro hdisj
      have hdt : ∀ b ∈ t, ¬ (p b && q b) := by
        intro b hb
        exact hdisj b (by simp [hb])
      have ih' := ih hdt
      cases hp : p a <;> cases hq : q a
      · simpa [hp, hq] using ih'
      · simpa [hp, hq] using (List.perm_cons_append_cons a ih')
      · simpa [hp, hq] using ih'.cons a
      · exfalso
        exact hdisj a (by simp) (by simp [hp, hq])

private theorem lanes_split (m : W) (a : Planes) (d : Nat) :
    List.Perm (lanes m) (lanes (m &&& a d) ++ lanes (m &&& ~~~(a d))) := by
  unfold lanes
  have hp := filter_or_append_perm
    (fun i : Nat => m.getLsbD i && (a d).getLsbD i)
    (fun i : Nat => m.getLsbD i && ! (a d).getLsbD i) (List.range 64) (by
      intro i hi hai
      have h1 := Bool.and_eq_true_iff.mp hai
      have h2 := Bool.and_eq_true_iff.mp h1.1
      have h3 := Bool.and_eq_true_iff.mp h1.2
      cases hbit : (a d).getLsbD i <;> simp [hbit] at h2 h3 ⊢)
  have hleft : (List.range 64).filter (fun i =>
      (m.getLsbD i && (a d).getLsbD i) || (m.getLsbD i && ! (a d).getLsbD i)) =
      (List.range 64).filter m.getLsbD := by
    apply List.filter_congr
    intro i hi
    cases m.getLsbD i <;> cases (a d).getLsbD i <;> simp
  have hp1 : ((List.range 64).filter fun i => m.getLsbD i && (a d).getLsbD i) =
      (List.range 64).filter (fun i => (m &&& a d).getLsbD i) := by
    apply List.filter_congr
    intro i hi
    rw [BitVec.getLsbD_and]
  have hp0 : ((List.range 64).filter fun i => m.getLsbD i && ! (a d).getLsbD i) =
      (List.range 64).filter (fun i => (m &&& ~~~(a d)).getLsbD i) := by
    apply List.filter_congr
    intro i hi
    have hi64 := List.mem_range.mp hi
    rw [BitVec.getLsbD_and, BitVec.getLsbD_not]
    simp [hi64]
  rw [hleft, hp1, hp0] at hp
  exact hp

private theorem eraseDups_length_const_zero (l : List Nat) (h : ∀ a ∈ l, a = 0) :
    l.eraseDups.length = if l.length = 0 then 0 else 1 := by
  induction l with
  | nil => simp
  | cons a t ih =>
      have ha : a = 0 := h a (by simp)
      subst a
      by_cases ht : t.length = 0
      · have ht' : t = [] := List.length_eq_zero_iff.mp ht
        subst t
        simp [List.eraseDups_cons]
      · have hf : (t.filter fun b => ! b == 0) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro b hb
          have hb0 := h b (by simp [hb])
          subst b
          simp
        rw [List.eraseDups_cons, hf]
        simp [ht]

private theorem occupied_prefix_count (x : Nat → B) : ∀ d, d ≤ 8 → ∀ m : W,
    occupied (planes x) m d = (((lanes m).map fun i => prefLow (x i) d).eraseDups).length := by
  intro d
  induction d with
  | zero =>
      intro hd m
      by_cases hm : (m == 0) = true
      · have hm0 : m = 0 := by simpa using hm
        subst m
        have hl : lanes (0 : W) = [] := by
          unfold lanes
          apply List.filter_eq_nil_iff.mpr
          intro i hi
          simp
        rw [occupied, ite_eq_left hm, hl]
        simp
      · rw [occupied, ite_eq_right hm]
        rw [show (lanes m).map (fun i => prefLow (x i) 0) = (lanes m).map (fun _ => 0) from by
          apply List.map_congr_left
          intro i hi
          simp [prefLow]]
        have hlen := eraseDups_length_const_zero ((lanes m).map fun _ => 0) (by
          intro a ha
          rcases List.mem_map.mp ha with ⟨i, hi, rfl⟩
          rfl)
        have hmzero : (lanes m).length ≠ 0 := by
          intro hzero
          have hl : lanes m = [] := List.length_eq_zero_iff.mp hzero
          have hbits : ∀ i, i < 64 → m.getLsbD i = false := by
            intro i hi
            by_cases hb : m.getLsbD i = true
            · have hin : i ∈ lanes m := by simp [lanes, hi, hb]
              rw [hl] at hin
              simpa using hin
            · cases hbit : m.getLsbD i <;> simp [hbit] at hb ⊢
          have hm0 : m = 0 := by
            apply BitVec.eq_of_getLsbD_eq
            intro i hi
            by_cases hi64 : i < 64
            · simpa using hbits i hi64
            · simpa using getLsbD_ge_width m (Nat.le_of_not_gt hi64)
          exact hm (by simp [hm0])
        have hlenmap : ((lanes m).map fun _ => 0).length = (lanes m).length := by simp
        rw [hlen, hlenmap]
        simp [hmzero]
  | succ d ih =>
      intro hd m
      have hd' : d ≤ 8 := by omega
      rw [occupied]
      by_cases hm : (m == 0) = true
      · have hm0 : m = 0 := by simpa using hm
        subst m
        have hl : lanes (0 : W) = [] := by
          unfold lanes
          apply List.filter_eq_nil_iff.mpr
          intro i hi
          simp
        rw [ite_eq_left hm, hl]
        simp
      · rw [ite_eq_right hm]
        let m1 := m &&& planes x d
        let m0 := m &&& ~~~(planes x d)
        rw [ih hd' m1, ih hd' m0]
        have hmapeq : ((lanes m1).map fun i => prefLow (x i) d + 2^d) =
            ((lanes m1).map fun i => prefLow (x i) d).map (fun p => p + 2^d) := by
          rw [List.map_map]
          rfl
        have hmap1 : (((lanes m1).map fun i => prefLow (x i) d).eraseDups).length =
            (((lanes m1).map fun i => prefLow (x i) d + 2^d).eraseDups).length := by
          symm
          rw [hmapeq]
          exact eraseDups_length_map_of_inj (fun p : Nat => p + 2^d) (by
            intro a b
            constructor <;> intro h <;> omega) ((lanes m1).map fun i => prefLow (x i) d)
        rw [hmap1]
        have hpref1 : ∀ i, m1.getLsbD i = true → prefLow (x i) (d+1) = prefLow (x i) d + 2^d := by
          intro i hi
          have hi64 : i < 64 := BitVec.lt_of_getLsbD hi
          have hbit : (x i).getLsbD d = true := by
            have hb := hi
            dsimp [m1] at hb
            rw [BitVec.getLsbD_and] at hb
            have hb2 := (Bool.and_eq_true_iff.mp hb).2
            dsimp [planes] at hb2
            rw [pack_getLsbD (fun t => (x t).getLsbD d) i hi64] at hb2
            exact hb2
          rw [prefLow]
          simp [hbit]
        have hpref0 : ∀ i, m0.getLsbD i = true → prefLow (x i) (d+1) = prefLow (x i) d := by
          intro i hi
          have hi64 : i < 64 := BitVec.lt_of_getLsbD hi
          have hbit : (x i).getLsbD d = false := by
            have hb := hi
            dsimp [m0] at hb
            rw [BitVec.getLsbD_and, BitVec.getLsbD_not] at hb
            have hb2 := (Bool.and_eq_true_iff.mp hb).2
            simp only [decide_eq_true hi64, Bool.true_and] at hb2
            dsimp [planes] at hb2
            rw [pack_getLsbD (fun t => (x t).getLsbD d) i hi64] at hb2
            cases hbx : (x i).getLsbD d <;> simp [hbx] at hb2 ⊢
          rw [prefLow]
          simp [hbit]
        have hperm := lanes_split m (planes x) d
        have hpermMap : List.Perm ((lanes m).map fun i => prefLow (x i) (d+1))
            (((lanes m1).map fun i => prefLow (x i) d + 2^d) ++
             ((lanes m0).map fun i => prefLow (x i) d)) := by
          have hpm := hperm.map (fun i => prefLow (x i) (d+1))
          rw [List.map_append] at hpm
          refine hpm.trans ?_
          apply List.Perm.append
          · apply List.Perm.of_eq
            apply List.map_congr_left
            intro i hi
            have hiL : m1.getLsbD i = true := by
              rw [lanes, List.mem_filter] at hi
              simpa [m1] using hi.2
            exact hpref1 i hiL
          · apply List.Perm.of_eq
            apply List.map_congr_left
            intro i hi
            have hiL : m0.getLsbD i = true := by
              rw [lanes, List.mem_filter] at hi
              simpa [m0] using hi.2
            exact hpref0 i hiL
        rw [eraseDups_length_perm hpermMap]
        symm
        apply eraseDups_append_disjoint
        intro a ha b hb
        rcases List.mem_map.mp ha with ⟨i, hi, rfl⟩
        rcases List.mem_map.mp hb with ⟨j, hj, rfl⟩
        apply bne_iff_ne.mpr
        intro heq
        have hiLT := prefLow_lt_pow (x i) d hd'
        have hjLT := prefLow_lt_pow (x j) d hd'
        omega

theorem OccupiedCodes_sound : TinylambOpt.ResearchGoals.PlaneKernels.OccupiedCodes.Goal := by
  intro x m
  rw [occupied_prefix_count x 8 (by omega) m]
  have hinj : ∀ a b : B, a.toNat = b.toNat ↔ a = b := by
    intro a b
    exact BitVec.toNat_inj
  rw [show (lanes m).map (fun i => prefLow (x i) 8) = (lanes m).map (fun i => (x i).toNat) from by
    apply List.map_congr_left
    intro i hi
    exact prefLow_8 (x i)]
  rw [show (lanes m).map (fun i => (x i).toNat) = ((lanes m).map x).map BitVec.toNat from by
    simp [List.map_map]]
  exact eraseDups_length_map_of_inj BitVec.toNat hinj ((lanes m).map x)

end TinylambOpt.ResearchProofs.Wave7PlaneKernels

