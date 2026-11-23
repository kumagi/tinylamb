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

-- TODO(OccupiedCodes): the trie-split/distinct-prefix counting invariant needs
-- a permutation-invariant `eraseDups.length` lemma that is not available in Std.
-- TODO(RadixSelect): the imperative radix loop needs a rank/prefix invariant
-- plus a sorted-list kth characterization; not settled here.

end TinylambOpt.ResearchProofs.Wave7PlaneKernels

