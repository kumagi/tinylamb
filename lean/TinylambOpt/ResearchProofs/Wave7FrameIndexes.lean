import TinylambOpt.ResearchGoals.ExecutionStrategies.FrameIndexes

namespace TinylambOpt.ResearchProofs.Wave7FrameIndexes

open TinylambOpt.ResearchGoals.FrameIndexes

private theorem foldl_nullable_aux (xs : List (Option Int)) (n : Nat) (s : Int) :
    xs.foldl (fun (n, s) x => (n + x.isSome.toNat, s + x.getD 0)) (n, s) =
      (n + (xs.filterMap id).length, s + (xs.map fun x => x.getD 0).sum) := by
  induction xs generalizing n s with
  | nil => simp
  | cons x xs ih =>
      cases x <;> simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm,
        Int.add_assoc, Int.add_comm, Int.add_left_comm]

private theorem prefix_pair (xs : List (Option Int)) (i : Nat) (hi : i ≤ xs.length) :
    (sums xs)[i]?.getD (0, 0) =
      (((xs.take i).filterMap id).length, ((xs.take i).map fun x => x.getD 0).sum) := by
  have hg := List.getElem?_scanl
    (f := fun (p : Nat × Int) (x : Option Int) => (p.1 + x.isSome.toNat, p.2 + x.getD 0))
    (a := (0, (0 : Int))) (l := xs) (i := i)
  rw [if_pos hi] at hg
  rw [sums, List.getElem?_toArray, hg]
  simp only [Option.getD_some]
  simpa using foldl_nullable_aux (xs.take i) 0 0

private theorem sum_filterMap_eq_sum_getD (ys : List (Option Int)) :
    (ys.filterMap id).sum = (ys.map fun x => x.getD 0).sum := by
  induction ys with
  | nil => simp
  | cons x xs ih => cases x <;> simp [ih]

theorem PrefixNullableSum_sound : TinylambOpt.ResearchGoals.FrameIndexes.PrefixNullableSum.Goal := by
  intro xs l r hl hr
  let p := sums xs
  let a := p[l]?.getD (0, 0)
  let b := p[r]?.getD (0, 0)
  let vs := (frame xs l r).filterMap id
  have ha : a = (((xs.take l).filterMap id).length, ((xs.take l).map fun x => x.getD 0).sum) := by
    simpa [p, a] using prefix_pair xs l (by omega)
  have hb : b = (((xs.take r).filterMap id).length, ((xs.take r).map fun x => x.getD 0).sum) := by
    simpa [p, b] using prefix_pair xs r hr
  have htake : xs.take r = xs.take l ++ frame xs l r := by
    unfold frame
    conv => lhs; rw [← List.take_append_drop l (xs.take r)]
    simp [List.take_take, Nat.min_eq_left hl, List.drop_take]
  have hcnt : b.1 = a.1 + vs.length := by
    rw [ha, hb, htake]
    simp [vs, List.filterMap_append, List.length_append]
  have hsum : b.2 = a.2 + ((frame xs l r).map (fun x : Option Int => x.getD 0)).sum := by
    rw [ha, hb, htake]
    simp [List.filterMap_append, List.map_append, List.sum_append]
  have hvsum : vs.sum = ((frame xs l r).map (fun x : Option Int => x.getD 0)).sum := by
    simpa [vs] using sum_filterMap_eq_sum_getD (frame xs l r)
  change (if a.1 == b.1 then none else some (b.2 - a.2)) =
    (if vs.isEmpty then none else some vs.sum)
  by_cases heq : a.1 = b.1
  · have hvlen : vs.length = 0 := by omega
    have hnil : vs = [] := by
      by_cases h : vs = []
      · exact h
      · have hp := List.ne_nil_iff_length_pos.mp h
        omega
    have hvempty : vs.isEmpty = true := List.isEmpty_iff.mpr hnil
    simp [heq, hvempty]
  · have hle : a.1 ≤ b.1 := by omega
    have hlt : a.1 < b.1 := Nat.lt_of_le_of_ne hle heq
    have hvpos : 0 < vs.length := by omega
    have hvne : vs.isEmpty = false := by
      cases hv : vs.isEmpty with
      | false => rfl
      | true =>
          have hnil : vs = [] := List.isEmpty_iff.mp hv
          have hz : vs.length = 0 := by rw [hnil]; simp
          omega
    have hbool : (a.1 == b.1) = false := by simp [heq]
    simp [hbool, hvne]
    rw [hvsum, hsum]
    omega

private theorem min?_eq_some_of_ne_nil (l : List Int) (h : l ≠ []) : ∃ m, l.min? = some m := by
  cases l with
  | nil => contradiction
  | cons x xs => simp [List.min?_cons]

private theorem min?_append_getD (l₁ : List Int) (l₂ : List Int) (h₂ : l₂ ≠ []) :
    l₁ ≠ [] → ((l₁ ++ l₂).min?.getD 0) =
      min (l₁.min?.getD 0) (l₂.min?.getD 0) := by
  induction l₁ with
  | nil => simp
  | cons x xs ih =>
      intro h₁
      obtain ⟨m₂, hm₂⟩ := min?_eq_some_of_ne_nil l₂ h₂
      cases xs with
      | nil =>
          simp [List.min?_cons, hm₂]
      | cons y ys =>
          have htail : y :: ys ≠ [] := by simp
          obtain ⟨m₁, hm₁⟩ := min?_eq_some_of_ne_nil (y :: ys) htail
          have iht := ih htail
          simp [List.min?_cons, hm₁, hm₂] at iht ⊢
          omega

private theorem min_blocks (ys : List Int) (w : Nat) (hwpos : 0 < w)
    (hwle : w ≤ ys.length) (hlt : ys.length < 2 * w) :
    min ((ys.take w).min?.getD 0) ((ys.drop (ys.length - w)).min?.getD 0) =
      ys.min?.getD 0 := by
  by_cases hweq : w = ys.length
  · subst w
    simp
  · have hltw : w < ys.length := by omega
    let n := ys.length
    let d := n - w
    let e := 2 * w - n
    have hdpos : 0 < d := by dsimp [d, n]; omega
    have hepos : 0 < e := by dsimp [e, n]; omega
    have hde : d + e = w := by dsimp [d, e, n]; omega
    let A := ys.take d
    let M := (ys.drop d).take e
    let C := ys.drop w
    have hAne : A ≠ [] := by
      apply List.length_pos_iff.mp
      dsimp [A]
      rw [List.length_take, Nat.min_eq_left (by dsimp [d, n]; omega)]
      exact hdpos
    have hdropne : ys.drop d ≠ [] := by
      apply List.length_pos_iff.mp
      rw [List.length_drop]
      dsimp [d, n] at hdpos ⊢
      omega
    have hMne : M ≠ [] := by
      apply List.length_pos_iff.mp
      dsimp [M]
      rw [List.length_take]
      by_cases hle : e ≤ (ys.drop d).length
      · rw [Nat.min_eq_left hle]
        exact hepos
      · rw [Nat.min_eq_right (by omega)]
        exact List.length_pos_iff.mpr hdropne
    have hCne : C ≠ [] := by
      apply List.length_pos_iff.mp
      dsimp [C]
      rw [List.length_drop]
      omega
    have hfirst : ys.take w = A ++ M := by
      dsimp [A, M]
      conv => lhs; rw [← List.take_append_drop d (ys.take w)]
      rw [List.take_take, Nat.min_eq_left (by dsimp [d, n]; omega)]
      rw [List.drop_take]
      rw [show w - d = e by dsimp [d, e, n]; omega]
    have hsecond : ys.drop d = M ++ C := by
      dsimp [M, C]
      conv => lhs; rw [← List.take_append_drop e (ys.drop d)]
      have hdrope : List.drop e (List.drop d ys) = List.drop w ys := by
        rw [List.drop_drop]
        rw [hde]
      rw [hdrope]
    have hwhole : ys = A ++ (M ++ C) := by
      conv => lhs; rw [← List.take_append_drop d ys]
      dsimp [A]
      rw [hsecond]
    have hlen_eq : ys.length - w = d := by dsimp [d, n]
    have hAMne : A ++ M ≠ [] := by simp [hAne]
    have hMCne : M ++ C ≠ [] := by simp [hMne]
    have hAMCne : A ++ (M ++ C) ≠ [] := by simp [hAne]
    rw [hlen_eq, hfirst, hsecond, hwhole]
    rw [min?_append_getD A (M ++ C) hMCne hAne]
    rw [min?_append_getD A M hMne hAne]
    rw [min?_append_getD M C hCne hMne]
    omega

private theorem frame_take_eq (xs : List Int) (l r w : Nat) (hlr : l ≤ r)
    (hr : r ≤ xs.length) (hw : w ≤ r - l) :
    frame xs l (l + w) = (frame xs l r).take w := by
  unfold frame
  rw [List.take_drop]
  rw [List.take_take]
  rw [Nat.min_eq_left (by omega : l + w ≤ r)]

private theorem frame_drop_eq (xs : List Int) (l r w : Nat) (hlr : l ≤ r)
    (hr : r ≤ xs.length) (hw : w ≤ r - l) :
    frame xs (r - w) r = (frame xs l r).drop (r - l - w) := by
  unfold frame
  rw [List.drop_drop]
  congr 1
  omega

theorem SparseMinimum_sound : TinylambOpt.ResearchGoals.FrameIndexes.SparseMinimum.Goal := by
  intro xs l r hlr hr
  let n := r - l
  have hnne : n ≠ 0 := by dsimp [n]; omega
  let w := 2 ^ n.log2
  have hwpos : 0 < w := by dsimp [w]; exact Nat.two_pow_pos _
  have hwle : w ≤ n := by
    dsimp [w]
    exact Nat.log2_self_le hnne
  have hlt : n < 2 * w := by
    have h := Nat.lt_log2_self (n := n)
    simpa [w, Nat.pow_succ, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc] using h
  let ys := frame xs l r
  have hylen : ys.length = n := by
    dsimp [ys, frame]
    rw [List.length_drop, List.length_take]
    rw [Nat.min_eq_left hr]
  have hle : l ≤ r := Nat.le_of_lt hlr
  have h1 : frame xs l (l + w) = ys.take w := frame_take_eq xs l r w hle hr hwle
  have h2 : frame xs (r - w) r = ys.drop (ys.length - w) := by
    rw [frame_drop_eq xs l r w hle hr hwle]
    congr 1
    omega
  change min ((frame xs l (l + w)).min?.getD 0) ((frame xs (r - w) r).min?.getD 0) =
    (frame xs l r).min?.getD 0
  rw [h1, h2]
  exact min_blocks ys w hwpos (by simpa [hylen] using hwle) (by simpa [hylen] using hlt)

private theorem xor_eq_zero_eq (a b : Nat) (h : a ^^^ b = 0) : a = b := by
  apply Nat.eq_iff_testBit_eq.mpr
  intro i
  have hb := congrArg (fun t : Nat => t.testBit i) h
  simp [Nat.testBit_xor] at hb
  exact hb

private theorem take_all_of_length_le {α : Type} (l : List α) {i : Nat} (h : l.length ≤ i) :
    l.take i = l := by
  induction l generalizing i with
  | nil => simp
  | cons x xs ih =>
      cases i with
      | zero => simp at h
      | succ i =>
          change x :: xs.take i = x :: xs
          rw [ih (by simpa using h)]

private theorem frame_append (xs : List String) (l cut r : Nat) (hlc : l ≤ cut) (hcr : cut ≤ r)
    (hr : r ≤ xs.length) :
    frame xs l r = frame xs l cut ++ frame xs cut r := by
  unfold frame
  simp only [List.drop_take]
  have hdrop : List.drop cut xs = List.drop (cut - l) (List.drop l xs) := by
    rw [List.drop_drop]
    congr 1
    omega
  rw [hdrop]
  have hlenl : (List.drop l xs).length = xs.length - l := by simp
  have hcut_le_len : cut - l ≤ (List.drop l xs).length := by
    rw [hlenl]
    omega
  conv => lhs; rw [← List.take_append_drop (cut - l) (List.drop l xs)]
  rw [List.take_append]
  rw [List.length_take, Nat.min_eq_left hcut_le_len]
  rw [take_all_of_length_le (List.take (cut - l) (List.drop l xs)) (i := r - l)
    (by rw [List.length_take, Nat.min_eq_left hcut_le_len]; omega)]
  congr 1
  have hsplit : r - l = (r - cut) + (cut - l) := by omega
  rw [hsplit]
  rw [Nat.add_sub_cancel]

theorem DisjointRangeFold_sound : TinylambOpt.ResearchGoals.FrameIndexes.DisjointRangeFold.Goal := by
  intro xs l r hlr hr
  let y := r - 1
  have hlty : l < y := by dsimp [y]; omega
  let z := l ^^^ y
  let d := z.log2
  let w := 2 ^ d
  let m := 2 * w
  have hz0 : z ≠ 0 := by
    intro hz
    have : l = y := xor_eq_zero_eq l y (by simpa [z] using hz)
    omega
  have hwval : w = 2 ^ d := rfl
  have hmval : m = 2 ^ (d + 1) := by
    dsimp [m, w, d]
    rw [Nat.pow_succ]
    omega
  have hzw : w ≤ z := by
    dsimp [w, d]
    exact Nat.log2_self_le hz0
  have hzm : z < m := by
    rw [hmval]
    dsimp [d]
    exact Nat.lt_log2_self (n := z)
  have hzbit : z.testBit d = true := by
    dsimp [d]
    exact Nat.testBit_log2 hz0
  have hxor_bit : (l.testBit d ^^ y.testBit d) = true := by
    have hb : (l ^^^ y).testBit d = true := by simpa [z] using hzbit
    simpa [Nat.testBit_xor] using hb
  have hdivz : z / m = 0 := Nat.div_eq_of_lt hzm
  have hxor_div : (l / m) ^^^ (y / m) = 0 := by
    rw [hmval]
    rw [← Nat.xor_div_two_pow]
    simpa [m, hmval] using hdivz
  have hquot : l / m = y / m := xor_eq_zero_eq (l / m) (y / m) hxor_div
  let q := l / m
  let a := l % m
  let b := y % m
  have hmpos : 0 < m := by dsimp [m, w]; exact Nat.mul_pos (by decide) (Nat.two_pow_pos d)
  have hltw_m : w < m := by dsimp [m, w]; omega
  have ha_lt : a < m := by dsimp [a]; exact Nat.mod_lt l hmpos
  have hb_lt : b < m := by dsimp [b]; exact Nat.mod_lt y hmpos
  have hl_decomp : l = q * m + a := by
    dsimp [q, a]
    rw [Nat.mul_comm]
    exact (Nat.div_add_mod l m).symm
  have hy_decomp : y = q * m + b := by
    dsimp [q, b]
    rw [hquot]
    rw [Nat.mul_comm]
    exact (Nat.div_add_mod y m).symm
  have hab : a < b := by omega
  have hmodbit_a : a.testBit d = l.testBit d := by
    dsimp [a]
    rw [hmval]
    simpa using Nat.testBit_mod_two_pow l (d + 1) d
  have hmodbit_b : b.testBit d = y.testBit d := by
    dsimp [b]
    rw [hmval]
    simpa using Nat.testBit_mod_two_pow y (d + 1) d
  have ha_bit_false : l.testBit d = false := by
    by_cases hla : l.testBit d = true
    · have hb_false : y.testBit d = false := by
        cases hl : l.testBit d <;> cases hy : y.testBit d <;> simp_all
      have ha_true : a.testBit d = true := by rw [hmodbit_a, hla]
      have hb_test_false : b.testBit d = false := by rw [hmodbit_b, hb_false]
      have ha_ge : w ≤ a := Nat.ge_two_pow_of_testBit ha_true
      have hb_lt_w : b < w := by
        by_cases hbge : w ≤ b
        · have hb_test_true : b.testBit d = true :=
            Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt
              (by simpa [w] using hbge) (by simpa [hmval] using hb_lt)
          simp [hb_test_false] at hb_test_true
        · omega
      omega
    · cases hbit : l.testBit d <;> simp_all
  have hb_bit_true : y.testBit d = true := by
    cases hl : l.testBit d <;> cases hy : y.testBit d <;> simp_all
  have ha_lt_w : a < w := by
    have ha_test_false : a.testBit d = false := by rw [hmodbit_a, ha_bit_false]
    by_cases hage : w ≤ a
    · have ha_test_true : a.testBit d = true :=
        Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt
          (by simpa [w] using hage) (by simpa [hmval] using ha_lt)
      simp [ha_test_false] at ha_test_true
    · omega
  have hb_ge_w : w ≤ b := by
    have hb_test_true : b.testBit d = true := by rw [hmodbit_b, hb_bit_true]
    exact Nat.ge_two_pow_of_testBit hb_test_true
  let cut := l / (2 * w) * (2 * w) + w
  have hcut : cut = q * m + w := by
    dsimp [cut, q, m]
  have hlcut : l < cut := by rw [hl_decomp, hcut]; omega
  have hcutr : cut < r := by
    rw [hcut]
    have hle : q * m + w ≤ y := by
      rw [hy_decomp]
      omega
    dsimp [y] at hle
    omega
  constructor
  · simpa [w, m, d, z, y] using hlcut
  constructor
  · simpa [w, m, d, z, y] using hcutr
  · have happ := frame_append xs l cut r (Nat.le_of_lt hlcut) (by omega) hr
    dsimp [w, d, z, y] at *
    change String.join (frame xs l cut) ++ String.join (frame xs cut r) =
      String.join (frame xs l r)
    rw [← String.join_append, ← happ]

private def sessSteps (gap prev count : Nat) : List Nat → Nat
  | [] => count
  | x :: xs => sessSteps gap x (count + (decide (prev + gap < x)).toNat) xs

private theorem sessSteps_add (gap prev count d : Nat) (xs : List Nat) :
    sessSteps gap prev (count + d) xs = sessSteps gap prev count xs + d := by
  induction xs generalizing count prev with
  | nil => simp [sessSteps]
  | cons x xs ih =>
      simp only [sessSteps]
      rw [show count + d + (decide (prev + gap < x)).toNat =
          (count + (decide (prev + gap < x)).toNat) + d by omega]
      rw [ih]

private theorem sessSteps_eq_count (gap prev count : Nat) (xs : List Nat) :
    sessSteps gap prev count xs =
      count + ((prev :: xs).zip xs).countP (fun p : Nat × Nat => p.1 + gap < p.2) := by
  induction xs generalizing count prev with
  | nil => simp [sessSteps]
  | cons x xs ih =>
      simp only [sessSteps]
      rw [ih]
      simp only [List.countP_cons, List.zip_cons_cons]
      cases h : decide (prev + gap < x) <;> simp [h] at * <;> omega

private theorem sessSteps_append (gap prev count : Nat) (xs b : List Nat) (hb : b ≠ []) :
    sessSteps gap prev count (xs ++ b) =
      sessSteps gap (xs.getLast?.getD prev) (sessSteps gap prev count xs) b := by
  induction xs generalizing count prev with
  | nil => simp [sessSteps]
  | cons x xs ih =>
      simp only [sessSteps, List.getLast?_cons]
      exact ih x (count + (decide (prev + gap < x)).toNat)

theorem SessionBoundary_sound : TinylambOpt.ResearchGoals.FrameIndexes.SessionBoundary.Goal := by
  intro gap a b
  cases a with
  | nil => simp [sessions]
  | cons x xs =>
      cases b with
      | nil => simp [sessions]
      | cons y ys =>
          have hsa : sessions gap (x :: xs) = sessSteps gap x 1 xs := by
            simpa [sessions] using (sessSteps_eq_count gap x 1 xs).symm
          have hsb : sessions gap (y :: ys) = sessSteps gap y 1 ys := by
            simpa [sessions] using (sessSteps_eq_count gap y 1 ys).symm
          have hwhole : sessions gap (x :: (xs ++ (y :: ys))) =
              sessSteps gap x 1 (xs ++ (y :: ys)) := by
            simpa [sessions] using (sessSteps_eq_count gap x 1 (xs ++ (y :: ys))).symm
          have happ : sessions gap ((x :: xs) ++ (y :: ys)) =
              sessSteps gap (xs.getLast?.getD x) (sessions gap (x :: xs)) (y :: ys) := by
            rw [show (x :: xs) ++ (y :: ys) = x :: (xs ++ (y :: ys)) by rfl, hwhole]
            rw [hsa]
            exact sessSteps_append gap x 1 xs (y :: ys) (by simp)
          rw [happ]
          simp only [sessSteps]
          rw [sessSteps_eq_count]
          rw [show sessions gap (y :: ys) = 1 +
              ((y :: ys).zip ys).countP (fun p : Nat × Nat => p.1 + gap < p.2) by rfl]
          simp only [List.getLast?_cons, List.head?_cons]
          have hspos : 1 ≤ sessions gap (x :: xs) := by
            simp [sessions]
          have hbr : (decide (xs.getLast?.getD x + gap < y)).toNat +
              (decide (y ≤ xs.getLast?.getD x + gap)).toNat = 1 := by
            by_cases hlt : xs.getLast?.getD x + gap < y
            · have hnot : ¬ y ≤ xs.getLast?.getD x + gap := by omega
              simp [decide_eq_true hlt, decide_eq_false hnot]
            · have hy : y ≤ xs.getLast?.getD x + gap := by omega
              simp [decide_eq_false hlt, decide_eq_true hy]
          omega

theorem ExcludePeerSelect_sound : TinylambOpt.ResearchGoals.FrameIndexes.ExcludePeerSelect.Goal := by
  intro xs a b k hab hb
  have hal : a ≤ xs.length := by omega
  have hta : (xs.take a).length = a := by
    simp [Nat.min_eq_left hal]
  by_cases hk : k < a
  · simp [hk, hta, List.getElem?_append]
  · have hak : a ≤ k := by omega
    simp [hk, hta, List.getElem?_append]
    congr 1
    omega

private def tbLensFrom (q rem start : Nat) : Nat → List Nat
  | 0 => []
  | d + 1 => tbLensFrom q rem start d ++ [q + (decide (start + d < rem)).toNat]

private def tbLens (q rem : Nat) (m : Nat) : List Nat := tbLensFrom q rem 0 m

private theorem tbLensFrom_length (q rem start s : Nat) : (tbLensFrom q rem start s).length = s := by
  induction s with
  | zero => simp [tbLensFrom]
  | succ s ih => simp [tbLensFrom, ih, Nat.add_assoc]

private theorem tbLens_length (q rem s : Nat) : (tbLens q rem s).length = s :=
  tbLensFrom_length q rem 0 s

private theorem tbLensFrom_succ (q rem start d : Nat) :
    tbLensFrom q rem start (d + 1) =
      tbLensFrom q rem start d ++ [q + (decide (start + d < rem)).toNat] := by
  rfl

private def tbPairs (start : Nat) : List Nat → List (Nat × Nat)
  | [] => []
  | l :: ls => (l, start) :: tbPairs (start + 1) ls

private theorem tbPairs_append (start : Nat) (l₁ l₂ : List Nat) :
    tbPairs start (l₁ ++ l₂) = tbPairs start l₁ ++ tbPairs (start + l₁.length) l₂ := by
  induction l₁ generalizing start with
  | nil => simp [tbPairs]
  | cons l ls ih =>
      rw [List.cons_append]
      simp only [tbPairs, List.length_cons]
      rw [ih]
      simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem sizes_aux_from (q rem start m : Nat) :
    (List.range m).flatMap (fun i =>
      List.replicate (q + (decide (start + i < rem)).toNat) (start + i + 1)) =
      (tbPairs (start + 1) (tbLensFrom q rem start m)).flatMap
        (fun p : Nat × Nat => List.replicate p.1 p.2) := by
  induction m generalizing start with
  | zero => simp [tbLensFrom, tbPairs]
  | succ m ih =>
      rw [List.range_succ, List.flatMap_append, ih (start := start)]
      rw [tbLensFrom_succ, tbPairs_append]
      rw [tbLensFrom_length]
      simp only [List.flatMap_append]
      congr 1
      simp [tbPairs, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem sizes_eq_blocks (n m : Nat) :
    sizes n m = (tbPairs 1 (tbLens (n / m) (n % m) m)).flatMap
      (fun p : Nat × Nat => List.replicate p.1 p.2) := by
  unfold sizes
  simpa [tbLens] using sizes_aux_from (n / m) (n % m) 0 m

private theorem tbLensFrom_take (q rem start s m : Nat) (hs : s ≤ m) :
    (tbLensFrom q rem start m).take s = tbLensFrom q rem start s := by
  induction m with
  | zero => simp at hs; subst s; simp [tbLensFrom]
  | succ m ih =>
      rw [tbLensFrom_succ]
      by_cases hs' : s ≤ m
      · rw [List.take_append_of_le_length]
        · exact ih hs'
        · rw [tbLensFrom_length]
          exact hs'
      · have hs_eq : s = m + 1 := by omega
        subst s
        rw [tbLensFrom_succ]
        rw [List.take_append]
        rw [take_all_of_length_le (tbLensFrom q rem start m) (i := m + 1)
          (by rw [tbLensFrom_length]; omega)]
        rw [tbLensFrom_length]
        rw [show m + 1 - m = 1 by omega]
        simp

private theorem tbLens_take (q rem s m : Nat) (hs : s ≤ m) :
    (tbLens q rem m).take s = tbLens q rem s := by
  simpa [tbLens] using tbLensFrom_take q rem 0 s m hs

private theorem tbLensFrom_drop (q rem start s m : Nat) (hs : s ≤ m) :
    (tbLensFrom q rem start m).drop s = tbLensFrom q rem (start + s) (m - s) := by
  induction m with
  | zero => simp at hs; subst s; simp [tbLensFrom]
  | succ m ih =>
      rw [tbLensFrom_succ]
      by_cases hs' : s ≤ m
      · rw [List.drop_append_of_le_length]
        · rw [ih hs']
          rw [show m + 1 - s = (m - s) + 1 by omega, tbLensFrom_succ]
          have hsm : s + (m - s) = m := Nat.add_sub_of_le hs'
          rw [show start + s + (m - s) = start + m by omega]
        · rw [tbLensFrom_length]
          exact hs'
      · have hs_eq : s = m + 1 := by omega
        subst s
        rw [List.drop_append, tbLensFrom_length]
        rw [show m + 1 - m = 1 by omega]
        rw [List.drop_eq_nil_of_le (i := m + 1) (by rw [tbLensFrom_length]; omega)]
        simp [tbLensFrom]

private theorem tbLens_drop (q rem s m : Nat) (hs : s ≤ m) :
    (tbLens q rem m).drop s = tbLensFrom q rem s (m - s) := by
  simpa [tbLens] using tbLensFrom_drop q rem 0 s m hs

private theorem tbLensFrom_sum (q rem start s : Nat) :
    (tbLensFrom q rem start s).sum = s * q + (min (start + s) rem - min start rem) := by
  induction s with
  | zero => simp [tbLensFrom]
  | succ s ih =>
      rw [tbLensFrom_succ, List.sum_append, List.sum_singleton, ih]
      by_cases hs : start + s < rem
      · have hdec : (decide (start + s < rem)).toNat = 1 := by simp [decide_eq_true hs]
        rw [hdec]
        have hle : min (start + s) rem = start + s := Nat.min_eq_left (by omega)
        have hle' : min (start + (s + 1)) rem = start + s + 1 := by
          rw [show start + (s + 1) = start + s + 1 by omega]
          exact Nat.min_eq_left (by omega)
        rw [hle, hle']
        grind
      · have hdec : (decide (start + s < rem)).toNat = 0 := by simp [decide_eq_false hs]
        rw [hdec]
        have hle : min (start + s) rem = rem := Nat.min_eq_right (by omega)
        have hle' : min (start + (s + 1)) rem = rem := by
          rw [show start + (s + 1) = start + s + 1 by omega]
          exact Nat.min_eq_right (by omega)
        rw [hle, hle']
        grind

private theorem tbLens_sum (q rem s : Nat) : (tbLens q rem s).sum = s * q + min s rem := by
  simpa [tbLens] using tbLensFrom_sum q rem 0 s

private def blockAt (bs : List (Nat × Nat)) (pos : Nat) : Nat → Option Nat
  | i => match bs with
    | [] => none
    | (len, v) :: rest => if i < pos + len then some v else blockAt rest (pos + len) i

private theorem flatMap_blocks_getElem (bs : List (Nat × Nat)) (pos i : Nat) (hpos : pos ≤ i) :
    (bs.flatMap (fun p : Nat × Nat => List.replicate p.1 p.2))[i - pos]? = blockAt bs pos i := by
  induction bs generalizing pos with
  | nil => simp [blockAt]
  | cons b bs ih =>
      rcases b with ⟨len, v⟩
      simp only [List.flatMap_cons, List.getElem?_append, List.getElem?_replicate]
      dsimp [blockAt]
      by_cases hi : i - pos < len
      · have hlt : i < pos + len := by omega
        simp [hi, hlt]
      · have hge : pos + len ≤ i := by omega
        have hidx : i - pos - len = i - (pos + len) := by omega
        have hnlt : ¬ i < pos + len := by omega
        simp [hi, hnlt]
        rw [hidx]
        exact ih hge (pos := pos + len)

private theorem tbLensFrom_cons (q rem start d : Nat) (hd : 0 < d) :
    ∃ rest, tbLensFrom q rem start d = (q + (decide (start < rem)).toNat) :: rest := by
  induction d with
  | zero => omega
  | succ d ih =>
      rw [tbLensFrom_succ]
      cases d with
      | zero => simp [tbLensFrom]
      | succ d =>
          obtain ⟨rest, hr⟩ := ih (by omega)
          rw [hr]
          simp

private theorem blockAt_tbPairs_drop (start pos s i : Nat) (lens : List Nat)
    (h : pos + (lens.take s).sum ≤ i) :
    blockAt (tbPairs start lens) pos i =
      blockAt (tbPairs (start + s) (lens.drop s)) (pos + (lens.take s).sum) i := by
  induction s generalizing lens start pos with
  | zero => simp [tbPairs]
  | succ s ih =>
      cases lens with
      | nil => simp [tbPairs, blockAt]
      | cons l ls =>
          have htake : (l :: ls).take (s + 1) = l :: ls.take s := by simp
          have hfirst : pos + l ≤ i := by
            have hh : pos + (l + (ls.take s).sum) ≤ i := by simpa [htake] using h
            omega
          have hskip : ¬ i < pos + l := by omega
          simp only [tbPairs, blockAt]
          rw [if_neg hskip]
          have h' : (pos + l) + (ls.take s).sum ≤ i := by
            have hh : pos + (l + (ls.take s).sum) ≤ i := by simpa [htake] using h
            omega
          simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using
            ih (start := start + 1) (pos := pos + l) (lens := ls) h'

private theorem blockAt_tbPairs_head (start pos i len : Nat) (hl : pos ≤ i) (hi : i < pos + len) :
    blockAt (tbPairs start (len :: ls)) pos i = some start := by
  simp [tbPairs, blockAt, hi]

theorem TileArithmetic_sound : TinylambOpt.ResearchGoals.FrameIndexes.TileArithmetic.Goal := by
  intro n m i hm hi
  let q := n / m
  let rem := n % m
  let big := rem * (q + 1)
  have hrem : rem < m := by dsimp [rem]; exact Nat.mod_lt n hm
  have hrem_le : rem ≤ m := Nat.le_of_lt hrem
  have hn : q * m + rem = n := by
    dsimp [q, rem]
    rw [Nat.mul_comm]
    exact Nat.div_add_mod n m
  have hbig : big + (m - rem) * q = n := by
    dsimp [big]
    have hrm : rem + (m - rem) = m := Nat.add_sub_of_le hrem_le
    grind
  by_cases hA : i < big
  · let s := i / (q + 1)
    have hq1 : 0 < q + 1 := Nat.succ_pos q
    have hsrem : s < rem := by
      dsimp [s]
      rw [Nat.div_lt_iff_lt_mul hq1]
      simpa [big, Nat.mul_comm] using hA
    have hsle : s ≤ rem := Nat.le_of_lt hsrem
    have hslo : s * (q + 1) ≤ i := by
      have := Nat.div_mul_le_self i (q + 1)
      simpa [s, Nat.mul_comm] using this
    have hshi : i < (s + 1) * (q + 1) := by
      have hlt : i / (q + 1) < s + 1 := by dsimp [s]; omega
      exact Nat.lt_mul_of_div_lt hlt hq1
    have hsumlo : (tbLens q rem s).sum ≤ i := by
      rw [tbLens_sum]
      have hmin : min s rem = s := Nat.min_eq_left hsle
      rw [hmin]
      grind
    have hsumhi0 : i < (tbLens q rem (s + 1)).sum := by
      rw [tbLens_sum]
      have hmin : min (s + 1) rem = s + 1 := Nat.min_eq_left (by omega)
      rw [hmin]
      grind
    have hsum_eq : (tbLens q rem (s + 1)).sum = (tbLens q rem s).sum + (q + 1) := by
      rw [tbLens_sum, tbLens_sum]
      have hmin1 : min (s + 1) rem = s + 1 := Nat.min_eq_left (by omega)
      have hmin0 : min s rem = s := Nat.min_eq_left hsle
      rw [hmin1, hmin0]
      grind
    have hsumhi : i < (tbLens q rem s).sum + (q + 1) := by
      rw [← hsum_eq]
      exact hsumhi0
    have hA' : i < n % m * (n / m + 1) := by simpa [big, q, rem] using hA
    have ht : tile n m i = s + 1 := by
      simp [tile, hA', s, q]
    rw [ht]
    rw [sizes_eq_blocks]
    have hge0 := flatMap_blocks_getElem (tbPairs 1 (tbLens q rem m)) 0 i (by omega)
    simp only [Nat.sub_zero] at hge0
    rw [hge0]
    rw [blockAt_tbPairs_drop (start := 1) (pos := 0) (s := s) (i := i)
      (lens := tbLens q rem m)]
    · rw [tbLens_take q rem s m (by omega)]
      rw [tbLens_drop q rem s m (by omega)]
      simp only [Nat.zero_add]
      change some (s + 1) = blockAt (tbPairs (1 + s) (tbLensFrom q rem s (m - s)))
        ((tbLens q rem s).sum) i
      have hmdpos : 0 < m - s := by omega
      obtain ⟨rest, hrest⟩ := tbLensFrom_cons q rem s (m - s) hmdpos
      rw [hrest]
      have hdec : (decide (s < rem)).toNat = 1 := by simp [decide_eq_true hsrem]
      rw [hdec]
      simp only [tbPairs, blockAt]
      rw [if_pos (by simpa [Nat.add_assoc] using hsumhi)]
      congr 1
      omega
    · rw [tbLens_take q rem s m (by omega)]
      simpa using hsumlo
  · let d := i - big
    have hqpos : 0 < q := by
      by_cases hq0 : q = 0
      · have hn' := hn
        rw [hq0] at hn'
        have hrem_eq : rem = n := by simpa using hn'
        have hbig0 : big = n := by
          dsimp [big]
          rw [hq0]
          simpa [hrem_eq]
        have hnot : ¬ i < n := by simpa [hbig0] using hA
        exact (hnot hi).elim
      · exact Nat.pos_of_ne_zero hq0
    let s := d / q
    have hdle : d < (m - rem) * q := by
      have : big + d = i := Nat.add_sub_of_le (by omega : big ≤ i)
      omega
    have hsbound : s < m - rem := by
      dsimp [s]
      rw [Nat.div_lt_iff_lt_mul hqpos]
      simpa [Nat.mul_comm] using hdle
    have hsloq : s * q ≤ d := by
      have := Nat.div_mul_le_self d q
      simpa [s, Nat.mul_comm] using this
    have hshiq : d < (s + 1) * q := by
      have hlt : d / q < s + 1 := by dsimp [s]; omega
      exact Nat.lt_mul_of_div_lt hlt hqpos
    let t := rem + s
    have htge : rem ≤ t := by change rem ≤ rem + s; exact Nat.le_add_right rem s
    have htnlt : ¬ t < rem := Nat.not_lt_of_ge htge
    have htlo : (tbLens q rem t).sum ≤ i := by
      rw [tbLens_sum]
      have htge : rem ≤ t := by change rem ≤ rem + s; exact Nat.le_add_right rem s
      have hmin : min t rem = rem := Nat.min_eq_right htge
      rw [hmin]
      dsimp [t]
      have hsumd : big + d = i := Nat.add_sub_of_le (by omega : big ≤ i)
      grind
    have hthi0 : i < (tbLens q rem (t + 1)).sum := by
      rw [tbLens_sum]
      have htge : rem ≤ t := by change rem ≤ rem + s; exact Nat.le_add_right rem s
      have htge' : rem ≤ t + 1 := by omega
      have hmin : min (t + 1) rem = rem := Nat.min_eq_right htge'
      rw [hmin]
      dsimp [t] at *
      have hsumd : big + d = i := Nat.add_sub_of_le (by omega : big ≤ i)
      grind
    have hsum_eq_t : (tbLens q rem (t + 1)).sum = (tbLens q rem t).sum + q := by
      rw [tbLens_sum, tbLens_sum]
      have hmin1 : min (t + 1) rem = rem := Nat.min_eq_right (by omega)
      rw [hmin1]
      have hmin0 : min t rem = rem := Nat.min_eq_right htge
      rw [hmin0]
      grind
    have hthi : i < (tbLens q rem t).sum + q := by
      rw [← hsum_eq_t]
      exact hthi0
    have ht_m : t + 1 ≤ m := by dsimp [t]; omega
    have hA' : ¬ i < n % m * (n / m + 1) := by simpa [big, q, rem] using hA
    have ht_tile : tile n m i = t + 1 := by
      simp [tile, hA', t, s, d, q, rem, big]
    rw [ht_tile]
    rw [sizes_eq_blocks]
    have hge0 := flatMap_blocks_getElem (tbPairs 1 (tbLens q rem m)) 0 i (by omega)
    simp only [Nat.sub_zero] at hge0
    rw [hge0]
    rw [blockAt_tbPairs_drop (start := 1) (pos := 0) (s := t) (i := i)
      (lens := tbLens q rem m)]
    · rw [tbLens_take q rem t m (by omega)]
      rw [tbLens_drop q rem t m (by omega)]
      simp only [Nat.zero_add]
      change some (t + 1) = blockAt (tbPairs (1 + t) (tbLensFrom q rem t (m - t)))
        ((tbLens q rem t).sum) i
      have hmdpos : 0 < m - t := by omega
      obtain ⟨rest, hrest⟩ := tbLensFrom_cons q rem t (m - t) hmdpos
      rw [hrest]
      have hdec : (decide (t < rem)).toNat = 0 := by
        simp [decide_eq_false htnlt]
      rw [hdec]
      simp only [tbPairs, blockAt]
      rw [if_pos (by simpa [Nat.add_assoc] using hthi)]
      congr 1
      omega
    · rw [tbLens_take q rem t m (by omega)]
      simpa using htlo

private def rankRunsFrom (n : Nat) : List (Int × Nat) → List (Nat × Nat)
  | [] => []
  | (_, m) :: rs => List.replicate m (n + 1, n + m) ++ rankRunsFrom (n + m) rs

private theorem rankRunsFrom_foldl (runs : List (Int × Nat)) (n : Nat)
    (out : List (List (Nat × Nat))) :
    (runs.foldl (fun (p : Nat × List (List (Nat × Nat))) (e : Int × Nat) =>
      (p.1 + e.2, List.replicate e.2 (p.1 + 1, p.1 + e.2) :: p.2)) (n, out)).2 =
      (runs.foldl (fun (p : Nat × List (List (Nat × Nat))) (e : Int × Nat) =>
        (p.1 + e.2, List.replicate e.2 (p.1 + 1, p.1 + e.2) :: p.2)) (n, [])).2 ++ out := by
  induction runs generalizing n out with
  | nil => simp
  | cons e rs ih =>
      rcases e with ⟨k, m⟩
      simp only [List.foldl_cons]
      rw [ih]
      simp [ih (n := n + m) (out := [List.replicate m (n + 1, n + m)])]

private theorem rankRuns_eq_from (runs : List (Int × Nat)) :
    rankRuns runs = rankRunsFrom 0 runs := by
  unfold rankRuns
  have aux : ∀ (rs : List (Int × Nat)) (n : Nat),
      (rs.foldl (fun (p : Nat × List (List (Nat × Nat))) (e : Int × Nat) =>
        (p.1 + e.2, List.replicate e.2 (p.1 + 1, p.1 + e.2) :: p.2)) (n, [])).2.reverse.flatten =
        rankRunsFrom n rs := by
    intro rs
    induction rs with
    | nil => intro n; simp [rankRunsFrom]
    | cons e rs ih =>
        rcases e with ⟨k, m⟩
        intro n
        rw [rankRunsFrom]
        simp only [List.foldl_cons]
        rw [rankRunsFrom_foldl rs (n + m) [List.replicate m (n + 1, n + m)]]
        rw [List.reverse_append]
        simp [ih (n + m)]
  exact aux runs 0

private theorem length_filter_replicate {α : Type} (m : Nat) (a : α) (p : α → Bool) :
    ((List.replicate m a).filter p).length = if p a then m else 0 := by
  induction m with
  | zero => simp
  | succ m ih =>
      by_cases h : p a = true <;> simp [List.filter_cons, h, ih]

private theorem filter_count_replicate_append
    (m : Nat) (a x : Int) (ys : List Int) :
    ((List.replicate m a ++ ys).filter fun y => y < x).length =
      (if a < x then m else 0) + ((ys.filter fun y => y < x).length) ∧
    ((List.replicate m a ++ ys).filter fun y => y ≤ x).length =
      (if a ≤ x then m else 0) + ((ys.filter fun y => y ≤ x).length) := by
  simp [List.filter_append, length_filter_replicate]

private theorem filter_eq_nil_of_all_gt (k : Int) (ys : List Int)
    (h : ∀ y ∈ ys, k < y) :
    ys.filter (fun y : Int => y < k) = [] ∧
      ys.filter (fun y : Int => y ≤ k) = [] := by
  constructor
  · apply List.filter_eq_nil_iff.mpr
    intro y hy
    simp [decide_eq_false (show ¬ y < k by have hygt := h y hy; omega)]
  · apply List.filter_eq_nil_iff.mpr
    intro y hy
    simp [decide_eq_false (show ¬ y ≤ k by have hygt := h y hy; omega)]

private theorem mem_flatMap_replicate_key (rs : List (Int × Nat)) {y : Int} :
    y ∈ rs.flatMap (fun (j, m) => List.replicate m j) →
      ∃ j m, (j, m) ∈ rs ∧ y = j := by
  induction rs with
  | nil => simp
  | cons e rs ih =>
      rcases e with ⟨j, m⟩
      simp only [List.flatMap_cons, List.mem_append]
      intro hy
      rcases hy with hy | hy
      · have heq : y = j := (List.mem_replicate.mp hy).2
        subst y
        exact ⟨j, m, by simp, rfl⟩
      · obtain ⟨j', m', hmem, heq⟩ := ih hy
        subst y
        exact ⟨j', m', by simp [hmem], rfl⟩

private theorem rankRunsFrom_sound (runs : List (Int × Nat)) (n : Nat)
    (hp : (runs.map Prod.fst).Pairwise (· < ·)) :
    let ys := runs.flatMap fun (k, m) => List.replicate m k
    rankRunsFrom n runs =
      ys.map fun x => (n + 1 + (ys.filter fun y => y < x).length,
        n + (ys.filter fun y => y ≤ x).length) := by
  induction runs generalizing n with
  | nil => simp [rankRunsFrom]
  | cons e rs ih =>
      rcases e with ⟨k, m⟩
      simp only [List.map_cons, List.pairwise_cons] at hp
      let ys := rs.flatMap fun (j, m) => List.replicate m j
      have htail := ih (n + m) hp.2
      have hall : ∀ y ∈ ys, k < y := by
        intro y hy
        obtain ⟨j, mj, hmem, heq⟩ := mem_flatMap_replicate_key rs (by simpa [ys] using hy)
        subst y
        exact hp.1 j (by exact List.mem_map.mpr ⟨(j, mj), hmem, rfl⟩)
      have hnil := filter_eq_nil_of_all_gt k ys hall
      have hltK : ((List.replicate m k ++ ys).filter fun y : Int => y < k).length = 0 := by
        rw [List.filter_append, List.length_append, length_filter_replicate]
        simp [hnil.1]
      have hleK : ((List.replicate m k ++ ys).filter fun y : Int => y ≤ k).length = m := by
        rw [List.filter_append, List.length_append, length_filter_replicate]
        simp [hnil.2]
      rw [rankRunsFrom]
      dsimp only
      rw [htail]
      simp only [List.flatMap_cons, List.map_append, List.map_replicate]
      rw [hltK, hleK]
      simp only [Nat.add_zero, Nat.zero_add]
      congr 1
      apply List.map_congr_left
      intro x hx
      have hxk : k < x := hall x (by simpa [ys] using hx)
      have hlt := (filter_count_replicate_append m k x ys).1
      have hle := (filter_count_replicate_append m k x ys).2
      rw [if_pos hxk] at hlt
      rw [if_pos (show k ≤ x by omega)] at hle
      rw [hlt, hle]
      simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm, ys]

private theorem pow_succ_two (d : Nat) : 2 ^ (d + 1) = 2 ^ d * 2 := by
  rw [Nat.pow_succ]

private theorem mod_succ_false (x d : Nat) (hbit : x.testBit d = false) :
    x % 2 ^ (d + 1) = x % 2 ^ d := by
  let r := x % 2 ^ (d + 1)
  have hrlt : r < 2 ^ (d + 1) := Nat.mod_lt _ (Nat.two_pow_pos _)
  have hrbit : r.testBit d = false := by
    rw [show r = x % 2 ^ (d + 1) by rfl, Nat.testBit_mod_two_pow]
    simp [hbit]
  have hrsmall : r < 2 ^ d := by
    by_cases hge : 2 ^ d ≤ r
    · have : r.testBit d = true := Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hge
        (by simpa [pow_succ_two] using hrlt)
      simp [hrbit] at this
    · omega
  have hdecomp : x = (x / 2 ^ (d + 1)) * 2 ^ (d + 1) + r := by
    dsimp [r]
    rw [Nat.mul_comm]
    exact (Nat.div_add_mod x (2 ^ (d + 1))).symm
  rw [hdecomp]
  have hmul : (x / 2 ^ (d + 1)) * 2 ^ (d + 1) = (2 ^ d) * ((x / 2 ^ (d + 1)) * 2) := by
    rw [pow_succ_two]
    grind
  rw [hmul]
  rw [Nat.add_comm (2 ^ d * ((x / 2 ^ (d + 1)) * 2)) r]
  rw [Nat.add_mul_mod_self_left]
  rw [pow_succ_two]
  have hmul2 : 2 ^ d * ((x / (2 ^ d * 2)) * 2) = (2 ^ d * 2) * (x / (2 ^ d * 2)) := by grind
  rw [hmul2]
  rw [Nat.add_mul_mod_self_left]
  rw [Nat.mod_eq_of_lt (by omega : r < 2 ^ d * 2)]
  exact (Nat.mod_eq_of_lt hrsmall).symm

private theorem mod_succ_true (x d : Nat) (hbit : x.testBit d = true) :
    x % 2 ^ (d + 1) = 2 ^ d + x % 2 ^ d := by
  let r := x % 2 ^ (d + 1)
  have hrlt : r < 2 ^ (d + 1) := Nat.mod_lt _ (Nat.two_pow_pos _)
  have hrbit : r.testBit d = true := by
    rw [show r = x % 2 ^ (d + 1) by rfl, Nat.testBit_mod_two_pow]
    simp [hbit]
  have hrge : 2 ^ d ≤ r := Nat.ge_two_pow_of_testBit hrbit
  have hdecomp : x = (x / 2 ^ (d + 1)) * 2 ^ (d + 1) + r := by
    dsimp [r]
    rw [Nat.mul_comm]
    exact (Nat.div_add_mod x (2 ^ (d + 1))).symm
  have hxmodd : x % 2 ^ d = r - 2 ^ d := by
    rw [hdecomp]
    have hmul : (x / 2 ^ (d + 1)) * 2 ^ (d + 1) = (2 ^ d) * ((x / 2 ^ (d + 1)) * 2) := by
      rw [pow_succ_two]
      grind
    rw [hmul]
    rw [Nat.add_comm (2 ^ d * ((x / 2 ^ (d + 1)) * 2)) r]
    rw [Nat.add_mul_mod_self_left]
    have hrlt2 : r - 2 ^ d < 2 ^ d := by omega
    have hrdecomp : r = (r - 2 ^ d) + 2 ^ d * 1 := by omega
    rw [hrdecomp]
    rw [Nat.add_mul_mod_self_left]
    rw [show r - 2 ^ d + 2 ^ d * 1 - 2 ^ d = r - 2 ^ d by omega]
    exact Nat.mod_eq_of_lt hrlt2
  rw [hxmodd]
  omega

private theorem sorted_mergeSortNat (ys : List Nat) :
    ys.mergeSort.Pairwise (fun a b : Nat => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Nat => a ≤ b)
    (by intro a b c hab hbc
        exact decide_eq_true (by
          have hab' : a ≤ b := of_decide_eq_true hab
          have hbc' : b ≤ c := of_decide_eq_true hbc
          omega))
    (by intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have h' : b ≤ a := by omega
          simp [h']) ys
  exact h.imp (by intro a b hab; exact of_decide_eq_true hab)

private theorem perm_sorted_eq (l₁ l₂ : List Nat) (hp : l₁.Perm l₂)
    (h1 : l₁.Pairwise (fun a b : Nat => a ≤ b))
    (h2 : l₂.Pairwise (fun a b : Nat => a ≤ b)) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      exact List.Perm.nil_eq hp
  | cons a xs ih =>
      cases l₂ with
      | nil =>
          have hnil := List.Perm.eq_nil hp
          simp at hnil
      | cons b ys =>
          have hb1 : b ∈ a :: xs := hp.mem_iff.mpr (by simp)
          have ha2 : a ∈ b :: ys := hp.mem_iff.mp (by simp)
          simp only [List.mem_cons] at hb1 ha2
          have hab : a ≤ b := by
            rcases hb1 with hbeq | hb1
            · omega
            · exact (List.pairwise_cons.mp h1).1 b hb1
          have hba : b ≤ a := by
            rcases ha2 with haeq | ha2
            · omega
            · exact (List.pairwise_cons.mp h2).1 a ha2
          have heq : a = b := by omega
          cases heq
          rw [ih ys (List.Perm.cons_inv hp) (List.Pairwise.of_cons h1) (List.Pairwise.of_cons h2)]

private theorem mergeSort_split (low high : List Nat) (d : Nat)
    (hlow : ∀ x ∈ low, x < 2 ^ d) (hhigh : ∀ x ∈ high, 2 ^ d ≤ x) :
    (low ++ high).mergeSort = low.mergeSort ++ high.mergeSort := by
  apply perm_sorted_eq
  · exact (List.mergeSort_perm (low ++ high) (fun a b : Nat => a ≤ b)).trans
      ((List.Perm.append (List.mergeSort_perm low (fun a b : Nat => a ≤ b))
        (List.mergeSort_perm high (fun a b : Nat => a ≤ b))).symm)
  · exact sorted_mergeSortNat _
  · rw [List.pairwise_append]
    refine ⟨sorted_mergeSortNat _, sorted_mergeSortNat _, ?_⟩
    intro a ha b hb
    have ha' : a ∈ low := (List.mergeSort_perm low (fun x y : Nat => x ≤ y)).mem_iff.mp ha
    have hb' : b ∈ high := (List.mergeSort_perm high (fun x y : Nat => x ≤ y)).mem_iff.mp hb
    have := hlow a ha'
    have := hhigh b hb'
    omega

private theorem mergeSort_map_add (low : List Nat) (c : Nat) :
    (low.map fun x => c + x).mergeSort = low.mergeSort.map fun x => c + x := by
  apply perm_sorted_eq
  · exact (List.mergeSort_perm (low.map fun x => c + x) (fun a b : Nat => a ≤ b)).trans
      (((List.mergeSort_perm low (fun a b : Nat => a ≤ b)).map fun x => c + x).symm)
  · exact sorted_mergeSortNat _
  · have hs := sorted_mergeSortNat low
    exact hs.map (fun x => c + x) (by intro a b hab; omega)

private theorem filter_take_eq {α : Type} (ys : List α) (p : α → Bool) (i : Nat) :
    (ys.take i).filter p = (ys.filter p).take ((ys.take i).filter p).length := by
  induction i generalizing ys with
  | zero => simp
  | succ i ih =>
      cases ys with
      | nil => simp
      | cons y ys =>
          rw [List.take_cons (by omega : 0 < i + 1)]
          simp only [Nat.add_sub_cancel_right]
          by_cases hp : p y = true
          · simp only [List.filter_cons, hp, ite_true, List.length_cons]
            rw [ih]
            have hle : (List.filter p (List.take i ys)).length ≤ (List.filter p ys).length :=
              (List.Sublist.filter p (List.take_sublist i ys)).length_le
            rw [List.length_take, Nat.min_eq_left hle]
            rw [List.take_cons (by omega : 0 < (List.filter p (List.take i ys)).length + 1)]
            simp
          · simp only [List.filter_cons, hp, ite_false]
            rw [ih]
            simp

private theorem foldl_zero_count (xs : List Nat) (d n : Nat) :
    xs.foldl (fun n x => n + (!(x.testBit d)).toNat) n =
      n + (xs.filter fun x => !(x.testBit d)).length := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x.testBit d <;> simp [h, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem array_zero_count (xs : List Nat) (d i : Nat) (hi : i ≤ xs.length) :
    (((xs.scanl (fun n x => n + (!(x.testBit d)).toNat) 0).toArray)[i]?.getD 0) =
      ((xs.take i).filter fun x => !(x.testBit d)).length := by
  have hg := List.getElem?_scanl
    (f := fun (n : Nat) (x : Nat) => n + (!(x.testBit d)).toNat)
    (a := 0) (l := xs) (i := i)
  rw [if_pos hi] at hg
  rw [List.getElem?_toArray, hg]
  simp only [Option.getD_some]
  simpa using foldl_zero_count (xs.take i) d 0

private theorem filter_bit_complement_length (ys : List Nat) (d : Nat) :
    (ys.filter fun x => x.testBit d).length =
      ys.length - (ys.filter fun x => !(x.testBit d)).length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : y.testBit d
      · simp [h]
        have hle : (ys.filter fun x => !(x.testBit d)).length ≤ ys.length :=
          List.length_filter_le _ _
        omega
      · simp [h, ih]

private theorem perm_cons_append_move (y : Nat) : ∀ A B : List Nat,
    (y :: (A ++ B)).Perm (A ++ (y :: B))
  | [], B => by simp
  | a :: A, B => by
      have hswap : (y :: a :: (A ++ B)).Perm (a :: y :: (A ++ B)) :=
        (List.Perm.swap y a (A ++ B)).symm
      exact hswap.trans (perm_cons_append_move y A B |>.cons a)

private theorem perm_filter_split (ys : List Nat) (p : Nat → Bool) :
    ys.Perm (ys.filter p ++ ys.filter (fun x => !p x)) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : p y = true
      · simp only [List.filter_cons, h, Bool.not_true, ite_true, ite_false]
        exact ih.cons y
      · simp only [List.filter_cons, h, Bool.not_false, ite_false, ite_true]
        exact (ih.cons y).trans
          (perm_cons_append_move y (ys.filter p) (ys.filter (fun x => !p x)))

private theorem map_mask_high (ys : List Nat) (d : Nat) :
    (ys.filter fun x => x.testBit d).map (fun x => x % 2 ^ (d + 1)) =
      ((ys.filter fun x => x.testBit d).map (fun x => x % 2 ^ d)).map (fun y => 2 ^ d + y) := by
  rw [List.map_map]
  apply List.map_congr_left
  intro x hx
  have hbit : x.testBit d = true := (List.mem_filter.mp hx).2
  exact mod_succ_true x d hbit

private theorem mergeSort_mask_split (ys : List Nat) (d : Nat) :
    (ys.map fun x => x % 2 ^ (d + 1)).mergeSort =
      ((ys.filter fun x => !(x.testBit d)).map fun x => x % 2 ^ (d + 1)).mergeSort ++
      (((ys.filter fun x => x.testBit d).map fun x => x % 2 ^ d).map
        (fun y => 2 ^ d + y)).mergeSort := by
  let low := (ys.filter fun x => !(x.testBit d)).map fun x => x % 2 ^ (d + 1)
  let high := ((ys.filter fun x => x.testBit d).map fun x => x % 2 ^ d).map
    (fun y => 2 ^ d + y)
  have hpperm : (ys.map fun x => x % 2 ^ (d + 1)).Perm (low ++ high) := by
    have hp := (perm_filter_split ys (fun x => !(x.testBit d))).map
      (fun x => x % 2 ^ (d + 1))
    dsimp [low, high]
    simpa [map_mask_high ys d] using hp
  have hEq : (ys.map fun x => x % 2 ^ (d + 1)).mergeSort = (low ++ high).mergeSort := by
    apply perm_sorted_eq
    · exact ((List.mergeSort_perm (ys.map fun x => x % 2 ^ (d + 1))
        (fun a b : Nat => a ≤ b)).trans hpperm).trans
        ((List.mergeSort_perm (low ++ high) (fun a b : Nat => a ≤ b)).symm)
    · exact sorted_mergeSortNat _
    · exact sorted_mergeSortNat _
  rw [hEq]
  change (low ++ high).mergeSort = low.mergeSort ++ high.mergeSort
  apply mergeSort_split (d := d)
  · intro x hx
    rcases List.mem_map.mp hx with ⟨x0, hx0, rfl⟩
    have hbit : x0.testBit d = false := by
      have hz := (List.mem_filter.mp hx0).2
      simpa using hz
    rw [mod_succ_false x0 d hbit]
    exact Nat.mod_lt _ (Nat.two_pow_pos d)
  · intro y hy
    rcases List.mem_map.mp hy with ⟨y0, hy0, rfl⟩
    rcases List.mem_map.mp hy0 with ⟨x, hx, rfl⟩
    omega

private theorem mergeSort_map_comp_add (ys : List Nat) (d c : Nat) :
    (ys.map ((fun y : Nat => c + y) ∘ (fun x : Nat => x % 2 ^ d))).mergeSort =
      (ys.map (fun x : Nat => x % 2 ^ d)).mergeSort.map (fun y : Nat => c + y) := by
  simpa [Function.comp_apply, List.map_map] using
    mergeSort_map_add (ys.map (fun x : Nat => x % 2 ^ d)) c

private theorem getD_eq_getElem (l : List Nat) (k : Nat) (hk : k < l.length) :
    l[k]?.getD 0 = l[k] := by
  have hopt := List.getElem?_eq_some_iff.mpr ⟨hk, rfl⟩
  rw [hopt]
  rfl

private theorem quantile_general (d : Nat) (xs : List Nat) (l r k : Nat)
    (hr : r ≤ xs.length) (hk : k < r - l) :
    quantile d (wave xs d) l r k =
      (((frame xs l r).map fun x => x % 2 ^ d).mergeSort)[k]?.getD 0 := by
  induction d generalizing xs l r k with
  | zero =>
      have hlen : ((frame xs l r).map fun x => x % 2 ^ 0).length = r - l := by
        unfold frame
        rw [List.length_map, List.length_drop, List.length_take, Nat.min_eq_left hr]
      have hklen : k < (((frame xs l r).map fun x => x % 2 ^ 0).mergeSort).length := by
        have hlenm := (List.mergeSort_perm ((frame xs l r).map fun x => x % 2 ^ 0)
          (fun a b : Nat => a ≤ b)).length_eq
        rw [hlenm, hlen]
        exact hk
      have hmem : (((frame xs l r).map fun x => x % 2 ^ 0).mergeSort)[k] ∈
          (frame xs l r).map (fun x => x % 2 ^ 0) :=
        (List.mergeSort_perm _ (fun a b : Nat => a ≤ b)).mem_iff.mp
          (List.getElem_mem hklen)
      rcases List.mem_map.mp hmem with ⟨x, hx, hxeq⟩
      have hget : (((frame xs l r).map fun x => x % 2 ^ 0).mergeSort)[k]?.getD 0 =
          (((frame xs l r).map fun x => x % 2 ^ 0).mergeSort)[k] := by
        have hopt0 : (((frame xs l r).map fun x => x % 2 ^ 0).mergeSort)[k]? =
            some ((((frame xs l r).map fun x => x % 2 ^ 0).mergeSort)[k]) := by
          exact List.getElem?_eq_some_iff.mpr ⟨hklen, rfl⟩
        rw [hopt0]
        rfl
      rw [hget]
      simpa [quantile, Nat.mod_one] using hxeq
  | succ d ih =>
      have hxsne : xs ≠ [] := by
        intro h
        subst xs
        simp at hr
        omega
      let zero : Nat → Bool := fun x => !(x.testBit d)
      let ones : Nat → Bool := fun x => x.testBit d
      let zs := xs.filter zero
      let os := xs.filter ones
      have hwave : wave xs (d + 1) =
          .node ((xs.scanl (fun n x => n + (zero x).toNat) 0).toArray)
            (wave zs d) (wave os d) := by
        simp [wave, hxsne, zero, zs, os, ones]
      rw [hwave]
      simp only [quantile]
      have ha := array_zero_count xs d l (by omega)
      have hb := array_zero_count xs d r hr
      rw [ha, hb]
      let a := ((xs.take l).filter zero).length
      let b := ((xs.take r).filter zero).length
      have htake : xs.take r = xs.take l ++ frame xs l r := by
        unfold frame
        conv => lhs; rw [← List.take_append_drop l (xs.take r)]
        simp [List.take_take, Nat.min_eq_left (by omega : l ≤ r), List.drop_take]
      have hftake : (xs.take r).filter zero =
          (xs.take l).filter zero ++ (frame xs l r).filter zero := by
        rw [htake, List.filter_append]
      have hztake_r : (xs.take r).filter zero = zs.take b := by
        rw [filter_take_eq]
      have hztake_l : (xs.take l).filter zero = zs.take a := by
        rw [filter_take_eq]
      have ha_le : a ≤ zs.length := by
        dsimp [a, zs]
        exact (List.Sublist.filter zero (List.take_sublist l xs)).length_le
      have hb_le : b ≤ zs.length := by
        dsimp [b, zs]
        exact (List.Sublist.filter zero (List.take_sublist r xs)).length_le
      have hftake' : zs.take b = zs.take a ++ (frame xs l r).filter zero := by
        rw [← hztake_r, ← hztake_l]
        exact hftake
      have hdrop_zero : (zs.take b).drop a = (frame xs l r).filter zero := by
        rw [hftake']
        rw [List.drop_append_of_le_length]
        · rw [List.drop_eq_nil_of_le (i := a)
            (by rw [List.length_take, Nat.min_eq_left ha_le]; omega)]
          simp
        · rw [List.length_take, Nat.min_eq_left ha_le]; omega
      have hframe_zero : (frame xs l r).filter zero = frame zs a b := by
        change (frame xs l r).filter zero = (zs.take b).drop a
        exact hdrop_zero.symm
      have hlenZero : ((frame xs l r).filter zero).length = b - a := by
        have hfl := congrArg List.length hftake
        rw [List.length_append] at hfl
        omega
      have hones_l : l - a = ((xs.take l).filter ones).length := by
        rw [filter_bit_complement_length]
        rw [List.length_take, Nat.min_eq_left (by omega : l ≤ xs.length)]
      have hones_r : r - b = ((xs.take r).filter ones).length := by
        rw [filter_bit_complement_length]
        rw [List.length_take, Nat.min_eq_left hr]
      have hftake_one : (xs.take r).filter ones =
          (xs.take l).filter ones ++ (frame xs l r).filter ones := by
        rw [htake, List.filter_append]
      have hotake_r : (xs.take r).filter ones = os.take (r - b) := by
        rw [filter_take_eq]
        rw [← hones_r]
      have hotake_l : (xs.take l).filter ones = os.take (l - a) := by
        rw [filter_take_eq]
        rw [← hones_l]
      have ha_one_le : l - a ≤ os.length := by
        dsimp [os]
        rw [hones_l]
        exact (List.Sublist.filter ones (List.take_sublist l xs)).length_le
      have hb_one_le : r - b ≤ os.length := by
        dsimp [os]
        rw [hones_r]
        exact (List.Sublist.filter ones (List.take_sublist r xs)).length_le
      have hftake_one' : os.take (r - b) = os.take (l - a) ++ (frame xs l r).filter ones := by
        rw [← hotake_r, ← hotake_l]
        exact hftake_one
      have hframe_one : (frame xs l r).filter ones = frame os (l - a) (r - b) := by
        change (frame xs l r).filter ones = (os.take (r - b)).drop (l - a)
        rw [hftake_one']
        rw [List.drop_append_of_le_length]
        · rw [List.drop_eq_nil_of_le (i := l - a)
            (by rw [List.length_take, Nat.min_eq_left ha_one_le]; omega)]
          simp
        · rw [List.length_take, Nat.min_eq_left ha_one_le]; omega
      have hlowmask : (frame zs a b).map (fun x => x % 2 ^ d) =
          ((frame xs l r).filter zero).map (fun x => x % 2 ^ (d + 1)) := by
        rw [← hframe_zero]
        apply List.map_congr_left
        intro x hx
        have hbit : x.testBit d = false := by
          have hz : zero x = true := (List.mem_filter.mp hx).2
          dsimp [zero] at hz
          simpa using hz
        exact (mod_succ_false x d hbit).symm
      have hhighmask : (frame os (l - a) (r - b)).map (fun x => x % 2 ^ d) =
          ((frame xs l r).filter ones).map (fun x => x % 2 ^ d) := by
        rw [← hframe_one]
      have ha_le_l : a ≤ l := by
        dsimp [a]
        have ht := List.length_filter_le zero (xs.take l)
        rw [List.length_take, Nat.min_eq_left (by omega : l ≤ xs.length)] at ht
        exact ht
      have hb_le_r : b ≤ r := by
        dsimp [b]
        have ht := List.length_filter_le zero (xs.take r)
        rw [List.length_take, Nat.min_eq_left hr] at ht
        exact ht
      have hbdecomp : b = a + ((frame xs l r).filter zero).length := by
        have hfl := congrArg List.length hftake
        rw [List.length_append] at hfl
        exact hfl
      have hlenframe : (frame xs l r).length = r - l := by
        unfold frame
        rw [List.length_drop, List.length_take, Nat.min_eq_left hr]
      have hba_le : b - a ≤ r - l := by
        have hfilt := List.length_filter_le zero (frame xs l r)
        rw [hlenZero, hlenframe] at hfilt
        exact hfilt
      have hlenOneFrame : ((frame xs l r).filter ones).length = (r - b) - (l - a) := by
        rw [hframe_one]
        unfold frame
        rw [List.length_drop, List.length_take, Nat.min_eq_left hb_one_le]
      have hkfull : k < ((((frame xs l r).filter zero).map (fun x => x % 2 ^ (d + 1))).mergeSort ++
          ((((frame xs l r).filter ones).map (fun x => x % 2 ^ d)).map
            (fun y => 2 ^ d + y)).mergeSort).length := by
        have hl1 := (List.mergeSort_perm (((frame xs l r).filter zero).map
          (fun x => x % 2 ^ (d + 1))) (fun x y : Nat => x ≤ y)).length_eq
        have hl2 := (List.mergeSort_perm ((((frame xs l r).filter ones).map
          (fun x => x % 2 ^ d)).map (fun y => 2 ^ d + y))
          (fun x y : Nat => x ≤ y)).length_eq
        rw [List.length_append, hl1, hl2, List.length_map, List.length_map, List.length_map,
          hlenZero, hlenOneFrame]
        have htotal : (r - l) = (b - a) + ((r - b) - (l - a)) := by omega
        omega
      by_cases hkz : k < b - a
      · have hlowlen : (frame zs a b).length = b - a := by
          unfold frame
          rw [List.length_drop, List.length_take, Nat.min_eq_left hb_le]
        have hik : k < (frame zs a b).length := by omega
        have hrec := ih zs a b k hb_le (by simpa [hlowlen] using hik)
        let lowms := (((frame xs l r).filter zero).map (fun x => x % 2 ^ (d + 1))).mergeSort
        have hlenLow : k < lowms.length := by
          have hlenm := (List.mergeSort_perm
            (((frame xs l r).filter zero).map (fun x => x % 2 ^ (d + 1)))
            (fun x y : Nat => x ≤ y)).length_eq
          dsimp [lowms]
          rw [hlenm, List.length_map, hlenZero]
          exact hkz
        have hrec' : quantile d (wave zs d) a b k = lowms[k] := by
          dsimp [lowms]
          rw [hrec, hlowmask, getD_eq_getElem]
        rw [if_pos hkz]
        rw [hrec']
        rw [mergeSort_mask_split (frame xs l r) d]
        rw [getD_eq_getElem _ _ hkfull]
        dsimp [lowms]
        rw [List.getElem_append_left (by simpa [lowms] using hlenLow)]
      · let kk := k - (b - a)
        have hkk : kk < (r - b) - (l - a) := by
          dsimp [kk]
          have htotal : (r - l) = (b - a) + ((r - b) - (l - a)) := by omega
          omega
        have hrec := ih os (l - a) (r - b) kk
          (by omega) (by simpa using hkk)
        let highbase := ((frame xs l r).filter ones).map (fun x => x % 2 ^ d)
        have hlenFrameOne : (frame os (l - a) (r - b)).length = (r - b) - (l - a) := by
          unfold frame
          rw [List.length_drop, List.length_take, Nat.min_eq_left hb_one_le]
        have hlenHigh : kk < highbase.mergeSort.length := by
          have hlenm := (List.mergeSort_perm highbase (fun x y : Nat => x ≤ y)).length_eq
          dsimp [highbase]
          rw [hlenm, List.length_map]
          rw [hframe_one, hlenFrameOne]
          exact hkk
        have hrec' : quantile d (wave os d) (l - a) (r - b) kk = highbase.mergeSort[kk] := by
          dsimp [highbase]
          rw [hrec, hhighmask, getD_eq_getElem]
        rw [if_neg hkz]
        rw [hrec']
        rw [mergeSort_mask_split (frame xs l r) d]
        rw [getD_eq_getElem _ _ hkfull]
        rw [List.getElem_append]
        have hlenLowEq : ((((frame xs l r).filter zero).map
            (fun x => x % 2 ^ (d + 1))).mergeSort).length = b - a := by
          have hlenm := (List.mergeSort_perm
            (((frame xs l r).filter zero).map (fun x => x % 2 ^ (d + 1)))
            (fun x y : Nat => x ≤ y)).length_eq
          rw [hlenm, List.length_map, hlenZero]
        simp [hlenLowEq, hkz, mergeSort_map_comp_add, highbase]
        congr 1

theorem WaveletQuantile_sound : TinylambOpt.ResearchGoals.FrameIndexes.WaveletQuantile.Goal := by
  intro xs d l r k hall hr hk
  have hq := quantile_general d xs l r k hr hk
  have hmap : ((frame xs l r).map fun x => x % 2 ^ d) = frame xs l r := by
    have aux : ∀ ys : List Nat, (∀ x ∈ ys, x < 2 ^ d) →
        ys.map (fun x => x % 2 ^ d) = ys := by
      intro ys hys
      induction ys with
      | nil => simp
      | cons x xs ih =>
          have hx : x < 2 ^ d := hys x (by simp)
          have ht : ∀ y ∈ xs, y < 2 ^ d := by
            intro y hy
            exact hys y (by simp [hy])
          simp [Nat.mod_eq_of_lt hx, ih ht]
    exact aux (frame xs l r) (by
      intro x hx
      exact hall x (by
        unfold frame at hx
        exact List.mem_of_mem_take (List.mem_of_mem_drop hx)))
  rw [hmap] at hq
  rw [hq]
  have hlenframe : (frame xs l r).length = r - l := by
    unfold frame
    rw [List.length_drop, List.length_take, Nat.min_eq_left hr]
  have hklen : k < (frame xs l r).mergeSort.length := by
    have hm := (List.mergeSort_perm (frame xs l r) (fun a b : Nat => a ≤ b)).length_eq
    rw [hm, hlenframe]
    exact hk
  have hopt : (frame xs l r).mergeSort[k]? = some ((frame xs l r).mergeSort[k]) := by
    exact List.getElem?_eq_some_iff.mpr ⟨hklen, rfl⟩
  have hget : (frame xs l r).mergeSort[k]?.getD 0 = (frame xs l r).mergeSort[k] := by
    rw [hopt]
    rfl
  rw [hget]
  exact hopt.symm

theorem PeerRankRuns_sound : TinylambOpt.ResearchGoals.FrameIndexes.PeerRankRuns.Goal := by
  intro runs hp
  rw [rankRuns_eq_from]
  simpa using rankRunsFrom_sound runs 0 hp

-- TODO(SlidingMinimum): queue invariant proof not completed in this wave.

end TinylambOpt.ResearchProofs.Wave7FrameIndexes
