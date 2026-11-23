/-
Soundness of the packed-bit Hamming-one (≤ 1 mismatch) string matcher.

Model: after processing a prefix `text` of the input, bit `k` (for `k < pat.length`)
of the machine's `exact` register holds `E pat text k` (the last `k+1` text bytes
match the first `k+1` pattern bytes exactly), and bit `k` of `one` holds
`O pat text k` (their pairwise mismatch count is at most one).  The snoc induction
on the text uses the window recurrence
`winPairs pat (ys ++ [a]) (k+1) = winPairs pat ys k ++ [(pat[k+1], a)]`,
which mirrors the Shift-And style recurrence
`one' = ((one <<< 1 ||| 1) &&& eqMask) ||| ((exact <<< 1) ||| 1)`.
Hits are accumulated in reverse; `hitsModel` is the ascending model list, and a
reindexing lemma turns it into the reference's filtered `List.range`.
-/
import TinylambOpt.ResearchGoals.HammingOneSearch

namespace TinylambOpt.ResearchProofs.HammingOneSearch

open TinylambOpt.ResearchGoals.HammingOneSearch

/-! ## Generic helpers -/

private theorem id_pure' {α : Type} (x : α) : (pure x : Id α) = x := rfl
private theorem id_bind' {α β : Type} (x : α) (f : α → Id β) : (x >>= f : Id β) = f x := rfl
private theorem idrun' {α : Type} (x : Id α) : Id.run x = x := rfl

private theorem snoc_induction {α : Type} {C : List α → Prop} (h0 : C [])
    (hs : ∀ (a : α) (l : List α), C l → C (l ++ [a])) (l : List α) : C l := by
  have key : ∀ m : List α, C m.reverse := by
    intro m
    induction m with
    | nil => exact h0
    | cons a as ih => simp only [List.reverse_cons]; exact hs a as.reverse ih
  have h := key l.reverse
  rwa [List.reverse_reverse] at h

private theorem testBit_toNat_cases (b c : UInt8) (j : Nat) :
    Nat.testBit ((b == c).toNat) j = (decide (b = c) && decide (j = 0)) := by
  have aux : Nat.testBit 1 j = decide (j = 0) := by
    induction j with
    | zero => simp [Nat.testBit_zero]
    | succ j ih =>
        rw [Nat.testBit_add_one, show (1 : Nat) / 2 = 0 by decide, Nat.zero_testBit,
          decide_eq_false (Nat.succ_ne_zero j)]
  by_cases h : b = c
  · rw [show (b == c) = true from by simpa using h, Bool.toNat_true, aux]
    simp [h]
  · rw [show (b == c) = false from by simpa using h, Bool.toNat_false, Nat.zero_testBit]
    simp [h]

private theorem any_true_iff {α : Type} {l : List α} {p : α → Bool} :
    (l.any p = true) ↔ ∃ x ∈ l, p x = true := by
  induction l with
  | nil => simp
  | cons a as ih =>
      constructor
      · intro h
        rw [List.any_cons, Bool.or_eq_true] at h
        rcases h with h | h
        · exact ⟨a, List.mem_cons_self, h⟩
        · obtain ⟨x, hx, hp⟩ := ih.mp h
          exact ⟨x, List.mem_cons_of_mem a hx, hp⟩
      · rintro ⟨x, hx, hp⟩
        rw [List.any_cons, Bool.or_eq_true]
        rcases List.mem_cons.mp hx with rfl | hx
        · exact Or.inl hp
        · exact Or.inr (ih.mpr ⟨x, hx, hp⟩)

private theorem zipIdx_snoc {α : Type} (ys : List α) (a : α) :
    (ys ++ [a]).zipIdx = ys.zipIdx ++ [(a, ys.length)] := by
  rw [List.zipIdx_append]
  show ys.zipIdx ++ [(a, 0 + ys.length)] = ys.zipIdx ++ [(a, ys.length)]
  rw [Nat.zero_add]

private theorem length_snoc {α : Type} (ys : List α) (a : α) :
    (ys ++ [a]).length = ys.length + 1 := by
  rw [List.length_append]
  simp

/-! ## List helpers -/

private theorem zip_snoc {α β : Type} (xs : List α) (ys : List β) (a : α) (b : β)
    (h : xs.length = ys.length) : (xs ++ [a]).zip (ys ++ [b]) = xs.zip ys ++ [(a, b)] := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys => simp at h
  | cons x xs ih =>
      cases ys with
      | nil => simp at h
      | cons y ys =>
          have h1 : (x :: xs ++ [a]).zip (y :: ys ++ [b]) = (x, y) :: (xs ++ [a]).zip (ys ++ [b]) := rfl
          rw [h1, ih ys (by simpa using h)]
          show (x, y) :: (xs.zip ys ++ [(a, b)]) = (x :: xs).zip (y :: ys) ++ [(a, b)]
          rw [show (x :: xs).zip (y :: ys) = (x, y) :: xs.zip ys from rfl]
          rfl

private theorem take_snoc_pat (pat : Bytes) (p : UInt8) (k : Nat) (hpa : pat[k]? = some p) :
    pat.take (k + 1) = pat.take k ++ [p] := by
  obtain ⟨hlt, he⟩ := List.getElem?_eq_some_iff.1 hpa
  rw [List.take_succ_eq_append_getElem hlt, he]

private theorem drop_snoc_eq (ys : Bytes) (a : UInt8) (k : Nat) (hk : k ≤ ys.length + 1) :
    (ys ++ [a]).drop ((ys ++ [a]).length - (k + 1)) = ys.drop (ys.length - k) ++ [a] := by
  have h1 : (ys ++ [a]).length - (k + 1) = ys.length - k := by
    rw [length_snoc]
    omega
  rw [h1, List.drop_append_of_le_length (by omega : ys.length - k ≤ ys.length)]

private theorem zip_take_length {α β : Type} (l₁ : List α) (l₂ : List β) :
    l₁.zip (l₂.take l₁.length) = l₁.zip l₂ := by
  induction l₁ generalizing l₂ with
  | nil => simp
  | cons x xs ih =>
      cases l₂ with
      | nil => simp
      | cons y ys => simpa using ih ys

private theorem filter_singleton_ne (p a : UInt8) :
    ([(p, a)].filter fun q => q.1 != q.2) = if p = a then [] else [(p, a)] := by
  by_cases h : p = a <;> simp [h]

/-! ## Bit masks -/

private theorem not_decide_lt (j k : Nat) : (!decide (k < j)) = decide (j ≤ k) := by
  by_cases h : k < j
  · rw [decide_eq_true h, decide_eq_false (by omega : ¬ (j ≤ k)), Bool.not_true]
  · rw [decide_eq_false h, decide_eq_true (by omega : j ≤ k)]
    rfl

private theorem getLsbD_one' (k : Nat) (hk : k < 64) : (1 : W).getLsbD k = decide (k = 0) := by
  show (BitVec.ofNat 64 1).getLsbD k = decide (k = 0)
  rw [BitVec.getLsbD_ofNat, decide_eq_true hk, Bool.true_and]
  rcases Nat.eq_zero_or_pos k with rfl | hk0
  · rfl
  · rw [decide_eq_false (Nat.ne_of_gt hk0)]
    exact Nat.testBit_lt_two_pow (x := 1) (Nat.one_lt_two_pow (Nat.ne_of_gt hk0))

private theorem getLsbD_zero' (k : Nat) : (0 : W).getLsbD k = false := by
  show (BitVec.ofNat 64 0).getLsbD k = false
  rw [BitVec.getLsbD_ofNat, Nat.testBit_eq_decide_div_mod_eq, Nat.zero_div, Nat.zero_mod,
    decide_eq_false (by omega : ¬ ((0 : Nat) = 1)), Bool.and_false]

private theorem getLsbD_shift_beq (b c : UInt8) (j k : Nat) (hk : k < 64) :
    ((BitVec.ofNat 64 (b == c).toNat) <<< j).getLsbD k = (decide (j = k) && decide (b = c)) := by
  rw [BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ofNat, testBit_toNat_cases]
  rw [decide_eq_true hk, Bool.true_and]
  by_cases hjk : j ≤ k
  · have h1 : decide (k - j < 64) = true := decide_eq_true (by omega)
    have h2 : decide (k - j = 0) = decide (j = k) :=
      decide_eq_decide.mpr ⟨fun h => by omega, fun h => by omega⟩
    have h2' : (!decide (k < j)) = decide (j ≤ k) := not_decide_lt j k
    rw [h1, h2, h2', decide_eq_true hjk, Bool.true_and]
    simp [Bool.and_comm]
  · rw [decide_eq_true (show k < j from by omega), Bool.not_true, Bool.false_and,
      decide_eq_false (show ¬ (j = k) from by omega), Bool.false_and]

private theorem getLsbD_foldl (xs : List (UInt8 × Nat)) (w : W) (c : UInt8) (k : Nat)
    (hk : k < 64) :
    (xs.foldl (fun acc e => acc ||| ((BitVec.ofNat 64 (e.1 == c).toNat) <<< e.2)) w).getLsbD k =
      (w.getLsbD k || xs.any (fun e => decide (e.2 = k) && decide (e.1 = c))) := by
  induction xs generalizing w with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, List.any_cons, BitVec.getLsbD_or,
        getLsbD_shift_beq x.1 c x.2 k hk, Bool.or_assoc]

private theorem getLsbD_eqMask (pat : Bytes) (c : UInt8) (k : Nat) (hk : k < 64) :
    (eqMask pat c).getLsbD k = decide (pat[k]? = some c) := by
  unfold eqMask
  rw [getLsbD_foldl _ 0 c k hk]
  rw [getLsbD_zero' k, Bool.false_or]
  by_cases h : pat[k]? = some c
  · rw [decide_eq_true h]
    rw [any_true_iff]
    refine ⟨(c, k), List.mem_zipIdx_iff_getElem?.2 h, by simp⟩
  · rw [decide_eq_false h]
    refine Bool.eq_false_of_ne_true (fun hcon => ?_)
    rw [any_true_iff] at hcon
    obtain ⟨⟨u, v⟩, hme, hf⟩ := hcon
    rw [List.mem_zipIdx_iff_getElem?] at hme
    rw [Bool.and_eq_true] at hf
    obtain ⟨hv, huc⟩ := hf
    obtain rfl : v = k := of_decide_eq_true hv
    obtain rfl : u = c := of_decide_eq_true huc
    exact h hme

/-! ## Window model -/

private def winPairs (pat text : Bytes) (k : Nat) : List (UInt8 × UInt8) :=
  (pat.take (k + 1)).zip (text.drop (text.length - (k + 1)))

private def mism (pat text : Bytes) (k : Nat) : Nat :=
  ((winPairs pat text k).filter fun q => q.1 != q.2).length

private def E (pat text : Bytes) (k : Nat) : Bool :=
  decide (k + 1 ≤ text.length) && decide (mism pat text k ≤ 0)

private def O (pat text : Bytes) (k : Nat) : Bool :=
  decide (k + 1 ≤ text.length) && decide (mism pat text k ≤ 1)

private theorem bool_decomp (d x y : Bool) (h : y = true → x = true) :
    (d && x) = ((d && x) || (d && y)) := by
  cases d <;> cases x <;> cases y <;> first
    | exact h rfl
    | simp

private theorem winPairs_snoc0 (pat : Bytes) (p : UInt8) (ys : Bytes) (a : UInt8)
    (hpa : pat[0]? = some p) : winPairs pat (ys ++ [a]) 0 = [(p, a)] := by
  unfold winPairs
  rw [take_snoc_pat pat p 0 hpa, drop_snoc_eq ys a 0 (by omega), Nat.sub_zero,
    List.drop_of_length_le (Nat.le_refl _), List.take_zero]
  rfl

private theorem winPairs_snoc' (pat : Bytes) (ys : Bytes) (a : UInt8) (k : Nat)
    (hkl : k + 1 < pat.length) (hk : k + 1 ≤ ys.length) :
    winPairs pat (ys ++ [a]) (k + 1) = winPairs pat ys k ++ [(pat[k+1], a)] := by
  have hpa : pat[k+1]? = some pat[k+1] := List.getElem?_eq_getElem hkl
  unfold winPairs
  rw [take_snoc_pat pat pat[k+1] (k+1) hpa, drop_snoc_eq ys a (k+1) (by omega)]
  have hlen : (pat.take (k+1)).length = (ys.drop (ys.length - (k+1))).length := by
    rw [List.length_take_of_le (by omega : k + 1 ≤ pat.length), List.length_drop]
    omega
  rw [zip_snoc (pat.take (k+1)) (ys.drop (ys.length - (k+1))) pat[k+1] a hlen]

private theorem E_snoc0 (pat : Bytes) (p : UInt8) (ys : Bytes) (a : UInt8)
    (hpa : pat[0]? = some p) : E pat (ys ++ [a]) 0 = decide (p = a) := by
  have hw := winPairs_snoc0 pat p ys a hpa
  unfold E mism
  rw [show decide (0 + 1 ≤ (ys ++ [a]).length) = true from
    decide_eq_true (by rw [length_snoc]; omega), hw, filter_singleton_ne]
  by_cases h : p = a
  · rw [if_pos h, List.length_nil, decide_eq_true (Nat.le_refl 0), Bool.true_and,
      decide_eq_true h]
  · rw [if_neg h, List.length_singleton, decide_eq_false (Nat.not_succ_le_zero 0),
      Bool.true_and, decide_eq_false h]

private theorem O_snoc0 (pat : Bytes) (ys : Bytes) (a : UInt8) :
    O pat (ys ++ [a]) 0 = true := by
  cases pat with
  | nil => unfold O mism winPairs; simp
  | cons p ps =>
      have hw := winPairs_snoc0 (p :: ps) p ys a (List.getElem?_eq_getElem (by simp))
      unfold O mism
      rw [show decide (0 + 1 ≤ (ys ++ [a]).length) = true from
        decide_eq_true (by rw [length_snoc]; omega), hw, filter_singleton_ne]
      by_cases h : p = a
      · rw [if_pos h, List.length_nil, decide_eq_true (by omega), Bool.true_and]
      · rw [if_neg h, List.length_singleton, decide_eq_true (by omega), Bool.true_and]

private theorem E_snoc' (pat : Bytes) (ys : Bytes) (a : UInt8) (k : Nat)
    (hkl : k + 1 < pat.length) (hk : k + 1 ≤ ys.length) :
    E pat (ys ++ [a]) (k + 1) = (E pat ys k && decide (pat[k+1]? = some a)) := by
  have hw := winPairs_snoc' pat ys a k hkl hk
  have hl : decide (k + 1 + 1 ≤ (ys ++ [a]).length) = decide (k + 1 ≤ ys.length) := by
    rw [length_snoc, decide_eq_decide]
    exact ⟨fun h => by omega, fun h => by omega⟩
  have hpa : pat[k+1]? = some pat[k+1] := List.getElem?_eq_getElem hkl
  unfold E mism
  rw [hl, hw, List.filter_append, filter_singleton_ne]
  by_cases hmt : pat[k+1] = a
  · rw [show decide (pat[k+1]? = some a) = true from
      decide_eq_true (hpa.trans (congrArg some hmt)), if_pos hmt, List.append_nil,
      Bool.and_true]
  · rw [show decide (pat[k+1]? = some a) = false from
      decide_eq_false (fun hc => hmt (by rw [hpa] at hc; exact Option.some.inj hc)),
      if_neg hmt, List.length_append, List.length_singleton,
      decide_eq_false (Nat.not_succ_le_zero _), Bool.and_false, Bool.and_false]

private theorem O_snoc' (pat : Bytes) (ys : Bytes) (a : UInt8) (k : Nat)
    (hkl : k + 1 < pat.length) (hk : k + 1 ≤ ys.length) :
    O pat (ys ++ [a]) (k + 1) = ((O pat ys k && decide (pat[k+1]? = some a)) || E pat ys k) := by
  have hw := winPairs_snoc' pat ys a k hkl hk
  have hl : decide (k + 1 + 1 ≤ (ys ++ [a]).length) = decide (k + 1 ≤ ys.length) := by
    rw [length_snoc, decide_eq_decide]
    exact ⟨fun h => by omega, fun h => by omega⟩
  have hpa : pat[k+1]? = some pat[k+1] := List.getElem?_eq_getElem hkl
  unfold O E mism
  rw [hl, hw, List.filter_append, filter_singleton_ne]
  by_cases hmt : pat[k+1] = a
  · have hd : decide (pat[k+1]? = some a) = true := decide_eq_true (hpa.trans (congrArg some hmt))
    rw [hd, if_pos hmt, List.append_nil, Bool.and_true]
    exact (bool_decomp (decide (k + 1 ≤ List.length ys))
        (decide (((winPairs pat ys k).filter fun q => q.1 != q.2).length ≤ 1))
        (decide (((winPairs pat ys k).filter fun q => q.1 != q.2).length ≤ 0))
        fun h => decide_eq_true (Nat.le_trans (of_decide_eq_true h) (by omega)))
  · have hd : decide (pat[k+1]? = some a) = false :=
      decide_eq_false fun hc => hmt (by rw [hpa] at hc; exact Option.some.inj hc)
    rw [hd, if_neg hmt]
    rw [List.length_append, List.length_singleton]
    rw [show decide (((winPairs pat ys k).filter fun q => q.1 != q.2).length + 1 ≤ 1) =
        decide (((winPairs pat ys k).filter fun q => q.1 != q.2).length ≤ 0) from
      decide_eq_decide.mpr ⟨fun h => by omega, fun h => by omega⟩]
    rw [Bool.and_false, Bool.false_or]

/-! ## Hits model -/

private def hitsModel (pat : Bytes) (m : Nat) (text : Bytes) : List Nat :=
  ((List.range text.length).filter fun t => O pat (text.take (t + 1)) m).map
    fun t => t + 1 - (m + 1)

private theorem hitsModel_snoc (pat : Bytes) (m : Nat) (ys : Bytes) (a : UInt8) :
    hitsModel pat m (ys ++ [a]) =
      hitsModel pat m ys ++ (if O pat (ys ++ [a]) m then [ys.length + 1 - (m + 1)] else []) := by
  have h1 : ∀ t ∈ List.range ys.length,
      O pat ((ys ++ [a]).take (t + 1)) m = O pat (ys.take (t + 1)) m := by
    intro t ht
    rw [List.mem_range] at ht
    rw [List.take_append_of_le_length (Nat.succ_le_of_lt ht)]
  have htake : (ys ++ [a]).take (ys.length + 1) = ys ++ [a] :=
    List.take_of_length_le (by rw [length_snoc]; omega : (ys ++ [a]).length ≤ ys.length + 1)
  have hlast : O pat ((ys ++ [a]).take (ys.length + 1)) m = O pat (ys ++ [a]) m := by
    rw [htake]
  unfold hitsModel
  rw [length_snoc, List.range_succ, List.filter_append, List.map_append]
  rw [List.filter_congr h1]
  have h2 : List.filter (fun t => O pat ((ys ++ [a]).take (t + 1)) m) [ys.length] =
      if O pat (ys ++ [a]) m then [ys.length] else [] := by
    rw [List.filter_cons, hlast, List.filter_nil]
  rw [h2]
  split
  · simp only [List.map_singleton, List.map_nil, List.append_nil]
  · simp only [List.map_nil, List.append_nil]

/-! ## The machine -/

private def step (pat : Bytes) (eq : W) (i : Nat) (st : W × W × List Nat) : W × W × List Nat :=
  let extend := (st.1 <<< 1) ||| 1
  let one' := (((st.2.1 <<< 1) ||| 1) &&& eq) ||| extend
  (extend &&& eq, one',
    if one'.getLsbD (pat.length - 1) then (i + 1 - pat.length) :: st.2.2 else st.2.2)

private theorem step_fst (pat : Bytes) (eq : W) (i : Nat) (st : W × W × List Nat) :
    (step pat eq i st).1 = ((st.1 <<< 1) ||| 1) &&& eq := by
  cases st with
  | mk x yz =>
      cases yz with
      | mk y hits => simp [step]

private theorem step_snd_fst (pat : Bytes) (eq : W) (i : Nat) (st : W × W × List Nat) :
    (step pat eq i st).2.1 = (((st.2.1 <<< 1) ||| 1) &&& eq) ||| ((st.1 <<< 1) ||| 1) := by
  cases st with
  | mk x yz =>
      cases yz with
      | mk y hits => simp [step]

private theorem step_snd_snd2 (pat : Bytes) (eq : W) (i : Nat) (st : W × W × List Nat) :
    (step pat eq i st).2.2
      = if (step pat eq i st).2.1.getLsbD (pat.length - 1) then
          (i + 1 - pat.length) :: st.2.2
        else st.2.2 := by
  cases st with
  | mk x yz =>
      cases yz with
      | mk y hits => simp [step]

private theorem step_mk (pat : Bytes) (eq : W) (i : Nat) (st : W × W × List Nat) :
    step pat eq i st = (((st.1 <<< 1) ||| (1 : W)) &&& eq,
      ((st.2.1 <<< 1) ||| (1 : W)) &&& eq ||| ((st.1 <<< 1) ||| (1 : W)),
      if ((((st.2.1 <<< 1) ||| (1 : W)) &&& eq) ||| ((st.1 <<< 1) ||| (1 : W))).getLsbD
          (pat.length - 1) = true
      then (i + 1 - pat.length) :: st.2.2
      else st.2.2) := by
  cases st with
  | mk x yz =>
      cases yz with
      | mk y hits => simp [step]

private def loop (pat : Bytes) (text : Bytes) : W × W × List Nat :=
  (List.zipIdx text).foldl (fun st c => step pat (eqMask pat c.1) c.2 st) (0, 0, [])

private theorem loop_snoc (pat : Bytes) (ys : Bytes) (a : UInt8) :
    loop pat (ys ++ [a]) = step pat (eqMask pat a) ys.length (loop pat ys) := by
  unfold loop
  rw [zipIdx_snoc, List.foldl_append, List.foldl_cons, List.foldl_nil]

private theorem masks_spec (pat : Bytes) (c : UInt8) :
    (compile pat)[c.toNat]?.getD 0 = eqMask pat c := by
  have hb : c.toNat < 256 := by
    have hs := UInt8.toNat_lt_size c
    have hsz : UInt8.size = 256 := rfl
    omega
  unfold compile
  rw [Array.getElem?_ofFn (i := c.toNat)]
  rw [dif_pos hb]
  rw [Option.getD_some]
  simp

private theorem search_eq_loop (pat text : Bytes) (hne : pat ≠ []) :
    search pat text = (loop pat text).2.2.reverse := by
  unfold search
  rw [ite_eq_right (by cases pat with
    | nil => exact absurd rfl hne
    | cons a as => simp)]
  simp only [id_pure', id_bind', idrun', masks_spec]
  have hstep : (fun (x : UInt8 × Nat) (s : W × W × List Nat) =>
      if (((s.2.1 <<< 1) ||| (1 : W)) &&& eqMask pat x.1 ||| ((s.1 <<< 1) ||| (1 : W))).getLsbD
          (pat.length - 1) = true
      then
        ForInStep.yield (((s.1 <<< 1) ||| (1 : W)) &&& eqMask pat x.1,
          ((s.2.1 <<< 1) ||| (1 : W)) &&& eqMask pat x.1 ||| ((s.1 <<< 1) ||| (1 : W)),
          (x.2 + 1 - pat.length) :: s.2.2)
      else
        ForInStep.yield (((s.1 <<< 1) ||| (1 : W)) &&& eqMask pat x.1,
          ((s.2.1 <<< 1) ||| (1 : W)) &&& eqMask pat x.1 ||| ((s.1 <<< 1) ||| (1 : W)),
          s.2.2))
      = fun (x : UInt8 × Nat) (s : W × W × List Nat) =>
        (pure (ForInStep.yield (step pat (eqMask pat x.1) x.2 s)) :
          Id (ForInStep (W × W × List Nat))) := by
    funext x s
    rw [step_mk]
    by_cases hc : ((((s.2.1 <<< 1) ||| (1 : W)) &&& eqMask pat x.1) ||| ((s.1 <<< 1) ||| (1 : W))).getLsbD
        (pat.length - 1) = true
    · rw [if_pos hc, if_pos hc, id_pure']
    · rw [if_neg hc, if_neg hc, id_pure']
  rw [hstep]
  rw [List.forIn_pure_yield_eq_foldl (m := Id)]
  simp only [id_pure']
  unfold loop
  rfl

/-! ## Bit invariants -/

private theorem bits_lemma (pat : Bytes) (m : Nat) (hm : pat.length = m + 1) (m64 : m < 64) :
    ∀ text : Bytes,
      (∀ k : Nat, k < pat.length → (loop pat text).1.getLsbD k = E pat text k) ∧
      (∀ k : Nat, k < pat.length → (loop pat text).2.1.getLsbD k = O pat text k) ∧
      (loop pat text).2.2.reverse = hitsModel pat m text := by
  refine snoc_induction ?_ ?_
  · -- base: empty text
    refine ⟨?_, ?_, ?_⟩
    · intro k hk
      have h0 : loop pat [] = (0#64, 0#64, []) := by
        unfold loop; simp
      rw [h0, BitVec.getLsbD_zero]
      unfold E mism winPairs
      simp
    · intro k hk
      have h0 : loop pat [] = (0#64, 0#64, []) := by
        unfold loop; simp
      rw [h0, BitVec.getLsbD_zero]
      unfold O mism winPairs
      simp
    · have h0 : loop pat [] = (0#64, 0#64, []) := by
        unfold loop; simp
      rw [h0]
      unfold hitsModel
      simp
  · -- snoc step
    intro a ys ih
    obtain ⟨hex, hone, hhits⟩ := ih
    have hsnoc : loop pat (ys ++ [a]) = step pat (eqMask pat a) ys.length (loop pat ys) :=
      loop_snoc pat ys a
    have hex' : ∀ k < pat.length, (loop pat (ys ++ [a])).1.getLsbD k = E pat (ys ++ [a]) k := by
      intro k hk
      rw [hsnoc, step_fst, BitVec.getLsbD_and, getLsbD_eqMask pat a k (by omega),
        BitVec.getLsbD_or, getLsbD_one' k (by omega), BitVec.getLsbD_shiftLeft]
      rcases Nat.eq_zero_or_pos k with rfl | hk0
      · -- k = 0
        rw [E_snoc0 pat pat[0] ys a (List.getElem?_eq_getElem hk)]
        simp [List.getElem?_eq_getElem hk, Option.some.injEq]
      · obtain ⟨k', rfl⟩ : ∃ k', k = k' + 1 :=
          Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk0)
        rw [Nat.add_sub_cancel]
        have hrec : ((loop pat ys).1.getLsbD k' : Bool) = E pat ys k' := hex k' (by omega)
        rw [hrec]
        by_cases hy : k' + 1 ≤ ys.length
        · rw [E_snoc' pat ys a k' (by omega) hy]
          simp only [decide_eq_true (show k' + 1 < 64 from by omega),
            decide_eq_false (show ¬ (k' + 1 < 1) from by omega),
            decide_eq_true (show (0 : Nat) < 64 from by decide),
            decide_eq_false (Nat.succ_ne_zero k'),
            Bool.not_false, Bool.true_and, Bool.and_true, Bool.or_false, Bool.false_or,
            Bool.and_false, Bool.false_and, Bool.and_self]
        · unfold E mism
          rw [show decide (k' + 1 + 1 ≤ (ys ++ [a]).length) = false from
            decide_eq_false (fun h => hy (by rw [length_snoc] at h; omega)),
            decide_eq_false hy]
          simp only [decide_eq_true (show k' + 1 < 64 from by omega),
            decide_eq_false (show ¬ (k' + 1 < 1) from by omega),
            decide_eq_true (show (0 : Nat) < 64 from by decide),
            decide_eq_false (Nat.succ_ne_zero k'),
            Bool.not_false, Bool.true_and, Bool.and_true, Bool.or_false, Bool.false_or,
            Bool.and_false, Bool.false_and]
    have hone' : ∀ k < pat.length, (loop pat (ys ++ [a])).2.1.getLsbD k = O pat (ys ++ [a]) k := by
      intro k hk
      rw [hsnoc, step_snd_fst, BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_or,
        BitVec.getLsbD_or, getLsbD_eqMask pat a k (by omega),
        getLsbD_one' k (by omega), BitVec.getLsbD_shiftLeft, BitVec.getLsbD_shiftLeft]
      rcases Nat.eq_zero_or_pos k with rfl | hk0
      · -- k = 0
        rw [O_snoc0]
        simp
      · obtain ⟨k', rfl⟩ : ∃ k', k = k' + 1 :=
          Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk0)
        rw [Nat.add_sub_cancel]
        rw [hone k' (by omega), hex k' (by omega)]
        have hg : (((1 : Nat) : W)).getLsbD (k' + 1) = false := by
          show (1 : W).getLsbD (k' + 1) = false
          rw [getLsbD_one' (k' + 1) (by omega), decide_eq_false (Nat.succ_ne_zero k')]
        by_cases hy : k' + 1 ≤ ys.length
        · rw [O_snoc' pat ys a k' (by omega) hy]
          simp only [hg, show (BitVec.ofNat 64 1).getLsbD (k' + 1) = false from
              (getLsbD_one' (k' + 1) (by omega)).trans (decide_eq_false (Nat.succ_ne_zero k')),
            decide_eq_true (show k' + 1 < 64 from by omega),
            decide_eq_false (show ¬ (k' + 1 < 1) from by omega),
            decide_eq_true (show (0 : Nat) < 64 from by decide),
            decide_eq_false (Nat.succ_ne_zero k'),
            Bool.not_false, Bool.true_and, Bool.and_true, Bool.or_false, Bool.false_or,
            Bool.and_false, Bool.false_and, Bool.and_self]
        · unfold O E mism
          rw [show decide (k' + 1 + 1 ≤ (ys ++ [a]).length) = false from
            decide_eq_false (fun h => hy (by rw [length_snoc] at h; omega)),
            decide_eq_false hy]
          simp only [hg, show (BitVec.ofNat 64 1).getLsbD (k' + 1) = false from
              (getLsbD_one' (k' + 1) (by omega)).trans (decide_eq_false (Nat.succ_ne_zero k')),
            decide_eq_true (show k' + 1 < 64 from by omega),
            decide_eq_false (show ¬ (k' + 1 < 1) from by omega),
            decide_eq_true (show (0 : Nat) < 64 from by decide),
            decide_eq_false (Nat.succ_ne_zero k'),
            Bool.not_false, Bool.true_and, Bool.and_true, Bool.or_false, Bool.false_or,
            Bool.and_false, Bool.false_and]
    have hhits' : (loop pat (ys ++ [a])).2.2.reverse = hitsModel pat m (ys ++ [a]) := by
      have hlen1 : pat.length - 1 = m := by omega
      have h22 : (loop pat (ys ++ [a])).2.2
          = if O pat (ys ++ [a]) m then (ys.length + 1 - (m + 1)) :: (loop pat ys).2.2
            else (loop pat ys).2.2 := by
        rw [hsnoc, step_snd_snd2, hlen1, ← hsnoc, hone' m (by omega)]
        rw [show (ys.length + 1 - pat.length) = (ys.length + 1 - (m + 1)) from by rw [hm]]
      rw [h22, hitsModel_snoc, ← hhits]
      split
      · rw [List.reverse_cons]
      · rw [List.append_nil]
    exact ⟨hex', hone', hhits'⟩

/-! ## Relating the model to the reference -/

private theorem O_bridge (pat : Bytes) (text : Bytes) (m i : Nat) (hm : pat.length = m + 1)
    (hi : i + m + 1 ≤ text.length) :
    O pat (text.take (i + m + 1)) m =
      (decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1)) := by
  unfold O mism winPairs
  have hl : (text.take (i + m + 1)).length = i + m + 1 := List.length_take_of_le hi
  have h1 : pat.take (m + 1) = pat :=
    List.take_of_length_le (by omega : pat.length ≤ m + 1)
  have h2 : (text.take (i + m + 1)).drop ((text.take (i + m + 1)).length - (m + 1)) =
      (text.drop i).take (m + 1) := by
    rw [hl, show i + m + 1 - (m + 1) = i from by omega, List.take_drop]
    rfl
  have hl2 : pat.zip ((text.drop i).take (m + 1)) = pat.zip (text.drop i) := by
    rw [← zip_take_length pat (text.drop i), show m + 1 = pat.length from (hm).symm]
  rw [h2, h1, hl2, hl]
  rw [show decide (m + 1 ≤ i + m + 1) = true from decide_eq_true (by omega), Bool.true_and]

private theorem O_take (pat : Bytes) (text : Bytes) (m t : Nat) (hm : pat.length = m + 1)
    (ht : t < text.length) :
    O pat (text.take (t + 1)) m =
      (decide (m + 1 ≤ t + 1) &&
        decide (((pat.zip (text.drop (t + 1 - (m + 1)))).filter fun (a, b) => a != b).length ≤ 1)) := by
  rcases Nat.lt_or_ge (t + 1) (m + 1) with hlt | hge
  · unfold O mism
    rw [show decide (m + 1 ≤ (text.take (t + 1)).length) = false from
      decide_eq_false (fun hc => by rw [List.length_take] at hc; omega)]
    rw [show decide (m + 1 ≤ t + 1) = false from decide_eq_false (by omega)]
    exact (Bool.false_and _).symm
  · have heq : t + 1 - (m + 1) + m + 1 = t + 1 := by omega
    have h1 : O pat (text.take (t + 1)) m =
        O pat (text.take (t + 1 - (m + 1) + m + 1)) m := by
      rw [heq]
    rw [h1, O_bridge pat text m (t + 1 - (m + 1)) hm (by omega)]
    rw [show decide (m + 1 ≤ t + 1) = true from decide_eq_true (by omega), Bool.true_and]

private theorem reindex (n m : Nat) (f : Nat → Bool) (h : ∀ t, t < n → t < m → f t = false) :
    ((List.range n).filter f).map (fun t => t + 1 - (m + 1)) =
      (List.range (n - m)).filter (fun i => f (i + m)) := by
  induction n generalizing m with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.map_append]
      have h1 : List.map (fun t => t + 1 - (m + 1)) (List.filter f [n]) =
          if f n then [n + 1 - (m + 1)] else [] := by
        rw [List.filter_cons, List.filter_nil]
        by_cases hf : f n = true
        · rw [if_pos hf, List.map_cons, List.map_nil, if_pos hf]
        · rw [if_neg hf, List.map_nil, if_neg hf]
      rw [h1, ih m (fun t ht1 ht2 => h t (by omega) ht2)]
      by_cases hm : n < m
      · have hfn : f n = false := h n (by omega) hm
        rw [hfn]
        have hnm : n + 1 - m = 0 := by omega
        have hnm2 : n - m = 0 := by omega
        rw [hnm, hnm2, List.range_zero, List.filter_nil]
        simp
      · have h1' : n + 1 - m = n - m + 1 := by omega
        have hm' : m ≤ n := by omega
        rw [h1', List.range_succ, List.filter_append,
          show n + 1 - (m + 1) = n - m from by omega]
        by_cases hfn : f n = true <;> simp [hfn, Nat.sub_add_cancel hm']

private theorem filter_range_le (k n : Nat) (g : Nat → Bool)
    (h : ∀ i, k ≤ i → g i = false) (hk : k ≤ n) :
    (List.range k).filter g = (List.range n).filter g := by
  induction n generalizing k with
  | zero =>
      have : k = 0 := by omega
      subst this
      rfl
  | succ n ih =>
      rcases Nat.lt_or_ge k (n + 1) with hlt | hge
      · have hk2 : k ≤ n := by omega
        have ih' := ih k (fun i hi => h i (by omega)) (by omega)
        have h1 : (List.range (n + 1)).filter g =
            (List.range n).filter g ++ (if g n then [n] else []) := by
          rw [List.range_succ, List.filter_append]
          cases hgn : g n <;> simp [hgn]
        rw [ih', h1, h n hk2]
        simp
      · have : k = n + 1 := by omega
        subst this
        rfl

private theorem hitsModel_reference (pat : Bytes) (text : Bytes) (m : Nat)
    (hm : pat.length = m + 1) : hitsModel pat m text = reference pat text := by
  have hsmall : ∀ t, t < text.length → t < m → O pat (text.take (t + 1)) m = false := by
    intro t ht1 ht2
    unfold O mism
    rw [show decide (m + 1 ≤ (text.take (t + 1)).length) = false from
      decide_eq_false (fun hc => by rw [List.length_take] at hc; omega), Bool.false_and]
  have h1 : hitsModel pat m text =
      (List.range (text.length - m)).filter (fun i => O pat (text.take (i + m + 1)) m) := by
    unfold hitsModel
    rw [reindex text.length m (fun t => O pat (text.take (t + 1)) m) hsmall]
  have h2 : ∀ i, i < text.length - m →
      O pat (text.take (i + m + 1)) m =
        (decide (i + m + 1 ≤ text.length) &&
          decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1)) := by
    intro i hi
    have hi' : i + m + 1 ≤ text.length := by omega
    rw [O_take pat text m (i + m) hm (by omega)]
    rw [show (i + m + 1) - (m + 1) = i from by omega,
      show decide (m + 1 ≤ i + m + 1) = true from decide_eq_true (by omega),
      decide_eq_true hi']
  have h3 : (List.range (text.length - m)).filter (fun i => O pat (text.take (i + m + 1)) m) =
      (List.range (text.length - m)).filter (fun i =>
        decide (i + m + 1 ≤ text.length) &&
          decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1)) := by
    refine List.filter_congr (fun i hi => ?_)
    rw [List.mem_range] at hi
    rw [h2 i hi]
  have h4 : (List.range (text.length - m)).filter (fun i =>
      decide (i + m + 1 ≤ text.length) &&
        decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1)) =
      (List.range (text.length + 1)).filter (fun i =>
        decide (i + m + 1 ≤ text.length) &&
          decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1)) := by
    refine filter_range_le (text.length - m) (text.length + 1)
      (fun i => decide (i + m + 1 ≤ text.length) &&
        decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1))
      (fun i hi => by
        rw [show decide (i + m + 1 ≤ text.length) = false from decide_eq_false (by omega)]
        exact Bool.false_and _)
      (by omega)
  have h5 : (List.range (text.length + 1)).filter (fun i =>
      decide (i + m + 1 ≤ text.length) &&
        decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1)) =
      reference pat text := by
    unfold reference
    refine List.filter_congr (fun i hi => ?_)
    rw [List.mem_range] at hi
    show (decide (i + m + 1 ≤ text.length) &&
        decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1))
      = (decide (i + pat.length ≤ text.length) &&
        decide (((pat.zip (text.drop i)).filter fun (a, b) => a != b).length ≤ 1))
    rw [hm]
    rfl
  rw [h1, h3, h4, h5]

/-! ## Soundness -/

private theorem filter_true {α : Type} (l : List α) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [List.filter_cons, ih]

theorem sound : Goal := by
  intro pat text hlen
  by_cases hnil : pat = []
  · subst hnil
    have hs : search ([] : Bytes) text = List.range (text.length + 1) := by
      unfold search
      simp
    have hr : reference ([] : Bytes) text = List.range (text.length + 1) := by
      unfold reference
      refine Eq.trans (List.filter_congr
        (p := fun i => decide (i + List.length ([] : Bytes) ≤ text.length) &&
          decide (((([] : Bytes).zip (List.drop i text)).filter fun x => x.1 != x.snd).length ≤ 1))
        (q := fun _ => true) (l := List.range (text.length + 1)) (fun i hi => ?_))
        (filter_true (List.range (text.length + 1)))
      rw [List.mem_range] at hi
      refine Bool.and_eq_true_iff.mpr ⟨decide_eq_true (by rw [show List.length ([] : Bytes) = 0 from rfl]; omega), ?_⟩
      rw [show ([] : Bytes).zip (List.drop i text) = [] from rfl, List.filter_nil, List.length_nil]
      exact decide_eq_true (by omega)
    rw [hs, hr]
  · have hpl : 0 < pat.length := by
      cases pat with
      | nil => exact absurd rfl hnil
      | cons a as => simp
    have h0 : pat.length - 1 < 64 := by omega
    rw [search_eq_loop pat text hnil]
    rw [(bits_lemma pat (pat.length - 1) (by omega) h0 text).2.2]
    exact hitsModel_reference pat text (pat.length - 1) (by omega)

end TinylambOpt.ResearchProofs.HammingOneSearch
