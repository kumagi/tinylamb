import Lean
import TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved (88 claims, including the following)

- `AppendFields.AddressValidity` — All present coordinates refer to an actual array
  cell, possibly a NULL one.
- `AppendFields.Boundary` — `∀ s, 0 < s.right.length → selectPos model s s.left.length
  = some (none,some 0)`
- `AppendFields.Cardinality` — Avoid materializing UNNEST when only its row count is
  consumed.

Refuted by counterexample (12 claims, including the following)

- `AppendFields.PayloadDedup` — Can DISTINCT on output payloads be dropped if offsets
  are distinct?
- `Cartesian.PayloadDedup` — Can DISTINCT on output payloads be dropped if offsets are
  distinct?
- `EvenOffsets.PayloadDedup` — Can DISTINCT on output payloads be dropped if offsets
  are distinct?
-/
open Lean Elab Term

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option linter.unnecessarySimpa false

elab "pFind" : term => do
  let env ← getEnv
  let cs := env.constants.toList.filter (fun (n, _) => n.toString.endsWith "MatchValue.findMatch")
  match cs with
  | [] => throwError "not found"
  | (n,_)::_ => return mkConst n

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

/-! Generic contract implications for the ten array-offset models. -/

theorem cardinality_of_ordinal (m : Model) (h : ordinal m) : cardinality m := by
  intro s
  have h1 := h s (m.count s)
  simp [selectPos] at h1
  have h2 := h s (m.rows s).length
  have hlen : (m.rows s)[(m.rows s).length]? = none :=
    List.getElem?_eq_none (by omega)
  rw [hlen] at h2
  by_cases hlt : (m.rows s).length < m.count s
  · simp [selectPos, hlt] at h2
  · omega

theorem leftOuterCount_of_cardinality (m : Model) (h : cardinality m) : leftOuterCount m := by
  intro s
  rw [h s]
  unfold outer
  by_cases he : (m.rows s) = []
  · simp [he]
  · have hpos : 0 < (m.rows s).length := by
      cases hs : m.rows s with
      | nil => contradiction
      | cons a t => simp [hs]
    simp [he]
    omega

theorem lateralOffset_of_ordinal (m : Model) (h : ordinal m) : lateralOffset m := by
  have hc : cardinality m := cardinality_of_ordinal m h
  intro ss
  induction ss with
  | nil =>
    intro i
    rfl
  | cons s ss ih =>
    intro i
    by_cases hi : i < m.count s
    · simp [skipParents, hi]
      rw [h s i]
      rw [List.getElem?_append]
      have hilen : i < (m.rows s).length := by rw [← hc s]; exact hi
      simp only [if_pos hilen]
    · simp [skipParents, hi]
      rw [ih]
      rw [List.getElem?_append]
      have hge : (m.rows s).length ≤ i := by rw [← hc s]; omega
      have hsub : i - (m.rows s).length = i - m.count s := by rw [hc s]
      simp only [if_neg (Nat.not_lt.mpr hge)]
      rw [hsub]

theorem lateSlice_of_ordinal (m : Model) (h : ordinal m) : lateSlice m := by
  have hc : cardinality m := cardinality_of_ordinal m h
  intro s off n
  apply List.ext_getElem?
  intro k
  have hL : (((List.range (min n (m.count s - off))).map (fun j => m.address s (off + j)))[k]?) =
      if k < min n (m.count s - off) then some (m.address s (off + k)) else none := by
    by_cases hk' : k < min n (m.count s - off) <;> simp [hk']
  have hR : (((m.rows s).drop off).take n)[k]? =
      if k < n then (m.rows s)[off + k]? else none := by
    by_cases hkn' : k < n <;> simp [hkn', List.getElem?_take, List.getElem?_drop]
  rw [hL, hR]
  by_cases hk : k < min n (m.count s - off)
  · have hk_n : k < n := (Nat.lt_min.mp hk).1
    have hidx : off + k < m.count s := by
      have hk_off : k < m.count s - off := (Nat.lt_min.mp hk).2
      omega
    have ho := h s (off + k)
    simp [selectPos, hidx] at ho
    simp [hk, hk_n, ho]
  · rw [if_neg hk]
    by_cases hkn : k < n
    · have hko : m.count s - off ≤ k := Nat.le_of_not_gt (by
        intro hlt
        exact hk (Nat.lt_min.mpr ⟨hkn, hlt⟩))
      have hnone : (m.rows s)[off + k]? = none := by
        apply List.getElem?_eq_none
        rw [(hc s).symm]
        omega
      rw [if_pos hkn, hnone]
    · rw [if_neg hkn]





private theorem zip_rows_getElem (s : Input) (k : Nat) :
    (ZipShortest.model.rows s)[k]? =
      if k < min s.left.length s.right.length then some (both k k) else none := by
  by_cases hk : k < min s.left.length s.right.length
  · have hkl : k < s.left.length := Nat.lt_of_lt_of_le hk (Nat.min_le_left _ _)
    have hkr : k < s.right.length := Nat.lt_of_lt_of_le hk (Nat.min_le_right _ _)
    simp [ZipShortest.model, ids, List.zip_eq_zipWith, List.getElem?_zipWith, hk, hkl, hkr]
  · simp [ZipShortest.model, ids, List.zip_eq_zipWith, List.getElem?_zipWith, hk]

private theorem indexIf_eq_some_iff (n i : Nat) : indexIf n i = some i ↔ i < n := by
  unfold indexIf
  by_cases h : i < n <;> simp [h]

private theorem mem_indexIf {n i j : Nat} (h : j ∈ indexIf n i) : j < n := by
  unfold indexIf at h
  split at h
  · simp_all
  · simp at h

private theorem cart_len (l r : Nat) :
    ((List.range l).flatMap (fun a => (List.range r).map (both a))).length = l * r := by
  induction l with
  | zero => simp
  | succ l ih =>
    rw [List.range_succ, Nat.succ_mul, List.flatMap_append, List.length_append, ih]
    simp

private theorem cart_getElem (l r i : Nat) :
    ((List.range l).flatMap (fun a => (List.range r).map (both a)))[i]? =
      if i < l * r then some (both (i / r) (i % r)) else none := by
  induction l generalizing i with
  | zero => simp
  | succ l ih =>
    rw [List.range_succ, Nat.succ_mul, List.flatMap_append, List.getElem?_append]
    have hlen : ((List.range l).flatMap (fun a => (List.range r).map (both a))).length = l * r := cart_len l r
    rw [hlen]
    by_cases hi : i < l * r
    · rw [ite_eq_left hi, ite_eq_left (by omega)]
      have hir := ih i
      rw [hir]
      simp [hi]
    · rw [ite_eq_right hi]
      rw [List.flatMap_cons]
      have htail : i - l * r < r ↔ i < l * r + r := by omega
      by_cases hit : i - l * r < r
      · rw [ite_eq_left (htail.1 hit)]
        have hr : 0 < r := by omega
        have hdiv : i / r = l ∧ i % r = i - l * r := by
          rw [Nat.div_mod_unique hr]
          constructor
          · rw [Nat.mul_comm r l]
            omega
          · exact hit
        have hri : (List.range r)[i - l * r]? = some (i - l * r) := List.getElem?_range hit
        simp [hri, hdiv.1, hdiv.2]
      · rw [ite_eq_right]
        · simp [Nat.le_of_not_gt hit]
        · intro hi'
          exact hit (htail.2 hi')

private theorem block_count (a i r : Nat) :
    ((List.range r).map (both a)).countP (fun p => p.1 == some i) = if a = i then r else 0 := by
  induction r with
  | zero => simp
  | succ r ih =>
    rw [List.range_succ, List.map_append, List.countP_append, ih]
    by_cases hai : a = i <;> simp [hai, both]

private theorem block_count' (a i r : Nat) :
    List.countP ((fun p => p.1 == some i) ∘ both a) (List.range r) = if a = i then r else 0 := by
  simpa [Function.comp_def] using block_count a i r

private theorem cart_countP_zero (l r i : Nat) (hi : ¬ i < l) :
    ((List.range l).flatMap (fun a => (List.range r).map (both a))).countP
      (fun p => p.1 == some i) = 0 := by
  induction l with
  | zero => simp
  | succ l ih =>
    rw [List.range_succ, List.flatMap_append, List.countP_append]
    have hi' : ¬ i < l := by omega
    rw [ih hi']
    simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.countP_append, List.countP_map, block_count']
    have hal : l ≠ i := by omega
    simp [hal]

private theorem cart_countP (l r i : Nat) (hi : i < l) :
    ((List.range l).flatMap (fun a => (List.range r).map (both a))).countP
      (fun p => p.1 == some i) = r := by
  induction l with
  | zero => omega
  | succ l ih =>
    rw [List.range_succ, List.flatMap_append, List.countP_append]
    simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.countP_append, List.countP_map, block_count']
    by_cases hil : i < l
    · rw [ih hil]
      have hli : l ≠ i := by omega
      simp [hli]
    · have hil_eq : l = i := by omega
      rw [cart_countP_zero l r i hil]
      simp [hil_eq]

private theorem liveIdx_length (L : Arr) :
    ∀ n, n ≤ L.length →
      ((List.range n).filter (fun i => (L[i]?.join).isSome)).length =
        (L.take n).countP Option.isSome
  | 0, _ => by simp
  | n + 1, hn => by
    have hn' : n ≤ L.length := by omega
    rw [List.range_succ, List.filter_append, List.length_append, liveIdx_length L n hn']
    rw [List.take_succ, List.countP_append]
    cases hopt : L[n]? with
    | none => simp [hopt]
    | some v => cases v <;> simp [hopt]

private theorem live_length (L : Arr) : (live L).length = L.countP Option.isSome := by
  have h := liveIdx_length L L.length (by omega)
  simpa [live, ids] using h

private theorem even_filter_even (k : Nat) :
    (List.range (2 * k)).filter (fun i => i % 2 == 0) = (List.range k).map (fun i => 2 * i) := by
  induction k with
  | zero => simp
  | succ k ih =>
    rw [show 2 * (k + 1) = 2 * k + 2 by omega, List.range_add]
    rw [List.filter_append, ih]
    have hfilter : (List.map (fun x => 2 * k + x) (List.range 2)).filter (fun i => i % 2 == 0) = [2 * k] := by
      have h0 : (2 * k) % 2 = 0 := by simpa using Nat.mul_mod_left 2 k
      have h1 : (2 * k + 1) % 2 = 1 := by
        rw [Nat.add_comm, Nat.add_mul_mod_self_left]
      simp only [List.range_succ, List.range_zero, List.map_cons, List.map_nil]
      simp [h0, h1]
    rw [hfilter]
    rw [List.range_succ, List.map_append]
    simp

private theorem even_filter_odd (k : Nat) :
    (List.range (2 * k + 1)).filter (fun i => i % 2 == 0) = (List.range (k + 1)).map (fun i => 2 * i) := by
  rw [List.range_succ, List.filter_append, even_filter_even]
  rw [List.range_succ, List.map_append]
  simp

private theorem even_filter_range (n : Nat) :
    (List.range n).filter (fun i => i % 2 == 0) = (List.range ((n + 1) / 2)).map (fun i => 2 * i) := by
  rcases Nat.mod_two_eq_zero_or_one n with hzero | hone
  · have hn : n = 2 * (n / 2) := by omega
    rw [hn]
    rw [show (2 * (n / 2) + 1) / 2 = n / 2 by omega]
    simpa using even_filter_even (n / 2)
  · have hn : n = 2 * (n / 2) + 1 := by omega
    rw [hn]
    rw [show (2 * (n / 2) + 1 + 1) / 2 = n / 2 + 1 by omega]
    simpa using even_filter_odd (n / 2)

private theorem rows_even (s : Input) :
    EvenOffsets.model.rows s = (List.range ((s.left.length + 1) / 2)).map (fun i => lpos (2 * i)) := by
  simp [EvenOffsets.model, ids, even_filter_range]



private theorem lead_rows_getElem (s : Input) (k : Nat) :
    (LeadPair.model.rows s)[k]? =
      if k < min s.left.length (s.right.length - 1) then some (both k (k + 1)) else none := by
  by_cases hk : k < min s.left.length (s.right.length - 1)
  · have hkL : k < s.left.length := (Nat.lt_min.mp hk).1
    have hkR1 : k < s.right.length - 1 := (Nat.lt_min.mp hk).2
    have hkR : k + 1 < s.right.length := by omega
    simp [LeadPair.model, ids, List.zip_eq_zipWith, List.getElem?_zipWith,
      hk, hkL, hkR, Nat.add_comm]
  · simp [LeadPair.model, ids, List.zip_eq_zipWith, List.getElem?_zipWith, hk]

private theorem lead_len (s : Input) :
    (LeadPair.model.rows s).length = min s.left.length (s.right.length - 1) := by
  simp [LeadPair.model, ids, List.length_zip, List.length_tail]

private theorem pf_nil (R : Arr) (n : Nat) : pFind R [] n = emptyPos := by rfl
private theorem pf_none (R : Arr) (xs : List (Option Int × Nat)) (i n : Nat) :
    pFind R ((none,i)::xs) n = pFind R xs n := by rfl
private theorem pf_some (R : Arr) (v : Int) (i : Nat) (xs : List (Option Int × Nat)) (n : Nat) :
    pFind R ((some v,i)::xs) n =
      if n < R.count (some v) then
        (some i, ((ids R).filter (fun j => R[j]? == some (some v)))[n]?)
      else pFind R xs (n - R.count (some v)) := by rfl

private def groups (R : Arr) : List (Option Int × Nat) → List Pos
  | [] => []
  | (none,_)::xs => groups R xs
  | (some v,i)::xs =>
      ((ids R).filter (fun j => R[j]? == some (some v))).map (fun j => both i j) ++ groups R xs

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  cases h1 : (a == b) <;> cases h2 : (b == a) <;> simp_all [beq_iff_eq]

private theorem matchIdx_length_aux (R : Arr) (v : Int) :
    ∀ n, n ≤ R.length →
      ((List.range n).filter (fun j => R[j]? == some (some v))).length =
        (R.take n).count (some v)
  | 0, _ => by simp
  | n + 1, hn => by
    have hn' : n ≤ R.length := by omega
    rw [List.range_succ, List.filter_append, List.length_append, matchIdx_length_aux R v n hn']
    rw [List.take_succ, List.count_append]
    cases hRj : R[n]? with
    | none => simp [hRj]
    | some w =>
      cases w with
      | none => simp [hRj]
      | some z => by_cases hzv : z = v <;> simp [hRj, hzv, beq_comm_int]

private theorem offsets_length (R : Arr) (v : Int) :
    ((ids R).filter (fun j => R[j]? == some (some v))).length = R.count (some v) := by
  have h := matchIdx_length_aux R v R.length (by omega)
  simpa [ids] using h

private theorem live_filter_none (R : Arr) :
    ((live R).filter (fun j => some none == R[j]?)) = [] := by
  rw [List.filter_eq_nil_iff]
  intro j hj
  simp [live, ids, List.mem_filter] at hj
  cases hRj : R[j]? with
  | none => simp [hRj] at hj
  | some w =>
    cases w with
    | none =>
      have hf : (R[j]?.join).isSome = false := by rw [hRj]; rfl
      rw [hf] at hj
      simp at hj
    | some z => simp [hRj]

private theorem group_eq_offsets (R : Arr) (v : Int) :
    ((live R).filter (fun j => some (some v) == R[j]?)) =
      ((ids R).filter (fun j => R[j]? == some (some v))) := by
  simp only [live, ids, List.filter_filter]
  apply List.filter_congr
  intro j hj
  cases hRj : R[j]? with
  | none => simp [hRj]
  | some w =>
    cases w with
    | none => simp [hRj]
    | some z => by_cases hzv : z = v <;> simp [hRj, hzv, beq_comm_int]

private theorem rows_groups_aux (R L : Arr) (start : Nat) :
    (ids L).flatMap (fun k => ((live R).filter (fun j => L[k]? == R[j]?)).map (both (start + k))) =
      groups R (L.zipIdx start) := by
  induction L generalizing start with
  | nil => simp [ids, groups]
  | cons a as ih =>
    have hids : ids (a :: as) = 0 :: (ids as).map (fun i => i + 1) := by
      simpa [ids] using (List.range_succ_eq_map (n := as.length))
    rw [hids, List.flatMap_cons, List.flatMap_map]
    simp only [Nat.zero_add, List.getElem?_cons_zero]
    have hshift : (fun k => both (start + (k + 1))) = (fun k => both ((start + 1) + k)) := by
      funext k
      funext j
      unfold both
      apply Prod.ext <;> simp <;> omega
    have hfun2 : (fun k => List.map (both (start + (k + 1)))
          (List.filter (fun j => as[k]? == R[j]?) (live R))) =
        (fun k => List.map (both ((start + 1) + k))
          (List.filter (fun j => as[k]? == R[j]?) (live R))) := by
      funext k
      congr 1
      exact congrFun hshift k
    cases a with
    | none =>
      rw [live_filter_none]
      simp only [List.map_nil, List.flatMap_nil, List.nil_append]
      change List.flatMap (fun k => List.map (both (start + (k + 1)))
          (List.filter (fun j => as[k]? == R[j]?) (live R))) (ids as) = groups R (as.zipIdx (start + 1))
      rw [hfun2]
      exact ih (start := start + 1)
    | some v =>
      rw [group_eq_offsets]
      rw [show groups R (List.zipIdx (some v :: as) start) =
        ((ids R).filter (fun j => R[j]? == some (some v))).map (both (start + 0)) ++
          groups R (as.zipIdx (start + 1)) by simp [groups]]
      simp only [List.getElem?_cons_succ]
      rw [hfun2]
      rw [ih (start := start + 1)]

private theorem rows_groups (s : Input) : MatchValue.model.rows s = groups s.right (s.left.zipIdx) := by
  simpa [MatchValue.model] using rows_groups_aux s.right s.left 0

private theorem count_groups_aux (R L : Arr) (start : Nat) :
    (groups R (L.zipIdx start)).length =
      ((L.filterMap id).map (fun v => R.count (some v))).sum := by
  induction L generalizing start with
  | nil => simp [groups]
  | cons a as ih =>
    cases a with
    | none =>
      simp [groups, ih (start := start + 1)]
    | some v =>
      simp [groups, ih (start := start + 1), offsets_length]

private theorem count_groups (s : Input) :
    MatchValue.model.count s = (groups s.right (s.left.zipIdx)).length := by
  simpa [MatchValue.model] using (count_groups_aux s.right s.left 0).symm

private theorem pf_getElem (R : Arr) : ∀ (xs : List (Option Int × Nat)) n,
    n < (groups R xs).length → (groups R xs)[n]? = some (pFind R xs n)
  | [], n, h => by simp [groups] at h
  | (none, i)::xs, n, h => by
    change (groups R xs)[n]? = some (pFind R xs n)
    exact pf_getElem R xs n h
  | (some v, i)::xs, n, h => by
    have hlen : ((ids R).filter (fun j => R[j]? == some (some v))).length = R.count (some v) := offsets_length R v
    simp [groups] at h
    change ((((ids R).filter (fun j => R[j]? == some (some v))).map (fun j => both i j)) ++ groups R xs)[n]? = some (pFind R ((some v, i)::xs) n)
    by_cases hn : n < R.count (some v)
    · rw [pf_some]
      simp only [if_pos hn]
      rw [List.getElem?_append]
      simp only [List.length_map]
      have hnG : n < ((ids R).filter (fun j => R[j]? == some (some v))).length := by omega
      rw [ite_eq_left hnG]
      cases hG : ((ids R).filter (fun j => R[j]? == some (some v)))[n]? with
      | none =>
        have hx : ((ids R).filter (fun j => R[j]? == some (some v))).length ≤ n := by
          rw [← List.getElem?_eq_none_iff]
          exact hG
        omega
      | some j =>
        simp [hG, both]
    · rw [pf_some]
      simp only [if_neg hn]
      rw [List.getElem?_append]
      simp only [List.length_map]
      have hnG : ¬ n < ((ids R).filter (fun j => R[j]? == some (some v))).length := by omega
      rw [ite_eq_right hnG]
      have hrest : n - R.count (some v) < (groups R xs).length := by omega
      rw [hlen]
      exact pf_getElem R xs (n - R.count (some v)) hrest

private theorem groups_valid_aux (R L : Arr) (start : Nat) :
    ∀ p ∈ groups R (L.zipIdx start),
      (∀ i ∈ p.1, i < start + L.length) ∧ (∀ j ∈ p.2, j < R.length) := by
  induction L generalizing start with
  | nil => simp [groups]
  | cons a as ih =>
    cases a with
    | none =>
      rw [show groups R (List.zipIdx (none :: as) start) = groups R (List.zipIdx as (start + 1)) by rfl]
      intro p hp
      have h := ih (start := start + 1) p hp
      constructor
      · intro i hi
        simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using h.1 i hi
      · exact h.2
    | some v =>
      rw [show groups R (List.zipIdx (some v :: as) start) =
        ((ids R).filter (fun j => R[j]? == some (some v))).map (fun j => both start j) ++
          groups R (List.zipIdx as (start + 1)) by rfl]
      intro p hp
      rcases List.mem_append.mp hp with hp | hp
      · rcases List.mem_map.mp hp with ⟨j, hj, rfl⟩
        have hjl : j < R.length := by
          have hmem : j ∈ ids R := (List.mem_filter.mp hj).1
          exact List.mem_range.mp hmem
        constructor
        · intro ii hii
          simp [both] at hii
          subst ii
          have hlen : (some v :: as).length = as.length + 1 := by simp
          rw [hlen]
          omega
        · intro kk hkk
          simp [both] at hkk
          subst kk
          exact hjl
      · simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (start := start + 1) p hp

theorem ord_LeftElements : LeftElements.Ordinal.Goal := by
  intro s i
  by_cases hi : i < s.left.length
  · simp [selectPos, LeftElements.model, ids, hi]
  · simp [selectPos, LeftElements.model, ids, hi]

theorem addr_LeftElements : LeftElements.AddressValidity.Goal := by
  intro s p hp
  simp [LeftElements.model, ids, List.mem_map] at hp
  rcases hp with ⟨i, hi, rfl⟩
  simp [lpos]
  exact hi

theorem keepNull_LeftElements : LeftElements.KeepNull.Goal := by
  intro s i hi
  have hil : i < s.left.length := (List.getElem?_eq_some_iff.mp hi).choose
  simp [LeftElements.model, ids, List.mem_map]
  exact ⟨i, hil, rfl⟩

theorem noRight_LeftElements : LeftElements.NoRight.Goal := by
  intro s p hp
  simp [LeftElements.model, ids, List.mem_map] at hp
  rcases hp with ⟨i, hi, rfl⟩
  rfl

theorem ord_ReverseOffsets : ReverseOffsets.Ordinal.Goal := by
  intro s i
  by_cases hi : i < s.left.length
  · have hrev := List.getElem?_reverse (l := ids s.left) (i := i) (by simpa [ids] using hi)
    have hlt : s.left.length - 1 - i < s.left.length := by omega
    simp [selectPos, ReverseOffsets.model, ids, hi, hrev, hlt]
  · simp [selectPos, ReverseOffsets.model, ids, hi]

theorem addr_ReverseOffsets : ReverseOffsets.AddressValidity.Goal := by
  intro s p hp
  simp [ReverseOffsets.model, ids, List.mem_map, List.mem_reverse] at hp
  rcases hp with ⟨i, hi, rfl⟩
  simp [lpos]
  exact hi

theorem reverseFirst_ReverseOffsets : ReverseOffsets.ReverseFirst.Goal := by
  intro s hn
  have hlt : s.left.length - 1 < s.left.length := by omega
  simp [ReverseOffsets.model, ids, List.head?_reverse, List.getLast?_eq_getElem?, hlt]

theorem lastZero_ReverseOffsets : ReverseOffsets.LastZero.Goal := by
  intro s hn
  simp [ReverseOffsets.model, ids, List.getLast?_reverse, List.head?_eq_getElem?, hn]

theorem ord_ZipShortest : ZipShortest.Ordinal.Goal := by
  intro s i
  change (if i < min s.left.length s.right.length then some (both i i) else none) =
    (ZipShortest.model.rows s)[i]?
  rw [zip_rows_getElem]

theorem addr_ZipShortest : ZipShortest.AddressValidity.Goal := by
  intro s p hp
  rw [List.mem_iff_getElem?] at hp
  rcases hp with ⟨k, hk⟩
  rw [zip_rows_getElem] at hk
  by_cases hkl : k < min s.left.length s.right.length
  · simp [hkl] at hk
    subst p
    simp [both]
    constructor
    · exact Nat.lt_of_lt_of_le hkl (Nat.min_le_left _ _)
    · exact Nat.lt_of_lt_of_le hkl (Nat.min_le_right _ _)
  · simp [hkl] at hk

theorem diagonal_ZipShortest : ZipShortest.Diagonal.Goal := by
  intro s i j hp
  rw [List.mem_iff_getElem?] at hp
  rcases hp with ⟨k, hk⟩
  rw [zip_rows_getElem] at hk
  by_cases hkl : k < min s.left.length s.right.length
  · simp [hkl, both] at hk
    omega
  · simp [hkl] at hk

theorem emptyRight_ZipShortest : ZipShortest.EmptyRight.Goal := by
  intro a
  simp [ZipShortest.model, ids]

theorem ord_ZipPad : ZipPad.Ordinal.Goal := by
  intro s i
  by_cases hi : i < max s.left.length s.right.length <;>
    simp [selectPos, ZipPad.model, hi]

theorem addr_ZipPad : ZipPad.AddressValidity.Goal := by
  intro s p hp
  simp [ZipPad.model, List.mem_map] at hp
  rcases hp with ⟨i, hi, rfl⟩
  simp [padded]
  constructor
  · intro j hj
    exact mem_indexIf hj
  · intro j hj
    exact mem_indexIf hj

theorem rightTail_ZipPad : ZipPad.RightTail.Goal := by
  intro s i hli hri
  rw [List.mem_iff_getElem?]
  refine ⟨i, ?_⟩
  have himax : i < max s.left.length s.right.length := by omega
  have hrow : (ZipPad.model.rows s)[i]? = some (padded s i) := by
    simp [ZipPad.model, himax]
  rw [hrow]
  congr 1
  simp [padded, indexIf, hli, hri]

theorem noDoublePad_ZipPad : ZipPad.NoDoublePad.Goal := by
  intro s hp
  rw [List.mem_iff_getElem?] at hp
  rcases hp with ⟨k, hk⟩
  have hkmax : k < max s.left.length s.right.length := by
    by_cases hle : k < max s.left.length s.right.length
    · exact hle
    · exfalso
      have hnone : (ZipPad.model.rows s)[k]? = none := by
        apply List.getElem?_eq_none
        simp [ZipPad.model]
        omega
      rw [hnone] at hk
      cases hk
  have hrow : (ZipPad.model.rows s)[k]? = some (padded s k) := by
    simp only [ZipPad.model, List.getElem?_map]
    have hrmax : (List.range (max s.left.length s.right.length))[k]? = some k :=
      List.getElem?_range hkmax
    simp [hrmax]
  rw [hrow] at hk
  simp only [Option.some.injEq] at hk
  by_cases hkl : k < s.left.length
  · simp [padded, indexIf, hkl, emptyPos] at hk
  · have hkr : k < s.right.length := by omega
    simp [padded, indexIf, hkl, hkr, emptyPos] at hk

theorem ord_Cartesian : Cartesian.Ordinal.Goal := by
  intro s i
  change (if i < s.left.length * s.right.length then some (both (i / s.right.length) (i % s.right.length)) else none) =
    (Cartesian.model.rows s)[i]?
  simpa [Cartesian.model, ids] using (cart_getElem s.left.length s.right.length i).symm

theorem addr_Cartesian : Cartesian.AddressValidity.Goal := by
  intro s p hp
  rw [List.mem_iff_getElem?] at hp
  rcases hp with ⟨k, hk⟩
  have hrows : (Cartesian.model.rows s)[k]? =
      if k < s.left.length * s.right.length then some (both (k / s.right.length) (k % s.right.length)) else none := by
    simpa [Cartesian.model, ids] using cart_getElem s.left.length s.right.length k
  rw [hrows] at hk
  by_cases hlt : k < s.left.length * s.right.length
  · simp [hlt] at hk
    subst p
    simp [both]
    have hr : 0 < s.right.length := by
      by_cases hz : s.right.length = 0
      · simp [hz] at hlt
      · omega
    constructor
    · rw [Nat.div_lt_iff_lt_mul hr]
      exact hlt
    · exact Nat.mod_lt _ hr
  · simp [hlt] at hk

theorem inverseOffset_Cartesian : Cartesian.InverseOffset.Goal := by
  intro s i j hi hj
  have hr : 0 < s.right.length := by omega
  have hlt : i * s.right.length + j < s.left.length * s.right.length := by
    have hle : (i + 1) * s.right.length ≤ s.left.length * s.right.length :=
      Nat.mul_le_mul_right s.right.length (Nat.succ_le_of_lt hi)
    rw [Nat.succ_mul] at hle
    omega
  have hdivmod : (i * s.right.length + j) / s.right.length = i ∧
      (i * s.right.length + j) % s.right.length = j := by
    rw [Nat.div_mod_unique hr]
    constructor
    · rw [Nat.mul_comm s.right.length i]
      exact Nat.add_comm _ _
    · exact hj
  simp [selectPos, Cartesian.model, hlt, hdivmod.1, hdivmod.2]

theorem factor_Cartesian : Cartesian.Factor.Goal := by
  intro s i hi
  rw [← List.countP_eq_length_filter]
  simpa [Cartesian.model, ids] using cart_countP s.left.length s.right.length i hi

theorem ord_NonNullOnly : NonNullOnly.Ordinal.Goal := by
  intro s i
  simp only [selectPos, NonNullOnly.model, List.getElem?_map]
  have hc : s.left.countP Option.isSome = (live s.left).length := (live_length s.left).symm
  by_cases hi : i < (live s.left).length
  · have hi' : i < s.left.countP Option.isSome := by rw [hc]; exact hi
    simp [hi', hi]
  · have hi' : ¬ i < s.left.countP Option.isSome := by rw [hc]; exact hi
    simp [hi', hi, List.getElem?_eq_none (Nat.le_of_not_gt hi)]

theorem addr_NonNullOnly : NonNullOnly.AddressValidity.Goal := by
  intro s p hp
  simp [NonNullOnly.model, List.mem_map] at hp
  rcases hp with ⟨i, hi, rfl⟩
  simp [lpos]
  simp [live, ids, List.mem_filter] at hi
  exact hi.1

theorem physicalIndex_NonNullOnly : NonNullOnly.PhysicalIndex.Goal := by
  intro s i
  constructor
  · intro hp
    simp [NonNullOnly.model, List.mem_map] at hp
    rcases hp with ⟨k, hk, heq⟩
    have hki : k = i := by simpa [lpos] using heq
    subst k
    simp [live, ids, List.mem_filter] at hk
    exact hk.2
  · intro hs
    have hil : i < s.left.length := by
      by_cases hlt : i < s.left.length
      · exact hlt
      · exfalso
        have hnone : s.left[i]? = none := List.getElem?_eq_none (Nat.le_of_not_gt hlt)
        rw [hnone] at hs
        cases hs
    have hs' : s.left[i].isSome = true := by simpa [hil] using hs
    simp [NonNullOnly.model, List.mem_map]
    refine ⟨i, ?_, rfl⟩
    simp [live, ids, List.mem_filter, hil, hs']

theorem dropNulls_NonNullOnly : NonNullOnly.DropNulls.Goal := by
  intro s
  change s.left.countP Option.isSome + List.count none s.left = s.left.length
  induction s.left with
  | nil => simp
  | cons x xs ih =>
    cases x with
    | none => simp; omega
    | some v => cases v <;> simp <;> omega

theorem ord_EvenOffsets : EvenOffsets.Ordinal.Goal := by
  intro s i
  change (if i < (s.left.length + 1) / 2 then some (lpos (2 * i)) else none) =
    (EvenOffsets.model.rows s)[i]?
  rw [rows_even]
  by_cases hi : i < (s.left.length + 1) / 2 <;> simp [hi]

theorem addr_EvenOffsets : EvenOffsets.AddressValidity.Goal := by
  intro s p hp
  rw [rows_even] at hp
  simp [List.mem_map] at hp
  rcases hp with ⟨i, hi, rfl⟩
  simp [lpos]
  have : 2 * i < s.left.length := by omega
  exact this

theorem noOdd_EvenOffsets : EvenOffsets.NoOdd.Goal := by
  intro s i hp
  rw [rows_even] at hp
  simp [List.mem_map] at hp
  rcases hp with ⟨k, hk, heq⟩
  simp [lpos] at heq
  omega

theorem lastEven_EvenOffsets : EvenOffsets.LastEven.Goal := by
  intro s hn
  rw [rows_even]
  rw [List.getLast?_eq_getElem?]
  simp only [List.length_map, List.length_range]
  have hpos : 0 < (s.left.length + 1) / 2 := by omega
  have hidx : (s.left.length + 1) / 2 - 1 = (s.left.length - 1) / 2 := by omega
  rw [hidx]
  have hlt : (s.left.length - 1) / 2 < (s.left.length + 1) / 2 := by omega
  rw [List.getElem?_eq_some_iff]
  exact ⟨by simpa using hlt, by simp⟩

theorem ord_AppendFields : AppendFields.Ordinal.Goal := by
  intro s i
  simp only [selectPos, AppendFields.model]
  by_cases hi : i < s.left.length
  · have hitot : i < s.left.length + s.right.length := by omega
    simp [ids, hi, hitot]
  · by_cases hitot : i < s.left.length + s.right.length
    · have hidx : i - s.left.length < s.right.length := by omega
      rw [List.getElem?_append]
      simp [ids, hi, hitot, hidx]
    · have hidxge : s.right.length ≤ i - s.left.length := by omega
      rw [List.getElem?_append]
      have hnone : (List.range s.right.length)[i - s.left.length]? = none :=
        List.getElem?_eq_none (by simpa using hidxge)
      simp [ids, hi, hitot, hnone]

theorem addr_AppendFields : AppendFields.AddressValidity.Goal := by
  intro s p hp
  simp [AppendFields.model, ids] at hp
  rcases hp with hp | hp
  · rcases hp with ⟨i, hi, rfl⟩
    simp [lpos]
    exact hi
  · rcases hp with ⟨j, hj, rfl⟩
    simp
    exact hj

theorem boundary_AppendFields : AppendFields.Boundary.Goal := by
  intro s hr
  have hlt : s.left.length < s.left.length + s.right.length := by omega
  simp [selectPos, AppendFields.model, hlt]

theorem leftPrefix_AppendFields : AppendFields.LeftPrefix.Goal := by
  intro s
  simp [AppendFields.model, ids, List.take_append]

theorem ord_LeadPair : LeadPair.Ordinal.Goal := by
  intro s i
  change (if i < min s.left.length (s.right.length - 1) then some (both i (i + 1)) else none) =
    (LeadPair.model.rows s)[i]?
  rw [lead_rows_getElem]

theorem card_LeadPair : LeadPair.Cardinality.Goal := by
  intro s
  exact (lead_len s).symm

theorem addr_LeadPair : LeadPair.AddressValidity.Goal := by
  intro s p hp
  rw [List.mem_iff_getElem?] at hp
  rcases hp with ⟨k, hk⟩
  rw [lead_rows_getElem] at hk
  by_cases hkl : k < min s.left.length (s.right.length - 1)
  · simp [hkl] at hk
    subst p
    simp [both]
    constructor
    · exact (Nat.lt_min.mp hkl).1
    · have : k < s.right.length - 1 := (Nat.lt_min.mp hkl).2
      omega
  · simp [hkl] at hk

theorem noFirstRight_LeadPair : LeadPair.NoFirstRight.Goal := by
  intro s i hp
  rw [List.mem_iff_getElem?] at hp
  rcases hp with ⟨k, hk⟩
  rw [lead_rows_getElem] at hk
  by_cases hkl : k < min s.left.length (s.right.length - 1)
  · simp [hkl, both] at hk
  · simp [hkl] at hk

theorem unitRight_LeadPair : LeadPair.UnitRight.Goal := by
  intro s hr
  apply List.ext_getElem?
  intro k
  rw [lead_rows_getElem]
  simp [hr]

theorem ord_MatchValue : MatchValue.Ordinal.Goal := by
  intro s i
  change (if i < MatchValue.model.count s then some (MatchValue.model.address s i) else none) =
    (MatchValue.model.rows s)[i]?
  rw [rows_groups]
  have hc := count_groups s
  by_cases hi : i < MatchValue.model.count s
  · rw [ite_eq_left hi]
    have hi' : i < (groups s.right (s.left.zipIdx)).length := by omega
    rw [pf_getElem s.right (s.left.zipIdx) i hi']
    simp only [MatchValue.model]
  · rw [ite_eq_right hi]
    have hnone : (groups s.right (s.left.zipIdx))[i]? = none := by
      apply List.getElem?_eq_none
      omega
    rw [hnone]

theorem allMatches_MatchValue : MatchValue.AllMatches.Goal := by
  intro n k v
  simp [MatchValue.model]

theorem addr_MatchValue : MatchValue.AddressValidity.Goal := by
  intro s p hp
  rw [rows_groups] at hp
  have h := groups_valid_aux s.right s.left 0 p hp
  constructor
  · intro i hi
    simpa using h.1 i hi
  · exact h.2

theorem noNullMatch_MatchValue : MatchValue.NoNullMatch.Goal := by
  intro s i j hp
  simp [MatchValue.model, ids, List.mem_flatMap, List.mem_map, List.mem_filter] at hp
  rcases hp with ⟨a, ha, a1, ⟨hjlive, heq⟩, hpos⟩
  simp [both] at hpos
  rcases hpos with ⟨hai, haj⟩
  subst i
  subst j
  simp [live, ids, List.mem_filter] at hjlive
  cases hRj : s.right[a1]? with
  | none => simp [hRj] at hjlive
  | some w =>
    cases w with
    | none =>
      have hf : (s.right[a1]?.join).isSome = false := by rw [hRj]; rfl
      rw [hf] at hjlive
      simp at hjlive
    | some v =>
      rw [← heq] at hRj
      rw [hRj]
      simp

theorem val_LeftElements : LeftElements.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [LeftElements.model, hsl]
  · intro i
    simp [selectPos, LeftElements.model, ← hsl]
    all_goals rfl

theorem val_ReverseOffsets : ReverseOffsets.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [ReverseOffsets.model, hsl]
  · intro i
    simp [selectPos, ReverseOffsets.model, ← hsl]
    all_goals rfl

theorem val_ZipShortest : ZipShortest.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [ZipShortest.model, hsl, hsr]
  · intro i
    simp [selectPos, ZipShortest.model, ← hsl, ← hsr]
    all_goals rfl

theorem val_ZipPad : ZipPad.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [ZipPad.model, hsl, hsr]
  · intro i
    simp [selectPos, ZipPad.model, padded, ← hsl, ← hsr]
    all_goals rfl

theorem val_Cartesian : Cartesian.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [Cartesian.model, hsl, hsr]
  · intro i
    simp [selectPos, Cartesian.model, ← hsl, ← hsr]
    all_goals rfl

theorem val_EvenOffsets : EvenOffsets.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [EvenOffsets.model, hsl]
  · intro i
    simp [selectPos, EvenOffsets.model, ← hsl]
    all_goals rfl

theorem val_AppendFields : AppendFields.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [AppendFields.model, hsl, hsr]
  · intro i
    simp [selectPos, AppendFields.model, ← hsl, ← hsr]
    all_goals rfl

theorem val_LeadPair : LeadPair.ValueBlind.Goal := by
  intro s t hsl hsr
  constructor
  · simp [LeadPair.model, hsl, hsr]
  · intro i
    simp [selectPos, LeadPair.model, ← hsl, ← hsr]
    all_goals rfl

theorem not_val_NonNullOnly : ¬ NonNullOnly.ValueBlind.Goal := by
  intro h
  have h0 := h ⟨[none], []⟩ ⟨[some 1], []⟩ (by rfl) (by rfl)
  have hne : NonNullOnly.model.count ⟨[none], []⟩ ≠ NonNullOnly.model.count ⟨[some 1], []⟩ := by native_decide
  exact hne h0.1

theorem not_val_MatchValue : ¬ MatchValue.ValueBlind.Goal := by
  intro h
  have h0 := h ⟨[none], [some 1]⟩ ⟨[some 1], [some 1]⟩ (by rfl) (by rfl)
  have hne : MatchValue.model.count ⟨[none], [some 1]⟩ ≠ MatchValue.model.count ⟨[some 1], [some 1]⟩ := by native_decide
  exact hne h0.1

theorem not_pay_LeftElements : ¬ LeftElements.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], []⟩
  revert h0
  native_decide

theorem not_pay_ReverseOffsets : ¬ ReverseOffsets.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], []⟩
  revert h0
  native_decide

theorem not_pay_ZipShortest : ¬ ZipShortest.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], [some 1, some 1]⟩
  revert h0
  native_decide

theorem not_pay_ZipPad : ¬ ZipPad.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], [some 1, some 1]⟩
  revert h0
  native_decide

theorem not_pay_Cartesian : ¬ Cartesian.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], [some 1]⟩
  revert h0
  native_decide

theorem not_pay_NonNullOnly : ¬ NonNullOnly.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], []⟩
  revert h0
  native_decide

theorem not_pay_EvenOffsets : ¬ EvenOffsets.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1, some 1, some 1], []⟩
  revert h0
  native_decide

theorem not_pay_AppendFields : ¬ AppendFields.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], []⟩
  revert h0
  native_decide

theorem not_pay_LeadPair : ¬ LeadPair.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1, some 1], [some 1, some 1, some 1]⟩
  revert h0
  native_decide

theorem not_pay_MatchValue : ¬ MatchValue.PayloadDedup.Goal := by
  intro h
  have h0 := h ⟨[some 1, some 1], [some 1, some 1]⟩
  revert h0
  native_decide

theorem cardinality_LeftElements : LeftElements.Cardinality.Goal := by exact cardinality_of_ordinal LeftElements.model ord_LeftElements

theorem ordinal_LeftElements : LeftElements.Ordinal.Goal := by exact ord_LeftElements

theorem lateraloffset_LeftElements : LeftElements.LateralOffset.Goal := by exact lateralOffset_of_ordinal LeftElements.model ord_LeftElements

theorem leftoutercount_LeftElements : LeftElements.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality LeftElements.model cardinality_LeftElements

theorem addressvalidity_LeftElements : LeftElements.AddressValidity.Goal := by exact addr_LeftElements

theorem payloaddedup_LeftElements : ¬ LeftElements.PayloadDedup.Goal := by exact not_pay_LeftElements

theorem lateslice_LeftElements : LeftElements.LateSlice.Goal := by exact lateSlice_of_ordinal LeftElements.model ord_LeftElements

theorem valueblind_LeftElements : LeftElements.ValueBlind.Goal := by exact val_LeftElements

theorem cardinality_ReverseOffsets : ReverseOffsets.Cardinality.Goal := by exact cardinality_of_ordinal ReverseOffsets.model ord_ReverseOffsets

theorem ordinal_ReverseOffsets : ReverseOffsets.Ordinal.Goal := by exact ord_ReverseOffsets

theorem lateraloffset_ReverseOffsets : ReverseOffsets.LateralOffset.Goal := by exact lateralOffset_of_ordinal ReverseOffsets.model ord_ReverseOffsets

theorem leftoutercount_ReverseOffsets : ReverseOffsets.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality ReverseOffsets.model cardinality_ReverseOffsets

theorem addressvalidity_ReverseOffsets : ReverseOffsets.AddressValidity.Goal := by exact addr_ReverseOffsets

theorem payloaddedup_ReverseOffsets : ¬ ReverseOffsets.PayloadDedup.Goal := by exact not_pay_ReverseOffsets

theorem lateslice_ReverseOffsets : ReverseOffsets.LateSlice.Goal := by exact lateSlice_of_ordinal ReverseOffsets.model ord_ReverseOffsets

theorem valueblind_ReverseOffsets : ReverseOffsets.ValueBlind.Goal := by exact val_ReverseOffsets

theorem cardinality_ZipShortest : ZipShortest.Cardinality.Goal := by exact cardinality_of_ordinal ZipShortest.model ord_ZipShortest

theorem ordinal_ZipShortest : ZipShortest.Ordinal.Goal := by exact ord_ZipShortest

theorem lateraloffset_ZipShortest : ZipShortest.LateralOffset.Goal := by exact lateralOffset_of_ordinal ZipShortest.model ord_ZipShortest

theorem leftoutercount_ZipShortest : ZipShortest.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality ZipShortest.model cardinality_ZipShortest

theorem addressvalidity_ZipShortest : ZipShortest.AddressValidity.Goal := by exact addr_ZipShortest

theorem payloaddedup_ZipShortest : ¬ ZipShortest.PayloadDedup.Goal := by exact not_pay_ZipShortest

theorem lateslice_ZipShortest : ZipShortest.LateSlice.Goal := by exact lateSlice_of_ordinal ZipShortest.model ord_ZipShortest

theorem valueblind_ZipShortest : ZipShortest.ValueBlind.Goal := by exact val_ZipShortest

theorem cardinality_ZipPad : ZipPad.Cardinality.Goal := by exact cardinality_of_ordinal ZipPad.model ord_ZipPad

theorem ordinal_ZipPad : ZipPad.Ordinal.Goal := by exact ord_ZipPad

theorem lateraloffset_ZipPad : ZipPad.LateralOffset.Goal := by exact lateralOffset_of_ordinal ZipPad.model ord_ZipPad

theorem leftoutercount_ZipPad : ZipPad.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality ZipPad.model cardinality_ZipPad

theorem addressvalidity_ZipPad : ZipPad.AddressValidity.Goal := by exact addr_ZipPad

theorem payloaddedup_ZipPad : ¬ ZipPad.PayloadDedup.Goal := by exact not_pay_ZipPad

theorem lateslice_ZipPad : ZipPad.LateSlice.Goal := by exact lateSlice_of_ordinal ZipPad.model ord_ZipPad

theorem valueblind_ZipPad : ZipPad.ValueBlind.Goal := by exact val_ZipPad

theorem cardinality_Cartesian : Cartesian.Cardinality.Goal := by exact cardinality_of_ordinal Cartesian.model ord_Cartesian

theorem ordinal_Cartesian : Cartesian.Ordinal.Goal := by exact ord_Cartesian

theorem lateraloffset_Cartesian : Cartesian.LateralOffset.Goal := by exact lateralOffset_of_ordinal Cartesian.model ord_Cartesian

theorem leftoutercount_Cartesian : Cartesian.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality Cartesian.model cardinality_Cartesian

theorem addressvalidity_Cartesian : Cartesian.AddressValidity.Goal := by exact addr_Cartesian

theorem payloaddedup_Cartesian : ¬ Cartesian.PayloadDedup.Goal := by exact not_pay_Cartesian

theorem lateslice_Cartesian : Cartesian.LateSlice.Goal := by exact lateSlice_of_ordinal Cartesian.model ord_Cartesian

theorem valueblind_Cartesian : Cartesian.ValueBlind.Goal := by exact val_Cartesian

theorem cardinality_NonNullOnly : NonNullOnly.Cardinality.Goal := by exact cardinality_of_ordinal NonNullOnly.model ord_NonNullOnly

theorem ordinal_NonNullOnly : NonNullOnly.Ordinal.Goal := by exact ord_NonNullOnly

theorem lateraloffset_NonNullOnly : NonNullOnly.LateralOffset.Goal := by exact lateralOffset_of_ordinal NonNullOnly.model ord_NonNullOnly

theorem leftoutercount_NonNullOnly : NonNullOnly.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality NonNullOnly.model cardinality_NonNullOnly

theorem addressvalidity_NonNullOnly : NonNullOnly.AddressValidity.Goal := by exact addr_NonNullOnly

theorem payloaddedup_NonNullOnly : ¬ NonNullOnly.PayloadDedup.Goal := by exact not_pay_NonNullOnly

theorem lateslice_NonNullOnly : NonNullOnly.LateSlice.Goal := by exact lateSlice_of_ordinal NonNullOnly.model ord_NonNullOnly

theorem valueblind_NonNullOnly : ¬ NonNullOnly.ValueBlind.Goal := by exact not_val_NonNullOnly

theorem cardinality_EvenOffsets : EvenOffsets.Cardinality.Goal := by exact cardinality_of_ordinal EvenOffsets.model ord_EvenOffsets

theorem ordinal_EvenOffsets : EvenOffsets.Ordinal.Goal := by exact ord_EvenOffsets

theorem lateraloffset_EvenOffsets : EvenOffsets.LateralOffset.Goal := by exact lateralOffset_of_ordinal EvenOffsets.model ord_EvenOffsets

theorem leftoutercount_EvenOffsets : EvenOffsets.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality EvenOffsets.model cardinality_EvenOffsets

theorem addressvalidity_EvenOffsets : EvenOffsets.AddressValidity.Goal := by exact addr_EvenOffsets

theorem payloaddedup_EvenOffsets : ¬ EvenOffsets.PayloadDedup.Goal := by exact not_pay_EvenOffsets

theorem lateslice_EvenOffsets : EvenOffsets.LateSlice.Goal := by exact lateSlice_of_ordinal EvenOffsets.model ord_EvenOffsets

theorem valueblind_EvenOffsets : EvenOffsets.ValueBlind.Goal := by exact val_EvenOffsets

theorem cardinality_AppendFields : AppendFields.Cardinality.Goal := by exact cardinality_of_ordinal AppendFields.model ord_AppendFields

theorem ordinal_AppendFields : AppendFields.Ordinal.Goal := by exact ord_AppendFields

theorem lateraloffset_AppendFields : AppendFields.LateralOffset.Goal := by exact lateralOffset_of_ordinal AppendFields.model ord_AppendFields

theorem leftoutercount_AppendFields : AppendFields.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality AppendFields.model cardinality_AppendFields

theorem addressvalidity_AppendFields : AppendFields.AddressValidity.Goal := by exact addr_AppendFields

theorem payloaddedup_AppendFields : ¬ AppendFields.PayloadDedup.Goal := by exact not_pay_AppendFields

theorem lateslice_AppendFields : AppendFields.LateSlice.Goal := by exact lateSlice_of_ordinal AppendFields.model ord_AppendFields

theorem valueblind_AppendFields : AppendFields.ValueBlind.Goal := by exact val_AppendFields

theorem cardinality_LeadPair : LeadPair.Cardinality.Goal := by exact card_LeadPair

theorem ordinal_LeadPair : LeadPair.Ordinal.Goal := by exact ord_LeadPair

theorem lateraloffset_LeadPair : LeadPair.LateralOffset.Goal := by exact lateralOffset_of_ordinal LeadPair.model ord_LeadPair

theorem leftoutercount_LeadPair : LeadPair.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality LeadPair.model cardinality_LeadPair

theorem addressvalidity_LeadPair : LeadPair.AddressValidity.Goal := by exact addr_LeadPair

theorem payloaddedup_LeadPair : ¬ LeadPair.PayloadDedup.Goal := by exact not_pay_LeadPair

theorem lateslice_LeadPair : LeadPair.LateSlice.Goal := by exact lateSlice_of_ordinal LeadPair.model ord_LeadPair

theorem valueblind_LeadPair : LeadPair.ValueBlind.Goal := by exact val_LeadPair

theorem cardinality_MatchValue : MatchValue.Cardinality.Goal := by exact cardinality_of_ordinal MatchValue.model ord_MatchValue

theorem ordinal_MatchValue : MatchValue.Ordinal.Goal := by exact ord_MatchValue

theorem lateraloffset_MatchValue : MatchValue.LateralOffset.Goal := by exact lateralOffset_of_ordinal MatchValue.model ord_MatchValue

theorem leftoutercount_MatchValue : MatchValue.LeftOuterCount.Goal := by exact leftOuterCount_of_cardinality MatchValue.model cardinality_MatchValue

theorem addressvalidity_MatchValue : MatchValue.AddressValidity.Goal := by exact addr_MatchValue

theorem payloaddedup_MatchValue : ¬ MatchValue.PayloadDedup.Goal := by exact not_pay_MatchValue

theorem lateslice_MatchValue : MatchValue.LateSlice.Goal := by exact lateSlice_of_ordinal MatchValue.model ord_MatchValue

theorem valueblind_MatchValue : ¬ MatchValue.ValueBlind.Goal := by exact not_val_MatchValue

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common
