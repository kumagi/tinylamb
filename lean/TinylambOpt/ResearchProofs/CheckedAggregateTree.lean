import TinylambOpt.ResearchGoals.CheckedAggregateTree

/-!
Proved

- `Goal` — Prove against the same independently specified scalar scan for every
  bracketing and valid incoming state. Safe inputs must take the summary path.

The reference model is defined in `TinylambOpt.ResearchGoals.CheckedAggregateTree`.
-/

namespace TinylambOpt.ResearchProofs.CheckedAggregateTree

open TinylambOpt.ResearchGoals.CheckedAggregateTree
open TinylambOpt.CheckedEagerSum (InRange)

private def rowVal (r : Row) : Int := (r.value.map Int64.toInt).getD 0

private def valSum : List Row → Int
  | [] => 0
  | r :: rs => rowVal r + valSum rs

private def nonNullCount : List Row → Nat
  | [] => 0
  | r :: rs => (if r.value.isSome then 1 else 0) + nonNullCount rs

private theorem valSum_append (xs ys : List Row) : valSum (xs ++ ys) = valSum xs + valSum ys := by
  induction xs with
  | nil => simp [valSum]
  | cons x xs ih => simp [valSum, ih]; omega

private theorem nonNullCount_append (xs ys : List Row) :
    nonNullCount (xs ++ ys) = nonNullCount xs + nonNullCount ys := by
  induction xs with
  | nil => simp [nonNullCount]
  | cons x xs ih => simp [nonNullCount, ih]; omega

private theorem valSum_take_succ (r : Row) (rs : List Row) (m : Nat) :
    valSum ((r :: rs).take (m + 1)) = rowVal r + valSum (rs.take m) := by
  rw [List.take_succ_cons]
  rfl

private def allSafe (acc : Option Int) (rows : List Row) : Prop :=
  ∀ n ≤ rows.length, InRange (((rows.take n).foldl (fun a r => add a r.value) acc).getD 0)

private theorem fold_add_getD (acc : Option Int) (xs : List Row) :
    (xs.foldl (fun a r => add a r.value) acc).getD 0 = acc.getD 0 + valSum xs := by
  induction xs generalizing acc with
  | nil => simp [valSum]
  | cons r rs ih =>
      rw [List.foldl_cons]
      cases hr : r.value with
      | none =>
          simpa [add, hr, valSum, rowVal] using ih acc
      | some v =>
          have h := ih (add acc (some v))
          rw [h]
          simp [add, valSum, rowVal, hr]
          omega

private theorem valSum_eq_zero_of_count_zero {xs : List Row} (h : nonNullCount xs = 0) :
    valSum xs = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases hx : x.value with
      | none =>
          simp [nonNullCount, hx] at h
          simpa [valSum, rowVal, hx] using ih h
      | some v =>
          simp [nonNullCount, hx] at h

private theorem fold_add_eq (acc : Option Int) (xs : List Row) :
    xs.foldl (fun a r => add a r.value) acc =
      if nonNullCount xs = 0 then acc else some (acc.getD 0 + valSum xs) := by
  induction xs generalizing acc with
  | nil => simp [nonNullCount, valSum]
  | cons r rs ih =>
      rw [List.foldl_cons]
      cases hr : r.value with
      | none =>
          simpa [nonNullCount, add, hr, valSum, rowVal] using ih acc
      | some v =>
          have h := ih (some (acc.getD 0 + v.toInt))
          change List.foldl (fun a r => add a r.value) (some (acc.getD 0 + v.toInt)) rs = _
          rw [h]
          by_cases hcnt : nonNullCount rs = 0
          · have hz := valSum_eq_zero_of_count_zero hcnt
            simp [nonNullCount, hcnt, add, valSum, rowVal, hr, hz]
          · simp [nonNullCount, hcnt, add, valSum, rowVal, hr]
            omega

private theorem sequential_ok_of_allSafe (acc : Option Int) (xs : List Row)
    (h : allSafe acc xs) :
    sequential acc xs = .ok (xs.foldl (fun a r => add a r.value) acc) := by
  induction xs generalizing acc with
  | nil => rfl
  | cons r rs ih =>
      let next := add acc r.value
      have hnext : InRange (next.getD 0) := by
        simpa [allSafe, next] using h 1 (by simp)
      simp only [sequential]
      rw [ite_eq_left hnext]
      apply ih
      intro n hn
      have hs := h (n + 1) (by simp [hn])
      simpa [allSafe, List.take_succ_cons, next] using hs

private theorem sequential_ok_inRange (acc : Option Int) (xs : List Row)
    (hacc : InRange (acc.getD 0)) {b : Option Int} (hres : sequential acc xs = .ok b) :
    InRange (b.getD 0) := by
  induction xs generalizing acc with
  | nil =>
      cases hres
      exact hacc
  | cons r rs ih =>
      simp only [sequential] at hres
      by_cases hnext : InRange ((add acc r.value).getD 0)
      · rw [ite_eq_left hnext] at hres
        exact ih (add acc r.value) hnext hres
      · rw [ite_eq_right hnext] at hres
        cases hres

private theorem sequential_append (acc : Option Int) (xs ys : List Row) :
    sequential acc (xs ++ ys) =
      match sequential acc xs with
      | .error i => .error i
      | .ok b => sequential b ys := by
  induction xs generalizing acc with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : InRange ((add acc r.value).getD 0)
      · cases hs : sequential (add acc r.value) rs with
        | error i =>
            have hih := ih (add acc r.value)
            rw [hs] at hih
            simpa [sequential, h, hs] using hih
        | ok b =>
            have hih := ih (add acc r.value)
            rw [hs] at hih
            simpa [sequential, h, hs] using hih
      · simp [sequential, h]

private structure Spec (s : Summary) (rows : List Row) : Prop where
  total : s.total = valSum rows
  count : s.count = nonNullCount rows
  low_bound : ∀ n ≤ rows.length, s.low ≤ valSum (rows.take n)
  low_attain : ∃ n ≤ rows.length, valSum (rows.take n) = s.low
  high_bound : ∀ n ≤ rows.length, valSum (rows.take n) ≤ s.high
  high_attain : ∃ n ≤ rows.length, valSum (rows.take n) = s.high

private def initSummary : Summary := ⟨0, 0, 0, 0⟩

private def step (s : Summary) (r : Row) : Summary := combine s (rowSummary r)

private def sumRows (rows : List Row) : Summary := rows.foldl step initSummary

private theorem combine_assoc (a b c : Summary) :
    combine (combine a b) c = combine a (combine b c) := by
  cases a
  cases b
  cases c
  simp [combine]
  omega

private theorem combine_init_right (a : Summary) (hlo : a.low ≤ a.total) (hhi : a.total ≤ a.high) :
    combine a initSummary = a := by
  cases a with
  | mk total low high count =>
      simp [combine, initSummary] at hlo hhi ⊢
      omega

private theorem combine_init_left (a : Summary) (hlo : a.low ≤ 0) (hhi : 0 ≤ a.high) :
    combine initSummary a = a := by
  cases a with
  | mk total low high count =>
      simp [combine, initSummary] at hlo hhi ⊢
      omega

private theorem rowSummary_low_nonpos (r : Row) : (rowSummary r).low ≤ 0 := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega

private theorem rowSummary_high_nonneg (r : Row) : 0 ≤ (rowSummary r).high := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega

private theorem rowSummary_low_le_total (r : Row) : (rowSummary r).low ≤ (rowSummary r).total := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega

private theorem rowSummary_total_le_high (r : Row) : (rowSummary r).total ≤ (rowSummary r).high := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega

private theorem rowSummary_total (r : Row) : (rowSummary r).total = rowVal r := by
  rcases r with ⟨_, v⟩
  cases v <;> rfl

private theorem rowSummary_count (r : Row) : (rowSummary r).count = if r.value.isSome then 1 else 0 := by
  rcases r with ⟨_, v⟩
  cases v <;> rfl

private theorem foldl_combine_eq (s a : Summary) (rows : List Row) :
    rows.foldl step (combine s a) = combine s (rows.foldl step a) := by
  induction rows generalizing a with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons]
      have hassoc : step (combine s a) r = combine s (step a r) := by
        simp [step, combine_assoc]
      rw [hassoc]
      exact ih (step a r)

private theorem sumRows_cons (r : Row) (rs : List Row) :
    sumRows (r :: rs) = combine (rowSummary r) (sumRows rs) := by
  unfold sumRows
  simp only [List.foldl_cons, step]
  calc
    rs.foldl step (combine initSummary (rowSummary r))
        = rs.foldl step (rowSummary r) := by
            rw [combine_init_left _ (rowSummary_low_nonpos r) (rowSummary_high_nonneg r)]
    _ = rs.foldl step (combine (rowSummary r) initSummary) := by
            rw [combine_init_right _ (rowSummary_low_le_total r) (rowSummary_total_le_high r)]
    _ = combine (rowSummary r) (rs.foldl step initSummary) :=
            foldl_combine_eq (rowSummary r) initSummary rs

private theorem take_append_prefix (ls rs : List Row) (m : Nat) :
    (ls ++ rs).take (ls.length + m) = ls ++ rs.take m := by
  induction ls with
  | nil => simp
  | cons x xs ih =>
      rw [show (x :: xs).length + m = (xs.length + m) + 1 by simp; omega]
      change List.take ((xs.length + m) + 1) (x :: (xs ++ rs)) = x :: (xs ++ List.take m rs)
      rw [List.take_succ_cons, ih]

private theorem take_append_left (xs ys : List Row) {n : Nat} (h : n ≤ xs.length) :
    (xs ++ ys).take n = xs.take n := by
  induction xs generalizing n with
  | nil =>
      simp at h ⊢
      omega
  | cons x xs ih =>
      cases n with
      | zero => simp
      | succ n =>
          have hn : n ≤ xs.length := by simpa using h
          simpa using ih (n := n) hn

private theorem spec_combine_append (sl sr : Summary) (ls rs : List Row)
    (hsl : Spec sl ls) (hsr : Spec sr rs) : Spec (combine sl sr) (ls ++ rs) := by
  refine ⟨?tot, ?cnt, ?lob, ?loa, ?hib, ?hia⟩
  · simp [combine, valSum_append, hsl.total, hsr.total]
  · simp [combine, nonNullCount_append, hsl.count, hsr.count]
  · intro n hn
    by_cases hle : n ≤ ls.length
    · have ht : (ls ++ rs).take n = ls.take n := take_append_left ls rs hle
      rw [ht]
      have hb := hsl.low_bound n hle
      simp [combine]
      omega
    · have hnlen : ls.length ≤ n := by omega
      let m := n - ls.length
      have hm : m ≤ rs.length := by
        dsimp [m]
        simp at hn
        omega
      have ht : (ls ++ rs).take n = ls ++ rs.take m := by
        rw [show n = ls.length + m by dsimp [m]; omega]
        exact take_append_prefix ls rs m
      rw [ht, valSum_append]
      have hb := hsr.low_bound m hm
      simp [combine, hsl.total]
      omega
  · have hlo0 : sr.low ≤ 0 := by
      have hb := hsr.low_bound 0 (by simp)
      simp [valSum] at hb
      exact hb
    by_cases hmin : sl.low ≤ sl.total + sr.low
    · have hcl : (combine sl sr).low = sl.low := by simp [combine]; omega
      obtain ⟨n, hn, ha⟩ := hsl.low_attain
      refine ⟨n, by simpa using (show n ≤ ls.length + rs.length by omega), ?_⟩
      calc
        valSum ((ls ++ rs).take n) = valSum (ls.take n) := by rw [take_append_left ls rs hn]
        _ = sl.low := ha
        _ = (combine sl sr).low := hcl.symm
    · have hcl : (combine sl sr).low = sl.total + sr.low := by simp [combine]; omega
      obtain ⟨n, hn, ha⟩ := hsr.low_attain
      refine ⟨ls.length + n, by simp [hn], ?_⟩
      calc
        valSum ((ls ++ rs).take (ls.length + n)) = valSum (ls ++ rs.take n) := by rw [take_append_prefix]
        _ = valSum ls + valSum (rs.take n) := valSum_append ls (rs.take n)
        _ = valSum ls + sr.low := by rw [ha]
        _ = sl.total + sr.low := by rw [← hsl.total]
        _ = (combine sl sr).low := hcl.symm
  · intro n hn
    by_cases hle : n ≤ ls.length
    · have ht : (ls ++ rs).take n = ls.take n := take_append_left ls rs hle
      rw [ht]
      have hb := hsl.high_bound n hle
      simp [combine]
      omega
    · have hnlen : ls.length ≤ n := by omega
      let m := n - ls.length
      have hm : m ≤ rs.length := by
        dsimp [m]
        simp at hn
        omega
      have ht : (ls ++ rs).take n = ls ++ rs.take m := by
        rw [show n = ls.length + m by dsimp [m]; omega]
        exact take_append_prefix ls rs m
      rw [ht, valSum_append]
      have hb := hsr.high_bound m hm
      simp [combine, hsl.total]
      omega
  · have hhi0 : 0 ≤ sr.high := by
      have hb := hsr.high_bound 0 (by simp)
      simp [valSum] at hb
      exact hb
    by_cases hmax : sl.total + sr.high ≤ sl.high
    · have hch : (combine sl sr).high = sl.high := by simp [combine]; omega
      obtain ⟨n, hn, ha⟩ := hsl.high_attain
      refine ⟨n, by simpa using (show n ≤ ls.length + rs.length by omega), ?_⟩
      calc
        valSum ((ls ++ rs).take n) = valSum (ls.take n) := by rw [take_append_left ls rs hn]
        _ = sl.high := ha
        _ = (combine sl sr).high := hch.symm
    · have hch : (combine sl sr).high = sl.total + sr.high := by simp [combine]; omega
      obtain ⟨n, hn, ha⟩ := hsr.high_attain
      refine ⟨ls.length + n, by simp [hn], ?_⟩
      calc
        valSum ((ls ++ rs).take (ls.length + n)) = valSum (ls ++ rs.take n) := by rw [take_append_prefix]
        _ = valSum ls + valSum (rs.take n) := valSum_append ls (rs.take n)
        _ = valSum ls + sr.high := by rw [ha]
        _ = sl.total + sr.high := by rw [← hsl.total]
        _ = (combine sl sr).high := hch.symm

private theorem rowSummary_spec (r : Row) : Spec (rowSummary r) [r] := by
  rcases r with ⟨ord, val⟩
  cases val with
  | none =>
      refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
      · simp [rowSummary, rowVal, valSum]
      · simp [rowSummary, nonNullCount]
      · intro n hn
        simp at hn
        have hn' : n = 0 ∨ n = 1 := by omega
        rcases hn' with rfl | rfl <;> simp [valSum, rowVal, rowSummary]
      · refine ⟨0, by simp, ?_⟩
        simp [valSum, rowVal, rowSummary]
      · intro n hn
        simp at hn
        have hn' : n = 0 ∨ n = 1 := by omega
        rcases hn' with rfl | rfl <;> simp [valSum, rowVal, rowSummary]
      · refine ⟨0, by simp, ?_⟩
        simp [valSum, rowVal, rowSummary]
  | some v =>
      refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
      · simp [rowSummary, rowVal, valSum]
      · simp [rowSummary, nonNullCount]
      · intro n hn
        simp at hn
        have hn' : n = 0 ∨ n = 1 := by omega
        rcases hn' with rfl | rfl <;> simp [valSum, rowVal, rowSummary] <;> omega
      · by_cases hv : v.toInt ≤ 0
        · refine ⟨1, by simp, ?_⟩
          simp [valSum, rowVal, rowSummary]
          omega
        · refine ⟨0, by simp, ?_⟩
          simp [valSum, rowVal, rowSummary]
          omega
      · intro n hn
        simp at hn
        have hn' : n = 0 ∨ n = 1 := by omega
        rcases hn' with rfl | rfl <;> simp [valSum, rowVal, rowSummary] <;> omega
      · by_cases hv : 0 ≤ v.toInt
        · refine ⟨1, by simp, ?_⟩
          simp [valSum, rowVal, rowSummary]
          omega
        · refine ⟨0, by simp, ?_⟩
          simp [valSum, rowVal, rowSummary]
          omega

private theorem sumRows_spec (rows : List Row) : Spec (sumRows rows) rows := by
  induction rows with
  | nil =>
      refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩ <;> simp [sumRows, initSummary, valSum, nonNullCount]
  | cons r rs ih =>
      rw [sumRows_cons]
      exact spec_combine_append (rowSummary r) (sumRows rs) [r] rs (rowSummary_spec r) ih

private theorem prepare_spec (tree : Tree) : Spec (prepare tree).summary tree.rows := by
  induction tree with
  | chunk rows =>
      change Spec (sumRows rows) rows
      exact sumRows_spec rows
  | join l r ihl ihr =>
      simp only [prepare, Cached.summary, Tree.rows]
      exact spec_combine_append (prepare l).summary (prepare r).summary l.rows r.rows ihl ihr

private def guard (acc : Option Int) (s : Summary) : Prop :=
  InRange (acc.getD 0 + s.low) ∧ InRange (acc.getD 0 + s.high)

private theorem guard_iff_allSafe (rows : List Row) (s : Summary) (hs : Spec s rows)
    (acc : Option Int) (hacc : InRange (acc.getD 0)) :
    guard acc s ↔ allSafe acc rows := by
  constructor
  · intro hg n hn
    have hl := hs.low_bound n hn
    have hh := hs.high_bound n hn
    have hf := fold_add_getD acc (rows.take n)
    dsimp [guard] at hg
    unfold InRange at *
    omega
  · intro hsafe
    obtain ⟨nl, hnl, hal⟩ := hs.low_attain
    obtain ⟨nh, hnh, hah⟩ := hs.high_attain
    have h1 := hsafe nl hnl
    have h2 := hsafe nh hnh
    rw [fold_add_getD, hal] at h1
    rw [fold_add_getD, hah] at h2
    exact ⟨h1, h2⟩

private theorem run_chunk_noguard_fst (acc : Option Int) (rows : List Row)
    (hg : ¬ guard acc (prepare (.chunk rows)).summary) :
    (run acc (prepare (.chunk rows))).1 = sequential acc rows := by
  dsimp [guard] at hg
  conv => lhs; unfold run
  rw [ite_eq_right hg]
  simp only [prepare]

private theorem run_join_noguard_fst (acc : Option Int) (l r : Tree)
    (hg : ¬ guard acc (prepare (.join l r)).summary) :
    (run acc (prepare (.join l r))).1 =
      match (run acc (prepare l)).1 with
      | .error i => .error i
      | .ok b => (run b (prepare r)).1 := by
  dsimp [guard] at hg
  conv => lhs; unfold run
  rw [ite_eq_right hg]
  simp only [prepare]
  cases hrun : run acc (prepare l) with
  | mk res tr =>
      cases res with
      | error i => simp [hrun]
      | ok b => simp [hrun]

private theorem run_join_noguard_snd (acc : Option Int) (l r : Tree)
    (hg : ¬ guard acc (prepare (.join l r)).summary) :
    (run acc (prepare (.join l r))).2 =
      match run acc (prepare l) with
      | (res, trace) =>
        match res with
        | .error i => .descend :: trace
        | .ok b => .descend :: (trace ++ (run b (prepare r)).2) := by
  dsimp [guard] at hg
  conv => lhs; unfold run
  rw [ite_eq_right hg]
  simp only [prepare]
  cases hrun : run acc (prepare l) with
  | mk res tr =>
      cases res with
      | error i => simp [hrun]
      | ok b => simp [hrun]

private theorem run_fst_eq (tree : Tree) (acc : Option Int) (hacc : InRange (acc.getD 0)) :
    (run acc (prepare tree)).1 = sequential acc tree.rows := by
  induction tree generalizing acc with
  | chunk rows =>
      by_cases hg : guard acc (prepare (.chunk rows)).summary
      · have hs := prepare_spec (.chunk rows)
        have hsafe := (guard_iff_allSafe rows (prepare (.chunk rows)).summary hs acc hacc).mp hg
        have hseq := sequential_ok_of_allSafe acc rows hsafe
        have hfold := fold_add_eq acc rows
        unfold run
        dsimp [guard] at hg
        rw [ite_eq_left hg]
        change (.ok (if (prepare (.chunk rows)).summary.count = 0 then acc
          else some (acc.getD 0 + (prepare (.chunk rows)).summary.total))) = sequential acc rows
        rw [hseq]
        congr 1
        rw [hfold]
        simp [hs.total, hs.count]
        rfl
      · exact run_chunk_noguard_fst acc rows hg
  | join l r ihl ihr =>
      by_cases hg : guard acc (prepare (.join l r)).summary
      · have hs := prepare_spec (.join l r)
        have hsafe := (guard_iff_allSafe (l.rows ++ r.rows) (prepare (.join l r)).summary hs acc hacc).mp hg
        have hseq := sequential_ok_of_allSafe acc (l.rows ++ r.rows) hsafe
        have hfold := fold_add_eq acc (l.rows ++ r.rows)
        unfold run
        dsimp [guard] at hg
        rw [ite_eq_left hg]
        change (.ok (if (prepare (.join l r)).summary.count = 0 then acc
          else some (acc.getD 0 + (prepare (.join l r)).summary.total))) = sequential acc (l.rows ++ r.rows)
        rw [hseq]
        congr 1
        rw [hfold]
        simp [hs.total, hs.count]
        rfl
      · rw [run_join_noguard_fst acc l r hg]
        change (match (run acc (prepare l)).1 with
          | .error i => .error i
          | .ok b => (run b (prepare r)).1) = sequential acc (l.rows ++ r.rows)
        cases hres : (run acc (prepare l)).1 with
        | error i =>
            have hseqL' : sequential acc l.rows = .error i := by
              have ht := ihl acc hacc
              simpa [hres] using ht.symm
            have happ := sequential_append acc l.rows r.rows
            rw [hseqL'] at happ
            simp at happ
            simp [hres]
            exact happ.symm
        | ok b =>
            have hseqL' : sequential acc l.rows = .ok b := by
              have ht := ihl acc hacc
              simpa [hres] using ht.symm
            have hb : InRange (b.getD 0) := sequential_ok_inRange acc l.rows hacc hseqL'
            have hseqR := ihr b hb
            have happ := sequential_append acc l.rows r.rows
            rw [hseqL'] at happ
            simp at happ
            simp [hres]
            rw [hseqR]
            exact happ.symm

private theorem run_trace_eq_skip (tree : Tree) (acc : Option Int) (hacc : InRange (acc.getD 0)) :
    (run acc (prepare tree)).2 = [.skip] ↔ allSafe acc tree.rows := by
  have hiff := guard_iff_allSafe tree.rows (prepare tree).summary (prepare_spec tree) acc hacc
  by_cases hg : guard acc (prepare tree).summary
  · constructor
    · intro _
      exact hiff.mp hg
    · intro _
      unfold run
      dsimp [guard] at hg
      rw [ite_eq_left hg]
  · constructor
    · intro ht
      cases tree with
      | chunk rows =>
          have htr : (run acc (prepare (.chunk rows))).2 = [.openChunk] := by
            have hg' : ¬ (InRange (acc.getD 0 + (prepare (.chunk rows)).summary.low) ∧
                InRange (acc.getD 0 + (prepare (.chunk rows)).summary.high)) := by
              simpa [guard] using hg
            conv => lhs; unfold run
            rw [ite_eq_right hg']
            simp only [prepare]
          rw [htr] at ht
          cases ht
      | join l r =>
          rw [run_join_noguard_snd acc l r hg] at ht
          cases hrunL : run acc (prepare l) with
          | mk resL trL =>
              cases hresL : resL with
              | error i => simp [hrunL, hresL] at ht
              | ok b =>
                  cases hrunR : run b (prepare r) with
                  | mk resR trR => simp [hrunL, hresL, hrunR] at ht
    · intro hsafe
      exact False.elim (hg (hiff.mpr hsafe))

theorem sound : TinylambOpt.ResearchGoals.CheckedAggregateTree.Goal := by
  intro tree acc hacc
  exact ⟨run_fst_eq tree acc hacc, run_trace_eq_skip tree acc hacc⟩

end TinylambOpt.ResearchProofs.CheckedAggregateTree
