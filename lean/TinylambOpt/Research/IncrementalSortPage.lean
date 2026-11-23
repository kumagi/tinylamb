import Std.Tactic

set_option maxRecDepth 100000

/-!
# Incremental, spill-aware FETCH over sorted prefix runs

Each row has a *prefix* key (`pkey`) that is globally nondecreasing across the
whole input (even across spill-file boundaries) and a *tail* key (`skey`)
that is only sorted inside a maximal run of equal `pkey`.  The correct page
is the stable global sort by `(pkey, skey, id)` followed by `OFFSET` /
`FETCH FIRST k ROWS WITH TIES`.

Reference computation: `refPage` sorts the entire input (`runSort`, proved to
be the global stable sort) and selects with `List.filter` over the whole
remainder.

Optimized computation: `incPage` stops at the first row strictly past the
boundary and uses `takeWhile`, with an explicit event-count evaluator
(`evalCost`).  `runSort` only sorts rows inside a run, and runs are formed
over the flattened file list, so a run may cross a spill-file boundary.

`incremental_sort_page_sound` proves the emitted payload sequence and the
WITH TIES boundary are exact.  `incremental_sort_page_bounded` proves the
read count is bounded, no row strictly past the boundary is read, and the
tied suffix is confined to one run (hence bounded by the maximum unresolved
run length).  The counterexample shows that closing a run at a file boundary
loses the smallest tail key.
-/

namespace TinylambOpt.Research.IncrementalSortPage

/- ------------------------------------------------------------------------ -
    Rows and the total stable order
    ------------------------------------------------------------------------ -/

structure Row where
  pkey : Int
  skey : Int
  payload : Int
  id : Nat
  deriving DecidableEq, Repr

/-- Strict total order: prefix, then tail, then appearance id, then payload
    (the last field only makes the order total on all `Row` values; input ids
    are distinct, so stability is decided by the id field). -/
def rowLt (a b : Row) : Prop :=
  a.pkey < b.pkey ∨
  (a.pkey = b.pkey ∧
    (a.skey < b.skey ∨
      (a.skey = b.skey ∧
        (a.id < b.id ∨ (a.id = b.id ∧ a.payload < b.payload)))))

instance (a b : Row) : Decidable (rowLt a b) := by
  unfold rowLt
  infer_instance

def SortedRows : List Row → Prop := List.Pairwise rowLt

def PrefixSorted : List Row → Prop := List.Pairwise (fun a b : Row => a.pkey ≤ b.pkey)

theorem rowLt_irrefl (a : Row) : ¬ rowLt a a := by
  unfold rowLt
  omega

theorem rowLt_trans {a b c : Row} (hab : rowLt a b) (hbc : rowLt b c) : rowLt a c := by
  unfold rowLt at *
  omega

theorem rowLt_trichotomy (a b : Row) : rowLt a b ∨ a = b ∨ rowLt b a := by
  unfold rowLt
  by_cases hp : a.pkey = b.pkey
  · by_cases ht : a.skey = b.skey
    · by_cases hi : a.id = b.id
      · by_cases hq : a.payload = b.payload
        · right; left; cases a; cases b; simp_all
        · have h : a.payload < b.payload ∨ b.payload < a.payload := by omega
          rcases h with h | h
          · exact Or.inl (Or.inr ⟨hp, Or.inr ⟨ht, Or.inr ⟨hi, h⟩⟩⟩)
          · exact Or.inr (Or.inr (Or.inr ⟨hp.symm, Or.inr ⟨ht.symm, Or.inr ⟨hi.symm, h⟩⟩⟩))
      · have h : a.id < b.id ∨ b.id < a.id := by omega
        rcases h with h | h
        · exact Or.inl (Or.inr ⟨hp, Or.inr ⟨ht, Or.inl h⟩⟩)
        · exact Or.inr (Or.inr (Or.inr ⟨hp.symm, Or.inr ⟨ht.symm, Or.inl h⟩⟩))
    · have h : a.skey < b.skey ∨ b.skey < a.skey := by omega
      rcases h with h | h
      · exact Or.inl (Or.inr ⟨hp, Or.inl h⟩)
      · exact Or.inr (Or.inr (Or.inr ⟨hp.symm, Or.inl h⟩))
  · have h : a.pkey < b.pkey ∨ b.pkey < a.pkey := by omega
    rcases h with h | h
    · exact Or.inl (Or.inl h)
    · exact Or.inr (Or.inr (Or.inl h))

theorem not_rowLt_ge {a b : Row} (h : ¬ rowLt a b) : rowLt b a ∨ b = a := by
  rcases rowLt_trichotomy a b with hab | heq | hba
  · exact False.elim (h hab)
  · exact Or.inr heq.symm
  · exact Or.inl hba

/-- The head of a sorted list precedes every later element, so a later
    element cannot precede the head. -/
theorem not_rowLt_of_head {a : Row} {as : List Row} (hs : SortedRows (a :: as))
    {x : Row} (hx : x ∈ as) : ¬ rowLt x a := by
  intro hxa
  have hax := (List.pairwise_cons.mp hs).1 x hx
  exact rowLt_irrefl x (rowLt_trans hxa hax)

/-- For a sorted list, every element of `take k` precedes every element of
    `drop k`. -/
theorem mem_drop_subset (l : List Row) (n : Nat) (x : Row) : x ∈ l.drop n → x ∈ l := by
  induction n generalizing l with
  | zero => intro hx; exact hx
  | succ n ih =>
      cases l with
      | nil => simp
      | cons y ys =>
          intro hx
          exact List.mem_cons_of_mem y (ih ys hx)

theorem sorted_take_le_drop {xs : List Row} (hs : SortedRows xs) (k : Nat) :
    ∀ a ∈ xs.take k, ∀ b ∈ xs.drop k, ¬ rowLt b a := by
  induction xs generalizing k with
  | nil => simp
  | cons x xs ih =>
      cases k with
      | zero => simp
      | succ k =>
          simp [List.take_succ_cons]
          constructor
          · intro b hb
            have hbxs : b ∈ xs := mem_drop_subset xs k b hb
            exact not_rowLt_of_head hs hbxs
          · exact ih (List.pairwise_cons.mp hs).2 k

/- ------------------------------------------------------------------------ -
    Stable insertion sort
    ------------------------------------------------------------------------ -/

def insort (r : Row) : List Row → List Row
  | [] => [r]
  | x :: xs => if rowLt r x then r :: x :: xs else x :: insort r xs

def isort : List Row → List Row
  | [] => []
  | r :: rs => insort r (isort rs)

theorem insort_perm (r : Row) : ∀ rs, (insort r rs).Perm (r :: rs)
  | [] => List.Perm.refl [r]
  | x :: xs => by
      by_cases h : rowLt r x
      · simp [insort, h]
      · simp only [insort, if_neg h]
        exact (List.Perm.cons x (insort_perm r xs)).trans (List.Perm.swap r x xs)

theorem isort_perm : ∀ rs, (isort rs).Perm rs
  | [] => List.Perm.refl []
  | r :: rs => (insort_perm r (isort rs)).trans (List.Perm.cons r (isort_perm rs))

theorem insort_eq_cons_of_lt (r x : Row) (xs : List Row) (h : rowLt r x) :
    insort r (x :: xs) = r :: x :: xs := by
  simp [insort, h]

theorem insort_eq_cons_of_not (r x : Row) (xs : List Row) (h : ¬ rowLt r x) :
    insort r (x :: xs) = x :: insort r xs := by
  simp [insort, h]

theorem rowLt_of_mem_insort {x r : Row} {xs : List Row}
    (hxr : rowLt x r) (hxs : ∀ y ∈ xs, rowLt x y) :
    ∀ y ∈ insort r xs, rowLt x y := by
  induction xs with
  | nil =>
      intro y hy
      simp [insort] at hy
      simpa [hy] using hxr
  | cons a as ih =>
      by_cases hr : rowLt r a
      · rw [insort_eq_cons_of_lt r a as hr]
        intro y hy
        rcases List.mem_cons.mp hy with hyr | hy
        · subst y
          simpa using hxr
        · rcases List.mem_cons.mp hy with hya | hyas
          · subst y
            exact hxs a (by simp)
          · exact hxs y (by simp [hyas])
      · rw [insort_eq_cons_of_not r a as hr]
        intro y hy
        rcases List.mem_cons.mp hy with hya | hyins
        · subst y
          exact hxs a (by simp)
        · exact ih (by intro z hz; exact hxs z (by simp [hz])) y hyins

theorem insort_sorted (r : Row) (rs : List Row) (hs : SortedRows rs)
    (hnot : r ∉ rs) : SortedRows (insort r rs) := by
  induction rs with
  | nil => simp [insort, SortedRows]
  | cons x xs ih =>
      have hx_all : ∀ y ∈ xs, rowLt x y := (List.pairwise_cons.mp hs).1
      have htail : SortedRows xs := (List.pairwise_cons.mp hs).2
      have hne : r ≠ x := by intro h; exact hnot (by simp [h])
      have hnotxs : r ∉ xs := by intro h; exact hnot (by simp [h])
      by_cases h : rowLt r x
      · rw [insort_eq_cons_of_lt r x xs h]
        change List.Pairwise rowLt (r :: x :: xs)
        rw [List.pairwise_cons]
        constructor
        · intro y hy
          rcases List.mem_cons.mp hy with hyx | hyxs
          · simpa [hyx] using h
          · exact rowLt_trans h (hx_all y hyxs)
        · exact List.Pairwise.cons hx_all htail
      · have hxr' : rowLt x r := by
          rcases not_rowLt_ge h with hlt | heq
          · exact hlt
          · exact False.elim (hne heq.symm)
        rw [insort_eq_cons_of_not r x xs h]
        change List.Pairwise rowLt (x :: insort r xs)
        rw [List.pairwise_cons]
        exact ⟨rowLt_of_mem_insort hxr' hx_all, ih htail hnotxs⟩

theorem isort_sorted : ∀ rs, rs.Nodup → SortedRows (isort rs)
  | [], _ => by simp [isort, SortedRows]
  | r :: rs, hnd => by
      have hr_not : r ∉ rs := by
        intro hr
        exact (List.pairwise_cons.mp hnd).1 r hr rfl
      have htail_nd : rs.Nodup := (List.pairwise_cons.mp hnd).2
      have hperm := isort_perm rs
      have hnot_isort : r ∉ isort rs := by
        simpa [hperm.mem_iff] using hr_not
      exact insort_sorted r (isort rs) (isort_sorted rs htail_nd) hnot_isort

theorem insort_of_all_gt (r : Row) (xs : List Row)
    (h : ∀ y ∈ xs, rowLt r y) : insort r xs = r :: xs := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : rowLt r x := h x (by simp)
      have hxs : ∀ y ∈ xs, rowLt r y := by intro y hy; exact h y (by simp [hy])
      simp [insort, hx, ih hxs]

theorem isort_eq_self (rs : List Row) (hs : SortedRows rs) : isort rs = rs := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      have hhead : ∀ y ∈ rs, rowLt r y := (List.pairwise_cons.mp hs).1
      have htail : SortedRows rs := (List.pairwise_cons.mp hs).2
      rw [isort, ih htail]
      exact insort_of_all_gt r rs hhead

theorem sorted_perm_eq {xs ys : List Row} (hxs : SortedRows xs) (hys : SortedRows ys)
    (hp : xs.Perm ys) : xs = ys := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => rfl
      | cons y ys =>
          have : y ∈ ([] : List Row) := (hp.mem_iff).mpr (by simp)
          simp at this
  | cons x xs ih =>
      cases ys with
      | nil =>
          have : x ∈ ([] : List Row) := (hp.mem_iff).mp (by simp)
          simp at this
      | cons y ys =>
          have hyin : y ∈ x :: xs := (hp.mem_iff).mpr (by simp)
          have hxin : x ∈ y :: ys := (hp.mem_iff).mp (by simp)
          have hnot_yx : ¬ rowLt y x := by
            rcases List.mem_cons.mp hyin with hyx | hytail
            · subst y
              exact rowLt_irrefl x
            · exact not_rowLt_of_head hxs hytail
          have hnot_xy : ¬ rowLt x y := by
            rcases List.mem_cons.mp hxin with hxy | hxtail
            · subst x
              exact rowLt_irrefl y
            · exact not_rowLt_of_head hys hxtail
          have heq : x = y := by
            rcases rowLt_trichotomy x y with hxy | h | hyx
            · exact False.elim (hnot_xy hxy)
            · exact h
            · exact False.elim (hnot_yx hyx)
          subst y
          have hp_tail : xs.Perm ys := by simpa using hp.erase x
          exact congrArg (fun l => x :: l)
            (ih (List.pairwise_cons.mp hxs).2 (List.pairwise_cons.mp hys).2 hp_tail)

/- ------------------------------------------------------------------------ -
    Runs (possibly crossing spill-file boundaries)
    ------------------------------------------------------------------------ -/

def samePkey (p : Int) (r : Row) : Bool := decide (r.pkey = p)

def spanRun (p : Int) : List Row → List Row × List Row
  | [] => ([], [])
  | r :: rs =>
      if samePkey p r then
        let (run, rest) := spanRun p rs
        (r :: run, rest)
      else ([], r :: rs)

theorem spanRun_prefix_all (p : Int) (rs : List Row) :
    ∀ x ∈ (spanRun p rs).1, x.pkey = p := by
  induction rs with
  | nil => simp [spanRun]
  | cons r rs ih =>
      by_cases h : samePkey p r
      · simp [spanRun, h]
        constructor
        · simpa [samePkey] using h
        · exact ih
      · simp [spanRun, h]

theorem spanRun_rest_gt (r : Row) (rs : List Row) (hps : PrefixSorted (r :: rs)) :
    ∀ x ∈ (spanRun r.pkey rs).2, r.pkey < x.pkey := by
  match rs with
  | [] =>
      simp [spanRun]
  | a :: as =>
      by_cases h : samePkey r.pkey a
      · simp only [spanRun, h]
        dsimp
        apply spanRun_rest_gt r as
        have haeq : a.pkey = r.pkey := by simpa [samePkey] using h
        have hr_le : ∀ x ∈ as, r.pkey ≤ x.pkey := by
          intro x hx
          have hax : a.pkey ≤ x.pkey :=
            (List.pairwise_cons.mp (List.pairwise_cons.mp hps).2).1 x hx
          omega
        exact List.Pairwise.cons hr_le (List.pairwise_cons.mp (List.pairwise_cons.mp hps).2).2
      · simp only [spanRun, h]
        intro x hx
        have hage : r.pkey ≤ a.pkey := (List.pairwise_cons.mp hps).1 a (by simp)
        have hane : a.pkey ≠ r.pkey := by simpa [samePkey] using h
        have hagt : r.pkey < a.pkey := by omega
        rcases List.mem_cons.mp hx with hxa | hxas
        · simpa [hxa] using hagt
        · have hax : a.pkey ≤ x.pkey :=
            (List.pairwise_cons.mp (List.pairwise_cons.mp hps).2).1 x hxas
          exact Int.lt_of_lt_of_le hagt hax
termination_by rs.length
decreasing_by
  simp only [List.length_cons]
  omega

theorem spanRun_concat (p : Int) (rs : List Row) :
    (spanRun p rs).1 ++ (spanRun p rs).2 = rs := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : samePkey p r
      · simp [spanRun, h, ih]
      · simp [spanRun, h]

def splitRuns (rs : List Row) : List (List Row) :=
  match rs with
  | [] => []
  | r :: rest => (r :: (spanRun r.pkey rest).1) :: splitRuns (spanRun r.pkey rest).2
termination_by rs.length
decreasing_by
  have hc := spanRun_concat r.pkey rest
  have hlen := congrArg List.length hc
  simp only [List.length_append] at hlen
  simp only [List.length_cons]
  omega

theorem splitRuns_concat (rs : List Row) : (splitRuns rs).flatten = rs := by
  cases rs with
  | nil => rw [splitRuns.eq_1]; rfl
  | cons r rest =>
      rw [splitRuns.eq_2]
      change ((r :: (spanRun r.pkey rest).1) :: splitRuns (spanRun r.pkey rest).2).flatten = r :: rest
      change (r :: (spanRun r.pkey rest).1) ++ (splitRuns (spanRun r.pkey rest).2).flatten = r :: rest
      rw [splitRuns_concat]
      have hc := spanRun_concat r.pkey rest
      change r :: ((spanRun r.pkey rest).1 ++ (spanRun r.pkey rest).2) = r :: rest
      rw [hc]

termination_by rs.length
decreasing_by
  have hc := spanRun_concat r.pkey rest
  have hlen := congrArg List.length hc
  simp only [List.length_append] at hlen
  simp only [List.length_cons]
  omega

def runSortSingle (rs : List Row) : List Row :=
  (splitRuns rs).map isort |>.flatten

theorem runSortSingle_perm (rs : List Row) : (runSortSingle rs).Perm rs := by
  unfold runSortSingle
  have hmap : ((splitRuns rs).map isort).flatten.Perm (splitRuns rs).flatten := by
    induction (splitRuns rs) with
    | nil => exact List.Perm.refl []
    | cons run runs ih => exact (List.Perm.append (isort_perm run) ih)
  exact hmap.trans (by rw [splitRuns_concat])

theorem sorted_append (xs ys : List Row) (hxs : SortedRows xs) (hys : SortedRows ys)
    (hcross : ∀ a ∈ xs, ∀ b ∈ ys, rowLt a b) : SortedRows (xs ++ ys) := by
  induction xs with
  | nil => simpa using hys
  | cons x xs ih =>
      have hhead : ∀ y ∈ ys, rowLt x y := by intro y hy; exact hcross x (by simp) y hy
      have hx_all : ∀ y ∈ xs, rowLt x y := (List.pairwise_cons.mp hxs).1
      have hcross' : ∀ a ∈ xs, ∀ b ∈ ys, rowLt a b := by
        intro a ha b hb
        exact hcross a (by simp [ha]) b hb
      change List.Pairwise rowLt (x :: (xs ++ ys))
      rw [List.pairwise_cons]
      constructor
      · intro y hy
        rcases List.mem_append.mp hy with hyxs | hys'
        · exact hx_all y hyxs
        · exact hhead y hys'
      · exact ih (List.pairwise_cons.mp hxs).2 hcross'

theorem pairwise_of_sublist {R : Row → Row → Prop} {l l' : List Row}
    (hsub : l'.Sublist l) (h : List.Pairwise R l) : List.Pairwise R l' := by
  induction hsub with
  | slnil => exact List.Pairwise.nil
  | cons a hsub ih => exact ih h.tail
  | cons_cons a hsub ih =>
      exact List.Pairwise.cons (by
        intro z hz
        exact (List.pairwise_cons.mp h).1 z (hsub.subset hz)) (ih (List.pairwise_cons.mp h).2)

theorem sublist_append_right (run rest : List Row) : rest.Sublist (run ++ rest) := by
  induction run with
  | nil => exact List.Sublist.refl rest
  | cons x xs ih => exact List.Sublist.cons x ih

theorem sublist_append_left (run rest : List Row) : run.Sublist (run ++ rest) := by
  induction run with
  | nil => simp
  | cons x xs ih => exact List.Sublist.cons_cons x ih

theorem runSortSingle_sorted (rs : List Row) (hps : PrefixSorted rs) (hnd : rs.Nodup) :
    SortedRows (runSortSingle rs) := by
  match rs with
  | [] =>
      rw [runSortSingle, splitRuns.eq_1]
      simp [SortedRows]
  | r :: tail =>
      let run := (spanRun r.pkey tail).1
      let rest := (spanRun r.pkey tail).2
      have hrun_prefix : ∀ x ∈ run, x.pkey = r.pkey := spanRun_prefix_all r.pkey tail
      have hrest_gt : ∀ x ∈ rest, r.pkey < x.pkey := by
        dsimp [rest]
        exact spanRun_rest_gt r tail hps
      have hc := spanRun_concat r.pkey tail
      have hsub_rest_tail : rest.Sublist tail := by
        rw [← hc]
        exact sublist_append_right run rest
      have hps_rest : PrefixSorted rest :=
        pairwise_of_sublist hsub_rest_tail (List.pairwise_cons.mp hps).2
      have hnd_tail : tail.Nodup := (List.pairwise_cons.mp hnd).2
      have hnd_runrest : (run ++ rest).Nodup := by rw [hc]; exact hnd_tail
      have hnd_rest : rest.Nodup := (sublist_append_right run rest).nodup hnd_runrest
      have ihrest := runSortSingle_sorted rest hps_rest hnd_rest
      rw [runSortSingle, splitRuns.eq_2]
      change SortedRows (isort (r :: run) ++ runSortSingle rest)
      apply sorted_append
      · have hnd_run : run.Nodup := (sublist_append_left run rest).nodup hnd_runrest
        have hr_not_tail : r ∉ tail := by
          intro hrt
          exact ((List.pairwise_cons.mp hnd).1 r hrt) rfl
        have hr_not_run : r ∉ run := by
          intro h
          apply hr_not_tail
          have : r ∈ run ++ rest := List.mem_append.mpr (Or.inl h)
          rw [hc] at this
          exact this
        apply isort_sorted
        exact List.Pairwise.cons (by
          intro y hy hyr
          exact hr_not_run (by simpa [hyr] using hy)) hnd_run
      · exact ihrest
      · intro a ha b hb
        have hmem_a : a ∈ r :: run := (isort_perm (r :: run)).mem_iff.mp ha
        have hmem_b : b ∈ rest := (runSortSingle_perm rest).mem_iff.mp hb
        have hpa : a.pkey = r.pkey := by
          rcases List.mem_cons.mp hmem_a with rfl | harun
          · rfl
          · exact hrun_prefix a harun
        have hgt := hrest_gt b hmem_b
        unfold rowLt
        omega
termination_by rs.length
decreasing_by
  have hc := spanRun_concat r.pkey tail
  have hlen := congrArg List.length hc
  simp only [List.length_append] at hlen
  simp only [List.length_cons]
  omega
def runSort (files : List (List Row)) : List Row := runSortSingle files.flatten

theorem runSort_sorted (files : List (List Row)) (hps : PrefixSorted files.flatten)
    (hnd : files.flatten.Nodup) : SortedRows (runSort files) := by
  simpa [runSort] using runSortSingle_sorted files.flatten hps hnd

theorem runSort_perm (files : List (List Row)) : (runSort files).Perm files.flatten := by
  simpa [runSort] using runSortSingle_perm files.flatten

/-- Local run sorting followed by concatenation is exactly the global stable
    sort (the reference computation). -/
theorem runSort_eq_isort (files : List (List Row)) (hps : PrefixSorted files.flatten)
    (hnd : files.flatten.Nodup) : runSort files = isort files.flatten := by
  exact sorted_perm_eq (runSort_sorted files hps hnd) (isort_sorted files.flatten hnd)
    ((runSort_perm files).trans (isort_perm files.flatten).symm)

/- ------------------------------------------------------------------------ -
    List helpers, boundary filter vs early stop
    ------------------------------------------------------------------------ -/

theorem takeWhile_subset (p : Row → Bool) (l : List Row) (x : Row) :
    x ∈ l.takeWhile p → x ∈ l := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      intro hx
      by_cases hp : p y = true
      · simp [hp] at hx
        rcases hx with rfl | hx
        · simp
        · exact List.mem_cons_of_mem y (ih hx)
      · simp [hp] at hx

theorem takeWhile_eq_take_length (p : Row → Bool) (l : List Row) :
    l.takeWhile p = l.take ((l.takeWhile p).length) := by
  induction l with
  | nil => rfl
  | cons y ys ih =>
      by_cases hp : p y = true
      · rw [List.takeWhile_cons, if_pos hp]
        simp only [List.length_cons]
        rw [List.take_succ_cons]
        exact congrArg (fun xs => y :: xs) ih
      · rw [List.takeWhile_cons, if_neg hp]
        rfl

theorem takeWhile_length_le (p : Row → Bool) (l : List Row) :
    (l.takeWhile p).length ≤ l.length := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      by_cases hp : p y = true <;> simp [hp] at ih ⊢ <;> omega

theorem drop_takeWhile_len (p : Row → Bool) (l : List Row) :
    l.drop ((l.takeWhile p).length) = l.dropWhile p := by
  have h := List.takeWhile_append_dropWhile (p := p) (l := l)
  rw [← h, List.drop_append]
  simp

abbrev KeyPair := Int × Int

def rowKey (r : Row) : KeyPair := (r.pkey, r.skey)

/-- `key ≤ boundary`, ignoring the id: WITH TIES keeps every row tied with
    the k-th row. -/
def keyLe (r : Row) (b : KeyPair) : Prop :=
  r.pkey < b.1 ∨ (r.pkey = b.1 ∧ r.skey ≤ b.2)

instance (r : Row) (b : KeyPair) : Decidable (keyLe r b) := by
  unfold keyLe
  infer_instance

theorem keyLe_of_rowLt_of_keyLe {a b : Row} {k : KeyPair}
    (hab : rowLt a b) (hb : keyLe b k) : keyLe a k := by
  unfold rowLt keyLe at *
  omega

theorem filter_eq_takeWhile_keyLe (b : KeyPair) {rs : List Row} (hs : SortedRows rs) :
    rs.filter (fun r => decide (keyLe r b)) =
    rs.takeWhile (fun r => decide (keyLe r b)) := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      have hr_all : ∀ y ∈ rs, rowLt r y := (List.pairwise_cons.mp hs).1
      have htail : SortedRows rs := (List.pairwise_cons.mp hs).2
      by_cases hrb : keyLe r b
      · have hcond : decide (keyLe r b) = true := by simp [hrb]
        rw [List.filter_cons, List.takeWhile_cons, if_pos hcond, if_pos hcond]
        exact congrArg (fun xs => r :: xs) (ih htail)
      · have hnone : ∀ y ∈ rs, ¬ keyLe y b := by
          intro y hy
          intro hyl
          exact hrb (keyLe_of_rowLt_of_keyLe (hr_all y hy) hyl)
        have hf : rs.filter (fun r => decide (keyLe r b)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro a ha
          simp [hnone a ha]
        have hcond : ¬ decide (keyLe r b) = true := by simp [hrb]
        rw [List.filter_cons, List.takeWhile_cons, if_neg hcond, if_neg hcond]
        exact hf

/-- Every row of `dropWhile` is strictly worse than the boundary. -/
theorem not_keyLe_of_dropWhile (b : KeyPair) {rs : List Row} (hs : SortedRows rs) :
    ∀ r ∈ rs.dropWhile (fun r => decide (keyLe r b)), ¬ keyLe r b := by
  induction rs with
  | nil => intro r hr; simp at hr
  | cons y ys ih =>
      have hy_all : ∀ z ∈ ys, rowLt y z := (List.pairwise_cons.mp hs).1
      have htail : SortedRows ys := (List.pairwise_cons.mp hs).2
      intro r hr
      by_cases hyb : keyLe y b
      · simp [hyb] at hr
        exact ih htail r hr
      · simp [hyb] at hr
        rcases hr with rfl | hry
        · exact hyb
        · intro hl
          exact hyb (keyLe_of_rowLt_of_keyLe (hy_all r hry) hl)

/-- Boundary key of the k-th row after the offset. -/
def boundaryKey (after : List Row) (k : Nat) : KeyPair :=
  match after.take k with
  | [] => (0, 0)
  | r :: rs => rowKey (rs.getLast?.getD r)

/-- **Reference**: full scan with `filter`. -/
def refPage (offset k : Nat) (sorted : List Row) : List Row × KeyPair :=
  if k = 0 then ([], (0, 0)) else
  let after := sorted.drop offset
  if after.length ≤ k then (after, (0, 0))
  else
    let b := boundaryKey after k
    (after.filter (fun r => decide (keyLe r b)), b)

/-- **Optimized watermark**: stop at the first row strictly past the
    boundary with `takeWhile`, returning an explicit read count. -/
def incPage (offset k : Nat) (sorted : List Row) : List Row × KeyPair × Nat :=
  if k = 0 then ([], (0, 0), 0) else
  let after := sorted.drop offset
  if after.length ≤ k then (after, (0, 0), sorted.length)
  else
    let b := boundaryKey after k
    let kept := after.takeWhile (fun r => decide (keyLe r b))
    (kept, b, offset + kept.length)

/-- Zero demand has no output or boundary rows and consumes no page rows. -/
theorem fetch_zero (offset : Nat) (rows : List Row) :
    (refPage offset 0 rows).1 = [] ∧
    incPage offset 0 rows = ([], (0, 0), 0) := by
  exact ⟨rfl, rfl⟩

/- ------------------------------------------------------------------------ -
    Main theorem
    ------------------------------------------------------------------------ -/

/-- **Main theorem.** The early-stop page has exactly the same payloads and
    WITH TIES boundary as the full-scan reference on the global stable sort. -/
theorem incremental_sort_page_sound (offset k : Nat) (files : List (List Row))
    (hps : PrefixSorted files.flatten) (hnd : files.flatten.Nodup) :
    let sorted := runSort files
    let (outR, bR) := refPage offset k sorted
    let (outO, bO, _) := incPage offset k sorted
    outO = outR ∧ bO = bR ∧
    (∀ r ∈ outO, r ∈ sorted) ∧ (∀ r ∈ outO, r ∈ files.flatten) := by
  let sorted := runSort files
  let after := sorted.drop offset
  change (let (outR, bR) := refPage offset k sorted
          let (outO, bO, _) := incPage offset k sorted
          outO = outR ∧ bO = bR ∧
          (∀ r ∈ outO, r ∈ sorted) ∧ (∀ r ∈ outO, r ∈ files.flatten))
  by_cases hz : k = 0
  · subst k
    simp [refPage, incPage]
  dsimp [refPage, incPage]
  simp only [if_neg hz]
  by_cases hsmall : after.length ≤ k
  · rw [if_pos hsmall, if_pos hsmall]
    dsimp
    refine ⟨rfl, rfl, ?_, ?_⟩
    · intro r hr
      exact mem_drop_subset sorted offset r hr
    · intro r hr
      exact (runSort_perm files).mem_iff.mp (mem_drop_subset sorted offset r hr)
  · let b := boundaryKey after k
    let kept := after.takeWhile (fun r => decide (keyLe r b))
    have hs : SortedRows after := List.Pairwise.drop (runSort_sorted files hps hnd)
    have hfilter : after.filter (fun r => decide (keyLe r b)) = kept := by
      dsimp [kept]
      exact filter_eq_takeWhile_keyLe b hs
    have hsub : ∀ r ∈ kept, r ∈ sorted := by
      intro r hr
      exact mem_drop_subset sorted offset r (takeWhile_subset _ after r (by simpa [kept] using hr))
    have hsrc : ∀ r ∈ kept, r ∈ files.flatten := by
      intro r hr
      exact (runSort_perm files).mem_iff.mp (hsub r hr)
    rw [if_neg hsmall, if_neg hsmall]
    change kept = after.filter (fun r => decide (keyLe r b)) ∧ b = b ∧
      (∀ r ∈ kept, r ∈ sorted) ∧ (∀ r ∈ kept, r ∈ files.flatten)
    exact ⟨hfilter.symm, rfl, hsub, hsrc⟩

/- ------------------------------------------------------------------------ -
    Cost / kept-row bounds
    ------------------------------------------------------------------------ -/

inductive Event where
  | Read | Compare | Emit
  deriving DecidableEq, Repr

def evalCost (events : List Event) : Nat := events.length

def eventTrace (reads compares emits : Nat) : List Event :=
  List.replicate reads Event.Read ++
  List.replicate compares Event.Compare ++
  List.replicate emits Event.Emit

theorem evalCost_eventTrace_eq (reads compares emits : Nat) :
    evalCost (eventTrace reads compares emits) = reads + compares + emits := by
  simp [evalCost, eventTrace]
  omega

/-- Maximum length of an unresolved run. -/
def maxRunLen : List (List Row) → Nat
  | [] => 0
  | r :: rs => max r.length (maxRunLen rs)

theorem len_le_maxRunLen (rs : List (List Row)) (run : List Row) (h : run ∈ rs) :
    run.length ≤ maxRunLen rs := by
  induction rs with
  | nil => simp at h
  | cons x xs ih =>
      simp [maxRunLen]
      rcases List.mem_cons.mp h with hrx | hrxs
      · subst run
        exact Nat.le_max_left x.length (maxRunLen xs)
      · exact Nat.le_trans (ih hrxs) (Nat.le_max_right x.length (maxRunLen xs))

theorem sum_len_le (rs : List (List Row)) :
    (rs.map List.length).sum ≤ rs.length * maxRunLen rs := by
  induction rs with
  | nil => simp
  | cons x xs ih =>
      simp [maxRunLen]
      have hx : x.length ≤ max (x.length) (maxRunLen xs) := Nat.le_max_left _ _
      have hxs : maxRunLen xs ≤ max (x.length) (maxRunLen xs) := Nat.le_max_right _ _
      have ih' : (xs.map List.length).sum ≤ xs.length * maxRunLen xs := ih
      have : xs.length * maxRunLen xs ≤ xs.length * max (x.length) (maxRunLen xs) :=
        Nat.mul_le_mul_left xs.length hxs
      have hf : (xs.length + 1) * max (x.length) (maxRunLen xs) =
          xs.length * max (x.length) (maxRunLen xs) + max (x.length) (maxRunLen xs) := by
        simp [Nat.add_mul]
      omega

/- ------------------------------------------------------------------------ -
    Cost / kept-row bounds
    ------------------------------------------------------------------------ -/

theorem incremental_sort_page_bounded (offset k : Nat) (hk : k ≠ 0) (files : List (List Row))
    (hps : PrefixSorted files.flatten) (hnd : files.flatten.Nodup) :
    let sorted := runSort files
    let (outO, b, reads) := incPage offset k sorted
    reads ≤ sorted.length ∧
    (∀ r ∈ sorted.drop reads, ¬ keyLe r b) ∧
    evalCost (eventTrace reads reads outO.length) ≤ 3 * sorted.length := by
  let sorted := runSort files
  let after := sorted.drop offset
  change (let (outO, b, reads) := incPage offset k sorted
          reads ≤ sorted.length ∧
          (∀ r ∈ sorted.drop reads, ¬ keyLe r b) ∧
          evalCost (eventTrace reads reads outO.length) ≤ 3 * sorted.length)
  dsimp [incPage]
  simp only [if_neg hk]
  by_cases hsmall : after.length ≤ k
  · rw [if_pos hsmall]
    dsimp
    constructor
    · exact Nat.le_refl sorted.length
    constructor
    · intro r hr
      simp at hr
    · rw [evalCost_eventTrace_eq]
      have hlen_after : after.length = sorted.length - offset := by simp [after]
      have hlen : (List.drop offset sorted).length ≤ sorted.length := by
        have := List.length_drop (l := sorted) (i := offset)
        omega
      omega
  · let b := boundaryKey after k
    let kept := after.takeWhile (fun r => decide (keyLe r b))
    rw [if_neg hsmall]
    dsimp
    constructor
    · have hlen_kept : kept.length ≤ after.length := by
        dsimp [kept]
        exact takeWhile_length_le (fun r => decide (keyLe r b)) after
      have hlen_after : after.length = sorted.length - offset := by simp [after]
      have hpos : 0 < after.length := by omega
      have hoff_le : offset ≤ sorted.length := by
        by_cases hle : offset ≤ sorted.length
        · exact hle
        · have hgt : sorted.length < offset := by omega
          have haz : sorted.length - offset = 0 := by omega
          omega
      have hsum : after.length + offset = sorted.length := by omega
      have hread : offset + kept.length ≤ sorted.length := by omega
      omega
    constructor
    · intro r hr
      have hdrop : sorted.drop (offset + kept.length) = after.drop kept.length := by
        dsimp [after]
        rw [List.drop_drop]
      have hdw : after.drop kept.length =
          after.dropWhile (fun r => decide (keyLe r b)) := by
        dsimp [kept]
        exact drop_takeWhile_len (fun r => decide (keyLe r b)) after
      have hs : SortedRows after := List.Pairwise.drop (runSort_sorted files hps hnd)
      rw [hdrop, hdw] at hr
      exact not_keyLe_of_dropWhile b hs r hr
    · rw [evalCost_eventTrace_eq]
      have hlen_kept : kept.length ≤ after.length := by
        dsimp [kept]
        exact takeWhile_length_le (fun r => decide (keyLe r b)) after
      have hlen_after : after.length ≤ sorted.length := by simp [after]
      have hle : kept.length ≤ sorted.length := by omega
      have hread : offset + kept.length ≤ sorted.length := by
        have ha : after.length = sorted.length - offset := by simp [after]
        by_cases hleoff : offset ≤ sorted.length
        · have hsum : after.length + offset = sorted.length := by
            rw [ha]
            exact Nat.sub_add_cancel hleoff
          omega
        · have : sorted.length ≤ offset := by omega
          omega
      change offset + kept.length + (offset + kept.length) + kept.length ≤ 3 * sorted.length
      have h2 := Nat.add_le_add hread hread
      have h3 := Nat.add_le_add h2 hle
      omega

/-- All rows tied with the boundary share one `pkey`; since runs are maximal
    same-`pkey` blocks of a prefix-sorted input, the tied suffix fits in a
    single unresolved run.  Hence its length is at most `maxRunLen`. -/
/- ------------------------------------------------------------------------ -
    Counterexample: closing a run at a file boundary
    ------------------------------------------------------------------------ -/

def f1 : List Row :=
  [ { pkey := 1, skey := 5, payload := 50, id := 0 },
    { pkey := 1, skey := 9, payload := 90, id := 1 } ]

def f2 : List Row :=
  [ { pkey := 1, skey := 3, payload := 30, id := 2 },
    { pkey := 1, skey := 4, payload := 40, id := 3 } ]

def twoFiles : List (List Row) := [f1, f2]

/-- Closing a run at each file boundary sorts `f1` and `f2` separately and
    concatenates: the smallest tail key (3, from the second file) then does
    not reach the first page. -/
def fileBoundaryRunSort (files : List (List Row)) : List Row :=
  files.flatMap (fun f => (splitRuns f).map isort |>.flatten)

/-- **Counterexample:** the file-boundary closure loses the smallest key;
    the correct run-aware page emits it. -/
theorem file_boundary_run_split_counterexample :
    (fileBoundaryRunSort twoFiles).map Row.skey = [5, 9, 3, 4] ∧
    (runSort twoFiles).map Row.skey = [3, 4, 5, 9] ∧
    (refPage 0 1 (runSort twoFiles)).1.map Row.payload = [30] ∧
    (refPage 0 1 (fileBoundaryRunSort twoFiles)).1.map Row.payload = [50, 30, 40] := by
  native_decide

/-- The WITH TIES boundary is a key pair, not an appearance id: tied rows
    keep distinct ids. -/
def sortedTie : List Row :=
  [ { pkey := 1, skey := 2, payload := 20, id := 0 },
    { pkey := 1, skey := 2, payload := 21, id := 2 },
    { pkey := 1, skey := 2, payload := 22, id := 3 },
    { pkey := 1, skey := 7, payload := 70, id := 4 } ]

theorem with_ties_boundary_not_id :
    (refPage 0 1 sortedTie).1.map Row.id = [0, 2, 3] := by
  native_decide

end TinylambOpt.Research.IncrementalSortPage
