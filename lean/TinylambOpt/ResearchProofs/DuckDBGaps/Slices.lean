import TinylambOpt.ResearchGoals.DuckDBGaps.Slices
import TinylambOpt.ResearchProofs.CloudBI.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DuckDBGaps.Slices

open TinylambOpt.ResearchGoals.DuckDBGaps.Slices
open TinylambOpt.ResearchGoals.CloudBI.PublicBI

/-! Common helpers for the DuckDB slice-gap goals. -/

private theorem map_congr' {α β : Type} (f g : α → β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem bne_false (b : Bool) : (false != b) = b := by cases b <;> rfl
private theorem bne_true (b : Bool) : (true != b) = (!b) := by cases b <;> rfl

private theorem bool_not_not (b : Bool) : (!(!b)) = b := by cases b <;> rfl

/-- A list splits into the hits of `P` and of its complement. -/
private theorem filter_len_compl {α : Type} (l : List α) (P : α → Bool) :
    (l.filter P).length + (l.filter (fun x => ! P x)).length = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases h : P x <;> simp [h, ih] <;> omega

private theorem mem_filter_range {P : Nat → Bool} {i n : Nat} :
    i ∈ (List.range n).filter P ↔ i < n ∧ P i = true := by
  rw [List.mem_filter, List.mem_range]

private theorem contains_filter_range (P : Nat → Bool) (i n : Nat) (hlt : i < n) :
    ((List.range n).filter P).contains i = P i := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact (mem_filter_range.mp (List.contains_iff_mem.mp h)).2
  · intro h
    apply List.contains_iff_mem.mpr
    exact mem_filter_range.mpr ⟨hlt, h⟩

/-- The minority-side code list never exceeds half the dictionary. -/
theorem sound_MinorityCodes :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.MinorityCodes.Goal := by
  intro c hc
  dsimp only
  unfold labels exceptions
  dsimp only
  split
  · dsimp only [Prod.fst, Prod.snd]
    constructor
    · rw [List.map_map]
      apply map_congr'
      intro i hi
      have hlt : i < c.dict.size := hc i hi
      rw [contains_filter_range (fun i => outfield c.dict[i]?.join) i c.dict.size hlt, bne_false]
      rfl
    · have hlen := filter_len_compl (List.range c.dict.size) (fun i => outfield c.dict[i]?.join)
      rw [List.length_range] at hlen
      omega
  · dsimp only [Prod.fst, Prod.snd]
    constructor
    · rw [List.map_map]
      apply map_congr'
      intro i hi
      have hlt : i < c.dict.size := hc i hi
      rw [contains_filter_range (fun i => !outfield c.dict[i]?.join) i c.dict.size hlt, bne_true,
        bool_not_not]
      rfl
    · have hlen := filter_len_compl (List.range c.dict.size) (fun i => outfield c.dict[i]?.join)
      rw [List.length_range] at hlen
      omega

/-- The minority classification of a valid old code agrees with its label. -/
private theorem exceptions_classify (d : Array (Option String)) (i : Nat) (hlt : i < d.size) :
    (let (D, ex) := exceptions d; D != ex.contains i) = outfield d[i]?.join := by
  unfold exceptions
  dsimp only
  split
  · dsimp only [Prod.fst, Prod.snd]
    rw [contains_filter_range (fun i => outfield d[i]?.join) i d.size hlt, bne_false]
  · dsimp only [Prod.fst, Prod.snd]
    rw [contains_filter_range (fun i => !outfield d[i]?.join) i d.size hlt, bne_true, bool_not_not]

/-- Every code stored in the minority list is in range. -/
private theorem exceptions_mem_lt (d : Array (Option String)) {i : Nat}
    (hi : i ∈ (let (_, ex) := exceptions d; ex)) : i < d.size := by
  unfold exceptions at hi
  dsimp only at hi
  split at hi
  · exact (mem_filter_range.mp hi).1
  · exact (mem_filter_range.mp hi).1

private theorem bne_false_right (b : Bool) : (b != false) = b := by cases b <;> rfl
private theorem bne_true_right (b : Bool) : (b != true) = (!b) := by cases b <;> rfl

/-- The appended-entry classification agrees with its own label. -/
private theorem patch_classify (d added : Array (Option String)) (j : Nat) (hlt : j < added.size) :
    ((exceptions d).1 != ((List.range added.size).filter
      (fun i => outfield added[i]?.join != (exceptions d).1)).contains j) =
      outfield added[j]?.join := by
  unfold exceptions
  dsimp only
  split
  · dsimp only [Prod.fst]
    rw [contains_filter_range (fun i => outfield added[i]?.join != false) j added.size hlt]
    rw [bne_false_right, bne_false]
  · dsimp only [Prod.fst]
    rw [contains_filter_range (fun i => outfield added[i]?.join != true) j added.size hlt]
    rw [bne_true_right, bne_true, bool_not_not]

/-- Decoding an appended array: the old range is unchanged. -/
private theorem append_get_left (d added : Array (Option String)) (i : Nat) (hlt : i < d.size) :
    (d ++ added)[i]?.join = d[i]?.join := by
  rw [Array.getElem?_append]
  simp [hlt]

/-- Decoding an appended array: an appended index shifts by the old size. -/
private theorem append_get_right (d added : Array (Option String)) (i : Nat) (hge : d.size ≤ i) :
    (d ++ added)[i]?.join = added[i - d.size]?.join := by
  rw [Array.getElem?_append]
  simp [show ¬ i < d.size by omega]

/-- Out-of-range appended-array positions decode to SQL NULL. -/
private theorem append_get_oob (d added : Array (Option String)) (i : Nat)
    (hge : d.size + added.size ≤ i) : (d ++ added)[i]?.join = none := by
  have h : (d ++ added).size ≤ i := by
    rw [Array.size_append]
    omega
  rw [Array.getElem?_eq_none_iff.mpr h]
  rfl

/-- Appending a shifted list cannot hit a position below the shift. -/
private theorem contains_shift_old {old patch : List Nat} (k : Nat) (hold : ∀ x ∈ old, x < k)
    (i : Nat) (hlt : i < k) :
    (old ++ patch.map (k + ·)).contains i = old.contains i := by
  rw [List.contains_append]
  have hshift : (patch.map (k + ·)).contains i = false := by
    apply Bool.eq_false_of_ne_true
    intro ht
    obtain ⟨j, hj, heq⟩ := List.mem_map.mp (List.contains_iff_mem.mp ht)
    have : k + j = i := heq
    omega
  rw [hshift, Bool.or_false]

/-- Appending a shifted list cannot be found in the old (smaller) range. -/
private theorem contains_shift_patch {old patch : List Nat} (k : Nat) (hold : ∀ x ∈ old, x < k)
    (i : Nat) (hge : k ≤ i) :
    (old ++ patch.map (k + ·)).contains i = (patch.map (k + ·)).contains i := by
  rw [List.contains_append]
  have holdc : old.contains i = false := by
    apply Bool.eq_false_of_ne_true
    intro ht
    have hm := List.contains_iff_mem.mp ht
    have := hold i hm
    omega
  rw [holdc, Bool.false_or]

/-- Membership in a shifted list translates back to the unshifted index. -/
private theorem contains_map_add {patch : List Nat} (k m : Nat) (hpatch : ∀ x ∈ patch, x < m)
    (i : Nat) (hge : k ≤ i) :
    (patch.map (k + ·)).contains i = patch.contains (i - k) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro ht
    obtain ⟨j, hj, heq⟩ := List.mem_map.mp (List.contains_iff_mem.mp ht)
    have hjlt := hpatch j hj
    have hji : j = i - k := by omega
    rw [← hji]
    exact List.contains_iff_mem.mpr hj
  · intro ht
    have hj := List.contains_iff_mem.mp ht
    apply List.contains_iff_mem.mpr
    apply List.mem_map.mpr
    refine ⟨i - k, hj, ?_⟩
    omega

/-- The pointwise classification of every (potentially appended) code. -/
private theorem append_map_pointwise (d added : Array (Option String)) (D : Bool) (old : List Nat)
    (hold : ∀ x ∈ old, x < d.size)
    (hclass_old : ∀ (i : Nat) (hlt : i < d.size), (D != old.contains i) = outfield d[i]?.join)
    (hclass_patch : ∀ (j : Nat) (hlt : j < added.size),
      (D != ((List.range added.size).filter (fun i => outfield added[i]?.join != D)).contains j) =
        outfield added[j]?.join)
    (ids : List Nat) :
    ids.map (fun i => decide (i < d.size + added.size) &&
        (D != (old ++ ((List.range added.size).filter
          (fun i => outfield added[i]?.join != D)).map (d.size + ·)).contains i)) =
      ids.map (fun i => outfield (d ++ added)[i]?.join) := by
  apply map_congr'
  intro i hi
  let patch := (List.range added.size).filter (fun i => outfield added[i]?.join != D)
  have hpatch : ∀ x ∈ patch, x < added.size := by
    intro x hx
    exact (mem_filter_range.mp hx).1
  by_cases hlt1 : i < d.size
  · have hlt2 : i < d.size + added.size := by omega
    rw [contains_shift_old d.size hold i hlt1 (patch := patch), hclass_old i hlt1,
      append_get_left d added i hlt1, decide_eq_true hlt2]
    rfl
  · by_cases hlt2 : i < d.size + added.size
    · have hge : d.size ≤ i := Nat.le_of_not_gt hlt1
      have hsublt : i - d.size < added.size := by omega
      rw [contains_shift_patch d.size hold i hge (patch := patch),
        contains_map_add d.size added.size hpatch i hge,
        hclass_patch (i - d.size) hsublt, append_get_right d added i hge,
        decide_eq_true hlt2]
      rfl
    · have hge : d.size + added.size ≤ i := Nat.le_of_not_gt hlt2
      rw [append_get_oob d added i hge]
      simp [hlt2]
      rfl

theorem sound_AppendDictionary :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.AppendDictionary.Goal := by
  intro d added ids
  dsimp only
  unfold labels
  rw [List.map_map]
  dsimp
  change ids.map (fun i => decide (i < d.size + added.size) &&
      ((exceptions d).1 != ((exceptions d).2 ++ ((List.range added.size).filter
        (fun j => outfield added[j]?.join != (exceptions d).1)).map (d.size + ·)).contains i)) =
    ids.map (fun i => outfield (d ++ added)[i]?.join)
  let D := (exceptions d).1
  let old := (exceptions d).2
  have hold : ∀ x ∈ old, x < d.size := by
    intro x hx
    exact exceptions_mem_lt d hx
  have hclass_old : ∀ (i : Nat) (hlt : i < d.size), (D != old.contains i) = outfield d[i]?.join := by
    intro i hlt
    exact exceptions_classify d i hlt
  have hclass_patch : ∀ (j : Nat) (hlt : j < added.size),
      (D != ((List.range added.size).filter (fun i => outfield added[i]?.join != D)).contains j) =
        outfield added[j]?.join := by
    intro j hlt
    exact patch_classify d added j hlt
  simpa [D, old] using append_map_pointwise d added D old hold hclass_old hclass_patch ids

/-- Splitting a list by a Boolean predicate splits its weighted Nat sum. -/
private theorem sum_filter_partition {α : Type} (l : List α) (P : α → Bool) (w : α → Nat) :
    ((l.filter P).map w).sum + ((l.filter (fun x => ! P x)).map w).sum = (l.map w).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases h : P x <;> simp [h, ih] <;> omega

/-- A weighted sum of if-then-else weights equals the sum over the filtered list. -/
private theorem sum_if_eq_filter_sum {α : Type} (l : List α) (P : α → Bool) (w : α → Nat) :
    (l.map (fun x => if P x then w x else 0)).sum = ((l.filter P).map w).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases h : P x <;> simp [h, ih]

/-- Nested filtering equals filtering by the conjunction. -/
private theorem filter_filter_and {α : Type} (l : List α) (p q : α → Bool) :
    (l.filter p).filter q = l.filter (fun x => p x && q x) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hp : p x <;> cases hq : q x <;> simp [hp, hq, ih]

/-- Counting hits in a bag built from weighted keys. -/
private theorem countP_flatMap_replicate {α : Type} (counts : List (α × Nat)) (p : α → Bool) :
    (counts.flatMap (fun (k, n) => List.replicate n k)).countP p =
      (counts.map (fun x => if p x.1 then x.2 else 0)).sum := by
  induction counts with
  | nil => rfl
  | cons x xs ih =>
      rcases x with ⟨k, n⟩
      simp [List.flatMap_cons, List.countP_append, ih, List.countP_replicate]

private theorem sum_if_and_filter {α : Type} (l : List α) (p q : α → Bool) (w : α → Nat) :
    (l.map (fun x => if p x && q x then w x else 0)).sum =
      (((l.filter p).filter q).map w).sum := by
  rw [sum_if_eq_filter_sum, filter_filter_and]

private theorem beq_true_eq_self (b : Bool) : (b == true) = b := by cases b <;> rfl
private theorem beq_false_eq_not (b : Bool) : (b == false) = (!b) := by cases b <;> rfl

/-- The reference bag count for a flag is the sum of that flag's group weights. -/
private theorem complement_count (counts : List (Dim × Nat)) (league : Option String) (flag : Bool) :
    (counts.flatMap (fun (k, n) => List.replicate n k)).countP
        (fun (l, p) => l == league && outfield p == flag) =
      (((counts.filter (fun x => x.1.1 == league)).filter
        (fun x => outfield x.1.2 == flag)).map Prod.snd).sum := by
  rw [countP_flatMap_replicate]
  rw [sum_if_and_filter]

/-- Looking up a flag in the (positive-only) complement pair. -/
private theorem lookup_splitCounts (t f : Nat) (flag : Bool) :
    ([(true, t), (false, f)].filter (fun x => x.2 > 0)).lookup flag =
      (if flag then (if t > 0 then some t else none) else (if f > 0 then some f else none)) := by
  cases flag <;> by_cases ht : t > 0 <;> by_cases hf : f > 0 <;> simp [List.lookup, ht, hf]

theorem sound_ComplementGroups :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.ComplementGroups.Goal := by
  intro counts league flag
  dsimp only
  let sel := counts.filter (fun x => x.1.1 == league)
  let tot := (sel.map Prod.snd).sum
  let t := ((sel.filter (fun x => outfield x.1.2)).map Prod.snd).sum
  let f := ((sel.filter (fun x => ! outfield x.1.2)).map Prod.snd).sum
  have htf : t + f = tot := by
    dsimp [t, f, tot]
    exact sum_filter_partition sel (fun x => outfield x.1.2) Prod.snd
  have hsub : tot - t = f := by omega
  have hcnt := complement_count counts league flag
  cases flag with
  | true =>
      have hflag : ((sel.filter (fun x => outfield x.1.2 == true)).map Prod.snd).sum = t := by
        dsimp [t]
        simp [beq_true_eq_self]
      rw [hcnt, hflag]
      change ([(true, t), (false, tot - t)].filter (fun x => x.2 > 0)).lookup true =
        (if t == 0 then none else some t)
      rw [hsub, lookup_splitCounts]
      by_cases ht : t > 0 <;> simp [ht] <;> omega
  | false =>
      have hflag : ((sel.filter (fun x => outfield x.1.2 == false)).map Prod.snd).sum = f := by
        dsimp [f]
        simp [beq_false_eq_not]
      rw [hcnt, hflag]
      change ([(true, t), (false, tot - t)].filter (fun x => x.2 > 0)).lookup false =
        (if f == 0 then none else some f)
      rw [hsub, lookup_splitCounts]
      by_cases hf : f > 0 <;> simp [hf] <;> omega

/-- Optional extrema, used to fold `mergeExt`. -/
private def mmax (x y : Option Int) : Option Int := (([x, y]).filterMap id).max?
private def mmin (x y : Option Int) : Option Int := (([x, y]).filterMap id).min?

private theorem mmax_none (o : Option Int) : mmax none o = o := by cases o <;> rfl
private theorem mmax_right_none (o : Option Int) : mmax o none = o := by cases o <;> rfl
private theorem mmin_none (o : Option Int) : mmin none o = o := by cases o <;> rfl
private theorem mmin_right_none (o : Option Int) : mmin o none = o := by cases o <;> rfl

private theorem mmax_assoc (a b m : Option Int) : mmax (mmax a b) m = mmax a (mmax b m) := by
  cases a <;> cases b <;> cases m <;> simp [mmax, Int.max_assoc]
private theorem mmin_assoc (a b m : Option Int) : mmin (mmin a b) m = mmin a (mmin b m) := by
  cases a <;> cases b <;> cases m <;> simp [mmin, Int.min_assoc]

private theorem maxAppend (xs ys : List Int) : (xs ++ ys).max? = mmax xs.max? ys.max? := by
  cases hx : xs.max? with
  | none =>
      have hxs : xs = [] := List.max?_eq_none_iff.mp hx
      subst hxs
      rw [mmax_none, List.nil_append]
  | some x =>
      obtain ⟨hxm, hxb⟩ := (List.max?_eq_some_iff (α := Int)).mp hx
      cases hy : ys.max? with
      | none =>
          have hys : ys = [] := List.max?_eq_none_iff.mp hy
          subst hys
          rw [mmax_right_none, List.append_nil]
          exact hx
      | some y =>
          obtain ⟨hym, hyb⟩ := (List.max?_eq_some_iff (α := Int)).mp hy
          have hmn : mmax (some x) (some y) = some (max x y) := rfl
          rw [hmn, show (max x y) = (if x ≤ y then y else x) from rfl]
          split
          · refine List.max?_eq_some_iff.mpr ⟨List.mem_append.mpr (Or.inr hym), ?_⟩
            intro z hz
            rw [List.mem_append] at hz
            rcases hz with hzx | hzy
            · have hbz := hxb z hzx
              omega
            · have hbz := hyb z hzy
              omega
          · refine List.max?_eq_some_iff.mpr ⟨List.mem_append.mpr (Or.inl hxm), ?_⟩
            intro z hz
            rw [List.mem_append] at hz
            rcases hz with hzx | hzy
            · have hbz := hxb z hzx
              omega
            · have hbz := hyb z hzy
              omega

private theorem minAppend (xs ys : List Int) : (xs ++ ys).min? = mmin xs.min? ys.min? := by
  cases hx : xs.min? with
  | none =>
      have hxs : xs = [] := List.min?_eq_none_iff.mp hx
      subst hxs
      rw [mmin_none, List.nil_append]
  | some x =>
      obtain ⟨hxm, hxb⟩ := (List.min?_eq_some_iff (α := Int)).mp hx
      cases hy : ys.min? with
      | none =>
          have hys : ys = [] := List.min?_eq_none_iff.mp hy
          subst hys
          rw [mmin_right_none, List.append_nil]
          exact hx
      | some y =>
          obtain ⟨hym, hyb⟩ := (List.min?_eq_some_iff (α := Int)).mp hy
          have hmn : mmin (some x) (some y) = some (min x y) := rfl
          rw [hmn, show (min x y) = (if x ≤ y then x else y) from rfl]
          split
          · refine List.min?_eq_some_iff.mpr ⟨List.mem_append.mpr (Or.inl hxm), ?_⟩
            intro z hz
            rw [List.mem_append] at hz
            rcases hz with hzx | hzy
            · have hbz := hxb z hzx
              omega
            · have hbz := hyb z hzy
              omega
          · refine List.min?_eq_some_iff.mpr ⟨List.mem_append.mpr (Or.inr hym), ?_⟩
            intro z hz
            rw [List.mem_append] at hz
            rcases hz with hzx | hzy
            · have hbz := hxb z hzx
              omega
            · have hbz := hyb z hzy
              omega

private theorem max_cons (a : Int) (l : List Int) : (a :: l).max? = mmax (some a) l.max? := by
  simpa using (maxAppend [a] l)
private theorem min_cons (a : Int) (l : List Int) : (a :: l).min? = mmin (some a) l.min? := by
  simpa using (minAppend [a] l)

/-- Pairwise min/max: the highs carry every pair's maximum, the lows its minimum. -/
private theorem pairCandidates_spec : ∀ vs : List Int,
    let (hi, lo) := pairCandidates vs
    hi.max? = vs.max? ∧ lo.min? = vs.min? ∧
      hi.length + lo.length = vs.length + vs.length % 2
  | [] => by simp [pairCandidates]
  | [x] => by simp [pairCandidates]
  | x :: y :: xs => by
      have ih := pairCandidates_spec xs
      dsimp only at ih
      unfold pairCandidates
      dsimp only
      split
      · constructor
        · rw [max_cons y (pairCandidates xs).1, ih.1]
          rw [max_cons x (y :: xs), max_cons y xs]
          rw [← mmax_assoc]
          rw [show mmax (some x) (some y) = some (max x y) from rfl,
            show max x y = y by omega]
        · constructor
          · rw [min_cons x (pairCandidates xs).2, ih.2.1]
            rw [min_cons x (y :: xs), min_cons y xs]
            rw [← mmin_assoc]
            rw [show mmin (some x) (some y) = some (min x y) from rfl,
              show min x y = x by omega]
          · simp [ih.2.2]
            omega
      · constructor
        · rw [max_cons x (pairCandidates xs).1, ih.1]
          rw [max_cons x (y :: xs), max_cons y xs]
          rw [← mmax_assoc]
          rw [show mmax (some x) (some y) = some (max x y) from rfl,
            show max x y = x by omega]
        · constructor
          · rw [min_cons y (pairCandidates xs).2, ih.2.1]
            rw [min_cons x (y :: xs), min_cons y xs]
            rw [← mmin_assoc]
            rw [show mmin (some x) (some y) = some (min x y) from rfl,
              show min x y = y by omega]
          · simp [ih.2.2]
            omega

theorem sound_PairwiseExtrema :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.PairwiseExtrema.Goal := by
  intro xs
  dsimp only
  let vs := xs.filterMap id
  have hs := pairCandidates_spec vs
  dsimp only at hs
  refine ⟨?_, ?_⟩
  · change ((pairCandidates vs).1.max?, (pairCandidates vs).2.min?) = (vs.max?, vs.min?)
    exact Prod.ext hs.1 hs.2.1
  · exact hs.2.2

/-- Merging per-list extrema computes the extrema of the concatenation. -/
private theorem extrema_append (xs ys : List (Option Int)) :
    extrema (xs ++ ys) = mergeExt (extrema xs) (extrema ys) := by
  unfold extrema mergeExt
  dsimp only
  rw [List.filterMap_append, maxAppend, minAppend]
  rfl

theorem sound_DominatedColumn :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.DominatedColumn.Goal := by
  intro seen page lo hi hpage
  dsimp only
  by_cases hdom : dominated (extrema seen) lo hi
  · rw [if_pos hdom]
    unfold dominated at hdom
    rcases hSeen : extrema seen with ⟨hival, loval⟩
    cases hival with
    | none => simp [hSeen] at hdom
    | some mx =>
      cases loval with
      | none => simp [hSeen] at hdom
      | some mn =>
        cases lo with
        | none => simp [hSeen] at hdom
        | some l =>
          cases hi with
          | none => simp [hSeen] at hdom
          | some h =>
            simp [hSeen] at hdom
            have hbnds : mn ≤ l ∧ h ≤ mx := hdom
            have hmax_seen : (seen.filterMap id).max? = some mx := by
              simpa [extrema] using congrArg Prod.fst hSeen
            have hmin_seen : (seen.filterMap id).min? = some mn := by
              simpa [extrema] using congrArg Prod.snd hSeen
            have hmxmem : mx ∈ seen.filterMap id := (List.max?_eq_some_iff.mp hmax_seen).1
            have hmnmem : mn ∈ seen.filterMap id := (List.min?_eq_some_iff.mp hmin_seen).1
            have hle_seen : ∀ v ∈ seen.filterMap id, v ≤ mx :=
              (List.max?_eq_some_iff.mp hmax_seen).2
            have hge_seen : ∀ v ∈ seen.filterMap id, mn ≤ v :=
              (List.min?_eq_some_iff.mp hmin_seen).2
            have kept_bounds : ∀ v ∈ ((page.filter Prod.fst).map Prod.snd).filterMap id,
                mn ≤ v ∧ v ≤ mx := by
              intro v hv
              rcases List.mem_filterMap.mp hv with ⟨y, hy, hyv⟩
              rcases List.mem_map.mp hy with ⟨r, hr, hry⟩
              have hrpage : r ∈ page := (List.mem_filter.mp hr).1
              have hxsome : r.2 = some v := by
                rw [← hry] at hyv
                simpa using hyv
              have hb := hpage r hrpage v hxsome
              have hlv : l ≤ v := by simpa using hb.1
              have hvh : v ≤ h := by simpa using hb.2
              omega
            have hmax_all :
                (((seen ++ (page.filter Prod.fst).map Prod.snd).filterMap id).max?) = some mx := by
              rw [List.filterMap_append]
              refine (List.max?_eq_some_iff.mpr ⟨List.mem_append.mpr (Or.inl hmxmem), ?_⟩)
              intro v hv
              rw [List.mem_append] at hv
              rcases hv with hv | hv
              · exact hle_seen v hv
              · exact (kept_bounds v hv).2
            have hmin_all :
                (((seen ++ (page.filter Prod.fst).map Prod.snd).filterMap id).min?) = some mn := by
              rw [List.filterMap_append]
              refine (List.min?_eq_some_iff.mpr ⟨List.mem_append.mpr (Or.inl hmnmem), ?_⟩)
              intro v hv
              rw [List.mem_append] at hv
              rcases hv with hv | hv
              · exact hge_seen v hv
              · exact (kept_bounds v hv).1
            apply Prod.ext
            · simpa [extrema] using hmax_all.symm
            · simpa [extrema] using hmin_all.symm
  · rw [if_neg hdom]
    exact (extrema_append seen ((page.filter Prod.fst).map Prod.snd)).symm

/-- A non-NULL value decoded from a list position is one of the list's values. -/
private theorem mem_filterMap_of_getElem_join (xs : List (Option Int)) (i : Nat) {v : Int}
    (h : xs[i]?.join = some v) : v ∈ xs.filterMap id := by
  have hget : xs[i]? = some (some v) := by
    cases hxi : xs[i]? with
    | none => simp [hxi] at h
    | some o =>
        cases o with
        | none => simp [hxi] at h
        | some w =>
            simp [hxi] at h
            subst v
            rfl
  obtain ⟨hlt, hval⟩ := (List.getElem?_eq_some_iff.mp hget)
  have hmem : some v ∈ xs := List.mem_of_getElem hval
  exact List.mem_filterMap.mpr ⟨some v, hmem, rfl⟩

theorem sound_WitnessedExtrema :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.WitnessedExtrema.Goal := by
  intro xs ids hi lo h
  dsimp only
  intro hHi hLo
  let gather := ids.map (fun i => xs[i]?.join)
  change extrema gather = (some hi, some lo)
  have hmax : (xs.filterMap id).max? = some hi := by
    simpa [extrema] using congrArg Prod.fst h
  have hmin : (xs.filterMap id).min? = some lo := by
    simpa [extrema] using congrArg Prod.snd h
  have hmaxbound : ∀ v ∈ xs.filterMap id, v ≤ hi := (List.max?_eq_some_iff.mp hmax).2
  have hminbound : ∀ v ∈ xs.filterMap id, lo ≤ v := (List.min?_eq_some_iff.mp hmin).2
  have hi_mem : hi ∈ gather.filterMap id := by
    apply List.mem_filterMap.mpr
    refine ⟨some hi, ?_, rfl⟩
    exact List.contains_iff_mem.mp hHi
  have lo_mem : lo ∈ gather.filterMap id := by
    apply List.mem_filterMap.mpr
    refine ⟨some lo, ?_, rfl⟩
    exact List.contains_iff_mem.mp hLo
  have gather_sub : ∀ v ∈ gather.filterMap id, v ∈ xs.filterMap id := by
    intro v hv
    rcases List.mem_filterMap.mp hv with ⟨y, hy, hyv⟩
    rcases List.mem_map.mp hy with ⟨i, hiids, hiy⟩
    have hjoin : xs[i]?.join = some v := by
      rw [← hiy] at hyv
      simpa using hyv
    exact mem_filterMap_of_getElem_join xs i hjoin
  have hmax_g : (gather.filterMap id).max? = some hi := by
    refine (List.max?_eq_some_iff.mpr ⟨hi_mem, ?_⟩)
    intro v hv
    exact hmaxbound v (gather_sub v hv)
  have hmin_g : (gather.filterMap id).min? = some lo := by
    refine (List.min?_eq_some_iff.mpr ⟨lo_mem, ?_⟩)
    intro v hv
    exact hminbound v (gather_sub v hv)
  apply Prod.ext
  · simpa [extrema] using hmax_g
  · simpa [extrema] using hmin_g

/-- A Bool version of Nat order for `mergeSort`. -/
private theorem nat_le_bool_trans : ∀ a b c : Nat,
    decide (a ≤ b) = true → decide (b ≤ c) = true → decide (a ≤ c) = true := by
  intro a b c h1 h2
  exact decide_eq_true (Nat.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))

private theorem nat_le_bool_total : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
  intro a b
  by_cases h : a ≤ b
  · simp [h]
  · have hba : b ≤ a := Nat.le_of_not_ge h
    simp [h, hba]

private theorem pairwise_range_le : ∀ n : Nat, (List.range n).Pairwise (fun a b : Nat => a ≤ b)
  | 0 => by simp
  | n + 1 => by
      rw [List.range_succ, List.pairwise_append]
      refine ⟨pairwise_range_le n, List.pairwise_singleton _ n, ?_⟩
      intro a ha b hb
      simp at hb
      subst b
      exact Nat.le_of_lt (List.mem_range.mp ha)

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hxnot : x ∉ xs := (List.nodup_cons.mp h).1
      have hnd : xs.Nodup := (List.nodup_cons.mp h).2
      have hnd' : (x :: xs.filter p).Nodup := by
        refine List.Pairwise.cons ?_ (ih hnd)
        intro a ha heq
        exact hxnot (by simpa [heq] using (List.mem_filter.mp ha).1)
      cases hp : p x with
      | true => simpa [hp] using hnd'
      | false => simpa [hp] using (ih hnd)

/-- The deduplicated posting list and the range scan select the same rows. -/
private theorem sparse_mem_iff (n : Nat) (a b c : List Nat) (i : Nat) :
    i ∈ a.eraseDups.filter (fun j => j < n && b.contains j && c.contains j) ↔
      i ∈ (List.range n).filter (fun j => a.contains j && b.contains j && c.contains j) := by
  constructor
  · intro hi
    rw [List.mem_filter] at hi ⊢
    simp only [Bool.and_eq_true] at hi
    rcases hi.2 with ⟨⟨hlt, hb⟩, hc⟩
    have hia : i ∈ a := List.mem_eraseDups.mp hi.1
    refine ⟨List.mem_range.mpr (of_decide_eq_true hlt), ?_⟩
    simp only [Bool.and_eq_true]
    exact ⟨⟨List.contains_iff_mem.mpr hia, hb⟩, hc⟩
  · intro hi
    rw [List.mem_filter] at hi ⊢
    simp only [Bool.and_eq_true] at hi
    rcases hi.2 with ⟨⟨ha, hb⟩, hc⟩
    refine ⟨List.mem_eraseDups.mpr (List.contains_iff_mem.mp ha), ?_⟩
    simp only [Bool.and_eq_true]
    exact ⟨⟨decide_eq_true (List.mem_range.mp hi.1), hb⟩, hc⟩

theorem sound_SparsePosting :
    TinylambOpt.ResearchGoals.DuckDBGaps.Slices.SparsePosting.Goal := by
  intro n a b c values
  dsimp only
  let Q : Nat → Bool := fun i => i < n && b.contains i && c.contains i
  let rows := (List.range n).filter (fun i => a.contains i && b.contains i && c.contains i)
  let le : Nat → Nat → Bool := fun x y => decide (x ≤ y)
  let touched := (a.eraseDups.filter Q).mergeSort le
  have hmem : ∀ i, i ∈ a.eraseDups.filter Q ↔ i ∈ rows := by
    intro i
    exact sparse_mem_iff n a b c i
  have hrows_nodup : rows.Nodup := by
    exact nodup_filter (fun i : Nat => a.contains i && b.contains i && c.contains i)
      List.nodup_range
  have hperm_filter : (a.eraseDups.filter Q).Perm rows :=
    TinylambOpt.ResearchProofs.CloudBI.Helpers.perm_of_nodup_mem
      (nodup_filter Q (TinylambOpt.ResearchProofs.CloudBI.Helpers.nodup_eraseDups a))
      rows hrows_nodup hmem
  have hperm : touched.Perm rows :=
    (List.mergeSort_perm (a.eraseDups.filter Q) le).trans hperm_filter
  have hle_touched : touched.Pairwise (fun x y : Nat => x ≤ y) := by
    exact (List.pairwise_mergeSort nat_le_bool_trans nat_le_bool_total
      (a.eraseDups.filter Q)).imp (fun h => of_decide_eq_true h)
  have hle_rows : rows.Pairwise (fun x y : Nat => x ≤ y) := by
    exact List.Pairwise.filter (fun i : Nat => a.contains i && b.contains i && c.contains i)
      (pairwise_range_le n)
  have heq : touched = rows :=
    List.Perm.eq_of_pairwise (fun x y _ _ hxy hyx => Nat.le_antisymm hxy hyx)
      hle_touched hle_rows hperm
  constructor
  · change touched.map (fun i => values[i]?.join) = rows.map (fun i => values[i]?.join)
    rw [heq]
  · change touched.length ≤ a.eraseDups.length
    rw [List.length_mergeSort]
    exact List.length_filter_le Q (a.eraseDups)

end TinylambOpt.ResearchProofs.DuckDBGaps.Slices
