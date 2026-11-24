import TinylambOpt.ResearchGoals.CloudBI.PublicBI
import TinylambOpt.ResearchProofs.CloudBI.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.CloudBI.PublicBI

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.CloudBI.PublicBI
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-!
# Proof strategy overview

We prove the eleven PublicBI propositions from `PublicBI.lean`:

* `ConstantGroup` (Q1): after filtering rows to the single literal key, the
  erased group list is empty or the singleton group with totals over all hits.
* `PartialExtrema` (Q2): merging per-block extrema with `mergeExt` equals the
  extrema over the flattened hit list; empty blocks are skipped on both sides.
* `OccupiedAxis` (Q3): deduplicating codes before decoding yields the same
  sorted, deduplicated axis as decoding every selected code.
* `GroupBeforeSuffix` (Q4): folding the fine-grained histogram's bag weights
  with the suffix CASE equals counting matching rows directly, and the
  histogram size is bounded by the row count.
* `TripleBitmap` (Q5): the bitmap-style `(i,j,k)` membership test agrees with
  decoding each triple and comparing against the literal tuple.
* `AttributeWitness` (Q6): a constant projected attribute list is a `take`
  prefix of the per-column extrema.
* `LiftedDecimals` (Q7): scaling decoded extrema by a positive coefficient
  equals scaling after decoding, `none` staying `none`.
* `AxisRetraction` (Q8): a label present before and absent after a deletion
  must be counted among the deleted values; the conjunctions agree.
* `ClassificationCache` (Q9): the memoized fold outputs one label per code in
  order (cache hits reuse the stored label) and calls `outfield` exactly once
  per distinct code, since `(ps ++ [r]).eraseDups` drops duplicate snocs.
* `SliceBounds` (Q9'): summing the per-block marginal lower bounds
  `a + c − |ys|` and upper bounds `min a c` over strata brackets the flat
  joint count by inclusion–exclusion per block.
* `CodeExtrema` (Q10): collapsing the deduplicated occupied values to
  `(some x, some x)` when all equal agrees with the general `extrema` over
  the selected codes; all-NULL gives `(none, none)`.
-/

private def mmax (x y : Option Int) : Option Int := (([x,y]).filterMap id).max?
private def mmin (x y : Option Int) : Option Int := (([x,y]).filterMap id).min?

private theorem mmax_none (o : Option Int) : mmax none o = o := by
  cases o <;> rfl
private theorem mmax_right_none (o : Option Int) : mmax o none = o := by
  cases o <;> rfl
private theorem mmin_none (o : Option Int) : mmin none o = o := by
  cases o <;> rfl
private theorem mmin_right_none (o : Option Int) : mmin o none = o := by
  cases o <;> rfl

/-- `max?` after prepending one optional value folds the pair first. -/
private theorem stepMerge (o v : Option Int) (l : List (Option Int)) :
    ((mmax o v :: l).filterMap id).max? = ((o :: v :: l).filterMap id).max? := by
  cases o <;> cases v <;> rfl

private theorem foldmmax (vs : List (Option Int)) (o : Option Int) :
    vs.foldl mmax o = ((o :: vs).filterMap id).max? := by
  induction vs generalizing o with
  | nil => cases o <;> rfl
  | cons v vs ih =>
      rw [List.foldl_cons, ih]
      exact stepMerge o v vs

private theorem stepMergeM (o v : Option Int) (l : List (Option Int)) :
    ((mmin o v :: l).filterMap id).min? = ((o :: v :: l).filterMap id).min? := by
  cases o <;> cases v <;> rfl

private theorem foldmmin (vs : List (Option Int)) (o : Option Int) :
    vs.foldl mmin o = ((o :: vs).filterMap id).min? := by
  induction vs generalizing o with
  | nil => cases o <;> rfl
  | cons v vs ih =>
      rw [List.foldl_cons, ih]
      exact stepMergeM o v vs

private theorem fold_fst (f : α → Ext) (xs : List α) (z : Ext) :
    (xs.foldl (fun a x => mergeExt a (f x)) z).1 = (xs.map (fun x => (f x).1)).foldl mmax z.1 := by
  induction xs generalizing z with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.foldl_cons, List.map_cons]
      exact ih (mergeExt z (f x))

private theorem fold_snd (f : α → Ext) (xs : List α) (z : Ext) :
    (xs.foldl (fun a x => mergeExt a (f x)) z).2 = (xs.map (fun x => (f x).2)).foldl mmin z.2 := by
  induction xs generalizing z with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.foldl_cons, List.map_cons]
      exact ih (mergeExt z (f x))

/-- `max?` of an append merges the two optional extrema. -/
private theorem maxAppend (xs ys : List Int) :
    (xs ++ ys).max? = mmax xs.max? ys.max? := by
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

private theorem minAppend (xs ys : List Int) :
    (xs ++ ys).min? = mmin xs.min? ys.min? := by
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

/-- Prepending one optional value to a plain list before taking `max?`. -/
private theorem maxOptAppend (o : Option Int) (ys : List Int) :
    List.max? (Option.toList o ++ ys) = mmax o ys.max? := by
  cases o with
  | none => rw [mmax_none]; rfl
  | some v =>
      cases hy : ys.max? with
      | none =>
          have hys : ys = [] := List.max?_eq_none_iff.mp hy
          subst hys
          show List.max? [v] = mmax (some v) none
          rw [mmax_right_none]
          rfl
      | some y =>
          obtain ⟨hym, hyb⟩ := (List.max?_eq_some_iff (α := Int)).mp hy
          show List.max? (v :: ys) = mmax (some v) (some y)
          have h1 : mmax (some v) (some y) = some (max v y) := rfl
          rw [h1, show (max v y) = (if v ≤ y then y else v) from rfl]
          split
          · refine List.max?_eq_some_iff.mpr ⟨List.mem_cons.mpr (Or.inr hym), ?_⟩
            intro z hz
            rw [List.mem_cons] at hz
            rcases hz with rfl | hz
            · omega
            · have hbz := hyb z hz
              omega
          · refine List.max?_eq_some_iff.mpr ⟨List.mem_cons.mpr (Or.inl rfl), ?_⟩
            intro z hz
            rw [List.mem_cons] at hz
            rcases hz with rfl | hz
            · omega
            · have hbz := hyb z hz
              omega

private theorem minOptAppend (o : Option Int) (ys : List Int) :
    List.min? (Option.toList o ++ ys) = mmin o ys.min? := by
  cases o with
  | none => rw [mmin_none]; rfl
  | some v =>
      cases hy : ys.min? with
      | none =>
          have hys : ys = [] := List.min?_eq_none_iff.mp hy
          subst hys
          show List.min? [v] = mmin (some v) none
          rw [mmin_right_none]
          rfl
      | some y =>
          obtain ⟨hym, hyb⟩ := (List.min?_eq_some_iff (α := Int)).mp hy
          show List.min? (v :: ys) = mmin (some v) (some y)
          have h1 : mmin (some v) (some y) = some (min v y) := rfl
          rw [h1, show (min v y) = (if v ≤ y then v else y) from rfl]
          split
          · refine List.min?_eq_some_iff.mpr ⟨List.mem_cons.mpr (Or.inl rfl), ?_⟩
            intro z hz
            rw [List.mem_cons] at hz
            rcases hz with rfl | hz
            · omega
            · have hbz := hyb z hz
              omega
          · refine List.min?_eq_some_iff.mpr ⟨List.mem_cons.mpr (Or.inr hym), ?_⟩
            intro z hz
            rw [List.mem_cons] at hz
            rcases hz with rfl | hz
            · omega
            · have hbz := hyb z hz
              omega

private theorem consFilterMap (o : Option Int) (L : List (Option Int)) :
    ((o :: L).filterMap id) = o.toList ++ L.filterMap id := by
  cases o <;> rfl

private theorem maxCollapseF (h : α → List Int) (bs : List α) :
    ((bs.map (fun b => (h b).max?)).filterMap id).max? = (bs.map h).flatten.max? := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      have hred : (List.map (fun c => (h c).max?) (b :: bs)).filterMap id
          = (h b).max?.toList ++ (List.map (fun c => (h c).max?) bs).filterMap id := by
        rw [List.map_cons, consFilterMap]
      rw [hred, maxOptAppend, ih]
      exact (maxAppend (h b) (List.map h bs).flatten).symm

private theorem minCollapseF (h : α → List Int) (bs : List α) :
    ((bs.map (fun b => (h b).min?)).filterMap id).min? = (bs.map h).flatten.min? := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      have hred : (List.map (fun c => (h c).min?) (b :: bs)).filterMap id
          = (h b).min?.toList ++ (List.map (fun c => (h c).min?) bs).filterMap id := by
        rw [List.map_cons, consFilterMap]
      rw [hred, minOptAppend, ih]
      exact (minAppend (h b) (List.map h bs).flatten).symm

private theorem filter_flattenL {α : Type} (bs : List (List α)) (p : α → Bool) :
    bs.flatten.filter p = (bs.map (fun b => b.filter p)).flatten := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      rw [List.flatten_cons, List.filter_append, List.map_cons, ih]
      rfl

private theorem flatten_filter_nonempty {α : Type} (bs : List (List α)) :
    (bs.filter (fun b => !b.isEmpty)).flatten = bs.flatten := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      cases b with
      | nil => simp [List.flatten_cons, ih]
      | cons x xs =>
          have hb : (!(x :: xs).isEmpty) = true := by simp
          rw [List.filter_cons_of_pos (p := fun b : List α => !b.isEmpty) hb,
            List.flatten_cons, ih]
          rfl

private theorem filterMap_flatten {α β : Type} (g : α → Option β) (bs : List (List α)) :
    bs.flatten.filterMap g = (bs.map (fun b => b.filterMap g)).flatten := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      rw [List.flatten_cons, List.filterMap_append, List.map_cons, ih]
      rfl

private theorem filterMap_map_opt (xs : List (Option Int)) (g : Int → Int) :
    (xs.map (fun v => v.map g)).filterMap id = (xs.filterMap id).map g := by
  induction xs with
  | nil => rfl
  | cons x xs ih => cases x <;> simp [ih]

private theorem flatten_filterMap_map (bs : List (List Row)) (f : Row → Option Int) :
    (bs.map (fun b => (b.map f).filterMap id)).flatten = (bs.flatten.map f).filterMap id := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      rw [List.map_cons, List.flatten_cons, ih, List.flatten_cons,
        List.map_append, List.filterMap_append]

private theorem map_congr' {α β : Type} (f g : α → β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

/-- Folding `mergeExt` over blocks computes the extrema of the concatenation. -/
private theorem foldCells (bs : List (List Row)) (c : Nat) :
    bs.foldl (fun a b => mergeExt a (cell b c)) (none,none)
      = extrema (bs.flatten.map fun r => (r.values[c]?).join) := by
  refine Prod.ext ?_ ?_
  · have h3 := flatten_filterMap_map bs (fun r => (r.values[c]?).join)
    have key := maxCollapseF (fun b : List Row => (b.map fun r => (r.values[c]?).join).filterMap id) bs
    rw [h3] at key
    rw [fold_fst, foldmmax]
    exact key
  · have h3 := flatten_filterMap_map bs (fun r => (r.values[c]?).join)
    have key := minCollapseF (fun b : List Row => (b.map fun r => (r.values[c]?).join).filterMap id) bs
    rw [h3] at key
    rw [fold_snd, foldmmin]
    exact key

private def keptRows (blocks : List (List Row)) (k : Key) : List (List Row) :=
  (blocks.map (fun b => b.filter (fun r => r.key == k))).filter (fun b => !b.isEmpty)

private def hitsRows (blocks : List (List Row)) (k : Key) : List Row :=
  blocks.flatten.filter (fun r => r.key == k)

private theorem hitsRows_eq (blocks : List (List Row)) (k : Key) :
    hitsRows blocks k = (keptRows blocks k).flatten := by
  unfold hitsRows keptRows
  rw [filter_flattenL, flatten_filter_nonempty]

theorem sound_PartialExtrema : PartialExtrema.Goal := by
  intro blocks k c
  dsimp only
  have hflat : hitsRows blocks k = (keptRows blocks k).flatten := hitsRows_eq blocks k
  refine Prod.ext ?_ ?_
  · show (keptRows blocks k).isEmpty = (hitsRows blocks k).isEmpty
    by_cases hz : keptRows blocks k = []
    · rw [hz] at hflat
      rw [hz, hflat]
      rfl
    · obtain ⟨b, bs, hr⟩ := List.exists_cons_of_ne_nil hz
      have hmemR : b ∈ keptRows blocks k := by rw [hr]; exact List.mem_cons.mpr (Or.inl rfl)
      have hb : !b.isEmpty = true := by
        rw [keptRows] at hmemR
        exact (List.mem_filter.mp hmemR).2
      rw [hr] at hflat ⊢
      cases b with
      | nil =>
          simp at hb
      | cons x xs =>
          simp only [List.flatten_cons] at hflat
          rw [hflat]
          simp
  · show (keptRows blocks k).foldl (fun a b => mergeExt a (cell b c)) (none,none)
      = cell (hitsRows blocks k) c
    rw [foldCells]
    exact congrArg extrema (congrArg (List.map fun r => (r.values[c]?).join) hflat.symm)

private def scaled (b : Int × List (Option Int)) : Ext :=
  let (hi, lo) := extrema b.2
  (hi.map (fun v => v * b.1), lo.map (fun v => v * b.1))

theorem sound_LiftedDecimals : LiftedDecimals.Goal := by
  intro blocks hpos
  dsimp only
  show blocks.foldl (fun a x => mergeExt a (scaled x)) (none,none)
    = extrema (blocks.flatMap (fun (m,xs) => xs.map (fun v => v.map (fun x => x*m))))
  have hmax : (blocks.map (fun b => (scaled b).1))
      = blocks.map (fun p : Int × List (Option Int) =>
          ((p.2.filterMap id).map (fun v => v * p.1)).max?) := by
    apply map_congr'
    intro b hb
    exact (int_max?_map_mul b.1 (hpos b hb) (b.2.filterMap id)).symm
  have hmin : (blocks.map (fun b => (scaled b).2))
      = blocks.map (fun p : Int × List (Option Int) =>
          ((p.2.filterMap id).map (fun v => v * p.1)).min?) := by
    apply map_congr'
    intro b hb
    exact (int_min?_map_mul b.1 (hpos b hb) (b.2.filterMap id)).symm
  have hflat : (blocks.map fun p : Int × List (Option Int) =>
        (p.2.filterMap id).map (fun v => v * p.1)).flatten
      = (blocks.flatMap fun (m,xs) => xs.map fun v => v.map (fun x => x * m)).filterMap id := by
    show (blocks.map fun p : Int × List (Option Int) =>
        (p.2.filterMap id).map (fun v => v * p.1)).flatten
        = ((blocks.map fun p : Int × List (Option Int) =>
          p.2.map fun v => v.map (fun x => x * p.1)).flatten).filterMap id
    rw [filterMap_flatten]
    congr 1
    rw [List.map_map]
    apply map_congr'
    intro p hp
    exact (filterMap_map_opt p.2 (fun v => v * p.1)).symm
  refine Prod.ext ?_ ?_
  · have key := maxCollapseF (fun p : Int × List (Option Int) =>
      (p.2.filterMap id).map (fun v => v * p.1)) blocks
    rw [hflat] at key
    rw [fold_fst, foldmmax, hmax]
    exact key
  · have key := minCollapseF (fun p : Int × List (Option Int) =>
      (p.2.filterMap id).map (fun v => v * p.1)) blocks
    rw [hflat] at key
    rw [fold_snd, foldmmin, hmin]
    exact key

private theorem beq_decide {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α] (a b : α) :
    (a == b) = decide (a = b) := by
  by_cases h2 : a = b
  · rw [decide_eq_true h2]
    exact beq_iff_eq.mpr h2
  · rw [decide_eq_false h2]
    cases h : (a == b) with
    | true => exact absurd (beq_iff_eq.mp h) h2
    | false => exact rfl

/-- BEq is symmetric. -/
private theorem beq_sym {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α] (a b : α) :
    (a == b) = (b == a) := by
  by_cases h2 : a = b
  · subst h2
    rfl
  · rw [beq_decide, beq_decide, decide_eq_false h2, decide_eq_false (fun hh => h2 hh.symm)]

/-- Filtering by a predicate true on every element keeps the list. -/
private theorem filter_id (p : α → Bool) (l : List α) (h : ∀ x ∈ l, p x = true) :
    l.filter p = l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.filter_cons_of_pos (p := p) (a := x) (l := xs) (h x (by simp)),
        ih (fun y hy => h y (by simp [hy]))]

/-- Pointwise congruence for `filter`. -/
private theorem filter_congr' (p q : α → Bool) (l : List α) (h : ∀ x, p x = q x) :
    l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = q x := h x
      cases hq : q x with
      | true =>
          rw [List.filter_cons_of_pos (p := q) (a := x) (l := xs) hq,
            List.filter_cons_of_pos (p := p) (a := x) (l := xs) (by rw [hx]; exact hq),
            ih]
      | false =>
          rw [List.filter_cons_of_neg (p := q) (a := x) (l := xs) (by simpa using hq),
            List.filter_cons_of_neg (p := p) (a := x) (l := xs) (by rw [hx]; simpa using hq),
            ih]

/-- Element-wise `count` recursion, exposed for rewriting. -/
private theorem cntCons (a : Option String) (x : Option String) (l : List (Option String)) :
    List.count a (x :: l) = if a == x then List.count a l + 1 else List.count a l := by
  rw [List.count_cons, beq_sym]
  split <;> omega

private theorem cnt_eq_of_not_mem (a : Option String) :
    ∀ (u : List (Option String)) (l), a ∉ u → (u ++ l).count a = l.count a := by
  intro u
  induction u with
  | nil => intro l _; rfl
  | cons x u ih =>
      intro l hnm
      have hx : (a == x) = false := by
        cases h2 : a == x with
        | true =>
            have ha : a = x := beq_iff_eq.mp h2
            have hm : a ∈ (x :: u : List (Option String)) := by rw [ha]; simp
            exact absurd hm hnm
        | false => exact rfl
      rw [List.cons_append, cntCons, if_neg (by simp [hx]), ih l (fun h => hnm (by simp [h]))]
/-- Membership in a mapped list, in the shape the axis proofs need. -/
private theorem mem_map_iff' {α β : Type} (f : α → β) (l : List α) (b : β) :
    b ∈ l.map f ↔ ∃ a, a ∈ l ∧ f a = b := by
  induction l with
  | nil =>
      constructor
      · intro h; exact absurd h List.not_mem_nil
      · rintro ⟨a, ha, _⟩; exact absurd ha List.not_mem_nil
  | cons a as ih =>
      constructor
      · intro h
        rw [List.map_cons, List.mem_cons] at h
        rcases h with h | h
        · exact ⟨a, by simp, h.symm⟩
        · obtain ⟨x, hx, rfl⟩ := ih.mp h
          exact ⟨x, List.mem_cons_of_mem a hx, rfl⟩
      · rintro ⟨x, hx, rfl⟩
        rw [List.map_cons, List.mem_cons]
        rcases (List.mem_cons (a := x) (b := a) (l := as)).mp hx with rfl | h'
        · exact Or.inl rfl
        · exact Or.inr (ih.mpr ⟨x, h', rfl⟩)

/-- A nonempty constant list erases down to a singleton. -/
private theorem mapConstEraseDups {α β : Type} [BEq β] [LawfulBEq β] (a : β) :
    ∀ (l : List α), l ≠ [] → (l.map (fun _ => a)).eraseDups = [a] := by
  intro l
  induction l with
  | nil => intro h; exact absurd rfl h
  | cons x xs ih =>
      intro hne
      rw [List.map_cons, List.eraseDups_cons]
      have hfilter : ((xs.map fun _ => a).filter fun b => !b == a) = [] := by
        apply filter_eq_nil_of_all_false
        intro b hb
        obtain ⟨c, _, rfl2⟩ := (mem_map_iff' (fun _ => a) xs b).mp hb
        have hba : (b == a) = true := beq_iff_eq.mpr (by simpa using rfl2.symm)
        rw [hba]
        exact Bool.not_true
      rw [hfilter]
      rfl

/-- Axis membership: `axes` preserves exactly the reachable labels. -/
private theorem axes_mem_iff (keys : List (Option String)) (a : Option String) :
    a ∈ axes keys ↔ a ∈ keys := by
  unfold axes
  rw [List.mem_mergeSort, List.mem_eraseDups]

/-- A code test in a bitmap equals the decoded comparison. -/
private theorem contains_hits {α : Type} [BEq α] [LawfulBEq α] (d : Array (Option α)) (v : α) (i : Nat) :
    (hits d v).contains i = (d[i]?.join == some v) := by
  by_cases hb : (d[i]?.join == some v) = true
  · have hin : i ∈ hits d v := by
      unfold hits
      rw [List.mem_filter, List.mem_range]
      by_cases hlt : i < d.size
      · exact ⟨hlt, hb⟩
      · have hnone : d[i]? = none := (Array.getElem?_eq_none_iff).mpr (by omega : d.size ≤ i)
        rw [hnone] at hb
        exact absurd hb (by simp)
    rw [hb]
    exact List.contains_iff_mem.mpr hin
  · have hnc : (hits d v).contains i = false := by
      cases hc : (hits d v).contains i with
      | false => exact rfl
      | true =>
          exfalso
          have hin : i ∈ hits d v := List.contains_iff_mem.mp hc
          unfold hits at hin
          rw [List.mem_filter] at hin
          obtain ⟨hr, hq⟩ := hin
          rw [List.mem_range] at hr
          exact absurd (by simpa using hq) hb
    rw [hnc]
    cases hq : (d[i]?.join == some v) with
    | true => exact absurd hq hb
    | false => exact rfl

theorem sound_ConstantGroup : ConstantGroup.Goal := by
  intro rows l p y cols
  dsimp only
  by_cases hsel : rows.filter (fun r => r.key == (some l, some p, some y)) = []
  · rw [hsel]
    rfl
  · obtain ⟨x, xs, hx⟩ := List.exists_cons_of_ne_nil hsel
    rw [hx]
    have hkey : ∀ r ∈ (x :: xs : List Row), r.key = (some l, some p, some y) := by
      intro r hr
      have hr' : r ∈ rows.filter (fun r => r.key == (some l, some p, some y)) := by rw [hx]; exact hr
      exact beq_iff_eq.mp
        ((List.mem_filter (p := fun r : Row => r.key == (some l, some p, some y))).mp hr').2
    have hmap : (x :: xs : List Row).map Row.key
        = (x :: xs : List Row).map (fun _ => (some l, some p, some y)) := by
      apply map_congr'
      intro r hr
      exact hkey r hr
    rw [hmap, mapConstEraseDups (some l, some p, some y) (x :: xs) (by simp)]
    show [( (some l, some p, some y),
        attrs ((x :: xs).filter fun r => r.key == (some l, some p, some y)) cols)]
      = (if List.isEmpty (x :: xs) then []
        else [( (some l, some p, some y), attrs (x :: xs) cols)])
    rw [filter_id (fun r => r.key == (some l, some p, some y)) (x :: xs)
      (fun r hr => beq_iff_eq.mpr (hkey r hr))]
    rfl

theorem sound_AttributeWitness : AttributeWitness.Goal := by
  intro xs
  change display ((xs.filterMap id).max?, (xs.filterMap id).min?)
    = match (xs.filterMap id).eraseDups with
      | [] => Attr.absent | [x] => Attr.single x | _ => Attr.multiple
  cases h : (xs.filterMap id).eraseDups with
  | nil =>
      have hnil : xs.filterMap id = [] := by
        cases hq : xs.filterMap id with
        | nil => rfl
        | cons y ys =>
            have hy : y ∈ xs.filterMap id := by rw [hq]; simp
            have hy' : y ∈ (xs.filterMap id).eraseDups := List.mem_eraseDups.mpr hy
            rw [h] at hy'
            exact absurd hy' (by simp)
      rw [hnil]
      rfl
  | cons x t =>
      cases t with
      | nil =>
          have hxm : x ∈ (xs.filterMap id).eraseDups := by rw [h]; simp
          have hmax : (xs.filterMap id).max? = some x := by
            refine (List.max?_eq_some_iff (α := Int)).mpr ⟨List.mem_eraseDups.mp hxm, ?_⟩
            intro y hy
            have hy' : y ∈ (xs.filterMap id).eraseDups := List.mem_eraseDups.mpr hy
            rw [h] at hy'
            obtain rfl : y = x := List.mem_singleton.mp hy'
            omega
          have hmin : (xs.filterMap id).min? = some x := by
            refine (List.min?_eq_some_iff (α := Int)).mpr ⟨List.mem_eraseDups.mp hxm, ?_⟩
            intro y hy
            have hy' : y ∈ (xs.filterMap id).eraseDups := List.mem_eraseDups.mpr hy
            rw [h] at hy'
            obtain rfl : y = x := List.mem_singleton.mp hy'
            omega
          rw [hmax, hmin]
          show (if x == x then Attr.single x else Attr.multiple) = Attr.single x
          rw [if_pos (beq_iff_eq.mpr rfl)]
      | cons b rest =>
          have hrhs : (match (x :: b :: rest) with
            | [] => Attr.absent | [x] => Attr.single x | _ => Attr.multiple) = Attr.multiple := by
            cases rest <;> rfl
          have hnodup : List.Nodup (x :: b :: rest) := by
            have h2 := nodup_eraseDups (xs.filterMap id)
            rw [h] at h2
            exact h2
          have hne : ¬ (x == b) := by
            intro hxb
            rw [beq_iff_eq] at hxb
            have hnin : ¬ (x ∈ (b :: rest : List Int)) := (List.nodup_cons.mp hnodup).1
            exact hnin (by rw [hxb]; simp)
          have hxin : x ∈ xs.filterMap id := List.mem_eraseDups.mp (by rw [h]; simp)
          have hbin : b ∈ xs.filterMap id := List.mem_eraseDups.mp (by rw [h]; simp)
          cases hmx : (xs.filterMap id).max? with
          | none =>
              rw [List.max?_eq_none_iff] at hmx
              rw [hmx] at hxin
              exact absurd hxin (by simp)
          | some hi =>
              cases hmn : (xs.filterMap id).min? with
              | none =>
                  rw [List.min?_eq_none_iff] at hmn
                  rw [hmn] at hxin
                  exact absurd hxin (by simp)
              | some lo =>
                  have hbe : (hi == lo) = false := by
                    cases h_eq : hi == lo with
                    | true =>
                        have h1 : x ≤ hi := (List.max?_eq_some_iff.mp hmx).2 x hxin
                        have h2 : b ≤ hi := (List.max?_eq_some_iff.mp hmx).2 b hbin
                        have h3 : lo ≤ x := (List.min?_eq_some_iff.mp hmn).2 x hxin
                        have h4 : lo ≤ b := (List.min?_eq_some_iff.mp hmn).2 b hbin
                        rw [← beq_iff_eq.mp h_eq] at h3 h4
                        have hb : x = b := by omega
                        exact absurd (beq_iff_eq.mpr hb) hne
                    | false => exact rfl
                  show (if hi == lo then Attr.single hi else Attr.multiple) = Attr.multiple
                  refine if_neg ?_
                  rw [hbe]
                  exact Bool.false_ne_true

theorem sound_OccupiedAxis : OccupiedAxis.Goal := by
  intro blocks
  apply optStr_mergeSort_eraseDups_eq
  intro a
  constructor
  · intro h
    rw [List.mem_flatMap] at h
    obtain ⟨b, hb₀, hb⟩ := h
    rw [List.mem_flatMap]
    refine ⟨b, hb₀, ?_⟩
    rw [labels]
    obtain ⟨i, hi, heq⟩ := (mem_map_iff' (fun i => b.dict[i]?.join) b.codes.eraseDups a).mp hb
    exact (mem_map_iff' (fun i => b.dict[i]?.join) b.codes a).mpr
      ⟨i, List.mem_eraseDups.mp hi, heq⟩
  · intro h
    rw [List.mem_flatMap] at h
    obtain ⟨b, hb₀, hb⟩ := h
    rw [List.mem_flatMap]
    refine ⟨b, hb₀, ?_⟩
    rw [labels] at hb
    obtain ⟨i, hi, heq⟩ := (mem_map_iff' (fun i => b.dict[i]?.join) b.codes a).mp hb
    exact (mem_map_iff' (fun i => b.dict[i]?.join) b.codes.eraseDups a).mpr
      ⟨i, List.mem_eraseDups.mpr hi, heq⟩

theorem sound_AxisRetraction : AxisRetraction.Goal := by
  intro keep drop label
  dsimp only
  by_cases hC : (axes ((keep ++ drop).map Prod.snd)).contains label = true
  · rw [hC]
    simp only [Bool.true_and]
    by_cases hin : label ∈ axes (keep.map Prod.snd)
    · have hmem : label ∈ keep.map Prod.snd := (axes_mem_iff (keep.map Prod.snd) label).mp hin
      obtain ⟨u, v, huv⟩ := exists_decomp_of_mem (keep.map Prod.snd) label hmem
      have hc : (axes (keep.map Prod.snd)).contains label = true :=
        List.contains_iff_mem.mpr hin
      rw [hc, Bool.not_true, beq_decide]
      have hne : ¬ (((keep ++ drop).map Prod.snd).count label
          = (drop.map Prod.snd).count label) := by
        rw [List.map_append, huv, List.append_assoc, List.count_append, List.cons_append, cntCons,
          if_pos (beq_iff_eq.mpr rfl), List.count_append]
        intro hh
        omega
      rw [decide_eq_false hne]
    · have hc : (axes (keep.map Prod.snd)).contains label = false := by
        cases hq : (axes (keep.map Prod.snd)).contains label with
        | true => exact absurd (List.contains_iff_mem.mp hq) hin
        | false => exact rfl
      have hnm : label ∉ keep.map Prod.snd :=
        fun h => hin ((axes_mem_iff (keep.map Prod.snd) label).mpr h)
      rw [hc, Bool.not_false, beq_decide]
      have heq : ((keep ++ drop).map Prod.snd).count label
          = (drop.map Prod.snd).count label := by
        rw [List.map_append,
          cnt_eq_of_not_mem label (keep.map Prod.snd) (drop.map Prod.snd) hnm]
      rw [decide_eq_true heq]
  · have hc : (axes ((keep ++ drop).map Prod.snd)).contains label = false := by
      cases hq : (axes ((keep ++ drop).map Prod.snd)).contains label with
      | true => exact absurd hq hC
      | false => exact rfl
    rw [hc]
    simp only [Bool.false_and]

theorem sound_TripleBitmap : TripleBitmap.Goal := by
  intro ls ps ys codes league pos yr
  dsimp only
  refine filter_congr' (fun (i, j, k) =>
    (hits ls league).contains i && (hits ps pos).contains j && (hits ys yr).contains k)
    (fun t => decode ls ps ys t == (some league, some pos, some yr)) codes ?_
  intro ⟨i, j, k⟩
  dsimp only
  rw [contains_hits ls league i, contains_hits ps pos j, contains_hits ys yr k]
  rw [Bool.and_assoc]
  exact rfl

/-- Pointwise predicate transfer for `countP`. -/
private theorem countP_congr {α : Type} (p q : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = q x) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.countP_cons, List.countP_cons, h x (by simp), ih (fun y hy => h y (by simp [hy]))]

/-- `countP` over an append splits into a sum. -/
private theorem countP_append {α : Type} (p : α → Bool) (as bs : List α) :
    (as ++ bs).countP p = as.countP p + bs.countP p := by
  induction as with
  | nil => rw [List.nil_append, List.countP_nil, Nat.zero_add]
  | cons a as ih =>
      rw [List.cons_append, List.countP_cons, List.countP_cons, ih]
      omega

/-- Filtering can be internalized into the predicate. -/
private theorem countP_filter {α : Type} (p q : α → Bool) (l : List α) :
    (l.filter p).countP q = l.countP (fun x => p x && q x) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hp : p x with
      | true =>
          rw [List.filter_cons_of_pos (p := p) (a := x) (l := xs) hp, List.countP_cons,
            List.countP_cons, ih, hp]
          simp
      | false =>
          rw [List.filter_cons_of_neg (p := p) (a := x) (l := xs) (by simp [hp]), List.countP_cons,
            ih, hp]
          simp

/-- Monotonicity: hits of a conjunction are bounded by hits of each side. -/
private theorem countP_and_le_left {α : Type} (p q : α → Bool) (l : List α) :
    l.countP (fun x => p x && q x) ≤ l.countP p := by
  induction l with
  | nil => simp [List.countP_nil]
  | cons x xs ih =>
      rw [List.countP_cons, List.countP_cons]
      cases hp : p x <;> cases hq : q x <;> simp [hp, hq] <;> omega

private theorem countP_and_le_right {α : Type} (p q : α → Bool) (l : List α) :
    l.countP (fun x => p x && q x) ≤ l.countP q := by
  induction l with
  | nil => simp [List.countP_nil]
  | cons x xs ih =>
      rw [List.countP_cons, List.countP_cons]
      cases hp : p x <;> cases hq : q x <;> simp [hp, hq] <;> omega

/-- Inclusion-exclusion: conjunction hits plus the complement budget. -/
private theorem countP_and_ge {α : Type} (p q : α → Bool) (l : List α) :
    l.countP (fun x => p x && q x) + l.length ≥ l.countP p + l.countP q := by
  induction l with
  | nil => simp [List.countP_nil]
  | cons x xs ih =>
      rw [List.countP_cons, List.countP_cons, List.countP_cons, List.length_cons]
      cases hp : p x <;> cases hq : q x <;> simp [hp, hq] <;> omega

private theorem sum_le_map {α : Type} (f g : α → Nat) (bs : List α)
    (h : ∀ b ∈ bs, f b ≤ g b) : (bs.map f).sum ≤ (bs.map g).sum := by
  induction bs with
  | nil => exact Nat.le_refl _
  | cons b bs ih =>
      rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h b (by simp)) (ih (fun c hc => h c (by simp [hc])))

private theorem countP_filter_flatten {α : Type} (p q : α → Bool) (bs : List (List α)) :
    (bs.map (fun b => (b.filter p).countP q)).sum = (bs.flatten.filter p).countP q := by
  induction bs with
  | nil => rfl
  | cons b bs ih =>
      rw [List.map_cons, List.sum_cons, List.flatten_cons, ih, List.filter_append, countP_append]

/-- A row's key test against the triple literal is exactly the pointwise conjunction. -/
private theorem key_triple (l p : String) (y : Int) (r : Row) :
    ((r.key.1 == some l) && ((r.key.2.1 == some p) && (r.key.2.2 == some y)))
      = (r.key == (some l, some p, some y)) := rfl

theorem sound_SliceBounds : SliceBounds.Goal := by
  intro blocks l p y marg n
  -- Canonical per-block conjunction form of the key test.
  have keytest : ∀ (X : List Row),
      X.countP (fun r => (r.key.1 == some l) && ((r.key.2.1 == some p) && (r.key.2.2 == some y)))
        = X.countP (fun r => r.key == (some l, some p, some y)) := by
    intro X
    refine countP_congr _ _ _ (fun r _ => (key_triple l p y r).symm)
  have hfst : ∀ (b : List Row), (marg b).1
      = (b.filter (fun r => r.key.2.2 == some y)).countP (fun r => r.key.1 == some l)
        + (b.filter (fun r => r.key.2.2 == some y)).countP (fun r => r.key.2.1 == some p)
          - (b.filter (fun r => r.key.2.2 == some y)).length := fun _ => rfl
  have hsnd : ∀ (b : List Row), (marg b).2
      = min ((b.filter (fun r => r.key.2.2 == some y)).countP (fun r => r.key.1 == some l))
        ((b.filter (fun r => r.key.2.2 == some y)).countP (fun r => r.key.2.1 == some p)) := fun _ => rfl
  -- Year predicate can be conjuncted into the key test without changing hits.
  have hz : ∀ (r : Row), r ∈ blocks.flatten →
      ((r.key.2.2 == some y) && ((r.key.1 == some l)
        && ((r.key.2.1 == some p) && (r.key.2.2 == some y))))
        = ((r.key.1 == some l) && ((r.key.2.1 == some p) && (r.key.2.2 == some y))) := by
    intro r _
    cases h : (r.key.2.2 == some y) with
    | true => simp
    | false => simp
  have conjchain : (blocks.map (fun b =>
      (b.filter (fun r => r.key.2.2 == some y)).countP (fun r =>
        (r.key.1 == some l) && ((r.key.2.1 == some p) && (r.key.2.2 == some y))))).sum = n := by
    rw [countP_filter_flatten, countP_filter,
      countP_congr (fun r => (r.key.2.2 == some y) && ((r.key.1 == some l)
        && ((r.key.2.1 == some p) && (r.key.2.2 == some y))))
        (fun r => (r.key.1 == some l) && ((r.key.2.1 == some p) && (r.key.2.2 == some y)))
        _ (fun r hr => hz r hr),
      keytest]
  refine ⟨?_, ?_⟩
  · rw [← conjchain, List.map_map]
    refine sum_le_map _ _ blocks (fun b _ => ?_)
    show (marg b).1 ≤ _
    rw [hfst]
    have hz' : ∀ r ∈ b.filter (fun r => r.key.2.2 == some y),
        ((r.key.1 == some l) && ((r.key.2.1 == some p) && (r.key.2.2 == some y)))
          = ((r.key.1 == some l) && (r.key.2.1 == some p)) := by
      intro r hr
      rw [List.mem_filter] at hr
      rw [hr.2]
      simp
    rw [countP_congr _ _ _ hz']
    have hh := countP_and_ge (fun (r : Row) => r.key.1 == some l)
      (fun (r : Row) => r.key.2.1 == some p) (b.filter (fun r => r.key.2.2 == some y))
    omega
  · rw [← conjchain,
      show ((blocks.map marg).map Prod.snd).sum = (blocks.map (fun b => (marg b).2)).sum from by
        rw [List.map_map]; rfl]
    refine sum_le_map _ _ blocks (fun b _ => ?_)
    have ha := countP_and_le_left (fun (r : Row) => r.key.1 == some l)
      (fun (r : Row) => (r.key.2.1 == some p) && (r.key.2.2 == some y))
      (b.filter (fun r => r.key.2.2 == some y))
    have hb := Nat.le_trans (countP_and_le_right (fun (r : Row) => r.key.1 == some l)
      (fun (r : Row) => (r.key.2.1 == some p) && (r.key.2.2 == some y))
      (b.filter (fun r => r.key.2.2 == some y)))
      (countP_and_le_left (fun (r : Row) => r.key.2.1 == some p)
        (fun (r : Row) => r.key.2.2 == some y) (b.filter (fun r => r.key.2.2 == some y)))
    show _ ≤ (marg b).2
    rw [hsnd]
    exact Nat.le_min.mpr ⟨ha, hb⟩

/-- Mapping then keeping the payloads equals `filterMap` with the decoder. -/
private theorem filterMap_map_id (g : Nat → Option Int) (l : List Nat) :
    (l.map g).filterMap id = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.map_cons, List.filterMap_cons, List.filterMap_cons, ih]
      cases g a <;> rfl

private def cT (dict : Array (Option Int)) (selected : List Nat) : List Int :=
  (selected.map fun i => dict[i]?.join).filterMap id

theorem sound_CodeExtrema : CodeExtrema.Goal := by
  intro dict selected
  have hT : cT dict selected = selected.filterMap fun i => dict[i]?.join :=
    filterMap_map_id (fun i => dict[i]?.join) selected
  have hE : extrema (selected.map fun i => dict[i]?.join) =
      ((cT dict selected).max?, (cT dict selected).min?) := rfl
  -- The deduplicated occupied axis and the decoded measure list carry the same values.
  have hbridge0 : ∀ v : Int,
      v ∈ selected.eraseDups.filterMap (fun i => dict[i]?.join) ↔ v ∈ cT dict selected := by
    intro v
    rw [hT, List.mem_filterMap, List.mem_filterMap]
    exact ⟨fun ⟨i, hi, e⟩ => ⟨i, List.mem_eraseDups.mp hi, e⟩,
      fun ⟨i, hi, e⟩ => ⟨i, List.mem_eraseDups.mpr hi, e⟩⟩
  cases hocc : selected.eraseDups.filterMap (fun i => dict[i]?.join) with
  | nil =>
      rw [hocc] at hbridge0
      show (none, none) = extrema (selected.map fun i => dict[i]?.join)
      have hTnil : cT dict selected = [] := by
        cases h : cT dict selected with
        | nil => rfl
        | cons a rest =>
            have hc : a ∈ cT dict selected := by rw [h]; simp
            have ha : a ∈ ([] : List Int) := (hbridge0 a).mpr hc
            exact absurd ha (by simp)
      rw [hE, hTnil]
      exact rfl
  | cons x rest =>
      rw [hocc] at hbridge0
      show (if rest.all (fun y => y == x) then (some x, some x)
          else ((x :: rest).max?, (x :: rest).min?)) =
        extrema (selected.map fun i => dict[i]?.join)
      cases h' : rest.all (fun y => y == x) with
      | true =>
          have h2 : rest.all (fun y => y == x) = decide (∀ y ∈ rest, y == x) := List.all_eq
          have hall' : ∀ y ∈ rest, (y == x) = true := of_decide_eq_true (h2 ▸ h')
          have hxT : x ∈ cT dict selected := (hbridge0 x).mp (by simp)
          have hmax : (cT dict selected).max? = some x :=
            (List.max?_eq_some_iff (α := Int)).mpr
              ⟨hxT, fun b hb => by
                have hbo : b ∈ x :: rest := (hbridge0 b).mpr hb
                rcases List.mem_cons.mp hbo with he | hm
                · omega
                · have : b = x := (beq_iff_eq (α := Int)).mp (hall' b hm)
                  omega⟩
          have hmin : (cT dict selected).min? = some x :=
            (List.min?_eq_some_iff (α := Int)).mpr
              ⟨hxT, fun b hb => by
                have hbo : b ∈ x :: rest := (hbridge0 b).mpr hb
                rcases List.mem_cons.mp hbo with he | hm
                · omega
                · have : b = x := (beq_iff_eq (α := Int)).mp (hall' b hm)
                  omega⟩
          show (some x, some x) = extrema (selected.map fun i => dict[i]?.join)
          rw [hE, hmax, hmin]
      | false =>
          have hxT : x ∈ cT dict selected := (hbridge0 x).mp (by simp)
          have hmax : (x :: rest).max? = (cT dict selected).max? := by
            cases h : (cT dict selected).max? with
            | none =>
                rw [List.max?_eq_none_iff] at h
                rw [h] at hxT
                exact absurd hxT (by simp)
            | some m =>
                have hm := (List.max?_eq_some_iff (α := Int)).mp h
                exact (List.max?_eq_some_iff (α := Int)).mpr
                  ⟨(hbridge0 m).mpr hm.1, fun b hbo => hm.2 b ((hbridge0 b).mp hbo)⟩
          have hmin : (x :: rest).min? = (cT dict selected).min? := by
            cases h : (cT dict selected).min? with
            | none =>
                rw [List.min?_eq_none_iff] at h
                rw [h] at hxT
                exact absurd hxT (by simp)
            | some m =>
                have hm := (List.min?_eq_some_iff (α := Int)).mp h
                exact (List.min?_eq_some_iff (α := Int)).mpr
                  ⟨(hbridge0 m).mpr hm.1, fun b hbo => hm.2 b ((hbridge0 b).mp hbo)⟩
          show ((x :: rest).max?, (x :: rest).min?) = extrema (selected.map fun i => dict[i]?.join)
          rw [hE, hmax, hmin]

private def histFrom (init : Std.HashMap Dim Nat) (rows : List Dim) : Std.HashMap Dim Nat :=
  rows.foldl (fun (m : Std.HashMap Dim Nat) k => m.insert k (m.getD k 0 + 1)) init

private theorem histFrom_nil (init : Std.HashMap Dim Nat) : histFrom init [] = init := rfl

private theorem histFrom_cons (init : Std.HashMap Dim Nat) (a : Dim) (as : List Dim) :
    histFrom init (a :: as) = histFrom (init.insert a (init.getD a 0 + 1)) as := rfl

/-- `getD` counts occurrences: initial count plus bag multiplicity. -/
private theorem hist_getD : ∀ (init : Std.HashMap Dim Nat) (rows : List Dim) (j : Dim),
    (histFrom init rows).getD j 0 = init.getD j 0 + rows.countP fun r => r == j := by
  intro init rows j
  induction rows generalizing init with
  | nil =>
      rw [histFrom_nil, List.countP_nil, Nat.add_zero]
  | cons a as ih =>
      rw [histFrom_cons, ih, Std.HashMap.getD_insert, List.countP_cons]
      cases h : (a == j) with
      | true =>
          obtain rfl : a = j := beq_iff_eq.mp h
          simp
          omega
      | false =>
          simp

/-- A positive stored count implies the key is present. -/
private theorem hist_getElem?_some : ∀ (init : Std.HashMap Dim Nat) (rows : List Dim) (j : Dim),
    ((init.getD j 0 = 0) ↔ init[j]? = none) →
    0 < (histFrom init rows).getD j 0 → ∃ v, (histFrom init rows)[j]? = some v := by
  intro init rows j
  induction rows generalizing init with
  | nil =>
      intro H hpos
      rw [histFrom_nil] at hpos ⊢
      cases h : init[j]? with
      | some v => exact ⟨v, rfl⟩
      | none =>
          rw [H.mpr h] at hpos
          omega
  | cons a as ih =>
      intro H hpos
      rw [histFrom_cons] at hpos ⊢
      have H' : ((init.insert a (init.getD a 0 + 1)).getD j 0 = 0) ↔
          (init.insert a (init.getD a 0 + 1))[j]? = none := by
        rw [Std.HashMap.getD_insert, Std.HashMap.getElem?_insert]
        cases h : (a == j) with
        | true =>
            simp
        | false =>
            exact H
      exact ih (init.insert a (init.getD a 0 + 1)) H' hpos

/-- Every histogram entry stores exactly the count of its key. -/
private theorem hist_toList_weights (rows : List Dim) :
    ∀ e ∈ (histFrom {} rows).toList, e.2 = rows.countP fun r => r == e.1 := by
  intro e he
  have he' : (e.1, e.2) ∈ (histFrom {} rows).toList := he
  have hget : (histFrom {} rows)[e.1]? = some e.2 :=
    (Std.HashMap.mem_toList_iff_getElem?_eq_some (m := histFrom {} rows)).mp he'
  have hc : (histFrom {} rows).contains e.1 = true := by
    rw [Std.HashMap.contains_eq_isSome_getElem?, hget]
    exact rfl
  have h2 := Std.HashMap.getElem?_eq_some_getD_of_contains (m := histFrom {} rows) (a := e.1)
    (fallback := 0) hc
  rw [hget] at h2
  have h3 : e.2 = (histFrom {} rows).getD e.1 0 := Option.some.inj h2
  rw [h3, hist_getD]
  have h0 : ({} : Std.HashMap Dim Nat) = (∅ : Std.HashMap Dim Nat) := rfl
  rw [h0, Std.HashMap.getD_empty, Nat.zero_add]

/-- The histogram never has more keys than input rows. -/
private theorem hist_size_le (rows : List Dim) : (histFrom {} rows).size ≤ rows.length := by
  have key : ∀ (init : Std.HashMap Dim Nat) (rows : List Dim),
      (histFrom init rows).size ≤ init.size + rows.length := by
    intro init rows
    induction rows generalizing init with
    | nil => simp [histFrom_nil]
    | cons a as ih =>
        rw [histFrom_cons]
        have h1 := Std.HashMap.size_insert_le (m := init) (k := a) (v := init.getD a 0 + 1)
        have h2 := ih (init.insert a (init.getD a 0 + 1))
        have h3 : (a :: as).length = as.length + 1 := List.length_cons
        omega
  have h0 : ({} : Std.HashMap Dim Nat) = (∅ : Std.HashMap Dim Nat) := rfl
  have hb := key (∅ : Std.HashMap Dim Nat) rows
  rw [Std.HashMap.size_empty, Nat.zero_add] at hb
  rw [← h0] at hb
  exact hb

private theorem countP_eq_zero_of_not_mem {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x, x ∈ l → False) : l.countP p = 0 := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, ih (fun x hx => h x (by simp [hx]))]
      exact False.elim (h a (List.Mem.head _))

/-- An element that occurs is counted at least once. -/
private theorem countP_pos_of_mem (rows : List Dim) (j : Dim) (hj : j ∈ rows) :
    0 < rows.countP fun r => r == j := by
  induction rows with
  | nil => exact absurd hj (by simp)
  | cons a as ih =>
      rw [List.countP_cons]
      rw [List.mem_cons] at hj
      cases hj with
      | inl heq =>
          obtain rfl := heq
          simp
      | inr hmem =>
          have hi := ih hmem
          omega

/-- Split counting into two complementary filters. -/
private theorem countP_split {α : Type} (p q1 q2 : α → Bool) (l : List α)
    (h : ∀ x, q1 x = ! (q2 x)) :
    l.countP p = (l.filter q1).countP p + (l.filter q2).countP p := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, ih]
      cases hq : q1 a with
      | true =>
          have h2 : q2 a = false := by
            rw [h a] at hq
            simp at hq
            exact hq
          rw [List.filter_cons_of_pos (p := q1) (a := a) (l := as) hq,
            List.filter_cons_of_neg (p := q2) (a := a) (l := as) (by simpa using h2),
            List.countP_cons]
          omega
      | false =>
          have h2 : q2 a = true := by
            rw [h a] at hq
            simp at hq
            exact hq
          rw [List.filter_cons_of_neg (p := q1) (a := a) (l := as) (by simpa using hq),
            List.filter_cons_of_pos (p := q2) (a := a) (l := as) h2,
            List.countP_cons]
          omega

/-- Rows equal to `k` contribute their multiplicity times the flag of `k`. -/
private theorem countP_filter_eq {α : Type} [BEq α] [LawfulBEq α] (p : α → Bool) (k : α)
    (l : List α) :
    (l.filter fun r => r == k).countP p = l.countP (fun r => r == k) * (if p k = true then 1 else 0) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.countP_cons]
      cases hk : (a == k) with
      | true =>
          rw [List.filter_cons_of_pos (p := fun r => r == k) (a := a) (l := as) hk,
            List.countP_cons, ih]
          obtain rfl : a = k := beq_iff_eq.mp hk
          simp
          by_cases hJ : p a = true
          · rw [if_pos hJ, Nat.mul_one, Nat.mul_one]
          · rw [if_neg hJ, Nat.mul_zero, Nat.mul_zero]
      | false =>
          rw [List.filter_cons_of_neg (p := fun r => r == k) (a := a) (l := as) (by simpa using hk),
            ih]
          simp [hk]

/-- Removing all copies of `a` does not change counts of a different key. -/
private theorem countP_filter_ne {α : Type} [BEq α] [LawfulBEq α] (a b : α) (l : List α)
    (h : ¬ a = b) :
    (l.filter fun x => ! (x == a)).countP (fun x => x == b) = l.countP (fun x => x == b) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hx : (x == a) with
      | true =>
          have hb : (x == b) = false := by
            cases h3 : (x == b) with
            | true =>
                exact absurd (beq_iff_eq.mp h3) (fun hh => h (by rw [← beq_iff_eq.mp hx]; exact hh))
            | false => rfl
          rw [List.filter_cons_of_neg (p := fun t => ! (t == a)) (a := x) (l := xs) (by simp [hx]),
            List.countP_cons, ih]
          simp [hb]
      | false =>
          rw [List.filter_cons_of_pos (p := fun t => ! (t == a)) (a := x) (l := xs) (by simp [hx]),
            List.countP_cons, List.countP_cons, ih]

/-- Folding the grouped histogram sums exactly the matching bag multiplicities. -/
private theorem fold_hist (es : List (Dim × Nat)) (league : Option String) (flag : Bool) :
    ∀ (rows : List Dim) (P : Dim → Bool) (acc : Nat),
      (∀ e ∈ es, e.2 = rows.countP fun r => r == e.1) →
      List.Pairwise (fun x y => (x.1 == y.1) = false) es →
      (∀ j, j ∈ rows → ∃ e ∈ es, e.1 == j) →
      es.foldl (fun a e => if P e.1 then a + e.2 else a) acc =
        acc + rows.countP P := by
  induction es with
  | nil =>
      intro rows P acc hw hpd hcov
      have hz : rows.countP P = 0 :=
        countP_eq_zero_of_not_mem P rows fun j hj => by
          obtain ⟨e, he, _⟩ := hcov j hj
          exact absurd he (by simp)
      rw [List.foldl_nil, hz, Nat.add_zero]
  | cons ee rest ih =>
      intro rows P acc hw hpd hcov
      obtain h1 := by
        cases hpd with
        | cons h h' => exact h
      obtain h2 := by
        cases hpd with
        | cons h h' => exact h'
      rw [List.foldl_cons]
      have hwk : ee.2 = rows.countP fun r => r == ee.1 := hw ee (List.Mem.head rest)
      have hw' : ∀ e ∈ rest,
          e.2 = (rows.filter fun r => ! (r == ee.1)).countP fun r => r == e.1 := by
        intro e he
        have hne : ¬ ee.1 = e.1 := by
          intro hh
          have htf : (ee.1 == e.1) = false := h1 e he
          rw [beq_iff_eq.mpr hh] at htf
          exact absurd htf (by simp)
        rw [hw e (List.Mem.tail ee he), countP_filter_ne ee.1 e.1 rows hne]
      have hcov' : ∀ j ∈ rows.filter fun r => ! (r == ee.1), ∃ e ∈ rest, e.1 == j := by
        intro j hj
        have hjm : j ∈ rows := (List.mem_filter.mp hj).1
        obtain ⟨e, he, heq⟩ := hcov j hjm
        cases he with
        | head _ =>
            have hjp : ! (j == ee.1) = true := (List.mem_filter.mp hj).2
            have hc : (ee.1 == j) = false := by simpa [beq_sym] using hjp
            exact absurd heq (by simp [hc])
        | tail _ hrest => exact ⟨e, hrest, heq⟩
      have hsplit := countP_split P (fun r => ! (r == ee.1)) (fun r => r == ee.1) rows
        (fun x => rfl)
      rw [countP_filter_eq P ee.1 rows] at hsplit
      rw [ih (rows.filter fun r => ! (r == ee.1)) P (if P ee.1 then acc + ee.2 else acc) hw' h2 hcov']
      rw [hsplit, hwk]
      cases h : P ee.1 with
      | true =>
          simp at *
          omega
      | false =>
          simp

theorem sound_GroupBeforeSuffix : GroupBeforeSuffix.Goal := by
  intro rows league flag fine result
  show fine.toList.foldl
      (fun a e =>
        if (fun (l, p) => l == league && outfield p == flag) e.1 = true then a + e.2 else a) 0 =
      rows.countP (fun (l, p) => l == league && outfield p == flag) ∧ fine.size ≤ rows.length
  have hfe : fine = histFrom {} rows := rfl
  have hw : ∀ e ∈ fine.toList, e.2 = rows.countP fun r => r == e.1 := by
    intro e he
    exact hist_toList_weights rows e (by rw [← hfe]; exact he)
  constructor
  · have hpd : List.Pairwise (fun x y => (x.1 == y.1) = false) fine.toList := by
      rw [hfe]
      exact Std.HashMap.distinct_keys_toList (m := histFrom {} rows)
    have hcov : ∀ j, j ∈ rows → ∃ e ∈ fine.toList, e.1 == j := by
      intro j hj
      have hpos : 0 < rows.countP fun r => r == j := countP_pos_of_mem rows j hj
      have hpos2 : 0 < (∅ : Std.HashMap Dim Nat).getD j 0 + rows.countP fun r => r == j := by
        rw [Std.HashMap.getD_empty, Nat.zero_add]
        exact hpos
      rw [← hist_getD (init := (∅ : Std.HashMap Dim Nat))] at hpos2
      obtain ⟨v, hv⟩ := hist_getElem?_some (init := (∅ : Std.HashMap Dim Nat)) (rows := rows)
        (j := j) (by simp [Std.HashMap.getD_empty, Std.HashMap.getElem?_empty]) hpos2
      exact ⟨(j, v), by rw [hfe]; exact Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hv,
        by simp⟩
    have hfold := fold_hist fine.toList league flag rows
      (fun (l, p) => l == league && outfield p == flag) 0 hw hpd hcov
    rw [hfold]
    exact Nat.zero_add _
  · rw [hfe]
    exact hist_size_le rows

/-- One step of `classify` on an explicit cache/output/call state. -/
private def cStep (c : Chunk) (st : Std.HashMap Nat Bool × List Bool × Nat) (code : Nat) :
    Std.HashMap Nat Bool × List Bool × Nat :=
  match st with
  | (cache, out, calls) =>
      match cache[code]? with
      | some b => (cache, b :: out, calls)
      | none =>
          let b := outfield c.dict[code]?.join
          (cache.insert code b, b :: out, calls + 1)

private theorem snoc_removeAll_mem [BEq α] [LawfulBEq α] {l : List α} {a : α} (h : a ∈ l) :
    [(a : α)].removeAll l = [] := by
  induction l with
  | nil => exact absurd h (by simp)
  | cons x xs ih =>
      rw [List.removeAll_cons]
      by_cases hx : (a == x) = true
      · rw [beq_iff_eq.mp hx]
        simp
      · have hne : ¬ a = x := fun hh => hx (beq_iff_eq.mpr hh)
        have hxs : a ∈ xs := by
          rw [List.mem_cons] at h
          cases h with
          | inl he => exact absurd he hne
          | inr hm => exact hm
        have hf : (([(a : α)]).filter fun b => !b == x) = [a] := by
          simp [hx]
        rw [hf, ih hxs]

private theorem snoc_removeAll_not [BEq α] [LawfulBEq α] {l : List α} {a : α} (h : ¬ a ∈ l) :
    [(a : α)].removeAll l = [a] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.removeAll_cons]
      by_cases hx : (a == x) = true
      · exact absurd (beq_iff_eq.mp hx) (fun hh => h (by rw [hh]; exact List.Mem.head xs))
      · have hxs : ¬ a ∈ xs := fun m => h (List.Mem.tail x m)
        have hf : (([(a : α)]).filter fun b => !b == x) = [a] := by
          simp [hx]
        rw [hf, ih hxs]

private theorem eraseDups_length_snoc [BEq α] [LawfulBEq α] (l : List α) (a : α) (h : a ∈ l) :
    (l ++ [a]).eraseDups.length = l.eraseDups.length := by
  rw [List.eraseDups_append, List.length_append, snoc_removeAll_mem h]
  simp

private theorem eraseDups_length_snoc_not [BEq α] [LawfulBEq α] (l : List α) (a : α)
    (h : ¬ a ∈ l) : (l ++ [a]).eraseDups.length = l.eraseDups.length + 1 := by
  rw [List.eraseDups_append, List.length_append, snoc_removeAll_not h, List.eraseDups_cons,
    List.filter_nil, List.eraseDups_nil]
  simp

/-- The fold in `classify` preserves the decode-equality of cached labels, the exact
cache domain, and the call count as the number of distinct codes seen so far. -/
private theorem cc_fold (c : Chunk) :
    ∀ (ps : List Nat) (cache : Std.HashMap Nat Bool) (out : List Bool) (calls : Nat)
      (remaining : List Nat),
      out.reverse = ps.map (fun i => outfield c.dict[i]?.join) →
      (∀ (code : Nat), (∃ b, cache[code]? = some b) ↔ code ∈ ps) →
      (∀ (a : Nat) (b : Bool), cache[a]? = some b → b = outfield c.dict[a]?.join) →
      calls = ps.eraseDups.length →
      ((remaining.foldl (cStep c) (cache, out, calls)).2.1).reverse =
          (ps ++ remaining).map (fun i => outfield c.dict[i]?.join) ∧
        (∀ (code : Nat),
          (∃ b, (remaining.foldl (cStep c) (cache, out, calls)).1[code]? = some b) ↔
          code ∈ ps ++ remaining) ∧
        (∀ (a : Nat) (b : Bool), (remaining.foldl (cStep c) (cache, out, calls)).1[a]? = some b →
          b = outfield c.dict[a]?.join) ∧
        (remaining.foldl (cStep c) (cache, out, calls)).2.2 = (ps ++ remaining).eraseDups.length := by
  intro ps cache out calls remaining
  induction remaining generalizing ps cache out calls with
  | nil =>
      intro h1 h2 h3 h4
      simp only [List.foldl_nil, List.append_nil]
      exact ⟨h1, h2, h3, h4⟩
  | cons r rem ih =>
      intro h1 h2 h3 h4
      rw [List.foldl_cons]
      simp only [cStep]
      cases h : cache[r]? with
      | some b =>
          have hrps : r ∈ ps := (h2 r).1 ⟨b, h⟩
          have hres := ih (ps ++ [r]) cache (b :: out) calls
            (by
              rw [List.reverse_cons, List.map_append, List.map_cons, List.map_nil, h1,
                h3 r b h])
            (by
              intro code
              rw [List.mem_append, List.mem_singleton, ← h2 code]
              refine ⟨Or.inl, ?_⟩
              intro o
              cases o with
              | inl oo => exact oo
              | inr he =>
                  rw [he]
                  exact (h2 r).2 hrps)
            h3
            (by
              exact h4.trans (eraseDups_length_snoc ps r hrps).symm)
          rw [List.append_assoc] at hres
          exact hres
      | none =>
          have hrps : ¬ r ∈ ps := by
            intro m
            obtain ⟨w, hw⟩ := (h2 r).2 m
            rw [h] at hw
            exact absurd hw (by simp)
          have hres := ih (ps ++ [r]) (cache.insert r (outfield c.dict[r]?.join))
            ((outfield c.dict[r]?.join) :: out) (calls + 1)
            (by
              rw [List.reverse_cons, List.map_append, List.map_cons, List.map_nil, h1])
            (by
              intro code
              rw [List.mem_append, List.mem_singleton, ← h2 code,
                Std.HashMap.getElem?_insert]
              cases hc : r == code with
              | true =>
                  rw [beq_iff_eq.mp hc]
                  simp
              | false =>
                  have hnt : ¬((r == code) = true) := by
                    intro ht
                    rw [hc] at ht
                    exact absurd ht (by simp)
                  simp
                  intro he
                  exact absurd (beq_iff_eq.mpr he.symm) hnt)
            (by
              intro a bb hq
              rw [Std.HashMap.getElem?_insert] at hq
              cases ha : r == a with
              | true =>
                  rw [if_pos ha] at hq
                  obtain rfl := Option.some.inj hq
                  rw [beq_iff_eq.mp ha]
              | false =>
                  have hnt : ¬((r == a) = true) := by
                    intro ht
                    rw [ha] at ht
                    exact absurd ht (by simp)
                  rw [if_neg hnt] at hq
                  exact h3 a bb hq)
            (by
              rw [eraseDups_length_snoc_not ps r hrps]
              omega)
          rw [List.append_assoc] at hres
          exact hres

theorem sound_ClassificationCache : ClassificationCache.Goal := by
  intro c
  have h := cc_fold c [] {} [] 0 c.codes
    (by simp)
    (by
      intro code
      rw [Std.HashMap.getElem?_empty]
      exact ⟨fun o => absurd o (by simp), fun m => absurd m (by simp)⟩)
    (by
      intro a b hq
      rw [Std.HashMap.getElem?_empty] at hq
      exact absurd hq (by simp))
    (by simp)
  have h1 : (classify c).2.1.reverse = (labels c).map outfield := by
    rw [show (classify c).2.1 = (c.codes.foldl (cStep c) ({}, [], 0)).2.1 from rfl, h.1,
      List.nil_append]
    rw [show labels c = c.codes.map (fun i => c.dict[i]?.join) from rfl]
    rw [List.map_map]
    rfl
  have h2 : (classify c).2.2 = c.codes.eraseDups.length := by
    rw [show (classify c).2.2 = (c.codes.foldl (cStep c) ({}, [], 0)).2.2 from rfl, h.2.2.2,
      List.nil_append]
  exact ⟨h1, h2⟩

end TinylambOpt.ResearchProofs.CloudBI.PublicBI
