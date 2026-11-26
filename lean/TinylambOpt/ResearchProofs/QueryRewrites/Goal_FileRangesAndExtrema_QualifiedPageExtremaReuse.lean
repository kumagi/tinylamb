import TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema

/-!
# QualifiedPageExtremaReuse

For pages fully selected by the query, stored extrema can be combined with extrema
computed by scanning mixed pages. This reduces the number of metric values decoded.
-/


set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_QualifiedPageExtremaReuse

def optMinI : Option Int → Option Int → Option Int
  | none, none => none
  | some a, none => some a
  | none, some b => some b
  | some a, some b => some (min a b)

def optMaxI : Option Int → Option Int → Option Int
  | none, none => none
  | some a, none => some a
  | none, some b => some b
  | some a, some b => some (max a b)

theorem optMinI_none_left (o : Option Int) : optMinI none o = o := by cases o <;> rfl
theorem optMaxI_none_left (o : Option Int) : optMaxI none o = o := by cases o <;> rfl

theorem min_append (xs ys : List Int) : (xs ++ ys).min? = optMinI xs.min? ys.min? := by
  induction xs with
  | nil => exact (optMinI_none_left ys.min?).symm
  | cons x xs ih =>
      simp only [List.cons_append, List.min?_cons, ih]
      cases hx : xs.min? with
      | none => cases hy : ys.min? <;> simp [optMinI, hx, hy, Int.min_comm]
      | some a => cases hy : ys.min? with
        | none => simp [optMinI, hx, hy]
        | some b => simp [optMinI, hx, hy, Int.min_assoc]

theorem max_append (xs ys : List Int) : (xs ++ ys).max? = optMaxI xs.max? ys.max? := by
  induction xs with
  | nil => exact (optMaxI_none_left ys.max?).symm
  | cons x xs ih =>
      simp only [List.cons_append, List.max?_cons, ih]
      cases hx : xs.max? with
      | none => cases hy : ys.max? <;> simp [optMaxI, hx, hy, Int.max_comm]
      | some a => cases hy : ys.max? with
        | none => simp [optMaxI, hx, hy]
        | some b => simp [optMaxI, hx, hy, Int.max_assoc]

theorem min_flat_extrema {α : Type} (ps : List α) (f : α → List Int) :
    ((ps.map (fun p => ((f p).min?, (f p).max?))).filterMap Prod.fst).min? =
      (ps.map f).flatten.min? := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
      simp only [List.map_cons, List.filterMap_cons]
      cases h : (f p).min? with
      | none =>
        simp only [h, id_eq, List.map_cons, List.flatten_cons]
        rw [min_append, h]
        simpa [optMinI_none_left] using ih
      | some a =>
        simp only [h, id_eq, List.map_cons, List.flatten_cons]
        rw [min_append, h]
        rw [List.min?_cons, ih]
        cases hf : (List.map f ps).flatten.min? <;> simp [optMinI, hf]

theorem max_flat_extrema {α : Type} (ps : List α) (f : α → List Int) :
    ((ps.map (fun p => ((f p).min?, (f p).max?))).filterMap Prod.snd).max? =
      (ps.map f).flatten.max? := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
      simp only [List.map_cons, List.filterMap_cons]
      cases h : (f p).max? with
      | none =>
        simp only [h, id_eq, List.map_cons, List.flatten_cons]
        rw [max_append, h]
        simpa [optMaxI_none_left] using ih
      | some a =>
        simp only [h, id_eq, List.map_cons, List.flatten_cons]
        rw [max_append, h]
        rw [List.max?_cons, ih]
        cases hf : (List.map f ps).flatten.max? <;> simp [optMaxI, hf]

theorem flatten_filter_filterMap (pages : List (List Nat)) (passes : Nat → Bool)
    (metric : Nat → Option Int) :
    (pages.map (fun p => (p.filter passes).filterMap metric)).flatten =
      (pages.flatten.filter passes).filterMap metric := by
  induction pages with
  | nil => rfl
  | cons p ps ih =>
      simp [List.filter_flatten, List.filterMap_append, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema.QualifiedPageExtremaReuse.Goal := by
  intro pages passes metric col
  dsimp only
  have hmap : pages.map (fun p =>
      if p.all passes then ((p.filterMap (metric col)).min?, (p.filterMap (metric col)).max?)
      else (((p.filter passes).filterMap (metric col)).min?,
            ((p.filter passes).filterMap (metric col)).max?)) =
      pages.map (fun p => (((p.filter passes).filterMap (metric col)).min?,
            ((p.filter passes).filterMap (metric col)).max?)) := by
    apply List.map_congr_left
    intro p hp
    by_cases h : p.all passes = true
    · have hp' : p.filter passes = p := List.filter_eq_self.mpr (List.all_eq_true.mp h)
      simp [h, hp']
    · simp [h]
  rw [hmap]
  rw [← flatten_filter_filterMap]
  rw [min_flat_extrema, max_flat_extrema]

end TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_QualifiedPageExtremaReuse


-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
