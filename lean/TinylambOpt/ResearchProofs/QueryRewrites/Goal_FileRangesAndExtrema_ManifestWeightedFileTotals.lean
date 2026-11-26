import TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# ManifestWeightedFileTotals

Per-file counts and row-ID sums weighted by manifest multiplicity reproduce the modeled
joined totals. The executor can avoid expanding each matching manifest row.
-/


set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_ManifestWeightedFileTotals

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_map_nsum)

theorem sum_map_zero_nat {α : Type} (xs : List α) : (xs.map fun _ => 0).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa using ih

theorem nsum_eq_of_length_sum (xs ys : List Int) (hl : xs.length = ys.length) (hs : xs.sum = ys.sum) :
    nsum (xs.map some) = nsum (ys.map some) := by
  by_cases h : xs.length = 0
  · have hx : xs = [] := List.length_eq_zero_iff.mp h
    have hy : ys = [] := List.length_eq_zero_iff.mp (by omega)
    simp [hx, hy, nsum]
  · have hx : xs ≠ [] := by intro hx; subst hx; simp at h
    have hy : ys ≠ [] := by intro hy; subst hy; simp [hl] at h
    simp [nsum, hx, hy, hs]

theorem sum_flatMap_replicate (rows : List (Nat × Int)) (fs : List Nat) :
    (rows.flatMap (fun (x,r) => List.replicate (fs.count x) r)).sum =
      (rows.map (fun (x,r) => (fs.count x : Int) * r)).sum := by
  induction rows with
  | nil => simp
  | cons r rows ih =>
      obtain ⟨x, v⟩ := r
      simp [List.sum_replicate_int, ih]

theorem length_cons_decomp (rows : List (Nat × Int)) (f : Nat) (fs : List Nat) :
    (rows.flatMap (fun (x,r) => List.replicate ((f::fs).count x) r)).length =
      (rows.flatMap (fun (x,r) => List.replicate (fs.count x) r)).length +
      (rows.filter (fun r => r.1 == f)).length := by
  rw [List.length_flatMap, List.length_flatMap]
  simp only [List.length_replicate]
  induction rows with
  | nil => simp
  | cons r rows ih =>
      obtain ⟨x, v⟩ := r
      simp only [List.map_cons, List.sum_cons, List.filter_cons, ih]
      by_cases h : x == f
      · have hxf : x = f := by simpa using h
        have hfx : f = x := hxf.symm
        simp [List.count_cons, hfx]
        omega
      · have hfx : f ≠ x := by intro hfx; exact h (by simp [hfx.symm])
        have hxf : x ≠ f := by intro hxf; exact hfx hxf.symm
        simp [List.count_cons, hfx, hxf]
        omega

theorem sum_cons_decomp (rows : List (Nat × Int)) (f : Nat) (fs : List Nat) :
    (rows.flatMap (fun (x,r) => List.replicate ((f::fs).count x) r)).sum =
      (rows.flatMap (fun (x,r) => List.replicate (fs.count x) r)).sum +
      ((rows.filter (fun r => r.1 == f)).map Prod.snd).sum := by
  rw [sum_flatMap_replicate, sum_flatMap_replicate]
  induction rows with
  | nil => simp
  | cons r rows ih =>
      obtain ⟨x, v⟩ := r
      simp only [List.map_cons, List.sum_cons, List.filter_cons, List.map_cons, ih]
      by_cases h : x == f
      · have hxf : x = f := by simpa using h
        have hfx : f = x := hxf.symm
        simp [List.count_cons, hfx, Int.natCast_add, Int.add_mul, Int.add_comm, Int.add_left_comm, Int.add_assoc]
      · have hfx : f ≠ x := by intro hfx; exact h (by simp [hfx.symm])
        have hxf : x ≠ f := by intro hxf; exact hfx hxf.symm
        simp [List.count_cons, hfx, hxf, Int.add_assoc]

theorem length_eq (files : List Nat) (rows : List (Nat × Int)) :
    (rows.flatMap (fun (f,r) => List.replicate (files.count f) r)).length =
      (files.flatMap (fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd)).length := by
  induction files with
  | nil => simp [sum_map_zero_nat]
  | cons f fs ih =>
      simp only [List.flatMap_cons, List.length_append]
      rw [length_cons_decomp rows f fs, ih]
      simp [List.length_map]
      omega

theorem sum_eq (files : List Nat) (rows : List (Nat × Int)) :
    (rows.flatMap (fun (f,r) => List.replicate (files.count f) r)).sum =
      (files.flatMap (fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd)).sum := by
  induction files with
  | nil =>
      induction rows with
      | nil => simp
      | cons r rows ihr => simpa using ihr
  | cons f fs ih =>
      simp only [List.flatMap_cons, List.sum_append]
      rw [sum_cons_decomp rows f fs, ih]
      exact Int.add_comm _ _

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema.ManifestWeightedFileTotals.Goal := by
  intro files rows
  dsimp only
  apply Prod.ext
  · rw [← List.length_flatMap]
    exact length_eq files rows
  · simp only [Prod.snd]
    have hright :
        nsum (files.map (fun f =>
          nsum (((rows.filter (fun r => r.1 == f)).map Prod.snd).map some))) =
        nsum ((files.flatMap (fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd)).map some) := by
      simpa only [List.map_map, Function.comp_def, List.flatMap_def, List.map_flatten] using
        nsum_map_nsum (files.map (fun f =>
          ((rows.filter (fun r => r.1 == f)).map Prod.snd).map some))
    rw [hright]
    exact nsum_eq_of_length_sum _ _ (length_eq files rows) (sum_eq files rows)

end TinylambOpt.ResearchProofs.QueryRewrites.FileRangesAndExtrema.Goal_ManifestWeightedFileTotals

