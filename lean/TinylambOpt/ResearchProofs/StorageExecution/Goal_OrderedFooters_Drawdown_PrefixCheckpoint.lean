import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `Drawdown.PrefixCheckpoint` — A saved prefix plus page states yields a scan
  checkpoint at each boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

-- The fold state of a nonempty list: (minimum, maximum, maximum ordered drop).
private def lo (xs : List Nat) : Nat := xs.min?.getD 0
private def hi (xs : List Nat) : Nat := xs.max?.getD 0
private def dd (xs : List Nat) : Nat :=
  peak (xs.zipIdx.map (fun (x, i) => x - ((xs.drop i).min?.getD x)))

private theorem lo_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    lo (a ++ b) = min (lo a) (lo b) := by
  cases a with
  | nil => contradiction
  | cons x xs =>
      simp only [lo, List.cons_append, List.min?_cons', Option.getD_some]
      rw [List.foldl_append]
      rw [List.foldl_min_eq_min hb]
      simp [List.min?_eq_some_min hb]

private theorem hi_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    hi (a ++ b) = max (hi a) (hi b) := by
  cases a with
  | nil => contradiction
  | cons x xs =>
      simp only [hi, List.cons_append, List.max?_cons', Option.getD_some]
      rw [List.foldl_append]
      rw [List.foldl_max_eq_max hb]
      simp [List.max?_eq_some_max hb]

private theorem lo_cons_nonempty (x : Nat) (xs : List Nat) (hxs : xs ≠ []) :
    lo (x :: xs) = min x (lo xs) := by
  cases h : xs.min? with
  | none =>
      have hnil : xs = [] := by simpa using h
      exfalso
      exact hxs hnil
  | some m => simp [lo, List.min?_cons, h]

private theorem hi_cons_nonempty (x : Nat) (xs : List Nat) (hxs : xs ≠ []) :
    hi (x :: xs) = max x (hi xs) := by
  cases h : xs.max? with
  | none =>
      have hnil : xs = [] := by simpa using h
      exfalso
      exact hxs hnil
  | some m => simp [hi, List.max?_cons, h]

-- Prepending x contributes one new candidate `x - lo (x :: xs)` and keeps
-- all suffix minima of xs unchanged (the indices are shifted by one).
private theorem dd_cons (x : Nat) (xs : List Nat) :
    dd (x :: xs) = max (x - lo (x :: xs)) (dd xs) := by
  unfold dd lo peak
  rw [List.zipIdx_cons, List.zipIdx_eq_map_add]
  simp only [List.map_cons, List.drop_zero, Nat.zero_add]
  have hmap : ((List.zipIdx xs).map (fun ⟨a, j⟩ => (a, 1 + j))).map
        (fun p => p.1 - (((x :: xs).drop p.2).min?.getD p.1)) =
      (List.zipIdx xs).map (fun p => p.1 - ((xs.drop p.2).min?.getD p.1)) := by
    simp only [List.map_map]
    apply List.map_congr_left
    intro p
    simp [List.drop_succ_cons, Nat.add_comm]
  rw [hmap]
  cases h : ((List.zipIdx xs).map (fun p => p.1 - ((xs.drop p.2).min?.getD p.1))).max? with
  | none => simp [h]
  | some m => simp [h]

private theorem reference_nonempty (xs : List Nat) (hxs : xs ≠ []) :
    agg.reference xs = some (lo xs, hi xs, dd xs) := by
  cases xs with
  | nil => contradiction
  | cons x xs => simp [agg, lo, hi, dd, peak]

private theorem sub_min_self (x a : Nat) : x - min x a = x - a := by omega
private theorem sub_min (x a b : Nat) : x - min a b = max (x - a) (x - b) := by omega
private theorem max_sub_right (a b c : Nat) : max a b - c = max (a - c) (b - c) := by omega

-- The state summary is a monoid: empty lists are the identity and joining
-- two nonempty summaries equals summarizing the concatenation.
private theorem stitch_agg : stitch agg := by
  intro a
  induction a with
  | nil =>
      intro b
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys => simp [agg, optionalJoin]
  | cons x as ih =>
      intro b
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          have hbne : (y :: ys) ≠ [] := by simp
          by_cases has : as = []
          · subst as
            simp only [List.singleton_append]
            rw [reference_nonempty [x] (by simp), reference_nonempty (y::ys) hbne,
                reference_nonempty (x::(y::ys)) (by simp)]
            simp only [agg, optionalJoin, Option.some.injEq]
            simp only [Prod.mk.injEq]
            have hlo : lo (x :: (y :: ys)) = min x (lo (y::ys)) :=
              lo_cons_nonempty x (y::ys) hbne
            have hhi : hi (x :: (y :: ys)) = max x (hi (y::ys)) :=
              hi_cons_nonempty x (y::ys) hbne
            have hdd : dd (x :: (y :: ys)) = max (x - lo (x::(y::ys))) (dd (y::ys)) :=
              dd_cons x (y::ys)
            simp [lo, hi, dd, peak] at hlo hhi hdd ⊢
            omega
          · have ih' := ih (y::ys)
            rw [reference_nonempty as has, reference_nonempty (y::ys) hbne,
                reference_nonempty (as++(y::ys)) (by simp [has])] at ih'
            rw [reference_nonempty (x::as) (by simp), reference_nonempty (y::ys) hbne,
                reference_nonempty ((x::as)++(y::ys)) (by simp)]
            simp only [agg, optionalJoin, Option.some.injEq] at ih' ⊢
            simp only [Prod.mk.injEq] at ih' ⊢
            have ihlo : min (lo as) (lo (y::ys)) = lo (as++(y::ys)) := ih'.1
            have ihhi : max (hi as) (hi (y::ys)) = hi (as++(y::ys)) := ih'.2.1
            have ihdd : max (max (dd as) (dd (y::ys))) (hi as - lo (y::ys)) =
                dd (as++(y::ys)) := ih'.2.2
            have h1 : lo (x::as) = min x (lo as) := lo_cons_nonempty x as has
            have h2 : hi (x::as) = max x (hi as) := hi_cons_nonempty x as has
            have h3 : dd (x::as) = max (x - lo (x::as)) (dd as) := dd_cons x as
            have h4 : lo (x::(as++(y::ys))) = min x (lo (as++(y::ys))) :=
              lo_cons_nonempty x (as++(y::ys)) (by simp [hbne])
            have h5 : hi (x::(as++(y::ys))) = max x (hi (as++(y::ys))) :=
              hi_cons_nonempty x (as++(y::ys)) (by simp [hbne])
            have h6 : dd (x::(as++(y::ys))) = max (x - lo (x::(as++(y::ys))))
                (dd (as++(y::ys))) := dd_cons x (as++(y::ys))
            have h7 : lo (as++(y::ys)) = min (lo as) (lo (y::ys)) :=
              lo_append as (y::ys) has hbne
            have h8 : hi (as++(y::ys)) = max (hi as) (hi (y::ys)) :=
              hi_append as (y::ys) has hbne
            simp only [List.cons_append]
            constructor
            · rw [h1, h4, ← ihlo, Nat.min_assoc]
            constructor
            · rw [h2, h5, ← ihhi, Nat.max_assoc]
            · rw [h3, h6, ← ihdd, h1, h2, h4, ← ihlo]
              rw [sub_min_self]
              rw [sub_min]
              rw [sub_min]
              rw [max_sub_right]
              simp [Nat.max_left_comm, Nat.max_comm]

private theorem foldl_reference {σ : Type} (f : Fold σ) (h : stitch f)
    (l : List (List Nat)) (acc : List Nat) :
    List.foldl f.join (f.reference acc) (l.map f.reference) =
      f.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem prefixCheckpoint_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    prefixCheckpoint f := by
  intro init ps
  apply List.ext_getElem
  · simp
  · intro i hi₁ hi₂
    rw [List.getElem_map (fun i => f.reference (init ++ (ps.take i).flatten)),
      List.getElem_range, List.getElem_scanl]
    rw [← List.map_take]
    exact foldl_reference f h (ps.take i) init

theorem sound_PrefixCheckpoint : Drawdown.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_PrefixCheckpoint
