import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BalanceFloor.TreeReduce` — Merge an arbitrary spill tree without visiting the raw
  rows again.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def balStep (x : Nat) : Int :=
  if x % 2 == 0 then (x / 2 : Int) else -((x / 2 + 1 : Nat) : Int)

private def mpref (ys : List Int) : Int := (ys.scanl (· + ·) 0).min?.getD 0

private def optMin : Option Int → Option Int → Option Int
  | none, b => b
  | a, none => a
  | some a, some b => some (min a b)

private theorem scanl_ne_nil (f : β → α → β) (b : β) (l : List α) : l.scanl f b ≠ [] := by
  cases l with
  | nil => simp
  | cons x xs => simp [List.scanl]

private theorem min?_eq_none_iff (l : List Int) : l.min? = none ↔ l = [] := by
  cases l with
  | nil => simp
  | cons x xs => simp [List.min?]

private theorem min?_append_opt (xs ys : List Int) :
    (xs ++ ys).min? = optMin xs.min? ys.min? := by
  induction xs with
  | nil => simp [optMin]
  | cons x xs ih =>
      simp only [List.cons_append, List.min?_cons]
      rw [ih]
      cases h1 : xs.min? with
      | none =>
          cases h2 : ys.min? with
          | none => simp [optMin]
          | some b => simp [optMin]
      | some a =>
          cases h2 : ys.min? with
          | none => simp [optMin]
          | some b => simp [optMin]

private theorem min?_map_add (c : Int) (l : List Int) :
    (l.map (fun p => c + p)).min? = l.min?.map (fun p => c + p) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.min?_cons]
      rw [ih]
      cases h : xs.min? with
      | none => simp
      | some a => simp

private theorem foldl_add_eq (s : Int) (ys : List Int) : ys.foldl (· + ·) s = s + ys.sum := by
  induction ys generalizing s with
  | nil => simp
  | cons y ys ih =>
      rw [List.foldl_cons, ih (s + y), List.sum_cons]
      omega

private theorem scanl_add_shift (c d : Int) (zs : List Int) :
    zs.scanl (· + ·) (c + d) = (zs.scanl (· + ·) d).map (fun p => c + p) := by
  induction zs generalizing c d with
  | nil => simp
  | cons z zs ih =>
      simp only [List.scanl_cons, List.map_cons]
      congr 1
      have hadd : c + d + z = c + (d + z) := by omega
      rw [hadd]
      exact ih c (d + z)

private theorem mpref_shift (c : Int) (zs : List Int) :
    (zs.scanl (· + ·) c).min?.getD c = c + mpref zs := by
  unfold mpref
  rw [show c = c + 0 by omega]
  rw [scanl_add_shift c 0 zs]
  rw [min?_map_add]
  cases (zs.scanl (· + ·) 0).min? with
  | none => simp
  | some a => simp

private theorem scanl_min_le_final (s : Int) (ys : List Int) :
    (ys.scanl (· + ·) s).min?.getD s ≤ s + ys.sum := by
  induction ys generalizing s with
  | nil => simp
  | cons y ys ih =>
      rw [List.scanl_cons]
      have hne : (ys.scanl (· + ·) (s + y)).min? ≠ none := by
        intro h
        have : ys.scanl (· + ·) (s + y) = [] := (min?_eq_none_iff _).mp h
        exact scanl_ne_nil _ _ _ this
      cases h : (ys.scanl (· + ·) (s + y)).min? with
      | none => contradiction
      | some v =>
          simp [h]
          have hv : v ≤ s + y + ys.sum := by
            have hh := ih (s + y)
            simpa [h] using hh
          omega

private theorem scanl_append_min (s : Int) (ys zs : List Int) :
    ((ys ++ zs).scanl (· + ·) s).min?.getD s =
      min ((ys.scanl (· + ·) s).min?.getD s)
          ((zs.scanl (· + ·) (s + ys.sum)).min?.getD (s + ys.sum)) := by
  rw [List.scanl_append]
  rw [foldl_add_eq]
  rw [min?_append_opt]
  have hA : (ys.scanl (· + ·) s).min?.getD s ≤ s + ys.sum := scanl_min_le_final s ys
  let A := (ys.scanl (· + ·) s).min?
  let T := ((zs.scanl (· + ·) (s + ys.sum)).tail).min?
  let B := (zs.scanl (· + ·) (s + ys.sum)).min?
  have hB : B = T.elim (s + ys.sum) (min (s + ys.sum)) := by
    dsimp [B, T]
    rw [show (zs.scanl (· + ·) (s + ys.sum)) =
        (s + ys.sum) :: (zs.scanl (· + ·) (s + ys.sum)).tail by
      cases zs with
      | nil => simp [List.scanl]
      | cons z zs => simp [List.scanl]]
    exact List.min?_cons
  cases hA' : A with
  | none =>
      have hne : A ≠ none := by
        dsimp [A]
        intro h
        have : ys.scanl (· + ·) s = [] := (min?_eq_none_iff _).mp h
        exact scanl_ne_nil _ _ _ this
      contradiction
  | some a =>
      dsimp [A] at hA'
      cases hT : T with
      | none =>
          simp [optMin, hA', hT, hB, T, B] at hA ⊢
          omega
      | some t =>
          simp [optMin, hA', hT, hB, T, B] at hA ⊢
          omega

private theorem mpref_append (ys zs : List Int) :
    mpref (ys ++ zs) = min (mpref ys) (ys.sum + mpref zs) := by
  have hshift := mpref_shift ys.sum zs
  unfold mpref at hshift ⊢
  rw [scanl_append_min 0 ys zs]
  rw [show 0 + ys.sum = ys.sum by omega]
  rw [hshift]

private theorem stitch_agg : stitch agg := by
  intro a b
  change ( (a.map balStep).sum + (b.map balStep).sum,
           min (mpref (a.map balStep)) ((a.map balStep).sum + mpref (b.map balStep)) ) =
    (((a ++ b).map balStep).sum, mpref ((a ++ b).map balStep))
  rw [List.map_append, List.sum_append, mpref_append]

private theorem treeReduce_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) = f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

theorem sound_TreeReduce : BalanceFloor.TreeReduce.Goal := by
  exact treeReduce_of_stitch agg stitch_agg

#guard agg.join (agg.reference [0, 1, 4, 3]) (agg.reference [2, 7]) == agg.reference [0, 1, 4, 3, 2, 7]
#guard rootState agg (.node (.leaf [0, 1, 4]) (.node (.leaf [3, 2]) (.leaf [7]))) ==
  agg.reference (plain (.node (.leaf [0, 1, 4]) (.node (.leaf [3, 2]) (.leaf [7]))))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_TreeReduce
