import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `MonotoneBreaks.AmendLeaf` — Update one cached block using the left and right partial
  states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun b => ls.all (fun c =>
    let ps : List (List Nat) := [a, b, c]
    (List.range (ps.length + 1)).all (fun i =>
      ls.all (fun xs =>
        i < ps.length →
        combine agg (ps.set i xs) =
          agg.join (combine agg (ps.take i)) (agg.join (agg.reference xs) (combine agg (ps.drop (i + 1)))))))))

#eval probe 2

private def mix (x y : Nat × Nat × Nat) : Nat × Nat × Nat :=
  (x.1, y.2.1, x.2.2 + y.2.2 + (decide (y.1 < x.2.1)).toNat)

private theorem join_eq : agg.join = optionalJoin mix := by
  rfl

private theorem mix_assoc (x y z : Nat × Nat × Nat) : mix (mix x y) z = mix x (mix y z) := by
  rcases x with ⟨a, b, n⟩
  rcases y with ⟨c, d, m⟩
  rcases z with ⟨e, f, k⟩
  simp [mix]
  ac_rfl

private theorem join_assoc : ∀ a b c : Option (Nat × Nat × Nat),
    agg.join (agg.join a b) c = agg.join a (agg.join b c) := by
  intro a b c
  rw [join_eq]
  cases a <;> cases b <;> cases c <;> simp [optionalJoin, mix_assoc]

private theorem join_left_identity : ∀ x : Option (Nat × Nat × Nat), agg.join none x = x := by
  intro x
  rw [join_eq]
  cases x <;> rfl

private theorem join_right_identity : ∀ x : Option (Nat × Nat × Nat), agg.join x none = x := by
  intro x
  rw [join_eq]
  cases x <;> rfl

private theorem fold_with_acc {σ : Type} (join : σ → σ → σ) (e : σ) (a : σ)
    (hleft : ∀ x, join e x = x) (hright : ∀ x, join x e = x)
    (hassoc : ∀ x y z, join (join x y) z = join x (join y z)) :
    ∀ L : List σ, L.foldl join a = join a (L.foldl join e) := by
  intro L
  induction L generalizing a with
  | nil => simp [hright]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [hleft x]
      rw [ih (a := join a x)]
      rw [ih (a := x)]
      rw [hassoc]

private theorem amend_general {σ : Type} (join : σ → σ → σ) (ref : List Nat → σ) (e : σ)
    (hleft : ∀ x, join e x = x) (hright : ∀ x, join x e = x)
    (hassoc : ∀ x y z, join (join x y) z = join x (join y z)) :
    ∀ (a : σ) (ps : List (List Nat)) i xs, i < ps.length →
      ((ps.set i xs).map ref).foldl join a =
        join (((ps.take i).map ref).foldl join a)
          (join (ref xs) (((ps.drop (i + 1)).map ref).foldl join e)) := by
  intro a ps
  induction ps generalizing a with
  | nil =>
      intro i xs hi
      simp at hi
  | cons p ps ih =>
      intro i xs hi
      cases i with
      | zero =>
          simp only [List.set_cons_zero, List.take_zero, List.map_cons, List.foldl_cons]
          rw [fold_with_acc join e (join a (ref xs)) hleft hright hassoc]
          exact hassoc a (ref xs) ((ps.map ref).foldl join e)
      | succ k =>
          have hk : k < ps.length := by simpa using hi
          simpa [List.set, List.take, List.drop] using ih (join a (ref p)) k xs hk

private theorem ref_nil : agg.reference [] = none := by
  rfl

theorem sound_AmendLeaf : MonotoneBreaks.AmendLeaf.Goal := by
  intro ps i xs hi
  simpa [combine, ref_nil] using
    (amend_general agg.join agg.reference none
      join_left_identity join_right_identity join_assoc
      (agg.reference []) ps i xs hi)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_MonotoneBreaks_AmendLeaf
