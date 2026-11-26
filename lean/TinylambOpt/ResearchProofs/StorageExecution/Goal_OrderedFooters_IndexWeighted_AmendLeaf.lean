import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

/-! Small-input truth check: `combine` is a left fold of the page summaries,
and the index-weighted join is an associative monoid with identity `(0,0,0)`,
so replacing one page must decompose as prefix join page join suffix. -/
private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs, 3 :: xs])

private def pages (n : Nat) : List (List (List Nat)) :=
  let ls := gen n
  [[]] ++ ls.map (fun a => [a]) ++ ls.flatMap (fun a => ls.map (fun b => [a, b]))

private def probe (n : Nat) : Bool :=
  let psys := pages n
  let reps : List (List Nat) := [[], [0], [1, 2], [3, 4, 5]]
  psys.all (fun ps => (List.range ps.length).all (fun i =>
    reps.all (fun xs =>
      combine agg (ps.set i xs) ==
        agg.join (combine agg (ps.take i))
          (agg.join (agg.reference xs) (combine agg (ps.drop (i + 1)))))))

#eval probe 2
#eval probe 3

private theorem join_left_identity : ∀ x : Nat × Nat × Nat, agg.join (0, 0, 0) x = x := by
  intro x
  rcases x with ⟨a, b, c⟩
  simp [agg]

private theorem join_right_identity : ∀ x : Nat × Nat × Nat, agg.join x (0, 0, 0) = x := by
  intro x
  rcases x with ⟨a, b, c⟩
  simp [agg]

private theorem join_assoc : ∀ a b c : Nat × Nat × Nat,
    agg.join (agg.join a b) c = agg.join a (agg.join b c) := by
  intro a b c
  rcases a with ⟨a₁, a₂, a₃⟩
  rcases b with ⟨b₁, b₂, b₃⟩
  rcases c with ⟨c₁, c₂, c₃⟩
  simp [agg, Nat.mul_add, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm, Nat.mul_comm]

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

private theorem ref_nil : agg.reference [] = (0, 0, 0) := by
  rfl

theorem sound_AmendLeaf : IndexWeighted.AmendLeaf.Goal := by
  intro ps i xs hi
  simpa [combine, ref_nil] using
    (amend_general agg.join agg.reference (0, 0, 0)
      join_left_identity join_right_identity join_assoc
      (agg.reference []) ps i xs hi)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_AmendLeaf
