import TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_RotateWitness

open TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

private theorem rotate_perm (a : Nat) (rest : List Nat) : (rest ++ [a]).Perm (a :: rest) := by
  induction rest with
  | nil => simp
  | cons b bs ih =>
      have h1 : (b :: (bs ++ [a])).Perm (b :: (a :: bs)) := ih.cons b
      have h2 : (b :: (a :: bs)).Perm (a :: (b :: bs)) := List.Perm.swap a b bs
      exact h1.trans h2

private theorem all_zip_rotate (g : Graph) (a b last : Nat) (bs : List Nat) :
    (List.zipWith Prod.mk (b :: (bs ++ [a])) (bs ++ [a, last])).all
        (fun p : Nat × Nat => (edge g p.1).any (fun e => e.holder == p.2)) =
      ((List.zipWith Prod.mk (b :: bs) (bs ++ [a])).all
        (fun p : Nat × Nat => (edge g p.1).any (fun e => e.holder == p.2)) &&
       (edge g a).any (fun e => e.holder == last)) := by
  induction bs generalizing b with
  | nil => simp [List.zipWith]
  | cons c cs ih =>
      simp [List.zipWith]
      rw [ih c]
      simp [Bool.and_assoc]

private theorem chain_rotate (g : Graph) (a : Nat) (rest : List Nat) :
    chain g (a :: rest) = chain g (rest ++ [a]) := by
  unfold chain
  cases rest with
  | nil => simp
  | cons b bs =>
      simp [List.drop, List.take, List.zip_eq_zipWith, List.zipWith_cons_cons,
        List.all_cons, all_zip_rotate, Bool.and_assoc, Bool.and_comm, Bool.and_left_comm]

theorem sound : RotateWitness.Goal := by
  intro g a rest
  constructor
  · intro h
    rcases h with ⟨hne, hnd, hch⟩
    refine ⟨by simp, (rotate_perm a rest).nodup_iff.mpr hnd, ?_⟩
    rwa [← chain_rotate]
  · intro h
    rcases h with ⟨hne, hnd, hch⟩
    refine ⟨by simp, (rotate_perm a rest).nodup_iff.mp hnd, ?_⟩
    rwa [chain_rotate]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_RotateWitness
