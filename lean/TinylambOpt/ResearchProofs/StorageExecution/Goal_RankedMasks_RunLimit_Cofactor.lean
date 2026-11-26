import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `RunLimit.Cofactor` — Compute a parallel shard count by conditioning on its prefix
  state.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_Cofactor

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.RunLimit

-- Truth check on small inputs (including states not reachable from the start state).
private def check : Bool :=
  (List.range 9).all fun a =>
  (List.range 9).all fun b =>
  (List.range 4).all fun q =>
    decide (count model (a + b) q =
      ((words a).map (fun pre => ((run model q pre).map (count model b)).getD 0)).sum)

#eval check

private theorem option_map_ext {α : Type} (o : Option α) (f g : α → Nat)
    (h : ∀ x, f x = g x) : (o.map f).getD 0 = (o.map g).getD 0 := by
  cases o with
  | none => simp
  | some x => simp [h]

private theorem map_bind_getD {α β : Type} (o : Option α) (f : α → Option β)
    (g : β → Nat) :
    ((o.bind f).map g).getD 0 =
      (o.map (fun x => ((f x).map g).getD 0)).getD 0 := by
  cases o <;> simp

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  cases h : m.step q b with
  | none => simp [run, h]
  | some q' => simp [run, h]

@[simp] private theorem sum_map_zero {β : Type} (ws : List β) :
    (ws.map (fun _ : β => (0 : Nat))).sum = 0 := by
  induction ws with
  | nil => rfl
  | cons w ws ih => simpa using ih

private theorem sum_map_getD {α β : Type} (o : Option α) (f : α → β → Nat)
    (ws : List β) :
    (ws.map (fun pre => (o.map (fun x => f x pre)).getD 0)).sum =
      (o.map (fun x => (ws.map (fun pre => f x pre)).sum)).getD 0 := by
  cases o <;> simp

-- The cofactor identity holds for every machine, not just this model.
private theorem cofactor_general (m : Machine) : cofactor m := by
  intro q a
  induction a generalizing q with
  | zero =>
      intro b
      simp [run, words]
  | succ a ih =>
      intro b
      rw [show a + 1 + b = (a + b) + 1 by omega]
      simp only [count]
      simp only [words, List.map_append, List.sum_append, List.map_map, Function.comp_def]
      have hfalse :
          ((m.step q false).map (count m (a + b))).getD 0 =
            ((m.step q false).map (fun q' =>
              ((words a).map (fun pre => ((run m q' pre).map (count m b)).getD 0)).sum)).getD 0 := by
        apply option_map_ext
        intro q'
        exact ih q' b
      have htrue :
          ((m.step q true).map (count m (a + b))).getD 0 =
            ((m.step q true).map (fun q' =>
              ((words a).map (fun pre => ((run m q' pre).map (count m b)).getD 0)).sum)).getD 0 := by
        apply option_map_ext
        intro q'
        exact ih q' b
      rw [hfalse, htrue]
      have hcons (c : Bool) :
          ((words a).map (fun pre => ((run m q (c :: pre)).map (count m b)).getD 0)).sum =
            ((m.step q c).map (fun q' =>
              ((words a).map (fun pre => ((run m q' pre).map (count m b)).getD 0)).sum)).getD 0 := by
        rw [← sum_map_getD (m.step q c)
              (fun q' pre => ((run m q' pre).map (count m b)).getD 0) (words a)]
        simp only [run_cons, map_bind_getD]
      rw [hcons false, hcons true]

theorem sound_Cofactor : RunLimit.Cofactor.Goal := by
  exact cofactor_general model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_RunLimit_Cofactor
