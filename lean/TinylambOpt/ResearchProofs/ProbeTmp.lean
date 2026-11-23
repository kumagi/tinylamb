import TinylambOpt.ResearchGoals.HammingOneSearch

namespace TinylambOpt.ResearchProofs.ProbeTmp
open TinylambOpt.ResearchGoals.HammingOneSearch

private def step (pat : Bytes) (eq : W) (i : Nat) (st : W × W × List Nat) : W × W × List Nat :=
  let extend := (st.1 <<< 1) ||| 1
  let one' := (((st.2.1 <<< 1) ||| 1) &&& eq) ||| extend
  (extend &&& eq, one',
    if one'.getLsbD (pat.length - 1) then (i + 1 - pat.length) :: st.2.2 else st.2.2)

private def loop (pat text : Bytes) : W × W × List Nat :=
  (text.zipIdx).foldl (fun st p => step pat (eqMask pat p.1) p.2 st) (0, 0, [])

private theorem id_pure' {α : Type} (x : α) : (pure x : Id α) = x := rfl
private theorem id_bind' {α β : Type} (x : α) (f : α → Id β) : (x >>= f : Id β) = f x := rfl
private theorem id_map' {α β : Type} (f : α → β) (x : α) : (f <$> x : Id β) = f x := rfl
private theorem idrun' {α : Type} (x : Id α) : Id.run x = x := rfl

private theorem search_eq_loop (pat text : Bytes) (hne : pat ≠ []) :
    search pat text = (loop pat text).2.2.reverse := by
  unfold search
  rw [if_neg (by cases pat <;> simp_all)]
  simp only [id_bind', id_map', idrun']
  set_option pp.all true in
  trace_state

end TinylambOpt.ResearchProofs.ProbeTmp
