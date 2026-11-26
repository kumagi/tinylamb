import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
set_option linter.unusedSimpArgs false

/-! Two rows agreeing on the union of all guarded bits select the same first version; the proof shows each mask is contained in that union. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_PrecomputeGuardOnUniformRows

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.PrecomputeGuardOnUniformRows

theorem bool_false_of_not {p : Bool} (h : ¬ p) : p = false := by
  cases p <;> simp_all

theorem find_cons_of_false {α : Type} (p : α → Bool) (a : α) (l : List α) (ha : ¬ p a) :
    (a :: l).find? p = l.find? p := by
  simp [List.find?, bool_false_of_not ha]

theorem dispatch_cons_of_false (e : Entry) (l : List Entry) (w : Word) (he : ¬ accepts e w) :
    dispatch (e :: l) w = dispatch l w := by
  rw [dispatch, dispatch, find_cons_of_false (accepts · w) e l he]

theorem accepts_of_union (e : Entry) (a b m : Word) (he : e.mask &&& m = e.mask) (hab : a &&& m = b &&& m) :
    accepts e a ↔ accepts e b := by
  unfold accepts
  have hae : a &&& e.mask = b &&& e.mask := by
    ext i
    have h1 := congrArg (fun v : Word => v.getLsbD i) he
    have h2 := congrArg (fun v : Word => v.getLsbD i) hab
    simp only [BitVec.getLsbD_and] at h1 h2 ⊢
    cases hmk : e.mask.getLsbD i <;> simp_all
  rw [hae]

theorem foldl_or_mem (acc : Word) (l : List Word) (x : Word) (hx : x &&& acc = x ∨ x ∈ l) :
    x &&& l.foldl (fun p q => p ||| q) acc = x := by
  induction l generalizing acc with
  | nil =>
      simp at hx ⊢
      exact hx
  | cons y ys ih =>
      simp only [List.foldl_cons]
      apply ih (acc ||| y)
      rcases hx with hx | hx
      · left
        ext i
        have hxi := congrArg (fun v : Word => v.getLsbD i) hx
        simp only [BitVec.getLsbD_and] at hxi ⊢
        cases xb : x.getLsbD i <;> simp_all
      · simp only [List.mem_cons] at hx
        rcases hx with rfl | hx
        · left
          ext i
          cases x.getLsbD i <;> simp_all
        · right
          exact hx

theorem dispatch_union (es : List Entry) (a b m : Word)
    (hm : ∀ e ∈ es, e.mask &&& m = e.mask) (hab : a &&& m = b &&& m) :
    dispatch es a = dispatch es b := by
  induction es with
  | nil => simp [dispatch]
  | cons e es ih =>
      have he : e.mask &&& m = e.mask := hm e (by simp)
      have hes : ∀ f ∈ es, f.mask &&& m = f.mask := by
        intro f hf
        exact hm f (by simp [hf])
      have hiff : accepts e a ↔ accepts e b := accepts_of_union e a b m he hab
      by_cases heacc : accepts e a
      · have heaccb : accepts e b := hiff.mp heacc
        rw [show dispatch (e :: es) a = some e.code by simp [dispatch, heacc]]
        rw [show dispatch (e :: es) b = some e.code by simp [dispatch, heaccb]]
      · have heaccb : ¬ accepts e b := by
          intro hb
          exact heacc (hiff.mpr hb)
        rw [dispatch_cons_of_false e es a heacc]
        rw [dispatch_cons_of_false e es b heaccb]
        exact ih hes

theorem sound : PrecomputeGuardOnUniformRows.Goal := by
  intro es a b
  let m := (es.map Entry.mask).foldl (fun p q => p ||| q) 0
  intro hab
  apply dispatch_union es a b m
  intro e he
  apply foldl_or_mem 0 (es.map Entry.mask) e.mask
  right
  exact List.mem_map.mpr ⟨e, he, rfl⟩

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_PrecomputeGuardOnUniformRows
