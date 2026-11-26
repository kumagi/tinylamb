import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

/-!
After a certified (fault-free) prefix `a`, the first faulting position of the
fused chunk equals the first faulting position of `b` re-measured from the
post-prefix accumulator `s + a.sum`, shifted by `a.length`: the fused scan
lattice is `path a` followed (up to the duplicated boundary, which `tail`
removes) by the shifted scan of `b`, and the boundary value itself cannot
fault because it lies in `path a`. The proof splits `findIdx?` along the
append with `List.findIdx?_append`, drops the all-clean left part via
`List.findIdx?_eq_none_iff`, transfers the shifted scan with a map/congruence
argument, and re-bases the successor index.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_FailIndexAfterCleanPrefix
open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

private theorem foldl_sum (s : Int) (xs : List Int) :
    xs.foldl (·+·) s = s + xs.sum := by
  induction xs generalizing s with
  | nil => simp
  | cons a t ih => rw [List.foldl_cons, ih, List.sum_cons]; omega

private theorem add_sum_mem_scanl :
    ∀ (a : List Int) (s : Int), s + a.sum ∈ a.scanl (·+·) s := by
  intro a
  induction a with
  | nil => intro s; rw [List.sum_nil, Int.add_zero]; exact List.mem_singleton_self _
  | cons c t ih =>
    intro s
    rw [List.sum_cons, ← Int.add_assoc, List.scanl_cons]
    exact List.mem_cons.mpr (Or.inr (ih (s + c)))

private theorem path_append_form (a b : List Int) :
    path (a++b) = path a ++ (b.scanl (·+·) a.sum).tail := by
  show (a++b).scanl (·+·) 0 = _
  rw [List.scanl_append, foldl_sum, Int.zero_add, path]

private theorem path_shift (u : Int) :
    ∀ (t : List Int), t.scanl (·+·) u = (path t).map (u + ·) := by
  intro t
  induction t generalizing u with
  | nil => simp [path]
  | cons c s ih =>
    simp only [List.scanl_cons, path, Int.zero_add, Int.add_zero, List.map_cons]
    rw [ih (u + c), ih c, List.map_map, List.cons.injEq]
    refine ⟨rfl, congrArg (fun h => List.map h (path s))
      (funext (fun x => by simp only [Function.comp_apply]; omega))⟩

private theorem length_path (xs : List Int) : (path xs).length = xs.length + 1 := by
  unfold path
  rw [List.length_scanl]

private theorem findIdx?_map (g : Int → Int) :
    ∀ (l : List Int) (p : Int → Bool), (l.map g).findIdx? p =
      l.findIdx? (fun x => p (g x)) := by
  intro l
  induction l with
  | nil => intro p; rfl
  | cons x xs ih =>
    intro p
    rw [List.map_cons, List.findIdx?_cons, List.findIdx?_cons, ih]

theorem sound : FailIndexAfterCleanPrefix.Goal := by
  intro lo hi s a b hall
  have hnone : (path a).findIdx? (fun v => s + v < lo || hi < s + v) = none := by
    refine List.findIdx?_eq_none_iff.mpr (fun v hv => ?_)
    have h1 := List.all_eq_true.mp hall v hv
    rw [Bool.and_eq_true_iff] at h1
    obtain ⟨hle, hup⟩ := h1
    have hle := of_decide_eq_true hle
    have hup := of_decide_eq_true hup
    rw [Bool.or_eq_false_iff]
    exact ⟨decide_eq_false (by omega), decide_eq_false (by omega)⟩
  obtain ⟨rest, hrest⟩ : ∃ rest, b.scanl (·+·) a.sum = a.sum :: rest := by
    cases b with
    | nil => exact ⟨[], by rw [List.scanl_nil]⟩
    | cons c t => exact ⟨t.scanl (·+·) (a.sum + c), by rw [List.scanl_cons]⟩
  have hPb : (fun v => (s + v < lo || hi < s + v : Bool)) a.sum = false := by
    rw [Bool.or_eq_false_iff]
    have hsum : a.sum ∈ path a := by
      have h : (0 : Int) + a.sum ∈ a.scanl (·+·) 0 := add_sum_mem_scanl a 0
      simpa [path] using h
    have h1 := List.all_eq_true.mp hall a.sum hsum
    rw [Bool.and_eq_true_iff] at h1
    obtain ⟨hle, hup⟩ := h1
    have hleP := of_decide_eq_true hle
    have hupP := of_decide_eq_true hup
    refine ⟨decide_eq_false (by omega), decide_eq_false (by omega)⟩
  show (path (a++b)).findIdx? (fun v => s + v < lo || hi < s + v) =
      Option.map (fun i => a.length + i)
        ((path b).findIdx? (fun w => s + a.sum + w < lo || hi < s + a.sum + w))
  rw [path_append_form, hrest, List.tail_cons, List.findIdx?_append, hnone,
    length_path]
  -- key: shifting `rest` up by the dropped boundary re-bases successor indices
  have hkey : (rest.findIdx? (fun v => (s + v < lo || hi < s + v : Bool))).map (fun i => i + 1)
      = (path b).findIdx? (fun w => s + a.sum + w < lo || hi < s + a.sum + w) := by
    have h1 : (a.sum :: rest).findIdx? (fun v => (s + v < lo || hi < s + v : Bool))
        = (rest.findIdx? (fun v => (s + v < lo || hi < s + v : Bool))).map (fun i => i + 1) := by
      rw [List.findIdx?_cons, if_neg (by simpa using hPb)]
    have h2 : (a.sum :: rest).findIdx? (fun v => (s + v < lo || hi < s + v : Bool))
        = (b.scanl (·+·) a.sum).findIdx? (fun v => (s + v < lo || hi < s + v : Bool)) := by
      rw [hrest]
    have h3 : (b.scanl (·+·) a.sum).findIdx? (fun v => (s + v < lo || hi < s + v : Bool))
        = ((path b).map (fun v => a.sum + v)).findIdx? (fun v => (s + v < lo || hi < s + v : Bool)) := by
      rw [← path_shift a.sum]
    have h4 : ((path b).map (fun v => a.sum + v)).findIdx? (fun v => (s + v < lo || hi < s + v : Bool))
        = (path b).findIdx? (fun w => ((s + (a.sum + w) < lo || hi < s + (a.sum + w)) : Bool)) := by
      rw [findIdx?_map]
    have h5 : (fun w => ((s + (a.sum + w) < lo || hi < s + (a.sum + w)) : Bool))
        = (fun w => (s + a.sum + w < lo || hi < s + a.sum + w : Bool)) := by
      funext w
      rw [Int.add_assoc]
    rw [← h1, h2, h3, h4, h5]
  rw [show (fun i => i + (a.length + 1)) = ((fun j => a.length + j) ∘ fun i => i + 1) from
    funext (fun i => by simp only [Function.comp_apply]; omega), ← Option.map_map, hkey]
  rfl

end TinylambOpt.ResearchProofs.JitKernels.ExcursionGuards_FailIndexAfterCleanPrefix
