import TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

/-!
Proves `FiniteOverflowLaneReplay.Goal`: a finite scalar prefix may be skipped
when locating the first infinity on a delayed cold path. Formally, if no
prefix-sum accumulator of `a` (seeded with `s`) is infinite — i.e.
`firstInf s a = none` — then the first-infinity search over the concatenation
`a ++ b` equals the search re-seeded at the true fold total `total s a`, with
found indices shifted by `a.length`. The proof is pure list mechanics
(`List.scanl_append`, `List.findIdx?_append`, `List.findIdx?_cons`): the
dropped head of the re-seeded scan is exactly `total s a`, which is an
element of the prefix scan and hence not infinite, so the head test fails and
indices rebase by one. No IEEE-specific reasoning is required. This lets the
JIT kernel resume an infinity scan at a chunk boundary without replaying the
hot prefix, provided the prefix was already proven inf-free.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_FiniteOverflowLaneReplay
open TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

private theorem foldl_mem_scanl (l : List Float) (s : Float) :
    l.foldl (·+·) s ∈ l.scanl (·+·) s := by
  induction l generalizing s with
  | nil => rw [List.scanl_nil, List.foldl_nil]; exact List.mem_singleton_self _
  | cons x t ih => rw [List.scanl_cons, List.foldl_cons]; exact List.mem_cons_of_mem _ (ih (s + x))

theorem sound : FiniteOverflowLaneReplay.Goal := by
  intro s a b hnone
  have hfor := List.findIdx?_eq_none_iff.mp hnone
  have hacc : Float.isInf (a.foldl (·+·) s) = false := by
    simpa using hfor (a.foldl (·+·) s) (foldl_mem_scanl a s)
  have hnone' : List.findIdx? Float.isInf (List.scanl (·+·) s a) = none := hnone
  show ((a ++ b).scanl (·+·) s).findIdx? Float.isInf
      = ((b.scanl (·+·) (a.foldl (·+·) s)).findIdx? Float.isInf).map (fun i => a.length + i)
  rw [List.scanl_append, List.findIdx?_append, hnone', List.length_scanl, Option.none_or]
  show Option.map (fun i => i + (a.length + 1))
      (((b.scanl (·+·) (a.foldl (·+·) s)).tail).findIdx? Float.isInf)
      = ((b.scanl (·+·) (a.foldl (·+·) s)).findIdx? Float.isInf).map (fun i => a.length + i)
  cases b with
  | nil =>
    rw [List.scanl_nil, List.tail_cons, List.findIdx?_nil, List.findIdx?_cons,
      ite_eq_right (by simpa using hacc), List.findIdx?_nil]
    rfl
  | cons x t =>
    rw [List.scanl_cons, List.tail_cons, List.findIdx?_cons,
      ite_eq_right (by simpa using hacc), Option.map_map]
    refine congrArg (fun g => Option.map g
      (List.findIdx? Float.isInf (List.scanl (·+·) (a.foldl (·+·) s + x) t))) ?_
    funext i
    show i + (a.length + 1) = a.length + (i + 1)
    omega

end TinylambOpt.ResearchProofs.JitKernels.FloatingReduction_FiniteOverflowLaneReplay
