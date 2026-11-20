import TinylambOpt.PhysicalJitFusion
import Std.Tactic

namespace TinylambOpt.DelimitedCaptureJit

def firstResult (f : α → Option β) : List α → Option β
  | [] => none
  | x :: xs => (f x).orElse (fun _ => firstResult f xs)

/-- Greedy optional literal. A failed later match still tries the skipped
    alternative for that prefix before moving to the next prefix. -/
theorem prefix_expansion (prefixes : List (List UInt8)) (literal : List UInt8)
    (finish : List UInt8 → Option β) :
    firstResult finish (prefixes.flatMap (fun p => [p ++ literal, p])) =
      firstResult (fun p => (finish (p ++ literal)).orElse (fun _ => finish p))
        prefixes := by
  induction prefixes with
  | nil => rfl
  | cons p ps ih =>
    simp only [List.flatMap_cons, List.cons_append, List.nil_append, firstResult, ih]
    cases finish (p ++ literal) <;> rfl

def beforeDelimiter (delimiter : UInt8) (bytes : List UInt8) : List UInt8 :=
  bytes.takeWhile (· != delimiter)

/-- The validated VM's negated byte class and the generated first-delimiter
    search consume the same run. The tail after the delimiter is irrelevant. -/
theorem first_delimiter_capture (delimiter : UInt8)
    (captured tail : List UInt8) (noDelimiter : delimiter ∉ captured) :
    beforeDelimiter delimiter (captured ++ delimiter :: tail) = captured := by
  induction captured with
  | nil => simp [beforeDelimiter]
  | cons b bs ih =>
    have h : b ≠ delimiter := by
      intro eq
      exact noDelimiter (by simp [eq])
    have rest : delimiter ∉ bs := fun mem => noDelimiter (List.mem_cons_of_mem _ mem)
    simp [beforeDelimiter] at ih
    simp [beforeDelimiter, h, ih rest]

/-- A delimiter at the start makes a positive-length capture fail, so that
    the next prefix alternative (or unchanged input) supplies the result. -/
theorem delimiter_at_start (delimiter : UInt8) (tail : List UInt8) :
    beforeDelimiter delimiter (delimiter :: tail) = [] := by
  simp [beforeDelimiter]

theorem packed_slice (start length : BitVec 32) :
    let packed := (start.zeroExtend 64 <<< 32) ||| length.zeroExtend 64
    (packed >>> 32).truncate 32 = start ∧ packed.truncate 32 = length := by
  bv_decide

end TinylambOpt.DelimitedCaptureJit
