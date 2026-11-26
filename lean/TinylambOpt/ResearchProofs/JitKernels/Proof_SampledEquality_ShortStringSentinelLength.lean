import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "omit the length from a padded first-word comparison for short strings whose
last byte is nonzero": for `|a| ≤ 4` the padded word `(a ++ 0⁴).take 4` is exactly
`a ++ 0^(4-|a|)`. If the padded words of `a` and `b` agree and `|a| < |b|`, then
`b = (take |b| a) ++ 0^(|b|-|a|)` ends in a zero byte, contradicting the sentinel
`b.getLast? ≠ some 0`; the case `|b| < |a|` is symmetric, so the lengths are equal and
taking the common prefix of the padded words yields `a = b`. This lets a short-string
comparison skip a separate length check when every key carries a nonzero last byte.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ShortStringSentinelLength
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

private theorem pad_eq (x : Bytes) (hx : x.length ≤ 4) :
    (x ++ List.replicate 4 0).take 4 = x ++ List.replicate (4 - x.length) 0 := by
  rw [List.take_append, List.take_of_length_le hx, List.take_replicate,
    Nat.min_eq_left (by omega)]

theorem sound : ShortStringSentinelLength.Goal := by
  intro a b ha hb hga hgb h
  rw [pad_eq a ha, pad_eq b hb] at h
  rcases Nat.lt_trichotomy a.length b.length with hlt | heq | hgt
  · -- `a` shorter: then `b` ends in a padding zero
    have hb0 : b.length - a.length ≠ 0 := by omega
    have hmin : b.length - a.length ≤ 4 - a.length := by omega
    have this := congrArg (fun l => l.take b.length) h
    have h1 : List.take b.length (a ++ List.replicate (4 - a.length) 0)
        = a.take b.length ++ List.replicate (b.length - a.length) 0 := by
      rw [List.take_append, List.take_replicate, Nat.min_eq_left hmin]
    have h2 : List.take b.length (b ++ List.replicate (4 - b.length) 0) = b := by
      rw [List.take_append, List.take_of_length_le (Nat.le_refl _), Nat.sub_self,
        List.take_zero, List.append_nil]
    rw [h1, h2] at this
    have hbz : b.getLast? = some 0 := by
      rw [← this, List.getLast?_append, List.getLast?_replicate, if_neg hb0]
      simp
    exact absurd hbz hgb
  · -- equal lengths: compare the common prefix
    have this := congrArg (fun l => l.take a.length) h
    have h1 : List.take a.length (a ++ List.replicate (4 - a.length) 0) = a := by
      rw [List.take_append, List.take_of_length_le (Nat.le_refl _), Nat.sub_self,
        List.take_zero, List.append_nil]
    have h2 : List.take a.length (b ++ List.replicate (4 - b.length) 0) = b := by
      rw [List.take_append, List.take_of_length_le (by omega : b.length ≤ a.length),
        (by omega : a.length - b.length = 0), List.take_zero, List.append_nil]
    rw [h1, h2] at this
    exact this
  · -- `b` shorter: symmetric argument on `a`
    have ha0 : a.length - b.length ≠ 0 := by omega
    have hmin : a.length - b.length ≤ 4 - b.length := by omega
    have this := congrArg (fun l => l.take a.length) h
    have h1 : List.take a.length (a ++ List.replicate (4 - a.length) 0) = a := by
      rw [List.take_append, List.take_of_length_le (Nat.le_refl _), Nat.sub_self,
        List.take_zero, List.append_nil]
    have h2 : List.take a.length (b ++ List.replicate (4 - b.length) 0)
        = b.take a.length ++ List.replicate (a.length - b.length) 0 := by
      rw [List.take_append, List.take_of_length_le (by omega : b.length ≤ a.length),
        List.take_replicate, Nat.min_eq_left hmin]
    rw [h1, h2] at this
    have haz : a.getLast? = some 0 := by
      rw [this, List.getLast?_append, List.getLast?_replicate, if_neg ha0]
      simp
    exact absurd haz hga

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ShortStringSentinelLength
