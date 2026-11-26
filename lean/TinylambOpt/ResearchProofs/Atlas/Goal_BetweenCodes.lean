import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Wavelet.BetweenCodes` — `∀ (xs : Word) level lo hi, lo ≤ hi → xs.countP (fun x =>
  lo*2^level ≤ x && x < hi*2^level) = xs.countP (fun x => lo ≤ x/2^level && x/2^level
  < hi)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BetweenCodes

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Wavelet

/-- Brute-force sanity check on all words of length ≤ 3 over alphabet {0,1,2,3},
    all levels 0..3 and all lo,hi in 0..5 (returned `true`). -/
private def genWords (n : Nat) : List Word :=
  match n with
  | 0 => [[]]
  | k + 1 => List.flatMap (fun w => (List.range 4).map (fun x => w ++ [x])) (genWords k)

private def bruteBetweenCodes : Bool :=
  let check xs level lo hi :=
    if lo ≤ hi then
      xs.countP (fun x => lo * 2 ^ level ≤ x && x < hi * 2 ^ level) ==
        xs.countP (fun x => lo ≤ x / 2 ^ level && x / 2 ^ level < hi)
    else true
  let ws := List.flatMap genWords (List.range 4)
  List.foldl (fun acc xs => acc &&
    List.foldl (fun acc1 level => acc1 &&
      List.foldl (fun acc2 lo => acc2 &&
        List.foldl (fun acc3 hi => acc3 && check xs level lo hi) true (List.range 6)) true (List.range 6)) true (List.range 4)) true ws

#guard bruteBetweenCodes

/-- With the positive modulus `m = 2^level`, `Nat.le_div_iff_mul_le` and
    `Nat.div_lt_iff_lt_mul` turn each side of the counted predicate into the
    other, so `List.countP_congr` finishes pointwise. -/
theorem sound_BetweenCodes : BetweenCodes.Goal := by
  intro xs level lo hi hlohi
  have hp : 0 < 2 ^ level := Nat.pow_pos (by decide)
  apply List.countP_congr
  intro x hx
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · intro h
    exact ⟨(Nat.le_div_iff_mul_le hp).mpr h.1, (Nat.div_lt_iff_lt_mul hp).mpr h.2⟩
  · intro h
    exact ⟨(Nat.le_div_iff_mul_le hp).mp h.1, (Nat.div_lt_iff_lt_mul hp).mp h.2⟩

end TinylambOpt.ResearchProofs.Atlas.Goal_BetweenCodes
