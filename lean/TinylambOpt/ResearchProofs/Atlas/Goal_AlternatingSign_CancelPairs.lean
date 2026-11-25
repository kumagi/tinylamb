import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingSign_CancelPairs

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem even_or_odd (n : Nat) : n % 2 = 0 ∨ n % 2 = 1 := by omega

private def altTerm (c : Config) (i : Nat) : Int :=
  (if i % 2 == 0 then (1 : Int) else -1) * (c.base + c.step * (i : Int))

private theorem rows_alt (c : Config) :
    (rows layout c).map Prod.snd = (List.range c.n).map (altTerm c) := by
  unfold rows
  simp [layout, altTerm]

private theorem alt_sum_closed (c : Config) : ∀ n : Nat,
    ((List.range n).map (altTerm c)).sum = -c.step * ((n / 2 : Nat) : Int) +
      if n % 2 == 1 then c.base + c.step * (n - 1 : Nat) else 0 := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      by_cases hn : n = 0
      · subst n
        simp [altTerm]
      · rcases even_or_odd n with he | ho
        · have hn2 : (n + 1) / 2 = n / 2 := by omega
          have hodd1 : (n + 1) % 2 = 1 := by omega
          rw [range_succ_map_sum, ih, hn2]
          simp [altTerm, he, hodd1]
        · have hn2 : (n + 1) / 2 = n / 2 + 1 := by omega
          have hodd0 : (n + 1) % 2 = 0 := by omega
          have hncast : ((n : Nat) : Int) = ((n - 1 : Nat) : Int) + 1 := by omega
          rw [range_succ_map_sum, ih, hn2]
          simp [altTerm, ho, hodd0, Int.natCast_add]
          rw [hncast]
          simp [Int.mul_add]
          omega

theorem sound_AlternatingSign_CancelPairs : AlternatingSign.CancelPairs.Goal := by
  intro c
  rw [rows_alt]
  exact alt_sum_closed c c.n

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingSign_CancelPairs
