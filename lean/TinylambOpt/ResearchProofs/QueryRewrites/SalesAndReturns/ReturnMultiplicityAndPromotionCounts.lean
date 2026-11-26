import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ReturnMultiplicityAndPromotionCounts

The modeled outer-join counts can be recovered from return multiplicity and promotion
presence.

The executor can update the counts without materializing one joined row for each
duplicate return.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ReturnMultiplicityAndPromotionCounts
theorem replicate_append_eq {α : Type} (n m : Nat) (a : α) :
    List.replicate n a ++ List.replicate m a = List.replicate (n+m) a := by
  induction n with
  | zero => simp
  | succ n ih => simp [ih, Nat.succ_add]

theorem flatMap_replicate_true (n k : Nat) :
    (List.replicate n true).flatMap (List.replicate k) = List.replicate (n*k) true := by
  induction n with
  | zero => simp
  | succ n ih =>
    simp only [List.replicate, List.flatMap_cons, ih]
    rw [replicate_append_eq, Nat.succ_mul, Nat.add_comm k (n*k)]

theorem flatMap_replicate_false (n k : Nat) :
    (List.replicate n false).flatMap (List.replicate k) = List.replicate (n*k) false := by
  induction n with
  | zero => simp
  | succ n ih =>
    simp only [List.replicate, List.flatMap_cons, ih]
    rw [replicate_append_eq, Nat.succ_mul, Nat.add_comm k (n*k)]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ReturnMultiplicityAndPromotionCounts.Goal := by
  intro promoCopies returnCopies
  dsimp
  by_cases h : promoCopies == 0
  · have hp : promoCopies = 0 := beq_iff_eq.mp h
    subst promoCopies
    simp [flatMap_replicate_false, List.count_replicate]
  · have hpne : promoCopies ≠ 0 := by
      intro he
      apply h
      simp [he]
    have hpos : 0 < promoCopies := Nat.pos_of_ne_zero hpne
    simp [h, flatMap_replicate_true, List.count_replicate]
    have hmax : max 1 promoCopies = promoCopies := Nat.max_eq_right (Nat.succ_le_iff.mpr hpos)
    simp [hmax]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ReturnMultiplicityAndPromotionCounts
