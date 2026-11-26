import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific_PackedKeys
import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Refutations_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Proved

- `ShardLocal.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ShardLocal_ReuseForSort

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys.ShardLocal
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Specific

theorem sound : ShardLocal.ReuseForSort.Goal := by
  apply reuseForSort_of ShardLocal.codec (by decide)
  intro k1 k2 A B C D hf1 hf2 hA hB hC hD
  unfold ShardLocal.codec at hf1 hf2
  simp [pick] at hf1 hf2
  rcases k1 with ⟨x1,y1⟩
  rcases k2 with ⟨x2,y2⟩
  cases x1 with
  | none => simp at hf1
  | some x =>
    cases y1 with
    | none => simp at hf1
    | some y =>
      cases x2 with
      | none => simp at hf2
      | some u =>
        cases y2 with
        | none => simp at hf2
        | some v =>
          simp [pair] at hf1 hf2
          cases hux : unsigned x with
          | none => simp [hux] at hf1
          | some xa =>
            rw [hux] at hf1
            simp at hf1
            cases huy : unsigned y with
            | none => simp [huy] at hf1
            | some ya =>
              rw [huy] at hf1
              simp at hf1
              by_cases hsh1 : xa / 65536 == ya / 65536
              · rcases hf1 with ⟨hsh1p, rfl, rfl⟩
                cases hux2 : unsigned u with
                | none => simp [hux2] at hf2
                | some xb =>
                  rw [hux2] at hf2
                  simp at hf2
                  cases huy2 : unsigned v with
                  | none => simp [huy2] at hf2
                  | some yb =>
                    rw [huy2] at hf2
                    simp at hf2
                    by_cases hsh2 : xb / 65536 == yb / 65536
                    · rcases hf2 with ⟨hsh2p, rfl, rfl⟩
                      have hxnon : 0 ≤ x := by unfold unsigned at hux; by_cases h : x < 0 <;> simp [h] at hux <;> omega
                      have hunon : 0 ≤ u := by unfold unsigned at hux2; by_cases h : u < 0 <;> simp [h] at hux2 <;> omega
                      have hxaI : (xa : Int) = x := by
                        have hv : x.toNat = xa := by simpa [unsigned, (show ¬ x < 0 by omega)] using hux
                        rw [← hv]; exact Int.toNat_of_nonneg hxnon
                      have hxbI : (xb : Int) = u := by
                        have hv : u.toNat = xb := by simpa [unsigned, (show ¬ u < 0 by omega)] using hux2
                        rw [← hv]; exact Int.toNat_of_nonneg hunon
                      have hynon : 0 ≤ y := by unfold unsigned at huy; by_cases h : y < 0 <;> simp [h] at huy <;> omega
                      have hvnon : 0 ≤ v := by unfold unsigned at huy2; by_cases h : v < 0 <;> simp [h] at huy2 <;> omega
                      have hyaI : (ya : Int) = y := by
                        have hv : y.toNat = ya := by simpa [unsigned, (show ¬ y < 0 by omega)] using huy
                        rw [← hv]; exact Int.toNat_of_nonneg hynon
                      have hybI : (yb : Int) = v := by
                        have hv : v.toNat = yb := by simpa [unsigned, (show ¬ v < 0 by omega)] using huy2
                        rw [← hv]; exact Int.toNat_of_nonneg hvnon
                      have hsh1p : xa / 65536 = ya / 65536 := of_decide_eq_true hsh1
                      have hsh2p : xb / 65536 = yb / 65536 := of_decide_eq_true hsh2
                      have hvdecomp : ya / 65536 * 65536 + ya % 65536 = ya := by
                        rw [Nat.mul_comm]
                        exact Nat.div_add_mod ya 65536
                      have hv2decomp : yb / 65536 * 65536 + yb % 65536 = yb := by
                        rw [Nat.mul_comm]
                        exact Nat.div_add_mod yb 65536
                      change decide (xa < xb ∨ xa = xb ∧ ya % 65536 < yb % 65536) = lex (pair x y) (pair u v)
                      rw [lex_pair]
                      apply decide_eq_decide.mpr
                      constructor
                      · intro hh
                        rcases hh with hlt | ⟨heq, hlt⟩
                        · left; omega
                        · right; constructor <;> omega
                      · intro hh
                        rcases hh with hlt | ⟨heq, hlt⟩
                        · left; omega
                        · right; constructor <;> omega
                    · rcases hf2 with ⟨hsh2p, _, _⟩
                      have hb : (xb / 65536 == yb / 65536) = true := by simp [hsh2p]
                      exact False.elim (hsh2 hb)
              · rcases hf1 with ⟨hsh1p, _, _⟩
                have hb : (xa / 65536 == ya / 65536) = true := by simp [hsh1p]
                exact False.elim (hsh1 hb)

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Goal_ShardLocal_ReuseForSort
