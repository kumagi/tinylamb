import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common_PackedKeys

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.SortProofs

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common

theorem orderBiasedReuse : OrderBiased.ReuseForSort.Goal := by
  apply reuseForSort_of OrderBiased.codec (by decide)
  intro k1 k2 A B C D hf1 hf2 hA hB hC hD
  unfold OrderBiased.codec at hf1 hf2
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
          cases hx1 : biased 32 x with
          | none => simp [hx1] at hf1
          | some xb =>
            cases hy1 : biased 32 y with
            | none => simp [hy1] at hf1
            | some yb =>
              simp [hx1, hy1] at hf1
              cases hx2 : biased 32 u with
              | none => simp [hx2] at hf2
              | some ub =>
                cases hy2 : biased 32 v with
                | none => simp [hy2] at hf2
                | some vb =>
                  simp [hx2, hy2] at hf2
                  rcases hf1 with ⟨rfl, rfl⟩
                  rcases hf2 with ⟨rfl, rfl⟩
                  obtain ⟨hux, hAeq⟩ := biased_some_eq hx1
                  obtain ⟨huy, hBeq⟩ := biased_some_eq hy1
                  obtain ⟨hux2, hCeq⟩ := biased_some_eq hx2
                  obtain ⟨huy2, hDeq⟩ := biased_some_eq hy2
                  simp [hAeq, hBeq, hCeq, hDeq] at hA hB hC hD
                  have hAint : ((x + 2147483648).toNat : Int) = x + 2147483648 := Int.toNat_of_nonneg hux
                  have hCint : ((u + 2147483648).toNat : Int) = u + 2147483648 := Int.toNat_of_nonneg hux2
                  have hBint : ((y + 2147483648).toNat : Int) = y + 2147483648 := Int.toNat_of_nonneg huy
                  have hDint : ((v + 2147483648).toNat : Int) = v + 2147483648 := Int.toNat_of_nonneg huy2
                  have hxbI : (xb : Int) = x + 2147483648 := by rw [hAeq]; exact hAint
                  have hubI : (ub : Int) = u + 2147483648 := by rw [hCeq]; exact hCint
                  have hybI : (yb : Int) = y + 2147483648 := by rw [hBeq]; exact hBint
                  have hvbI : (vb : Int) = v + 2147483648 := by rw [hDeq]; exact hDint
                  change decide (xb < ub ∨ xb = ub ∧ yb < vb) = lex (pair x y) (pair u v)
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

theorem correlatedDeltaReuse : CorrelatedDelta.ReuseForSort.Goal := by
  apply reuseForSort_of CorrelatedDelta.codec (by decide)
  intro k1 k2 A B C D hf1 hf2 hA hB hC hD
  unfold CorrelatedDelta.codec at hf1 hf2
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
          unfold biased at hf1 hf2
          cases hx1 : unsigned x with
          | none => simp [hx1] at hf1
          | some xa =>
            cases hy1 : unsigned (y - x + 32768) with
            | none => simp [hx1, hy1] at hf1
            | some ya =>
              simp [hx1, hy1, biased] at hf1
              cases hx2 : unsigned u with
              | none => simp [hx2] at hf2
              | some xb =>
                cases hy2 : unsigned (v - u + 32768) with
                | none => simp [hx2, hy2] at hf2
                | some yb =>
                  simp [hx2, hy2, biased] at hf2
                  rcases hf1 with ⟨rfl, rfl⟩
                  rcases hf2 with ⟨rfl, rfl⟩
                  have hxnon : 0 ≤ x := by unfold unsigned at hx1; by_cases h : x < 0 <;> simp [h] at hx1 <;> omega
                  have hunon : 0 ≤ u := by unfold unsigned at hx2; by_cases h : u < 0 <;> simp [h] at hx2 <;> omega
                  have hdy1non : 0 ≤ y - x + 32768 := by unfold unsigned at hy1; by_cases h : y - x + 2 ^ (16 - 1) < 0 <;> simp [h] at hy1 <;> omega
                  have hdy2non : 0 ≤ v - u + 32768 := by unfold unsigned at hy2; by_cases h : v - u + 2 ^ (16 - 1) < 0 <;> simp [h] at hy2 <;> omega
                  have hAint : (xa : Int) = x := by
                    have hxv : x.toNat = xa := by simpa [unsigned, (show ¬ x < 0 by omega)] using hx1
                    rw [← hxv]; exact Int.toNat_of_nonneg hxnon
                  have hCint : (xb : Int) = u := by
                    have huv : u.toNat = xb := by simpa [unsigned, (show ¬ u < 0 by omega)] using hx2
                    rw [← huv]; exact Int.toNat_of_nonneg hunon
                  have hBint : (ya : Int) = y - x + 32768 := by
                    have hh := by simpa [unsigned, (show ¬ y - x + 32768 < 0 by omega)] using hy1
                    have hv : (y - x + 32768).toNat = ya := hh
                    rw [← hv]; exact Int.toNat_of_nonneg hdy1non
                  have hDint : (yb : Int) = v - u + 32768 := by
                    have hh := by simpa [unsigned, (show ¬ v - u + 32768 < 0 by omega)] using hy2
                    have hv : (v - u + 32768).toNat = yb := hh
                    rw [← hv]; exact Int.toNat_of_nonneg hdy2non
                  change decide (xa < xb ∨ xa = xb ∧ ya < yb) = lex (pair x y) (pair u v)
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

theorem taggedNullReuse : TaggedNull.ReuseForSort.Goal := by
  apply reuseForSort_of TaggedNull.codec (by decide)
  intro k1 k2 A B C D hf1 hf2 hA hB hC hD
  rcases k1 with ⟨x1,p⟩
  rcases k2 with ⟨x2,q⟩
  cases x1 with
  | none => simp [TaggedNull.codec] at hf1
  | some x =>
    cases x2 with
    | none => simp [TaggedNull.codec] at hf2
    | some u =>
      unfold TaggedNull.codec at hf1 hf2
      simp at hf1 hf2
      cases hx1 : unsigned x with
      | none => simp [hx1] at hf1
      | some xa =>
        cases hx2 : unsigned u with
        | none => simp [hx2] at hf2
        | some xb =>
          rw [hx1] at hf1
          rw [hx2] at hf2
          simp at hf1 hf2
          cases p with
          | none =>
            simp at hf1
            rcases hf1 with ⟨rfl, rfl⟩
            cases q with
            | none =>
              simp at hf2
              rcases hf2 with ⟨rfl, rfl⟩
              -- A=xa,B=0,C=xb,D=0
              have hxnon : 0 ≤ x := by unfold unsigned at hx1; by_cases h : x < 0 <;> simp [h] at hx1 <;> omega
              have hunon : 0 ≤ u := by unfold unsigned at hx2; by_cases h : u < 0 <;> simp [h] at hx2 <;> omega
              have hAint : (xa : Int) = x := by
                have hv : x.toNat = xa := by simpa [unsigned, (show ¬ x < 0 by omega)] using hx1
                rw [← hv]; exact Int.toNat_of_nonneg hxnon
              have hCint : (xb : Int) = u := by
                have hv : u.toNat = xb := by simpa [unsigned, (show ¬ u < 0 by omega)] using hx2
                rw [← hv]; exact Int.toNat_of_nonneg hunon
              rw [lex_fst]
              apply decide_eq_decide.mpr
              constructor <;> intro hh <;> rcases hh with hlt | ⟨heq,hlt⟩ <;> simp [compare] at hlt ⊢ <;> omega
            | some v =>
              simp at hf2
              cases hy2 : unsigned v with
              | none => simp [hy2] at hf2
              | some yb =>
                rw [hy2] at hf2
                simp at hf2
                rcases hf2 with ⟨rfl, rfl⟩
                have hxnon : 0 ≤ x := by unfold unsigned at hx1; by_cases h : x < 0 <;> simp [h] at hx1 <;> omega
                have hunon : 0 ≤ u := by unfold unsigned at hx2; by_cases h : u < 0 <;> simp [h] at hx2 <;> omega
                have hvnon : 0 ≤ v := by unfold unsigned at hy2; by_cases h : v < 0 <;> simp [h] at hy2 <;> omega
                have hAint : (xa : Int) = x := by
                  have hv : x.toNat = xa := by simpa [unsigned, (show ¬ x < 0 by omega)] using hx1
                  rw [← hv]; exact Int.toNat_of_nonneg hxnon
                have hCint : (xb : Int) = u := by
                  have hv : u.toNat = xb := by simpa [unsigned, (show ¬ u < 0 by omega)] using hx2
                  rw [← hv]; exact Int.toNat_of_nonneg hunon
                rw [lex_fst]
                apply decide_eq_decide.mpr
                constructor <;> intro hh <;> rcases hh with hlt | ⟨heq,hlt⟩ <;> simp [compare] at hlt ⊢ <;> omega
          | some y =>
            simp at hf1
            cases hy1 : unsigned y with
            | none => simp [hy1] at hf1
            | some ya =>
              rw [hy1] at hf1
              simp at hf1
              rcases hf1 with ⟨rfl, rfl⟩
              cases q with
              | none =>
                simp at hf2
                rcases hf2 with ⟨rfl, rfl⟩
                have hxnon : 0 ≤ x := by unfold unsigned at hx1; by_cases h : x < 0 <;> simp [h] at hx1 <;> omega
                have hunon : 0 ≤ u := by unfold unsigned at hx2; by_cases h : u < 0 <;> simp [h] at hx2 <;> omega
                have hynon : 0 ≤ y := by unfold unsigned at hy1; by_cases h : y < 0 <;> simp [h] at hy1 <;> omega
                have hAint : (xa : Int) = x := by
                  have hv : x.toNat = xa := by simpa [unsigned, (show ¬ x < 0 by omega)] using hx1
                  rw [← hv]; exact Int.toNat_of_nonneg hxnon
                have hCint : (xb : Int) = u := by
                  have hv : u.toNat = xb := by simpa [unsigned, (show ¬ u < 0 by omega)] using hx2
                  rw [← hv]; exact Int.toNat_of_nonneg hunon
                rw [lex_fst]
                apply decide_eq_decide.mpr
                constructor <;> intro hh <;> rcases hh with hlt | ⟨heq,hlt⟩ <;> simp [compare] at hlt ⊢ <;> omega
              | some v =>
                simp at hf2
                cases hy2 : unsigned v with
                | none => simp [hy2] at hf2
                | some yb =>
                  rw [hy2] at hf2
                  simp at hf2
                  rcases hf2 with ⟨rfl, rfl⟩
                  have hxnon : 0 ≤ x := by unfold unsigned at hx1; by_cases h : x < 0 <;> simp [h] at hx1 <;> omega
                  have hunon : 0 ≤ u := by unfold unsigned at hx2; by_cases h : u < 0 <;> simp [h] at hx2 <;> omega
                  have hynon : 0 ≤ y := by unfold unsigned at hy1; by_cases h : y < 0 <;> simp [h] at hy1 <;> omega
                  have hvnon : 0 ≤ v := by unfold unsigned at hy2; by_cases h : v < 0 <;> simp [h] at hy2 <;> omega
                  have hAint : (xa : Int) = x := by
                    have hv : x.toNat = xa := by simpa [unsigned, (show ¬ x < 0 by omega)] using hx1
                    rw [← hv]; exact Int.toNat_of_nonneg hxnon
                  have hCint : (xb : Int) = u := by
                    have hv : u.toNat = xb := by simpa [unsigned, (show ¬ u < 0 by omega)] using hx2
                    rw [← hv]; exact Int.toNat_of_nonneg hunon
                  have hyaint : (ya : Int) = y := by
                    have hv : y.toNat = ya := by simpa [unsigned, (show ¬ y < 0 by omega)] using hy1
                    rw [← hv]; exact Int.toNat_of_nonneg hynon
                  have hybint : (yb : Int) = v := by
                    have hv : v.toNat = yb := by simpa [unsigned, (show ¬ v < 0 by omega)] using hy2
                    rw [← hv]; exact Int.toNat_of_nonneg hvnon
                  rw [lex_fst]
                  apply decide_eq_decide.mpr
                  constructor <;> intro hh <;> rcases hh with hlt | ⟨heq,hlt⟩ <;> simp [compare] at hlt ⊢ <;> omega

theorem scaledKeyReuse : ScaledKey.ReuseForSort.Goal := by
  apply reuseForSort_of ScaledKey.codec (by decide)
  intro k1 k2 A B C D hf1 hf2 hA hB hC hD
  unfold ScaledKey.codec at hf1 hf2
  rcases k1 with ⟨x1,y1⟩
  rcases k2 with ⟨x2,y2⟩
  cases x1 with
  | none => simp [ScaledKey.codec, pick] at hf1
  | some x =>
    cases y1 with
    | none => simp [ScaledKey.codec, pick] at hf1
    | some y =>
      cases x2 with
      | none => simp [ScaledKey.codec, pick] at hf2
      | some u =>
        cases y2 with
        | none => simp [ScaledKey.codec, pick] at hf2
        | some v =>
          simp [pick, pair] at hf1 hf2
          rcases hf1 with ⟨hmod1, hf1⟩
          rcases hf2 with ⟨hmod2, hf2⟩
          cases hqx : unsigned (x / 100) with
          | none => simp [hqx] at hf1
          | some xa =>
            rw [hqx] at hf1
            simp at hf1
            cases hqy : unsigned y with
            | none => simp [hqy] at hf1
            | some ya =>
              rw [hqy] at hf1
              simp at hf1
              rcases hf1 with ⟨rfl, rfl⟩
              cases hqu : unsigned (u / 100) with
              | none => simp [hqu] at hf2
              | some xb =>
                rw [hqu] at hf2
                simp at hf2
                cases hqv : unsigned v with
                | none => simp [hqv] at hf2
                | some yb =>
                  rw [hqv] at hf2
                  simp at hf2
                  rcases hf2 with ⟨rfl, rfl⟩
                  have hq1non : 0 ≤ x / 100 := by unfold unsigned at hqx; by_cases h : x / 100 < 0 <;> simp [h] at hqx <;> omega
                  have hq2non : 0 ≤ u / 100 := by unfold unsigned at hqu; by_cases h : u / 100 < 0 <;> simp [h] at hqu <;> omega
                  have hynon : 0 ≤ y := by unfold unsigned at hqy; by_cases h : y < 0 <;> simp [h] at hqy <;> omega
                  have hvnon : 0 ≤ v := by unfold unsigned at hqv; by_cases h : v < 0 <;> simp [h] at hqv <;> omega
                  have hdiv1 : x / 100 * 100 = x := by
                    have h := Int.ediv_mul_add_emod x 100
                    omega
                  have hdiv2 : u / 100 * 100 = u := by
                    have h := Int.ediv_mul_add_emod u 100
                    omega
                  have hxaint : (xa : Int) = x / 100 := by
                    have hv : (x / 100).toNat = xa := by simpa [unsigned, (show ¬ x / 100 < 0 by omega)] using hqx
                    rw [← hv]; exact Int.toNat_of_nonneg hq1non
                  have hxbint : (xb : Int) = u / 100 := by
                    have hv : (u / 100).toNat = xb := by simpa [unsigned, (show ¬ u / 100 < 0 by omega)] using hqu
                    rw [← hv]; exact Int.toNat_of_nonneg hq2non
                  have hyaint : (ya : Int) = y := by
                    have hv : y.toNat = ya := by simpa [unsigned, (show ¬ y < 0 by omega)] using hqy
                    rw [← hv]; exact Int.toNat_of_nonneg hynon
                  have hybint : (yb : Int) = v := by
                    have hv : v.toNat = yb := by simpa [unsigned, (show ¬ v < 0 by omega)] using hqv
                    rw [← hv]; exact Int.toNat_of_nonneg hvnon
                  change decide (xa < xb ∨ xa = xb ∧ ya < yb) = lex (pair x y) (pair u v)
                  rw [lex_pair]
                  apply decide_eq_decide.mpr
                  constructor <;> intro hh <;> rcases hh with hlt | ⟨heq,hlt⟩ <;> omega

theorem shardLocalReuse : ShardLocal.ReuseForSort.Goal := by
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

theorem scaledKeyStride : ScaledKey.Stride.Goal := by
  intro k hk
  have hxmod0 : (100 * (k : Int)) % 100 = 0 := by
    exact Int.emod_eq_zero_of_dvd (a := (100 : Int)) (b := 100 * (k : Int)) ⟨(k : Int), rfl⟩
  have hxmod : (100 * (k : Int)) % 100 == 0 := by simp [hxmod0]
  have hxdiv : (100 * (k : Int)) / 100 = (k : Int) := by omega
  have hknon : ¬ (k : Int) < 0 := by omega
  have hf : ScaledKey.codec.fields (pair (100 * (k : Int)) 0) = some (k, 0) := by
    unfold ScaledKey.codec
    simp [pick, pair, unsigned, hxmod, hxdiv, hknon]
  change encode ScaledKey.codec (pair (100 * (k : Int)) 0) = some (BitVec.ofNat 64 (k * 4294967296))
  rw [encode, hf]
  unfold ScaledKey.codec
  dsimp
  change k < 4294967296 at hk
  rw [ite_eq_left (by simp [hk])]

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.SortProofs
