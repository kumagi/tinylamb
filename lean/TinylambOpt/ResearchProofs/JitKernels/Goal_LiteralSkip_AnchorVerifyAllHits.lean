import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
Verified anchor candidates are exactly the true starts. A prefix match at offset i
necessarily leaves room for the whole needle and fixes every needle byte, so
filtering the anchor candidates by a full prefix check recovers `starts` without
losing a match. SQL execution may use one compiled byte to nominate candidates.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_AnchorVerifyAllHits

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem prefix_iff (p t : B) : p.isPrefixOf t = true ↔ ∃ u, p ++ u = t := by
  exact List.isPrefixOf_iff_prefix

theorem prefix_length_le (p s : B) (i : Nat) (hi : i ≤ s.length)
    (h : p.isPrefixOf (s.drop i) = true) : i + p.length ≤ s.length := by
  rcases (prefix_iff p (s.drop i)).mp h with ⟨u, hu⟩
  have hlen := congrArg List.length hu
  simp [List.length_drop] at hlen
  omega

theorem anchor_of_prefix (p s : B) (i j : Nat) (hj : j < p.length)
    (h : p.isPrefixOf (s.drop i) = true) : anchor p s j i = true := by
  rcases (prefix_iff p (s.drop i)).mp h with ⟨u, hu⟩
  have hget : s[i + j]? = p[j]? := by
    calc
      s[i + j]? = (s.drop i)[j]? := (List.getElem?_drop).symm
      _ = (p ++ u)[j]? := by rw [hu]
      _ = p[j]? := by
        rw [List.getElem?_append]
        simp [hj]
  unfold anchor
  simp [hget]

theorem filter_eq_of_pred (l : List Nat) (p q : Nat → Bool)
    (h : ∀ i ∈ l, p i = q i) : l.filter p = l.filter q := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.filter_cons, List.filter_cons, ih (by intro i hi; exact h i (by simp [hi]))]
      rw [h a (by simp)]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.AnchorVerifyAllHits.Goal := by
  intro p s j hj
  by_cases hp : p = []
  · subst p
    simp at hj
  · have hp_pos : 0 < p.length := by
      cases p with
      | nil => contradiction
      | cons a as => simp
    let N := s.length + 1
    let K := N - p.length
    have hKN : K ≤ N := Nat.sub_le _ _
    have hrange : List.range N = List.range K ++ (List.range N).drop K := by
      rw [← List.take_append_drop K (List.range N), List.take_range]
      simp [hKN]
    have htail : ∀ i ∈ (List.range N).drop K, p.isPrefixOf (s.drop i) = false := by
      intro i hi
      by_cases hpf' : p.isPrefixOf (s.drop i) = true
      · rcases List.mem_iff_getElem.mp hi with ⟨t, ht, hget⟩
        rw [List.getElem_drop] at hget
        have hlen_drop : (List.drop K (List.range N)).length = N - K := by simp
        rw [hlen_drop] at ht
        have hKtN : K + t < N := by omega
        have hlenN : (List.range N).length = N := by simp
        have hgetr : (List.range N)[K + t] = K + t :=
          List.getElem_range (by rw [hlenN]; exact hKtN)
        have hi_eq : i = K + t := by omega
        have hi_lt_N : i < N := by omega
        have hi_le_s : i ≤ s.length := by
          dsimp [N] at hi_lt_N
          omega
        have hlen := prefix_length_le p s i hi_le_s hpf'
        dsimp [K, N] at hi_eq
        omega
      · cases h : p.isPrefixOf (s.drop i) with
        | false => rfl
        | true => exact False.elim (hpf' h)
    have hstarts_small : (List.range N).filter (fun i => p.isPrefixOf (s.drop i)) =
        (List.range K).filter (fun i => p.isPrefixOf (s.drop i)) := by
      rw [hrange, List.filter_append]
      have htailnil : (List.drop K (List.range N)).filter
          (fun i => p.isPrefixOf (s.drop i)) = [] :=
        List.filter_eq_nil_iff.mpr (by
          intro i hi
          cases h : p.isPrefixOf (s.drop i) with
          | false => intro hf; cases hf
          | true => intro hf; have ht2 := htail i hi; rw [h] at ht2; contradiction)
      rw [htailnil]
      simp
    have hcand_eq : ((List.range K).filter (fun i => anchor p s j i)).filter
          (fun i => p.isPrefixOf (s.drop i)) =
        (List.range K).filter (fun i => p.isPrefixOf (s.drop i)) := by
      rw [List.filter_filter]
      apply filter_eq_of_pred
      intro i hi
      have hiN : i ∈ List.range N := by
        simp only [List.mem_range] at hi ⊢
        dsimp [K, N] at hi ⊢
        omega
      by_cases hpf : p.isPrefixOf (s.drop i) = true
      · have han := anchor_of_prefix p s i j hj hpf
        rw [han, hpf]
        rfl
      · cases h : p.isPrefixOf (s.drop i) with
        | false =>
            rfl
        | true => exact False.elim (hpf h)
    unfold starts cand
    dsimp [N, K] at hstarts_small hcand_eq
    rw [hstarts_small]
    exact hcand_eq

end TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_AnchorVerifyAllHits
