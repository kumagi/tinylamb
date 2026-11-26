import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
A match owned by a chunk consumes at most needle-length-minus-one bytes beyond
the chunk boundary because the first matching byte is inside the chunk. Starting
positions before that boundary depend only on the chunk and that halo, so chunk
local literal search can be specialized without losing cross-boundary matches.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_LiteralChunkHaloOwnership

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem prefix_iff (p t : B) : p.isPrefixOf t = true ↔ ∃ u, p ++ u = t := by
  exact List.isPrefixOf_iff_prefix

theorem prefix_take_iff (p t : B) : p.isPrefixOf t = true ↔ t.take p.length = p := by
  rw [prefix_iff]
  constructor
  · rintro ⟨u, hu⟩
    rw [← hu, List.take_left]
  · intro h
    refine ⟨t.drop p.length, ?_⟩
    calc
      p ++ t.drop p.length = t.take p.length ++ t.drop p.length := by rw [h]
      _ = t := List.take_append_drop p.length t

theorem take_halo : ∀ (A b : B) (n h : Nat), 0 < A.length → n ≤ h + 1 →
    (A ++ b).take n = (A ++ b.take h).take n
  | [], b, n, h, hA, hle => by simp at hA
  | [x], b, 0, h, hA, hle => by simp
  | [x], b, n + 1, h, hA, hle => by
      have hnle : n ≤ h := by omega
      simp [List.take_succ_cons, List.take_take, Nat.min_eq_left hnle]
  | x :: y :: A, b, 0, h, hA, hle => by simp
  | x :: y :: A, b, n + 1, h, hA, hle => by
      have hrec := take_halo (y :: A) b n h (by simp) (by omega)
      simpa [List.cons_append, List.take_succ_cons] using
        congrArg (fun l : B => x :: l) hrec

theorem prefix_halo (p a b : B) (hp : p ≠ []) (i : Nat) (hi : i < a.length) :
    p.isPrefixOf ((a ++ b).drop i) = p.isPrefixOf ((a ++ b.take (p.length - 1)).drop i) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [prefix_take_iff] at h ⊢
    have hle : i ≤ a.length := by omega
    rw [List.drop_append] at h ⊢
    simp only [Nat.sub_eq_zero_of_le hle, List.drop_zero] at h ⊢
    have hE := take_halo (a.drop i) b p.length (p.length - 1)
      (by simp [List.length_drop]; omega) (by
        have hp_pos : 0 < p.length := by
          cases p with
          | nil => contradiction
          | cons x xs => simp
        omega)
    rw [← hE]
    exact h
  · intro h
    rw [prefix_take_iff] at h ⊢
    have hle : i ≤ a.length := by omega
    rw [List.drop_append] at h ⊢
    simp only [Nat.sub_eq_zero_of_le hle, List.drop_zero] at h ⊢
    have hE := take_halo (a.drop i) b p.length (p.length - 1)
      (by simp [List.length_drop]; omega) (by
        have hp_pos : 0 < p.length := by
          cases p with
          | nil => contradiction
          | cons x xs => simp
        omega)
    rw [hE]
    exact h

theorem filter_eq_of_pred (l : List Nat) (p q : Nat → Bool)
    (h : ∀ i ∈ l, p i = q i) : l.filter p = l.filter q := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.filter_cons, List.filter_cons, ih (by intro i hi; exact h i (by simp [hi]))]
      rw [h a (by simp)]


theorem filter_range_eq_small (N K : Nat) (hKN : K ≤ N) (f : Nat → Bool)
    (hf : ∀ i, K ≤ i → i < N → f i = false) :
    (List.range N).filter f = (List.range K).filter f := by
  have hrange : List.range N = List.range K ++ (List.range N).drop K := by
    rw [← List.take_append_drop K (List.range N), List.take_range]
    simp [hKN]
  rw [hrange, List.filter_append]
  have hnil : (List.drop K (List.range N)).filter f = [] := by
    rw [List.filter_eq_nil_iff]
    intro i hi
    rcases List.mem_iff_getElem.mp hi with ⟨t, ht, hget⟩
    rw [List.getElem_drop] at hget
    have hlen_drop : (List.drop K (List.range N)).length = N - K := by simp
    rw [hlen_drop] at ht
    have hKtN : K + t < N := by omega
    have hlenN : (List.range N).length = N := by simp
    have hgetr : (List.range N)[K + t] = K + t :=
      List.getElem_range (by rw [hlenN]; exact hKtN)
    have hi_eq : i = K + t := by omega
    have hiN : i < N := by omega
    have hfalse : f i = false := by
      cases hfi : f i with
      | false => rfl
      | true =>
          have hf_false := hf i (by omega) hiN
          rw [hfi] at hf_false
          contradiction
    intro htrue
    rw [hfalse] at htrue
    contradiction
  rw [hnil]
  simp

theorem sound : LiteralChunkHaloOwnership.Goal := by
  intro p a b hp
  unfold starts
  rw [List.filter_filter, List.filter_filter]
  let N1 := (a ++ b).length + 1
  let N2 := (a ++ b.take (p.length - 1)).length + 1
  let K := a.length
  have hK1 : K ≤ N1 := by simp [K, N1, List.length_append]; omega
  have hK2 : K ≤ N2 := by simp [K, N2, List.length_append]; omega
  rw [filter_range_eq_small N1 K hK1]
  · rw [filter_range_eq_small N2 K hK2]
    · apply filter_eq_of_pred
      intro i hi
      have hlt : i < a.length := by
        simpa [K] using hi
      have hph := prefix_halo p a b hp i hlt
      rw [hph]
    · intro i hKi hiN
      have hnot : ¬ i < a.length := by omega
      simp [hnot]
  · intro i hKi hiN
    have hnot : ¬ i < a.length := by omega
    simp [hnot]

end TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_LiteralChunkHaloOwnership
