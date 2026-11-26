import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ResolutionDomainProductOffsetBound

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# ResolutionDomainProductOffsetBound

Distinct resolution pairs are contained in the Cartesian product of the two
observed marginal domains, so a product bound on those domains makes the
offset past 10000 empty. The executor can bound the dictionary before building
the pair list.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : (l : List α) → l.eraseDups.Nodup
  | [] => List.Pairwise.nil
  | x :: l => by
      rw [List.eraseDups_cons]
      refine List.Pairwise.cons (fun b hb => ?_) (nodup_eraseDups (l.filter (fun b => !b == x)))
      intro hxb
      have h2 := List.mem_filter.mp (List.mem_eraseDups.mp hb)
      rw [hxb] at h2
      simp at h2
  termination_by l => l.length
  decreasing_by
    all_goals
      refine Nat.lt_of_le_of_lt (List.length_filter_le ..) ?_
      exact Nat.lt_succ_self _

private theorem nodup_length_le_of_mem {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (hnd : l₁.Nodup) (hm : ∀ x ∈ l₁, x ∈ l₂) : l₁.length ≤ l₂.length := by
  induction l₁ generalizing l₂ with
  | nil => simp
  | cons a as ih =>
      rcases List.nodup_cons.mp hnd with ⟨haas, hasnd⟩
      have ha2 : a ∈ l₂ := hm a (by simp)
      have hasub : ∀ x ∈ as, x ∈ l₂.erase a := by
        intro x hx
        have hx2 : x ∈ l₂ := hm x (by simp [hx])
        have hxa : x ≠ a := by
          intro h
          apply haas
          simpa [h] using hx
        simpa [List.mem_erase_of_ne hxa] using hx2
      have ih' := ih hasnd hasub
      have hlen : (l₂.erase a).length = l₂.length - 1 := by
        rw [List.length_erase]
        simp [ha2]
      have hpos : 0 < l₂.length := by
        rw [List.length_pos_iff]
        intro h
        subst l₂
        simp at ha2
      simp only [List.length_cons]
      omega

private def pairProduct (as bs : List Nat) : List (Nat × Nat) :=
  match as with
  | [] => []
  | a :: as => bs.map (fun b => (a, b)) ++ pairProduct as bs

private theorem length_pairProduct (as bs : List Nat) :
    (pairProduct as bs).length = as.length * bs.length := by
  induction as with
  | nil => simp [pairProduct]
  | cons a as ih =>
      simp [pairProduct, ih, Nat.succ_mul]
      omega

private theorem mem_pairProduct (as bs : List Nat) {a b : Nat} (ha : a ∈ as) (hb : b ∈ bs) :
    (a, b) ∈ pairProduct as bs := by
  revert a b
  induction as with
  | nil =>
      intro a b ha hb
      simp at ha
  | cons x xs ih =>
      intro a b ha hb
      by_cases hax : a = x
      · subst a
        simp [pairProduct, hb]
      · have haxs : a ∈ xs := by simpa [hax] using ha
        simp [pairProduct]
        exact Or.inr (ih haxs hb)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ResolutionDomainProductOffsetBound.Goal := by
  intro dims h
  let fsts := (dims.map Prod.fst).eraseDups
  let snds := (dims.map Prod.snd).eraseDups
  have hmem : ∀ p ∈ dims.eraseDups, p ∈ pairProduct fsts snds := by
    intro p hp
    have hp_dims : p ∈ dims := List.mem_eraseDups.mp hp
    have hf : p.1 ∈ fsts := by
      dsimp [fsts]
      rw [List.mem_eraseDups]
      exact List.mem_map.mpr ⟨p, hp_dims, rfl⟩
    have hs : p.2 ∈ snds := by
      dsimp [snds]
      rw [List.mem_eraseDups]
      exact List.mem_map.mpr ⟨p, hp_dims, rfl⟩
    exact mem_pairProduct fsts snds hf hs
  have hlen := nodup_length_le_of_mem (nodup_eraseDups dims) hmem
  rw [length_pairProduct] at hlen
  dsimp [fsts, snds] at hlen h
  rw [List.drop_eq_nil_iff]
  omega
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_ResolutionDomainProductOffsetBound
