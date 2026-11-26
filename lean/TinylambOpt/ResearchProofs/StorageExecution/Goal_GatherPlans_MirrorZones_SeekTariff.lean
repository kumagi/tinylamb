import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_SeekTariff

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.MirrorZones

private theorem pairwise_head?_le {α : Type} {le : α → α → Bool}
    (hrefl : ∀ x, le x x = true)
    {l : List α} {a b : α} (hpw : l.Pairwise (fun x y => le x y = true))
    (hhead : l.head? = some a) (hmem : b ∈ l) : le a b = true := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
    simp at hhead
    subst hhead
    rw [List.pairwise_cons] at hpw
    rcases List.mem_cons.mp hmem with rfl | hmem
    · exact hrefl _
    · exact hpw.1 b hmem

private theorem choose_mem_covers (f : Price) (xs : List Nat) (hne : covers (points xs) ≠ []) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  have hperm := List.mergeSort_perm (covers (points xs)) (fun a b => decide (cost f a ≤ cost f b))
  have hne' : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact hne this
  cases h : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) with
  | nil => exact absurd h hne'
  | cons hd tl =>
    have hmem : hd ∈ (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) := by
      simp [h]
    rw [List.mem_mergeSort] at hmem
    simpa [h] using hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) :
    cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil =>
    simp [h] at hmem'
  | cons hd tl =>
    simp only [List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair
      (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simp only [le, decide_eq_true_eq] at hle
    exact hle

private theorem covers_nonempty : (xs : List Nat) → covers xs ≠ []
  | [] => by simp [covers]
  | x :: xs => by
    have ih := covers_nonempty xs
    simp only [covers, ne_eq]
    intro h
    cases hxs : covers xs with
    | nil => exact ih hxs
    | cons p ps =>
      have : (match p with | [] => [[(x,x)]] | (_,b)::rest => [(x,x)::p,(x,b)::rest]) = [] := by
        have hall : ∀ ps' ∈ p :: ps,
            (match ps' with | [] => [[(x,x)]] | (_,b)::rest => [(x,x)::ps',(x,b)::rest]) = [] := by
          simp only [List.flatMap_eq_nil_iff] at h
          rw [hxs] at h
          exact h
        exact hall p (by simp)
      cases p with
      | nil => simp at this
      | cons hd tl => simp at this

private theorem cost_add_shift (d : Nat) (g : Price) (ps : List Extent) :
    cost (fun a b => d + g a b) ps = ps.length * d + cost g ps := by
  induction ps with
  | nil => simp [cost]
  | cons e ps ih =>
    rcases e with ⟨a, b⟩
    simp only [cost, List.map_cons, List.sum_cons, List.length_cons] at ih ⊢
    rw [Nat.succ_mul]
    omega

theorem sound_SeekTariff : MirrorZones.SeekTariff.Goal := by
  intro zone penalty xs a b hab
  let glo : Price := fun l r => a + price zone penalty l r
  let ghi : Price := fun l r => b + price zone penalty l r
  let d := b - a
  have hd : 0 < d := by omega
  have hfun : ghi = fun l r => d + glo l r := by
    funext l r
    simp only [ghi, glo, d]
    omega
  have hcost : ∀ ps, cost ghi ps = ps.length * d + cost glo ps := by
    intro ps
    rw [hfun]
    exact cost_add_shift d glo ps
  have hcov : covers (points xs) ≠ [] := covers_nonempty (points xs)
  have hmem_lo : choose glo xs ∈ covers (points xs) := choose_mem_covers glo xs hcov
  have hmem_hi : choose ghi xs ∈ covers (points xs) := choose_mem_covers ghi xs hcov
  have hle_hi := choose_le ghi xs (choose glo xs) hmem_lo
  have hle_lo := choose_le glo xs (choose ghi xs) hmem_hi
  rw [hcost (choose ghi xs), hcost (choose glo xs)] at hle_hi
  change (choose ghi xs).length ≤ (choose glo xs).length
  have hmul : (choose ghi xs).length * d ≤ (choose glo xs).length * d := by
    omega
  exact Nat.le_of_mul_le_mul_right hmul hd

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_MirrorZones_SeekTariff
