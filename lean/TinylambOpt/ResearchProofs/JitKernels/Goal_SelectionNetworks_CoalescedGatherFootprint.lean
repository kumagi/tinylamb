import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! For line-sorted requests the number of line transitions equals the number of distinct lines. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_CoalescedGatherFootprint

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CoalescedGatherFootprint

def changes (prev : Option Nat) : List Nat → Nat
  | [] => 0
  | a :: l => (if some a != prev then 1 else 0) + changes (some a) l

theorem countFrom_eq_changes (prev : Option Nat) (l : List Nat) :
    (l.zip (prev :: l.map some)).countP (fun p => some p.1 != p.2) = changes prev l := by
  induction l generalizing prev with
  | nil => simp [changes]
  | cons a l ih =>
      have hih : (List.zipWith Prod.mk l (some a :: List.map some l)).countP (fun p => some p.1 != p.2) = changes (some a) l := by
        simpa [List.zip] using ih (some a)
      simp [changes, List.zip, List.countP_cons, hih]
      omega

theorem filter_ne_of_lt (p : Nat) (l : List Nat) (h : ∀ x ∈ l, p < x) :
    l.filter (fun x => !(x == p)) = l := by
  rw [List.filter_eq_self]
  intro x hx
  have hlt := h x hx
  have hxp : x ≠ p := by omega
  simp [hxp]

theorem changes_some_eq (p : Nat) (l : List Nat)
    (hsorted : l.Pairwise (fun a b => a ≤ b)) (hge : ∀ x ∈ l, p ≤ x) :
    changes (some p) l = (l.filter (fun x => !(x == p))).eraseDups.length := by
  induction l generalizing p with
  | nil => simp [changes]
  | cons a t ih =>
      have hge_a : p ≤ a := hge a (by simp)
      have hge_t : ∀ x ∈ t, p ≤ x := by intro x hx; exact hge x (by simp [hx])
      cases hsorted with
      | cons hforall htail =>
          have hge_at : ∀ x ∈ t, a ≤ x := hforall
          by_cases heq : a == p
          · have hap : a = p := (beq_iff_eq.mp heq)
            subst a
            simp [changes]
            exact ih p htail hge_t
          · have hne : a ≠ p := by
              intro h
              exact heq (by simp [h])
            have hlt : p < a := by omega
            have hfilter_t : t.filter (fun x => !(x == p)) = t := filter_ne_of_lt p t (by
              intro x hx
              have := hge_at x hx
              omega)
            simp [changes, hne, hfilter_t, List.eraseDups_cons]
            have hrec := ih a htail hge_at
            omega

theorem changes_none_eq_eraseDups (l : List Nat) (hsorted : l.Pairwise (fun a b => a ≤ b)) :
    changes none l = l.eraseDups.length := by
  cases l with
  | nil => simp [changes]
  | cons a t =>
      have htail : t.Pairwise (fun a b => a ≤ b) := hsorted.tail
      have hge : ∀ x ∈ t, a ≤ x := by
        cases hsorted with
        | cons hforall htail' => exact hforall
      have hch := changes_some_eq a t htail hge
      simp [changes, hch, List.eraseDups_cons]
      omega

theorem pairwise_map_div (l : List Nat) (n : Nat)
    (h : l.Pairwise (fun a b => decide (a / n ≤ b / n) = true)) :
    (l.map (fun x => x / n)).Pairwise (fun a b => a ≤ b) := by
  induction h with
  | nil => simp
  | cons hforall htail ih =>
      simp only [List.map_cons]
      constructor
      · intro y hy
        rcases List.mem_map.mp hy with ⟨x, hx, rfl⟩
        have hx' := hforall x hx
        simpa using hx'
      · exact ih

theorem sound : TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.CoalescedGatherFootprint.Goal := by
  intro sel n hn
  have htrans : ∀ a b c : Nat, decide (a / n ≤ b / n) = true → decide (b / n ≤ c / n) = true → decide (a / n ≤ c / n) = true := by
    intro a b c hab hbc
    simp at hab hbc ⊢
    omega
  have htotal : ∀ a b : Nat, (decide (a / n ≤ b / n) || decide (b / n ≤ a / n)) = true := by
    intro a b
    by_cases h : a / n ≤ b / n
    · simp [h]
    · have h' : b / n ≤ a / n := by omega
      simp [h, h']
  have hpairTile : (tile sel n).Pairwise (fun a b => decide (a / n ≤ b / n) = true) := by
    simpa [tile] using List.pairwise_mergeSort htrans htotal sel
  have hpair : ((tile sel n).map (fun x => x / n)).Pairwise (fun a b => a ≤ b) :=
    pairwise_map_div (tile sel n) n hpairTile
  have hcount := countFrom_eq_changes none ((tile sel n).map (fun x => x / n))
  change (((tile sel n).map (fun x => x / n)).zip (none :: ((tile sel n).map (fun x => x / n)).map some)).countP
      (fun p => some p.1 != p.2) = ((tile sel n).map (fun x => x / n)).eraseDups.length
  rw [hcount]
  exact changes_none_eq_eraseDups ((tile sel n).map (fun x => x / n)) hpair

end TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_CoalescedGatherFootprint
