import TinylambOpt.ResearchGoals.DatabaseCorrectness.WinnerRecovery

/-! Per-key eligible edit order determines the recovered value.

If two permutations of the log present the same eligible after-images for a key, the last one wins in both. SQL recovery may reorder segment files or physical records arbitrarily as long as the reader of each key sees the same edit sequence. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.WinnerRecovery_ReorderedSegmentsPerKeyCriterion

open TinylambOpt.ResearchGoals.DatabaseCorrectness.WinnerRecovery
open TinylambOpt.ResearchGoals.RestartRaces.Recovery.DestroySummary (Rec Op owners)

private theorem winners_append (a b : List Entry) : winners (a ++ b) = winners a ++ winners b := by
  simp [winners, owners, List.map_append, List.filterMap_append]

private theorem getLast_cons_getD (a : Option Nat) (l : List (Option Nat)) (base : Option Nat) :
    ((a :: l).getLast?).getD base = (l.getLast?).getD a := by
  rw [List.getLast?_cons]
  cases hl : l.getLast? <;> simp [hl]

private theorem replay_fold_eq_last (known : List Nat) (k : Nat) : ∀ (log : List Entry) base,
    log.foldl (apply known k) base = ((edits known k log).getLast?).getD base := by
  intro log
  induction log with
  | nil => intro base; rfl
  | cons r rs ih =>
      intro base
      cases hr : r.action with
      | put key val =>
          by_cases h : key == k && known.contains r.owner
          · have h' := h
            simp only [Bool.and_eq_true] at h'
            have hkey : key = k := beq_iff_eq.mp h'.1
            have hcont : r.owner ∈ known := by simpa using h'.2
            simp only [List.foldl_cons]
            simp [apply, hr, hkey, hcont]
            rw [ih val]
            simp [edits, hr, hkey, hcont, getLast_cons_getD]
          · have h' := h
            simp only [Bool.and_eq_true] at h'
            have hfalse : ¬ (key = k ∧ r.owner ∈ known) := by
              intro hp
              exact h' ⟨by simp [hp.1], by simpa [hp.2]⟩
            simp [edits, apply, hr, hfalse, ih]
      | commit => simp [edits, apply, hr, ih]
      | abort => simp [edits, apply, hr, ih]
      | marker => simp [edits, apply, hr, ih]

private theorem getD_append (A B : List (Option Nat)) (base : Option Nat) :
    ((A ++ B).getLast?).getD base = (B.getLast?).getD ((A.getLast?).getD base) := by
  cases B <;> simp

private theorem mem_lt_max_getD_add_one (l : List Nat) (x : Nat) (hx : x ∈ l) :
    x < (l.max?.getD 0) + 1 := by
  cases l with
  | nil => simp at hx
  | cons a tl =>
      have hxle : x ≤ (a :: tl).max (by simp) := List.le_max_of_mem hx
      simp [List.max, List.max?] at hxle ⊢
      omega

private theorem replay_segments (W : List Nat) k : ∀ (segs : List (List Entry)) base,
    segs.foldl (fun s seg => seg.foldl (apply W k) s) base =
      ((segs.flatMap (edits W k)).getLast?).getD base := by
  intro segs
  induction segs with
  | nil => intro base; simp
  | cons seg segs ih =>
      intro base
      simp only [List.foldl_cons, List.flatMap_cons]
      rw [replay_fold_eq_last W k seg base]
      rw [ih (((edits W k seg).getLast?).getD base)]
      rw [getD_append]

private theorem winners_flatten (segs : List (List Entry)) :
    winners segs.flatten = segs.flatMap winners := by
  induction segs with
  | nil => simp [winners, owners]
  | cons seg segs ih =>
      simp [winners_append, ih]

private theorem edits_flatten (W : List Nat) k (segs : List (List Entry)) :
    segs.flatMap (edits W k) = edits W k segs.flatten := by
  induction segs with
  | nil => simp [edits]
  | cons seg segs ih => simp [edits, ih]

example : SegmentedWinnerReplayAgreement.Goal := by
  intro segs k base
  let W := segs.flatMap winners
  unfold replay
  rw [winners_flatten]
  rw [replay_fold_eq_last W k segs.flatten base]
  rw [replay_segments W k segs base]
  rw [edits_flatten]

private theorem winners_filter_owner (log : List Entry) (owner : Nat) (h : owner ∉ winners log) :
    winners (log.filter (fun r => r.owner != owner)) = winners log := by
  induction log with
  | nil => simp [winners, owners]
  | cons r rs ih =>
      by_cases hro : r.owner == owner
      · have heq : r.owner = owner := beq_iff_eq.mp hro
        have hncommit : r.action ≠ .commit := by
          intro hac
          apply h
          simp [winners, owners, header, hac, heq]
        have htail : owner ∉ winners rs := by
          have hw : winners (r :: rs) = winners rs := by simp [winners, owners, header, hncommit]
          simpa [hw] using h
        have hdrop : (r.owner != owner) = false := by simp [heq]
        rw [List.filter_cons]
        simp only [hdrop, Bool.false_eq_true, ↓reduceIte]
        rw [show winners (r :: rs) = winners rs by simp [winners, owners, header, hncommit]]
        exact ih htail
      · have hroP : r.owner ≠ owner := by intro heq; exact hro (by simp [heq])
        have hkeep : (r.owner != owner) = true := by simp [hroP]
        have htail : owner ∉ winners rs := by
          intro ht
          apply h
          cases hr : r.action with
          | commit =>
              have hw : winners (r :: rs) = r.owner :: winners rs := by simp [winners, owners, header, hr]
              rw [hw]
              exact List.mem_cons_of_mem _ ht
          | put =>
              have hw : winners (r :: rs) = winners rs := by simp [winners, owners, header, hr]
              rwa [hw]
          | abort =>
              have hw : winners (r :: rs) = winners rs := by simp [winners, owners, header, hr]
              rwa [hw]
          | marker =>
              have hw : winners (r :: rs) = winners rs := by simp [winners, owners, header, hr]
              rwa [hw]
        rw [List.filter_cons]
        simp only [hkeep, ↓reduceIte]
        cases hr : r.action with
        | commit =>
            simpa [winners, owners, header, hr] using congrArg (fun l => r.owner :: l) (ih htail)
        | put => simpa [winners, owners, header, hr] using ih htail
        | abort => simpa [winners, owners, header, hr] using ih htail
        | marker => simpa [winners, owners, header, hr] using ih htail

private theorem edits_filter_owner (known : List Nat) (k owner : Nat) (log : List Entry)
    (h : owner ∉ known) :
    edits known k (log.filter (fun r => r.owner != owner)) = edits known k log := by
  let f : Entry → Option (Option Nat) := fun r =>
    match r.action with
    | .put key v => if key == k && known.contains r.owner then some v else none
    | _ => none
  have hfun : ∀ r : Entry, (if (r.owner != owner) = true then f r else none) = f r := by
    intro r
    by_cases hro : r.owner == owner
    · have heq : r.owner = owner := beq_iff_eq.mp hro
      have hdrop : (r.owner != owner) = false := by simp [heq]
      have hf : f r = none := by
        cases hr : r.action with
        | put key val =>
            have hnot : ¬ ((key == k) = true ∧ known.contains r.owner = true) := by
              intro hp
              have hk : key = k := beq_iff_eq.mp hp.1
              have hc : r.owner ∈ known := by simpa using hp.2
              exact h (by simpa [heq] using hc)
            have hb : (key == k && known.contains r.owner) = false := by
              simpa [Bool.and_eq_true] using hnot
            have hnotP : ¬ (key = k ∧ r.owner ∈ known) := by
              intro hp
              exact hnot ⟨by simp [hp.1], by simpa [hp.2]⟩
            simp [f, hr, hnotP]
        | commit => simp [f, hr]
        | abort => simp [f, hr]
        | marker => simp [f, hr]
      simp [hdrop, hf]
    · have hroP : r.owner ≠ owner := by intro heq; exact hro (by simp [heq])
      have hkeep : (r.owner != owner) = true := by simp [hroP]
      simp [hkeep]
  change List.filterMap f (log.filter (fun r => r.owner != owner)) = List.filterMap f log
  rw [List.filterMap_filter]
  induction log with
  | nil => rfl
  | cons r rs ih =>
      rw [List.filterMap_cons, List.filterMap_cons, hfun r, ih]

private theorem edits_congr_of_owners (L : List Entry) (W1 W2 : List Nat) (k : Nat)
    (h : ∀ owner key v, Entry.mk owner (.put key v) ∈ L → (owner ∈ W1 ↔ owner ∈ W2)) :
    edits W1 k L = edits W2 k L := by
  let f : List Nat → Entry → Option (Option Nat) := fun W r =>
    match r.action with
    | .put key v => if key == k && W.contains r.owner then some v else none
    | _ => none
  have hfun : ∀ r ∈ L, f W1 r = f W2 r := by
    intro r hrL
    cases hr : r.action with
    | put key val =>
        have hh : Entry.mk r.owner (.put key val) = r := by
          cases r with
          | mk o a =>
              cases hr
              rfl
        have hmem : Entry.mk r.owner (.put key val) ∈ L := by
          rw [hh]
          exact hrL
        have hiff := h r.owner key val hmem
        by_cases hk : key = k
        · by_cases hw1 : r.owner ∈ W1
          · have hw2 : r.owner ∈ W2 := hiff.mp hw1
            simp [f, hr, hk, hw1, hw2]
          · have hw2 : ¬ r.owner ∈ W2 := by intro h2; exact hw1 (hiff.mpr h2)
            simp [f, hr, hk, hw1, hw2]
        · simp [f, hr, hk]
    | commit => simp [f, hr]
    | abort => simp [f, hr]
    | marker => simp [f, hr]
  change List.filterMap (f W1) L = List.filterMap (f W2) L
  induction L with
  | nil => rfl
  | cons r rs ih =>
      have htail : ∀ owner key v, Entry.mk owner (.put key v) ∈ rs → (owner ∈ W1 ↔ owner ∈ W2) := by
        intro o k' v hm
        apply h o k' v
        exact List.mem_cons_of_mem r hm
      have htailpred : ∀ (o k' : Nat) (v : Option Nat), Entry.mk o (.put k' v) ∈ rs → (o ∈ W1 ↔ o ∈ W2) := by
        intro o k' v hm
        apply h o k' v
        exact List.mem_cons_of_mem r hm
      have htailfun : ∀ x ∈ rs, f W1 x = f W2 x := by
        intro x hx
        apply hfun
        exact List.mem_cons_of_mem r hx
      rw [List.filterMap_cons, List.filterMap_cons, hfun r (by simp), ih htailpred htailfun]

theorem sound : ReorderedSegmentsPerKeyCriterion.Goal := by
  intro a b k base hperm hedits
  unfold replay
  rw [replay_fold_eq_last, replay_fold_eq_last]
  rw [hedits]
end TinylambOpt.ResearchProofs.DatabaseCorrectness.WinnerRecovery_ReorderedSegmentsPerKeyCriterion
