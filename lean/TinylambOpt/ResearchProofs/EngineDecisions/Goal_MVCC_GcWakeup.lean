import TinylambOpt.ResearchGoals.EngineDecisions.MVCC

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `GcWakeup` — After one sweep, watch the second version's timestamp and the last
  version's joint snapshot/durability fence. Revisit exactly when another sweep would
  release an entry, even when only WAL durability advances.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_GcWakeup

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.GcWakeup

private theorem filter_not_length (p : V → Bool) (l : List V) :
    (l.filter p).length + (l.filter fun x => !p x).length = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hp : p x <;> simp [hp, ih] <;> omega

private theorem first_le_last_ts (x : V) (ys : List V)
    (h : (x :: ys).Pairwise fun a b => a.ts < b.ts) :
    x.ts ≤ ((x :: ys).getLast?.map V.ts).getD 0 := by
  induction ys generalizing x with
  | nil => simp
  | cons y ys ih =>
      have htail : (y :: ys).Pairwise (fun a b : V => a.ts < b.ts) := (List.pairwise_cons.mp h).2
      have hxy : x.ts < y.ts := (List.pairwise_cons.mp h).1 y (by simp)
      have ih' := ih y htail
      simpa [List.getLast?_cons] using (show x.ts ≤ ((y :: ys).getLast?.map V.ts).getD 0 by omega)

private theorem filter_le_empty (h' : Nat) (x : V) (ys : List V)
    (h : (x :: ys).Pairwise fun a b => a.ts < b.ts) (hx : h' < x.ts) :
    (x :: ys).filter (fun v => v.ts ≤ h') = [] := by
  induction ys generalizing x with
  | nil => simp [hx]
  | cons y ys ih =>
      have htail : (y :: ys).Pairwise (fun a b : V => a.ts < b.ts) := (List.pairwise_cons.mp h).2
      have hxy : x.ts < y.ts := (List.pairwise_cons.mp h).1 y (by simp)
      have hy : h' < y.ts := by omega
      simp [hx, ih y htail hy]

private theorem pairwise_keep (l : List V) (h' : Nat)
    (h : l.Pairwise fun a b => a.ts < b.ts) :
    ((l.filter fun v => v.ts ≤ h').getLast?.toList ++
      (l.filter fun v => h' < v.ts)).Pairwise (fun a b => a.ts < b.ts) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hhead : ∀ a ∈ xs, x.ts < a.ts := (List.pairwise_cons.mp h).1
      have htail : xs.Pairwise (fun a b : V => a.ts < b.ts) := (List.pairwise_cons.mp h).2
      have ih' := ih htail
      by_cases hx : x.ts ≤ h'
      · have hnlt : ¬ h' < x.ts := by omega
        rw [show (x :: xs).filter (fun v => v.ts ≤ h') = x :: (xs.filter fun v => v.ts ≤ h') by simp [hx]]
        rw [show (x :: xs).filter (fun v => h' < v.ts) = (xs.filter fun v => h' < v.ts) by simp [hnlt]]
        cases hf : xs.filter (fun v => v.ts ≤ h') with
        | nil =>
            have hp1 : ∀ a ∈ xs.filter (fun v => h' < v.ts), x.ts < a.ts := by
              intro a ha
              exact hhead a (List.mem_filter.mp ha).1
            have hp2 : (xs.filter fun v => h' < v.ts).Pairwise (fun a b : V => a.ts < b.ts) :=
              List.Pairwise.filter _ htail
            simpa [hf] using (List.pairwise_cons.mpr ⟨hp1, hp2⟩)
        | cons z zs =>
            simpa [List.getLast?_cons, hf] using ih'
      · have hx' : h' < x.ts := by omega
        have hempty := filter_le_empty h' x xs h hx'
        have hgself : (xs.filter fun v => h' < v.ts) = xs := by
          apply List.filter_eq_self.mpr
          intro a ha
          have hh := hhead a ha
          simp [show h' < a.ts by omega]
        rw [show (x :: xs).filter (fun v => v.ts ≤ h') = [] by simpa [hx] using hempty]
        rw [show (x :: xs).filter (fun v => h' < v.ts) = x :: xs by simp [hx', hgself]]
        simp [List.pairwise_cons]
        exact ⟨hhead, htail⟩

private theorem pairwise_vacuum (chain : List V) (pending : Bool) (h d : Nat)
    (hchain : chain.Pairwise fun a b => a.ts < b.ts) :
    (vacuum chain pending h d).Pairwise (fun a b => a.ts < b.ts) := by
  unfold vacuum
  dsimp only
  by_cases hrem : (!pending && chain.getLast?.any fun v => v.ts ≤ h && v.lsn ≤ d) = true
  · rw [if_pos hrem]
    simp
  · rw [if_neg hrem]
    exact pairwise_keep chain h hchain

private theorem due_change (live : List V) (pending : Bool) (h' d' : Nat)
    (hlive : live.Pairwise fun a b => a.ts < b.ts) :
    due live pending h' d' = true ↔ vacuum live pending h' d' ≠ live := by
  cases live with
  | nil => simp [due, vacuum]
  | cons x xs =>
      cases xs with
      | nil =>
          by_cases hp : pending
          · by_cases ht : x.ts ≤ h' <;> simp [due, vacuum, hp, ht]
          · by_cases ht : x.ts ≤ h'
            · by_cases hd : x.lsn ≤ d' <;> simp [due, vacuum, hp, ht, hd]
            · simp [due, vacuum, hp, ht]
      | cons y ys =>
          have htail : (y :: ys).Pairwise (fun a b : V => a.ts < b.ts) := (List.pairwise_cons.mp hlive).2
          have hxy : x.ts < y.ts := (List.pairwise_cons.mp hlive).1 y (by simp)
          by_cases hy : y.ts ≤ h'
          · have hxle : x.ts ≤ h' := by omega
            have hdue : due (x :: y :: ys) pending h' d' = true := by simp [due, hy]
            rw [hdue]
            rw [eq_self_iff_true]
            simp only [true_iff]
            unfold vacuum
            dsimp only
            by_cases hrem : (!pending && (x :: y :: ys).getLast?.any (fun v => v.ts ≤ h' && v.lsn ≤ d')) = true
            · rw [if_pos hrem]
              simp
            · rw [if_neg hrem]
              have hf_eq : (x :: y :: ys).filter (fun v => v.ts ≤ h') =
                  x :: y :: (ys.filter fun v => v.ts ≤ h') := by simp [hxle, hy]
              have hnx : ¬ h' < x.ts := by omega
              have hny : ¬ h' < y.ts := by omega
              have hg_eq : (x :: y :: ys).filter (fun v => h' < v.ts) =
                  (ys.filter fun v => h' < v.ts) := by simp [hnx, hny]
              rw [hf_eq, hg_eq]
              intro heq
              have hlen := congrArg List.length heq
              simp [List.getLast?_cons] at hlen
              have hle := List.length_filter_le (fun v : V => h' < v.ts) ys
              omega
          · have hygt : h' < y.ts := by omega
            have htail_empty := filter_le_empty h' y ys htail hygt
            have hallgt : ∀ z ∈ y :: ys, h' < z.ts := by
              intro z hz
              have hnle := (List.filter_eq_nil_iff.mp htail_empty) z hz
              have hnle' : ¬ z.ts ≤ h' := by
                intro hle
                exact hnle (by simp [hle])
              omega
            have hno : (y :: ys).getLast?.any (fun v => v.ts ≤ h' && v.lsn ≤ d') = false := by
              cases hl : (y :: ys).getLast? with
              | none => simp [hl]
              | some z =>
                  have hzin : z ∈ y :: ys := by
                    have : z ∈ (y :: ys).getLast? := by rw [hl]; simp
                    exact List.mem_of_mem_getLast? this
                  have hzgt' : h' < z.ts := hallgt z hzin
                  have hnle : ¬ z.ts ≤ h' := Nat.not_le.mpr hzgt'
                  simp [hl, hnle]
            have hdue : due (x :: y :: ys) pending h' d' = false := by
              simp [due, hy, hno]
            rw [hdue]
            rw [Bool.false_eq_true]
            simp only [false_iff]
            unfold vacuum
            dsimp only
            rw [if_neg (by simp [hno])]
            have hg_tail : (y :: ys).filter (fun v => h' < v.ts) = y :: ys := by
              apply List.filter_eq_self.mpr
              intro z hz
              simpa [show h' < z.ts by exact hallgt z hz]
            by_cases hx : x.ts ≤ h'
            · rw [show (x :: y :: ys).filter (fun v => v.ts ≤ h') = [x] by simp [hx, htail_empty]]
              rw [show (x :: y :: ys).filter (fun v => h' < v.ts) = y :: ys by simp [show ¬ h' < x.ts by omega, hg_tail]]
              simp
            · have hxgt : h' < x.ts := by omega
              rw [show (x :: y :: ys).filter (fun v => v.ts ≤ h') = [] by simp [show ¬ x.ts ≤ h' by omega, htail_empty]]
              rw [show (x :: y :: ys).filter (fun v => h' < v.ts) = x :: y :: ys by simp [hxgt, hg_tail]]
              simp

theorem sound : Goal := by
  intro chain pending h d h' d' hchain hh hd
  let live := vacuum chain pending h d
  have hpair : live.Pairwise (fun a b => a.ts < b.ts) := by
    dsimp [live]
    exact pairwise_vacuum chain pending h d hchain
  exact due_change live pending h' d' hpair

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_GcWakeup

-- C++ references to this model or its proof.
-- transaction/gc_wakeup.hpp
-- transaction/transaction_manager.cpp
