import TinylambOpt.ResearchGoals.EngineDecisions.MVCC

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_ChainSummary

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.ChainSummary

private def aux {α : Type} [BEq α] (f : Version → α) (prev : α) : List Version → List Version
  | [] => []
  | v :: vs => (if f v == prev then [] else [v]) ++ aux f (f v) vs

private theorem changes_eq_aux {α : Type} [BEq α] [LawfulBEq α] (f : Version → α) :
    ∀ (a : Version) (l : List Version), changes f (a :: l) = a :: aux f (f a) l := by
  intro a l
  induction l generalizing a with
  | nil => simp [changes, aux]
  | cons b bs ih =>
      have ihb := ih b
      simp [changes, aux] at ihb ⊢
      by_cases hfab : f a == f b
      · have hEq : f a = f b := by simpa using hfab
        simp [hEq, hfab, ihb]
      · have hEq : f b ≠ f a := by
          intro h
          apply hfab
          simpa [h]
        simp [hfab, hEq, ihb]

private theorem aux_mem {α : Type} [BEq α] (f : Version → α) (prev : α) :
    ∀ {l v}, v ∈ aux f prev l → v ∈ l := by
  intro l
  induction l generalizing prev with
  | nil => simp [aux]
  | cons b bs ih =>
      intro v hv
      simp [aux] at hv
      rcases hv with hv | hv
      · simp [hv]
      · exact List.mem_cons_of_mem b (ih (f b) hv)

private theorem aux_tail_gt {α : Type} [BEq α] (f : Version → α) (prev : α)
    (t : Nat) (l : List Version) (hall : ∀ v ∈ l, t < v.ts) :
    (aux f prev l).filter (fun v => v.ts ≤ t) = [] := by
  apply List.filter_eq_nil_iff.mpr
  intro v hv
  have hvmem := aux_mem f prev hv
  have hgt := hall v hvmem
  simp
  omega

private theorem changes_last_map {α : Type} [BEq α] [LawfulBEq α]
    (f : Version → α) (a : Version) (l : List Version) (t : Nat)
    (hpair : (a :: l).Pairwise fun x y => x.ts < y.ts) :
    ((changes f (a :: l)).filter fun v => v.ts ≤ t).getLast?.map f =
      ((a :: l).filter fun v => v.ts ≤ t).getLast?.map f := by
  induction l generalizing a with
  | nil => simp [changes, aux]
  | cons b bs ih =>
      have htail : (b :: bs).Pairwise (fun x y : Version => x.ts < y.ts) := (List.pairwise_cons.mp hpair).2
      have hab : a.ts < b.ts := (List.pairwise_cons.mp hpair).1 b (by simp)
      have ih' := ih b htail
      rw [changes_eq_aux f a (b :: bs)]
      rw [changes_eq_aux f b bs] at ih'
      by_cases hbt : b.ts ≤ t
      · have hat : a.ts ≤ t := by omega
        rw [show aux f (f a) (b :: bs) = (if f b == f a then [] else [b]) ++ aux f (f b) bs by rfl]
        cases hAf : (aux f (f b) bs).filter (fun v => v.ts ≤ t) with
        | nil =>
            have hleft : ((b :: aux f (f b) bs).filter fun v => v.ts ≤ t) = [b] := by
              simp [hbt, hAf]
            have ihb : some (f b) = ((b :: bs).filter fun v => v.ts ≤ t).getLast?.map f := by
              rw [hleft] at ih'
              simpa using ih'
            by_cases hfab : f b == f a
            · have hfab' : f b = f a := by simpa using hfab
              rw [hfab'] at ihb
              simpa [hat, hbt, hfab, hAf] using ihb
            · simpa [hat, hbt, hfab, hAf] using ihb
        | cons z zs =>
            have ihb := ih'
            by_cases hfab : f b == f a
            · have hfab' : f b = f a := by simpa using hfab
              rw [← hfab']
              simpa [hat, hbt, hfab, hAf, List.getLast?_cons] using ihb
            · simpa [hat, hbt, hfab, hAf, List.getLast?_cons] using ihb
      · have hbgt : t < b.ts := by omega
        have hall : ∀ v ∈ b :: bs, t < v.ts := by
          intro v hv
          rcases List.mem_cons.mp hv with rfl | hv
          · exact hbgt
          · have hbv : b.ts < v.ts := (List.pairwise_cons.mp htail).1 v hv
            omega
        have hAgt := aux_tail_gt f (f a) t (b :: bs) hall
        have hbsgt : (b :: bs).filter (fun v => v.ts ≤ t) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro v hv
          have hgt := hall v hv
          simp
          omega
        by_cases hat : a.ts ≤ t <;> simp [hat, hAgt, hbsgt]

private theorem any_lt_last (x : Version) (xs : List Version) (snap : Nat)
    (h : (x :: xs).Pairwise fun a b => a.ts < b.ts) :
    ((x :: xs).any fun v => snap < v.ts) = true ↔
      snap < ((x :: xs).getLast?.map Version.ts).getD 0 := by
  induction xs generalizing x with
  | nil => simp
  | cons y ys ih =>
      have htail : (y :: ys).Pairwise (fun a b : Version => a.ts < b.ts) := (List.pairwise_cons.mp h).2
      have hxy : x.ts < y.ts := (List.pairwise_cons.mp h).1 y (by simp)
      have ih' := ih y htail
      by_cases hx : snap < x.ts
      · have hy : snap < y.ts := by omega
        have hyany : ((y :: ys).any fun v => snap < v.ts) = true := by simp [hy]
        have hlast : snap < (ys.getLast?.getD y).ts := by
          simpa [List.getLast?_cons] using ih'.mp hyany
        have hgoal : ((x :: y :: ys).any fun v => snap < v.ts) = true := by simp [hx]
        rw [hgoal]
        simpa [List.getLast?_cons] using (show True ↔ snap < (ys.getLast?.getD y).ts from
          ⟨fun _ => hlast, fun _ => trivial⟩)
      · simpa [hx, List.getLast?_cons] using ih'

private theorem last_gt_iff_any (xs : List Version) (snap : Nat)
    (hpair : xs.Pairwise fun x y => x.ts < y.ts) :
    (xs.getLast?.map Version.ts).any (snap < ·) = true ↔ xs.any (fun v => snap < v.ts) = true := by
  cases xs with
  | nil => simp
  | cons a l =>
      have h := any_lt_last a l snap hpair
      cases l with
      | nil => simp at h ⊢
      | cons b bs =>
          simpa [List.getLast?_cons] using h.symm

theorem sound : Goal := by
  intro xs durable snap hpair
  cases xs with
  | nil => simp [changes, atSnap, failedWrite]
  | cons a l =>
      have hpair' : (a :: l).Pairwise (fun x y : Version => x.ts < y.ts) := hpair
      have hpay0 := changes_last_map Version.payload a l snap hpair'
      have hdep0 := changes_last_map (dependency durable) a l snap hpair'
      have hjoin (o : Option Version) : (o.map Version.payload).join = o.bind Version.payload := by
        cases o <;> rfl
      have hpay := congrArg (fun o : Option (Option Int) => o.join) hpay0
      have hdep := congrArg (fun o : Option Nat => o.getD 0) hdep0
      have hhead := last_gt_iff_any (a :: l) snap hpair'
      simp only [atSnap, failedWrite]
      simp only [hjoin] at hpay
      have hthird := Bool.eq_iff_iff.mpr hhead
      rw [hpay, hdep, hthird]

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_ChainSummary
