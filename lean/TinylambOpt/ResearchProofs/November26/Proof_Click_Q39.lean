import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q39
open TinylambOpt.ResearchGoals.November26.Click
open TinylambOpt.ResearchGoals.BenchmarkRematch

def cmp (a b : Nat × Nat) : Bool := decide (b.2 ≤ a.2)
def hiPred (cutoff : Nat) (r : Nat × Nat) : Bool := decide (cutoff < r.2)
def notHiPred (cutoff : Nat) (r : Nat × Nat) : Bool := decide (¬ cutoff < r.2)
def eqPred (cutoff : Nat) (r : Nat × Nat) : Bool := decide (r.2 == cutoff)
def loPred (cutoff : Nat) (r : Nat × Nat) : Bool := decide (r.2 < cutoff)

theorem cmp_trans : ∀ a b c : Nat × Nat, cmp a b = true → cmp b c = true → cmp a c = true := by
  intro a b c hab hbc
  simp [cmp] at hab hbc ⊢
  omega

theorem cmp_total : ∀ a b : Nat × Nat, (cmp a b || cmp b a) = true := by
  intro a b
  simp [cmp]
  omega

theorem filter_hi_prefix (cutoff : Nat) : ∀ l : List (Nat × Nat), l.Pairwise (fun a b => cmp a b = true) →
    l.filter (hiPred cutoff) ++ l.filter (notHiPred cutoff) = l
  | [], _ => by simp
  | a :: as, h => by
      have hp := List.pairwise_cons.mp h
      rw [List.filter_cons, List.filter_cons]
      by_cases ha : cutoff < a.2
      · have hpa : hiPred cutoff a = true := by simp [hiPred, ha]
        have hnpa : notHiPred cutoff a = false := by simp [notHiPred, ha]
        rw [hpa, hnpa]
        simp only [ite_true]
        exact congrArg (fun xs => a :: xs) (filter_hi_prefix cutoff as hp.2)
      · have hpa : hiPred cutoff a = false := by simp [hiPred, ha]
        have hnpa : notHiPred cutoff a = true := by simp [notHiPred, ha]
        rw [hpa, hnpa]
        simp only [ite_true]
        have hnone : ∀ x ∈ as, ¬ cutoff < x.2 := by
          intro x hx
          have hax : cmp a x = true := hp.1 x hx
          have hxle : x.2 ≤ a.2 := by simpa [cmp] using hax
          omega
        have hfil : as.filter (hiPred cutoff) = [] := by
          rw [List.filter_eq_nil_iff]
          intro x hx
          simp [hiPred, hnone x hx]
        have htailnot : ∀ x ∈ as, notHiPred cutoff x = true := by
          intro x hx
          simp [notHiPred, hnone x hx]
        have hfil2 : as.filter (notHiPred cutoff) = as := List.filter_eq_self.mpr htailnot
        rw [hfil, hfil2]
        simp

theorem filter_eq_prefix (cutoff : Nat) : ∀ l : List (Nat × Nat), l.Pairwise (fun a b => cmp a b = true) →
    (∀ r ∈ l, r.2 ≤ cutoff) →
    l.filter (eqPred cutoff) ++ l.filter (loPred cutoff) = l
  | [], _, _ => by simp
  | a :: as, h, hall => by
      have hp := List.pairwise_cons.mp h
      have hale : a.2 ≤ cutoff := hall a (by simp)
      rw [List.filter_cons, List.filter_cons]
      by_cases ha : a.2 == cutoff
      · have haeq : a.2 = cutoff := beq_iff_eq.mp ha
        have hpa : eqPred cutoff a = true := by simp [eqPred, ha]
        have hla : loPred cutoff a = false := by simp [loPred, haeq]
        rw [hpa, hla]
        simp only [ite_true]
        have htail : ∀ r ∈ as, r.2 ≤ cutoff := by intro r hr; exact hall r (by simp [hr])
        exact congrArg (fun xs => a :: xs) (filter_eq_prefix cutoff as hp.2 htail)
      · have hane : a.2 ≠ cutoff := by intro h'; apply ha; simp [h']
        have hlt : a.2 < cutoff := Nat.lt_of_le_of_ne hale hane
        have hpa : eqPred cutoff a = false := by simp [eqPred, ha]
        have hla : loPred cutoff a = true := by simp [loPred, hlt]
        rw [hpa, hla]
        simp only [ite_true]
        have hnone : ∀ x ∈ as, ¬ x.2 == cutoff := by
          intro x hx
          have hax : cmp a x = true := hp.1 x hx
          have hxle : x.2 ≤ a.2 := by simpa [cmp] using hax
          intro hxeq
          have : x.2 = cutoff := beq_iff_eq.mp hxeq
          omega
        have hfil : as.filter (eqPred cutoff) = [] := by
          rw [List.filter_eq_nil_iff]
          intro x hx
          simp [eqPred, hnone x hx]
        have htail : ∀ x ∈ as, loPred cutoff x = true := by
          intro x hx
          have hax : cmp a x = true := hp.1 x hx
          have hxle : x.2 ≤ a.2 := by simpa [cmp] using hax
          simp [loPred]
          omega
        have hfil2 : as.filter (loPred cutoff) = as := List.filter_eq_self.mpr htail
        rw [hfil, hfil2]
        simp

theorem tied_pairwise (groups : List (Nat × Nat)) (cutoff : Nat) :
    (groups.filter (eqPred cutoff)).Pairwise (fun a b => cmp a b = true) := by
  rw [List.pairwise_filter]
  induction groups with
  | nil => simp
  | cons a as ih =>
      rw [List.pairwise_cons]
      constructor
      · intro b hb hpa hpb
        have ha' : a.2 = cutoff := by simpa [eqPred] using hpa
        have hb' : b.2 = cutoff := by simpa [eqPred] using hpb
        simp [cmp, ha', hb']
      · exact ih

theorem tied_sublist_ranked (groups : List (Nat × Nat)) (cutoff : Nat) :
    (groups.filter (eqPred cutoff)).Sublist (groups.mergeSort cmp) := by
  exact List.sublist_mergeSort cmp_trans cmp_total (tied_pairwise groups cutoff) List.filter_sublist

theorem filter_eq_ranked_eq_tied (groups : List (Nat × Nat)) (cutoff : Nat) :
    (groups.mergeSort cmp).filter (eqPred cutoff) = groups.filter (eqPred cutoff) := by
  have hsub0 := tied_sublist_ranked groups cutoff
  have hsub : (groups.filter (eqPred cutoff)).Sublist ((groups.mergeSort cmp).filter (eqPred cutoff)) := by
    have h := hsub0.filter (eqPred cutoff)
    have hself : (groups.filter (eqPred cutoff)).filter (eqPred cutoff) = groups.filter (eqPred cutoff) := by
      apply List.filter_eq_self.mpr
      intro r hr
      simp [eqPred] at hr ⊢
      exact hr.2
    simpa [hself] using h
  have hperm := (List.mergeSort_perm groups cmp).filter (eqPred cutoff)
  exact (hsub.eq_of_length hperm.length_eq.symm).symm

theorem filter_filter_eq {α : Type} (l : List α) (p q : α → Bool)
    (himp : ∀ r, p r = true → q r = true) : (l.filter q).filter p = l.filter p := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true
      · have hq : q a = true := himp a hp
        simp [List.filter_cons, hp, hq, ih]
      · by_cases hq : q a = true <;> simp [List.filter_cons, hp, hq, ih]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q39.Goal := by
  intro groups cutoff higher tied ranked hh hbig
  let hi := ranked.filter (hiPred cutoff)
  let mid := ranked.filter (notHiPred cutoff)
  let lo := ranked.filter (loPred cutoff)
  have hpair : ranked.Pairwise (fun a b => cmp a b = true) := by
    change (groups.mergeSort cmp).Pairwise (fun a b => cmp a b = true)
    exact List.pairwise_mergeSort cmp_trans cmp_total groups
  have hpfx : hi ++ mid = ranked := by
    dsimp [hi, mid]
    exact filter_hi_prefix cutoff ranked hpair
  have hmidpair : mid.Pairwise (fun a b => cmp a b = true) := by
    dsimp [mid]
    exact hpair.filter (notHiPred cutoff)
  have hmidall : ∀ r ∈ mid, r.2 ≤ cutoff := by
    intro r hr
    have hnp : notHiPred cutoff r = true := (List.mem_filter.mp hr).2
    simp [notHiPred] at hnp
    omega
  have hmid : mid.filter (eqPred cutoff) ++ mid.filter (loPred cutoff) = mid := by
    dsimp [mid]
    exact filter_eq_prefix cutoff (ranked.filter (notHiPred cutoff)) hmidpair hmidall
  have himpEq : ∀ r : Nat × Nat, eqPred cutoff r = true → notHiPred cutoff r = true := by
    intro r hr
    have hr' : r.2 = cutoff := by simpa [eqPred] using hr
    simp [notHiPred, hr']
  have himpLo : ∀ r : Nat × Nat, loPred cutoff r = true → notHiPred cutoff r = true := by
    intro r hr
    simp [loPred] at hr
    simp [notHiPred]
    omega
  have hmidEq : mid.filter (eqPred cutoff) = ranked.filter (eqPred cutoff) := by
    dsimp [mid]
    exact filter_filter_eq ranked (eqPred cutoff) (notHiPred cutoff) himpEq
  have hmidLo : mid.filter (loPred cutoff) = lo := by
    dsimp [mid, lo]
    exact filter_filter_eq ranked (loPred cutoff) (notHiPred cutoff) himpLo
  have hranked0 : ranked = hi ++ ranked.filter (eqPred cutoff) ++ lo := by
    calc
      ranked = hi ++ mid := hpfx.symm
      _ = hi ++ (mid.filter (eqPred cutoff) ++ mid.filter (loPred cutoff)) := by rw [hmid]
      _ = hi ++ (ranked.filter (eqPred cutoff) ++ lo) := by rw [hmidEq, hmidLo]
      _ = hi ++ ranked.filter (eqPred cutoff) ++ lo := by simp [List.append_assoc]
  have heqFilt : ranked.filter (eqPred cutoff) = tied := by
    change (groups.mergeSort cmp).filter (eqPred cutoff) = groups.filter (eqPred cutoff)
    exact filter_eq_ranked_eq_tied groups cutoff
  have hranked : ranked = hi ++ tied ++ lo := by
    calc
      ranked = hi ++ ranked.filter (eqPred cutoff) ++ lo := hranked0
      _ = hi ++ tied ++ lo := by rw [heqFilt]
  have hhiLen : hi.length = higher := by
    change ((groups.mergeSort cmp).filter (hiPred cutoff)).length = (groups.filter (hiPred cutoff)).length
    exact ((List.mergeSort_perm groups cmp).filter (hiPred cutoff)).length_eq
  have hdlen : 1000 = hi.length + (1000 - higher) := by
    rw [hhiLen]
    omega
  have hdrop : ranked.drop 1000 = (tied ++ lo).drop (1000 - higher) := by
    rw [hranked, hdlen]
    rw [← List.drop_drop]
    have hinner : (hi ++ tied ++ lo).drop hi.length = tied ++ lo := by
      simp [List.append_assoc]
    rw [hinner]
    have harith : hi.length + (1000 - higher) - higher = 1000 - higher := by
      rw [hhiLen]
      omega
    rw [harith]
  rw [hdrop]
  have hdle : 1000 - higher ≤ tied.length := by omega
  rw [List.drop_append_of_le_length hdle]
  have htake : 10 ≤ (tied.drop (1000 - higher)).length := by
    simp only [List.length_drop]
    omega
  rw [List.take_append_of_le_length htake]
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q39
