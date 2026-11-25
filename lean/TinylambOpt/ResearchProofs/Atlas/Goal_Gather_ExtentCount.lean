import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Gather_ExtentCount

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather

private theorem nodup_eraseDups [BEq α] [LawfulBEq α] : ∀ xs : List α, xs.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => ! b == a) as := (List.mem_eraseDups.mp h)
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact nodup_eraseDups (List.filter (fun b => ! b == a) as)
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)


private def startsAux (prev : Option Nat) (l : List Nat) : Nat :=
  match l with
  | [] => 0
  | x::xs => (if (prev.map (fun p => p+1)) == some x then 0 else 1) + startsAux (some x) xs

private def bridge (prev : Option Nat) (l : List Nat) : Nat :=
  match prev with
  | none => 0
  | some p => if l.contains (p+1) then 1 else 0

private def cntFrom (prev : Option Nat) (start : Nat) (l : List Nat) : Nat :=
  (l.zipIdx start).countP (fun (v,i) =>
    !(((if i == start then prev else some (l[i - start - 1]?.getD v)).map (fun p => p+1)) == some v))

private theorem cntFrom_eq_startsAux (prev : Option Nat) (start : Nat) (l : List Nat) :
    cntFrom prev start l = startsAux prev l := by
  induction l generalizing start prev with
  | nil => simp [cntFrom, startsAux]
  | cons z zs ih =>
      unfold cntFrom
      rw [List.zipIdx_cons, List.countP_cons]
      have hfirst :
          (!(((if start == start then prev else some ((z::zs)[start - start - 1]?.getD z)).map (fun p => p+1)) == some z)) =
          (!(((prev.map (fun p => p+1)) == some z))) := by simp
      simp only [hfirst]
      have htail :
          (List.filter (fun x : Nat × Nat => !(((if x.snd == start then prev else some ((z::zs)[x.snd - start - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) (zs.zipIdx (start+1))) =
          (List.filter (fun x : Nat × Nat => !(((if x.snd == start+1 then some z else some (zs[x.snd - (start+1) - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) (zs.zipIdx (start+1))) := by
        apply List.filter_congr
        intro x hx
        have hle : start+1 ≤ x.snd := (List.mk_mem_zipIdx_iff_le_and_getElem?_sub.mp hx).1
        have hstart_false : (x.snd == start) = false := by
          by_cases h : x.snd == start
          · have heq : x.snd = start := by simpa using h
            omega
          · exact Bool.eq_false_of_ne_true h
        by_cases hfirst2 : x.snd == start+1
        · have hs : x.snd = start + 1 := by simpa using hfirst2
          have hidx0 : x.snd - start - 1 = 0 := by omega
          simp [hs]
        · have hne : x.snd ≠ start+1 := by intro hh; exact hfirst2 (by simp [hh])
          have hgt : start+1 < x.snd := by omega
          have hget : (z::zs)[x.snd - start - 1]? = zs[x.snd - (start+1) - 1]? := by
            have hj : x.snd - start - 1 = (x.snd - (start+1) - 1) + 1 := by omega
            rw [hj, List.getElem?_cons_succ]
          rw [hstart_false]
          simp [hfirst2]
          rw [hget]
      have htailc :
          (zs.zipIdx (start+1)).countP (fun x : Nat × Nat => !(((if x.snd == start then prev else some ((z::zs)[x.snd - start - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) =
          (zs.zipIdx (start+1)).countP (fun x : Nat × Nat => !(((if x.snd == start+1 then some z else some (zs[x.snd - (start+1) - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) := by
        rw [List.countP_eq_length_filter, List.countP_eq_length_filter, htail]
      rw [htailc]
      change cntFrom (some z) (start+1) zs +
          (if (!((prev.map (fun p => p+1)) == some z)) then 1 else 0) =
        (if ((prev.map (fun p => p+1)) == some z) then 0 else 1) + startsAux (some z) zs
      rw [ih (some z) (start+1)]
      cases hc : ((prev.map (fun p => p+1)) == some z) <;> simp <;> omega

private theorem extentsFilter_eq_cntFrom (l : List Nat) :
    (l.zipIdx.filter (fun (v,i) => i == 0 || (l[i-1]?.getD v)+1 != v)).length = cntFrom none 0 l := by
  unfold cntFrom
  rw [List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  rintro ⟨v,i⟩ hi
  by_cases h : i == 0
  · have hz : i = 0 := by simpa using h
    simp [hz]
  · have hne : i ≠ 0 := by intro hh; exact h (by simp [hh])
    have hpos : 0 < i := by omega
    simp [h]
    rfl

private theorem startsAux_aux (prev : Option Nat) (l : List Nat)
    (hprev : ∀ x ∈ l, ∀ p, prev = some p → p < x)
    (hs : l.Pairwise (fun a b => a ≤ b)) (hn : l.Nodup) :
    startsAux prev l + l.countP (fun p => l.contains (p+1)) + bridge prev l = l.length := by
  induction l generalizing prev with
  | nil => cases prev <;> simp [startsAux, bridge]
  | cons x xs ih =>
      have hpx_all : ∀ p, prev = some p → p < x := hprev x (by simp)
      cases xs with
      | nil =>
          cases hp : prev with
          | none => simp [startsAux, bridge]
          | some p =>
              have hpx : p < x := hpx_all p hp
              by_cases hEq : p+1 = x
              · subst x
                simp [startsAux, bridge]
              · have hbool : (p+1 == x) = false := by simp [hEq]
                simp [startsAux, bridge, hbool]
                exact hEq
      | cons y ys =>
          have hpw_tail : (y::ys).Pairwise (fun a b => a ≤ b) := (List.pairwise_cons.mp hs).2
          have hn_tail : (y::ys).Nodup := (List.nodup_cons.mp hn).2
          have hxnot : x ∉ y::ys := (List.nodup_cons.mp hn).1
          have hprev_tail : ∀ z ∈ y::ys, ∀ p, (some x) = some p → p < z := by
            intro z hz p hp
            have hxp : x = p := by simpa using hp
            subst p
            have hxz : x ≤ z := by
              rcases List.mem_cons.mp hz with hzy | hzys
              · subst z
                exact (List.pairwise_cons.mp hs).1 y (by simp)
              · exact (List.pairwise_cons.mp hs).1 z (by simp [hzys])
            have hne : z ≠ x := by intro hh; subst z; exact hxnot hz
            omega
          have ih' := ih (some x) hprev_tail hpw_tail hn_tail
          have hheadbridge : bridge prev (x::y::ys) = (if (prev.map (fun p => p+1)) == some x then 1 else 0) := by
            cases hp : prev with
            | none => simp [bridge]
            | some p =>
                have hpx : p < x := hpx_all p hp
                have hmin : ∀ z ∈ y::ys, y ≤ z := by
                  intro z hz
                  rcases List.mem_cons.mp hz with hzy | hzys
                  · subst z
                    exact Nat.le_refl y
                  · exact (List.pairwise_cons.mp hpw_tail).1 z hzys
                have hxylt : x < y := by
                  have hxy : x ≤ y := (List.pairwise_cons.mp hs).1 y (by simp)
                  have hne : x ≠ y := by intro hh; subst y; exact hxnot (by simp)
                  omega
                have hnot : p+1 ∉ y::ys := by
                  intro hm
                  have hy_le : y ≤ p+1 := hmin (p+1) hm
                  omega
                have htailnot : (y::ys).contains (p+1) = false := by simpa using hnot
                have hb : bridge (some p) (x::y::ys) = (if p+1 == x then 1 else 0) := by
                  simp [bridge]
                  have hney : p+1 ≠ y := by intro hh; apply hnot; simp [hh]
                  have hne_ys : p+1 ∉ ys := by intro hh; apply hnot; simp [hh]
                  simp [hney, hne_ys]
                simpa [hp] using hb
          have hpred : ∀ q ∈ y::ys, (x::y::ys).contains (q+1) = (y::ys).contains (q+1) := by
            intro q hq
            have hxq : x < q := by
              have hxleq : x ≤ q := (List.pairwise_cons.mp hs).1 q hq
              have hne : q ≠ x := by intro hh; subst q; exact hxnot hq
              omega
            by_cases hEq : (q+1 == x) = true
            · have heq : q+1 = x := by simpa using hEq
              omega
            · have hbool : (q+1 == x) = false := Bool.eq_false_of_ne_true hEq
              simp
              intro hh
              omega
          have hcp : (y::ys).countP (fun p => (x::y::ys).contains (p+1)) =
              (y::ys).countP (fun p => (y::ys).contains (p+1)) := by
            rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
            apply congrArg List.length
            apply List.filter_congr
            intro q hq
            exact hpred q hq
          have hhead : (x::y::ys).contains (x+1) = (y::ys).contains (x+1) := by
            have hne : x+1 ≠ x := by omega
            simp
          have hcnthead : (x::y::ys).countP (fun p => (x::y::ys).contains (p+1)) =
              (y::ys).countP (fun p => (y::ys).contains (p+1)) +
                (if (y::ys).contains (x+1) then 1 else 0) := by
            rw [List.countP_cons, hcp, hhead]
          simp [bridge] at ih'
          rw [startsAux]
          simp only [List.length_cons]
          rw [hheadbridge, hcnthead]
          rw [← ih']
          unfold startsAux
          split <;> simp <;> omega

theorem sound_ExtentCount : ExtentCount.Goal := by
  intro cap ids hcap
  let ps := pages cap ids
  let sorted := ps.mergeSort
  have hperm : sorted.Perm ps := by
    dsimp [sorted]
    exact List.mergeSort_perm _ _
  have hnodup : sorted.Nodup := hperm.symm.nodup (nodup_eraseDups _)
  have hsorted : sorted.Pairwise (fun a b => a ≤ b) := by
    have htotal : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba]
    have hpw := List.pairwise_mergeSort
      (le := fun a b : Nat => decide (a ≤ b))
      (by intro a b c hab hbc; simp at hab hbc ⊢; omega) htotal ps
    simpa [sorted] using hpw
  have hstart : startsAux none sorted + sorted.countP (fun p => sorted.contains (p+1)) = sorted.length := by
    have h := startsAux_aux none sorted (by intro x hx p hp; cases hp) hsorted hnodup
    simpa [bridge] using h
  have hpred_eq : (fun p : Nat => sorted.contains (p+1)) = (fun p : Nat => ps.contains (p+1)) := by
    funext p
    exact hperm.contains_eq
  rw [hpred_eq] at hstart
  have hcnt : sorted.countP (fun p => ps.contains (p+1)) = ps.countP (fun p => ps.contains (p+1)) := by
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
    exact (hperm.filter (fun p => ps.contains (p+1))).length_eq
  dsimp [extents]
  change (sorted.zipIdx.filter (fun (v,i) => i == 0 || (sorted[i-1]?.getD v)+1 != v)).length +
      ps.countP (fun p => ps.contains (p+1)) = ps.length
  rw [extentsFilter_eq_cntFrom sorted, cntFrom_eq_startsAux none 0 sorted]
  rw [← hcnt]
  rw [hstart, hperm.length_eq]


end TinylambOpt.ResearchProofs.Atlas.Goal_Gather_ExtentCount
