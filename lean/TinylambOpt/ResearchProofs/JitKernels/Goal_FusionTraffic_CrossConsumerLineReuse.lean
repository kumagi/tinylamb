import Std
import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! Soundness of cross-consumer line reuse: footprint deduplication makes the shared cache-line
count an intersection, so the union plus intersection identity accounts for every line once. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_CrossConsumerLineReuse

open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

theorem sound : TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.CrossConsumerLineReuse.Goal := by
  have hndFilter : ∀ (l : List Nat) (p : Nat → Bool), (l.filter p).eraseDups.Nodup := by
    intro l
    induction l with
    | nil => intro p; simp
    | cons a l ih =>
        intro p
        by_cases hp : p a = true
        · have ih' := ih (fun b => !b == a && p b)
          simpa [hp, List.eraseDups_cons, List.filter_filter] using ih'
        · simpa [hp] using ih p
  have hndErase : ∀ (l : List Nat), l.eraseDups.Nodup := by
    intro l
    have hf : l.filter (fun _ => true) = l := by
      induction l with
      | nil => rfl
      | cons a l ih => simp [ih]
    rw [← hf]
    exact hndFilter l (fun _ => true)
  have hEraseLenEq : ∀ (l m : List Nat), (∀ x, x ∈ l ↔ x ∈ m) →
      l.eraseDups.length = m.eraseDups.length := by
    intro l m hmem
    apply Nat.le_antisymm
    · exact (hndErase l).length_le_of_subset (by
        intro x hx
        exact List.mem_eraseDups.mpr ((hmem x).mp (List.mem_eraseDups.mp hx)))
    · exact (hndErase m).length_le_of_subset (by
        intro x hx
        exact List.mem_eraseDups.mpr ((hmem x).mpr (List.mem_eraseDups.mp hx)))
  have hfilterNodup : ∀ (l : List Nat) (p : Nat → Bool), l.Nodup → (l.filter p).Nodup := by
    intro l p h
    exact List.Nodup.sublist List.filter_sublist h
  have heraseLen : ∀ (l : List Nat), l.Nodup → (l.eraseDups).length = l.length := by
    intro l
    induction l with
    | nil => intro h; rfl
    | cons a l ih =>
        intro h
        have ha : a ∉ l := (List.nodup_cons.mp h).1
        have hl : l.Nodup := (List.nodup_cons.mp h).2
        rw [List.eraseDups_cons]
        have hfilt : l.filter (fun b => !b == a) = l := by
          apply List.filter_eq_self.mpr
          intro b hb
          have hba : b ≠ a := by intro heq; exact ha (heq ▸ hb)
          simpa [hba]
        rw [hfilt]
        simp only [List.length_cons]
        rw [ih hl]
  have heraseSubAux : ∀ (l : List Nat) (p : Nat → Bool), List.Sublist (l.filter p).eraseDups (l.filter p) := by
    intro l
    induction l with
    | nil => intro p; simp
    | cons a l ih =>
        intro p
        by_cases hp : p a = true
        · have ih' := ih (fun b => !b == a && p b)
          simp only [List.filter_cons]
          simp [hp]
          rw [List.eraseDups_cons, List.filter_filter]
          apply List.Sublist.cons₂
          have hsub : List.Sublist (List.filter (fun b => !b == a) (List.filter p l)) (List.filter p l) := List.filter_sublist
          rw [List.filter_filter] at hsub
          exact List.Sublist.trans ih' hsub
        · simpa [hp] using ih p
  have heraseSub : ∀ (l : List Nat), List.Sublist l.eraseDups l := by
    intro l
    have h := heraseSubAux l (fun _ => true)
    have hf : l.filter (fun _ => true) = l := by
      induction l with
      | nil => rfl
      | cons a l ih => simp [ih]
    simpa [hf] using h
  have herase : ∀ (l : List Nat), l.Nodup → l.eraseDups = l := by
    intro l h
    exact (List.Sublist.length_eq (heraseSub l)).mp (heraseLen l h)
  have hpart : ∀ (l : List Nat) (p : Nat → Bool),
      (l.filter p).length + (l.filter (fun x => !p x)).length = l.length := by
    intro l
    induction l with
    | nil => intro p; rfl
    | cons a l ih =>
        intro p
        by_cases hp : p a = true
        · have hh := ih p
          simp [hp]
          omega
        · have hh := ih p
          simp [hp]
          omega
  have hinterCount : ∀ (A B : List Nat), A.Nodup → B.Nodup →
      A.countP (fun x => decide (x ∈ B)) = B.countP (fun x => decide (x ∈ A)) := by
    intro A
    induction A with
    | nil => intro B hA hB; simp
    | cons a A ih =>
        intro B hA hB
        have haA : a ∉ A := (List.nodup_cons.mp hA).1
        have hA' : A.Nodup := (List.nodup_cons.mp hA).2
        have hhelper : ∀ (B : List Nat), B.Nodup →
            B.countP (fun x => decide (x = a ∨ x ∈ A)) =
              (if a ∈ B then 1 else 0) + B.countP (fun x => decide (x ∈ A)) := by
          intro B hB
          induction B with
          | nil => simp
          | cons b B ihB =>
              have hbB : b ∉ B := (List.nodup_cons.mp hB).1
              have hB' : B.Nodup := (List.nodup_cons.mp hB).2
              by_cases hba : b = a
              · subst b
                have hnotB : a ∉ B := hbB
                rw [List.countP_cons_of_pos (p := fun x => decide (x = a ∨ x ∈ A)) (by simp [haA])]
                rw [ihB hB']
                rw [List.countP_cons_of_neg (p := fun x => decide (x ∈ A)) (by simpa [haA])]
                simp [hnotB]
                omega
              · by_cases hbA : b ∈ A
                · rw [List.countP_cons_of_pos (p := fun x => decide (x = a ∨ x ∈ A)) (by simp [hba, hbA])]
                  rw [ihB hB']
                  rw [List.countP_cons_of_pos (p := fun x => decide (x ∈ A)) (by simp [hbA])]
                  simp [hba, Ne.symm hba, Nat.add_comm, Nat.add_left_comm]
                · rw [List.countP_cons_of_neg (p := fun x => decide (x = a ∨ x ∈ A)) (by simp [hba, hbA])]
                  rw [ihB hB']
                  rw [List.countP_cons_of_neg (p := fun x => decide (x ∈ A)) (by simp [hbA])]
                  simp [Ne.symm hba]
        have hh := hhelper B hB
        have hR : B.countP (fun x => decide (x ∈ a :: A)) =
            B.countP (fun x => decide (x = a ∨ x ∈ A)) := by
          apply List.countP_congr
          intro x hx
          simp [List.mem_cons]
        rw [hR]
        by_cases haB : a ∈ B
        · rw [List.countP_cons_of_pos (p := fun x => decide (x ∈ B)) (by simp [haB])]
          rw [ih B hA' hB]
          rw [hh]
          simp [haB]
          omega
        · rw [List.countP_cons_of_neg (p := fun x => decide (x ∈ B)) (by simp [haB])]
          rw [ih B hA' hB]
          rw [hh]
          simp [haB]
  have hinter : ∀ (A B : List Nat), A.Nodup → B.Nodup →
      (A.filter (fun x => B.contains x)).length = (B.filter (fun x => A.contains x)).length := by
    intro A B hA hB
    rw [← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
    have hAB : A.countP (fun x => B.contains x) = A.countP (fun x => decide (x ∈ B)) := by
      apply List.countP_congr
      intro x hx
      rw [List.contains, List.elem_eq_mem]
    have hBA : B.countP (fun x => A.contains x) = B.countP (fun x => decide (x ∈ A)) := by
      apply List.countP_congr
      intro x hx
      rw [List.contains, List.elem_eq_mem]
    rw [hAB, hBA]
    exact hinterCount A B hA hB
  have hcard : ∀ (A B : List Nat), A.Nodup → B.Nodup →
      ((A ++ B).eraseDups).length + (A.filter (fun x => B.contains x)).length =
        A.length + B.length := by
    intro A B hA hB
    have hfN : (B.removeAll A).Nodup := by
      simpa [List.removeAll] using (hfilterNodup B (fun x => !A.contains x) hB)
    rw [List.eraseDups_append, herase A hA, herase _ hfN]
    simp only [List.length_append]
    have hi := hinter A B hA hB
    have hp : (B.filter (fun x => A.contains x)).length + (B.removeAll A).length = B.length := by
      simpa [List.removeAll] using (hpart B (fun x => A.contains x))
    omega
  intro base stride w a b
  dsimp only
  let la := footprint base stride w a
  let lb := footprint base stride w b
  have hla : la.Nodup := by
    dsimp [la, footprint]
    exact hndErase _
  have hlb : lb.Nodup := by
    dsimp [lb, footprint]
    exact hndErase _
  have hlenUnion : (footprint base stride w (a ++ b)).length = ((la ++ lb).eraseDups).length := by
    dsimp [footprint, la, lb]
    rw [List.map_append]
    apply hEraseLenEq
    intro x
    simp [List.mem_eraseDups]
  rw [hlenUnion]
  exact hcard la lb hla hlb

end TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_CrossConsumerLineReuse
