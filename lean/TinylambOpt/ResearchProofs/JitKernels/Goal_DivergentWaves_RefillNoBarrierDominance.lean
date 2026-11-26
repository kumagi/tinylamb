import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_RefillNoBarrierDominance

open TinylambOpt.ResearchGoals.JitKernels
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.RefillNoBarrierDominance.Goal := by
  intro steps w hw
  let num := (steps.length + w - 1) / w
  let wp : Nat → Nat := fun j => peak ((steps.drop (j * w)).take w)

  have countP_modify_ge : ∀ (l : List Nat) (i : Nat) (f : Nat → Nat) (p : Nat → Bool),
      List.countP p l ≤ List.countP p (l.modify i f) + 1 := by
    intro l
    induction l with
    | nil => intro i f p; simp
    | cons x xs ih =>
        intro i f p
        cases i with
        | zero =>
            by_cases hpx : p x = true <;> by_cases hpfx : p (f x) = true <;>
              simp [hpx, hpfx] at ih ⊢ <;> omega
        | succ i =>
            simp [List.countP_cons]
            have h := ih i f p
            omega
  have min_le_of_count : ∀ (B : Nat) (loads : List Nat),
      0 < List.countP (fun x => decide (x ≤ B)) loads → (loads.min?.getD 0) ≤ B := by
    intro B loads hc
    rcases (List.countP_pos_iff.mp hc) with ⟨y, hymem, hyp⟩
    have hmin : loads.min? = some (loads.min?.getD 0) := by
      rw [List.min?_eq_some_min (List.ne_nil_of_mem hymem)]
      simp
    have hyB : y ≤ B := by simpa using hyp
    have hminprops := (List.min?_eq_some_iff.mp hmin)
    have hley : loads.min?.getD 0 ≤ y := hminprops.2 y hymem
    exact Nat.le_trans hley hyB
  have countP_of_all_le : ∀ (B : Nat) (loads : List Nat), loads.length = w →
      (∀ (k : Nat) (hk : k < loads.length), loads[k]'hk ≤ B) →
      w ≤ List.countP (fun x => decide (x ≤ B)) loads := by
    intro B loads hlen hall
    have hcount : List.countP (fun x => decide (x ≤ B)) loads = loads.length := by
      apply List.countP_eq_length.mpr
      intro a ha
      rcases List.getElem_of_mem ha with ⟨i, hi, hia⟩
      have haB : loads[i]'hi ≤ B := hall i hi
      simpa [← hia] using (show decide (loads[i]'hi ≤ B) = true by simp [haB])
    simpa [hcount, hlen]
  have wave_aux : ∀ (B P : Nat) (chunk loads : List Nat) (j : Nat),
      j + chunk.length ≤ w → loads.length = w →
      (∀ (k : Nat) (hk : k < loads.length), loads[k]'hk ≤ B + P) →
      w - j ≤ List.countP (fun x => decide (x ≤ B)) loads →
      (∀ q ∈ chunk, q ≤ P) →
      ∀ (k : Nat) (hk : k < (chunk.foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign loads).length),
        (chunk.foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign loads)[k]'hk ≤ B + P := by
    intro B P chunk
    induction chunk with
    | nil =>
        intro loads j hjlen hlen hall hcount hq k hk
        exact hall k hk
    | cons q qs ih =>
        intro loads j hjlen hlen hall hcount hq
        have hjlt : j < w := by
          simp only [List.length_cons] at hjlen
          omega
        let m := loads.min?.getD 0
        let i := loads.idxOf m
        let loads' := TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign loads q
        have hgood : 0 < List.countP (fun x => decide (x ≤ B)) loads := by
          have : 0 < w - j := by omega
          omega
        have hmB : m ≤ B := by
          simpa [m] using min_le_of_count B loads hgood
        have hqP : q ≤ P := hq q (by simp)
        have hmemm : m ∈ loads := by
          have hne : loads ≠ [] := by
            intro hnil
            subst hnil
            simp at hlen
            omega
          have hm_eq : m = loads.min hne := by
            rw [show m = loads.min?.getD 0 by rfl, List.min?_eq_some_min hne]
            simp
          have hmin : loads.min? = some m := by
            simpa [hm_eq] using (List.min?_eq_some_min hne)
          exact List.min?_mem hmin
        have hi : i < loads.length := by
          simpa [i] using (List.idxOf_lt_length_of_mem hmemm)
        have hgeti : loads[i]'hi = m := by
          simpa [i] using (List.getElem_idxOf hi)
        have hlen' : loads'.length = w := by
          simp [loads', TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign, i, hlen]
        have hall' : ∀ (k : Nat) (hk : k < loads'.length), loads'[k]'hk ≤ B + P := by
          intro k hk
          have hkmod : k < (loads.modify i (fun x => x + q)).length := by
            simpa [loads', TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign, TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign] using hk
          have hget : loads'[k]'hk = if i = k then loads[k] + q else loads[k] := by
            simpa [loads', TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign, TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign] using
              (List.getElem_modify (fun x => x + q) i loads k hkmod)
          rw [hget]
          split
          · have hk_eq : k = i := by omega
            have hkloads : k < loads.length := by omega
            have hloadk : loads[k]'hkloads = m := by simpa [hk_eq] using hgeti
            rw [hloadk]
            omega
          · have hkloads : k < loads.length := by omega
            exact Nat.le_trans (hall k hkloads) (by omega)
        have hcount' : w - (j + 1) ≤ List.countP (fun x => decide (x ≤ B)) loads' := by
          have hcmod := countP_modify_ge loads i (fun x => x + q) (fun x => decide (x ≤ B))
          have hc1 : List.countP (fun x => decide (x ≤ B)) loads ≤
              List.countP (fun x => decide (x ≤ B)) loads' + 1 := by
            simpa [loads', TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign, TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign] using hcmod
          omega
        have htail_len : (j + 1) + qs.length ≤ w := by
          simp only [List.length_cons] at hjlen
          omega
        have htail_q : ∀ x ∈ qs, x ≤ P := by
          intro x hx
          exact hq x (by simp [hx])
        have htail := ih loads' (j + 1) htail_len hlen' hall' hcount' htail_q
        intro k hk
        have hk' : k < (qs.foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign loads').length := by
          simpa [loads', TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign] using hk
        exact htail k hk'

  have mem_le_peak : ∀ (xs : List Nat) (x : Nat), x ∈ xs → x ≤ peak xs := by
    intro xs x hx
    by_cases hnil : xs = []
    · simp [hnil] at hx
    · have hmax : xs.max? = some (xs.max hnil) := List.max?_eq_some_max hnil
      have hpeak_eq : peak xs = xs.max hnil := by
        rw [show peak xs = xs.max?.getD 0 by rfl, hmax]
        simp
      rw [hpeak_eq]
      exact (List.max?_eq_some_iff.mp hmax).2 x hx
  have take_add_drop_take : ∀ (l : List Nat) (i k : Nat),
      l.take (i + k) = l.take i ++ (l.drop i).take k := by
    intro l
    induction l with
    | nil => intro i k; simp
    | cons x xs ih =>
        intro i k
        cases i with
        | zero => simp
        | succ i =>
            rw [show i + 1 + k = (i + k) + 1 by omega]
            simp [ih]
  have hceil : steps.length ≤ num * w := by
    rw [Nat.mul_comm]
    have hdecomp := Nat.mod_add_div (steps.length + w - 1) w
    have hmod := Nat.mod_lt (steps.length + w - 1) hw
    simp [num]
    omega
  have hdropnil : steps.drop (num * w) = [] := List.drop_eq_nil_iff.mpr hceil
  have htakeall : steps.take (num * w) = steps := by
    have h := List.take_append_drop (num * w) steps
    simpa [hdropnil] using h
  have hwaves : waves w steps = (List.range num).map wp := by
    rfl
  have hwaves_len : (waves w steps).length = num := by
    simp [hwaves]
  have assign_len : ∀ (xs : List Nat) (q : Nat),
      (TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign xs q).length = xs.length := by
    intro xs q
    simp [TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign]
  have foldl_assign_len : ∀ (xs : List Nat) (acc : List Nat),
      (xs.foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign acc).length = acc.length := by
    intro xs
    induction xs with
    | nil => intro acc; rfl
    | cons q qs ih =>
        intro acc
        rw [List.foldl_cons, ih, assign_len]
  have hprefix : ∀ j, j ≤ num →
      ∀ (k : Nat) (hk : k < ((steps.take (j * w)).foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign (List.replicate w 0)).length),
        ((steps.take (j * w)).foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign (List.replicate w 0))[k]'hk ≤
          (List.take j (waves w steps)).sum := by
    intro j hj
    induction j with
    | zero =>
        intro k hk
        simpa
    | succ j ih =>
        have hjlt : j < num := Nat.lt_of_succ_le hj
        have ih' := ih (Nat.le_of_lt hjlt)
        let loads := (steps.take (j * w)).foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign (List.replicate w 0)
        let chunk := (steps.drop (j * w)).take w
        let B := (List.take j (waves w steps)).sum
        let P := wp j
        have htakeStep : steps.take ((j + 1) * w) = steps.take (j * w) ++ chunk := by
          rw [Nat.succ_mul]
          exact take_add_drop_take steps (j * w) w
        have hfold : (steps.take ((j + 1) * w)).foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign (List.replicate w 0) =
            chunk.foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign loads := by
          rw [htakeStep, List.foldl_append]
        have hsumStep : (List.take (j + 1) (waves w steps)).sum = B + P := by
          rw [List.take_succ]
          have hwjget : (waves w steps)[j]? = some P := by
            simp [hwaves, wp, P, List.getElem?_range, hjlt]
          rw [hwjget]
          simp [B, P]
        have hchunklen : chunk.length ≤ w := by
          simp [chunk]
          omega
        have hchunkP : ∀ q ∈ chunk, q ≤ P := by
          intro q hq
          have hq' : q ∈ (steps.drop (j * w)).take w := by simpa [chunk] using hq
          have hq'' : q ∈ (steps.drop (j * w)).take w := hq'
          simpa [P, wp] using mem_le_peak ((steps.drop (j * w)).take w) q hq''
        have hlenloads : loads.length = w := by
          have := foldl_assign_len (steps.take (j * w)) (List.replicate w 0)
          simpa [loads] using this
        have hallB : ∀ (k : Nat) (hk : k < loads.length), loads[k]'hk ≤ B := by
          intro k hk
          exact ih' k hk
        have hcount : w ≤ List.countP (fun x => decide (x ≤ B)) loads :=
          countP_of_all_le B loads hlenloads hallB
        have hallBP : ∀ (k : Nat) (hk : k < loads.length), loads[k]'hk ≤ B + P := by
          intro k hk
          exact Nat.le_trans (hallB k hk) (by omega)
        have hwb := wave_aux B P chunk loads 0 (by simpa using hchunklen) hlenloads hallBP
          (by simpa using hcount) hchunkP
        intro k hk
        have hkchunk : k < (chunk.foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign loads).length := by
          simpa [hfold] using hk
        have := hwb k hkchunk
        simpa [hfold, hsumStep] using this
  have peak_le_of_all : ∀ (xs : List Nat),
      (∀ (k : Nat) (hk : k < xs.length), xs[k]'hk ≤ ticks w steps) → peak xs ≤ ticks w steps := by
    intro xs hall
    by_cases hnil : xs = []
    · simp [hnil, peak]
    · have hmax : xs.max? = some (xs.max hnil) := List.max?_eq_some_max hnil
      have hpeak_eq : peak xs = xs.max hnil := by
        rw [show peak xs = xs.max?.getD 0 by rfl, hmax]
        simp
      rw [hpeak_eq]
      rcases List.getElem_of_mem (List.max?_mem hmax) with ⟨i, hi, himax⟩
      simpa [himax] using hall i hi
  unfold TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.refill
  apply peak_le_of_all
  intro k hk
  have hkfinal : k < ((steps.take (num * w)).foldl TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.assign (List.replicate w 0)).length := by
    simpa [htakeall] using hk
  have hfinal := hprefix num (by omega) k hkfinal
  have htakeallwaves : List.take num (waves w steps) = waves w steps := by
    rw [← hwaves_len]
    apply List.take_length
  simpa [htakeall, htakeallwaves, TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.ticks] using hfinal

end TinylambOpt.ResearchProofs.JitKernels.Goal_DivergentWaves_RefillNoBarrierDominance
