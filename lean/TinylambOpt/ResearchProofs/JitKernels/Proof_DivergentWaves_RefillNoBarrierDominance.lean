import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.RefillNoBarrierDominance.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Immediate lane refill is no worse than fixed packet barriers: a work-conserving greedy schedule cannot finish after the barrier schedule that idles lanes at wave ends. The proof tracks a per-lane bound across complete barrier blocks and uses an untouched-lane invariant inside each block. -/

namespace Nov26.Proof_DivergentWaves_RefillNoBarrierDominance

open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves.RefillNoBarrierDominance

def assignTrace : List Nat → List Nat → List Nat × List Nat
  | loads, [] => (loads, [])
  | loads, s :: js =>
      let i := loads.idxOf (loads.min?.getD 0)
      let loads1 := loads.modify i (fun v => v + s)
      let (loads2, is) := assignTrace loads1 js
      (loads2, i :: is)

theorem trace_fst_eq_foldl : ∀ (loads jobs : List Nat),
    (assignTrace loads jobs).1 = jobs.foldl assign loads := by
  intro loads jobs
  induction jobs generalizing loads with
  | nil => rfl
  | cons s js ih =>
      simp only [assignTrace, List.foldl_cons]
      simp [assign, ih]

theorem idxOf_lt_length : ∀ (l : List Nat) (a : Nat), a ∈ l → l.idxOf a < l.length := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      by_cases hxa : x = a
      · subst a
        simp
      · have hmem : a ∈ xs := by simpa [eq_comm, hxa] using ha
        have ht := ih a hmem
        rw [List.idxOf_cons]
        have hbeq : (x == a) = false := by simp [hxa]
        simp [hbeq]
        omega

theorem min?_some_of_mem_nat2 : ∀ (l : List Nat) (a : Nat), a ∈ l → ∃ m, l.min? = some m := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      cases xs with
      | nil => simp
      | cons y ys => simp [List.min?_cons]

theorem min?_le_of_all : ∀ (l : List Nat) (C : Nat), l ≠ [] → (∀ v ∈ l, v ≤ C) → l.min?.getD 0 ≤ C := by
  intro l C hne hall
  have hmem : l.head hne ∈ l := List.head_mem hne
  obtain ⟨m, hm⟩ := min?_some_of_mem_nat2 l (l.head hne) hmem
  have hmle := hall m (List.min?_mem hm)
  simp [hm]
  exact hmle

theorem min?_le_mem : ∀ (l : List Nat) (a : Nat), a ∈ l → l.min?.getD 0 ≤ a := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · cases xs with
        | nil => simp
        | cons y ys =>
            obtain ⟨m, hm⟩ := min?_some_of_mem_nat2 (y :: ys) y (by simp)
            simp [List.min?_cons, hm]
            exact Nat.min_le_left a m
      · cases xs with
        | nil => simp at ha
        | cons y ys =>
            obtain ⟨m, hm⟩ := min?_some_of_mem_nat2 (y :: ys) y (by simp)
            have hle := ih a ha
            simp [List.min?_cons, hm] at hle ⊢
            omega

theorem length_filter_ne_lt : ∀ (l : List Nat) (p : Nat → Bool) (a : Nat),
    a ∈ l → p a = false → (l.filter p).length < l.length := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro p a ha hpa
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · simp [hpa]
        have hle := List.length_filter_le p xs
        omega
      · by_cases hx : p x = true
        · simp [hx, ih p a ha hpa]
        · have hxf : p x = false := by cases h : p x <;> simp_all
          simp [hx]
          have hle := List.length_filter_le p xs
          omega

theorem exists_not_mem_of_length_lt : ∀ (chosen : List Nat) (w : Nat),
    chosen.length < w → ∃ k, k < w ∧ k ∉ chosen := by
  intro chosen w
  revert chosen
  induction w with
  | zero => intro chosen h; simp at h
  | succ w ih =>
      intro chosen hlen
      by_cases hlast : w ∈ chosen
      · let chosen' := chosen.filter (fun k => k != w)
        have hpw : (fun k : Nat => k != w) w = false := by simp
        have hlt : chosen'.length < chosen.length := length_filter_ne_lt chosen (fun k => k != w) w hlast hpw
        have hlen' : chosen'.length < w := by omega
        obtain ⟨k, hklt, hknot⟩ := ih chosen' hlen'
        refine ⟨k, by omega, ?_⟩
        intro hkin
        have hkne : k ≠ w := by omega
        have hkneq : (k != w) = true := by simp [hkne]
        have hknew : k ∈ chosen' := by
          simp [chosen', hkneq, hkin]
        have hknot' : ¬ (k ∈ chosen') := hknot
        exact hknot' hknew
      · exact ⟨w, by omega, hlast⟩

theorem waves_get?_in : ∀ (steps : List Nat) (w i : Nat),
    i < (steps.length + w - 1) / w →
    (waves w steps)[i]? = some (peak ((steps.drop (i * w)).take w)) := by
  intro steps w i hi
  unfold waves
  rw [List.getElem?_map, List.getElem?_range hi]
  simp

theorem waves_get?_out : ∀ (steps : List Nat) (w i : Nat),
    (steps.length + w - 1) / w ≤ i → (waves w steps)[i]? = none := by
  intro steps w i hi
  unfold waves
  rw [List.getElem?_map, List.getElem?_eq_none]
  · simp
  · simpa using hi

theorem waves_take_drop : ∀ (steps : List Nat) (w : Nat), 0 < w → steps ≠ [] →
    waves w steps = peak (steps.take w) :: waves w (steps.drop w) := by
  intro steps w hw hne
  have hkeq : (steps.length + w - 1) / w = (((steps.drop w).length + w - 1) / w) + 1 := by
    rw [List.length_drop]
    have hrewrite : steps.length + w - 1 = (steps.length - 1) + 1 * w := by
      have hposlen : 0 < steps.length := List.length_pos_iff.mpr hne
      omega
    rw [hrewrite, Nat.add_mul_div_right (steps.length - 1) 1 hw]
    by_cases hle : steps.length ≤ w
    · have hn1 : (steps.length - 1) / w = 0 := Nat.div_eq_of_lt (by omega)
      have hn2 : (steps.length - w + w - 1) / w = 0 := Nat.div_eq_of_lt (by omega)
      rw [hn1, hn2]
    · have heq : steps.length - w + w - 1 = steps.length - 1 := by omega
      rw [heq]
  apply List.ext_getElem?
  intro i
  cases i with
  | zero =>
      have hpos : 0 < (steps.length + w - 1) / w := by
        apply Nat.div_pos
        · have hposlen : 0 < steps.length := List.length_pos_iff.mpr hne
          omega
        · exact hw
      rw [waves_get?_in steps w 0 hpos]
      simp
  | succ j =>
      by_cases hj : j < ((steps.drop w).length + w - 1) / w
      · have hj1 : j + 1 < (steps.length + w - 1) / w := by omega
        rw [waves_get?_in steps w (j + 1) hj1]
        rw [List.getElem?_cons_succ]
        rw [waves_get?_in (steps.drop w) w j hj]
        simp only [List.drop_drop]
        apply congrArg some
        apply congrArg (fun xs : List Nat => peak (xs.take w))
        have hdrop : (j + 1) * w = w + j * w := by
          rw [Nat.succ_mul]
          omega
        rw [hdrop]
      · have hj1 : (steps.length + w - 1) / w ≤ j + 1 := by omega
        rw [waves_get?_out steps w (j + 1) hj1]
        rw [List.getElem?_cons_succ]
        rw [waves_get?_out (steps.drop w) w j (by omega)]

def assignTraceC (loads chosen : List Nat) : List Nat → List Nat × List Nat
  | [] => (loads, chosen)
  | s :: js =>
      let i := loads.idxOf (loads.min?.getD 0)
      let loads1 := loads.modify i (fun v => v + s)
      assignTraceC loads1 (chosen ++ [i]) js

theorem assignTraceC_fst : ∀ (loads chosen jobs : List Nat),
    (assignTraceC loads chosen jobs).1 = jobs.foldl assign loads := by
  intro loads chosen jobs
  induction jobs generalizing loads chosen with
  | nil => rfl
  | cons s js ih =>
      simp only [assignTraceC, List.foldl_cons]
      simp [assign, ih]

theorem assignTraceC_snd : ∀ (loads chosen jobs : List Nat),
    (assignTraceC loads chosen jobs).2 = chosen ++ (assignTraceC loads [] jobs).2 := by
  intro loads chosen jobs
  induction jobs generalizing loads chosen with
  | nil => simp [assignTraceC]
  | cons s js ih =>
      dsimp [assignTraceC]
      rw [ih (loads.modify (List.idxOf (loads.min?.getD 0) loads) (fun v => v + s)) (chosen ++ [List.idxOf (loads.min?.getD 0) loads])]
      rw [ih (loads.modify (List.idxOf (loads.min?.getD 0) loads) (fun v => v + s)) [List.idxOf (loads.min?.getD 0) loads]]
      simp [List.append_assoc]

theorem assignTraceC_bound : ∀ (loads chosen jobs : List Nat) (w C M : Nat),
    loads.length = w → 0 < w → chosen.length + jobs.length ≤ w →
    (∀ k (_hk : k < w), k ∉ chosen → (loads[k]?).getD 0 ≤ C) →
    (∀ k (_hk : k < w), (loads[k]?).getD 0 ≤ C + M) →
    (∀ s ∈ jobs, s ≤ M) →
    let (loads', chosen') := assignTraceC loads chosen jobs
    (∀ k (_hk : k < w), (loads'[k]?).getD 0 ≤ C + M) ∧
    (∀ k (_hk : k < w), k ∉ chosen ++ chosen' → (loads'[k]?).getD 0 ≤ C) ∧
    chosen'.length = chosen.length + jobs.length := by
  intro loads chosen jobs
  induction jobs generalizing loads chosen with
  | nil =>
      intro w C M hlen hw hcnt hunch hbound hjob
      simp [assignTraceC]
      refine ⟨hbound, ?_⟩
      intro k hk
      exact hunch k hk
  | cons s js ih =>
      intro w C M hlen hw hcnt hunch hbound hjob
      have hne : loads ≠ [] := by
        intro h
        subst loads
        simp at hlen
        omega
      simp only [List.length_cons] at hcnt
      have hcnt0 : chosen.length < w := by omega
      obtain ⟨k0, hk0lt, hk0not⟩ := exists_not_mem_of_length_lt chosen w hcnt0
      have hk0bound : (loads[k0]?).getD 0 ≤ C := hunch k0 hk0lt hk0not
      have hminC : (loads.min?.getD 0) ≤ C := by
        have hmle : (loads.min?.getD 0) ≤ (loads[k0]?).getD 0 := min?_le_mem loads ((loads[k0]?).getD 0) (by
          have hk0len : k0 < loads.length := by simpa [hlen] using hk0lt
          have hk0eq : (loads[k0]?).getD 0 = loads[k0] := by
            rw [List.getElem?_eq_getElem hk0len]
            simp
          rw [hk0eq]
          exact List.getElem_mem hk0len)
        exact Nat.le_trans hmle hk0bound
      let i := loads.idxOf (loads.min?.getD 0)
      have hiw : i < w := by
        have hi : i < loads.length := by
          dsimp [i]
          apply idxOf_lt_length
          obtain ⟨m, hm⟩ := min?_some_of_mem_nat2 loads (loads.head hne) (List.head_mem hne)
          simpa [hm] using (List.min?_mem hm)
        omega
      let loads1 := loads.modify i (fun v => v + s)
      have hlen1 : loads1.length = w := by simp [loads1, hlen]
      have hsle : s ≤ M := hjob s (by simp)
      have hloads1_bound : ∀ k (hk : k < w), (loads1[k]?).getD 0 ≤ C + M := by
        intro k hk
        have hkget : (loads1[k]?).getD 0 = loads1[k] := by
          rw [List.getElem?_eq_getElem (by simpa [hlen1] using hk)]
          rfl
        rw [hkget]
        have hmod := List.getElem_modify (fun v : Nat => v + s) i loads k (by simpa [hlen] using hk)
        by_cases hik : i = k
        · subst k
          rw [hmod]
          simp
          have hi_eq : loads[i] = loads.min?.getD 0 := List.getElem_idxOf (by simpa [hlen] using hiw)
          rw [hi_eq]
          omega
        · rw [hmod]
          simp [hik]
          have hkeq : (loads[k]?).getD 0 = loads[k] := by
            rw [List.getElem?_eq_getElem (by simpa [hlen] using hk)]
            simp
          have hvk : loads[k] ≤ C + M := by simpa [hkeq] using hbound k hk
          omega
      have hunch_tail : ∀ k (hk : k < w), k ∉ chosen ++ [i] → (loads1[k]?).getD 0 ≤ C := by
        intro k hk hknot
        simp only [List.mem_append, List.mem_singleton] at hknot
        have hkne : k ≠ i := by
          intro h
          subst k
          exact hknot (Or.inr rfl)
        have hknotchosen : k ∉ chosen := by
          intro h
          exact hknot (Or.inl h)
        dsimp [loads1]
        rw [List.getElem?_modify]
        have hik : i ≠ k := by omega
        simp [hik]
        exact hunch k hk hknotchosen
      have hcnt_tail : (chosen ++ [i]).length + js.length ≤ w := by
        simp
        omega
      have htail := ih loads1 (chosen ++ [i]) w C M hlen1 hw hcnt_tail hunch_tail hloads1_bound (by
        intro t ht
        exact hjob t (by simp [ht]))
      dsimp [assignTraceC]
      dsimp at htail
      rcases htail with ⟨htb, htu, htl⟩
      refine ⟨htb, ?_, ?_⟩
      · intro k hk hknot
        apply htu k hk
        intro hmem
        apply hknot
        rw [assignTraceC_snd] at hmem
        rw [assignTraceC_snd]
        simp only [List.mem_append, List.mem_singleton] at hmem ⊢
        by_cases hc : k ∈ chosen
        · exact Or.inl hc
        · by_cases hki : k = i
          · subst k
            simp [hc, i]
          · simp [hc, hki] at hmem ⊢
            right
            exact hmem
      · change (assignTraceC loads1 (chosen ++ [i]) js).snd.length = chosen.length + (js.length + 1)
        simp only [List.length_append, List.length_singleton] at htl
        omega

theorem max?_some_of_mem_nat2 : ∀ (l : List Nat) (a : Nat), a ∈ l → ∃ m, l.max? = some m := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      cases xs with
      | nil => simp
      | cons y ys => simp [List.max?_cons]

theorem le_peak : ∀ (l : List Nat) (a : Nat), a ∈ l → a ≤ peak l := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      simp only [List.mem_cons] at ha
      rcases ha with rfl | ha
      · cases xs with
        | nil => simp [peak]
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem_nat2 (y :: ys) y (by simp)
            simp [peak, List.max?_cons, hm]
            exact Nat.le_max_left a m
      · cases xs with
        | nil => simp at ha
        | cons y ys =>
            obtain ⟨m, hm⟩ := max?_some_of_mem_nat2 (y :: ys) y (by simp)
            have hle := ih a ha
            simp [peak, List.max?_cons, hm] at hle ⊢
            omega

theorem peak_le_of_forall : ∀ (l : List Nat) (c : Nat), (∀ x ∈ l, x ≤ c) → peak l ≤ c := by
  intro l c h
  by_cases hnil : l = []
  · subst l
    simp [peak]
  · have hmem : l.head hnil ∈ l := List.head_mem hnil
    obtain ⟨m, hm⟩ := max?_some_of_mem_nat2 l (l.head hnil) hmem
    have hmle := h m (List.max?_mem hm)
    simp [peak, hm]
    exact hmle

theorem length_foldl_assign : ∀ (l loads : List Nat), (l.foldl assign loads).length = loads.length := by
  intro l
  induction l with
  | nil => intro loads; rfl
  | cons s js ih =>
      intro loads
      simp only [List.foldl_cons]
      rw [ih]
      simp [assign]

theorem block_schedule_bound_aux : ∀ n : Nat, ∀ (steps : List Nat), steps.length = n →
    0 < n → ∀ (w : Nat), 0 < w → ∀ (loads : List Nat) (C : Nat), loads.length = w →
    (∀ k (_hk : k < w), (loads[k]?).getD 0 ≤ C) →
    ∀ k (_hk : k < w), ((steps.foldl assign loads)[k]?).getD 0 ≤ C + ticks w steps := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro steps hsteps hpos w hw loads C hlen hbound k hk
      by_cases hnil : steps = []
      · subst steps
        have hdiv : (w - 1) / w = 0 := Nat.div_eq_of_lt (by omega)
        simpa [ticks, waves, hdiv] using hbound k hk
      · have hlenpos : 0 < steps.length := List.length_pos_iff.mpr hnil
        let block := steps.take w
        let rest := steps.drop w
        have hblocklen : block.length ≤ w := by
          dsimp [block]
          exact List.length_take_le w steps
        have hblockbound : ∀ s ∈ block, s ≤ peak block := by
          intro s hs
          exact le_peak block s hs
        have htrace := assignTraceC_bound loads [] block w C (peak block) hlen hw (by simpa using hblocklen)
          (by intro j hj hjnot; exact hbound j hj)
          (by intro j hj; exact Nat.le_trans (hbound j hj) (Nat.le_add_right C (peak block)))
          hblockbound
        dsimp at htrace
        rcases htrace with ⟨hloads1, _, _⟩
        let loads1 := block.foldl assign loads
        have hlen1 : loads1.length = w := by
          dsimp [loads1]
          rw [length_foldl_assign, hlen]
        have hbound1 : ∀ j (hj : j < w), (loads1[j]?).getD 0 ≤ C + peak block := by
          intro j hj
          have hs := hloads1 j hj
          simpa [loads1, assignTraceC_fst] using hs
        have htake : steps = block ++ rest := by
          dsimp [block, rest]
          exact (List.take_append_drop w steps).symm
        have hfold : steps.foldl assign loads = rest.foldl assign loads1 := by
          rw [htake]
          dsimp [loads1]
          rw [List.foldl_append]
        by_cases hrestnil : rest = []
        · rw [hrestnil] at hfold
          simp at hfold
          have hticks : ticks w steps = peak block := by
            unfold ticks
            rw [waves_take_drop steps w hw hnil]
            have hdiv : (w - 1) / w = 0 := Nat.div_eq_of_lt (by omega)
            simp [block, rest, hrestnil, waves, hdiv]
          rw [hfold, hticks]
          exact hbound1 k hk
        · have hrestpos : 0 < rest.length := List.length_pos_iff.mpr hrestnil
          have hrestlen : rest.length < n := by
            rw [← hsteps]
            dsimp [rest]
            rw [List.length_drop]
            exact Nat.sub_lt hlenpos hw
          have hrest := ih rest.length hrestlen rest rfl hrestpos w hw loads1 (C + peak block) hlen1 hbound1 k hk
          rw [hfold]
          have hticks : ticks w steps = peak block + ticks w rest := by
            unfold ticks
            rw [waves_take_drop steps w hw hnil]
            simp [block, rest]
          rw [hticks]
          omega

theorem sound : Goal := by
  intro steps w hw
  unfold refill
  by_cases hnil : steps = []
  · subst steps
    have hdiv : (w - 1) / w = 0 := Nat.div_eq_of_lt (by omega)
    simp [ticks, waves, hdiv]
    apply Nat.le_antisymm
    · apply peak_le_of_forall
      intro v hv
      rw [List.mem_replicate] at hv
      rw [hv.2]
      omega
    · have hmem : (0 : Nat) ∈ List.replicate w 0 := by
        rw [List.mem_replicate]
        exact ⟨by omega, rfl⟩
      exact le_peak (List.replicate w 0) 0 hmem
  · have hpos : 0 < steps.length := List.length_pos_iff.mpr hnil
    have hbound := block_schedule_bound_aux steps.length steps rfl hpos w hw (List.replicate w 0) 0 (by simp) (by
      intro k hk
      rw [List.getElem?_replicate]
      simp [hk])
    apply peak_le_of_forall
    intro v hv
    rw [List.mem_iff_getElem] at hv
    obtain ⟨k, hk⟩ := hv
    have hlen : (steps.foldl assign (List.replicate w 0)).length = w := by
      rw [length_foldl_assign]
      simp
    have hklt : k < w := by simpa [hlen] using hk.1
    have hb := hbound k hklt
    have hkeq : ((steps.foldl assign (List.replicate w 0))[k]?).getD 0 =
        (steps.foldl assign (List.replicate w 0))[k] := by
      rw [List.getElem?_eq_getElem (by simpa [hlen] using hk.1)]
      rfl
    rw [← hk.2]
    simpa [hkeq] using hb

end Nov26.Proof_DivergentWaves_RefillNoBarrierDominance
