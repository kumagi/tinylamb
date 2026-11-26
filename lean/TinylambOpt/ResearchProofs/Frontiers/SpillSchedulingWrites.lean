import TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `SingleSlot` — `∀ t state, legal t 1 state → saved t state ≤ selectArcs ((arcs
  t).mergeSort (fun a b => a.2 ≤ b.2)) 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingWrites

open TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

abbrev Arc := Nat × Nat

private theorem boolLe_trans (a b c : Nat) (hab : decide (a ≤ b) = true) (hbc : decide (b ≤ c) = true) :
    decide (a ≤ c) = true := by
  apply decide_eq_true
  exact Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc)

private theorem boolLe_total (a b : Nat) : (decide (a ≤ b) || decide (b ≤ a)) = true := by
  by_cases h : a ≤ b
  · simp [h]
  · have hba : b ≤ a := by omega
    simp [h, hba]

private theorem arcLe_trans (a b c : Arc) (hab : decide (a.2 ≤ b.2) = true)
    (hbc : decide (b.2 ≤ c.2) = true) : decide (a.2 ≤ c.2) = true :=
  boolLe_trans a.2 b.2 c.2 hab hbc

private theorem arcLe_total (a b : Arc) : (decide (a.2 ≤ b.2) || decide (b.2 ≤ a.2)) = true :=
  boolLe_total a.2 b.2

private theorem sorted_arcs_pairwise (t : Trace) :
    ((arcs t).mergeSort (fun a b : Arc => decide (a.2 ≤ b.2))).Pairwise (fun a b => a.2 ≤ b.2) := by
  apply (List.pairwise_mergeSort arcLe_trans arcLe_total (arcs t)).imp
  intro a b h
  exact of_decide_eq_true h

private def Sep (chosen : List Arc) : Prop :=
  ∀ e1 ∈ chosen, ∀ e2 ∈ chosen, e1 ≠ e2 → e1.2 ≤ e2.1 ∨ e2.2 ≤ e1.1

private def Good (es : List Arc) (endAt : Nat) (chosen : List Arc) : Prop :=
  chosen.Nodup ∧ (∀ e ∈ chosen, e ∈ es) ∧
    (∀ e ∈ chosen, endAt ≤ e.1) ∧ Sep chosen

private theorem head_end_le_of_pairwise {e : Arc} {rest : List Arc}
    (h : (e :: rest).Pairwise (fun a b : Arc => a.2 ≤ b.2)) : ∀ f ∈ rest, e.2 ≤ f.2 := by
  intro f hf
  exact List.rel_of_pairwise_cons h hf

private theorem exists_min_end (l : List Arc) (hne : l ≠ []) :
    ∃ e ∈ l, ∀ f ∈ l, e.2 ≤ f.2 := by
  induction l with
  | nil => exact (hne rfl).elim
  | cons e rest ih =>
      by_cases hr : rest = []
      · refine ⟨e, by simp, ?_⟩
        intro f hf
        simp [hr] at hf
        subst f
        exact Nat.le_refl _
      · obtain ⟨m, hm, hmin⟩ := ih hr
        by_cases hem : e.2 ≤ m.2
        · refine ⟨e, by simp, ?_⟩
          intro f hf
          simp only [List.mem_cons] at hf
          rcases hf with rfl | hf
          · exact Nat.le_refl _
          · exact Nat.le_trans hem (hmin f hf)
        · have hme : m.2 ≤ e.2 := by omega
          refine ⟨m, by simp [hm], ?_⟩
          intro f hf
          simp only [List.mem_cons] at hf
          rcases hf with rfl | hf
          · exact hme
          · exact hmin f hf

private theorem not_mem_erase_self_of_nodup {e : Arc} {l : List Arc} (hnd : l.Nodup) :
    e ∉ l.erase e := by
  induction l with
  | nil => simp
  | cons a rest ih =>
      rw [List.nodup_cons] at hnd
      by_cases hae : a = e
      · subst a
        simpa using hnd.1
      · have hne : ¬(a == e) = true := by simpa using hae
        rw [List.erase_cons_tail hne]
        simp only [List.mem_cons]
        intro h
        rcases h with h | h
        · exact hae h.symm
        · exact ih hnd.2 h

private theorem ne_of_mem_erase {e f : Arc} {l : List Arc} (hnd : l.Nodup) (hf : f ∈ l.erase e) : f ≠ e := by
  intro hfe
  subst f
  exact not_mem_erase_self_of_nodup hnd hf

private theorem length_erase_of_mem {e : Arc} {l : List Arc} (hmem : e ∈ l) :
    (l.erase e).length = l.length - 1 := by
  simp [hmem]

private theorem greedy_opt : ∀ (es : List Arc) (endAt : Nat),
    es.Pairwise (fun a b : Arc => a.2 ≤ b.2) →
    (∀ e ∈ es, e.1 < e.2) →
    ∀ chosen, Good es endAt chosen → chosen.length ≤ selectArcs es endAt := by
  intro es
  induction es with
  | nil =>
      intro endAt hp hlt chosen hgood
      rcases hgood with ⟨hnd, hmem, _, _⟩
      have : chosen = [] := by
        cases chosen with
        | nil => rfl
        | cons e rest =>
            have : e ∈ ([] : List Arc) := hmem e (by simp)
            simp at this
      simp [this, selectArcs]
  | cons e0 rest ih =>
      intro endAt hp hlt chosen hgood
      rcases e0 with ⟨a, b⟩
      have hp' : rest.Pairwise (fun a b : Arc => a.2 ≤ b.2) := by
        cases hp with | cons _ h => exact h
      have hlt' : ∀ e ∈ rest, e.1 < e.2 := by
        intro e he; exact hlt e (by simp [he])
      have hhead : ∀ f ∈ rest, b ≤ f.2 := head_end_le_of_pairwise hp
      rcases hgood with ⟨hnd, hmem, hstart, hsep⟩
      by_cases htake : endAt ≤ a
      · simp [selectArcs, htake]
        by_cases hfirst : (a, b) ∈ chosen
        · let chosen' := chosen.erase (a, b)
          have hnd' : chosen'.Nodup := hnd.erase _
          have hmem' : ∀ f ∈ chosen', f ∈ rest := by
            intro f hf
            have hfchosen : f ∈ chosen := List.mem_of_mem_erase hf
            have hfmem := hmem f hfchosen
            simp only [List.mem_cons] at hfmem
            rcases hfmem with hEq | hfrest
            · exact ((ne_of_mem_erase hnd hf) hEq).elim
            · exact hfrest
          have hstart' : ∀ f ∈ chosen', b ≤ f.1 := by
            intro f hf
            have hfchosen : f ∈ chosen := List.mem_of_mem_erase hf
            have hsep_f := hsep (a, b) hfirst f hfchosen ((ne_of_mem_erase hnd hf).symm)
            rcases hsep_f with hsep_f | hsep_f
            · exact hsep_f
            · have hb2 : b ≤ f.2 := hhead f (hmem' f hf)
              have hab : a < b := hlt (a, b) (by simp)
              omega
          have hsep' : Sep chosen' := by
            intro e1 he1 e2 he2 hne
            exact hsep e1 (List.mem_of_mem_erase he1) e2 (List.mem_of_mem_erase he2) hne
          have hab : a < b := hlt (a, b) (by simp)
          have hih := ih b hp' hlt' chosen' ⟨hnd', by
            intro f hf; exact hmem' f hf, hstart', hsep'⟩
          have hpos : 0 < chosen.length := List.length_pos_of_mem hfirst
          have hlen : chosen'.length = chosen.length - 1 := length_erase_of_mem hfirst
          omega
        · by_cases hempty : chosen = []
          · simp [hempty]
          · obtain ⟨m, hm, hmin⟩ := exists_min_end chosen hempty
            rcases m with ⟨c, d⟩
            have hm_in_es : (c, d) ∈ (a, b) :: rest := hmem (c, d) hm
            have hbd : b ≤ d := by
              simp only [List.mem_cons] at hm_in_es
              rcases hm_in_es with hEq | hrest
              · cases hEq; exact Nat.le_refl _
              · exact hhead (c, d) hrest
            have hab : a < b := hlt (a, b) (by simp)
            let chosen' := ((a, b) :: chosen.erase (c, d))
            have hcd_ne : (c, d) ≠ (a, b) := by
              intro h; exact hfirst (by simpa [h] using hm)
            have hnd' : chosen'.Nodup := by
              dsimp [chosen']
              rw [List.nodup_cons]
              constructor
              · intro h
                exact hfirst (List.mem_of_mem_erase h)
              · exact hnd.erase _
            have hstart' : ∀ f ∈ chosen', endAt ≤ f.1 := by
              intro f hf
              dsimp [chosen'] at hf
              simp only [List.mem_cons] at hf
              rcases hf with rfl | hf
              · exact htake
              · exact hstart f (List.mem_of_mem_erase hf)
            have hsep' : Sep chosen' := by
              intro e1 he1 e2 he2 hne
              dsimp [chosen'] at he1 he2
              simp only [List.mem_cons] at he1 he2
              rcases he1 with rfl | he1
              · rcases he2 with rfl | he2
                · exact (hne rfl).elim
                · have he2c : e2 ∈ chosen := List.mem_of_mem_erase he2
                  have hsep_e := hsep (c, d) hm e2 he2c ((ne_of_mem_erase hnd he2).symm)
                  rcases hsep_e with hde | hed
                  · left; exact Nat.le_trans hbd hde
                  · have hcd : c < d := hlt (c, d) (hmem (c, d) hm)
                    have hd_le_e2 : d ≤ e2.2 := hmin e2 he2c
                    omega
              · rcases he2 with rfl | he2
                · have he1c : e1 ∈ chosen := List.mem_of_mem_erase he1
                  have hsep_e := hsep e1 he1c (c, d) hm (ne_of_mem_erase hnd he1)
                  rcases hsep_e with hsep_e1c | hd_le_e1
                  · have hcd : c < d := hlt (c, d) (hmem (c, d) hm)
                    have hd_le_e1s : d ≤ e1.2 := hmin e1 he1c
                    omega
                  · right; exact Nat.le_trans hbd hd_le_e1
                · exact hsep e1 (List.mem_of_mem_erase he1) e2 (List.mem_of_mem_erase he2) hne
            have hmem' : ∀ f ∈ chosen', f ∈ (a, b) :: rest := by
              intro f hf
              dsimp [chosen'] at hf
              simp only [List.mem_cons] at hf
              rcases hf with rfl | hf
              · exact List.mem_cons_self
              · have hfc : f ∈ chosen := List.mem_of_mem_erase hf
                have hfes := hmem f hfc
                simp only [List.mem_cons] at hfes
                rcases hfes with hEq | hrest
                · exact (hfirst (by simpa [hEq] using hfc)).elim
                · exact List.mem_cons_of_mem _ hrest
            let chosen'' := chosen.erase (c, d)
            have hnd'' : chosen''.Nodup := hnd.erase _
            have hmem'' : ∀ f ∈ chosen'', f ∈ rest := by
              intro f hf
              have hfc : f ∈ chosen := List.mem_of_mem_erase hf
              have hfes := hmem f hfc
              simp only [List.mem_cons] at hfes
              rcases hfes with hEq | hrest
              · exact (hfirst (by simpa [hEq] using hfc)).elim
              · exact hrest
            have hstart'' : ∀ f ∈ chosen'', b ≤ f.1 := by
              intro f hf
              have hfc : f ∈ chosen := List.mem_of_mem_erase hf
              have hsep_e := hsep (c, d) hm f hfc ((ne_of_mem_erase hnd hf).symm)
              rcases hsep_e with hde | hfe
              · exact Nat.le_trans hbd hde
              · have hcd : c < d := hlt (c, d) (hmem (c, d) hm)
                have hd_le_f : d ≤ f.2 := hmin f hfc
                omega
            have hsep'' : Sep chosen'' := by
              intro e1 he1 e2 he2 hne
              exact hsep e1 (List.mem_of_mem_erase he1) e2 (List.mem_of_mem_erase he2) hne
            have hih := ih b hp' hlt' chosen'' ⟨hnd'', hmem'', hstart'', hsep''⟩
            have hpos : 0 < chosen.length := List.length_pos_of_mem hm
            have hlen : chosen''.length = chosen.length - 1 := length_erase_of_mem hm
            omega
      · have hskip : a < endAt := by omega
        have hmem_rest : ∀ f ∈ chosen, f ∈ rest := by
          intro f hf
          have hfes := hmem f hf
          simp only [List.mem_cons] at hfes
          rcases hfes with hEq | hrest
          · subst f
            exact (htake (hstart (a, b) hf)).elim
          · exact hrest
        have hih := ih endAt hp' hlt' chosen ⟨hnd, hmem_rest, hstart, hsep⟩
        simpa [selectArcs, htake] using hih




private def lastOcc : Nat → List Nat → Option Nat
  | _, [] => none
  | p, x :: xs => match lastOcc p xs with
      | some j => some (j + 1)
      | none => if x == p then some 0 else none

private theorem lastOcc_specs (p : Nat) : ∀ l : List Nat,
    (∀ j, lastOcc p l = some j →
      j < l.length ∧ l[j]? = some p ∧
        (∀ m, j < m → m < l.length → l[m]? ≠ some p)) ∧
    (p ∈ l → ∃ j, lastOcc p l = some j) := by
  intro l
  induction l with
  | nil => simp [lastOcc]
  | cons x xs ih =>
      by_cases hex : ∃ j0, lastOcc p xs = some j0
      · obtain ⟨j0, hj0⟩ := hex
        have hdef : lastOcc p (x :: xs) = some (j0 + 1) := by simp [lastOcc, hj0]
        constructor
        · intro j hj
          rw [hdef] at hj
          cases hj
          obtain ⟨hjlen, hget, hno⟩ := ih.1 j0 hj0
          refine ⟨by simpa using hjlen, ?_, ?_⟩
          · simpa using hget
          · intro m hm hmlen
            cases m with
            | zero => omega
            | succ m' =>
                simp only [List.getElem?_cons_succ]
                exact hno m' (by omega) (by simpa using hmlen)
        · intro hmem
          exact ⟨j0 + 1, hdef⟩
      · have hnone : lastOcc p xs = none := by
          cases h : lastOcc p xs with
          | none => rfl
          | some j => exact (hex ⟨j, h⟩).elim
        constructor
        · intro j hj
          have hEqs : x = p ∧ 0 = j := by simpa [lastOcc, hnone] using hj
          have hx' : x = p := hEqs.1
          subst x
          have hdef : lastOcc p (p :: xs) = some 0 := by simp [lastOcc, hnone]
          rw [hdef] at hj
          cases hj
          refine ⟨by simp, by simp, ?_⟩
          intro m h0 hmlen
          cases m with
          | zero => omega
          | succ m' =>
              simp only [List.getElem?_cons_succ]
              intro hget
              have hmems : p ∈ xs := by
                rw [List.mem_iff_getElem]
                exact ⟨m', by simpa using hmlen, (List.getElem?_eq_some_iff.mp hget).choose_spec⟩
              obtain ⟨j, hj⟩ := ih.2 hmems
              simp [hnone] at hj
        · intro hmem
          simp only [List.mem_cons] at hmem
          rcases hmem with hxp | hmemxs
          · have hxp' : x = p := hxp.symm
            subst x
            exact ⟨0, by simp [lastOcc, hnone]⟩
          · obtain ⟨j, hj⟩ := ih.2 hmemxs
            simp [hnone] at hj

private def prevOcc (t : Trace) (i : Nat) (p : Nat) : Option Nat := lastOcc p (t.take i)

private theorem prevOcc_spec {t : Trace} {i p j : Nat} (hi : i ≤ t.length)
    (h : prevOcc t i p = some j) :
    j < i ∧ t[j]? = some p ∧ (∀ m, j < m → m < i → t[m]? ≠ some p) := by
  unfold prevOcc at h
  obtain ⟨hjlen, hget, hno⟩ := (lastOcc_specs p (t.take i)).1 j h
  have htilen : (t.take i).length = i := by simp [List.length_take, Nat.min_eq_left hi]
  have hjlt : j < i := by simpa [htilen] using hjlen
  refine ⟨hjlt, ?_, ?_⟩
  · rw [List.getElem?_take] at hget
    simpa [ite_eq_left hjlt] using hget
  · intro m hjm hmi hmget
    apply hno m hjm (by simpa [htilen] using hmi)
    rw [List.getElem?_take]
    simpa [ite_eq_left hmi] using hmget

private theorem prevOcc_of_mem {t : Trace} {i p : Nat} (_hi : i ≤ t.length)
    (hm : p ∈ t.take i) : ∃ j, prevOcc t i p = some j := by
  unfold prevOcc
  exact (lastOcc_specs p (t.take i)).2 hm

private theorem arc_of_prev (t : Trace) {i j p : Nat} (hi : i < t.length) (hji : j < i)
    (htj : t[j]? = some p) (hti : t[i]? = some p)
    (hno : ∀ m, j < m → m < i → t[m]? ≠ some p) : (j, i) ∈ arcs t := by
  have hji1 : j + 1 ≤ i := by omega
  have hdrop_len : (t.drop (j+1)).length = t.length - (j+1) := by simp
  have hidx_get : (t.drop (j+1))[i - (j+1)]? = some p := by
    rw [List.getElem?_drop]
    have hsum : j + 1 + (i - (j+1)) = i := by omega
    simpa [hsum] using hti
  have hidxlt : i - (j+1) < (t.drop (j+1)).length := by
    exact (List.getElem?_eq_some_iff.mp hidx_get).choose
  have hidx_eq : (t.drop (j+1)).idxOf? p = some (i - (j+1)) := by
    apply List.idxOf?_eq_some_iff.mpr
    refine ⟨hidxlt, (List.getElem?_eq_some_iff.mp hidx_get).choose_spec, ?_⟩
    intro k hk hget
    have hklt : j + 1 + k < i := by omega
    have htget : t[j + 1 + k]? = some p := by
      rw [← List.getElem?_drop]
      exact List.getElem?_eq_some_iff.mpr ⟨by simpa [hdrop_len] using (show k < t.length - (j+1) by omega), hget⟩
    exact hno (j + 1 + k) (by omega) hklt htget
  have hzip_mem : (p, j) ∈ t.zipIdx := by
    rw [List.mem_iff_getElem]
    refine ⟨j, by simpa using (Nat.lt_trans hji hi), ?_⟩
    have hget : (t.zipIdx)[j]? = some (p, j) := by
      rw [List.getElem?_zipIdx]
      simp [htj]
    exact (List.getElem?_eq_some_iff.mp hget).choose_spec
  apply List.mem_filterMap.mpr
  refine ⟨(p, j), hzip_mem, ?_⟩
  simp only [hidx_eq, Option.map_some]
  congr <;> omega


private theorem mem_state_exists_prev (t : Trace) (state : Nat → List Nat)
    (hl : legal t 1 state) :
    ∀ i, i < t.length → ∀ p, p ∈ state i → ∃ k, k < i ∧ t[k]? = some p := by
  intro i
  induction i with
  | zero =>
      intro hi p hp
      have h0 : state 0 = [] := hl.1
      simp [h0] at hp
  | succ i ih =>
      intro hi p hp
      have hi' : i < t.length := by omega
      have hstep := (hl.2 i hi').2
      rcases hstep p hp with hprev | hacc
      · obtain ⟨k, hk, hpk⟩ := ih hi' p hprev
        exact ⟨k, by omega, hpk⟩
      · exact ⟨i, by omega, hacc⟩

private theorem mem_state_between (t : Trace) (state : Nat → List Nat)
    (hl : legal t 1 state) :
    ∀ i, i < t.length → ∀ j p, j < i → t[j]? = some p →
      (∀ m, j < m → m < i → t[m]? ≠ some p) →
      p ∈ state i → ∀ m, j < m → m ≤ i → p ∈ state m := by
  intro i
  induction i with
  | zero =>
      intro hi j p hji
      omega
  | succ i ih =>
      intro hi j p hji htj hno hin m hjm hmi
      by_cases hm : m = i + 1
      · subst m
        exact hin
      · have hmi' : m ≤ i := by omega
        have hi' : i < t.length := by omega
        rcases (hl.2 i hi').2 p hin with hprev | hacc
        · exact ih hi' j p (by omega) htj (by
              intro k hjk hki
              exact hno k hjk (by omega)) hprev m hjm hmi'
        · have hji' : j < i := by omega
          exact (hno i hji' (by omega) hacc).elim

private def hitArcs (t : Trace) (state : Nat → List Nat) : List Arc :=
  t.zipIdx.filterMap (fun x =>
    if (state x.2).contains x.1 then (prevOcc t x.2 x.1).map (fun j => (j, x.2)) else none)

private theorem length_filterMap_if_eq_countP {α β : Type} (l : List α)
    (g : α → Option β) (b : α → Bool)
    (h : ∀ a ∈ l, b a = true → (g a).isSome = true) :
    (l.filterMap (fun a => if b a then g a else none)).length = l.countP b := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hba : b a = true
      · have hga : (g a).isSome = true := h a (by simp) hba
        cases hgc : g a with
        | none => simp [hgc] at hga
        | some v =>
            have hl : ∀ a' ∈ l, b a' = true → (g a').isSome = true := by
              intro a' ha' hb'
              exact h a' (by simp [ha']) hb'
            have ih' := ih hl
            simp [hba, hgc, ih']
      · have hl : ∀ a' ∈ l, b a' = true → (g a').isSome = true := by
          intro a' ha' hb'
          exact h a' (by simp [ha']) hb'
        have ih' := ih hl
        simp [hba, ih']

private theorem hitArcs_length (t : Trace) (state : Nat → List Nat)
    (hl : legal t 1 state) : (hitArcs t state).length = saved t state := by
  unfold hitArcs saved
  apply length_filterMap_if_eq_countP
  intro x hx hhit
  rcases x with ⟨p, i⟩
  have hidx := List.mem_zipIdx hx
  have hi_le : i ≤ t.length := by omega
  have hp_state : p ∈ state i := of_decide_eq_true hhit
  obtain ⟨k, hk, hpk⟩ := mem_state_exists_prev t state hl i (by omega) p hp_state
  have hmem_take : p ∈ t.take i := by
    rw [List.mem_iff_getElem]
    refine ⟨k, by simpa [List.length_take, Nat.min_eq_left hi_le] using hk, ?_⟩
    have htake_get : (t.take i)[k]? = some p := by
      rw [List.getElem?_take]
      simp [ite_eq_left hk]
      exact hpk
    exact (List.getElem?_eq_some_iff.mp htake_get).choose_spec
  obtain ⟨j, hj⟩ := prevOcc_of_mem hi_le hmem_take
  simp [hj]

private theorem nodup_zipIdx_filterMapAux (t : Trace) (base : Nat)
    (f : Nat → Nat → Option Arc)
    (hsnd : ∀ p i e, f p i = some e → e.2 = i) :
    ((t.zipIdx base).filterMap (fun x => f x.1 x.2)).Nodup := by
  induction t generalizing base with
  | nil => simp
  | cons p ps ih =>
      simp only [List.zipIdx_cons]
      cases hf0 : f p base with
      | none =>
          simp [hf0]
          exact ih (base + 1)
      | some e0 =>
          simp [hf0]
          constructor
          · intro q i hzip hfeq
            have hbase_lt_i : base < i := (List.mem_zipIdx hzip).1
            have htail : e0.2 = i := hsnd q i e0 hfeq
            have hhead : e0.2 = base := hsnd p base e0 hf0
            omega
          · exact ih (base + 1)

private theorem hitArcs_nodup (t : Trace) (state : Nat → List Nat) :
    (hitArcs t state).Nodup := by
  unfold hitArcs
  apply nodup_zipIdx_filterMapAux t 0 (fun p i =>
    if (state i).contains p then (prevOcc t i p).map (fun j => (j, i)) else none)
  intro p i e he
  by_cases hhit : (state i).contains p = true
  · have heq0 := he
    rw [hhit] at heq0
    have heq : (prevOcc t i p).map (fun j => (j, i)) = some e := by simpa using heq0
    cases hprev : prevOcc t i p with
    | none => simp [hprev] at heq
    | some j =>
        simp [hprev] at heq
        simp [← heq]
  · have hb : (state i).contains p = false := by
      cases h : (state i).contains p <;> simp_all
    have heq0 := he
    rw [hb] at heq0
    simp at heq0

private theorem hitArcs_spec_of_mem (t : Trace) (state : Nat → List Nat)
    (_hl : legal t 1 state) {e : Arc} (he : e ∈ hitArcs t state) :
    ∃ p i j, e = (j, i) ∧ i < t.length ∧ t[i]? = some p ∧ j < i ∧ t[j]? = some p ∧
      (∀ m, j < m → m < i → t[m]? ≠ some p) ∧ p ∈ state i := by
  unfold hitArcs at he
  obtain ⟨⟨p, i⟩, hzip, heq⟩ := List.mem_filterMap.mp he
  have hidx := List.mem_zipIdx hzip
  have hi : i < t.length := by omega
  have hti : t[i]? = some p := by
    have hp_eq : p = t[i] := by simpa using hidx.2.2
    have hget : t[i]? = some t[i] := List.getElem?_eq_some_iff.mpr ⟨by simpa using hi, rfl⟩
    rw [← hp_eq] at hget
    exact hget
  by_cases hhit : (state i).contains p = true
  · have hp_state : p ∈ state i := of_decide_eq_true hhit
    rw [hhit] at heq
    cases hprev : prevOcc t i p with
    | none => simp [hprev] at heq
    | some j =>
        simp [hprev] at heq
        obtain ⟨hjlt, htj, hno⟩ := prevOcc_spec (by omega : i ≤ t.length) hprev
        exact ⟨p, i, j, heq.symm, hi, hti, hjlt, htj, hno, hp_state⟩
  · have hb : (state i).contains p = false := by
      cases h : (state i).contains p <;> simp_all
    rw [hb] at heq
    simp at heq

private theorem hitArcs_mem_arcs (t : Trace) (state : Nat → List Nat)
    (hl : legal t 1 state) : ∀ e ∈ hitArcs t state, e ∈ arcs t := by
  intro e he
  obtain ⟨p, i, j, heq, hi, hti, hji, htj, hno, hpst⟩ := hitArcs_spec_of_mem t state hl he
  rw [heq]
  exact arc_of_prev t hi hji htj hti hno

private theorem hitArcs_lt (t : Trace) (state : Nat → List Nat)
    (hl : legal t 1 state) : ∀ e ∈ hitArcs t state, e.1 < e.2 := by
  intro e he
  obtain ⟨p, i, j, heq, hi, hti, hji, htj, hno, hpst⟩ := hitArcs_spec_of_mem t state hl he
  rw [heq]
  exact hji

private theorem eq_of_mem_len_le_one {α : Type} {x y : α} {l : List α}
    (hlen : l.length ≤ 1) (hx : x ∈ l) (hy : y ∈ l) : x = y := by
  cases l with
  | nil => simp at hx
  | cons a l =>
      cases l with
      | nil =>
          simp at hx hy
          subst x
          subst y
          rfl
      | cons b l =>
          simp at hlen

private theorem hitArcs_sep (t : Trace) (state : Nat → List Nat)
    (hl : legal t 1 state) : Sep (hitArcs t state) := by
  intro e1 he1 e2 he2 hne
  obtain ⟨p1, i1, j1, he1eq, hi1, hti1, hj1i1, htj1, hno1, hpst1⟩ := hitArcs_spec_of_mem t state hl he1
  obtain ⟨p2, i2, j2, he2eq, hi2, hti2, hj2i2, htj2, hno2, hpst2⟩ := hitArcs_spec_of_mem t state hl he2
  rw [he1eq, he2eq]
  by_cases hcase : i1 ≤ j2 ∨ i2 ≤ j1
  · exact hcase
  have hj2i1 : j2 < i1 := by omega
  have hj1i2 : j1 < i2 := by omega
  let m := max j1 j2 + 1
  have hj1m : j1 < m := by dsimp [m]; omega
  have hj2m : j2 < m := by dsimp [m]; omega
  have hm_le_i1 : m ≤ i1 := by dsimp [m]; omega
  have hm_le_i2 : m ≤ i2 := by dsimp [m]; omega
  have hp1m : p1 ∈ state m := mem_state_between t state hl i1 hi1 j1 p1 hj1i1 htj1 hno1 hpst1 m hj1m hm_le_i1
  have hp2m : p2 ∈ state m := mem_state_between t state hl i2 hi2 j2 p2 hj2i2 htj2 hno2 hpst2 m hj2m hm_le_i2
  have hlen_state : (state m).length ≤ 1 := (hl.2 (m - 1) (by omega)).1
  have hp_eq : p1 = p2 := eq_of_mem_len_le_one hlen_state hp1m hp2m
  subst p2
  have hj_eq : j1 = j2 := by
    by_cases hlt : j1 < j2
    · exact (hno1 j2 hlt hj2i1 htj2).elim
    · by_cases hgt : j2 < j1
      · exact (hno2 j1 hgt hj1i2 htj1).elim
      · omega
  subst j2
  have hi_eq : i1 = i2 := by
    by_cases hlt : i1 < i2
    · exact (hno2 i1 (by omega) (by omega) hti1).elim
    · by_cases hgt : i2 < i1
      · exact (hno1 i2 (by omega) (by omega) hti2).elim
      · omega
  subst i2
  exact False.elim (hne (by rw [he1eq, he2eq]))

theorem SingleSlot_sound : TinylambOpt.ResearchGoals.Frontiers.SpillScheduling.SingleSlot.Goal := by
  intro t state hl
  let chosen := hitArcs t state
  let sarcs := (arcs t).mergeSort (fun a b : Arc => decide (a.2 ≤ b.2))
  have hgood : Good sarcs 0 chosen := by
    refine ⟨hitArcs_nodup t state, ?_, ?_, hitArcs_sep t state hl⟩
    · intro e he
      exact (List.mem_mergeSort (le := fun a b : Arc => decide (a.2 ≤ b.2))).2 (hitArcs_mem_arcs t state hl e he)
    · intro e he
      omega
  have hsorted : sarcs.Pairwise (fun a b : Arc => a.2 ≤ b.2) := by
    dsimp [sarcs]
    exact sorted_arcs_pairwise t
  have hlt : ∀ e ∈ sarcs, e.1 < e.2 := by
    intro e he
    have hearcs : e ∈ arcs t := (List.mem_mergeSort (le := fun a b : Arc => decide (a.2 ≤ b.2))).1 (by simpa [sarcs] using he)
    rcases List.mem_filterMap.mp hearcs with ⟨⟨p, i⟩, hzip, heq⟩
    have hidx := List.mem_zipIdx hzip
    cases hnext : (t.drop (i + 1)).idxOf? p with
    | none => simp [hnext] at heq
    | some k =>
        simp [hnext] at heq
        rw [← heq]
        omega
  have hgreedy := greedy_opt sarcs 0 hsorted hlt chosen hgood
  rw [← hitArcs_length t state hl]
  exact hgreedy

/-! `MinimumWrites.Goal` (Huffman optimality) is left unproven. The missing
lemma is a formal Huffman exchange: for a tree with at least two leaves, a
deepest sibling-leaf pair can be relabelled with the two lightest leaves
without increasing `writes`, via the standard deepest-sibling exchange
argument.  Its proof needs: sibling-pair invariance under leaf swaps, a
swap-cost lemma for the weighted external path length, and the identity
`huffman (l.length) l = x + y + huffman (l.length - 1) ((x+y)::rest)` when
`x,y` are the two lightest weights of `l`. -/

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingWrites
