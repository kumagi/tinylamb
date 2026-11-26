import TinylambOpt.ResearchGoals.JitFrontiers.BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState

theorem refRun_cons (p : Policy) (s : State) (r : Row) (rs : List Row) :
    refRun p s (r :: rs) = refRun p (step p s r) rs := by
  simp [refRun]

theorem refRun_append (p : Policy) (s : State) (l₁ l₂ : List Row) :
    refRun p s (l₁ ++ l₂) = refRun p (refRun p s l₁) l₂ := by
  simp [refRun, List.foldl_append]

theorem getElem?_head? (l : List Row) (k : Nat) : l[k]? = (l.drop k).head? := by
  rw [List.head?_eq_getElem?, List.getElem?_drop]
  simp

theorem drive_spec (p : Policy) (cap : Nat) : ∀ (i : Nat) (s : State) (rs : List Row),
    ∃ k, k ≤ rs.length ∧
      drive p cap i s rs = ⟨i + k, refRun p s (rs.take k)⟩ ∧
      (k = rs.length ∨
        canCommit p cap (refRun p s (rs.take k)) ((rs.drop k).head?.getD (0, none)) = false) := by
  intro i s rs
  induction rs generalizing i s with
  | nil =>
      refine ⟨0, by simp, ?_, ?_⟩
      · simp [drive, refRun]
      · simp
  | cons r rs ih =>
      by_cases h : canCommit p cap s r
      · obtain ⟨k, hklen, hdrive, hbail⟩ := ih (i + 1) (step p s r)
        refine ⟨k + 1, ?_, ?_, ?_⟩
        · simp only [List.length_cons]
          omega
        · simp [drive, h]
          rw [hdrive]
          congr 1
          omega
        · simpa [refRun_cons] using hbail
      · refine ⟨0, by simp, ?_, ?_⟩
        · simp [drive, h, refRun]
        · have hfalse : canCommit p cap s r = false := by
            cases hc : canCommit p cap s r <;> simp_all
          simpa [refRun] using hfalse

theorem drive_cursor_ge (p : Policy) (cap i : Nat) (s : State) (rs : List Row) :
    i ≤ (drive p cap i s rs).cursor := by
  obtain ⟨k, _, hk, _⟩ := drive_spec p cap i s rs
  simpa [hk] using Nat.le_add_right i k

theorem run_spec (p : Policy) (cap : Nat) (s : State) (rs : List Row) :
    ∃ k, k ≤ rs.length ∧
      run p cap s rs = ⟨k, refRun p s (rs.take k)⟩ ∧
      (k = rs.length ∨
        canCommit p cap (refRun p s (rs.take k)) ((rs.drop k).head?.getD (0, none)) = false) := by
  simpa [run] using drive_spec p cap 0 s rs

theorem committedPrefix_of (p : Policy) : committedPrefix p := by
  intro cap s rs
  obtain ⟨k, hklen, hrun, _⟩ := run_spec p cap s rs
  constructor
  · rw [hrun]
    simpa using hklen
  · simpa [hrun]

theorem restartRow_of (p : Policy) : restartRow p := by
  intro cap s rs
  obtain ⟨k, hklen, hrun, _⟩ := run_spec p cap s rs
  unfold resume
  rw [hrun]
  simp only
  rw [← refRun_append]
  rw [List.take_append_drop]

theorem bailReason_of (p : Policy) : bailReason p := by
  unfold bailReason
  intro cap s rs i r
  change i = (run p cap s rs).cursor → rs[i]? = some r →
    canCommit p cap (run p cap s rs).state r = false
  intro hrun hri
  obtain ⟨k, hklen, hk, hbail⟩ := run_spec p cap s rs
  have hcur : i = k := by simpa [hk] using hrun
  rw [hcur] at hri
  have hklt : k < rs.length := by
    by_cases hle : rs.length ≤ k
    · have hn : rs[k]? = none := List.getElem?_eq_none_iff.mpr hle
      rw [hn] at hri
      cases hri
    · omega
  rcases hbail with hkeq | hb
  · omega
  · have hhead : (rs.drop k).head? = some r := by
      rw [← getElem?_head?]
      exact hri
    rw [hhead] at hb
    simpa [hk] using hb

theorem fullTableHit_of (p : Policy) : fullTableHit p := by
  intro cap s r k hhot hkey hlookup
  have hc : canCommit p cap s r = true := by
    simp [canCommit, hhot, hkey, hlookup]
  simp [run, drive, hc]

theorem canCommit_mono (p : Policy) {a b : Nat} (hab : a ≤ b) (s : State) (r : Row) :
    canCommit p a s r = true → canCommit p b s r = true := by
  intro h
  unfold canCommit at h ⊢
  cases hh : p.hot r <;> simp [hh] at h ⊢
  cases hk : p.key r with
  | none => simp
  | some k =>
      simp [hk] at h ⊢
      rcases h with hs | hlen
      · exact Or.inl hs
      · exact Or.inr (by omega)

theorem drive_shift (p : Policy) (cap : Nat) : ∀ (i : Nat) (s : State) (rs : List Row),
    (drive p cap i s rs).cursor = i + (drive p cap 0 s rs).cursor ∧
    (drive p cap i s rs).state = (drive p cap 0 s rs).state := by
  intro i s rs
  induction rs generalizing i s with
  | nil => simp [drive]
  | cons r rs ih =>
      by_cases h : canCommit p cap s r
      · have hi := ih (i + 1) (step p s r)
        have h1 := ih 1 (step p s r)
        simp [drive, h]
        constructor
        · omega
        · rw [hi.2, h1.2]
      · simp [drive, h]

theorem increaseCapacity_of (p : Policy) : increaseCapacity p := by
  unfold increaseCapacity
  intro a b s rs hab
  induction rs generalizing s with
  | nil => simp [run, drive]
  | cons r rs ih =>
      by_cases ha : canCommit p a s r
      · have hb : canCommit p b s r = true := canCommit_mono p hab s r ha
        simp [run, drive, ha, hb]
        have hla := drive_shift p a 1 (step p s r) rs
        have hlb := drive_shift p b 1 (step p s r) rs
        have ihres := ih (step p s r)
        dsimp [run] at hla hlb ihres ⊢
        omega
      · simpa [run, drive, ha] using drive_cursor_ge p b 0 s (r :: rs)

theorem chunkHandoff_of (p : Policy) : chunkHandoff p := by
  unfold chunkHandoff
  intro s a b cap
  dsimp
  intro hfull
  induction a generalizing s with
  | nil => simp [run, drive]
  | cons r a ih =>
      have hc : canCommit p cap s r = true := by
        cases hc' : canCommit p cap s r with
        | false =>
            have hcur : (run p cap s (r :: a)).cursor = 0 := by
              simp [run, drive, hc']
            simp only [List.length_cons] at hfull
            omega
        | true => rfl
      have hrun_head : run p cap s (r :: a) = drive p cap 1 (step p s r) a := by
        simp [run, drive, hc]
      have hshift := drive_shift p cap 1 (step p s r) a
      have hfull' : (run p cap (step p s r) a).cursor = a.length := by
        rw [hrun_head] at hfull
        simp only [List.length_cons] at hfull
        dsimp [run] at hshift ⊢
        omega
      have heq_state : (run p cap s (r :: a)).state = (run p cap (step p s r) a).state := by
        rw [hrun_head]
        exact hshift.2
      have ihres := ih (step p s r) hfull'
      change drive p cap 0 s ((r :: a) ++ b) =
        drive p cap (r :: a).length (run p cap s (r :: a)).state b
      rw [List.cons_append]
      simp [drive, hc]
      dsimp [run]
      have hright := drive_shift p cap (a.length + 1) (drive p cap 0 s (r :: a)).state b
      have hleft := drive_shift p cap 1 (step p s r) (a ++ b)
      have hs2 := drive_shift p cap a.length (run p cap (step p s r) a).state b
      dsimp [run] at ihres hs2
      have heq_state' :
          (drive p cap 0 s (r :: a)).state = (drive p cap 0 (step p s r) a).state := by
        simpa [run] using heq_state
      have hcur_eq :
          (drive p cap 1 (step p s r) (a ++ b)).cursor =
          (drive p cap (a.length + 1) (drive p cap 0 s (r :: a)).state b).cursor := by
        rw [hleft.1, hright.1, ihres, hs2.1]
        rw [← heq_state']
        omega
      have hst_eq :
          (drive p cap 1 (step p s r) (a ++ b)).state =
          (drive p cap (a.length + 1) (drive p cap 0 s (r :: a)).state b).state := by
        rw [hleft.2, hright.2, ihres, hs2.2]
        rw [← heq_state']
      cases h₁ : drive p cap 1 (step p s r) (a ++ b)
      cases h₂ : drive p cap (a.length + 1) (drive p cap 0 s (r :: a)).state b
      simp_all

theorem earlyAdvance_refuted (p : Policy) (r : Row)
    (hcan : canCommit p 0 [] r = false) (hstep : step p [] r ≠ []) : ¬ earlyAdvance p := by
  intro h
  have hh := h 0 [] [r]
  dsimp at hh
  have hr : run p 0 [] [r] = ⟨0, []⟩ := by
    simp [run, drive, hcan]
  rw [hr] at hh
  simp at hh
  exact hstep hh.symm

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem not_mem_erase_of_nodup : ∀ {l : List Nat}, l.Nodup → ∀ {b : Nat}, b ∈ l → b ∉ l.erase b
| [], hnd, b, hb => by cases hb
| a::l, hnd, b, hb => by
    by_cases hab : a == b
    · have hab' : a = b := beq_iff_eq.mp hab
      subst a
      rw [List.erase_cons_head]
      exact (List.nodup_cons.mp hnd).1
    · have hane : a ≠ b := by
        intro h
        subst a
        simp at hab
      have hbne : b ≠ a := fun h => hane h.symm
      have hb' : b ∈ l := by simpa [hbne] using hb
      rw [List.erase_cons_tail (by simpa using hab)]
      intro hmem
      simp at hmem
      rcases hmem with hba | hmem
      · exact hane hba.symm
      · exact (not_mem_erase_of_nodup ((List.nodup_cons.mp hnd).2) hb') hmem

theorem length_le_of_subset_nodup (l₂ : List Nat) (hnd₂ : l₂.Nodup) :
    ∀ l₁ : List Nat, l₁.Nodup → (∀ x, x ∈ l₁ → x ∈ l₂) → l₁.length ≤ l₂.length := by
  induction l₂ with
  | nil =>
      intro l₁ hnd₁ hsub
      cases l₁ with
      | nil => simp
      | cons x xs =>
          have hx := hsub x (by simp)
          simp at hx
  | cons b l₂ ih =>
      intro l₁ hnd₁ hsub
      by_cases hb : b ∈ l₁
      · have hsub' : ∀ x, x ∈ l₁.erase b → x ∈ l₂ := by
          intro x hx
          have hx₁ := List.mem_of_mem_erase hx
          have hxne : x ≠ b := by
            intro hxb
            subst x
            exact (not_mem_erase_of_nodup hnd₁ hb) hx
          have hx₂ := hsub x hx₁
          simp at hx₂
          rcases hx₂ with hxb | hx₂
          · exact False.elim (hxne hxb)
          · exact hx₂
        have ihres := ih ((List.nodup_cons.mp hnd₂).2) (l₁.erase b) (hnd₁.erase b) hsub'
        have hlen := List.length_erase_of_mem hb
        simp only [List.length_cons]
        omega
      · have hsub' : ∀ x, x ∈ l₁ → x ∈ l₂ := by
          intro x hx
          have hx₂ := hsub x hx
          simp at hx₂
          rcases hx₂ with hxb | hx₂
          · subst x
            exact False.elim (hb hx)
          · exact hx₂
        have ihres := ih ((List.nodup_cons.mp hnd₂).2) l₁ hnd₁ hsub'
        simp only [List.length_cons]
        omega

theorem strict_length_of_subset (l₂ : List Nat) (hnd₂ : l₂.Nodup) :
    ∀ (l₁ : List Nat), l₁.Nodup → ∀ (k : Nat), (∀ x, x ∈ l₁ → x ∈ l₂) → k ∈ l₂ → k ∉ l₁ →
      l₁.length < l₂.length := by
  induction l₂ with
  | nil =>
      intro l₁ hnd₁ k hsub hk hnot
      simp at hk
  | cons b l₂ ih =>
      intro l₁ hnd₁ k hsub hk hnot
      by_cases hb : b ∈ l₁
      · have hsub' : ∀ x, x ∈ l₁.erase b → x ∈ l₂ := by
          intro x hx
          have hx₁ := List.mem_of_mem_erase hx
          have hxne : x ≠ b := by
            intro hxb
            subst x
            exact (not_mem_erase_of_nodup hnd₁ hb) hx
          have hx₂ := hsub x hx₁
          simp at hx₂
          rcases hx₂ with hxb | hx₂
          · exact False.elim (hxne hxb)
          · exact hx₂
        have hk' : k ∈ l₂ := by
          have hk2 := hk
          simp at hk2
          rcases hk2 with hkb | hk2
          · subst k
            exact False.elim (hnot hb)
          · exact hk2
        have hnot' : k ∉ l₁.erase b := by
          intro hk'
          exact hnot (List.mem_of_mem_erase hk')
        have ihres := ih ((List.nodup_cons.mp hnd₂).2) (l₁.erase b) (hnd₁.erase b) k hsub' hk' hnot'
        have hlen := List.length_erase_of_mem hb
        simp only [List.length_cons]
        omega
      · have hsub' : ∀ x, x ∈ l₁ → x ∈ l₂ := by
          intro x hx
          have hx₂ := hsub x hx
          simp at hx₂
          rcases hx₂ with hxb | hx₂
          · subst x
            exact False.elim (hb hx)
          · exact hx₂
        have hle := length_le_of_subset_nodup l₂ ((List.nodup_cons.mp hnd₂).2) l₁ hnd₁ hsub'
        simp only [List.length_cons]
        omega

theorem lookup_isSome_iff_mem_fst : ∀ (s : State) (k : Nat), (s.lookup k).isSome ↔ k ∈ s.map Prod.fst
| [], k => by simp
| (a,v)::s, k => by simp [lookup_isSome_iff_mem_fst s k]

theorem map_fst_filter (s : State) (k : Nat) :
    (s.filter (fun e => e.1 != k)).map Prod.fst = (s.map Prod.fst).filter (fun x => x != k) := by
  induction s with
  | nil => simp
  | cons e s ih => by_cases h : e.1 != k <;> simp [h, ih]

theorem step_nodup (p : Policy) (s : State) (r : Row) :
    (s.map Prod.fst).Nodup → ((step p s r).map Prod.fst).Nodup := by
  intro hnd
  cases hk : p.key r with
  | none => simpa [step, hk] using hnd
  | some k =>
      simp only [step, hk, List.map_cons]
      rw [map_fst_filter]
      apply List.nodup_cons.mpr
      constructor
      · simp
      · exact hnd.filter _

theorem step_mem_fst (p : Policy) (s : State) (r : Row) (k : Nat) :
    k ∈ (step p s r).map Prod.fst → k ∈ s.map Prod.fst ∨ p.key r = some k := by
  intro hk
  cases hkey : p.key r with
  | none =>
      have hk' : k ∈ s.map Prod.fst := by simpa [step, hkey] using hk
      exact Or.inl hk'
  | some j =>
      simp only [step, hkey, List.map_cons, List.mem_cons] at hk
      rcases hk with hkj | hkf
      · right
        have hkj' : k = j := beq_iff_eq.mp (by simpa using hkj)
        simp [hkj']
      · left
        rw [map_fst_filter] at hkf
        exact (List.mem_filter.mp hkf).1

theorem length_filter_not_mem_lt (l : List Nat) (a : Nat) (ha : a ∈ l) :
    (l.filter (fun b => !b == a)).length < l.length := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        simp
        have hle := List.length_filter_le (fun b : Nat => !b == a) bs
        omega
      · have hbne : b ≠ a := by
          intro h
          subst b
          simp at hba
        have hab : a ≠ b := fun h => hbne h.symm
        have ha' : a ∈ bs := by simpa [hab] using ha
        have ihres := ih ha'
        simp [hba]
        omega

theorem nodup_eraseDups : ∀ l : List Nat, l.eraseDups.Nodup
| [] => by simp
| a::l => by
    by_cases ha : a ∈ l
    · rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · rw [List.mem_eraseDups]
        simp
      · exact nodup_eraseDups (l.filter (fun b => !b == a))
    · have hf : l.filter (fun b => !b == a) = l := by
        apply List.filter_eq_self.mpr
        intro b hb
        by_cases hba : b == a
        · have : b = a := beq_iff_eq.mp hba
          subst b
          exact False.elim (ha hb)
        · simpa using hba
      rw [List.eraseDups_cons, hf]
      apply List.nodup_cons.mpr
      constructor
      · rw [List.mem_eraseDups]
        exact ha
      · exact nodup_eraseDups l
termination_by l => l.length
decreasing_by
  · simp only [List.length_cons]
    have hlt := length_filter_not_mem_lt l a ha
    omega
  · simp only [List.length_cons]
    omega

theorem reserveExactlyAux (p : Policy) (ks : List Nat) (hksnd : ks.Nodup) :
    ∀ (i : Nat) (rs : List Row) (s : State),
      (s.map Prod.fst).Nodup →
      (∀ k, k ∈ s.map Prod.fst → k ∈ ks) →
      (∀ r ∈ rs, ∀ k, p.key r = some k → k ∈ ks) →
      rs.all p.hot = true →
      drive p ks.length i s rs = ⟨i + rs.length, refRun p s rs⟩ := by
  intro i rs
  induction rs generalizing i with
  | nil =>
      intro s hnd hsub hrow hhot
      simp [drive, refRun]
  | cons r rs ih =>
      intro s hnd hsub hrow hhot
      have hhot' := hhot
      simp at hhot'
      have hhotr : p.hot r = true := hhot'.1
      have hhotrs : rs.all p.hot = true := by simpa using hhot'.2
      have hrowrs : ∀ r' ∈ rs, ∀ k, p.key r' = some k → k ∈ ks := by
        intro r' hr'
        exact hrow r' (by simp [hr'])
      have hc : canCommit p ks.length s r = true := by
        cases hk : p.key r with
        | none => simp [canCommit, hk, hhotr]
        | some k =>
            have hkks : k ∈ ks := hrow r (by simp) k (by simp [hk])
            by_cases hkmem : k ∈ s.map Prod.fst
            · have hsome : (s.lookup k).isSome := (lookup_isSome_iff_mem_fst s k).mpr hkmem
              simp [canCommit, hk, hsome, hhotr]
            · have hslt : (s.map Prod.fst).length < ks.length :=
                strict_length_of_subset ks hksnd (s.map Prod.fst) hnd k hsub hkks hkmem
              have hslt' : s.length < ks.length := by simpa using hslt
              simp [canCommit, hk, hslt', hhotr]
      simp [drive, hc]
      have hsnd' : ((step p s r).map Prod.fst).Nodup := step_nodup p s r hnd
      have hsub' : ∀ k, k ∈ (step p s r).map Prod.fst → k ∈ ks := by
        intro k hk
        rcases step_mem_fst p s r k hk with hkold | hknew
        · exact hsub k hkold
        · exact hrow r (by simp) k hknew
      have hres := ih (i + 1) (step p s r) hsnd' hsub' hrowrs hhotrs
      simpa [refRun, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hres

theorem reserveExactly_of (p : Policy) : reserveExactly p := by
  intro rs s hsnd hhot
  let ks := keys p s rs
  have hksnd : ks.Nodup := by
    unfold ks keys
    exact nodup_eraseDups _
  have hsub : ∀ k, k ∈ s.map Prod.fst → k ∈ ks := by
    intro k hk
    unfold ks keys
    rw [List.mem_eraseDups]
    simp [hk]
  have hrow : ∀ r ∈ rs, ∀ k, p.key r = some k → k ∈ ks := by
    intro r hr k hk
    unfold ks keys
    rw [List.mem_eraseDups]
    simp only [List.mem_append, List.mem_map, List.mem_filterMap]
    right
    exact ⟨r, hr, hk⟩
  have hres := reserveExactlyAux p ks hksnd 0 rs s hsnd hsub hrow hhot
  simpa [run, ks] using hres

theorem lookup_step_same (p : Policy) (s : State) (r : Row) (k : Nat)
    (hk : p.key r = some k) :
    (step p s r).lookup k = some (p.bump (s.lookup k) r.2) := by
  induction s with
  | nil => simp [step, hk]
  | cons e s ih => simp [step, hk]

theorem lookup_step_other (p : Policy) (s : State) (r : Row) (j k : Nat)
    (hk : p.key r = some k) (hjk : j ≠ k) :
    (step p s r).lookup j = s.lookup j := by
  have hjkfalse : (j == k) = false := by
    cases h : j == k <;> simp_all
  induction s with
  | nil => simp [step, hk, hjk]
  | cons e s ih =>
      have hfilter : List.lookup j (s.filter (fun x => x.1 != k)) = List.lookup j s := by
        have hstep_eq : step p s r = (k, p.bump (s.lookup k) r.2) :: s.filter (fun x => x.1 != k) := by
          simp [step, hk]
        rw [← ih, hstep_eq, List.lookup_cons, hjkfalse]
      by_cases hek : e.1 == k
      · have hne : (e.1 != k) = false := by
          cases h : e.1 != k <;> simp_all
        have heq : e.1 = k := beq_iff_eq.mp hek
        subst heq
        simp [step, hk, hne]
        rw [List.lookup_cons, hjkfalse]
        simp [hne, hfilter]
        rw [List.lookup_cons, hjkfalse]
      · have hne : (e.1 != k) = true := by
          cases h : e.1 != k <;> simp_all
        simp [step, hk, hne, hek]
        rw [List.lookup_cons, hjkfalse]
        simp [hne]
        rw [List.lookup_cons]
        by_cases hje : j == e.1
        · rw [List.lookup_cons, hje]
        · have hjefalse : (j == e.1) = false := by
            cases h : j == e.1 <;> simp_all
          rw [List.lookup_cons, hjefalse]
          exact hfilter

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutState.Common
