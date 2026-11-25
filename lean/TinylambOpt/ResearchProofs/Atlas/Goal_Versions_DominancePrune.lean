import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Versions_DominancePrune

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.Versions

private theorem earlier_iff (a b : Event) :
    earlier a b = true ↔ a.tx < b.tx ∨ (a.tx = b.tx ∧ a.ordinal < b.ordinal) := by
  unfold earlier
  simp [beq_iff_eq]

private theorem earlier_iff_false (a b : Event) :
    earlier a b = false ↔ ¬ (a.tx < b.tx ∨ (a.tx = b.tx ∧ a.ordinal < b.ordinal)) := by
  unfold earlier
  simp [beq_iff_eq]

private theorem earlier_trans {a b c : Event} (hab : earlier a b = true) (hbc : earlier b c = true) :
    earlier a c = true := by
  rw [earlier_iff] at hab hbc ⊢
  omega

private theorem earlier_irrefl (a : Event) : earlier a a = false := by
  rw [earlier_iff_false]
  simp

private theorem earlier_asym {a b : Event} (hab : earlier a b = true) : earlier b a = false := by
  rw [earlier_iff_false]
  rw [earlier_iff] at hab
  simp
  omega

private theorem le_trans : ∀ a b c : Event, (!earlier a b) = true → (!earlier b c) = true →
    (!earlier a c) = true := by
  intro a b c hab hbc
  have habf : earlier a b = false := by simpa only [Bool.not_eq_true'] using hab
  have hbcf : earlier b c = false := by simpa only [Bool.not_eq_true'] using hbc
  have habp := (earlier_iff_false a b).mp habf
  have hbcp := (earlier_iff_false b c).mp hbcf
  have hgoalp : ¬ (a.tx < c.tx ∨ (a.tx = c.tx ∧ a.ordinal < c.ordinal)) := by
    intro hac
    omega
  have hgoalf : earlier a c = false := (earlier_iff_false a c).mpr hgoalp
  simpa only [Bool.not_eq_true'] using hgoalf

private theorem le_total : ∀ a b : Event, ((!earlier a b) || (!earlier b a)) = true := by
  intro a b
  by_cases h : earlier a b = true
  · have hba : earlier b a = false := earlier_asym h
    simp [h, hba]
  · have habf : earlier a b = false := by cases h' : earlier a b <;> simp_all
    simp [habf]

private theorem pairwise_append_head {α : Type} {le : α → α → Bool} {l₁ l₂ : List α} {a : α}
    (hp : (l₁ ++ a :: l₂).Pairwise (fun x y => le x y = true)) :
    ∀ b ∈ l₂, le a b = true := by
  induction l₁ with
  | nil =>
      intro b hb
      exact (List.pairwise_cons.mp hp).1 b (by simp [hb])
  | cons x xs ih =>
      apply ih
      have hp' := (List.pairwise_cons.mp hp).2
      simpa using hp'

private theorem newest_cons_drop (a : Event) (l : List Event) (h : ∃ b ∈ l, earlier a b = true) :
    newest (a :: l) = newest l := by
  unfold newest
  obtain ⟨l₁, l₂, hms, hmsl, hbefore⟩ :=
    List.mergeSort_cons le_trans le_total a l
  rw [hms, hmsl]
  have hsorted := List.pairwise_mergeSort le_trans le_total (a :: l)
  rw [hms] at hsorted
  have hl2 : ∀ b ∈ l₂, earlier a b = false := by
    intro b hb
    have hleab : (!earlier a b) = true := pairwise_append_head hsorted b hb
    simpa only [Bool.not_eq_true'] using hleab
  have hl1ne : l₁ ≠ [] := by
    intro hnil
    rcases h with ⟨b, hb, hnew⟩
    have hbl : b ∈ l₁ ++ l₂ := by
      rw [← hmsl]
      exact (List.mem_mergeSort.mpr hb)
    rw [hnil] at hbl
    simp at hbl
    have hfalse := hl2 b hbl
    rw [hnew] at hfalse
    cases hfalse
  cases l₁ with
  | nil => contradiction
  | cons x xs => simp

private theorem newest_cons_keep (a : Event) (l : List Event) (h : ∀ b ∈ l, earlier a b = false) :
    newest (a :: l) = some a := by
  unfold newest
  obtain ⟨l₁, l₂, hms, hmsl, hbefore⟩ :=
    List.mergeSort_cons le_trans le_total a l
  rw [hms]
  have hl1nil : l₁ = [] := by
    cases l₁ with
    | nil => rfl
    | cons b bs =>
        have hba : earlier a b = true := by
          simpa only [Bool.not_eq_true', Bool.not_eq_false'] using hbefore b (by simp)
        have hbms : b ∈ l.mergeSort (fun a b => !earlier a b) := by
          rw [hmsl]
          simp
        have hfalse := h b (List.mem_mergeSort.mp hbms)
        rw [hba] at hfalse
        cases hfalse
  rw [hl1nil]
  simp

private theorem earlier_of_earlier_of_not_earlier {a b m : Event}
    (hab : earlier a b = true) (hmb : earlier m b = false) : earlier a m = true := by
  rw [earlier_iff] at hab ⊢
  rw [earlier_iff_false] at hmb
  omega

private theorem newest_none_iff (l : List Event) : newest l = none ↔ l = [] := by
  unfold newest
  cases hs : l.mergeSort (fun a b => !earlier a b) with
  | nil =>
      constructor
      · intro _
        have hperm := List.mergeSort_perm l (fun a b => !earlier a b)
        rw [hs] at hperm
        exact hperm.symm.eq_nil
      · intro hl
        subst l
        simp
  | cons x xs =>
      constructor
      · intro hnone
        simp at hnone
      · intro hl
        subst l
        simp at hs

private theorem newest_eq_some_maximal {l : List Event} {m : Event} (hm : newest l = some m) :
    m ∈ l ∧ ∀ f ∈ l, earlier m f = false := by
  unfold newest at hm
  have hp := List.pairwise_mergeSort le_trans le_total l
  have hperm := List.mergeSort_perm l (fun a b => !earlier a b)
  cases hs : l.mergeSort (fun a b => !earlier a b) with
  | nil => simp [hs] at hm
  | cons x xs =>
      have hx : x = m := by simpa [hs] using hm
      constructor
      · have hmms : m ∈ l.mergeSort (fun a b => !earlier a b) := by
          rw [hs]
          rw [← hx]
          simp
        exact List.mem_mergeSort.mp hmms
      · rw [hs] at hp
        intro f hf
        have hfx : f ∈ x :: xs := by
          have htmp := hperm.mem_iff.mpr hf
          simpa [hs] using htmp
        rcases List.mem_cons.mp hfx with rfl | hfrest
        · rw [hx]
          exact earlier_irrefl m
        · have hle := (List.pairwise_cons.mp hp).1 f hfrest
          have hxf : earlier x f = false := by simpa only [Bool.not_eq_true'] using hle
          simpa [hx] using hxf

private theorem dominated_by_maximal (a : Event) (l : List Event)
    (hdom : ∃ b ∈ l, earlier a b = true) :
    ∃ m ∈ l, (∀ f ∈ l, earlier m f = false) ∧ earlier a m = true := by
  rcases hdom with ⟨b, hb, hab⟩
  cases hml : newest l with
  | none =>
      have hl := (newest_none_iff l).mp hml
      subst l
      simp at hb
  | some m =>
      rcases newest_eq_some_maximal hml with ⟨hm_mem, hm_max⟩
      refine ⟨m, hm_mem, hm_max, ?_⟩
      exact earlier_of_earlier_of_not_earlier hab (hm_max b hb)

private theorem newest_eq_first_maximal (xs : List Event) :
    newest xs = (xs.filter (fun m => decide (∀ f ∈ xs, earlier m f = false))).head? := by
  induction xs with
  | nil => simp [newest]
  | cons a l ih =>
      by_cases hdom : ∃ b ∈ l, earlier a b = true
      · rw [newest_cons_drop a l hdom, ih]
        have hfilter : (l.filter (fun m => decide (∀ f ∈ l, earlier m f = false))) =
            ((a :: l).filter (fun m => decide (∀ f ∈ a :: l, earlier m f = false))) := by
          simp only [List.filter_cons]
          have hnotmaxa : ¬ (∀ f ∈ a :: l, earlier a f = false) := by
            rcases hdom with ⟨b, hb, hab⟩
            intro hm
            have hbad := hm b (by simp [hb])
            rw [hab] at hbad
            cases hbad
          rw [decide_eq_false hnotmaxa]
          simp only [Bool.false_eq_true, ite_false]
          apply List.filter_congr
          intro m hm
          have hpq : (∀ f ∈ l, earlier m f = false) ↔ (∀ f ∈ a :: l, earlier m f = false) := by
            constructor
            · intro hmax f hf
              rcases List.mem_cons.mp hf with hfa | hf
              · subst f
                rcases hdom with ⟨b, hb, hab⟩
                cases hma : earlier m a
                · rfl
                · have hmb := earlier_trans hma hab
                  have hbmax := hmax b hb
                  rw [hmb] at hbmax
                  cases hbmax
              · exact hmax f hf
            · intro hmax f hf
              exact hmax f (List.mem_cons_of_mem a hf)
          by_cases hp : ∀ f ∈ l, earlier m f = false
          · have hq : ∀ f ∈ a :: l, earlier m f = false := hpq.mp hp
            simp [hq]
          · have hq : ¬ (∀ f ∈ a :: l, earlier m f = false) := by
              intro hq
              exact hp (hpq.mpr hq)
            simp [hp]
        rw [← hfilter]
      · have hkeep : ∀ b ∈ l, earlier a b = false := by
          intro b hb
          cases h : earlier a b
          · rfl
          · exact False.elim (hdom ⟨b, hb, h⟩)
        rw [newest_cons_keep a l hkeep]
        have hmaxa : ∀ f ∈ a :: l, earlier a f = false := by
          intro f hf
          rcases List.mem_cons.mp hf with hfa | hf
          · subst f
            exact earlier_irrefl a
          · cases h : earlier a f
            · rfl
            · exact False.elim (hdom ⟨f, hf, h⟩)
        rw [List.filter_cons]
        rw [decide_eq_true hmaxa]
        simp

theorem sound_Versions_DominancePrune : DominancePrune.Goal := by
  intro es k t v
  unfold readAt winner
  let E := es.filter (eligible k t v)
  let F := (frontier es t).filter (eligible k t v)
  let maxE : Event → Bool := fun m => decide (∀ f ∈ E, earlier m f = false)
  let maxF : Event → Bool := fun m => decide (∀ f ∈ F, earlier m f = false)
  change (newest F).bind Event.payload = (newest E).bind Event.payload
  have hsubFE : ∀ e ∈ F, e ∈ E := by
    intro e he
    have he_frontier := (List.mem_filter.mp he).1
    have he_es := (List.mem_filter.mp (List.mem_filter.mp he_frontier).1).1
    exact List.mem_filter.mpr ⟨he_es, (List.mem_filter.mp he).2⟩
  have hmaxToF : ∀ m ∈ E, (∀ f ∈ E, earlier m f = false) → m ∈ F := by
    intro m hmE hmaxm
    have hmel := (List.mem_filter.mp hmE).2
    have hmel' : ((m.key = k ∧ m.tx ≤ t) ∧ m.since ≤ v) ∧ v < m.stop := by
      simpa [eligible, Bool.and_eq_true, decide_eq_true_eq] using hmel
    rcases hmel' with ⟨⟨⟨hmk, hmt⟩, hms⟩, hmv⟩
    have hnotany : es.any (fun f => f.tx ≤ t && f.key == m.key && earlier m f &&
        f.since ≤ m.since && m.stop ≤ f.stop) ≠ true := by
      intro hany
      rcases List.any_eq_true.mp hany with ⟨f, hfes, hf⟩
      have hf' : (((f.tx ≤ t ∧ f.key = m.key) ∧ earlier m f = true) ∧
          f.since ≤ m.since) ∧ m.stop ≤ f.stop := by
        simpa [Bool.and_eq_true, decide_eq_true_eq] using hf
      rcases hf' with ⟨⟨⟨⟨hft, hfk⟩, hmf⟩, hfs⟩, hstop⟩
      have hfel : eligible k t v f = true := by
        simp [eligible, hft, hfk, hmk, Int.le_trans hfs hms,
          Int.lt_of_lt_of_le hmv hstop]
      have hbad := hmaxm f (List.mem_filter.mpr ⟨hfes, hfel⟩)
      rw [hmf] at hbad
      cases hbad
    have hno : (!es.any (fun f => f.tx ≤ t && f.key == m.key && earlier m f &&
        f.since ≤ m.since && m.stop ≤ f.stop)) = true := by
      cases h : es.any (fun f => f.tx ≤ t && f.key == m.key && earlier m f &&
        f.since ≤ m.since && m.stop ≤ f.stop) with
      | false => rfl
      | true => exact False.elim (hnotany h)
    apply List.mem_filter.mpr
    constructor
    · unfold frontier
      apply List.mem_filter.mpr
      exact ⟨List.mem_filter.mpr ⟨(List.mem_filter.mp hmE).1, decide_eq_true hmt⟩, hno⟩
    · exact hmel
  have hEfilter : E.filter maxE =
      es.filter (fun m => maxE m && eligible k t v m) := by
    unfold E
    rw [List.filter_filter]
  have hFfilter : F.filter maxF =
      es.filter (fun m => maxF m &&
        (eligible k t v m && (!es.any (fun f => f.tx ≤ t && f.key == m.key &&
          earlier m f && f.since ≤ m.since && m.stop ≤ f.stop) && decide (m.tx ≤ t)))) := by
    unfold F frontier
    rw [List.filter_filter, List.filter_filter, List.filter_filter]
    apply congrArg (fun p : Event → Bool => es.filter p)
    funext m
    simp only [Bool.and_assoc]
  rw [newest_eq_first_maximal E, newest_eq_first_maximal F, hEfilter, hFfilter]
  have hfilterEq :
      es.filter (fun m => maxF m &&
        (eligible k t v m && (!es.any (fun f => f.tx ≤ t && f.key == m.key &&
          earlier m f && f.since ≤ m.since && m.stop ≤ f.stop) && decide (m.tx ≤ t)))) =
      es.filter (fun m => maxE m && eligible k t v m) := by
    apply List.filter_congr
    intro m hm
    by_cases hel : eligible k t v m = true
    · have hmel : ((m.key = k ∧ m.tx ≤ t) ∧ m.since ≤ v) ∧ v < m.stop := by
        simpa [eligible, Bool.and_eq_true, decide_eq_true_eq] using hel
      rcases hmel with ⟨⟨⟨hmk, hmt⟩, hms⟩, hmv⟩
      by_cases hnd : es.any (fun f => f.tx ≤ t && f.key == m.key && earlier m f &&
          f.since ≤ m.since && m.stop ≤ f.stop) = true
      · have hpfalse : maxE m = false := by
          apply Bool.eq_false_of_ne_true
          intro hpmax
          have hp : ∀ f ∈ E, earlier m f = false := by
            simpa [maxE] using hpmax
          have hmF := hmaxToF m (List.mem_filter.mpr ⟨hm, hel⟩) hp
          have hm_frontier := (List.mem_filter.mp hmF).1
          have hno := (List.mem_filter.mp hm_frontier).2
          rw [hnd] at hno
          cases hno
        simp [maxE, maxF, hel, hmt, hnd, hpfalse]
      · have hpq : maxE m = maxF m := by
          by_cases hp : maxE m = true
          · have hp' : ∀ f ∈ E, earlier m f = false := by simpa [maxE] using hp
            have hmF := hmaxToF m (List.mem_filter.mpr ⟨hm, hel⟩) hp'
            have hf : ∀ f ∈ F, earlier m f = false := by
              intro f hf
              exact hp' f (hsubFE f hf)
            rw [hp]
            simp only [maxF]
            exact (decide_eq_true hf).symm
          · have hpfalse : maxE m = false := Bool.eq_false_of_ne_true hp
            have hpnot : ¬ ∀ f ∈ E, earlier m f = false := by simpa [maxE] using hp
            have hf : maxF m = false := by
              apply Bool.eq_false_of_ne_true
              intro hfmax
              have hf' : ∀ f ∈ F, earlier m f = false := by simpa [maxF] using hfmax
              have hany : E.any (fun f => earlier m f) = true := by
                cases h : E.any (fun f => earlier m f) with
                | false =>
                    have hall : ∀ f ∈ E, earlier m f = false := by
                      intro f hf
                      have hne : earlier m f ≠ true := by
                        intro ht
                        have ha := List.any_eq_true.mpr ⟨f, hf, ht⟩
                        rw [h] at ha
                        cases ha
                      cases he : earlier m f <;> simp_all
                    exact False.elim (hpnot hall)
                | true => rfl
              rcases List.any_eq_true.mp hany with ⟨f, hfE, hmf⟩
              rcases dominated_by_maximal m E ⟨f, hfE, hmf⟩ with
                ⟨g, hgE, hgmax, hmg⟩
              have hgF := hmaxToF g hgE hgmax
              have hbad := hf' g hgF
              rw [hmg] at hbad
              cases hbad
            simp [maxE, maxF, hpfalse, hf]
        simp [maxE, maxF, hel, hmt, hnd, hpq]
    · simp [maxE, maxF, hel]
  rw [hfilterEq]

end TinylambOpt.ResearchProofs.Atlas.Goal_Versions_DominancePrune
