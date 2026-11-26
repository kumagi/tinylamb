import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `Versions.RunTournament` — `∀ (runs : List (List Event)) k t v, winner runs.flatten k
  t v = newest (runs.filterMap (fun r => winner r k t v))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Versions_RunTournament

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

private theorem earlier_of_le_of_earlier {a e b : Event}
    (hae : earlier a e = false) (hab : earlier a b = true) : earlier e b = true := by
  rw [earlier_iff_false] at hae
  rw [earlier_iff] at hab ⊢
  omega

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
        have : some x = none := by simpa [hs] using hnone
        cases this
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
            simp [hp, hq]
          · have hq : ¬ (∀ f ∈ a :: l, earlier m f = false) := by
              intro hq
              exact hp (hpq.mpr hq)
            simp [hp, hq]
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

private theorem newest_cons (a : Event) (l : List Event) :
    newest (a :: l) = if (∃ b ∈ l, earlier a b = true) then newest l else some a := by
  by_cases h : ∃ b ∈ l, earlier a b = true
  · rw [if_pos h, newest_cons_drop a l h]
  · rw [if_neg h]
    apply newest_cons_keep a l
    intro b hb
    cases h' : earlier a b
    · rfl
    · exact False.elim (h ⟨b, hb, h'⟩)

private theorem newest_earlier_iff {E : List Event} {m : Event} (hm : newest E = some m) (a : Event) :
    (∃ b ∈ E, earlier a b = true) ↔ earlier a m = true := by
  rcases newest_eq_some_maximal hm with ⟨hmmem, hmmax⟩
  constructor
  · intro h
    rcases h with ⟨b, hb, hab⟩
    exact earlier_of_earlier_of_not_earlier hab (hmmax b hb)
  · intro ham
    exact ⟨m, hmmem, ham⟩

private def step (acc : Option Event) (e : Event) : Option Event :=
  match acc with
  | none => some e
  | some a => if earlier a e = true then some e else acc

private def stepOpt (acc : Option Event) (m : Option Event) : Option Event :=
  match m with
  | none => acc
  | some e => step acc e

private theorem foldl_step_eq_acc (l : List Event) (acc : Option Event) :
    l.foldl step acc =
      match acc with
      | none => newest l
      | some a => if (∃ b ∈ l, earlier a b = true) then newest l else some a := by
  induction l generalizing acc with
  | nil =>
      cases acc <;> simp [newest]
  | cons e es ih =>
      cases acc with
      | none =>
          simp [step, ih, newest_cons]
      | some a =>
          simp only [List.foldl_cons]
          by_cases hae : earlier a e = true
          · rw [show step (some a) e = some e by simp [step, hae]]
            rw [ih]
            change (if (∃ b ∈ es, earlier e b = true) then newest es else some e) =
              (if (∃ b ∈ e :: es, earlier a b = true) then newest (e :: es) else some a)
            have hex : ∃ b ∈ e :: es, earlier a b = true := ⟨e, by simp, hae⟩
            rw [if_pos hex]
            rw [newest_cons]
          · have hiff : (∃ b ∈ e :: es, earlier a b = true) ↔ (∃ b ∈ es, earlier a b = true) := by
              constructor
              · intro h
                rcases h with ⟨b, hb, hab⟩
                rcases List.mem_cons.mp hb with hb | hb
                · subst b
                  exact False.elim (hae hab)
                · exact ⟨b, hb, hab⟩
              · intro h
                rcases h with ⟨b, hb, hab⟩
                exact ⟨b, by simp [hb], hab⟩
            rw [show step (some a) e = some a by simp [step, hae]]
            rw [ih]
            change (if (∃ b ∈ es, earlier a b = true) then newest es else some a) =
              (if (∃ b ∈ e :: es, earlier a b = true) then newest (e :: es) else some a)
            by_cases hex : ∃ b ∈ es, earlier a b = true
            · have hex' : ∃ b ∈ e :: es, earlier a b = true := hiff.mpr hex
              rw [if_pos hex]
              rw [if_pos hex']
              rcases hex with ⟨b, hb, hab⟩
              have hae_false : earlier a e = false := by cases h : earlier a e <;> simp_all
              have heb : earlier e b = true := earlier_of_le_of_earlier hae_false hab
              rw [newest_cons_drop e es ⟨b, hb, heb⟩]
            · have hex' : ¬ (∃ b ∈ e :: es, earlier a b = true) := by
                intro h
                exact hex (hiff.mp h)
              rw [if_neg hex]
              rw [if_neg hex']

private theorem newest_eq_foldl (l : List Event) : newest l = l.foldl step none := by
  rw [foldl_step_eq_acc]

private theorem foldl_step_mem {l : List Event} {a : Event} (h : l.foldl step none = some a) : a ∈ l := by
  have hn : newest l = some a := by rw [newest_eq_foldl]; exact h
  exact (newest_eq_some_maximal hn).1

private theorem foldl_filter_eq_step_newest (es : List Event) (P : Event → Bool) (acc : Option Event) :
    (es.filter P).foldl step acc = stepOpt acc (newest (es.filter P)) := by
  cases acc with
  | none =>
      cases hnew : newest (es.filter P) with
      | none =>
          have hnil : es.filter P = [] := (newest_none_iff _).mp hnew
          simp [stepOpt, hnil]
      | some m =>
          simp [stepOpt, step]
          rw [← newest_eq_foldl, hnew]
  | some a =>
      rw [foldl_step_eq_acc]
      change (if (∃ b ∈ es.filter P, earlier a b = true) then newest (es.filter P) else some a) =
        stepOpt (some a) (newest (es.filter P))
      cases hnew : newest (es.filter P) with
      | none =>
          have hnil : es.filter P = [] := (newest_none_iff _).mp hnew
          simp [hnil, stepOpt]
      | some m =>
          have hiff := newest_earlier_iff hnew a
          by_cases hex : ∃ b ∈ es.filter P, earlier a b = true
          · rw [if_pos hex]
            have ham := hiff.mp hex
            simp [stepOpt, step, ham]
          · rw [if_neg hex]
            have hnam : earlier a m ≠ true := by
              intro h
              exact hex (hiff.mpr h)
            have hf : earlier a m = false := by cases h : earlier a m <;> simp_all
            simp [stepOpt, step, hf]

theorem sound_Versions_RunTournament : RunTournament.Goal := by
  intro runs k t v
  unfold winner
  let P : Event → Bool := eligible k t v
  change newest (runs.flatten.filter P) =
    newest (runs.filterMap (fun r => newest (r.filter P)))
  have hfoldruns : ∀ acc : Option Event, (runs.flatten.filter P).foldl step acc =
      runs.foldl (fun acc r => stepOpt acc (newest (r.filter P))) acc := by
    intro acc
    induction runs generalizing acc with
    | nil => simp
    | cons r rs ih =>
        rw [show (r :: rs).flatten.filter P = (r.filter P) ++ (rs.flatten.filter P) by
          simp [List.filter_append]]
        rw [List.foldl_append]
        rw [ih]
        simp only [List.foldl_cons]
        rw [foldl_filter_eq_step_newest r P acc]
  have hfoldw : ∀ acc : Option Event,
      (runs.filterMap (fun r => newest (r.filter P))).foldl step acc =
        runs.foldl (fun acc r => stepOpt acc (newest (r.filter P))) acc := by
    clear hfoldruns
    intro acc
    induction runs generalizing acc with
    | nil => simp
    | cons r rs ih =>
        cases h : newest (r.filter P) with
        | none => simpa [h, stepOpt] using ih acc
        | some m => simpa [h, stepOpt] using ih (step acc m)
  rw [newest_eq_foldl, hfoldruns none]
  rw [newest_eq_foldl]
  exact (hfoldw none).symm

end TinylambOpt.ResearchProofs.Atlas.Goal_Versions_RunTournament
