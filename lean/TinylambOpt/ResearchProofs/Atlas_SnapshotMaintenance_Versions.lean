import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `Versions.DominancePrune` — `∀ es k t v, readAt (frontier es t) k t v = readAt es k t
  v`
- `Versions.LocalPublish` — `∀ es e k t v, (∀ old ∈ es, earlier old e = true) → readAt
  (es++[e]) k t v = if eligible k t v e then e.payload else readAt es k t v`
- `Versions.RunTournament` — `∀ (runs : List (List Event)) k t v, winner runs.flatten k
  t v = newest (runs.filterMap (fun r => winner r k t v))`
- `Versions.SlabReuse` — `∀ es k t l h, l ≤ h → (∀ e ∈ es, e.key = k → e.tx ≤ t →
  (e.since ≤ l ∨ h < e.since) ∧ (e.stop ≤ l ∨ h < e.stop)) → ∀ v, l ≤ v → v ≤ h →
  winner es k t v = winner es k t l`
-/

namespace TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.Versions

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.Versions

set_option maxHeartbeats 0

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

theorem sound_SlabReuse : SlabReuse.Goal := by
  intro es k t l h hle hcond v hlv hvh
  unfold winner
  have hfilter : es.filter (eligible k t v) = es.filter (eligible k t l) := by
    apply List.filter_congr
    intro e he
    unfold eligible
    by_cases hkey : (e.key == k) = true
    · by_cases htx : e.tx ≤ t
      · have hspec := hcond e he (beq_iff_eq.mp hkey) htx
        rcases hspec with ⟨hsince, hstop⟩
        have hsince_iff : (e.since ≤ v) ↔ (e.since ≤ l) := by
          constructor
          · intro hsv
            rcases hsince with hsl | hsh
            · exact hsl
            · omega
          · intro hsl
            omega
        have hstop_iff : (v < e.stop) ↔ (l < e.stop) := by
          constructor
          · intro hvs
            rcases hstop with hst | hsh
            · omega
            · omega
          · intro hls
            rcases hstop with hst | hsh
            · omega
            · omega
        have hsBool : (decide (e.since ≤ v)) = (decide (e.since ≤ l)) := by
          rcases hsince with hsl | hsh
          · have hv : e.since ≤ v := by omega
            simp [hsl, hv]
          · have hv : ¬ e.since ≤ v := by omega
            have hl : ¬ e.since ≤ l := by omega
            simp [hv, hl]
        have hpBool : (decide (v < e.stop)) = (decide (l < e.stop)) := by
          rcases hstop with hst | hsh
          · have hv : ¬ v < e.stop := by omega
            have hl : ¬ l < e.stop := by omega
            simp [hv, hl]
          · have hv : v < e.stop := by omega
            have hl : l < e.stop := by omega
            simp [hv, hl]
        simp [hkey, htx, hsBool, hpBool]
      · simp [hkey, htx]
    · simp [hkey]
  rw [hfilter]

theorem sound_LocalPublish : LocalPublish.Goal := by
  intro es e k t v hold
  unfold readAt winner
  let P : Event → Bool := eligible k t v
  let A := es.filter P
  change ((newest ((es ++ [e]).filter P)).bind Event.payload) =
    (if P e then e.payload else ((newest A).bind Event.payload))
  by_cases hP : P e = true
  · have hstep : step (A.foldl step none) e = some e := by
      cases hacc : A.foldl step none with
      | none => simp [step]
      | some a =>
          have haA : a ∈ A := foldl_step_mem hacc
          have haes : a ∈ es := (List.mem_filter.mp haA).1
          have hae := hold a haes
          simp [step, hae]
    have hfilter : (es ++ [e]).filter P = A ++ [e] := by
      simp [A, hP]
    rw [newest_eq_foldl, hfilter, List.foldl_append]
    simp [hP, hstep]
  · have hfilter : (es ++ [e]).filter P = A := by
      simp [A, hP]
    simp [hP, hfilter]

theorem sound_RunTournament : RunTournament.Goal := by
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

private theorem filter_filter_mem_general {α : Type} (P R Q : α → Bool) (l : List α)
    (hQ : ∀ e ∈ l, Q e = true ↔ e ∈ l.filter R) :
    (l.filter P).filter Q = (l.filter R).filter P := by
  calc
    (l.filter P).filter Q = l.filter (fun e => Q e && P e) := by rw [List.filter_filter]
    _ = l.filter (fun e => P e && Q e) := by
      apply List.filter_congr
      intro e he
      exact Bool.and_comm (Q e) (P e)
    _ = l.filter (fun e => P e && R e) := by
      apply List.filter_congr
      intro e he
      by_cases hp : P e = true
      · by_cases hqe : Q e = true
        · have heR : e ∈ l.filter R := (hQ e he).mp hqe
          have hRe : R e = true := (List.mem_filter.mp heR).2
          simp [hp, hqe, hRe]
        · have heRnot : e ∉ l.filter R := by
            intro h
            exact hqe ((hQ e he).mpr h)
          have hRe : R e = false := by cases h : R e <;> simp_all
          simp [hp, hqe, hRe]
      · simp [hp]
    _ = (l.filter R).filter P := by rw [List.filter_filter]

private theorem bool_eq_of_iff {P Q : Prop} [Decidable P] [Decidable Q] (h : P ↔ Q) :
    decide P = decide Q := by
  by_cases hp : P <;> by_cases hq : Q <;> simp_all

private theorem newest_filter_dominated (E : List Event) (Q : Event → Bool)
    (hQdom : ∀ e ∈ E, Q e = false → ∃ f ∈ E, Q f = true ∧ earlier e f = true) :
    newest (E.filter Q) = newest E := by
  rw [newest_eq_first_maximal E, newest_eq_first_maximal (E.filter Q)]
  have hright : (E.filter Q).filter (fun m => decide (∀ f ∈ E.filter Q, earlier m f = false)) =
      E.filter (fun m => Q m && decide (∀ f ∈ E.filter Q, earlier m f = false)) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro m hm
    cases Q m <;> cases decide (∀ f ∈ E.filter Q, earlier m f = false) <;> simp
  rw [hright]
  have hfeq : E.filter (fun m => Q m && decide (∀ f ∈ E.filter Q, earlier m f = false)) =
      E.filter (fun m => decide (∀ f ∈ E, earlier m f = false)) := by
    apply List.filter_congr
    intro m hmE
    by_cases hQm : Q m = true
    · have hpq : (∀ f ∈ E, earlier m f = false) ↔ (∀ f ∈ E.filter Q, earlier m f = false) := by
        constructor
        · intro h f hf
          exact h f (List.mem_filter.mp hf).1
        · intro h f hfE
          by_cases htf : earlier m f = true
          · rcases dominated_by_maximal m E ⟨f, hfE, htf⟩ with ⟨g, hgE, hgmax, hmg⟩
            have hQg : Q g = true := by
              by_cases hq : Q g = true
              · exact hq
              · have hqf : Q g = false := by cases h' : Q g <;> simp_all
                rcases hQdom g hgE hqf with ⟨u, hu⟩
                have hbad := hgmax u hu.1
                rw [hu.2.2] at hbad
                simp at hbad
            have hgF : g ∈ E.filter Q := List.mem_filter.mpr ⟨hgE, hQg⟩
            have hbad := h g hgF
            rw [hmg] at hbad
            simp at hbad
          · cases h' : earlier m f
            · rfl
            · exact False.elim (htf h')
      have hleft : (Q m && decide (∀ f ∈ E.filter Q, earlier m f = false)) =
          decide (∀ f ∈ E.filter Q, earlier m f = false) := by
        cases hdm : decide (∀ f ∈ E.filter Q, earlier m f = false) <;> simp [hQm, hdm]
      rw [hleft]
      exact bool_eq_of_iff hpq.symm
    · have hQmf : Q m = false := by cases h : Q m <;> simp_all
      have hpfalse : ¬ (∀ f ∈ E, earlier m f = false) := by
        intro hp
        rcases hQdom m hmE hQmf with ⟨f, hf⟩
        have hbad := hp f hf.1
        rw [hf.2.2] at hbad
        cases hbad
      have hleft : (Q m && decide (∀ f ∈ E.filter Q, earlier m f = false)) = false := by
        cases hdm : decide (∀ f ∈ E.filter Q, earlier m f = false) <;> simp [hQm, hdm]
      rw [hleft, decide_eq_false hpfalse]
  rw [hfeq]

theorem sound_DominancePrune : DominancePrune.Goal := by
  classical
  intro es k t v
  unfold readAt winner
  let E := es.filter (eligible k t v)
  let R : Event → Bool := fun e => (decide (e.tx ≤ t) && !(es.any (fun f =>
      f.tx ≤ t && f.key == e.key && earlier e f && f.since ≤ e.since && e.stop ≤ f.stop)))
  let F := (frontier es t).filter (eligible k t v)
  change (newest F).bind Event.payload = (newest E).bind Event.payload
  have hfrontier : frontier es t = es.filter R := by
    unfold frontier R
    rw [List.filter_filter]
    apply List.filter_congr
    intro e he
    cases decide (e.tx ≤ t) <;> cases es.any (fun f => f.tx ≤ t && f.key == e.key &&
      earlier e f && f.since ≤ e.since && e.stop ≤ f.stop) <;> simp
  let Q : Event → Bool := fun e => decide (e ∈ frontier es t)
  have hEF : E.filter Q = F := by
    unfold E F
    rw [hfrontier]
    apply filter_filter_mem_general
    intro e he
    simpa [Q, hfrontier]
  have hel_of_memE : ∀ {m : Event}, m ∈ E → m.key = k ∧ m.tx ≤ t ∧ m.since ≤ v ∧ v < m.stop := by
    intro m hm
    have h := (List.mem_filter.mp hm).2
    unfold eligible at h
    simp at h
    rcases h with ⟨⟨⟨hkey, htx⟩, hsin⟩, hstop⟩
    exact ⟨hkey, htx, hsin, hstop⟩
  have hQdom : ∀ m ∈ E, Q m = false → ∃ f ∈ E, Q f = true ∧ earlier m f = true := by
    intro m hmE hQm
    have hmnotfrontier : m ∉ frontier es t := by
      intro hmf
      have htrue : Q m = true := by simp [Q, hmf]
      rw [hQm] at htrue
      cases htrue
    have hel := hel_of_memE hmE
    unfold frontier at hmnotfrontier
    have hmfirst : m ∈ es.filter (fun x => decide (x.tx ≤ t)) :=
      List.mem_filter.mpr ⟨(List.mem_filter.mp hmE).1, by simp [hel.2.1]⟩
    have hany : es.any (fun f => f.tx ≤ t && f.key == m.key && earlier m f &&
        f.since ≤ m.since && m.stop ≤ f.stop) = true := by
      cases hn : es.any (fun f => f.tx ≤ t && f.key == m.key && earlier m f &&
          f.since ≤ m.since && m.stop ≤ f.stop)
      · exfalso
        have hno' : (!es.any (fun f => f.tx ≤ t && f.key == m.key && earlier m f &&
            f.since ≤ m.since && m.stop ≤ f.stop)) = true := by simp [hn]
        exact hmnotfrontier (List.mem_filter.mpr ⟨hmfirst, hno'⟩)
      · rfl
    let Cands := E.filter (fun f => decide (f.since ≤ m.since ∧ m.stop ≤ f.stop ∧ earlier m f = true))
    have hc_nonempty : Cands ≠ [] := by
      rcases List.any_eq_true.mp hany with ⟨f, hfes, hf⟩
      simp only [Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at hf
      rcases hf with ⟨⟨⟨⟨hftx, hfkey⟩, hef⟩, hfsin⟩, hfsto⟩
      have hfsinv : f.since ≤ v := by omega
      have hvfstop : v < f.stop := by omega
      have hfel : eligible k t v f = true := by
        unfold eligible
        simp [hfkey ▸ hel.1, hftx, hfsinv, hvfstop]
      have hfE : f ∈ E := List.mem_filter.mpr ⟨hfes, hfel⟩
      intro hnil
      have hfnot : f ∉ Cands := by rw [hnil]; simp
      exact hfnot (List.mem_filter.mpr ⟨hfE, by simp [hfsin, hfsto, hef]⟩)
    have hcnew : newest Cands ≠ none := by
      intro hnone
      have hempty := (newest_none_iff Cands).mp hnone
      contradiction
    cases hnew : newest Cands with
    | none => contradiction
    | some g =>
        have hgmax := newest_eq_some_maximal hnew
        have hgCmem := (List.mem_filter.mp hgmax.1)
        have hgProps : g ∈ E ∧ g.since ≤ m.since ∧ m.stop ≤ g.stop ∧ earlier m g = true := by
          have hp := hgCmem.2
          simp only [decide_eq_true_eq] at hp
          rcases hp with ⟨hgsin, hgsto, heg⟩
          exact ⟨hgCmem.1, hgsin, hgsto, heg⟩
        refine ⟨g, hgProps.1, ?_, hgProps.2.2.2⟩
        have hgfrontier : g ∈ frontier es t := by
          unfold frontier
          apply List.mem_filter.mpr
          constructor
          · apply List.mem_filter.mpr
            constructor
            · exact (List.mem_filter.mp hgProps.1).1
            · exact (by simp [(hel_of_memE hgProps.1).2.1] : decide (g.tx ≤ t) = true)
          · have hnotany : ¬ (es.any (fun f => f.tx ≤ t && f.key == g.key && earlier g f &&
                f.since ≤ g.since && g.stop ≤ f.stop) = true) := by
              intro ha
              rcases List.any_eq_true.mp ha with ⟨u, hues, hu⟩
              simp only [Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at hu
              rcases hu with ⟨⟨⟨⟨hutx, hukey⟩, hegu⟩, husin⟩, husto⟩
              have hgel := hel_of_memE hgProps.1
              have husinv : u.since ≤ v := by omega
              have hvustop : v < u.stop := by omega
              have huel : eligible k t v u = true := by
                unfold eligible
                simp [hukey ▸ hgel.1, hutx, husinv, hvustop]
              have huE : u ∈ E := List.mem_filter.mpr ⟨hues, huel⟩
              have huC : u ∈ Cands := by
                apply List.mem_filter.mpr
                refine ⟨huE, ?_⟩
                exact decide_eq_true ⟨by omega, by omega, earlier_trans hgProps.2.2.2 hegu⟩
              have hbad := hgmax.2 u huC
              rw [hbad] at hegu
              simp at hegu
            by_cases ha : es.any (fun f => f.tx ≤ t && f.key == g.key && earlier g f &&
                  f.since ≤ g.since && g.stop ≤ f.stop) = true
            · exact False.elim (hnotany ha)
            · have hf : es.any (fun f => f.tx ≤ t && f.key == g.key && earlier g f &&
                  f.since ≤ g.since && g.stop ≤ f.stop) = false :=
                Bool.eq_false_of_ne_true ha
              simp [hf]
        simp [Q, hgfrontier]
  rw [← hEF]
  exact congrArg (fun o : Option Event => o.bind Event.payload) (newest_filter_dominated E Q hQdom)

end TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.Versions
