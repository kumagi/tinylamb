import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Proofs for the twenty `Atlas.SnapshotMaintenance` research goals.

The two strong hypotheses that are false retain their original goal types and
are refuted by the concrete examples documented in `issues/research/atlas.md`.
-/

namespace TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.Versions
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.IntervalDeletes
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## Versions -/

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

private theorem earlier_of_le_of_earlier {a b c : Event}
    (hab : earlier a b = false) (hac : earlier a c = true) : earlier b c = true := by
  rw [earlier_iff_false] at hab
  rw [earlier_iff] at hac ⊢
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

private theorem newest_cons (a : Event) (l : List Event) :
    newest (a :: l) = if (∃ b ∈ l, earlier a b = true) then newest l else some a := by
  by_cases h : ∃ b ∈ l, earlier a b = true
  · rw [ite_eq_left h, newest_cons_drop a l h]
  · rw [ite_eq_right h]
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
            dsimp only
            have hex : ∃ b ∈ e :: es, earlier a b = true := ⟨e, by simp, hae⟩
            rw [ite_eq_left hex]
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
            dsimp only
            by_cases hex : ∃ b ∈ es, earlier a b = true
            · have hex' : ∃ b ∈ e :: es, earlier a b = true := hiff.mpr hex
              rw [ite_eq_left hex]
              rw [ite_eq_left hex']
              rcases hex with ⟨b, hb, hab⟩
              have hae_false : earlier a e = false := by cases h : earlier a e <;> simp_all
              have heb : earlier e b = true := earlier_of_le_of_earlier hae_false hab
              rw [newest_cons_drop e es ⟨b, hb, heb⟩]
            · have hex' : ¬ (∃ b ∈ e :: es, earlier a b = true) := by
                intro h
                exact hex (hiff.mp h)
              rw [ite_eq_right hex]
              rw [ite_eq_right hex']

private theorem newest_eq_foldl (l : List Event) : newest l = l.foldl step none := by
  rw [foldl_step_eq_acc]

private theorem foldl_step_mem_acc (l : List Event) (acc : Option Event) {a : Event}
    (h : l.foldl step acc = some a) : a ∈ l ∨ ∃ x, acc = some x ∧ a = x := by
  induction l generalizing acc with
  | nil =>
      simp only [List.foldl_nil] at h
      exact Or.inr ⟨a, h, rfl⟩
  | cons x xs ih =>
      rw [List.foldl_cons] at h
      rcases ih (step acc x) h with hxs | ⟨y, hy, ha⟩
      · exact Or.inl (List.mem_cons_of_mem x hxs)
      · cases acc with
    | none =>
        change some x = some y at hy
        injection hy with hxy
        have hax : a = x := by simpa [hxy] using ha
        exact Or.inl (by simp [hax])
    | some z =>
        by_cases hzx : earlier z x = true
        · have hs : step (some z) x = some x := by simp [step, hzx]
          rw [hs] at hy
          injection hy with hxy
          have hax : a = x := by simpa [hxy] using ha
          exact Or.inl (by simp [hax])
        · have hs : step (some z) x = some z := by simp [step, hzx]
          rw [hs] at hy
          injection hy with hxy
          have haz : a = z := by simpa [hxy] using ha
          exact Or.inr ⟨z, rfl, haz⟩

private theorem foldl_step_mem {l : List Event} {a : Event} (h : l.foldl step none = some a) : a ∈ l := by
  rcases foldl_step_mem_acc l none h with hmem | ⟨x, hx, _⟩
  · exact hmem
  · simp at hx

private theorem foldl_filter_eq_step_newest (es : List Event) (P : Event → Bool) (acc : Option Event) :
    (es.filter P).foldl step acc = stepOpt acc (newest (es.filter P)) := by
  cases acc with
  | none =>
      simp only [stepOpt]
      rw [newest_eq_foldl]
      cases (es.filter P).foldl step none <;> rfl
  | some a =>
      rw [foldl_step_eq_acc]
      dsimp only
      cases hnew : newest (es.filter P) with
      | none =>
          have hnil : es.filter P = [] := (newest_none_iff _).mp hnew
          simp [hnil, stepOpt]
      | some m =>
          have hiff := newest_earlier_iff hnew a
          by_cases hex : ∃ b ∈ es.filter P, earlier a b = true
          · rw [ite_eq_left hex]
            have ham := hiff.mp hex
            simp [stepOpt, step, ham]
          · rw [ite_eq_right hex]
            have hnam : earlier a m ≠ true := by
              intro h
              exact hex (hiff.mpr h)
            have hf : earlier a m = false := by cases h : earlier a m <;> simp_all
            simp [stepOpt, step, hf]

theorem sound_Versions_SlabReuse : SlabReuse.Goal := by
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
        by_cases hsl : e.since ≤ l
        · have hsv : e.since ≤ v := Int.le_trans hsl hlv
          by_cases hst : e.stop ≤ l
          · have hvs : ¬ v < e.stop := by omega
            have hls : ¬ l < e.stop := by omega
            simp [hkey, htx, hsl, hsv, hvs, hls]
          · have hsh : h < e.stop := hstop.resolve_left hst
            have hvs : v < e.stop := Int.lt_of_le_of_lt hvh hsh
            have hls : l < e.stop := Int.lt_of_le_of_lt hle hsh
            simp [hkey, htx, hsl, hsv, hvs, hls]
        · have hsh : h < e.since := hsince.resolve_left hsl
          have hsv : ¬ e.since ≤ v := by omega
          by_cases hst : e.stop ≤ l
          · have hvs : ¬ v < e.stop := by omega
            have hls : ¬ l < e.stop := by omega
            simp [hkey, htx, hsl, hsv, hvs, hls]
          · have hstop' : h < e.stop := hstop.resolve_left hst
            have hvs : v < e.stop := Int.lt_of_le_of_lt hvh hstop'
            have hls : l < e.stop := Int.lt_of_le_of_lt hle hstop'
            simp [hkey, htx, hsl, hsv, hvs, hls]
      · simp [hkey, htx]
    · simp [hkey]
  rw [hfilter]

theorem sound_Versions_LocalPublish : LocalPublish.Goal := by
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
    simp [hstep, hP]
  · have hfilter : (es ++ [e]).filter P = A := by
      simp [A, hP]
    simp [hP, hfilter]

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
        rw [foldl_filter_eq_step_newest r P acc]
        simp only [List.foldl_cons]
        rw [ih]
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

/-- A newer tombstone masks the older payload; dropping tombstones resurrects it. -/
theorem refutation_Versions_DropTombstones :
    ¬ Versions.DropTombstones.Goal := by
  intro h
  let old : Versions.Event := ⟨3, 1, 0, 0, 10, some 7⟩
  let fresh : Versions.Event := ⟨3, 2, 0, 0, 10, none⟩
  let log : List Versions.Event := [old, fresh]
  have hwinner : Versions.newest log = some fresh := by
    rw [show log = [old, fresh] from rfl,
      newest_cons_drop old [fresh] ⟨fresh, by simp, by decide⟩]
    simp [Versions.newest]
  have hfilter : log.filter (fun e => e.payload.isSome) = [old] := by decide
  have hold : Versions.newest [old] = some old := by simp [Versions.newest]
  have hh := h log 3 2 1
  have hneq :
      ¬ (Versions.readAt (log.filter (fun e => e.payload.isSome)) 3 2 1 =
        Versions.readAt log 3 2 1) := by
    simp [log, old, fresh, Versions.readAt, Versions.winner, eligible,
      hwinner, hfilter, hold]
  exact hneq hh

/-! ## IntervalDeletes -/

/-! ### Signed endpoints -/

private theorem span_depth_contrib (a b i : Nat) :
    ((if a < b && a ≤ i then 1 else 0 : Nat) : Int) -
      ((if a < b && b ≤ i then 1 else 0 : Nat) : Int) =
        ((if hit (a, b) i then 1 else 0 : Nat) : Int) := by
  by_cases hab : a < b <;> by_cases hai : a ≤ i <;> by_cases hbi : b ≤ i <;>
    by_cases hib : i < b <;> simp [hit, hab, hai, hbi, hib] <;> omega

private theorem depth_eq_count_hit (rs : List Span) (i : Nat) :
    depth rs i = (rs.countP (fun r => hit r i) : Int) := by
  induction rs with
  | nil => simp [depth]
  | cons r rs ih =>
      rcases r with ⟨a, b⟩
      simp only [depth, List.countP_cons]
      have hcast (p : Bool) (c : Nat) :
          ((c + (if p then 1 else 0) : Nat) : Int) =
            ((if p then 1 else 0 : Nat) : Int) + (c : Int) := by
        cases p <;> omega
      rw [hcast (a < b && a ≤ i) (rs.countP (fun r : Span => r.1 < r.2 && r.1 ≤ i))]
      rw [hcast (a < b && b ≤ i) (rs.countP (fun r : Span => r.1 < r.2 && r.2 ≤ i))]
      rw [hcast (hit (a, b) i) (rs.countP (fun r : Span => hit r i))]
      rw [show ∀ A B C D : Int, (A + B) - (C + D) = (A - C) + (B - D) by
        intro A B C D; omega]
      rw [span_depth_contrib a b i, ← ih]
      simp only [depth]

private theorem decide_pos_int (n : Nat) : decide (0 < (n : Int)) = decide (0 < n) := by
  by_cases h : 0 < n <;> simp [h] <;> omega

theorem sound_IntervalDeletes_SignedEndpoints : SignedEndpoints.Goal := by
  intro rs i
  rw [depth_eq_count_hit]
  change blocked rs i = decide (0 < (rs.countP (fun r => hit r i) : Int))
  rw [decide_pos_int]
  unfold blocked
  induction rs with
  | nil => simp
  | cons r rs ih =>
      cases h : hit r i <;> simp [h, ih]

/-! ### Deletion union -/

private theorem filter_and_not_count (xs : List Nat) (p q : Nat → Bool) :
    (xs.filter (fun x => p x && !q x)).length + (xs.filter p).countP q =
      (xs.filter p).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases hp : p x <;> cases hq : q x <;> simp [hp, hq, ih] <;> omega

private theorem blocked_append (a b : List Span) (i : Nat) :
    blocked (a ++ b) i = (blocked a i || blocked b i) := by
  simp [blocked]

theorem sound_IntervalDeletes_OverlapDebt : OverlapDebt.Goal := by
  intro n a b
  unfold live
  have hba : ∀ i, blocked (a ++ b) i = (blocked a i || blocked b i) := blocked_append a b
  simpa [hba] using
    (filter_and_not_count (List.range n) (fun i => !blocked a i) (fun i => blocked b i))

/-! ### Rebasing a mask -/

private theorem bool_eq_of_iff {u v : Bool} (h : (u = true) ↔ (v = true)) : u = v := by
  cases u <;> cases v <;> simp_all

private theorem hit_rebase (base i a b : Nat) :
    hit (a - base, b - base) i = hit (a, b) (base + i) := by
  apply bool_eq_of_iff
  simp [hit]
  constructor <;> intro h <;> rcases h with ⟨h1, h2⟩ <;> constructor
  · by_cases ha : a ≤ base <;> omega
  · by_cases hb : b ≤ base <;> omega
  · by_cases ha : a ≤ base <;> omega
  · by_cases hb : b ≤ base <;> omega

private theorem blocked_rebase (base i : Nat) (rs : List Span) :
    blocked (rs.map (fun p : Span => (p.1 - base, p.2 - base))) i = blocked rs (base + i) := by
  induction rs with
  | nil => simp [blocked]
  | cons r rs ih =>
      rcases r with ⟨a, b⟩
      simp only [blocked, List.map_cons, List.any_cons]
      rw [hit_rebase base i a b]
      have ih' : (List.map (fun p : Span => (p.1 - base, p.2 - base)) rs).any
          (fun x => hit x i) = rs.any (fun x => hit x (base + i)) := by
        simpa [blocked] using ih
      rw [ih']

theorem sound_IntervalDeletes_RebaseMask : RebaseMask.Goal := by
  intro base n rs
  unfold live
  apply List.filter_congr
  intro i hi
  rw [blocked_rebase base i rs]

/-! ### Knot structure -/

private theorem pairwise_eraseDups_sorted : ∀ l : List Nat, l.Pairwise (· ≤ ·) →
    l.eraseDups.Pairwise (· < ·)
  | [], h => by simp
  | a :: as, h => by
      have hrel : ∀ b ∈ as, a ≤ b := fun b hb => List.rel_of_pairwise_cons h hb
      have htail : as.Pairwise (· ≤ ·) := h.of_cons
      rw [List.eraseDups_cons]
      constructor
      · intro b hb
        rw [List.mem_eraseDups] at hb
        rw [List.mem_filter] at hb
        have hab_le := hrel b hb.1
        have hne : a ≠ b := by
          intro heq
          have hbeq : b == a := by simp [heq]
          have hnot : (!b == a) = true := hb.2
          simp [hbeq] at hnot
        omega
      · exact pairwise_eraseDups_sorted (as.filter (fun b => !b == a)) (htail.filter _)
termination_by l => l.length
decreasing_by
  have hle := List.length_filter_le (fun b : Nat => !b == a) as
  simp only [List.length_cons]
  omega

private theorem knots_pairwise (n rs) : (knots n rs).Pairwise (· < ·) := by
  unfold knots
  let L : List Nat := 0 :: n :: rs.flatMap (fun (a, b) => [min a n, min b n])
  change (L.mergeSort (fun a b => decide (a ≤ b))).eraseDups.Pairwise (· < ·)
  apply pairwise_eraseDups_sorted
  have hsorted : (L.mergeSort (fun a b => decide (a ≤ b))).Pairwise
      (fun a b => (decide (a ≤ b) : Bool) = true) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp at hab hbc ⊢
      omega
    · intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba]
  simpa using hsorted

private theorem knots_mem_zero (n rs) : 0 ∈ knots n rs := by
  unfold knots
  simp

private theorem knots_mem_n (n rs) : n ∈ knots n rs := by
  unfold knots
  simp

private theorem knots_le_n (n rs) : ∀ x ∈ knots n rs, x ≤ n := by
  intro x hx
  unfold knots at hx
  rw [List.mem_eraseDups, List.mem_mergeSort] at hx
  simp only [List.mem_cons] at hx
  rcases hx with hx0 | hxn | hxflat
  · subst x
    omega
  · subst x
    exact Nat.le_refl n
  · rcases List.mem_flatMap.mp hxflat with ⟨p, hp, hxmem⟩
    rcases p with ⟨a, b⟩
    simp only [List.mem_cons, List.mem_nil_iff, or_false] at hxmem
    rcases hxmem with hxeq | hxeq
    · subst x
      exact Nat.min_le_right a n
    · subst x
      exact Nat.min_le_right b n

private theorem endpoint_in_knots (n rs) (r : Span) (hr : r ∈ rs) :
    min r.1 n ∈ knots n rs ∧ min r.2 n ∈ knots n rs := by
  rcases r with ⟨a, b⟩
  unfold knots
  constructor
  · rw [List.mem_eraseDups, List.mem_mergeSort]
    simp only [List.mem_cons]
    right; right
    exact List.mem_flatMap.mpr ⟨(a, b), hr, by simp⟩
  · rw [List.mem_eraseDups, List.mem_mergeSort]
    simp only [List.mem_cons]
    right; right
    exact List.mem_flatMap.mpr ⟨(a, b), hr, by simp⟩

private theorem piece_knots (n rs a b) (hp : (a, b) ∈ pieces n rs) :
    a ∈ knots n rs ∧ b ∈ knots n rs := by
  unfold pieces at hp
  rcases List.of_mem_zip hp with ⟨ha, hb⟩
  exact ⟨ha, List.mem_of_mem_drop hb⟩

private theorem piece_right_le_n (n rs a b) (hp : (a, b) ∈ pieces n rs) : b ≤ n :=
  knots_le_n n rs b (piece_knots n rs a b hp).2

private theorem head_le_mem {ks : List Nat} (hpair : ks.Pairwise (· < ·)) {x : Nat}
    (hx : x ∈ ks) : ks.head?.getD 0 ≤ x := by
  cases ks with
  | nil => simp at hx
  | cons y ys =>
      simp only [List.head?_cons, Option.getD_some]
      rcases List.mem_cons.mp hx with hxy | hxys
      · subst x
        omega
      · have hrel := List.rel_of_pairwise_cons hpair hxys
        omega

private theorem mem_le_last {ks : List Nat} (hpair : ks.Pairwise (· < ·)) {x : Nat}
    (hx : x ∈ ks) : x ≤ ks.getLast?.getD 0 := by
  cases ks with
  | nil => simp at hx
  | cons y ys =>
      cases ys with
      | nil =>
          simp at hx
          subst x
          simp
      | cons z zs =>
          have hyz : y < z := (List.pairwise_cons.mp hpair).1 z (by simp)
          have htail : (z :: zs).Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
          rcases List.mem_cons.mp hx with hxy | hxys
          · subst x
            have hzl := mem_le_last htail (by simp : z ∈ z :: zs)
            have hy_le : y ≤ (z :: zs).getLast?.getD 0 := by omega
            simpa [List.getLast?_cons_cons] using hy_le
          · have hxl := mem_le_last htail hxys
            simpa [List.getLast?_cons_cons] using hxl

private theorem knots_head_zero (n rs) : (knots n rs).head?.getD 0 = 0 := by
  let ks := knots n rs
  have hpair : ks.Pairwise (· < ·) := knots_pairwise n rs
  have h0 : 0 ∈ ks := knots_mem_zero n rs
  exact Nat.le_antisymm (head_le_mem hpair h0) (Nat.zero_le _)

private theorem last_eq_of_mem_le (ks : List Nat) (n : Nat)
    (hpair : ks.Pairwise (· < ·)) (hn : n ∈ ks) (hle : ∀ x ∈ ks, x ≤ n) :
    ks.getLast?.getD 0 = n := by
  revert hpair hn hle
  induction ks with
  | nil =>
      intro hpair hn hle
      simp at hn
  | cons y ys ih =>
      intro hpair hn hle
      cases ys with
      | nil =>
          simp at hn
          subst n
          simp
      | cons z zs =>
          have hyz : y < z := (List.pairwise_cons.mp hpair).1 z (by simp)
          have htail : (z :: zs).Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
          have hn_tail : n ∈ z :: zs := by
            rcases List.mem_cons.mp hn with hyn | hnt
            · subst n
              have hzle := hle z (by simp)
              omega
            · exact hnt
          have hle_tail : ∀ x ∈ z :: zs, x ≤ n := by
            intro x hx
            exact hle x (by simp [hx])
          have ih' : (z :: zs).getLast?.getD 0 = n := ih htail hn_tail hle_tail
          simpa [List.getLast?_cons_cons] using ih'

private theorem knots_last_n (n rs) : (knots n rs).getLast?.getD 0 = n :=
  last_eq_of_mem_le (knots n rs) n (knots_pairwise n rs) (knots_mem_n n rs) (knots_le_n n rs)

private theorem knot_outside : ∀ (ks : List Nat), ks.Pairwise (· < ·) →
    ∀ {x a b : Nat}, x ∈ ks → (a, b) ∈ ks.zip (ks.drop 1) → x ≤ a ∨ b ≤ x
  | [], hpair, x, a, b, hx, hp => by simp at hp
  | y :: ys, hpair, x, a, b, hx, hp => by
      cases ys with
      | nil => simp at hp
      | cons z zs =>
          have htail : (z :: zs).Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
          have hyz : y < z := (List.pairwise_cons.mp hpair).1 z (by simp)
          simp at hp
          rcases hp with hp | hp'
          · rcases hp with ⟨hya, hzb⟩
            subst a
            subst b
            rcases List.mem_cons.mp hx with rfl | hxtail
            · left
              exact Nat.le_refl _
            · right
              have hzle := head_le_mem htail hxtail
              simpa using hzle
          · rcases List.mem_cons.mp hx with rfl | hxtail
            · left
              have haz := head_le_mem htail (List.of_mem_zip hp').1
              simp only [List.head?_cons, Option.getD_some] at haz
              omega
            · exact knot_outside (z :: zs) htail hxtail hp'

private def pieceRange (p : Nat × Nat) : List Nat :=
  (List.range (p.2 - p.1)).map (fun x => p.1 + x)

private theorem mem_pieceRange_bounds {a b i : Nat} (hi : i ∈ pieceRange (a, b)) :
    a ≤ i ∧ i < b := by
  unfold pieceRange at hi
  rw [List.mem_map] at hi
  rcases hi with ⟨j, hj, rfl⟩
  rw [List.mem_range] at hj
  constructor <;> omega

private theorem endpoint_cmp_eq (n : Nat) (rs : List Span) (a b i x : Nat)
    (hp : (a, b) ∈ pieces n rs) (hi1 : a ≤ i) (hi2 : i < b)
    (hxknot : min x n ∈ knots n rs) : (x ≤ i) = (x ≤ a) := by
  have hbn : b ≤ n := piece_right_le_n n rs a b hp
  have hout := knot_outside (knots n rs) (knots_pairwise n rs) hxknot hp
  rcases hout with hxa | hbx
  · have ha_lt_n : a < n := by omega
    have hxk_lt : min x n < n := by omega
    have hxlt : x < n := by
      by_cases hlt : x < n
      · exact hlt
      · have hnx : n ≤ x := Nat.le_of_not_gt hlt
        have hxeq : min x n = n := Nat.min_eq_right hnx
        rw [hxeq] at hxk_lt
        omega
    have hxeq : min x n = x := Nat.min_eq_left (Nat.le_of_lt hxlt)
    have hxa' : x ≤ a := by omega
    have hxi' : x ≤ i := by omega
    simp [hxa', hxi']
  · have hxi_false : ¬ x ≤ i := by
      by_cases hxlt : x < n
      · have hxeq : min x n = x := Nat.min_eq_left (Nat.le_of_lt hxlt)
        omega
      · have hnx : n ≤ x := Nat.le_of_not_gt hxlt
        omega
    have hxa_false : ¬ x ≤ a := by
      by_cases hxlt : x < n
      · have hxeq : min x n = x := Nat.min_eq_left (Nat.le_of_lt hxlt)
        omega
      · have hnx : n ≤ x := Nat.le_of_not_gt hxlt
        omega
    simp [hxi_false, hxa_false]

private theorem countP_bool_eq_of_forall {α : Type} (f g : α → Bool) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.countP f = l.countP g := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.countP_cons]
      rw [h x (by simp)]
      exact congrArg (fun z => z + (if g x then 1 else 0))
        (ih (by intro y hy; exact h y (by simp [hy])))

private theorem depth_on_piece (n rs) :
    ∀ p ∈ pieces n rs, ∀ i, p.1 ≤ i → i < p.2 → depth rs i = depth rs p.1 := by
  intro p hp i hi1 hi2
  unfold depth
  congr 1
  · exact congrArg (fun z : Nat => (z : Int)) (countP_bool_eq_of_forall
      (fun r : Span => r.1 < r.2 && r.1 ≤ i)
      (fun r : Span => r.1 < r.2 && r.1 ≤ p.1) rs (by
        intro r hr
        have hcmp := endpoint_cmp_eq n rs p.1 p.2 i r.1 hp hi1 hi2
          (endpoint_in_knots n rs r hr).1
        by_cases hrd : r.1 < r.2
        · simp [hrd, hcmp]
        · simp [hrd]))
  · exact congrArg (fun z : Nat => (z : Int)) (countP_bool_eq_of_forall
      (fun r : Span => r.1 < r.2 && r.2 ≤ i)
      (fun r : Span => r.1 < r.2 && r.2 ≤ p.1) rs (by
        intro r hr
        have hcmp := endpoint_cmp_eq n rs p.1 p.2 i r.2 hp hi1 hi2
          (endpoint_in_knots n rs r hr).2
        by_cases hrd : r.1 < r.2
        · simp [hrd, hcmp]
        · simp [hrd]))

/-! ### Range partition by knots -/

private theorem range_split (a b last : Nat) (hab : a ≤ b) (hbl : b ≤ last) :
    pieceRange (a, b) ++ (List.range (last - b)).map (fun x => b + x) =
      (List.range (last - a)).map (fun x => a + x) := by
  unfold pieceRange
  have hdecomp : last - a = (b - a) + (last - b) := by omega
  rw [hdecomp, List.range_add, List.map_append]
  congr 1
  rw [List.map_map]
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  omega

private theorem partition_list (ks : List Nat) (h : ks.Pairwise (· < ·)) :
    (ks.zip (ks.drop 1)).flatMap pieceRange =
      (List.range (ks.getLast?.getD 0 - ks.head?.getD 0)).map (fun x => ks.head?.getD 0 + x) := by
  induction ks with
  | nil => simp
  | cons a ks ih =>
      cases ks with
      | nil => simp
      | cons b bs =>
          have hab : a < b := (List.pairwise_cons.mp h).1 b (by simp)
          have htail : (b :: bs).Pairwise (· < ·) := (List.pairwise_cons.mp h).2
          have ih' := ih htail
          simp only [List.drop_succ_cons, List.drop_zero] at ih'
          simp only [List.drop_succ_cons, List.drop_zero, List.zip_cons_cons, List.flatMap_cons]
          rw [ih']
          have hlast : (a :: b :: bs).getLast?.getD 0 = (b :: bs).getLast?.getD 0 := by simp
          have hlast' : (b :: bs).getLast?.getD 0 = (b :: bs).getLast?.getD 0 := rfl
          rw [hlast]
          have hlead : (a :: b :: bs).head?.getD 0 = a := by simp
          have hlead_tail : (b :: bs).head?.getD 0 = b := by simp
          rw [hlead, hlead_tail]
          exact range_split a b ((b :: bs).getLast?.getD 0) (Nat.le_of_lt hab)
            (mem_le_last htail (by simp : b ∈ b :: bs))

private theorem pieces_flatMap_eq_range (n rs) :
    (pieces n rs).flatMap pieceRange = List.range n := by
  unfold pieces
  let ks := knots n rs
  have hpair : ks.Pairwise (· < ·) := knots_pairwise n rs
  have hhead : ks.head?.getD 0 = 0 := knots_head_zero n rs
  have hlast : ks.getLast?.getD 0 = n := knots_last_n n rs
  change (ks.zip (ks.drop 1)).flatMap pieceRange = List.range n
  rw [partition_list ks hpair, hhead, hlast]
  simp

private theorem not_blocked_eq (rs : List Span) (i : Nat) :
    !blocked rs i = (depth rs i == 0) := by
  have hdnonneg : 0 ≤ depth rs i := by
    rw [depth_eq_count_hit]
    exact Int.natCast_nonneg _
  rw [sound_IntervalDeletes_SignedEndpoints rs i]
  by_cases hd0 : depth rs i = 0
  · simp [hd0]
  · have hdpos : 0 < depth rs i := by omega
    simp [hd0, hdpos]

private theorem filter_flatMap_pieces (ps : List (Nat × Nat)) (f : Nat → Bool)
    (hconst : ∀ p ∈ ps, ∀ i ∈ pieceRange p, f i = f p.1) :
    (ps.flatMap pieceRange).filter f = (ps.filter (fun p => f p.1)).flatMap pieceRange := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      have hp := hconst p (by simp)
      have htail : ∀ q ∈ ps, ∀ i ∈ pieceRange q, f i = f q.1 := by
        intro q hq i hi
        exact hconst q (by simp [hq]) i hi
      simp only [List.flatMap_cons]
      rw [List.filter_append]
      by_cases hpv : f p.1
      · simp [hpv]
        have hf_all : (pieceRange p).filter f = pieceRange p := by
          apply List.filter_eq_self.mpr
          intro i hi
          rw [hp i hi]
          exact hpv
        rw [hf_all, ih htail]
      · simp [hpv]
        have hf_nil : (pieceRange p).filter f = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro i hi
          rw [hp i hi]
          exact hpv
        rw [hf_nil, ih htail]
        simp

private theorem live_decomp (n rs) :
    live n rs = ((pieces n rs).filter (fun p => !blocked rs p.1)).flatMap pieceRange := by
  unfold live
  rw [← pieces_flatMap_eq_range n rs]
  apply filter_flatMap_pieces
  intro p hp i hi
  have hib := mem_pieceRange_bounds hi
  have hdep := depth_on_piece n rs p hp i hib.1 hib.2
  rw [sound_IntervalDeletes_SignedEndpoints rs i, sound_IntervalDeletes_SignedEndpoints rs p.1, hdep]

/-! ### choose2 arithmetic -/

private theorem two_dvd_mul_consec (n : Nat) : 2 ∣ n * (n + 1) := by
  by_cases h : n % 2 = 0
  · apply Nat.dvd_of_mod_eq_zero
    rw [Nat.mul_mod, h]
    simp
  · have h1 : n % 2 = 1 := by omega
    have hdiv : 2 ∣ n + 1 := by
      apply Nat.dvd_of_mod_eq_zero
      rw [Nat.add_mod, h1]
    rcases hdiv with ⟨k, hk⟩
    refine ⟨n * k, ?_⟩
    rw [hk]
    simp [Nat.mul_assoc, Nat.mul_comm]

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => simp [choose2]
  | succ m =>
      unfold choose2
      simp only [Nat.add_sub_cancel_right]
      have hdA : 2 ∣ (m + 2) * (m + 1) := by
        simpa [Nat.mul_comm, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using
          (two_dvd_mul_consec (m + 1))
      have hdB : 2 ∣ (m + 1) * m := by simpa [Nat.mul_comm] using two_dvd_mul_consec m
      have hA : 2 * (((m + 2) * (m + 1)) / 2) = (m + 2) * (m + 1) :=
        Nat.mul_div_cancel_left' hdA
      have hB : 2 * (((m + 1) * m) / 2) = (m + 1) * m :=
        Nat.mul_div_cancel_left' hdB
      have h2 : 2 * (((m + 2) * (m + 1)) / 2) =
          2 * ((((m + 1) * m) / 2) + (m + 1)) := by
        rw [hA]
        calc
          (m + 2) * (m + 1) = (m + 1) * m + 2 * (m + 1) := by
            rw [Nat.add_mul, Nat.mul_comm m (m + 1)]
          _ = 2 * (((m + 1) * m) / 2) + 2 * (m + 1) := by rw [hB]
          _ = 2 * ((((m + 1) * m) / 2) + (m + 1)) := by
            rw [Nat.mul_add]
            omega
      apply Nat.eq_of_mul_eq_mul_left (n := 2) (by decide)
      calc
        2 * (((m + 1 + 1) * (m + 1)) / 2) = (m + 2) * (m + 1) := by
          simpa [Nat.add_assoc] using hA
        _ = 2 * ((((m + 1) * m) / 2) + (m + 1)) := by
          rw [← hA]
          exact h2

private theorem choose2_mono {a b : Nat} (hab : a ≤ b) : choose2 a ≤ choose2 b := by
  induction b with
  | zero =>
      have ha0 : a = 0 := by omega
      subst a
      exact Nat.le_refl _
  | succ b ih =>
      by_cases h : a ≤ b
      · have ih' := ih h
        rw [choose2_succ]
        omega
      · have haeq : a = b + 1 := by omega
        subst a
        exact Nat.le_refl _

private theorem sum_pieceRange_len (a d : Nat) :
    (pieceRange (a, a + d)).sum = choose2 (a + d) - choose2 a := by
  induction d with
  | zero => simp [pieceRange, choose2]
  | succ d ih =>
      rw [show a + (d + 1) = (a + d) + 1 by omega]
      have hstep : pieceRange (a, (a + d) + 1) = pieceRange (a, a + d) ++ [a + d] := by
        unfold pieceRange
        rw [show (a + d + 1) - a = d + 1 by omega, List.range_succ, List.map_append]
        simp
      rw [hstep, List.sum_append, ih, choose2_succ]
      simp only [List.sum_cons, List.sum_nil, Nat.add_zero]
      have hmono : choose2 a ≤ choose2 (a + d) := choose2_mono (by omega)
      omega

private theorem sum_pieceRange (a b : Nat) :
    (pieceRange (a, b)).sum = choose2 b - choose2 a := by
  by_cases hab : a ≤ b
  · have h := sum_pieceRange_len a (b - a)
    have hb : a + (b - a) = b := Nat.add_sub_of_le hab
    simpa [hb] using h
  · have hba : b ≤ a := Nat.le_of_not_ge hab
    have hz : b - a = 0 := Nat.sub_eq_zero_of_le hba
    have hc : choose2 b ≤ choose2 a := choose2_mono hba
    simp [pieceRange, hz, Nat.sub_eq_zero_of_le hc]

private theorem length_flatMap_pieceRange (ps : List (Nat × Nat)) :
    (ps.flatMap pieceRange).length = (ps.map (fun p => p.2 - p.1)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp [pieceRange, ih]

private theorem sum_flatMap_pieceRange (ps : List (Nat × Nat)) :
    (ps.flatMap pieceRange).sum = (ps.map (fun p => choose2 p.2 - choose2 p.1)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      simp [ih, sum_pieceRange]

/-! ### Footer runs -/

theorem sound_IntervalDeletes_FooterRuns : FooterRuns.Goal := by
  intro n rs
  dsimp only
  let gaps := (pieces n rs).filter (fun p => depth rs p.1 == 0)
  change ((live n rs).length, (live n rs).sum) =
    ((gaps.map (fun p => p.2 - p.1)).sum,
     (gaps.map (fun p => choose2 p.2 - choose2 p.1)).sum)
  have hg : gaps = (pieces n rs).filter (fun p => !blocked rs p.1) := by
    dsimp [gaps]
    apply List.filter_congr
    intro p hp
    rw [sound_IntervalDeletes_SignedEndpoints rs p.1]
    have hdnonneg : 0 ≤ depth rs p.1 := by
      rw [depth_eq_count_hit]
      exact Int.natCast_nonneg _
    by_cases hd0 : depth rs p.1 = 0
    · simp [hd0]
    · have hdpos : 0 < depth rs p.1 := by omega
      simp [hd0, hdpos]
  rw [live_decomp n rs, hg]
  apply Prod.ext
  · exact length_flatMap_pieceRange _
  · exact sum_flatMap_pieceRange _

/-! ### Selecting a live row -/

private theorem pieceRange_get (a b k : Nat) :
    (pieceRange (a, b))[k]? = if k < b - a then some (a + k) else none := by
  unfold pieceRange
  rw [List.getElem?_map]
  by_cases h : k < b - a
  · rw [List.getElem?_range h]
    simp [h]
  · rw [List.getElem?_eq_none_iff.mpr (by simpa using Nat.le_of_not_gt h)]
    simp [h]

private theorem seek_pieces (ps : List (Nat × Nat)) (k : Nat) :
    seek ps k = (ps.flatMap pieceRange)[k]? := by
  induction ps generalizing k with
  | nil => simp [seek]
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      unfold seek
      simp only [List.flatMap_cons]
      rw [List.getElem?_append]
      have hlen : (pieceRange (a, b)).length = b - a := by simp [pieceRange]
      rw [hlen, pieceRange_get]
      by_cases hk : k < b - a
      · simp [hk]
      · simp [hk, ih]

theorem sound_IntervalDeletes_LiveSelect : LiveSelect.Goal := by
  intro n rs k
  rw [live_decomp n rs]
  exact seek_pieces ((pieces n rs).filter (fun p => !blocked rs p.1)) k

/-! ## ThresholdViews -/

private theorem countP_eq_sum_indicator {α : Type} (p : α → Bool) (ks : List α) :
    (ks.countP p : Int) = ((ks.map (fun k => indicator (p k))).sum) := by
  induction ks with
  | nil => simp [indicator]
  | cons a as ih =>
      rw [List.countP_cons]
      simp only [List.map_cons, List.sum_cons]
      by_cases h : p a = true
      · simp [h, indicator, ih]
        omega
      · simp [h, indicator, ih]

private theorem count_eq_sum_indicator (xs : Bag) :
    (count xs : Int) = ((keys xs).map (fun k => indicator (active xs k))).sum := by
  unfold count
  exact countP_eq_sum_indicator (active xs) (keys xs)

private theorem mem_keys (xs : Bag) (k : Nat) : k ∈ keys xs ↔ k ∈ xs.map Prod.fst := by
  simp [keys]

private theorem weight_append (xs ys : Bag) (k : Nat) :
    weight (xs ++ ys) k = weight xs k + weight ys k := by
  simp [weight]

private theorem weight_of_not_mem (xs : Bag) (k : Nat) (h : k ∉ xs.map Prod.fst) :
    weight xs k = 0 := by
  induction xs with
  | nil => simp [weight]
  | cons r rs ih =>
      simp only [List.map_cons, List.mem_cons] at h
      have hrk : k ≠ r.1 := by intro he; exact h (Or.inl he)
      have hrs : k ∉ rs.map Prod.fst := by intro hk; exact h (Or.inr hk)
      have hr : r.1 ≠ k := by intro he; exact hrk he.symm
      have ih' : ((rs.filter (fun r => r.1 == k)).map Prod.snd).sum = 0 := by
        simpa [weight] using ih hrs
      simp [weight, hr, ih']

private theorem weight_of_not_keys (xs : Bag) (k : Nat) (h : k ∉ keys xs) :
    weight xs k = 0 := by
  exact weight_of_not_mem xs k (by simpa using (not_congr (mem_keys xs k)).mp h)

private theorem active_of_weight_zero (xs : Bag) (k : Nat) (h : weight xs k = 0) :
    active xs k = false := by
  simp [active, h]

private theorem active_append_of_weight_zero (xs ys : Bag) (k : Nat)
    (h : weight ys k = 0) : active (xs ++ ys) k = active xs k := by
  simp [active, weight_append, h]

private def delta (old patch : Bag) (k : Nat) : Int :=
  indicator (active (old ++ patch) k) - indicator (active old k)

private theorem delta_zero_of_weight_zero (q p : Bag) (k : Nat) (h : weight p k = 0) :
    delta q p k = 0 := by
  simp [delta, active_append_of_weight_zero q p k h]

private theorem delta_telescope (old a b : Bag) (k : Nat) :
    delta old a k + delta (old ++ a) b k = delta old (a ++ b) k := by
  dsimp [delta]
  rw [List.append_assoc]
  omega

private theorem delta_of_weight_zero_left (old a b : Bag) (k : Nat)
    (h : weight a k = 0) : delta old (a ++ b) k = delta (old ++ a) b k := by
  dsimp [delta]
  rw [List.append_assoc]
  have hw : weight (old ++ (a ++ b)) k = weight ((old ++ a) ++ b) k := by
    simp [weight_append, h, List.append_assoc]
  have ha : active (old ++ (a ++ b)) k = active ((old ++ a) ++ b) k := by
    simp [active, hw]
  have hb : active (old ++ a) k = active old k := active_append_of_weight_zero old a k h
  rw [ha, hb]

private theorem delta_of_weight_zero_right (old a b : Bag) (k : Nat)
    (h : weight b k = 0) : delta old (a ++ b) k = delta old a k := by
  dsimp [delta]
  have hw : weight (old ++ (a ++ b)) k = weight (old ++ a) k := by
    simp [weight_append, h]
  have ha : active (old ++ (a ++ b)) k = active (old ++ a) k := by
    simp [active, hw]
  rw [ha]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  refine Nat.strongRecOn (motive := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup)
    l.length ?_ l rfl
  intro n ih l hl
  cases l with
  | nil => simp
  | cons a as =>
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      constructor
      · intro hm
        have hfil : a ∈ (as.filter fun b => !(b == a)) := List.mem_eraseDups.mp hm
        have hc := (List.mem_filter.mp hfil).2
        simp at hc
      · have hlt : (as.filter fun b => !(b == a)).length < n := by
          rw [← hl]
          exact Nat.lt_succ_of_le (List.length_filter_le _ as)
        exact ih _ hlt (as.filter fun b => !(b == a)) rfl

private theorem weight_map_nodup (f : Nat → Int) (ks : List Nat) (hnd : ks.Nodup) (k : Nat) :
    (((ks.map (fun k => (k, f k))).filter (fun r => r.1 == k)).map Prod.snd).sum =
      if k ∈ ks then f k else 0 := by
  induction ks with
  | nil => simp
  | cons a as ih =>
      have hnot : a ∉ as := (List.nodup_cons.mp hnd).1
      have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
      simp only [List.map_cons, List.filter_cons, List.mem_cons]
      by_cases hak : a = k
      · subst a
        have hknot : k ∉ as := by simpa using hnot
        simp [hknot, ih hndas]
      · have hbe : (a == k) = false := by simp [hak]
        have hkne : k ≠ a := by intro he; exact hak he.symm
        simp [hbe, hkne, ih hndas]

private theorem weight_changes (old p : Bag) (k : Nat) :
    weight (changes old p) k = if k ∈ keys p then delta old p k else 0 := by
  unfold changes weight keys
  exact weight_map_nodup (delta old p) ((p.map Prod.fst).eraseDups) (nodup_eraseDups _) k

private theorem sum_eq_zero_of_all_zero (f : Nat → Int) (s : List Nat)
    (h : ∀ x ∈ s, f x = 0) : (s.map f).sum = 0 := by
  induction s with
  | nil => simp
  | cons a as ih =>
      have ha : f a = 0 := h a (by simp)
      have has : ∀ x ∈ as, f x = 0 := by intro x hx; exact h x (by simp [hx])
      simp only [List.map_cons, List.sum_cons]
      rw [ha, ih has]
      simp

private theorem sum_eq_erase_add (a : Nat) (f : Nat → Int) (s : List Nat) (hs : s.Nodup)
    (ha : a ∈ s) : (s.map f).sum = f a + ((s.erase a).map f).sum := by
  induction s with
  | nil => simp at ha
  | cons b bs ih =>
      have hbnot : b ∉ bs := (List.nodup_cons.mp hs).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hs).2
      by_cases hbe : (b == a) = true
      · have hba : b = a := beq_iff_eq.mp hbe
        subst b
        rw [List.erase_cons_head]
        simp
      · have habs : a ∈ bs := by
          simp only [List.mem_cons] at ha
          rcases ha with h | h
          · have : (b == a) = true := by simp [h.symm]
            exact False.elim (hbe this)
          · exact h
        rw [List.erase_cons_tail hbe]
        simp only [List.map_cons, List.sum_cons]
        rw [ih hbsnd habs]
        omega

private theorem ne_of_mem_erase_of_nodup {a x : Nat} {s : List Nat} (hs : s.Nodup)
    (h : x ∈ s.erase a) : x ≠ a := by
  induction s with
  | nil => simp at h
  | cons b bs ih =>
      have hbnot : b ∉ bs := (List.nodup_cons.mp hs).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hs).2
      by_cases hba : (b == a) = true
      · have hba_eq : b = a := beq_iff_eq.mp hba
        subst b
        rw [List.erase_cons_head] at h
        intro hxa
        subst x
        exact hbnot h
      · rw [List.erase_cons_tail hba] at h
        rcases List.mem_cons.mp h with hxb | hxbs
        · intro hxa
          apply hba
          have hba_eq : b = a := by omega
          simp [hba_eq]
        · exact ih hbsnd hxbs

private theorem sum_subset_of_vanish (f : Nat → Int) (s : List Nat) (hs : s.Nodup)
    (t : List Nat) (ht : t.Nodup) (hsub : ∀ x ∈ t, x ∈ s)
    (hz : ∀ x ∈ s, x ∉ t → f x = 0) : (t.map f).sum = (s.map f).sum := by
  induction t generalizing s with
  | nil =>
      symm
      apply sum_eq_zero_of_all_zero
      intro x hx
      exact hz x hx (by simp)
  | cons a t ih =>
      have hnot : a ∉ t := (List.nodup_cons.mp ht).1
      have htnd : t.Nodup := (List.nodup_cons.mp ht).2
      have has : a ∈ s := hsub a (by simp)
      have hs_erase : (s.erase a).Nodup := hs.erase a
      have hsub' : ∀ x ∈ t, x ∈ s.erase a := by
        intro x hxt
        have hxs : x ∈ s := hsub x (by simp [hxt])
        have hxa : x ≠ a := by
          intro he
          apply hnot
          simpa [he] using hxt
        exact (List.mem_erase_of_ne hxa).mpr hxs
      have hz' : ∀ x ∈ s.erase a, x ∉ t → f x = 0 := by
        intro x hx hxt
        apply hz x (List.mem_of_mem_erase hx)
        intro hxa_t
        rcases List.mem_cons.mp hxa_t with heq | htmem
        · exact (ne_of_mem_erase_of_nodup hs hx) heq
        · exact hxt htmem
      have ih' := ih (s.erase a) hs_erase htnd hsub' hz'
      have hsum_s := sum_eq_erase_add a f s hs has
      simp only [List.map_cons, List.sum_cons]
      rw [hsum_s, ih']

private theorem sum_sub (f g : Nat → Int) (s : List Nat) :
    (s.map (fun k => f k - g k)).sum = (s.map f).sum - (s.map g).sum := by
  induction s with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem keys_append_mem (a b : Bag) (k : Nat) :
    k ∈ keys (a ++ b) ↔ k ∈ keys a ∨ k ∈ keys b := by
  simp [keys]

private theorem weight_cons (a k : Nat) (v : Int) (rs : Bag) :
    weight ((a, v) :: rs) k = (if a == k then v else 0) + weight rs k := by
  by_cases h : (a == k) = true
  · have hak : a = k := beq_iff_eq.mp h
    subst a
    simp [weight]
  · have hak : a ≠ k := by intro he; exact h (by simp [he])
    simp [weight, h]

private theorem sum_add (f g : Nat → Int) (s : List Nat) :
    (s.map (fun k => f k + g k)).sum = (s.map f).sum + (s.map g).sum := by
  induction s with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_single_of_nodup (g : Nat → Int) (ks : List Nat) (hnd : ks.Nodup) (a : Nat) :
    (ks.map (fun k => if k = a then g k else 0)).sum = if a ∈ ks then g a else 0 := by
  induction ks with
  | nil => simp
  | cons b bs ih =>
      have hbnot : b ∉ bs := (List.nodup_cons.mp hnd).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hba : b = a
      · subst b
        have hanot : a ∉ bs := by simpa using hbnot
        simp [hanot, ih hbsnd]
      · have hab : a ≠ b := by intro he; exact hba he.symm
        simp [hba, hab, ih hbsnd]

private theorem changes_weighted_sum (right patch : Bag) (f : Nat → Int) :
    ((changes right patch).map (fun (k, d) => d * f k)).sum =
      ((keys patch).map (fun k => delta right patch k * f k)).sum := by
  unfold changes
  simp only [List.map_map]
  rfl

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem indicator_mul (b1 b2 : Bool) (v : Int) :
    (if b1 then v else 0) - (if b2 then v else 0) = (indicator b1 - indicator b2) * v := by
  cases b1 <;> cases b2 <;> simp [indicator]

private theorem natAbs_sum_le_sum_natAbs (xs : List Int) :
    xs.sum.natAbs ≤ (xs.map Int.natAbs).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.le_trans (Int.natAbs_add_le x xs.sum) (Nat.add_le_add_left ih x.natAbs)

private theorem keys_old_subset (old patch : Bag) :
    ∀ k ∈ keys old, k ∈ keys (old ++ patch) := by
  intro k hk
  exact (keys_append_mem old patch k).mpr (Or.inl hk)

theorem sound_ThresholdViews_ZeroCrossing :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.ZeroCrossing.Goal := by
  intro old patch
  rw [count_eq_sum_indicator (old ++ patch), count_eq_sum_indicator old]
  let S := keys (old ++ patch)
  let fnew : Nat → Int := fun k => indicator (active (old ++ patch) k)
  let fold : Nat → Int := fun k => indicator (active old k)
  let c : Nat → Int := fun k => delta old patch k
  change (S.map fnew).sum - ((keys old).map fold).sum =
    ((changes old patch).map Prod.snd).sum
  have hSnd : S.Nodup := by
    dsimp [S]
    exact nodup_eraseDups _
  have hsub_old : ∀ k ∈ keys old, k ∈ S := by
    intro k hk
    dsimp [S]
    exact keys_old_subset old patch k hk
  have hzero_fold : ∀ k ∈ S, k ∉ keys old → fold k = 0 := by
    intro k hkS hknot
    have hw : weight old k = 0 := weight_of_not_keys old k hknot
    have ha : active old k = false := active_of_weight_zero old k hw
    simp [fold, ha, indicator]
  have hsum_fold : (S.map fold).sum = ((keys old).map fold).sum := by
    symm
    exact sum_subset_of_vanish fold S hSnd (keys old) (nodup_eraseDups _) hsub_old hzero_fold
  have hsub_patch : ∀ k ∈ keys patch, k ∈ S := by
    intro k hk
    dsimp [S]
    exact (keys_append_mem old patch k).mpr (Or.inr hk)
  have hzero_c : ∀ k ∈ S, k ∉ keys patch → c k = 0 := by
    intro k hkS hknot
    have hw : weight patch k = 0 := weight_of_not_keys patch k hknot
    dsimp [c]
    exact delta_zero_of_weight_zero old patch k hw
  have hsum_c : (S.map c).sum = ((keys patch).map c).sum := by
    symm
    exact sum_subset_of_vanish c S hSnd (keys patch) (nodup_eraseDups _) hsub_patch hzero_c
  have hdiff : (S.map fnew).sum - ((keys old).map fold).sum = (S.map c).sum := by
    rw [← hsum_fold]
    rw [← sum_sub fnew fold S]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rfl
  rw [hdiff, hsum_c]
  dsimp [c]
  unfold changes
  simp only [List.map_map]
  rfl

theorem sound_ThresholdViews_Coalesce :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.Coalesce.Goal := by
  intro old a b k
  rw [weight_append]
  rw [weight_changes old (a ++ b) k]
  rw [weight_changes old a k]
  rw [weight_changes (old ++ a) b k]
  have hmem : k ∈ keys (a ++ b) ↔ k ∈ keys a ∨ k ∈ keys b := keys_append_mem a b k
  by_cases hpa : k ∈ keys a
  · by_cases hpb : k ∈ keys b
    · have hpab : k ∈ keys (a ++ b) := hmem.mpr (Or.inl hpa)
      simp [hpa, hpb, hpab]
      exact (delta_telescope old a b k).symm
    · have hpab : k ∈ keys (a ++ b) := hmem.mpr (Or.inl hpa)
      simp [hpa, hpb, hpab]
      have hw : weight b k = 0 := weight_of_not_keys b k hpb
      exact delta_of_weight_zero_right old a b k hw
  · by_cases hpb : k ∈ keys b
    · have hpab : k ∈ keys (a ++ b) := hmem.mpr (Or.inr hpb)
      simp [hpa, hpb, hpab]
      have hw : weight a k = 0 := weight_of_not_keys a k hpa
      exact delta_of_weight_zero_left old a b k hw
    · have hpab : k ∉ keys (a ++ b) := by
        intro hk
        rcases hmem.mp hk with h | h <;> contradiction
      simp [hpa, hpb, hpab]

theorem sound_ThresholdViews_SemijoinRepair :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.SemijoinRepair.Goal := by
  intro left right patch
  induction left with
  | nil =>
      simp [semi, weight, sum_map_zero]
  | cons r rs ih =>
      rcases r with ⟨a, v⟩
      have hR :
          ((changes right patch).map (fun (k, d) => d * weight ((a, v) :: rs) k)).sum =
            ((changes right patch).map (fun (k, d) => d * weight rs k)).sum +
              delta right patch a * v := by
        rw [changes_weighted_sum, changes_weighted_sum]
        have hsplit : ∀ k, delta right patch k * weight ((a, v) :: rs) k =
            delta right patch k * weight rs k +
              (if k = a then delta right patch k * v else 0) := by
          intro k
          rw [weight_cons]
          by_cases hk : k = a
          · subst k
            simp
            rw [Int.mul_add]
            omega
          · have hak : a ≠ k := by intro he; exact hk he.symm
            simp [hk, hak]
        rw [show (keys patch).map (fun k => delta right patch k * weight ((a, v) :: rs) k) =
              (keys patch).map (fun k =>
                delta right patch k * weight rs k +
                  (if k = a then delta right patch k * v else 0)) by
              apply List.map_congr_left
              intro k hk
              exact hsplit k]
        rw [sum_add]
        rw [sum_single_of_nodup (fun k => delta right patch k * v) (keys patch)
          (nodup_eraseDups _) a]
        by_cases ha : a ∈ keys patch
        · simp [ha]
        · have hw : weight patch a = 0 := weight_of_not_keys patch a ha
          have hd : delta right patch a = 0 := delta_zero_of_weight_zero right patch a hw
          simp [ha, hd]
      have hda : (if a ∈ keys patch then delta right patch a * v else 0) =
          delta right patch a * v := by
        by_cases ha : a ∈ keys patch
        · simp [ha]
        · have hw : weight patch a = 0 := weight_of_not_keys patch a ha
          have hd : delta right patch a = 0 := delta_zero_of_weight_zero right patch a hw
          simp [ha, hd]
      have hfirst : (if active (right ++ patch) a then v else 0) -
            (if active right a then v else 0) = delta right patch a * v := by
        dsimp [delta]
        exact indicator_mul (active (right ++ patch) a) (active right a) v
      have hsemi1 : semi ((a, v) :: rs) (right ++ patch) =
            (if active (right ++ patch) a then v else 0) + semi rs (right ++ patch) := by
        by_cases h : active (right ++ patch) a = true
        · simp [semi, h]
        · have hf : active (right ++ patch) a = false := Bool.eq_false_of_ne_true h
          simp [semi, hf]
      have hsemi2 : semi ((a, v) :: rs) right =
            (if active right a then v else 0) + semi rs right := by
        by_cases h : active right a = true
        · simp [semi, h]
        · have hf : active right a = false := Bool.eq_false_of_ne_true h
          simp [semi, hf]
      rw [hsemi1, hsemi2, hR]
      calc
        ((if active (right ++ patch) a then v else 0) + semi rs (right ++ patch)) -
            ((if active right a then v else 0) + semi rs right) =
            ((if active (right ++ patch) a then v else 0) -
              (if active right a then v else 0)) +
            (semi rs (right ++ patch) - semi rs right) := by omega
        _ = delta right patch a * v +
            ((changes right patch).map (fun (k, d) => d * weight rs k)).sum := by
              rw [hfirst, ih]
        _ = ((changes right patch).map (fun (k, d) => d * weight rs k)).sum +
            delta right patch a * v := by omega

theorem sound_ThresholdViews_MarginCertificate :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.MarginCertificate.Goal := by
  intro old patch k h
  have hsum : (weight patch k).natAbs ≤
      ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)).sum := by
    have hs := natAbs_sum_le_sum_natAbs ((patch.filter (fun r => r.1 == k)).map Prod.snd)
    have hmap : (((patch.filter (fun r => r.1 == k)).map Prod.snd).map Int.natAbs) =
        ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)) := by
      rw [List.map_map]
      rfl
    simpa [weight, hmap] using hs
  have hlt : (weight patch k).natAbs < (weight old k).natAbs := by omega
  have hltI : ((weight patch k).natAbs : Int) < ((weight old k).natAbs : Int) :=
    Int.ofNat_lt.mpr hlt
  by_cases hpos : 0 < weight old k
  · have hold : ((weight old k).natAbs : Int) = weight old k :=
      Int.natAbs_of_nonneg (by omega)
    have hp_le : weight patch k ≤ ((weight patch k).natAbs : Int) := Int.le_natAbs
    have hnew : 0 < weight (old ++ patch) k := by
      rw [weight_append]
      omega
    simp [active, hpos, hnew]
  · have hne : weight old k ≠ 0 := by
      intro hz
      rw [hz] at hlt
      have hznonneg : 0 ≤ (weight patch k).natAbs := Nat.zero_le _
      omega
    have holdneg : weight old k < 0 := by omega
    have hold : ((weight old k).natAbs : Int) = -(weight old k) := by
      rw [← Int.natAbs_neg]
      exact Int.natAbs_of_nonneg (by omega)
    have hp_ge : -(weight patch k) ≤ ((weight patch k).natAbs : Int) := by
      have ht : -weight patch k ≤ (((-weight patch k).natAbs : Int)) := Int.le_natAbs
      rwa [Int.natAbs_neg] at ht
    have hnew : ¬ 0 < weight (old ++ patch) k := by
      rw [weight_append]
      omega
    simp [active, hpos, hnew]

theorem sound_ThresholdViews_ReplayLedger :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.ReplayLedger.Goal := by
  intro old p k
  rw [weight_changes]
  by_cases hk : k ∈ keys p
  · simp [hk, delta]
    omega
  · have hw : weight p k = 0 := weight_of_not_keys p k hk
    have ha : active (old ++ p) k = active old k := active_append_of_weight_zero old p k hw
    simp [hk, ha]

/-! ## BatchWatermarks -/

private theorem beq_eq_decide (a b : Nat) : (a == b) = decide (a = b) := by
  by_cases h : a == b
  · have hab : a = b := beq_iff_eq.mp h
    simp [hab]
  · have hab : a ≠ b := by intro he; exact h (by simp [he])
    simp [hab]

private theorem mem_le_maxD (ns : List Nat) (n : Nat) (hn : n ∈ ns) : n ≤ ns.max?.getD 0 := by
  cases ns with
  | nil => simp at hn
  | cons a as =>
      have hmax : (a :: as).max? = some ((a :: as).max?.getD 0) := by
        rw [List.max?_cons]
        simp
      exact (List.max?_eq_some_iff.mp hmax).2 n (by simp [hn])

private theorem row_le_stamp (r : Row) (rs : List Row) (hr : r ∈ rs) : r.1 ≤ stamp rs := by
  dsimp [stamp]
  apply mem_le_maxD
  exact List.mem_map.mpr ⟨r, hr, rfl⟩

private theorem score_append (t : Nat) (xs ys : List Row) :
    score t (xs ++ ys) = score t xs + score t ys := by
  simp [score, visible, List.filter_append]

private theorem score_flatten (t : Nat) (runs : List (List Row)) :
    score t runs.flatten = (runs.map (score t)).sum := by
  induction runs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rw [List.flatten_cons, score_append]
      simp [ih]

private theorem score_eq_sum_of_stamp_le (t : Nat) (r : List Row) (h : stamp r ≤ t) :
    score t r = (r.map Prod.snd).sum := by
  have hv : visible t r = r := by
    apply List.filter_eq_self.mpr
    intro row hr
    exact decide_eq_true (by have h' := row_le_stamp row r hr; omega)
  simp [score, hv]

private theorem score_summaries (t : Nat) (runs : List (List Row)) :
    score t (summaries runs) =
      ((runs.filter (fun r => stamp r ≤ t)).map (fun r => (r.map Prod.snd).sum)).sum := by
  induction runs with
  | nil => simp [score, visible, summaries]
  | cons r rs ih =>
      by_cases h : stamp r ≤ t
      · simp [summaries, score, visible, h, List.filter_map, Function.comp_def]
      · simp [summaries, score, visible, h, List.filter_map, Function.comp_def]

private theorem score_cons (t : Nat) (a : Nat) (w : Int) (rs : List Row) :
    score t ((a, w) :: rs) = (if a ≤ t then w else 0) + score t rs := by
  by_cases h : a ≤ t <;> simp [score, visible, h]

private theorem score_zero (xs : List Row) :
    score 0 xs = ((xs.filter (fun r => r.1 == 0)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons]
      have ih' : score 0 rs = ((rs.filter (fun r => decide (r.1 = 0))).map Prod.snd).sum := by
        simpa [beq_eq_decide] using ih
      rw [ih']
      by_cases haeq : a = 0 <;>
        simp [haeq, beq_eq_decide] <;> omega

private theorem score_succ (xs : List Row) (n : Nat) :
    score (n + 1) xs = score n xs + ((xs.filter (fun r => r.1 == n + 1)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons, score_cons]
      have ih' : score (n + 1) rs =
          score n rs + ((rs.filter (fun r => decide (r.1 = n + 1))).map Prod.snd).sum := by
        simpa [beq_eq_decide] using ih
      rw [ih']
      by_cases h1 : a ≤ n + 1 <;> by_cases h0 : a ≤ n <;> by_cases haeq : a = n + 1 <;>
        simp [h1, h0, haeq, beq_eq_decide] <;> omega

theorem sound_BatchWatermarks_PartialOpen :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.PartialOpen.Goal := by
  intro runs t
  rw [score_flatten t runs, score_summaries t runs]
  induction runs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : stamp r ≤ t
      · have hs := score_eq_sum_of_stamp_le t r h
        have hnot : ¬ t < stamp r := by omega
        simp [h, hnot, hs, ih]
        omega
      · have hlt : t < stamp r := by omega
        simp [h, hlt, ih]
        omega

theorem sound_BatchWatermarks_CoarseTime :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.CoarseTime.Goal := by
  intro xs t step hstep
  have hroundedDecide : score (t / step) (rounded step xs) =
      score t xs +
        ((xs.filter (fun r => decide (r.1 / step = t / step) && decide (t < r.1))).map Prod.snd).sum := by
    induction xs with
    | nil => simp [score, visible, rounded]
    | cons r rs ih =>
        rcases r with ⟨a, w⟩
        change score (t / step) ((a / step, w) :: rounded step rs) =
          score t ((a, w) :: rs) +
            ((((a, w) :: rs).filter (fun r => decide (r.1 / step = t / step) && decide (t < r.1))).map Prod.snd).sum
        rw [score_cons, score_cons]
        by_cases hat : a ≤ t
        · have hdiv : a / step ≤ t / step := Nat.div_le_div_right hat
          have hnlt : ¬ t < a := by omega
          simp [hat, hdiv, hnlt, ih]
          rw [Int.add_assoc]
        · have hta : t < a := by omega
          by_cases hdiv : a / step ≤ t / step
          · have hdiv' : t / step ≤ a / step := Nat.div_le_div_right (by omega : t ≤ a)
            have heq : a / step = t / step := by omega
            simp [hat, hta, heq, ih]
            ac_rfl
          · have hne : a / step ≠ t / step := by omega
            simp [hat, hta, hne, hdiv, ih]
  have hrounded : score (t / step) (rounded step xs) =
      score t xs + ((xs.filter (fun r => r.1 / step == t / step && t < r.1)).map Prod.snd).sum := by
    simpa only [beq_eq_decide] using hroundedDecide
  rw [hrounded]
  omega

private theorem band_score (lo hi : Nat) (xs : List Row) (hlohi : lo ≤ hi) :
    score hi xs - score lo xs =
      ((xs.filter (fun r => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons, score_cons]
      have hsplit : (if a ≤ hi then w else 0) + score hi rs -
          ((if a ≤ lo then w else 0) + score lo rs) =
          (score hi rs - score lo rs) + ((if a ≤ hi then w else 0) - (if a ≤ lo then w else 0)) := by
        omega
      rw [hsplit, ih]
      by_cases hhi : a ≤ hi <;> by_cases hltlo : lo < a <;>
        simp [hhi, hltlo] <;> omega

theorem sound_BatchWatermarks_HistoricalBand :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.HistoricalBand.Goal := by
  intro xs late lo hi hlohi
  rw [score_append, score_append]
  have hb := band_score lo hi late hlohi
  omega

theorem sound_BatchWatermarks_EquivalentHistory :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.EquivalentHistory.Goal := by
  intro a b
  constructor
  · intro h t
    cases t with
    | zero =>
        have ha := score_zero a
        have hb := score_zero b
        have ht := h 0
        omega
    | succ n =>
        have ha := score_succ a n
        have hb := score_succ b n
        have ht := h (n + 1)
        have hp := h n
        omega
  · intro h t
    induction t with
    | zero =>
        have ha := score_zero a
        have hb := score_zero b
        have ht := h 0
        omega
    | succ n ih =>
        have ha := score_succ a n
        have hb := score_succ b n
        have ht := h (n + 1)
        omega

/-- Equal final stamps and totals do not determine the intermediate prefixes. -/
theorem refutation_BatchWatermarks_FinalSummary :
    ¬ BatchWatermarks.FinalSummary.Goal := by
  intro h
  let a : List BatchWatermarks.Row := [(0, 4), (1, -4)]
  let b : List BatchWatermarks.Row := [(0, 0), (1, 0)]
  have hh := h a b (by decide) (by decide) 0
  have hneq : ¬ (BatchWatermarks.score 0 a = BatchWatermarks.score 0 b) := by
    simp [a, b, BatchWatermarks.score, BatchWatermarks.visible]
  exact hneq hh

end TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance
