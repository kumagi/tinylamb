import TinylambOpt.ResearchGoals.RestartRaces.Catalog
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Catalog_OnlineUnique
open TinylambOpt.ResearchGoals.RestartRaces.Catalog.OnlineUnique
theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      constructor
      · intro hmem
        rw [List.mem_eraseDups] at hmem
        simp at hmem
      · exact nodup_eraseDups (as.filter fun b => !b == a)
termination_by l => l.length
decreasing_by
  rw [List.length_cons]
  have := List.length_filter_le (fun b => !b == a) as
  omega

theorem filter_partition (cs : List Change)
    (hcs : cs.Pairwise (fun a b => a.ts ≤ b.ts)) (i cut endTs : Nat) (hle : cut ≤ endTs) :
    cs.filter (fun c => c.rid == i && c.ts ≤ endTs) =
      cs.filter (fun c => c.rid == i && c.ts ≤ cut) ++
      cs.filter (fun c => c.rid == i && cut < c.ts && c.ts ≤ endTs) := by
  induction cs with
  | nil => simp
  | cons c cs ih =>
      have hp := List.pairwise_cons.mp hcs
      have hhead : ∀ d ∈ cs, c.ts ≤ d.ts := hp.1
      have htail : cs.Pairwise (fun a b => a.ts ≤ b.ts) := hp.2
      by_cases hri : c.rid == i
      · by_cases hc : c.ts ≤ cut
        · have he : c.ts ≤ endTs := Nat.le_trans hc hle
          simp [hri, hc, he, ih htail]
        · have hlt : cut < c.ts := by omega
          have hnoPre : cs.filter (fun d => d.rid == i && d.ts ≤ cut) = [] := by
            rw [List.filter_eq_nil_iff]
            intro d hd
            by_cases hrid : d.rid == i
            · by_cases hlecut : d.ts ≤ cut
              · have hleD := hhead d hd
                omega
              · simp [hrid, hlecut]
            · simp [hrid]
          by_cases he : c.ts ≤ endTs
          · simp [hri, hc, hlt, he, hnoPre, ih htail]
          · have hnoAll : cs.filter (fun d => d.rid == i && d.ts ≤ endTs) = [] := by
              rw [List.filter_eq_nil_iff]
              intro d hd
              by_cases hrid : d.rid == i
              · by_cases hleend : d.ts ≤ endTs
                · have hleD := hhead d hd
                  omega
                · simp [hrid, hleend]
              · simp [hrid]
            have hnoWin : cs.filter (fun d => d.rid == i && cut < d.ts && d.ts ≤ endTs) = [] := by
              rw [List.filter_eq_nil_iff]
              intro d hd
              by_cases hrid : d.rid == i
              · by_cases hleend : d.ts ≤ endTs
                · have hleD := hhead d hd
                  omega
                · simp [hrid, hleend]
              · simp [hrid]
            simp [hri, hc, hlt, he, hnoPre, hnoAll, hnoWin]
      · simp [hri, ih htail]

theorem changes_filter (cs : List Change) (cut : Nat → Nat) (endTs i : Nat) :
    (cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)).filter (fun c => c.rid == i) =
      cs.filter (fun c => c.rid == i && cut i < c.ts && c.ts ≤ endTs) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro c hc
  by_cases hri : c.rid == i
  · have hri' : c.rid = i := by simpa using hri
    simp [hri, hri']
  · simp [hri]

theorem last_append_nonempty (base : Option Nat) (l₁ l₂ : List Change) (hne : l₂ ≠ []) :
    last base (l₁ ++ l₂) = last none l₂ := by
  cases hl2 : l₂.getLast? with
  | none =>
      have : l₂ = [] := List.getLast?_eq_none_iff.mp hl2
      exact False.elim (hne this)
  | some c =>
      simp [last, List.getLast?_append, hl2]

theorem mem_dirty (changes : List Change) (i : Nat) :
    i ∈ (changes.map Change.rid).eraseDups ↔ ∃ c ∈ changes, c.rid = i := by
  simp [List.mem_eraseDups, List.mem_map]

theorem rebuilt_eq_final (cs : List Change) (base : Nat → Option Nat) (cut : Nat → Nat)
    (endTs i : Nat) (hcs : cs.Pairwise (fun a b => a.ts ≤ b.ts))
    (hcut : cut i ≤ endTs)
    (hdirty : i ∈ ((cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)).map Change.rid).eraseDups) :
    last none ((cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)).filter (fun c => c.rid == i)) =
      asOf base cs i endTs := by
  let changes := cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)
  let win := changes.filter (fun c => c.rid == i)
  have hwinne : win ≠ [] := by
    intro hempty
    rcases (mem_dirty changes i).1 hdirty with ⟨c, hc, hcrid⟩
    have hcwin : c ∈ win := by simp [win, hc, hcrid]
    simpa [hempty] using hcwin
  have hpart := filter_partition cs hcs i (cut i) endTs hcut
  have hwinEq : cs.filter (fun c => c.rid == i && cut i < c.ts && c.ts ≤ endTs) = win := by
    simpa [changes, win] using (changes_filter cs cut endTs i).symm
  have hall : cs.filter (fun c => c.rid == i && c.ts ≤ endTs) =
      cs.filter (fun c => c.rid == i && c.ts ≤ cut i) ++ win := by
    rw [hpart, hwinEq]
  change last none win = asOf base cs i endTs
  rw [asOf]
  rw [hall]
  exact (last_append_nonempty (base i) _ _ hwinne).symm

theorem scanned_eq_final (cs : List Change) (base : Nat → Option Nat) (cut : Nat → Nat)
    (endTs i : Nat) (hcs : cs.Pairwise (fun a b => a.ts ≤ b.ts))
    (hcut : cut i ≤ endTs)
    (hdirty : i ∉ ((cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)).map Change.rid).eraseDups) :
    asOf base cs i (cut i) = asOf base cs i endTs := by
  let changes := cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)
  let win := changes.filter (fun c => c.rid == i)
  have hwinNil : win = [] := by
    rw [List.filter_eq_nil_iff]
    intro c hc
    by_cases hri : c.rid == i
    · have hrid : c.rid = i := by simpa using hri
      have : i ∈ (changes.map Change.rid).eraseDups := (mem_dirty changes i).2 ⟨c, hc, hrid⟩
      exact False.elim (hdirty (by simpa [changes] using this))
    · simp [hri]
  have hpart := filter_partition cs hcs i (cut i) endTs hcut
  have hwinEq : cs.filter (fun c => c.rid == i && cut i < c.ts && c.ts ≤ endTs) = win := by
    simpa [changes, win] using (changes_filter cs cut endTs i).symm
  have hall : cs.filter (fun c => c.rid == i && c.ts ≤ endTs) =
      cs.filter (fun c => c.rid == i && c.ts ≤ cut i) ++ win := by
    rw [hpart, hwinEq]
  rw [asOf, asOf, hall, hwinNil]
  simp [last, List.getLast?_append]

theorem mem_entries (n : Nat) (state : Nat → Option Nat) (v i : Nat) :
    (v, i) ∈ entries n state ↔ i < n ∧ state i = some v := by
  simp [entries, List.mem_filterMap]
  constructor
  · rintro ⟨a, ha, a₁, hs, hv, hi⟩
    subst a₁
    subst a
    exact ⟨ha, hs⟩
  · rintro ⟨hi, hs⟩
    exact ⟨i, hi, v, hs, rfl, rfl⟩


theorem nodup_filterMap_of_inj {α β : Type} (f : α → Option β) (l : List α)
    (hl : l.Nodup)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, ∀ va vb, f a = some va → f b = some vb → va = vb → a = b) :
    (l.filterMap f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      cases hfa : f a with
      | none =>
          simp [hfa]
          apply ih hl.2
          intro x hx y hy vx vy hxv hyv hvy
          exact hinj x (by simp [hx]) y (by simp [hy]) vx vy hxv hyv hvy
      | some b =>
          simp [hfa]
          constructor
          · intro c hc hfc
            have hca : c = a := hinj c (by simp [hc]) a (by simp) b b hfc (by simpa using hfa) rfl
            exact hl.1 (by simpa [hca] using hc)
          · apply ih hl.2
            intro x hx y hy vx vy hxv hyv hvy
            exact hinj x (by simp [hx]) y (by simp [hy]) vx vy hxv hyv hvy

theorem nodup_filter {α : Type} (l : List α) (hl : l.Nodup) (p : α → Bool) : (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      by_cases hp : p a = true
      · simp [hp]
        exact ⟨hl.1, ih hl.2⟩
      · have hp' : p a = false := by cases h : p a <;> simp_all
        simp [hp']
        exact ih hl.2

theorem nodup_append {α : Type} {l₁ l₂ : List α} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hdis : ∀ a, a ∈ l₁ → a ∈ l₂ → False) : (l₁ ++ l₂).Nodup := by
  induction l₁ with
  | nil => simpa using h₂
  | cons a as ih =>
      rw [List.nodup_cons] at h₁
      change (a :: (as ++ l₂)).Nodup
      rw [List.nodup_cons]
      exact ⟨fun ha2 => hdis a (by simp) (by simpa [List.mem_append, h₁.1] using ha2),
        ih h₁.2 (fun x hx1 hx2 => hdis x (by simp [hx1]) hx2)⟩

theorem nodup_entries (n : Nat) (state : Nat → Option Nat) : (entries n state).Nodup := by
  unfold entries
  apply nodup_filterMap_of_inj
  · exact List.nodup_range
  · intro a ha b hb va vb hva hvb heq
    cases hsa : state a with
    | none => simp [hsa] at hva
    | some wa =>
        have hva' : va = (wa, a) := by simpa [hsa] using hva.symm
        cases hsb : state b with
        | none => simp [hsb] at hvb
        | some wb =>
            have hvb' : vb = (wb, b) := by simpa [hsb] using hvb.symm
            rw [hva', hvb'] at heq
            exact congrArg Prod.snd heq

theorem mem_repair (n : Nat) (scanned : Nat → Option Nat) (dirty : List Nat)
    (hdirty : ∀ i ∈ dirty, i < n) (rebuilt : Nat → Option Nat) (v i : Nat) :
    (v, i) ∈ (entries n scanned).filter (fun e => !dirty.contains e.2) ++
        dirty.filterMap (fun j => (rebuilt j).map (fun w => (w, j))) ↔
      i < n ∧ (if dirty.contains i then rebuilt i else scanned i) = some v := by
  constructor
  · intro h
    rcases List.mem_append.mp h with hA | hB
    · rw [List.mem_filter] at hA
      have he := (mem_entries n scanned v i).1 hA.1
      have hnotmem : i ∉ dirty := by simpa using hA.2
      have hs : scanned i = some v := he.2
      exact ⟨he.1, by simp [hnotmem, hs]⟩
    · rw [List.mem_filterMap] at hB
      rcases hB with ⟨j, hj, hji⟩
      have hjmem : j ∈ dirty := by simpa using hj
      cases hr : rebuilt j with
      | none => simp [hr] at hji
      | some w =>
          simp [hr] at hji
          have hwv : w = v := hji.1
          have hji' : j = i := hji.2
          subst j
          exact ⟨hdirty i hjmem, by simp [hjmem, hr, hwv]⟩
  · intro h
    rw [List.mem_append]
    by_cases himem : i ∈ dirty
    · right
      rw [List.mem_filterMap]
      refine ⟨i, ?_, ?_⟩
      · simpa using himem
      · have hv : rebuilt i = some v := by simpa [himem] using h.2
        cases hr : rebuilt i with
        | none => simp [hr] at hv
        | some w => simpa [hr] using hv
    · left
      rw [List.mem_filter]
      have hv : scanned i = some v := by simpa [himem] using h.2
      exact ⟨(mem_entries n scanned v i).2 ⟨h.1, hv⟩, by simp [himem]⟩

theorem nodup_rebuilt (dirty : List Nat) (hd : dirty.Nodup) (rebuilt : Nat → Option Nat) :
    (dirty.filterMap (fun j => (rebuilt j).map (fun w => (w, j)))).Nodup := by
  apply nodup_filterMap_of_inj
  · exact hd
  · intro a ha b hb va vb hva hvb heq
    cases hra : rebuilt a with
    | none => simp [hra] at hva
    | some wa =>
        have hva' : va = (wa, a) := by simpa [hra] using hva.symm
        cases hrb : rebuilt b with
        | none => simp [hrb] at hvb
        | some wb =>
            have hvb' : vb = (wb, b) := by simpa [hrb] using hvb.symm
            rw [hva', hvb'] at heq
            exact congrArg Prod.snd heq

theorem sound : Goal := by
  intro base cs cut n endTs hcs hcut hrid
  let changes := cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)
  let dirty := (changes.map Change.rid).eraseDups
  let scanned := fun i => asOf base cs i (cut i)
  let rebuilt := fun i => last none (changes.filter (fun c => c.rid == i))
  let finalState := fun i => asOf base cs i endTs
  let built := (entries n scanned).filter (fun e => !dirty.contains e.2) ++
    dirty.filterMap (fun i => (rebuilt i).map (fun w => (w, i)))
  let fresh := entries n finalState
  change built.Perm fresh ∧ ((built.map Prod.fst).Nodup ↔ (fresh.map Prod.fst).Nodup)
  have hdirty_subset : ∀ i ∈ dirty, i < n := by
    intro i hi
    rcases (mem_dirty changes i).1 hi with ⟨c, hc, hcri⟩
    have hc' : c ∈ cs ∧ cut c.rid < c.ts ∧ c.ts ≤ endTs := by simpa [changes] using hc
    simpa [hcri] using hrid c hc'.1
  have hdirty_nodup : dirty.Nodup := by
    simpa [dirty] using nodup_eraseDups (changes.map Change.rid)
  have hA : ((entries n scanned).filter (fun e => !dirty.contains e.2)).Nodup := by
    apply nodup_filter
    exact nodup_entries n scanned
  have hB : (dirty.filterMap (fun i => (rebuilt i).map (fun w => (w, i)))).Nodup :=
    nodup_rebuilt dirty hdirty_nodup rebuilt
  have hdis : ∀ a, a ∈ (entries n scanned).filter (fun e => !dirty.contains e.2) →
      a ∈ dirty.filterMap (fun i => (rebuilt i).map (fun w => (w, i))) → False := by
    intro a ha hb
    rcases a with ⟨v, i⟩
    rw [List.mem_filter] at ha
    have hnot : i ∉ dirty := by simpa using ha.2
    rw [List.mem_filterMap] at hb
    rcases hb with ⟨j, hj, hji⟩
    have hjmem : j ∈ dirty := by simpa using hj
    cases hr : rebuilt j with
    | none => simp [hr] at hji
    | some w =>
        simp [hr] at hji
        have hji' : j = i := hji.2
        subst j
        exact hnot hjmem
  have hbuilt_nodup : built.Nodup := by
    change ((entries n scanned).filter (fun e => !dirty.contains e.2) ++
      dirty.filterMap (fun i => (rebuilt i).map (fun w => (w, i)))).Nodup
    exact nodup_append hA hB hdis
  have hfresh_nodup : fresh.Nodup := by
    change (entries n finalState).Nodup
    exact nodup_entries n finalState
  have hperm : built.Perm fresh := by
    apply (List.perm_ext_iff_of_nodup hbuilt_nodup hfresh_nodup).2
    intro a
    rcases a with ⟨v, i⟩
    rw [mem_repair n scanned dirty hdirty_subset rebuilt v i]
    rw [mem_entries n finalState v i]
    constructor
    · rintro ⟨hi, hv⟩
      refine ⟨hi, ?_⟩
      by_cases himem : i ∈ dirty
      · have hc := hcut i (by simpa using hi)
        have hr := rebuilt_eq_final cs base cut endTs i hcs hc (by simpa [dirty, changes] using himem)
        change rebuilt i = finalState i at hr
        have hv' : rebuilt i = some v := by simpa [himem] using hv
        rw [← hr]
        exact hv'
      · have hc := hcut i (by simpa using hi)
        have hs := scanned_eq_final cs base cut endTs i hcs hc (by simpa [dirty, changes] using himem)
        change scanned i = finalState i at hs
        have hv' : scanned i = some v := by simpa [himem] using hv
        rw [← hs]
        exact hv'
    · rintro ⟨hi, hv⟩
      refine ⟨hi, ?_⟩
      by_cases himem : i ∈ dirty
      · have hc := hcut i (by simpa using hi)
        have hr := rebuilt_eq_final cs base cut endTs i hcs hc (by simpa [dirty, changes] using himem)
        change rebuilt i = finalState i at hr
        have hv' : rebuilt i = some v := by rw [hr]; exact hv
        simpa [himem] using hv'
      · have hc := hcut i (by simpa using hi)
        have hs := scanned_eq_final cs base cut endTs i hcs hc (by simpa [dirty, changes] using himem)
        change scanned i = finalState i at hs
        have hv' : scanned i = some v := by rw [hs]; exact hv
        simpa [himem] using hv'
  refine ⟨hperm, ?_⟩
  have hpMap := hperm.map Prod.fst
  constructor
  · intro hn
    exact hpMap.nodup hn
  · intro hn
    exact hpMap.symm.nodup hn

end TinylambOpt.ResearchProofs.RestartRaces.Catalog_OnlineUnique
