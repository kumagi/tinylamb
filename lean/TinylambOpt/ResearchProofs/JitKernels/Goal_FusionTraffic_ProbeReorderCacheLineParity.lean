import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_ProbeReorderCacheLineParity

theorem sound : TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.ProbeReorderCacheLineParity.Goal := by
  intro base stride w a b hperm
  have perm_ed : ∀ {l₁ l₂ : List Nat}, l₁.Perm l₂ → l₁.eraseDups.Perm l₂.eraseDups := by
    have nodup_ed : ∀ (l : List Nat) (p : Nat → Bool), (l.filter p).eraseDups.Nodup := by
      intro l
      induction l with
      | nil => intro p; simp
      | cons a l ih =>
          intro p
          by_cases hp : p a = true
          · simp only [List.filter_cons, hp, ↓reduceIte, List.eraseDups_cons]
            rw [List.nodup_cons]
            constructor
            · intro hmem
              have hm := (List.mem_eraseDups.mp hmem)
              simp at hm
            · simpa [List.filter_filter, and_left_comm, and_assoc] using
                ih (fun b => ! b == a && p b)
          · simp [hp, ih]
    have perm_of_nodup_mem : ∀ {l₁ l₂ : List Nat}, l₁.Nodup → l₂.Nodup →
        (∀ x, x ∈ l₁ ↔ x ∈ l₂) → l₁.Perm l₂ := by
      intro l₁ l₂
      induction l₁ generalizing l₂ with
      | nil =>
          intro h₁ h₂ hmem
          have hnil : l₂ = [] := by
            cases l₂ with
            | nil => rfl
            | cons b bs =>
                have hb := (hmem b).mpr (by simp : b ∈ b :: bs)
                simp at hb
          simp [hnil]
      | cons a l₁ ih =>
          intro h₁ h₂ hmem
          have ha₁ : a ∉ l₁ := (List.nodup_cons.mp h₁).1
          have htail₁ : l₁.Nodup := (List.nodup_cons.mp h₁).2
          have ha₂ : a ∈ l₂ := (hmem a).mp (by simp)
          have p₂ : l₂.Perm (a :: l₂.erase a) := List.perm_cons_erase ha₂
          have h₂' : (l₂.erase a).Nodup := h₂.erase a
          have hmem' : ∀ x, x ∈ l₁ ↔ x ∈ l₂.erase a := by
            intro x
            constructor
            · intro hx1
              have hx : x ≠ a := by
                intro hxa
                subst x
                exact ha₁ hx1
              have hx2 : x ∈ l₂ := (hmem x).mp (by simp [hx1])
              exact ((List.Nodup.mem_erase_iff h₂).mpr ⟨hx, hx2⟩)
            · intro hx2e
              have hxinfo := (List.Nodup.mem_erase_iff h₂).mp hx2e
              have hx1all : x ∈ a :: l₁ := (hmem x).mpr hxinfo.2
              simpa [hxinfo.1] using hx1all
          exact ((ih htail₁ h₂' hmem').cons a).trans p₂.symm
    intro l₁ l₂ h
    have hfilt : ∀ l : List Nat, l.filter (fun _ => true) = l := by
      intro l
      induction l with
      | nil => rfl
      | cons a l ih => simp [ih]
    apply perm_of_nodup_mem
    · simpa [hfilt] using nodup_ed l₁ (fun _ => true)
    · simpa [hfilt] using nodup_ed l₂ (fun _ => true)
    · intro x
      simpa [List.mem_eraseDups] using h.mem_iff (a := x)
  unfold TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.footprint
  exact perm_ed (hperm.map (fun i => TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.lineOf base stride w i))

end TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_ProbeReorderCacheLineParity
