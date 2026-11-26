import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_ProbeReorderCacheLineParity

open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      constructor
      · intro hmem
        have h : a ∈ as.filter (fun b => !b == a) := (List.mem_eraseDups.mp hmem)
        simp at h
      · exact nodup_eraseDups (as.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) (Nat.lt_succ_self _)

theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha_not : a ∉ as := (List.nodup_cons.mp h).1
      have hnd_tail : as.Nodup := (List.nodup_cons.mp h).2
      rw [List.eraseDups_cons]
      have hfilter : as.filter (fun b => !b == a) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by intro heq; subst b; exact ha_not hb
        simp [beq_iff_eq, hba]
      rw [hfilter, ih hnd_tail]

theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := List.Nodup.sublist (List.filter_sublist (p := p)) h

theorem count_eraseDups {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α]
    (a : α) : ∀ l : List α, List.count a l.eraseDups = if a ∈ l then 1 else 0
  | [] => by simp
  | b :: bs => by
      rw [List.eraseDups_cons]
      by_cases hab : a == b
      · have heq : a = b := (beq_iff_eq.mp hab)
        subst b
        have hnot : a ∉ (bs.filter (fun b => !b == a)).eraseDups := by
          rw [List.mem_eraseDups]
          simp
        rw [List.count_cons]
        rw [List.count_eq_zero_of_not_mem hnot]
        simp
      · have hbfalse : (a == b) = false := Bool.eq_false_of_ne_true hab
        have hne : a ≠ b := by intro he; subst b; exact hab ((beq_iff_eq).2 rfl)
        rw [List.count_cons_of_ne (by intro hba; exact hne hba.symm)]
        rw [count_eraseDups a (bs.filter (fun x => !x == b))]
        have hmemfilter : a ∈ bs.filter (fun x => !x == b) ↔ a ∈ bs := by
          simp [List.mem_filter, hbfalse]
        by_cases has : a ∈ bs
        · have hf : a ∈ bs.filter (fun x => !x == b) := hmemfilter.mpr has
          simp [hf, has, hne]
        · have hf : a ∉ bs.filter (fun x => !x == b) := by intro hx; exact has (hmemfilter.mp hx)
          simp [hf, has, hne]
termination_by l => l.length
decreasing_by exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) (Nat.lt_succ_self _)

theorem count_eq_one_of_mem_of_nodup {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α]
    {a : α} {l : List α} (hnd : l.Nodup) (ha : a ∈ l) : List.count a l = 1 := by
  have hself : l.eraseDups = l := eraseDups_eq_self_of_nodup hnd
  rw [← hself, count_eraseDups]
  simp [ha]

theorem perm_eraseDups {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α]
    {l₁ l₂ : List α} (h : l₁.Perm l₂) : (l₁.eraseDups).Perm (l₂.eraseDups) := by
  rw [List.perm_iff_count]
  intro a
  rw [count_eraseDups, count_eraseDups]
  by_cases hm : a ∈ l₁
  · have hm2 : a ∈ l₂ := h.mem_iff.mp hm
    simp [hm, hm2]
  · have hm2 : a ∉ l₂ := by intro h2; exact hm (h.mem_iff.mpr h2)
    simp [hm, hm2]

theorem length_le_of_subset_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hsub : l₁ ⊆ l₂) :
    l₁.length ≤ l₂.length := by
  induction l₁ generalizing l₂ with
  | nil => simp
  | cons a l₁ ih =>
      have ha_not : a ∉ l₁ := (List.nodup_cons.mp h₁).1
      have hnd_tail : l₁.Nodup := (List.nodup_cons.mp h₁).2
      have ha2 : a ∈ l₂ := hsub (by simp)
      have hsub_tail : l₁ ⊆ l₂.erase a := by
        intro x hx
        have hx2 : x ∈ l₂ := hsub (by simp [hx])
        exact (List.mem_erase_of_ne (by intro hxa; subst a; exact ha_not hx)).2 hx2
      have ih' := ih hnd_tail (h₂.erase a) hsub_tail
      rw [List.length_erase_of_mem ha2] at ih'
      have hl2pos : 0 < l₂.length := List.length_pos_of_mem ha2
      simp only [List.length_cons]
      omega

theorem beq_add_cancel (k x a : Nat) : (k + x == k + a) = (x == a) := by
  by_cases h : x == a
  · have heq : x = a := (beq_iff_eq.mp h)
    subst a
    simp
  · have hne : x ≠ a := by intro he; exact h ((beq_iff_eq).2 he)
    have hne' : k + x ≠ k + a := by omega
    have hb : (k + x == k + a) = false := by
      apply Bool.eq_false_of_ne_true
      intro ht
      exact hne' ((beq_iff_eq).mp ht)
    have hb' : (x == a) = false := Bool.eq_false_of_ne_true h
    rw [hb, hb']

theorem map_eraseDups_shift (k : Nat) : ∀ l : List Nat,
    (l.map (fun x => k + x)).eraseDups = l.eraseDups.map (fun x => k + x)
  | [] => by simp
  | a :: as => by
      rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons, List.map_cons]
      congr 1
      rw [List.filter_map]
      rw [map_eraseDups_shift k (as.filter ((fun b => !b == k + a) ∘ (fun x => k + x)))]
      congr 1
      apply congrArg List.eraseDups
      apply List.filter_congr
      intro b hb
      simp only [Function.comp_apply]
      rw [beq_add_cancel]
termination_by l => l.length
decreasing_by exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) (Nat.lt_succ_self _)

theorem length_filter_complement {α : Type} (p : α → Bool) : ∀ l : List α,
    (l.filter p).length + (l.filter (fun x => !p x)).length = l.length
  | [] => by simp
  | a :: as => by
      have ih := length_filter_complement p as
      simp only [List.filter_cons, List.length_cons]
      by_cases h : p a = true
      · simp [h]; omega
      · have hf : p a = false := Bool.eq_false_of_ne_true h
        simp [hf]; omega

theorem filter_intersection_perm {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α]
    {A B : List α} (hA : A.Nodup) (hB : B.Nodup) :
    (A.filter fun x => B.contains x).Perm (B.filter fun x => A.contains x) := by
  rw [List.perm_iff_count]
  intro x
  have hA' : (A.filter fun y => B.contains y).Nodup := nodup_filter _ hA
  have hB' : (B.filter fun y => A.contains y).Nodup := nodup_filter _ hB
  by_cases hxA : x ∈ A
  · by_cases hxB : x ∈ B
    · have hmemA : x ∈ A.filter (fun y => B.contains y) := by
        rw [List.mem_filter]
        exact ⟨hxA, List.elem_iff.mpr hxB⟩
      have hmemB : x ∈ B.filter (fun y => A.contains y) := by
        rw [List.mem_filter]
        exact ⟨hxB, List.elem_iff.mpr hxA⟩
      rw [count_eq_one_of_mem_of_nodup hA' hmemA, count_eq_one_of_mem_of_nodup hB' hmemB]
    · have hnoA : x ∉ A.filter (fun y => B.contains y) := by
        intro hx
        exact hxB (List.elem_iff.mp (List.mem_filter.mp hx).2)
      have hnoB : x ∉ B.filter (fun y => A.contains y) := by simp [List.mem_filter, hxB]
      rw [List.count_eq_zero_of_not_mem hnoA, List.count_eq_zero_of_not_mem hnoB]
  · by_cases hxB : x ∈ B
    · have hnoA : x ∉ A.filter (fun y => B.contains y) := by simp [List.mem_filter, hxA]
      have hnoB : x ∉ B.filter (fun y => A.contains y) := by
        intro hx
        exact hxA (List.elem_iff.mp (List.mem_filter.mp hx).2)
      rw [List.count_eq_zero_of_not_mem hnoA, List.count_eq_zero_of_not_mem hnoB]
    · have hnoA : x ∉ A.filter (fun y => B.contains y) := by simp [List.mem_filter, hxA]
      have hnoB : x ∉ B.filter (fun y => A.contains y) := by simp [List.mem_filter, hxB]
      rw [List.count_eq_zero_of_not_mem hnoA, List.count_eq_zero_of_not_mem hnoB]



theorem sound : ProbeReorderCacheLineParity.Goal := by
  intro base stride w a b hp
  unfold footprint
  exact perm_eraseDups (hp.map (fun i => lineOf base stride w i))

end TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_ProbeReorderCacheLineParity
