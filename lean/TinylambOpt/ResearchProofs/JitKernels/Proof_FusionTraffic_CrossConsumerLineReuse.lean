import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! SOUND: Fusing two projections over one payload charges each shared cache line once.
The fused footprint is the deduplicated union `F(a++b) = F(a) ++ (lines of b off F(a))`,
while the correction term `F(a).filter (· ∈ F(b))` counts exactly the shared lines; by
inclusion–exclusion over the two duplicate-free line lists the total equals
`|F(a)| + |F(b)|`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_CrossConsumerLineReuse
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-- `eraseDups` of any list is duplicate-free. -/
private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  have key : ∀ n : Nat, ∀ l : List Nat, l.length ≤ n → l.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
        intro m hle
        match m with
        | [] => exact List.Pairwise.nil
        | a :: as =>
          rw [List.length_cons] at hle
          exact absurd hle (by omega)
    | succ k ih =>
        intro m hle
        match m with
        | [] => exact List.Pairwise.nil
        | a :: as =>
          have has : as.length ≤ k := by rw [List.length_cons] at hle; omega
          rw [List.eraseDups_cons, List.nodup_cons]
          refine ⟨fun hx => ?_,
            ih (as.filter fun b => !(b == a)) (Nat.le_trans (List.length_filter_le _ as) has)⟩
          rw [List.mem_eraseDups, List.mem_filter] at hx
          obtain ⟨_, hp⟩ := hx
          rw [Nat.not_beq_eq_true_eq] at hp
          exact hp rfl
  exact key l.length l (Nat.le_refl l.length)

/-- Filtering preserves duplicate-freeness. -/
private theorem nodup_filter {l : List Nat} (h : l.Nodup) (p : Nat → Bool) :
    (l.filter p).Nodup := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a as ih =>
    rw [List.nodup_cons] at h
    rw [List.filter_cons]
    split
    · rw [List.nodup_cons]
      refine ⟨fun hx => h.1 ((List.mem_filter (p := p)).mp hx).1, ih h.2⟩
    · exact ih h.2

/-- Splitting a list by a predicate and its negation preserves total length. -/
private theorem partition (q : Nat → Bool) : ∀ l : List Nat,
    (l.filter (fun x => q x)).length + (l.filter (fun x => (!(q x)))).length = l.length := by
  intro l
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [List.filter_cons, List.filter_cons]
    by_cases h : q a = true
    · rw [if_pos h]
      have hn : (!(q a)) = false := by rw [h]; rfl
      rw [hn, if_neg Bool.false_ne_true, List.length_cons, List.length_cons]
      omega
    · have h2 : q a = false := Bool.of_not_eq_true h
      rw [if_neg h, if_pos (by rw [h2]; rfl), List.length_cons, List.length_cons]
      omega

/-- Lines of `b` that miss `a`'s raw line list are exactly the footprint lines of `b`
that miss the footprint of `a`. -/
private theorem removeAll_lines {A B : List Nat} :
    List.Perm (B.removeAll A).eraseDups
      (B.eraseDups.filter fun l => !(A.eraseDups).contains l) := by
  refine ((List.perm_ext_iff_of_nodup (nodup_eraseDups _)
    (nodup_filter (nodup_eraseDups _) _)).mpr) (fun x => ?_)
  show x ∈ (B.removeAll A).eraseDups
      ↔ x ∈ List.filter (fun l => !(A.eraseDups).contains l) B.eraseDups
  rw [List.mem_eraseDups, List.removeAll, List.mem_filter, List.mem_filter,
    List.mem_eraseDups]
  simp [List.elem_eq_mem, List.contains_eq_mem]

/-- The two overlap filters count the same lines. -/
private theorem filter_swap {X Y : List Nat} (hX : X.Nodup) (hY : Y.Nodup) :
    (X.filter (fun l => Y.contains l)).length = (Y.filter (fun l => X.contains l)).length := by
  refine ((List.perm_ext_iff_of_nodup (nodup_filter hX _) (nodup_filter hY _)).mpr
    (fun x => ?_)).length_eq
  show x ∈ List.filter (fun l => Y.contains l) X
      ↔ x ∈ List.filter (fun l => X.contains l) Y
  rw [List.mem_filter, List.mem_filter]
  simp [List.contains_eq_mem]
  exact ⟨fun ⟨h1, h2⟩ => ⟨h2, h1⟩, fun ⟨h1, h2⟩ => ⟨h2, h1⟩⟩

/-- Inclusion–exclusion for fused footprints. -/
theorem sound : CrossConsumerLineReuse.Goal := by
  intro base stride w a b
  show (footprint base stride w (a ++ b)).length
      + ((footprint base stride w a).filter
        (fun l => (footprint base stride w b).contains l)).length
      = (footprint base stride w a).length + (footprint base stride w b).length
  have hstruct : footprint base stride w (a ++ b)
      = footprint base stride w a
        ++ ((List.map (lineOf base stride w) b).removeAll
          (List.map (lineOf base stride w) a)).eraseDups := by
    show (List.map (lineOf base stride w) (a ++ b)).eraseDups
        = (List.map (lineOf base stride w) a).eraseDups
          ++ ((List.map (lineOf base stride w) b).removeAll
            (List.map (lineOf base stride w) a)).eraseDups
    rw [List.map_append, List.eraseDups_append]
  have hT := (removeAll_lines (A := List.map (lineOf base stride w) a)
    (B := List.map (lineOf base stride w) b)).length_eq
  have hfa : footprint base stride w a = (List.map (lineOf base stride w) a).eraseDups := rfl
  have hfb : footprint base stride w b = (List.map (lineOf base stride w) b).eraseDups := rfl
  have hXY := filter_swap (X := footprint base stride w a) (Y := footprint base stride w b)
    (nodup_eraseDups _) (nodup_eraseDups _)
  have hpart : ((footprint base stride w b).filter
        (fun l => (footprint base stride w a).contains l)).length
      + ((footprint base stride w b).filter
        (fun l => !((footprint base stride w a).contains l))).length
      = (footprint base stride w b).length :=
    partition (fun l => (footprint base stride w a).contains l) (footprint base stride w b)
  rw [hstruct, List.length_append, hT, ← hfa, ← hfb, hXY]
  omega

end TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_CrossConsumerLineReuse
