import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

/-!
`SharedDictionaryOverlapCharge`: inclusion-exclusion for the dictionary payload.
The per-symbol bytes (length prefix plus payload) of the two local dictionaries sum
to the bytes of the merged shared dictionary plus the bytes of exactly the
overlapping symbols, so a compactor can predict the dictionary bytes saved before
rewriting both row groups.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_SharedDictionaryOverlapCharge
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Per-symbol payload charge: length prefix plus raw bytes. -/
private def wpay (s : Symbol) : Nat := ubytes s.length + s.length

/-! ## generic sum helpers -/

private theorem sum_eq_of_perm {l₁ l₂ : List Nat} (p : List.Perm l₁ l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | @cons a l₁ l₂ h ih => show a + l₁.sum = a + l₂.sum; rw [ih]
  | @swap a b l => show b + (a + l.sum) = a + (b + l.sum); omega
  | @trans l₁ l₂ l₃ t₁ t₂ ih₁ ih₂ => omega

/-- A duplicate-free list charges at most the payload sum of any list covering it. -/
private theorem sum_map_le (f : Symbol → Nat) :
    ∀ (n : List Symbol), ∀ (m : List Symbol), m.Nodup →
      (∀ s ∈ m, s ∈ n) → (m.map f).sum ≤ (n.map f).sum := by
  intro n
  induction n with
  | nil =>
      intro m _ hsub
      have hm : m = [] :=
        List.eq_nil_iff_forall_not_mem.mpr
          (by intro s hs; exact absurd (hsub s hs) (by simp))
      rw [hm]
      simp
  | cons y ns ih =>
      intro m hm hsub
      by_cases hy : y ∈ m
      · have hperm : List.Perm (m.map f) ((y :: m.erase y).map f) :=
          (List.perm_cons_erase hy).map f
        have hsum : (m.map f).sum = f y + ((m.erase y).map f).sum := by
          rw [sum_eq_of_perm hperm, List.map_cons, List.sum_cons]
        have hsub' : ∀ s ∈ m.erase y, s ∈ ns := by
          intro s hs
          have hmem : s ∈ m := List.mem_of_mem_erase hs
          rcases List.mem_cons.mp (hsub s hmem) with heq | hns
          · subst heq
            exact absurd hs hm.not_mem_erase
          · exact hns
        calc (m.map f).sum
            = f y + ((m.erase y).map f).sum := hsum
          _ ≤ f y + (ns.map f).sum := Nat.add_le_add_left (ih (m.erase y) (hm.erase y) hsub') _
          _ = ((y :: ns).map f).sum := by simp [List.map_cons, List.sum_cons]
      · have hsub' : ∀ s ∈ m, s ∈ ns := by
          intro s hs
          rcases List.mem_cons.mp (hsub s hs) with heq | hns
          · subst heq
            exact absurd hs hy
          · exact hns
        calc (m.map f).sum
            ≤ (ns.map f).sum := ih m hm hsub'
          _ ≤ f y + (ns.map f).sum := Nat.le_add_left _ _
          _ = ((y :: ns).map f).sum := by simp [List.map_cons, List.sum_cons]

/-- Two duplicate-free lists with the same members carry the same payload sum. -/
private theorem dictEq {d e : List Symbol} (f : Symbol → Nat)
    (hd : d.Nodup) (he : e.Nodup) (h : ∀ s, s ∈ d ↔ s ∈ e) :
    (d.map f).sum = (e.map f).sum :=
  Nat.le_antisymm
    (sum_map_le f e d hd fun s hs => (h s).mp hs)
    (sum_map_le f d e he fun s hs => (h s).mpr hs)

/-- Split a payload sum along a Boolean predicate. -/
private theorem sum_map_partition (f : Symbol → Nat) (p : Symbol → Bool) :
    ∀ l : List Symbol,
      ((l.filter p).map f).sum + ((l.filter (fun s => !p s)).map f).sum = (l.map f).sum := by
  intro l
  induction l with
  | nil => rfl
  | cons a as ih =>
      by_cases hpa : p a
      · rw [List.filter_cons_of_pos hpa,
          List.filter_cons_of_neg (by simpa using hpa)]
        have h := ih
        simp only [List.map_cons, List.sum_cons] at h ⊢
        omega
      · rw [List.filter_cons_of_neg hpa,
          List.filter_cons_of_pos (by simpa using hpa)]
        have h := ih
        simp only [List.map_cons, List.sum_cons] at h ⊢
        omega

private theorem nodup_filter (p : Symbol → Bool) {l : List Symbol} :
    l.Nodup → (l.filter p).Nodup := List.Pairwise.filter p

private theorem nodup_eraseDups_filter : ∀ (p : Symbol → Bool) (l : List Symbol),
    (l.filter p).eraseDups.Nodup := by
  intro p l
  induction l generalizing p with
  | nil => simp
  | cons a as ih =>
      by_cases hpa : p a
      · rw [List.filter_cons_of_pos hpa, List.eraseDups_cons, List.filter_filter]
        refine List.nodup_cons.mpr ⟨?_, ih (fun b => !(b == a) && p b)⟩
        intro hmem
        rw [List.mem_eraseDups, List.mem_filter] at hmem
        simp at hmem
      · rw [List.filter_cons_of_neg hpa]
        exact ih p

private theorem nodup_eraseDups (l : List Symbol) : l.eraseDups.Nodup := by
  have h := nodup_eraseDups_filter (fun _ => true) l
  have hfl : l.filter (fun _ => true) = l := List.filter_eq_self.mpr (by simp)
  rwa [hfl] at h

private theorem mem_removeAll_iff (xs ys : List Symbol) (s : Symbol) :
    s ∈ xs.removeAll ys ↔ s ∈ xs ∧ s ∉ ys := by
  show s ∈ xs.filter (fun x => !(ys.elem x)) ↔ s ∈ xs ∧ s ∉ ys
  rw [List.mem_filter]
  simp

theorem sound : SharedDictionaryOverlapCharge.Goal := by
  intro a b
  show (a.eraseDups.map wpay).sum + (b.eraseDups.map wpay).sum
      = ((a ++ b).eraseDups.map wpay).sum
        + ((a.eraseDups.filter (fun s => List.contains b.eraseDups s)).map wpay).sum
  -- the fresh part of the merged dictionary is exactly `b`'s symbols absent from `a`
  have hC : ((List.eraseDups (List.removeAll b a)).map wpay).sum
          = ((b.eraseDups.filter (fun s => !List.contains a.eraseDups s)).map wpay).sum := by
    refine dictEq wpay (nodup_eraseDups _)
      (nodup_filter _ (nodup_eraseDups _)) ?_
    intro s
    rw [List.mem_eraseDups, mem_removeAll_iff]
    simp [List.mem_filter, List.mem_eraseDups]
  -- the overlap counted from either side agrees
  have hAB : ((a.eraseDups.filter (fun s => List.contains b.eraseDups s)).map wpay).sum
          = ((b.eraseDups.filter (fun s => List.contains a.eraseDups s)).map wpay).sum := by
    refine dictEq wpay (nodup_filter _ (nodup_eraseDups _))
      (nodup_filter _ (nodup_eraseDups _)) ?_
    intro s
    rw [List.mem_filter, List.mem_filter]
    simp [List.mem_eraseDups, And.comm]
  -- partition of `b`'s dictionary along membership in `a`
  have hpart : (b.eraseDups.map wpay).sum
            = ((b.eraseDups.filter (fun s => List.contains a.eraseDups s)).map wpay).sum
              + ((b.eraseDups.filter (fun s => !List.contains a.eraseDups s)).map wpay).sum :=
    (sum_map_partition wpay (fun s => List.contains a.eraseDups s) b.eraseDups).symm
  rw [List.eraseDups_append, List.map_append, List.sum_append, hC, hAB]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary_SharedDictionaryOverlapCharge
