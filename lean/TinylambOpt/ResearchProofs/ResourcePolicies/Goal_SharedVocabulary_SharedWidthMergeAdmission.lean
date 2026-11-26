import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
/-!
Registers the checked result for `SharedVocabulary.SharedWidthMergeAdmission.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`SharedWidthMergeAdmission`: if both row groups already pay the merged id width,
merging their dictionaries can only save bytes: the merged dictionary covers each
local dictionary, and `byteCount` of the combined id stream splits subadditively
over the two row counts.  Admission control may therefore approve the merge purely
from dictionary sizes without measuring the resulting frame.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_SharedWidthMergeAdmission
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

/-- Per-symbol payload charge: length prefix plus raw bytes. -/
private def wpay (s : Symbol) : Nat := ubytes s.length + s.length

/-! ## generic helpers -/

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

/-- Packing bits are subadditive under concatenation of payloads. -/
private theorem byteCount_add (x y : Nat) : byteCount (x + y) ≤ byteCount x + byteCount y := by
  unfold byteCount
  omega

theorem sound : SharedWidthMergeAdmission.Goal := by
  intro a b m h1 h2
  dsimp only [charge, vocab, dictSize, idSize] at h1 h2 ⊢
  rw [show (fun s : Symbol => ubytes s.length + s.length) = wpay from rfl]
  have hm : width ((a ++ b).eraseDups.length - 1) = m := rfl
  rw [hm, h1, h2, List.length_append, Nat.add_mul]
  have hDict : ((a ++ b).eraseDups.map wpay).sum
      ≤ (a.eraseDups.map wpay).sum + (b.eraseDups.map wpay).sum := by
    rw [List.eraseDups_append, List.map_append, List.sum_append]
    apply Nat.add_le_add_left
    refine sum_map_le wpay b.eraseDups _ (nodup_eraseDups _) ?_
    intro s hs
    rw [List.mem_eraseDups] at hs ⊢
    exact (mem_removeAll_iff b a s).mp hs |>.1
  have hbc := byteCount_add (a.length * m) (b.length * m)
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_SharedWidthMergeAdmission
