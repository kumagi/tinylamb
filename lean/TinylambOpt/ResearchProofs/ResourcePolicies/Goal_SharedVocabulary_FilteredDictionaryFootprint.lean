import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
/-!
Registers the checked result for `SharedVocabulary.FilteredDictionaryFootprint.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`FilteredDictionaryFootprint`: projecting a row group through an equality predicate
never grows the frame.  The surviving dictionary is a subset of the original one, so
both its payload sum and its entry count (hence the id width) shrink, and fewer rows
are left to reference.  A scan may therefore push a selective predicate below the
dictionary decode step without risking a larger footprint.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_FilteredDictionaryFootprint
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

/-- A duplicate-free list is no longer than any list covering it. -/
private theorem length_le_of_subset :
    ∀ (n : List Symbol), ∀ (m : List Symbol), m.Nodup →
      (∀ s ∈ m, s ∈ n) → m.length ≤ n.length := by
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
      · have hlen : (m.erase y).length + 1 = m.length := by
          have hp : 0 < m.length := List.length_pos_of_mem hy
          rw [List.length_erase_of_mem hy]
          omega
        have hind := ih (m.erase y) (hm.erase y) (by
          intro s hs
          have hmem : s ∈ m := List.mem_of_mem_erase hs
          rcases List.mem_cons.mp (hsub s hmem) with heq | hns
          · subst heq
            exact absurd hs hm.not_mem_erase
          · exact hns)
        have hln : (y :: ns).length = ns.length + 1 := by simp
        omega
      · have hsub' : ∀ s ∈ m, s ∈ ns := by
          intro s hs
          rcases List.mem_cons.mp (hsub s hs) with heq | hns
          · subst heq
            exact absurd hs hy
          · exact hns
        have hind := ih m hm hsub'
        have hln : (y :: ns).length = ns.length + 1 := by simp
        omega

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

/-- The bit width is monotone in the largest value. -/
private theorem width_mono {u v : Nat} (h : u ≤ v) : width u ≤ width v := by
  show (if u == 0 then 0 else u.log2 + 1) ≤ (if v == 0 then 0 else v.log2 + 1)
  by_cases hu : u = 0
  · subst hu
    rw [if_pos (by simp)]
    exact Nat.zero_le _
  · have hv : ¬ (v = 0) := by
      intro heq
      subst heq
      exact hu (Nat.le_zero.mp h)
    rw [if_neg (by simpa using hu), if_neg (by simpa using hv)]
    have hl : u.log2 ≤ v.log2 :=
      (Nat.le_log2 (by simpa using hv)).mpr (Nat.le_trans (Nat.log2_self_le (by simpa using hu)) h)
    omega

/-- Packed byte counts are monotone in the bit count. -/
private theorem byteCount_mono {x y : Nat} (h : x ≤ y) : byteCount x ≤ byteCount y := by
  unfold byteCount
  omega

theorem sound : FilteredDictionaryFootprint.Goal := by
  intro xs p
  dsimp only [charge, vocab, dictSize, idSize]
  rw [show (fun s : Symbol => ubytes s.length + s.length) = wpay from rfl]
  have hmem : ∀ s ∈ (xs.filter p).eraseDups, s ∈ xs.eraseDups := by
    intro s hs
    have h1 := List.mem_eraseDups.mp hs
    obtain ⟨hx, _⟩ := List.mem_filter.mp h1
    exact List.mem_eraseDups.mpr hx
  have hdict : ((xs.filter p).eraseDups.map wpay).sum ≤ (xs.eraseDups.map wpay).sum :=
    sum_map_le wpay xs.eraseDups _ (nodup_eraseDups _) hmem
  have hkeys : ((xs.filter p).eraseDups).length ≤ (xs.eraseDups).length :=
    length_le_of_subset xs.eraseDups (xs.filter p).eraseDups (nodup_eraseDups _) hmem
  have hrows : (xs.filter p).length ≤ xs.length := List.length_filter_le p xs
  have hw : width ((xs.filter p).eraseDups.length - 1) ≤ width (xs.eraseDups.length - 1) :=
    width_mono (by omega)
  have hid : byteCount ((xs.filter p).length * width ((xs.filter p).eraseDups.length - 1))
      ≤ byteCount (xs.length * width (xs.eraseDups.length - 1)) :=
    byteCount_mono (Nat.mul_le_mul hrows hw)
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_FilteredDictionaryFootprint
