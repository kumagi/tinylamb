import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
/-!
Registers the checked result for `SharedVocabulary.RenumberedPackedIdWidth.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

/-!
`RenumberedPackedIdWidth`: when every row references exactly one local dictionary
entry and the row stream covers the whole dictionary, the peak id is `|d| - 1`
regardless of numbering, so any permutation `e` of a duplicate-free dictionary `d`
packs the id stream to the same byte count.  A compactor may therefore renumber a
shared dictionary (e.g. to cluster hot symbols) without changing frame size.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_RenumberedPackedIdWidth
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## helpers about `peak` -/

private theorem peak_nil : peak [] = 0 := rfl

/-- Folding `max` from an accumulator splits at the head element. -/
private theorem foldl_max (a : Nat) (l : List Nat) :
    l.foldl max a = max a (l.foldl max 0) := by
  induction l generalizing a with
  | nil => simp
  | cons y as ih =>
      show as.foldl max (max a y) = max a (as.foldl max y)
      rw [ih (max a y), ih y]
      omega

private theorem peak_eq_foldl (l : List Nat) : peak l = l.foldl max 0 := by
  cases l with
  | nil => rfl
  | cons y m =>
      show (y :: m).max?.getD 0 = m.foldl max (Nat.max 0 y)
      rw [List.max?_cons']
      simp [Nat.zero_max]

private theorem peak_cons (a : Nat) (l : List Nat) : peak (a :: l) = max a (peak l) := by
  have h1 : peak (a :: l) = l.foldl max a := rfl
  rw [h1, peak_eq_foldl]
  exact foldl_max a l

private theorem peak_le (l : List Nat) (M : Nat) (h : ∀ x ∈ l, x ≤ M) : peak l ≤ M := by
  induction l with
  | nil => rw [peak_nil]; omega
  | cons a as ih =>
      rw [peak_cons]
      refine Nat.max_le.mpr ⟨h a (by simp), ih ?_⟩
      intro x hx
      exact h x (by simp [hx])

private theorem le_peak (l : List Nat) (M : Nat) (h : M ∈ l) : M ≤ peak l := by
  induction l with
  | nil => cases h
  | cons a as ih =>
      rw [peak_cons]
      cases h with
      | head as => exact Nat.le_max_left _ _
      | tail b hm => exact Nat.le_trans (ih hm) (Nat.le_max_right _ _)

/-! ## helpers about dictionaries and `ids` -/

private theorem nodup_perm {d e : List Symbol} (hp : d.Perm e) : d.Nodup → e.Nodup := by
  intro hnd
  induction hp with
  | nil => exact hnd
  | @cons a l₁ l₂ h ih =>
      obtain ⟨h1, h2⟩ := List.nodup_cons.mp hnd
      exact List.nodup_cons.mpr ⟨fun hm => h1 (h.symm.mem_iff.mp hm), ih h2⟩
  | @swap a b l =>
      simp only [List.nodup_cons] at hnd ⊢
      obtain ⟨h1, h2, h3⟩ := hnd
      refine ⟨?_, fun hb => h1 (List.mem_cons_of_mem a hb), h3⟩
      intro hb
      rcases List.mem_cons.mp hb with hab | hl
      · exact h1 (by rw [← hab]; exact List.mem_cons_self)
      · exact h2 hl
  | @trans l₁ l₂ l₃ t₁ t₂ ih₁ ih₂ => exact ih₂ (ih₁ hnd)

/-- With one id per row and full coverage, the peak id is exactly `|d| - 1`. -/
private theorem peak_ids (d xs : List Symbol) (hd : d.Nodup)
    (hsd : ∀ x ∈ d, x ∈ xs) (hxd : ∀ x ∈ xs, x ∈ d) :
    peak (ids d xs) = d.length - 1 := by
  have hup : ∀ v ∈ ids d xs, v ≤ d.length - 1 := by
    intro v hv
    obtain ⟨x, hxm, rfl⟩ := List.mem_map.mp hv
    have hmem : x ∈ d := hxd x hxm
    have hlt : d.idxOf x < d.length := List.idxOf_lt_length_of_mem hmem
    omega
  have hle := peak_le (ids d xs) (d.length - 1) hup
  have hge : d.length - 1 ≤ peak (ids d xs) := by
    by_cases hz : d = []
    · subst hz
      have hxs : xs = [] :=
        List.eq_nil_iff_forall_not_mem.mpr
          (by intro x hx; exact absurd (hxd x hx) (by simp))
      subst xs
      simp [peak_nil]
    · cases d with
      | nil => exact absurd rfl hz
      | cons y m =>
          have hlt : m.length < (y :: m).length := by simp
          have hidx :
              (y :: m).idxOf ((y :: m)[m.length]'hlt) = m.length :=
            hd.idxOf_getElem m.length hlt
          have hmem : (y :: m)[m.length]'hlt ∈ (y :: m) := List.mem_of_getElem rfl
          have hinx : (y :: m)[m.length]'hlt ∈ xs := hsd _ hmem
          have hin : m.length ∈ ids (y :: m) xs := by
            show m.length ∈ xs.map (fun a => (y :: m).idxOf a)
            exact List.mem_map.mpr ⟨_, hinx, hidx⟩
          rw [show (y :: m).length - 1 = m.length from by simp]
          exact le_peak _ _ hin
  exact Nat.le_antisymm hle hge

theorem sound : RenumberedPackedIdWidth.Goal := by
  intro d e xs hp hd hsd hxd
  have he : e.Nodup := nodup_perm hp hd
  have hse : ∀ x ∈ e, x ∈ xs := by
    intro x hx
    exact hsd x (hp.symm.mem_iff.mp hx)
  have hexd : ∀ x ∈ xs, x ∈ e := by
    intro x hx
    exact hp.mem_iff.mp (hxd x hx)
  have h1 : peak (ids d xs) = d.length - 1 := peak_ids d xs hd hsd hxd
  have h2 : peak (ids e xs) = e.length - 1 := peak_ids e xs he hse hexd
  have hl : (ids d xs).length = (ids e xs).length := by
    dsimp only [ids]
    rw [List.length_map, List.length_map]
  unfold packedSize
  rw [h1, h2, hp.length_eq, hl]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SharedVocabulary_RenumberedPackedIdWidth
