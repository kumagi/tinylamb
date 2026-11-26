import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

/-!
Shared stability lemmas for the spill-merge model. `mergeSort` compares only
row keys, so equal keys keep their input occurrence order. This module proves
that filtering a sorted stream by one key recovers the original subsequence;
from that, sorted streams with the same multiset and the same per-key
subsequences are equal. These facts justify restarting a merge pass from its
published prefix and discarding unread runs under LIMIT.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

abbrev leRow (a b : Row) : Bool := decide (a.1 ≤ b.1)
abbrev byKey (k : Nat) (r : Row) : Bool := decide (r.1 == k)

theorem leRow_trans : ∀ a b c : Row, leRow a b = true → leRow b c = true → leRow a c = true := by
  intro a b c hab hbc
  simp [leRow] at hab hbc ⊢
  omega

theorem leRow_total : ∀ a b : Row, (leRow a b || leRow b a) = true := by
  intro a b
  by_cases h : a.1 ≤ b.1
  · simp [leRow, h]
  · have h' : b.1 ≤ a.1 := by omega
    simp [leRow, h, h']

theorem sorted_pairwise (l : List Row) : (sorted l).Pairwise (fun a b => leRow a b = true) := by
  exact List.pairwise_mergeSort leRow_trans leRow_total l

/-! Sorting by key is stable: selecting one key from the output gives exactly
the input rows with that key, in their original occurrence order. -/
theorem filter_sorted : ∀ (l : List Row) (k : Nat),
    (sorted l).filter (byKey k) = l.filter (byKey k)
  | [], _ => by simp [sorted]
  | [_], _ => by simp [sorted]
  | a :: b :: xs, k => by
      obtain ⟨l₁, l₂, h₁, h₂, h₃⟩ :=
        List.mergeSort_cons leRow_trans leRow_total a (b :: xs)
      change ((a :: b :: xs).mergeSort leRow).filter (byKey k) = (a :: b :: xs).filter (byKey k)
      by_cases hp : byKey k a
      · have hak : a.1 = k := by simpa [byKey] using hp
        have hl₁ : l₁.filter (byKey k) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro x hxbase
          have hle := h₃ x hxbase
          simp [leRow] at hle
          intro hxpk
          have hxk : x.1 = k := by simpa [byKey] using hxpk
          omega
        have hih := filter_sorted (b :: xs) k
        change ((b :: xs).mergeSort leRow).filter (byKey k) = (b :: xs).filter (byKey k) at hih
        rw [h₂] at hih
        simp only [List.filter_append, hl₁, List.nil_append] at hih
        rw [h₁]
        simp [List.filter_append, hp, hl₁, hih]
      · have hafalse : byKey k a = false := by cases hba : byKey k a <;> simp_all
        rw [h₁]
        simp [hafalse, List.filter_append]
        have hih := filter_sorted (b :: xs) k
        change ((b :: xs).mergeSort leRow).filter (byKey k) = (b :: xs).filter (byKey k) at hih
        rw [h₂] at hih
        simpa [List.filter_append] using hih

/-! Two sorted streams with identical per-key subsequences are identical. -/
theorem sorted_eq_of_same_filters : ∀ (s t : List Row),
    s.Pairwise (fun a b => leRow a b = true) →
    t.Pairwise (fun a b => leRow a b = true) →
    (∀ k, s.filter (byKey k) = t.filter (byKey k)) → s = t
  | [], t, _, ht, hf => by
      cases t with
      | nil => rfl
      | cons b bs =>
          have h := hf b.1
          simp [byKey] at h
  | a :: ss, t, hs, ht, hf => by
      have hmemf : a ∈ t.filter (byKey a.1) := by
        rw [← hf a.1]
        simp [byKey]
      have hmem : a ∈ t := (List.mem_filter.mp hmemf).1
      obtain ⟨pre, post, ht_eq, hnotpre⟩ := List.eq_append_cons_of_mem hmem
      rw [ht_eq] at ht hf hmemf
      have hfa := hf a.1
      simp [byKey, List.filter_append] at hfa
      have hsplit := List.append_eq_cons_iff.mp hfa.symm
      rcases hsplit with hgood | hbad
      · have hpre_filter : pre.filter (byKey a.1) = [] := hgood.1
        have hpost_filter : post.filter (byKey a.1) = ss.filter (byKey a.1) := by
          exact congrArg List.tail hgood.2
        have hpre_nil : pre = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro b hbpre
          have hbkey_ne : b.1 ≠ a.1 := by
            intro hbeq
            have hbpk : byKey a.1 b = true := by simpa [byKey, hbeq]
            have : b ∈ pre.filter (byKey a.1) := by simp [hbpk, hbpre]
            rw [hpre_filter] at this
            simp at this
          have hble : b.1 ≤ a.1 := by
            have hrel := ht.rel_of_mem_append hbpre (show a ∈ a :: post from List.mem_cons_self)
            simpa [leRow] using hrel
          have hbkey_lt : b.1 < a.1 := by omega
          have hs_nob : ∀ x ∈ (a :: ss), byKey b.1 x = false := by
            intro x hxmem
            cases hxmem with
            | head => simp [byKey]; omega
            | tail _ hxss =>
                have hge : a.1 ≤ x.1 := by
                  have hrel := List.rel_of_pairwise_cons hs hxss
                  simpa [leRow] using hrel
                simp [byKey]; omega
          have hsfilternil : (a :: ss).filter (byKey b.1) = [] :=
            List.filter_eq_nil_iff.mpr (by
              intro x hx hpx
              have hfalse := hs_nob x hx
              rw [hfalse] at hpx
              contradiction)
          have htfilternil : (pre ++ a :: post).filter (byKey b.1) = [] := by
            rw [← hf b.1, hsfilternil]
          have : b ∈ (pre ++ a :: post).filter (byKey b.1) := by simp [hbpre, byKey]
          rw [htfilternil] at this
          simp at this
        rw [hpre_nil] at ht hf hfa
        simp only [List.nil_append] at ht hf hfa
        have hpost_pair : post.Pairwise (fun a b => leRow a b = true) :=
          (List.pairwise_cons.mp ht).2
        have hfilters : ∀ key, ss.filter (byKey key) = post.filter (byKey key) := by
          intro key
          by_cases hkey : key = a.1
          · subst key
            exact hpost_filter.symm
          · have h := hf key
            have hkey' : a.1 ≠ key := by omega
            have hafalse : byKey key a = false := by simp [byKey, hkey']
            simpa [hafalse] using h
        have hrec := sorted_eq_of_same_filters ss post hs.tail hpost_pair hfilters
        simpa [ht_eq, hpre_nil, hrec]
      · rcases hbad with ⟨as', hprequality, _⟩
        have : a ∈ pre := (List.mem_filter.mp (by rw [hprequality]; simp)).1
        exact (hnotpre this).elim

/-! Combining multiset equality with equal per-key subsequences. -/
theorem sorted_eq_of_perm_of_filters {l₁ l₂ : List Row} (hp : l₁.Perm l₂)
    (hf : ∀ k, l₁.filter (byKey k) = l₂.filter (byKey k)) :
    sorted l₁ = sorted l₂ := by
  apply sorted_eq_of_same_filters
  · exact sorted_pairwise l₁
  · exact sorted_pairwise l₂
  · intro k
    rw [filter_sorted, filter_sorted, hf k]

theorem flatten_flatten (groups : List (List Run)) :
    groups.flatten.flatten = (groups.map (fun g => g.flatten)).flatten := by
  induction groups with
  | nil => simp
  | cons g gs ih => simp [List.flatten_append, ih]

/-! A published prefix of merge passes has the same multiset as the original
runs, because each completed pass only re-sorts its own group. -/
theorem published_perm (groups : List (List Run)) (rest : List Run) :
    (published groups rest).flatten.Perm (groups.flatten ++ rest).flatten := by
  unfold published
  simp only [List.flatten_append]
  have hgroups : (groups.map (fun g => sorted g.flatten)).flatten.Perm
      (groups.map (fun g => g.flatten)).flatten := by
    induction groups with
    | nil => simp
    | cons g gs ih =>
        simp only [List.map_cons, List.flatten_cons]
        exact (List.mergeSort_perm g.flatten leRow).append ih
  rw [flatten_flatten]
  exact hgroups.append (List.Perm.refl rest.flatten)

theorem published_filter (groups : List (List Run)) (rest : List Run) (k : Nat) :
    (published groups rest).flatten.filter (byKey k) =
      (groups.flatten ++ rest).flatten.filter (byKey k) := by
  unfold published
  simp only [List.flatten_append, List.filter_append]
  congr 1
  induction groups with
  | nil => simp
  | cons g gs ih =>
      simp only [List.map_cons, List.flatten_cons, List.filter_append]
      rw [List.flatten_append, List.filter_append, ih]
      simp [filter_sorted g.flatten k]

theorem published_sorted_eq (groups : List (List Run)) (rest : List Run) :
    sorted (published groups rest).flatten = sorted (groups.flatten ++ rest).flatten := by
  exact sorted_eq_of_perm_of_filters (published_perm groups rest) (published_filter groups rest)

end TinylambOpt.ResearchProofs.DatabaseCorrectness.SortRetryCommon
