import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# PartitionedUserBitmapDistinct

Summing distinct counts for disjoint user ID partitions gives the distinct count for the
whole group.

A grouped DISTINCT can maintain per-partition bitmaps and add their population counts.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PartitionedUserBitmapDistinct
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem eraseDups_nodup : ∀ l : List Nat, l.eraseDups.Nodup := by
  intro l
  generalize hl : l.length = n
  revert l
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hlen
      cases l with
      | nil => simp
      | cons a t =>
          have htlen : (t.filter (fun b => !(b == a))).length < n := by
            rw [← hlen]
            simp
            have hle := List.length_filter_le (fun b => !(b == a)) t
            omega
          rw [List.eraseDups_cons, List.nodup_cons]
          constructor
          · rw [List.mem_eraseDups, List.mem_filter]
            intro h
            simpa using h.2
          · exact ih _ htlen (t.filter (fun b => !(b == a))) rfl

theorem nodup_filter {α : Type} (p : α → Bool) : ∀ l : List α, l.Nodup → (l.filter p).Nodup
  | [], h => by simp
  | a :: l, h => by
      have h' := List.nodup_cons.mp h
      by_cases hp : p a = true
      · have hf : (a :: l).filter p = a :: l.filter p := by simp [hp]
        rw [hf, List.nodup_cons]
        constructor
        · intro hmem
          exact h'.1 ((List.mem_filter.mp hmem).1)
        · exact nodup_filter p l h'.2
      · have hpf : p a = false := by simpa using hp
        simpa [List.filter_cons, hpf] using nodup_filter p l h'.2

theorem nodup_map_of_injective {α β : Type} (f : α → β) (hf : Function.Injective f) :
    ∀ l : List α, l.Nodup → (l.map f).Nodup
  | [], h => by simp
  | a :: l, h => by
      have h' := List.nodup_cons.mp h
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨b, hb, hfb⟩
        have hba : b = a := hf hfb
        subst b
        exact h'.1 hb
      · exact nodup_map_of_injective f hf l h'.2

theorem inj_pair (w x y a b : Nat) (hy : y < w) (hb : b < w)
    (h : x * w + y = a * w + b) : x = a ∧ y = b := by
  rcases Nat.lt_trichotomy x a with h1 | h1 | h1
  · exfalso
    have := Nat.mul_le_mul_right w (show x + 1 ≤ a by omega)
    rw [Nat.add_one_mul] at this
    omega
  · subst h1
    exact ⟨rfl, by omega⟩
  · exfalso
    have := Nat.mul_le_mul_right w (show a + 1 ≤ x by omega)
    rw [Nat.add_one_mul] at this
    omega

theorem nodup_pages_bits (pages : List Nat) (w : Nat) (hpages : pages.Nodup)
    (bits : Nat → List Nat)
    (hbits_lt : ∀ p ∈ pages, ∀ b ∈ bits p, b < w)
    (hbits_nodup : ∀ p ∈ pages, (bits p).Nodup) :
    (pages.flatMap (fun p => (bits p).map (fun b => p * w + b))).Nodup := by
  induction pages with
  | nil => simp
  | cons p ps ih =>
      have hpages' := List.nodup_cons.mp hpages
      rw [List.flatMap_cons, List.nodup_append]
      refine ⟨?_, ?_, ?_⟩
      · apply nodup_map_of_injective (fun b => p * w + b)
        · intro b c hbc
          change p * w + b = p * w + c at hbc
          omega
        · exact hbits_nodup p (by simp)
      · apply ih hpages'.2
        · intro q hq b hb
          exact hbits_lt q (by simp [hq]) b hb
        · intro q hq
          exact hbits_nodup q (by simp [hq])
      · intro a ha b hb heq
        rcases List.mem_map.mp ha with ⟨ba, hba, haeq⟩
        rcases List.mem_flatMap.mp hb with ⟨q, hq, hbq⟩
        rcases List.mem_map.mp hbq with ⟨bq, hbq', hbeq⟩
        rw [← haeq, ← hbeq] at heq
        have hpneq : p ≠ q := by
          intro hpq
          subst hpq
          exact hpages'.1 hq
        have hba_lt := hbits_lt p (by simp) ba hba
        have hbq_lt := hbits_lt q (by simp [hq]) bq hbq'
        exact hpneq (inj_pair w p ba q bq hba_lt hbq_lt heq).1

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.PartitionedUserBitmapDistinct.Goal := by
  intro users w hw
  let pages := (users.map (fun u => u / w)).eraseDups
  let bits := fun p => (List.range w).filter (fun b => users.contains (p * w + b))
  let recon := pages.flatMap (fun p => (bits p).map (fun b => p * w + b))
  change (pages.map (fun p => (bits p).length)).sum = users.eraseDups.length
  have hpages_nodup : pages.Nodup := by
    unfold pages
    exact eraseDups_nodup _
  have hbits_lt : ∀ p ∈ pages, ∀ b ∈ bits p, b < w := by
    intro p hp b hb
    have hb' : b ∈ List.range w := (List.mem_filter.mp hb).1
    simpa using hb'
  have hbits_nodup : ∀ p ∈ pages, (bits p).Nodup := by
    intro p hp
    unfold bits
    exact nodup_filter _ _ List.nodup_range
  have hrecon_nodup : recon.Nodup := by
    unfold recon
    exact nodup_pages_bits pages w hpages_nodup bits hbits_lt hbits_nodup
  have hmem_recon : ∀ x : Nat, x ∈ recon ↔ x ∈ users := by
    intro x
    constructor
    · intro hx
      rcases List.mem_flatMap.mp hx with ⟨p, hp, hx⟩
      rcases List.mem_map.mp hx with ⟨b, hb, rfl⟩
      have hcontains : users.contains (p * w + b) = true := (List.mem_filter.mp hb).2
      exact List.contains_iff_mem.mp hcontains
    · intro hx
      let p := x / w
      let b := x % w
      have hp_pages : p ∈ pages := by
        rw [List.mem_eraseDups]
        apply List.mem_map.mpr
        refine ⟨x, hx, ?_⟩
        rfl
      have hb_range : b ∈ List.range w := by
        rw [List.mem_range]
        exact Nat.mod_lt x hw
      have hdec : p * w + b = x := by
        unfold p b
        rw [Nat.mul_comm]
        exact Nat.div_add_mod x w
      have hb_bits : b ∈ bits p := by
        rw [List.mem_filter]
        refine ⟨hb_range, ?_⟩
        rw [hdec]
        exact List.contains_iff_mem.mpr hx
      refine List.mem_flatMap.mpr ⟨p, hp_pages, ?_⟩
      exact List.mem_map.mpr ⟨b, hb_bits, hdec⟩
  have hperm : recon.Perm users.eraseDups :=
    (List.perm_ext_iff_of_nodup hrecon_nodup (eraseDups_nodup users)).mpr (by
      intro x
      rw [hmem_recon x, List.mem_eraseDups])
  have hL : (pages.map (fun p => (bits p).length)).sum = recon.length := by
    unfold recon
    rw [List.length_flatMap]
    simp
  rw [hL]
  exact hperm.length_eq
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PartitionedUserBitmapDistinct
