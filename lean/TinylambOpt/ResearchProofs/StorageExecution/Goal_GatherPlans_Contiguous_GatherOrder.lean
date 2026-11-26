import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_GatherOrder

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Contiguous

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  cases xs with
  | nil => simp
  | cons x xs =>
    rw [List.eraseDups_cons, List.nodup_cons]
    constructor
    · intro hx
      have hxf : x ∈ List.filter (fun b => !b == x) xs := List.mem_eraseDups.mp hx
      have hx' := List.mem_filter.mp hxf
      have hxx : x ∈ xs.eraseDups := List.mem_eraseDups.mpr hx'.1
      simp at hx'
    · exact eraseDups_nodup (List.filter (fun b => !b == x) xs)
termination_by xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b => !b == x) xs)

private theorem pairwise_lt_of_le_nodup {xs : List Nat}
    (hle : xs.Pairwise (fun a b => a ≤ b)) (hnd : xs.Nodup) :
    xs.Pairwise (fun a b => a < b) := by
  induction xs with
  | nil => exact .nil
  | cons x xs ih =>
    rw [List.pairwise_cons] at hle ⊢
    constructor
    · intro b hb
      have hne : x ≠ b := by
        intro heq
        subst b
        exact (List.nodup_cons.mp hnd).1 hb
      have hle' := hle.1 b hb
      omega
    · exact ih hle.2 (List.nodup_cons.mp hnd).2

private theorem points_sorted (xs : List Nat) :
    (points xs).Pairwise (fun a b => a < b) := by
  unfold points
  refine pairwise_lt_of_le_nodup ?_ ?_
  · have hs := List.pairwise_mergeSort
      (le := fun a b : Nat => decide (a ≤ b))
      (trans := fun a b c hab hbc => by
        simpa only [decide_eq_true_eq] using Nat.le_trans
          (of_decide_eq_true hab) (of_decide_eq_true hbc))
      (total := fun a b => by
        rcases Nat.le_total a b with h | h
        · simp [h]
        · simp [h])
      xs.eraseDups
    simpa only [decide_eq_true_eq] using hs
  · exact (List.mergeSort_perm xs.eraseDups (fun a b : Nat => a ≤ b)).nodup_iff.mpr
      (eraseDups_nodup xs)

private theorem covers_nonempty (xs : List Nat) : ∃ ps, ps ∈ covers xs := by
  induction xs with
  | nil => exact ⟨[], by simp [covers]⟩
  | cons x xs ih =>
    obtain ⟨q, hq⟩ := ih
    cases q with
    | nil =>
      exact ⟨[(x, x)], by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨[], hq, by simp⟩⟩
    | cons e rest =>
      exact ⟨(x, x) :: e :: rest, by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨e :: rest, hq, by simp⟩⟩

private theorem covers_mem_nil {xs : List Nat} : [] ∈ covers xs ↔ xs = [] := by
  cases xs with
  | nil => simp [covers]
  | cons x xs =>
    rw [covers, List.mem_flatMap]
    constructor
    · intro h
      obtain ⟨q, hq, hmem⟩ := h
      cases q <;> simp at hmem
    · intro h
      simp at h

private inductive Parts : List Nat → List Extent → Prop
  | nil : Parts [] []
  | singleton : (x : Nat) → (xs : List Nat) → (rest : List Extent) →
      Parts xs rest → Parts (x :: xs) ((x, x) :: rest)
  | bridge : (x : Nat) → (xs : List Nat) → (d : Nat) → (b : Nat) →
      (rest : List Extent) → xs[d]? = some b →
      Parts (xs.drop (d + 1)) rest → Parts (x :: xs) ((x, b) :: rest)

private theorem parts_first_split {xs : List Nat} {a b : Nat} {rest : List Extent}
    (h : Parts xs ((a, b) :: rest)) :
    ∃ d, xs[d]? = some b ∧ Parts (xs.drop (d + 1)) rest := by
  cases h with
  | singleton x xs rest hrest => exact ⟨0, by simp, hrest⟩
  | bridge x xs d b rest hget hrest =>
    exact ⟨d + 1, by simpa using hget, by simpa using hrest⟩

private theorem covers_parts {xs : List Nat} {ps : List Extent}
    (h : ps ∈ covers xs) : Parts xs ps := by
  induction xs generalizing ps with
  | nil =>
    simp only [covers, List.mem_singleton] at h
    subst ps
    exact .nil
  | cons x xs ih =>
    change ps ∈ (covers xs).flatMap (fun q => match q with
      | [] => [[(x, x)]]
      | (_, b) :: rest => [(x, x) :: q, (x, b) :: rest]) at h
    rw [List.mem_flatMap] at h
    obtain ⟨q, hq, hps⟩ := h
    cases q with
    | nil =>
      simp only [List.mem_singleton] at hps
      subst ps
      have hxs : xs = [] := covers_mem_nil.mp hq
      subst xs
      exact .singleton x [] [] .nil
    | cons e rest =>
      rw [List.mem_cons] at hps
      rcases hps with hps | hps
      · subst ps
        exact .singleton x xs (e :: rest) (ih hq)
      · rw [List.mem_singleton] at hps
        subst ps
        obtain ⟨d, hd, hrest⟩ := parts_first_split (ih hq)
        exact .bridge x xs d e.2 rest hd hrest

private theorem mem_drop_after {xs : List Nat} {d : Nat} {b m : Nat}
    (hsort : xs.Pairwise (fun a c => a < c)) (hget : xs[d]? = some b)
    (hm : m ∈ xs) (hbm : b < m) : m ∈ xs.drop (d + 1) := by
  induction xs generalizing d with
  | nil => simp at hget
  | cons y ys ih =>
    cases d with
    | zero =>
      simp at hget
      subst b
      simp only [List.mem_cons] at hm
      rcases hm with rfl | hm
      · omega
      · simpa using hm
    | succ d =>
      have hget' : ys[d]? = some b := by simpa using hget
      have hsort' : ys.Pairwise (fun a c => a < c) := (List.pairwise_cons.mp hsort).2
      have hbmem : b ∈ ys := List.mem_iff_getElem?.mpr ⟨d, hget'⟩
      have hyb : y < b := (List.pairwise_cons.mp hsort).1 b hbmem
      have hm' : m ∈ ys := by
        simp only [List.mem_cons] at hm
        rcases hm with rfl | hm
        · omega
        · exact hm
      exact ih hsort' hget' hm'

private theorem parts_cover : ∀ {xs : List Nat} {ps : List Extent}, Parts xs ps →
    ∀ (_ : xs.Pairwise (fun a c => a < c)),
      ∀ n ∈ xs, ∃ e ∈ ps, e.1 ≤ n ∧ n ≤ e.2 := by
  intro xs ps h
  induction h with
  | nil =>
    intro _hsort m hm
    simp at hm
  | singleton x tl rest hrest ih =>
    intro hsort m hm
    have hsort_rest : tl.Pairwise (fun a c => a < c) := (List.pairwise_cons.mp hsort).2
    simp only [List.mem_cons] at hm
    rcases hm with rfl | hm
    · exact ⟨(m, m), by simp, by simp, by simp⟩
    · obtain ⟨e, he, he1, he2⟩ := ih hsort_rest m hm
      exact ⟨e, by simp [he], he1, he2⟩
  | bridge x tl d b rest hget hrest ih =>
    intro hsort m hm
    have hsort_tl : tl.Pairwise (fun a c => a < c) := (List.pairwise_cons.mp hsort).2
    have hlt : ∀ z ∈ tl, x < z := (List.pairwise_cons.mp hsort).1
    have hbmem : b ∈ tl := List.mem_iff_getElem?.mpr ⟨d, hget⟩
    have hxb : x < b := hlt b hbmem
    simp only [List.mem_cons] at hm
    rcases hm with rfl | hm
    · exact ⟨(m, b), by simp, by omega, by omega⟩
    · have hxm : x < m := hlt m hm
      by_cases hmb : m ≤ b
      · exact ⟨(x, b), by simp, by omega, hmb⟩
      · have hbm : b < m := Nat.lt_of_not_ge hmb
        have hmdrop : m ∈ tl.drop (d + 1) := mem_drop_after hsort_tl hget hm hbm
        have hsort_drop : (tl.drop (d + 1)).Pairwise (fun a c => a < c) := hsort_tl.drop
        obtain ⟨e, he, he1, he2⟩ := ih hsort_drop m hmdrop
        exact ⟨e, by simp [he], he1, he2⟩

private theorem choose_mem_covers (f : Price) (xs : List Nat) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  let cs := covers (points xs)
  let cmp : List Extent → List Extent → Bool := fun a b => decide (cost f a ≤ cost f b)
  change (cs.mergeSort cmp).head?.getD [] ∈ cs
  have hne : cs ≠ [] := by
    obtain ⟨q, hq⟩ := covers_nonempty (points xs)
    intro hnil
    simp [cs, hnil] at hq
  have hsne : cs.mergeSort cmp ≠ [] := by
    intro hnil
    have hlen := (List.mergeSort_perm cs cmp).length_eq
    simp [hnil] at hlen
    exact hne (List.length_eq_zero_iff.mp hlen.symm)
  cases hsortlist : cs.mergeSort cmp with
  | nil => exact False.elim (hsne hsortlist)
  | cons e tl =>
    have he : e ∈ cs.mergeSort cmp := by simp [hsortlist]
    have hecs : e ∈ cs := (List.mergeSort_perm cs cmp).mem_iff.mp he
    simpa [hsortlist] using hecs

private theorem lookup_of_mem_of_value {l : List (Nat × Nat)} {m v : Nat}
    (hmem : (m, v) ∈ l) (hall : ∀ w, (m, w) ∈ l → w = v) :
    l.lookup m = some v := by
  induction l with
  | nil => simp at hmem
  | cons kv rest ih =>
    rcases kv with ⟨k, w⟩
    by_cases hkm : k = m
    · subst k
      have hwv : w = v := hall w (by simp)
      simp [hwv]
    · have hmem' : (m, v) ∈ rest := by
        simp only [List.mem_cons, Prod.mk.injEq] at hmem
        rcases hmem with h | h
        · exact False.elim (hkm h.1.symm)
        · exact h
      have hall' : ∀ w', (m, w') ∈ rest → w' = v := by
        intro w' hw'
        exact hall w' (by simp [hw'])
      have hmk : m ≠ k := by omega
      have hbeq : (m == k) = false := by simp [hmk]
      rw [List.lookup_cons, hbeq]
      exact ih hmem' hall'

private theorem tagged_mem (ps : List Extent) (src : Nat → Nat) (m : Nat)
    (h : ∃ e ∈ ps, e.1 ≤ m ∧ m ≤ e.2) : (m, src m) ∈ tagged ps src := by
  unfold tagged
  rw [List.mem_flatMap]
  rcases h with ⟨⟨a, b⟩, he, ham, hmb⟩
  refine ⟨(a, b), he, ?_⟩
  rw [List.mem_map]
  refine ⟨m - a, ?_, ?_⟩
  · rw [List.mem_range]
    omega
  · simp only [Prod.mk.injEq]
    constructor
    · omega
    · have ham' : a + (m - a) = m := by omega
      rw [ham']

private theorem tagged_value (ps : List Extent) (src : Nat → Nat) (m w : Nat)
    (h : (m, w) ∈ tagged ps src) : w = src m := by
  unfold tagged at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨⟨a, b⟩, he, hmem⟩
  rw [List.mem_map] at hmem
  rcases hmem with ⟨t, ht, hp⟩
  simp only [Prod.mk.injEq] at hp
  rcases hp with ⟨ham, hsw⟩
  subst m
  exact hsw.symm

private theorem tagged_lookup (ps : List Extent) (src : Nat → Nat) (m : Nat)
    (h : ∃ e ∈ ps, e.1 ≤ m ∧ m ≤ e.2) :
    (tagged ps src).lookup m = some (src m) := by
  exact lookup_of_mem_of_value (tagged_mem ps src m h) (tagged_value ps src m)

theorem sound_GatherOrder : Contiguous.GatherOrder.Goal := by
  intro fee xs src
  apply List.map_congr_left
  intro i hi
  let ps := choose (price fee) xs
  have hps : ps ∈ covers (points xs) := by
    simpa [ps] using choose_mem_covers (price fee) xs
  have hparts : Parts (points xs) ps := covers_parts hps
  have hcov : ∃ e ∈ ps, e.1 ≤ i ∧ i ≤ e.2 :=
    parts_cover hparts (points_sorted xs) i (by simpa [points] using hi)
  exact tagged_lookup ps src i hcov

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_GatherOrder
