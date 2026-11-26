import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_ShardBoundary

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.TieredPrefetch

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
      have hbeq : (x == x) = true := by simp
      simp [hbeq] at hx'
    · exact eraseDups_nodup (List.filter (fun b => !b == x) xs)
termination_by xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b => !b == x) xs)

private theorem points_mem (xs : List Nat) (x : Nat) : x ∈ points xs ↔ x ∈ xs := by
  simp [points]

private theorem points_nodup (xs : List Nat) : (points xs).Nodup := by
  unfold points
  exact (List.mergeSort_perm xs.eraseDups (fun a b : Nat => a ≤ b)).nodup_iff.mpr
    (eraseDups_nodup xs)

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

private theorem points_sorted (xs : List Nat) : (points xs).Pairwise (fun a b => a < b) := by
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

/-- Strictly increasing lists with the same elements are equal. -/
private theorem eq_of_perm_of_pairwise_lt {l₁ l₂ : List Nat}
    (p : l₁.Perm l₂) (h₁ : l₁.Pairwise (fun a b => a < b))
    (h₂ : l₂.Pairwise (fun a b => a < b)) : l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
    have hlen := p.length_eq
    cases l₂ with
    | nil => rfl
    | cons b bs => simp at hlen
  | cons a t ih =>
    have ha_mem : a ∈ l₂ := p.mem_iff.mp (by simp)
    obtain ⟨s, u, hlu⟩ := (List.mem_iff_append).mp ha_mem
    subst l₂
    have hs_nil : s = [] := by
      cases s with
      | nil => rfl
      | cons b s' =>
        have hb_l₁ : b ∈ a :: t := p.mem_iff.mpr (by simp)
        have hpair := (List.pairwise_append.mp h₂).2.2
        have hba : b < a := hpair b (by simp) a (by simp)
        rw [List.mem_cons] at hb_l₁
        rcases hb_l₁ with rfl | hbt
        · omega
        · have hab : a < b := (List.pairwise_cons.mp h₁).1 b hbt
          omega
    subst s
    simp only [List.nil_append] at p h₂
    have ht : t.Perm u := List.Perm.cons_inv p
    have ht_sorted : t.Pairwise (fun a b => a < b) := (List.pairwise_cons.mp h₁).2
    have hu_sorted : u.Pairwise (fun a b => a < b) := (List.pairwise_cons.mp h₂).2
    simpa using ih ht ht_sorted hu_sorted

private theorem removeAll_eq_self_of_disjoint (xs ys : List Nat)
    (hxy : ∀ x ∈ xs, ∀ y ∈ ys, x < y) : ys.removeAll xs = ys := by
  unfold List.removeAll
  apply List.filter_eq_self.mpr
  intro y hy
  by_cases hyx : y ∈ xs
  · have hlt := hxy y hyx y hy
    simp [List.elem_eq_mem, hyx] at hlt
  · simp [List.elem_eq_mem, hyx]

private theorem points_append (xs ys : List Nat)
    (hxy : ∀ x ∈ xs, ∀ y ∈ ys, x < y) :
    points (xs ++ ys) = points xs ++ points ys := by
  apply eq_of_perm_of_pairwise_lt
  · unfold points
    rw [List.eraseDups_append, removeAll_eq_self_of_disjoint xs ys hxy]
    exact (List.mergeSort_perm _ _).trans
      ((List.mergeSort_perm _ _).append (List.mergeSort_perm _ _)).symm
  · exact points_sorted (xs ++ ys)
  · rw [List.pairwise_append]
    refine ⟨points_sorted xs, points_sorted ys, ?_⟩
    intro a ha b hb
    have ha' : a ∈ xs := points_mem xs a |>.mp ha
    have hb' : b ∈ ys := points_mem ys b |>.mp hb
    exact hxy a ha' b hb'

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
      simp only [covers, List.mem_singleton] at hps
      have hxs : xs = [] := by
        cases xs with
        | nil => rfl
        | cons y ys =>
          rw [covers, List.mem_flatMap] at hq
          obtain ⟨q', hq', hnil⟩ := hq
          cases q' <;> simp_all
      subst xs
      subst ps
      exact .singleton x [] [] .nil
    | cons e rest =>
      simp only [covers] at hps
      rw [List.mem_cons] at hps
      rcases hps with hps | hps
      · subst ps
        exact .singleton x xs (e :: rest) (ih hq)
      · rw [List.mem_singleton] at hps
        subst ps
        obtain ⟨d, hd, hrest⟩ := parts_first_split (ih hq)
        exact .bridge x xs d e.2 rest hd hrest

private theorem parts_covers_aux (n : Nat) (xs : List Nat) (ps : List Extent)
    (hlen : xs.length = n) (h : Parts xs ps) : ps ∈ covers xs := by
  induction n generalizing xs ps with
  | zero =>
    cases xs with
    | nil =>
      cases h
      simp [covers]
    | cons x xs => simp at hlen
  | succ n ih =>
    cases h with
    | nil => simp [covers]
    | singleton x xs rest hrest =>
      have hr : rest ∈ covers xs := ih xs rest (by simp at hlen; omega) hrest
      rw [covers]
      exact List.mem_flatMap.mpr ⟨rest, hr, by cases rest <;> simp⟩
    | bridge x xs d b rest hget hrest =>
      cases xs with
      | nil => simp at hget
      | cons a tail =>
        have hp : Parts (a :: tail) ((a, b) :: rest) := by
          cases d with
          | zero =>
            simp only [List.getElem?_cons_zero, Option.some.injEq] at hget
            subst b
            exact .singleton a tail rest hrest
          | succ d =>
            exact .bridge a tail d b rest (by simpa using hget) hrest
        have hq : (a, b) :: rest ∈ covers (a :: tail) :=
          ih (a :: tail) ((a, b) :: rest) (by simp at hlen; omega) hp
        rw [covers]
        exact List.mem_flatMap.mpr ⟨(a, b) :: rest, hq, by simp⟩

private theorem parts_covers {xs : List Nat} {ps : List Extent}
    (h : Parts xs ps) : ps ∈ covers xs :=
  parts_covers_aux xs.length xs ps rfl h

private theorem covers_iff_parts {xs : List Nat} {ps : List Extent} :
    ps ∈ covers xs ↔ Parts xs ps :=
  ⟨covers_parts, parts_covers⟩

/-- A covering plan of each side concatenates to a covering plan of the union. -/
private theorem covers_append {xs ys : List Nat} {ps qs : List Extent}
    (hxy : ∀ x ∈ xs, ∀ y ∈ ys, x < y)
    (hps : ps ∈ covers xs) (hqs : qs ∈ covers ys) :
    ps ++ qs ∈ covers (xs ++ ys) := by
  have hp : Parts xs ps := covers_iff_parts.mp hps
  induction hp generalizing ys qs with
  | nil =>
    simpa using hqs
  | singleton x xs rest hrest ih =>
    have hxy' : ∀ x' ∈ xs, ∀ y ∈ ys, x' < y := by
      intro x' hx' y hy
      exact hxy x' (by simp [hx']) y hy
    have htail : rest ++ qs ∈ covers (xs ++ ys) :=
      ih hxy' (covers_iff_parts.mpr hrest) hqs
    change ((x, x) :: rest) ++ qs ∈
      (covers (xs ++ ys)).flatMap (fun q => match q with
        | [] => [[(x, x)]]
        | (_, b) :: rest' => [(x, x) :: q, (x, b) :: rest'])
    refine List.mem_flatMap.mpr ⟨rest ++ qs, htail, ?_⟩
    by_cases hnil : rest ++ qs = []
    · simp [hnil]
    · cases hL : rest ++ qs with
      | nil => exact (hnil hL).elim
      | cons e tl => simp [hL]
  | bridge x xs d b rest hget hrest ih =>
    have hxy' : ∀ x' ∈ xs.drop (d + 1), ∀ y ∈ ys, x' < y := by
      intro x' hx' y hy
      exact hxy x' (List.mem_cons_of_mem x (List.mem_of_mem_drop hx')) y hy
    have htail : rest ++ qs ∈ covers (xs.drop (d + 1) ++ ys) :=
      ih hxy' (covers_iff_parts.mpr hrest) hqs
    have hdlt : d < xs.length := (List.getElem?_eq_some_iff.mp hget).1
    have hget' : (xs ++ ys)[d]? = some b := by
      rw [List.getElem?_append]
      simpa [hdlt] using hget
    have hdrop' : (xs ++ ys).drop (d + 1) = xs.drop (d + 1) ++ ys := by
      rw [List.drop_append]
      have hle : d + 1 ≤ xs.length := Nat.succ_le_of_lt hdlt
      have hz : d + 1 - xs.length = 0 := Nat.sub_eq_zero_of_le hle
      rw [hz]
      simp
    apply covers_iff_parts.mpr
    have htp : Parts ((xs ++ ys).drop (d + 1)) (rest ++ qs) := by
      rw [hdrop']
      exact covers_iff_parts.mp htail
    exact .bridge x (xs ++ ys) d b (rest ++ qs) hget' htp

private theorem choose_mem {f : Price} {xs : List Nat} :
    choose f xs ∈ covers (points xs) := by
  let leRel : List Extent → List Extent → Bool := fun a b => cost f a ≤ cost f b
  unfold choose
  change ((covers (points xs)).mergeSort leRel).head?.getD [] ∈ covers (points xs)
  have hperm : ((covers (points xs)).mergeSort leRel).Perm (covers (points xs)) :=
    List.mergeSort_perm _ _
  obtain ⟨q, hq⟩ := covers_nonempty (points xs)
  have hqS : q ∈ (covers (points xs)).mergeSort leRel := hperm.mem_iff.mpr hq
  cases hS : (covers (points xs)).mergeSort leRel with
  | nil => simp [hS] at hqS
  | cons hd tl =>
    simp only [List.head?_cons, Option.getD_some]
    exact hperm.mem_iff.mp (by simp [hS])

private theorem choose_le {f : Price} {xs : List Nat} {ps : List Extent}
    (hps : ps ∈ covers (points xs)) : cost f (choose f xs) ≤ cost f ps := by
  let leRel : List Extent → List Extent → Bool := fun a b => cost f a ≤ cost f b
  let S := (covers (points xs)).mergeSort leRel
  unfold choose
  change cost f (S.head?.getD []) ≤ cost f ps
  have hperm : S.Perm (covers (points xs)) := by
    dsimp [S]
    exact List.mergeSort_perm _ _
  have hsorted : S.Pairwise (fun a b => cost f a ≤ cost f b) := by
    dsimp [S]
    have hs := List.pairwise_mergeSort
      (le := leRel)
      (trans := fun a b c hab hbc => by
        dsimp [leRel] at hab hbc ⊢
        simp only [decide_eq_true_eq] at hab hbc ⊢
        exact Nat.le_trans hab hbc)
      (total := fun a b => by
        dsimp [leRel]
        rcases Nat.le_total (cost f a) (cost f b) with h | h
        · simp [h]
        · simp [h])
      (covers (points xs))
    simpa only [leRel, decide_eq_true_eq] using hs
  obtain ⟨q, hq⟩ := covers_nonempty (points xs)
  have hqS : q ∈ S := hperm.mem_iff.mpr hq
  have hpsS : ps ∈ S := hperm.mem_iff.mpr hps
  cases hS : S with
  | nil => simp [hS] at hqS
  | cons hd tl =>
    have hs' : (hd :: tl).Pairwise (fun a b => cost f a ≤ cost f b) := by simpa [hS] using hsorted
    have hhead : ∀ a' ∈ tl, cost f hd ≤ cost f a' := by
      intro a' ha'
      have hle' := (List.pairwise_cons.mp hs').1 a' ha'
      simpa only [leRel, decide_eq_true_eq] using hle'
    simp only [List.head?_cons, Option.getD_some]
    rcases List.mem_cons.mp (by simpa [hS] using hpsS) with hEq | htail
    · subst hd
      exact Nat.le_refl (cost f ps)
    · exact hhead _ htail

/-- Splitting a request set at a value boundary cannot beat the independent plans. -/
private theorem shardBoundary_of_price (f : Price) : shardBoundary f := by
  intro xs ys hxy
  have hpts : points (xs ++ ys) = points xs ++ points ys := points_append xs ys hxy
  have hpx : choose f xs ∈ covers (points xs) := choose_mem
  have hpy : choose f ys ∈ covers (points ys) := choose_mem
  have hxy' : ∀ x ∈ points xs, ∀ y ∈ points ys, x < y := by
    intro x hx y hy
    exact hxy x (points_mem xs x |>.mp hx) y (points_mem ys y |>.mp hy)
  have hconcat : choose f xs ++ choose f ys ∈ covers (points (xs ++ ys)) := by
    rw [hpts]
    exact covers_append hxy' hpx hpy
  calc
    cost f (choose f (xs ++ ys)) ≤ cost f (choose f xs ++ choose f ys) :=
      choose_le hconcat
    _ = cost f (choose f xs) + cost f (choose f ys) := by simp [cost]

theorem sound_ShardBoundary : TieredPrefetch.ShardBoundary.Goal := by
  intro limit spill
  exact shardBoundary_of_price (price limit spill)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_TieredPrefetch_ShardBoundary
