import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `Sector.ShardBoundary` — `∀ (shift startup : Nat), shardBoundary (price shift
  startup)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_ShardBoundary

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Sector

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

/-- Exact recursive characterization of the plans enumerated by `covers`. -/
private inductive Parts : List Nat → List Extent → Prop
  | nil : Parts [] []
  | singleton : (x : Nat) → (xs : List Nat) → (rest : List Extent) →
      Parts xs rest → Parts (x :: xs) ((x, x) :: rest)
  | bridge : (x : Nat) → (xs : List Nat) → (d : Nat) → (b : Nat) →
      (rest : List Extent) → xs[d]? = some b →
      Parts (xs.drop (d + 1)) rest → Parts (x :: xs) ((x, b) :: rest)

private theorem parts_head_bridge {a b : Nat} {xs : List Nat} {rest : List Extent}
    (h : Parts xs ((a, b) :: rest)) :
    ∃ d, xs[d]? = some b ∧ Parts (xs.drop (d + 1)) rest := by
  cases h with
  | singleton y ys r hr =>
    exact ⟨0, by simp, by simpa using hr⟩
  | bridge y ys d b' r hg hr =>
    exact ⟨d + 1, by simpa using hg, by simpa using hr⟩

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
    have hparts_q : Parts xs q := ih hq
    cases q with
    | nil =>
      have hps' : ps = [(x, x)] := by simpa using hps
      subst ps
      exact .singleton x xs [] hparts_q
    | cons e rest =>
      simp at hps
      rcases hps with rfl | rfl
      · exact .singleton x xs (e :: rest) hparts_q
      · obtain ⟨d, hget, hrest⟩ := parts_head_bridge hparts_q
        exact .bridge x xs d e.2 rest (by simpa using hget) (by simpa using hrest)

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

/-- Concatenating two valid shard plans is a valid plan of the union. -/
private theorem parts_append {xs ys : List Nat} {ps qs : List Extent}
    (hps : Parts xs ps) (hqs : Parts ys qs) : Parts (xs ++ ys) (ps ++ qs) := by
  induction hps generalizing ys qs with
  | nil => simpa using hqs
  | singleton x xs rest hrest ih =>
    simpa using (Parts.singleton x (xs ++ ys) (rest ++ qs) (ih hqs))
  | bridge x xs d b rest hget hrest ih =>
    have hdlt : d < xs.length := (List.getElem?_eq_some_iff.mp hget).1
    have hget' : (xs ++ ys)[d]? = some b := by
      simpa [List.getElem?_append_left hdlt] using hget
    have hdrop : (xs ++ ys).drop (d + 1) = xs.drop (d + 1) ++ ys := by
      rw [List.drop_append]
      have hle : d + 1 ≤ xs.length := Nat.succ_le_of_lt hdlt
      simp [hle]
    have hrest' : Parts ((xs ++ ys).drop (d + 1)) (rest ++ qs) := by
      rw [hdrop]
      exact ih hqs
    exact Parts.bridge x (xs ++ ys) d b (rest ++ qs) hget' hrest'

private theorem covers_append {xs ys : List Nat} {ps qs : List Extent}
    (hps : ps ∈ covers xs) (hqs : qs ∈ covers ys) : ps ++ qs ∈ covers (xs ++ ys) := by
  rw [covers_iff_parts] at hps hqs ⊢
  exact parts_append hps hqs

private theorem covers_nonempty : (xs : List Nat) → covers xs ≠ []
  | [] => by simp [covers]
  | x :: xs => by
    have ih := covers_nonempty xs
    simp only [covers, ne_eq]
    intro h
    cases hxs : covers xs with
    | nil => exact ih hxs
    | cons p ps =>
      have : (match p with | [] => [[(x, x)]] | (_, b) :: rest => [(x, x) :: p, (x, b) :: rest]) = [] := by
        have hall : ∀ ps' ∈ p :: ps, (match ps' with | [] => [[(x, x)]] | (_, b) :: rest => [(x, x) :: ps', (x, b) :: rest]) = [] := by
          simp only [List.flatMap_eq_nil_iff] at h
          rw [hxs] at h
          exact h
        exact hall p (by simp)
      cases p with
      | nil => simp at this
      | cons hd tl => simp at this

private theorem pairwise_head?_le {α : Type} {le : α → α → Bool}
    (hrefl : ∀ x, le x x = true)
    {l : List α} {a b : α} (hpw : l.Pairwise (fun x y => le x y = true))
    (hhead : l.head? = some a) (hmem : b ∈ l) : le a b = true := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
    simp at hhead
    subst hhead
    rw [List.pairwise_cons] at hpw
    rcases List.mem_cons.mp hmem with rfl | hmem
    · exact hrefl _
    · exact hpw.1 b hmem

private theorem choose_mem_covers (f : Price) (xs : List Nat) (hne : covers (points xs) ≠ []) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  have hperm := List.mergeSort_perm (covers (points xs)) (fun a b => decide (cost f a ≤ cost f b))
  have hne' : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact hne this
  cases h : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) with
  | nil => exact absurd h hne'
  | cons hd tl =>
    have hmem : hd ∈ (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) := by
      simp [h]
    rw [List.mem_mergeSort] at hmem
    simpa [h] using hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) :
    cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil =>
    simp [h] at hmem'
  | cons hd tl =>
    simp only [h, List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair
      (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simp only [le, decide_eq_true_eq] at hle
    exact hle

theorem sound_ShardBoundary : Sector.ShardBoundary.Goal := by
  intro shift startup xs ys hxy
  let f : Price := price shift startup
  have hpts : points (xs ++ ys) = points xs ++ points ys := points_append xs ys hxy
  have hcxs : choose f xs ∈ covers (points xs) := choose_mem_covers f xs (covers_nonempty _)
  have hcys : choose f ys ∈ covers (points ys) := choose_mem_covers f ys (covers_nonempty _)
  have hjoin : choose f xs ++ choose f ys ∈ covers (points (xs ++ ys)) := by
    rw [hpts]
    exact covers_append hcxs hcys
  have hle := choose_le f (xs ++ ys) (choose f xs ++ choose f ys) hjoin
  simpa [cost] using hle

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_ShardBoundary
