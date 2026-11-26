import TinylambOpt.ResearchGoals.ClickBenchLayout.Execution

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_AnchorDemand

open TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
open AnchorDemand

private theorem map_congr {α : Type} : ∀ (ids : List Nat) (f g : Nat → α),
    (∀ i ∈ ids, f i = g i) → ids.map f = ids.map g := by
  intro ids
  induction ids with
  | nil => intro f g h; rfl
  | cons a t ih =>
    intro f g h
    rw [List.map_cons, List.map_cons,
      ih f g (fun i hi => h i (List.Mem.tail _ hi)), h a (List.Mem.head _)]

private theorem map_eq_at : ∀ (ids : List Nat) (f g : Nat → Option Word) (i : Nat),
    i ∈ ids → ids.map f = ids.map g → f i ≠ g i → False := by
  intro ids
  induction ids with
  | nil => intro f g i h; exact absurd h List.not_mem_nil
  | cons a t ih =>
    intro f g i hmem heq hne
    rw [List.mem_cons] at hmem
    rcases hmem with rfl | ht
    · rw [List.map_cons, List.map_cons] at heq
      rw [List.cons.injEq] at heq
      exact hne heq.1
    · rw [List.map_cons, List.map_cons] at heq
      rw [List.cons.injEq] at heq
      exact ih f g i ht heq.2 hne

private theorem mem_eraseDups_fwd (l : List Nat) (a : Nat) (h : a ∈ l) :
    a ∈ l.eraseDups := List.mem_eraseDups.mpr h

private theorem tag_ne (tags : List Tag) (i : Nat)
    (hbeq : (tags[i]? == some .copyBase) = true) : tags[i]? = some .copyBase := by
  cases h1 : tags[i]? with
  | none =>
    rw [h1] at hbeq
    exact absurd hbeq (by decide)
  | some t =>
    cases h1t : t with
    | literal v =>
      rw [h1, h1t] at hbeq
      have he : ((some (.literal v) : Option Tag) == some .copyBase) = false := rfl
      rw [he] at hbeq
      exact absurd hbeq (by decide)
    | copyBase => rfl

private theorem atRow_indep (tags : List Tag) (x y : Nat → Option Word) (i : Nat)
    (h : tags[i]? ≠ some .copyBase) : atRow tags x i = atRow tags y i := by
  cases h1 : tags[i]? with
  | none => simp only [atRow, h1]
  | some t =>
    cases h1t : t with
    | literal v => simp only [atRow, h1, h1t]
    | copyBase => exact absurd (by rw [h1, h1t]) h

private theorem main (tags : List Tag) (frameOf : Nat → Nat) (ids available : List Nat) :
    ((∀ f ∈ needed tags frameOf ids, f ∈ available) ↔
      ∀ (x y : Nat → Option Word), (∀ i, frameOf i ∈ available → x i = y i) →
        ids.map (atRow tags x) = ids.map (atRow tags y)) := by
  constructor
  · intro havail x y hxy
    refine map_congr ids (atRow tags x) (atRow tags y) (fun i hi => ?_)
    by_cases h1 : tags[i]? = some .copyBase
    · have hiF : i ∈ ids.filter (fun j => tags[j]? == some .copyBase) := by
        rw [List.mem_filter]
        refine ⟨hi, ?_⟩
        show (tags[i]? == some .copyBase) = true
        rw [h1]
        decide
      have hf : frameOf i ∈ needed tags frameOf ids :=
        List.mem_eraseDups.mpr (List.mem_map.mpr ⟨i, hiF, rfl⟩)
      rw [show atRow tags x i = x i from by simp only [atRow, h1],
        show atRow tags y i = y i from by simp only [atRow, h1]]
      exact hxy i (havail _ hf)
    · exact atRow_indep tags x y i h1
  · intro hmap f hf
    by_cases hfa : f ∈ available
    · exact hfa
    · obtain ⟨i, hiF, hfi⟩ := List.mem_map.mp (List.mem_eraseDups.mp hf)
      rw [List.mem_filter] at hiF
      obtain ⟨hiids, hbeq⟩ := hiF
      have h1 : tags[i]? = some .copyBase := tag_ne tags i hbeq
      let x : Nat → Option Word := fun k => if k = i then some ([0] : Word) else none
      have hxy : ∀ k, frameOf k ∈ available → x k = (fun _ => none) k := by
        intro k hk
        by_cases hkj : k = i
        · subst hkj
          rw [hfi] at hk
          exact absurd hk hfa
        · show (if k = i then some ([0] : Word) else none) = none
          rw [if_neg hkj]
      have hm := hmap x (fun _ => none) hxy
      have hxv : atRow tags x i = some ([0] : Word) := by
        show (match tags[i]? with
          | some (.literal v) => v | some .copyBase => x i | none => none) = some ([0] : Word)
        rw [h1]
        show x i = some ([0] : Word)
        exact if_pos rfl
      have hyi : atRow tags (fun _ => none) i = none := by
        show (match tags[i]? with
          | some (.literal v) => v | some .copyBase => none | none => none) = none
        rw [h1]
      refine False.elim (map_eq_at ids (atRow tags x) (atRow tags (fun _ => none)) i hiids hm ?_)
      rw [hxv, hyi]
      decide

theorem sound : Goal := main

end TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_AnchorDemand
