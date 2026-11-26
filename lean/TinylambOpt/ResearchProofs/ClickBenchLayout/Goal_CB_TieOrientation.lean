import TinylambOpt.ResearchGoals.ClickBenchLayout.Storage

set_option maxHeartbeats 0
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_TieOrientation

open TinylambOpt.ResearchGoals.ClickBenchLayout.Storage
open TieOrientation

def stepF (states : List (Word × Nat)) (group : List Word) : List (Word × Nat) :=
  if group.isEmpty then states else [group, group.reverse].map fun order =>
    (order.getLast?.getD [], (states.map (fun s => s.2 + charge s.1 order)).min?.getD 0)

def cand (S : List (Word × Nat)) (gs : List (List Word)) : List Nat :=
  S.flatMap (fun s => (orders gs).map (fun o => s.2 + charge s.1 o))

theorem min_getD_le (L : List Nat) (x : Nat) (hx : x ∈ L) : L.min?.getD 0 ≤ x := by
  cases hm : L.min? with
  | none => rw [List.min?_eq_none_iff] at hm; subst hm; simp at hx
  | some m =>
    have := (List.min?_eq_some_iff.mp hm).2 x hx
    simpa using this

theorem min_getD_mem (L : List Nat) (hL : L ≠ []) : L.min?.getD 0 ∈ L := by
  cases hm : L.min? with
  | none => rw [List.min?_eq_none_iff] at hm; exact absurd hm hL
  | some m => simpa using (List.min?_eq_some_iff.mp hm).1

theorem charge_append (prev : Word) (a b : List Word) :
    charge prev (a ++ b) = charge prev a + charge (a.getLast?.getD prev) b := by
  induction a generalizing prev with
  | nil => simp [charge]
  | cons x a ih =>
    simp only [List.cons_append, charge, ih, List.getLast?_cons]
    cases hl : a.getLast? <;> simp [hl] <;> omega

theorem getLast_default (o : List Word) (ho : o ≠ []) (d d' : Word) :
    o.getLast?.getD d = o.getLast?.getD d' := by
  cases hl : o.getLast? with
  | none => rw [List.getLast?_eq_none_iff] at hl; exact absurd hl ho
  | some x => simp

theorem stepF_ne (S : List (Word × Nat)) (g : List Word) (hS : S ≠ []) : stepF S g ≠ [] := by
  unfold stepF
  split
  · exact hS
  · simp

theorem mem_orders_cons (g : List Word) (gs : List (List Word)) (o : List Word) :
    o ∈ orders (g :: gs) ↔ ∃ t ∈ orders gs, o = g ++ t ∨ o = g.reverse ++ t := by
  simp only [orders, List.mem_flatMap, List.mem_cons, List.not_mem_nil, or_false]

theorem dp (gs : List (List Word)) : ∀ S : List (Word × Nat), S ≠ [] →
    (gs.foldl stepF S) ≠ [] ∧
    (((gs.foldl stepF S).map Prod.snd).min?.getD 0 ∈ cand S gs ∧
     ∀ y ∈ cand S gs, ((gs.foldl stepF S).map Prod.snd).min?.getD 0 ≤ y) := by
  induction gs with
  | nil =>
    intro S hS
    simp only [List.foldl_nil]
    refine ⟨hS, ?_, ?_⟩
    · have := min_getD_mem (S.map Prod.snd) (by simpa using hS)
      simp only [cand, orders, List.mem_flatMap, List.map_cons, List.map_nil, List.mem_singleton, charge]
      obtain ⟨s, hs, hse⟩ := List.mem_map.mp this
      exact ⟨s, hs, by simp [hse]⟩
    · intro y hy
      simp only [cand, orders, List.mem_flatMap, List.map_cons, List.map_nil, List.mem_singleton, charge] at hy
      obtain ⟨s, hs, rfl⟩ := hy
      simpa using min_getD_le (S.map Prod.snd) s.2 (List.mem_map.mpr ⟨s, hs, rfl⟩)
  | cons g gs ih =>
    intro S hS
    simp only [List.foldl_cons]
    by_cases hg : g = []
    · subst hg
      have hstep : stepF S [] = S := by simp [stepF]
      rw [hstep]
      obtain ⟨hne, hm, hle⟩ := ih S hS
      refine ⟨hne, ?_, ?_⟩
      · have : ((gs.foldl stepF S).map Prod.snd).min?.getD 0 ∈ cand S gs := hm
        simp only [cand, mem_orders_cons, List.mem_flatMap, List.mem_map] at this ⊢
        obtain ⟨s, hs, o, ho, hv⟩ := this
        exact ⟨s, hs, o, ⟨o, ho, Or.inl (by simp)⟩, hv⟩
      · intro y hy
        simp only [cand, mem_orders_cons, List.mem_flatMap, List.mem_map] at hy
        obtain ⟨s, hs, o, ⟨t, ht, h⟩, rfl⟩ := hy
        have h' : o = t := by rcases h with h | h <;> simpa using h
        subst h'
        exact hle _ (by
          simp only [cand, List.mem_flatMap, List.mem_map]
          exact ⟨s, hs, o, ht, rfl⟩)
    · have hS' : stepF S g ≠ [] := stepF_ne S g hS
      have hstep : stepF S g = [g, g.reverse].map fun order =>
          (order.getLast?.getD [], (S.map (fun s => s.2 + charge s.1 order)).min?.getD 0) := by
        simp [stepF, hg]
      obtain ⟨hne, hm, hle⟩ := ih (stepF S g) hS'
      refine ⟨hne, ?_, ?_⟩
      · -- membership
        simp only [cand, List.mem_flatMap, List.mem_map] at hm
        obtain ⟨s', hs', o', ho', hv⟩ := hm
        rw [hstep] at hs'
        obtain ⟨order, hord, rfl⟩ := List.mem_map.mp hs'
        dsimp only at hv
        have hordne : order ≠ [] := by
          rcases List.mem_cons.mp hord with rfl | h
          · exact hg
          · have : order = g.reverse := by simpa using h
            subst this; simpa using hg
        have hSne : (S.map (fun s => s.2 + charge s.1 order)) ≠ [] := by simpa using hS
        obtain ⟨s, hs, hse⟩ := List.mem_map.mp (min_getD_mem _ hSne)
        simp only [cand, List.mem_flatMap, List.mem_map]
        refine ⟨s, hs, order ++ o', ?_, ?_⟩
        · rw [mem_orders_cons]
          refine ⟨o', ho', ?_⟩
          rcases List.mem_cons.mp hord with rfl | h
          · exact Or.inl rfl
          · have : order = g.reverse := by simpa using h
            exact Or.inr (by rw [this])
        · rw [charge_append]
          rw [getLast_default order hordne s.1 []]
          have : s.2 + charge s.1 order = (S.map (fun s => s.2 + charge s.1 order)).min?.getD 0 := by
            exact hse
          omega
      · intro y hy
        simp only [cand, List.mem_flatMap, List.mem_map] at hy
        obtain ⟨s, hs, o, ho, rfl⟩ := hy
        obtain ⟨t, ht, hor⟩ := (mem_orders_cons g gs o).mp ho
        have hmem : ∀ order ∈ [g, g.reverse], order ≠ [] ∧ True := by
          intro order hord
          refine ⟨?_, trivial⟩
          rcases List.mem_cons.mp hord with rfl | h
          · exact hg
          · have : order = g.reverse := by simpa using h
            subst this; simpa using hg
        rcases hor with rfl | rfl
        · -- order = g
          have hornd : g ≠ [] := hg
          have key := hle (((S.map (fun s => s.2 + charge s.1 g)).min?.getD 0) +
              charge (g.getLast?.getD []) t) (by
            simp only [cand, List.mem_flatMap, List.mem_map]
            refine ⟨(g.getLast?.getD [], (S.map (fun s => s.2 + charge s.1 g)).min?.getD 0), ?_, t, ht, rfl⟩
            rw [hstep]; simp)
          have hle2 := min_getD_le (S.map (fun s => s.2 + charge s.1 g)) (s.2 + charge s.1 g)
            (List.mem_map.mpr ⟨s, hs, rfl⟩)
          rw [charge_append, getLast_default g hornd s.1 []]
          omega
        · have hornd : g.reverse ≠ [] := by simpa using hg
          have key := hle (((S.map (fun s => s.2 + charge s.1 g.reverse)).min?.getD 0) +
              charge (g.reverse.getLast?.getD []) t) (by
            simp only [cand, List.mem_flatMap, List.mem_map]
            refine ⟨(g.reverse.getLast?.getD [], (S.map (fun s => s.2 + charge s.1 g.reverse)).min?.getD 0), ?_, t, ht, rfl⟩
            rw [hstep]; simp)
          have hle2 := min_getD_le (S.map (fun s => s.2 + charge s.1 g.reverse)) (s.2 + charge s.1 g.reverse)
            (List.mem_map.mpr ⟨s, hs, rfl⟩)
          rw [charge_append, getLast_default g.reverse hornd s.1 []]
          omega

theorem sound : TieOrientation.Goal := by
  intro groups
  obtain ⟨_, hm, hle⟩ := dp groups [([], 0)] (by simp)
  have hopt : optimum groups = ((groups.foldl stepF [([], 0)]).map Prod.snd).min?.getD 0 := rfl
  have hL : cand [([], 0)] groups = (orders groups).map (charge []) := by
    simp [cand]
  rw [hL] at hm hle
  rw [hopt]
  symm
  rw [List.min?_eq_some_iff]
  exact ⟨hm, hle⟩

end TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_TieOrientation
