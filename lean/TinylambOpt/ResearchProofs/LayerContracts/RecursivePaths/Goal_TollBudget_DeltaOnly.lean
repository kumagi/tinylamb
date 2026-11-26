import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_DeltaOnly

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

def stepm (m : Model) (g : List Edge) (p q : Node) : Prop :=
  ∃ e ∈ g, e.src = p.1 ∧ m.next p.2 e.label = some q.2 ∧ e.dst = q.1

theorem mem_expand {m : Model} {g : List Edge} {wave : List Node} {p : Node} :
    p ∈ expand m g wave ↔ ∃ a, a ∈ wave ∧ stepm m g a p := by
  constructor
  · intro h
    simp [expand] at h
    rcases h with ⟨a,b,hab,e,he,hes,c,hnext,hpair⟩
    cases p with
    | mk v q =>
        simp at hpair
        rcases hpair with ⟨rfl, rfl⟩
        exact ⟨(a,b), hab, e, he, hes, hnext, rfl⟩
  · rintro ⟨a, ha, e, he, hes, hnext, hdst⟩
    simp [expand, stepm]
    refine ⟨a.1, a.2, ha, e, he, hes, p.2, ?_, ?_⟩
    · simpa using hnext
    · cases p
      simp at hdst ⊢
      exact hdst

theorem slow_succ_mem {m : Model} {g : List Edge} {v n : Nat} {p : Node} :
    p ∈ slow m g (v,0) (n+1) ↔ p ∈ slow m g (v,0) n ∨ ∃ a, a ∈ slow m g (v,0) n ∧ stepm m g a p := by
  simp [slow, mem_expand]

theorem foldl_insertNew_mem {l s : List Node} {p : Node} :
    p ∈ l.foldl (insertNew id) s ↔ p ∈ s ∨ p ∈ l := by
  induction l generalizing s with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons]
      rw [ih]
      have hstep : ∀ (q : Node), q ∈ insertNew id s x ↔ q ∈ s ∨ q = x := by
        intro q
        by_cases hx : x ∈ s <;> by_cases hqx : q = x <;> simp [insertNew, hx, hqx]
      rw [hstep]
      simp only [List.mem_cons]
      constructor
      · intro h
        rcases h with h | h
        · rcases h with h | h
          · exact Or.inl h
          · exact Or.inr (Or.inl h)
        · exact Or.inr (Or.inr h)
      · intro h
        rcases h with h | h
        · exact Or.inl (Or.inl h)
        · rcases h with h | h
          · exact Or.inl (Or.inr h)
          · exact Or.inr h

theorem insertNew_nodup {s : List Node} (hs : s.Nodup) (x : Node) : (insertNew id s x).Nodup := by
  by_cases hx : x ∈ s
  · simpa [insertNew, hx] using hs
  · have hn : (s ++ [x]).Nodup := by
      rw [List.nodup_append]
      refine ⟨hs, by simp, ?_⟩
      intro a ha b hb
      simp at hb
      subst b
      intro heq
      apply hx
      simpa [heq] using ha
    simpa [insertNew, hx] using hn

theorem foldl_insertNew_nodup {l s : List Node} (hs : s.Nodup) : (l.foldl (insertNew id) s).Nodup := by
  induction l generalizing s with
  | nil => simpa using hs
  | cons x xs ih =>
      rw [List.foldl_cons]
      exact ih (insertNew_nodup hs x)

theorem foldl_insertNew_eq_append (l : List Node) (s : List Node) :
    ∃ t : List Node, l.foldl (insertNew id) s = s ++ t ∧ (∀ p, p ∈ t → p ∈ l) := by
  induction l generalizing s with
  | nil =>
      refine ⟨[], by simp, ?_⟩
      simp
  | cons x xs ih =>
      by_cases hx : x ∈ s
      · have hacc : insertNew id s x = s := by simp [insertNew, hx]
        rw [List.foldl_cons, hacc]
        obtain ⟨t, ht, hmem⟩ := ih s
        refine ⟨t, ht, ?_⟩
        intro p hp
        exact List.mem_cons_of_mem x (hmem p hp)
      · have hacc : insertNew id s x = s ++ [x] := by simp [insertNew, hx]
        rw [List.foldl_cons, hacc]
        obtain ⟨t, ht, hmem⟩ := ih (s ++ [x])
        refine ⟨[x] ++ t, ?_, ?_⟩
        · simp [ht, List.append_assoc]
        · intro p hp
          simp only [List.mem_append, List.mem_singleton] at hp
          rcases hp with rfl | hp
          · simp
          · exact List.mem_cons_of_mem x (hmem p hp)

theorem mem_of_mem_drop {l : List Node} {n : Nat} {p : Node} (h : p ∈ l.drop n) : p ∈ l := by
  induction n generalizing l with
  | zero => simpa using h
  | succ n ih =>
      cases l with
      | nil => simp at h
      | cons a as => exact List.mem_cons_of_mem a (ih h)

theorem slow_stable {m : Model} {g : List Edge} {v k : Nat}
    (hstab : ∀ p, p ∈ slow m g (v,0) (k+1) ↔ p ∈ slow m g (v,0) k) :
    ∀ r p, p ∈ slow m g (v,0) (k+1+r) ↔ p ∈ slow m g (v,0) k := by
  intro r
  induction r with
  | zero => intro p; simpa using hstab p
  | succ r ih =>
      intro p
      rw [show k + 1 + (r + 1) = (k + 1 + r) + 1 by omega]
      rw [slow_succ_mem]
      constructor
      · intro h
        rcases h with hp | ⟨a, ha, hstep⟩
        · exact (ih p).mp hp
        · have ha' : a ∈ slow m g (v,0) k := (ih a).mp ha
          have hp' : p ∈ slow m g (v,0) (k+1) := by
            rw [slow_succ_mem]
            exact Or.inr ⟨a, ha', hstep⟩
          exact (hstab p).mp hp'
      · intro hp
        exact Or.inl ((ih p).mpr hp)

theorem walk_mem_eq_slow_aux (m : Model) (g : List Edge) (v : Nat) :
    ∀ n k (seen wave : List Node),
      seen.Nodup →
      (∀ p, p ∈ seen ↔ p ∈ slow m g (v,0) k) →
      (∀ p, p ∈ wave → p ∈ seen) →
      (∀ p q, p ∈ seen → stepm m g p q → q ∈ seen ∨ q ∈ expand m g wave) →
      ∀ p, p ∈ walk m g id n seen wave ↔ p ∈ slow m g (v,0) (k+n) := by
  intro n
  induction n with
  | zero =>
      intro k seen wave hnd hmem hwav hcl p
      simp [walk]
      exact hmem p
  | succ n ih =>
      intro k seen wave hnd hmem hwav hcl p
      simp only [walk]
      let allSeen := (expand m g wave).foldl (insertNew id) seen
      have hall_mem : ∀ p, p ∈ allSeen ↔ p ∈ seen ∨ p ∈ expand m g wave := by
        intro p
        exact foldl_insertNew_mem
      have hall_nodup : allSeen.Nodup := foldl_insertNew_nodup hnd
      have hexp_sub : ∀ p, p ∈ expand m g (slow m g (v,0) k) → p ∈ seen ∨ p ∈ expand m g wave := by
        intro p hp
        rw [mem_expand] at hp
        rcases hp with ⟨a, ha, hstep⟩
        have ha' : a ∈ seen := (hmem a).mpr ha
        exact hcl a p ha' hstep
      have hall_eq : ∀ p, p ∈ allSeen ↔ p ∈ slow m g (v,0) (k+1) := by
        intro p
        rw [hall_mem, slow_succ_mem]
        constructor
        · intro h
          rcases h with h | h
          · exact Or.inl ((hmem p).mp h)
          · rw [mem_expand] at h
            rcases h with ⟨a, ha, hstep⟩
            exact Or.inr ⟨a, (hmem a).mp (hwav a ha), hstep⟩
        · intro h
          rcases h with h | ⟨a, ha, hstep⟩
          · exact Or.inl ((hmem p).mpr h)
          · have ha' : a ∈ seen := (hmem a).mpr ha
            exact hcl a p ha' hstep
      obtain ⟨t, ht, htmem⟩ := foldl_insertNew_eq_append (expand m g wave) seen
      have hnew : allSeen.drop seen.length = t := by simp [allSeen, ht]
      by_cases hstop : allSeen.length == seen.length
      · have htlen : t.length = 0 := by
          have hh : seen.length + t.length = seen.length := by simpa [allSeen, ht] using hstop
          omega
        have htempty : t = [] := by
          cases t with
          | nil => rfl
          | cons a as => simp at htlen
        have hall_seen : allSeen = seen := by simp [allSeen, ht, htempty]
        have hstab : ∀ p, p ∈ slow m g (v,0) (k+1) ↔ p ∈ slow m g (v,0) k := by
          intro p
          rw [← hall_eq p, hall_seen]
          exact hmem p
        rw [ite_eq_left hstop]
        rw [show k + (n + 1) = k + 1 + n by omega]
        exact (hmem p).trans (slow_stable hstab n p).symm
      · rw [ite_eq_right hstop]
        have hwav2 : ∀ p, p ∈ allSeen.drop seen.length → p ∈ allSeen := by
          intro p hp
          exact mem_of_mem_drop hp
        have hcl2 : ∀ p q, p ∈ allSeen → stepm m g p q →
            q ∈ allSeen ∨ q ∈ expand m g (allSeen.drop seen.length) := by
          intro p q hp hstep
          have hp' : p ∈ seen ∨ p ∈ expand m g wave := (hall_mem p).mp hp
          rcases hp' with hpseen | hpexp
          · have hq := hcl p q hpseen hstep
            rcases hq with hq | hq
            · exact Or.inl ((hall_mem q).mpr (Or.inl hq))
            · exact Or.inl ((hall_mem q).mpr (Or.inr hq))
          · have hpall : p ∈ allSeen := (hall_mem p).mpr (Or.inr hpexp)
            have hpt : p ∈ seen ∨ p ∈ t := by simpa [allSeen, ht] using hpall
            rcases hpt with hpseen | hpt
            · have hq := hcl p q hpseen hstep
              rcases hq with hq | hq
              · exact Or.inl ((hall_mem q).mpr (Or.inl hq))
              · exact Or.inl ((hall_mem q).mpr (Or.inr hq))
            · right
              rw [hnew]
              exact mem_expand.mpr ⟨p, hpt, hstep⟩
        have hh := ih (k+1) allSeen (allSeen.drop seen.length) hall_nodup hall_eq hwav2 hcl2 p
        rw [show (k+1)+n = k+(n+1) by omega] at hh
        exact hh

theorem walk_id_mem_slow (m : Model) (g : List Edge) (v n : Nat) (p : Node) :
    p ∈ walk m g id n [(v,0)] [(v,0)] ↔ p ∈ slow m g (v,0) n := by
  have hs0 : ∀ q : Node, q ∈ [(v,0)] ↔ q ∈ slow m g (v,0) 0 := by
    intro q
    simp [slow]
  have hw0 : ∀ q : Node, q ∈ [(v,0)] → q ∈ [(v,0)] := by
    intro q h
    exact h
  have hcl0 : ∀ p q : Node, p ∈ [(v,0)] → stepm m g p q →
      q ∈ [(v,0)] ∨ q ∈ expand m g [(v,0)] := by
    intro p q hp hstep
    simp at hp
    subst p
    right
    exact mem_expand.mpr ⟨(v,0), by simp, hstep⟩
  simpa using (walk_mem_eq_slow_aux m g v n 0 [(v,0)] [(v,0)] (by simp) hs0 hw0 hcl0 p)

theorem delta_of_slow (m : Model) (g : List Edge) (v n x : Nat) :
    x ∈ query m g v n id ↔ ∃ q, (x,q) ∈ slow m g (v,0) n ∧ m.accept q := by
  simp only [query, List.mem_map, List.mem_filter]
  constructor
  · rintro ⟨⟨a,b⟩, ⟨hp, hacc⟩, hfst⟩
    simp at hfst
    subst x
    exact ⟨b, (walk_id_mem_slow m g v n (a,b)).mp hp, hacc⟩
  · rintro ⟨q, hs, hacc⟩
    refine ⟨(x,q), ⟨?_, hacc⟩, rfl⟩
    exact (walk_id_mem_slow m g v n (x,q)).mpr hs

theorem sound : TollBudget.DeltaOnly.Goal := by
  intro g v n x
  exact delta_of_slow TollBudget.model g v n x

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_TollBudget_DeltaOnly
