import TinylambOpt.ResearchGoals.ClickstreamLayout.Execution

namespace TinylambOpt.ResearchProofs.Batch5.ClickstreamLayout.Execution.Goal_DefaultGroups

open TinylambOpt.ResearchGoals.ClickstreamLayout.Execution


/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Proved

- `DefaultGroups` — Compute the exact GROUP BY multiplicity of any projected tuple from
  exception row IDs, including the multiplicity of the all-default group.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Scratch.ClickstreamLayout.DefaultGroups

open TinylambOpt.ResearchGoals.ClickstreamLayout.Execution
open DefaultGroups

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  cases l with
  | nil => simp
  | cons a t =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        rw [List.mem_eraseDups, List.mem_filter] at ha
        simpa [beq_iff_eq] using ha.2
      · exact nodup_eraseDups (t.filter fun b => ! b == a)
termination_by l.length
decreasing_by
  have hle := List.length_filter_le (fun b : α => ! b == a) t
  have hd : (a :: t).length = t.length + 1 := rfl
  omega

private theorem mem_touched {lanes : List Lane} {i : Nat} (hi : i ∈ touched lanes) :
    ∃ l ∈ lanes, i ∈ l.2.map Prod.fst := by
  unfold touched at hi
  rw [List.mem_eraseDups] at hi
  simpa only [List.mem_flatMap] using hi

private theorem lookup_none (es : List (Nat × Val)) (i : Nat) (hi : i ∉ es.map Prod.fst) :
    es.lookup i = none := by
  rw [List.lookup_eq_none_iff]
  intro p hp
  have hpk : p.1 ∈ es.map Prod.fst := by
    rw [List.mem_map]
    exact ⟨p, hp, rfl⟩
  have hne : i ≠ p.1 := by
    intro heq
    apply hi
    simpa [heq] using hpk
  simp [beq_iff_eq, hne]

private theorem default_at (lanes : List Lane) (i : Nat) (hi : i ∉ touched lanes) :
    tupleAt lanes i = lanes.map Prod.fst := by
  unfold tupleAt
  apply List.map_congr_left
  intro l hl
  rcases l with ⟨d, es⟩
  have hnot : i ∉ es.map Prod.fst := by
    intro him
    apply hi
    unfold touched
    rw [List.mem_eraseDups]
    exact List.mem_flatMap.mpr ⟨(d, es), hl, him⟩
  change (es.lookup i).getD d = d
  rw [lookup_none es i hnot]
  simp

private theorem count_partition {α : Type} [BEq α] [LawfulBEq α]
    (L : List Nat) (f : Nat → α) (s : List Nat) (k : α) :
    (L.map f).count k =
      ((L.filter (fun i => i ∈ s)).map f).count k +
        ((L.filter (fun i => i ∉ s)).map f).count k := by
  induction L with
  | nil => simp
  | cons i L ih =>
      by_cases hi : i ∈ s
      · simp only [List.map_cons, List.count_cons]
        simp [hi] at ih ⊢
        simp only [List.count_cons]
        omega
      · simp only [List.map_cons, List.count_cons]
        simp [hi] at ih ⊢
        simp only [List.count_cons]
        omega

private theorem filter_mem_perm (s : List Nat) (hs : s.Nodup) : ∀ n,
    (∀ i ∈ s, i < n) → ((List.range n).filter (fun i => i ∈ s)).Perm s := by
  intro n
  induction n generalizing s with
  | zero =>
      intro hlt
      have hsnil : s = [] := by
        apply List.eq_nil_of_length_eq_zero
        induction s with
        | nil => rfl
        | cons a t iht =>
            have ha : a < 0 := hlt a (by simp)
            omega
      subst s
      simp
  | succ n ih =>
      intro hlt
      by_cases hn : n ∈ s
      · let s' := s.erase n
        have hs' : s'.Nodup := hs.erase n
        have hnnot : n ∉ s' := by
          have hnd : (n :: s.erase n).Nodup := (List.perm_cons_erase hn).nodup hs
          dsimp [s']
          exact (List.nodup_cons.mp hnd).1
        have hlt' : ∀ i ∈ s', i < n := by
          intro i hi
          have his : i ∈ s := by
            dsimp [s'] at hi
            exact List.mem_of_mem_erase hi
          have hine : i ≠ n := by
            intro heq
            subst heq
            exact hnnot hi
          have h := hlt i his
          omega
        have hperm := ih s' hs' hlt'
        have hfilter : (List.range n).filter (fun i => i ∈ s) =
            (List.range n).filter (fun i => i ∈ s') := by
          apply List.filter_congr
          intro i hi
          simp only [List.mem_range] at hi
          by_cases his : i ∈ s
          · have hine : i ≠ n := by omega
            have his' : i ∈ s' := by
              dsimp [s']
              exact (List.mem_erase_of_ne hine).mpr his
            simp [his, his']
          · have his' : i ∉ s' := by
              intro hm
              apply his
              dsimp [s'] at hm
              exact List.mem_of_mem_erase hm
            simp [his, his']
        have hfilterSucc : (List.range (n + 1)).filter (fun i => i ∈ s) =
            (List.range n).filter (fun i => i ∈ s) ++ [n] := by
          rw [show n + 1 = n.succ by rfl, List.range_succ, List.filter_append]
          simp [hn]
        rw [hfilterSucc, hfilter]
        exact (List.Perm.append_right [n] hperm).trans
          ((List.perm_append_singleton n s').trans (List.perm_cons_erase hn).symm)
      · have hlt' : ∀ i ∈ s, i < n := by
          intro i hi
          have h := hlt i hi
          have hine : i ≠ n := by
            intro heq
            subst heq
            exact hn hi
          omega
        have hperm := ih s hs hlt'
        have hfilterSucc : (List.range (n + 1)).filter (fun i => i ∈ s) =
            (List.range n).filter (fun i => i ∈ s) := by
          rw [show n + 1 = n.succ by rfl, List.range_succ, List.filter_append]
          simp [hn]
        rw [hfilterSucc]
        exact hperm

private theorem length_filter_not_mem (L s : List Nat) :
    (L.filter (fun i => i ∈ s)).length + (L.filter (fun i => i ∉ s)).length = L.length := by
  induction L with
  | nil => simp
  | cons a L ih =>
      by_cases h : a ∈ s
      · simp [h] at ih ⊢
        omega
      · simp [h] at ih ⊢
        omega

private theorem count_const {α β : Type} [BEq β] [LawfulBEq β]
    (L : List α) (g : α → β) (d k : β) (h : ∀ i ∈ L, g i = d) :
    (L.map g).count k = if k == d then L.length else 0 := by
  induction L with
  | nil => simp
  | cons a L ih =>
      have ha := h a (by simp)
      have hrest : ∀ i ∈ L, g i = d := by
        intro i hi
        exact h i (by simp [hi])
      rw [List.map_cons, List.count_cons, ih hrest, ha]
      by_cases hk : (k == d) = true
      · have hkd : k = d := (beq_iff_eq.mp hk)
        simp [hk, beq_iff_eq, hkd]
      · have hk' : (k == d) = false := by simpa using hk
        have hkd : k ≠ d := (beq_eq_false_iff_ne.mp hk')
        have hdk : d ≠ k := fun h' => hkd h'.symm
        simp [hk, hk', beq_iff_eq, hdk]

theorem sound : TinylambOpt.ResearchGoals.ClickstreamLayout.Execution.DefaultGroups.Goal := by
  intro lanes n key hsmall
  dsimp only
  let ids := touched lanes
  let f := tupleAt lanes
  let dflt := lanes.map Prod.fst
  have hnodup : ids.Nodup := by
    dsimp [ids, touched]
    exact nodup_eraseDups _
  have hlt : ∀ i ∈ ids, i < n := by
    intro i hi
    obtain ⟨l, hl, him⟩ := mem_touched hi
    obtain ⟨d, es⟩ := l
    obtain ⟨e, hes, heq⟩ := List.mem_map.mp him
    have hb := hsmall (d, es) hl e hes
    rw [heq] at hb
    exact hb
  have hperm := filter_mem_perm ids hnodup n hlt
  have hcount1 : (((List.range n).filter (fun i => i ∈ ids)).map f).count key =
      (ids.map f).count key := by
    exact (hperm.map f).count_eq key
  have hlencomp : ((List.range n).filter (fun i => i ∉ ids)).length = n - ids.length := by
    have hlen1 := hperm.length_eq
    have hlen2 := length_filter_not_mem (List.range n) ids
    simp at hlen2
    rw [hlen1] at hlen2
    have hpred : (List.filter (fun i => decide ¬ i ∈ ids) (List.range n)) =
        (List.filter (fun i => !decide (i ∈ ids)) (List.range n)) := by
      apply List.filter_congr
      intro i hi
      by_cases hii : i ∈ ids <;> simp [hii]
    rw [hpred]
    omega
  have hcount2 : (((List.range n).filter (fun i => i ∉ ids)).map f).count key =
      (if key == dflt then n - ids.length else 0) := by
    have hall : ∀ i ∈ (List.range n).filter (fun i => i ∉ ids), f i = dflt := by
      intro i hi
      have hinot_bool := (List.mem_filter.mp hi).2
      have hinot : i ∉ ids := of_decide_eq_true hinot_bool
      exact default_at lanes i hinot
    rw [count_const _ f dflt key hall, hlencomp]
  have hpart := count_partition (List.range n) f ids key
  rw [hpart, hcount1, hcount2]

end Scratch.ClickstreamLayout.DefaultGroups
end TinylambOpt.ResearchProofs.Batch5.ClickstreamLayout.Execution.Goal_DefaultGroups
