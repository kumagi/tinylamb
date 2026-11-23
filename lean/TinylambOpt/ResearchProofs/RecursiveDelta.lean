import TinylambOpt.ResearchGoals.RecursiveDelta

namespace TinylambOpt.ResearchProofs.RecursiveDelta

open TinylambOpt.ResearchGoals.RecursiveDelta
open List

/-! ### List helpers -/

/-- A member of a list can be exposed as a cons cell between two appended halves. -/
private theorem exists_append_of_mem {α : Type} {l : List α} {b : α} (h : b ∈ l) :
    ∃ l₁ l₂ : List α, l = l₁ ++ b :: l₂ := by
  induction l with
  | nil => exact absurd h List.not_mem_nil
  | cons a as ih =>
      cases List.mem_cons.mp h with
      | inl heq => exact ⟨[], as, by rw [List.nil_append, ← heq]⟩
      | inr h' =>
          obtain ⟨l₁, l₂, hl⟩ := ih h'
          exact ⟨a :: l₁, l₂, by rw [List.cons_append, hl]⟩

/-- `eraseDups` always produces a duplicate-free list. -/
private theorem nodup_eraseDups : ∀ (l : List Nat), l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List Nat), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            have hlen : (as.filter fun b => !b == a).length < n := by
              rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length hlen _ rfl)
            have hb' := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
            rw [Nat.not_beq_eq_true_eq] at hb'
            exact fun heq => hb' heq.symm
  exact key l.length l rfl

/-- Moving a cons cell across an append. -/
private theorem perm_cons_move {α : Type} (b : α) (l₁ l₂ : List α) :
    l₁ ++ b :: l₂ ~ b :: (l₁ ++ l₂) := by
  induction l₁ with
  | nil => rfl
  | cons a as ih =>
      calc a :: as ++ b :: l₂
          ~ a :: b :: (as ++ l₂) := List.Perm.cons a ih
        _ ~ b :: a :: (as ++ l₂) := List.Perm.swap b a (as ++ l₂)
        _ ~ b :: (a :: as ++ l₂) := List.Perm.cons b (List.Perm.refl _)

/-- A duplicate-free list whose members all occur in `l'` embeds as a sublist of
a permutation of `l'`. -/
private theorem exists_sublist_perm {α : Type} [DecidableEq α] :
    ∀ (l' : List α) (l : List α),
      l.Nodup → (∀ x ∈ l, x ∈ l') → ∃ s, Sublist s l' ∧ s ~ l := by
  intro l'
  induction l' with
  | nil =>
      intro l _ h
      have hl0 : l = [] := by
        cases l with
        | nil => rfl
        | cons a as => exact absurd (h a (List.mem_cons_self (a := a))) List.not_mem_nil
      refine ⟨[], List.Sublist.slnil, ?_⟩
      rw [hl0]
  | cons b bs ih =>
      intro l hl h
      by_cases hb : b ∈ l
      · obtain ⟨t₁, t₂, rfl⟩ := exists_append_of_mem hb
        obtain ⟨hn1, hn2, hdisj⟩ := List.nodup_append.mp hl
        obtain ⟨hbt, hn2'⟩ := List.nodup_cons.mp hn2
        have hn12 : (t₁ ++ t₂).Nodup :=
          List.nodup_append.mpr
            ⟨hn1, hn2', fun x hx₁ y hy => hdisj x hx₁ y (List.mem_cons_of_mem b hy)⟩
        have claim : ∀ x ∈ t₁ ++ t₂, x ∈ bs := by
          intro x hx
          have hxb : x ∈ t₁ ++ b :: t₂ := by
            cases List.mem_append.mp hx with
            | inl hx₁ => exact List.mem_append_left (b :: t₂) hx₁
            | inr hx₂ => exact List.mem_append_right t₁ (List.mem_cons_of_mem b hx₂)
          cases List.mem_cons.mp (h x hxb) with
          | inl heq =>
              cases List.mem_append.mp hx with
              | inl hx₁ => exact absurd heq (hdisj x hx₁ b List.mem_cons_self)
              | inr hx₂ =>
                  have : b ∈ t₂ := by rw [← heq]; exact hx₂
                  exact absurd this hbt
          | inr hbs => exact hbs
        obtain ⟨s, hs, hp⟩ := ih (t₁ ++ t₂) hn12 claim
        refine ⟨b :: s, List.Sublist.cons_cons b hs, ?_⟩
        calc b :: s
            ~ b :: (t₁ ++ t₂) := List.Perm.cons _ hp
          _ ~ t₁ ++ b :: t₂ := (perm_cons_move b t₁ t₂).symm
      · obtain ⟨s, hs, hp⟩ :=
          ih l hl (by
            intro x hx
            cases List.mem_cons.mp (h x hx) with
            | inl heq => subst heq; exact absurd hx hb
            | inr hbs => exact hbs)
        exact ⟨s, List.Sublist.cons b hs, hp⟩

/-- Pigeonhole: a duplicate-free list all of whose members occur in `l'` is no
longer than `l'`. -/
private theorem nodup_length_le {α : Type} [DecidableEq α] {l l' : List α} (hl : l.Nodup)
    (h : ∀ x ∈ l, x ∈ l') : l.length ≤ l'.length := by
  obtain ⟨s, hs, hp⟩ := exists_sublist_perm l' l hl h
  have h1 : s.length ≤ l'.length := List.Sublist.length_le hs
  have h2 : l.length = s.length := hp.symm.length_eq
  omega

/-! ### Round and walk mechanics -/

private abbrev AdjMap := Std.HashMap Nat (List Nat)

private theorem mem_round_frontier {adj : AdjMap} {s : State} {v : Nat}
    (h : v ∈ (round adj s).frontier) :
    v ∈ s.frontier.flatMap (adj.getD · []) ∧ v ∉ s.seen := by
  have h1 :
      v ∈ ((s.frontier.flatMap (adj.getD · [])).filter (fun v => !s.seen.contains v)).eraseDups := h
  have h2 := List.mem_filter.mp (List.mem_eraseDups.mp h1)
  refine ⟨h2.1, ?_⟩
  intro hmem
  have hn : ¬s.seen.contains v = true := by simpa using h2.2
  exact hn (List.contains_iff_mem.mpr hmem)

private theorem round_seen_eq (adj : AdjMap) (s : State) :
    (round adj s).seen = s.seen ++ (round adj s).frontier := rfl

private theorem round_expanded_eq (adj : AdjMap) (s : State) :
    (round adj s).expanded = s.expanded ++ s.frontier := rfl

private theorem round_frontier_nodup (adj : AdjMap) (s : State) :
    (round adj s).frontier.Nodup := nodup_eraseDups _

private theorem round_nil_frontier {adj : AdjMap} {s : State} (h : s.frontier = []) :
    (round adj s).frontier = [] := by
  show ((s.frontier.flatMap (adj.getD · [])).filter (fun v => !s.seen.contains v)).eraseDups = []
  rw [h]
  simp

private theorem round_id {adj : AdjMap} {s : State} (h : s.frontier = []) : round adj s = s := by
  cases s with
  | mk seen frontier expanded =>
      have h' : frontier = [] := h
      show (⟨seen ++ ((frontier.flatMap (adj.getD · [])).filter (fun v => !seen.contains v)).eraseDups,
             ((frontier.flatMap (adj.getD · [])).filter (fun v => !seen.contains v)).eraseDups,
             expanded ++ frontier⟩ : State) = ⟨seen, frontier, expanded⟩
      rw [h']
      simp

private def sn (adj : AdjMap) (roots : List Nat) : Nat → State
  | 0 => ⟨roots, roots, []⟩
  | k + 1 => round adj (sn adj roots k)

private theorem sn_zero {adj : AdjMap} {roots : List Nat} :
    sn adj roots 0 = ⟨roots, roots, []⟩ := rfl

private theorem sn_succ {adj : AdjMap} {roots : List Nat} (k : Nat) :
    sn adj roots (k + 1) = round adj (sn adj roots k) := rfl

private theorem sn_stable {adj : AdjMap} {roots : List Nat} (i : Nat)
    (h : (sn adj roots i).frontier = []) : ∀ k : Nat, sn adj roots (i + k) = sn adj roots i := by
  intro k
  induction k with
  | zero => rw [Nat.add_zero]
  | succ k ih => rw [Nat.add_succ, sn_succ, ih]; exact round_id h

private theorem walk_sn {adj : AdjMap} {roots : List Nat} :
    ∀ (i n : Nat), walk adj n (sn adj roots i) = sn adj roots (i + n) := by
  intro i n
  induction n generalizing i with
  | zero => rw [Nat.add_zero]; rfl
  | succ n ih =>
      show (if (sn adj roots i).frontier.isEmpty then sn adj roots i
        else walk adj n (round adj (sn adj roots i))) = sn adj roots (i + (n + 1))
      split
      next hf =>
        have hf' : (sn adj roots i).frontier = [] := List.isEmpty_iff.mp hf
        exact (sn_stable i hf' (n + 1)).symm
      next _ =>
        rw [← sn_succ, ih (i + 1)]
        apply congrArg
        omega

/-! ### The adjacency index -/

private theorem index_fold_getD_mem (edges : List Edge) (m : AdjMap) (u v : Nat) :
    v ∈ (edges.foldl (fun m p => m.insert p.1 (p.2 :: m.getD p.1 [])) m).getD u [] ↔
      v ∈ m.getD u [] ∨ (u, v) ∈ edges := by
  induction edges generalizing m with
  | nil => simp
  | cons e es ih =>
      cases e with
      | mk x y =>
          rw [List.foldl_cons, ih, Std.HashMap.getD_insert]
          by_cases hx : (x == u) = true
          · have h : x = u := by simpa using hx
            subst h
            simp
            simp [or_assoc, or_comm, or_left_comm]
          · have hxu : x ≠ u := by
              intro hh
              exact hx (by simp [hh])
            simp [hxu]
            by_cases hux : u = x
            · subst hux
              exact absurd rfl hxu
            · simp [hux]

private theorem index_getD_mem (edges : List Edge) (u v : Nat) :
    v ∈ (index edges).getD u [] ↔ (u, v) ∈ edges := by
  show v ∈
    (edges.foldl (fun (m : AdjMap) (p : Edge) => m.insert p.1 (p.2 :: m.getD p.1 [])) {}).getD u [] ↔
    (u, v) ∈ edges
  rw [index_fold_getD_mem]
  simp

/-! ### The state sequence of `execute` -/

private abbrev adjE (edges : List Edge) : AdjMap := index edges
private def rootsE (seeds : List Nat) : List Nat := seeds.eraseDups
private def S (edges : List Edge) (seeds : List Nat) (k : Nat) : State :=
  sn (adjE edges) (rootsE seeds) k

private theorem S0 (edges : List Edge) (seeds : List Nat) :
    S edges seeds 0 = ⟨rootsE seeds, rootsE seeds, []⟩ := rfl

private theorem SS (edges : List Edge) (seeds : List Nat) (k : Nat) :
    S edges seeds (k + 1) = round (adjE edges) (S edges seeds k) := rfl

private theorem execute_eq (edges : List Edge) (seeds : List Nat) :
    execute edges seeds = S edges seeds (edges.length + 1) := by
  show walk (index edges) (edges.length + 1) ⟨seeds.eraseDups, seeds.eraseDups, []⟩ =
    sn (index edges) (seeds.eraseDups) (edges.length + 1)
  have h0 : (⟨seeds.eraseDups, seeds.eraseDups, []⟩ : State) =
    sn (index edges) (seeds.eraseDups) 0 := rfl
  rw [h0, walk_sn, Nat.zero_add]

private theorem S_struct (edges : List Edge) (seeds : List Nat) : ∀ k : Nat,
    (S edges seeds k).seen = (S edges seeds k).expanded ++ (S edges seeds k).frontier ∧
      (S edges seeds k).seen.Nodup := by
  intro k
  induction k with
  | zero =>
      rw [S0]
      exact ⟨by simp, nodup_eraseDups _⟩
  | succ k ih =>
      obtain ⟨heq, hn⟩ := ih
      rw [SS]
      refine ⟨?_, ?_⟩
      · rw [round_seen_eq, round_expanded_eq, heq]
      · rw [round_seen_eq, List.nodup_append]
        refine ⟨hn, round_frontier_nodup _ _, ?_⟩
        intro a ha₁ b hb₂
        intro h
        subst h
        exact (mem_round_frontier hb₂).2 ha₁

private theorem S_frontier_subset_seen (edges : List Edge) (seeds : List Nat) (k : Nat) (v : Nat)
    (h : v ∈ (S edges seeds k).frontier) : v ∈ (S edges seeds k).seen := by
  have heq := (S_struct edges seeds k).1
  rw [heq]
  exact List.mem_append_right _ h

private theorem S_seen_succ_subset (edges : List Edge) (seeds : List Nat) (k : Nat) :
    (S edges seeds k).seen ⊆ (S edges seeds (k + 1)).seen := by
  intro v hv
  rw [SS, round_seen_eq]
  exact List.mem_append_left _ hv

private theorem S_seen_mono {edges : List Edge} {seeds : List Nat} :
    ∀ {m n : Nat}, m ≤ n → (S edges seeds m).seen ⊆ (S edges seeds n).seen := by
  intro m n hmn
  obtain ⟨j, rfl⟩ : ∃ j, n = m + j := ⟨n - m, by omega⟩
  induction j with
  | zero => rw [Nat.add_zero]; exact fun _ hv => hv
  | succ j ih =>
      intro v hv
      rw [Nat.add_succ]
      exact S_seen_succ_subset edges seeds (m + j) (ih (Nat.le_add_right m j) hv)

private theorem S_frontier_fresh (edges : List Edge) (seeds : List Nat) (k : Nat) (v : Nat)
    (h : v ∈ (S edges seeds (k + 1)).frontier) : v ∉ (S edges seeds k).seen :=
  (mem_round_frontier h).2

private theorem mem_flatMap_adjE (edges : List Edge) (seeds : List Nat) (k : Nat) (w : Nat) :
    w ∈ (S edges seeds k).frontier.flatMap (fun v => (adjE edges).getD v []) ↔
      ∃ u, u ∈ (S edges seeds k).frontier ∧ (u, w) ∈ edges := by
  rw [List.mem_flatMap]
  constructor
  · rintro ⟨u, hu, hw⟩
    exact ⟨u, hu, (index_getD_mem edges u w).mp hw⟩
  · rintro ⟨u, hu, hwe⟩
    exact ⟨u, hu, (index_getD_mem edges u w).mpr hwe⟩

private theorem S_expanded_closed (edges : List Edge) (seeds : List Nat) :
    ∀ (k : Nat) {u w : Nat}, u ∈ (S edges seeds k).expanded → (u, w) ∈ edges →
      w ∈ (S edges seeds k).seen := by
  intro k
  induction k with
  | zero =>
      intro u w hu _
      rw [S0] at hu
      exact absurd hu List.not_mem_nil
  | succ k ih =>
      intro u w hu hme
      rw [SS, round_expanded_eq] at hu
      rw [SS, round_seen_eq]
      cases List.mem_append.mp hu with
      | inl hu₁ => exact List.mem_append_left _ (ih hu₁ hme)
      | inr hu₂ =>
          have hfm : w ∈ (S edges seeds k).frontier.flatMap (fun v => (adjE edges).getD v []) :=
            (mem_flatMap_adjE edges seeds k w).mpr ⟨u, hu₂, hme⟩
          by_cases hsw : w ∈ (S edges seeds k).seen
          · exact List.mem_append_left _ hsw
          · have hpw : !((S edges seeds k).seen.contains w) = true := by
              cases h : (S edges seeds k).seen.contains w with
              | true => exact absurd (List.contains_iff_mem.mp h) hsw
              | false => rfl
            have hf : w ∈ ((S edges seeds k).frontier.flatMap (fun v => (adjE edges).getD v [])).filter (fun v => !((S edges seeds k).seen.contains v)) :=
              List.mem_filter.mpr ⟨hfm, hpw⟩
            exact List.mem_append_right _ (List.mem_eraseDups.mpr hf)

/-! ### Termination via distinct-edge witnesses -/

/-- `Es = [e₁, …, eₘ]` with each `eⱼ ∈ edges` landing (via its second component)
in the frontier of round `j`. -/
private inductive Chain (edges : List Edge) (seeds : List Nat) : List Edge → Prop where
  | base : Chain edges seeds []
  | snoc {Es : List Edge} {e : Edge} (h : Chain edges seeds Es) (he : e ∈ edges)
      (h2 : e.2 ∈ (S edges seeds (Es.length + 1)).frontier) : Chain edges seeds (Es ++ [e])

private theorem Chain.nodup_of (edges : List Edge) (seeds : List Nat) :
    ∀ {Es : List Edge}, Chain edges seeds Es →
      Es.Nodup ∧ ∀ p ∈ Es, p ∈ edges ∧ ∃ r, r ≤ Es.length ∧ p.2 ∈ (S edges seeds r).frontier := by
  intro Es h
  induction h with
  | base => exact ⟨List.Pairwise.nil, fun p hp => absurd hp List.not_mem_nil⟩
  | @snoc Es e h he h2 ih =>
      obtain ⟨hn, hm⟩ := ih
      refine ⟨?_, ?_⟩
      · rw [List.nodup_append]
        refine ⟨hn, ?_, ?_⟩
        · exact List.Pairwise.cons (fun b hb => absurd hb List.not_mem_nil) List.Pairwise.nil
        · intro p hp₁ b hb hpeq
          have hpe : p = e := hpeq.trans (List.mem_singleton.mp hb)
          obtain ⟨r, hrle, hrf⟩ := (hm p hp₁).2
          have hseen : p.2 ∈ (S edges seeds Es.length).seen :=
            S_seen_mono hrle (S_frontier_subset_seen edges seeds r p.2 hrf)
          have hfresh : e.2 ∉ (S edges seeds Es.length).seen :=
            S_frontier_fresh edges seeds Es.length e.2 h2
          rw [hpe] at hseen
          exact absurd hseen hfresh
      · intro p hp
        cases List.mem_append.mp hp with
        | inl hp₁ =>
            obtain ⟨hpe, r, hrle, hrf⟩ := hm p hp₁
            have hlen : (Es ++ [e]).length = Es.length + 1 := by simp [List.length_append]
            rw [hlen]
            exact ⟨hpe, r, Nat.le_trans hrle (Nat.le_add_right Es.length 1), hrf⟩
        | inr hp₂ =>
            have hpe := List.mem_singleton.mp hp₂
            subst hpe
            have hlen : (Es ++ [p]).length = Es.length + 1 := by simp [List.length_append]
            rw [hlen]
            exact ⟨he, Es.length + 1, Nat.le_refl _, h2⟩

private theorem chain_exists (edges : List Edge) (seeds : List Nat) :
    ∀ k : Nat, (S edges seeds (k + 1)).frontier ≠ [] →
      ∃ Es : List Edge, Chain edges seeds Es ∧ Es.length = k + 1 := by
  intro k
  induction k with
  | zero =>
      intro hne
      cases hf : (S edges seeds 1).frontier with
      | nil => exact absurd hf hne
      | cons a as =>
          have hw : a ∈ (S edges seeds 1).frontier := by rw [hf]; exact List.mem_cons_self
          obtain ⟨u, hwe⟩ : ∃ u, (u, a) ∈ edges := by
            obtain ⟨v', _, hv'⟩ :=
              (mem_flatMap_adjE edges seeds 0 a).mp (mem_round_frontier hw).1
            exact ⟨v', hv'⟩
          exact ⟨[(u, a)], Chain.snoc (Es := []) (e := (u, a)) Chain.base hwe hw, by simp⟩
  | succ k ih =>
      intro hne
      have back : (S edges seeds (k + 1)).frontier ≠ [] := by
        intro hz
        have h2 : (S edges seeds (k + 1 + 1)).frontier = [] := by rw [SS, round_nil_frontier hz]
        exact absurd h2 hne
      obtain ⟨Es, hch, hlen⟩ := ih back
      cases hf : (S edges seeds (k + 1 + 1)).frontier with
      | nil => exact absurd hf hne
      | cons a as =>
          have hw : a ∈ (S edges seeds (k + 1 + 1)).frontier :=
            by rw [hf]; exact List.mem_cons_self
          obtain ⟨u, hwe⟩ : ∃ u, (u, a) ∈ edges := by
            obtain ⟨v', _, hv'⟩ :=
              (mem_flatMap_adjE edges seeds (k + 1) a).mp (mem_round_frontier hw).1
            exact ⟨v', hv'⟩
          refine ⟨Es ++ [(u, a)], Chain.snoc (Es := Es) (e := (u, a)) hch hwe ?_, ?_⟩
          · show a ∈ (S edges seeds (Es.length + 1)).frontier
            rw [hlen]
            exact hw
          · simp [hlen]

private theorem S_frontier_nil (edges : List Edge) (seeds : List Nat) :
    (S edges seeds (edges.length + 1)).frontier = [] := by
  by_cases h : (S edges seeds (edges.length + 1)).frontier = []
  · exact h
  · obtain ⟨Es, hch, hlen⟩ := chain_exists edges seeds edges.length h
    obtain ⟨hn, hm⟩ := Chain.nodup_of edges seeds hch
    have hle : Es.length ≤ edges.length :=
      nodup_length_le hn (fun p hp => (hm p hp).1)
    rw [hlen] at hle
    omega

/-! ### `seen` is exactly the reachable set -/

private theorem reach_of_mem_S_seen (edges : List Edge) (seeds : List Nat) :
    ∀ (k : Nat) {v : Nat}, v ∈ (S edges seeds k).seen → Reach edges seeds v := by
  intro k
  induction k with
  | zero =>
      intro v hv
      rw [S0] at hv
      exact Reach.seed (List.mem_eraseDups.mp hv)
  | succ k ih =>
      intro v hv
      rw [SS, round_seen_eq] at hv
      cases List.mem_append.mp hv with
      | inl hv₁ => exact ih hv₁
      | inr hv₂ =>
          obtain ⟨hu2, _⟩ := mem_round_frontier hv₂
          obtain ⟨u, hu, hwe⟩ := (mem_flatMap_adjE edges seeds k v).mp hu2
          exact Reach.step (ih (S_frontier_subset_seen edges seeds k u hu)) hwe

private theorem mem_S_seen_of_reach (edges : List Edge) (seeds : List Nat) (v : Nat)
    (hv : Reach edges seeds v) : v ∈ (S edges seeds (edges.length + 1)).seen := by
  induction hv with
  | seed hs =>
      exact S_seen_mono (Nat.zero_le _)
        (by rw [S0]; exact List.mem_eraseDups.mpr hs)
  | step huu hmem ih =>
      have hst := (S_struct edges seeds (edges.length + 1)).1
      rw [hst, S_frontier_nil edges seeds, List.append_nil] at ih
      exact S_expanded_closed edges seeds (edges.length + 1) ih hmem

theorem sound : TinylambOpt.ResearchGoals.RecursiveDelta.Goal := by
  intro edges seeds
  show (∀ v, v ∈ (execute edges seeds).seen ↔ Reach edges seeds v) ∧
      (execute edges seeds).seen.Nodup ∧ (execute edges seeds).frontier = [] ∧
      (execute edges seeds).expanded.Perm (execute edges seeds).seen
  rw [execute_eq edges seeds]
  refine ⟨?_, ?_, ?_, ?_⟩
  · intro v
    exact ⟨fun h => reach_of_mem_S_seen edges seeds _ h, fun h => mem_S_seen_of_reach edges seeds v h⟩
  · exact (S_struct edges seeds _).2
  · exact S_frontier_nil edges seeds
  · have heq := (S_struct edges seeds (edges.length + 1)).1
    rw [S_frontier_nil edges seeds, List.append_nil] at heq
    rw [heq]

end TinylambOpt.ResearchProofs.RecursiveDelta
