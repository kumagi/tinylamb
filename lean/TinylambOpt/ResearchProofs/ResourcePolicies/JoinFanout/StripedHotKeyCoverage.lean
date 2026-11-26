import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout_StripedHotKeyCoverage
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

private theorem perm_cons_append {α : Type} : ∀ (x : α) (l : List α) (t : List α),
    (x :: l ++ t).Perm (l ++ x :: t) := by
  intro x l
  induction l with
  | nil => intro t; exact List.Perm.refl _
  | cons y m ih =>
      intro t
      rw [List.cons_append]
      exact (List.Perm.swap' y x (List.Perm.refl (m ++ t))).trans (List.Perm.cons y (ih t))

private theorem perm_append_comm {α : Type} : ∀ (l₁ l₂ : List α), (l₁ ++ l₂).Perm (l₂ ++ l₁) := by
  intro l₁
  induction l₁ with
  | nil => intro l₂; rw [List.nil_append, List.append_nil]
  | cons x t ih =>
      intro l₂
      rw [List.cons_append]
      exact (List.Perm.cons x (ih l₂)).trans (perm_cons_append x l₂ t)

private theorem perm_swap4 {α : Type} (a b c d : List α) :
    ((a ++ b) ++ (c ++ d)).Perm ((a ++ c) ++ (b ++ d)) := by
  rw [List.append_assoc a b (c ++ d), List.append_assoc a c (b ++ d)]
  refine List.Perm.append_left a ?_
  rw [← List.append_assoc b c d]
  exact ((List.Perm.append_right d (perm_append_comm b c)).trans
    (List.Perm.of_eq (List.append_assoc c b d)))

private theorem flatMap_dist_perm {α β : Type} : ∀ (l : List α) (f g : α → List β),
    (l.flatMap fun s => f s ++ g s).Perm (l.flatMap f ++ l.flatMap g) := by
  intro l
  induction l with
  | nil => intro f g; simp [List.flatMap]
  | cons z t ih =>
      intro f g
      rw [List.flatMap_cons, List.flatMap_cons, List.flatMap_cons]
      exact (List.Perm.append_left (f z ++ g z) (ih f g)).trans
        (perm_swap4 (f z) (g z) (List.flatMap f t) (List.flatMap g t))

private theorem flatMap_absent {α : Type} : ∀ (l : List Nat) (i : Nat) (t : List α),
    i ∉ l → l.flatMap (fun s => if s == i then t else []) = [] := by
  intro l
  induction l with
  | nil => intro i t h; simp [List.flatMap]
  | cons z r ih =>
      intro i t h
      rw [List.flatMap_cons]
      have hz : ¬ (z = i) := fun heq => h (by subst heq; simp)
      rcases Decidable.em (z = i) with hzeq | hzeq
      · exact absurd hzeq hz
      · have hb : ¬ ((z == i) = true) := by
          intro hh
          exact hzeq (by simpa using hh)
        have hr : i ∉ r := fun hh => h (List.mem_cons.mpr (Or.inr hh))
        rw [if_neg hb, ih i t hr]
        simp

private theorem stripe_pick {α : Type} : ∀ (l : List Nat) (i : Nat) (t : List α),
    i ∈ l → l.Nodup → l.flatMap (fun s => if s == i then t else []) = t := by
  intro l
  induction l with
  | nil => intro i t hi; exact absurd hi List.not_mem_nil
  | cons z r ih =>
      intro i t hi hn
      obtain ⟨hz, hr⟩ := (List.nodup_cons).mp hn
      rw [List.flatMap_cons]
      rcases Decidable.em (z = i) with hzeq | hzeq
      · subst hzeq
        have hb : ((z == z) = true) := by simp
        rw [if_pos hb]
        rw [flatMap_absent r z t hz]
        simp
      · have hb : ¬ ((z == i) = true) := by
          intro hh
          exact hzeq (by simpa using hh)
        rw [if_neg hb]
        have hin : i ∈ r := by
          rcases (List.mem_cons).mp hi with heq | h'
          · exact absurd heq.symm hzeq
          · exact h'
        exact ih i t hin hr

/--
Stripes assigned by build-row ordinal cover every duplicate pair exactly once:
concatenating the per-stripe sub-joins permutes the unsharded pair list.
-/
theorem sound : StripedHotKeyCoverage.Goal := by
  intro rows
  induction rows with
  | zero =>
      intro probes stripes h
      rw [List.range_zero]
      simp [List.flatMap]
  | succ n ih =>
      intro probes stripes h
      rw [List.range_succ]
      simp only [List.filter_append, List.flatMap_append, List.flatMap_cons, List.flatMap_nil,
        List.append_nil]
      refine ((flatMap_dist_perm (List.range stripes)
        (fun s => ((List.range n).filter (fun i => i % stripes == s)).flatMap (fun i =>
          (List.range probes).map fun j => (i, j)))
        (fun s => ([n].filter (fun i => i % stripes == s)).flatMap (fun i =>
          (List.range probes).map fun j => (i, j)))).trans) ?_
      refine List.Perm.append (ih probes stripes h) ?_
      have hf : (fun s => ([n].filter (fun i => i % stripes == s)).flatMap (fun i =>
          (List.range probes).map fun j => (i, j))) =
          (fun s => if s == n % stripes then (List.range probes).map fun j => (n, j) else []) := by
        funext s
        cases h2 : n % stripes == s with
        | true =>
            have hflt : ([n].filter fun i => i % stripes == s) = [n] := by
              simp [List.filter_cons, List.filter_nil, h2]
            have hif : (s == n % stripes) = true := by rw [Bool.beq_comm]; exact h2
            rw [hflt, hif]
            simp [List.flatMap_cons, List.flatMap_nil]
        | false =>
            have hflt : ([n].filter fun i => i % stripes == s) = [] := by
              simp [List.filter_cons, List.filter_nil, h2]
            have hif : (s == n % stripes) = false := by rw [Bool.beq_comm]; exact h2
            rw [hflt, hif]
            simp [List.flatMap]
      rw [hf]
      rw [stripe_pick (List.range stripes) (n % stripes)
        ((List.range probes).map fun j => (n, j))
        (List.mem_range.mpr (Nat.mod_lt n h)) List.nodup_range]

end TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout_StripedHotKeyCoverage
