import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Stage-local reclaim callbacks may report overlapping victims; their
success counts add exactly only when the reported id sets are disjoint. For
disjoint duplicate-free lists `eraseDups` is the identity, so the merged victim
byte sum splits into the two per-stage sums. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_DisjointVictimAccounting
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

/-- Filtering by a predicate that holds everywhere is the identity. -/
private theorem filter_id_of_true {α : Type} (p : α → Bool) : ∀ l : List α,
    (∀ x ∈ l, p x = true) → l.filter p = l := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a l ih =>
    intro hall
    rw [List.filter_cons, if_pos (hall a List.mem_cons_self),
      ih (fun x hx => hall x (List.mem_cons_of_mem a hx))]

/-- Deduplication is the identity on a list without duplicates. -/
private theorem eraseDups_id : ∀ l : List Nat, l.Nodup → l.eraseDups = l := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a l ih =>
    intro h
    obtain ⟨hna, hnl⟩ := List.nodup_cons.mp h
    have hf : List.filter (fun b => !b == a) l = l :=
      filter_id_of_true (fun b => !b == a) l (fun b hb => by
        cases hbe : (b == a) with
        | true =>
          have hba : b = a := by simpa [beq_iff_eq] using hbe
          exact absurd (hba ▸ hb) hna
        | false => rfl)
    rw [List.eraseDups_cons, hf]
    exact congrArg (List.cons a) (ih hnl)

/-- Concatenating two duplicate-free lists that share no id stays
duplicate-free. -/
private theorem nodup_app : ∀ l₁ l₂ : List Nat, l₁.Nodup → l₂.Nodup →
    (∀ x ∈ l₁, x ∉ l₂) → (l₁ ++ l₂).Nodup := by
  intro l₁
  induction l₁ with
  | nil => intro l₂ _ h2 _; exact h2
  | cons a l ih =>
    intro l₂ h1 h2 hd
    rw [List.cons_append]
    obtain ⟨hna, hnl⟩ := List.nodup_cons.mp h1
    refine List.nodup_cons.mpr ⟨fun hm => ?_,
      ih l₂ hnl h2 (fun x hx hy => hd x (List.mem_cons_of_mem a hx) hy)⟩
    rcases List.mem_append.mp hm with hl | h2
    · exact hna hl
    · exact hd a List.mem_cons_self h2

/-- Merging two disjoint duplicate-free victim lists with `eraseDups` keeps
every report exactly once, so the byte sums add. -/
theorem sound : DisjointVictimAccounting.Goal := by
  intro a b bytes ha hb hdisj
  have hn : (a ++ b).Nodup := nodup_app a b ha hb hdisj
  rw [eraseDups_id (a ++ b) hn, List.map_append, List.sum_append]

end TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_DisjointVictimAccounting
