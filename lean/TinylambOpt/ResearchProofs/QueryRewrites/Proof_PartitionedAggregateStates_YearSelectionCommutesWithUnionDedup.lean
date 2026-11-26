import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_YearSelectionCommutesWithUnionDedup

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
Selection by a retained year distributes through union distinct without
conflating payloads: filtering rows and deduplicating commute because the
selection predicate depends only on the row value. Hence a filter may be applied
before or after the union's DISTINCT.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- Filtering pairwise and flattening equals filtering the flattened union. -/
theorem filter_flatten {α} (channels : List (List α)) (p : α → Bool) :
    channels.flatMap (List.filter p) = (channels.flatten).filter p := by
  induction channels with
  | nil => simp
  | cons c cs ih => simp [List.filter_append, ih]

/-- Two value-only filters commute. -/
theorem filter_filter_comm {α} (xs : List α) (p q : α → Bool) :
    (xs.filter p).filter q = (xs.filter q).filter p := by
  rw [List.filter_filter, List.filter_filter]
  apply congrArg (fun f : α → Bool => xs.filter f)
  funext b
  cases hp : p b <;> cases hq : q b <;> simp [hp, hq]

/-- Deduplication commutes with a value-only selection predicate. -/
theorem eraseDups_filter {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (p : α → Bool) :
    (xs.filter p).eraseDups = xs.eraseDups.filter p := by
  induction hlen : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
      cases xs with
      | nil => simp
      | cons x xs =>
          have hn : n = xs.length + 1 := by simpa using hlen.symm
          have hsmall : (xs.filter (fun b => !b == x)).length < n := by
            have hle := List.length_filter_le (fun b => !b == x) xs
            omega
          by_cases hx : p x = true
          · rw [List.filter_cons_of_pos hx]
            simp only [List.eraseDups_cons]
            rw [List.filter_cons_of_pos hx]
            have ih' := ih ((xs.filter (fun b => !b == x)).length) hsmall
              (xs.filter (fun b => !b == x)) rfl
            rw [← filter_filter_comm xs (fun b => !b == x) p]
            exact congrArg (fun t => x :: t) ih'
          · rw [List.filter_cons_of_neg hx]
            simp only [List.eraseDups_cons]
            rw [List.filter_cons_of_neg hx]
            have hpneq : (xs.filter p).filter (fun b => !b == x) = xs.filter p := by
              apply List.filter_eq_self.mpr
              intro b hb
              have hbp : p b = true := (List.mem_filter.mp hb).2
              by_cases hbx : (b == x) = true
              · have hbeq : b = x := LawfulBEq.eq_of_beq hbx
                rw [hbeq] at hbp
                exact False.elim (hx hbp)
              · simp [hbx]
            have hyseq : (xs.filter (fun b => !b == x)).filter p = xs.filter p := by
              rw [filter_filter_comm]
              exact hpneq
            have ih' := ih ((xs.filter (fun b => !b == x)).length) hsmall
              (xs.filter (fun b => !b == x)) rfl
            rw [hyseq] at ih'
            exact ih'

theorem sound : YearSelectionCommutesWithUnionDedup.Goal := by
  intro channels
  dsimp
  rw [filter_flatten]
  exact eraseDups_filter channels.flatten (fun p : Nat × Int × Int => p.1 == 2001 || p.1 == 2002)
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_YearSelectionCommutesWithUnionDedup
