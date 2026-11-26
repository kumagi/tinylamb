import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup

theorem ed_idem : ∀ l : List Nat, l.eraseDups.eraseDups = l.eraseDups
  | [] => rfl
  | a :: l => by
    have hlen : (l.filter (fun b => !b == a)).length < (a :: l).length := by
      have := List.length_filter_le (fun b => !b == a) l
      simp; omega
    have ih := ed_idem (l.filter (fun b => !b == a))
    rw [List.eraseDups_cons]
    rw [List.eraseDups_cons]
    have : ((l.filter (fun b => !b == a)).eraseDups).filter (fun b => !b == a)
        = (l.filter (fun b => !b == a)).eraseDups := by
      apply List.filter_eq_self.mpr
      intro x hx
      rw [List.mem_eraseDups, List.mem_filter] at hx
      exact hx.2
    rw [this, ih]
termination_by l => l.length

theorem ed_filter (q : Nat → Bool) : ∀ l : List Nat, (l.filter q).eraseDups = l.eraseDups.filter q
  | [] => rfl
  | a :: l => by
    have hlen : (l.filter (fun b => !b == a)).length < (a :: l).length := by
      have := List.length_filter_le (fun b => !b == a) l
      simp; omega
    have ih := ed_filter q (l.filter (fun b => !b == a))
    have hcomm : (l.filter (fun b => !b == a)).filter q = (l.filter q).filter (fun b => !b == a) := by
      rw [List.filter_filter, List.filter_filter]
      apply List.filter_congr
      intro x _
      exact Bool.and_comm _ _
    rw [List.eraseDups_cons]
    by_cases hq : q a = true
    · rw [List.filter_cons_of_pos hq, List.filter_cons_of_pos hq, List.eraseDups_cons, ← ih, hcomm]
    · have hq' : q a = false := by simpa using hq
      rw [List.filter_cons_of_neg hq, List.filter_cons_of_neg hq, ← ih]
      have : (l.filter (fun b => !b == a)).filter q = l.filter q := by
        rw [List.filter_filter]
        apply List.filter_congr
        intro x hx
        by_cases hxa : x = a
        · subst hxa; simp [hq']
        · simp [hxa]
      rw [this]
termination_by l => l.length

end TinylambOpt.ResearchProofs.BenchmarkRematch.Dedup
