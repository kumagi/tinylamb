import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q28_DictCodeHostExtractAndHavingBoundGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem filter_sum_eq_count_add {α : Type} [BEq α] [LawfulBEq α]
    (a : α) (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum =
      (l.count a : Int) * (if p a = true then f a else 0) +
        (((l.filter (fun x => !x == a)).filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons b l ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        by_cases hp : p a = true
        · simp [hp, ih]
          rw [Int.add_mul]
          simp [Int.add_assoc, Int.add_comm, Int.add_left_comm]
        · simp [hp, ih]
      · have hbne : b ≠ a := by
          intro h
          subst b
          simp at hba
        have hnb : (!b == a) = true := by simpa using hba
        by_cases hp : p a = true
        · by_cases hpb : p b = true <;>
            simp [hp, hpb, hnb, List.count_cons_of_ne hbne, ih,
              Int.add_assoc, Int.add_comm, Int.add_left_comm]
        · by_cases hpb : p b = true <;>
            simp [hp, hpb, hnb, List.count_cons_of_ne hbne, ih,
              Int.add_assoc, Int.add_comm, Int.add_left_comm]

private theorem weighted_frequency_sum {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (f : α → Int) (p : α → Bool) :
    ((xs.filter p).map f).sum =
      (((xs.eraseDups.map (fun a => (a, xs.count a))).filter (fun r => p r.1)).map
        (fun r => (r.2 : Int) * f r.1)).sum := by
  generalize hn : xs.length = n
  revert xs
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hn
      cases xs with
      | nil => simp
      | cons a tail =>
          let ys := tail.filter (fun x => !x == a)
          have hyslen : ys.length < n := by
            rw [← hn]
            simp [ys]
            have hle := List.length_filter_le (fun x => !x == a) tail
            omega
          have ihys := ih ys.length hyslen ys rfl
          have hmapcnt :
              (ys.eraseDups.map (fun b => (b, tail.count b))) =
                (ys.eraseDups.map (fun b => (b, ys.count b))) := by
            apply List.map_congr_left
            intro b hb
            have hbmem : b ∈ ys := List.mem_eraseDups.mp hb
            have hbneBool : (!b == a) = true := (List.mem_filter.mp hbmem).2
            have hcnt : ys.count b = tail.count b := by
              simpa [ys] using (List.count_filter (l := tail) hbneBool)
            exact congrArg (fun m => (b, m)) hcnt.symm
          have hmapcnt2 :
              (ys.eraseDups.map (fun b => (b, (a :: tail).count b))) =
                (ys.eraseDups.map (fun b => (b, tail.count b))) := by
            apply List.map_congr_left
            intro b hb
            have hbmem : b ∈ ys := List.mem_eraseDups.mp hb
            have hbneBool : (!b == a) = true := (List.mem_filter.mp hbmem).2
            have hbne : b ≠ a := by
              intro h
              subst b
              simp at hbneBool
            exact congrArg (fun m => (b, m)) (List.count_cons_of_ne (a := b) (b := a) hbne.symm)
          have hmain :
              (((((a :: tail).eraseDups.map (fun b => (b, (a :: tail).count b))).filter
                    (fun r => p r.1)).map (fun r => (r.2 : Int) * f r.1)).sum) =
                ((a :: tail).count a : Int) * (if p a = true then f a else 0) +
                  (((ys.eraseDups.map (fun b => (b, (a :: tail).count b))).filter
                    (fun r => p r.1)).map (fun r => (r.2 : Int) * f r.1)).sum := by
            simp [List.eraseDups_cons, ys]
            by_cases hp : p a = true <;> simp [hp]
          rw [hmain, hmapcnt2, hmapcnt, ← ihys]
          rw [List.count_cons_self]
          by_cases hp : p a = true
          · simp [hp, ys, List.filter_filter]
            rw [filter_sum_eq_count_add a tail p f]
            simp [hp, ys, List.filter_filter]
            rw [Int.add_mul]
            simp [Int.add_assoc, Int.add_comm, Int.add_left_comm]
          · simp [hp, ys, List.filter_filter]
            rw [filter_sum_eq_count_add a tail p f]
            simp [hp, ys, List.filter_filter]

theorem sound : Q28_DictCodeHostExtractAndHavingBoundGoal := by
  intro codes hostOf lenOf h
  unfold total frequency
  exact weighted_frequency_sum codes lenOf (fun c => hostOf c == h)

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q28_DictCodeHostExtractAndHavingBoundGoal
