import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# SubstringCountFromDictionaryFrequency

Weighting each qualifying URL dictionary entry by its use count preserves the row-level
COUNT. The executor can run the substring predicate once per distinct URL.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SubstringCountFromDictionaryFrequency
open Std
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem filter_length_eq_count_add {α : Type} [BEq α] [LawfulBEq α]
    (a : α) (l : List α) (p : α → Bool) :
    (l.filter p).length = l.count a * (if p a = true then 1 else 0) +
      ((l.filter (fun x => !x == a)).filter p).length := by
  induction l with
  | nil => simp
  | cons b l ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        by_cases hp : p a = true <;> simp [hp, List.count_cons_self, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have hbne : b ≠ a := by
          intro h
          subst b
          simpa using hba
        have hnb : (!b == a) = true := by simpa using hba
        by_cases hp : p a = true
        · by_cases hpb : p b = true <;> simp [hp, hpb, hnb, List.count_cons_of_ne hbne, ih, Nat.add_comm, Nat.add_left_comm]
        · by_cases hpb : p b = true <;> simp [hp, hpb, hnb, List.count_cons_of_ne hbne, ih, Nat.add_assoc, Nat.add_comm]

theorem sum_frequency_filter {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (p : α → Bool) :
    (((xs.eraseDups.map (fun a => (a, xs.count a))).filter (fun r => p r.1)).map Prod.snd).sum =
      (xs.filter p).length := by
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
            exact lt_of_le_of_lt (List.length_filter_le _ tail) (by simp)
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
            exact congrArg (fun n => (b, n)) hcnt.symm
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
              simpa using hbneBool
            exact congrArg (fun n => (b, n)) (List.count_cons_of_ne (a := b) (b := a) hbne.symm)
          have hmain :
              ((((a :: tail).eraseDups.map (fun b => (b, (a :: tail).count b))).filter (fun r => p r.1)).map Prod.snd).sum =
                (if p a = true then (a :: tail).count a else 0) +
                  (((ys.eraseDups.map (fun b => (b, (a :: tail).count b))).filter (fun r => p r.1)).map Prod.snd).sum := by
            simp [List.eraseDups_cons, ys]
            by_cases hp : p a = true <;> simp [hp]
          rw [hmain]
          rw [hmapcnt2, hmapcnt, ihys]
          rw [List.count_cons_self]
          by_cases hp : p a = true
          · simp [hp]
            rw [filter_length_eq_count_add a tail p]
            simp [hp, ys, List.filter_filter, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
          · simp [hp]
            rw [filter_length_eq_count_add a tail p]
            simp [hp, ys, List.filter_filter]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.SubstringCountFromDictionaryFrequency.Goal := by
  intro urls
  change (((urls.eraseDups.map (fun u => (u, urls.count u))).filter (fun r => r.1.contains "google")).map Prod.snd).sum =
    (urls.filter (fun u => u.contains "google")).length
  exact sum_frequency_filter urls (fun u => u.contains "google")
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.SubstringCountFromDictionaryFrequency
