import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q37ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

def removeAll {α : Type} [BEq α] (a : α) (xs : List α) : List α :=
  xs.filter (fun x => !x == a)

theorem count_removeAll_ne {α : Type} [BEq α] [LawfulBEq α] (a x : α) (h : x ≠ a) (xs : List α) :
    (removeAll a xs).count x = xs.count x := by
  unfold removeAll
  rw [List.count_filter]
  simp [beq_iff_eq, h]

theorem count_removeAll_self {α : Type} [BEq α] [LawfulBEq α] (a : α) (xs : List α) :
    (removeAll a xs).count a = 0 := by
  unfold removeAll
  rw [List.count_eq_zero]
  simp [beq_iff_eq]

theorem filter_split {α : Type} [BEq α] [LawfulBEq α] (a : α) (p : α → Bool) :
    ∀ xs : List α,
      (xs.filter p).length = (if p a then xs.count a else 0) + ((removeAll a xs).filter p).length := by
  intro xs
  induction xs with
  | nil => simp [removeAll]
  | cons y ys ih =>
      by_cases hya : y == a
      · have hy : y = a := by simpa [beq_iff_eq] using hya
        subst y
        have hc : (a :: ys).count a = ys.count a + 1 := by simp [List.count_cons]
        have hrm : removeAll a (a :: ys) = removeAll a ys := by simp [removeAll]
        rw [hrm, hc]
        by_cases hpa : p a = true <;> simp [hpa, ih] <;> omega
      · have hyaf : (y == a) = false := by simpa using hya
        have hc : (y :: ys).count a = ys.count a := by
          simp [List.count_cons, hyaf]
        have hrm : removeAll a (y :: ys) = y :: removeAll a ys := by simp [removeAll, hyaf]
        rw [hrm, hc]
        by_cases hpa : p a = true <;> by_cases hpy : p y = true <;> simp [hpa, hpy, ih] <;> omega

theorem frequency_sum_filter_len {α : Type} [BEq α] [LawfulBEq α] :
    ∀ n (xs : List α), xs.length = n → ∀ p : α → Bool,
      let hist := TinylambOpt.ResearchGoals.BenchmarkRematch.frequency xs
      (((hist.filter (fun r => p r.1)).map Prod.snd).sum = (xs.filter p).length) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hlen p
      cases xs with
      | nil => simp [TinylambOpt.ResearchGoals.BenchmarkRematch.frequency]
      | cons a xs =>
          let ys := removeAll a xs
          have hyslen : ys.length < n := by
            rw [← hlen]
            dsimp [ys, removeAll]
            have hle := List.length_filter_le (fun x => !x == a) xs
            simp
            omega
          have ihys := ih ys.length hyslen ys rfl p
          have hfreq : TinylambOpt.ResearchGoals.BenchmarkRematch.frequency (a :: xs) =
              (a, (a :: xs).count a) :: ys.eraseDups.map (fun x => (x, ys.count x)) := by
            simp [TinylambOpt.ResearchGoals.BenchmarkRematch.frequency, List.eraseDups_cons, ys, removeAll]
            intro x hx hxne
            have hax : a ≠ x := by intro h; exact hxne h.symm
            have hc1 : (a :: xs).count x = xs.count x := by
              simp [List.count_cons, beq_iff_eq, hax]
            have hc2 : (xs.filter (fun b => !b == a)).count x = xs.count x :=
              count_removeAll_ne a x hxne xs
            simp [hc1, hc2]
          have hcounta : (a :: xs).count a = xs.count a + 1 := by simp [List.count_cons]
          have ihys' : (((ys.eraseDups.map (fun x => (x, ys.count x))).filter (fun r => p r.1)).map Prod.snd).sum =
              (ys.filter p).length := by
            simpa [TinylambOpt.ResearchGoals.BenchmarkRematch.frequency] using ihys
          dsimp [ys] at ihys'
          rw [hfreq]
          by_cases hpa : p a = true
          · simp [hpa, hcounta]
            rw [ihys']
            have hsp := filter_split a p xs
            simp [hpa] at hsp
            omega
          · simp [hpa]
            rw [ihys']
            have hsp := filter_split a p xs
            simp [hpa] at hsp
            omega

theorem frequency_sum_filter (xs : List Nat) (p : Nat → Bool) :
    let hist := TinylambOpt.ResearchGoals.BenchmarkRematch.frequency xs
    (((hist.filter (fun r => p r.1)).map Prod.snd).sum = (xs.filter p).length) :=
  frequency_sum_filter_len xs.length xs rfl p

theorem filter_map_eq (xs : List Nat) (f : Nat → String) (key : String) :
    (xs.filter (fun c => f c == key)).length = ((xs.map f).filter (fun s => s == key)).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : f x == key <;> simp [h, ih]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q37ClickHouse.Goal := by
  intro pages key
  dsimp only
  induction pages with
  | nil => simp [TinylambOpt.ResearchGoals.BenchmarkRematch.countWhere]
  | cons page pages ih =>
      rcases page with ⟨rs, dict⟩
      have hpage := frequency_sum_filter ((rs.filter Prod.fst).map Prod.snd) (fun c => dict c == key)
      have hfilter := filter_map_eq ((rs.filter Prod.fst).map Prod.snd) dict key
      simp [TinylambOpt.ResearchGoals.BenchmarkRematch.countWhere, hpage, hfilter, ih] <;>
        simpa [Function.comp_def]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q37ClickHouse
