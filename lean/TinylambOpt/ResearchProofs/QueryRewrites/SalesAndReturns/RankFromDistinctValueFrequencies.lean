import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# RankFromDistinctValueFrequencies

Descending SQL RANK is one plus the number of rows with a strictly greater value, which
equals the sum of their frequencies.

A window operator can process distinct values with counts while preserving shared ranks
and gaps after ties.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RankFromDistinctValueFrequencies
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-- Splitting a filter on a disjunction counts the positive branch plus the
part of the other branch that avoids it. -/
theorem len_or {α : Type} (l : List α) (q r : α → Bool) :
    (l.filter (fun b => q b || r b)).length =
      (l.filter q).length + (l.filter (fun b => !q b && r b)).length := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    cases hq : q a <;> cases hr : r a <;> simp [hq, hr, ih] <;> try omega

/-- Summing the frequency of the distinct elements satisfying `p` counts
exactly the elements of the list satisfying `p`. -/
theorem freq_sum {α : Type} [BEq α] [LawfulBEq α] (p : α → Bool) :
    ∀ (xs : List α),
      (((xs.eraseDups.map (fun x => (x, xs.count x))).filter
            (fun (q : α × Nat) => p q.1)).map Prod.snd).sum = (xs.filter p).length := by
  have key : ∀ (n : Nat) (xs : List α), xs.length ≤ n →
      (((xs.eraseDups.map (fun x => (x, xs.count x))).filter
            (fun (q : α × Nat) => p q.1)).map Prod.snd).sum = (xs.filter p).length := by
    intro n
    induction n with
    | zero =>
      intro xs hx
      cases xs with
      | nil => rfl
      | cons a as => exact absurd (Nat.eq_zero_of_le_zero hx) (by simp)
    | succ n ih =>
      intro xs hx
      cases xs with
      | nil => rfl
      | cons a as =>
        have hlen : as.length ≤ n := by
          have h1 : as.length + 1 ≤ n + 1 := by simpa [List.length_cons] using hx
          exact Nat.le_of_add_le_add_right h1
        let f : α → Bool := fun (b : α) => !(b == a)
        have hfdef : ∀ b, f b = !(b == a) := fun _ => rfl
        have htail := ih (as.filter f) (Nat.le_trans (List.length_filter_le f as) hlen)
        have hcnt : ∀ x ∈ (as.filter f).eraseDups,
            (a :: as).count x = (as.filter f).count x := by
          intro x hx
          obtain ⟨hxm, hxf⟩ := List.mem_filter.mp (List.mem_eraseDups.mp hx)
          have hna : (x == a) = false := by
            cases e : x == a with
            | true =>
              have h2 : f x = false := by rw [hfdef, e]; rfl
              exact absurd (h2 ▸ hxf) (by decide)
            | false => rfl
          have hc1 : (a :: as).count x = as.count x := by
            refine List.count_cons_of_ne (fun he => ?_)
            have htrue : (x == x) = true := BEq.rfl
            have hx2 : (x == x) = false := he ▸ hna
            rw [htrue] at hx2
            exact absurd hx2 (by decide)
          have hc2 : as.count x = (as.filter f).count x := by
            rw [List.count_eq_length_filter, List.count_eq_length_filter, List.filter_filter]
            refine congrArg List.length (List.filter_congr (fun b _ => ?_))
            by_cases hb : (b == x)
            · have beq : b = x := eq_of_beq hb
              rw [hb, beq, hfdef, hna]
              simp
            · have hb' : (b == x) = false := by
                cases e : (b == x) with
                | true => exact absurd e hb
                | false => rfl
              rw [hb']
              simp
          rw [hc1, hc2]
        rw [List.eraseDups_cons, List.map_cons]
        have hmap : List.map (fun x => (x, (a :: as).count x)) ((as.filter f).eraseDups) =
            List.map (fun x => (x, (as.filter f).count x)) ((as.filter f).eraseDups) :=
          List.map_congr_left (fun x hx => congrArg (fun n => (x, n)) (hcnt x hx))
        rw [hmap, List.filter_cons]
        have hcond : (a, (a :: as).count a).1 = a := rfl
        rw [hcond]
        have htf : ((as.filter f).filter p).length =
            (as.filter (fun b => !(b == a) && p b)).length := by
          rw [List.filter_filter]
          refine congrArg List.length (List.filter_congr (fun b _ => ?_))
          rw [hfdef, Bool.and_comm]
        by_cases hpa : p a
        · simp only [hpa, ite_true, List.map_cons, List.sum_cons]
          rw [htail, List.count_cons_self]
          have hrh : ((a :: as).filter p).length = (as.filter p).length + 1 := by simp [hpa]
          have hp2 : (as.filter p).length =
              (as.filter (fun b => !(b == a) && p b)).length +
                (as.filter (fun b => b == a)).length := by
            calc (as.filter p).length
                = (as.filter (fun b => (!(b == a) && p b) || (b == a))).length :=
                    congrArg List.length (List.filter_congr (fun b _ => by
                      by_cases hb : (b == a)
                      · have : b = a := eq_of_beq hb
                        rw [this, show (a == a) = true from BEq.rfl, hpa]
                        simp
                      · simp [hb]))
              _ = (as.filter (fun b => !(b == a) && p b)).length +
                    (as.filter (fun b => !(!(b == a) && p b) && (b == a))).length :=
                    len_or _ _ _
              _ = (as.filter (fun b => !(b == a) && p b)).length +
                    (as.filter (fun b => b == a)).length := by
                    congr 1
                    refine congrArg List.length (List.filter_congr (fun b _ => ?_))
                    by_cases hb : (b == a)
                    · have beq : b = a := eq_of_beq hb
                      rw [beq, show (a == a) = true from BEq.rfl, hpa]
                      simp
                    · simp [hb]
          have hc3 : (as.filter (fun b => b == a)).length = as.count a :=
            List.count_eq_length_filter.symm
          rw [hrh, htf, hp2, hc3]
          omega
        · have hnf : p a = false := by
            cases e : p a with
            | true => exact absurd e hpa
            | false => rfl
          simp only [hnf, Bool.false_eq_true, ite_false]
          rw [htail]
          have hrh : ((a :: as).filter p).length = (as.filter p).length := by simp [hnf]
          rw [hrh, htf]
          refine congrArg List.length (List.filter_congr (fun b _ => ?_))
          by_cases hb : (b == a)
          · have : b = a := eq_of_beq hb
            rw [this]
            simp [hnf]
          · simp [hb]
  intro xs
  exact key xs.length xs (Nat.le_refl _)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.RankFromDistinctValueFrequencies.Goal := by
  intro partition value
  simp only [countWhere]
  exact congrArg (fun n : Nat => 1 + n) (freq_sum (fun x : Int => value < x) partition)
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RankFromDistinctValueFrequencies
