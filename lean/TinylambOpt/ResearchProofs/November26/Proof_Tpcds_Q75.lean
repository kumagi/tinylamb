import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q75
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] :
    ∀ xs : List α, xs.eraseDups.Nodup := by
  have key : ∀ (n : Nat) (xs : List α), xs.length ≤ n → xs.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
      intro xs hx
      cases xs with
      | nil => simp
      | cons a as => exact absurd (Nat.eq_zero_of_le_zero hx) (by simp)
    | succ n ih =>
      intro xs hx
      cases xs with
      | nil => simp
      | cons a as =>
        have hlen : (as.filter (fun b => !(b == a))).length ≤ n := by
          have hAs : as.length ≤ n := by
            simpa [List.length_cons] using (Nat.succ_le_succ_iff.mp hx)
          exact Nat.le_trans (List.length_filter_le (fun b => !(b == a)) as) hAs
        have htail := ih (as.filter (fun b => !(b == a))) hlen
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro ha
          have hf : a ∈ as.filter (fun b => !(b == a)) := List.mem_eraseDups.mp ha
          simpa using (List.mem_filter.mp hf).2
        · exact htail
  intro xs
  exact key xs.length xs (Nat.le_refl _)

theorem perm_sum {l₁ l₂ : List Int} (h : List.Perm l₁ l₂) : l₁.sum = l₂.sum := by
  exact h.foldr_eq' (by intros; simp [Int.add_left_comm]) 0

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q75.Goal := by
  intro channels k
  dsimp only
  let p : Nat × Int × Int → Bool := fun r => r.1 == k
  let A := ((channels.flatMap List.eraseDups).eraseDups.filter p)
  let B := (channels.flatten.eraseDups.filter p)
  have hnodA : A.Nodup := (nodup_eraseDups (channels.flatMap List.eraseDups)).filter p
  have hnodB : B.Nodup := (nodup_eraseDups channels.flatten).filter p
  have hmem : ∀ r, r ∈ A ↔ r ∈ B := by
    intro r
    simp [A, B, List.mem_eraseDups, List.mem_filter, p]
  have hperm : List.Perm A B := (List.perm_ext_iff_of_nodup hnodA hnodB).2 hmem
  have hsum : (A.map (fun r => r.2.1)).sum = (B.map (fun r => r.2.1)).sum :=
    perm_sum (hperm.map (fun r => r.2.1))
  simpa [A, B, total, p] using hsum
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q75
