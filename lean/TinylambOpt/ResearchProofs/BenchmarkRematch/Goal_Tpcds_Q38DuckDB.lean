import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q38DuckDB

private abbrev K := Option String × Option String × Nat

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem nodup_eraseDups : ∀ (l : List K), l.eraseDups.Nodup := by
  have key : ∀ (n : Nat) (l : List K), l.length ≤ n → l.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
      intro l hl
      cases l with
      | nil => exact List.Pairwise.nil
      | cons a t =>
        have h1 : (a :: t).length ≤ 0 := hl
        simp only [List.length_cons] at h1
        omega
    | succ m ih =>
      intro l hl
      cases l with
      | nil => exact List.Pairwise.nil
      | cons a t =>
        rw [List.eraseDups_cons, List.nodup_cons]
        refine ⟨?_, ih (t.filter (fun b => !b == a)) ?_⟩
        · intro hx
          have h2 := (List.mem_filter.mp (List.mem_eraseDups.mp hx)).2
          simp at h2
        · have hlen : (a :: t).length = t.length + 1 := List.length_cons
          have ht : t.length ≤ m := by omega
          exact Nat.le_trans (List.length_filter_le _ _) ht
  intro l
  exact key l.length l (Nat.le_refl _)

private theorem eraseDups_id (l : List K) (h : l.Nodup) : l.eraseDups = l := by
  have key : ∀ (n : Nat) (l : List K), l.length ≤ n → l.Nodup → l.eraseDups = l := by
    intro n
    induction n with
    | zero =>
      intro l hl hn
      cases l with
      | nil => rfl
      | cons a t =>
        have h1 : (a :: t).length ≤ 0 := hl
        simp only [List.length_cons] at h1
        omega
    | succ m ih =>
      intro l hl hn
      cases l with
      | nil => rfl
      | cons a t =>
        obtain ⟨hnin, hNt⟩ := List.nodup_cons.mp hn
        have hlen : (a :: t).length = t.length + 1 := List.length_cons
        have ht : t.length ≤ m := by omega
        rw [List.eraseDups_cons]
        have hfe : t.filter (fun b => !b == a) = t := by
          refine List.filter_eq_self.mpr (fun b hb => ?_)
          cases h : (b == a) with
          | true => exact absurd (beq_iff_eq.mp h) fun he => hnin (he ▸ hb)
          | false => rfl
        rw [hfe, ih t ht hNt]
  exact key l.length l (Nat.le_refl _) h

theorem sound : Q38DuckDB.Goal := by
  intro a b c
  have hX : (a.eraseDups.filter (fun k => b.contains k)).Nodup :=
    List.Pairwise.filter (fun k => b.contains k) (nodup_eraseDups a)
  simp only [members, countWhere]
  rw [eraseDups_id _ hX, List.filter_filter]
  refine congrArg List.length (List.filter_congr (fun k _ => Bool.and_comm _ _))

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q38DuckDB
