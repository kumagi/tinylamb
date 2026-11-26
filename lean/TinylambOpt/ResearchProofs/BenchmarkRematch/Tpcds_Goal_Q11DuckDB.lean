import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q11DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem eraseDups_map_inj {α β : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β) (hf : Function.Injective f) :
    ∀ (n : Nat) (l : List α), l.length ≤ n → (l.map f).eraseDups = l.eraseDups.map f := by
  intro n
  induction n with
  | zero =>
      intro l hl
      have h0 : l.length = 0 := by omega
      cases l with
      | nil => simp
      | cons a l => simp at h0
  | succ n ih =>
      intro l hl
      cases l with
      | nil => simp
      | cons a l =>
          simp at hl
          have hfilt : (l.map f).filter (fun b => ! b == f a) = (l.filter (fun b => ! b == a)).map f := by
            induction l with
            | nil => simp
            | cons x xs ih2 =>
                have hxsle : xs.length ≤ n := by
                  have hc : (x :: xs).length = xs.length + 1 := rfl
                  omega
                by_cases hxa : x == a
                · have hfxa : (f x == f a) = true := by simp [beq_iff_eq.mp hxa]
                  simp [hxa, hfxa] at ih2 ⊢
                  exact ih2 hxsle
                · have hneq : f x ≠ f a := by
                    intro h
                    exact hxa (beq_iff_eq.mpr (hf h))
                  have hfxa : (f x == f a) = false := by simpa [beq_iff_eq] using hneq
                  simp [hxa, hfxa] at ih2 ⊢
                  exact ih2 hxsle
          simp only [List.map_cons, List.eraseDups_cons]
          rw [hfilt]
          have hlen : (l.filter (fun b => ! b == a)).length ≤ n := by
            have hle := List.length_filter_le (fun b => ! b == a) l
            omega
          exact congrArg (fun xs => f a :: xs) (ih (l.filter (fun b => ! b == a)) hlen)

theorem filter_wide : ∀ (sales : List (Nat × Option Int)) (attr : Nat → String) (k : Nat),
  ((sales.map (fun (k,v) => ((k, attr k), v))).filter (fun r => r.1 == (k, attr k))).map Prod.snd =
    (sales.filter (fun r => r.1 == k)).map Prod.snd := by
  intro sales
  induction sales with
  | nil => simp
  | cons r sales ih =>
      intro attr k
      rcases r with ⟨k',v⟩
      by_cases h : k' == k
      · have heq : k' = k := beq_iff_eq.mp h
        subst k'
        simp at ih ⊢
        exact ih attr k
      · have hkne : k' ≠ k := by simpa [beq_iff_eq] using h
        have hp : ((k', attr k') == (k, attr k)) = false := by
          simp [hkne]
        simp [h, hp, ih attr k]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q11DuckDB.Goal := by
  intro sales attr
  dsimp only
  let f : Nat → Nat × String := fun k => (k, attr k)
  have hf : Function.Injective f := by
    intro a b h
    exact congrArg Prod.fst h
  have hwk : ((sales.map (fun (k,v) => ((k, attr k), v))).map Prod.fst).eraseDups =
      (sales.map Prod.fst).eraseDups.map f := by
    simpa [List.map_map, Function.comp_def] using
      (eraseDups_map_inj f hf (sales.map Prod.fst).length (sales.map Prod.fst) (by omega))
  rw [hwk]
  simp only [List.map_map]
  apply List.map_congr_left
  intro k hk
  dsimp [Function.comp, f]
  rw [filter_wide sales attr k]
