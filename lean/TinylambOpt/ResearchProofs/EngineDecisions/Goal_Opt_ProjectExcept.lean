import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `ProjectExcept` — Exact local condition for pushing a non-injective projection below
  EXCEPT ALL. Different rows in one fiber must not cancel opposite-sign debts.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_ProjectExcept

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.ProjectExcept

theorem sub_add_sub_le (a b c d : Nat) : (a+c)-(b+d) ≤ (a-b)+(c-d) := by omega

theorem diff_ge_sub (f : Fiber) :
    (f.map Prod.fst).sum - (f.map Prod.snd).sum ≤ (f.map (fun p => p.1 - p.2)).sum := by
  induction f with
  | nil => simp
  | cons p f ih =>
    rcases p with ⟨l,r⟩
    simp only [List.map_cons, List.sum_cons]
    have h := sub_add_sub_le l r (f.map Prod.fst).sum (f.map Prod.snd).sum
    omega

theorem sum_fst_le_sum_snd (f : Fiber) (h : ∀ p ∈ f, p.1 ≤ p.2) :
    (f.map Prod.fst).sum ≤ (f.map Prod.snd).sum := by
  induction f with
  | nil => simp
  | cons p f ih =>
    rcases p with ⟨l,r⟩
    simp only [List.map_cons, List.sum_cons]
    have hl : l ≤ r := h (l,r) (by simp)
    have hf : ∀ q ∈ f, q.1 ≤ q.2 := by intro q hq; exact h q (by simp [hq])
    have := ih hf
    omega

theorem sum_snd_le_sum_fst (f : Fiber) (h : ∀ p ∈ f, p.2 ≤ p.1) :
    (f.map Prod.snd).sum ≤ (f.map Prod.fst).sum := by
  induction f with
  | nil => simp
  | cons p f ih =>
    rcases p with ⟨l,r⟩
    simp only [List.map_cons, List.sum_cons]
    have hl : r ≤ l := h (l,r) (by simp)
    have hf : ∀ q ∈ f, q.2 ≤ q.1 := by intro q hq; exact h q (by simp [hq])
    have := ih hf
    omega

theorem diff_zero_of_le (f : Fiber) (h : ∀ p ∈ f, p.1 ≤ p.2) :
    (f.map (fun p => p.1 - p.2)).sum = 0 := by
  induction f with
  | nil => simp
  | cons p f ih =>
    rcases p with ⟨l,r⟩
    simp only [List.map_cons, List.sum_cons]
    have hl : l ≤ r := h (l,r) (by simp)
    have hf : ∀ q ∈ f, q.1 ≤ q.2 := by intro q hq; exact h q (by simp [hq])
    have := ih hf
    omega


theorem le_of_sum_le_of_ge (f : Fiber) (hge : ∀ p ∈ f, p.2 ≤ p.1)
    (hs : (f.map Prod.fst).sum ≤ (f.map Prod.snd).sum) :
    ∀ p ∈ f, p.1 ≤ p.2 := by
  induction f with
  | nil => intro p hp; cases hp
  | cons p f ih =>
    rcases p with ⟨l,r⟩
    simp only [List.map_cons, List.sum_cons] at hs ⊢
    have hlr : r ≤ l := hge (l,r) (by simp)
    have hfge : ∀ q ∈ f, q.2 ≤ q.1 := by intro q hq; exact hge q (by simp [hq])
    have hfsLR : (f.map Prod.fst).sum ≤ (f.map Prod.snd).sum := by omega
    have hfsRL : (f.map Prod.snd).sum ≤ (f.map Prod.fst).sum :=
      sum_snd_le_sum_fst f hfge
    have hres := ih hfge hfsLR
    intro q hq
    simp only [List.mem_cons] at hq
    rcases hq with hq | hq
    · subst q; omega
    · exact hres q hq


theorem ge_of_sum_ge_of_le (f : Fiber) (hle : ∀ p ∈ f, p.1 ≤ p.2)
    (hs : (f.map Prod.snd).sum ≤ (f.map Prod.fst).sum) :
    ∀ p ∈ f, p.2 ≤ p.1 := by
  induction f with
  | nil => intro p hp; cases hp
  | cons p f ih =>
    rcases p with ⟨l,r⟩
    simp only [List.map_cons, List.sum_cons] at hs ⊢
    have hrl : l ≤ r := hle (l,r) (by simp)
    have hfle : ∀ q ∈ f, q.1 ≤ q.2 := by intro q hq; exact hle q (by simp [hq])
    have hfsRL : (f.map Prod.snd).sum ≤ (f.map Prod.fst).sum := by omega
    have hfsLR : (f.map Prod.fst).sum ≤ (f.map Prod.snd).sum :=
      sum_fst_le_sum_snd f hfle
    have hres := ih hfle hfsRL
    intro q hq
    simp only [List.mem_cons] at hq
    rcases hq with hq | hq
    · subst q; omega
    · exact hres q hq

theorem sound : Goal := by
  intro f
  induction f with
  | nil => simp [before, afterProjection]
  | cons p f ih =>
    rcases p with ⟨l,r⟩
    simp only [before, afterProjection, List.map_cons, List.sum_cons] at ih ⊢
    let D := (f.map (fun x => x.1 - x.2)).sum
    let L := (f.map Prod.fst).sum
    let R := (f.map Prod.snd).sum
    change l - r + D = (l + L) - (r + R) ↔
      ((∀ q : Nat × Nat, q ∈ (l,r)::f → q.1 ≤ q.2) ∨
       (∀ q : Nat × Nat, q ∈ (l,r)::f → q.2 ≤ q.1))
    constructor
    · intro heq
      have hge : L - R ≤ D := by
        dsimp [D, L, R]
        exact diff_ge_sub f
      have hsub : (l + L) - (r + R) ≤ (l - r) + (L - R) := sub_add_sub_le l r L R
      have hD : D = L - R := by omega
      have hcond := ih.mp hD
      rcases hcond with hle | hge'
      · by_cases hlr : l ≤ r
        · left
          intro q hq
          simp only [List.mem_cons] at hq
          rcases hq with hq | hq
          · subst q; exact hlr
          · exact hle q hq
        · have hge' : ∀ q ∈ f, q.2 ≤ q.1 := by
            have hD0 : D = 0 := by dsimp [D]; exact diff_zero_of_le f hle
            have hLR : L ≤ R := by dsimp [L, R]; exact sum_fst_le_sum_snd f hle
            have hRL : R ≤ L := by omega
            dsimp [L, R] at hRL
            exact ge_of_sum_ge_of_le f hle hRL
          right
          intro q hq
          simp only [List.mem_cons] at hq
          rcases hq with hq | hq
          · subst q; omega
          · exact hge' q hq
      · by_cases hrl : r ≤ l
        · right
          intro q hq
          simp only [List.mem_cons] at hq
          rcases hq with hq | hq
          · subst q; exact hrl
          · exact hge' q hq
        · have hgeAll : ∀ q ∈ f, q.2 ≤ q.1 := hge'
          have hRL : R ≤ L := by dsimp [L, R]; exact sum_snd_le_sum_fst f hgeAll
          have hLR : L ≤ R := by
            dsimp [D, L, R] at hD hRL ⊢
            omega
          have hle' : ∀ q ∈ f, q.1 ≤ q.2 := by
            dsimp [L, R] at hLR
            exact le_of_sum_le_of_ge f hgeAll hLR
          left
          intro q hq
          simp only [List.mem_cons] at hq
          rcases hq with hq | hq
          · subst q; omega
          · exact hle' q hq
    · intro hcond
      rcases hcond with hleft | hright
      · have hl : l ≤ r := hleft (l,r) (by simp)
        have hf : ∀ q ∈ f, q.1 ≤ q.2 := by intro q hq; exact hleft q (by simp [hq])
        have hD0 : D = 0 := by dsimp [D]; exact diff_zero_of_le f hf
        have hLR : L ≤ R := by dsimp [L, R]; exact sum_fst_le_sum_snd f hf
        omega
      · have hl : r ≤ l := hright (l,r) (by simp)
        have hf : ∀ q ∈ f, q.2 ≤ q.1 := by intro q hq; exact hright q (by simp [hq])
        have hD : D = L - R := by
          dsimp [D, L, R]
          exact ih.mpr (Or.inr hf)
        have hRL : R ≤ L := by dsimp [L, R]; exact sum_snd_le_sum_fst f hf
        omega

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_ProjectExcept

-- C++ references to this model or its proof.
-- plan/optimizer_decisions.hpp
