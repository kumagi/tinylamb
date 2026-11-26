import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `ProjectedOrder` — Elide a suffix re-sort after a monotone, non-injective projection
  exactly when adjacent nonempty runs coalescing into one bucket have compatible
  endpoints.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_ProjectedOrder

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.ProjectedOrder

def firstRow (f : Nat → Nat) (r : Run) : Nat × Nat := (f r.1, r.2.head?.getD 0)
def lastRow (f : Nat → Nat) (r : Run) : Nat × Nat := (f r.1, r.2.getLast?.getD 0)

theorem lex_refl (a : Nat × Nat) : lexLE a a := by
  dsimp [lexLE]
  right
  exact ⟨rfl, Nat.le_refl _⟩

theorem lex_trans {a b c : Nat × Nat} (hab : lexLE a b) (hbc : lexLE b c) : lexLE a c := by
  dsimp [lexLE] at hab hbc ⊢
  rcases hab with hlt | ⟨heq, hle⟩
  · rcases hbc with hlt2 | ⟨heq2, hle2⟩ <;> left <;> omega
  · rcases hbc with hlt2 | ⟨heq2, hle2⟩
    · left; omega
    · right; constructor <;> omega

theorem getLast_mem_of_some (l : List Nat) (v : Nat) (h : l.getLast? = some v) : v ∈ l := by
  obtain ⟨ys, rfl⟩ := List.getLast?_eq_some_iff.mp h
  simp

theorem getLast?_getD_mem (l : List Nat) (hne : ¬ l.isEmpty) : l.getLast?.getD 0 ∈ l := by
  cases hg : l.getLast? with
  | none =>
    have : l = [] := List.getLast?_eq_none_iff.mp hg
    subst l
    simp at hne
  | some v =>
    have hd : (some v).getD 0 = v := by rfl
    rw [hd]
    exact getLast_mem_of_some l v hg

theorem getD_eq_of_ne (l : List Nat) (hne : ¬ l.isEmpty) (d1 d2 : Nat) :
    l.getLast?.getD d1 = l.getLast?.getD d2 := by
  cases hg : l.getLast? with
  | none =>
    have : l = [] := List.getLast?_eq_none_iff.mp hg
    subst l
    simp at hne
  | some v => simp [hg]

theorem mem_le_last (l : List Nat) (hs : l.Pairwise (fun a b => a ≤ b)) :
    ∀ t ∈ l, t ≤ l.getLast?.getD 0 := by
  induction l with
  | nil => intro t ht; cases ht
  | cons a as ih =>
    intro t ht
    simp only [List.getLast?_cons]
    simp only [List.mem_cons] at ht
    rcases ht with hta | ht
    · subst t
      cases as with
      | nil => simp
      | cons b bs =>
        have hs_tail : (b :: bs).Pairwise (fun a b => a ≤ b) := by
          cases hs with | cons hh ht => exact ht
        have hble : b ≤ (b :: bs).getLast?.getD 0 := ih hs_tail b (by simp)
        have hab : a ≤ b := by
          cases hs with | cons hh ht => exact hh b (by simp)
        simpa [List.getLast?_cons] using (Nat.le_trans hab hble)
    · have hne_as : ¬ as.isEmpty := by
        cases as with
        | nil => simp at ht
        | cons b bs => simp
      have heqd : as.getLast?.getD a = as.getLast?.getD 0 := getD_eq_of_ne as hne_as a 0
      have hi := ih (by cases hs with | cons hh ht => exact ht) t ht
      simpa [heqd] using hi

theorem head_le_mem (l : List Nat) (hs : l.Pairwise (fun a b => a ≤ b)) :
    ∀ t ∈ l, l.head?.getD 0 ≤ t := by
  cases l with
  | nil => intro t ht; cases ht
  | cons a as =>
    cases hs with
    | cons hh ht =>
      intro t hmem
      simp only [List.head?_cons]
      simp only [List.mem_cons] at hmem
      rcases hmem with hta | htmem
      · subst t; simp
      · exact hh t htmem

theorem rows_cons (f : Nat → Nat) (r : Run) (rs : List Run) :
    rows f (r :: rs) = rows f [r] ++ rows f rs := by
  simp [rows]

theorem firstRow_mem (f : Nat → Nat) (r : Run) (hne : ¬ r.2.isEmpty) :
    firstRow f r ∈ rows f [r] := by
  cases r with
  | mk key ts =>
    cases ts with
    | nil => simp at hne
    | cons t ts =>
      dsimp [rows, firstRow]
      simp

theorem lastRow_mem (f : Nat → Nat) (r : Run) (hne : ¬ r.2.isEmpty) :
    lastRow f r ∈ rows f [r] := by
  cases r with
  | mk key ts =>
    cases ts with
    | nil => simp at hne
    | cons t ts =>
      have hm := getLast?_getD_mem (t :: ts) (by simp)
      dsimp [rows, lastRow]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.map_cons,
        List.getLast?_cons]
      simpa [List.getLast?_cons] using hm

theorem firstRow_mem_rows (f : Nat → Nat) (r : Run) (hne : ¬ r.2.isEmpty) (rs : List Run) :
    firstRow f r ∈ rows f (r :: rs) := by
  rw [rows_cons]
  exact List.mem_append_left _ (firstRow_mem f r hne)

theorem lastRow_mem_rows (f : Nat → Nat) (r : Run) (hne : ¬ r.2.isEmpty) (rs : List Run) :
    lastRow f r ∈ rows f (r :: rs) := by
  rw [rows_cons]
  exact List.mem_append_left _ (lastRow_mem f r hne)

theorem boundary_implies_lex (f : Nat → Nat) (hmono : ∀ a b, a ≤ b → f a ≤ f b)
    {r0 r1 : Run} (hkey : r0.1 ≤ r1.1) (hb : boundary f r0 r1) :
    lexLE (lastRow f r0) (firstRow f r1) := by
  by_cases hfe : f r0.1 = f r1.1
  · have hright : r0.2.getLast?.getD 0 ≤ r1.2.head?.getD 0 := by
      dsimp [boundary] at hb
      rcases hb with hne' | hle
      · exact False.elim (hne' hfe)
      · exact hle
    dsimp [lexLE, lastRow, firstRow]
    right
    exact ⟨hfe, hright⟩
  · have hlt : f r0.1 < f r1.1 := by
      have := hmono r0.1 r1.1 hkey
      omega
    dsimp [lexLE, lastRow, firstRow]
    left
    exact hlt

theorem lex_implies_boundary {f : Nat → Nat} {r0 r1 : Run}
    (hlex : lexLE (lastRow f r0) (firstRow f r1)) : boundary f r0 r1 := by
  dsimp [lexLE, lastRow, firstRow] at hlex
  dsimp [boundary]
  rcases hlex with hlt | ⟨hfe, hle⟩
  · left; omega
  · right; exact hle

theorem fwd (f : Nat → Nat) (_hmono : ∀ a b, a ≤ b → f a ≤ f b) : ∀ rs : List Run,
    rs.Pairwise (fun a b => a.1 ≤ b.1) →
    (∀ r ∈ rs, r.2.Pairwise (fun a b => a ≤ b)) →
    (∀ r ∈ rs, ¬ r.2.isEmpty) →
    (rows f rs).Pairwise lexLE →
    ∀ pair ∈ rs.zip (rs.drop 1), boundary f pair.1 pair.2 := by
  intro rs
  induction rs with
  | nil => simp
  | cons r0 rs ih =>
    intro hkeys htails hne hpair
    cases rs with
    | nil => simp
    | cons r1 rs' =>
      rw [rows_cons] at hpair
      have hparts := List.pairwise_append.mp hpair
      have htailpair : (rows f (r1 :: rs')).Pairwise lexLE := hparts.2.1
      have hcross := hparts.2.2
      cases hkeys with
      | cons hkhead hktail =>
        have htails_tail : ∀ r ∈ r1 :: rs', r.2.Pairwise (fun a b => a ≤ b) := by
          intro r hr; exact htails r (by simp [hr])
        have hne_tail : ∀ r ∈ r1 :: rs', ¬ r.2.isEmpty := by
          intro r hr; exact hne r (by simp [hr])
        have ihres := ih hktail htails_tail hne_tail htailpair
        intro pair hp
        have hp' : pair = (r0, r1) ∨ pair ∈ (r1 :: rs').zip rs' := by
          simpa using hp
        rcases hp' with rfl | hp'
        · apply lex_implies_boundary
          exact hcross (lastRow f r0) (lastRow_mem f r0 (hne r0 (by simp)))
            (firstRow f r1) (firstRow_mem_rows f r1 (hne r1 (by simp)) rs')
        · exact ihres pair hp'

theorem pairwise_single (f : Nat → Nat) (r : Run)
    (hs : r.2.Pairwise (fun a b => a ≤ b)) : (rows f [r]).Pairwise lexLE := by
  cases r with
  | mk key ts =>
    dsimp [rows]
    simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
    induction ts with
    | nil => simp
    | cons t ts ih =>
      cases hs with
      | cons hh ht =>
        simp only [List.map_cons]
        apply List.Pairwise.cons
        · intro y hy
          simp only [List.mem_map] at hy
          obtain ⟨y', hy'mem, rfl⟩ := hy
          dsimp [lexLE]
          right
          exact ⟨rfl, hh y' hy'mem⟩
        · exact ih ht

theorem bwd (f : Nat → Nat) (hmono : ∀ a b, a ≤ b → f a ≤ f b) : ∀ rs : List Run,
    rs.Pairwise (fun a b => a.1 ≤ b.1) →
    (∀ r ∈ rs, r.2.Pairwise (fun a b => a ≤ b)) →
    (∀ r ∈ rs, ¬ r.2.isEmpty) →
    (∀ pair ∈ rs.zip (rs.drop 1), boundary f pair.1 pair.2) →
    (rows f rs).Pairwise lexLE := by
  intro rs
  induction rs with
  | nil => simp [rows]
  | cons r0 rs ih =>
    intro hkeys htails hne hadj
    cases rs with
    | nil =>
      exact pairwise_single f r0 (htails r0 (by simp))
    | cons r1 rs' =>
      cases hkeys with
      | cons hkhead hktail =>
        have htails_tail : ∀ r ∈ r1 :: rs', r.2.Pairwise (fun a b => a ≤ b) := by
          intro r hr; exact htails r (by simp [hr])
        have hne_tail : ∀ r ∈ r1 :: rs', ¬ r.2.isEmpty := by
          intro r hr; exact hne r (by simp [hr])
        have hadj_tail : ∀ pair ∈ (r1 :: rs').zip rs', boundary f pair.1 pair.2 := by
          intro pair hp
          apply hadj
          simpa using (Or.inr hp : pair = (r0, r1) ∨ pair ∈ (r1 :: rs').zip rs')
        have hpair2 : (rows f (r1 :: rs')).Pairwise lexLE :=
          ih hktail htails_tail hne_tail hadj_tail
        rw [rows_cons]
        apply List.pairwise_append.mpr
        constructor
        · exact pairwise_single f r0 (htails r0 (by simp))
        constructor
        · exact hpair2
        · intro a ha b hb
          have hb0 : boundary f r0 r1 := hadj (r0, r1) (by simp)
          have hkey01 : r0.1 ≤ r1.1 := hkhead r1 (by simp)
          have hlex01 : lexLE (lastRow f r0) (firstRow f r1) :=
            boundary_implies_lex f hmono hkey01 hb0
          have hlexa : lexLE a (lastRow f r0) := by
            dsimp [rows] at ha
            simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.mem_map] at ha
            obtain ⟨t, htmem, rfl⟩ := ha
            dsimp [lexLE, lastRow]
            right
            exact ⟨rfl, mem_le_last r0.2 (htails r0 (by simp)) t htmem⟩
          have hlexb : lexLE (firstRow f r1) b := by
            rw [rows_cons] at hpair2
            have hparts2 := List.pairwise_append.mp hpair2
            have hcross2 := hparts2.2.2
            rw [rows_cons] at hb
            simp only [List.mem_append] at hb
            rcases hb with hb1 | hb2
            · dsimp [rows] at hb1
              simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil, List.mem_map] at hb1
              obtain ⟨t, htmem, rfl⟩ := hb1
              dsimp [lexLE, firstRow]
              right
              exact ⟨rfl, head_le_mem r1.2 (htails r1 (by simp)) t htmem⟩
            · exact hcross2 (firstRow f r1) (firstRow_mem f r1 (hne r1 (by simp))) b hb2
          exact lex_trans hlexa (lex_trans hlex01 hlexb)

theorem pairwise_filter {α} (R : α → α → Prop) (p : α → Bool) :
    ∀ l : List α, l.Pairwise R → (l.filter p).Pairwise R := by
  intro l h
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.filter_cons]
    cases hp : p x
    · exact ih (by cases h with | cons hh ht => exact ht)
    · cases h with
      | cons hh ht =>
        apply List.Pairwise.cons
        · intro y hy
          have hy' : y ∈ xs ∧ p y = true := by simpa using hy
          exact hh y hy'.1
        · exact ih ht

theorem rows_filter_live (f : Nat → Nat) : ∀ runs : List Run,
    rows f (runs.filter (fun r => !r.2.isEmpty)) = rows f runs := by
  intro runs
  induction runs with
  | nil => rfl
  | cons r rs ih =>
    dsimp [rows] at ih ⊢
    cases r with
    | mk k ts =>
      by_cases hne : ts.isEmpty = true
      · have hnil : ts = [] := List.isEmpty_iff.mp hne
        subst ts
        simp [hne, ih]
      · have hne' : (!ts.isEmpty) = true := by
          cases h : ts.isEmpty <;> simp [h] at hne ⊢
        simp [hne', ih]

theorem sound : Goal := by
  intro f runs hmono hkeys htails
  let live := runs.filter (fun r => !r.2.isEmpty)
  have hrows : rows f live = rows f runs := by
    dsimp [live]
    exact rows_filter_live f runs
  have hkeys_live : live.Pairwise (fun a b => a.1 ≤ b.1) := by
    dsimp [live]
    exact pairwise_filter (fun a b : Run => a.1 ≤ b.1) (fun r => !r.2.isEmpty) runs hkeys
  have htails_live : ∀ r ∈ live, r.2.Pairwise (fun a b => a ≤ b) := by
    intro r hr
    dsimp [live] at hr
    simp only [List.mem_filter] at hr
    exact htails r hr.1
  have hne_live : ∀ r ∈ live, ¬ r.2.isEmpty := by
    intro r hr
    dsimp [live] at hr
    simp only [List.mem_filter] at hr
    have hb : (!r.2.isEmpty) = true := hr.2
    cases h : r.2.isEmpty <;> simp [h] at hb ⊢
  constructor
  · intro hpair
    apply fwd f hmono live hkeys_live htails_live hne_live
    rwa [hrows]
  · intro hadj
    have hp := bwd f hmono live hkeys_live htails_live hne_live hadj
    rw [← hrows]
    exact hp

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_ProjectedOrder

-- C++ references to this model or its proof.
-- plan/optimizer_decisions.hpp
