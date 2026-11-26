import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.DispatchWindowOutputCap.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
A dispatch window plus a per-morsel row cap bounds retained rows.

Claim (`DispatchWindowOutputCap.Goal`): if each unretired morsel reports at most
once (`(rs.map Prod.fst).Nodup`), every reported id lies inside the dispatch
window `[start, start+window)`, and each report carries at most `rows` rows, then
`rs` retains at most `window * rows` rows in total. By the pigeonhole principle a
duplicate-free set of ids inside a window of size `window` has at most `window`
elements, and each contributes at most `rows` rows. This bounds the memory the
coordinator retains for in-flight retirement reports.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_DispatchWindowOutputCap
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

private theorem length_flatMap_snd (rs : List Report) :
    (rs.flatMap Prod.snd).length = (rs.map (fun r => (r.2).length)).sum := by
  induction rs with
  | nil => rfl
  | cons x xs ih =>
    rw [List.flatMap_cons, List.length_append, List.map_cons, List.sum_cons, ih]

private theorem sum_le_mul (l : List Nat) (b : Nat) (h : ∀ x ∈ l, x ≤ b) :
    l.sum ≤ l.length * b := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    refine Nat.le_trans (Nat.add_le_add (h x (by simp))
      (ih (fun y hy => h y (by simp [hy])))) ?_
    rw [List.length_cons, Nat.add_mul, Nat.one_mul, Nat.add_comm b (xs.length * b)]
    exact Nat.le_refl _

private theorem mem_filter_of (p : Nat → Bool) :
    ∀ (l : List Nat) (x : Nat), x ∈ List.filter p l → x ∈ l := by
  intro l
  induction l with
  | nil => intro x hx; cases hx
  | cons a as ih =>
    intro x hx
    cases hpa : p a
    · rw [List.filter_cons_of_neg (by rw [hpa]; decide)] at hx
      exact List.mem_cons_of_mem a (ih x hx)
    · rw [List.filter_cons_of_pos hpa] at hx
      rcases List.mem_cons.mp hx with e | hm
      · subst e
        simp
      · exact List.mem_cons_of_mem a (ih x hm)

private theorem nodup_filter (p : Nat → Bool) :
    ∀ (l : List Nat), l.Nodup → (List.filter p l).Nodup := by
  intro l
  induction l with
  | nil => intro _; exact List.nodup_nil
  | cons a as ih =>
    intro hd
    cases hd with
    | cons hna hds =>
      cases hpa : p a
      · rw [List.filter_cons_of_neg (by rw [hpa]; decide)]
        exact ih hds
      · rw [List.filter_cons_of_pos hpa]
        constructor
        · intro x hx
          exact hna x (mem_filter_of p as x hx)
        · exact ih hds

private theorem len_split (w : Nat) : ∀ (ids : List Nat),
    ids.length = (List.filter (fun j => j == w) ids).length
      + (List.filter (fun j => !(j == w)) ids).length := by
  intro ids
  induction ids with
  | nil => simp
  | cons a as ih =>
    cases h : a == w
    · rw [List.filter_cons_of_neg (p := fun j => j == w) (by rw [h]; decide),
        List.filter_cons_of_pos (p := fun j => !(j == w)) (by rw [h]; decide),
        List.length_cons, ih, List.length_cons]
      omega
    · rw [List.filter_cons_of_pos (p := fun j => j == w) h,
        List.filter_cons_of_neg (p := fun j => !(j == w)) (by rw [h]; decide),
        List.length_cons, ih, List.length_cons]
      omega

private theorem nodup_subsingleton_len (w : Nat) :
    ∀ (l : List Nat), l.Nodup → (∀ x ∈ l, x = w) → l.length ≤ 1 := by
  intro l
  induction l with
  | nil => intro _ _; simp
  | cons a as ih =>
    intro hd hb
    cases hd with
    | cons hna hds =>
      cases as with
      | nil => simp
      | cons b bs =>
        exfalso
        apply hna b (by simp)
        rw [hb a (by simp), hb b (by simp)]

private theorem pigeonhole :
    ∀ (w : Nat) (ids : List Nat) (s : Nat), ids.Nodup →
      (∀ i ∈ ids, s ≤ i ∧ i < s + w) → ids.length ≤ w := by
  intro w
  induction w with
  | zero =>
    intro ids s hd hb
    cases ids with
    | nil => simp
    | cons a as =>
      have := hb a (by simp)
      omega
  | succ v ih =>
    intro ids s hd hb
    have hsplit := len_split (s + v) ids
    have hn1 : (List.filter (fun j => j == s + v) ids).Nodup :=
      nodup_filter (fun j => j == s + v) ids hd
    have hn2 : (List.filter (fun j => !(j == s + v)) ids).Nodup :=
      nodup_filter (fun j => !(j == s + v)) ids hd
    have hb1 : ∀ x ∈ List.filter (fun j => j == s + v) ids, x = s + v := by
      intro x hx
      simp only [List.mem_filter] at hx
      obtain ⟨_, hpx⟩ := hx
      simpa using hpx
    have le1 : (List.filter (fun j => j == s + v) ids).length ≤ 1 :=
      nodup_subsingleton_len (s + v) _ hn1 hb1
    have hb2 : ∀ i ∈ List.filter (fun j => !(j == s + v)) ids, s ≤ i ∧ i < s + v := by
      intro i hi
      simp only [List.mem_filter] at hi
      obtain ⟨him, hne⟩ := hi
      have ⟨h1, h2⟩ := hb i him
      refine ⟨h1, ?_⟩
      rcases Nat.lt_or_ge i (s + v) with hlt | hge
      · exact hlt
      · exfalso
        have hii : i = s + v := by omega
        rw [hii] at hne
        simp at hne
    have le2 := ih _ s hn2 hb2
    omega

theorem sound : DispatchWindowOutputCap.Goal := by
  intro rs start window rows hd hb
  have hb2 : ∀ i ∈ List.map Prod.fst rs, start ≤ i ∧ i < start + window := by
    intro i hi
    obtain ⟨r, hr, rfl⟩ := List.mem_map.mp hi
    obtain ⟨h1, h2, -⟩ := hb r hr
    exact ⟨h1, h2⟩
  have hlen := length_flatMap_snd rs
  have hsum := sum_le_mul (List.map (fun r => (r.2).length) rs) rows
    (fun r hr => by
      obtain ⟨r', hr', rfl⟩ := List.mem_map.mp hr
      obtain ⟨-, -, h3⟩ := hb r' hr'
      exact h3)
  have hcard : (List.map (fun r => (r.2).length) rs).length ≤ window := by
    rw [List.length_map, ← List.length_map (f := Prod.fst)]
    exact pigeonhole window (List.map Prod.fst rs) start hd hb2
  rw [hlen]
  exact Nat.le_trans hsum (Nat.mul_le_mul_right rows hcard)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_DispatchWindowOutputCap
