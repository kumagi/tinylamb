import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_C1
open TinylambOpt.ResearchGoals.BenchmarkRematch

abbrev Row := Int × Int × Nat

def qual (r : Row) : Bool := decide (5 < r.1 ∧ r.2.1 < 10000)

def poss (p : List Row) : Bool :=
  (p.map Prod.fst).max?.any (5 < ·) && (p.map (fun r => r.2.1)).min?.any (· < 10000)

theorem max_any_gt (xs : List Int) (x : Int) (hx : x ∈ xs) (hgt : 5 < x) :
    xs.max?.any (5 < ·) = true := by
  have hxs : xs ≠ [] := by
    intro he
    subst he
    simp at hx
  obtain ⟨m, hm⟩ : ∃ m, xs.max? = some m := by
    cases xs with
    | nil => contradiction
    | cons a t =>
        simp [List.max?_cons]
  have hmem : m ∈ xs ∧ ∀ b ∈ xs, b ≤ m := List.max?_eq_some_iff.mp hm
  have hle : x ≤ m := hmem.2 x hx
  rw [hm]
  simpa using (Int.lt_of_lt_of_le hgt hle)

theorem min_any_lt (xs : List Int) (x : Int) (hx : x ∈ xs) (hlt : x < 10000) :
    xs.min?.any (· < 10000) = true := by
  have hxs : xs ≠ [] := by
    intro he
    subst he
    simp at hx
  obtain ⟨m, hm⟩ : ∃ m, xs.min? = some m := by
    cases xs with
    | nil => contradiction
    | cons a t =>
        simp [List.min?_cons]
  have hmem : m ∈ xs ∧ ∀ b ∈ xs, m ≤ b := List.min?_eq_some_iff.mp hm
  have hle : m ≤ x := hmem.2 x hx
  rw [hm]
  simpa using (Int.lt_of_le_of_lt hle hlt)

theorem possible_of_qual_mem (p : List Row) (r : Row) (hr : r ∈ p) (hq : qual r = true) :
    poss p = true := by
  have hscore : 5 < r.1 := by
    have := of_decide_eq_true hq
    exact this.1
  have hviews : r.2.1 < 10000 := by
    have := of_decide_eq_true hq
    exact this.2
  have hsmem : r.1 ∈ p.map Prod.fst := List.mem_map.mpr ⟨r, hr, rfl⟩
  have hvmem : r.2.1 ∈ p.map (fun r => r.2.1) := List.mem_map.mpr ⟨r, hr, rfl⟩
  unfold poss
  rw [max_any_gt (p.map Prod.fst) r.1 hsmem hscore]
  rw [min_any_lt (p.map (fun r => r.2.1)) r.2.1 hvmem hviews]
  rfl

theorem filter_qual_nil (p : List Row) (h : ∀ r ∈ p, qual r = false) : p.filter qual = [] := by
  induction p with
  | nil => rfl
  | cons r t ih =>
      simp [h r (by simp), ih (fun x hx => h x (by simp [hx]))]

theorem flatten_map_filter (q : Row → Bool) : ∀ xs : List (List Row),
    (xs.map (List.filter q)).flatten = xs.flatten.filter q
  | [] => rfl
  | a :: t => by
      simp only [List.map_cons, List.flatten_cons, List.filter_append]
      rw [flatten_map_filter q t]

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.C1.Goal := by
  intro pages
  dsimp only
  induction pages with
  | nil => rfl
  | cons page t ih =>
      by_cases hp : poss page = true
      · have hp' : ((page.map Prod.fst).max?.any (5 < ·) &&
            (page.map (fun r => r.2.1)).min?.any (· < 10000)) = true := by
          simpa [poss] using hp
        simp [hp']
        rw [flatten_map_filter, flatten_map_filter]
        exact ih
      · have hp' : ¬ ((page.map Prod.fst).max?.any (5 < ·) &&
            (page.map (fun r => r.2.1)).min?.any (· < 10000)) = true := by
          simpa [poss] using hp
        have hnone : ∀ r ∈ page, qual r = false := by
          intro r hr
          cases hqr : qual r with
          | false => rfl
          | true =>
              exfalso
              exact hp (possible_of_qual_mem page r hr hqr)
        have hf : page.filter qual = [] := filter_qual_nil page hnone
        have hf' : page.filter (fun p => decide (5 < p.1) && decide (p.2.1 < 10000)) = [] := by
          simpa [qual] using hf
        simp [hp']
        rw [flatten_map_filter (fun p : Row => decide (5 < p.1) && decide (p.2.1 < 10000))]
        rw [flatten_map_filter (fun p : Row => decide (5 < p.1) && decide (p.2.1 < 10000))]
        rw [hf']
        rw [List.nil_append]
        exact ih


#print axioms TinylambOpt.ResearchProofs.November26.Analytics.Goal_C1.sound
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_C1
