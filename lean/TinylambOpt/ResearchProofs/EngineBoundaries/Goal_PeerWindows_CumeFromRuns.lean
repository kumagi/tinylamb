import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_CumeFromRuns

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-- Strong induction on the length of a list of keys. -/
theorem keyListInduction {p : List Key → Prop}
    (ih : ∀ l, (∀ l', l'.length < l.length → p l') → p l) (l : List Key) : p l := by
  have key : ∀ n : Nat, ∀ l : List Key, l.length ≤ n → p l := by
    intro n
    induction n with
    | zero =>
      intro l hl
      cases l with
      | nil => exact ih [] (fun l' h => absurd h (Nat.not_lt_zero _))
      | cons a as => simp at hl
    | succ n ihn =>
      intro l hl
      exact ih l (fun l' h2 => ihn l' (Nat.le_of_lt_succ (Nat.lt_of_lt_of_le h2 hl)))
  exact key l.length l (Nat.le_refl _)

/-- Strictly smaller keys are never `==`-equal. -/
theorem before_beq_false {x k : Key} (h : before x k) : (x == k) = false := by
  cases h2 : (x == k) with
  | false => rfl
  | true =>
    exfalso
    cases x <;> cases k <;> simp_all [before] <;> omega

/-- Two pointwise-disjoint predicates distribute over `filter` additively. -/
theorem filter_or_of_disjoint (xs : List Key) (p q : Key → Bool)
    (h : ∀ x ∈ xs, !(p x && q x) = true) :
    (xs.filter fun x => p x || q x).length
      = (xs.filter p).length + (xs.filter q).length := by
  induction xs with
  | nil => rfl
  | cons y ys ih =>
    have ihh := ih (fun x hx => h x (List.mem_cons_of_mem y hx))
    rw [List.filter_cons, List.filter_cons, List.filter_cons]
    cases hp : p y with
    | true =>
      cases hq : q y with
      | true => exact absurd (h y List.mem_cons_self) (by rw [hp, hq]; simp)
      | false =>
        simp
        rw [ihh]
        omega
    | false =>
      cases hq : q y with
      | true =>
        simp
        rw [ihh]
        omega
      | false =>
        simp
        rw [ihh]

/-- `eraseDups` produces a duplicate-free list. -/
theorem nodup_eraseDups (l : List Key) : l.eraseDups.Nodup := by
  refine keyListInduction (p := fun c => c.eraseDups.Nodup) ?_ l
  intro c IH
  cases c with
  | nil => exact List.nodup_nil
  | cons a t =>
    rw [List.eraseDups_cons]
    refine List.nodup_cons.mpr ⟨?_, IH _
      (Nat.lt_of_le_of_lt (List.length_filter_le _ t) (Nat.lt_succ_self _))⟩
    intro hin
    have hb : a ∈ t.filter fun c => !c == a := List.mem_eraseDups.mp hin
    exact absurd (List.mem_filter.mp hb).2 (by simp)

/-- A boolean that is not `false` must be `true`. -/
theorem eq_true_of_not_eq_false {b : Bool} (hb : b = false) : ¬ b = true := by
  cases b <;> simp_all

/-- Filtering preserves being duplicate-free. -/
theorem nodup_filter {l : List Key} (p : Key → Bool) : l.Nodup → (l.filter p).Nodup := by
  induction l with
  | nil => intro _; exact List.nodup_nil
  | cons a t ih =>
    intro h
    rcases List.nodup_cons.mp h with ⟨hna, ht⟩
    cases hp : p a with
    | true =>
      rw [List.filter_cons_of_pos hp]
      refine List.nodup_cons.mpr ⟨?_, ih ht⟩
      intro hin
      exact hna (List.mem_filter.mp hin).1
    | false =>
      rw [List.filter_cons_of_neg (by simpa using hp)]
      exact ih ht

/-- Peer counts over a duplicate-free key list sum to the covered row count. -/
theorem sum_count_eq_filter (xs d : List Key) : d.Nodup →
    (d.map fun x => xs.count x).sum = (xs.filter fun x => d.contains x).length := by
  induction d generalizing xs with
  | nil =>
    intro _
    rw [List.map_nil, List.sum_nil]
    induction xs with
    | nil => rfl
    | cons y ys ih =>
      have hy : (y :: ys).filter (fun x => ([] : List Key).contains x)
          = ys.filter (fun x => ([] : List Key).contains x) := by simp
      rw [hy, ih]
  | cons a d ih =>
    intro hd
    rcases List.nodup_cons.mp hd with ⟨hna, hd'⟩
    rw [List.map_cons, List.sum_cons]
    have hcong : xs.filter (fun x => (a :: d).contains x)
        = xs.filter fun x => x == a || d.contains x := by
      refine List.filter_congr (fun x _ => ?_)
      rw [List.contains_cons]
    have hdisj : ∀ x ∈ xs, !((fun (y : Key) => y == a) x && d.contains x) = true := by
      intro x hxm
      cases hcon : ((fun (y : Key) => y == a) x && d.contains x) with
      | false => rfl
      | true =>
        have h1 : (x == a) = true ∧ d.contains x = true := by simpa using hcon
        have hxa : x = a := LawfulBEq.eq_of_beq h1.1
        exact absurd (hxa ▸ List.contains_iff_mem.mp h1.2) hna
    have hsplit : (xs.filter fun x => x == a || d.contains x).length
        = (xs.filter fun x => x == a).length + (xs.filter fun x => d.contains x).length :=
      filter_or_of_disjoint xs _ _ hdisj
    rw [hcong, hsplit, List.count_eq_length_filter, ih xs hd']

/-- Compressed peer runs recover the CUME_DIST numerator exactly. -/
theorem cume_aux (xs : List Key) (k : Key) :
    ((xs.eraseDups.filter fun x => before x k || x == k).map fun x => xs.count x).sum
      = (lower xs k).length + xs.count k := by
  have hd : (xs.eraseDups.filter fun x => before x k || x == k).Nodup :=
    nodup_filter (fun x => before x k || x == k) (nodup_eraseDups xs)
  obtain h := sum_count_eq_filter xs (xs.eraseDups.filter fun x => before x k || x == k) hd
  have hcontains : xs.filter (fun x =>
        (xs.eraseDups.filter fun y => before y k || y == k).contains x)
      = xs.filter fun x => before x k || x == k := by
    refine List.filter_congr (fun x hx => ?_)
    cases hq : (before x k || x == k) with
    | true =>
      rw [List.contains_iff_mem]
      exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hx, hq⟩
    | false =>
      cases hc : ((xs.eraseDups.filter fun y => before y k || y == k).contains x) with
      | true =>
        exact absurd (List.contains_iff_mem.mp hc) fun hin =>
          absurd (List.mem_filter.mp hin).2 (eq_true_of_not_eq_false hq)
      | false => simp
  have hdisj : ∀ x ∈ xs,
      !((fun (y : Key) => before y k) x && (fun (y : Key) => y == k) x) = true := by
    intro x _
    cases hcon : ((fun (y : Key) => before y k) x && (fun (y : Key) => y == k) x) with
    | false => rfl
    | true =>
      have h1 : before x k = true ∧ (x == k) = true := by simpa using hcon
      exact absurd h1.2 (eq_true_of_not_eq_false (before_beq_false h1.1))
  have hsplit := filter_or_of_disjoint xs (fun y => before y k) (fun y => y == k) hdisj
  have hlower : (xs.filter (before · k)).length = (lower xs k).length := rfl
  have hcount : (xs.filter fun x => x == k).length = xs.count k :=
    (List.count_eq_length_filter (a := k)).symm
  rw [hcontains] at h
  rw [hsplit, hcount, hlower] at h
  exact h

theorem sound : CumeFromRuns.Goal := by
  intro xs k
  exact (cume_aux xs k).symm

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_CumeFromRuns
