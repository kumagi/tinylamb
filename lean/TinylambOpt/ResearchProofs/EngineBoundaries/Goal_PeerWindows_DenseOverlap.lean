import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Proved

- `DenseOverlap` — Merging DENSE_RANK summaries subtracts the distinct predecessors
  shared by both shards.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_DenseOverlap

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

/-- Filtering out copies of an absent key is the identity. -/
theorem filter_ne_self {t : List Key} (a : Key) (h : ∀ b ∈ t, b ≠ a) :
    t.filter (fun b => !b == a) = t := by
  induction t with
  | nil => rfl
  | cons x ts ih =>
    have hc : (!x == a) = true := by
      cases hab : (x == a)
      · simp
      · exact absurd (LawfulBEq.eq_of_beq hab) (h x List.mem_cons_self)
    have he : (x :: ts).filter (fun b => !b == a) = x :: ts.filter (fun b => !b == a) := by
      simp [hc]
    rw [he, ih fun b hb => h b (List.mem_cons_of_mem x hb)]

/-- Dropping a present key shrinks the distinct count by exactly one. -/
theorem eraseDups_length_filter_has {d : List Key} (x : Key) (h : x ∈ d) :
    (d.filter fun b => !b == x).eraseDups.length + 1 = d.eraseDups.length := by
  refine keyListInduction (p := fun c => ∀ y : Key, y ∈ c →
      (c.filter fun b => !b == y).eraseDups.length + 1 = c.eraseDups.length) ?_ d x h
  intro c IH y hy
  cases c with
  | nil => contradiction
  | cons a t =>
    by_cases hay : a = y
    · -- The head `a` equals `y`, so it is dropped by the filter and kept by `eraseDups`.
      subst hay
      have hf : (a :: t).filter (fun b => !b == a) = t.filter (fun b => !b == a) := by simp
      rw [hf, List.eraseDups_cons, List.length_cons]
    · -- The head survives both the filter and `eraseDups`.
      have hp : (fun b => !b == y) a = true := by
        cases h2 : (a == y) with
        | true => exact absurd (LawfulBEq.eq_of_beq h2) hay
        | false => simp [h2]
      have hyt : y ∈ t := by
        cases List.mem_cons.mp hy with
        | inl e => exact absurd e.symm hay
        | inr h' => exact h'
      have hU : y ∈ t.filter (fun b => !b == a) :=
        List.mem_filter.mpr ⟨hyt, by
          cases h3 : (y == a) with
          | true => exact absurd (LawfulBEq.eq_of_beq h3).symm hay
          | false => simp⟩
      have hl : (t.filter fun b => !b == a).length < (a :: t).length :=
        Nat.lt_of_le_of_lt (List.length_filter_le _ t) (Nat.lt_succ_self _)
      have hIH := IH (t.filter fun b => !b == a) hl y hU
      have hswap : (t.filter fun z => !z == y).filter (fun z => !z == a)
          = (t.filter fun z => !z == a).filter (fun z => !z == y) := by
        rw [List.filter_filter, List.filter_filter]
        refine List.filter_congr (fun z _ => ?_)
        cases h1 : (z == y) <;> cases h2 : (z == a) <;> simp
      have hfc : (a :: t).filter (fun b => !b == y) = a :: t.filter (fun b => !b == y) :=
        List.filter_cons_of_pos hp
      have he1 : (a :: t.filter fun b => !b == y).eraseDups
          = a :: ((t.filter fun b => !b == y).filter fun b => !b == a).eraseDups :=
        List.eraseDups_cons
      have he2 : (a :: t).eraseDups = a :: (t.filter fun b => !b == a).eraseDups :=
        List.eraseDups_cons
      rw [hfc, he1, he2, hswap, List.length_cons, List.length_cons]
      omega

/-- The master overlap identity for distinct predecessor counts. -/
theorem overlap_length (A B : List Key) :
    (A ++ B).eraseDups.length
        + (A.eraseDups.filter fun x => B.contains x).length
      = A.eraseDups.length + B.eraseDups.length := by
  refine keyListInduction (p := fun c => ∀ d : List Key, (c ++ d).eraseDups.length
      + (c.eraseDups.filter fun x => d.contains x).length
      = c.eraseDups.length + d.eraseDups.length) ?_ A B
  intro c IH d
  cases c with
  | nil => simp
  | cons x t =>
    obtain hIH := IH (t.filter fun b => !b == x)
      (Nat.lt_of_le_of_lt (List.length_filter_le _ t) (Nat.lt_succ_self _))
      (d.filter fun b => !b == x)
    have hcons1 : ((x :: t) ++ d).eraseDups
        = x :: (t.filter (fun b => !b == x) ++ d.filter (fun b => !b == x)).eraseDups := by
      show (x :: (t ++ d)).eraseDups = _
      rw [List.eraseDups_cons, List.filter_append]
    have hcons2 : (x :: t).eraseDups = x :: (t.filter fun b => !b == x).eraseDups :=
      List.eraseDups_cons
    rw [hcons1, hcons2, List.length_cons, List.length_cons]
    by_cases hx : x ∈ d
    · have hsplit :
        (x :: (t.filter fun b => !b == x).eraseDups).filter (fun y => d.contains y)
          = x :: (t.filter fun b => !b == x).eraseDups.filter (fun y => d.contains y) := by
        rw [List.filter_cons, ite_eq_left (by simpa using hx)]
      have hcong :
        (t.filter fun b => !b == x).eraseDups.filter (fun y => d.contains y)
          = (t.filter fun b => !b == x).eraseDups.filter
            (fun y => (d.filter fun b => !b == x).contains y) := by
        refine List.filter_congr (fun y hy => ?_)
        have hy' : y ∈ t.filter fun b => !b == x := List.mem_eraseDups.mp hy
        have hpy : (fun b => !b == x) y = true := (List.mem_filter.mp hy').2
        rw [List.contains_eq_mem, List.contains_eq_mem]
        simp only [List.mem_filter, hpy, and_true]
      have hT := eraseDups_length_filter_has x hx
      rw [hsplit, hcong, List.length_cons]
      omega
    · have hT : d.filter (fun b => !b == x) = d :=
        filter_ne_self x fun b hb heq => hx (heq ▸ hb)
      have hnosplit :
        (x :: (t.filter fun b => !b == x).eraseDups).filter (fun y => d.contains y)
          = (t.filter fun b => !b == x).eraseDups.filter (fun y => d.contains y) := by
        rw [List.filter_cons, ite_eq_right (by simpa using hx)]
      rw [hnosplit]
      rw [hT] at hIH ⊢
      omega

theorem sound : DenseOverlap.Goal := by
  intro a b k
  have hsplit : lower (a ++ b) k = lower a k ++ lower b k := by
    show (a ++ b).filter (before · k) = a.filter (before · k) ++ b.filter (before · k)
    exact List.filter_append _ _
  obtain h := overlap_length (lower a k) (lower b k)
  simp only [dense, hsplit]
  omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_DenseOverlap
