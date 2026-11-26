import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Proved

- `CollapseRanks` — For a present key, rank and dense rank coincide exactly without
  repeated smaller keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_CollapseRanks

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

theorem eraseDups_length_le (l : List Key) : l.eraseDups.length ≤ l.length := by
  refine keyListInduction (p := fun L => L.eraseDups.length ≤ L.length) ?_ l
  intro L IH
  cases L with
  | nil => exact Nat.le_refl 0
  | cons a t =>
    rw [List.eraseDups_cons]
    refine Nat.succ_le_succ (Nat.le_trans (IH _ ?_) (List.length_filter_le _ _))
    exact Nat.lt_of_le_of_lt (List.length_filter_le _ t) (Nat.lt_succ_self _)

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

/-- Removing all copies of a present key strictly shrinks the list. -/
theorem filter_ne_length_lt (t : List Key) (a : Key) (h : a ∈ t) :
    (t.filter fun b => !b == a).length < t.length := by
  induction t with
  | nil => contradiction
  | cons x ts ih =>
    rcases List.mem_cons.mp h with heq | h'
    · subst heq
      have he : (a :: ts).filter (fun b => !b == a) = ts.filter (fun b => !b == a) := by simp
      rw [he]
      exact Nat.lt_of_le_of_lt (List.length_filter_le _ ts) (Nat.lt_succ_self _)
    · rw [List.filter_cons]
      split
      · exact Nat.succ_lt_succ (ih h')
      · exact Nat.lt_of_le_of_lt (List.length_filter_le _ ts) (Nat.lt_succ_self _)

theorem eraseDups_length_eq_iff_nodup (l : List Key) :
    l.eraseDups.length = l.length ↔ l.Nodup := by
  refine keyListInduction (p := fun L => (L.eraseDups.length = L.length ↔ L.Nodup)) ?_ l
  intro L IH
  cases L with
  | nil =>
    exact ⟨fun _ => List.nodup_nil, fun _ =>
      Nat.le_antisymm (eraseDups_length_le _) (Nat.zero_le _)⟩
  | cons a t =>
    refine ⟨fun hlen => ?_, fun hnd => ?_⟩
    · by_cases hmem : a ∈ t
      · have hlt := filter_ne_length_lt t a hmem
        have edle := eraseDups_length_le (t.filter fun b => !b == a)
        simp only [List.eraseDups_cons, List.length_cons] at hlen
        omega
      · have hft : t.filter (fun b => !b == a) = t :=
          filter_ne_self a fun b hb heq => hmem (heq ▸ hb)
        rw [List.eraseDups_cons, hft] at hlen
        simp only [List.length_cons] at hlen
        refine List.nodup_cons.mpr ⟨hmem, ?_⟩
        exact (IH t (Nat.lt_succ_self _)).mp (by omega)
    · have hb : ¬a ∈ t ∧ t.Nodup := List.nodup_cons.mp hnd
      have hft : t.filter (fun b => !b == a) = t :=
          filter_ne_self a fun b hb' heq => hb.1 (heq ▸ hb')
      rw [List.eraseDups_cons, hft]
      simp only [List.length_cons, (IH t (Nat.lt_succ_self _)).mpr hb.2]

theorem sound : CollapseRanks.Goal := by
  intro xs k hk
  constructor
  · intro h
    refine ((eraseDups_length_eq_iff_nodup (lower xs k)).mp ?_)
    simp only [rankOf, dense, lower] at h ⊢
    omega
  · intro h
    simp only [rankOf, dense, lower]
    have := (eraseDups_length_eq_iff_nodup (xs.filter (before · k))).mpr h
    omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_CollapseRanks
