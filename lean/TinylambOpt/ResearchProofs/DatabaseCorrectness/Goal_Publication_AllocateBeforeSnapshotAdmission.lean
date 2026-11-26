import TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_AllocateBeforeSnapshotAdmission

open TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication

theorem foldl_min_le_self (l : List Nat) (a : Nat) : l.foldl min a ≤ a := by
  induction l generalizing a with
  | nil => simp
  | cons x xs ih =>
      exact Nat.le_trans (ih (min a x)) (Nat.min_le_left a x)

theorem foldl_min_le_mem (l : List Nat) (a m : Nat) (hmem : m ∈ l) :
    l.foldl min a ≤ m := by
  induction l generalizing a with
  | nil => simp at hmem
  | cons x xs ih =>
      simp at hmem
      rcases hmem with hmx | hm
      · subst m
        exact Nat.le_trans (foldl_min_le_self xs (min a x)) (Nat.min_le_right a x)
      · exact ih (min a x) hm

theorem foldl_min_le_of_all_le (l : List Nat) (a b : Nat) (ha : a ≤ b)
    (hall : ∀ x ∈ l, x ≤ b) : l.foldl min a ≤ b := by
  induction l generalizing a with
  | nil => exact ha
  | cons x xs ih =>
      apply ih (min a x)
      · exact Nat.le_trans (Nat.min_le_right a x) (hall x (by simp))
      · intro y hy
        exact hall y (by simp [hy])

theorem min?_cons_large_of_ne (l : List Nat) (a : Nat) (hne : l ≠ []) (h : ∀ x ∈ l, x < a) :
    (a :: l).min? = l.min? := by
  cases l with
  | nil => simp at hne
  | cons b bs =>
      have hb : b ≤ a := Nat.le_of_lt (h b (by simp))
      simp [List.min?, hb]

theorem min?_none_nil (l : List Nat) : l.min? = none → l = [] := by
  cases l <;> simp [List.min?]

theorem min?_getD_le (l : List Nat) (d m : Nat) (hmem : m ∈ l) :
    (l.min?.getD d) ≤ m := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
      simp [List.min?]
      simp at hmem
      rcases hmem with hmx | hm
      · subst m
        exact foldl_min_le_self xs x
      · exact foldl_min_le_mem xs x m hm

theorem min?_le_of_all_le (l : List Nat) (d b : Nat) (hd : d ≤ b)
    (hall : ∀ x ∈ l, x ≤ b) : (l.min?.getD d) ≤ b := by
  cases l with
  | nil => simpa using hd
  | cons a as =>
      simp [List.min?]
      apply foldl_min_le_of_all_le as a b (hall a (by simp))
      intro x hx
      exact hall x (by simp [hx])

theorem sound_newTicket (c : Clock) (hc : sound c) : sound (newTicket c) := by
  rcases hc with ⟨hnod, hstable, hfence⟩
  simp [sound, newTicket] at ⊢
  constructor
  · constructor
    · intro hmem
      have := hfence _ hmem
      omega
    · exact hnod
  · constructor
    · omega
    · constructor
      · omega
      · intro t ht
        exact ⟨(hfence t ht).1, by have := (hfence t ht).2; omega⟩

theorem candidate_newTicket (c : Clock) (hc : sound c) :
    candidate (newTicket c) = candidate c := by
  by_cases hempty : c.pending = []
  · simp [candidate, newTicket, hempty]
  · have hmin := min?_cons_large_of_ne c.pending (c.allocated + 1) hempty (by
      intro x hx
      have hxle := hc.2.2 x hx
      omega)
    cases hm : c.pending.min? with
    | none =>
        have hnil := min?_none_nil _ hm
        exact (hempty hnil).elim
    | some m =>
        simp [candidate, newTicket, hmin, hm]

theorem foldl_min_eq_head_or_mem (l : List Nat) (a : Nat) :
    ∃ x ∈ a :: l, l.foldl min a = x := by
  induction l generalizing a with
  | nil => exact ⟨a, by simp, rfl⟩
  | cons x xs ih =>
      rcases ih (min a x) with ⟨y, hy, hval⟩
      have hy' : y ∈ a :: x :: xs := by
        simp at hy
        rcases hy with rfl | hy
        · by_cases hax : a ≤ x
          · simp [Nat.min_eq_left hax]
          · have hxa : x ≤ a := Nat.le_of_not_ge hax
            simp [Nat.min_eq_right hxa]
        · simp [hy]
      exact ⟨y, hy', hval⟩

theorem min?_mem_of_some (l : List Nat) (m : Nat) (hm : l.min? = some m) : m ∈ l := by
  cases l with
  | nil => simp [List.min?] at hm
  | cons x xs =>
      simp [List.min?] at hm
      rcases foldl_min_eq_head_or_mem xs x with ⟨y, hy, hval⟩
      rw [hval] at hm
      cases hm
      exact hy

theorem sound_resolve (c : Clock) (hc : sound c) (t : Nat) : sound (resolve c t) := by
  rcases hc with ⟨hnod, hstable, hfence⟩
  simp [sound, resolve] at ⊢
  constructor
  · exact hnod.erase t
  · constructor
    · exact hstable
    · intro x hx
      have hx' : x ∈ c.pending := List.mem_of_mem_erase hx
      exact hfence x hx'

theorem sound_events (c : Clock) (hc : sound c) (events : List Event) :
    sound (events.foldl change c) := by
  induction events generalizing c with
  | nil => exact hc
  | cons e es ih =>
      cases e with
      | allocate => exact ih (newTicket c) (sound_newTicket c hc)
      | finish t => exact ih (resolve c t) (sound_resolve c hc t)

theorem candidate_lt_of_mem (c : Clock) (hc : sound c) (t : Nat) (hmem : t ∈ c.pending) :
    candidate c < t := by
  cases hm : c.pending.min? with
  | none =>
      have hnil := min?_none_nil _ hm
      simp [hnil] at hmem
  | some m =>
      have hle := min?_getD_le c.pending c.allocated t hmem
      simp [hm] at hle
      simp [candidate, hm]
      have htpos : 0 < t := by
        have := (hc.2.2 t hmem).1
        omega
      omega

theorem candidate_le_allocated (c : Clock) (hc : sound c) : candidate c ≤ c.allocated := by
  cases hm : c.pending.min? with
  | none => simp [candidate, hm]
  | some m =>
      have hle := min?_le_of_all_le c.pending c.allocated c.allocated (Nat.le_refl _)
        (by intro x hx; exact (hc.2.2 x hx).2)
      simp [hm] at hle
      simp [candidate, hm]
      exact Nat.le_trans hle (Nat.le_succ _)

theorem candidate_le_resolve (c : Clock) (hc : sound c) (t : Nat) :
    candidate c ≤ candidate (resolve c t) := by
  cases hm : c.pending.min? with
  | none =>
      have hnil := min?_none_nil _ hm
      simp [resolve, candidate, hnil]
  | some m =>
      cases hm2 : (c.pending.erase t).min? with
      | none =>
          have hmmem : m ∈ c.pending := min?_mem_of_some _ _ hm
          have hmle : m ≤ c.allocated := (hc.2.2 m hmmem).2
          simp [resolve, candidate, hm, hm2]
          omega
      | some m2 =>
          have hm2mem : m2 ∈ c.pending := List.mem_of_mem_erase (min?_mem_of_some _ _ hm2)
          have hle := min?_getD_le c.pending c.allocated m2 hm2mem
          simp [hm] at hle
          have hm2pos : 0 < m2 := by
            have := (hc.2.2 m2 hm2mem).1
            omega
          simp [resolve, candidate, hm, hm2]
          omega

theorem candidate_step (c : Clock) (e : Event) (hc : sound c) :
    candidate c ≤ candidate (change c e) := by
  cases e with
  | allocate =>
      change candidate c ≤ candidate (newTicket c)
      rw [candidate_newTicket c hc]
      exact Nat.le_refl _
  | finish t => exact candidate_le_resolve c hc t

theorem candidate_events (c : Clock) (hc : sound c) (events : List Event) :
    candidate c ≤ candidate (events.foldl change c) := by
  induction events generalizing c with
  | nil => exact Nat.le_refl _
  | cons e es ih =>
      exact Nat.le_trans (candidate_step c e hc) (ih (change c e) (by
        cases e with
        | allocate => exact sound_newTicket c hc
        | finish t => exact sound_resolve c hc t))

theorem foldl_resolve_pending (c : Clock) (order : List Nat) :
    (order.foldl resolve c).pending = order.foldl (fun acc t => acc.erase t) c.pending := by
  induction order generalizing c with
  | nil => rfl
  | cons t ts ih =>
      simp [List.foldl, resolve, ih]

theorem count_foldl_erase (l order : List Nat) (a : Nat) :
    (order.foldl (fun acc t => acc.erase t) l).count a = l.count a - order.count a := by
  induction order generalizing l with
  | nil => simp
  | cons t ts ih =>
      rw [List.foldl_cons, ih, List.count_erase]
      by_cases hta : t = a
      · subst t
        simp
        omega
      · simp [hta]

theorem foldl_resolve_alloc (c : Clock) (order : List Nat) :
    (order.foldl resolve c).allocated = c.allocated := by
  induction order generalizing c with
  | nil => rfl
  | cons t ts ih =>
      change (ts.foldl resolve (resolve c t)).allocated = c.allocated
      have halloc : (resolve c t).allocated = c.allocated := rfl
      rw [← halloc]
      exact ih (resolve c t)

theorem sound_goal : AllocateBeforeSnapshotAdmission.Goal := by
  intro c hc
  exact ⟨sound_newTicket c hc, candidate_newTicket c hc⟩

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_Publication_AllocateBeforeSnapshotAdmission
