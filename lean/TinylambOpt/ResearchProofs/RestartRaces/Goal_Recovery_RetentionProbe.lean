import TinylambOpt.ResearchGoals.RestartRaces.Recovery

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `RetentionProbe` — Are no pending commits and one pending commit at a time a complete
  test for every possible future commit subset? If true, a retention certificate for
  after-images needs only linearly many crash outcomes per key. Stamps are globally
  unique image identities shared by old and new representations.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Recovery_RetentionProbe

open TinylambOpt.ResearchGoals.RestartRaces.Recovery.RetentionProbe

private def optMax (a b : Option Nat) : Option Nat :=
  match a, b with
  | none, x => x
  | x, none => x
  | some x, some y => some (max x y)

private theorem max?_ne_none {l : List Nat} (h : l ≠ []) : l.max? ≠ none := by
  cases l with
  | nil => contradiction
  | cons a as => simp [List.max?]

private theorem init_le_foldl_max (l : List Nat) (c : Nat) : c ≤ l.foldl max c := by
  induction l generalizing c with
  | nil => simp
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans (Nat.le_max_left c a) (ih (max c a))

private theorem mem_le_foldl_max (l : List Nat) (c x : Nat) (h : x ∈ l) : x ≤ l.foldl max c := by
  induction l generalizing c x with
  | nil => simp at h
  | cons a l ih =>
      simp only [List.mem_cons] at h
      simp only [List.foldl_cons]
      rcases h with hxa | hx
      · subst x
        exact Nat.le_trans (Nat.le_max_right c a) (init_le_foldl_max l (max c a))
      · exact ih (max c a) x hx

private theorem foldl_max_le (l : List Nat) (c t : Nat) (h : ∀ x ∈ l, x ≤ t) (hc : c ≤ t) :
    l.foldl max c ≤ t := by
  induction l generalizing c with
  | nil => exact hc
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact ih (max c a) (fun x hx => h x (by simp [hx]))
        (Nat.max_le.mpr ⟨hc, h a (by simp)⟩)

private theorem mem_le_maxD (l : List Nat) (x : Nat) (h : x ∈ l) : x ≤ (l.max?).getD 0 := by
  cases l with
  | nil => simp at h
  | cons a as =>
      simp only [List.max?, List.foldl_cons, Option.getD_some]
      simp only [List.mem_cons] at h
      rcases h with hxa | hx
      · subst x
        exact init_le_foldl_max as a
      · exact mem_le_foldl_max as a x hx

private theorem maxD_le (l : List Nat) (t : Nat) (h : ∀ x ∈ l, x ≤ t) : (l.max?).getD 0 ≤ t := by
  cases l with
  | nil => simp [List.max?]
  | cons a as =>
      simp [List.max?]
      exact foldl_max_le as a t (fun x hx => h x (by simp [hx])) (h a (by simp))

private theorem max?_eq_of_mem {l l' : List Nat} (h : ∀ x, x ∈ l ↔ x ∈ l') : l.max? = l'.max? := by
  by_cases hn : l = []
  · subst l
    have hn' : l' = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro x hx
      have hx' : x ∈ ([] : List Nat) := (h x).mpr hx
      simp at hx'
    simp [hn']
  · have hn' : l' ≠ [] := by
      intro hz
      apply hn
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro x hx
      have hx' : x ∈ l' := (h x).mp hx
      simpa [hz] using hx'
    have hle1 : (l.max?).getD 0 ≤ (l'.max?).getD 0 := by
      apply maxD_le
      intro x hx
      exact mem_le_maxD l' x ((h x).mp hx)
    have hle2 : (l'.max?).getD 0 ≤ (l.max?).getD 0 := by
      apply maxD_le
      intro x hx
      exact mem_le_maxD l x ((h x).mpr hx)
    cases hm : l.max? with
    | none => exact (max?_ne_none hn hm).elim
    | some a =>
        cases hm' : l'.max? with
        | none => exact (max?_ne_none hn' hm').elim
        | some b =>
            have hab : a = b := Nat.le_antisymm (by simpa [hm, hm'] using hle1)
              (by simpa [hm, hm'] using hle2)
            simp [hm, hm', hab]

private theorem foldl_max_append (l l' : List Nat) (c : Nat) :
    (l ++ l').foldl max c = l'.foldl max (l.foldl max c) := by
  induction l generalizing c with
  | nil => simp
  | cons a l ih =>
      simp only [List.foldl_cons, List.cons_append]
      exact ih (max c a)

private theorem foldl_max_eq (l : List Nat) (c : Nat) : l.foldl max c = max c ((l.max?).getD 0) := by
  cases l with
  | nil => simp [List.max?]
  | cons a as =>
      simp only [List.max?, List.foldl_cons, Option.getD_some]
      rw [foldl_max_eq as (max c a)]
      rw [foldl_max_eq as a]
      simpa [List.max?] using (Nat.max_assoc c a (as.foldl max 0))

private theorem max_append (l l' : List Nat) : (l ++ l').max? = optMax l.max? l'.max? := by
  cases l with
  | nil => simp [optMax]
  | cons a as =>
      cases l' with
      | nil => simp [optMax]
      | cons b bs =>
          have h1 : (a :: as).max? = some ((a :: as).foldl max 0) := by simp [List.max?]
          have h2 : (b :: bs).max? = some ((b :: bs).foldl max 0) := by simp [List.max?]
          have h3 : ((a :: as) ++ (b :: bs)).max? = some (((a :: as) ++ (b :: bs)).foldl max 0) := by
            simp [List.max?]
          rw [h1, h2, h3]
          simp [optMax]
          rw [foldl_max_eq bs (max (List.foldl max a as) b)]
          rw [foldl_max_eq bs b]
          exact Nat.max_assoc (List.foldl max a as) b (bs.max?.getD 0)

private theorem max_filter_or (xs : List Image) (p q : Image → Bool) :
    ((xs.filter (fun x => p x || q x)).map Image.stamp).max? =
      optMax ((xs.filter p).map Image.stamp).max? ((xs.filter q).map Image.stamp).max? := by
  rw [← max_append]
  apply max?_eq_of_mem
  intro x
  simp [List.mem_filter, List.mem_map, List.mem_append]
  constructor
  · rintro ⟨a, ⟨ha, hpq⟩, rfl⟩
    rcases hpq with hp | hq
    · exact Or.inl ⟨a, ⟨ha, hp⟩, rfl⟩
    · exact Or.inr ⟨a, ⟨ha, hq⟩, rfl⟩
  · rintro (⟨a, ⟨ha, hp⟩, rfl⟩ | ⟨a, ⟨ha, hq⟩, rfl⟩)
    · exact ⟨a, ⟨ha, Or.inl hp⟩, rfl⟩
    · exact ⟨a, ⟨ha, Or.inr hq⟩, rfl⟩

private def elig (finish : List Nat) (x : Image) : Bool := x.owner.all finish.contains

private theorem elig_cons_eq (o : Nat) (finish : List Nat) (x : Image) :
    elig (o :: finish) x = (elig [o] x || elig finish x) := by
  cases hown : x.owner with
  | none => simp [elig, hown]
  | some val =>
      by_cases hval : val = o <;> by_cases hfin : val ∈ finish <;> simp [elig, hown, hval, hfin]

private theorem winner_cons (xs : List Image) (o : Nat) (finish : List Nat) :
    winner xs (o :: finish) = optMax (winner xs [o]) (winner xs finish) := by
  unfold winner
  have hfilt : xs.filter (elig (o :: finish)) = xs.filter (fun x => elig [o] x || elig finish x) := by
    induction xs with
    | nil => rfl
    | cons x xs ih =>
        simp only [List.filter_cons]
        rw [ih, elig_cons_eq]
  change ((xs.filter (elig (o :: finish))).map Image.stamp).max? =
    optMax ((xs.filter (elig [o])).map Image.stamp).max?
      ((xs.filter (elig finish)).map Image.stamp).max?
  rw [hfilt]
  exact max_filter_or xs (elig [o]) (elig finish)

theorem sound : Goal := by
  intro before after openOwners
  constructor
  · rintro ⟨hbase, hsingle⟩ finish hsub
    induction finish with
    | nil => exact hbase
    | cons o os ih =>
        have hso : o ∈ openOwners := hsub o (by simp)
        have hs1 := hsingle o hso
        have hsubos : ∀ x ∈ os, x ∈ openOwners := by
          intro x hx
          exact hsub x (List.mem_cons_of_mem o hx)
        have ihos := ih hsubos
        rw [winner_cons before o os, winner_cons after o os, hs1, ihos]
  · intro hfin
    constructor
    · exact hfin [] (by simp)
    · intro o ho
      exact hfin [o] (by
        intro x hx
        have hxeq : x = o := by simpa using hx
        simpa [hxeq] using ho)

end TinylambOpt.ResearchProofs.RestartRaces.Recovery_RetentionProbe
