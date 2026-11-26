import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# SparsePhraseEngineCounters

Counting engine IDs inside each phrase posting recovers the composite-key
histogram restricted to that phrase. The executor can therefore keep one
counter per engine in a sparse phrase index and derive the per-phrase
histogram without scanning unrelated phrases; first-occurrence order and
multiplicity are preserved.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.SparsePhraseEngineCounters

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates



set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

open TinylambOpt.ResearchGoals.QueryRewrites

theorem filter_filter_comm {α : Type} (p q : α → Bool) (l : List α) :
    (l.filter p).filter q = (l.filter q).filter p := by
  simp [List.filter_filter, Bool.and_comm]

theorem filter_filter_ne_of_false {α : Type} [BEq α] [LawfulBEq α]
    (a : α) (l : List α) (p : α → Bool) (hp : p a = false) :
    (l.filter p).filter (fun x => !x == a) = l.filter p := by
  induction l with
  | nil => simp
  | cons b l ih =>
      have ih' : (List.filter (fun x => !x == a && p x) l) = l.filter p := by
        simpa [List.filter_filter] using ih
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        simpa [hp] using congrArg (fun xs => a :: xs) ih'
      · have hnb : (!b == a) = true := by simpa using hba
        by_cases hpb : p b = true <;> simp [hnb, hpb, ih']

theorem filter_eraseDups_eq {α : Type} [BEq α] [LawfulBEq α]
    (p : α → Bool) : ∀ l : List α, (l.filter p).eraseDups = (l.eraseDups).filter p := by
  intro l
  generalize hn : l.length = n
  revert l
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hn
      cases l with
      | nil => simp
      | cons a l =>
          have hlen : (List.filter (fun x => !x == a) l).length < n := by
            rw [← hn]
            simp
            exact Nat.lt_of_le_of_lt (List.length_filter_le _ l) (by simp)
          have hrec := ih (List.filter (fun x => !x == a) l).length hlen
            (List.filter (fun x => !x == a) l) rfl
          simp only [List.filter_cons]
          by_cases hp : p a = true
          · rw [ite_eq_left hp]
            rw [List.eraseDups_cons (a := a) (as := l.filter p)]
            rw [List.eraseDups_cons (a := a) (as := l)]
            simp only [List.filter_cons, hp, Bool.true_and]
            rw [filter_filter_comm]
            exact congrArg (fun xs => a :: xs) hrec
          · have hpfalse : p a = false := by
              cases hpa : p a <;> simp [hpa] at hp ⊢
            rw [ite_eq_right hp]
            rw [List.eraseDups_cons (a := a) (as := l)]
            simp only [List.filter_cons, hp, Bool.false_and]
            have hl : l.filter p = (List.filter (fun x => !x == a) l).filter p := by
              rw [filter_filter_comm]
              exact (filter_filter_ne_of_false a l p hpfalse).symm
            rw [hl]
            exact hrec

theorem filter_frequency {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (p : α → Bool) :
    (frequency xs).filter (fun r => p r.1) = frequency (xs.filter p) := by
  simp only [frequency]
  rw [filter_eraseDups_eq]
  rw [List.filter_map]
  apply List.map_congr_left
  intro x hx
  have hpx : p x = true := (List.mem_filter.mp hx).2
  rw [List.count_filter hpx]

theorem filter_fst_ne_eq {α : Type} [BEq α] [LawfulBEq α] (e : α) (phrase : String) :
    ∀ rs : List (α × String), (∀ r ∈ rs, (r.2 == phrase) = true) →
    (List.filter (fun b => !b == e) (rs.map Prod.fst)) =
      (rs.filter (fun b : α × String => !b == (e, phrase))).map Prod.fst := by
  intro rs
  induction rs with
  | nil => intro hall; simp
  | cons r rs ih =>
      intro hall
      rcases r with ⟨a, t⟩
      have ht : t = phrase := beq_iff_eq.mp (hall (a,t) (by simp))
      subst t
      have hall_tail : ∀ r ∈ rs, (r.2 == phrase) = true := by
        intro r hr; exact hall r (by simp [hr])
      by_cases hae : a == e
      · have h : a = e := beq_iff_eq.mp hae
        subst a
        simp [ih hall_tail]
      · have hneProp : a ≠ e := by intro h; apply hae; simp [h]
        have hne : ((a, phrase) == (e, phrase)) = false := by simp [hneProp]
        simp [hae, hneProp, hne, ih hall_tail]

theorem eraseDups_map_fst_fixed {α : Type} [BEq α] [LawfulBEq α]
    (phrase : String) : ∀ rs : List (α × String),
    (∀ r ∈ rs, (r.2 == phrase) = true) →
    (rs.map Prod.fst).eraseDups.map (fun e => (e, phrase)) = rs.eraseDups := by
  intro rs hall
  generalize hn : rs.length = n
  revert rs
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro rs hall hn
      cases rs with
      | nil => simp
      | cons r rs =>
          rcases r with ⟨e, s⟩
          have hs : s = phrase := beq_iff_eq.mp (hall (e, s) (by simp))
          subst s
          have hall_tail : ∀ r ∈ rs, (r.2 == phrase) = true := by
            intro r hr; exact hall r (by simp [hr])
          have hlen : (rs.filter (fun b : α × String => !b == (e, phrase))).length < n := by
            have hle := List.length_filter_le (fun b : α × String => !b == (e, phrase)) rs
            simp at hn
            omega
          have hrec := ih (rs.filter (fun b : α × String => !b == (e, phrase))).length hlen
            (rs.filter (fun b : α × String => !b == (e, phrase)))
            (by intro r hr; exact hall_tail r (List.mem_filter.mp hr).1) rfl
          simp only [List.map_cons]
          rw [List.eraseDups_cons, List.eraseDups_cons]
          rw [filter_fst_ne_eq e phrase rs hall_tail]
          simpa [hrec]

theorem count_map_fst_fixed {α : Type} [BEq α] [LawfulBEq α]
    (phrase : String) : ∀ rs : List (α × String),
    (∀ r ∈ rs, (r.2 == phrase) = true) → ∀ e : α,
    (rs.map Prod.fst).count e = rs.count (e, phrase) := by
  intro rs
  induction rs with
  | nil => intro hall e; simp
  | cons r rs ih =>
      intro hall e
      rcases r with ⟨a, s⟩
      have hs : s = phrase := beq_iff_eq.mp (hall (a,s) (by simp))
      subst s
      have hall_tail : ∀ r ∈ rs, (r.2 == phrase) = true := by
        intro r hr; exact hall r (by simp [hr])
      by_cases hae : a == e
      · have h : a = e := beq_iff_eq.mp hae
        subst a
        simp [ih hall_tail]
      · have hneProp : a ≠ e := by intro h; apply hae; simp [h]
        have hne : ((a, phrase) == (e, phrase)) = false := by simp [hneProp]
        simp [hae, hneProp, hne, ih hall_tail]

theorem mapped_frequency_fixed {α : Type} [BEq α] [LawfulBEq α]
    (phrase : String) (rs : List (α × String)) :
    (∀ r ∈ rs, (r.2 == phrase) = true) →
    (rs.map Prod.fst).eraseDups.map (fun e => ((e, phrase), (rs.map Prod.fst).count e)) =
      rs.eraseDups.map (fun r => (r, rs.count r)) := by
  intro hall
  calc
    (rs.map Prod.fst).eraseDups.map (fun e => ((e, phrase), (rs.map Prod.fst).count e))
        = ((rs.map Prod.fst).eraseDups.map (fun e => (e, phrase))).map
            (fun r => (r, (rs.map Prod.fst).count r.1)) := by
              rw [List.map_map]
              rfl
    _ = rs.eraseDups.map (fun r => (r, (rs.map Prod.fst).count r.1)) := by
              rw [eraseDups_map_fst_fixed phrase rs hall]
    _ = rs.eraseDups.map (fun r => (r, rs.count r)) := by
              apply List.map_congr_left
              intro r hr
              have hrp : r ∈ rs := List.mem_eraseDups.mp hr
              have hrs : (r.2 == phrase) = true := hall r hrp
              have hrs_eq : (r.1, phrase) = r := by
                cases r with
                | mk a s =>
                    have hs : s = phrase := beq_iff_eq.mp hrs
                    subst s
                    rfl
              rw [count_map_fst_fixed phrase rs hall r.1]
              rw [hrs_eq]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.SparsePhraseEngineCounters.Goal := by
  intro rs phrase
  have hall : ∀ r ∈ rs.filter (fun r : Nat × String => r.2 == phrase), (r.2 == phrase) = true := by
    intro r hr
    exact (List.mem_filter.mp hr).2
  rw [filter_frequency rs (fun r : Nat × String => r.2 == phrase)]
  simp only [frequency, List.map_map]
  change ((rs.filter (fun r : Nat × String => r.2 == phrase)).map Prod.fst).eraseDups.map
      (fun e => ((e, phrase), ((rs.filter (fun r : Nat × String => r.2 == phrase)).map Prod.fst).count e)) =
    (rs.filter (fun r : Nat × String => r.2 == phrase)).eraseDups.map
      (fun r => (r, (rs.filter (fun r : Nat × String => r.2 == phrase)).count r))
  rw [mapped_frequency_fixed phrase (rs.filter (fun r : Nat × String => r.2 == phrase)) hall]
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.SparsePhraseEngineCounters
