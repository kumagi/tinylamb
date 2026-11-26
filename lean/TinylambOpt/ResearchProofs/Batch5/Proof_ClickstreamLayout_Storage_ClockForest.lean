import TinylambOpt.ResearchGoals.ClickstreamLayout.Storage

namespace TinylambOpt.ResearchProofs.Batch5.ClickstreamLayout.Storage.Goal_ClockForest

open TinylambOpt.ResearchGoals.ClickstreamLayout.Storage


/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Proved

- `ClockForest` — Can the writer minimize actual directed payload bytes over *acyclic*
  anchor choices? The optimum must be decodable and beat every legal assignment.
  Scan-time read amplification is a separate objective.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace Scratch.ClickstreamLayout.ClockForest

open TinylambOpt.ResearchGoals.ClickstreamLayout.Storage
open TinylambOpt.ResearchGoals.ClickstreamLayout.Storage.ClockForest
open ClockForest

def LegD (D T : List Nat) (parent : Parent) (rank : Nat → Nat) : Prop :=
  ∀ c ∈ T, ∀ q, parent c = some q → q ∈ D ∨ (q ∈ T ∧ rank q < rank c)

theorem sum_erase (g : Nat → Nat) (T : List Nat) (c : Nat) (h : c ∈ T) :
    (T.map g).sum = g c + ((T.erase c).map g).sum := by
  induction T with
  | nil => simp at h
  | cons x xs ih =>
    by_cases hx : x = c
    · subst hx; simp
    · have hc : c ∈ xs := by
        rcases List.mem_cons.mp h with rfl | h
        · exact absurd rfl hx
        · exact h
      have hbeq : (x == c) = false := by simpa using hx
      simp [List.erase_cons, hbeq]
      rw [ih hc]
      omega

theorem argmin_by (r : Nat → Nat) : ∀ L : List Nat, L ≠ [] → ∃ a ∈ L, ∀ b ∈ L, r a ≤ r b := by
  intro L
  induction L with
  | nil => intro h; exact absurd rfl h
  | cons x L ih =>
    intro _
    by_cases hL : L = []
    · subst hL; exact ⟨x, by simp, by simp⟩
    · obtain ⟨a, ha, hmin⟩ := ih hL
      by_cases hc : r x ≤ r a
      · refine ⟨x, by simp, ?_⟩
        intro b hb
        rcases List.mem_cons.mp hb with rfl | hb
        · exact Nat.le_refl _
        · exact Nat.le_trans hc (hmin b hb)
      · refine ⟨a, List.mem_cons_of_mem _ ha, ?_⟩
        intro b hb
        rcases List.mem_cons.mp hb with rfl | hb
        · omega
        · exact hmin b hb

theorem min_getD_le (L : List Nat) (x : Nat) (hx : x ∈ L) : L.min?.getD 0 ≤ x := by
  cases hm : L.min? with
  | none => rw [List.min?_eq_none_iff] at hm; subst hm; simp at hx
  | some m =>
    have := (List.min?_eq_some_iff.mp hm).2 x hx
    simpa using this

theorem min_getD_mem (L : List Nat) (hL : L ≠ []) : L.min?.getD 0 ∈ L := by
  cases hm : L.min? with
  | none => rw [List.min?_eq_none_iff] at hm; exact absurd hm hL
  | some m => simpa using (List.min?_eq_some_iff.mp hm).1

theorem cheapest_succ (price : Nat → Option Nat → Nat) (D : List Nat) (n : Nat) (T : List Nat) :
    cheapest price D (n+1) T = (T.map (fun c =>
        (price c none :: D.map (fun p => price c (some p))).min?.getD 0 +
          cheapest price (c :: D) n (T.erase c))).min?.getD 0 := rfl

theorem lb (price : Nat → Option Nat → Nat) : ∀ n (D T : List Nat), T.Nodup → T.length = n →
    ∀ (p : Parent) (rank : Nat → Nat), LegD D T p rank →
    cheapest price D n T ≤ total price T p := by
  intro n
  induction n with
  | zero =>
    intro D T _ hl p rank _
    have : T = [] := List.eq_nil_of_length_eq_zero hl
    subst this
    simp [cheapest]
  | succ n ih =>
    intro D T hn hl p rank hleg
    have hne : T ≠ [] := by intro h; subst h; simp at hl
    obtain ⟨c0, hc0, hmin⟩ := argmin_by rank T hne
    have hpar : ∀ q, p c0 = some q → q ∈ D := by
      intro q hq
      rcases hleg c0 hc0 q hq with h | ⟨hqT, hlt⟩
      · exact h
      · have := hmin q hqT; omega
    have hchoice : (price c0 none :: D.map (fun q => price c0 (some q))).min?.getD 0 ≤ price c0 (p c0) := by
      apply min_getD_le
      cases hp : p c0 with
      | none => simp
      | some q => exact List.mem_cons_of_mem _ (List.mem_map.mpr ⟨q, hpar q hp, rfl⟩)
    have hlen : (T.erase c0).length = n := by rw [List.length_erase_of_mem hc0]; omega
    have hleg' : LegD (c0 :: D) (T.erase c0) p rank := by
      intro c hc q hq
      have hcT : c ∈ T := List.mem_of_mem_erase hc
      have hcne : c ≠ c0 := ((hn.mem_erase_iff).mp hc).1
      rcases hleg c hcT q hq with h | ⟨hqT, hlt⟩
      · exact Or.inl (List.mem_cons_of_mem _ h)
      · by_cases hqc : q = c0
        · exact Or.inl (by simp [hqc])
        · exact Or.inr ⟨(hn.mem_erase_iff).mpr ⟨hqc, hqT⟩, hlt⟩
    have ih' := ih (c0 :: D) (T.erase c0) (hn.erase c0) hlen p rank hleg'
    have hF : cheapest price D (n+1) T ≤
        (price c0 none :: D.map (fun q => price c0 (some q))).min?.getD 0 +
          cheapest price (c0 :: D) n (T.erase c0) := by
      rw [cheapest_succ]
      apply min_getD_le
      exact List.mem_map.mpr ⟨c0, hc0, rfl⟩
    have hsum := sum_erase (fun c => price c (p c)) T c0 hc0
    unfold total at ih' ⊢
    omega

theorem ex (price : Nat → Option Nat → Nat) : ∀ n (D T : List Nat), T.Nodup → T.length = n →
    ∃ (p : Parent) (rank : Nat → Nat), LegD D T p rank ∧ total price T p = cheapest price D n T := by
  intro n
  induction n with
  | zero =>
    intro D T _ hl
    have : T = [] := List.eq_nil_of_length_eq_zero hl
    subst this
    exact ⟨fun _ => none, fun _ => 0, by intro c hc; simp at hc, by simp [total, cheapest]⟩
  | succ n ih =>
    intro D T hn hl
    have hne : T ≠ [] := by intro h; subst h; simp at hl
    have hLne : (T.map (fun c =>
        (price c none :: D.map (fun p => price c (some p))).min?.getD 0 +
          cheapest price (c :: D) n (T.erase c))) ≠ [] := by
      intro h; apply hne; exact List.map_eq_nil_iff.mp h
    obtain ⟨c0, hc0, hF⟩ := List.mem_map.mp (min_getD_mem _ hLne)
    have hcheap : cheapest price D (n+1) T =
        (price c0 none :: D.map (fun q => price c0 (some q))).min?.getD 0 +
          cheapest price (c0 :: D) n (T.erase c0) := by
      rw [cheapest_succ]; exact hF.symm
    -- the chosen option
    have hmem := min_getD_mem (price c0 none :: D.map (fun q => price c0 (some q))) (by simp)
    obtain ⟨o, ho, hov⟩ : ∃ o : Option Nat, (∀ q, o = some q → q ∈ D) ∧
        price c0 o = (price c0 none :: D.map (fun q => price c0 (some q))).min?.getD 0 := by
      rcases List.mem_cons.mp hmem with h | h
      · exact ⟨none, by simp, h.symm⟩
      · obtain ⟨q, hq, hqe⟩ := List.mem_map.mp h
        exact ⟨some q, by intro q' h'; cases h'; exact hq, hqe⟩
    have hlen : (T.erase c0).length = n := by rw [List.length_erase_of_mem hc0]; omega
    obtain ⟨p', rank', hleg', htot'⟩ := ih (c0 :: D) (T.erase c0) (hn.erase c0) hlen
    refine ⟨fun c => if c = c0 then o else p' c, fun c => if c = c0 then 0 else rank' c + 1, ?_, ?_⟩
    · intro c hc q hq
      by_cases hcc : c = c0
      · subst hcc
        simp at hq
        exact Or.inl (ho q hq)
      · simp only [hcc, if_false] at hq
        have hcE : c ∈ T.erase c0 := (hn.mem_erase_iff).mpr ⟨hcc, hc⟩
        rcases hleg' c hcE q hq with h | ⟨hqE, hlt⟩
        · rcases List.mem_cons.mp h with rfl | h
          · exact Or.inr ⟨hc0, by simp [hcc]⟩
          · exact Or.inl h
        · have hqne : q ≠ c0 := ((hn.mem_erase_iff).mp hqE).1
          have hqT : q ∈ T := ((hn.mem_erase_iff).mp hqE).2
          exact Or.inr ⟨hqT, by simp [hqne, hcc]; omega⟩
    · rw [hcheap, ← htot']
      unfold total
      rw [sum_erase _ T c0 hc0]
      have hcong : ((T.erase c0).map (fun c => price c (if c = c0 then o else p' c))).sum =
          ((T.erase c0).map (fun c => price c (p' c))).sum := by
        congr 1
        apply List.map_congr_left
        intro c hc
        have : c ≠ c0 := ((hn.mem_erase_iff).mp hc).1
        simp [this]
      rw [hcong]
      simp [hov]

theorem sound : TinylambOpt.ResearchGoals.ClickstreamLayout.Storage.ClockForest.Goal := by
  intro cols price hn
  dsimp only
  obtain ⟨p, rank, hleg, htot⟩ := ex price cols.length [] cols hn rfl
  refine ⟨⟨p, ⟨rank, fun c hc q hq => ?_⟩, htot⟩, ?_⟩
  · rcases hleg c hc q hq with h | h
    · simp at h
    · exact h
  · intro p ⟨rank, h⟩
    exact lb price cols.length [] cols hn rfl p rank (fun c hc q hq => Or.inr (h c hc q hq))

end Scratch.ClickstreamLayout.ClockForest
end TinylambOpt.ResearchProofs.Batch5.ClickstreamLayout.Storage.Goal_ClockForest
