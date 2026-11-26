import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `SearchFence` — Reuse the negative result only up to its certificate. The equivalence
  quantifies over every admissible completion of the *same* search frontier.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_SearchFence

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.SearchFence

theorem foldl_min_all (c init : Nat) (l : List Nat) :
    c ≤ l.foldl min init ↔ c ≤ init ∧ ∀ x ∈ l, c ≤ x := by
  induction l generalizing init with
  | nil => simp
  | cons b bs ih =>
    simp only [List.foldl_cons]
    rw [ih]
    constructor
    · intro h
      rcases h with ⟨hcm, hbs⟩
      constructor
      · exact Nat.le_trans hcm (Nat.min_le_left init b)
      · intro x hx
        simp only [List.mem_cons] at hx
        rcases hx with hxb | hx
        · subst x
          exact Nat.le_trans hcm (Nat.min_le_right init b)
        · exact hbs x hx
    · intro h
      rcases h with ⟨hci, hball⟩
      constructor
      · by_cases hib : init ≤ b
        · simpa [Nat.min_eq_left hib] using hci
        · have hbi : b ≤ init := by omega
          simpa [Nat.min_eq_right hbi] using hball b (by simp)
      · intro x hx
        exact hball x (by simp [hx])

theorem min?_all_prop (c : Nat) (l : List Nat) :
    ((l.min?.all (fun x => decide (c ≤ x))) = true) ↔ ∀ x ∈ l, c ≤ x := by
  cases l with
  | nil => simp
  | cons a as =>
    simp only [List.min?, Option.all]
    rw [decide_eq_true_eq]
    rw [foldl_min_all]
    simp [List.mem_cons]

theorem zip_fst_mem (bs : List Nat) (as : List (List Nat)) :
    ∀ p ∈ bs.zip as, p.1 ∈ bs := by
  induction bs generalizing as with
  | nil => intro p hp; cases hp
  | cons b bs ih =>
    cases as with
    | nil => simp
    | cons a as =>
      intro p hp
      simp only [List.zip_cons_cons, List.mem_cons] at hp
      rcases hp with hp | hp
      · subst p
        simp
      · have hb := ih as p hp
        simp [hb]

theorem snd_mem_zip (bs : List Nat) (as : List (List Nat)) (hlen : as.length = bs.length) :
    ∀ l ∈ as, ∃ b, (b,l) ∈ bs.zip as := by
  induction bs generalizing as with
  | nil =>
    intro l hl
    cases as with
    | nil => simp at hl
    | cons a as => simp at hlen
  | cons b bs ih =>
    cases as with
    | nil => simp at hlen
    | cons a as =>
      simp only [List.length_cons] at hlen
      have hlen' : as.length = bs.length := by omega
      intro l hl
      simp only [List.mem_cons] at hl
      rcases hl with hla | hlas
      · subst l
        exact ⟨b, by simp⟩
      · obtain ⟨b', hb'⟩ := ih as hlen' l hlas
        exact ⟨b', by simp [hb']⟩

theorem zip_map_singleton (bs : List Nat) :
    ∀ p ∈ bs.zip (bs.map (fun b => [b])), ∃ b ∈ bs, p = (b, [b]) := by
  induction bs with
  | nil => intro p hp; cases hp
  | cons b bs ih =>
    intro p hp
    simp only [List.map_cons, List.zip_cons_cons, List.mem_cons] at hp
    rcases hp with hp | hp
    · subst p
      exact ⟨b, by simp, rfl⟩
    · obtain ⟨b', hb', rfl⟩ := ih p hp
      exact ⟨b', by simp [hb'], rfl⟩

theorem flatten_map_singleton (bs : List Nat) :
    (bs.map (fun b => [b])).flatten = bs := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

theorem sound : Goal := by
  intro done bounds cutoff
  change ((certificate done bounds).all (fun x => decide (cutoff ≤ x)) = true) ↔ _
  constructor
  · intro h actual hlen hbound
    have hall : ∀ x ∈ done ++ bounds, cutoff ≤ x := by
      dsimp [certificate] at h
      rwa [min?_all_prop] at h
    dsimp [canImprove]
    cases hres : (done ++ actual.flatten).any (fun x => decide (x < cutoff)) with
    | false => rfl
    | true =>
      rw [List.any_eq_true] at hres
      obtain ⟨x, hxmem, hxlt⟩ := hres
      simp only [decide_eq_true_eq] at hxlt
      have hxge : cutoff ≤ x := by
        simp only [List.mem_append] at hxmem
        rcases hxmem with hd | hf
        · exact hall x (List.mem_append_left _ hd)
        · obtain ⟨l, hl, hxl⟩ := List.mem_flatten.mp hf
          obtain ⟨b, hbz⟩ := snd_mem_zip bounds actual hlen l hl
          have hbmem : b ∈ bounds := zip_fst_mem bounds actual (b,l) hbz
          have hbge : cutoff ≤ b := hall b (List.mem_append_right _ hbmem)
          have hbv : b ≤ x := hbound (b,l) hbz x hxl
          exact Nat.le_trans hbge hbv
      omega
  · intro h
    have hall : ∀ x ∈ done ++ bounds, cutoff ≤ x := by
      let actual : List (List Nat) := bounds.map (fun b => [b])
      have hlen : actual.length = bounds.length := by simp [actual]
      have hb : ∀ p ∈ bounds.zip actual, ∀ v ∈ p.2, p.1 ≤ v := by
        intro p hp v hv
        have hp' : p ∈ bounds.zip (bounds.map (fun b => [b])) := by simpa [actual] using hp
        obtain ⟨b, hbmem, rfl⟩ := zip_map_singleton bounds p hp'
        simp at hv
        omega
      have hres := h actual hlen hb
      dsimp [canImprove] at hres
      rw [flatten_map_singleton] at hres
      intro x hx
      cases hany : (done ++ bounds).any (fun y => decide (y < cutoff)) with
      | true => rw [hany] at hres; contradiction
      | false =>
        have hnot : ¬ ∃ y ∈ done ++ bounds, y < cutoff := by
          intro ⟨y, hy, hylt⟩
          have ht : (done ++ bounds).any (fun y => decide (y < cutoff)) = true :=
            List.any_eq_true.mpr ⟨y, hy, by simp [hylt]⟩
          rw [hany] at ht
          contradiction
        by_cases hcx : cutoff ≤ x
        · exact hcx
        · exfalso
          have hxlt : x < cutoff := by omega
          exact hnot ⟨x, hx, hxlt⟩
    unfold certificate
    change ((done ++ bounds).min?.all (fun x => decide (cutoff ≤ x))) = true
    rwa [min?_all_prop]

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_SearchFence

-- C++ references to this model or its proof.
-- plan/optimizer_decisions.hpp
