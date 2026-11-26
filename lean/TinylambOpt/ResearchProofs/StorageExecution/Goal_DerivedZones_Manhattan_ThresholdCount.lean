import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_ThresholdCount

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

abbrev Page := List (Option Row) × Bounds × Bounds

-- Small-input truth checks before attempting the proof.
private def smallBounds : List Bounds :=
  (List.range 4).flatMap fun lo =>
    ((List.range 4).filter (fun hi => decide (lo ≤ hi))).map fun hi => ⟨lo, hi⟩

private def siCounterexamples : List (Bounds × Bounds × Nat × Nat × Nat) :=
  smallBounds.flatMap fun a =>
  smallBounds.flatMap fun b =>
  (List.range 4).flatMap fun t =>
  (List.range 4).flatMap fun x =>
  (List.range 4).filterMap fun y =>
    if decide (a.lo ≤ x ∧ x ≤ a.hi ∧ b.lo ≤ y ∧ y ≤ b.hi ∧
        ¬ ((kernel.bounds t a b).lo ≤ kernel.score t (x, y) ∧
           kernel.score t (x, y) ≤ (kernel.bounds t a b).hi)) then
      some (a, b, t, x, y)
    else none

#eval siCounterexamples.length
#guard siCounterexamples = []

-- A concrete set of covered pages, checked against every small cut.
private def samplePages : List Page :=
  [([some (0, 0), none, some (1, 3)], ⟨0, 1⟩, ⟨0, 3⟩),
   ([some (2, 4)], ⟨2, 2⟩, ⟨4, 4⟩),
   ([none], ⟨0, 0⟩, ⟨0, 0⟩)]

private def tcCheck (cut : Nat) : Bool :=
  let n := fun (e : Page) => (e.1.filterMap id).length
  let actual := ((samplePages.map Prod.fst).flatten.filterMap
    (fun r => r.map (kernel.score 2))).countP (· ≥ cut)
  decide (((samplePages.filter (fun e => cut ≤ (kernel.bounds 2 e.2.1 e.2.2).lo)).map n).sum ≤ actual ∧
    actual ≤ ((samplePages.filter (fun e => cut ≤ (kernel.bounds 2 e.2.1 e.2.2).hi)).map n).sum)

#eval (List.range 10).map tcCheck
#guard (List.range 10).all tcCheck

/-- The Manhattan score at a point lies inside the certified interval. -/
private theorem sound_SoundInterval : Manhattan.SoundInterval.Goal := by
  intro t a b x y hax hby
  obtain ⟨haxlo, haxhi⟩ := hax
  obtain ⟨hblo, hbhi⟩ := hby
  simp only [inside, kernel, gapMin, gapMax, gap]
  constructor <;> omega

private theorem inside_scores {f : Kernel} (hf : soundInterval f) {t p a b} (hc : covers p a b) :
    ∀ v ∈ scores f t p, inside (f.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hr, hm⟩ : ∃ r ∈ p, r.map (f.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ f.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
      cases xy with
      | mk x y => exact ⟨x, y, hr, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact hf t a b x y hax hby

private theorem length_scores {f : Kernel} {t p} : (scores f t p).length = (p.filterMap id).length := by
  induction p with
  | nil => rfl
  | cons o rest ih =>
    cases o with
    | none => simpa [scores] using ih
    | some xy => simpa [scores] using ih

private theorem countP_len_of {l : List Nat} {p : Nat → Bool} (h : ∀ v ∈ l, p v) :
    l.countP p = l.length := by
  rw [List.countP_eq_length]
  exact h

private theorem countP_zero_of {l : List Nat} {p : Nat → Bool} (h : ∀ v ∈ l, ¬ p v) :
    l.countP p = 0 := by
  rw [List.countP_eq_zero]
  exact h

private theorem sum_le_sum_of {α : Type} (ps : List α) {g h : α → Nat} (hh : ∀ e ∈ ps, g e ≤ h e) :
    (ps.map g).sum ≤ (ps.map h).sum := by
  induction ps with
  | nil => simp
  | cons a t ih =>
    rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
    exact Nat.add_le_add (hh a (List.mem_cons_self (l := t)))
      (ih (fun e he => hh e (List.mem_cons_of_mem a he)))

private theorem sum_filtered_eq_if {α : Type} (ps : List α) (c : α → Prop) [DecidablePred c]
    (n : α → Nat) :
    ((ps.filter c).map n).sum = (ps.map (fun e => if c e then n e else 0)).sum := by
  induction ps with
  | nil => rfl
  | cons a t ih =>
    by_cases h : c a
    · rw [List.filter_cons_of_pos (by simpa using h), List.map_cons, List.sum_cons, ih,
        List.map_cons, List.sum_cons, if_pos h]
    · rw [List.filter_cons_of_neg (by simpa using h), List.map_cons, List.sum_cons, ih, if_neg h]
      rw [Nat.zero_add]

private theorem countP_map_flatten {f : Kernel} {t : Nat} {cut : Nat}
    (ps : List Page) :
    ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      = (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum := by
  induction ps with
  | nil => rfl
  | cons e rest ih =>
    obtain ⟨p, ab, bb⟩ := e
    change List.countP (fun x => decide (x ≥ cut))
        ((p ++ (List.map Prod.fst rest).flatten).filterMap (fun r => Option.map (f.score t) r)) = _
    rw [List.filterMap_append, List.countP_append, ih, List.map_cons, List.sum_cons]
    simp only [scores]

/-- A sound interval suffices for the threshold count bounds. -/
private theorem thresholdCount_of (f : Kernel) (hf : soundInterval f) : thresholdCount f := by
  intro t ps cut hall
  show ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map
        (fun e => (e.1.filterMap id).length)).sum
      ≤ ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      ∧ ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      ≤ ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).hi)).map
        (fun e => (e.1.filterMap id).length)).sum
  have hact : ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
      = (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum :=
    countP_map_flatten ps
  rw [hact]
  refine ⟨?_, ?_⟩
  · have h1 : (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).lo
        then (e.1.filterMap id).length else 0)).sum
        ≤ (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum :=
      sum_le_sum_of ps (fun e he => by
        by_cases hc2 : cut ≤ (f.bounds t e.2.1 e.2.2).lo
        · rw [if_pos hc2]
          rw [← length_scores (f := f) (t := t) (p := e.1)]
          rw [countP_len_of (fun v hv => by
            simpa using Nat.le_trans hc2 (inside_scores hf (hall e he) v hv).1)]
          exact Nat.le_refl _
        · rw [if_neg hc2]
          exact Nat.zero_le _)
    have h2 : List.sum ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map
          (fun e => (e.1.filterMap id).length))
        = List.sum (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).lo
            then (e.1.filterMap id).length else 0)) :=
      sum_filtered_eq_if ps (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)
        (fun e => (e.1.filterMap id).length)
    rw [h2]; exact h1
  · have h1 : (ps.map (fun e => (scores f t e.1).countP (· ≥ cut))).sum
        ≤ (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).hi
            then (e.1.filterMap id).length else 0)).sum :=
      sum_le_sum_of ps (fun e he => by
        by_cases hc2 : cut ≤ (f.bounds t e.2.1 e.2.2).hi
        · rw [if_pos hc2, ← length_scores (f := f) (t := t) (p := e.1)]
          exact List.countP_le_length
        · rw [if_neg hc2]
          rw [countP_zero_of (fun v hv hcut => by
            simpa using hc2 (Nat.le_trans (by simpa using hcut)
              (inside_scores hf (hall e he) v hv).2))]
          exact Nat.le_refl _)
    have h2 : List.sum ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).hi)).map
          (fun e => (e.1.filterMap id).length))
        = List.sum (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).hi
            then (e.1.filterMap id).length else 0)) :=
      sum_filtered_eq_if ps (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).hi)
        (fun e => (e.1.filterMap id).length)
    rw [h2]; exact h1

theorem sound_ThresholdCount : Manhattan.ThresholdCount.Goal :=
  thresholdCount_of Manhattan.kernel sound_SoundInterval

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_ThresholdCount
