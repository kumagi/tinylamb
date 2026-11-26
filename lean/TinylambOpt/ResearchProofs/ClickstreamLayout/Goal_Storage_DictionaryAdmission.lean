import TinylambOpt.ResearchGoals.ClickstreamLayout.Storage

/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Proved

- `DictionaryAdmission` — Select frames that amortize both dictionary storage and cold
  projection cost. The tree recurrence must bound every admission and attain that
  bound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickstreamLayout.Storage_DictionaryAdmission

open TinylambOpt.ResearchGoals.ClickstreamLayout.Storage
open TinylambOpt.ResearchGoals.ClickstreamLayout.Storage.DictionaryAdmission
open DictionaryAdmission

theorem flag0 (u : Nat → Bool) : ∀ t, (score u t).1 = false → (score u t).2 = 0 := by
  intro t
  induction t with
  | frame i s =>
    intro h
    simp only [score] at h ⊢
    split at h <;> simp_all
  | bundle c l r ihl ihr =>
    intro h
    simp only [score] at h ⊢
    have hl := ihl
    have hr := ihr
    by_cases ha : (score u l).1 = true
    · simp [ha] at h
    · by_cases hb : (score u r).1 = true
      · simp [hb] at h
      · have ha' : (score u l).1 = false := by simpa using ha
        have hb' : (score u r).1 = false := by simpa using hb
        have := hl ha'
        have := hr hb'
        simp [ha', hb', *]

theorem bound (u : Nat → Bool) : ∀ t, (score u t).2 ≤ best t := by
  intro t
  induction t with
  | frame i s =>
    simp only [score, best]
    split
    · exact Int.le_max_right _ _
    · exact Int.le_max_left _ _
  | bundle c l r ihl ihr =>
    simp only [score, best]
    have h1 := ihl
    have h2 := ihr
    by_cases hf : ((score u l).1 || (score u r).1) = true
    · simp only [hf, ite_true]
      omega
    · have hf' : ((score u l).1 || (score u r).1) = false := by simpa using hf
      have ha : (score u l).1 = false := by simpa using (Bool.or_eq_false_iff.mp hf').1
      have hb : (score u r).1 = false := by simpa using (Bool.or_eq_false_iff.mp hf').2
      have := flag0 u l ha
      have := flag0 u r hb
      simp only [hf, ite_false]
      omega

theorem score_congr (u v : Nat → Bool) : ∀ t, (∀ i ∈ ids t, u i = v i) → score u t = score v t := by
  intro t
  induction t with
  | frame i s =>
    intro h
    simp only [score]
    rw [h i (by simp [ids])]
  | bundle c l r ihl ihr =>
    intro h
    simp only [score]
    rw [ihl (fun i hi => h i (by simp [ids, hi])), ihr (fun i hi => h i (by simp [ids, hi]))]

theorem score_false : ∀ t, score (fun _ => false) t = (false, 0) := by
  intro t
  induction t with
  | frame i s => simp [score]
  | bundle c l r ihl ihr => simp [score, ihl, ihr]

theorem attain : ∀ t, (ids t).Nodup → ∃ u, (score u t).2 = best t := by
  intro t
  induction t with
  | frame i s =>
    intro _
    by_cases hs : 0 ≤ s
    · refine ⟨fun _ => true, ?_⟩
      simp only [score, best]
      simp
      omega
    · refine ⟨fun _ => false, ?_⟩
      simp only [score, best]
      simp
      omega
  | bundle c l r ihl ihr =>
    intro hn
    simp only [ids] at hn
    obtain ⟨hnl, hnr, hdisj⟩ := List.nodup_append.mp hn
    obtain ⟨ul, hul⟩ := ihl hnl
    obtain ⟨ur, hur⟩ := ihr hnr
    by_cases hpos : 0 < best l + best r - (c : Int)
    · let u : Nat → Bool := fun i => if i ∈ ids l then ul i else ur i
      have e1 : score u l = score ul l :=
        score_congr u ul l (fun i hi => by simp [u, hi])
      have e2 : score u r = score ur r :=
        score_congr u ur r (fun i hi => by
          have : i ∉ ids l := fun h => hdisj i h i hi rfl
          simp [u, this])
      refine ⟨u, ?_⟩
      simp only [score, best]
      rw [e1, e2]
      have hfl : ((score ul l).1 || (score ur r).1) = true := by
        refine Classical.byContradiction fun hf => ?_
        have hf' : ((score ul l).1 || (score ur r).1) = false := by simpa using hf
        have ha : (score ul l).1 = false := (Bool.or_eq_false_iff.mp hf').1
        have hb : (score ur r).1 = false := (Bool.or_eq_false_iff.mp hf').2
        have := flag0 ul l ha
        have := flag0 ur r hb
        have := bound ul l
        omega
      simp only [hfl, ite_true]
      omega
    · refine ⟨fun _ => false, ?_⟩
      rw [score_false]
      simp only [best]
      omega

theorem sound : DictionaryAdmission.Goal := by
  intro t hn
  exact ⟨fun u => bound u t, attain t hn⟩

end TinylambOpt.ResearchProofs.ClickstreamLayout.Storage_DictionaryAdmission
