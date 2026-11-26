import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `BatchWatermarks.CoarseTime` — `∀ xs t step, 0 < step → score t xs = score (t/step)
  (rounded step xs) - ((xs.filter (fun r => r.1/step == t/step && t < r.1)).map
  Prod.snd).sum`
- `BatchWatermarks.EquivalentHistory` — `∀ a b, (∀ t, score t a = score t b) ↔ ∀ t,
  ((a.filter (fun r => r.1 == t)).map Prod.snd).sum = ((b.filter (fun r => r.1 ==
  t)).map Prod.snd).sum`
- `BatchWatermarks.HistoricalBand` — `∀ xs late lo hi, lo ≤ hi → (score hi
  (xs++late)-score lo (xs++late)) - (score hi xs-score lo xs) = ((late.filter (fun r
  => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum`
- `BatchWatermarks.PartialOpen` — `∀ runs t, score t runs.flatten = score t (summaries
  runs) + ((runs.filter (fun r => t < stamp r)).map (score t)).sum`
-/

namespace TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.BatchWatermarks

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

set_option maxHeartbeats 0

private theorem beq_eq_decide (a b : Nat) : (a == b) = decide (a = b) := by
  by_cases h : a == b
  · have hab : a = b := beq_iff_eq.mp h
    simp [hab]
  · have hab : a ≠ b := by intro he; exact h (by simp [he])
    simp [hab]

private theorem mem_le_maxD (ns : List Nat) (n : Nat) (hn : n ∈ ns) : n ≤ ns.max?.getD 0 := by
  cases ns with
  | nil => simp at hn
  | cons a as =>
      have hmax : (a :: as).max? = some ((a :: as).max?.getD 0) := by
        rw [List.max?_cons]
        simp
      exact (List.max?_eq_some_iff.mp hmax).2 n (by simp [hn])

private theorem row_le_stamp (r : Row) (rs : List Row) (hr : r ∈ rs) : r.1 ≤ stamp rs := by
  dsimp [stamp]
  apply mem_le_maxD
  exact List.mem_map.mpr ⟨r, hr, rfl⟩

private theorem score_append (t : Nat) (xs ys : List Row) :
    score t (xs ++ ys) = score t xs + score t ys := by
  simp [score, visible, List.filter_append]

private theorem score_flatten (t : Nat) (runs : List (List Row)) :
    score t runs.flatten = (runs.map (score t)).sum := by
  induction runs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rw [List.flatten_cons, score_append]
      simp [ih]

private theorem score_eq_sum_of_stamp_le (t : Nat) (r : List Row) (h : stamp r ≤ t) :
    score t r = (r.map Prod.snd).sum := by
  have hv : visible t r = r := by
    apply List.filter_eq_self.mpr
    intro row hr
    exact decide_eq_true (by have h' := row_le_stamp row r hr; omega)
  simp [score, hv]

private theorem score_summaries (t : Nat) (runs : List (List Row)) :
    score t (summaries runs) =
      ((runs.filter (fun r => stamp r ≤ t)).map (fun r => (r.map Prod.snd).sum)).sum := by
  induction runs with
  | nil => simp [score, visible, summaries]
  | cons r rs ih =>
      by_cases h : stamp r ≤ t
      · simp [summaries, score, visible, h, List.filter_map, Function.comp_def]
      · simp [summaries, score, visible, h, List.filter_map, Function.comp_def]

private theorem score_cons (t : Nat) (a : Nat) (w : Int) (rs : List Row) :
    score t ((a, w) :: rs) = (if a ≤ t then w else 0) + score t rs := by
  by_cases h : a ≤ t <;> simp [score, visible, h]

private theorem score_zero (xs : List Row) :
    score 0 xs = ((xs.filter (fun r => r.1 == 0)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons]
      have ih' : score 0 rs = ((rs.filter (fun r => decide (r.1 = 0))).map Prod.snd).sum := by
        simpa [beq_eq_decide] using ih
      rw [ih']
      by_cases haeq : a = 0 <;>
        simp [haeq, beq_eq_decide] <;> omega

private theorem score_succ (xs : List Row) (n : Nat) :
    score (n + 1) xs = score n xs + ((xs.filter (fun r => r.1 == n + 1)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons, score_cons]
      have ih' : score (n + 1) rs =
          score n rs + ((rs.filter (fun r => decide (r.1 = n + 1))).map Prod.snd).sum := by
        simpa [beq_eq_decide] using ih
      rw [ih']
      by_cases h1 : a ≤ n + 1 <;> by_cases h0 : a ≤ n <;> by_cases haeq : a = n + 1 <;>
        simp [h1, h0, haeq, beq_eq_decide] <;> omega

theorem sound_PartialOpen :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.PartialOpen.Goal := by
  intro runs t
  rw [score_flatten t runs, score_summaries t runs]
  induction runs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : stamp r ≤ t
      · have hs := score_eq_sum_of_stamp_le t r h
        have hnot : ¬ t < stamp r := by omega
        simp [h, hnot, hs, ih]
        omega
      · have hlt : t < stamp r := by omega
        simp [h, hlt, ih]
        omega

theorem sound_CoarseTime :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.CoarseTime.Goal := by
  intro xs t step hstep
  have hroundedDecide : score (t / step) (rounded step xs) =
      score t xs +
        ((xs.filter (fun r => decide (r.1 / step = t / step) && decide (t < r.1))).map Prod.snd).sum := by
    induction xs with
    | nil => simp [score, visible, rounded]
    | cons r rs ih =>
        rcases r with ⟨a, w⟩
        change score (t / step) ((a / step, w) :: rounded step rs) =
          score t ((a, w) :: rs) +
            ((((a, w) :: rs).filter (fun r => decide (r.1 / step = t / step) && decide (t < r.1))).map Prod.snd).sum
        rw [score_cons, score_cons]
        by_cases hat : a ≤ t
        · have hdiv : a / step ≤ t / step := Nat.div_le_div_right hat
          have hnlt : ¬ t < a := by omega
          simp [hat, hdiv, hnlt, ih]
          rw [Int.add_assoc]
        · have hta : t < a := by omega
          by_cases hdiv : a / step ≤ t / step
          · have hdiv' : t / step ≤ a / step := Nat.div_le_div_right (by omega : t ≤ a)
            have heq : a / step = t / step := by omega
            simp [hat, hta, heq, ih]
            ac_rfl
          · have hne : a / step ≠ t / step := by omega
            simp [hat, hta, hne, hdiv, ih]
  have hrounded : score (t / step) (rounded step xs) =
      score t xs + ((xs.filter (fun r => r.1 / step == t / step && t < r.1)).map Prod.snd).sum := by
    simpa only [beq_eq_decide] using hroundedDecide
  rw [hrounded]
  omega

private theorem band_score (lo hi : Nat) (xs : List Row) (hlohi : lo ≤ hi) :
    score hi xs - score lo xs =
      ((xs.filter (fun r => lo < r.1 && r.1 ≤ hi)).map Prod.snd).sum := by
  induction xs with
  | nil => simp [score, visible]
  | cons r rs ih =>
      rcases r with ⟨a, w⟩
      rw [score_cons, score_cons]
      have hsplit : (if a ≤ hi then w else 0) + score hi rs -
          ((if a ≤ lo then w else 0) + score lo rs) =
          (score hi rs - score lo rs) + ((if a ≤ hi then w else 0) - (if a ≤ lo then w else 0)) := by
        omega
      rw [hsplit, ih]
      by_cases hhi : a ≤ hi <;> by_cases hltlo : lo < a <;>
        simp [hhi, hltlo] <;> omega

theorem sound_HistoricalBand :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.HistoricalBand.Goal := by
  intro xs late lo hi hlohi
  rw [score_append, score_append]
  have hb := band_score lo hi late hlohi
  omega

theorem sound_EquivalentHistory :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.EquivalentHistory.Goal := by
  intro a b
  constructor
  · intro h t
    cases t with
    | zero =>
        have ha := score_zero a
        have hb := score_zero b
        have ht := h 0
        omega
    | succ n =>
        have ha := score_succ a n
        have hb := score_succ b n
        have ht := h (n + 1)
        have hp := h n
        omega
  · intro h t
    induction t with
    | zero =>
        have ha := score_zero a
        have hb := score_zero b
        have ht := h 0
        omega
    | succ n ih =>
        have ha := score_succ a n
        have hb := score_succ b n
        have ht := h (n + 1)
        omega

end TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.BatchWatermarks
