import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_AnalyticsClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage

private abbrev Frame := List Nat

/-- Index entries for `frames` laid out starting at absolute position `s`. -/
private def R (enc : Frame → Frame) (s : Nat) (frames : List Frame) :
    List (Nat × (Nat × Frame)) :=
  ((frames.map List.length).scanl (· + ·) s).zip
    (frames.map (fun b => (b.length, enc b)))

private def span (dec : Frame → Frame) (start len : Nat) (entry : Nat × (Nat × Frame)) :
    Frame :=
  let (pos, sz, body) := entry
  if pos + sz ≤ start || start + len ≤ pos then []
  else ((dec body).drop (start - pos)).take (start + len - max pos start)

private theorem R_nil (enc : Frame → Frame) (s : Nat) : R enc s [] = [] := by
  dsimp only [R]
  simp

private theorem R_cons (enc : Frame → Frame) (f : Frame) (fs : List Frame) (s : Nat) :
    R enc s (f :: fs) = (s, (f.length, enc f)) :: R enc (s + f.length) fs := by
  dsimp only [R]
  rw [List.map_cons, List.scanl_cons, List.map_cons, List.zip_cons_cons]

/-- Every entry position is at least the layout start. -/
private theorem R_pos (enc : Frame → Frame) :
    ∀ (frames : List Frame) (s : Nat), ∀ e ∈ R enc s frames, s ≤ e.1 := by
  intro frames
  induction frames with
  | nil => intro s e he; rw [R_nil] at he; cases he
  | cons f fs ih =>
    intro s e he
    rw [R_cons] at he
    cases he with
    | head => exact Nat.le_refl _
    | tail _ he' =>
      have := ih (s + f.length) e he'
      omega

private theorem flatMap_all_nil {α β : Type} (g : α → List β) (l : List α)
    (h : ∀ e ∈ l, g e = []) : l.flatMap g = [] := by
  induction l with
  | nil => rfl
  | cons e es ih =>
    rw [List.flatMap_cons, h e (by simp), ih (fun x hx => h x (by simp [hx])),
      List.nil_append]

private theorem span_nil_of_before (dec : Frame → Frame) (start len pos sz : Nat) (body : Frame)
    (h : pos + sz ≤ start) : span dec start len (pos, sz, body) = [] := by
  have hc : (decide (pos + sz ≤ start) || decide (start + len ≤ pos)) = true := by
    simp [h]
  dsimp only [span]
  rw [if_pos hc]

private theorem span_nil_of_after (dec : Frame → Frame) (start len pos sz : Nat) (body : Frame)
    (h : start + len ≤ pos) : span dec start len (pos, sz, body) = [] := by
  have hc : (decide (pos + sz ≤ start) || decide (start + len ≤ pos)) = true := by
    simp [h]
  dsimp only [span]
  rw [if_pos hc]

private theorem span_overlap (dec : Frame → Frame) (start len pos sz : Nat) (body : Frame)
    (h1 : ¬ (pos + sz ≤ start)) (h2 : ¬ (start + len ≤ pos)) :
    span dec start len (pos, sz, body)
      = ((dec body).drop (start - pos)).take (start + len - max pos start) := by
  have hc : (decide (pos + sz ≤ start) || decide (start + len ≤ pos)) = false := by
    simp [h1, h2]
  have hnc : ¬ ((decide (pos + sz ≤ start) || decide (start + len ≤ pos)) = true) := by
    rw [hc]; exact Bool.false_ne_true
  dsimp only [span]
  rw [if_neg hnc]

private theorem drop_ge (x : List α) (i : Nat) (h : x.length ≤ i) :
    List.drop i x = [] := by
  induction x generalizing i with
  | nil => simp
  | cons a xs ih =>
    rcases Nat.eq_zero_or_pos i with rfl | hi
    · simp only [List.length_cons] at h
      omega
    · rw [List.drop_cons hi]
      refine ih (i - 1) ?_
      simp only [List.length_cons] at h
      omega

private theorem drop_len_add (x y : List α) (t : Nat) :
    List.drop (List.length x + t) (x ++ y) = List.drop t y := by
  rw [List.drop_append, drop_ge x _ (by omega), List.nil_append]
  rw [show x.length + t - x.length = t from by omega]

private theorem take_len_add (x y : List α) : ∀ t,
    List.take (List.length x + t) (x ++ y) = x ++ List.take t y := by
  induction x with
  | nil => intro t; simp
  | cons a xs ih =>
    intro t
    rw [List.length_cons, List.cons_append, List.cons_append,
      List.take_cons (by omega)]
    rw [show (xs.length + 1 + t) - 1 = xs.length + t from by omega, ih t]

private theorem range_read (dec enc : Frame → Frame) :
    ∀ (frames : List Frame), (∀ b ∈ frames, dec (enc b) = b) →
      ∀ s start len : Nat,
        (R enc s frames).flatMap (span dec start len)
          = if s ≤ start then ((frames.flatten).drop (start - s)).take len
            else (frames.flatten).take (start + len - s) := by
  intro frames
  induction frames with
  | nil =>
    intro _ s start len
    rw [R_nil, List.flatMap_nil]
    split <;> simp
  | cons f fs ih =>
    intro hdec s start len
    have hmem : ∀ b ∈ fs, dec (enc b) = b := by
      intro b hb; exact hdec b (by simp [hb])
    have hrec := ih hmem (s + f.length) start len
    rw [R_cons, List.flatMap_cons]
    by_cases hA : s + f.length ≤ start
    · -- head lies entirely before the window
      have hle : s ≤ start := by omega
      rw [span_nil_of_before dec start len s f.length (enc f) hA, List.nil_append, hrec,
        if_pos hA, if_pos hle]
      have h1 : start - s - f.length = start - (s + f.length) := by omega
      have h2 : start - s = f.length + (start - s - f.length) := by omega
      have hf : (f :: fs).flatten = f ++ fs.flatten := rfl
      rw [hf, h2, drop_len_add, h1]
    · by_cases hB : start + len ≤ s
      · -- window ends before the head position: nothing is read
        have hall : (R enc (s + f.length) fs).flatMap (span dec start len) = [] := by
          refine flatMap_all_nil (span dec start len) _ ?_
          intro e he
          have hge := R_pos enc fs (s + f.length) e he
          exact span_nil_of_after dec start len e.1 e.2.1 e.2.2 (by omega)
        rw [span_nil_of_after dec start len s f.length (enc f) hB, List.nil_append, hall]
        split <;> first
          | rw [show len = 0 from by omega, List.take_zero]
          | rw [show start + len - s = 0 from by omega, List.take_zero]
      · -- the window overlaps the head frame
        rw [span_overlap dec start len s f.length (enc f) hA hB, hdec f (by simp), hrec,
          if_neg hA]
        by_cases hC : s ≤ start
        · -- outer if takes the `s ≤ start` branch
          have hmax : max s start = start := Nat.max_eq_right hC
          have hsub : start + len - start = len := by omega
          have hdn : start - s - f.length = 0 := by omega
          have h2 : start + len - (s + f.length) = len - (f.length - (start - s)) := by omega
          rw [if_pos hC, hmax, hsub]
          have hf : (f :: fs).flatten = f ++ fs.flatten := rfl
          rw [hf, List.drop_append, hdn, List.drop_zero, List.take_append,
            List.length_drop, ← h2]
        · -- outer if takes the `start < s` branch
          have hlt : start < s := by omega
          have hmax : max s start = s := Nat.max_eq_left (Nat.le_of_lt hlt)
          have hsub : start - s = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt)
          have h2 : start + len - (s + f.length) = (start + len - s) - f.length := by omega
          rw [if_neg hC, hmax, hsub, List.drop_zero]
          have hf : (f :: fs).flatten = f ++ fs.flatten := rfl
          rw [hf, List.take_append, ← h2]

theorem sound : AnalyticsClickHouse.Goal := by
  intro frames enc dec start len hdec
  have h := range_read dec enc frames hdec 0 start len
  rw [if_pos (Nat.zero_le start)] at h
  simp only [Nat.sub_zero] at h
  exact h

end TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_AnalyticsClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Storage_AnalyticsClickHouse.sound
