import TinylambOpt.ResearchGoals.SegmentedXor

/-!
Proved

- `Goal` — `∀ carry t, parallel carry t = stream carry (flatten t)`

The reference model is defined in `TinylambOpt.ResearchGoals.SegmentedXor`.
-/

namespace TinylambOpt.ResearchProofs.SegmentedXor

open TinylambOpt.ResearchGoals.SegmentedXor

/-- The summary composition used by `up` on an internal node. -/
def combine (a b : Summary) : Summary :=
  ⟨a.reset || b.reset, applySummary b a.bits⟩

/-- A composed summary acts as the sequential composition of the two summaries. -/
theorem applySummary_combine (a b : Summary) (x : UInt64) :
    applySummary (combine a b) x = applySummary b (applySummary a x) := by
  unfold combine applySummary
  cases a.reset <;> cases b.reset <;> simp [UInt64.xor_assoc]

/-- `stream` over an append splits into the two runs, passing the intermediate carry. -/
theorem stream_append (carry : UInt64) (xs ys : List Token) :
    stream carry (xs ++ ys) =
      let (v, c') := stream carry xs
      let (w, c'') := stream c' ys
      (v ++ w, c'') := by
  induction xs generalizing carry with
  | nil => simp [stream]
  | cons t ts ih =>
      cases t <;> simp [stream, ih]

/-- The upward summary and downward pass agree with the sequential decoder. -/
theorem up_correct : ∀ (carry : UInt64) (t : Tree),
    down carry (up t).2 = (stream carry (flatten t)).1 ∧
    applySummary (up t).1 carry = (stream carry (flatten t)).2 := by
  intro carry t
  induction t generalizing carry with
  | empty =>
      simp [up, down, flatten, stream, applySummary]
  | leaf t =>
      cases t <;> simp [up, down, flatten, stream, applySummary]
  | node l r ihl ihr =>
      have hl := ihl carry
      have hr := ihr ((stream carry (flatten l)).2)
      constructor
      · simp only [up, down, flatten]
        rw [hl.1, hl.2, hr.1]
        rw [stream_append]
      · simp only [up, flatten]
        change applySummary (combine (up l).1 (up r).1) carry =
          (stream carry (flatten l ++ flatten r)).2
        rw [applySummary_combine, hl.2, hr.2]
        rw [stream_append]

theorem sound : TinylambOpt.ResearchGoals.SegmentedXor.Goal := by
  intro carry t
  have h := up_correct carry t
  simpa [parallel] using Prod.ext h.1 h.2
