import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_EarlyDemand

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction

/-!
  Truth check: all Bool masks of length at most 4, all value lists over
  {-1,0,1} of the same length, and all k < 6.  `#eval` prints `true`.
-/
#eval
  let rec masks : Nat → List Flags
    | 0 => [[]]
    | n+1 => (masks n).flatMap (fun xs => [false, true].map (fun b => b :: xs))
  let rec vals : Nat → List (List Int)
    | 0 => [[]]
    | n+1 => (vals n).flatMap (fun xs => [-1, 0, 1].map (fun v => v :: xs))
  ((List.range 5).flatMap (fun n =>
    (masks n).flatMap (fun m =>
      (vals n).flatMap (fun v =>
        (List.range 6).map (fun k =>
          let stop := ((slots m)[k]?).getD m.length
          decide ((pack m v).take k = pack (m.take stop) (v.take stop))))))).all id

/-! `zipIdx` with a start offset is the index list shifted by that offset. -/
private theorem zipIdx_add (m : List Bool) (o : Nat) :
    m.zipIdx o = (m.zipIdx).map (fun p => (p.1, p.2 + o)) := by
  induction m generalizing o with
  | nil => simp [List.zipIdx]
  | cons b m ih =>
      rw [show (b::m).zipIdx o = (b,o)::m.zipIdx (o+1) by simp [List.zipIdx]]
      rw [show (b::m).zipIdx = (b,0)::m.zipIdx 1 by simp [List.zipIdx]]
      simp only [List.map_cons, List.cons.injEq]
      constructor
      · simp
      · rw [ih (o+1)]
        rw [ih 1]
        simp only [List.map_map]
        congr 1
        funext p
        rcases p with ⟨a,j⟩
        simp [Nat.add_comm, Nat.add_left_comm]

/-! Filtering after mapping commutes when the filter predicate is preserved. -/
private theorem filter_map_pair {α : Type} (p : α → Bool) (f : α → α)
    (h : ∀ x, p (f x) = p x) (l : List α) :
    (l.map f).filter p = (l.filter p).map f := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      by_cases hp : p a
      · simp [hp, h, ih]
      · have hf : p (f a) = false := by simpa [h] using hp
        simp [hp, hf, ih]

private theorem filter_map_zipIdx_add (m : List Bool) (o : Nat) :
    ((m.zipIdx o).filter (fun p => p.1)).map Prod.snd =
      (((m.zipIdx).filter (fun p => p.1)).map Prod.snd).map (fun i => i + o) := by
  rw [zipIdx_add m o]
  rw [filter_map_pair (fun p : Bool × Nat => p.1) (fun p => (p.1, p.2 + o))]
  · rw [List.map_map, List.map_map]
    simp [Function.comp_def]
  · intro p
    rfl

/-! Prepending an unselected lane shifts every later slot by one. -/
private theorem slots_false (m : List Bool) : slots (false :: m) = (slots m).map (fun i => i+1) := by
  unfold slots
  rw [show (false::m).zipIdx = (false,0)::m.zipIdx 1 by simp [List.zipIdx]]
  simp
  rw [filter_map_zipIdx_add m 1]
  rw [List.map_map]

/-! Prepending a selected lane adds slot 0 and shifts the rest by one. -/
private theorem slots_true (m : List Bool) : slots (true :: m) = 0 :: (slots m).map (fun i => i+1) := by
  unfold slots
  rw [show (true::m).zipIdx = (true,0)::m.zipIdx 1 by simp [List.zipIdx]]
  simp
  rw [filter_map_zipIdx_add m 1]
  rw [List.map_map]

private theorem getD_map_succ (l : List Nat) (k d : Nat) :
    ((l.map (fun i => i+1))[k]?).getD (d+1) = ((l[k]?).getD d)+1 := by
  induction l generalizing k with
  | nil => simp
  | cons a l ih =>
      cases k with
      | zero => simp
      | succ k =>
          simp only [List.map_cons, List.getElem?_cons_succ]
          rw [ih]

theorem sound_LaneCompaction_EarlyDemand : EarlyDemand.Goal := by
  intro m
  induction m with
  | nil =>
      intro xs k hlen
      cases xs with
      | nil => simp [slots, pack]
      | cons x xs => simp at hlen
  | cons ok m ih =>
      intro xs k hlen
      cases xs with
      | nil => simp at hlen
      | cons x xs =>
          simp at hlen
          cases ok with
          | false =>
              rw [slots_false]
              change List.take k (pack (false::m) (x::xs)) =
                pack (List.take ((((slots m).map (fun i=>i+1))[k]?).getD (m.length+1)) (false::m))
                     (List.take ((((slots m).map (fun i=>i+1))[k]?).getD (m.length+1)) (x::xs))
              rw [getD_map_succ]
              simpa [pack] using ih xs k hlen
          | true =>
              rw [slots_true]
              cases k with
              | zero => simp [pack]
              | succ k =>
                  change List.take (k+1) (pack (true::m) (x::xs)) =
                    pack (List.take (((0::(slots m).map (fun i=>i+1))[k+1]?).getD (m.length+1)) (true::m))
                         (List.take (((0::(slots m).map (fun i=>i+1))[k+1]?).getD (m.length+1)) (x::xs))
                  simp only [List.getElem?_cons_succ]
                  rw [getD_map_succ]
                  simpa [pack] using ih xs k hlen

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_EarlyDemand
