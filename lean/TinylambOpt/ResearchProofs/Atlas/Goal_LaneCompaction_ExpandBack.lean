import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `LaneCompaction.ExpandBack` — `∀ m (v : List Int), m.length = v.length → recover m
  (pack m v) = (m.zip v).map (fun (ok,x) => if ok then some x else none)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_ExpandBack

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction

/-!
  Small-input exhaustive check: all Bool masks of length at most 5, each paired
  with every value list over {0,1,2} of the same length.  `#eval` prints `true`.
-/
#eval
  let vals : List Int := [0,1,2]
  let masks : List Flags := (List.range 6).flatMap (fun n =>
    (List.range (2 ^ n)).map (fun k =>
      (List.range n).map (fun i => (k >>> i) % 2 == 1)))
  masks.all (fun m =>
    (List.range (3 ^ m.length)).all (fun code =>
      let v := (List.range m.length).map (fun i => vals[(code / 3 ^ i) % 3]?.getD 0)
      recover m (pack m v) == (m.zip v).map (fun (ok,x) => if ok then some x else none)))

/-- `zipIdx` with a nonzero start offset is the plain index list shifted by that offset. -/
theorem zipIdx_add (m : List Bool) (o : Nat) :
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

/-- The i-th indexed entry of `zipIdx` is the i-th list entry paired with i. -/
theorem zipIdx_get (m : List Bool) (i : Nat) :
    (m.zipIdx)[i]? = (m[i]?).map (fun b => (b,i)) := by
  induction m generalizing i with
  | nil => cases i <;> simp [List.zipIdx]
  | cons b m ih =>
      cases i with
      | zero => simp [List.zipIdx]
      | succ i =>
          rw [show (b::m).zipIdx = (b,0)::m.zipIdx 1 by simp [List.zipIdx]]
          simp only [List.getElem?_cons_succ]
          rw [zipIdx_add m 1]
          simp only [List.getElem?_map]
          rw [ih i]
          simp [Function.comp_def]

/-- The i-th entry of `zip` pairs the i-th entries of both lists. -/
theorem zip_get (m : List Bool) (v : List Int) (h : m.length = v.length) (i : Nat) :
    (m.zip v)[i]? = match m[i]?, v[i]? with | some a, some b => some (a,b) | _, _ => none := by
  induction m generalizing v i with
  | nil =>
      cases v with
      | nil => simp
      | cons x xs => simp at h
  | cons b m ih =>
      cases v with
      | nil => simp at h
      | cons x xs =>
          simp at h
          cases i with
          | zero => simp
          | succ i => simpa using ih xs h i

/-- A selected lane's dense position equals the number of earlier selected lanes. -/
theorem pack_nth_true : ∀ (m : List Bool) (v : List Int), m.length = v.length → ∀ i,
    m[i]? = some true → (pack m v)[(m.take i).count true]? = v[i]? := by
  intro m
  induction m with
  | nil =>
      intro v h i hi
      cases i <;> simp at hi
  | cons ok m ih =>
      intro v h i hi
      cases v with
      | nil => simp at h
      | cons x xs =>
          simp at h
          cases i with
          | zero =>
              cases ok <;> simp [pack] at hi ⊢
          | succ i =>
              cases ok <;> simp at hi
              · simpa [pack] using ih xs h i hi
              · simpa [pack] using ih xs h i hi

theorem sound_LaneCompaction_ExpandBack : ExpandBack.Goal := by
  intro m v hlen
  apply List.ext_getElem?
  intro i
  simp only [recover]
  rw [List.getElem?_map]
  rw [zipIdx_get]
  simp only [Option.map_map]
  rw [List.getElem?_map]
  rw [zip_get m v hlen i]
  have hindex : ∀ {a}, m[i]? = some a → ∃ b, v[i]? = some b := by
    intro a ha
    rcases (List.getElem?_eq_some_iff.mp ha) with ⟨hm, _⟩
    have hvlt : i < v.length := by omega
    exact ⟨v[i], List.getElem?_eq_some_iff.mpr ⟨hvlt, rfl⟩⟩
  cases hmi : m[i]? with
  | none => simp
  | some ok =>
      obtain ⟨y, hvy⟩ := hindex hmi
      rw [hvy]
      cases ok
      · simp
      · simp [hvy, pack_nth_true m v hlen i hmi]

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_ExpandBack
