import TinylambOpt.ResearchGoals.FilterLoopFootprint

/-!
Proved

- `Goal` — `∀ (xs : List Int64) (k : Int64), (schedule xs.length).flatMap accessed =
  List.range xs.length ∧ writes xs k = xs.zipIdx.map (fun (x,i) => (i, some (if x ==
  k then 1 else 0)))`

The reference model is defined in `TinylambOpt.ResearchGoals.FilterLoopFootprint`.
-/

namespace TinylambOpt.ResearchProofs.FilterLoopFootprint

open TinylambOpt.ResearchGoals.FilterLoopFootprint

private theorem flatMap_flatMap {α β γ : Type} (xs : List α) (f : α → List β) (g : β → List γ) :
    (xs.flatMap f).flatMap g = xs.flatMap (fun x => (f x).flatMap g) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.flatMap_cons, ih]

private theorem flatMap_singleton {α β : Type} (xs : List α) (f : α → β) :
    xs.flatMap (fun x => [f x]) = xs.map f := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem range'_append (s l₁ l₂ : Nat) :
    List.range' s l₁ ++ List.range' (s + l₁) l₂ = List.range' s (l₁ + l₂) := by
  simp only [List.range'_eq_map_range]
  rw [List.range_add]
  simp [Function.comp_apply, Nat.add_assoc]

private theorem accessed_eq_range' (a : Access) :
    accessed a = List.range' a.first a.lanes := by
  exact List.range'_eq_map_range.symm

private theorem flatMap_range'_step (base step count : Nat) :
    (List.range count).flatMap (fun i => List.range' (base + step * i) step) =
      List.range' base (step * count) := by
  induction count with
  | zero => simp
  | succ count ih =>
      rw [List.range_succ, List.flatMap_append, ih]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      rw [range'_append]
      congr 1

private theorem range'_append0 (l₁ l₂ : Nat) :
    List.range' 0 l₁ ++ List.range' l₁ l₂ = List.range' 0 (l₁ + l₂) := by
  simpa using (range'_append 0 l₁ l₂)

private theorem schedule_cover (n : Nat) : (schedule n).flatMap accessed = List.range n := by
  unfold schedule
  simp only [List.flatMap_append, flatMap_flatMap, List.flatMap_map]
  have hmain :
      ((List.range (n / 32)).flatMap fun i => (List.range 4).flatMap fun j =>
        accessed ⟨32 * i + 8 * j, 8⟩) = List.range (32 * (n / 32)) := by
    simp only [accessed_eq_range']
    have hcong : (List.range (n / 32)).flatMap
          (fun i => (List.range 4).flatMap (fun j => List.range' (32 * i + 8 * j) 8)) =
        (List.range (n / 32)).flatMap (fun i => List.range' (32 * i) (8 * 4)) := by
      apply congrArg (fun f => (List.range (n / 32)).flatMap f)
      funext i
      exact flatMap_range'_step (32 * i) 8 4
    rw [hcong]
    simpa [List.range'_eq_map_range] using (flatMap_range'_step 0 32 (n / 32))
  have hepi :
      ((List.range ((n % 32) / 8)).flatMap fun i => accessed ⟨n / 32 * 32 + 8 * i, 8⟩) =
        List.range' (n / 32 * 32) (8 * ((n % 32) / 8)) := by
    simp only [accessed_eq_range']
    simpa using (flatMap_range'_step (n / 32 * 32) 8 ((n % 32) / 8))
  have htail :
      ((List.range (n % 8)).flatMap fun j => accessed ⟨n / 8 * 8 + j, 1⟩) =
        List.range' (n / 8 * 8) (n % 8) := by
    simpa [accessed, List.range'_eq_map_range] using
      (flatMap_singleton (List.range (n % 8)) (fun j => n / 8 * 8 + j))
  rw [hmain, hepi, htail]
  have hmul : n / 32 * 32 = 32 * (n / 32) := by omega
  rw [hmul]
  rw [show List.range (32 * (n / 32)) = List.range' 0 (32 * (n / 32)) by
        simp [List.range'_eq_map_range]]
  rw [range'_append0]
  have harith : 32 * (n / 32) + 8 * ((n % 32) / 8) = 8 * (n / 8) := by omega
  rw [harith]
  have hmul8 : n / 8 * 8 = 8 * (n / 8) := by omega
  rw [hmul8, range'_append0]
  have hn : 8 * (n / 8) + n % 8 = n := by omega
  rw [hn]
  simp [List.range'_eq_map_range]

private theorem range_succ_map {α : Type} (f : Nat → α) (len : Nat) :
    (List.range (len + 1)).map f = f 0 :: (List.range len).map (fun i => f (i + 1)) := by
  rw [show List.range (len + 1) = List.range' 0 (len + 1) by
        simp [List.range'_eq_map_range]]
  rw [List.range'_succ]
  simp only [List.map_cons]
  congr 1
  rw [List.range'_eq_map_range, List.map_map]
  congr 1
  funext i
  exact congrArg f (Nat.add_comm 1 i)

private theorem range_getElem_map_from {α β : Type} (xs : List α) (f : α → β) (n : Nat) :
    (List.range xs.length).map (fun i => (n + i, (xs[i]?).map f)) =
      (xs.zipIdx n).map (fun (x, i) => (i, some (f x))) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.length_cons]
      rw [range_succ_map (fun i => (n + i, Option.map f (x :: xs)[i]?)) xs.length]
      simp only [List.map_cons, List.getElem?_cons_zero, Option.map_some, List.zipIdx_cons,
        List.getElem?_cons_succ]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

theorem sound : Goal := by
  intro xs k
  constructor
  · exact schedule_cover xs.length
  · unfold writes
    rw [← List.map_flatMap]
    rw [schedule_cover]
    simpa using (range_getElem_map_from xs (fun x : Int64 => if x == k then (1 : UInt8) else 0) 0)

end TinylambOpt.ResearchProofs.FilterLoopFootprint
