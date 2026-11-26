import TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Exec_LengthRollup

open TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
open LengthRollup

private abbrev byteLen (v : Option Word) : Nat := (v.getD []).length
private abbrev nonemptyFlag (v : Option Word) : Nat := ((v.getD []).length != 0).toNat
private abbrev flag (n : Nat) : Nat := (n != 0).toNat

private theorem foldl_add (ys : List Nat) (s : Nat) : ys.foldl (· + ·) s = s + ys.sum := by
  induction ys generalizing s with
  | nil => simp
  | cons y ys ih =>
    simp only [List.foldl_cons, List.sum_cons]
    rw [ih (s + y)]
    omega

private theorem scanl_get (ys : List Nat) (i : Nat) (hi : i ≤ ys.length) :
    (ys.scanl (· + ·) 0)[i]? = some ((ys.take i).sum) := by
  rw [List.getElem?_scanl, if_pos hi, foldl_add]
  simp

private theorem drop_take_sum (ys : List Nat) (a k : Nat) :
    ((ys.drop a).take k).sum = (ys.take (a + k)).sum - (ys.take a).sum := by
  induction ys generalizing a k with
  | nil =>
    cases a <;> cases k <;> rfl
  | cons y ys ih =>
    cases a with
    | zero =>
      cases k with
      | zero => rfl
      | succ l =>
        rw [show ((y :: ys).drop 0).take (l + 1) = (y :: ys).take (l + 1) from rfl]
        rw [show 0 + (l + 1) = l + 1 by omega]
        simp
    | succ j =>
      rw [show (y :: ys).drop (j + 1) = ys.drop j from rfl]
      rw [show (y :: ys).take (j + 1) = y :: ys.take j from rfl]
      rw [show j + 1 + k = (j + k) + 1 from by omega]
      rw [show (y :: ys).take ((j + k) + 1) = y :: ys.take (j + k) from rfl]
      simp only [List.sum_cons]
      rw [ih j k]
      omega

private theorem cut_seg (ys : List Nat) (a k : Nat) :
    (ys.take (min ys.length (a + k))).sum - (ys.take (min ys.length a)).sum =
      ((ys.drop a).take k).sum := by
  by_cases hac : a ≤ ys.length
  · have ha : min ys.length a = a := Nat.min_eq_right hac
    rw [ha]
    by_cases hb : a + k ≤ ys.length
    · rw [Nat.min_eq_right hb]
      exact (drop_take_sum ys a k).symm
    · have hmin : min ys.length (a + k) = ys.length := Nat.min_eq_left (by omega)
      rw [hmin]
      have htake : (ys.drop a).take k = ys.drop a := by
        apply List.take_of_length_le
        rw [List.length_drop]
        omega
      have h1 : a + (ys.length - a) = ys.length := by omega
      have hseg := drop_take_sum ys a (ys.length - a)
      rw [h1, List.take_length] at hseg
      have htake' : (ys.drop a).take (ys.length - a) = ys.drop a := by
        apply List.take_of_length_le
        rw [List.length_drop]
        omega
      rw [List.take_length, ← hseg, htake', htake]
  · have ha : min ys.length a = ys.length := Nat.min_eq_left (by omega)
    have hb : min ys.length (a + k) = ys.length := Nat.min_eq_left (by omega)
    rw [ha, hb]
    have hdrop : ys.drop a = [] := List.drop_eq_nil_of_le (by omega)
    rw [hdrop]
    simp

private theorem cut_eq (ys : List Nat) (a k : Nat) :
    cut (ys.scanl (· + ·) 0) ys.length (a, k) = ((ys.drop a).take k).sum := by
  unfold cut
  rw [scanl_get ys (min ys.length (a + k)) (Nat.min_le_left _ _)]
  rw [scanl_get ys (min ys.length a) (Nat.min_le_left _ _)]
  simp only [Option.getD_some, Nat.zero_add]
  exact cut_seg ys a k

private theorem lens_slice (xs : List (Option Word)) (a k : Nat) :
    (((xs.drop a).take k).map byteLen) = ((lengths xs).drop a).take k := by
  unfold lengths byteLen
  rw [← List.map_drop, ← List.map_take]

private theorem pres_slice (xs : List (Option Word)) (a k : Nat) :
    (((lengths xs).map flag).drop a).take k = ((xs.drop a).take k).map nonemptyFlag := by
  unfold lengths
  rw [List.map_map]
  have hfun : (flag ∘ fun v : Option Word => (v.getD []).length) = nonemptyFlag := by
    funext v
    rfl
  rw [hfun, ← List.map_drop, ← List.map_take]

private theorem sum_map_flatten {α : Type} (f : α → Nat) : ∀ L : List (List α),
    (L.flatten.map f).sum = (L.map (fun ys => (ys.map f).sum)).sum := by
  intro L
  induction L with
  | nil => rfl
  | cons y L ih =>
    simp only [List.flatten_cons, List.map_append, List.sum_append]
    rw [ih]
    simp

private theorem sum_slices_byte (xs : List (Option Word)) (slices : List Slice) :
    (slices.map (fun s => (((xs.drop s.1).take s.2).map byteLen).sum)).sum =
      ((slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2)).map byteLen).sum := by
  let L := slices.map (fun s => (xs.drop s.1).take s.2)
  rw [show slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2) = L.flatten by
    simp [L, List.flatMap]]
  have hmapL : L.map (fun ys => (ys.map byteLen).sum) =
      slices.map (fun s => (((xs.drop s.1).take s.2).map byteLen).sum) := by
    unfold L
    rw [List.map_map]
    rfl
  rw [← hmapL]
  change (L.map (fun ys => (ys.map byteLen).sum)).sum = (L.flatten.map byteLen).sum
  exact (sum_map_flatten byteLen L).symm

private theorem sum_slices_flag (xs : List (Option Word)) (slices : List Slice) :
    (slices.map (fun s => (((xs.drop s.1).take s.2).map nonemptyFlag).sum)).sum =
      ((slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2)).map nonemptyFlag).sum := by
  let L := slices.map (fun s => (xs.drop s.1).take s.2)
  rw [show slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2) = L.flatten by
    simp [L, List.flatMap]]
  have hmapL : L.map (fun ys => (ys.map nonemptyFlag).sum) =
      slices.map (fun s => (((xs.drop s.1).take s.2).map nonemptyFlag).sum) := by
    unfold L
    rw [List.map_map]
    rfl
  rw [← hmapL]
  change (L.map (fun ys => (ys.map nonemptyFlag).sum)).sum = (L.flatten.map nonemptyFlag).sum
  exact (sum_map_flatten nonemptyFlag L).symm

private def keptRow (v : Option Word) : Option Word :=
  match v with
  | none => none
  | some w => if w.isEmpty then none else some w

private theorem keptRow_eq (v : Option Word) :
    keptRow v = keptRow v := by
  cases v with
  | none => rfl
  | some w => rfl

private theorem filterMap_keptRow_none (rs : List (Option Word)) :
    (none :: rs).filterMap keptRow = rs.filterMap keptRow := by
  rfl

private theorem filterMap_bind_eq (rows : List (Option Word)) :
    rows.filterMap (fun v => v.bind (fun w => if w.isEmpty then none else some w)) =
      rows.filterMap keptRow := by
  induction rows with
  | nil => rfl
  | cons v vs ih =>
    cases v with
    | none =>
      rw [filterMap_keptRow_none]
      simpa [List.isEmpty_iff] using ih
    | some w =>
      cases w with
      | nil => simpa [keptRow, List.isEmpty_iff] using ih
      | cons u us => simpa [keptRow, List.isEmpty_iff] using ih

private theorem kept_sum (rows : List (Option Word)) :
    (rows.map byteLen).sum = ((rows.filterMap keptRow).map List.length).sum := by
  induction rows with
  | nil => rfl
  | cons v rs ih =>
    rcases v with _ | w
    · rw [filterMap_keptRow_none]
      simpa [byteLen] using ih
    · cases w with
      | nil => simp [keptRow, byteLen, ih]
      | cons u us => simp [keptRow, byteLen, ih]

private theorem kept_len (rows : List (Option Word)) :
    (rows.map nonemptyFlag).sum = (rows.filterMap keptRow).length := by
  induction rows with
  | nil => rfl
  | cons v rs ih =>
    rcases v with _ | w
    · rw [filterMap_keptRow_none]
      simpa [nonemptyFlag] using ih
    · cases w with
      | nil => simp [keptRow, nonemptyFlag, ih]
      | cons u us => simp [keptRow, nonemptyFlag, ih]; omega

private theorem byte_component (xs : List (Option Word)) (slices : List Slice) :
    (slices.map (fun s => cut ((lengths xs).scanl (· + ·) 0) xs.length s)).sum =
      (List.map List.length ((slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2)).filterMap keptRow)).sum := by
  have hbody : (fun s => cut ((lengths xs).scanl (· + ·) 0) xs.length s) =
      (fun (s : Slice) => (((xs.drop s.1).take s.2).map byteLen).sum) := by
    funext s
    rcases s with ⟨a, k⟩
    simpa [byteLen, lengths] using cut_eq (lengths xs) a k
  rw [hbody, sum_slices_byte, kept_sum]

private theorem flag_component (xs : List (Option Word)) (slices : List Slice) :
    (slices.map (fun s => cut (((lengths xs).map flag).scanl (· + ·) 0) xs.length s)).sum =
      (((slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2)).filterMap keptRow).length) := by
  have hbody : (fun s => cut (((lengths xs).map flag).scanl (· + ·) 0) xs.length s) =
      (fun (s : Slice) => ((((lengths xs).map flag).drop s.1).take s.2).sum) := by
    funext s
    rcases s with ⟨a, k⟩
    simpa [lengths] using cut_eq ((lengths xs).map flag) a k
  have hbody2 : (fun (s : Slice) => ((((lengths xs).map flag).drop s.1).take s.2).sum) =
      (fun (s : Slice) => (((xs.drop s.1).take s.2).map nonemptyFlag).sum) := by
    funext s
    rcases s with ⟨a, k⟩
    simpa using congrArg List.sum (pres_slice xs a k)
  rw [hbody, hbody2, sum_slices_flag, kept_len]

theorem sound : Goal := by
  intro xs slices
  unfold summarized
  dsimp only
  rw [filterMap_bind_eq]
  apply Prod.ext
  · simpa using byte_component xs slices
  · simpa [flag] using flag_component xs slices

end TinylambOpt.ResearchProofs.ClickBenchLayout.Exec_LengthRollup
