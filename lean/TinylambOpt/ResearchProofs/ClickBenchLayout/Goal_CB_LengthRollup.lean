import TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_LengthRollup

open TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
open LengthRollup

private abbrev byteLen (v : Option Word) : Nat := (v.getD []).length
private abbrev nonemptyFlag (v : Option Word) : Nat := ((v.getD []).length != 0).toNat
private abbrev flag (n : Nat) : Nat := (n != 0).toNat

private def keep (v : Option Word) : Option Word :=
  v.bind (fun w => if List.isEmpty w = true then none else some w)

private theorem keep_none : keep none = none := rfl
private theorem keep_nilw : keep (some ([] : List UInt8)) = none := rfl
private theorem keep_cons (u : UInt8) (t : List UInt8) :
    keep (some (u :: t)) = some (u :: t) := rfl

private theorem bln : (List.length ((none : Option Word).getD []) : Nat) = 0 := rfl
private theorem blw : (List.length ((some ([] : Word) : Option Word).getD []) : Nat) = 0 := rfl
private theorem blc (u : UInt8) (t : List UInt8) :
    (List.length ((some (u :: t) : Option Word).getD []) : Nat) = t.length + 1 := rfl
private theorem nfn : ((List.length ((none : Option Word).getD []) != 0).toNat : Nat) = 0 := rfl
private theorem nfw :
    ((List.length ((some ([] : Word) : Option Word).getD []) != 0).toNat : Nat) = 0 := rfl
private theorem nfc (u : UInt8) (t : List UInt8) :
    ((List.length ((some (u :: t) : Option Word).getD []) != 0).toNat : Nat) = 1 := rfl

private theorem fm_none (rs : List (Option Word)) :
    List.filterMap keep (none :: rs) = List.filterMap keep rs := rfl
private theorem fm_nilw (rs : List (Option Word)) :
    List.filterMap keep (some ([] : Word) :: rs) = List.filterMap keep rs := rfl
private theorem fm_cons (u : UInt8) (t : List UInt8) (rs : List (Option Word)) :
    List.filterMap keep (some (u :: t) :: rs) = (u :: t) :: List.filterMap keep rs := rfl

private theorem foldl_add : ∀ (ys : List Nat) (s : Nat), ys.foldl (·+·) s = s + ys.sum := by
  intro ys
  induction ys with
  | nil => intro s; rfl
  | cons y ys ih =>
    intro s
    simp only [List.foldl_cons, List.sum_cons]
    rw [ih (s + y)]
    omega

private theorem scanl_get (ys : List Nat) (s i : Nat) (hi : i ≤ ys.length) :
    (ys.scanl (·+·) s)[i]? = some (s + (ys.take i).sum) := by
  rw [List.getElem?_scanl, ite_eq_left hi, foldl_add]

private theorem drop_take_sum (ys : List Nat) (a k : Nat) :
    ((ys.drop a).take k).sum = (ys.take (a + k)).sum - (ys.take a).sum := by
  induction ys generalizing a k with
  | nil =>
    cases a with
    | zero => cases k with
      | zero => rfl
      | succ l => rfl
    | succ j => cases k with
      | zero => rfl
      | succ l => rfl
  | cons y ys ih =>
    cases a with
    | zero =>
      cases k with
      | zero => rfl
      | succ l =>
        have hx : ((y :: ys).drop 0).take (l + 1) = (y :: ys).take (l + 1) := rfl
        have hz : ((y :: ys).take 0 : List Nat) = [] := rfl
        rw [hx, hz, List.sum_nil, Nat.sub_zero, Nat.zero_add]
    | succ j =>
      have h1 : ((y :: ys).drop (j + 1)) = ys.drop j := rfl
      rw [h1]
      rw [show ((y :: ys).take (j + 1)) = y :: ys.take j from rfl]
      rw [show (j + 1 + k) = (j + k) + 1 from by omega]
      rw [show ((y :: ys).take ((j + k) + 1)) = y :: ys.take (j + k) from rfl]
      rw [List.sum_cons, List.sum_cons]
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
        have hk : (ys.drop a).length ≤ k := by
          rw [List.length_drop]
          omega
        exact List.take_of_length_le hk
      have h1 : a + (ys.length - a) = ys.length := by omega
      have hseg := drop_take_sum ys a (ys.length - a)
      rw [h1, List.take_length] at hseg
      have hlen : (ys.drop a).length = ys.length - a := List.length_drop
      rw [← hlen, List.take_length] at hseg
      rw [List.take_length, htake, ← hseg]
  · have ha : min ys.length a = ys.length := Nat.min_eq_left (by omega)
    have hb : min ys.length (a + k) = ys.length := Nat.min_eq_left (by omega)
    rw [ha, hb]
    have hdrop : ys.drop a = [] := by
      apply List.drop_eq_nil_of_le
      omega
    rw [hdrop]
    simp

private theorem cut_eq (ys : List Nat) (a k : Nat) :
    cut (ys.scanl (·+·) 0) ys.length (a, k) = ((ys.drop a).take k).sum := by
  unfold LengthRollup.cut
  rw [scanl_get ys 0 _ (Nat.min_le_left _ _),
    scanl_get ys 0 _ (Nat.min_le_left _ _)]
  simp only [Option.getD_some, Nat.zero_add]
  exact cut_seg ys a k

private theorem sum_slices_gen (f : Option Word → Nat) (g : Slice → List (Option Word))
    (slices : List Slice) :
    (slices.map (fun s => ((g s).map f).sum)).sum = ((slices.flatMap g).map f).sum := by
  induction slices with
  | nil => rfl
  | cons s ss ih =>
    show ((g s).map f).sum + (ss.map (fun s => ((g s).map f).sum)).sum = _
    have hfl : List.flatMap g (s :: ss) = g s ++ List.flatMap g ss := rfl
    rw [hfl, List.map_append, List.sum_append, ih]

private theorem kept_sum (rows : List (Option Word)) :
    (rows.map byteLen).sum = ((rows.filterMap keep).map List.length).sum := by
  unfold byteLen
  induction rows with
  | nil => rfl
  | cons v rs ih =>
    rw [List.map_cons, List.sum_cons]
    rcases v with _|w
    · rw [fm_none, bln, Nat.zero_add]
      rw [ih]
    · cases w with
      | nil =>
        rw [fm_nilw, blw, Nat.zero_add]
        rw [ih]
      | cons u t =>
        rw [fm_cons, blc, List.map_cons, List.sum_cons, List.length_cons]
        rw [ih]

private theorem kept_len (rows : List (Option Word)) :
    (rows.map nonemptyFlag).sum = (rows.filterMap keep).length := by
  unfold nonemptyFlag
  induction rows with
  | nil => rfl
  | cons v rs ih =>
    rw [List.map_cons, List.sum_cons]
    rcases v with _|w
    · rw [fm_none, nfn, Nat.zero_add]
      rw [ih]
    · cases w with
      | nil =>
        rw [fm_nilw, nfw, Nat.zero_add]
        rw [ih]
      | cons u t =>
        rw [fm_cons, nfc, List.length_cons]
        rw [ih]
        omega

private theorem main (xs : List (Option Word)) (slices : List Slice) :
    summarized xs slices =
      let rows := slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2)
      let kept := rows.filterMap keep
      ((kept.map List.length).sum, kept.length) := by
  unfold LengthRollup.summarized
  dsimp only
  apply Prod.ext
  · have hn : (LengthRollup.lengths xs).length = xs.length := by
      unfold LengthRollup.lengths
      exact List.length_map _
    have hbody : (fun (s : Slice) => cut ((lengths xs).scanl (·+·) 0) xs.length s) =
        (fun s => (((xs.drop s.1).take s.2).map byteLen).sum) := by
      funext s
      rcases s with ⟨a, k⟩
      rw [← hn]
      rw [cut_eq (ys := LengthRollup.lengths xs)]
      congr 1
      unfold LengthRollup.lengths byteLen
      rw [List.map_take, List.map_drop]
    have hbody' : (cut ((lengths xs).scanl (·+·) 0) xs.length) =
        (fun (s : Slice) => (((xs.drop s.1).take s.2).map byteLen).sum) := hbody
    rw [hbody', sum_slices_gen, kept_sum]
  · have hn : (((LengthRollup.lengths xs).map flag).length = xs.length) := by
      unfold LengthRollup.lengths
      rw [List.length_map, List.length_map]
    have hbody : (fun (s : Slice) => cut (((lengths xs).map flag).scanl (·+·) 0) xs.length s) =
        (fun (s : Slice) => ((((lengths xs).map flag).drop s.1).take s.2).sum) := by
      funext s
      rcases s with ⟨a, k⟩
      rw [← hn]
      rw [cut_eq (ys := (LengthRollup.lengths xs).map flag)]
    have hbody2 : (fun (s : Slice) => ((((lengths xs).map flag).drop s.1).take s.2).sum) =
        (fun (s : Slice) => (((xs.drop s.1).take s.2).map nonemptyFlag).sum) := by
      funext s
      rcases s with ⟨a, k⟩
      show ((((xs.map (fun s' => (s'.getD []).length)).map flag).drop a).take k).sum =
          (((xs.drop a).take k).map nonemptyFlag).sum
      unfold nonemptyFlag flag
      rw [List.map_map, List.map_take, List.map_drop]
      rfl
    have hbody' : cut (((lengths xs).map flag).scanl (·+·) 0) xs.length =
        (fun (s : Slice) => (((xs.drop s.1).take s.2).map nonemptyFlag).sum) :=
      hbody.trans hbody2
    rw [hbody', sum_slices_gen, kept_len]

theorem sound : TinylambOpt.ResearchGoals.ClickBenchLayout.Execution.LengthRollup.Goal := by
  intro xs slices
  exact main xs slices

end TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_LengthRollup
