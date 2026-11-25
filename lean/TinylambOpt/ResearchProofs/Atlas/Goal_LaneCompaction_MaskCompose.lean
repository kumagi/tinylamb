import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_MaskCompose

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction

private theorem zipIdxShift (xs : List α) (k j : Nat) :
    xs.zipIdx (k + j) = (xs.zipIdx k).map (fun p => (p.1, p.2 + j)) := by
  induction xs generalizing k j with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      have hidx : (k + j) + 1 = (k + 1) + j := by omega
      rw [hidx, ih (k + 1) j]
      simp

private theorem zipIdxShiftOne (xs : List α) :
    xs.zipIdx 1 = (xs.zipIdx).map (fun p => (p.1, p.2 + 1)) := by
  simpa using zipIdxShift xs 0 1

private theorem countTakeCons (ok : Bool) (m : Flags) (i : Nat) :
    ((ok :: m).take (i + 1)).count true = (m.take i).count true + (if ok then 1 else 0) := by
  cases ok <;> simp

private def recoverFrom {α : Type} (m : Flags) (dense : List α) (shift : Nat) : List (Option α) :=
  m.zipIdx.map (fun (ok, i) => if ok then dense[(m.take i).count true + shift]? else none)

private theorem recoverEqFrom {α : Type} (m : Flags) (dense : List α) :
    recover m dense = recoverFrom m dense 0 := by
  simp [recover, recoverFrom]

private theorem recoverFromCons (ok : Bool) (m : Flags) {α : Type} (dense : List α) (shift : Nat) :
    recoverFrom (ok :: m) dense shift =
      (if ok then dense[shift]? else none) ::
        recoverFrom m dense (shift + (if ok then 1 else 0)) := by
  unfold recoverFrom
  rw [List.zipIdx_cons]
  simp only [List.map_cons]
  congr 1
  · cases ok <;> simp
  · rw [zipIdxShiftOne]
    simp only [List.map_map]
    apply List.map_congr_left
    intro p hp
    rcases p with ⟨ok', i⟩
    by_cases hok' : ok'
    · by_cases hok : ok <;> simp [hok', hok] <;> congr 1 <;> omega
    · simp [hok']

private theorem getElemAppendAdd {α : Type} (pre xs : List α) (j : Nat) :
    (pre ++ xs)[pre.length + j]? = xs[j]? := by
  rw [List.getElem?_append]
  have hnot : ¬ pre.length + j < pre.length := by omega
  simp [hnot]

private theorem recoverFromAppend (m : Flags) (pre second : List Bool) :
    recoverFrom m (pre ++ second) pre.length = recover m second := by
  rw [recoverEqFrom]
  unfold recoverFrom
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨ok, i⟩
  by_cases hok : ok
  · simp [hok]
    rw [Nat.add_comm]
    exact getElemAppendAdd pre second ((m.take i).count true)
  · simp [hok]

private theorem recoverConsFalse (m : Flags) (second : List Bool) :
    recover (false :: m) second = none :: recover m second := by
  rw [recoverEqFrom, recoverEqFrom]
  rw [recoverFromCons]
  simp

private theorem recoverConsTrue (m : Flags) (b : Bool) (second : List Bool) :
    recover (true :: m) (b :: second) = some b :: recover m second := by
  rw [recoverEqFrom, recoverEqFrom]
  rw [recoverFromCons]
  simp
  rw [show b :: second = [b] ++ second by rfl]
  exact recoverFromAppend m [b] second

private theorem packConsTrue {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (true :: m) (x :: v) = x :: pack m v := by
  simp [pack]

private theorem packConsFalse {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (false :: m) (x :: v) = pack m v := by
  simp [pack]

theorem sound_LaneCompaction_MaskCompose : LaneCompaction.MaskCompose.Goal := by
  intro v m second hlen hsecond
  induction m generalizing v second with
  | nil =>
      cases v with
      | nil =>
          cases second with
          | nil => simp [pack, recover]
          | cons b bs => simp at hsecond
      | cons x xs => simp at hlen
  | cons ok m ih =>
      cases v with
      | nil => simp at hlen
      | cons x v' =>
          have hv : m.length = v'.length := by simpa using hlen
          cases ok
          · have hs : second.length = m.count true := by simpa using hsecond
            rw [packConsFalse, recoverConsFalse]
            simp [packConsFalse]
            exact ih v' second hv hs
          · cases second with
            | nil => simp at hsecond
            | cons b second' =>
                have hs : second'.length = m.count true := by simpa using hsecond
                rw [packConsTrue, recoverConsTrue]
                simp
                cases b
                · simp [packConsFalse]
                  exact ih v' second' hv hs
                · simp [packConsTrue]
                  exact ih v' second' hv hs

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_MaskCompose
