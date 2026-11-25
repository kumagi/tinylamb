import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

namespace TinylambOpt.ResearchProofs.Atlas.ExecutorResources.LaneCompaction

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction

set_option maxHeartbeats 0

private def slotsAt (m : Flags) (k : Nat) : List Nat :=
  ((m.zipIdx k).filter Prod.fst).map Prod.snd

private theorem zipIdx_shift (xs : List α) (k j : Nat) :
    xs.zipIdx (k + j) = (xs.zipIdx k).map (fun p => (p.1, p.2 + j)) := by
  induction xs generalizing k j with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      have hidx : (k + j) + 1 = (k + 1) + j := by omega
      rw [hidx, ih (k + 1) j]
      simp

private theorem zipIdx_shift_one (xs : List α) :
    xs.zipIdx 1 = (xs.zipIdx).map (fun p => (p.1, p.2 + 1)) := by
  simpa using zipIdx_shift xs 0 1

private theorem slotsAt_cons (ok : Bool) (m : Flags) (k : Nat) :
    slotsAt (ok :: m) k = (if ok then [k] else []) ++ slotsAt m (k + 1) := by
  unfold slotsAt
  rw [List.zipIdx_cons]
  cases ok <;> simp

private theorem slots_aux (m : Flags) (k : Nat) :
    slotsAt m k = (slots m).map (fun i => k + i) := by
  induction m generalizing k with
  | nil => simp [slotsAt, slots]
  | cons ok m ih =>
      rw [slotsAt_cons]
      cases ok
      · rw [ih (k + 1)]
        have hf : slots (false :: m) = slotsAt m 1 := by
          simp [slots, slotsAt, List.zipIdx_cons]
        rw [hf, ih 1]
        rw [List.map_map]
        simp only [Function.comp_def]
        apply List.map_congr_left
        intro i hi
        omega
      · rw [ih (k + 1)]
        have ht : slots (true :: m) = 0 :: slotsAt m 1 := by
          simp [slots, slotsAt, List.zipIdx_cons]
        rw [ht, ih 1]
        simp only [ite_true, List.map_cons]
        rw [List.map_map]
        simp only [Function.comp_def]
        simp
        congr 1
        intro i hi
        omega

private theorem slots_cons_false (m : Flags) : slots (false :: m) = (slots m).map Nat.succ := by
  have hf : slots (false :: m) = slotsAt m 1 := by
    simp [slots, slotsAt, List.zipIdx_cons]
  rw [hf, slots_aux]
  apply List.map_congr_left
  intro i hi
  omega

private theorem slots_cons_true (m : Flags) : slots (true :: m) = 0 :: (slots m).map Nat.succ := by
  have ht : slots (true :: m) = 0 :: slotsAt m 1 := by
    simp [slots, slotsAt, List.zipIdx_cons]
  rw [ht, slots_aux]
  congr 1
  apply List.map_congr_left
  intro i hi
  omega

private theorem count_take_cons (ok : Bool) (m : Flags) (i : Nat) :
    ((ok :: m).take (i + 1)).count true = (m.take i).count true + (if ok then 1 else 0) := by
  cases ok <;> simp

private def recoverFrom {α : Type} (m : Flags) (dense : List α) (shift : Nat) : List (Option α) :=
  m.zipIdx.map (fun (ok, i) => if ok then dense[(m.take i).count true + shift]? else none)

private theorem recover_eq_recoverFrom {α : Type} (m : Flags) (dense : List α) :
    recover m dense = recoverFrom m dense 0 := by
  simp [recover, recoverFrom]

private theorem recoverFrom_cons (ok : Bool) (m : Flags) {α : Type} (dense : List α) (shift : Nat) :
    recoverFrom (ok :: m) dense shift =
      (if ok then dense[shift]? else none) ::
        recoverFrom m dense (shift + (if ok then 1 else 0)) := by
  unfold recoverFrom
  rw [List.zipIdx_cons]
  simp only [List.map_cons]
  congr 1
  · cases ok <;> simp
  · rw [zipIdx_shift_one]
    simp only [List.map_map]
    apply List.map_congr_left
    intro p hp
    rcases p with ⟨ok', i⟩
    by_cases hok' : ok'
    · by_cases hok : ok <;> simp [hok', hok] <;> congr 1 <;> omega
    · simp [hok']

private theorem getElem?_append_add {α : Type} (pre xs : List α) (j : Nat) :
    (pre ++ xs)[pre.length + j]? = xs[j]? := by
  rw [List.getElem?_append]
  have hnot : ¬ pre.length + j < pre.length := by omega
  simp [hnot]

private theorem recoverFrom_eq_recover_of_append (m : Flags) (pre second : List Bool) :
    recoverFrom m (pre ++ second) pre.length = recover m second := by
  rw [recover_eq_recoverFrom]
  unfold recoverFrom
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨ok, i⟩
  by_cases hok : ok
  · simp [hok]
    rw [Nat.add_comm]
    exact getElem?_append_add pre second ((m.take i).count true)
  · simp [hok]

private theorem recover_cons_false (m : Flags) (second : List Bool) :
    recover (false :: m) second = none :: recover m second := by
  rw [recover_eq_recoverFrom, recover_eq_recoverFrom]
  rw [recoverFrom_cons]
  simp

private theorem recover_cons_true (m : Flags) (b : Bool) (second : List Bool) :
    recover (true :: m) (b :: second) = some b :: recover m second := by
  rw [recover_eq_recoverFrom, recover_eq_recoverFrom]
  rw [recoverFrom_cons]
  simp
  rw [show b :: second = [b] ++ second by rfl]
  exact recoverFrom_eq_recover_of_append m [b] second

private theorem pack_cons_true {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (true :: m) (x :: v) = x :: pack m v := by
  simp [pack]

private theorem pack_cons_false {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (false :: m) (x :: v) = pack m v := by
  simp [pack]

private theorem recoverFrom_pack_append {α : Type} (m : Flags) (v : List α)
    (h : m.length = v.length) (pre : List α) :
    recoverFrom m (pre ++ pack m v) pre.length =
      (m.zip v).map (fun (ok, x) => if ok then some x else none) := by
  induction m generalizing v pre with
  | nil =>
      cases v with
      | nil => simp [recoverFrom]
      | cons x v' => simp at h
  | cons ok m ih =>
      cases v with
      | nil => simp at h
      | cons x v' =>
          have hv : m.length = v'.length := by simpa using h
          cases ok
          · rw [recoverFrom_cons]
            simp [pack_cons_false, ih v' hv pre]
          · rw [recoverFrom_cons]
            simp only [ite_true, pack_cons_true]
            have hget : (pre ++ (x :: pack m v'))[pre.length]? = some x := by
              rw [List.getElem?_append]
              simp
            rw [hget]
            rw [show pre ++ x :: pack m v' = (pre ++ [x]) ++ pack m v' by simp]
            rw [show pre.length + 1 = (pre ++ [x]).length by simp]
            rw [ih v' hv (pre ++ [x])]
            simp

private theorem pack_sum_partition (m : Flags) (widths : List Nat)
    (h : m.length = widths.length) :
    widths.sum = (pack m widths).sum + (pack (m.map (!·)) widths).sum := by
  induction m generalizing widths with
  | nil =>
      cases widths with
      | nil => simp [pack]
      | cons w ws => simp at h
  | cons ok m ih =>
      cases widths with
      | nil => simp at h
      | cons w ws =>
          have hw : m.length = ws.length := by simpa using h
          specialize ih ws hw
          cases ok
          · simp [pack_cons_false, pack_cons_true]
            omega
          · simp [pack_cons_true, pack_cons_false]
            omega

private def stopAt (m : Flags) (k : Nat) : Nat := ((slots m)[k]?).getD m.length

private theorem stopAt_cons_false (m : Flags) (k : Nat) :
    stopAt (false :: m) k = stopAt m k + 1 := by
  unfold stopAt
  rw [slots_cons_false]
  simp only [List.length_cons, List.getElem?_map]
  cases (slots m)[k]? with
  | none => simp
  | some i => simp

private theorem stopAt_cons_true_zero (m : Flags) : stopAt (true :: m) 0 = 0 := by
  unfold stopAt
  rw [slots_cons_true]
  simp

private theorem stopAt_cons_true_succ (m : Flags) (k : Nat) :
    stopAt (true :: m) (k + 1) = stopAt m k + 1 := by
  unfold stopAt
  rw [slots_cons_true]
  simp only [List.getElem?_cons_succ, List.length_cons, List.getElem?_map]
  cases (slots m)[k]? with
  | none => simp
  | some i => simp

theorem sound_MaskCompose : MaskCompose.Goal := by
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
            rw [pack_cons_false, recover_cons_false]
            simp [pack_cons_false]
            exact ih v' second hv hs
          · cases second with
            | nil => simp at hsecond
            | cons b second' =>
                have hs : second'.length = m.count true := by simpa using hsecond
                rw [pack_cons_true, recover_cons_true]
                simp
                cases b
                · simp [pack_cons_false]
                  exact ih v' second' hv hs
                · simp [pack_cons_true]
                  exact ih v' second' hv hs

theorem sound_ChunkOffsets : ChunkOffsets.Goal := by
  intro a b
  change slotsAt (a ++ b) 0 = slots a ++ (slots b).map (fun i => a.length + i)
  rw [show slotsAt (a ++ b) 0 = slotsAt a 0 ++ slotsAt b a.length by
    unfold slotsAt
    rw [List.zipIdx_append]
    simp]
  rw [show slotsAt a 0 = slots a by rfl, slots_aux]

theorem sound_ExpandBack : ExpandBack.Goal := by
  intro m v hlen
  simpa [recover_eq_recoverFrom] using (recoverFrom_pack_append m v hlen [])

theorem sound_DecodeSavings : DecodeSavings.Goal := by
  intro m widths hlen
  have hsum := pack_sum_partition m widths hlen
  omega

theorem sound_EarlyDemand : EarlyDemand.Goal := by
  intro m xs k hlen
  change (pack m xs).take k = pack (m.take (stopAt m k)) (xs.take (stopAt m k))
  induction m generalizing xs k with
  | nil =>
      cases xs with
      | nil => simp [stopAt, pack, slots]
      | cons x xs' => simp at hlen
  | cons ok m ih =>
      cases xs with
      | nil => simp at hlen
      | cons x xs' =>
          have hxs : m.length = xs'.length := by simpa using hlen
          cases ok
          · rw [pack_cons_false, stopAt_cons_false]
            rw [show (false :: m).take (stopAt m k + 1) = false :: m.take (stopAt m k) by simp]
            rw [show (x :: xs').take (stopAt m k + 1) = x :: xs'.take (stopAt m k) by simp]
            rw [pack_cons_false]
            exact ih xs' k hxs
          · cases k with
            | zero =>
                rw [stopAt_cons_true_zero]
                simp [pack]
            | succ k' =>
                rw [pack_cons_true, stopAt_cons_true_succ]
                rw [show (true :: m).take (stopAt m k' + 1) = true :: m.take (stopAt m k') by simp]
                rw [show (x :: xs').take (stopAt m k' + 1) = x :: xs'.take (stopAt m k') by simp]
                rw [pack_cons_true]
                simpa using ih xs' k' hxs

end TinylambOpt.ResearchProofs.Atlas.ExecutorResources.LaneCompaction
