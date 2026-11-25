import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

namespace TinylambOpt.ResearchProofs.Atlas.ExecutorResources

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline Pipeline
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges Exchanges
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather Gather
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction LaneCompaction

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! ## Pipeline -/

private def pipeSumF (xs : List Job) : Nat := (xs.map Prod.fst).sum
private def pipeSumS (xs : List Job) : Nat := (xs.map Prod.snd).sum

private def pipeCuts (s : Nat) : List Job → List Nat
  | [] => []
  | (a, b) :: rest => (s + a + b + pipeSumS rest) :: pipeCuts (s + a) rest

private theorem pipeMaxCons (x : Nat) (xs : List Nat) :
    (x :: xs).max?.getD 0 = max x (xs.max?.getD 0) := by
  rw [List.max?_cons]
  cases h : xs.max? <;> simp [h]

private theorem pipeCutListEq (s : Nat) (xs : List Job) :
    xs.zipIdx.map (fun (_, i) =>
      s + ((xs.take (i + 1)).map Prod.fst).sum + ((xs.drop i).map Prod.snd).sum) =
      pipeCuts s xs := by
  induction xs generalizing s with
  | nil => rfl
  | cons j rest ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons]
      dsimp [pipeCuts]
      congr 1
      · simp [pipeSumS]
        omega
      · rw [show rest.zipIdx 1 = (rest.zipIdx 0).map (fun p : Job × Nat => (p.1, p.2 + 1)) by
          simpa using (List.zipIdx_succ (l := rest) (i := 0))]
        rw [List.map_map]
        rw [← ih (s + j.1)]
        apply List.map_congr_left
        intro p hp
        rcases p with ⟨x, k⟩
        dsimp
        simp
        omega

private theorem pipeFoldBound (s t : Nat) (xs : List Job) :
    (xs.foldl step (s, t)).2 = max (t + pipeSumS xs) ((pipeCuts s xs).max?.getD 0) := by
  induction xs generalizing s t with
  | nil => simp [pipeSumS, pipeCuts]
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp only [step]
      rw [ih]
      change max (max (s + a) t + b + pipeSumS rest) ((pipeCuts (s + a) rest).max?.getD 0) =
        max (t + (b + pipeSumS rest)) (((s + a + b + pipeSumS rest) :: pipeCuts (s + a) rest).max?.getD 0)
      rw [pipeMaxCons]
      omega

private theorem pipeFinishBound (xs : List Job) :
    finish xs = max (pipeSumS xs) ((pipeCuts 0 xs).max?.getD 0) := by
  dsimp [finish]
  rw [pipeFoldBound]
  simp

private theorem pipeFoldFst (s t : Nat) (xs : List Job) :
    (xs.foldl step (s, t)).1 = s + pipeSumF xs := by
  induction xs generalizing s t with
  | nil => simp [pipeSumF]
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp only [step]
      rw [ih]
      simp [pipeSumF]
      omega

private theorem pipeFstLeSnd (s t : Nat) (xs : List Job) (h : s ≤ t) :
    (xs.foldl step (s, t)).1 ≤ (xs.foldl step (s, t)).2 := by
  induction xs generalizing s t with
  | nil => simpa using h
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp only [step]
      apply ih
      omega

private theorem pipeCutsAdd (s c : Nat) (xs : List Job) :
    pipeCuts (s + c) xs = (pipeCuts s xs).map (fun v => c + v) := by
  induction xs generalizing s with
  | nil => rfl
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      dsimp [pipeCuts]
      simp only [List.map_cons]
      rw [← ih (s + a)]
      rw [show s + c + a = s + a + c by omega]
      congr 1
      omega

private theorem pipeCutsZero (s : Nat) (xs : List Job) :
    pipeCuts s xs = (pipeCuts 0 xs).map (fun v => s + v) := by
  simpa using pipeCutsAdd 0 s xs

private theorem pipeMaxMapAdd (c : Nat) {xs : List Nat} (h : xs ≠ []) :
    (xs.map (fun v => c + v)).max?.getD 0 = c + xs.max?.getD 0 := by
  induction xs with
  | nil => contradiction
  | cons x xs ih =>
      rw [List.map_cons, pipeMaxCons, pipeMaxCons]
      cases xs with
      | nil => simp
      | cons y ys =>
          rw [ih (by simp : y :: ys ≠ [])]
          omega

private theorem pipeFinishSingle (a b : Nat) : finish [(a, b)] = a + b := by
  simp [finish, step]

private theorem pipeReplicateSucc (n : Nat) (j : Job) :
    List.replicate (n + 1) j = List.replicate n j ++ [j] := by
  induction n with
  | zero => simp
  | succ n ih =>
      simpa [List.replicate_succ] using congrArg (List.cons j) ih

theorem sound_CriticalCut : CriticalCut.Goal := by
  intro xs
  rw [pipeFinishBound]
  have hle : pipeSumS xs ≤ (pipeCuts 0 xs).max?.getD 0 := by
    cases xs with
    | nil => simp [pipeSumS]
    | cons j rest =>
        rcases j with ⟨a, b⟩
        dsimp [pipeCuts]
        rw [pipeMaxCons]
        simp [pipeSumS]
        omega
  have hm : xs.zipIdx.map (fun (_, i) =>
        ((xs.take (i + 1)).map Prod.fst).sum + ((xs.drop i).map Prod.snd).sum) =
      pipeCuts 0 xs := by
    simpa only [Nat.zero_add] using pipeCutListEq 0 xs
  rw [hm]
  apply Nat.le_antisymm
  · exact Nat.max_le_of_le_of_le hle (Nat.le_refl _)
  · exact Nat.le_max_right _ _

theorem sound_BatchSummary : BatchSummary.Goal := by
  intro a b
  let s0 := (a.foldl step (0, 0)).1
  let t0 := (a.foldl step (0, 0)).2
  have hs0 : s0 = pipeSumF a := by
    dsimp [s0]
    rw [pipeFoldFst]
    simp
  have ht0 : t0 = finish a := rfl
  have hst : s0 ≤ t0 := by
    dsimp [s0, t0]
    exact pipeFstLeSnd 0 0 a (by omega)
  have hfin : finish (a ++ b) = max (t0 + pipeSumS b) ((pipeCuts s0 b).max?.getD 0) := by
    dsimp [finish, s0, t0]
    rw [List.foldl_append]
    rw [pipeFoldBound]
  cases b with
  | nil =>
      simp [pipeCuts, pipeSumS, finish] at hfin ⊢
      have hf_le : pipeSumF a ≤ finish a := by
        rw [← hs0, ← ht0]
        exact hst
      simp only [pipeSumF] at hf_le ⊢
      omega
  | cons j bs =>
      have hne : pipeCuts 0 (j :: bs) ≠ [] := by simp [pipeCuts]
      rw [pipeCutsZero] at hfin
      rw [pipeMaxMapAdd s0 hne] at hfin
      have hfb : finish (j :: bs) = max (pipeSumS (j :: bs)) ((pipeCuts 0 (j :: bs)).max?.getD 0) :=
        pipeFinishBound _
      rw [hfin, hfb]
      rw [hs0, ht0] at *
      simp only [pipeSumF] at *
      by_cases hsc : pipeSumS (j :: bs) ≤ (pipeCuts 0 (j :: bs)).max?.getD 0
      · rw [Nat.max_eq_right hsc]
        rfl
      · have hcs : (pipeCuts 0 (j :: bs)).max?.getD 0 ≤ pipeSumS (j :: bs) := by omega
        rw [Nat.max_eq_left hcs]
        have hAC : (a.map Prod.fst).sum + (pipeCuts 0 (j :: bs)).max?.getD 0 ≤
            (a.map Prod.fst).sum + pipeSumS (j :: bs) :=
          Nat.add_le_add_left hcs _
        have hAS : (a.map Prod.fst).sum + pipeSumS (j :: bs) ≤
            finish a + pipeSumS (j :: bs) :=
          Nat.add_le_add_right hst _
        calc
          max (finish a + pipeSumS (j :: bs))
              ((a.map Prod.fst).sum + (pipeCuts 0 (j :: bs)).max?.getD 0) =
              finish a + pipeSumS (j :: bs) :=
            Nat.max_eq_left (Nat.le_trans hAC hAS)
          _ = max (finish a + pipeSumS (j :: bs))
                ((a.map Prod.fst).sum + pipeSumS (j :: bs)) :=
            (Nat.max_eq_left hAS).symm

theorem sound_SteadyState : SteadyState.Goal := by
  intro n a b hn
  induction n with
  | zero => omega
  | succ n ih =>
      cases n with
      | zero => simp [finish, step]
      | succ n =>
          have hi := ih (by omega : 0 < Nat.succ n)
          rw [show List.replicate (Nat.succ (Nat.succ n)) (a, b) =
              List.replicate (Nat.succ n) (a, b) ++ [(a, b)] by
            rw [show Nat.succ (Nat.succ n) = Nat.succ n + 1 by omega]
            exact pipeReplicateSucc (Nat.succ n) (a, b)]
          rw [sound_BatchSummary]
          simp [pipeSumF, pipeSumS, pipeFinishSingle] at hi ⊢
          rw [hi]
          clear ih hi
          by_cases hab : a ≤ b
          · have hm : max a b = b := Nat.max_eq_right hab
            simp only [hm] at ⊢
            simp only [Nat.mul_add, Nat.add_mul, Nat.one_mul] at ⊢
            have hmul : n * a ≤ n * b := by
              simpa [Nat.mul_comm] using Nat.mul_le_mul_right n hab
            omega
          · have hba : b ≤ a := by omega
            have hm : max a b = a := Nat.max_eq_left hba
            simp only [hm] at ⊢
            simp only [Nat.mul_add, Nat.add_mul, Nat.one_mul] at ⊢
            have hmul : n * b ≤ n * a := by
              simpa [Nat.mul_comm] using Nat.mul_le_mul_right n hba
            omega

private theorem pipeStepTMono (s t₁ t₂ : Nat) (h : t₁ ≤ t₂) (xs : List Job) :
    (xs.foldl step (s, t₁)).2 ≤ (xs.foldl step (s, t₂)).2 := by
  induction xs generalizing s t₁ t₂ with
  | nil => simpa using h
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons, List.foldl_cons]
      dsimp only [step]
      apply ih
      omega

private theorem pipeSplitPair (s t a b c d : Nat) :
    let p := [(a, b), (c, d)].foldl step (s, t)
    let q := [(a + c, b + d)].foldl step (s, t)
    p.1 = q.1 ∧ p.2 ≤ q.2 := by
  dsimp
  simp [step]
  constructor <;> omega

theorem sound_SplitOverlap : SplitOverlap.Goal := by
  intro l r a b c d
  dsimp [finish]
  rw [show (l ++ [(a, b), (c, d)] ++ r).foldl step (0, 0) =
      r.foldl step ([(a, b), (c, d)].foldl step (l.foldl step (0, 0))) by
    rw [List.foldl_append, List.foldl_append]]
  rw [show (l ++ [(a + c, b + d)] ++ r).foldl step (0, 0) =
      r.foldl step ([(a + c, b + d)].foldl step (l.foldl step (0, 0))) by
    rw [List.foldl_append, List.foldl_append]]
  let s := (l.foldl step (0, 0)).1
  let t := (l.foldl step (0, 0)).2
  change (r.foldl step ([(a, b), (c, d)].foldl step (s, t))).2 ≤
    (r.foldl step ([(a + c, b + d)].foldl step (s, t))).2
  have hgen := pipeSplitPair s t a b c d
  dsimp at hgen
  have hmono := pipeStepTMono (([(a, b), (c, d)].foldl step (s, t)).1) _ _ hgen.2 r
  have hfst : ([(a, b), (c, d)].foldl step (s, t)).1 =
      ([(a + c, b + d)].foldl step (s, t)).1 := by
    simpa [step] using hgen.1
  rw [hfst] at hmono
  have hleft : (([(a + c, b + d)].foldl step (s, t)).fst,
      (step (step (s, t) (a, b)) (c, d)).snd) =
      [(a, b), (c, d)].foldl step (s, t) := by
    rw [← hfst]
    exact Prod.eta _
  have hright : (([(a + c, b + d)].foldl step (s, t)).fst,
      (step (s, t) (a + c, b + d)).snd) =
      [(a + c, b + d)].foldl step (s, t) := by
    exact Prod.eta _
  rw [hleft, hright] at hmono
  exact hmono

/-! ## Exchanges -/

theorem refutation_Exchanges_GlobalPacking :
    ¬ Exchanges.GlobalPacking.Goal := by
  intro h
  have hh := h 4 [(0, 1), (1, 1)] (by decide)
  have hneq : ¬ (Exchanges.traffic 4 [(0, 1), (1, 1)] =
      Exchanges.packets 4 ([(0, 1), (1, 1)].map Prod.snd).sum) := by
    native_decide
  exact hneq hh

/-! ## Gather -/

private theorem gatherNodupEraseDups [BEq α] [LawfulBEq α] : ∀ xs : List α, xs.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => ! b == a) as := (List.mem_eraseDups.mp h)
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact gatherNodupEraseDups (as.filter (fun b => ! b == a))
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem gatherEraseDupsLenLe [BEq α] : ∀ xs : List α, xs.eraseDups.length ≤ xs.length
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      simp only [List.length_cons]
      exact Nat.succ_le_succ (Nat.le_trans (gatherEraseDupsLenLe (as.filter (fun b => ! b == a)))
        (List.length_filter_le _ _))
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem gatherEraseDupsCompMapLe [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    [BEq γ] [LawfulBEq γ] (f : α → β) (g : β → γ) (xs : List α) :
    (xs.map (fun x => g (f x))).eraseDups.length ≤ (xs.map f).eraseDups.length := by
  have hperm : (xs.map (fun x => g (f x))).eraseDups.Perm
      (((xs.map f).eraseDups.map g).eraseDups) := by
    apply (List.perm_ext_iff_of_nodup (gatherNodupEraseDups _) (gatherNodupEraseDups _)).mpr
    intro z
    simp only [List.mem_eraseDups, List.mem_map]
    constructor
    · rintro ⟨a, ha, rfl⟩
      exact ⟨f a, ⟨a, ha, rfl⟩, rfl⟩
    · rintro ⟨_, ⟨a, ha, rfl⟩, rfl⟩
      exact ⟨a, ha, rfl⟩
  have hlen := hperm.length_eq
  rw [hlen]
  simpa using (gatherEraseDupsLenLe ((xs.map f).eraseDups.map g))

theorem sound_CoarserPages : CoarserPages.Goal := by
  intro ids cap n hcap hn
  dsimp [pages]
  simpa [Nat.div_div_eq_div_mul] using
    (gatherEraseDupsCompMapLe (fun id : Nat => id / cap) (fun p : Nat => p / n) ids)

theorem sound_ScatterBack : ScatterBack.Goal := by
  intro payload ids
  dsimp
  apply List.map_congr_left
  intro i hi
  have hiaddr : i ∈ ids.mergeSort.eraseDups := by
    rw [List.mem_eraseDups]
    simpa using hi
  have hidxlt : ids.mergeSort.eraseDups.idxOf i < ids.mergeSort.eraseDups.length :=
    List.idxOf_lt_length_of_mem hiaddr
  have hget : ids.mergeSort.eraseDups[ids.mergeSort.eraseDups.idxOf i] = i :=
    List.getElem_idxOf hidxlt
  have hcache : (ids.mergeSort.eraseDups.map (fun i => payload[i]?))[ids.mergeSort.eraseDups.idxOf i]? = some (payload[i]?) := by
    simp [List.getElem?_eq_getElem hidxlt, hget]
  rw [hcache]
  cases hp : payload[i]? <;> simp [hp]

/-! ## Lane compaction -/

private def laneSlotsAt (m : Flags) (k : Nat) : List Nat :=
  ((m.zipIdx k).filter Prod.fst).map Prod.snd

private theorem laneZipIdxShift (xs : List α) (k j : Nat) :
    xs.zipIdx (k + j) = (xs.zipIdx k).map (fun p => (p.1, p.2 + j)) := by
  induction xs generalizing k j with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      have hidx : (k + j) + 1 = (k + 1) + j := by omega
      rw [hidx, ih (k + 1) j]
      simp

private theorem laneZipIdxShiftOne (xs : List α) :
    xs.zipIdx 1 = (xs.zipIdx).map (fun p => (p.1, p.2 + 1)) := by
  simpa using laneZipIdxShift xs 0 1

private theorem laneSlotsAtCons (ok : Bool) (m : Flags) (k : Nat) :
    laneSlotsAt (ok :: m) k = (if ok then [k] else []) ++ laneSlotsAt m (k + 1) := by
  unfold laneSlotsAt
  rw [List.zipIdx_cons]
  cases ok <;> simp

private theorem laneSlotsAux (m : Flags) (k : Nat) :
    laneSlotsAt m k = (slots m).map (fun i => k + i) := by
  induction m generalizing k with
  | nil => simp [laneSlotsAt, slots]
  | cons ok m ih =>
      rw [laneSlotsAtCons]
      cases ok
      · rw [ih (k + 1)]
        have hf : slots (false :: m) = laneSlotsAt m 1 := by
          simp [slots, laneSlotsAt, List.zipIdx_cons]
        rw [hf, ih 1]
        rw [List.map_map]
        simp only [Function.comp_def]
        apply List.map_congr_left
        intro i hi
        omega
      · rw [ih (k + 1)]
        have ht : slots (true :: m) = 0 :: laneSlotsAt m 1 := by
          simp [slots, laneSlotsAt, List.zipIdx_cons]
        rw [ht, ih 1]
        simp only [ite_true, List.map_cons]
        rw [List.map_map]
        simp only [Function.comp_def]
        simp
        congr 1
        intro i hi
        omega

private theorem laneSlotsConsFalse (m : Flags) : slots (false :: m) = (slots m).map Nat.succ := by
  have hf : slots (false :: m) = laneSlotsAt m 1 := by
    simp [slots, laneSlotsAt, List.zipIdx_cons]
  rw [hf, laneSlotsAux]
  apply List.map_congr_left
  intro i hi
  omega

private theorem laneSlotsConsTrue (m : Flags) : slots (true :: m) = 0 :: (slots m).map Nat.succ := by
  have ht : slots (true :: m) = 0 :: laneSlotsAt m 1 := by
    simp [slots, laneSlotsAt, List.zipIdx_cons]
  rw [ht, laneSlotsAux]
  congr 1
  apply List.map_congr_left
  intro i hi
  omega

private theorem laneCountTakeCons (ok : Bool) (m : Flags) (i : Nat) :
    ((ok :: m).take (i + 1)).count true = (m.take i).count true + (if ok then 1 else 0) := by
  cases ok <;> simp

private def laneRecoverFrom {α : Type} (m : Flags) (dense : List α) (shift : Nat) : List (Option α) :=
  m.zipIdx.map (fun (ok, i) => if ok then dense[(m.take i).count true + shift]? else none)

private theorem laneRecoverEqFrom {α : Type} (m : Flags) (dense : List α) :
    recover m dense = laneRecoverFrom m dense 0 := by
  simp [recover, laneRecoverFrom]

private theorem laneRecoverFromCons (ok : Bool) (m : Flags) {α : Type} (dense : List α) (shift : Nat) :
    laneRecoverFrom (ok :: m) dense shift =
      (if ok then dense[shift]? else none) ::
        laneRecoverFrom m dense (shift + (if ok then 1 else 0)) := by
  unfold laneRecoverFrom
  rw [List.zipIdx_cons]
  simp only [List.map_cons]
  congr 1
  · cases ok <;> simp
  · rw [laneZipIdxShiftOne]
    simp only [List.map_map]
    apply List.map_congr_left
    intro p hp
    rcases p with ⟨ok', i⟩
    by_cases hok' : ok'
    · by_cases hok : ok <;> simp [hok', hok] <;> congr 1 <;> omega
    · simp [hok']

private theorem laneGetElemAppendAdd {α : Type} (pre xs : List α) (j : Nat) :
    (pre ++ xs)[pre.length + j]? = xs[j]? := by
  rw [List.getElem?_append]
  have hnot : ¬ pre.length + j < pre.length := by omega
  simp [hnot]

private theorem laneRecoverFromAppend (m : Flags) (pre second : List Bool) :
    laneRecoverFrom m (pre ++ second) pre.length = recover m second := by
  rw [laneRecoverEqFrom]
  unfold laneRecoverFrom
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨ok, i⟩
  by_cases hok : ok
  · simp [hok]
    rw [Nat.add_comm]
    exact laneGetElemAppendAdd pre second ((m.take i).count true)
  · simp [hok]

private theorem laneRecoverConsFalse (m : Flags) (second : List Bool) :
    recover (false :: m) second = none :: recover m second := by
  rw [laneRecoverEqFrom, laneRecoverEqFrom]
  rw [laneRecoverFromCons]
  simp

private theorem laneRecoverConsTrue (m : Flags) (b : Bool) (second : List Bool) :
    recover (true :: m) (b :: second) = some b :: recover m second := by
  rw [laneRecoverEqFrom, laneRecoverEqFrom]
  rw [laneRecoverFromCons]
  simp
  rw [show b :: second = [b] ++ second by rfl]
  exact laneRecoverFromAppend m [b] second

private theorem lanePackConsTrue {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (true :: m) (x :: v) = x :: pack m v := by
  simp [pack]

private theorem lanePackConsFalse {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (false :: m) (x :: v) = pack m v := by
  simp [pack]

private theorem laneRecoverFromPackAppend {α : Type} (m : Flags) (v : List α)
    (h : m.length = v.length) (pre : List α) :
    laneRecoverFrom m (pre ++ pack m v) pre.length =
      (m.zip v).map (fun (ok, x) => if ok then some x else none) := by
  induction m generalizing v pre with
  | nil =>
      cases v with
      | nil => simp [laneRecoverFrom]
      | cons x v' => simp at h
  | cons ok m ih =>
      cases v with
      | nil => simp at h
      | cons x v' =>
          have hv : m.length = v'.length := by simpa using h
          cases ok
          · rw [laneRecoverFromCons]
            simp [lanePackConsFalse, ih v' hv pre]
          · rw [laneRecoverFromCons]
            simp only [ite_true, lanePackConsTrue]
            have hget : (pre ++ (x :: pack m v'))[pre.length]? = some x := by
              rw [List.getElem?_append]
              simp
            rw [hget]
            rw [show pre ++ x :: pack m v' = (pre ++ [x]) ++ pack m v' by simp]
            rw [show pre.length + 1 = (pre ++ [x]).length by simp]
            rw [ih v' hv (pre ++ [x])]
            simp

private theorem lanePackSumPartition (m : Flags) (widths : List Nat)
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
          · simp [lanePackConsFalse, lanePackConsTrue]
            omega
          · simp [lanePackConsTrue, lanePackConsFalse]
            omega

private def laneStopAt (m : Flags) (k : Nat) : Nat := ((slots m)[k]?).getD m.length

private theorem laneStopAtConsFalse (m : Flags) (k : Nat) :
    laneStopAt (false :: m) k = laneStopAt m k + 1 := by
  unfold laneStopAt
  rw [laneSlotsConsFalse]
  simp only [List.length_cons, List.getElem?_map]
  cases (slots m)[k]? with
  | none => simp
  | some i => simp

private theorem laneStopAtConsTrueZero (m : Flags) : laneStopAt (true :: m) 0 = 0 := by
  unfold laneStopAt
  rw [laneSlotsConsTrue]
  simp

private theorem laneStopAtConsTrueSucc (m : Flags) (k : Nat) :
    laneStopAt (true :: m) (k + 1) = laneStopAt m k + 1 := by
  unfold laneStopAt
  rw [laneSlotsConsTrue]
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
            rw [lanePackConsFalse, laneRecoverConsFalse]
            simp [lanePackConsFalse]
            exact ih v' second hv hs
          · cases second with
            | nil => simp at hsecond
            | cons b second' =>
                have hs : second'.length = m.count true := by simpa using hsecond
                rw [lanePackConsTrue, laneRecoverConsTrue]
                simp
                cases b
                · simp [lanePackConsFalse]
                  exact ih v' second' hv hs
                · simp [lanePackConsTrue]
                  exact ih v' second' hv hs

theorem sound_ChunkOffsets : ChunkOffsets.Goal := by
  intro a b
  change laneSlotsAt (a ++ b) 0 = slots a ++ (slots b).map (fun i => a.length + i)
  rw [show laneSlotsAt (a ++ b) 0 = laneSlotsAt a 0 ++ laneSlotsAt b a.length by
    unfold laneSlotsAt
    rw [List.zipIdx_append]
    simp]
  rw [show laneSlotsAt a 0 = slots a by rfl, laneSlotsAux]

theorem sound_ExpandBack : ExpandBack.Goal := by
  intro m v hlen
  simpa [laneRecoverEqFrom] using (laneRecoverFromPackAppend m v hlen [])

theorem sound_DecodeSavings : DecodeSavings.Goal := by
  intro m widths hlen
  have hsum := lanePackSumPartition m widths hlen
  omega

theorem sound_EarlyDemand : EarlyDemand.Goal := by
  intro m xs k hlen
  change (pack m xs).take k = pack (m.take (laneStopAt m k)) (xs.take (laneStopAt m k))
  induction m generalizing xs k with
  | nil =>
      cases xs with
      | nil => simp [laneStopAt, pack, slots]
      | cons x xs' => simp at hlen
  | cons ok m ih =>
      cases xs with
      | nil => simp at hlen
      | cons x xs' =>
          have hxs : m.length = xs'.length := by simpa using hlen
          cases ok
          · rw [lanePackConsFalse, laneStopAtConsFalse]
            rw [show (false :: m).take (laneStopAt m k + 1) = false :: m.take (laneStopAt m k) by simp]
            rw [show (x :: xs').take (laneStopAt m k + 1) = x :: xs'.take (laneStopAt m k) by simp]
            rw [lanePackConsFalse]
            exact ih xs' k hxs
          · cases k with
            | zero =>
                rw [laneStopAtConsTrueZero]
                simp [pack]
            | succ k' =>
                rw [lanePackConsTrue, laneStopAtConsTrueSucc]
                rw [show (true :: m).take (laneStopAt m k' + 1) = true :: m.take (laneStopAt m k') by simp]
                rw [show (x :: xs').take (laneStopAt m k' + 1) = x :: xs'.take (laneStopAt m k') by simp]
                rw [lanePackConsTrue]
                simpa using ih xs' k' hxs

end TinylambOpt.ResearchProofs.Atlas.ExecutorResources
