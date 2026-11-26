import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Generic machinery for the StorageExecution.FrameSelection research batch:
wire size helpers, `parts` segmentation facts, boundary-DP correctness,
mergeSort head-minimality, and the model-independent frame-selection
properties (ByteFrontier, StrictSavings, HotPointReads, SeekCap,
ParallelEncode, MandatoryCuts, BoundedRaw). -/

namespace TinylambOpt.ResearchProofs.StorageExecution.SECommon
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open List

private theorem congrArg₂' {α β γ : Sort _} (f : α → β → γ) {a a' : α} {b b' : β}
    (ha : a = a') (hb : b = b') : f a b = f a' b' := by
  rw [ha, hb]

/-! ## Wire helpers -/

theorem uleb_len (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

theorem ubytes_pos (n : Nat) : 1 ≤ ubytes n := by
  unfold ubytes; exact Nat.le_max_left _ _

theorem little_len (n k : Nat) : (little n k).length = k := by
  simp [little]

theorem fixed_len (xs : List Nat) : (fixed xs).length = 4 * xs.length := by
  simp only [fixed, List.length_flatMap]
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, little_len, ih, List.length_cons]
      omega

theorem raw_len (xs : List Nat) :
    (raw xs).length = 1 + ubytes xs.length + 4 * xs.length := by
  simp only [raw, List.length_append, List.length_cons, List.length_nil, fixed_len,
    uleb_len]

theorem frame_len (f : Writer) (xs : List Nat) :
    (frame f xs).length = 1 + ubytes xs.length + (f xs).length := by
  simp only [frame, List.length_append, List.length_cons, List.length_nil, uleb_len]

theorem chosen_le_raw (f : Writer) (xs : List Nat) :
    chosen f xs ≤ (raw xs).length := Nat.min_le_right _ _

theorem width_mono {n m : Nat} (h : n ≤ m) : width n ≤ width m := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · have hm : m ≠ 0 := by omega
    have h1 : (if n == 0 then 0 else n.log2 + 1) = n.log2 + 1 := by simp [hn]
    have h2 : (if m == 0 then 0 else m.log2 + 1) = m.log2 + 1 := by simp [hm]
    show (if n == 0 then 0 else n.log2 + 1) ≤ (if m == 0 then 0 else m.log2 + 1)
    rw [h1, h2]
    refine Nat.succ_le_succ (Nat.le_of_not_gt ?_)
    intro hlt
    have h2p : m < 2 ^ n.log2 := (Nat.log2_lt hm).mp hlt
    have : 2 ^ n.log2 ≤ n := Nat.log2_self_le hn
    omega

theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  have hw : width n ≤ width m := width_mono h
  have hd : (width n + 6) / 7 ≤ (width m + 6) / 7 :=
    Nat.div_le_div_right (Nat.add_le_add_right hw 6)
  unfold ubytes
  rcases Nat.le_total 1 ((width n + 6) / 7) with hc | hc
  · rw [Nat.max_eq_right hc]
    exact Nat.le_trans hd (Nat.le_max_right 1 ((width m + 6) / 7))
  · rw [Nat.max_eq_left hc]
    exact Nat.le_max_left 1 ((width m + 6) / 7)

/-! ## `parts` facts -/

private theorem if_pos {c : Prop} [Decidable c] (hc : c) {α : Type} {t e : α} :
    (if c then t else e) = t := by simp [hc]
private theorem if_neg {c : Prop} [Decidable c] (hc : ¬c) {α : Type} {t e : α} :
    (if c then t else e) = e := by simp [hc]

theorem parts_nil_eq : parts ([] : List Nat) = [[]] := rfl

private theorem parts_flatten (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : ps.flatten = xs := by
  induction xs generalizing ps with
  | nil =>
      simp [parts] at h
      simp [h]
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨ps', hps', hmem⟩
      have hflat := ih ps' hps'
      cases ps' with
      | nil =>
          dsimp only at hmem
          have hsing : ps = [[x]] := by simpa using hmem
          subst ps
          simp [hflat]
      | cons a rest =>
          dsimp only at hmem
          have hcases : ps = [x] :: a :: rest ∨ ps = (x :: a) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hcases with rfl | rfl <;>
            simpa [List.flatten_cons] using congrArg (List.cons x) hflat

theorem parts_length_sum (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : (ps.map List.length).sum = xs.length := by
  have hf := parts_flatten xs ps h
  have := congrArg List.length hf
  simpa [List.length_flatten] using this

private theorem parts_ne_nil (xs : List Nat) : parts xs ≠ [] := by
  obtain ⟨ps, hps⟩ : ∃ ps, ps ∈ parts xs := by
    induction xs with
    | nil => exact ⟨[], by simp [parts]⟩
    | cons x xs ih =>
        obtain ⟨qs, hqs⟩ := ih
        rw [parts]
        rcases qs with rfl | ⟨a, rest⟩
        · refine ⟨[[x]], List.mem_flatMap.mpr ⟨[], hqs, ?_⟩⟩
          simp
        · refine ⟨[x] :: a :: rest, List.mem_flatMap.mpr ⟨a :: rest, hqs, ?_⟩⟩
          simp
  intro h
  rw [h] at hps
  exact absurd hps List.not_mem_nil

theorem parts_decompose (xs : List Nat) (hxs : xs ≠ []) (ps : List (List Nat))
    (h : ps ∈ parts xs) :
    ∃ k, 0 < k ∧ k ≤ xs.length ∧ ∃ tail,
      ps = xs.take k :: tail ∧ tail ∈ parts (xs.drop k) := by
  induction xs generalizing ps with
  | nil => exact (hxs rfl).elim
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨ps', hps', hmem⟩
      cases ps' with
      | nil =>
          have hxsnil : xs = [] := by simpa using parts_flatten xs [] hps'
          subst xs
          dsimp only at hmem
          have hsing : ps = [[x]] := by simpa using hmem
          subst ps
          exact ⟨1, by decide, by simp, [], by simp,
            by change ([] : List (List Nat)) ∈ parts []; rw [parts_nil_eq]; simp⟩
      | cons a rest =>
          have hxsne : xs ≠ [] := by
            intro hnil
            subst xs
            simp [parts] at hps'
          have hcases : ps = [x] :: a :: rest ∨ ps = (x :: a) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hcases with rfl | rfl
          · exact ⟨1, by decide, by simp, a :: rest, by simp, hps'⟩
          · obtain ⟨k, hk, hklen, tail, hps, htail⟩ := ih hxsne (a :: rest) hps'
            have hcons : a :: rest = xs.take k :: tail := hps
            have ha : a = xs.take k := (List.cons.inj hcons).1
            have hrt : rest = tail := (List.cons.inj hcons).2
            subst a
            subst tail
            refine ⟨k + 1, by omega, by simp only [List.length_cons]; omega,
              rest, ?_, htail⟩
            simp [List.take_succ_cons, List.drop_succ_cons]

theorem parts_prepend (xs : List Nat) (k : Nat) (tail : List (List Nat))
    (hk : 0 < k) (hklen : k ≤ xs.length) (htail : tail ∈ parts (xs.drop k)) :
    xs.take k :: tail ∈ parts xs := by
  induction k generalizing xs with
  | zero => omega
  | succ k ih =>
      cases xs with
      | nil => simp at hklen
      | cons x xs =>
          simp only [List.length_cons] at hklen
          cases k with
          | zero =>
              cases tail with
              | nil =>
                  have hnil : xs = [] := by simpa using parts_flatten xs [] htail
                  subst xs
                  simp [parts]
              | cons a rest =>
                  rw [parts]
                  exact List.mem_flatMap.mpr ⟨a :: rest, htail, by simp⟩
          | succ k' =>
              rw [List.take_succ_cons]
              rw [List.drop_succ_cons] at htail
              rw [parts]
              exact List.mem_flatMap.mpr ⟨xs.take (k' + 1) :: tail,
                ih xs (by omega) (by omega) htail, by simp⟩

private theorem parts_append (a b : List Nat) (pa pb : List (List Nat))
    (ha : pa ∈ parts a) (hb : pb ∈ parts b) : pa ++ pb ∈ parts (a ++ b) := by
  induction a generalizing pa with
  | nil =>
      simp [parts] at ha
      subst pa
      simpa using hb
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at ha
      rcases ha with ⟨ps', hps', hmem⟩
      have hrec : ps' ++ pb ∈ parts (xs ++ b) := ih ps' hps'
      rw [List.cons_append, parts]
      cases ps' with
      | nil =>
          dsimp only at hmem
          have hsing : pa = [[x]] := by simpa using hmem
          subst pa
          simp only [List.nil_append, List.cons_append]
          cases pb with
          | nil =>
              exact List.mem_flatMap.mpr ⟨[], by simpa using hrec, by simp⟩
          | cons b1 brest =>
              exact List.mem_flatMap.mpr ⟨b1 :: brest, by simpa using hrec, by simp⟩
      | cons a1 rest =>
          have hmem' : pa = [x] :: a1 :: rest ∨ pa = (x :: a1) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hmem' with rfl | rfl
          · cases pb with
            | nil =>
                simp only [List.append_nil, List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: rest, by simpa using hrec, by simp⟩
            | cons b1 brest =>
                simp only [List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: (rest ++ b1 :: brest), hrec, by simp⟩
          · cases pb with
            | nil =>
                simp only [List.append_nil, List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: rest, by simpa using hrec, by simp⟩
            | cons b1 brest =>
                simp only [List.cons_append]
                exact List.mem_flatMap.mpr ⟨a1 :: (rest ++ b1 :: brest), hrec, by simp⟩

/-! ## charge / chosen arithmetic -/

private theorem min_add_sub_eq (a b : Nat) : min a b + (b - a) = b := by
  omega

private theorem chosen_add_saved (f : Writer) (xs : List Nat) :
    chosen f xs + saved f xs = (raw xs).length := by
  unfold chosen saved
  simpa [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using
    min_add_sub_eq (frame f xs).length (raw xs).length

private theorem raw_len_ge_two (xs : List Nat) : 2 ≤ (raw xs).length := by
  rw [raw_len]
  have : 1 ≤ ubytes xs.length := ubytes_pos _
  omega

private theorem frame_len_ge_two (f : Writer) (xs : List Nat) :
    2 ≤ (frame f xs).length := by
  rw [frame_len]
  have : 1 ≤ ubytes xs.length := ubytes_pos _
  omega

private theorem chosen_ge_two (f : Writer) (xs : List Nat) : 2 ≤ chosen f xs :=
  Nat.le_min.mpr ⟨frame_len_ge_two f xs, raw_len_ge_two xs⟩

private theorem charge_append (f : Writer) (fee : Nat) (ps qs : List (List Nat)) :
    charge f fee (ps ++ qs) = charge f fee ps + charge f fee qs := by
  unfold charge
  simp [List.map_append, List.sum_append]

/-! ## min? helpers -/

private theorem min?_getD_le_mem_nat (l : List Nat) (x : Nat) (hx : x ∈ l) :
    l.min?.getD 0 ≤ x := by
  cases l with
  | nil => simp at hx
  | cons y ys =>
      have hs : (y :: ys).min? ≠ none := by
        intro hnone
        have : (y :: ys) = [] := List.min?_eq_none_iff.mp hnone
        simp at this
      rcases hmn : (y :: ys).min? with _ | M
      · simp [hmn] at hs
      · exact ((List.le_min?_iff hmn).1 (Nat.le_refl _)) x hx

private theorem min?_eq_some_getD_of_mem_nat (l : List Nat) (x : Nat) (hx : x ∈ l) :
    l.min? = some (l.min?.getD 0) := by
  cases h : l.min? with
  | none =>
      have : l = [] := List.min?_eq_none_iff.mp h
      subst this
      simp at hx
  | some v => simp [h]

private theorem parts_filter_mono (xs : List Nat) (a b : Nat) (hab : a ≤ b)
    (ps : List (List Nat))
    (h : ps ∈ (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))) :
    ps ∈ (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b)) := by
  simp only [List.mem_filter] at h ⊢
  refine ⟨h.1, ?_⟩
  have h2 := h.2
  rw [List.all_eq_true] at h2 ⊢
  intro p hp
  have hp' := h2 p hp
  simp only [decide_eq_true_eq] at hp' ⊢
  omega

/-! ## The five generic properties -/

private theorem strictSavings_holds' (f : Writer) : strictSavings f := by
  intro ps fee
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hp := chosen_add_saved f p
      simp only [Nat.mul_add, Nat.mul_one] at ih ⊢
      omega

private theorem pageLimit_le_of_cover (n cap k : Nat) (hcap : 0 < cap)
    (hk : n ≤ k * cap) : pageLimit n cap ≤ k := by
  unfold pageLimit
  rw [Nat.div_le_iff_le_mul_add_pred hcap]
  have hm : cap * k = k * cap := Nat.mul_comm _ _
  omega

private theorem charge_ge_mul (f : Writer) (fee : Nat) (ps : List (List Nat))
    (h : ∀ p ∈ ps, 2 ≤ chosen f p) :
    (fee + 2) * ps.length ≤ charge f fee ps := by
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hp : 2 ≤ chosen f p := h p (by simp)
      have ih' : (fee + 2) * ps.length ≤
          (ps.map (fun p => fee + chosen f p)).sum := by
        apply ih
        intro q hq
        exact h q (by simp [hq])
      have hmul : (fee + 2) * (ps.length + 1) =
          (fee + 2) * ps.length + (fee + 2) := by
        rw [Nat.mul_add, Nat.mul_one]
      omega

private theorem mandatoryCuts_holds' (f : Writer) : mandatoryCuts f := by
  intro xs cap fee ps hcap hps hall
  have hsum : (ps.map List.length).sum = xs.length := parts_length_sum xs ps hps
  have hle : xs.length ≤ ps.length * cap := by
    rw [← hsum]
    clear hsum hps
    induction ps with
    | nil => simp
    | cons p ps ih =>
        simp only [List.map_cons, List.sum_cons, List.length_cons]
        have hp := hall p (by simp)
        have ih' := ih (fun q hq => hall q (by simp [hq]))
        have hmul : (ps.length + 1) * cap = ps.length * cap + cap := by
          simp [Nat.add_mul, Nat.one_mul]
        omega
  have hpl : pageLimit xs.length cap ≤ ps.length :=
    pageLimit_le_of_cover xs.length cap ps.length hcap hle
  have hch : (fee + 2) * ps.length ≤ charge f fee ps :=
    charge_ge_mul f fee ps (fun p _ => chosen_ge_two f p)
  exact Nat.le_trans (Nat.mul_le_mul_left _ hpl) hch

private theorem seekCap_holds' (f : Writer) : seekCap f := by
  intro xs a b s hab hb
  unfold budget at hb ⊢
  let la := ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ a))).map (charge f 0)
  let lb := ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ b))).map (charge f 0)
  have hs_mem : s ∈ la := by
    rcases List.min?_eq_some_iff.mp hb with ⟨hmem, _⟩
    exact hmem
  have hs_in_lb : s ∈ lb := by
    rcases List.mem_map.mp hs_mem with ⟨ps, hps, rfl⟩
    exact List.mem_map.mpr ⟨ps, parts_filter_mono xs a b hab ps hps, rfl⟩
  refine ⟨lb.min?.getD 0, min?_getD_le_mem_nat lb s hs_in_lb, ?_⟩
  exact min?_eq_some_getD_of_mem_nat lb s hs_in_lb

/-! ## DP machinery -/

private def objective (f : Writer) (fee price : Nat) (ps : List (List Nat)) : Nat :=
  charge f fee ps + price * seekWork ps

private def seg (n off : Nat) (edge : Nat → Nat → Nat) : Nat :=
  match n with
  | 0 => 0
  | m + 1 =>
      ((List.range (m + 1)).map (fun d =>
        edge off (off + d + 1) + seg (m - d) (off + d + 1) edge)).min?.getD 0

private theorem seg_succ (m off : Nat) (edge : Nat → Nat → Nat) :
    seg (m + 1) off edge =
      ((List.range (m + 1)).map (fun d =>
        edge off (off + d + 1) + seg (m - d) (off + d + 1) edge)).min?.getD 0 := by
  simp [seg]

private def dpStep (edge : Nat → Nat → Nat) (n : Nat)
    (suffix : Array Nat) (done : Nat) : Array Nat :=
  suffix.push (((List.range (done + 1)).map (fun d =>
    edge (n - done - 1) (n - done - 1 + d + 1) +
      suffix[done - d]?.getD 0)).min?.getD 0)

private def suffixAt (edge : Nat → Nat → Nat) (n : Nat) : Nat → Array Nat
  | 0 => #[0]
  | done + 1 => dpStep edge n (suffixAt edge n done) done

private theorem forIn_yield_eq_foldl {α β : Type} (xs : List α) (init : β)
    (step : β → α → β) :
    Id.run (forIn xs init fun x s => pure (ForInStep.yield (step s x))) =
      xs.foldl step init := by
  induction xs generalizing init with
  | nil => rfl
  | cons x xs ih => simp [List.foldl_cons, ih]

private theorem foldl_range_suffixAt (edge : Nat → Nat → Nat) (n done : Nat) :
    (List.range done).foldl (dpStep edge n) #[0] = suffixAt edge n done := by
  induction done with
  | zero => rfl
  | succ done ih =>
      rw [List.range_succ, List.foldl_append, ih]
      simp [suffixAt]

private theorem suffixAt_size (edge : Nat → Nat → Nat) (n done : Nat) :
    (suffixAt edge n done).size = done + 1 := by
  induction done with
  | zero => rfl
  | succ done ih => simp [suffixAt, dpStep, Array.size_push, ih]

private theorem suffixAt_get (edge : Nat → Nat → Nat) (n done : Nat) (hdone : done ≤ n) :
    ∀ j, j ≤ done → (suffixAt edge n done)[j]? = some (seg j (n - j) edge) := by
  induction done with
  | zero =>
      intro j hj
      have : j = 0 := by omega
      subst j
      simp [suffixAt, seg]
  | succ done ih =>
      intro j hj
      rw [suffixAt, dpStep, Array.getElem?_push, suffixAt_size]
      by_cases hjeq : j = done + 1
      · subst j
        rw [if_pos rfl]
        have hopt :
            ((List.range (done + 1)).map (fun d =>
              edge (n - done - 1) (n - done - 1 + d + 1) +
                (suffixAt edge n done)[done - d]?.getD 0)) =
            ((List.range (done + 1)).map (fun d =>
              edge (n - done - 1) (n - done - 1 + d + 1) +
                seg (done - d) (n - done - 1 + d + 1) edge)) := by
          apply List.map_congr_left
          intro d hd
          have hd' : d ≤ done := by
            have hmem := hd
            simp [List.mem_range] at hmem
            omega
          rw [ih (by omega) (done - d) (by omega)]
          have harg : n - (done - d) = n - done - 1 + d + 1 := by omega
          rw [harg]
          simp
        apply congrArg some
        change (((List.range (done + 1)).map (fun d =>
          edge (n - done - 1) (n - done - 1 + d + 1) +
            (suffixAt edge n done)[done - d]?.getD 0)).min?.getD 0) = _
        rw [hopt, show n - (done + 1) = n - done - 1 by omega]
        simp [seg]
      · rw [if_neg hjeq]
        exact ih (by omega) j (by omega)

theorem boundaryCost_eq_seg (n : Nat) (edge : Nat → Nat → Nat) :
    boundaryCost n edge = seg n 0 edge := by
  unfold boundaryCost
  change (Id.run (forIn (List.range n) #[0] fun done suffix =>
    pure (ForInStep.yield (dpStep edge n suffix done)))).back?.getD 0 = seg n 0 edge
  rw [forIn_yield_eq_foldl, foldl_range_suffixAt]
  rw [Array.back?_eq_getElem?, suffixAt_size, Nat.add_sub_cancel]
  rw [suffixAt_get edge n n (Nat.le_refl n) n (Nat.le_refl n)]
  simp

private theorem objective_cons (f : Writer) (fee price : Nat) (xs : List Nat)
    (k : Nat) (tail : List (List Nat)) (hk : k ≤ xs.length) :
    objective f fee price (xs.take k :: tail) =
      fee + chosen f (xs.take k) + price * k ^ 2 + objective f fee price tail := by
  unfold objective charge seekWork
  simp only [List.map_cons, List.sum_cons, List.length_take, Nat.min_eq_left hk,
    Nat.mul_add]
  omega

private def intervalEdge (f : Writer) (fee price : Nat) (xs : List Nat)
    (a b : Nat) : Nat :=
  fee + chosen f ((xs.drop a).take (b - a)) + price * (b - a) ^ 2

private theorem intervalEdge_rfl (f : Writer) (fee price : Nat) (xs : List Nat) :
    (fun a b => fee + chosen f ((xs.drop a).take (b - a)) + price * (b - a) ^ 2) =
      intervalEdge f fee price xs := rfl

private theorem seg_shift (edge : Nat → Nat → Nat) (n off : Nat) :
    seg n off edge = seg n 0 (fun a b => edge (off + a) (off + b)) := by
  revert edge off
  induction n using Nat.strongRecOn with
  | ind n ih =>
    intro edge off
    cases n with
    | zero => simp [seg]
    | succ m =>
      simp only [seg]
      apply congrArg (fun l : List Nat => l.min?.getD 0)
      apply List.map_congr_left
      intro d hd
      have hd' : d ≤ m := by
        have hmem := hd
        simp [List.mem_range] at hmem
        omega
      rw [ih (m - d) (by omega) edge (off + d + 1)]
      simp only [Nat.zero_add]
      rw [ih (m - d) (by omega) (fun a b => edge (off + a) (off + b)) (d + 1)]
      rw [show off + d + 1 = off + (d + 1) by omega]
      congr 1
      apply congrArg (fun e => seg (m - d) 0 e)
      funext a b
      exact congrArg₂' edge (by omega) (by omega)

private theorem intervalEdge_shift (f : Writer) (fee price : Nat) (xs : List Nat)
    (k : Nat) :
    (fun a b => intervalEdge f fee price xs (k + a) (k + b)) =
      intervalEdge f fee price (xs.drop k) := by
  funext a b
  have hsub : k + b - (k + a) = b - a := by omega
  simp [intervalEdge, List.drop_drop, Nat.add_comm, hsub]

private theorem seg_interval_drop (f : Writer) (fee price : Nat) (xs : List Nat)
    (n k : Nat) :
    seg n k (intervalEdge f fee price xs) =
      seg n 0 (intervalEdge f fee price (xs.drop k)) := by
  rw [seg_shift, intervalEdge_shift]

/-! ## costOf: the DP sum along a segmentation -/

private def costOf (edge : Nat → Nat → Nat) : Nat → List (List Nat) → Nat
  | off, [] => 0
  | off, p :: rest => edge off (off + p.length) + costOf edge (off + p.length) rest

private theorem take_len_eq (xs : List Nat) (k : Nat) (hk : k ≤ xs.length) :
    (xs.take k).length = k := by
  rw [List.length_take, Nat.min_eq_left hk]

private theorem cost_objective (f : Writer) (fee price : Nat) :
    ∀ (n : Nat) (ys : List Nat), ys.length = n →
      ∀ (k : Nat) (xs : List Nat), xs.drop k = ys ->
        ∀ ps, ps ∈ parts ys ->
          costOf (intervalEdge f fee price xs) k ps = objective f fee price ps := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ys hlen k xs hxy ps hps
      cases ys with
      | nil =>
          have hps' : ps = [] := by simpa [parts_nil_eq] using hps
          subst hps'
          simp [costOf, objective, charge, seekWork]
      | cons y yt =>
          have hne : y :: yt ≠ [] := by simp
          obtain ⟨ h, hpos, hlen', tail, hps2, htail⟩ :=
            parts_decompose (y :: yt) hne ps hps
          rw [hps2]
          have htl : ((y :: yt).take h).length = h := take_len_eq (y :: yt) h hlen'
          have hE : intervalEdge f fee price xs k (k + h) =
              fee + chosen f ((y :: yt).take h) + price * h ^ 2 := by
            unfold intervalEdge
            have h1 : k + h - k = h := by omega
            have h2 : (xs.drop k).take h = (y :: yt).take h := by rw [hxy]
            rw [h1, h2]
          have hdrop : xs.drop (k + h) = (y :: yt).drop h := by
            rw [← hxy, List.drop_drop]
          have hrec := ih (List.length (List.drop h (y :: yt)))
            (by rw [List.length_drop, hlen]; omega) (List.drop h (y :: yt)) rfl
            (k + h) xs hdrop tail htail
          have hcons := objective_cons f fee price (y :: yt) h tail hlen'
          rw [costOf, htl, hE, hcons, hrec]

private theorem seg_le_cost :
    ∀ (n : Nat) (ys : List Nat), ys.length = n →
      ∀ (k : Nat) (edge : Nat → Nat → Nat) (ps : List (List Nat)),
        ps ∈ parts ys → seg ys.length k edge ≤ costOf edge k ps := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ys hlen k edge ps hps
      subst hlen
      cases ys with
      | nil =>
          have hps' : ps = [] := by simpa [parts_nil_eq] using hps
          subst hps'
          simp [seg, costOf]
      | cons y yt =>
          have hlen1 : (y :: yt).length = yt.length + 1 := rfl
          have hne : y :: yt ≠ [] := by simp
          obtain ⟨ h, hpos, hlen', tail, hps2, htail⟩ :=
            parts_decompose (y :: yt) hne ps hps
          have htl : ((y :: yt).take h).length = h := take_len_eq (y :: yt) h hlen'
          have hmem : (edge k (k + h) + seg (yt.length - (h - 1)) (k + h) edge) ∈
              (List.range (yt.length + 1)).map (fun d =>
                edge k (k + d + 1) + seg (yt.length - d) (k + d + 1) edge) := by
            refine List.mem_map.mpr ⟨h - 1, List.mem_range.mpr (by omega), ?_⟩
            have hc : k + (h - 1) + 1 = k + h := by omega
            rw [hc]
          have hle := min?_getD_le_mem_nat _ _ hmem
          have hid := ih (List.length (List.drop h (y :: yt)))
            (by rw [List.length_drop, hlen1]; omega) (List.drop h (y :: yt)) rfl
            (k + h) edge tail htail
          have hl2 : (List.drop h (y :: yt)).length = yt.length - (h - 1) := by
            rw [List.length_drop]
            omega
          rw [hl2] at hid
          rw [hps2, costOf, htl]
          rw [hlen1, seg_succ]
          omega

private theorem exists_cost_le_seg :
    ∀ (n : Nat) (ys : List Nat), ys.length = n →
      ∀ (k : Nat) (edge : Nat → Nat → Nat),
        ∃ ps, ps ∈ parts ys ∧ costOf edge k ps ≤ seg ys.length k edge := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ys hlen k edge
      subst hlen
      cases ys with
      | nil =>
          refine ⟨ [], by simp [parts_nil_eq], ?_⟩
          simp [seg, costOf]
      | cons y yt =>
          have hlen1 : (y :: yt).length = yt.length + 1 := rfl
          let vals := (List.range (yt.length + 1)).map (fun d =>
            edge k (k + d + 1) + seg (yt.length - d) (k + d + 1) edge)
          have hne : vals ≠ [] := by
            unfold vals
            intro hh
            have hl := congrArg List.length hh
            rw [List.length_map, List.length_range] at hl
            simp at hl
          rcases hmn : vals.min? with _ | v
          · have : vals = [] := List.min?_eq_none_iff.mp hmn
            exact absurd this hne
          · obtain ⟨ d, hd, hveq⟩ : ∃ d ∈ List.range (yt.length + 1),
                v = edge k (k + d + 1) + seg (yt.length - d) (k + d + 1) edge := by
              have hmem := List.min?_mem hmn
              unfold vals at hmem
              rcases List.mem_map.mp hmem with ⟨ d, hd, hmeq⟩
              exact ⟨ d, hd, hmeq ▸ rfl⟩
            obtain ⟨ tail, htail, hcost⟩ :=
              ih ((y :: yt).drop (d + 1)).length
                (by rw [List.length_drop]; omega) ((y :: yt).drop (d + 1)) rfl
                (k + d + 1) edge
            have hd' : d < yt.length + 1 := List.mem_range.mp hd
            refine ⟨ (y :: yt).take (d + 1) :: tail,
              parts_prepend (y :: yt) (d + 1) tail (by omega) (by omega) htail, ?_⟩
            have htl : ((y :: yt).take (d + 1)).length = d + 1 :=
              take_len_eq (y :: yt) (d + 1) (by omega)
            have hlen3 : ((y :: yt).drop (d + 1)).length = yt.length - d := by
              rw [List.length_drop]
              omega
            have hrec : costOf edge (k + d + 1) tail ≤
                seg (yt.length - d) (k + d + 1) edge := by
              rw [← hlen3]
              exact hcost
            have hseg : seg (y :: yt).length k edge = vals.min?.getD 0 := by
              unfold vals
              rw [hlen1, seg_succ]
            rw [costOf, htl, hseg, hmn, Option.getD_some]
            rw [← Nat.add_assoc]
            omega


/-! ## plan minimality -/

private theorem mergeSort_head_min {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c : α, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b : α, (le a b || le b a) = true)
    {l : List α} {a : α} {t : List α} (h : l.mergeSort le = a :: t) :
    ∀ b ∈ l, le a b = true := by
  have hs := List.pairwise_mergeSort trans total l
  rw [h] at hs
  have hhead : ∀ b ∈ t, le a b = true := (List.pairwise_cons.mp hs).1
  intro b hb
  have hb' : b ∈ l.mergeSort le := by simpa using hb
  rw [h] at hb'
  simp only [List.mem_cons] at hb'
  rcases hb' with rfl | hbt
  · simpa using total b b
  · exact hhead b hbt

private theorem plan_mem_parts (f : Writer) (fee price : Nat) (xs : List Nat) :
    plan f fee price xs ∈ parts xs := by
  let le := fun a b => decide (objective f fee price a ≤ objective f fee price b)
  change ((parts xs).mergeSort le).head?.getD [] ∈ parts xs
  cases hm : (parts xs).mergeSort le with
  | nil =>
      have hlen := List.length_mergeSort (parts xs) (le := le)
      rw [hm] at hlen
      exact (parts_ne_nil xs (List.length_eq_zero_iff.mp hlen.symm)).elim
  | cons a t =>
      simp only [List.head?_cons, Option.getD_some]
      apply List.mem_mergeSort.mp
      rw [hm]
      exact List.mem_cons_self

private theorem plan_min (f : Writer) (fee price : Nat) (xs : List Nat)
    (ps : List (List Nat)) (hps : ps ∈ parts xs) :
    objective f fee price (plan f fee price xs) ≤ objective f fee price ps := by
  let le := fun a b => decide (objective f fee price a ≤ objective f fee price b)
  have htrans : ∀ a b c : List (List Nat), le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc))
  have htotal : ∀ a b : List (List Nat), (le a b || le b a) = true := by
    intro a b
    by_cases h : objective f fee price a ≤ objective f fee price b
    · simp [le, h]
    · have h' : objective f fee price b ≤ objective f fee price a := Nat.le_of_not_ge h
      simp [le, h, h']
  have hne : (parts xs).mergeSort le ≠ [] := by
    intro hm
    have hlen := List.length_mergeSort (parts xs) (le := le)
    rw [hm] at hlen
    exact parts_ne_nil xs (List.length_eq_zero_iff.mp hlen.symm)
  cases hm : (parts xs).mergeSort le with
  | nil => exact (hne hm).elim
  | cons a t =>
      have hle := mergeSort_head_min le htrans htotal hm ps hps
      have hnat : objective f fee price a ≤ objective f fee price ps :=
        of_decide_eq_true hle
      change objective f fee price (((parts xs).mergeSort le).head?.getD []) ≤
        objective f fee price ps
      rw [hm]
      simpa [le] using hnat

theorem hotPointReads_holds (f : Writer) : hotPointReads f := by
  intro xs fee p q hpq
  let A := plan f fee p xs
  let B := plan f fee q xs
  have hA : objective f fee p A ≤ objective f fee p B :=
    plan_min f fee p xs B (plan_mem_parts f fee q xs)
  have hB : objective f fee q B ≤ objective f fee q A :=
    plan_min f fee q xs A (plan_mem_parts f fee p xs)
  change seekWork B ≤ seekWork A
  apply Nat.le_of_not_gt
  intro hBA
  change seekWork A < seekWork B at hBA
  let r := q - p
  have hq : q = p + r := by dsimp [r]; omega
  have hpos : 0 < r := by dsimp [r]; omega
  have hr : r * seekWork A < r * seekWork B :=
    Nat.mul_lt_mul_of_pos_left hBA hpos
  have hqB : q * seekWork B = p * seekWork B + r * seekWork B := by
    rw [hq, Nat.add_mul]
  have hqA : q * seekWork A = p * seekWork A + r * seekWork A := by
    rw [hq, Nat.add_mul]
  simp only [objective] at hA hB
  omega

theorem parallelEncode_holds (f : Writer) : parallelEncode f := by
  intro a b fee
  have hp := plan_min f fee 0 (a ++ b) (plan f fee 0 a ++ plan f fee 0 b)
    (parts_append a b _ _ (plan_mem_parts f fee 0 a) (plan_mem_parts f fee 0 b))
  simpa [objective, charge_append, Nat.zero_mul, Nat.add_zero] using hp

theorem strictSavings_holds (f : Writer) : strictSavings f :=
  strictSavings_holds' f

theorem mandatoryCuts_holds (f : Writer) : mandatoryCuts f :=
  mandatoryCuts_holds' f

theorem seekCap_holds (f : Writer) : seekCap f :=
  seekCap_holds' f

/-! ## ByteFrontier -/

theorem byteFrontier_gen (f : Writer) : byteFrontier f := by
  intros xs fee price ps hx hps pick
  let E := intervalEdge f fee price xs
  have hq : quotedOptimum f fee price xs = seg xs.length 0 E := by
    unfold quotedOptimum
    rw [boundaryCost_eq_seg, intervalEdge_rfl]
  have hplan := plan_mem_parts f fee price xs
  have h1 : seg xs.length 0 E ≤ costOf E 0 (plan f fee price xs) :=
    seg_le_cost xs.length xs rfl 0 E _ hplan
  have hps1 : seg xs.length 0 E ≤ costOf E 0 ps :=
    seg_le_cost xs.length xs rfl 0 E ps hps
  obtain ⟨q, hqmb, hqle⟩ := exists_cost_le_seg xs.length xs rfl 0 E
  have hplanq : objective f fee price (plan f fee price xs) ≤
      objective f fee price q :=
    plan_min f fee price xs q hqmb
  have hpo : objective f fee price (plan f fee price xs) =
      costOf E 0 (plan f fee price xs) :=
    (cost_objective f fee price xs.length xs rfl 0 xs rfl
      (plan f fee price xs) hplan).symm
  have hqc : objective f fee price q = costOf E 0 q :=
    (cost_objective f fee price xs.length xs rfl 0 xs rfl q hqmb).symm
  have hps2 : costOf E 0 ps = objective f fee price ps :=
    cost_objective f fee price xs.length xs rfl 0 xs rfl ps hps
  have h1' : seg xs.length 0 E ≤ objective f fee price (plan f fee price xs) := by
    show seg xs.length 0 E ≤ objective f fee price (plan f fee price xs)
    rw [hpo]
    exact h1
  have hqe : objective f fee price (plan f fee price xs) = seg xs.length 0 E :=
    Nat.le_antisymm
      (Nat.le_trans hplanq (Nat.le_trans (Nat.le_of_eq hqc) hqle)) h1'
  refine ⟨?_, ?_⟩
  · show quotedOptimum f fee price xs =
        objective f fee price (plan f fee price xs)
    rw [hq]
    exact hqe.symm
  · show quotedOptimum f fee price xs ≤ objective f fee price ps
    rw [hq]
    exact Nat.le_trans hps1 (Nat.le_of_eq hps2)

/-! ## chunks and BoundedRaw -/

def chunks : Nat → Nat → List Nat → List (List Nat)
  | _, _, [] => []
  | 0, cap, xs@(_ :: _) => [xs]
  | f + 1, cap, x :: xt => take cap (x :: xt) :: chunks f cap (drop cap (x :: xt))

theorem chunks_flatten : ∀ (f cap : Nat) (xs : List Nat),
    (chunks f cap xs).flatten = xs := by
  intro f
  induction f with
  | zero =>
      intro cap xs
      cases xs with
      | nil => simp [chunks]
      | cons x xt => simp [chunks]
  | succ f ih =>
      intro cap xs
      cases xs with
      | nil => simp [chunks]
      | cons x xt =>
          rw [show chunks (f + 1) cap (x :: xt) =
              take cap (x :: xt) :: chunks f cap (List.drop cap (x :: xt)) from rfl,
            List.flatten_cons, ih cap (List.drop cap (x :: xt)), List.take_append_drop]

theorem chunks_in_parts (cap : Nat) (hcap : 0 < cap) :
    ∀ (f : Nat) (xs : List Nat), pageLimit xs.length cap ≤ f →
      chunks f cap xs ∈ parts xs := by
  intro f
  induction f with
  | zero =>
      intro xs hle
      have h0 : xs.length = 0 := by
        rcases Nat.eq_zero_or_pos xs.length with hz | hz
        · exact hz
        · have h1 : 1 ≤ pageLimit xs.length cap := by
            unfold pageLimit
            rw [Nat.one_le_div_iff hcap]
            omega
          omega
      rw [List.length_eq_zero_iff] at h0
      subst h0
      simp [chunks, parts_nil_eq]
  | succ f ih =>
      intro xs hle
      cases xs with
      | nil => simp [chunks, parts_nil_eq]
      | cons x xt =>
          have hl : (x :: xt).length = xt.length + 1 := rfl
          have htake : List.take cap (x :: xt) =
              List.take (min cap (xt.length + 1)) (x :: xt) := by
            rcases Nat.le_total cap (xt.length + 1) with h | h
            · rw [Nat.min_eq_left h]
            · have hlcap : (x :: xt).length ≤ cap := by rw [hl]; exact h
              rw [Nat.min_eq_right h, List.take_of_length_le hlcap, hl.symm,
                List.take_length]
          have hdrop : List.drop cap (x :: xt) =
              List.drop (min cap (xt.length + 1)) (x :: xt) := by
            rcases Nat.le_total cap (xt.length + 1) with h | h
            · rw [Nat.min_eq_left h]
            · rw [Nat.min_eq_right h, hl.symm, List.drop_length,
                ← List.length_eq_zero_iff, List.length_drop, hl]
              omega
          rw [show chunks (f + 1) cap (x :: xt) =
              List.take cap (x :: xt) :: chunks f cap (List.drop cap (x :: xt)) from rfl]
          rw [htake, hdrop]
          refine parts_prepend (x :: xt) (min cap (xt.length + 1))
            (chunks f cap (List.drop (min cap (xt.length + 1)) (x :: xt)))
            (by omega : 0 < min cap (xt.length + 1))
            (Nat.min_le_right _ _) ?_
          have hlen : ((x :: xt).drop (min cap (xt.length + 1))).length =
              if cap ≤ xt.length + 1 then xt.length + 1 - cap else 0 := by
            split
            · rename_i h
              rw [Nat.min_eq_left h, List.length_drop, hl]
            · rename_i h
              rw [Nat.min_eq_right (by omega : xt.length + 1 ≤ cap),
                List.length_drop, hl]
              omega
          have hpage :
              pageLimit ((x :: xt).drop (min cap (xt.length + 1))).length cap ≤ f := by
            rw [hlen]
            split
            · rename_i h
              have hge : cap ≤ xt.length + 1 := h
              have hsub : xt.length + 1 - cap + cap - 1 = xt.length := by omega
              unfold pageLimit
              rw [hsub]
              unfold pageLimit at hle
              rw [show (x :: xt).length = xt.length + 1 from rfl] at hle
              have hcur : xt.length + 1 + cap - 1 = xt.length + cap := by omega
              rw [hcur] at hle
              have hdiv : (xt.length + cap) / cap = xt.length / cap + 1 := by
                rw [← Nat.add_mul_div_right xt.length 1 (by omega : 0 < cap), Nat.one_mul]
              rw [hdiv] at hle
              omega
            · rename_i h
              unfold pageLimit
              rw [show 0 + cap - 1 = cap - 1 by omega]
              rw [Nat.div_eq_of_lt (by omega : cap - 1 < cap)]
              omega
          exact ih ((x :: xt).drop (min cap (xt.length + 1))) hpage

theorem chunks_len_le (cap : Nat) (hcap : 0 < cap) :
    ∀ (f : Nat) (xs : List Nat), pageLimit xs.length cap ≤ f →
      ∀ p ∈ chunks f cap xs, p.length ≤ cap := by
  intro f
  induction f with
  | zero =>
      intro xs hle p hp
      have h0 : xs.length = 0 := by
        rcases Nat.eq_zero_or_pos xs.length with hz | hz
        · exact hz
        · have h1 : 1 ≤ pageLimit xs.length cap := by
            unfold pageLimit
            rw [Nat.one_le_div_iff hcap]
            omega
          omega
      rw [List.length_eq_zero_iff] at h0
      subst h0
      simp [chunks] at hp
  | succ f ih =>
      intro xs hle p hp
      cases xs with
      | nil => simp [chunks] at hp
      | cons x xt =>
          rw [show chunks (f + 1) cap (x :: xt) =
              take cap (x :: xt) :: chunks f cap (List.drop cap (x :: xt)) from rfl] at hp
          simp only [List.mem_cons, List.not_mem_nil, or_false] at hp
          rcases hp with rfl | hp
          · rw [List.length_take]
            exact Nat.min_le_left _ _
          · have hge2 : cap ≤ xt.length + 1 ∨ xt.length + 1 < cap := by omega
            have hlen1 : (x :: xt).length = xt.length + 1 := rfl
            have hpage : pageLimit ((x :: xt).drop cap).length cap ≤ f := by
              unfold pageLimit
              rw [List.length_drop, hlen1]
              rcases hge2 with hge | hlt
              · have h1 : xt.length + 1 - cap + cap - 1 = xt.length := by omega
                rw [h1]
                unfold pageLimit at hle
                rw [hlen1] at hle
                have h2 : xt.length + 1 + cap - 1 = xt.length + cap := by omega
                have hdiv : (xt.length + cap) / cap = xt.length / cap + 1 := by
                  rw [← Nat.add_mul_div_right xt.length 1 (by omega : 0 < cap), Nat.one_mul]
                rw [h2, hdiv] at hle
                omega
              · have h1 : xt.length + 1 - cap = 0 := by omega
                rw [h1, Nat.zero_add]
                rw [Nat.div_eq_of_lt (by omega : cap - 1 < cap)]
                omega
            exact ih ((x :: xt).drop cap) hpage p hp

theorem chunks_length (cap : Nat) (hcap : 0 < cap) :
    ∀ (f : Nat) (xs : List Nat), pageLimit xs.length cap ≤ f →
      (chunks f cap xs).length = pageLimit xs.length cap := by
  intro f
  induction f with
  | zero =>
      intro xs hle
      have h0 : xs.length = 0 := by
        rcases Nat.eq_zero_or_pos xs.length with hz | hz
        · exact hz
        · have h1 : 1 ≤ pageLimit xs.length cap := by
            unfold pageLimit
            rw [Nat.one_le_div_iff hcap]
            omega
          omega
      rw [List.length_eq_zero_iff] at h0
      subst h0
      simp [chunks, pageLimit]
      rw [Nat.div_eq_of_lt (by omega : cap - 1 < cap)]
  | succ f ih =>
      intro xs hle
      cases xs with
      | nil =>
          rw [show chunks (f + 1) cap [] = [] from rfl]
          simp [pageLimit]
          rw [Nat.div_eq_of_lt (by omega : cap - 1 < cap)]
      | cons x xt =>
          have hlen1 : (x :: xt).length = xt.length + 1 := rfl
          have hcnt : (chunks (f + 1) cap (x :: xt)).length =
              1 + (chunks f cap ((x :: xt).drop cap)).length := by
            rw [show chunks (f + 1) cap (x :: xt) =
                take cap (x :: xt) :: chunks f cap (List.drop cap (x :: xt)) from rfl]
            rw [List.length_cons]
            omega
          unfold pageLimit at hle ⊢
          rw [hlen1] at hle ⊢
          have h2 : xt.length + 1 + cap - 1 = xt.length + cap := by omega
          rw [h2] at hle ⊢
          rcases Nat.lt_or_ge (xt.length + 1) cap with hlt | hge
          · have hd : (x :: xt).drop cap = [] := by
              rw [← List.length_eq_zero_iff, List.length_drop]
              omega
            have hdiv : (xt.length + cap) / cap = 1 := by
              apply Nat.le_antisymm
              · rw [Nat.div_le_iff_le_mul_add_pred hcap]
                omega
              · rw [Nat.one_le_div_iff hcap]
                omega
            have hc2 : chunks f cap ((x :: xt).drop cap) = [] := by
              rw [hd]
              simp [chunks]
            rw [hcnt, hc2, List.length_nil, hdiv]
          · have h1 : xt.length + 1 - cap + cap - 1 = xt.length := by omega
            have hdiv : (xt.length + cap) / cap = xt.length / cap + 1 := by
              rw [← Nat.add_mul_div_right xt.length 1 (by omega : 0 < cap), Nat.one_mul]
            have hpage : pageLimit ((x :: xt).drop cap).length cap ≤ f := by
              unfold pageLimit
              rw [List.length_drop, hlen1, h1]
              rw [hdiv] at hle
              omega
            have hlenp : pageLimit ((x :: xt).drop cap).length cap =
                xt.length / cap := by
              unfold pageLimit
              rw [List.length_drop, hlen1, h1]
            have key := ih ((x :: xt).drop cap) hpage
            rw [hlenp] at key
            rw [hcnt, hdiv, key]
            omega

private theorem sum_le_sum_nat (f g : List Nat → Nat) (l : List (List Nat))
    (h : ∀ p ∈ l, f p ≤ g p) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons p l ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h p (by simp)) (ih (fun q hq => h q (by simp [hq])))

theorem boundedRaw_gen (f : Writer) : boundedRaw f := by
  intro xs cap s hcap hb
  obtain ⟨hmem, _hmin⟩ := List.min?_eq_some_iff.mp hb
  rcases List.mem_map.mp hmem with ⟨ps, hps, heq⟩
  subst heq
  rw [List.mem_filter] at hps
  have hchg : charge f 0 ps ≤ charge f 0 (chunks (pageLimit xs.length cap) cap xs) :=
    ((List.le_min?_iff hb).1 (Nat.le_refl _)) _ (by
      refine List.mem_map.mpr ⟨chunks (pageLimit xs.length cap) cap xs,
        List.mem_filter.mpr ⟨chunks_in_parts cap hcap _ xs (Nat.le_refl _), ?_⟩, rfl⟩
      rw [List.all_eq_true]
      intro p hp
      exact decide_eq_true (chunks_len_le cap hcap _ xs (Nat.le_refl _) p hp))
  have hbound : charge f 0 (chunks (pageLimit xs.length cap) cap xs) ≤
      xs.length * 4 + pageLimit xs.length cap * (1 + ubytes cap) := by
    have hin := chunks_in_parts cap hcap _ xs (Nat.le_refl _)
    have hall := chunks_len_le cap hcap _ xs (Nat.le_refl _)
    have hlen := chunks_length cap hcap _ xs (Nat.le_refl _)
    unfold charge
    have hsum : ((chunks (pageLimit xs.length cap) cap xs).map List.length).sum =
        xs.length := parts_length_sum xs _ hin
    refine Nat.le_trans (sum_le_sum_nat (fun p => 0 + chosen f p)
        (fun p => 1 + ubytes cap + 4 * p.length) _ ?_) ?_
    · intro a ha
      have h1 : chosen f a ≤ (raw a).length := chosen_le_raw f a
      rw [raw_len] at h1
      have h2 : ubytes a.length ≤ ubytes cap := ubytes_mono (hall a ha)
      omega
    · have hgen : ∀ (l : List (List Nat)) (c : Nat),
        (l.map (fun p => c + 4 * p.length)).sum = l.length * c
          + 4 * (l.map List.length).sum := by
        intro l c
        induction l with
        | nil => simp
        | cons p l ih =>
            simp only [List.map_cons, List.sum_cons, List.length_cons]
            rw [ih, Nat.add_mul, Nat.one_mul, Nat.mul_add]
            omega
      have hsplit : ((chunks (pageLimit xs.length cap) cap xs).map
          (fun p => 1 + ubytes cap + 4 * p.length)).sum =
          (chunks (pageLimit xs.length cap) cap xs).length * (1 + ubytes cap)
            + 4 * ((chunks (pageLimit xs.length cap) cap xs).map List.length).sum :=
        hgen _ _
      rw [hsplit, hlen, hsum, Nat.mul_comm (4 : Nat) xs.length]
      omega
  exact Nat.le_trans hchg hbound

end SECommon
