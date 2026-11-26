import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

/-!
# ByteLaneTransposeRoundtrip

Transposing bytes into position-modulo-width lanes preserves each byte at its computed
lane offset. A storage reader can address the original byte after reorganizing a
fixed-width frame for compression.
-/


set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_ByteLaneTransposeRoundtrip

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

theorem mod_succ_eq_mod {w j r : Nat} (hw : 0 < w) (hr : r < w) :
    (j + 1) % w = (r + 1) % w ↔ j % w = r := by
  by_cases hw1 : w = 1
  · subst w
    have : r = 0 := by omega
    subst r
    simp
  · have hwgt : 1 < w := by omega
    have h1 : 1 % w = 1 := Nat.mod_eq_of_lt hwgt
    rw [Nat.add_mod, h1]
    have hlt : j % w < w := Nat.mod_lt _ hw
    constructor
    · intro h
      by_cases hjlast : j % w + 1 = w
      · rw [hjlast, Nat.mod_self] at h
        by_cases hrlast : r + 1 = w
        · rw [hrlast, Nat.mod_self] at h
          omega
        · have hrhs : (r + 1) % w = r + 1 := Nat.mod_eq_of_lt (by omega)
          rw [hrhs] at h
          omega
      · have hjlhs : (j % w + 1) % w = j % w + 1 := Nat.mod_eq_of_lt (by omega)
        rw [hjlhs] at h
        by_cases hrlast : r + 1 = w
        · rw [hrlast, Nat.mod_self] at h
          omega
        · have hrhs : (r + 1) % w = r + 1 := Nat.mod_eq_of_lt (by omega)
          rw [hrhs] at h
          omega
    · intro h
      rw [h]

theorem zip_filter_shift (page : List Nat) (w start r : Nat) (hw : 0 < w) (hr : r < w) :
    ((page.zipIdx (start + 1)).filter (fun p => p.2 % w == (r + 1) % w)).map Prod.fst =
    ((page.zipIdx start).filter (fun p => p.2 % w == r)).map Prod.fst := by
  induction page generalizing start with
  | nil => simp
  | cons b bs ih =>
      simp only [List.zipIdx_cons, List.filter_cons]
      have hcond : ((start + 1) % w == (r + 1) % w) = (start % w == r) := by
        apply Bool.eq_iff_iff.mpr
        simpa using (mod_succ_eq_mod (j := start) hw hr)
      rw [hcond]
      by_cases hc : (start % w == r) = true
      · simp [hc]
        rw [ih (start + 1)]
      · simp [hc]
        rw [ih (start + 1)]

theorem byteLane_zero (a : Nat) (rest : List Nat) (w : Nat) (hw : 0 < w) :
    byteLane (a :: rest) w 0 = a :: byteLane rest w (w - 1) := by
  unfold byteLane
  simp only [List.zipIdx_cons, List.filter_cons, List.map_cons]
  have hcond : (0 % w == 0) = true := by simp
  simp [hcond, List.map_cons]
  have hshift := zip_filter_shift rest w 0 (w - 1) hw (by omega : w - 1 < w)
  have hmod : ((w - 1 + 1) % w) = 0 := by
    rw [Nat.sub_add_cancel (by omega : 1 ≤ w), Nat.mod_self]
  rw [hmod] at hshift
  rw [hshift]

theorem byteLane_succ (a : Nat) (rest : List Nat) (w r : Nat) (hw : 0 < w) (hr : r + 1 < w) :
    byteLane (a :: rest) w (r + 1) = byteLane rest w r := by
  unfold byteLane
  simp only [List.zipIdx_cons, List.filter_cons, List.map_cons]
  have hcond : (0 % w == r + 1) = false := by simp [show r + 1 ≠ 0 by omega]
  simp [hcond, List.map_cons]
  have hshift := zip_filter_shift rest w 0 r hw (by omega : r < w)
  have hmod : (r + 1) % w = r + 1 := Nat.mod_eq_of_lt hr
  rw [hmod] at hshift
  rw [hshift]

theorem byteLane_get (page : List Nat) (w r q : Nat) (hw : 0 < w) (hr : r < w) :
    (byteLane page w r)[q]? = page[q * w + r]? := by
  induction page generalizing r q with
  | nil => simp [byteLane]
  | cons a rest ih =>
      by_cases hr0 : r = 0
      · subst r
        rw [byteLane_zero a rest w hw]
        cases q with
        | zero => simp
        | succ q =>
            simp only [List.getElem?_cons_succ]
            rw [ih (w - 1) q (by omega)]
            rw [List.getElem?_cons]
            have hnz : q.succ * w + 0 ≠ 0 := by
              have hp : 0 < q.succ * w := Nat.mul_pos (by omega) hw
              omega
            simp [hnz, Nat.succ_mul, show w ≠ 0 by omega]
            congr 1
            omega
      · have hrpos : 0 < r := by omega
        obtain ⟨r', rfl⟩ : ∃ r', r = r' + 1 := ⟨r - 1, by omega⟩
        rw [byteLane_succ a rest w r' hw (by omega)]
        rw [ih r' q (by omega)]
        rw [List.getElem?_cons]
        have hnz : q * w + (r' + 1) ≠ 0 := by omega
        simp [hnz]

theorem getElem?_flatten_lane {α : Type} (lanes : List (List α)) (r q : Nat)
    (hr : r < lanes.length) (hq : q < (lanes[r]?.getD []).length) :
    lanes.flatten[((lanes.take r).map List.length).sum + q]? = (lanes[r]?.getD [])[q]? := by
  induction lanes generalizing r with
  | nil => simp at hr
  | cons l ls ih =>
      cases r with
      | zero =>
          have hq' : q < l.length := by simpa using hq
          simp only [List.take_zero, List.map_nil, List.sum_nil, Nat.zero_add]
          rw [List.flatten_cons]
          rw [List.getElem?_append_left hq']
          simp
      | succ r =>
          have hr' : r < ls.length := by
            simp only [List.length_cons] at hr
            omega
          simp only [List.take_succ_cons, List.map_cons, List.sum_cons, List.flatten_cons]
          rw [List.getElem?_append_right (by omega)]
          have hidx : l.length + ((List.map List.length (List.take r ls)).sum) + q - l.length =
              ((List.map List.length (List.take r ls)).sum) + q := by
            rw [Nat.add_assoc, Nat.add_sub_cancel_left]
          rw [hidx]
          have hq' : q < (ls[r]?.getD []).length := by simpa [List.getElem?_cons_succ] using hq
          rw [ih r hr' hq']
          simp [List.getElem?_cons_succ]

theorem getElem?_some {α : Type} (l : List α) (i : Nat) (h : i < l.length) :
    ∃ v, l[i]? = some v := by
  induction l generalizing i with
  | nil => simp at h
  | cons a as ih =>
      cases i with
      | zero => exact ⟨a, by simp⟩
      | succ j =>
        have hj : j < as.length := by simp only [List.length_cons] at h; omega
        exact ih j hj

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs.ByteLaneTransposeRoundtrip.Goal := by
  intro page w i hw hi
  dsimp only
  let lanes := (List.range w).map (byteLane page w)
  have hmod : i % w < w := Nat.mod_lt _ hw
  have hr : i % w < lanes.length := by
    dsimp [lanes]
    simpa using hmod
  have hlane : lanes[i % w]? = some (byteLane page w (i % w)) := by
    dsimp [lanes]
    rw [List.getElem?_map, List.getElem?_range hmod]
    simp
  obtain ⟨v, hv⟩ := getElem?_some page i hi
  have hdecomp : (i / w) * w + i % w = i := by
    rw [Nat.mul_comm]
    exact Nat.div_add_mod i w
  have hbl : (byteLane page w (i % w))[i / w]? = some v := by
    rw [byteLane_get page w (i % w) (i / w) hw hmod]
    simpa [hdecomp] using hv
  have hq0 : i / w < (byteLane page w (i % w)).length :=
    (List.getElem?_eq_some_iff.mp hbl).1
  have hq : i / w < (lanes[i % w]?.getD []).length := by
    simpa [hlane] using hq0
  have hflat := getElem?_flatten_lane lanes (i % w) (i / w) hr hq
  rw [hflat]
  simpa [hlane, hv] using hbl

end TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_ByteLaneTransposeRoundtrip


-- C++ references to this model or its proof.
-- table/rematch_storage_codecs.hpp
