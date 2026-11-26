import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `Drawdown.FrameSlice` — Use only the two ragged boundaries and whole-page summaries
  for a window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Ground check on small inputs (both probes return `true`). -/

private def smallLists : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let ls := smallLists n; ls ++ ls.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := smallLists n
  ls.all (fun a => ls.all (fun b =>
    let ps := [a, b]
    (List.range (a.length + 1)).all (fun lo =>
      (List.range 4).all (fun hi =>
        agg.join (agg.reference (a.drop lo))
            (agg.join (combine agg ps) (agg.reference (b.take hi))) =
          agg.reference (((a ++ ps.flatten ++ b).drop lo).take
            (a.length - lo + ps.flatten.length + hi))))))

#eval probe 2
#eval probe 3

/-! FrameSlice is a generic consequence of stitch: page summaries fold into
the summary of their concatenation, and the two ragged boundary pieces
concatenate to exactly the requested window. -/

section Generic

variable {σ : Type} (f : Fold σ)

private theorem foldl_reference (h : stitch f) (l : List (List Nat)) (acc : List Nat) :
    List.foldl f.join (f.reference acc) (l.map f.reference) =
      f.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem combine_reference (h : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  simpa [combine] using foldl_reference f h ps []

private theorem slice_concat (a p z : List Nat) (lo hi : Nat) (hlo : lo ≤ a.length) :
    a.drop lo ++ p ++ z.take hi =
      ((a ++ p ++ z).drop lo).take (a.length - lo + p.length + hi) := by
  rw [show a ++ p ++ z = a ++ (p ++ z) by simp only [List.append_assoc],
    List.drop_append_of_le_length hlo, List.take_append]
  have hd : (a.drop lo).length = a.length - lo := by simp
  rw [hd]
  have hN : a.length - lo + p.length + hi - (a.length - lo) = p.length + hi := by omega
  rw [hN, List.take_append]
  have hp : p.length + hi - p.length = hi := by omega
  rw [hp]
  have htakeA : List.take (a.length - lo + p.length + hi) (a.drop lo) = a.drop lo :=
    List.take_of_length_le (by simp [hd]; omega)
  have htakeP : List.take (p.length + hi) p = p :=
    List.take_of_length_le (by omega)
  rw [htakeA, htakeP]
  exact List.append_assoc _ _ _

private theorem frameSlice_of_stitch (h : stitch f) : frameSlice f := by
  intro a ps z lo hi hlo
  rw [combine_reference f h, h, h]
  refine congrArg f.reference ?_
  simpa only [List.append_assoc] using slice_concat a ps.flatten z lo hi hlo

end Generic

/-! Stitch for the Drawdown fold. The state of a nonempty list is
`(minimum, maximum, maximum drop)`. Minimum and maximum compose with
`min`/`max`, and the cross drop is `maximum a - minimum b`. -/

private def mn (xs : List Nat) := xs.min?.getD 0
private def mx (xs : List Nat) := peak xs
private def ddList (xs : List Nat) : List Nat :=
  xs.zipIdx.map (fun (x, i) => x - ((xs.drop i).min?.getD x))
private def dd (xs : List Nat) := peak (ddList xs)

private theorem reference_cons (x : Nat) (xs : List Nat) :
    agg.reference (x :: xs) = some (mn (x :: xs), mx (x :: xs), dd (x :: xs)) := by
  rfl

private theorem min_getD_eq_mn (l : List Nat) (x : Nat) (hl : l ≠ []) :
    l.min?.getD x = mn l := by
  cases l with
  | nil => contradiction
  | cons y ys => simp [mn, List.min?]

private theorem mn_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    mn (a ++ b) = min (mn a) (mn b) := by
  have hane : a.min? ≠ none := by
    intro h; exact ha (List.min?_eq_none_iff.mp h)
  have hbne : b.min? ≠ none := by
    intro h; exact hb (List.min?_eq_none_iff.mp h)
  cases hma : a.min? with
  | none => exact (hane hma).elim
  | some ma =>
    cases hmb : b.min? with
    | none => exact (hbne hmb).elim
    | some mb =>
      have hmaP := List.min?_eq_some_iff.mp hma
      have hmbP := List.min?_eq_some_iff.mp hmb
      have hmab : (a ++ b).min? = some (min ma mb) := by
        apply List.min?_eq_some_iff.mpr
        constructor
        · by_cases hle : ma ≤ mb
          · rw [Nat.min_eq_left hle]; simp [hmaP.1]
          · rw [Nat.min_eq_right (Nat.le_of_not_ge hle)]; simp [hmbP.1]
        · intro x hx
          simp only [List.mem_append] at hx
          rcases hx with hxa | hxb
          · exact Nat.le_trans (Nat.min_le_left ma mb) (hmaP.2 x hxa)
          · exact Nat.le_trans (Nat.min_le_right ma mb) (hmbP.2 x hxb)
      simp [mn, hma, hmb, hmab]

private theorem mx_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    mx (a ++ b) = max (mx a) (mx b) := by
  have hane : a.max? ≠ none := by
    intro h; exact ha (List.max?_eq_none_iff.mp h)
  have hbne : b.max? ≠ none := by
    intro h; exact hb (List.max?_eq_none_iff.mp h)
  cases hma : a.max? with
  | none => exact (hane hma).elim
  | some ma =>
    cases hmb : b.max? with
    | none => exact (hbne hmb).elim
    | some mb =>
      have hmaP := List.max?_eq_some_iff.mp hma
      have hmbP := List.max?_eq_some_iff.mp hmb
      have hmab : (a ++ b).max? = some (max ma mb) := by
        apply List.max?_eq_some_iff.mpr
        constructor
        · by_cases hle : ma ≤ mb
          · rw [Nat.max_eq_right hle]; simp [hmbP.1]
          · rw [Nat.max_eq_left (Nat.le_of_not_ge hle)]; simp [hmaP.1]
        · intro x hx
          simp only [List.mem_append] at hx
          rcases hx with hxa | hxb
          · exact Nat.le_trans (hmaP.2 x hxa) (Nat.le_max_left ma mb)
          · exact Nat.le_trans (hmbP.2 x hxb) (Nat.le_max_right ma mb)
      simp [mx, peak, hma, hmb, hmab]

private theorem le_mx {x : Nat} {xs : List Nat} (hx : x ∈ xs) : x ≤ mx xs := by
  cases h : xs.max? with
  | none =>
      have hnil : xs = [] := List.max?_eq_none_iff.mp h
      subst xs
      simp at hx
  | some m =>
      have hP := List.max?_eq_some_iff.mp h
      simpa [mx, peak, h] using hP.2 x hx

private theorem member_le_dd {v : Nat} {xs : List Nat} (hv : v ∈ ddList xs) : v ≤ dd xs := by
  cases h : (ddList xs).max? with
  | none =>
      have hnil : ddList xs = [] := List.max?_eq_none_iff.mp h
      rw [hnil] at hv
      simp at hv
  | some m =>
      have hle : v ≤ m := (List.max?_le_iff h).mp (Nat.le_refl m) v hv
      simpa [dd, peak, h] using hle

private theorem drop_ne_nil_of_lt {l : List Nat} {i : Nat} (hi : i < l.length) :
    l.drop i ≠ [] := by
  intro hnil
  have hlen : (l.drop i).length = 0 := by simp [hnil]
  simp [List.length_drop] at hlen
  omega

private theorem dd_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    dd (a ++ b) = max (max (dd a) (dd b)) (mx a - mn b) := by
  apply Nat.le_antisymm
  · simp only [dd, peak]
    cases hM : (ddList (a ++ b)).max? with
    | none => simp
    | some m =>
      simp only [Option.getD]
      apply (List.max?_le_iff hM).mpr
      intro v hv
      simp only [ddList, List.mem_map] at hv
      rcases hv with ⟨⟨x, i⟩, hxi, rfl⟩
      have hzip : (a ++ b)[i]? = some x := List.mem_zipIdx_iff_getElem?.mp hxi
      by_cases hi : i < a.length
      · have hx : x = a[i] := by
          have hzipa : a[i]? = some x := by simpa [List.getElem?_append_left hi] using hzip
          rcases List.getElem?_eq_some_iff.mp hzipa with ⟨_, hval⟩
          exact hval.symm
        have hdropne : a.drop i ≠ [] := drop_ne_nil_of_lt hi
        have hdrop : (a ++ b).drop i = a.drop i ++ b := by
          rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (Nat.le_of_lt hi)]
        have happne : a.drop i ++ b ≠ [] := by simp [hdropne, hb]
        have hget : ((a.drop i ++ b).min?.getD x) = min (mn (a.drop i)) (mn b) := by
          rw [min_getD_eq_mn _ x happne, mn_append _ _ hdropne hb]
        rw [hdrop, hget, hx]
        by_cases hle : mn (a.drop i) ≤ mn b
        · rw [Nat.min_eq_left hle]
          have hmemA : (a[i] - ((a.drop i).min?.getD a[i])) ∈ ddList a := by
            change (a[i] - ((a.drop i).min?.getD a[i])) ∈
              a.zipIdx.map (fun (x, i) => x - ((a.drop i).min?.getD x))
            exact List.mem_map.mpr ⟨(a[i], i),
              List.mem_zipIdx_iff_getElem?.mpr (List.getElem?_eq_some_iff.mpr ⟨hi, rfl⟩), rfl⟩
          have hleA : a[i] - mn (a.drop i) ≤ dd a := by
            have := member_le_dd hmemA
            rwa [min_getD_eq_mn _ _ hdropne] at this
          exact Nat.le_trans hleA
            (Nat.le_trans (Nat.le_max_left (dd a) (dd b))
              (Nat.le_max_left (max (dd a) (dd b)) (mx a - mn b)))
        · have hle' : mn b ≤ mn (a.drop i) := Nat.le_of_not_ge hle
          rw [Nat.min_eq_right hle']
          have hleMX : a[i] ≤ mx a := le_mx (List.getElem_mem hi)
          exact Nat.le_trans (Nat.sub_le_sub_right hleMX (mn b))
            (Nat.le_max_right (max (dd a) (dd b)) (mx a - mn b))
      · have hge : a.length ≤ i := Nat.le_of_not_gt hi
        have hlen : i < (a ++ b).length := by
          by_cases hlt : i < (a ++ b).length
          · exact hlt
          · have hnone : (a ++ b)[i]? = none :=
              List.getElem?_eq_none_iff.mpr (Nat.le_of_not_gt hlt)
            rw [hnone] at hzip
            contradiction
        have hj : i - a.length < b.length := by
          simp [List.length_append] at hlen
          omega
        have hx : x = b[i - a.length] := by
          have hzipb : b[i - a.length]? = some x := by
            rwa [List.getElem?_append_right hge] at hzip
          rcases List.getElem?_eq_some_iff.mp hzipb with ⟨_, hval⟩
          exact hval.symm
        have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
          rw [List.drop_append, List.drop_eq_nil_of_le hge]
          simp
        rw [hdrop, hx]
        have hmemB : (b[i - a.length] - ((b.drop (i - a.length)).min?.getD b[i - a.length])) ∈ ddList b := by
          change (b[i - a.length] - ((b.drop (i - a.length)).min?.getD b[i - a.length])) ∈
            b.zipIdx.map (fun (x, j) => x - ((b.drop j).min?.getD x))
          exact List.mem_map.mpr ⟨(b[i - a.length], i - a.length),
            List.mem_zipIdx_iff_getElem?.mpr
              (List.getElem?_eq_some_iff.mpr ⟨hj, rfl⟩), rfl⟩
        exact Nat.le_trans (member_le_dd hmemB)
          (Nat.le_trans (Nat.le_max_right (dd a) (dd b))
            (Nat.le_max_left (max (dd a) (dd b)) (mx a - mn b)))
  · apply Nat.max_le.mpr
    constructor
    · apply Nat.max_le.mpr
      constructor
      · -- dd a ≤ dd (a ++ b)
        cases hA : (ddList a).max? with
        | none => simp [dd, peak, hA]
        | some ma =>
          simp [dd, peak, hA]
          apply (List.max?_le_iff hA).mpr
          intro v hv
          simp only [ddList, List.mem_map] at hv
          rcases hv with ⟨⟨x, i⟩, hxi, rfl⟩
          have hzipa : a[i]? = some x := List.mem_zipIdx_iff_getElem?.mp hxi
          have hi : i < a.length := by
            by_cases hlt : i < a.length
            · exact hlt
            · have hnone : a[i]? = none := List.getElem?_eq_none_iff.mpr (Nat.le_of_not_gt hlt)
              rw [hnone] at hzipa
              contradiction
          have hx : x = a[i] := by
            rcases List.getElem?_eq_some_iff.mp hzipa with ⟨_, hval⟩
            exact hval.symm
          have hdropne : a.drop i ≠ [] := drop_ne_nil_of_lt hi
          have hdrop : (a ++ b).drop i = a.drop i ++ b := by
            rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (Nat.le_of_lt hi)]
          have happne : a.drop i ++ b ≠ [] := by simp [hdropne, hb]
          have hzipab : (a ++ b)[i]? = some a[i] := by
            rw [List.getElem?_append_left hi]
            exact List.getElem?_eq_some_iff.mpr ⟨hi, rfl⟩
          have hmemAB : (a[i] - (((a ++ b).drop i).min?.getD a[i])) ∈ ddList (a ++ b) := by
            change (a[i] - (((a ++ b).drop i).min?.getD a[i])) ∈
              (a ++ b).zipIdx.map (fun (x, k) => x - (((a ++ b).drop k).min?.getD x))
            exact List.mem_map.mpr ⟨(a[i], i),
              List.mem_zipIdx_iff_getElem?.mpr hzipab, rfl⟩
          have hle : a[i] - min (mn (a.drop i)) (mn b) ≤ dd (a ++ b) := by
            have hle' := member_le_dd hmemAB
            rw [hdrop, min_getD_eq_mn _ _ happne, mn_append _ _ hdropne hb] at hle'
            exact hle'
          rw [hx, min_getD_eq_mn _ _ hdropne]
          exact Nat.le_trans (Nat.sub_le_sub_left (Nat.min_le_left (mn (a.drop i)) (mn b)) _) hle
      · -- dd b ≤ dd (a ++ b)
        cases hB : (ddList b).max? with
        | none => simp [dd, peak, hB]
        | some mb =>
          simp [dd, peak, hB]
          apply (List.max?_le_iff hB).mpr
          intro v hv
          simp only [ddList, List.mem_map] at hv
          rcases hv with ⟨⟨x, j⟩, hxj, rfl⟩
          have hzipb : b[j]? = some x := List.mem_zipIdx_iff_getElem?.mp hxj
          have hj : j < b.length := by
            by_cases hlt : j < b.length
            · exact hlt
            · have hnone : b[j]? = none := List.getElem?_eq_none_iff.mpr (Nat.le_of_not_gt hlt)
              rw [hnone] at hzipb
              contradiction
          have hx : x = b[j] := by
            rcases List.getElem?_eq_some_iff.mp hzipb with ⟨_, hval⟩
            exact hval.symm
          have hzipab : (a ++ b)[a.length + j]? = some b[j] := by
            rw [List.getElem?_append_right (Nat.le_add_right a.length j)]
            simp
          have hmemAB : (b[j] - (((a ++ b).drop (a.length + j)).min?.getD b[j])) ∈ ddList (a ++ b) := by
            change (b[j] - (((a ++ b).drop (a.length + j)).min?.getD b[j])) ∈
              (a ++ b).zipIdx.map (fun (x, k) => x - (((a ++ b).drop k).min?.getD x))
            exact List.mem_map.mpr ⟨(b[j], a.length + j),
              List.mem_zipIdx_iff_getElem?.mpr hzipab, rfl⟩
          have hle : b[j] - ((b.drop j).min?.getD b[j]) ≤ dd (a ++ b) := by
            have hle' := member_le_dd hmemAB
            have hdropa : a.drop (a.length + j) = [] :=
              List.drop_eq_nil_of_le (Nat.le_add_right a.length j)
            rw [List.drop_append, hdropa, List.nil_append] at hle'
            simpa only [Nat.add_sub_cancel_left] using hle'
          rw [hx]
          exact hle
    · -- mx a - mn b ≤ dd (a ++ b)
      cases hM : a.max? with
      | none => exact False.elim (ha (List.max?_eq_none_iff.mp hM))
      | some ma =>
        have hmaP := List.max?_eq_some_iff.mp hM
        obtain ⟨i, hi, hvi⟩ := List.getElem_of_mem hmaP.1
        have hdropne : a.drop i ≠ [] := drop_ne_nil_of_lt hi
        have hdrop : (a ++ b).drop i = a.drop i ++ b := by
          rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (Nat.le_of_lt hi)]
        have happne : a.drop i ++ b ≠ [] := by simp [hdropne, hb]
        have hzipab : (a ++ b)[i]? = some a[i] := by
          rw [List.getElem?_append_left hi]
          exact List.getElem?_eq_some_iff.mpr ⟨hi, rfl⟩
        have hmemAB : (a[i] - (((a ++ b).drop i).min?.getD a[i])) ∈ ddList (a ++ b) := by
          change (a[i] - (((a ++ b).drop i).min?.getD a[i])) ∈
            (a ++ b).zipIdx.map (fun (x, k) => x - (((a ++ b).drop k).min?.getD x))
          exact List.mem_map.mpr ⟨(a[i], i),
            List.mem_zipIdx_iff_getElem?.mpr hzipab, rfl⟩
        have hle : a[i] - min (mn (a.drop i)) (mn b) ≤ dd (a ++ b) := by
          have hle' := member_le_dd hmemAB
          rw [hdrop, min_getD_eq_mn _ _ happne, mn_append _ _ hdropne hb] at hle'
          exact hle'
        have hmx : mx a = ma := by simp [mx, peak, hM]
        have hsub : mx a - mn b ≤ a[i] - min (mn (a.drop i)) (mn b) := by
          rw [hmx, ← hvi]
          exact Nat.sub_le_sub_left (Nat.min_le_right (mn (a.drop i)) (mn b)) _
        exact Nat.le_trans hsub hle

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons av as =>
    cases b with
    | nil => simp [agg, optionalJoin]
    | cons bv bs =>
      rw [show (av :: as) ++ (bv :: bs) = av :: (as ++ bv :: bs) by rfl]
      rw [reference_cons av as, reference_cons bv bs, reference_cons av (as ++ bv :: bs)]
      simp [agg, optionalJoin]
      constructor
      · exact (mn_append (av :: as) (bv :: bs) (by simp) (by simp)).symm
      · constructor
        · exact (mx_append (av :: as) (bv :: bs) (by simp) (by simp)).symm
        · rw [← Nat.max_assoc]
          exact (dd_append (av :: as) (bv :: bs) (by simp) (by simp)).symm

theorem sound_FrameSlice : FrameSlice.Goal := by
  exact frameSlice_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_FrameSlice
