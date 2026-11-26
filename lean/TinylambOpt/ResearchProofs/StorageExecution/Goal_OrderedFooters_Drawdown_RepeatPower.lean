import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_RepeatPower

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown

/-- Small-input truth checks (all four must print `true`). -/
private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n k : Nat) : Bool :=
  (gen n).all (fun xs =>
    repeatState agg (agg.reference xs) k == agg.reference (List.replicate k xs).flatten)

#eval probe 2 3
#eval probe 3 5
#eval probe 4 7
#eval probe 5 9

/-- Summary components for a non-empty list: min, max and maximum ordered drop. -/
private def lo (l : List Nat) := l.min?.getD 0
private def hi (l : List Nat) := l.max?.getD 0
private def ddList (l : List Nat) := l.zipIdx.map (fun (x, i) => x - ((l.drop i).min?.getD x))
private def dd (l : List Nat) := peak (ddList l)
private def moddList (l : List Nat) := l.zipIdx.flatMap (fun (x, i) => (l.drop i).map (fun y => x - y))
private def modd (l : List Nat) := peak (moddList l)
private def crossList (a b : List Nat) := a.flatMap (fun x => b.map (fun y => x - y))
private def cross (a b : List Nat) := peak (crossList a b)

private theorem nat_sub_min (n p q : Nat) : n - min p q = max (n - p) (n - q) := by
  by_cases h : p ≤ q
  · have hmin : min p q = p := Nat.min_eq_left h
    have hmax : max (n - p) (n - q) = n - p := Nat.max_eq_left (by omega)
    simp [hmin, hmax]
  · have hmin : min p q = q := Nat.min_eq_right (by omega)
    have hmax : max (n - p) (n - q) = n - q := Nat.max_eq_right (by omega)
    simp [hmin, hmax]

private theorem min?_getD_ne_nil {l : List Nat} (hl : l ≠ []) (d : Nat) : l.min?.getD d = lo l := by
  unfold lo
  cases hm : l.min? with
  | none => simp [List.min?_eq_none_iff.mp hm] at hl
  | some m => simp

private theorem max?_getD_ne_nil {l : List Nat} (hl : l ≠ []) (d : Nat) : l.max?.getD d = hi l := by
  unfold hi
  cases hm : l.max? with
  | none => simp [List.max?_eq_none_iff.mp hm] at hl
  | some m => simp

private theorem min?_append (a b : List Nat) :
    (a ++ b).min? = match a.min? with
      | none => b.min?
      | some x => match b.min? with | none => some x | some y => some (min x y) := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    cases hxs : xs.min? with
    | none =>
      have hnil : xs = [] := by simpa using hxs
      subst xs
      cases hb : b.min? <;> simp [List.min?_cons, hb]
    | some m =>
      cases hb : b.min? with
      | none => simp [hxs, hb, ih, List.min?_cons]
      | some y => simp [hxs, hb, ih, List.min?_cons, Nat.min_assoc]

private theorem max?_append (a b : List Nat) :
    (a ++ b).max? = match a.max? with
      | none => b.max?
      | some x => match b.max? with | none => some x | some y => some (max x y) := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    cases hxs : xs.max? with
    | none =>
      have hnil : xs = [] := by simpa using hxs
      subst xs
      cases hb : b.max? <;> simp [List.max?_cons, hb]
    | some m =>
      cases hb : b.max? with
      | none => simp [hxs, hb, ih, List.max?_cons]
      | some y => simp [hxs, hb, ih, List.max?_cons, Nat.max_assoc]

private theorem lo_append {a b : List Nat} (ha : a ≠ []) (hb : b ≠ []) :
    lo (a ++ b) = min (lo a) (lo b) := by
  unfold lo
  rw [min?_append]
  obtain ⟨x, hx⟩ := Option.isSome_iff_exists.mp (List.isSome_min?_of_ne_nil ha)
  obtain ⟨y, hy⟩ := Option.isSome_iff_exists.mp (List.isSome_min?_of_ne_nil hb)
  simp [hx, hy]

private theorem hi_append {a b : List Nat} (ha : a ≠ []) (hb : b ≠ []) :
    hi (a ++ b) = max (hi a) (hi b) := by
  unfold hi
  rw [max?_append]
  obtain ⟨x, hx⟩ := Option.isSome_iff_exists.mp (List.isSome_max?_of_ne_nil ha)
  obtain ⟨y, hy⟩ := Option.isSome_iff_exists.mp (List.isSome_max?_of_ne_nil hb)
  simp [hx, hy]

private theorem peak_le (l : List Nat) (m : Nat) (h : ∀ x ∈ l, x ≤ m) : peak l ≤ m := by
  unfold peak
  cases hm : l.max? with
  | none => simp
  | some v => exact (List.max?_le_iff hm).mpr h

private theorem le_peak (l : List Nat) (x : Nat) (hx : x ∈ l) : x ≤ peak l := by
  unfold peak
  cases hm : l.max? with
  | none => simp [List.max?_eq_none_iff.mp hm] at hx
  | some v => exact (List.max?_le_iff hm).mp (Nat.le_refl _) x hx

private theorem peak_cons (x : Nat) (l : List Nat) : peak (x :: l) = max x (peak l) := by
  unfold peak
  cases hm : l.max? with
  | none => simp [hm]
  | some v => simp [hm]

private theorem peak_map_sub (x : Nat) (s : List Nat) (hs : s ≠ []) :
    peak (s.map (fun y => x - y)) = x - lo s := by
  induction s with
  | nil => simp at hs
  | cons y ys ih =>
    cases ys with
    | nil => simp [peak, lo]
    | cons z zs =>
      have ih' := ih (by simp)
      have hlong : lo (y :: z :: zs) = min y (lo (z :: zs)) := by
        unfold lo
        cases hm : (z :: zs).min? with
        | none => simp at hm
        | some m => simp [List.min?_cons, hm]
      rw [hlong, nat_sub_min]
      rw [List.map_cons, peak_cons]
      rw [ih']

private theorem lo_le_of_mem (l : List Nat) (x : Nat) (hx : x ∈ l) : lo l ≤ x := by
  exact List.min?_getD_le_of_mem hx

private theorem cross_eq (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    cross a b = hi a - lo b := by
  apply Nat.le_antisymm
  · apply peak_le
    intro v hv
    obtain ⟨x, hxmem, hv'⟩ := List.mem_flatMap.mp hv
    obtain ⟨y, hymem, hvEq⟩ := List.mem_map.mp hv'
    rw [← hvEq]
    have h1 : x - y ≤ hi a - y := Nat.sub_le_sub_right (le_peak a x hxmem) y
    have h2 : hi a - y ≤ hi a - lo b := Nat.sub_le_sub_left (lo_le_of_mem b y hymem) (hi a)
    exact Nat.le_trans h1 h2
  · cases hma : a.max? with
    | none => simp [List.max?_eq_none_iff.mp hma] at ha
    | some mx =>
      cases hmb : b.min? with
      | none => simp [List.min?_eq_none_iff.mp hmb] at hb
      | some mn =>
        have hmxmem : mx ∈ a := List.max?_mem hma
        have hmymem : mn ∈ b := List.min?_mem hmb
        have hterm : hi a - lo b ∈ crossList a b := by
          apply List.mem_flatMap.mpr
          refine ⟨hi a, ?_, ?_⟩
          · simpa [hi, hma] using hmxmem
          · apply List.mem_map.mpr
            refine ⟨lo b, ?_, rfl⟩
            simpa [lo, hmb] using hmymem
        exact le_peak (crossList a b) (hi a - lo b) hterm

-- The suffix-minimum drop and the all-pairs drop agree.
private theorem dd_eq_modd (l : List Nat) : dd l = modd l := by
  apply Nat.le_antisymm
  · apply peak_le
    intro z hz
    obtain ⟨⟨x, i⟩, hxi, hzEq⟩ := List.mem_map.mp hz
    change x - ((l.drop i).min?.getD x) = z at hzEq
    rw [← hzEq]
    have hget : l[i]? = some x := List.mem_zipIdx_iff_getElem?.mp hxi
    obtain ⟨hilen, hval⟩ := List.getElem?_eq_some_iff.mp hget
    have hdropne : l.drop i ≠ [] := by
      intro hnil
      have hle : l.length ≤ i := List.drop_eq_nil_iff.mp hnil
      omega
    have hterm_eq : x - ((l.drop i).min?.getD x) = x - lo (l.drop i) := by
      rw [min?_getD_ne_nil hdropne x]
    rw [hterm_eq]
    rw [← peak_map_sub x (l.drop i) hdropne]
    apply peak_le
    intro t ht
    obtain ⟨y, hy, htEq⟩ := List.mem_map.mp ht
    rw [← htEq]
    apply le_peak
    apply List.mem_flatMap.mpr
    refine ⟨(x, i), hxi, ?_⟩
    exact List.mem_map.mpr ⟨y, hy, rfl⟩
  · apply peak_le
    intro z hz
    obtain ⟨⟨x, i⟩, hxi, hz'⟩ := List.mem_flatMap.mp hz
    change z ∈ (l.drop i).map (fun y => x - y) at hz'
    obtain ⟨y, hy, hzEq⟩ := List.mem_map.mp hz'
    rw [← hzEq]
    have hget : l[i]? = some x := List.mem_zipIdx_iff_getElem?.mp hxi
    obtain ⟨hilen, hval⟩ := List.getElem?_eq_some_iff.mp hget
    have hdropne : l.drop i ≠ [] := by
      intro hnil
      have hle : l.length ≤ i := List.drop_eq_nil_iff.mp hnil
      omega
    have hlole : lo (l.drop i) ≤ y := lo_le_of_mem (l.drop i) y hy
    have hterm_le : x - y ≤ x - lo (l.drop i) := Nat.sub_le_sub_left hlole x
    have hmemdd : x - lo (l.drop i) ∈ ddList l := by
      apply List.mem_map.mpr
      refine ⟨(x, i), hxi, ?_⟩
      change x - ((l.drop i).min?.getD x) = x - lo (l.drop i)
      rw [min?_getD_ne_nil hdropne x]
    have hddge := le_peak (ddList l) (x - lo (l.drop i)) hmemdd
    exact Nat.le_trans hterm_le hddge

-- All ordered pairs across a concatenation contribute exactly `hi a - lo b`.
private theorem modd_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    modd (a ++ b) = max (max (modd a) (modd b)) (hi a - lo b) := by
  apply Nat.le_antisymm
  · apply peak_le
    intro z hz
    obtain ⟨⟨x, i⟩, hxi, hz'⟩ := List.mem_flatMap.mp hz
    obtain ⟨y, hy, hzEq⟩ := List.mem_map.mp hz'
    rw [← hzEq]
    have hget : (a ++ b)[i]? = some x := List.mem_zipIdx_iff_getElem?.mp hxi
    by_cases hi_small : i < a.length
    · have hagetsome : a[i]? = some x := by
        simpa [List.getElem?_append_left hi_small] using hget
      obtain ⟨hia, hvala⟩ := List.getElem?_eq_some_iff.mp hagetsome
      have hxmem : x ∈ a := by
        rw [← hvala]
        exact List.getElem_mem hia
      have hdrop : (a ++ b).drop i = a.drop i ++ b :=
        List.drop_append_of_le_length (Nat.le_of_lt hi_small)
      rw [hdrop] at hy
      simp only [List.mem_append] at hy
      rcases hy with hy | hy
      · have hmem : x - y ∈ moddList a := by
          apply List.mem_flatMap.mpr
          refine ⟨(x, i), ?_, ?_⟩
          · exact List.mem_zipIdx_iff_getElem?.mpr hagetsome
          · exact List.mem_map.mpr ⟨y, hy, rfl⟩
        have hle : x - y ≤ modd a := le_peak (moddList a) (x - y) hmem
        exact Nat.le_trans hle (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
      · have hmem : x - y ∈ crossList a b := by
          apply List.mem_flatMap.mpr
          refine ⟨x, hxmem, ?_⟩
          exact List.mem_map.mpr ⟨y, hy, rfl⟩
        have hle : x - y ≤ cross a b := le_peak (crossList a b) (x - y) hmem
        rw [cross_eq a b ha hb] at hle
        exact Nat.le_trans hle (Nat.le_max_right _ _)
    · have hlen_ge : a.length ≤ i := by omega
      have hbgetsome : b[i - a.length]? = some x := by
        simpa [List.getElem?_append_right hlen_ge] using hget
      obtain ⟨hjb, hvalb⟩ := List.getElem?_eq_some_iff.mp hbgetsome
      have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
        rw [List.drop_append]
        have hnot : a.drop i = [] := by
          rw [List.drop_eq_nil_iff]
          omega
        simp [hnot]
      rw [hdrop] at hy
      have hmem : x - y ∈ moddList b := by
        apply List.mem_flatMap.mpr
        refine ⟨(x, i - a.length), ?_, ?_⟩
        · exact List.mem_zipIdx_iff_getElem?.mpr hbgetsome
        · exact List.mem_map.mpr ⟨y, hy, rfl⟩
      have hle : x - y ≤ modd b := le_peak (moddList b) (x - y) hmem
      exact Nat.le_trans hle (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · apply Nat.max_le.mpr
    constructor
    · apply Nat.max_le.mpr
      constructor
      · apply peak_le
        intro z hz
        obtain ⟨⟨x, i⟩, hxi, hz'⟩ := List.mem_flatMap.mp hz
        obtain ⟨y, hy, hzEq⟩ := List.mem_map.mp hz'
        rw [← hzEq]
        have hagetsome : a[i]? = some x := List.mem_zipIdx_iff_getElem?.mp hxi
        obtain ⟨hia, hvala⟩ := List.getElem?_eq_some_iff.mp hagetsome
        have hmem : x - y ∈ moddList (a ++ b) := by
          apply List.mem_flatMap.mpr
          refine ⟨(x, i), ?_, ?_⟩
          · exact List.mem_zipIdx_iff_getElem?.mpr (by simpa [List.getElem?_append_left hia] using hagetsome)
          · apply List.mem_map.mpr
            refine ⟨y, ?_, rfl⟩
            have hdrop : (a ++ b).drop i = a.drop i ++ b :=
              List.drop_append_of_le_length (Nat.le_of_lt hia)
            rw [hdrop]
            exact List.mem_append.mpr (Or.inl hy)
        exact le_peak (moddList (a ++ b)) (x - y) hmem
      · apply peak_le
        intro z hz
        obtain ⟨⟨x, j⟩, hxj, hz'⟩ := List.mem_flatMap.mp hz
        obtain ⟨y, hy, hzEq⟩ := List.mem_map.mp hz'
        rw [← hzEq]
        have hbgetsome : b[j]? = some x := List.mem_zipIdx_iff_getElem?.mp hxj
        obtain ⟨hjb, hvalb⟩ := List.getElem?_eq_some_iff.mp hbgetsome
        let i := a.length + j
        have hmem : x - y ∈ moddList (a ++ b) := by
          apply List.mem_flatMap.mpr
          refine ⟨(x, i), ?_, ?_⟩
          · apply List.mem_zipIdx_iff_getElem?.mpr
            simpa [i, List.getElem?_append_right (Nat.le_add_right a.length j)] using hbgetsome
          · apply List.mem_map.mpr
            refine ⟨y, ?_, rfl⟩
            have hdrop : (a ++ b).drop i = b.drop j := by
              simp [i, List.drop_append]
            rw [hdrop]
            exact hy
        exact le_peak (moddList (a ++ b)) (x - y) hmem
    · rw [← cross_eq a b ha hb]
      apply peak_le
      intro z hz
      obtain ⟨x, hxmem, hz'⟩ := List.mem_flatMap.mp hz
      obtain ⟨y, hymem, hzEq⟩ := List.mem_map.mp hz'
      rw [← hzEq]
      obtain ⟨i, hi_idx, hvali⟩ := List.getElem_of_mem hxmem
      have hagetsome : a[i]? = some x := by
        rw [← hvali]
        exact List.getElem?_eq_some_iff.mpr ⟨hi_idx, rfl⟩
      have hmem : x - y ∈ moddList (a ++ b) := by
        apply List.mem_flatMap.mpr
        refine ⟨(x, i), ?_, ?_⟩
        · exact List.mem_zipIdx_iff_getElem?.mpr (by simpa [List.getElem?_append_left hi_idx] using hagetsome)
        · apply List.mem_map.mpr
          refine ⟨y, ?_, rfl⟩
          have hdrop : (a ++ b).drop i = a.drop i ++ b :=
            List.drop_append_of_le_length (Nat.le_of_lt hi_idx)
          rw [hdrop]
          exact List.mem_append.mpr (Or.inr hymem)
      exact le_peak (moddList (a ++ b)) (x - y) hmem

private theorem dd_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    dd (a ++ b) = max (max (dd a) (dd b)) (hi a - lo b) := by
  rw [dd_eq_modd, dd_eq_modd, dd_eq_modd]
  exact modd_append a b ha hb

private theorem reference_eq (l : List Nat) (hl : l ≠ []) :
    agg.reference l = some (lo l, hi l, dd l) := by
  cases l with
  | nil => simp at hl
  | cons x xs => rfl

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
    cases b with
    | nil => simp [agg, optionalJoin]
    | cons y ys =>
      have hA : (x :: xs) ≠ [] := by simp
      have hB : (y :: ys) ≠ [] := by simp
      have hAB : (x :: xs ++ (y :: ys)) ≠ [] := by simp
      rw [reference_eq (x :: xs) hA, reference_eq (y :: ys) hB,
          reference_eq (x :: xs ++ (y :: ys)) hAB]
      have hlo := lo_append hA hB
      have hhi := hi_append hA hB
      have hdd := dd_append (x :: xs) (y :: ys) hA hB
      simp [agg, optionalJoin]
      exact ⟨hlo.symm, hhi.symm, by
        rw [← Nat.max_assoc]
        exact hdd.symm⟩

/-- Binary powering: any summary that stitches concatenations repeats correctly. -/
private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
    unfold repeatState
    by_cases hk : k = 0
    · simp [hk]
    · simp only [hk, dite_false]
      have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
      have ih2 := ih (k / 2) hdiv
      let a := repeatState f (f.reference xs) (k / 2)
      have ha' : a = f.reference (List.replicate (k / 2) xs).flatten := by simpa [a] using ih2
      have hjoin : f.join a a = f.reference ((List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten) := by
        rw [ha']; exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond]
        change f.join a a = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change f.join (f.join a a) (f.reference xs) = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, h]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega, ← List.replicate_append_replicate,
            List.flatten_append, List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

theorem sound_RepeatPower : Drawdown.RepeatPower.Goal := repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_Drawdown_RepeatPower
