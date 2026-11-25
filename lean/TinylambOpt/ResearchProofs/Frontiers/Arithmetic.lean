import TinylambOpt.ResearchGoals.Frontiers.Arithmetic

namespace TinylambOpt.ResearchProofs.Frontiers.Arithmetic

open TinylambOpt.ResearchGoals.Frontiers.Arithmetic

set_option maxHeartbeats 0

private theorem foldl_emod (m : Int) (xs : List Int) (acc : Int) :
    xs.foldl (fun a x => (a+x)%m) (acc % m) = (acc + xs.sum) % m := by
  induction xs generalizing acc with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons]
      rw [show (acc % m + x) % m = (acc + x) % m by rw [Int.emod_add_emod]]
      rw [ih]
      rw [List.sum_cons, Int.add_assoc]

private theorem lane_eq_emod (m : Int) (xs : List Int) : lane m xs = xs.sum % m := by
  dsimp [lane]
  simpa using foldl_emod m xs 0

private theorem sum_sq_diff (x : Int) (b : List Int) :
  (b.map fun y => (x-y)^2).sum = (b.length:Int)*x^2 + (b.map (·^2)).sum - 2*x*b.sum := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      have hcast : ((ys.length + 1 : Nat) : Int) = (ys.length : Int) + 1 := by omega
      rw [hcast]
      grind

private theorem sum_fst_shift (rows : List (Int × Int)) (ax : Int) :
    (rows.map (fun r => r.1 - ax)).sum = (rows.map Prod.fst).sum - (rows.length : Int) * ax := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨x,y⟩
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      have hcast : ((rs.length + 1 : Nat) : Int) = (rs.length : Int) + 1 := by omega
      rw [hcast]
      grind

private theorem sum_snd_shift (rows : List (Int × Int)) (ay : Int) :
    (rows.map (fun r => r.2 - ay)).sum = (rows.map Prod.snd).sum - (rows.length : Int) * ay := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨x,y⟩
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      have hcast : ((rs.length + 1 : Nat) : Int) = (rs.length : Int) + 1 := by omega
      rw [hcast]
      grind

private theorem sum_prod_shift (rows : List (Int × Int)) (ax ay : Int) :
    (rows.map (fun r => (r.1 - ax) * (r.2 - ay))).sum =
      (rows.map (fun r => r.1 * r.2)).sum - ax * (rows.map Prod.snd).sum -
        ay * (rows.map Prod.fst).sum + (rows.length : Int) * ax * ay := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨x,y⟩
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      have hcast : ((rs.length + 1 : Nat) : Int) = (rs.length : Int) + 1 := by omega
      rw [hcast]
      grind

private theorem dyadic_rescale (base e : Nat) (x : Int × Nat) (hbe : base ≤ e) (hex : e ≤ x.2) :
    dyadic base x = (2 : Int)^(e-base) * dyadic e x := by
  rcases x with ⟨c, t⟩
  dsimp [dyadic]
  have hpow : t - base = (e - base) + (t - e) := by omega
  rw [hpow, Int.pow_add]
  grind

private theorem group_rescale (base e : Nat) (xs : List (Int × Nat)) (hbe : base ≤ e)
    (hx : ∀ x ∈ xs, e ≤ x.2) :
    (xs.map (dyadic base)).sum = (2 : Int)^(e-base) * (xs.map (dyadic e)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hxhead : e ≤ x.2 := hx x (by simp)
      have hxtail : ∀ y ∈ xs, e ≤ y.2 := by intro y hy; exact hx y (by simp [hy])
      simp only [List.map_cons, List.sum_cons]
      rw [dyadic_rescale base e x hbe hxhead, ih hxtail]
      rw [Int.mul_add]

private theorem rounded_bounds (s scale : Int) :
    s / scale ≤ rounded s scale ∧ rounded s scale ≤ s / scale + 1 := by
  unfold rounded
  dsimp only
  by_cases h : (decide (2 * (s % scale) < scale) || (2 * (s % scale) == scale && (s / scale) % 2 == 0)) = true
  · simp [h]; omega
  · simp [h]; omega

private theorem rounded_eq_div_of_lt (s scale : Int) (h : 2 * (s % scale) < scale) :
    rounded s scale = s / scale := by
  simp [rounded, h]

private theorem rounded_eq_div_add_one_of_gt (s scale : Int) (h : scale < 2 * (s % scale)) :
    rounded s scale = s / scale + 1 := by
  have hnlt : ¬ 2 * (s % scale) < scale := by omega
  have hneq : (2 * (s % scale) == scale) = false := by
    by_cases hb : (2 * (s % scale) == scale) = true
    · have : 2 * (s % scale) = scale := beq_iff_eq.mp hb
      omega
    · exact Bool.eq_false_of_ne_true hb
  simp [rounded, hnlt, hneq]

private theorem rounded_eq_div_of_tie_even (s scale : Int) (h : 2 * (s % scale) = scale)
    (hpar : (s / scale) % 2 == 0) : rounded s scale = s / scale := by
  have hnlt : ¬ 2 * (s % scale) < scale := by omega
  simp [rounded, h, hpar]

private theorem rounded_eq_div_add_one_of_tie_odd (s scale : Int) (h : 2 * (s % scale) = scale)
    (hpar : ((s / scale) % 2 == 0) = false) : rounded s scale = s / scale + 1 := by
  have hnlt : ¬ 2 * (s % scale) < scale := by omega
  have hbeq : (2 * (s % scale) == scale) = true := by simp [h]
  simp [rounded, hnlt, hbeq, hpar]

private theorem rounded_mono {scale : Int} (hscale : 0 < scale) {s t : Int} (hst : s ≤ t) :
    rounded s scale ≤ rounded t scale := by
  let q1 : Int := s / scale
  let r1 : Int := s % scale
  let q2 : Int := t / scale
  let r2 : Int := t % scale
  have hd1 : q1 * scale + r1 = s := by dsimp [q1, r1]; exact Int.ediv_mul_add_emod s scale
  have hd2 : q2 * scale + r2 = t := by dsimp [q2, r2]; exact Int.ediv_mul_add_emod t scale
  have hqle : q1 ≤ q2 := by dsimp [q1, q2]; exact Int.ediv_le_ediv hscale hst
  have hsne : scale ≠ 0 := by omega
  have hr1n : 0 ≤ r1 := by dsimp [r1]; exact Int.emod_nonneg s hsne
  have hr2n : 0 ≤ r2 := by dsimp [r2]; exact Int.emod_nonneg t hsne
  have hr1l : r1 < scale := by dsimp [r1]; exact Int.emod_lt_of_pos s hscale
  have hr2l : r2 < scale := by dsimp [r2]; exact Int.emod_lt_of_pos t hscale
  have hb1 := rounded_bounds s scale
  have hb2 := rounded_bounds t scale
  change q1 ≤ rounded s scale ∧ rounded s scale ≤ q1 + 1 at hb1
  change q2 ≤ rounded t scale ∧ rounded t scale ≤ q2 + 1 at hb2
  by_cases hq : q1 < q2
  · omega
  · have hqeq : q1 = q2 := by omega
    have hdeq : q1 * scale = q2 * scale := by rw [hqeq]
    rw [hdeq] at hd1
    have hrle : r1 ≤ r2 := by omega
    by_cases hlt2 : 2 * r2 < scale
    · have hlt1 : 2 * r1 < scale := by omega
      have hs := rounded_eq_div_of_lt s scale (by simpa [r1] using hlt1)
      have ht := rounded_eq_div_of_lt t scale (by simpa [r2] using hlt2)
      change rounded s scale = q1 at hs
      change rounded t scale = q2 at ht
      omega
    · by_cases heq2 : 2 * r2 = scale
      · by_cases hpar2 : (q2 % 2 == 0) = true
        · have ht := rounded_eq_div_of_tie_even t scale (by simpa [r2] using heq2) (by simpa [q2, hqeq] using hpar2)
          change rounded t scale = q2 at ht
          have hsval : rounded s scale = q1 := by
            by_cases hlt1 : 2 * r1 < scale
            · have hs := rounded_eq_div_of_lt s scale (by simpa [r1] using hlt1)
              change rounded s scale = q1 at hs
              exact hs
            · have heq1 : 2 * r1 = scale := by omega
              have hpar1 : (q1 % 2 == 0) = true := by rw [hqeq]; exact hpar2
              have hs := rounded_eq_div_of_tie_even s scale (by simpa [r1] using heq1) (by simpa [q1] using hpar1)
              change rounded s scale = q1 at hs
              exact hs
          omega
        · have hpar2f : ((t / scale) % 2 == 0) = false := by
            have hf : (q2 % 2 == 0) = false := Bool.eq_false_of_ne_true hpar2
            simpa [q2] using hf
          have ht := rounded_eq_div_add_one_of_tie_odd t scale (by simpa [r2] using heq2) hpar2f
          change rounded t scale = q2 + 1 at ht
          omega
      · have hgt2 : scale < 2 * r2 := by omega
        have ht := rounded_eq_div_add_one_of_gt t scale (by simpa [r2] using hgt2)
        change rounded t scale = q2 + 1 at ht
        omega

private theorem natAbs_sum_le_sum_natAbs (xs : List Int) :
    xs.sum.natAbs ≤ (xs.map Int.natAbs).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.le_trans (Int.natAbs_add_le x xs.sum) (Nat.add_le_add_left ih x.natAbs)

private theorem quant_bound_one (x scale : Int) (hscale : 0 < scale) :
    x ≤ scale * (x / scale) + (if (x % scale == 0) then 0 else scale - 1) := by
  have hsne : scale ≠ 0 := by omega
  have hrnonneg : 0 ≤ x % scale := Int.emod_nonneg x hsne
  have hrlt : x % scale < scale := Int.emod_lt_of_pos x hscale
  have hdec : x = scale * (x / scale) + x % scale := by
    have h := Int.ediv_mul_add_emod x scale
    grind
  by_cases h : (x % scale == 0) = true
  · have hr0 : x % scale = 0 := beq_iff_eq.mp h
    simp [h]
    omega
  · have hrne : x % scale ≠ 0 := by intro he; exact h (by simp [he])
    simp [h]
    omega

private theorem countP_ne_eq_sum (xs : List Int) (scale : Int) :
    (xs.countP (fun x => x % scale != 0)) =
      (xs.map (fun x => if (x % scale == 0) then 0 else 1)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.countP_cons, List.map_cons, List.sum_cons]
      by_cases h : (x % scale == 0) = true
      · have hne : (x % scale != 0) = false := by
          by_cases h2 : x % scale != 0 <;> simp_all
        simp [h, hne, ih]
      · have hne : (x % scale != 0) = true := by
          by_cases h2 : x % scale != 0 <;> simp_all
        simp [h, hne, ih]
        omega

private theorem quant_sum_bound (xs : List Int) (scale : Int) (hscale : 0 < scale) :
    xs.sum ≤ scale * (xs.map (· / scale)).sum +
      (xs.countP (fun x => x % scale != 0) : Int) * (scale - 1) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hb := quant_bound_one x scale hscale
      rw [List.countP_cons]
      by_cases h : (x % scale != 0) = true
      · rw [ite_eq_left h]
        have hcast : ((xs.countP (fun y => y % scale != 0) + 1 : Nat) : Int) =
            (xs.countP (fun y => y % scale != 0) : Int) + 1 := by omega
        rw [hcast, Int.add_mul]
        simp only [List.map_cons, List.sum_cons]
        rw [Int.mul_add]
        simp [show (x % scale == 0) = false by
          by_cases h2 : x % scale == 0 <;> simp_all] at hb
        omega
      · rw [ite_eq_right h]
        have hcast : ((xs.countP (fun y => y % scale != 0) + 0 : Nat) : Int) =
            (xs.countP (fun y => y % scale != 0) : Int) + 0 := by omega
        rw [hcast, Int.add_mul]
        simp only [List.map_cons, List.sum_cons]
        rw [Int.mul_add]
        simp [show (x % scale == 0) = true by
          by_cases h2 : x % scale == 0 <;> simp_all] at hb
        omega

private theorem sum_emod_mod (xs : List Int) (m : Int) :
    (xs.map (fun x => x % m)).sum % m = xs.sum % m := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      calc
        (x % m + (List.map (fun y => y % m) xs).sum) % m =
            ((x % m) % m + (List.map (fun y => y % m) xs).sum % m) % m := by
              rw [Int.add_emod]
        _ = (x % m + xs.sum % m) % m := by rw [Int.emod_emod, ih]
        _ = (x + xs.sum) % m := by rw [← Int.add_emod]
        _ = (x :: xs).sum % m := by simp

private theorem sum_natAbs_prod_bound (rows : List (Int × Int)) (a b : Nat)
    (h : ∀ r ∈ rows, r.1.natAbs ≤ a ∧ r.2.natAbs ≤ b) :
    (rows.map (fun r : Int × Int => r.1*r.2)).sum.natAbs ≤ rows.length*a*b := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨x,y⟩
      have hr := h (x,y) (by simp)
      have htail : ∀ q ∈ rs, q.1.natAbs ≤ a ∧ q.2.natAbs ≤ b := by
        intro q hq; exact h q (by simp [hq])
      have hi := ih htail
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hhead : (x*y).natAbs ≤ a*b := by
        rw [Int.natAbs_mul]
        exact Nat.mul_le_mul hr.1 hr.2
      have hadd := Nat.add_le_add hhead hi
      grind

private def outProp (lo hi : Int) (xs : List Int) (s : Int) (j : Nat) : Prop :=
  s + (xs.take (j+1)).sum < lo ∨ hi < s + (xs.take (j+1)).sum

private def outAt (lo hi : Int) (xs : List Int) (s : Int) (j : Nat) : Bool :=
  decide (s + (xs.take (j+1)).sum < lo) || decide (hi < s + (xs.take (j+1)).sum)

private theorem outAt_eq_true (lo hi : Int) (xs : List Int) (s : Int) (j : Nat) :
    outAt lo hi xs s j = true ↔ outProp lo hi xs s j := by
  unfold outAt outProp
  simp

private theorem outAt_eq_false (lo hi : Int) (xs : List Int) (s : Int) (j : Nat) :
    outAt lo hi xs s j = false ↔ ¬ outProp lo hi xs s j := by
  constructor
  · intro h hp
    have ht : outAt lo hi xs s j = true := (outAt_eq_true lo hi xs s j).mpr hp
    rw [h] at ht
    cases ht
  · intro h
    cases hb : outAt lo hi xs s j
    · rfl
    · exact False.elim (h ((outAt_eq_true lo hi xs s j).mp hb))

private def failAt (lo hi : Int) (xs : List Int) (s : Int) : Nat :=
  (xs.zipIdx).findIdx (fun b : Int × Nat => outAt lo hi xs s b.2)

private theorem take_of_length_le (xs : List Int) (n : Nat) (h : xs.length ≤ n) :
    xs.take n = xs := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      cases n with
      | zero => simp at h
      | succ n =>
          rw [List.take_succ_cons]
          congr 1
          apply ih n
          simp only [List.length_cons] at h
          omega

private theorem failAt_eq_length_iff (lo hi : Int) (xs : List Int) (s : Int) :
    failAt lo hi xs s = xs.length ↔ ∀ j, j < xs.length → ¬ outProp lo hi xs s j := by
  unfold failAt
  rw [show xs.length = (xs.zipIdx).length by simp [List.length_zipIdx]]
  rw [List.findIdx_eq_length]
  constructor
  · intro h j hj
    have hidx : j < (xs.zipIdx).length := by simpa [List.length_zipIdx] using hj
    have hm : ((xs.zipIdx)[j]) ∈ xs.zipIdx := List.getElem_mem hidx
    have hp := h ((xs.zipIdx)[j]) hm
    rw [List.getElem_zipIdx hidx] at hp
    have hp' : outAt lo hi xs s j = false := by simpa using hp
    exact (outAt_eq_false lo hi xs s j).mp hp'
  · intro h x hx
    rcases List.mem_zipIdx hx with ⟨hk, hlen, hval⟩
    have h' := h x.2 (by simpa using hlen)
    exact (outAt_eq_false lo hi xs s x.2).mpr h'

private theorem failAt_of_lt (lo hi : Int) (xs : List Int) (s : Int)
    (hlt : failAt lo hi xs s < xs.length) :
    outProp lo hi xs s (failAt lo hi xs s) ∧
      ∀ j, j < failAt lo hi xs s → ¬ outProp lo hi xs s j := by
  unfold failAt at hlt ⊢
  let p : Int × Nat → Bool := fun b => outAt lo hi xs s b.2
  change (xs.zipIdx).findIdx p < xs.length at hlt
  have hfind : (xs.zipIdx).findIdx p = ((xs.zipIdx).findIdx? p).getD (xs.zipIdx).length :=
    List.findIdx_eq_getD_findIdx?
  cases hq : (xs.zipIdx).findIdx? p with
  | none =>
      have hall : ∀ x ∈ xs.zipIdx, p x = false := List.findIdx?_eq_none_iff.mp hq
      have heq : (xs.zipIdx).findIdx p = (xs.zipIdx).length := List.findIdx_eq_length.mpr hall
      rw [List.length_zipIdx] at heq
      rw [heq] at hlt
      omega
  | some i =>
      have hi' : (xs.zipIdx).findIdx p = i := by
        rw [hfind, hq]
        simp [List.length_zipIdx]
      rw [hi'] at hlt ⊢
      have hspec := List.findIdx?_eq_some_iff_getElem.mp hq
      have hidx : i < (xs.zipIdx).length := by simpa [List.length_zipIdx] using hlt
      have hgi := List.getElem_zipIdx (l := xs) (i := i) (j := 0) hidx
      constructor
      · have hptrue := hspec.2.1
        rw [hgi] at hptrue
        exact (outAt_eq_true lo hi xs s i).mp (by simpa [p] using hptrue)
      · intro j hj
        have hpj := hspec.2.2 j hj
        have hjidx : j < (xs.zipIdx).length := by
          rw [List.length_zipIdx]
          omega
        have hgj := List.getElem_zipIdx (l := xs) (i := j) (j := 0) hjidx
        rw [hgj] at hpj
        exact (outAt_eq_false lo hi xs s j).mp (by simpa [p] using hpj)

private theorem outProp_append_left (lo hi : Int) (xs ys : List Int) (s : Int) (k : Nat)
    (hk : k < xs.length) : outProp lo hi (xs++ys) s k ↔ outProp lo hi xs s k := by
  unfold outProp
  have hle : k+1 ≤ xs.length := by omega
  rw [List.take_append_of_le_length (l₁ := xs) (l₂ := ys) hle]

private theorem outProp_append_right (lo hi : Int) (xs ys : List Int) (s : Int) (k : Nat) :
    outProp lo hi (xs++ys) s (xs.length + k) ↔ outProp lo hi ys (s+xs.sum) k := by
  unfold outProp
  have htake : (xs ++ ys).take (xs.length + k + 1) = xs ++ ys.take (k+1) := by
    rw [List.take_append]
    have hsub : xs.length + k + 1 - xs.length = k+1 := by omega
    rw [hsub]
    rw [take_of_length_le xs (xs.length + k + 1) (by omega)]
  rw [htake]
  simp only [List.sum_append]
  constructor <;> intro h <;> omega

private theorem failAt_append_right (lo hi : Int) (xs ys : List Int) (s : Int)
    (heq : failAt lo hi xs s = xs.length) :
    failAt lo hi (xs++ys) s = xs.length + failAt lo hi ys (s+xs.sum) := by
  let f := failAt lo hi ys (s+xs.sum)
  have hf_le : f ≤ ys.length := by
    dsimp [f, failAt]
    rw [← List.length_zipIdx]
    exact List.findIdx_le_length
  have hall := (failAt_eq_length_iff lo hi xs s).mp heq
  by_cases hf_lt : f < ys.length
  · have hspec := failAt_of_lt lo hi ys (s+xs.sum) (by simpa [f] using hf_lt)
    have houtcat : outProp lo hi (xs++ys) s (xs.length + f) :=
      (outProp_append_right lo hi xs ys s f).mpr hspec.1
    have hcatl : xs.length + f < (xs++ys).length := by simp; omega
    have hcatlt : failAt lo hi (xs++ys) s < (xs++ys).length := by
      by_cases hh : failAt lo hi (xs++ys) s < (xs++ys).length
      · exact hh
      · have hleall : failAt lo hi (xs++ys) s ≤ (xs++ys).length := by
          unfold failAt
          rw [← List.length_zipIdx]
          exact List.findIdx_le_length
        have hce : failAt lo hi (xs++ys) s = (xs++ys).length := by omega
        have hnone := (failAt_eq_length_iff lo hi (xs++ys) s).mp hce
        exact False.elim (hnone (xs.length+f) hcatl houtcat)
    have hspecCat := failAt_of_lt lo hi (xs++ys) s hcatlt
    have hle : failAt lo hi (xs++ys) s ≤ xs.length + f := by
      by_cases hh : failAt lo hi (xs++ys) s ≤ xs.length + f
      · exact hh
      · exact False.elim (hspecCat.2 (xs.length+f) (by omega) houtcat)
    have hge : xs.length + f ≤ failAt lo hi (xs++ys) s := by
      by_cases hh : xs.length + f ≤ failAt lo hi (xs++ys) s
      · exact hh
      · have hsmall : failAt lo hi (xs++ys) s < xs.length + f := by omega
        have houtsmall := hspecCat.1
        by_cases hkxs : failAt lo hi (xs++ys) s < xs.length
        · have houtxs : outProp lo hi xs s (failAt lo hi (xs++ys) s) :=
            (outProp_append_left lo hi xs ys s _ hkxs).mp houtsmall
          exact False.elim (hall _ hkxs houtxs)
        · let kk := failAt lo hi (xs++ys) s - xs.length
          have hkksmall : kk < f := by omega
          have hidxcat : failAt lo hi (xs++ys) s = xs.length + kk := by omega
          rw [hidxcat] at houtsmall
          have houtys : outProp lo hi ys (s+xs.sum) kk :=
            (outProp_append_right lo hi xs ys s kk).mp houtsmall
          exact False.elim (hspec.2 kk hkksmall houtys)
    change failAt lo hi (xs++ys) s = xs.length + f
    omega
  · have hf_eq : f = ys.length := by omega
    have hfEq' : failAt lo hi ys (s+xs.sum) = ys.length := by simpa [f] using hf_eq
    have hlen : (xs++ys).length = xs.length + ys.length := by simp
    have hce : failAt lo hi (xs++ys) s = (xs++ys).length := by
      apply (failAt_eq_length_iff lo hi (xs++ys) s).mpr
      intro j hj
      by_cases hjxs : j < xs.length
      · intro hc
        exact hall j hjxs ((outProp_append_left lo hi xs ys s j hjxs).mp hc)
      · let kk := j - xs.length
        have hkks : kk < f := by omega
        have hjlen : j < xs.length + ys.length := by simpa [hlen] using hj
        have hkks' : kk < ys.length := by omega
        have hfEq' : failAt lo hi ys (s+xs.sum) = ys.length := by simpa [f] using hf_eq
        have hyall := (failAt_eq_length_iff lo hi ys (s+xs.sum)).mp hfEq'
        have hj_eq : j = xs.length + kk := by omega
        intro hc
        rw [hj_eq] at hc
        exact hyall kk hkks' ((outProp_append_right lo hi xs ys s kk).mp hc)
    change failAt lo hi (xs++ys) s = xs.length + f
    rw [hce, hlen]
    simp [f, hfEq']

private theorem failAt_append_left (lo hi : Int) (xs ys : List Int) (s : Int)
    (hlt : failAt lo hi xs s < xs.length) :
    failAt lo hi (xs++ys) s = failAt lo hi xs s := by
  let k := failAt lo hi xs s
  have hspec := failAt_of_lt lo hi xs s hlt
  have hcatl : k < (xs++ys).length := by simp; omega
  have houtcat : outProp lo hi (xs++ys) s k := (outProp_append_left lo hi xs ys s k hlt).mpr hspec.1
  have hcatlt : failAt lo hi (xs++ys) s < (xs++ys).length := by
    by_cases hh : failAt lo hi (xs++ys) s < (xs++ys).length
    · exact hh
    · have hleall : failAt lo hi (xs++ys) s ≤ (xs++ys).length := by
        unfold failAt
        rw [← List.length_zipIdx]
        exact List.findIdx_le_length
      have heq : failAt lo hi (xs++ys) s = (xs++ys).length := by omega
      have hall := (failAt_eq_length_iff lo hi (xs++ys) s).mp heq
      exact False.elim (hall k hcatl houtcat)
  have hspecCat := failAt_of_lt lo hi (xs++ys) s hcatlt
  have hle : failAt lo hi (xs++ys) s ≤ k := by
    by_cases hh : failAt lo hi (xs++ys) s ≤ k
    · exact hh
    · exact False.elim (hspecCat.2 k (by omega) houtcat)
  have hge : k ≤ failAt lo hi (xs++ys) s := by
    by_cases hh : k ≤ failAt lo hi (xs++ys) s
    · exact hh
    · have hsmall : failAt lo hi (xs++ys) s < k := by omega
      have houtsmall : outProp lo hi (xs++ys) s (failAt lo hi (xs++ys) s) := hspecCat.1
      have houtxs : outProp lo hi xs s (failAt lo hi (xs++ys) s) :=
        (outProp_append_left lo hi xs ys s _ (by omega : failAt lo hi (xs++ys) s < xs.length)).mp houtsmall
      exact False.elim (hspec.2 _ hsmall houtxs)
  omega
private theorem mem_prefixes (xs : List Int) (v : Int) :
    v ∈ prefixes xs ↔ ∃ j, j ≤ xs.length ∧ (xs.take j).sum = v := by
  unfold prefixes
  simp [List.mem_range, Nat.lt_succ_iff]

private theorem minmax_bound (lo hi : Int) (ys : List Int) (hne : ys ≠ []) (init : Int) :
    (lo ≤ init + ys.min?.getD 0 ∧ init + ys.max?.getD 0 ≤ hi) ↔
      ∀ v ∈ ys, lo ≤ init+v ∧ init+v ≤ hi := by
  have hmin_some : ∃ m, ys.min? = some m := by
    cases ys with
    | nil => contradiction
    | cons a as => simp [List.min?_cons]
  have hmax_some : ∃ M, ys.max? = some M := by
    cases ys with
    | nil => contradiction
    | cons a as => simp [List.max?_cons]
  rcases hmin_some with ⟨m, hm⟩
  rcases hmax_some with ⟨M, hM⟩
  have hmin := List.min?_eq_some_iff.mp hm
  have hmax := List.max?_eq_some_iff.mp hM
  rw [hm, hM]
  simp
  constructor
  · intro hb v hv
    have hmv : m ≤ v := hmin.2 v hv
    have hvM : v ≤ M := hmax.2 v hv
    omega
  · intro hb
    have h1 := hb m hmin.1
    have h2 := hb M hmax.1
    omega

private theorem safe_iff_prefixes (lo hi : Int) (xs : List Int) (init : Int) :
    safe xs init lo hi = true ↔ ∀ j, j ≤ xs.length →
      lo ≤ init + (xs.take j).sum ∧ init + (xs.take j).sum ≤ hi := by
  unfold safe
  simp only [Bool.and_eq_true]
  simp
  rw [minmax_bound lo hi (prefixes xs) (by unfold prefixes; simp) init]
  constructor
  · intro h j hj
    exact h _ ((mem_prefixes xs _).mpr ⟨j, hj, rfl⟩)
  · intro h v hv
    rcases (mem_prefixes xs v).mp hv with ⟨j, hj, hjv⟩
    rw [← hjv]
    exact h j hj

private theorem safe_iff_no_out (lo hi : Int) (xs : List Int) (init : Int)
    (hinit : lo ≤ init ∧ init ≤ hi) :
    safe xs init lo hi = true ↔ ∀ j, j < xs.length → ¬ outProp lo hi xs init j := by
  rw [safe_iff_prefixes lo hi xs init]
  constructor
  · intro h j hj
    intro hp
    have hb := h (j+1) (by omega)
    unfold outProp at hp
    omega
  · intro h j hj
    by_cases hj0 : j = 0
    · subst j
      simpa using hinit
    · obtain ⟨k, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hj0
      have hb := h k (by omega)
      unfold outProp at hb
      simp only [Nat.succ_eq_add_one] at hb ⊢
      omega

private theorem findIdx_zipIdx_succ {α : Type} (xs : List α) (k : Nat) (Q : α → Nat → Bool) :
    (xs.zipIdx (k+1)).findIdx (fun (x, j) => Q x j) =
      (xs.zipIdx k).findIdx (fun (x, j) => Q x (j+1)) := by
  induction xs generalizing k with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, List.findIdx_cons, List.findIdx_cons]
      by_cases h : Q a (k+1) = true
      · simp [h]
      · simp [h]
        rw [ih (k+1)]

theorem sound_SignedCRT :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.SignedCRT.Goal := by
  intro xs m n u v lo hm hn hbez hlo hhi
  let S : Int := xs.sum
  let D : Int := S - lo
  let a : Int := lane m xs - lo
  let b : Int := lane n xs - lo
  let C : Int := a*n*v + b*m*u
  have hmnpos : 0 < m*n := Int.mul_pos hm hn
  have hDnonneg : 0 ≤ D := by dsimp [D, S]; omega
  have hDlt : D < m*n := by dsimp [D, S]; omega
  have hmodem : lane m xs = S % m := by dsimp [S]; exact lane_eq_emod m xs
  have hmoden : lane n xs = S % n := by dsimp [S]; exact lane_eq_emod n xs
  have hdm : m ∣ (a - D) := by
    refine ⟨-(S / m), ?_⟩
    have hdec := Int.ediv_mul_add_emod S m
    dsimp [a, D]
    rw [hmodem]
    grind
  have hdn : n ∣ (b - D) := by
    refine ⟨-(S / n), ?_⟩
    have hdec := Int.ediv_mul_add_emod S n
    dsimp [b, D]
    rw [hmoden]
    grind
  have hidentity : C - D = (a - D)*n*v + (b - D)*m*u := by
    dsimp [C]
    grind
  have hCm : m ∣ (C - D) := by
    rcases hdm with ⟨k, hk⟩
    refine ⟨k*n*v + (b-D)*u, ?_⟩
    rw [hidentity, hk]
    grind
  have hCn : n ∣ (C - D) := by
    rcases hdn with ⟨l, hl⟩
    refine ⟨(a-D)*v + l*m*u, ?_⟩
    rw [hidentity, hl]
    grind
  have hmn : m*n ∣ (C - D) := by
    rcases hCm with ⟨k, hk⟩
    rcases hCn with ⟨l, hl⟩
    refine ⟨l*u + k*v, ?_⟩
    grind
  have hmodzero : (C - D) % (m*n) = 0 := Int.emod_eq_zero_of_dvd hmn
  have heqmod : C % (m*n) = D % (m*n) :=
    Int.emod_eq_emod_iff_emod_sub_eq_zero.mpr hmodzero
  have hDmod : D % (m*n) = D := Int.emod_eq_of_lt hDnonneg hDlt
  have hCeq : C % (m*n) = D := by rw [heqmod, hDmod]
  dsimp [crt]
  change xs.sum = lo + (a*n*v + b*m*u) % (m*n)
  change xs.sum = lo + C % (m*n)
  rw [hCeq]
  dsimp [D, S]
  omega

theorem sound_ZeroJoin :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.ZeroJoin.Goal := by
  intro terms m n hm hn hgcd hbudget
  let vals : List Int := terms.map (fun p => p.1*p.2)
  have hvals : vals.map Int.natAbs = terms.map (fun p : Int × Int => p.1.natAbs*p.2.natAbs) := by
    dsimp [vals]
    rw [List.map_map]
    apply List.map_congr_left
    intro t ht
    rcases t with ⟨a,b⟩
    simp [Int.natAbs_mul]
  have hbudget' : (terms.map (fun p : Int × Int => p.1.natAbs*p.2.natAbs)).sum < m*n := by
    simpa using hbudget
  have hsumabs : vals.sum.natAbs ≤ (terms.map (fun p : Int × Int => p.1.natAbs*p.2.natAbs)).sum := by
    rw [← hvals]
    exact natAbs_sum_le_sum_natAbs vals
  constructor
  · intro hzero
    have hzero' : vals.sum = 0 := by simpa [vals] using hzero
    rw [lane_eq_emod, lane_eq_emod, hzero']
    simp
  · intro hlanes
    rcases hlanes with ⟨hlanem, hlanen⟩
    have hmdvdI : (m : Int) ∣ vals.sum := by
      apply Int.dvd_of_emod_eq_zero
      rw [← lane_eq_emod (m : Int) vals]
      exact hlanem
    have hndvdI : (n : Int) ∣ vals.sum := by
      apply Int.dvd_of_emod_eq_zero
      rw [← lane_eq_emod (n : Int) vals]
      exact hlanen
    have hmdvd : m ∣ vals.sum.natAbs := by
      simpa using (Int.natAbs_dvd_natAbs.mpr hmdvdI)
    have hndvd : n ∣ vals.sum.natAbs := by
      simpa using (Int.natAbs_dvd_natAbs.mpr hndvdI)
    have hcop : m.Coprime n := Nat.coprime_iff_gcd_eq_one.mpr hgcd
    have hmndvd : m*n ∣ vals.sum.natAbs := hcop.mul_dvd_of_dvd_of_dvd hmdvd hndvd
    by_cases hzero : vals.sum = 0
    · simpa [vals] using hzero
    · have hpos : 0 < vals.sum.natAbs := (Int.natAbs_pos).mpr hzero
      have hle : m*n ≤ vals.sum.natAbs := Nat.le_of_dvd hpos hmndvd
      dsimp [vals] at hle hsumabs hbudget'
      omega

theorem sound_RoundingCertificate :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.RoundingCertificate.Goal := by
  intro read lo hi tail scale hscale hlo hhi heq
  have h1 : rounded (read + lo) scale ≤ rounded (read + tail.sum) scale :=
    rounded_mono hscale (by omega)
  have h2 : rounded (read + tail.sum) scale ≤ rounded (read + hi) scale :=
    rounded_mono hscale (by omega)
  omega

/-- LocalRounding is false: two halves of scale 2 round to 0+0, while their
sum 2 rounds to 1.  Both inputs satisfy the <= half-unit rounding-error
hypothesis: s=1, rounded 1 2=0, error 1, and 2*1 <= 2. -/
theorem refutation_LocalRounding :
    ¬ TinylambOpt.ResearchGoals.Frontiers.Arithmetic.LocalRounding.Goal := by
  intro h
  have hh := h [1, 1] 2 (by decide) (by native_decide)
  have hneq : ¬ ((List.map (fun x => rounded x 2) [1,1]).sum = rounded [1,1].sum 2) := by
    native_decide
  exact hneq hh

theorem sound_BinnedAccumulator :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.BinnedAccumulator.Goal := by
  intro groups base h
  induction groups with
  | nil => simp
  | cons g gs ih =>
      have hg := h g (by simp)
      have htail : ∀ q ∈ gs, base ≤ q.1 ∧ ∀ x ∈ q.2, q.1 ≤ x.2 := by
        intro q hq; exact h q (by simp [hq])
      simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons]
      rw [group_rescale base g.1 g.2 hg.1 hg.2]
      rw [ih htail]

theorem sound_FirstOverflow :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.FirstOverflow.Goal := by
  intro blocks
  induction blocks with
  | nil =>
      intro start lo hi hlo hhi bad i prior block failF
      simp [failF, i, prior, block]
  | cons b bs ih =>
      intro start lo hi hlo hhi bad i prior block failF
      let badTail : List Int × Nat → Bool := fun (c, j) =>
        !safe c ((start + b.sum) + ((bs.take j).map List.sum).sum) lo hi
      have hbad0 : bad (b, 0) = !safe b start lo hi := by
        dsimp [bad]
        simp
      have hbadshift : (fun (cj : List Int × Nat) => bad (cj.1, cj.2+1)) = badTail := by
        funext cj
        rcases cj with ⟨c, j⟩
        dsimp [bad, badTail]
        simp [Int.add_assoc]
      have hi_eq : i = if bad (b,0) then 0 else (bs.zipIdx.findIdx badTail) + 1 := by
        dsimp [i]
        rw [List.findIdx_cons]
        rw [show (bs.zipIdx 1).findIdx bad = (bs.zipIdx 0).findIdx (fun (c,j) => bad (c, j+1)) by
          simpa using findIdx_zipIdx_succ bs 0 (fun (c : List Int) (j : Nat) => bad (c,j))]
        rw [hbadshift]
      have hstart : lo ≤ start ∧ start ≤ hi := ⟨hlo, hhi⟩
      have hsafe_iff := safe_iff_no_out lo hi b start hstart
      by_cases hsafe : safe b start lo hi = true
      · have hbad0f : bad (b,0) = false := by rw [hbad0]; simp [hsafe]
        have hifalse : i = bs.zipIdx.findIdx badTail + 1 := by rw [hi_eq, hbad0f]; simp
        have hfb_len : failAt lo hi b start = b.length := by
          apply (failAt_eq_length_iff lo hi b start).mpr
          intro j hj hp
          exact (hsafe_iff.mp hsafe j hj) hp
        have hpref := (safe_iff_prefixes lo hi b start).mp hsafe b.length (by omega)
        have htake : b.take b.length = b := take_of_length_le b b.length (by omega)
        rw [htake] at hpref
        have hstart' : lo ≤ start + b.sum ∧ start + b.sum ≤ hi := by omega
        have ihres := ih (start + b.sum) lo hi hstart'.1 hstart'.2
        dsimp at ihres
        let priorTail : List Int := (bs.take (bs.zipIdx.findIdx badTail)).flatten
        let blockTail : List Int := bs[bs.zipIdx.findIdx badTail]?.getD []
        have hih : failAt lo hi bs.flatten (start+b.sum) =
            priorTail.length + failAt lo hi blockTail ((start+b.sum)+priorTail.sum) := by
          simpa [failAt, outAt, badTail, priorTail, blockTail, Int.add_assoc] using ihres
        have hcat : failAt lo hi (b::bs).flatten start =
            b.length + failAt lo hi bs.flatten (start+b.sum) := by
          change failAt lo hi (b ++ bs.flatten) start =
            b.length + failAt lo hi bs.flatten (start+b.sum)
          exact failAt_append_right lo hi b bs.flatten start hfb_len
        have hprior : prior = b ++ priorTail := by
          dsimp [prior, priorTail]
          rw [hifalse]
          simp
        have hblock : block = blockTail := by
          dsimp [block, blockTail]
          rw [hifalse]
          simp
        dsimp [failF]
        change failAt lo hi (b::bs).flatten start =
          prior.length + failAt lo hi block (start + prior.sum)
        rw [hcat, hih, hprior, hblock]
        simp only [List.length_append, List.sum_append]
        rw [Int.add_assoc]
        omega
      · have hsafef : safe b start lo hi = false := by
          cases h : safe b start lo hi
          · rfl
          · exact False.elim (hsafe h)
        have hbad0t : bad (b,0) = true := by
          rw [hbad0, hsafef]
          rfl
        have hifalse : i = 0 := by rw [hi_eq, hbad0t]; simp
        have hex : ∃ j, j < b.length ∧ outProp lo hi b start j := by
          by_cases hex' : ∃ j, j < b.length ∧ outProp lo hi b start j
          · exact hex'
          · exfalso
            apply hsafe
            apply hsafe_iff.mpr
            intro j hj hp
            exact hex' ⟨j, hj, hp⟩
        rcases hex with ⟨j, hj, hp⟩
        have hfb_lt : failAt lo hi b start < b.length := by
          by_cases hh : failAt lo hi b start < b.length
          · exact hh
          · have hle : failAt lo hi b start ≤ b.length := by
              unfold failAt
              rw [← List.length_zipIdx]
              exact List.findIdx_le_length
            have heq : failAt lo hi b start = b.length := by omega
            have hall := (failAt_eq_length_iff lo hi b start).mp heq
            exact False.elim (hall j hj hp)
        have hcat : failAt lo hi (b::bs).flatten start = failAt lo hi b start := by
          change failAt lo hi (b ++ bs.flatten) start = failAt lo hi b start
          exact failAt_append_left lo hi b bs.flatten start hfb_lt
        have hprior : prior = [] := by
          dsimp [prior]
          rw [hifalse]
          simp
        have hblock : block = b := by
          dsimp [block]
          rw [hifalse]
          simp
        dsimp [failF]
        change failAt lo hi (b::bs).flatten start =
          prior.length + failAt lo hi block (start + prior.sum)
        rw [hcat, hprior, hblock]
        simp

theorem sound_QuadraticJoin :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.QuadraticJoin.Goal := by
  intro a b
  induction a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.flatMap_cons, List.map_cons, List.sum_cons, List.length_cons]
      rw [List.sum_append]
      rw [sum_sq_diff, ih]
      have hcast : ((xs.length + 1 : Nat) : Int) = (xs.length : Int) + 1 := by omega
      rw [hcast]
      grind

theorem sound_QuantizedAverage :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.QuantizedAverage.Goal := by
  intro xs scale cut hscale hne qs missing hyp
  let A : Int := scale * (xs.map (fun x => x / scale)).sum
  let B : Int := (xs.countP (fun x => x % scale != 0) : Int) * (scale - 1)
  have hbound' : xs.sum ≤ A + B := by
    dsimp [A, B]
    exact quant_sum_bound xs scale hscale
  have hyp' : A + B < cut * (xs.length : Int) := by
    simpa [A, B] using hyp
  omega

theorem sound_CenteredProduct :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.CenteredProduct.Goal := by
  intro rows bits a b hbits hbounds hbudget modulus residues z
  let S : Int := (rows.map (fun r : Int × Int => r.1*r.2)).sum
  let half : Int := (2 : Int)^(bits-1)
  have hmodpos : 0 < modulus := by dsimp [modulus]; exact Int.pow_pos (by decide : (0 : Int) < 2)
  have hhalfpos : 0 < half := by dsimp [half]; exact Int.pow_pos (by decide : (0 : Int) < 2)
  have hbitspos : bits - 1 + 1 = bits := by omega
  have hmod_half : modulus = 2 * half := by
    dsimp [modulus, half]
    rw [← hbitspos, Int.pow_add]
    simp
    grind
  have hnatbound : S.natAbs ≤ rows.length*a*b := by
    dsimp [S]
    exact sum_natAbs_prod_bound rows a b hbounds
  have hSabs_lt_half : (S.natAbs : Int) < half := by
    have h1 : (S.natAbs : Int) ≤ (rows.length : Int) * (a : Int) * (b : Int) := by
      have h := Int.ofNat_le.mpr hnatbound
      simpa only [Int.natCast_mul] using h
    have h2 : (rows.length : Int) * (a : Int) * (b : Int) < half := by
      have h := Int.ofNat_lt.mpr hbudget
      dsimp [half]
      simpa [Int.natCast_mul, Int.natCast_pow] using h
    omega
  have hres_eq : residues = rows.map (fun r : Int × Int => (r.1*r.2) % modulus) := by
    dsimp [residues]
    apply List.map_congr_left
    intro r hr
    rcases r with ⟨x,y⟩
    exact (Int.mul_emod x y modulus).symm
  have hzmod : z = S % modulus := by
    dsimp [z]
    rw [lane_eq_emod]
    rw [hres_eq]
    dsimp [S]
    simpa [List.map_map, Function.comp_def] using
      sum_emod_mod (rows.map (fun r : Int × Int => r.1*r.2)) modulus
  by_cases hSnonneg : 0 ≤ S
  · have hSltmod : S < modulus := by omega
    have hz_eq : z = S := by rw [hzmod, Int.emod_eq_of_lt hSnonneg hSltmod]
    have hcond : 2*z < modulus := by rw [hz_eq]; omega
    rw [ite_eq_left hcond, hz_eq]
  · have hSneg : S < 0 := by omega
    have hSgtneghalf : -half < S := by
      have hnegnonneg : 0 ≤ -S := by omega
      have habsneg : (S.natAbs : Int) = -S := by
        rw [← Int.natAbs_neg]
        exact Int.natAbs_of_nonneg hnegnonneg
      omega
    have hplusnonneg : 0 ≤ S + modulus := by omega
    have hpluslt : S + modulus < modulus := by omega
    have hplusmod : (S + modulus) % modulus = S % modulus := by
      simp
    have hz_eq : z = S + modulus := by
      rw [hzmod, ← hplusmod]
      exact Int.emod_eq_of_lt hplusnonneg hpluslt
    have hcond : ¬ 2*z < modulus := by rw [hz_eq]; omega
    rw [ite_eq_right hcond, hz_eq]
    change S = S + modulus - modulus
    omega

theorem sound_ShiftedCovariance :
    TinylambOpt.ResearchGoals.Frontiers.Arithmetic.ShiftedCovariance.Goal := by
  intro rows ax ay
  let n : Int := rows.length
  let center := rows.map (fun (x,y) => (x-ax,y-ay))
  let numerator := fun (rs : List (Int × Int)) =>
    n*(rs.map (fun (x,y) => x*y)).sum - (rs.map Prod.fst).sum*(rs.map Prod.snd).sum
  have hfstmap : center.map Prod.fst = rows.map (fun r => r.1 - ax) := by
    dsimp [center]
    rw [List.map_map]
    apply List.map_congr_left
    intro r hr
    rfl
  have hsndmap : center.map Prod.snd = rows.map (fun r => r.2 - ay) := by
    dsimp [center]
    rw [List.map_map]
    apply List.map_congr_left
    intro r hr
    rfl
  have hprodmap : center.map (fun (x,y) => x*y) = rows.map (fun r => (r.1-ax)*(r.2-ay)) := by
    dsimp [center]
    rw [List.map_map]
    apply List.map_congr_left
    intro r hr
    rfl
  have hfst : (center.map Prod.fst).sum = (rows.map Prod.fst).sum - n*ax := by
    rw [hfstmap]
    dsimp [n]
    exact sum_fst_shift rows ax
  have hsnd : (center.map Prod.snd).sum = (rows.map Prod.snd).sum - n*ay := by
    rw [hsndmap]
    dsimp [n]
    exact sum_snd_shift rows ay
  have hprod : (center.map (fun (x,y) => x*y)).sum =
      (rows.map (fun (x,y) => x*y)).sum - ax*(rows.map Prod.snd).sum -
        ay*(rows.map Prod.fst).sum + n*ax*ay := by
    rw [hprodmap]
    dsimp [n]
    exact sum_prod_shift rows ax ay
  dsimp [numerator]
  rw [hprod, hfst, hsnd]
  grind

end TinylambOpt.ResearchProofs.Frontiers.Arithmetic
