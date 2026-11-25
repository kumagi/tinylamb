import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_PowerOfTwo

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

private def gray (r : Nat) : Nat := r ^^^ (r / 2)

private theorem xor_div_two (a b : Nat) : (a ^^^ b) / 2 = (a / 2) ^^^ (b / 2) := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp [Nat.testBit_div_two, Nat.testBit_xor]

private theorem xor_eq_zero_iff_eq (a b : Nat) : a ^^^ b = 0 ↔ a = b := by
  constructor
  · intro h
    apply Nat.eq_of_testBit_eq
    intro i
    have ht := congrArg (fun n : Nat => n.testBit i) h
    simp [Nat.testBit_xor] at ht
    cases ha : a.testBit i <;> cases hb : b.testBit i <;> simp [ha, hb] at ht ⊢
  · intro h
    subst b
    exact Nat.xor_self a

private def invGray (n : Nat) : Nat :=
  if n = 0 then 0 else n ^^^ invGray (n / 2)
termination_by n
decreasing_by exact Nat.div_lt_self (by omega) (by decide : 1 < 2)

private theorem invGray_half (n : Nat) : invGray n / 2 = invGray n ^^^ n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · rw [invGray]
        simp [hn]
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hih := ih (n / 2) hdiv
        rw [invGray]
        simp [hn]
        rw [xor_div_two, hih]
        calc
          n / 2 ^^^ (invGray (n / 2) ^^^ n / 2) =
              (invGray (n / 2) ^^^ n / 2) ^^^ n / 2 := by rw [Nat.xor_comm]
          _ = invGray (n / 2) ^^^ (n / 2 ^^^ n / 2) := by rw [Nat.xor_assoc]
          _ = invGray (n / 2) := by rw [Nat.xor_self, Nat.xor_zero]
          _ = (n ^^^ invGray (n / 2)) ^^^ n := by
            rw [Nat.xor_comm n (invGray (n / 2))]
            rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem gray_invGray (n : Nat) : gray (invGray n) = n := by
  unfold gray
  rw [invGray_half]
  rw [← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]

private theorem gray_ne_zero_of_pos {n : Nat} (hn : 0 < n) : gray n ≠ 0 := by
  intro h
  unfold gray at h
  have heq : n = n / 2 := (xor_eq_zero_iff_eq n (n / 2)).mp h
  omega

private theorem invGray_gray (n : Nat) : invGray (gray n) = n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · rw [invGray]
        simp [hn, gray]
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hih := ih (n / 2) hdiv
        have hpos : gray n ≠ 0 := gray_ne_zero_of_pos (by omega)
        have hhalf : gray n / 2 = gray (n / 2) := by
          unfold gray
          rw [xor_div_two]
        rw [invGray]
        simp [hpos]
        rw [hhalf, hih]
        unfold gray
        rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem invGray_lt_two_pow (d n : Nat) (hn : n < 2 ^ d) : invGray n < 2 ^ d := by
  induction n using Nat.strongRecOn generalizing d with
  | ind n ih =>
      by_cases hn0 : n = 0
      · rw [invGray]
        simp [hn0]
        exact Nat.two_pow_pos d
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hndiv : n / 2 < 2 ^ d := by omega
        have hih := ih (n / 2) hdiv d hndiv
        rw [invGray]
        simp [hn0]
        exact Nat.xor_lt_two_pow hn hih

private theorem gray_lt_two_pow (d n : Nat) (hn : n < 2 ^ d) : gray n < 2 ^ d := by
  unfold gray
  exact Nat.xor_lt_two_pow hn (by omega)



private theorem gray_inj (a b : Nat) (h : gray a = gray b) : a = b := by
  have hh := congrArg invGray h
  simpa [invGray_gray] using hh

private def grayIdx (w i : Nat) : Nat := (i / w) * w + gray (i % w)

private theorem grayIdx_div (d i : Nat) : grayIdx (2 ^ d) i / 2 ^ d = i / 2 ^ d := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := gray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact gray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) / w = i / w
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_div_left s (i / w) hw, Nat.div_eq_of_lt hs, Nat.zero_add]

private theorem grayIdx_mod (d i : Nat) : grayIdx (2 ^ d) i % 2 ^ d = gray (i % 2 ^ d) := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := gray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact gray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) % w = s
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hs]

private theorem grayIdx_bound (d n i : Nat) (hn : n % 2 ^ d = 0) (hi : i < n) :
    grayIdx (2 ^ d) i < n := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  have hnd : n = (n / w) * w := by
    have hd := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd
    rw [Nat.mul_comm] at hd
    exact hd.symm
  have hklt : i / w < n / w := by
    rw [Nat.div_lt_iff_lt_mul hw]
    rw [← hnd]
    exact hi
  calc
    grayIdx w i = (i / w) * w + (grayIdx w i) % w := by
      have hdec := Nat.div_add_mod (grayIdx w i) w
      rw [show grayIdx w i / w = i / w by simpa [w] using grayIdx_div d i] at hdec
      simpa [Nat.mul_comm] using hdec.symm
    _ < ((i / w) + 1) * w := by
      rw [Nat.add_mul, Nat.one_mul]
      have hs : (grayIdx w i) % w < w := Nat.mod_lt _ hw
      exact Nat.add_lt_add_left hs ((i / w) * w)
    _ ≤ n := by
      rw [hnd]
      exact Nat.mul_le_mul_right w (by omega)

private theorem grayIdx_inj (d a b : Nat)
    (h : grayIdx (2 ^ d) a = grayIdx (2 ^ d) b) : a = b := by
  let w := 2 ^ d
  have hd := congrArg (fun x : Nat => x / w) h
  have hm := congrArg (fun x : Nat => x % w) h
  change grayIdx w a / w = grayIdx w b / w at hd
  change grayIdx w a % w = grayIdx w b % w at hm
  rw [show grayIdx w a / w = a / w by simpa [w] using grayIdx_div d a] at hd
  rw [show grayIdx w b / w = b / w by simpa [w] using grayIdx_div d b] at hd
  rw [show grayIdx w a % w = gray (a % w) by simpa [w] using grayIdx_mod d a] at hm
  rw [show grayIdx w b % w = gray (b % w) by simpa [w] using grayIdx_mod d b] at hm
  have hmod : a % w = b % w := gray_inj _ _ hm
  calc
    a = (a / w) * w + a % w := by simpa [Nat.mul_comm] using (Nat.div_add_mod a w).symm
    _ = (b / w) * w + b % w := by rw [hd, hmod]
    _ = b := by simpa [Nat.mul_comm] using Nat.div_add_mod b w

private def idxInv (w i : Nat) : Nat := (i / w) * w + invGray (i % w)

private theorem idxInv_div (d i : Nat) : idxInv (2 ^ d) i / 2 ^ d = i / 2 ^ d := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := invGray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact invGray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) / w = i / w
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_div_left s (i / w) hw, Nat.div_eq_of_lt hs, Nat.zero_add]

private theorem idxInv_mod (d i : Nat) : idxInv (2 ^ d) i % 2 ^ d = invGray (i % 2 ^ d) := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := invGray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact invGray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) % w = s
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hs]

private theorem idxInv_bound (d n x : Nat) (hn : n % 2 ^ d = 0) (hx : x < n) :
    idxInv (2 ^ d) x < n := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  have hnd : n = (n / w) * w := by
    have hd := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd
    rw [Nat.mul_comm] at hd
    exact hd.symm
  have hklt : x / w < n / w := by
    rw [Nat.div_lt_iff_lt_mul hw]
    rw [← hnd]
    exact hx
  calc
    idxInv w x = (x / w) * w + (idxInv w x) % w := by
      have hdec := Nat.div_add_mod (idxInv w x) w
      rw [show idxInv w x / w = x / w by simpa [w] using idxInv_div d x] at hdec
      simpa [Nat.mul_comm] using hdec.symm
    _ < ((x / w) + 1) * w := by
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left (Nat.mod_lt _ hw) ((x / w) * w)
    _ ≤ n := by
      rw [hnd]
      exact Nat.mul_le_mul_right w (by omega)

private theorem grayIdx_idxInv (d x : Nat) : grayIdx (2 ^ d) (idxInv (2 ^ d) x) = x := by
  let w := 2 ^ d
  have hiDiv : idxInv w x / w = x / w := by simpa [w] using idxInv_div d x
  have hiMod : idxInv w x % w = invGray (x % w) := by simpa [w] using idxInv_mod d x
  have hgi : gray (invGray (x % w)) = x % w := gray_invGray (x % w)
  unfold grayIdx
  rw [hiDiv, hiMod, hgi]
  simpa [Nat.mul_comm] using Nat.div_add_mod x w

private theorem grayIdx_perm (d n : Nat) (hn : n % 2 ^ d = 0) :
    List.Perm ((List.range n).map (grayIdx (2 ^ d))) (List.range n) := by
  have hnd : ((List.range n).map (grayIdx (2 ^ d))).Nodup := by
    exact List.Pairwise.map (R := fun a b : Nat => a ≠ b) (S := fun a b : Nat => a ≠ b)
      (grayIdx (2 ^ d)) (fun a b hne hEq => hne (grayIdx_inj d a b hEq)) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnd List.nodup_range).mpr
  intro x
  constructor
  · intro hx
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hx
    exact List.mem_range.mpr (grayIdx_bound d n i hn (List.mem_range.mp hi))
  · intro hx
    refine List.mem_map.mpr ⟨idxInv (2 ^ d) x,
      List.mem_range.mpr (idxInv_bound d n x hn (List.mem_range.mp hx)), ?_⟩
    exact grayIdx_idxInv d x



private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem rows_grayTile (c : Config) :
    (rows
        GrayTile.layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat =>
        c.base + c.step * ((grayIdx (width c) i : Nat) : Int)) := by
  let w := width c
  have hrows : rows
        GrayTile.layout c =
      (List.range c.n).map (fun i : Nat =>
        (i, c.base + c.step * ((grayIdx w i : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * ((grayIdx w i : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [GrayTile.layout,
      grayIdx, gray, w]
  rw [hrows]
  simp [w, List.map_map, Function.comp_def]

private theorem rows_linearOrdinal (c : Config) :
    (rows
        LinearOrdinal.layout c).map Prod.snd =
      (List.range c.n).map (fun (j : Nat) => c.base + c.step * (j : Int)) := by
  have hrows : rows
        LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * (i : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [LinearOrdinal.layout]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

theorem sound_GrayTile_PowerOfTwo :
    GrayTile.PowerOfTwo.Goal := by
  intro c d hw hn
  rw [rows_grayTile c, rows_linearOrdinal c]
  rw [hw] at hn ⊢
  by_cases hs : c.step = 0
  · simp [hs]
  · have hp := grayIdx_perm d c.n hn
    have hval := List.Perm.map (fun j : Nat => c.base + c.step * (j : Int)) hp
    simpa [grayIdx, Function.comp_def] using hval

end TinylambOpt.ResearchProofs.Atlas.Goal_GrayTile_PowerOfTwo
