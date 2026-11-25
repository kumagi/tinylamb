import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E6

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Generic list helpers -/

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

private theorem filterMap_eq_filter_map {α β : Type} (l : List α) (p : α → Bool) (g : α → β) :
    l.filterMap (fun x => if p x then some (g x) else none) = (l.filter p).map g := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem perm_append_cons {α : Type} (u : List α) (a : α) (v : List α) :
    List.Perm (u ++ a :: v) (a :: (u ++ v)) := by
  induction u with
  | nil => simp
  | cons x xs ih =>
      have h1 := ih.cons x
      have h2 := List.Perm.swap a x (xs ++ v)
      simpa using h1.trans h2

private theorem perm_append_comm {α : Type} (a b : List α) : List.Perm (a ++ b) (b ++ a) := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      have h1 := ih.cons x
      have h2 := (perm_append_cons b x xs).symm
      exact h1.trans h2

private theorem filter_span_decomp (n s w : Nat) :
    (List.range n).filter (fun i => i < s || s + w ≤ i) =
      List.range (min n s) ++
        (List.range (n - min n (s + w))).map (fun j => min n (s + w) + j) := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hstep : List.range (n + 1) = List.range n ++ [n] := by
        rw [show n + 1 = Nat.succ n by omega, List.range_succ]
      rw [hstep, List.filter_append, ih]
      by_cases h1 : n < s
      · have hm_s : min n s = n := Nat.min_eq_left (by omega)
        have hm_sw : min n (s + w) = n := Nat.min_eq_left (by omega)
        have hm_s' : min (n + 1) s = n + 1 := Nat.min_eq_left (by omega)
        have hm_sw' : min (n + 1) (s + w) = n + 1 := Nat.min_eq_left (by omega)
        simp [h1, hm_s, hm_sw, hm_s', hm_sw']
        exact hstep.symm
      · by_cases h2 : s + w ≤ n
        · have hm_s : min n s = s := Nat.min_eq_right (by omega)
          have hm_sw : min n (s + w) = s + w := Nat.min_eq_right (by omega)
          have hm_s' : min (n + 1) s = s := Nat.min_eq_right (by omega)
          have hm_sw' : min (n + 1) (s + w) = s + w := Nat.min_eq_right (by omega)
          simp [h1, h2, hm_s, hm_sw, hm_s', hm_sw']
          rw [show (n + 1) - (s + w) = (n - (s + w)) + 1 by omega, List.range_succ,
            List.map_append]
          simp [Nat.add_sub_of_le h2]
        · have hm_s : min n s = s := Nat.min_eq_right (by omega)
          have hm_s' : min (n + 1) s = s := Nat.min_eq_right (by omega)
          have hm_sw : min n (s + w) = n := Nat.min_eq_left (by omega)
          have hm_sw' : min (n + 1) (s + w) = n + 1 := Nat.min_eq_left (by omega)
          simp [h1, h2, hm_s, hm_s', hm_sw, hm_sw']

/-- The k-th survivor of a contiguous deleted span. -/
private theorem kth_span_filter (n s w k : Nat) (hw : 0 < w)
    (hk : k < ((List.range n).filter (fun i => i < s || s + w ≤ i)).length) :
    ((List.range n).filter (fun i => i < s || s + w ≤ i))[k]? =
      some (if k < s then k else k + w) := by
  rw [filter_span_decomp n s w] at hk ⊢
  let a := min n s
  let b := min n (s + w)
  change k < (List.range a ++ (List.range (n - b)).map (fun j => b + j)).length at hk
  change (List.range a ++ (List.range (n - b)).map (fun j => b + j))[k]? = _
  simp only [List.length_append, List.length_range, List.length_map] at hk
  rw [List.getElem?_append]
  simp only [List.length_range]
  by_cases hka : k < a
  · rw [if_pos hka]
    rw [List.getElem?_range hka]
    have hks : k < s := by dsimp [a] at hka; omega
    simp [hks]
  · rw [if_neg hka]
    have hidx : k - a < n - b := by omega
    rw [List.getElem?_map, List.getElem?_range hidx]
    simp only [Option.map_some]
    have hbn : b < n := by
      by_cases h' : b < n
      · exact h'
      · have hbge : n ≤ b := by omega
        have hnb : n - b = 0 := Nat.sub_eq_zero_of_le hbge
        omega
    have hswn : s + w < n := by
      by_cases h' : s + w < n
      · exact h'
      · have hge : n ≤ s + w := by omega
        have hbeq : b = n := by dsimp [b]; exact Nat.min_eq_left hge
        omega
    have haeq : a = s := by dsimp [a]; exact Nat.min_eq_right (by omega)
    have hbeq : b = s + w := by dsimp [b]; exact Nat.min_eq_right (by omega)
    rw [haeq, hbeq]
    have hnle : ¬ k < s := by omega
    simp [hnle]
    omega

private theorem width_pos (c : Config) : 0 < width c := by
  dsimp [width]
  omega

private theorem rows_deletedSpan (c : Config) :
    rows DeletedSpan.layout c =
      ((List.range c.n).filter (fun i => i < c.split || c.split + width c ≤ i)).map
        (fun (i : Nat) => (i, c.base + c.step * (i : Int))) := by
  unfold rows
  have hcongr : (List.range c.n).filterMap
        (fun i => (DeletedSpan.layout.read c i).map (fun v => (i, v))) =
      (List.range c.n).filterMap (fun i =>
        if i < c.split || c.split + width c ≤ i
        then some (i, c.base + c.step * (i : Int)) else none) := by
    apply filterMap_congr'
    intro i hi
    by_cases hlt : i < c.split
    · have hnotle : ¬ c.split ≤ i := by omega
      simp [DeletedSpan.layout, hlt, hnotle]
    · by_cases hge : c.split + width c ≤ i
      · simp [DeletedSpan.layout, hlt, hge]
      · have hdelprop : c.split ≤ i ∧ i < c.split + width c := by omega
        simp [DeletedSpan.layout, hdelprop, hlt, hge]
  rw [hcongr]
  exact filterMap_eq_filter_map (l := List.range c.n)
    (p := fun i : Nat => i < c.split || c.split + width c ≤ i)
    (g := fun (i : Nat) => (i, c.base + c.step * (i : Int)))

theorem DS_SeekSurvivor :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan.SeekSurvivor.Goal := by
  intro c k hk
  rw [rows_deletedSpan c] at hk
  rw [rows_deletedSpan c]
  have hk' : k < ((List.range c.n).filter (fun i => i < c.split || c.split + width c ≤ i)).length := by
    simpa using hk
  rw [List.getElem?_map]
  rw [kth_span_filter c.n c.split (width c) k (width_pos c) hk']
  simp

/-! ## Rotation permutation -/

private theorem rotation_perm (n s : Nat) (hs : s < n) :
    List.Perm ((List.range n).map (fun i => (i + s) % n)) (List.range n) := by
  have hn1 : (n - s) + s = n := Nat.sub_add_cancel (by omega)
  have hn2 : s + (n - s) = n := Nat.add_sub_of_le (by omega : s ≤ n)
  have hrange1 : List.range n = List.range (n - s) ++ (List.range s).map (fun j => n - s + j) := by
    have h := List.range_add (n := n - s) (m := s)
    rw [hn1] at h
    exact h
  have hdecomp : (List.range n).map (fun i => (i + s) % n) =
      (List.range (n - s)).map (fun i => i + s) ++ List.range s := by
    rw [hrange1, List.map_append]
    congr 1
    · apply List.map_congr_left
      intro i hi
      have hi' : i < n - s := List.mem_range.mp hi
      exact Nat.mod_eq_of_lt (by omega)
    · rw [List.map_map]
      have hmap : List.map (fun j : Nat => (n - s + j + s) % n) (List.range s) =
          List.map (fun j : Nat => j) (List.range s) := by
        apply List.map_congr_left
        intro j hj
        have hj' : j < s := List.mem_range.mp hj
        rw [show n - s + j + s = j + n by omega, Nat.add_mod_right]
        exact Nat.mod_eq_of_lt (by omega)
      simpa [Function.comp_def] using hmap
  have hbase : List.range n = List.range s ++ (List.range (n - s)).map (fun i => i + s) := by
    have h := List.range_add (n := s) (m := n - s)
    rw [hn2] at h
    rw [h]
    congr 1
    apply List.map_congr_left
    intro i hi
    rw [Nat.add_comm]
  rw [hdecomp, hbase]
  exact perm_append_comm _ _

private theorem rows_rotatedPage (c : Config) :
    (rows RotatedPage.layout c).map Prod.snd =
      ((List.range c.n).map (fun i => (i + c.split % c.n) % c.n)).map
        (fun (j : Nat) => c.base + c.step * (j : Int)) := by
  have hrows : rows RotatedPage.layout c =
      (List.range c.n).map (fun i : Nat =>
        (i, c.base + c.step * (((i + c.split % c.n) % c.n : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * (((i + c.split % c.n) % c.n : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [RotatedPage.layout]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

private theorem rows_linearOrdinal (c : Config) :
    (rows LinearOrdinal.layout c).map Prod.snd =
      (List.range c.n).map (fun (j : Nat) => c.base + c.step * (j : Int)) := by
  have hrows : rows LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * (i : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [LinearOrdinal.layout]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

theorem RP_PermutationAggregate :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage.PermutationAggregate.Goal := by
  intro c
  rw [rows_rotatedPage c, rows_linearOrdinal c]
  by_cases hn : c.n = 0
  · simp [hn]
  · have hs : c.split % c.n < c.n := Nat.mod_lt _ (by omega)
    exact List.Perm.map (fun (j : Nat) => c.base + c.step * (j : Int))
      (rotation_perm c.n (c.split % c.n) hs)

/-! ## Gray code -/

private def gray (r : Nat) : Nat := r ^^^ (r / 2)

private theorem gray_div_two (r : Nat) : gray r / 2 = gray (r / 2) := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [← Nat.testBit_succ]
  simp only [gray, Nat.testBit_xor]
  rw [Nat.testBit_succ, Nat.testBit_succ]

private theorem bit_mod_two (x : Nat) : (if x.testBit 0 then 1 else 0) = x % 2 := by
  rw [Nat.testBit_zero]
  have hlt : x % 2 < 2 := Nat.mod_lt _ (by decide)
  by_cases h : x % 2 = 1
  · simp [h]
  · have h0 : x % 2 = 0 := by omega
    simp [h0]

private theorem gray_mod_two (r : Nat) : gray r % 2 = (r % 2) ^^^ ((r / 2) % 2) := by
  rw [← bit_mod_two, gray, Nat.testBit_xor, ← bit_mod_two, ← bit_mod_two]
  cases r.testBit 0 <;> cases (r / 2).testBit 0 <;> decide

private theorem gray_inj (a b : Nat) (h : gray a = gray b) : a = b := by
  induction hsum : a + b using Nat.strongRecOn generalizing a b with
  | ind s ih =>
      have hsab : a + b = s := hsum
      by_cases hz : a + b = 0
      · have ha0 : a = 0 := by omega
        have hb0 : b = 0 := by omega
        subst a; subst b; rfl
      · have hlt : a / 2 + b / 2 < s := by
          rw [← hsab]
          by_cases ha0 : a = 0
          · subst a
            have hbpos : 0 < b := by omega
            have hbdiv : b / 2 < b := Nat.div_lt_self hbpos (by decide : 1 < 2)
            simpa using hbdiv
          · have hapos : 0 < a := Nat.pos_of_ne_zero ha0
            have hadiv : a / 2 < a := Nat.div_lt_self hapos (by decide : 1 < 2)
            have hble : b / 2 ≤ b := Nat.div_le_self b 2
            omega
        have hdiv : gray (a / 2) = gray (b / 2) := by
          have hh := congrArg (fun x => x / 2) h
          simpa [gray_div_two] using hh
        have hab : a / 2 = b / 2 := ih (a / 2 + b / 2) hlt (a / 2) (b / 2) hdiv rfl
        have hmod : (a % 2) ^^^ ((a / 2) % 2) = (b % 2) ^^^ ((b / 2) % 2) := by
          have hh := congrArg (fun x => x % 2) h
          change gray a % 2 = gray b % 2 at hh
          simpa [gray_mod_two] using hh
        rw [hab] at hmod
        have hmod' : a % 2 = b % 2 := by
          have hh := congrArg (fun x => x ^^^ ((b / 2) % 2)) hmod
          simpa [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero] using hh
        have ha := Nat.div_add_mod a 2
        have hb := Nat.div_add_mod b 2
        omega

private theorem gray_xor_half (g u : Nat) (hu : gray u = g / 2) : (g ^^^ u) / 2 = u := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [← Nat.testBit_succ, Nat.testBit_xor]
  have hbit := congrArg (fun x => x.testBit i) hu
  change (u ^^^ (u / 2)).testBit i = (g / 2).testBit i at hbit
  rw [Nat.testBit_xor] at hbit
  have hgsucc : (g / 2).testBit i = g.testBit (i + 1) := (Nat.testBit_succ g i).symm
  have husucc : (u / 2).testBit i = u.testBit (i + 1) := (Nat.testBit_succ u i).symm
  rw [hgsucc, husucc] at hbit
  cases hg : g.testBit (i + 1) <;> cases hu1 : u.testBit (i + 1) <;>
    cases hui : u.testBit i <;> simp_all

private def ungray : Nat → Nat
  | 0 => 0
  | Nat.succ g => Nat.succ g ^^^ ungray (Nat.succ g / 2)
termination_by x => x
decreasing_by
  exact Nat.div_lt_self (Nat.succ_pos _) (by decide : 1 < 2)

private theorem gray_ungray (x : Nat) : gray (ungray x) = x := by
  induction x using Nat.strongRecOn with
  | ind x ih =>
      cases x with
      | zero => rw [ungray.eq_1]; rfl
      | succ x' =>
          have hlt : (x' + 1) / 2 < x' + 1 := Nat.div_lt_self (by omega) (by decide : 1 < 2)
          have hu : gray (ungray ((x' + 1) / 2)) = (x' + 1) / 2 := ih ((x' + 1) / 2) hlt
          have hhalf : ((x' + 1) ^^^ ungray ((x' + 1) / 2)) / 2 = ungray ((x' + 1) / 2) :=
            gray_xor_half (x' + 1) (ungray ((x' + 1) / 2)) hu
          change gray (ungray (Nat.succ x')) = Nat.succ x'
          rw [ungray.eq_2]
          unfold gray
          rw [hhalf]
          rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem gray_lt_pow_iff (s d : Nat) : gray s < 2 ^ d ↔ s < 2 ^ d := by
  induction d generalizing s with
  | zero =>
      simp only [Nat.pow_zero, Nat.lt_one_iff]
      constructor
      · intro h
        have hz : gray s = 0 := by omega
        unfold gray at hz
        have hsq : s = s / 2 := by
          have hx := congrArg (fun x => x ^^^ (s / 2)) hz
          simpa [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero] using hx
        by_cases hs : s = 0
        · omega
        · have hlt : s / 2 < s := Nat.div_lt_self (Nat.pos_of_ne_zero hs) (by decide : 1 < 2)
          omega
      · intro hs
        subst s
        simp [gray]
  | succ d ih =>
      constructor
      · intro h
        have hdiv : gray (s / 2) < 2 ^ d := by
          rw [← gray_div_two]
          have hmul : gray s < 2 * 2 ^ d := by
            rw [show 2 ^ (d + 1) = 2 * 2 ^ d by rw [Nat.pow_succ, Nat.mul_comm]] at h
            exact h
          omega
        have hs2 : s / 2 < 2 ^ d := (ih (s / 2)).mp hdiv
        have hsle : s ≤ (s / 2) * 2 + 1 := by
          have hm := Nat.div_add_mod s 2
          have hmod : s % 2 < 2 := Nat.mod_lt _ (by decide)
          omega
        rw [show 2 ^ (d + 1) = 2 * 2 ^ d by rw [Nat.pow_succ, Nat.mul_comm]]
        omega
      · intro hs
        exact Nat.xor_lt_two_pow hs (Nat.lt_of_le_of_lt (Nat.div_le_self s 2) hs)

private theorem gray_surj_pow (d r : Nat) (hr : r < 2 ^ d) :
    ∃ s, s < 2 ^ d ∧ gray s = r := by
  refine ⟨ungray r, ?_, gray_ungray r⟩
  exact (gray_lt_pow_iff (ungray r) d).mp (by simpa [gray_ungray] using hr)

private theorem rows_grayTile (c : Config) :
    (rows GrayTile.layout c).map Prod.snd =
      (List.range c.n).map (fun (i : Nat) =>
        c.base + c.step * ((i / width c * width c + gray (i % width c) : Nat) : Int)) := by
  have hrows : rows GrayTile.layout c =
      (List.range c.n).map (fun (i : Nat) =>
        (i, c.base + c.step * ((i / width c * width c + gray (i % width c) : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun (i : Nat) =>
      (i, c.base + c.step * ((i / width c * width c + gray (i % width c) : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [GrayTile.layout, gray]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

private theorem grayTile_index_inj (w : Nat) (hw : 0 < w)
    (hgray : ∀ r, r < w → gray r < w)
    {i₁ i₂ : Nat}
    (h : i₁ / w * w + gray (i₁ % w) = i₂ / w * w + gray (i₂ % w)) : i₁ = i₂ := by
  have hr1 : gray (i₁ % w) < w := hgray _ (Nat.mod_lt _ hw)
  have hr2 : gray (i₂ % w) < w := hgray _ (Nat.mod_lt _ hw)
  have hk1 : (i₁ / w * w + gray (i₁ % w)) / w = i₁ / w := by
    rw [Nat.add_comm (i₁ / w * w) (gray (i₁ % w)), Nat.mul_comm (i₁ / w) w,
      Nat.add_mul_div_left (gray (i₁ % w)) (i₁ / w) hw, Nat.div_eq_of_lt hr1, Nat.zero_add]
  have hk2 : (i₂ / w * w + gray (i₂ % w)) / w = i₂ / w := by
    rw [Nat.add_comm (i₂ / w * w) (gray (i₂ % w)), Nat.mul_comm (i₂ / w) w,
      Nat.add_mul_div_left (gray (i₂ % w)) (i₂ / w) hw, Nat.div_eq_of_lt hr2, Nat.zero_add]
  have hkeq : i₁ / w = i₂ / w := by
    have hh := congrArg (fun x => x / w) h
    simpa [hk1, hk2] using hh
  have hgreq : gray (i₁ % w) = gray (i₂ % w) := by
    rw [hkeq] at h
    omega
  have hreq : i₁ % w = i₂ % w := gray_inj _ _ hgreq
  have hk1' : w * (i₁ / w) = w * (i₂ / w) := by rw [hkeq]
  calc
    i₁ = w * (i₁ / w) + i₁ % w := (Nat.div_add_mod i₁ w).symm
    _ = w * (i₂ / w) + i₂ % w := by rw [hk1', hreq]
    _ = i₂ := Nat.div_add_mod i₂ w

private theorem grayTile_index_perm (n w d : Nat) (hw : 0 < w) (hw2 : w = 2 ^ d)
    (hn : n % w = 0) :
    List.Perm ((List.range n).map (fun i : Nat => i / w * w + gray (i % w))) (List.range n) := by
  let f : Nat → Nat := fun i => i / w * w + gray (i % w)
  have hq : n = (n / w) * w := by
    have hd := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd
    rw [Nat.mul_comm] at hd
    exact hd.symm
  have hgray : ∀ r, r < w → gray r < w := by
    intro r hr
    have hr' : r < 2 ^ d := by simpa [hw2] using hr
    have hh := (gray_lt_pow_iff r d).mpr hr'
    simpa [hw2] using hh
  have hfinj : ∀ i₁ i₂, f i₁ = f i₂ → i₁ = i₂ := by
    intro i₁ i₂ h
    exact grayTile_index_inj w hw hgray h
  have hnd : ((List.range n).map f).Nodup := by
    exact List.Pairwise.map (R := fun a b : Nat => a ≠ b) (S := fun a b : Nat => a ≠ b) f
      (fun a b hne hEq => hne (hfinj a b hEq)) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnd List.nodup_range).mpr
  intro x
  constructor
  · intro hx
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hx
    have hi' : i < n := List.mem_range.mp hi
    have hr : gray (i % w) < w := hgray _ (Nat.mod_lt _ hw)
    have hklt : i / w < n / w := by
      rw [Nat.div_lt_iff_lt_mul hw]
      rw [← hq]
      exact hi'
    have hbound : f i < ((i / w) + 1) * w := by
      dsimp [f]
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left hr (i / w * w)
    have hle : ((i / w) + 1) * w ≤ n := by
      rw [hq]
      exact Nat.mul_le_mul_right w (by omega)
    exact List.mem_range.mpr (by omega)
  · intro hx
    have hx' : x < n := List.mem_range.mp hx
    let k := x / w
    let r := x % w
    have hkw : k < n / w := by
      dsimp [k]
      rw [Nat.div_lt_iff_lt_mul hw]
      rw [← hq]
      exact hx'
    have hr : r < w := Nat.mod_lt _ hw
    obtain ⟨s, hsw, hgs⟩ := gray_surj_pow d r (by simpa [hw2] using hr)
    have hsw' : s < w := by simpa [hw2] using hsw
    let i := k * w + s
    have hin : i < n := by
      dsimp [i]
      rw [hq]
      have hkle : k + 1 ≤ n / w := by omega
      have hmul := Nat.mul_le_mul_right w hkle
      have hbound : k * w + s < (k + 1) * w := by
        rw [Nat.add_mul, Nat.one_mul]
        exact Nat.add_lt_add_left hsw' (k * w)
      omega
    refine List.mem_map.mpr ⟨i, List.mem_range.mpr hin, ?_⟩
    dsimp [f, i, k, r]
    have hiw : (k * w + s) / w = k := by
      rw [Nat.add_comm (k * w) s, Nat.mul_comm k w, Nat.add_mul_div_left s k hw,
        Nat.div_eq_of_lt hsw', Nat.zero_add]
    have him : (k * w + s) % w = s := by
      rw [Nat.add_comm (k * w) s, Nat.mul_comm k w, Nat.add_mul_mod_self_left]
      exact Nat.mod_eq_of_lt hsw'
    rw [hiw, him, hgs]
    have hxdecomp := Nat.div_add_mod x w
    rw [Nat.mul_comm w (x / w)] at hxdecomp
    dsimp [k, r] at *
    exact hxdecomp

theorem GT_PowerOfTwo :
    TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile.PowerOfTwo.Goal := by
  intro c d hw hd
  rw [rows_grayTile c, rows_linearOrdinal c]
  by_cases hs : c.step = 0
  · simp [hs]
  · have hwpos : 0 < width c := width_pos c
    have hperm := grayTile_index_perm c.n (width c) d hwpos hw hd
    have hval := List.Perm.map (fun j : Nat => c.base + c.step * (j : Int)) hperm
    simpa [Function.comp_def, Nat.mul_assoc] using hval

/-! ## Progressions and intersection -/

private theorem contains_iff (a : Progression) (v : Int) :
    contains a v = true ↔
      0 < a.n ∧ a.lo ≤ v ∧ v ≤ lastVal a ∧ (a.step : Int) ∣ v - a.lo := by
  unfold contains lastVal
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · intro h
    rcases h with ⟨⟨⟨hn, hlo⟩, hhi⟩, hmod⟩
    have hmodprop : ((a.step == 0) = true) ∨ (((v - a.lo) % (a.step : Int) == 0) = true) := by
      simpa [Bool.or_eq_true] using hmod
    rcases hmodprop with hmod | hmod
    · have hs0 : a.step = 0 := Nat.eq_of_beq_eq_true hmod
      have hz : v - a.lo = 0 := by
        simp [hs0] at hhi hlo ⊢
        omega
      exact ⟨hn, hlo, hhi, ⟨0, by simpa [hs0] using hz⟩⟩
    · have hm : (v - a.lo) % (a.step : Int) = 0 := of_decide_eq_true hmod
      exact ⟨hn, hlo, hhi, Int.dvd_of_emod_eq_zero hm⟩
  · rintro ⟨hn, hlo, hhi, hdvd⟩
    refine ⟨⟨⟨hn, hlo⟩, hhi⟩, ?_⟩
    by_cases hs : a.step = 0
    · simp [hs]
    · have hm : (v - a.lo) % (a.step : Int) = 0 := by
        obtain ⟨k, hk⟩ := hdvd
        exact Int.emod_eq_zero_of_dvd ⟨k, hk⟩
      simp [hs, hm]

private theorem bezout_spec (a b : Nat) :
    (a.gcd b : Int) = (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b
        simp [bezout]
      · have hmod : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
        have hrec := ih (a % b) hmod b
        have hfst : (bezout a b).1 = (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        have hsnd : (bezout a b).2 =
            (bezout b (a % b)).1 - ((a / b : Nat) : Int) * (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        rw [hfst, hsnd]
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        simp only [hbez]
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd]
        grind

private theorem bezout_reduced_inverse (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    ((((a / a.gcd b : Nat) : Int) * (bezout (a / a.gcd b) (b / a.gcd b)).1) - 1) %
        (b / a.gcd b : Nat) = 0 := by
  let g := a.gcd b
  let A := a / g
  let B := b / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left b ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right a hb
  have hcop : A.gcd B = 1 := by
    simpa [A, B, g] using Nat.gcd_div_gcd_div_gcd_of_pos_left ha
  have hspec := bezout_spec A B
  rw [hcop] at hspec
  simp only [Int.ofNat_one] at hspec
  apply Int.emod_eq_zero_of_dvd
  refine ⟨-(bezout A B).2, ?_⟩
  change (A : Int) * (bezout A B).1 - 1 = (B : Int) * -(bezout A B).2
  rw [hspec]
  grind

private theorem span_contains_iff (first step high x : Int) (hs : 0 < step) :
    let n := if first ≤ high then ((high - first) / step).toNat + 1 else 0
    contains ⟨first, step.toNat, n⟩ x = true ↔
      first ≤ x ∧ x ≤ high ∧ (step : Int) ∣ x - first := by
  dsimp only
  constructor
  · intro hx
    have hm := (contains_iff ⟨first, step.toNat,
      if first ≤ high then ((high - first) / step).toNat + 1 else 0⟩ x).mp hx
    rcases hm with ⟨hn, hlo, hhi, hdvd⟩
    by_cases hfh : first ≤ high
    · have ht : 0 ≤ (high - first) / step := Int.ediv_nonneg (by omega) (by omega)
      have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
        Int.toNat_of_nonneg ht
      have hle : (((high - first) / step).toNat : Int) * step ≤ high - first := by
        rw [htcast]
        exact Int.ediv_mul_le _ (by omega)
      have hstepcast : (step.toNat : Int) = step := Int.toNat_of_nonneg (by omega)
      rw [if_pos hfh] at hhi
      unfold lastVal at hhi
      dsimp at hhi
      simp only [hstepcast] at hhi
      refine ⟨hlo, ?_, ?_⟩
      · omega
      · simpa [hstepcast] using hdvd
    · simp [hfh] at hn
  · rintro ⟨hlo, hhi, hdvd⟩
    have hfh : first ≤ high := by omega
    rw [if_pos hfh]
    obtain ⟨q, hq⟩ := hdvd
    have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hs
    have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
      Int.toNat_of_nonneg (Int.ediv_nonneg (by omega) (by omega))
    have hqle : q ≤ (high - first) / step := by
      apply (Int.le_ediv_iff_mul_le hs).mpr
      calc
        q * step = x - first := by rw [Int.mul_comm q step]; exact hq.symm
        _ ≤ high - first := Int.sub_le_sub_right hhi first
    apply (contains_iff ⟨first, step.toNat, ((high - first) / step).toNat + 1⟩ x).mpr
    have hstepcast : (step.toNat : Int) = step := Int.toNat_of_nonneg (by omega)
    constructor
    · exact Nat.succ_pos _
    constructor
    · exact hlo
    constructor
    · have hqle' : q ≤ (((high - first) / step).toNat : Int) := by
        rw [htcast]
        exact hqle
      have hqmul : q * step ≤ (((high - first) / step).toNat : Int) * step :=
        Int.mul_le_mul_of_nonneg_right hqle' (by omega)
      have hxbound : x ≤ first + (((high - first) / step).toNat : Int) * step := by
        have hq' : x - first = q * step := by rw [Int.mul_comm]; exact hq
        omega
      rw [show lastVal ⟨first, step.toNat, ((high - first) / step).toNat + 1⟩ =
          first + (((high - first) / step).toNat : Int) * step by
        unfold lastVal
        rw [show ((high - first) / step).toNat + 1 - 1 = ((high - first) / step).toNat by omega]
        rw [hstepcast]]
      exact hxbound
    · refine ⟨q, ?_⟩
      rw [hstepcast]
      exact hq

private theorem common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨_, _, _, hadvd⟩ := (contains_iff a v).mp ha
  obtain ⟨_, _, _, hbdvd⟩ := (contains_iff b v).mp hb
  obtain ⟨i, hi⟩ := hadvd
  obtain ⟨j, hj⟩ := hbdvd
  have h1 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * i :=
    Int.dvd_mul_of_dvd_left (by exact_mod_cast Nat.gcd_dvd_left a.step b.step)
  have h2 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * j :=
    Int.dvd_mul_of_dvd_left (by exact_mod_cast Nat.gcd_dvd_right a.step b.step)
  have heq : (a.step : Int) * i - (b.step : Int) * j = b.lo - a.lo := by omega
  rw [← heq]
  exact Int.dvd_sub h1 h2

private theorem root_dvd (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (alo blo gap : Int)
    (hgapdef : gap = blo - alo) (hgap : (as.gcd bs : Int) ∣ gap) :
    let g := as.gcd bs
    let A := as / g
    let B := bs / g
    let root := alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int))
    (as : Int) ∣ root - alo ∧ (bs : Int) ∣ root - blo := by
  dsimp only
  let g := as.gcd bs
  let A := as / g
  let B := bs / g
  let t := (gap / (g : Int) * (bezout A B).1) % (B : Int)
  have hg : 0 < g := Nat.gcd_pos_of_pos_left bs ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left bs ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right as hb
  have hasg : (as : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left as bs)).symm
  have hbsg : (bs : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  obtain ⟨z, hz⟩ := hgap
  have hzdiv : gap / (g : Int) = z := Int.ediv_eq_of_eq_mul_right (by omega) hz
  have hinv := bezout_reduced_inverse as bs ha hb
  have hinv' : (B : Int) ∣ (A : Int) * (bezout A B).1 - 1 :=
    Int.dvd_of_emod_eq_zero (by simpa [A, B, g] using hinv)
  have hmod := Int.mul_ediv_add_emod (z * (bezout A B).1) (B : Int)
  have hrem : z * (bezout A B).1 - t =
      (B : Int) * ((z * (bezout A B).1) / (B : Int)) := by
    dsimp [t]
    rw [hzdiv]
    omega
  have hdiff : (B : Int) ∣ z * ((A : Int) * (bezout A B).1 - 1) :=
    Int.dvd_mul_of_dvd_right hinv'
  have hmult : (B : Int) ∣ (A : Int) * (z * (bezout A B).1 - t) :=
    Int.dvd_mul_of_dvd_right (Int.dvd_of_emod_eq_zero (by
      rw [hrem]
      exact Int.emod_eq_zero_of_dvd ⟨_, rfl⟩))
  have hcomb : (B : Int) ∣ (A : Int) * t - z := by
    have heq : z * ((A : Int) * (bezout A B).1 - 1) -
        (A : Int) * (z * (bezout A B).1 - t) = (A : Int) * t - z := by grind
    rw [← heq]
    exact Int.dvd_sub hdiff hmult
  have hbs : (bs : Int) ∣ (as : Int) * t - gap := by
    rw [hbsg, hasg, hz]
    have hh := Int.mul_dvd_mul_left (g : Int) hcomb
    simpa only [Int.mul_sub, Int.mul_assoc] using hh
  constructor
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - alo =
        (as : Int) * t := by dsimp [t]; omega
    rw [hroot]
    exact ⟨t, rfl⟩
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - blo =
        (as : Int) * t - gap := by dsimp [t]; rw [hgapdef]; omega
    rw [hroot]
    exact hbs

private theorem first_root_dvd (low root first period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) : period ∣ first - root := by
  have hm := Int.mul_ediv_add_emod (root - low) period
  refine ⟨-((root - low) / period), ?_⟩
  grind

private theorem first_le_of_congruence (low root first x period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) (hx : low ≤ x)
    (hxdvd : period ∣ x - root) : first ≤ x := by
  have hflow : low ≤ first := by
    have hremnon : 0 ≤ (root - low) % period := Int.emod_nonneg _ (by omega)
    omega
  have hfhigh : first < low + period := by
    have hremlt : (root - low) % period < period := Int.emod_lt_of_pos _ hp
    omega
  have hfroot := first_root_dvd low root first period hp hfirst
  by_cases hle : first ≤ x
  · exact hle
  · exfalso
    have hlt : x < first := by omega
    have hpos : 0 < first - x := by omega
    have hltp : first - x < period := by omega
    have hdiff : period ∣ first - x := by
      have heq : (first - root) - (x - root) = first - x := by omega
      rw [← heq]
      exact Int.dvd_sub hfroot hxdvd
    obtain ⟨k, hk⟩ := hdiff
    have hkpos : 0 < k := Int.pos_of_mul_pos_right (by omega) hp
    have hkge : 1 ≤ k := by omega
    have hpnon : (0 : Int) ≤ period := by omega
    have hmul := Int.mul_le_mul_of_nonneg_left hkge hpnon
    omega

private theorem period_dvd_of_both (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step)
    (v : Int) (ha' : contains a v = true) (hb' : contains b v = true) :
    let g := a.step.gcd b.step
    let A := a.step / g
    let B := b.step / g
    let gap := b.lo - a.lo
    let period := A * b.step
    let unit := (bezout A B).1
    let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % (B : Int))
    (period : Int) ∣ v - root := by
  dsimp only
  let g := a.step.gcd b.step
  let A := a.step / g
  let B := b.step / g
  let gap := b.lo - a.lo
  let period := A * b.step
  let unit := (bezout A B).1
  let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % (B : Int))
  have hgpos : 0 < g := Nat.gcd_pos_of_pos_left b.step ha
  have hApos : 0 < A := Nat.div_gcd_pos_of_pos_left b.step ha
  have hBpos : 0 < B := Nat.div_gcd_pos_of_pos_right a.step hb
  have hasg : (a.step : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
  have hbsg : (b.step : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
  have hgdiv : (g : Int) ∣ gap := common_gap_dvd a b v ha' hb'
  have hrootdvd := root_dvd a.step b.step ha hb a.lo b.lo gap rfl hgdiv
  obtain ⟨c, hc⟩ := hgdiv
  have hroot : (a.step : Int) ∣ root - a.lo ∧ (b.step : Int) ∣ root - b.lo := by
    simpa [g, A, B, root, gap, unit] using hrootdvd
  obtain ⟨_, _, _, hadvd⟩ := (contains_iff a v).mp ha'
  obtain ⟨_, _, _, hbdvd⟩ := (contains_iff b v).mp hb'
  obtain ⟨i, hi⟩ := hadvd
  obtain ⟨j, hj⟩ := hbdvd
  obtain ⟨t, ht⟩ := hroot.1
  obtain ⟨s, hs⟩ := hroot.2
  have hgpos' : (0 : Int) < g := by exact_mod_cast hgpos
  have hAi : (A : Int) * i - (B : Int) * j = c := by
    have heq : (g : Int) * (A : Int) * i - (g : Int) * (B : Int) * j = (g : Int) * c := by
      dsimp [gap] at hc
      rw [hasg] at hi
      rw [hbsg] at hj
      omega
    have heq' : (g : Int) * ((A : Int) * i - (B : Int) * j) = (g : Int) * c := by
      rw [Int.mul_sub]
      have hL : (g : Int) * ((A : Int) * i) = (g : Int) * (A : Int) * i := by ac_rfl
      have hR : (g : Int) * ((B : Int) * j) = (g : Int) * (B : Int) * j := by ac_rfl
      rw [hL, hR]
      exact heq
    exact (Int.mul_eq_mul_left_iff (by omega : (g : Int) ≠ 0)).mp heq'
  have hAt : (A : Int) * t - (B : Int) * s = c := by
    have heq : (g : Int) * (A : Int) * t - (g : Int) * (B : Int) * s = (g : Int) * c := by
      dsimp [gap] at hc
      rw [hasg] at ht
      rw [hbsg] at hs
      omega
    have heq' : (g : Int) * ((A : Int) * t - (B : Int) * s) = (g : Int) * c := by
      rw [Int.mul_sub]
      have hL : (g : Int) * ((A : Int) * t) = (g : Int) * (A : Int) * t := by ac_rfl
      have hR : (g : Int) * ((B : Int) * s) = (g : Int) * (B : Int) * s := by ac_rfl
      rw [hL, hR]
      exact heq
    exact (Int.mul_eq_mul_left_iff (by omega : (g : Int) ≠ 0)).mp heq'
  have hAB : (B : Int) ∣ (A : Int) * (i - t) := by
    refine ⟨j - s, ?_⟩
    have hzero : (A : Int) * (i - t) - (B : Int) * (j - s) = 0 := by
      rw [Int.mul_sub, Int.mul_sub]
      omega
    omega
  have hinv := bezout_reduced_inverse a.step b.step ha hb
  have hinv' : (B : Int) ∣ (A : Int) * unit - 1 :=
    Int.dvd_of_emod_eq_zero (by simpa [A, B, g] using hinv)
  have h1 : (B : Int) ∣ ((A : Int) * unit - 1) * (i - t) :=
    Int.dvd_mul_of_dvd_left hinv'
  obtain ⟨m, hm⟩ := hAB
  have h2 : (B : Int) ∣ ((A : Int) * (i - t)) * unit := by
    refine ⟨m * unit, ?_⟩
    rw [hm]
    ac_rfl
  obtain ⟨k, hk⟩ := hinv'
  have h2' : (B : Int) ∣ (A : Int) * unit * (i - t) := by
    have heq : (A : Int) * unit * (i - t) = ((A : Int) * (i - t)) * unit := by ac_rfl
    rcases h2 with ⟨w, hw⟩
    refine ⟨w, ?_⟩
    rw [heq]
    exact hw
  have hdiff : (B : Int) ∣ (A : Int) * unit * (i - t) - ((A : Int) * unit - 1) * (i - t) :=
    Int.dvd_sub h2' h1
  have heqdiff : (A : Int) * unit * (i - t) - ((A : Int) * unit - 1) * (i - t) = i - t := by
    rw [Int.sub_mul, Int.one_mul]
    omega
  have hBt : (B : Int) ∣ i - t := by
    rwa [heqdiff] at hdiff
  obtain ⟨u, hu⟩ := hBt
  have heqper : ((A * b.step : Nat) : Int) = (g : Int) * (A : Int) * (B : Int) := by
    have hbnat : b.step = g * B := (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
    have hnat : A * b.step = g * A * B := by
      rw [hbnat]
      ac_rfl
    exact_mod_cast hnat
  rw [heqper]
  have hi' : v - a.lo = (g : Int) * (A : Int) * i := by simpa [hasg] using hi
  have ht' : root - a.lo = (g : Int) * (A : Int) * t := by simpa [hasg] using ht
  have hstep : v - root = (g : Int) * (A : Int) * (i - t) := by
    have h1 : v - root = (v - a.lo) - (root - a.lo) := by omega
    rw [h1, hi', ht']
    rw [← Int.mul_sub]
  rw [hstep, hu]
  exact ⟨u, by ac_rfl⟩

private theorem intersection_mem_pos (a b : Progression) (ha : 0 < a.step) (hb : 0 < b.step)
    (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · simp [intersection, contains, han]
  · by_cases hbn : b.n = 0
    · simp [intersection, contains, hbn]
    · by_cases hgapmod : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0
      · let g := a.step.gcd b.step
        let A := a.step / g
        let B := b.step / g
        let gap := b.lo - a.lo
        let period := A * b.step
        let unit := (bezout A B).1
        let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % (B : Int))
        let low := max a.lo b.lo
        let first := low + (root - low) % (period : Int)
        let high := min (lastVal a) (lastVal b)
        have hp : (0 : Int) < period := by
          have hpN : 0 < period := by
            dsimp [period, A]
            exact Nat.mul_pos (Nat.div_gcd_pos_of_pos_left b.step ha) hb
          exact_mod_cast hpN
        have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
        have hrootdvd := root_dvd a.step b.step ha hb a.lo b.lo gap rfl hgap
        have hroot : (a.step : Int) ∣ root - a.lo ∧ (b.step : Int) ∣ root - b.lo := by
          simpa [g, A, B, root, gap, unit, period] using hrootdvd
        have hfirst : first = low + (root - low) % (period : Int) := rfl
        have hfroot := first_root_dvd low root first period hp hfirst
        have hpa : (a.step : Int) ∣ (period : Int) := by
          refine ⟨(B : Int), ?_⟩
          have hasg : (a.step : Int) = (g : Int) * (A : Int) := by
            exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left a.step b.step)).symm
          have hgB : (b.step : Int) = (g : Int) * (B : Int) := by
            exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right a.step b.step)).symm
          have hper : (period : Int) = (g : Int) * (A : Int) * (B : Int) := by
            dsimp [period]
            rw [hgB]
            ac_rfl
          rw [hper, hasg]
        have hpb : (b.step : Int) ∣ (period : Int) := by
          refine ⟨(A : Int), ?_⟩
          dsimp [period]
          ac_rfl
        unfold intersection
        simp [han, hbn, Nat.ne_of_gt ha, Nat.ne_of_gt hb, hgapmod]
        change contains ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ v = true ↔ _
        let n := if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0
        change contains ⟨first, period, n⟩ v = true ↔ _
        have hpeqInt : (((period : Int).toNat : Nat) : Int) = (period : Int) :=
          Int.toNat_of_nonneg (by omega : (0 : Int) ≤ period)
        have hpeq : ((period : Int).toNat) = period := by exact_mod_cast hpeqInt
        have hprog : (⟨first, (period : Int).toNat, n⟩ : Progression) = ⟨first, period, n⟩ := by
          rw [hpeq]
        rw [← hprog]
        apply Iff.trans (span_contains_iff first (period : Int) high v hp)
        constructor
        · rintro ⟨hfirstv, hvhigh, hxfirst⟩
          have hxroot : (period : Int) ∣ v - root := by
            have heq : v - root = (v - first) + (first - root) := by omega
            rw [heq]
            exact Int.dvd_add hxfirst hfroot
          have hxalo : (a.step : Int) ∣ v - a.lo := by
            rw [show v - a.lo = (v - root) + (root - a.lo) by omega]
            exact Int.dvd_add (Int.dvd_trans hpa hxroot) hroot.1
          have hxblo : (b.step : Int) ∣ v - b.lo := by
            rw [show v - b.lo = (v - root) + (root - b.lo) by omega]
            exact Int.dvd_add (Int.dvd_trans hpb hxroot) hroot.2
          have hremnon : 0 ≤ (root - low) % (period : Int) := Int.emod_nonneg _ (by omega : (period : Int) ≠ 0)
          have hlowle : low ≤ v := by
            dsimp [low] at hfirstv ⊢
            dsimp [first] at hfirstv
            omega
          have hA : contains a v = true := by
            apply (contains_iff a v).mpr
            refine ⟨?_, ?_, ?_, hxalo⟩
            · omega
            · dsimp [low] at hlowle
              omega
            · dsimp [high] at hvhigh
              omega
          have hB : contains b v = true := by
            apply (contains_iff b v).mpr
            refine ⟨?_, ?_, ?_, hxblo⟩
            · omega
            · dsimp [low] at hlowle
              omega
            · dsimp [high] at hvhigh
              omega
          exact ⟨hA, hB⟩
        · rintro ⟨ha', hb'⟩
          obtain ⟨_, halo, hahi, _⟩ := (contains_iff a v).mp ha'
          obtain ⟨_, hblo, hbhi, _⟩ := (contains_iff b v).mp hb'
          have hlowle : low ≤ v := by dsimp [low]; omega
          have hvhigh' : v ≤ high := by dsimp [high]; omega
          have hxroot : (period : Int) ∣ v - root := by
            simpa [g, A, B, gap, period, unit, root] using
              (period_dvd_of_both a b ha hb v ha' hb')
          have hxfirst : (period : Int) ∣ v - first := by
            have heq : v - first = (v - root) - (first - root) := by omega
            rw [heq]
            exact Int.dvd_sub hxroot hfroot
          exact ⟨first_le_of_congruence low root first v period hp hfirst hlowle hxroot,
            hvhigh', hxfirst⟩
      · unfold intersection
        simp [han, hbn, Nat.ne_of_gt ha, Nat.ne_of_gt hb, hgapmod]
        constructor
        · intro h
          simpa [contains] using h
        · rintro ⟨ha', hb'⟩
          exfalso
          exact hgapmod (Int.emod_eq_zero_of_dvd (common_gap_dvd a b v ha' hb'))

private theorem contains_const (a : Progression) (hs : a.step = 0) (v : Int) :
    contains a v = true ↔ 0 < a.n ∧ v = a.lo := by
  constructor
  · intro h
    obtain ⟨hn, hlo, hhi, hdvd⟩ := (contains_iff a v).mp h
    obtain ⟨k, hk⟩ := hdvd
    simp [hs] at hk
    constructor <;> omega
  · rintro ⟨hn, hv⟩
    subst v
    apply (contains_iff a a.lo).mpr
    refine ⟨hn, by omega, ?_, ?_⟩
    · unfold lastVal
      simp [hs]
    · exact ⟨0, by simp [hs]⟩

private theorem contains_unit (lo v : Int) : contains ⟨lo, 1, 1⟩ v = true ↔ v = lo := by
  unfold contains lastVal
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  constructor
  · intro h
    simp at h
    omega
  · intro hv
    subst v
    simp

private theorem intersection_mem (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases ha0 : a.step = 0
  · by_cases han : a.n = 0
    · simp [intersection, contains, han]
    · by_cases hbn : b.n = 0
      · simp [intersection, contains, hbn]
      · have hnpos : 0 < a.n := by omega
        by_cases hba : contains b a.lo = true
        · unfold intersection
          simp [han, hbn, ha0, hba]
          rw [contains_unit]
          rw [contains_const a ha0]
          constructor
          · intro hv
            subst v
            exact ⟨⟨hnpos, rfl⟩, hba⟩
          · rintro ⟨⟨_, hv⟩, _⟩
            subst v
            rfl
        · unfold intersection
          simp [han, hbn, ha0, hba]
          rw [contains_const a ha0]
          constructor
          · intro h
            unfold contains at h
            simp at h
          · rintro ⟨⟨_, hv⟩, hbv⟩
            subst v
            exact False.elim (hba hbv)
  · by_cases hb0 : b.step = 0
    · by_cases hbn : b.n = 0
      · simp [intersection, contains, hbn]
      · by_cases han : a.n = 0
        · simp [intersection, contains, han]
        · have hnpos : 0 < b.n := by omega
          by_cases hab : contains a b.lo = true
          · unfold intersection
            simp [han, hbn, ha0, hb0, hab]
            rw [contains_unit]
            rw [contains_const b hb0]
            constructor
            · intro hv
              subst v
              exact ⟨hab, ⟨hnpos, rfl⟩⟩
            · rintro ⟨_, ⟨_, hv⟩⟩
              subst v
              rfl
          · unfold intersection
            simp [han, hbn, ha0, hb0, hab]
            rw [contains_const b hb0]
            constructor
            · intro h
              unfold contains at h
              simp at h
            · rintro ⟨ha', ⟨_, hv⟩⟩
              subst v
              exact False.elim (hab ha')
    · exact intersection_mem_pos a b (Nat.pos_of_ne_zero ha0) (Nat.pos_of_ne_zero hb0) v

/-! ## Lane values and positions -/

end TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E6
