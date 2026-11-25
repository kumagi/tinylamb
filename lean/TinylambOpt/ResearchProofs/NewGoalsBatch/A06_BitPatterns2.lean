import TinylambOpt.ResearchGoals.Compression.BitPatterns

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A06

open TinylambOpt.ResearchGoals.Compression.BitPatterns
open TinylambOpt.ResearchGoals.Compression.Wire

/-!
Per-goal notes:

* `RawDictionary`: true.  Every element of `xs` survives in `xs.eraseDups`, and
  `idxOf` points at an equal element of that deduplicated list.
* `ZeroAlias`: false.  The list `[0, 2^63]` contains a negative zero bit image;
  rewriting it to zero changes the second lane.
* `ConstantPlanes` / `SparsePlanes`: true.  Both plane decoders reproduce the
  `p`-th bit of `xs[i]`, and a 64-bit value is determined by its 64 low bits.
* `ByteShuffle`: true.  Byte plane `j` is stored in block `j`, and eight little
  endian bytes reconstruct any value below `2^64 = 256^8`.
-/

private theorem foldl_congr {α β} (f g : β → α → β) (l : List α) (b : β)
    (h : ∀ (b' : β) (a : α), a ∈ l → f b' a = g b' a) : l.foldl f b = l.foldl g b := by
  induction l generalizing b with
  | nil => rfl
  | cons a l ih =>
      simp only [List.foldl_cons]
      rw [h b a (by simp)]
      apply ih
      intro b' a' ha
      exact h b' a' (by simp [ha])

private theorem rebuild_testbits (n k : Nat) (hn : n < 2^k) :
    (List.range k).foldl (fun acc p => if n.testBit p then acc + 2^p else acc) 0 = n := by
  induction k generalizing n with
  | zero =>
      have : n = 0 := by omega
      subst n
      simp
  | succ k ih =>
      have hlow : n % 2^k < 2^k := Nat.mod_lt _ (Nat.two_pow_pos k)
      have hlowfold := ih (n % 2^k) hlow
      have hbit_lower : ∀ p ∈ List.range k, n.testBit p = (n % 2^k).testBit p := by
        intro p hp
        have hp_lt : p < k := by simpa using hp
        have hm : (n % 2^k).testBit p = n.testBit p := by
          simpa [hp_lt] using (Nat.testBit_mod_two_pow n k p)
        exact hm.symm
      have hcongr :
          (List.range k).foldl (fun acc p => if n.testBit p then acc + 2^p else acc) 0 =
          (List.range k).foldl (fun acc p => if (n % 2^k).testBit p then acc + 2^p else acc) 0 := by
        apply foldl_congr
        intro b' p hp
        change (if n.testBit p then b' + 2^p else b') =
          (if (n % 2^k).testBit p then b' + 2^p else b')
        rw [hbit_lower p hp]
      have hfold_lower :
          (List.range k).foldl (fun acc p => if n.testBit p then acc + 2^p else acc) 0 =
            n % 2^k := by
        rw [hcongr]
        exact hlowfold
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [hfold_lower]
      rw [Nat.pow_succ] at hn
      have hq : n / 2^k < 2 := Nat.div_lt_of_lt_mul hn
      have hq_cases : n / 2^k = 0 ∨ n / 2^k = 1 :=
        Nat.le_one_iff_eq_zero_or_eq_one.mp (Nat.lt_succ_iff.mp (by simpa using hq))
      have hbit : n.testBit k = decide (n / 2^k % 2 = 1) :=
        Nat.testBit_eq_decide_div_mod_eq
      have hdecomp := Nat.mod_add_div n (2^k)
      rcases hq_cases with hq0 | hq1
      · have hbit0 : n.testBit k = false := by simp [hbit, hq0]
        simp [hbit0, hq0] at hdecomp ⊢
        exact hdecomp
      · have hbit1 : n.testBit k = true := by simp [hbit, hq1]
        simp [hbit1, hq1] at hdecomp ⊢
        exact hdecomp

private theorem rebuild_of_testBit (i v : Nat) (hv : v < 2^64) (test : Nat → Nat → Bool)
    (h : ∀ p, p < 64 → test p i = v.testBit p) :
    rebuild i test = v := by
  unfold rebuild
  have hcongr :
      (List.range 64).foldl (fun acc p => if test p i then acc + 2^p else acc) 0 =
      (List.range 64).foldl (fun acc p => if v.testBit p then acc + 2^p else acc) 0 := by
    apply foldl_congr
    intro b' p hp
    have hp_lt : p < 64 := by simpa using hp
    change (if test p i then b' + 2^p else b') = (if v.testBit p then b' + 2^p else b')
    rw [h p hp_lt]
  rw [hcongr]
  exact rebuild_testbits v 64 hv

private theorem fromLittle_append_singleton (l : List Nat) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256^l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256^xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m*256) = v % m + m * ((v/m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256*t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m*q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hmul_le : m*s ≤ m*255 := Nat.mul_le_mul_left m hs_le
  have hsum_lt : r + m*s < m*256 := by omega
  have hrewrite : v = (r + m*s) + (m*256)*t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m*256) = r + m*s := by
    calc
      v % (m*256) = ((r+m*s) + (m*256)*t) % (m*256) := by rw [hrewrite]
      _ = (r+m*s) % (m*256) := by rw [Nat.add_mul_mod_self_left]
      _ = r+m*s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v/m)%256) = r + m*s := by rfl
  exact hleft.trans hright.symm

private theorem fromLittle_little (v k : Nat) :
    fromLittle ((List.range k).map (fun j => (v/256^j)%256)) = v % 256^k := by
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 v (256^k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem zipIdx_positions_contains (bs : List Bool) (mark : Bool) (i : Nat)
    (hi : i < bs.length) :
    (((bs.zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd).contains i = true ↔
      bs[i] = mark) := by
  constructor
  · intro hc
    have hmem : i ∈ ((bs.zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd) :=
      List.elem_iff.mp hc
    rw [List.mem_map] at hmem
    rcases hmem with ⟨pair, hpair, hsnd⟩
    rcases pair with ⟨b, j⟩
    simp only at hsnd
    have hf := List.mem_filter.mp hpair
    have hz := List.mem_zipIdx hf.1
    have hb_eq : b = bs[j] := by simpa using hz.2.2
    have hb_mark : b = mark := LawfulBEq.eq_of_beq hf.2
    subst j
    rw [← hb_eq]
    exact hb_mark
  · intro hbs
    have hzopt : (bs.zipIdx)[i]? = some (bs[i], i) := by
      rw [List.getElem?_zipIdx]
      simp [List.getElem?_eq_getElem hi]
    rw [List.getElem?_eq_some_iff] at hzopt
    rcases hzopt with ⟨hlt, hval⟩
    have hzmem : (bs[i], i) ∈ bs.zipIdx := List.mem_of_getElem hval
    have hfmem : (bs[i], i) ∈ bs.zipIdx.filter (fun (b, _) => b == mark) := by
      rw [List.mem_filter]
      constructor
      · exact hzmem
      · simp [hbs]
    have hmapped : i ∈ ((bs.zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd) :=
      List.mem_map.mpr ⟨(bs[i], i), hfmem, rfl⟩
    exact List.elem_iff.mpr hmapped

private theorem sparse_bit (bs : List Bool) (mark : Bool) (i : Nat) (hi : i < bs.length) :
    xor (!mark) (((bs.zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd).contains i) =
      bs[i] := by
  have hc := zipIdx_positions_contains bs mark i hi
  cases hm : mark <;> cases hb : bs[i] <;> simp [hm, hb] at hc ⊢ <;> assumption

private theorem flatMap_getElem (f : Nat → List Nat) (n : Nat) (js : List Nat)
    (hlen : ∀ j ∈ js, (f j).length = n) :
    ∀ (pos : Nat) (hpos : pos < js.length) (i : Nat) (hi : i < n),
      (js.flatMap f)[pos*n+i]? = (f (js[pos]))[i]? := by
  induction js with
  | nil =>
      intro pos hpos
      simp at hpos
  | cons j0 rest ih =>
      intro pos hpos i hi
      simp only [List.flatMap_cons]
      cases pos with
      | zero =>
          have hlen0 : (f j0).length = n := hlen j0 (by simp)
          rw [Nat.zero_mul, Nat.zero_add]
          rw [List.getElem?_append]
          simp [hlen0, hi]
      | succ pos' =>
          have hlen0 : (f j0).length = n := hlen j0 (by simp)
          have hrestlen : ∀ j ∈ rest, (f j).length = n := by
            intro j hj
            exact hlen j (by simp [hj])
          have hpos' : pos' < rest.length := by
            simp at hpos
            omega
          have hn_le : n ≤ (pos'+1)*n := by
            have := Nat.mul_le_mul_right n (by omega : 1 ≤ pos'+1)
            simpa using this
          have hnot : ¬ (pos'+1)*n+i < (f j0).length := by
            rw [hlen0]
            omega
          rw [List.getElem?_append]
          rw [if_neg hnot]
          have hsub : (pos'+1)*n+i - (f j0).length = pos'*n+i := by
            rw [hlen0, Nat.succ_mul]
            omega
          rw [hsub]
          rw [ih hrestlen pos' hpos' i hi]
          simp

theorem RawDictionary_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.RawDictionary.Goal := by
  intro xs
  let d := xs.eraseDups
  have hpoint : ∀ x ∈ xs, d[d.idxOf x]?.getD 0 = x := by
    intro x hx
    have hmem : x ∈ d := by simpa [d] using (List.mem_eraseDups.mpr hx)
    have hlt : d.idxOf x < d.length := List.idxOf_lt_length_of_mem hmem
    have hget : d[d.idxOf x]? = some x := by
      rw [List.getElem?_eq_getElem hlt]
      rw [List.getElem_idxOf hlt]
    simp [hget]
  simpa using
    (List.map_congr_left (l := xs) (f := fun x => d[d.idxOf x]?.getD 0) (g := id) hpoint)

theorem ZeroAlias_disproof :
    ¬ TinylambOpt.ResearchGoals.Compression.BitPatterns.ZeroAlias.Goal := by
  intro h
  have hw := h [0, 2^63]
  simp at hw

theorem ConstantPlanes_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.ConstantPlanes.Goal := by
  intro xs i hlt hi
  let v := xs[i]?.getD 0
  have hv_eq : v = xs[i] := by
    dsimp [v]
    rw [List.getElem?_eq_getElem hi]
    simp
  have hv : v < 2^64 := by
    rw [hv_eq]
    exact hlt (xs[i]) (List.getElem_mem hi)
  apply rebuild_of_testBit i v hv
  intro p hp64
  have hplane_len : (plane xs p).length = xs.length := by simp [plane]
  have hi_plane : i < (plane xs p).length := by simpa [hplane_len] using hi
  have hget : (plane xs p)[i]?.getD false = v.testBit p := by
    rw [hv_eq]
    simp [plane, List.getElem?_eq_getElem hi]
  change (if (plane xs p).all id then true else (plane xs p)[i]?.getD false) = v.testBit p
  by_cases hall : (plane xs p).all id = true
  · have hbmem : (plane xs p)[i] ∈ plane xs p := List.getElem_mem hi_plane
    have hbtrue : (plane xs p)[i] = true := (List.all_eq_true.mp hall) _ hbmem
    have hbg : (plane xs p)[i]?.getD false = true := by
      rw [List.getElem?_eq_getElem hi_plane]
      exact hbtrue
    rw [if_pos hall, ← hget, hbg]
  · rw [if_neg hall, hget]

theorem SparsePlanes_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.SparsePlanes.Goal := by
  intro xs i hlt hi
  let v := xs[i]?.getD 0
  have hv_eq : v = xs[i] := by
    dsimp [v]
    rw [List.getElem?_eq_getElem hi]
    simp
  have hv : v < 2^64 := by
    rw [hv_eq]
    exact hlt (xs[i]) (List.getElem_mem hi)
  apply rebuild_of_testBit i v hv
  intro p hp64
  have hplane_len : (plane xs p).length = xs.length := by simp [plane]
  have hi_plane : i < (plane xs p).length := by simpa [hplane_len] using hi
  have hget : (plane xs p)[i]?.getD false = v.testBit p := by
    rw [hv_eq]
    simp [plane, List.getElem?_eq_getElem hi]
  let mark : Bool := decide ((plane xs p).count true ≤ (plane xs p).count false)
  change xor (!mark)
    ((((plane xs p).zipIdx.filter (fun (b, _) => b == mark)).map Prod.snd).contains i) =
      v.testBit p
  rw [sparse_bit (plane xs p) mark i hi_plane]
  rw [List.getElem?_eq_getElem hi_plane] at hget
  exact hget

theorem ByteShuffle_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.ByteShuffle.Goal := by
  intro xs i hi hlt
  let v := xs[i]?.getD 0
  have hv_eq : v = xs[i] := by
    dsimp [v]
    rw [List.getElem?_eq_getElem hi]
    simp
  have hv : v < 2^64 := by
    rw [hv_eq]
    exact hlt (xs[i]) (List.getElem_mem hi)
  have hbytes : ∀ j (hj : j < 8),
      (bytePlanes xs)[j*xs.length+i]?.getD 0 = (v/256^j)%256 := by
    intro j hj
    have hlen : ∀ k ∈ List.range 8,
        (xs.map (fun x => (x/256^k)%256)).length = xs.length := by
      intro k hk
      simp
    have hpos : j < (List.range 8).length := by simpa using hj
    have hidx := flatMap_getElem (fun k => xs.map (fun x => (x/256^k)%256)) xs.length
      (List.range 8) hlen j hpos i hi
    have hjval : (List.range 8)[j] = j := List.getElem_range hpos
    rw [hjval] at hidx
    unfold bytePlanes
    rw [hidx]
    simp [List.getElem?_eq_getElem hi, hv_eq]
  have hmap : ((List.range 8).map (fun j => (bytePlanes xs)[j*xs.length+i]?.getD 0)) =
      (List.range 8).map (fun j => (v/256^j)%256) := by
    apply List.map_congr_left
    intro j hj
    exact hbytes j (by simpa using hj)
  rw [hmap, fromLittle_little]
  have h256 : 256^8 = 2^64 := by decide
  rw [h256]
  rw [Nat.mod_eq_of_lt hv]
  rfl

end TinylambOpt.ResearchProofs.NewGoalsBatch.A06
