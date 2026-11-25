import TinylambOpt.ResearchGoals.Compression.BitPatterns

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.NewGoalsBatch.A06

open TinylambOpt.ResearchGoals.Compression.BitPatterns
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Per-goal notes:

* `RawDictionary`: `x ∈ xs` implies `x ∈ xs.eraseDups`, and `idxOf` on a member
  indexes back to `x`, so the dictionary lookup is the identity on the list.
* `ZeroAlias`: **false**.  The two-element list `[0, 2^63]` is mapped to
  `[0, 0]` because `2^63 == 2^63`, so the signed-zero alias loses bits.
* `ConstantPlanes`: when a plane is constant the conditional collapses to the
  row's own bit (a constant all-true plane has `true` at that row anyway), so the
  rebuilt word is exactly `xs[i]`; the key input is the bit expansion of a
  number below `2^64` from its test bits `0..63`.
* `SparsePlanes`: `mark` records the minority value, so the stored positions
  decode each bit via `xor (!mark) (b == mark) = b`; again only the bit
  expansion of `xs[i]` is needed.
* `ByteShuffle`: byte `j` of row `i` sits at flat index `j * length + i`; the
  little-endian reassembly is the base-256 expansion of `xs[i]`, which is
  `xs[i]` itself because `xs[i] < 2^64 = 256^8`.
-/

/-! ## RawDictionary -/

theorem RawDictionary_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.RawDictionary.Goal := by
  intro xs
  change xs.map (fun x => (xs.eraseDups)[(xs.eraseDups).idxOf x]?.getD 0) = xs
  have h : ∀ x ∈ xs, (xs.eraseDups)[(xs.eraseDups).idxOf x]?.getD 0 = x := by
    intro x hx
    have hmem : x ∈ xs.eraseDups := by simpa using hx
    have hidx : (xs.eraseDups).idxOf x < (xs.eraseDups).length :=
      List.idxOf_lt_length_of_mem hmem
    have hget : (xs.eraseDups)[(xs.eraseDups).idxOf x] = x := List.getElem_idxOf hidx
    rw [List.getElem?_eq_getElem hidx]
    simp [hget]
  calc xs.map (fun x => (xs.eraseDups)[(xs.eraseDups).idxOf x]?.getD 0)
      = xs.map id := List.map_congr_left h
    _ = xs := List.map_id xs

/-! ## ZeroAlias (false) -/

theorem ZeroAlias_disproof :
    ¬ TinylambOpt.ResearchGoals.Compression.BitPatterns.ZeroAlias.Goal := by
  intro h
  have hw := h [0, 2 ^ 63]
  exact absurd hw (by decide)

/-! ## Shared machinery: rebuilding a word from bit tests -/

private def rebuildAux (test : Nat → Nat → Bool) (n : Nat) : Nat → Nat
  | 0 => 0
  | k + 1 => rebuildAux test n k + (if test k n then 2 ^ k else 0)

private theorem foldl_range_rebuild (test : Nat → Nat → Bool) (n : Nat) :
    ∀ k, (List.range k).foldl (fun acc p => if test p n then acc + 2 ^ p else acc) 0
      = rebuildAux test n k
  | 0 => rfl
  | k + 1 => by
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [foldl_range_rebuild test n k]
      cases h : test k n <;> simp [rebuildAux, h]

private theorem rebuild_eq (test : Nat → Nat → Bool) (n : Nat) :
    rebuild n test = rebuildAux test n 64 := by
  unfold rebuild
  exact foldl_range_rebuild test n 64

private theorem rebuildAux_testBit (x n : Nat) :
    ∀ k, rebuildAux (fun p _ => x.testBit p) n k = x % 2 ^ k
  | 0 => by simp [rebuildAux]
  | k + 1 => by
      rw [rebuildAux, rebuildAux_testBit x n k, Nat.mod_pow_succ]
      have hbit : (if x.testBit k then 2 ^ k else 0) = 2 ^ k * (x / 2 ^ k % 2) := by
        rw [← Nat.toNat_testBit x k]
        cases x.testBit k <;> simp
      rw [hbit]

private theorem rebuild_testBits_eq (i x : Nat) (hx : x < 2 ^ 64) :
    rebuild i (fun p _ => x.testBit p) = x := by
  rw [rebuild_eq, rebuildAux_testBit x i 64, Nat.mod_eq_of_lt hx]

private theorem rebuild_congr (n : Nat) (t₁ t₂ : Nat → Nat → Bool)
    (h : ∀ p, t₁ p n = t₂ p n) : rebuild n t₁ = rebuild n t₂ := by
  unfold rebuild
  have hf : (fun acc p => if t₁ p n then acc + 2 ^ p else acc)
      = (fun acc p => if t₂ p n then acc + 2 ^ p else acc) := by
    funext acc p
    rw [h p]
  rw [hf]

/-! ## ConstantPlanes -/

theorem ConstantPlanes_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.ConstantPlanes.Goal := by
  intro xs i hlt hi
  change rebuild i (fun p row =>
      if (plane xs p).all id then true else (plane xs p)[row]?.getD false) = xs[i]?.getD 0
  have hxget : xs[i]? = some (xs[i]'hi) := List.getElem?_eq_getElem hi
  have hxlt : xs[i]'hi < 2 ^ 64 := hlt _ (List.getElem_mem hi)
  have hf : ∀ p, (if (plane xs p).all id then true else (plane xs p)[i]?.getD false)
      = (xs[i]'hi).testBit p := by
    intro p
    have hlen : (plane xs p).length = xs.length := by simp [plane]
    have hi' : i < (plane xs p).length := by rw [hlen]; exact hi
    have hrow : (plane xs p)[i]? = some ((xs[i]'hi).testBit p) := by
      show (xs.map (fun y => y.testBit p))[i]? = some ((xs[i]'hi).testBit p)
      rw [List.getElem?_map, hxget]
      rfl
    rw [hrow]
    cases hall : (plane xs p).all id
    · rfl
    · have htrue : (plane xs p)[i]'hi' = true := by
        have := List.all_eq_true.1 hall _ (List.getElem_mem hi')
        simpa using this
      have hsome := hrow
      rw [List.getElem?_eq_getElem hi'] at hsome
      have heq : (plane xs p)[i]'hi' = (xs[i]'hi).testBit p := Option.some.inj hsome
      rw [htrue] at heq
      exact heq
  have hcongr : rebuild i (fun p row =>
      if (plane xs p).all id then true else (plane xs p)[row]?.getD false)
      = rebuild i (fun p _ => (xs[i]'hi).testBit p) := rebuild_congr i _ _ hf
  rw [hcongr, rebuild_testBits_eq i (xs[i]'hi) hxlt, hxget]
  rfl

/-! ## SparsePlanes -/

private theorem xor_not_eq (b m : Bool) : xor (!m) (b == m) = b := by
  cases b <;> cases m <;> rfl

private theorem contains_filtered_zipIdx (bs : List Bool) (i : Nat) (hi : i < bs.length)
    (m : Bool) :
    ((bs.zipIdx.filter (fun (v, _) => v == m)).map Prod.snd).contains i = (bs[i] == m) := by
  have hiff : i ∈ ((bs.zipIdx.filter (fun (v, _) => v == m)).map Prod.snd) ↔ bs[i] = m := by
    rw [List.mem_map]
    constructor
    · rintro ⟨p, hp, hpi⟩
      rw [List.mem_filter] at hp
      obtain ⟨hpzip, hbeq⟩ := hp
      obtain ⟨v, k⟩ := p
      simp only at hpi hbeq hpzip
      subst hpi
      have hget : bs[i]? = some v := List.mk_mem_zipIdx_iff_getElem?.1 hpzip
      have hv : v = m := by simpa using hbeq
      rw [List.getElem?_eq_getElem hi] at hget
      have hb : bs[i] = v := (Option.some.inj hget).symm
      rw [hb, hv]
    · intro h
      refine ⟨(bs[i], i), ?_, rfl⟩
      rw [List.mem_filter]
      refine ⟨?_, by simp [h]⟩
      rw [List.mk_mem_zipIdx_iff_getElem?, List.getElem?_eq_getElem hi]
      rfl
  rw [List.contains_eq_mem]
  have h1 : decide (i ∈ ((bs.zipIdx.filter (fun (v, _) => v == m)).map Prod.snd))
      = decide (bs[i] = m) := decide_eq_decide.2 hiff
  have h2 : (bs[i] == m) = decide (bs[i] = m) := by
    cases bs[i] <;> cases m <;> rfl
  rw [h1, h2]

private theorem sparse_bit (bs : List Bool) (i : Nat) (hi : i < bs.length) :
    xor (!(decide (bs.count true ≤ bs.count false)))
        (((bs.zipIdx.filter (fun (v, _) => v == decide (bs.count true ≤ bs.count false))).map
          Prod.snd).contains i)
      = bs[i] := by
  have hc := contains_filtered_zipIdx bs i hi (decide (bs.count true ≤ bs.count false))
  rw [hc]
  exact xor_not_eq (bs[i]) (decide (bs.count true ≤ bs.count false))

theorem SparsePlanes_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.SparsePlanes.Goal := by
  intro xs i hlt hi
  change rebuild i (fun p row =>
      let mark := decide ((plane xs p).count true ≤ (plane xs p).count false)
      xor (!mark)
        (((plane xs p).zipIdx.filter (fun (v, _) => v == mark)).map Prod.snd |>.contains row))
    = xs[i]?.getD 0
  have hxget : xs[i]? = some (xs[i]'hi) := List.getElem?_eq_getElem hi
  have hxlt : xs[i]'hi < 2 ^ 64 := hlt _ (List.getElem_mem hi)
  have hf : ∀ p,
      (let mark := decide ((plane xs p).count true ≤ (plane xs p).count false)
       xor (!mark)
        (((plane xs p).zipIdx.filter (fun (v, _) => v == mark)).map Prod.snd |>.contains i))
      = (xs[i]'hi).testBit p := by
    intro p
    dsimp only
    have hlen : (plane xs p).length = xs.length := by simp [plane]
    have hi' : i < (plane xs p).length := by rw [hlen]; exact hi
    rw [sparse_bit (plane xs p) i hi']
    have hmap : (plane xs p)[i]? = some ((xs[i]'hi).testBit p) := by
      show (xs.map (fun y => y.testBit p))[i]? = some ((xs[i]'hi).testBit p)
      rw [List.getElem?_map, hxget]
      rfl
    rw [List.getElem?_eq_getElem hi'] at hmap
    exact Option.some.inj hmap
  have hcongr : rebuild i (fun p row =>
      let mark := decide ((plane xs p).count true ≤ (plane xs p).count false)
      xor (!mark)
        (((plane xs p).zipIdx.filter (fun (v, _) => v == mark)).map Prod.snd |>.contains row))
      = rebuild i (fun p _ => (xs[i]'hi).testBit p) := rebuild_congr i _ _ hf
  rw [hcongr, rebuild_testBits_eq i (xs[i]'hi) hxlt, hxget]
  rfl

/-! ## ByteShuffle -/

private theorem fromLittle_append : ∀ (a b : List Nat),
    fromLittle (a ++ b) = fromLittle a + 256 ^ a.length * fromLittle b
  | [], b => by simp [fromLittle]
  | x :: xs, b => by
      rw [List.cons_append, fromLittle, fromLittle_append xs b]
      rw [Nat.mul_add, ← Nat.add_assoc, ← Nat.mul_assoc, ← Nat.pow_succ']

private theorem fromLittle_digits (n : Nat) :
    ∀ k, fromLittle ((List.range k).map (fun j => (n / 256 ^ j) % 256)) = n % 256 ^ k
  | 0 => by simp [fromLittle]
  | k + 1 => by
      rw [List.range_succ, List.map_append, fromLittle_append, fromLittle_digits n k]
      simp only [List.length_map, List.length_range, fromLittle, Nat.mul_zero, Nat.add_zero]
      rw [Nat.mod_pow_succ]

private theorem length_flatMap_range (f : Nat → List α) (n m : Nat)
    (hlen : ∀ j < n, (f j).length = m) :
    ((List.range n).flatMap f).length = n * m := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.flatMap_append, List.length_append,
        ih (fun j hj => hlen j (Nat.lt_succ_of_lt hj)), hlen n (Nat.lt_succ_self n)]
      omega

private theorem getElem?_flatMap_range (f : Nat → List α) (n m : Nat)
    (hlen : ∀ j < n, (f j).length = m) :
    ∀ j i, j < n → i < m → ((List.range n).flatMap f)[j * m + i]? = (f j)[i]? := by
  induction n with
  | zero => intro j i hj; omega
  | succ n ih =>
      intro j i hj hi
      rw [List.range_succ, List.flatMap_append]
      have hlen' : ∀ k < n, (f k).length = m := fun k hk => hlen k (Nat.lt_succ_of_lt hk)
      have hlenlen : ((List.range n).flatMap f).length = n * m :=
        length_flatMap_range f n m hlen'
      rcases Nat.lt_or_eq_of_le (Nat.le_of_lt_succ hj) with hjn | hje
      · have hidx : j * m + i < ((List.range n).flatMap f).length := by
          rw [hlenlen]
          exact Nat.add_lt_add_left hi (j * m)
        rw [List.getElem?_append_left hidx, ih j i hjn hi]
      · subst hje
        have hge : ((List.range n).flatMap f).length ≤ n * m + i := by
          rw [hlenlen]; exact Nat.le_add_right _ _
        rw [List.getElem?_append_right hge, Nat.add_sub_cancel_left]

theorem ByteShuffle_sound :
    TinylambOpt.ResearchGoals.Compression.BitPatterns.ByteShuffle.Goal := by
  intro xs i hi hlt
  have hxget : xs[i]? = some (xs[i]'hi) := List.getElem?_eq_getElem hi
  have hxlt : xs[i]'hi < 2 ^ 64 := hlt _ (List.getElem_mem hi)
  have hpt : ∀ j ∈ List.range 8,
      (bytePlanes xs)[j * xs.length + i]?.getD 0 = (xs[i]'hi / 256 ^ j) % 256 := by
    intro j hj
    have hj8 : j < 8 := List.mem_range.1 hj
    have hlen : ∀ k < 8, ((fun j => xs.map (fun y => (y / 256 ^ j) % 256)) k).length
        = xs.length := by
      intro k _; simp
    have h1 : ((List.range 8).flatMap (fun j => xs.map (fun y => (y / 256 ^ j) % 256)))
        [j * xs.length + i]?
        = (xs.map (fun y => (y / 256 ^ j) % 256))[i]? := by
      have := getElem?_flatMap_range (fun j => xs.map (fun y => (y / 256 ^ j) % 256))
        8 xs.length hlen j i hj8 hi
      simpa using this
    have h2 : (xs.map (fun y => (y / 256 ^ j) % 256))[i]?
        = some ((xs[i]'hi / 256 ^ j) % 256) := by
      rw [List.getElem?_map, hxget]
      rfl
    have hbyte : (bytePlanes xs)[j * xs.length + i]? = some ((xs[i]'hi / 256 ^ j) % 256) := by
      rw [bytePlanes, h1, h2]
    rw [hbyte]
    rfl
  calc fromLittle ((List.range 8).map (fun j => (bytePlanes xs)[j * xs.length + i]?.getD 0))
      = fromLittle ((List.range 8).map (fun j => (xs[i]'hi / 256 ^ j) % 256)) := by
        rw [List.map_congr_left hpt]
    _ = xs[i]'hi % 256 ^ 8 := fromLittle_digits (xs[i]'hi) 8
    _ = xs[i]'hi := by
        rw [show (256 : Nat) ^ 8 = 2 ^ 64 by norm_num, Nat.mod_eq_of_lt hxlt]
    _ = xs[i]?.getD 0 := by rw [hxget]; rfl

end TinylambOpt.ResearchProofs.NewGoalsBatch.A06
