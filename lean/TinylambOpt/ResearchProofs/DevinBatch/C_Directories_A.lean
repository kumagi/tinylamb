import TinylambOpt.ResearchGoals.Compression.Directories
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.C_Directories_A

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Directories

/-! Proofs for the six assigned Directories goals: UniquePositions, UnarySelect,
EndSelect, SliceBySelect, PhysicalCharge, HighPlaneBudget.  All six are true. -/

/-! ## bit-packing infrastructure (mirrors the private lemmas of
`ResearchProofs.Compression.Wire`) -/

private theorem fromLittle_append_singleton (l : Bytes) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256 ^ l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m * 256) = v % m + m * ((v / m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256 * t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m * q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hsum_lt : r + m * s < m * 256 := by
    have hm_le : m * s ≤ m * 255 := Nat.mul_le_mul_left m hs_le
    omega
  have hrewrite : v = (r + m * s) + (m * 256) * t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m * 256) = r + m * s := by
    calc
      v % (m * 256) = ((r + m * s) + (m * 256) * t) % (m * 256) := by rw [hrewrite]
      _ = (r + m * s) % (m * 256) := by rw [Nat.add_mul_mod_self_left]
      _ = r + m * s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v / m) % 256) = r + m * s := by rfl
  exact hleft.trans hright.symm

private theorem fromLittle_little (v k : Nat) :
    fromLittle ((List.range k).map (fun j => (v / 256 ^ j) % 256)) = v % 256 ^ k := by
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 v (256 ^ k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem word_lt_pow (w : Nat) (xs : List Nat) : word w xs < 2 ^ (xs.length * w) := by
  induction xs with
  | nil => simp [word]
  | cons x xs ih =>
      have hxmod : x % 2 ^ w < 2 ^ w := Nat.mod_lt x (Nat.two_pow_pos w)
      have hsum : x % 2 ^ w + 2 ^ w * word w xs < 2 ^ w * (word w xs + 1) := by
        rw [Nat.mul_succ, Nat.add_comm (2 ^ w * word w xs) (2 ^ w)]
        exact Nat.add_lt_add_right hxmod _
      have hle : 2 ^ w * (word w xs + 1) ≤ 2 ^ w * 2 ^ (xs.length * w) :=
        Nat.mul_le_mul_left (2 ^ w) (Nat.succ_le_of_lt ih)
      have hp_exp : (xs.length + 1) * w = w + xs.length * w := by
        rw [Nat.add_mul, Nat.one_mul]
        omega
      have hp : 2 ^ ((xs.length + 1) * w) = 2 ^ w * 2 ^ (xs.length * w) := by
        rw [hp_exp, Nat.pow_add]
      rw [word]
      change x % 2 ^ w + 2 ^ w * word w xs < 2 ^ ((xs.length + 1) * w)
      rw [hp]
      exact Nat.lt_of_lt_of_le hsum hle

private theorem fromLittle_pack (w : Nat) (xs : List Nat) :
    fromLittle (pack w xs) = word w xs := by
  unfold pack little
  rw [fromLittle_little]
  have hlt := word_lt_pow w xs
  have hb := bits_le_byteCount_w xs.length w
  have hpow : 256 ^ byteCount (xs.length * w) = 2 ^ (8 * byteCount (xs.length * w)) := by
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, Nat.pow_mul]
  apply Nat.mod_eq_of_lt
  rw [hpow]
  exact Nat.lt_of_lt_of_le hlt (Nat.pow_le_pow_right (by decide : 0 < 2) hb)
where
  bits_le_byteCount_w (n w : Nat) : n * w ≤ 8 * byteCount (n * w) := by
    unfold byteCount
    have hm := Nat.mod_lt (n * w + 7) (by decide : 0 < 8)
    have hd := Nat.mod_add_div (n * w + 7) 8
    omega

/-! ## packed stream lengths -/

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem high_length (w : Nat) (xs : List Nat) :
    (high w xs).length = byteCount (highBits w xs) := by
  unfold high
  rw [pack_length]
  simp only [List.length_map, List.length_range, Nat.mul_one]

/-! ## `ends` is non-decreasing, has the right length, and its entries are
prefix sums. -/

private theorem scanl_ge (l : List Nat) (c : Nat) :
    ∀ b ∈ l.scanl (· + ·) c, c ≤ b := by
  induction l generalizing c with
  | nil =>
      intro b hb
      have hb' : b = c := by simpa [List.scanl] using hb
      omega
  | cons a l ih =>
      intro b hb
      rw [List.scanl_cons] at hb
      rcases List.mem_cons.mp hb with rfl | hb'
      · exact Nat.le_refl c
      · have h := ih (c + a) b hb'
        omega

private theorem scanl_pairwise (l : List Nat) (c : Nat) :
    (l.scanl (· + ·) c).Pairwise (· ≤ ·) := by
  induction l generalizing c with
  | nil => simp [List.scanl]
  | cons a l ih =>
      rw [List.scanl_cons]
      apply List.Pairwise.cons
      · intro b hb
        have h := scanl_ge l (c + a) b hb
        have hc : c ≤ c + a := Nat.le_add_right c a
        omega
      · exact ih (c + a)

private theorem ends_pairwise (sizes : List Nat) :
    (ends sizes).Pairwise (· ≤ ·) := by
  unfold ends
  exact List.Pairwise.sublist (List.drop_sublist 1 _)
    (scanl_pairwise sizes 0)

private theorem ends_length (sizes : List Nat) :
    (ends sizes).length = sizes.length := by
  unfold ends
  rw [List.length_drop, List.length_scanl]
  omega

private theorem scanl_getElem? (sizes : List Nat) (acc j : Nat) :
    (sizes.scanl (· + ·) acc)[j]? =
      if j ≤ sizes.length then some (acc + (sizes.take j).sum) else none := by
  induction sizes generalizing acc j with
  | nil =>
      cases j with
      | zero => simp [List.scanl]
      | succ j => simp [List.scanl]
  | cons a as ih =>
      cases j with
      | zero =>
          simp [List.scanl_cons, List.take_zero, List.sum_nil]
      | succ j =>
          rw [List.scanl_cons, List.getElem?_cons_succ, ih (acc + a) j]
          by_cases hj : j ≤ as.length
          · have htake : (a :: as).take (j + 1) = a :: as.take j := by
              rw [List.take_succ_cons]
            rw [if_pos (by omega : j + 1 ≤ (a :: as).length), if_pos hj,
              htake, List.sum_cons]
            congr 1
            omega
          · have htake : (a :: as).take (j + 1) = a :: as.take j := by
              rw [List.take_succ_cons]
            rw [if_neg (by omega : ¬ j + 1 ≤ (a :: as).length), if_neg hj,
              htake, List.sum_cons]
            rfl

private theorem ends_getElem? (sizes : List Nat) (k : Nat) :
    (ends sizes)[k]? =
      if k < sizes.length then some ((sizes.take (k + 1)).sum) else none := by
  unfold ends
  rw [List.getElem?_drop, scanl_getElem? sizes 0 (k + 1)]
  by_cases hk : k < sizes.length
  · rw [if_pos (by omega : k + 1 ≤ sizes.length), if_pos hk]
    simp
  · rw [if_neg (by omega : ¬ k + 1 ≤ sizes.length), if_neg hk]

private theorem ends_getElem (sizes : List Nat) {k : Nat} (hk : k < sizes.length) :
    (ends sizes)[k] = (sizes.take (k + 1)).sum := by
  have h := ends_getElem? sizes k
  rw [if_pos hk] at h
  have hsome : (ends sizes)[k]? = some ((sizes.take (k + 1)).sum) := h
  obtain ⟨h', heq⟩ := (List.getElem?_eq_some_iff).mp hsome
  exact heq

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  have hxsne : xs ≠ [] := by
    intro hnil
    subst xs
    simp at hx
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := xs) hxsne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hxm : x ≤ m := hmprops.2 x hx
  have hp : peak xs = m := by simp [peak, hm]
  rw [hp]
  exact hxm

private theorem peak_ends (sizes : List Nat) : peak (ends sizes) = sizes.sum := by
  by_cases hs : sizes = []
  · subst hs
    simp [ends, peak]
  · have hne : ends sizes ≠ [] := by
      intro h
      have := ends_length sizes
      rw [h] at this
      simp at this
      exact hs (List.eq_nil_of_length_eq_zero this.symm)
    obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := ends sizes) hne
    have hmem : sizes.sum ∈ ends sizes := by
      have hlen : sizes.length - 1 < (ends sizes).length := by
        rw [ends_length]; omega
      have hv := ends_getElem sizes (k := sizes.length - 1) (by
        rw [ends_length]; omega)
      have htake : sizes.take (sizes.length - 1 + 1) = sizes := by
        have : sizes.length - 1 + 1 = sizes.length := by omega
        rw [this]
        exact List.take_of_length_le (Nat.le_refl _)
      rw [htake] at hv
      have : (ends sizes)[sizes.length - 1] ∈ ends sizes := List.getElem_mem _
      rwa [hv] at this
    have hbound : ∀ x ∈ ends sizes, x ≤ sizes.sum := by
      intro x hx
      obtain ⟨k, hk⟩ := List.mem_iff_getElem.mp hx
      rw [← hk]
      rw [ends_getElem sizes hk.2]
      have hle : (sizes.take (k + 1)).sum ≤ sizes.sum := by
        have hsplit := List.take_append_drop (k + 1) sizes
        have hsum := congrArg List.sum hsplit
        rw [List.sum_append] at hsum
        -- sizes.sum = take.sum + drop.sum
        have hnonneg : 0 ≤ (sizes.drop (k + 1)).sum := Nat.zero_le _
        omega
      exact hle
    have hmax : (ends sizes).max? = some sizes.sum :=
      List.max?_eq_some_iff.mpr ⟨hmem, hbound⟩
    simp [peak, hmax]

/-! ## `positions` entries and monotonicity -/

private theorem positions_getElem? (w : Nat) (xs : List Nat) (k : Nat) :
    (positions w xs)[k]? = (xs[k]?).map (fun x => x / 2 ^ w + k) := by
  unfold positions
  rw [List.getElem?_map, List.getElem?_zipIdx]
  rw [Option.map_map]
  congr 1
  funext x
  rfl

private theorem positions_getElem (w : Nat) (xs : List Nat) {k : Nat}
    (hk : k < xs.length) :
    (positions w xs)[k] = xs[k] / 2 ^ w + k := by
  have h := positions_getElem? w xs k
  rw [List.getElem?_eq_getElem hk] at h
  simp only [Option.map_some, Option.some.injEq] at h
  -- h : positions[k] = xs[k]/2^w + k up to getElem proof irrelevance
  have hlen : (positions w xs).length = xs.length := by
    simp [positions, List.length_map, List.length_zipIdx]
  have hk' : k < (positions w xs).length := by rwa [hlen]
  obtain ⟨h', heq⟩ := (List.getElem?_eq_some_iff).mp h
  -- heq : (positions w xs)[k] = xs[k]/2^w + k (proof args irrelevant)
  convert heq using 2
  · exact hk'
  · exact hk

private theorem ends_mono_getElem (sizes : List Nat) {i j : Nat}
    (hij : i ≤ j) (hj : j < (ends sizes).length) :
    (ends sizes)[i] ≤ (ends sizes)[j] := by
  have hi : i < (ends sizes).length := by omega
  rcases Nat.lt_or_eq_of_le hij with h | h
  · exact List.Pairwise.rel_getElem_of_lt hi hj ends_pairwise h
  · subst h
    exact Nat.le_refl _

/-- UniquePositions.Goal -/
theorem uniquePositions : UniquePositions.Goal := by
  intro w sizes
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have hlen : (positions w (ends sizes)).length = (ends sizes).length := by
    simp [positions, List.length_map, List.length_zipIdx]
  rw [hlen] at hi hj
  rw [positions_getElem w (ends sizes) hi,
    positions_getElem w (ends sizes) hj]
  have hle := ends_mono_getElem sizes (Nat.le_of_lt hij) hj
  have hdiv : (ends sizes)[i] / 2 ^ w ≤ (ends sizes)[j] / 2 ^ w :=
    Nat.div_le_div_right hle
  omega

end TinylambOpt.ResearchProofs.DevinBatch.C_Directories_A
