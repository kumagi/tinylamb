import TinylambOpt.ResearchGoals.Compression.Directories

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.C_Directories_B

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Directories

/-! Shared helpers for the Directories wire model. -/

private theorem pack_length (w : Nat) (xs : List Nat) :
    (pack w xs).length = byteCount (xs.length * w) := by
  simp [pack, little]

private theorem high_length (w : Nat) (xs : List Nat) :
    (high w xs).length = byteCount (highBits w xs) := by
  unfold high
  rw [pack_length]
  simp

private theorem ends_length (s : List Nat) : (ends s).length = s.length := by
  unfold ends
  rw [List.length_drop, List.length_scanl]
  omega

private theorem ends_getElem? (s : List Nat) (k : Nat) (hk : k < s.length) :
    (ends s)[k]? = some ((s.take (k + 1)).sum) := by
  have hk' : k + 1 ≤ s.length := hk
  have h1k : 1 + k = k + 1 := Nat.add_comm 1 k
  unfold ends
  rw [List.getElem?_drop, List.getElem?_scanl, h1k]
  simp [hk']
  rw [← List.sum_eq_foldl_nat]

private theorem ends_getElem?_none (s : List Nat) (k : Nat) (hk : s.length ≤ k) :
    (ends s)[k]? = none := by
  have hk' : ¬ k + 1 ≤ s.length := by omega
  have h1k : 1 + k = k + 1 := Nat.add_comm 1 k
  unfold ends
  rw [List.getElem?_drop, List.getElem?_scanl, h1k]
  simp [hk']

private theorem positions_length (w : Nat) (xs : List Nat) :
    (positions w xs).length = xs.length := by
  unfold positions
  simp [List.length_map, List.length_zipIdx]

private theorem positions_getElem? (w : Nat) (xs : List Nat) (k : Nat) :
    (positions w xs)[k]? = xs[k]?.map (fun x => x / 2 ^ w + k) := by
  unfold positions
  rw [List.getElem?_map, List.getElem?_zipIdx]
  rw [Option.map_map]
  congr 1
  funext x
  simp [Nat.zero_add]

private theorem byteCount_add_le (a b : Nat) : byteCount (a + b) ≤ byteCount a + byteCount b := by
  unfold byteCount
  have ha := Nat.mod_add_div (a + 7) 8
  have hb := Nat.mod_add_div (b + 7) 8
  have hab := Nat.mod_add_div (a + b + 7) 8
  have hama := Nat.mod_lt (a + 7) (by decide : 0 < 8)
  have hbmb := Nat.mod_lt (b + 7) (by decide : 0 < 8)
  have habmb := Nat.mod_lt (a + b + 7) (by decide : 0 < 8)
  omega

private theorem byteCount_add_ge (a b : Nat) : byteCount a + byteCount b ≤ byteCount (a + b) + 1 := by
  unfold byteCount
  have ha := Nat.mod_add_div (a + 7) 8
  have hb := Nat.mod_add_div (b + 7) 8
  have hab := Nat.mod_add_div (a + b + 7) 8
  have hama := Nat.mod_lt (a + 7) (by decide : 0 < 8)
  have hbmb := Nat.mod_lt (b + 7) (by decide : 0 < 8)
  have habmb := Nat.mod_lt (a + b + 7) (by decide : 0 < 8)
  omega

private theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

private theorem sum_le_length_mul {l : List Nat} {c : Nat} (h : ∀ x ∈ l, x ≤ c) :
    l.sum ≤ l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.sum_cons, List.length_cons, Nat.succ_mul]
      have hx := h x (by simp)
      have hxs : ∀ y ∈ xs, y ≤ c := fun y hy => h y (by simp [hy])
      have := ih hxs
      omega

private theorem scanl_eq_cons_tail {f : Nat → Nat → Nat} {b : Nat} {l : List Nat} :
    List.scanl f b l = b :: (List.scanl f b l).tail := by
  cases l with
  | nil => rfl
  | cons a l => rw [List.scanl_cons]; rfl

private theorem ends_cons (x : Nat) (xs : List Nat) :
    ends (x :: xs) = List.scanl (· + ·) x xs := by
  unfold ends
  rw [List.scanl_cons]
  simp

private theorem ends_replicate_zero (n : Nat) :
    ends (List.replicate n 0) = List.replicate n 0 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.replicate_succ, ends_cons]
      have hT : (List.scanl (· + ·) 0 (List.replicate n 0)).tail = List.replicate n 0 := by
        have h := ih
        unfold ends at h
        rwa [List.drop_one] at h
      have h1 := scanl_eq_cons_tail (f := (· + ·)) (b := 0) (l := List.replicate n 0)
      rw [h1, hT]

private theorem peak_replicate_zero (n : Nat) : peak (List.replicate n 0) = 0 := by
  unfold peak
  cases h : (List.replicate n 0).max? with
  | none => rfl
  | some m =>
      have hmem : m ∈ List.replicate n 0 := (List.max?_eq_some_iff.mp h).1
      have hm0 : m = 0 := by
        rw [List.mem_replicate] at hmem
        exact hmem.2
      exact hm0

private theorem zipIdx_take {α : Type} (l : List α) (i : Nat) : ∀ (n : Nat),
    (l.zipIdx i).take n = (l.take n).zipIdx i := by
  induction l generalizing i with
  | nil => intro n; cases n <;> rfl
  | cons x xs ih =>
      intro n
      cases n with
      | zero => rfl
      | succ n =>
          simp only [List.zipIdx_cons]
          rw [List.take_cons (Nat.succ_pos n)]
          simp only [Nat.succ_sub_one]
          exact congrArg (fun t => (x, i) :: t) (ih (i + 1) n)

private theorem positions_take (w : Nat) (xs : List Nat) (n : Nat) :
    positions w (xs.take n) = (positions w xs).take n := by
  unfold positions
  rw [← List.map_take]
  congr 1
  exact (zipIdx_take xs 0 n).symm

private theorem ends_append (a b : List Nat) :
    ends (a ++ b) = ends a ++ (List.scanl (· + ·) a.sum b).tail := by
  unfold ends
  rw [List.scanl_append]
  have hlen : 1 ≤ (List.scanl (· + ·) 0 a).length := by
    rw [List.length_scanl]
    omega
  rw [List.drop_append_of_le_length hlen]
  have hfold : List.foldl (· + ·) 0 a = a.sum := (List.sum_eq_foldl_nat (xs := a)).symm
  rw [hfold]

private theorem ends_take_append (a b : List Nat) :
    (ends (a ++ b)).take a.length = ends a := by
  rw [ends_append, ← ends_length a, List.take_append_length]

private theorem foldl_add_eq (c : Nat) (l : List Nat) :
    List.foldl (· + ·) c l = c + l.sum := by
  induction l generalizing c with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.foldl_cons, List.sum_cons, ih, Nat.add_assoc]

private theorem ends_append_getElem?_shift (a b : List Nat) (k : Nat) (hk : k < b.length) :
    (ends (a ++ b))[a.length + k]? = some (a.sum + (b.take (k + 1)).sum) := by
  rw [ends_append]
  have hle : (ends a).length ≤ a.length + k := by
    rw [ends_length a]
    omega
  rw [List.getElem?_append_right hle, ends_length a, Nat.add_sub_cancel_left]
  rw [List.getElem?_tail, List.getElem?_scanl]
  split
  · rw [foldl_add_eq]
  · exact absurd hk (by omega)

private theorem sum_take_add_drop (s : List Nat) (i : Nat) :
    (s.take i).sum + (s.drop i).sum = s.sum := by
  rw [← List.sum_append_nat, List.take_append_drop]

private theorem sum_take_le (s : List Nat) (i : Nat) :
    (s.take i).sum ≤ s.sum := by
  rw [← sum_take_add_drop s i]
  omega

private theorem peak_ends (s : List Nat) : peak (ends s) = s.sum := by
  unfold peak
  by_cases hs : s = []
  · subst hs
    simp [ends, List.scanl_nil]
  · have hlen : 0 < s.length := by
      cases s with
      | nil => exact absurd rfl hs
      | cons => simp
    have hmax : (ends s).max? = some s.sum := by
      rw [List.max?_eq_some_iff]
      constructor
      · have hidx : s.length - 1 < s.length := by omega
        have hgs := ends_getElem? s (s.length - 1) hidx
        rw [show s.length - 1 + 1 = s.length from by omega,
            List.take_of_length_le (Nat.le_refl s.length)] at hgs
        exact List.mem_of_getElem? hgs
      · intro y hy
        rw [List.mem_iff_getElem?] at hy
        obtain ⟨i, hi⟩ := hy
        have hi_lt : i < s.length := by
          have hlt := (List.getElem?_eq_some_iff.mp hi).1
          rwa [ends_length] at hlt
        have hi' := ends_getElem? s i hi_lt
        rw [hi'] at hi
        simp only [Option.some.injEq] at hi
        rw [← hi]
        exact sum_take_le s (i + 1)
    rw [hmax]
    rfl

private theorem directoryBytes_ends (w : Nat) (s : List Nat) :
    directoryBytes w (ends s) =
      byteCount (s.length + s.sum / 2 ^ w) + byteCount (s.length * w) := by
  unfold directoryBytes
  rw [high_length, pack_length]
  unfold highBits
  rw [ends_length s, peak_ends s]

/-! ## AppendPrefix -/

theorem appendPrefix : AppendPrefix.Goal := by
  intro w a b
  rw [← positions_take]
  congr 1
  exact ends_take_append a b

/-! ## AlignedShift -/

theorem alignedShift : AlignedShift.Goal := by
  intro w a b hdvd
  apply List.ext_getElem?
  intro k
  by_cases hk : k < b.length
  · rw [List.getElem?_drop, positions_getElem?, ends_append_getElem?_shift a b k hk]
    rw [List.getElem?_map, positions_getElem?, ends_getElem? b k hk]
    simp only [Option.map_some]
    congr 1
    have hdiv : (a.sum + (b.take (k + 1)).sum) / 2 ^ w =
        a.sum / 2 ^ w + (b.take (k + 1)).sum / 2 ^ w := by
      obtain ⟨q, hq⟩ := hdvd
      rw [hq, Nat.mul_add_div (Nat.two_pow_pos w) q _, Nat.mul_div_right q (Nat.two_pow_pos w)]
    rw [hdiv]
    omega
  · rw [List.getElem?_drop, positions_getElem?, ends_getElem?_none _ _ (by
        rw [List.length_append]; omega)]
    rw [List.getElem?_map, positions_getElem?, ends_getElem?_none b k (by omega)]
    simp

/-! ## ResetHighBase -/

theorem resetHighBase : ResetHighBase.Goal := by
  intro w a b
  simp only [directoryBytes_ends]
  rw [List.length_append, List.sum_append_nat]
  have hdivle : a.sum / 2 ^ w + b.sum / 2 ^ w ≤ (a.sum + b.sum) / 2 ^ w :=
    Nat.div_add_div_le_add_div
  have hbc_hi : byteCount (a.length + a.sum / 2 ^ w) +
        byteCount (b.length + b.sum / 2 ^ w) ≤
      byteCount ((a.length + b.length) + (a.sum + b.sum) / 2 ^ w) + 1 := by
    have h1 := byteCount_add_ge (a.length + a.sum / 2 ^ w) (b.length + b.sum / 2 ^ w)
    have hX : (a.length + a.sum / 2 ^ w) + (b.length + b.sum / 2 ^ w) ≤
        (a.length + b.length) + (a.sum + b.sum) / 2 ^ w := by omega
    have h2 := byteCount_mono hX
    omega
  have hbc_lo : byteCount (a.length * w) + byteCount (b.length * w) ≤
      byteCount ((a.length + b.length) * w) + 1 := by
    have h1 := byteCount_add_ge (a.length * w) (b.length * w)
    have hmul : (a.length + b.length) * w = a.length * w + b.length * w := by
      rw [Nat.add_mul]
    omega
  omega

/-! ## EmptyRun -/

theorem emptyRun : EmptyRun.Goal := by
  intro n
  unfold directoryBytes
  rw [ends_replicate_zero]
  have hp : (pack 0 (List.replicate n 0)).length = 0 := by
    rw [pack_length]
    unfold byteCount
    simp
  have hh : (high 0 (List.replicate n 0)).length = byteCount n := by
    rw [high_length]
    unfold highBits
    rw [peak_replicate_zero]
    simp
  rw [hh, hp]
  simp

end TinylambOpt.ResearchProofs.DevinBatch.C_Directories_B
