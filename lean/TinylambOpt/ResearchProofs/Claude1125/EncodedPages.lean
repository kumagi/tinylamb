import TinylambOpt.ResearchGoals.Atlas.EncodedPages
/-!
Atlas.EncodedPages の `PagedTombstones.SkipEpochs` と `GrayTile.PowerOfTwo` の証明（2026-11-25）。
-/
namespace TinylambOpt.ResearchProofs.Claude1125.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages

theorem count_tiles (w : Nat) (hw : 0 < w) (n : Nat) :
    ((List.range n).filter (fun i => (i / w) % 2 == 0)).length
      = (n/(2*w))*w + min w (n%(2*w)) := by
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.range_succ, List.filter_append, List.length_append, ih]
    have h2 : 0 < 2*w := by omega
    obtain ⟨q, r, hr, rfl⟩ : ∃ q r, r < 2*w ∧ n = 2*w*q + r :=
      ⟨n/(2*w), n%(2*w), Nat.mod_lt _ h2, (Nat.div_add_mod n (2*w)).symm⟩
    have e1 : (2*w*q + r)/(2*w) = q := by
      rw [Nat.mul_add_div h2]; simp [Nat.div_eq_of_lt hr]
    have e2 : (2*w*q + r)%(2*w) = r := by
      rw [Nat.mul_add_mod]; exact Nat.mod_eq_of_lt hr
    have e3 : (2*w*q + r)/w = 2*q + r/w := by
      have : 2*w*q + r = w*(2*q) + r := by rw [Nat.mul_assoc, Nat.mul_left_comm]
      rw [this, Nat.mul_add_div hw]
    rw [e1, e2]; simp only [List.filter_cons, List.filter_nil, e3]
    by_cases hlt : r < w
    · have : r / w = 0 := Nat.div_eq_of_lt hlt
      rw [this]
      have hq : (2*q+0)%2 = 0 := by omega
      by_cases hr1 : r + 1 < 2*w
      · have a1 : (2*w*q + r + 1)/(2*w) = q := by
          have : 2*w*q + r + 1 = 2*w*q + (r+1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_div h2]; simp [Nat.div_eq_of_lt hr1]
        have a2 : (2*w*q + r + 1)%(2*w) = r+1 := by
          have : 2*w*q + r + 1 = 2*w*q + (r+1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_mod]; exact Nat.mod_eq_of_lt hr1
        simp [a1, a2, hq]; omega
      · omega
    · have h1 : r / w = 1 := by
        have : r / w < 2 := (Nat.div_lt_iff_lt_mul hw).2 (by omega)
        have : 1 ≤ r / w := (Nat.le_div_iff_mul_le hw).2 (by omega)
        omega
      rw [h1]
      have hq : (2*q+1)%2 = 1 := by omega
      by_cases hr1 : r + 1 < 2*w
      · have a1 : (2*w*q + r + 1)/(2*w) = q := by
          have : 2*w*q + r + 1 = 2*w*q + (r+1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_div h2]; simp [Nat.div_eq_of_lt hr1]
        have a2 : (2*w*q + r + 1)%(2*w) = r+1 := by
          have : 2*w*q + r + 1 = 2*w*q + (r+1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_mod]; exact Nat.mod_eq_of_lt hr1
        simp [a1, a2, hq]; omega
      · have hr' : r + 1 = 2*w := by omega
        have a1 : (2*w*q + r + 1)/(2*w) = q + 1 := by
          have : 2*w*q + r + 1 = 2*w*(q+1) := by rw [Nat.mul_add]; omega
          rw [this, Nat.mul_div_cancel_left _ h2]
        have a2 : (2*w*q + r + 1)%(2*w) = 0 := by
          have : 2*w*q + r + 1 = 2*w*(q+1) := by rw [Nat.mul_add]; omega
          rw [this]; exact Nat.mul_mod_right _ _
        simp [a1, a2, hq]
        rw [Nat.add_mul, show min w r = w from by omega]; omega

theorem rows_len_tiles (c : Config) (n : Nat) :
    ((List.range n).filterMap (fun i => (PagedTombstones.layout.read c i).map (i,·))).length
    = ((List.range n).filter (fun i => (i / width c) % 2 == 0)).length := by
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.range_succ, List.filterMap_append, List.filter_append, List.length_append,
      List.length_append, ih]
    by_cases h : (n / width c) % 2 = 0 <;> simp [PagedTombstones.layout, h]

theorem sound_PagedTombstones_SkipEpochs : PagedTombstones.SkipEpochs.Goal := by
  intro c
  have hw : 0 < width c := by unfold width; omega
  have := count_tiles (width c) hw c.n
  unfold rows
  rw [rows_len_tiles, this]

/-! Gray code -/
def gray (r : Nat) : Nat := r ^^^ (r/2)

theorem xor_div2 (a b : Nat) : (a ^^^ b)/2 = a/2 ^^^ b/2 := by
  apply Nat.eq_of_testBit_eq; intro i
  simp [Nat.testBit_xor, Nat.testBit_div_two]

theorem gray_xor (a b : Nat) : gray (a ^^^ b) = gray a ^^^ gray b := by
  unfold gray; rw [xor_div2]
  apply Nat.eq_of_testBit_eq; intro i
  simp only [Nat.testBit_xor]
  cases a.testBit i <;> cases b.testBit i <;> cases (a/2).testBit i <;> cases (b/2).testBit i <;> rfl

theorem gray_inj {a b : Nat} (h : gray a = gray b) : a = b := by
  have h0 : gray (a ^^^ b) = 0 := by rw [gray_xor, h]; simp
  have hd : (a ^^^ b) = (a ^^^ b)/2 := by
    unfold gray at h0
    have := congrArg (fun x => x ^^^ ((a ^^^ b)/2)) h0
    simpa [Nat.xor_assoc] using this
  have hz : (a ^^^ b) = 0 := by omega
  have := congrArg (fun x => x ^^^ b) hz
  simpa [Nat.xor_assoc] using this

theorem gray_lt {d r : Nat} (h : r < 2^d) : gray r < 2^d :=
  Nat.xor_lt_two_pow h (by omega)

def ginv (r : Nat) : Nat → Nat
  | 0 => 0
  | m+1 => ginv r m ^^^ (r >>> m)

theorem gray_ginv (r m : Nat) : gray (ginv r m) = r ^^^ (r >>> m) := by
  induction m with
  | zero => simp [ginv, gray]
  | succ m ih =>
    simp only [ginv]
    rw [gray_xor, ih]
    have h1 : (r >>> m) / 2 = r >>> (m+1) := by
      rw [Nat.shiftRight_eq_div_pow, Nat.shiftRight_eq_div_pow, Nat.div_div_eq_div_mul, Nat.pow_succ]
    unfold gray
    rw [h1]
    apply Nat.eq_of_testBit_eq; intro i
    simp only [Nat.testBit_xor]
    cases r.testBit i <;> cases (r >>> m).testBit i <;> cases (r >>> (m+1)).testBit i <;> rfl

theorem ginv_lt {d r : Nat} (h : r < 2^d) (m : Nat) : ginv r m < 2^d := by
  induction m with
  | zero => simp [ginv]; exact Nat.two_pow_pos d
  | succ m ih =>
    simp only [ginv]
    apply Nat.xor_lt_two_pow ih
    rw [Nat.shiftRight_eq_div_pow]
    exact Nat.lt_of_le_of_lt (Nat.div_le_self _ _) h

theorem gray_surj {d a : Nat} (h : a < 2^d) : ∃ b, b < 2^d ∧ gray b = a := by
  refine ⟨ginv a d, ginv_lt h d, ?_⟩
  rw [gray_ginv, Nat.shiftRight_eq_div_pow, Nat.div_eq_of_lt h]
  simp

theorem gray_perm (d : Nat) : ((List.range (2^d)).map gray).Perm (List.range (2^d)) := by
  have hnd : ((List.range (2^d)).map gray).Nodup :=
    List.Pairwise.map gray (fun a b hab h => hab (gray_inj h)) List.nodup_range
  refine (List.perm_ext_iff_of_nodup hnd List.nodup_range).2 ?_
  intro a
  constructor
  · intro hx
    obtain ⟨r, hr, rfl⟩ := List.mem_map.1 hx
    exact List.mem_range.2 (gray_lt (List.mem_range.1 hr))
  · intro ha
    obtain ⟨b, hb, rfl⟩ := gray_surj (List.mem_range.1 ha)
    exact List.mem_map.2 ⟨b, List.mem_range.2 hb, rfl⟩

theorem rows_snd_some (lay : Layout) (c : Config) (g : Nat → Int)
    (h : ∀ i, lay.read c i = some (g i)) :
    (rows lay c).map Prod.snd = (List.range c.n).map g := by
  unfold rows
  have : ∀ l : List Nat, (l.filterMap (fun i => (lay.read c i).map (i,·))).map Prod.snd = l.map g := by
    intro l; induction l with
    | nil => simp
    | cons a t ih => simp [List.filterMap_cons, h, ih]
  exact this _

theorem blocks_perm (w : Nat) (F G : Nat → Int)
    (hblk : ∀ m, ((List.range w).map (fun r => F (m*w+r))).Perm ((List.range w).map (fun r => G (m*w+r))))
    (m : Nat) : ((List.range (m*w)).map F).Perm ((List.range (m*w)).map G) := by
  induction m with
  | zero => simp
  | succ m ih =>
    rw [Nat.succ_mul, List.range_add, List.map_append, List.map_append]
    refine ih.append ?_
    simpa [List.map_map, Function.comp_def, Nat.add_comm] using hblk m

theorem sound_GrayTile_PowerOfTwo : GrayTile.PowerOfTwo.Goal := by
  intro c d hw hn
  have hwpos : 0 < width c := by rw [hw]; exact Nat.two_pow_pos d
  rw [rows_snd_some GrayTile.layout c
        (fun i => c.base + c.step * ((i / width c * width c + (i % width c ^^^ i % width c / 2) : Nat) : Int))
        (fun i => rfl),
      rows_snd_some LinearOrdinal.layout c (fun i => c.base + c.step * (i : Int)) (fun i => rfl)]
  have hnm : c.n = (c.n / width c) * width c := (Nat.div_mul_cancel (Nat.dvd_of_mod_eq_zero hn)).symm
  rw [hnm]
  refine blocks_perm (width c) _ _ ?_ _
  intro m
  have e1 : ∀ r ∈ List.range (width c),
      (c.base + c.step * ((((m * width c + r) / width c * width c
          + ((m * width c + r) % width c ^^^ (m * width c + r) % width c / 2) : Nat)) : Int))
        = c.base + c.step * ((m * width c + gray r : Nat) : Int) := by
    intro r hr
    have hr' := List.mem_range.1 hr
    have a1 : (m * width c + r) / width c = m := by
      rw [Nat.mul_comm, Nat.mul_add_div hwpos, Nat.div_eq_of_lt hr']; simp
    have a2 : (m * width c + r) % width c = r := by
      rw [Nat.mul_comm, Nat.mul_add_mod]; exact Nat.mod_eq_of_lt hr'
    rw [a1, a2]; rfl
  rw [List.map_congr_left e1]
  have : ((List.range (width c)).map (fun r => c.base + c.step * ((m * width c + gray r : Nat) : Int)))
      = ((List.range (2^d)).map gray).map (fun x => c.base + c.step * ((m * width c + x : Nat) : Int)) := by
    rw [List.map_map, hw]; rfl
  rw [this]
  have h2 := (gray_perm d).map (fun x => c.base + c.step * ((m * width c + x : Nat) : Int))
  rw [hw] at h2 ⊢
  simpa [List.map_map, Function.comp_def] using h2

end TinylambOpt.ResearchProofs.Claude1125.EncodedPages
