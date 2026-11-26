import TinylambOpt.ResearchGoals.Compression.Container

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.

Proved (16 claims, including the following)

- `AdjacentRequest` — `∀ k n p a b, 0 < k → ((rangeBlocks k n p a ++ rangeBlocks k n
  (p+a) b).eraseDups) = rangeBlocks k n p (a+b)`
- `AnchoredDirectory` — `∀ (bs : List Block) a i, a ≤ i → i ≤ bs.length → offset bs i =
  offset bs a + payload ((bs.drop a).take (i-a))`
- `AppendAtBoundary` — `∀ c k a b, 0 < k → a.length%k = 0 → (blocks c k (a++b)).map
  Block.body = (blocks c k a ++ blocks c k b).map Block.body`

Refuted by counterexample

- `FinerAlwaysWins` — `∀ c s k, 0 < k → onDisk (blocks c k s) ≤ onDisk (blocks c (2*k)
  s)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.Container

open TinylambOpt.ResearchGoals.Compression.Container
open TinylambOpt.ResearchGoals.Compression.Wire

/-! # Soundness/disproof of the Container research goals

Sixteen goals are TRUE and `FinerAlwaysWins` is FALSE (splitting a 2-byte
payload into two frames raises the on-disk cost from 51 B to 68 B with the
identity codec).  The proofs use only core Lean and `Std`.
-/


/-! ## foundational helpers and first goals -/

private theorem sum_map_congr {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ a ∈ l, f a = g a) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun b hb => h b (by simp [hb]))]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map f).sum + (l.map g).sum = (l.map (fun a => f a + g a)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [show (f a + (List.map f as).sum) + (g a + (List.map g as).sum) =
          (f a + g a) + ((List.map f as).sum + (List.map g as).sum) by omega]
      rw [ih]

private theorem sum_map_le_sum_map {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ a ∈ l, f a ≤ g a) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have hi := ih (fun b hb => h b (by simp [hb]))
      omega

/-! ## chunk helpers -/

private theorem take_mul_eq (s : Bytes) (m k : Nat) :
    s.take (m*k + k) = s.take (m*k) ++ (s.drop (m*k)).take k := by
  rw [← List.take_append_drop (m*k) (s.take (m*k+k))]
  congr 1
  · rw [List.take_take]
    congr 1
    omega
  · rw [List.drop_take]
    congr 1
    omega

private theorem chunk_prefix_flatten (k : Nat) (s : Bytes) (m : Nat) :
    ((List.range m).map (fun i => (s.drop (i*k)).take k)).flatten = s.take (m*k) := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.map_cons, List.flatten_append, ih]
      simp
      rw [show (m+1)*k = m*k+k by simp [Nat.add_mul]]
      exact (take_mul_eq s m k).symm

private theorem chunks_flatten (hk : 0 < k) (s : Bytes) : (chunks k s).flatten = s := by
  unfold chunks
  rw [chunk_prefix_flatten]
  have hle : s.length ≤ ((s.length+k-1)/k)*k := by
    have h2 := Nat.div_add_mod (s.length+k-1) k
    have hr := Nat.mod_lt (s.length+k-1) hk
    have hc : k * ((s.length+k-1)/k) = ((s.length+k-1)/k)*k := Nat.mul_comm _ _
    omega
  calc
    s.take (((s.length+k-1)/k)*k) =
        s.take (((s.length+k-1)/k)*k) ++ s.drop (((s.length+k-1)/k)*k) := by
          rw [List.drop_eq_nil_of_le hle, List.append_nil]
    _ = s := List.take_append_drop _ s

private theorem chunks_length_sum (hk : 0 < k) (s : Bytes) :
    ((chunks k s).map List.length).sum = s.length := by
  have h := congrArg List.length (chunks_flatten hk s)
  simpa [List.length_flatten, List.length_map] using h

private theorem choose_body_length_le (c : Codec) (b : Bytes) :
    (choose c b).body.length ≤ b.length := by
  unfold choose
  dsimp
  split
  · rename_i h
    simp
    omega
  · simp

private theorem chunk_length_le (_hk : 0 < k) {s : Bytes} {x : Bytes}
    (hx : x ∈ chunks k s) : x.length ≤ k := by
  unfold chunks at hx
  rw [List.mem_map] at hx
  obtain ⟨i, hi, rfl⟩ := hx
  simpa [List.length_take] using (Nat.min_le_left k (s.drop (i*k)).length)

private theorem unpack_choose (c : Codec) (b : Bytes)
    (h : c.decompress (c.compress b) = some b) : unpack c (choose c b) = some b := by
  unfold choose unpack
  dsimp
  split
  · simp [h]
  · rfl

private theorem blocks_unpack (_hk : 0 < k) (c : Codec) (s : Bytes)
    (h : ∀ x ∈ chunks k s, c.decompress (c.compress x) = some x) :
    (blocks c k s).mapM (unpack c) = some (chunks k s) := by
  unfold blocks
  generalize hc : chunks k s = xs
  have h' : ∀ x ∈ xs, c.decompress (c.compress x) = some x := by
    intro x hx
    exact h x (by simpa [hc] using hx)
  clear h hc
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp [unpack_choose c x (h' x (by simp)),
        ih (fun y hy => h' y (by simp [hy]))]

private theorem gain_body_sum (c : Codec) (b : Bytes) :
    (choose c b).body.length + (b.length - (c.compress b).length) = b.length := by
  unfold choose
  dsimp
  split
  · rename_i h
    simp
    omega
  · rename_i h
    simp
    omega

private theorem payload_blocks (c : Codec) (k : Nat) (s : Bytes) :
    payload (blocks c k s) =
      ((chunks k s).map (fun b => (choose c b).body.length)).sum := by
  unfold payload blocks
  simp [List.map_map, Function.comp_def]

private theorem gains_payload (hk : 0 < k) (c : Codec) (s : Bytes) :
    let gains := ((chunks k s).map (fun b => b.length-(c.compress b).length)).sum
    payload (blocks c k s) + gains = s.length := by
  dsimp only
  rw [payload_blocks]
  rw [sum_map_add]
  rw [sum_map_congr (chunks k s)
    (fun b => (choose c b).body.length + (b.length - (c.compress b).length))
    List.length (fun b _ => gain_body_sum c b)]
  exact chunks_length_sum hk s

/-! ## VerifiedContainer -/


private theorem chunks_length (k : Nat) (s : Bytes) :
    (chunks k s).length = (s.length+k-1)/k := by unfold chunks; simp

private theorem chunks_getElem (_hk : 0 < k) (s : Bytes) (i : Nat)
    (hi : i < (chunks k s).length) :
    (chunks k s)[i]?.getD [] = (s.drop (i*k)).take k := by
  unfold chunks
  rw [List.getElem?_map]
  have hirange : i < (List.range ((s.length+k-1)/k)).length := by
    rw [List.length_range]
    simpa [chunks_length] using hi
  have hget : (List.range ((s.length+k-1)/k))[i]? = some i := by
    rw [List.getElem?_eq_some_iff]
    exact ⟨hirange, by simp⟩
  rw [hget]
  simp

private theorem blocks_getElem (c : Codec) (k : Nat) (s : Bytes) (i : Nat) :
    (blocks c k s)[i]?.map Block.body =
      (chunks k s)[i]?.map (fun x => (choose c x).body) := by
  unfold blocks
  rw [List.getElem?_map]
  cases (chunks k s)[i]? <;> rfl

theorem sound_VerifiedContainer : VerifiedContainer.Goal := by
  intro c k s hk h
  rw [blocks_unpack hk c s h]
  simp [chunks_flatten hk s]

/-! ## RawEnvelope -/

theorem sound_RawEnvelope : RawEnvelope.Goal := by
  intro c k s hk
  unfold onDisk
  rw [payload_blocks]
  rw [← chunks_length k s]
  have hlen : (blocks c k s).length = (chunks k s).length := by
    unfold blocks
    simp
  rw [hlen]
  have hle : ((chunks k s).map (fun b => (choose c b).body.length)).sum ≤
      ((chunks k s).map List.length).sum :=
    sum_map_le_sum_map (chunks k s) (fun b => (choose c b).body.length)
      List.length (fun b _ => choose_body_length_le c b)
  rw [chunks_length_sum hk s] at hle
  omega

/-! ## BeatTarget -/

theorem sound_BeatTarget : BeatTarget.Goal := by
  intro c k s extra target hk
  dsimp only
  let gains := ((chunks k s).map (fun b => b.length-(c.compress b).length)).sum
  change onDisk (blocks c k s) + extra < target ↔
    s.length + 32 + 17*(chunks k s).length + extra < target + gains
  have hp : payload (blocks c k s) + gains = s.length := by
    dsimp [gains]
    exact gains_payload hk c s
  rw [payload_blocks] at hp
  have hlen : (blocks c k s).length = (chunks k s).length := by
    unfold blocks
    simp
  have hB : (onDisk (blocks c k s) + extra) + gains =
      s.length + 32 + 17*(chunks k s).length + extra := by
    unfold onDisk
    rw [payload_blocks, hlen]
    omega
  constructor
  · intro hlt
    have hs : (onDisk (blocks c k s) + extra) + gains < target + gains :=
      Nat.add_lt_add_right hlt gains
    rwa [hB] at hs
  · intro hlt
    have hs : (onDisk (blocks c k s) + extra) + gains < target + gains := by
      rwa [hB]
    exact Nat.add_lt_add_iff_right.mp hs

/-! ## PerChunkChoice -/

theorem sound_PerChunkChoice : PerChunkChoice.Goal := by
  intro cs sizes c hc
  dsimp only
  let best := fun (b : Bytes) =>
    (b.length::cs.map (fun f => (f.compress b).length)).min?.getD b.length
  change (sizes.map best).sum ≤ (sizes.map (fun b => (choose c b).body.length)).sum
  have hbest_len : ∀ b : Bytes, best b ≤ b.length := by
    intro b
    dsimp [best]
    exact List.min?_getD_le_of_mem (by simp)
  have hbest_clen : ∀ b : Bytes, best b ≤ (c.compress b).length := by
    intro b
    dsimp [best]
    have hmemc : (c.compress b).length ∈ cs.map (fun f => (f.compress b).length) := by
      rw [List.mem_map]
      exact ⟨c, hc, rfl⟩
    exact List.min?_getD_le_of_mem (by simp [hmemc])
  have hstep : ∀ b ∈ sizes, best b ≤ (choose c b).body.length := by
    intro b _
    unfold choose
    dsimp
    split
    · rename_i h
      exact hbest_clen b
    · rename_i h
      exact hbest_len b
  exact sum_map_le_sum_map sizes best (fun b => (choose c b).body.length) hstep

/-! ## PointFanout -/

theorem sound_PointFanout : PointFanout.Goal := by
  intro k n i hk
  unfold rangeBlocks
  by_cases h : i < n
  · have hstop : min n (i + 1) = i + 1 := by omega
    simp [h, hstop]
  · have hlt : ¬ i < min n (i + 1) := by omega
    simp [h, hlt]

/-! ## IndexBreakEven -/

theorem sound_IndexBreakEven : IndexBreakEven.Goal := by
  intro bs k _hk
  omega

/-! ## FinerAlwaysWins (false) -/

/-! Counterexample: the identity codec on the 2-byte payload `[3,4]` with
`k = 1` costs 68 B (two frames), while `k = 2` costs 51 B (one frame); hence
smaller frames do not always give a smaller file. -/
theorem refutation_FinerAlwaysWins : ¬ FinerAlwaysWins.Goal := by
  intro h
  let identity : Codec := ⟨id, some⟩
  have hh : onDisk (blocks identity 1 [3,4]) ≤ onDisk (blocks identity 2 [3,4]) := by
    simpa using h identity [3,4] 1 (by decide)
  revert hh
  decide



/-! ## range-block membership, read amplification and batched reads -/


/-! ## range-block membership, read amplification and batched reads -/

private theorem mem_rangeBlocks_iff (hk : 0 < k) {n pos len i : Nat} :
    i ∈ rangeBlocks k n pos len ↔
      ∃ x < n, pos ≤ x ∧ x < pos + len ∧ x / k = i := by
  unfold rangeBlocks
  dsimp only
  let stop := min n (pos+len)
  change (i ∈ if pos < stop then
      (List.range ((stop-1)/k - pos/k + 1)).map (pos/k+·) else []) ↔
    ∃ x < n, pos ≤ x ∧ x < pos + len ∧ x / k = i
  by_cases hps : pos < stop
  · rw [ite_eq_left hps, List.mem_map]
    have hle : pos ≤ stop - 1 := by omega
    constructor
    · rintro ⟨t, ht, rfl⟩
      have hrange : t < (stop-1)/k - pos/k + 1 := by simpa using ht
      let i0 := pos/k
      by_cases ht0 : t = 0
      · subst t
        refine ⟨pos, ?_, ?_, ?_, ?_⟩
        · exact Nat.lt_of_lt_of_le hps (Nat.min_le_left _ _)
        · omega
        · omega
        · simp
      · have htpos : 0 < t := Nat.pos_of_ne_zero ht0
        let x := (i0+t)*k
        have hle_j : i0 + t ≤ (stop-1)/k := by omega
        have hxle : x ≤ stop - 1 := (Nat.le_div_iff_mul_le hk).mp hle_j
        have hxpos_lt : pos < (i0+1)*k := by
          simpa [i0, Nat.mul_comm] using Nat.lt_mul_div_succ pos hk
        refine ⟨x, ?_, ?_, ?_, ?_⟩
        · omega
        · have : (i0+1)*k ≤ x := by dsimp [x]; exact Nat.mul_le_mul_right k (by omega)
          omega
        · omega
        · simpa [x, Nat.mul_comm] using Nat.mul_div_right (i0+t) hk
    · rintro ⟨x, hxn, hpx, hxlen, hxdiv⟩
      have hxstop : x < stop := by
        rw [show stop = min n (pos+len) by rfl]
        exact Nat.lt_min.mpr ⟨hxn, hxlen⟩
      have hxstop_le : x ≤ stop - 1 := by omega
      have hi0le : pos/k ≤ x/k := Nat.div_le_div_right hpx
      have hile : x/k ≤ (stop-1)/k := Nat.div_le_div_right hxstop_le
      subst i
      refine ⟨x/k - pos/k, ?_, ?_⟩
      · rw [List.mem_range]
        omega
      · omega
  · rw [ite_eq_right hps]
    constructor
    · intro h; simp at h
    · rintro ⟨x, hxn, hpx, hxlen, hxdiv⟩
      have hxstop : x < stop := by
        rw [show stop = min n (pos+len) by rfl]
        exact Nat.lt_min.mpr ⟨hxn, hxlen⟩
      omega

theorem sound_ReadAmplification : ReadAmplification.Goal := by
  intro k n pos len hk
  unfold rangeBlocks
  dsimp only
  let stop := min n (pos+len)
  change ((if pos < stop then
      (List.range ((stop-1)/k - pos/k + 1)).map (pos/k+·) else []) : List Nat).length*k ≤
      len + 2*(k-1)
  by_cases hps : pos < stop
  · rw [ite_eq_left hps]
    simp only [List.length_map, List.length_range]
    let i := pos/k
    let j := (stop-1)/k
    have hij : i ≤ j := by apply Nat.div_le_div_right; omega
    have hjle : j*k ≤ stop-1 := Nat.div_mul_le_self _ _
    have hj1 : (j+1)*k = j*k+k := by rw [Nat.add_mul, Nat.one_mul]
    have hcomm : k*i = i*k := Nat.mul_comm _ _
    have hposdec : pos = i*k + pos%k := by
      have := Nat.div_add_mod pos k
      dsimp [i] at hcomm ⊢
      omega
    have hr : pos%k < k := Nat.mod_lt pos hk
    have hstop : stop ≤ pos+len := Nat.min_le_right _ _
    have hji : j-i+1+i = j+1 := by omega
    have hcount_eq : (j-i+1)*k + i*k = (j+1)*k := by
      rw [← Nat.add_mul, hji]
    have h1 : (j-i+1)*k + i*k ≤ stop - 1 + k := by omega
    have h2 : stop - 1 + k ≤ pos + len + k - 1 := by omega
    have hd : (j-i+1)*k = ((stop-1)/k - pos/k + 1)*k := by rfl
    rw [hd] at h1
    omega
  · rw [ite_eq_right hps]
    simp

theorem sound_BatchedReads : BatchedReads.Goal := by
  intro k n req i hk
  rw [List.mem_eraseDups]
  rw [List.mem_flatMap]
  constructor
  · rintro ⟨p, hp, hi⟩
    rw [mem_rangeBlocks_iff hk] at hi
    obtain ⟨x, hxn, hpx, hxlen, hxdiv⟩ := hi
    exact ⟨p, hp, x, hxn, hpx, hxlen, hxdiv⟩
  · rintro ⟨p, hp, x, hxn, hpx, hxlen, hxdiv⟩
    refine ⟨p, hp, ?_⟩
    rw [mem_rangeBlocks_iff hk]
    exact ⟨x, hxn, hpx, hxlen, hxdiv⟩


/-! ## adjacent requests -/

private theorem rangeBlocks_pairwise_lt (_hk : 0 < k) (n pos len : Nat) :
    (rangeBlocks k n pos len).Pairwise (fun a b : Nat => a < b) := by
  unfold rangeBlocks
  dsimp only
  by_cases h : pos < min n (pos+len)
  · rw [ite_eq_left h]
    exact List.pairwise_lt_range.map (fun t => pos/k + t) (by intro a b hab; omega)
  · rw [ite_eq_right h]
    simp

private theorem pairwise_le_of_pairwise_lt {l : List Nat}
    (h : l.Pairwise (fun a b : Nat => a < b)) : l.Pairwise (fun a b : Nat => a ≤ b) :=
  h.imp (fun hab => Nat.le_of_lt hab)

private theorem eraseDups_eq_self_of_pairwise_lt {l : List Nat}
    (h : l.Pairwise (fun a b : Nat => a < b)) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.eraseDups_cons]
      have has := (List.pairwise_cons.mp h).2
      have hf : as.filter (fun b : Nat => !(b == a)) = as := by
        rw [List.filter_eq_self]
        intro b hb
        have hab : a < b := List.rel_of_pairwise_cons h hb
        simp [show b ≠ a by omega]
      rw [hf, ih has]

private theorem pairwise_lt_removeAll (l ys : List Nat)
    (h : l.Pairwise (fun a b : Nat => a < b)) :
    (l.removeAll ys).Pairwise (fun a b : Nat => a < b) := by
  induction ys generalizing l with
  | nil => simpa using h
  | cons y ys ih =>
      rw [List.removeAll_cons]
      exact ih (l.filter (fun x => !x == y)) (h.filter _)

theorem sound_AdjacentRequest : AdjacentRequest.Goal := by
  intro k n p a b hk
  let r1 := rangeBlocks k n p a
  let r2 := rangeBlocks k n (p+a) b
  have hp1lt := rangeBlocks_pairwise_lt hk n p a
  have hp2lt := rangeBlocks_pairwise_lt hk n (p+a) b
  have hcross : ∀ x ∈ r1, ∀ y ∈ r2, x ≤ y := by
    intro x hx y hy
    rw [mem_rangeBlocks_iff hk] at hx hy
    obtain ⟨u, hun, hpu, hua, hux⟩ := hx
    obtain ⟨v, hvn, hpav, hvb, hvy⟩ := hy
    subst x; subst y
    have huv : u ≤ v := by omega
    exact Nat.div_le_div_right huv
  have hE : (r1++r2).eraseDups = r1 ++ (r2.removeAll r1) := by
    rw [List.eraseDups_append]
    rw [eraseDups_eq_self_of_pairwise_lt hp1lt]
    rw [eraseDups_eq_self_of_pairwise_lt (pairwise_lt_removeAll r2 r1 hp2lt)]
  have hcross2 : ∀ x ∈ r1, ∀ y ∈ r2.removeAll r1, x < y := by
    intro x hx y hy
    simp [List.removeAll] at hy
    have hy2 : y ∈ r2 := hy.1
    have hne : y ∉ r1 := hy.2
    have hle := hcross x hx y hy2
    have : x ≠ y := by intro hxy; subst y; exact hne hx
    omega
  have happ_lt : (r1 ++ r2.removeAll r1).Pairwise (fun x y : Nat => x < y) := by
    rw [List.pairwise_append]
    exact ⟨hp1lt, pairwise_lt_removeAll r2 r1 hp2lt, hcross2⟩
  have hpRlt := rangeBlocks_pairwise_lt hk n p (a+b)
  have hperm : List.Perm (r1 ++ r2.removeAll r1) (rangeBlocks k n p (a+b)) := by
    apply (List.perm_ext_iff_of_nodup
      (happ_lt.imp (fun hab => by omega))
      (hpRlt.imp (fun hab => by omega))).mpr
    intro x
    simp only [List.mem_append]
    change (x ∈ r1 ∨ x ∈ r2.removeAll r1) ↔ x ∈ rangeBlocks k n p (a+b)
    have hxr2 : x ∈ r2.removeAll r1 ↔ x ∈ r2 ∧ x ∉ r1 := by
      simp [List.removeAll]
    rw [hxr2]
    rw [mem_rangeBlocks_iff (hk) (n:=n) (pos:=p+a) (len:=b) (i:=x)]
    rw [mem_rangeBlocks_iff (hk) (n:=n) (pos:=p) (len:=a+b) (i:=x)]
    constructor
    · rintro (hx1 | ⟨⟨y, hyn, hpy, hyb, hyd⟩, hnot⟩)
      · rw [mem_rangeBlocks_iff (hk) (n:=n) (pos:=p) (len:=a) (i:=x)] at hx1
        obtain ⟨z, hzn, hpz, hzla, hzdiv⟩ := hx1
        exact ⟨z, hzn, hpz, by omega, hzdiv⟩
      · exact ⟨y, hyn, by omega, by omega, hyd⟩
    · rintro ⟨y, hyn, hpy, hyab, hyd⟩
      by_cases hya : y < p+a
      · exact Or.inl (by
          rw [mem_rangeBlocks_iff (hk) (n:=n) (pos:=p) (len:=a) (i:=x)]
          exact ⟨y, hyn, hpy, hya, hyd⟩)
      · have hge : p+a ≤ y := by omega
        by_cases hxr1 : x ∈ r1
        · exact Or.inl hxr1
        · exact Or.inr ⟨⟨y, hyn, hge, by omega, hyd⟩, hxr1⟩
  rw [hE]
  exact List.Perm.eq_of_pairwise (fun x y _ _ hxy hyx => by omega)
    (pairwise_le_of_pairwise_lt happ_lt) (pairwise_le_of_pairwise_lt hpRlt) hperm


/-! ## reuse of unchanged frames -/

private theorem chunk_eq_of_pointwise (k i : Nat) {s t : Bytes}
    (h : ∀ j < k, s[i*k+j]? = t[i*k+j]?) :
    (s.drop (i*k)).take k = (t.drop (i*k)).take k := by
  apply List.ext_getElem?
  intro j
  by_cases hj : j < k
  · simp [List.getElem?_drop, hj, h j hj]
  · simp [List.getElem?_take, hj]

theorem sound_ReuseClean : ReuseClean.Goal := by
  intro c k s t i hk hlen hsame
  have hlen' : (chunks k s).length = (chunks k t).length := by
    rw [chunks_length, chunks_length, hlen]
  have hchunk : (chunks k s)[i]? = (chunks k t)[i]? := by
    cases hs : (chunks k s)[i]? with
    | none =>
        have hle : (chunks k s).length ≤ i := List.getElem?_eq_none_iff.mp hs
        have hle' : (chunks k t).length ≤ i := by omega
        have ht : (chunks k t)[i]? = none := List.getElem?_eq_none_iff.mpr hle'
        simp [ht]
    | some x =>
        obtain ⟨his, _⟩ := List.getElem?_eq_some_iff.mp hs
        have hit : i < (chunks k t).length := by omega
        have hx := chunks_getElem hk s i his
        have hy := chunks_getElem hk t i hit
        have hx' : x = (s.drop (i*k)).take k := by simpa [hs] using hx
        have hxt : (chunks k t)[i]?.getD [] = x := by
          rw [hy]
          rw [← chunk_eq_of_pointwise k i hsame]
          exact hx'.symm
        cases ht : (chunks k t)[i]? with
        | none =>
            have hne : (chunks k t)[i]? ≠ none := by
              intro hnone
              have hle : (chunks k t).length ≤ i := List.getElem?_eq_none_iff.mp hnone
              omega
            exact (hne ht).elim
        | some y =>
            have hxy : y = x := by simpa [ht] using hxt
            simp [hxy]
  rw [show ((blocks c k s)[i]?.map Block.body) =
      ((chunks k s)[i]?.map (fun x => (choose c x).body)) from blocks_getElem c k s i]
  rw [show ((blocks c k t)[i]?.map Block.body) =
      ((chunks k t)[i]?.map (fun x => (choose c x).body)) from blocks_getElem c k t i]
  rw [hchunk]


/-! ## body locator, relocation and anchored directory -/

private theorem bodyLocator_aux : ∀ (bs : List Block) (i : Nat), i < bs.length →
    ((bs.flatMap Block.body).drop (payload (bs.take i))).take ((bs[i]?.map (·.body.length)).getD 0) =
      (bs[i]?.map Block.body).getD [] := by
  intro bs
  induction bs with
  | nil => intro i hi; simp at hi
  | cons b bs ih =>
      intro i hi
      cases i with
      | zero =>
          change (b.body ++ bs.flatMap Block.body).take b.body.length = b.body
          rw [List.take_append_of_le_length (Nat.le_refl _)]
          simp
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          have ih' := ih i hi'
          have ih'' :
              List.take ((Option.map (fun x => List.length x.body) bs[i]?).getD 0)
                (List.drop (List.map (fun x => List.length x.body) (List.take i bs)).sum
                  (List.flatMap Block.body bs)) =
                (Option.map Block.body bs[i]?).getD [] := by
            simpa [payload] using ih'
          change ((b.body ++ bs.flatMap Block.body).drop (payload (b :: bs.take i))).take
            ((bs[i]?.map (·.body.length)).getD 0) = (bs[i]?.map Block.body).getD []
          dsimp [payload]
          simp only [List.map_cons, List.sum_cons]
          rw [← List.drop_drop]
          rw [List.drop_append_of_le_length (Nat.le_refl _)]
          simpa using ih''

theorem sound_BodyLocator : BodyLocator.Goal := by
  intro bs i hi
  have hoff : offset bs i - 32 = payload (bs.take i) := by
    unfold offset
    omega
  rw [hoff]
  exact bodyLocator_aux bs i hi

private theorem relocation_aux : ∀ (bs : List Block) (i j : Nat) (b : Block),
    i < j → j ≤ bs.length →
    (offset (bs.set i b) j : Int) - offset bs j =
      (b.body.length : Int) - ((bs[i]?.map (·.body.length)).getD 0) := by
  intro bs
  induction bs with
  | nil =>
      intro i j b hij hj
      simp at hj
      omega
  | cons b0 bs ih =>
      intro i j b hij hj
      cases j with
      | zero => omega
      | succ j =>
          cases i with
          | zero =>
              unfold offset
              simp [payload]
              omega
          | succ i =>
              have hij' : i < j := by omega
              have hj' : j ≤ bs.length := by simpa using hj
              have ih' := ih i j b hij' hj'
              unfold offset at ih' ⊢
              simp [payload] at ih' ⊢
              omega

theorem sound_Relocation : Relocation.Goal := by
  intro bs i j b hij hj
  exact relocation_aux bs i j b hij hj

private theorem take_append_take_drop (bs : List Block) (a i : Nat)
    (ha : a ≤ bs.length) (hai : a ≤ i) :
    bs.take i = bs.take a ++ (bs.drop a).take (i-a) := by
  calc
    bs.take i = (bs.take a ++ bs.drop a).take i := by rw [List.take_append_drop]
    _ = (bs.take a).take i ++ (bs.drop a).take (i - (bs.take a).length) := by rw [List.take_append]
    _ = bs.take a ++ (bs.drop a).take (i-a) := by
      have hlen : (bs.take a).length = a := by rw [List.length_take]; simp [ha]
      rw [hlen]
      rw [List.take_take]
      congr 2
      omega

theorem sound_AnchoredDirectory : AnchoredDirectory.Goal := by
  intro bs a i hai hi
  have ha : a ≤ bs.length := by omega
  unfold offset payload
  rw [take_append_take_drop bs a i ha hai]
  rw [List.map_append, List.sum_append]
  omega


/-! ## append at frame boundary -/

private theorem aeq (_hk : 0 < k) {a : Bytes} (ha : a.length % k = 0) :
    a.length = (a.length/k)*k := by
  have hd := Nat.div_add_mod a.length k
  have hcomm : k*(a.length/k) = (a.length/k)*k := Nat.mul_comm _ _
  rw [ha] at hd
  omega

private theorem ceil_eq_div_of_mod_zero (hk : 0 < k) {a : Nat} (ha : a % k = 0) :
    (a+k-1)/k = a/k := by
  have hd := Nat.div_add_mod a k
  have hcomm : k * (a/k) = (a/k)*k := Nat.mul_comm _ _
  rw [ha] at hd
  have heq : a = (a/k)*k := by omega
  rw [heq]
  have h1 : (a/k)*k + k - 1 = (k-1) + (a/k)*k := by omega
  rw [h1, Nat.add_mul_div_right (k-1) (a/k) hk]
  rw [Nat.div_eq_of_lt (by omega)]
  have h2 : (a/k)*k/k = a/k := by
    rw [Nat.mul_comm]
    exact Nat.mul_div_right _ hk
  rw [h2]
  omega

private theorem ceil_add_of_mod_zero (hk : 0 < k) {a b : Nat} (ha : a % k = 0) :
    (a+b+k-1)/k = (a+k-1)/k + (b+k-1)/k := by
  have hceil := ceil_eq_div_of_mod_zero hk ha
  have hakeq : a = (a/k)*k := by
    have hd := Nat.div_add_mod a k
    have hcomm : k*(a/k) = (a/k)*k := Nat.mul_comm _ _
    rw [ha] at hd
    omega
  rw [hceil, hakeq]
  have h1 : (a/k)*k + b + k - 1 = (b+k-1) + (a/k)*k := by omega
  rw [h1, Nat.add_mul_div_right (b+k-1) (a/k) hk]
  have h2 : (a/k)*k/k = a/k := by
    rw [Nat.mul_comm]
    exact Nat.mul_div_right _ hk
  rw [h2]
  omega

private theorem chunks_length_append (hk : 0 < k) {a b : Bytes} (ha : a.length % k = 0) :
    (chunks k (a++b)).length = (chunks k a).length + (chunks k b).length := by
  repeat rw [chunks_length]
  rw [List.length_append]
  exact ceil_add_of_mod_zero hk ha

private theorem getElem_eq_some_getD_of_mem {l : List Bytes} {i : Nat}
    (hi : i < l.length) : l[i]? = some (l[i]?.getD []) := by
  rw [List.getElem?_eq_getElem hi]
  simp

private theorem chunks_append_of_mod (hk : 0 < k) {a b : Bytes} (ha : a.length % k = 0) :
    chunks k (a++b) = chunks k a ++ chunks k b := by
  apply List.ext_getElem?
  intro t
  have hlenA : (chunks k a).length = a.length/k := by
    rw [chunks_length, ceil_eq_div_of_mod_zero hk ha]
  have hlenA_mul : (chunks k a).length * k = a.length := by
    rw [hlenA, ← aeq hk ha]
  have hlenAB := chunks_length_append (b:=b) hk ha
  by_cases ht : t < (chunks k a).length
  · have hta : t < a.length/k := by simpa [hlenA] using ht
    have htAB : t < (chunks k (a++b)).length := by rw [hlenAB]; omega
    have hxA := chunks_getElem hk a t ht
    have hxAB := chunks_getElem hk (a++b) t htAB
    have hq : (t+1)*k ≤ a.length := by
      have hq0 : (t+1)*k ≤ (a.length/k)*k := Nat.mul_le_mul_right k (by omega)
      rw [← aeq hk ha] at hq0
      exact hq0
    have hq' : t*k+k ≤ a.length := by
      have htk : (t+1)*k = t*k+k := by rw [Nat.add_mul, Nat.one_mul]
      rwa [htk] at hq
    have hdrop : (a++b).drop (t*k) = a.drop (t*k) ++ b := by
      apply List.drop_append_of_le_length
      omega
    have hlen_drop : k ≤ (a.drop (t*k)).length := by
      have hq'' : k + t*k ≤ a.length := by omega
      have hd : k ≤ a.length - t*k := Nat.le_sub_of_add_le hq''
      simpa [List.length_drop] using hd
    have htake2 : ((a.drop (t*k) ++ b).take k) = (a.drop (t*k)).take k :=
      List.take_append_of_le_length hlen_drop
    rw [getElem_eq_some_getD_of_mem htAB]
    rw [List.getElem?_append]
    simp only [ht, ↓reduceIte]
    rw [getElem_eq_some_getD_of_mem ht]
    rw [hxAB, hxA, hdrop, htake2]
  · let u := t - (chunks k a).length
    have ht_eq : t = (chunks k a).length + u := by
      dsimp [u]
      rw [Nat.add_sub_of_le (by omega : (chunks k a).length ≤ t)]
    by_cases htB : u < (chunks k b).length
    · have htAB : t < (chunks k (a++b)).length := by
        rw [hlenAB, ht_eq]
        omega
      have hxAB := chunks_getElem hk (a++b) t htAB
      have hxB := chunks_getElem hk b u htB
      have hdrop : (a++b).drop (t*k) = b.drop (u*k) := by
        rw [List.drop_append]
        have hale : a.length ≤ t*k := by
          rw [ht_eq, Nat.add_mul, hlenA_mul]
          omega
        rw [List.drop_eq_nil_of_le hale]
        simp
        rw [ht_eq, Nat.add_mul, hlenA_mul]
        simp
      rw [getElem_eq_some_getD_of_mem htAB]
      rw [List.getElem?_append]
      rw [ite_eq_right (by omega : ¬ t < (chunks k a).length)]
      rw [getElem_eq_some_getD_of_mem htB]
      rw [hxAB, hxB, hdrop]
    · have hnone1 : (chunks k (a++b))[t]? = none := by
        apply List.getElem?_eq_none_iff.mpr
        rw [hlenAB]
        dsimp [u] at htB
        omega
      have hnone2 : (chunks k a ++ chunks k b)[t]? = none := by
        apply List.getElem?_eq_none_iff.mpr
        rw [List.length_append]
        omega
      rw [hnone1, hnone2]

theorem sound_AppendAtBoundary : AppendAtBoundary.Goal := by
  intro c k a b hk ha
  have hc := chunks_append_of_mod (b:=b) hk ha
  unfold blocks
  rw [hc]
  rw [List.map_append]


/-! ## header widths -/

private theorem sum_take_le (l : List Nat) (i : Nat) : (l.take i).sum ≤ l.sum := by
  induction l generalizing i with
  | nil => simp
  | cons a as ih =>
      cases i with
      | zero => simp
      | succ i =>
          change (a :: as.take i).sum ≤ (a :: as).sum
          simp only [List.sum_cons]
          have h := ih i
          omega

private theorem payload_take_le (bs : List Block) (i : Nat) :
    payload (bs.take i) ≤ payload bs := by
  unfold payload
  rw [List.map_take]
  exact sum_take_le _ _

theorem sound_HeaderWidths : HeaderWidths.Goal := by
  intro c k s hk hk32 hdisk i hi
  constructor
  · have hle : offset (blocks c k s) i ≤ onDisk (blocks c k s) := by
      unfold offset onDisk
      have hp := payload_take_le (blocks c k s) i
      omega
    omega
  · have hbi : (blocks c k s)[i]? ≠ none := by
      intro hn
      have hle : (blocks c k s).length ≤ i := List.getElem?_eq_none_iff.mp hn
      omega
    obtain ⟨b, hb⟩ : ∃ b, (blocks c k s)[i]? = some b := by
      cases h : (blocks c k s)[i]? with
      | none => exact False.elim (hbi h)
      | some b => exact ⟨b, rfl⟩
    have hbg := blocks_getElem c k s i
    rw [hb] at hbg
    cases hci : (chunks k s)[i]? with
    | none => simp [hci] at hbg
    | some x =>
        have hb_wf : b.body.length = (choose c x).body.length := by
          exact congrArg List.length (by simpa [hci] using hbg)
        have hichi : i < (chunks k s).length := by
          obtain ⟨h', _⟩ := List.getElem?_eq_some_iff.mp hci
          exact h'
        have hxmem : x ∈ chunks k s := by
          have hx_eq : (chunks k s)[i] = x := by
            have h1 := List.getElem?_eq_getElem hichi
            rw [hci] at h1
            simpa using h1.symm
          rw [← hx_eq]
          exact List.getElem_mem hichi
        have hxlen : x.length ≤ k := chunk_length_le hk hxmem
        have hble : b.body.length ≤ k := by
          rw [hb_wf]
          exact Nat.le_trans (choose_body_length_le c x) hxlen
        have hgetd : ((blocks c k s)[i]?.map (fun b => b.body.length)).getD 0 = b.body.length := by
          simp [hb]
        rw [hgetd]
        omega


/-! ## random reads -/

private theorem slice_chunks (k : Nat) (xs : Bytes) (i m : Nat) :
    ((List.range m).map (fun t => (xs.drop ((i+t)*k)).take k)).flatten =
      (xs.drop (i*k)).take (m*k) := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.map_cons, List.flatten_append, ih]
      simp
      rw [show (m+1)*k = m*k+k by simp [Nat.add_mul]]
      rw [take_mul_eq]
      rw [List.drop_drop]
      rw [Nat.add_mul]

private theorem dirs_unpack_at (_hk : 0 < k) (c : Codec) (s : Bytes)
    (h : ∀ x ∈ chunks k s, c.decompress (c.compress x) = some x) (i : Nat)
    (_hi : i < (chunks k s).length) :
    (blocks c k s)[i]?.bind (unpack c) = (chunks k s)[i]? := by
  have hbg : (blocks c k s)[i]?.map Block.body =
      (chunks k s)[i]?.map (fun x => (choose c x).body) := by
    unfold blocks
    rw [List.getElem?_map]
    cases (chunks k s)[i]? <;> rfl
  cases hci : (chunks k s)[i]? with
  | none =>
      have hb : (blocks c k s)[i]? = none := by
        unfold blocks
        rw [List.getElem?_map, hci]
        rfl
      simp [hb]
  | some x =>
      have hxmem : x ∈ chunks k s := by
        obtain ⟨h', _⟩ := List.getElem?_eq_some_iff.mp hci
        have hx_eq : (chunks k s)[i] = x := by
          rw [List.getElem?_eq_getElem h'] at hci
          simpa using hci
        rw [← hx_eq]
        exact List.getElem_mem h'
      have hb : (blocks c k s)[i]? = some (choose c x) := by
        unfold blocks
        rw [List.getElem?_map, hci]
        rfl
      have hux : unpack c (choose c x) = some x := by
        unfold choose unpack
        dsimp
        split
        · simp [h x hxmem]
        · rfl
      simp [hb, hux]

private theorem mapM_congr {α β : Type} (f g : α → Option β) :
    ∀ l : List α, (∀ a ∈ l, f a = g a) → l.mapM f = l.mapM g
  | [], _ => rfl
  | a::as, h => by
      simp only [List.mapM_cons]
      rw [h a (by simp), mapM_congr f g as (fun b hb => h b (by simp [hb]))]

private theorem mapM_getElem_some (l : List Bytes) :
    ∀ R : List Nat, (∀ i ∈ R, i < l.length) →
      R.mapM (fun i => l[i]?) = some (R.map (fun i => l[i]?.getD []))
  | [], _ => rfl
  | i::is, h => by
      have hi : l[i]? = some (l[i]?.getD []) := by
        rw [List.getElem?_eq_getElem (h i (by simp))]
        simp
      simp only [List.mapM_cons]
      rw [hi, mapM_getElem_some l is (fun j hj => h j (by simp [hj]))]
      simp

private theorem mem_rangeBlocks_lt_chunks (hk : 0 < k) {n pos len i : Nat}
    (hi : i ∈ rangeBlocks k n pos len) : i < (n+k-1)/k := by
  obtain ⟨x, hxn, _, _, hxdiv⟩ := (mem_rangeBlocks_iff hk).mp hi
  subst i
  apply (Nat.div_lt_iff_lt_mul hk).mpr
  have hnle : n ≤ ((n+k-1)/k)*k := by
    have h2 := Nat.div_add_mod (n+k-1) k
    have hr := Nat.mod_lt (n+k-1) hk
    have hc : k * ((n+k-1)/k) = ((n+k-1)/k)*k := Nat.mul_comm _ _
    omega
  omega

private theorem dirs_unpack_some (hk : 0 < k) (c : Codec) (s : Bytes)
    (h : ∀ x ∈ chunks k s, c.decompress (c.compress x) = some x) (i : Nat)
    (hi : i < (chunks k s).length) : (blocks c k s)[i]?.bind (unpack c) ≠ none := by
  rw [dirs_unpack_at hk c s h i hi]
  intro hn
  have hle : (chunks k s).length ≤ i := List.getElem?_eq_none_iff.mp hn
  omega

theorem sound_RandomRead : RandomRead.Goal := by
  intro c k s pos len hk h
  let n := s.length
  let R := rangeBlocks k n pos len
  have hvalid : ∀ i ∈ R, i < (chunks k s).length := by
    intro i hi
    have hi' := mem_rangeBlocks_lt_chunks hk hi
    rw [chunks_length]
    exact hi'
  have hmap1 : R.mapM (fun i => (blocks c k s)[i]?.bind (unpack c)) =
      R.mapM (fun i => (chunks k s)[i]?) := by
    apply mapM_congr
    intro i hi
    exact dirs_unpack_at hk c s h i (hvalid i hi)
  have hmap2 : R.mapM (fun i => (chunks k s)[i]?) =
      some (R.map (fun i => (chunks k s)[i]?.getD [])) :=
    mapM_getElem_some (chunks k s) R hvalid
  unfold viaDirectory
  dsimp only
  rw [hmap1, hmap2]
  simp
  let stop := min n (pos+len)
  by_cases hlen0 : len = 0
  · subst len
    have hRnil : R = [] := by
      dsimp [R]
      unfold rangeBlocks
      dsimp only
      rw [ite_eq_right (show ¬ pos < min n (pos+0) by
        have hmn := Nat.min_le_left n (pos+0)
        omega)]
    rw [hRnil]
    simp only [List.map_nil, List.flatten_nil, List.drop_nil, List.take_nil]
    rw [List.take_zero]
  · by_cases hposn : pos < n
    · have hps : pos < stop := by
        dsimp [stop]
        exact Nat.lt_min.mpr ⟨hposn, by omega⟩
      have hR : R = (List.range ((stop-1)/k - pos/k + 1)).map (pos/k + ·) := by
        dsimp [R]
        unfold rangeBlocks
        dsimp only
        rw [ite_eq_left hps]
      let i0 := pos/k
      let m := (stop-1)/k - i0 + 1
      have hmapR : (R.map (fun i => (chunks k s)[i]?.getD [])) =
          (List.range m).map (fun t => (s.drop ((i0+t)*k)).take k) := by
        rw [hR, List.map_map]
        apply List.map_congr_left
        intro t ht
        have hit : i0 + t < (chunks k s).length := by
          apply hvalid
          rw [hR, List.mem_map]
          exact ⟨t, ht, rfl⟩
        exact chunks_getElem hk s (i0+t) hit
      rw [hmapR]
      rw [slice_chunks k s i0 m]
      have hposdec : pos = i0*k + pos%k := by
        have hc : k*(pos/k) = (pos/k)*k := Nat.mul_comm _ _
        have := Nat.div_add_mod pos k
        dsimp [i0] at hc ⊢
        omega
      rw [show List.drop (pos%k) ((s.drop (i0*k)).take (m*k)) =
          (s.drop pos).take (m*k - pos%k) by
        rw [List.drop_take]
        rw [List.drop_drop]
        rw [← hposdec]]
      let j := (stop-1)/k
      have hij : i0 ≤ j := by
        apply Nat.div_le_div_right
        omega
      have hjlow : stop - 1 < (j+1)*k := by
        have := Nat.lt_mul_div_succ (stop-1) hk
        simpa [j, Nat.mul_comm, Nat.mul_add, Nat.add_comm, Nat.add_left_comm] using this
      have hm_eq : m*k + i0*k = (j+1)*k := by
        rw [← Nat.add_mul]
        congr 1
        dsimp [m, i0, j]
        omega
      have hr : pos%k < k := Nat.mod_lt pos hk
      by_cases hle_n : pos+len ≤ n
      · have hstop_eq : stop = pos+len := by
          dsimp [stop]
          rw [Nat.min_eq_right hle_n]
        have hm_lb : m*k - pos%k ≥ stop - pos := by omega
        have ha : len ≤ m*k - pos%k := by omega
        rw [List.take_take]
        congr 1
        omega
      · have hstop_eq : stop = n := by
          dsimp [stop]
          rw [Nat.min_eq_left (by omega : n ≤ pos+len)]
        have hm_lb : m*k - pos%k ≥ stop - pos := by omega
        have ha : n - pos ≤ m*k - pos%k := by omega
        have htake_all : (s.drop pos).take (m*k - pos%k) = s.drop pos := by
          apply List.ext_getElem?
          intro t
          rw [List.getElem?_take]
          by_cases ht : t < m*k - pos%k
          · rw [ite_eq_left ht]
          · rw [ite_eq_right ht]
            have hle : (s.drop pos).length ≤ t := by
              rw [List.length_drop]
              have hnlen : n = s.length := rfl
              omega
            rw [List.getElem?_eq_none_iff.mpr hle]
        rw [htake_all]
    · have hstop_le : min n (pos+len) ≤ pos := by
        have hmn := Nat.min_le_left n (pos+len)
        omega
      have hRnil : R = [] := by
        dsimp [R]
        unfold rangeBlocks
        dsimp only
        rw [ite_eq_right (show ¬ pos < min n (pos+len) by omega)]
      rw [hRnil]
      simp only [List.map_nil, List.flatten_nil, List.drop_nil, List.take_nil]
      have hdropnil : s.drop pos = [] := by
        apply List.drop_eq_nil_of_le
        dsimp [n] at hposn
        omega
      rw [hdropnil]
      simp


end TinylambOpt.ResearchProofs.Compression.Container

-- C++ references to this model or its proof.
-- table/tlcs_compression.cpp
-- table/tlcs_compression.hpp
-- table/tlcs_file.cpp
-- table/tlcs_file.hpp
