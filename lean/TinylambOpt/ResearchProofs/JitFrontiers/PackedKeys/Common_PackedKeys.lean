import TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Named results include `guardRange_any`, `word_lt_two_pow64`, `word_toNat`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys

open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys

theorem guardRange_any (c : Codec) : guardRange c := by
  intro k
  simp only [encode]
  cases hf : c.fields k with
  | none => simp [hf]
  | some p =>
    rcases p with ⟨a, b⟩
    simp
    constructor
    · intro h
      exact ⟨a, b, ⟨⟨rfl, rfl⟩, h⟩⟩
    · rintro ⟨a1, b1, ⟨⟨ha, hb⟩, h⟩⟩
      subst a1
      subst b1
      exact h

theorem word_lt_two_pow64 (c : Codec) (hL : c.lowBits ≤ 64) {a b : Nat}
    (ha : a < 2 ^ (64 - c.lowBits)) (hb : b < 2 ^ c.lowBits) :
    a * 2 ^ c.lowBits + b < 2 ^ 64 := by
  have hp : 2 ^ (64 - c.lowBits) * 2 ^ c.lowBits = 2 ^ 64 := by
    rw [← Nat.pow_add]
    congr 1
    omega
  have hm : a * 2 ^ c.lowBits < 2 ^ (64 - c.lowBits) * 2 ^ c.lowBits :=
    Nat.mul_lt_mul_of_pos_right ha (Nat.two_pow_pos _)
  have ha1 : (a + 1) * 2 ^ c.lowBits ≤ 2 ^ (64 - c.lowBits) * 2 ^ c.lowBits :=
    Nat.mul_le_mul_right _ (Nat.succ_le_of_lt ha)
  have hsum : a * 2 ^ c.lowBits + b < a * 2 ^ c.lowBits + 2 ^ c.lowBits :=
    Nat.add_lt_add_left hb _
  have hs : a * 2 ^ c.lowBits + 2 ^ c.lowBits = (a + 1) * 2 ^ c.lowBits := by
    rw [Nat.succ_mul]
  rw [hs] at hsum
  omega

theorem word_toNat (c : Codec) (hL : c.lowBits ≤ 64) {a b : Nat}
    (ha : a < 2 ^ (64 - c.lowBits)) (hb : b < 2 ^ c.lowBits) :
    (BitVec.ofNat 64 (a * 2 ^ c.lowBits + b)).toNat = a * 2 ^ c.lowBits + b := by
  rw [BitVec.toNat_ofNat]
  exact Nat.mod_eq_of_lt (word_lt_two_pow64 c hL ha hb)

theorem word_div (c : Codec) {a b : Nat} (hb : b < 2 ^ c.lowBits) :
    (a * 2 ^ c.lowBits + b) / 2 ^ c.lowBits = a := by
  rw [Nat.mul_comm a, Nat.mul_add_div (Nat.two_pow_pos _)]
  rw [Nat.div_eq_of_lt hb, Nat.add_zero]

theorem word_mod (c : Codec) {a b : Nat} (hb : b < 2 ^ c.lowBits) :
    (a * 2 ^ c.lowBits + b) % 2 ^ c.lowBits = b := by
  rw [Nat.mul_comm a, Nat.add_comm, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hb]

theorem fields_of_encode (c : Codec) (hL : c.lowBits ≤ 64) {k : Key} {v : BitVec 64}
    (he : encode c k = some v) :
    ∃ a b, c.fields k = some (a, b) ∧ a < 2 ^ (64 - c.lowBits) ∧ b < 2 ^ c.lowBits ∧
      v.toNat / 2 ^ c.lowBits = a ∧ v.toNat % 2 ^ c.lowBits = b := by
  unfold encode at he
  cases hf : c.fields k with
  | none => simp [hf] at he
  | some p =>
    rcases p with ⟨a, b⟩
    simp [hf] at he
    by_cases hcond : a < 2 ^ (64 - c.lowBits) ∧ b < 2 ^ c.lowBits
    · have heq : BitVec.ofNat 64 (a * 2 ^ c.lowBits + b) = v := by simpa [hcond] using he
      have hv := congrArg BitVec.toNat heq
      rw [word_toNat c hL hcond.1 hcond.2] at hv
      refine ⟨a, b, rfl, hcond.1, hcond.2, ?_, ?_⟩
      · rw [← hv]
        exact word_div c hcond.2
      · rw [← hv]
        exact word_mod c hcond.2
    · simp [hcond] at he

theorem encode_word_of_encode (c : Codec) (hL : c.lowBits ≤ 64) {k : Key} {v : BitVec 64}
    (he : encode c k = some v) (a b : Nat) (hf : c.fields k = some (a, b))
    (ha : a < 2 ^ (64 - c.lowBits)) (hb : b < 2 ^ c.lowBits) :
    BitVec.ofNat 64 (a * 2 ^ c.lowBits + b) = v := by
  have hite : encode c k =
      if a < 2 ^ (64 - c.lowBits) && b < 2 ^ c.lowBits then
        some (BitVec.ofNat 64 (a * 2 ^ c.lowBits + b)) else none := by
    unfold encode
    rw [hf]
    simp
  rw [hite] at he
  rw [if_pos (by simp [ha, hb])] at he
  exact Option.some.inj he

theorem recover_of_back_fields (c : Codec) (hL : c.lowBits ≤ 64)
    (hback : ∀ k a b, c.fields k = some (a, b) → c.back (a, b) = k) : recover c := by
  intro k v he
  obtain ⟨a, b, hf, _, _, hdiv, hmod⟩ := fields_of_encode c hL he
  unfold decode
  have hf' : c.fields k = some (v.toNat / 2 ^ c.lowBits, v.toNat % 2 ^ c.lowBits) := by
    simpa [hdiv, hmod] using hf
  exact hback k _ _ hf'

theorem collisionFree_of_recover (c : Codec) (hr : recover c) : collisionFree c := by
  intro a b v ha hb
  rw [← hr a v ha, ← hr b v hb]

theorem hits_eq_beq (c : Codec) (hcf : collisionFree c) :
    ∀ a b : Key, hits c a b = (a == b) := by
  intro a b
  cases ha : encode c a with
  | none =>
    cases hb : encode c b with
    | none => simp [hits, ha, hb]
    | some y => simp [hits, ha, hb]
  | some x =>
    cases hb : encode c b with
    | none => simp [hits, ha, hb]
    | some y =>
      simp only [hits, ha, hb]
      by_cases hab : a = b
      · subst b
        have hxy' : x = y := Option.some.inj (ha.symm.trans hb)
        subst y
        simp
      · have hxy : x ≠ y := by
          intro h
          apply hab
          exact hcf a b x ha (by simpa [h] using hb)
        have hxb : (x == y) = false := by simp [hxy]
        have habb : (a == b) = false := by simp [hab]
        rw [hxb, habb]

theorem countP_beq_eq_count (k : Key) : ∀ l : List Key,
    l.countP (fun x => k == x) = l.count k := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
    by_cases h : k = x
    · subst x
      simp [ih]
    · have hb1 : (k == x) = false := by simp [h]
      have hxk : x ≠ k := by intro hx; exact h hx.symm
      have hb2 : (x == k) = false := by simp [hxk]
      simp [List.count_cons, hb1, hb2, ih]

theorem joinFanout_of_hits (c : Codec) (hh : ∀ a b : Key, hits c a b = (a == b)) :
    joinFanout c := by
  intro left right
  induction left with
  | nil => simp
  | cons k ks ih =>
    simp only [List.map_cons, List.sum_cons]
    rw [ih]
    have hp : (hits c k) = (fun x => k == x) := by funext x; exact hh k x
    rw [hp, countP_beq_eq_count]

theorem sqlNullGate_of_hits (c : Codec) (hh : ∀ a b : Key, hits c a b = (a == b)) :
    sqlNullGate c := by
  intro a b
  rw [hh]
  rcases a with ⟨a₁, a₂⟩
  rcases b with ⟨b₁, b₂⟩
  cases a₁ <;> cases a₂ <;> cases b₁ <;> cases b₂ <;>
    simp only [BEq.beq, instBEqOption.beq, Option.isSome_some, Option.isSome_none,
      Bool.true_and, Bool.and_false, Bool.false_and, Bool.and_true]

theorem filter_map_eq {α β γ : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    [BEq γ] [LawfulBEq γ]
    (f : α → β) (k : α → γ) (a : α) (l : List α)
    (hiff : ∀ x ∈ l, (f x == f a) = (k x == k a)) :
    (l.map f).filter (fun b => !(b == f a)) =
      (l.filter (fun b => !(k b == k a))).map f := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.filter_cons]
    have hxa : (f x == f a) = (k x == k a) := hiff x (by simp)
    rw [hxa]
    by_cases h : k x == k a
    · simp [h, ih (fun y hy => hiff y (by simp [hy]))]
    · simp [h, ih (fun y hy => hiff y (by simp [hy]))]

theorem eraseDups_map_eq {α β γ : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    [BEq γ] [LawfulBEq γ]
    (f : α → β) (g : β → γ) (k : α → γ) (l : List α)
    (hget : ∀ a ∈ l, g (f a) = k a)
    (hiff : ∀ a ∈ l, ∀ b ∈ l, (f a == f b) = (k a == k b)) :
    ((l.map f).eraseDups).map g = (l.map k).eraseDups := by
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
    cases l with
    | nil => simp
    | cons a as =>
      have hlenas : as.length < n := by
        have hn : n = as.length + 1 := by simpa using hlen.symm
        omega
      simp only [List.map_cons, List.eraseDups_cons]
      have hfa : g (f a) = k a := hget a (by simp)
      let fs := as.filter (fun b => !(k b == k a))
      have hgetfs : ∀ x ∈ fs, g (f x) = k x := by
        intro x hx
        have hxas : x ∈ as := (List.mem_filter.mp hx).1
        exact hget x (by simp [hxas])
      have hifffs : ∀ x ∈ fs, ∀ y ∈ fs, (f x == f y) = (k x == k y) := by
        intro x hx y hy
        have hxas : x ∈ as := (List.mem_filter.mp hx).1
        have hyas : y ∈ as := (List.mem_filter.mp hy).1
        exact hiff x (by simp [hxas]) y (by simp [hyas])
      have hfslen : fs.length < n := by
        have hle : fs.length ≤ as.length := List.length_filter_le _ _
        omega
      have ihfs := ih fs.length hfslen fs hgetfs hifffs rfl
      have hfilter : (List.filter (fun b : β => !(b == f a)) (as.map f)) = fs.map f := by
        rw [filter_map_eq f k a as (fun x hx => hiff x (by simp [hx]) a (by simp))]
      have hfilterk : (List.filter (fun b : γ => !(b == k a)) (as.map k)) = fs.map k := by
        rw [filter_map_eq k k a as (fun x hx => rfl)]
      rw [hfilter, ihfs, hfilterk]
      simp [hfa]

theorem filterMap_pair_map_fst (c : Codec) : ∀ xs : List Key,
    (xs.filterMap (fun k => (encode c k).map (fun v => (v, k)))).map Prod.fst =
      xs.filterMap (encode c) := by
  intro xs
  induction xs with
  | nil => rfl
  | cons k ks ih =>
    cases h : encode c k <;> simp [h, ih]

theorem filterMap_pair_map_snd (c : Codec) : ∀ xs : List Key,
    (xs.filterMap (fun k => (encode c k).map (fun v => (v, k)))).map Prod.snd =
      xs.filter (fun k => (encode c k).isSome) := by
  intro xs
  induction xs with
  | nil => rfl
  | cons k ks ih =>
    cases h : encode c k <;> simp [h, ih]

theorem distinctGroups_of_hits (c : Codec) (hr : recover c)
    (hh : ∀ a b : Key, hits c a b = (a == b)) : distinctGroups c := by
  intro xs
  have hsnd' := filterMap_pair_map_snd c xs
  have hfst' := filterMap_pair_map_fst c xs
  let ys : List (BitVec 64 × Key) := xs.filterMap (fun k => (encode c k).map (fun v => (v, k)))
  have hfst : ys.map Prod.fst = xs.filterMap (encode c) := by simpa [ys] using hfst'
  have hsnd : ys.map Prod.snd = xs.filter (fun k => (encode c k).isSome) := by simpa [ys] using hsnd'
  have hget : ∀ p ∈ ys, decode c p.1 = p.2 := by
    intro p hp
    simp only [ys, List.mem_filterMap] at hp
    obtain ⟨k, hk, hv⟩ := hp
    cases h : encode c k with
    | none => simp [h] at hv
    | some v =>
      simp [h] at hv
      subst p
      exact hr k v h
  have hiff : ∀ p ∈ ys, ∀ q ∈ ys, (p.1 == q.1) = (p.2 == q.2) := by
    intro p hp q hq
    simp only [ys, List.mem_filterMap] at hp hq
    obtain ⟨k, hk, hvk⟩ := hp
    obtain ⟨l, hl, hvl⟩ := hq
    cases henck : encode c k with
    | none => simp [henck] at hvk
    | some x =>
      simp [henck] at hvk
      subst p
      cases hencl : encode c l with
      | none => simp [hencl] at hvl
      | some y =>
        simp [hencl] at hvl
        subst q
        have hw : (x == y) = (hits c k l) := by simp [hits, henck, hencl]
        rw [hw, hh k l]
  have h := eraseDups_map_eq (fun p : BitVec 64 × Key => p.1) (fun v => decode c v)
    Prod.snd ys hget hiff
  rw [← hfst, ← hsnd, h]

theorem partition_perm (c : Codec) : ∀ l : List (Key × Nat),
    ((l.filter (fun p => (encode c p.1).isSome)) ++
      (l.filter (fun p => (encode c p.1).isNone))).Perm l := by
  intro l
  induction l with
  | nil => simp
  | cons p ps ih =>
    cases h : encode c p.1 with
    | none =>
      have hm := (List.perm_middle (a := p)
        (l₁ := List.filter (fun q => (encode c q.1).isSome) ps)
        (l₂ := List.filter (fun q => (encode c q.1).isNone) ps)).trans (ih.cons p)
      simpa [h] using hm
    | some v =>
      simpa [h] using ih.cons p

theorem fast_decode_map (c : Codec) (hr : recover c) : ∀ l : List (Key × Nat),
    ((l.filterMap (fun p => (encode c p.1).map (fun v => (p.2, v)))).map
        (fun (i, v) => (i, decode c v))) =
      (l.filter (fun p => (encode c p.1).isSome)).map (fun (k, i) => (i, k)) := by
  intro l
  induction l with
  | nil => rfl
  | cons p ps ih =>
    cases h : encode c p.1 with
    | none => simp [h, ih]
    | some v =>
      rcases p with ⟨k, i⟩
      simp [h, ih, hr k v h]

theorem sorted_zip_mapped : ∀ (n : Nat) (l : List Key),
    ((l.zipIdx n).map (fun (k, i) => (i, k))).Pairwise
      (fun a b : Nat × Key => a.1 ≤ b.1) := by
  intro n l
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
    simp only [List.zipIdx_cons, List.map_cons]
    constructor
    · intro y hy
      simp at hy
      obtain ⟨j, hj, hmem, hzip, rfl⟩ := hy
      have hh := List.mem_zipIdx hzip
      omega
    · exact ih (n + 1)

theorem fallbackBag_of_recover (c : Codec) (hr : recover c) : fallbackBag c := by
  intro xs
  let hot := (fast c xs).map (fun (i, v) => (i, decode c v))
  let slow := (cold c xs).map (fun (k, i) => (i, k))
  let zs := xs.zipIdx.map (fun (k, i) => (i, k))
  let le : (Nat × Key) → (Nat × Key) → Bool := fun a b => decide (a.1 ≤ b.1)
  change (((hot ++ slow).mergeSort le).map Prod.snd) = xs
  have hhot : hot = (xs.zipIdx.filter (fun p => (encode c p.1).isSome)).map (fun (k, i) => (i, k)) := by
    dsimp [hot, fast]
    exact fast_decode_map c hr xs.zipIdx
  have hslow : slow = (xs.zipIdx.filter (fun p => (encode c p.1).isNone)).map (fun (k, i) => (i, k)) := by
    rfl
  have hperm : (hot ++ slow).Perm zs := by
    rw [hhot, hslow]
    dsimp [zs]
    simpa [List.map_append] using (partition_perm c xs.zipIdx).map (fun (k, i) => (i, k))
  have htrans : ∀ a b d, le a b = true → le b d = true → le a d = true := by
    intro a b d hab hbd
    simp [le] at hab hbd ⊢
    omega
  have htotal : ∀ a b, (le a b || le b a) = true := by
    intro a b
    simp [le]
    omega
  have hanti : ∀ a b : Nat × Key,
      a ∈ (hot ++ slow).mergeSort le → b ∈ zs → le a b = true → le b a = true → a = b := by
    intro a b ha hb hab hba
    have ha' : a ∈ hot ++ slow := by simpa using ha
    have haz : (a.2, a.1) ∈ xs.zipIdx := by
      rw [hhot, hslow] at ha'
      simp only [List.mem_append, List.mem_map, List.mem_filter] at ha'
      rcases ha' with ha' | ha'
      · obtain ⟨p, ⟨hp, _⟩, hpEq⟩ := ha'
        rcases p with ⟨k, i⟩
        rw [← hpEq]
        simpa using hp
      · obtain ⟨p, ⟨hp, _⟩, hpEq⟩ := ha'
        rcases p with ⟨k, i⟩
        rw [← hpEq]
        simpa using hp
    have hbz : (b.2, b.1) ∈ xs.zipIdx := by
      dsimp [zs] at hb
      simp only [List.mem_map] at hb
      obtain ⟨p, hp, hpEq⟩ := hb
      rcases p with ⟨k, i⟩
      subst b
      simpa using hp
    have hi : a.1 = b.1 := by
      simp [le] at hab hba
      omega
    have hma := List.mem_zipIdx haz
    have hmb := List.mem_zipIdx hbz
    have hka : a.2 = xs[a.1] := by simpa using hma.2.2
    have hkb : b.2 = xs[b.1] := by simpa using hmb.2.2
    ext <;> simp [hi, hka, hkb]
  have hzsorted : zs.Pairwise (fun a b => le a b = true) := by
    dsimp [zs]
    exact (sorted_zip_mapped 0 xs).imp (by intro a b h; simp [le, h])
  have heqms : (hot ++ slow).mergeSort le = zs :=
    List.Perm.eq_of_pairwise hanti
      (List.pairwise_mergeSort htrans htotal (hot ++ slow))
      hzsorted
      ((List.mergeSort_perm (hot ++ slow) le).trans hperm)
  rw [heqms]
  dsimp [zs]
  rw [List.map_map]
  change List.map Prod.fst (xs.zipIdx 0) = xs
  exact List.zipIdx_map_fst 0 xs


theorem signedRaw_back {w : Nat} (hw : 0 < w) {x : Int} {n : Nat} (h : signedRaw w x = some n) :
    (BitVec.ofNat w n).toInt = x := by
  unfold signedRaw at h
  by_cases hc : -(2^(w-1):Int) ≤ x ∧ x < 2^(w-1)
  · have hn : (BitVec.ofInt w x).toNat = n := by simpa [hc] using h
    subst n
    rw [BitVec.ofNat_toNat, BitVec.setWidth_eq]
    exact BitVec.toInt_ofInt_eq_self hw hc.1 hc.2
  · simp [hc] at h

theorem unsigned_some {x : Int} (h : 0 ≤ x) : unsigned x = some x.toNat := by
  simp [unsigned, h]

theorem unbias_biased {w : Nat} {x : Int} (_h1 : -(2^(w-1):Int) ≤ x) (_h2 : x < 2^(w-1)) :
    unbias w ((x + 2^(w-1)).toNat) = x := by
  unfold unbias
  have hn : ¬ x + 2^(w-1) < 0 := by omega
  simp [biased, unsigned, hn]
  have : ((x + 2^(w-1)).toNat : Int) = x + 2^(w-1) := Int.toNat_of_nonneg (by omega)
  omega

theorem unzig_zig (x : Int) : unzig (zig x) = x := by
  unfold zig unzig
  by_cases hx : x < 0
  · simp [hx]
    have hpos : 0 < x.natAbs := by
      have : (x.natAbs : Int) = -x := by
        rw [← Int.natAbs_neg x]
        exact Int.natAbs_of_nonneg (by omega)
      omega
    have hxabs : (x.natAbs : Int) = -x := by
      rw [← Int.natAbs_neg x]
      exact Int.natAbs_of_nonneg (by omega)
    have hmod : (2 * x.natAbs - 1) % 2 = 1 := by omega
    have hdiv : (2 * x.natAbs - 1) / 2 = x.natAbs - 1 := by omega
    simp [hmod]
    omega
  · have hx0 : 0 ≤ x := by omega
    simp only [hx, ↓reduceIte]
    have hmod : (2 * x.toNat) % 2 = 0 := by omega
    have hdiv : (2 * x.toNat) / 2 = x.toNat := by omega
    simp [hmod, hdiv]
    rw [Int.max_eq_left hx0]

theorem zig_lt_bound : ∀ x : Int, zig x < 2^32 ↔ -(2^31:Int) ≤ x ∧ x < 2^31 := by
  intro x
  unfold zig
  by_cases hx : x < 0
  · simp [hx]
    have hxabs : (x.natAbs : Int) = -x := by
      rw [← Int.natAbs_neg x]
      exact Int.natAbs_of_nonneg (by omega)
    constructor
    · intro h
      omega
    · intro h
      omega
  · have hx0 : 0 ≤ x := by omega
    simp [hx]
    constructor
    · intro h
      have : (x.toNat : Int) = x := Int.toNat_of_nonneg hx0
      omega
    · intro h
      have : (x.toNat : Int) = x := Int.toNat_of_nonneg hx0
      omega


theorem unbias_biased_of_nonneg {w : Nat} {x : Int} (h : 0 ≤ x + 2^(w-1)) :
    unbias w ((x + 2^(w-1)).toNat) = x := by
  unfold unbias
  rw [show ((x + 2^(w-1)).toNat : Int) = x + 2^(w-1) from Int.toNat_of_nonneg h]
  omega


theorem xor_lt_two_pow_iff_div_eq {x y n : Nat} (hn : 0 < n) :
    x ^^^ y < 2 ^ n ↔ x / 2 ^ n = y / 2 ^ n := by
  constructor
  · intro hlt
    apply Nat.eq_of_testBit_eq
    intro j
    rw [Nat.testBit_div_two_pow, Nat.testBit_div_two_pow]
    have hp : 2 ^ n ≤ 2 ^ (j + n) := Nat.pow_le_pow_right (by decide) (by omega)
    have hlt' : x ^^^ y < 2 ^ (j + n) := by omega
    have hx := Nat.testBit_lt_two_pow (i := j + n) hlt'
    rw [Nat.testBit_xor] at hx
    cases h1 : x.testBit (j + n) <;> cases h2 : y.testBit (j + n) <;> simp [h1, h2] at hx ⊢
  · intro hdiv
    apply Nat.lt_pow_two_of_testBit
    intro i hi
    obtain ⟨j, rfl⟩ : ∃ j, i = j + n := ⟨i - n, by omega⟩
    rw [Nat.testBit_xor]
    rw [← Nat.testBit_div_two_pow, ← Nat.testBit_div_two_pow, hdiv]
    simp


theorem encode_eq_some (c : Codec) {k : Key} (a b : Nat)
    (hf : c.fields k = some (a, b)) (ha : a < 2 ^ (64 - c.lowBits)) (hb : b < 2 ^ c.lowBits) :
    encode c k = some (BitVec.ofNat 64 (a * 2 ^ c.lowBits + b)) := by
  unfold encode
  rw [hf]
  simp [ha, hb]

theorem encode_eq_none_of_fields_none (c : Codec) {k : Key} (hf : c.fields k = none) :
    encode c k = none := by
  unfold encode
  rw [hf]
  simp

theorem encode_eq_none_of_guard (c : Codec) {k : Key} {a b : Nat}
    (hf : c.fields k = some (a, b))
    (h : ¬(a < 2 ^ (64 - c.lowBits) ∧ b < 2 ^ c.lowBits)) : encode c k = none := by
  unfold encode
  rw [hf]
  simp [h]

theorem unsigned_val {x : Int} {n : Nat} (h : unsigned x = some n) : (n : Int) = x := by
  unfold unsigned at h
  by_cases hc : x < 0
  · simp [hc] at h
  · simp [hc] at h
    rw [← h]
    exact Int.toNat_of_nonneg (by omega)

theorem biased_val {w : Nat} {x : Int} {n : Nat} (h : biased w x = some n) :
    (n : Int) = x + 2 ^ (w - 1) := by
  unfold biased at h
  exact unsigned_val h

theorem packed_lt_iff {L a1 b1 a2 b2 : Nat} (hb1 : b1 < 2 ^ L) (hb2 : b2 < 2 ^ L) :
    a1 * 2 ^ L + b1 < a2 * 2 ^ L + b2 ↔ a1 < a2 ∨ (a1 = a2 ∧ b1 < b2) := by
  constructor
  · intro h
    rcases Nat.lt_trichotomy a1 a2 with hlt | heq | hgt
    · exact Or.inl hlt
    · subst a2
      exact Or.inr ⟨rfl, by omega⟩
    · exfalso
      have hmul : (a2 + 1) * 2 ^ L ≤ a1 * 2 ^ L :=
        Nat.mul_le_mul_right _ (Nat.succ_le_of_lt hgt)
      have h2 : a2 * 2 ^ L + 2 ^ L ≤ a1 * 2 ^ L := by
        rw [← Nat.succ_mul]
        exact hmul
      omega
  · intro h
    rcases h with hlt | ⟨heq, hlt⟩
    · have hmul : (a1 + 1) * 2 ^ L ≤ a2 * 2 ^ L :=
        Nat.mul_le_mul_right _ (Nat.succ_le_of_lt hlt)
      have h1 : a1 * 2 ^ L + 2 ^ L ≤ a2 * 2 ^ L := by
        rw [← Nat.succ_mul]
        exact hmul
      omega
    · subst a2
      omega

theorem reuseForSort_of (c : Codec) (hL : c.lowBits ≤ 64)
    (hord : ∀ k1 k2 : Key, ∀ A B C D : Nat, c.fields k1 = some (A, B) → c.fields k2 = some (C, D) →
        A < 2 ^ (64 - c.lowBits) → B < 2 ^ c.lowBits → C < 2 ^ (64 - c.lowBits) →
        D < 2 ^ c.lowBits →
        (decide (A < C ∨ (A = C ∧ B < D)) = lex k1 k2)) :
    reuseForSort c := by
  intro a b x y ha hb
  obtain ⟨A, B, hfa, hA, hB, hdiv, hmod⟩ := fields_of_encode c hL ha
  obtain ⟨C, D, hfb, hC, hD, hdiv2, hmod2⟩ := fields_of_encode c hL hb
  have hxP : A * 2 ^ c.lowBits + B < 2 ^ 64 := word_lt_two_pow64 c hL hA hB
  have hyP : C * 2 ^ c.lowBits + D < 2 ^ 64 := word_lt_two_pow64 c hL hC hD
  have hxv : BitVec.ofNat 64 (A * 2 ^ c.lowBits + B) = x :=
    encode_word_of_encode c hL ha A B hfa hA hB
  have hyv : BitVec.ofNat 64 (C * 2 ^ c.lowBits + D) = y :=
    encode_word_of_encode c hL hb C D hfb hC hD
  rw [← hxv, ← hyv]
  subst x
  subst y
  have hcmp : decide (BitVec.ofNat 64 (A * 2 ^ c.lowBits + B) <
      BitVec.ofNat 64 (C * 2 ^ c.lowBits + D)) =
      decide (A < C ∨ (A = C ∧ B < D)) := by
    refine decide_eq_decide.mpr ?_
    rw [BitVec.lt_def, BitVec.toNat_ofNat, BitVec.toNat_ofNat,
      Nat.mod_eq_of_lt hxP, Nat.mod_eq_of_lt hyP]
    exact packed_lt_iff hB hD
  rw [hcmp]
  exact hord a b A B C D hfa hfb hA hB hC hD

theorem lex_pair (x y u v : Int) :
    lex (pair x y) (pair u v) = decide (x < u ∨ (x = u ∧ y < v)) := by
  unfold lex pair
  simp only [Prod.fst, Prod.snd]
  by_cases h : x < u
  · have hc : compare (some x) (some u) = Ordering.lt := by simp [compare, h]
    simp [h, hc]
  · by_cases heq : x = u
    · subst u
      have h2 : (compare (some y) (some v) == Ordering.lt) = decide (y < v) := by
        by_cases hy : y < v <;> simp [compare, hy]
      simp [h2]
    · have hcmp : compare (some x) (some u) ≠ Ordering.lt := by
        intro hc
        have : x < u := by simpa [compare] using hc
        exact h this
      simp [h, heq, hcmp]

theorem compare_none_none : compare (none : Option Int) none = Ordering.eq := rfl
theorem compare_none_some (y : Int) : compare (none : Option Int) (some y) = Ordering.lt := by
  have h : (compare none (some y) : Ordering) = Ordering.lt := by
    show Ordering.lt = Ordering.lt
    rfl
  exact h
theorem compare_some_none (x : Int) : compare (some x : Option Int) none = Ordering.gt := by
  show Ordering.gt = Ordering.gt
  rfl

theorem lex_fst (x u : Int) (p q : Option Int) :
    lex (some x, p) (some u, q) = decide (x < u ∨ (x = u ∧ compare p q = Ordering.lt)) := by
  unfold lex
  simp only [Prod.fst, Prod.snd]
  by_cases hx : x < u
  · have hc : compare (some x) (some u) = Ordering.lt := by
      show compare x u = Ordering.lt
      exact (Int.compare_eq_lt).mpr hx
    simp [hx, hc]
  · have hne : ¬compare (some x) (some u) = Ordering.lt := by
      intro hc
      have : compare x u = Ordering.lt := hc
      exact hx (Int.compare_eq_lt.mp this)
    by_cases hxe : x = u
    · subst u
      simp [hx, hne] <;> cases p <;> cases q <;> rfl
    · have hge : u < x := by omega
      have hgt : compare (some x) (some u) = Ordering.gt := by
        show compare x u = Ordering.gt
        exact (Int.compare_eq_gt).mpr hge
      simp [hx, hxe, hgt]


theorem biased_some_eq {w : Nat} {x : Int} {n : Nat} (h : biased w x = some n) :
    0 ≤ x + 2^(w-1) ∧ n = (x + 2^(w-1)).toNat := by
  unfold biased unsigned at h
  by_cases hneg : x + 2^(w-1) < 0
  · simp [hneg] at h
  · exact ⟨by omega, by simpa [hneg] using h.symm⟩

end TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common
