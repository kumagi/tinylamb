import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

namespace TinylambOpt.ResearchProofs.StorageExecution.RankedMasks

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option maxErrors 0
set_option linter.unusedSimpArgs false

/-! ## Generic helpers -/

private theorem fromLittle_append (xs : List Nat) (b : Nat) :
    fromLittle (xs ++ [b]) = fromLittle xs + b * 256 ^ xs.length := by
  induction xs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
    simp [fromLittle, ih, Nat.pow_succ, Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.add_assoc]
    ac_rfl

private theorem fromLittle_little (n k : Nat) : fromLittle (little n k) = n % 256 ^ k := by
  induction k with
  | zero => simp [little, fromLittle, Nat.mod_one]
  | succ k ih =>
    have hmap : (List.range (k + 1)).map (fun i => n / 256 ^ i % 256) =
        (List.range k).map (fun i => n / 256 ^ i % 256) ++ [n / 256 ^ k % 256] := by
      rw [List.range_succ, List.map_append, List.map_singleton]
    have hlen : ((List.range k).map (fun i => n / 256 ^ i % 256)).length = k := by simp
    unfold little
    rw [hmap, fromLittle_append, hlen]
    have ih' : fromLittle ((List.range k).map (fun i => n / 256 ^ i % 256)) = n % 256 ^ k := by
      simpa [little] using ih
    rw [ih']
    have h256 : (0 : Nat) < 256 := by decide
    have hpk : 0 < 256 ^ k := Nat.pow_pos h256
    have hbound : n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k < 256 ^ (k + 1) := by
      rw [Nat.pow_succ, Nat.mul_comm (256 ^ k) 256]
      have hm1 : n / 256 ^ k % 256 ≤ 255 := Nat.le_of_lt_succ (Nat.mod_lt _ h256)
      have hm2 : n % 256 ^ k ≤ 256 ^ k - 1 := Nat.le_pred_of_lt (Nat.mod_lt _ hpk)
      calc n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k
          ≤ 255 * 256 ^ k + (256 ^ k - 1) := Nat.add_le_add (Nat.mul_le_mul_right _ hm1) hm2
        _ = 256 * 256 ^ k - 1 := by omega
        _ < 256 * 256 ^ k := by omega
    -- prove n % 256^(k+1) = n / 256^k % 256 * 256^k + n % 256^k
    have hmodk : n % 256 ^ (k + 1) = n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k := by
      have hpow : 256 ^ (k + 1) = 256 * 256 ^ k := by rw [Nat.pow_succ, Nat.mul_comm]
      rw [hpow]
      have hdiv : n / (256 * 256 ^ k) = n / 256 ^ k / 256 := by
        rw [Nat.div_div_eq_div_mul]
      have h1 : n = 256 ^ k * (n / 256 ^ k) + n % 256 ^ k := (Nat.div_add_mod n (256 ^ k)).symm
      have h2 : n / 256 ^ k = 256 * (n / 256 ^ k / 256) + n / 256 ^ k % 256 := (Nat.div_add_mod (n / 256 ^ k) 256).symm
      have hdecomp : n = 256 * 256 ^ k * (n / (256 * 256 ^ k)) + (n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k) := by
        rw [hdiv, h1, h2]
        simp only [Nat.mul_add, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm, Nat.mul_assoc, Nat.mul_left_comm, Nat.mul_comm]
      have hrem_lt' : n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k < 256 * 256 ^ k := by
        simpa [hpow] using hbound
      rw [hdecomp]
      rw [Nat.mul_add_mod, Nat.mod_eq_of_lt hrem_lt']
    rw [hmodk]

private theorem word_singleton (w r : Nat) : word w [r] = r % 2 ^ w := by
  simp [word]

private theorem get_pack_singleton (w r : Nat) (hw : r < 2 ^ w) :
    TinylambOpt.ResearchGoals.Compression.Wire.get (pack w [r]) w 0 = r := by
  unfold TinylambOpt.ResearchGoals.Compression.Wire.get pack
  simp [word_singleton, Nat.mod_eq_of_lt hw]
  rw [fromLittle_little]
  have hk : w ≤ 8 * byteCount w := by
    simp [byteCount]; omega
  have hpow : 256 ^ byteCount w = 2 ^ (8 * byteCount w) := by
    have : (256 : Nat) = 2 ^ 8 := by decide
    rw [this, ← Nat.pow_mul]
  rw [hpow]
  have hr : r % 2 ^ (8 * byteCount w) = r :=
    Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le hw (Nat.pow_le_pow_right (by decide) hk))
  simp [hr, Nat.mod_eq_of_lt hw]

private theorem width_lt (n : Nat) : n < 2 ^ width n := by
  simp only [width]
  split
  · rename_i h; simp at h; subst h; decide
  · rename_i h
    have _hn : 0 < n := Nat.pos_of_ne_zero (by simpa using h)
    exact Nat.lt_log2_self

private theorem codeBits_bound (m : Machine) (n q r : Nat) (hr : r < count m n q) :
    r < 2 ^ codeBits m n q := by
  simp only [codeBits]
  have hlt : count m n q - 1 < 2 ^ width (count m n q - 1) := width_lt _
  exact Nat.lt_of_le_of_lt (Nat.le_pred_of_lt hr) hlt

/-! ## Generic spaceTest -/

private theorem byteCount_lt_iff (bits cap : Nat) :
    byteCount bits < cap ↔ 0 < cap ∧ bits ≤ 8 * (cap - 1) := by
  simp only [byteCount]
  constructor
  · intro h
    refine ⟨?_, ?_⟩
    · omega
    · omega
  · intro ⟨hcap, hbits⟩
    omega

theorem spaceTest_generic (m : Machine) : spaceTest m := by
  intro n q pages h _hpages _hcount
  have hcap : h + byteCount (pages * codeBits m n q) < pages * byteCount n ↔
      byteCount (pages * codeBits m n q) < pages * byteCount n - h := by omega
  rw [hcap]
  constructor
  · intro hlt
    constructor
    · omega
    · have := (byteCount_lt_iff (pages * codeBits m n q) (pages * byteCount n - h)).mp hlt
      omega
  · intro ⟨hh, hbits⟩
    exact (byteCount_lt_iff (pages * codeBits m n q) (pages * byteCount n - h)).mpr (by constructor <;> omega)

/-! ## Generic rank/unrank machinery -/

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

private theorem run_nil (m : Machine) (q : Nat) : run m q [] = some q := by
  simp [run]

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  simp [run, List.foldlM]

private theorem run_append (m : Machine) (q : Nat) (a b : List Bool) :
    run m q (a ++ b) = (run m q a).bind (fun q' => run m q' b) := by
  induction a generalizing q with
  | nil => simp [run]
  | cons x xs ih =>
    simp [run_cons, ih, Option.bind_assoc]

private theorem legal_nil (m : Machine) (q : Nat) : legal m q [] = m.accept q := by
  simp [legal, run]

private theorem legal_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    legal m q (b :: bs) = (((m.step q b).map (fun q' => legal m q' bs)).getD false) := by
  simp [legal, run_cons]
  cases m.step q b <;> simp

private theorem words_length : ∀ n, (words n).length = 2 ^ n
  | 0 => by simp [words]
  | n + 1 => by simp [words, words_length n, Nat.pow_succ]; omega

private theorem length_mem_words : ∀ {n : Nat} {xs : List Bool}, xs ∈ words n → xs.length = n
  | 0, xs, h => by simp [words] at h; subst h; rfl
  | n + 1, xs, h => by
    simp [words, List.mem_append, List.mem_map] at h
    rcases h with ⟨ys, hy, rfl⟩ | ⟨ys, hy, rfl⟩ <;> simp [length_mem_words hy]

private theorem mem_words_of_length : ∀ {xs : List Bool}, xs ∈ words xs.length
  | [] => by simp [words]
  | b :: bs => by
    have ih := mem_words_of_length (xs := bs)
    simp only [List.length_cons, words, List.mem_append, List.mem_map]
    cases b
    · exact Or.inl ⟨bs, ih, rfl⟩
    · exact Or.inr ⟨bs, ih, rfl⟩

private theorem sum_map_bind_step (m : Machine) (q : Nat) (b : Bool) (n k : Nat) :
    (((words n).map (fun pre => (((m.step q b).bind (fun q' => run m q' pre)).map (count m k)).getD 0)).sum) =
    (((m.step q b).map (fun q' => ((words n).map (fun pre => ((run m q' pre).map (count m k)).getD 0)).sum)).getD 0) := by
  cases h : m.step q b with
  | none =>
    simp [h]
    rw [List.sum_eq_zero]
    intro x hx
    simp at hx
    rcases hx with ⟨_, _, rfl⟩
    rfl
  | some q' => simp [h]

theorem cofactor_generic (m : Machine) : cofactor m := by
  intro q a b
  induction a generalizing q with
  | zero =>
    simp [words, run, cofactor]
  | succ a ih =>
    have hsplit : count m (a + 1 + b) q = count m (a + b + 1) q := by
      have : a + 1 + b = a + b + 1 := by omega
      rw [this]
    rw [hsplit, count_succ]
    simp only [words, List.map_append, List.sum_append, List.map_map]
    have hl : ((words a).map (fun pre => ((run m q (false :: pre)).map (count m b)).getD 0)).sum = lowCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (false :: pre)).map (count m b)).getD 0 =
          (((m.step q false).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre; rw [run_cons]
      simp_rw [h1]
      rw [sum_map_bind_step]
      simp [lowCount]
      cases h : m.step q false with
      | none => simp
      | some q' => simp [ih q']
    have hr : ((words a).map (fun pre => ((run m q (true :: pre)).map (count m b)).getD 0)).sum = hiCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (true :: pre)).map (count m b)).getD 0 =
          (((m.step q true).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre; rw [run_cons]
      simp_rw [h1]
      rw [sum_map_bind_step]
      simp [hiCount]
      cases h : m.step q true with
      | none => simp
      | some q' => simp [ih q']
    rw [hl, hr]

/-! ## Rank bounds and legality -/

private theorem rank_some_legal (m : Machine) : ∀ (xs : List Bool) (q : Nat) (r : Nat),
    rank m q xs = some r → legal m q xs = true
  | [], q, r, h => by
    unfold rank at h
    by_cases hacc : m.accept q = true
    · simp [hacc] at h
      subst h
      simp [legal, run, hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at h
  | b :: bs, q, r, h => by
    unfold rank at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some tail =>
        simp [hr, Option.bind_some] at h
        have ih := rank_some_legal m bs q' tail hr
        simpa [legal, run_cons, hs] using ih

private theorem rank_bound (m : Machine) : ∀ (xs : List Bool) (q : Nat) (r : Nat),
    rank m q xs = some r → r < count m xs.length q
  | [], q, r, h => by
    unfold rank at h
    by_cases hacc : m.accept q = true
    · simp [hacc] at h
      subst h
      simp [count, hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at h
  | b :: bs, q, r, h => by
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, count_succ]
    unfold rank at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some tail =>
        simp [hr, Option.bind_some] at h
        subst h
        have ih := rank_bound m bs q' tail hr
        cases b with
        | false =>
          simp [lowCount, hs] at ih ⊢
          omega
        | true =>
          simp [lowCount, hiCount, hs] at ih ⊢
          omega

/-! ## Unrank properties -/

private theorem unrank_length (m : Machine) : ∀ (n q r : Nat) (xs : List Bool),
    unrank m n q r = some xs → xs.length = n
  | 0, q, r, xs, h => by
    unfold unrank at h
    by_cases hcond : (r == 0 && m.accept q) = true
    · simp only [hcond, ite_true, Option.some.injEq] at h
      subst h; rfl
    · simp only [hcond, Bool.false_eq_true, ite_false, reduceCtorEq] at h
  | n + 1, q, r, xs, h => by
    unfold unrank at h
    by_cases hle : lowCount m n q ≤ r
    · have hb : decide (lowCount m n q ≤ r) = true := by simpa using hle
      simp [hb] at h
      cases hs : m.step q true with
      | none => simp [hs] at h
      | some q' =>
        simp [hs] at h
        cases hu : unrank m n q' (r - lowCount m n q) with
        | none => simp [hu] at h
        | some bs =>
          simp [hu, Option.bind_some] at h
          subst h
          simp [unrank_length m n q' (r - lowCount m n q) bs hu]
    · have hb : decide (lowCount m n q ≤ r) = false := by simpa using hle
      simp [hb] at h
      cases hs : m.step q false with
      | none => simp [hs] at h
      | some q' =>
        simp [hs] at h
        cases hu : unrank m n q' r with
        | none => simp [hu] at h
        | some bs =>
          simp [hu, Option.bind_some] at h
          subst h
          simp [unrank_length m n q' r bs hu]

private theorem unrank_sound (m : Machine) : ∀ (n q r : Nat) (xs : List Bool),
    unrank m n q r = some xs → rank m q xs = some r ∧ r < count m n q
  | 0, q, r, xs, h => by
    unfold unrank at h
    by_cases hcond : (r == 0 && m.accept q) = true
    · simp only [hcond, ite_true, Option.some.injEq] at h
      subst h
      have hcond' := hcond
      simp only [Bool.and_eq_true, beq_iff_eq] at hcond'
      rcases hcond' with ⟨hr, hacc⟩
      subst hr
      unfold rank
      simp [hacc, count]
    · simp only [hcond, Bool.false_eq_true, ite_false, reduceCtorEq] at h
  | n + 1, q, r, xs, h => by
    rw [count_succ]
    unfold unrank at h
    by_cases hle : lowCount m n q ≤ r
    · have hb : decide (lowCount m n q ≤ r) = true := by simpa using hle
      simp [hb] at h
      cases hs : m.step q true with
      | none => simp [hs] at h
      | some q' =>
        simp [hs] at h
        cases hu : unrank m n q' (r - lowCount m n q) with
        | none => simp [hu] at h
        | some bs =>
          simp [hu, Option.bind_some] at h
          subst h
          obtain ⟨hrank, hbound⟩ := unrank_sound m n q' (r - lowCount m n q) bs hu
          constructor
          · unfold rank
            simp [hs, hrank, lowCount, Option.bind_some]
            omega
          · simp [lowCount, hiCount, hs] at hbound ⊢
            omega
    · have hlt : r < lowCount m n q := Nat.lt_of_not_ge hle
      have hb : decide (lowCount m n q ≤ r) = false := by simpa using hle
      simp [hb] at h
      cases hs : m.step q false with
      | none => simp [hs] at h
      | some q' =>
        simp [hs] at h
        cases hu : unrank m n q' r with
        | none => simp [hu] at h
        | some bs =>
          simp [hu, Option.bind_some] at h
          subst h
          obtain ⟨hrank, hbound⟩ := unrank_sound m n q' r bs hu
          constructor
          · unfold rank
            simp [hs, hrank, Option.bind_some]
          · simp [lowCount, hs] at hlt hbound ⊢
            omega

private theorem unrank_none_ge (m : Machine) : ∀ (n q r : Nat),
    count m n q ≤ r → unrank m n q r = none
  | 0, q, r, h => by
    unfold unrank
    by_cases hcond : (r == 0 && m.accept q) = true
    · have hcond' := hcond
      simp only [Bool.and_eq_true, beq_iff_eq] at hcond'
      rcases hcond' with ⟨hr, hacc⟩
      subst hr
      simp [count, hacc] at h
    · simp [hcond]
  | n + 1, q, r, h => by
    rw [count_succ] at h
    unfold unrank
    have hle : lowCount m n q ≤ r := by omega
    have hb : decide (lowCount m n q ≤ r) = true := by simpa using hle
    simp [hb]
    cases hs : m.step q true with
    | none =>
      simp [hiCount, hs] at h
      omega
    | some q' =>
      simp [hiCount, hs] at h
      have hle' : count m n q' ≤ r - lowCount m n q := by omega
      have ih := unrank_none_ge m n q' (r - lowCount m n q) hle'
      simp [ih]

private theorem unrank_some_of_lt (m : Machine) : ∀ (n q r : Nat),
    r < count m n q → (unrank m n q r).isSome = true
  | 0, q, r, h => by
    unfold unrank
    simp [count] at h
    by_cases hcond : (r == 0 && m.accept q) = true
    · simp [hcond]
    · have hcond' : r ≠ 0 ∨ m.accept q = false := by
        cases hr : r <;> cases ha : m.accept q <;> simp_all
      rcases hcond' with hr0 | hacc
      · -- r != 0 but count 0 q <= 1 so r==0 forced
        cases ha : m.accept q <;> simp [ha] at h <;> omega
      · simp [hacc] at h
  | n + 1, q, r, h => by
    rw [count_succ] at h
    unfold unrank
    by_cases hle : lowCount m n q ≤ r
    · have hb : decide (lowCount m n q ≤ r) = true := by simpa using hle
      simp [hb]
      cases hs : m.step q true with
      | none =>
        simp [hiCount, hs] at h
        omega
      | some q' =>
        simp [hiCount, hs] at h
        have hlt : r - lowCount m n q < count m n q' := by omega
        have ih := unrank_some_of_lt m n q' (r - lowCount m n q) hlt
        rcases hu2 : unrank m n q' (r - lowCount m n q) with _ | bs <;> simp_all
    · have hlt : r < lowCount m n q := Nat.lt_of_not_ge hle
      have hb : decide (lowCount m n q ≤ r) = false := by simpa using hle
      simp [hb]
      cases hs : m.step q false with
      | none =>
        simp [lowCount, hs] at hlt
      | some q' =>
        simp [lowCount, hs] at hlt
        have ih := unrank_some_of_lt m n q' r hlt
        rcases hu2 : unrank m n q' r with _ | bs <;> simp_all

theorem rejectSlack_generic (m : Machine) : rejectSlack m := by
  intro n q r
  cases h : unrank m n q r with
  | none =>
    simp
    by_contra hc
    have hlt : r < count m n q := by omega
    have hs := unrank_some_of_lt m n q r hlt
    simp [h] at hs
  | some xs =>
    simp
    have ⟨_, hbound⟩ := unrank_sound m n q r xs h
    omega

theorem decoder_generic (m : Machine) : decoder m := by
  intro q xs r hrank
  induction xs generalizing q r with
  | nil =>
    unfold rank at hrank
    by_cases hacc : m.accept q = true
    · simp [hacc] at hrank
      subst hrank
      unfold unrank
      simp [hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at hrank
  | cons b bs ih =>
    unfold rank at hrank
    cases hs : m.step q b with
    | none => simp [hs] at hrank
    | some q' =>
      simp [hs] at hrank
      cases ht : rank m q' bs with
      | none => simp [ht] at hrank
      | some tail =>
        simp [ht, Option.bind_some] at hrank
        subst hrank
        have ih' := ih q' tail ht
        have hbound := rank_bound m bs q' tail ht
        have hlen : (b :: bs).length = bs.length + 1 := by simp
        unfold unrank
        simp only [hlen]
        cases b with
        | false =>
          have hlt : ¬ (lowCount m bs.length q ≤ tail) := by
            simp [lowCount, hs] at hbound ⊢
            omega
          have hdec : decide (lowCount m bs.length q ≤ tail) = false := by simpa
          simp [hdec, hs, ih', Option.bind_some]
        | true =>
          have hle : lowCount m bs.length q ≤ tail + lowCount m bs.length q := by omega
          have hdec : decide (lowCount m bs.length q ≤ tail + lowCount m bs.length q) = true := by simpa
          simp [hdec, hs, Option.bind_some]
          have : tail + lowCount m bs.length q - lowCount m bs.length q = tail := by omega
          simp [this, ih']

/-! ## Ordinal -/

private theorem filter_words_count (m : Machine) : ∀ (n q : Nat),
    ((words n).filter (legal m q)).length = count m n q
  | 0, q => by
    simp [words, count, legal, run]
    cases m.accept q <;> simp
  | n + 1, q => by
    rw [count_succ]
    simp only [words, List.filter_append, List.length_append]
    have hleft : ((words n).map (false :: ·) |>.filter (legal m q)).length = lowCount m n q := by
      rw [List.filter_map]
      have hf : (fun a => legal m q (false :: a)) = (fun a => ((m.step q false).map (fun q' => legal m q' a)).getD false) := by
        funext a; rw [legal_cons]
      rw [hf]
      cases hs : m.step q false with
      | none => simp [lowCount, hs]
      | some q' =>
        simp only [Option.map_some, Option.getD_some, lowCount, hs]
        exact filter_words_count m n q'
    have hright : ((words n).map (true :: ·) |>.filter (legal m q)).length = hiCount m n q := by
      rw [List.filter_map]
      have hf : (fun a => legal m q (true :: a)) = (fun a => ((m.step q true).map (fun q' => legal m q' a)).getD false) := by
        funext a; rw [legal_cons]
      rw [hf]
      cases hs : m.step q true with
      | none => simp [hiCount, hs]
      | some q' =>
        simp only [Option.map_some, Option.getD_some, hiCount, hs]
        exact filter_words_count m n q'
    rw [hleft, hright]

private theorem idxOf_append_left {α : Type} [BEq α] [LawfulBEq α]
    (xs ys : List α) (a : α) (ha : a ∈ xs) :
    (xs ++ ys).idxOf a = xs.idxOf a := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
    simp [List.idxOf_cons]
    cases h : (x == a) <;> simp [h]
    · have hne : x ≠ a := by simpa using h
      have ha' : a ∈ xs := by
        simp at ha
        rcases ha with rfl | ha'
        · exact absurd rfl hne
        · exact ha'
      simpa using ih ha'

private theorem idxOf_append_right {α : Type} [BEq α] [LawfulBEq α]
    (xs ys : List α) (a : α) (ha : a ∉ xs) (hay : a ∈ ys) :
    (xs ++ ys).idxOf a = xs.length + ys.idxOf a := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp [List.idxOf_cons]
    have hne : (x == a) = false := by
      by_cases hxa : x = a
      · subst hxa; simp at ha
      · simp [hxa]
    simp [hne]
    have ha' : a ∉ xs := by intro h; exact ha (List.Mem.tail _ h)
    have ih' := ih ha' hay
    simp at ih' ⊢
    omega

private theorem idxOf_cons_map {α β : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (b : β) (xs : List α) (a : α) (ha : a ∈ xs)
    (hbeq : ∀ x y : α, (b :: x == b :: y) = (x == y)) :
    (xs.map (b :: ·)).idxOf (b :: a) = xs.idxOf a := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
    simp [List.idxOf_cons, hbeq]
    cases h : (x == a) <;> simp [h]
    · have hne : x ≠ a := by simpa using h
      have ha' : a ∈ xs := by
        simp at ha; rcases ha with rfl | ha'; exact absurd rfl hne; exact ha'
      simpa using ih ha'

private theorem beq_cons_bool (b : Bool) (x y : List Bool) : (b :: x == b :: y) = (x == y) := by
  simp [BEq.beq, List.beq]

private theorem mem_filter_words_of_legal (m : Machine) (q : Nat) (xs : List Bool)
    (hleg : legal m q xs = true) : xs ∈ (words xs.length).filter (legal m q) := by
  simp [List.mem_filter]
  exact ⟨mem_words_of_length, hleg⟩

theorem ordinal_generic (m : Machine) : ordinal m := by
  intro q xs hleg
  induction xs generalizing q with
  | nil =>
    simp [rank, legal, run] at hleg ⊢
    simp [words, hleg]
  | cons b bs ih =>
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    simp only [hlen]
    rw [words]
    simp only [List.filter_append]
    have hleg' : (((m.step q b).map (fun q' => legal m q' bs)).getD false) = true := by
      simpa [legal_cons] using hleg
    cases hs : m.step q b with
    | none => simp [hs] at hleg'
    | some q' =>
      simp [hs] at hleg'
      have ih' := ih q' hleg'
      have hmem : bs ∈ (words bs.length).filter (legal m q') := mem_filter_words_of_legal m q' bs hleg'
      have hleft_filter : ((words bs.length).map (false :: ·)).filter (legal m q) =
          if b = false then ((words bs.length).filter (legal m q')).map (false :: ·) else
          ((words bs.length).filter (fun a => ((m.step q false).map (fun q0 => legal m q0 a)).getD false)).map (false :: ·) := by
        rw [List.filter_map]
        have hf : (fun a => legal m q (false :: a)) = (fun a => ((m.step q false).map (fun q0 => legal m q0 a)).getD false) := by
          funext a; rw [legal_cons]
        rw [hf]
        cases b <;> simp [hs, List.filter_map]
      have hright_filter : ((words bs.length).map (true :: ·)).filter (legal m q) =
          if b = true then ((words bs.length).filter (legal m q')).map (true :: ·) else
          ((words bs.length).filter (fun a => ((m.step q true).map (fun q0 => legal m q0 a)).getD false)).map (true :: ·) := by
        rw [List.filter_map]
        have hf : (fun a => legal m q (true :: a)) = (fun a => ((m.step q true).map (fun q0 => legal m q0 a)).getD false) := by
          funext a; rw [legal_cons]
        rw [hf]
        cases b <;> simp [hs, List.filter_map]
      cases b with
      | false =>
        simp only [hleft_filter, hright_filter, ite_true, ite_false]
        have hidx_map := idxOf_cons_map false ((words bs.length).filter (legal m q')) bs hmem (beq_cons_bool false)
        have hmem_left : (false :: bs) ∈ ((words bs.length).filter (legal m q')).map (false :: ·) := by
          simp [hmem]
        rw [idxOf_append_left _ _ _ hmem_left, hidx_map]
        simp [rank, hs, ih']
      | true =>
        simp only [hleft_filter, hright_filter, ite_true, ite_false]
        have hidx_map := idxOf_cons_map true ((words bs.length).filter (legal m q')) bs hmem (beq_cons_bool true)
        have hmem_right : (true :: bs) ∈ ((words bs.length).filter (legal m q')).map (true :: ·) := by
          simp [hmem]
        have hnot_left : (true :: bs) ∉ ((words bs.length).filter (fun a => ((m.step q false).map (fun q0 => legal m q0 a)).getD false)).map (false :: ·) := by
          simp [List.mem_map]
          intro a _ heq
          cases heq
        rw [idxOf_append_right _ _ _ hnot_left hmem_right, hidx_map]
        have hleft_len : (((words bs.length).filter (fun a => ((m.step q false).map (fun q0 => legal m q0 a)).getD false)).map (false :: ·)).length = lowCount m bs.length q := by
          simp [List.length_map]
          cases hs0 : m.step q false with
          | none => simp [lowCount, hs0]
          | some q0 =>
            simp only [Option.map_some, Option.getD_some, lowCount, hs0]
            exact filter_words_count m bs.length q0
        rw [hleft_len]
        simp [rank, hs, ih', lowCount]

/-! ## SplitCode -/

theorem splitCode_generic (m : Machine) : splitCode m := by
  intro q a b q' off hdesc
  induction a generalizing q q' off with
  | nil =>
    simp [descend] at hdesc
    rcases hdesc with ⟨rfl, rfl⟩
    simp
  | cons x xs ih =>
    have hlen : (x :: xs).length + b.length = (xs.length + b.length) + 1 := by simp; omega
    rw [hlen] at hdesc
    simp [descend] at hdesc
    cases hs : m.step q x with
    | none => simp [hs] at hdesc
    | some q1 =>
      simp [hs] at hdesc
      cases hd : descend m (xs.length + b.length) q1 xs with
      | none => simp [hd] at hdesc
      | some p =>
        rcases p with ⟨last, off1⟩
        simp [hd] at hdesc
        rcases hdesc with ⟨rfl, rfl⟩
        have ih' := ih q1 last off1 hd
        simp [List.cons_append, rank, hs]
        rw [ih']
        cases hr : rank m last b with
        | none => simp
        | some rb => simp; omega

/-! ## PrefixRange -/

private theorem descend_bound (m : Machine) : ∀ (pre : List Bool) (n q q' off : Nat),
    descend m n q pre = some (q', off) →
    pre.length ≤ n ∧ off + count m (n - pre.length) q' ≤ count m n q
  | [], n, q, q', off, h => by
    simp [descend] at h
    rcases h with ⟨rfl, rfl⟩
    simp
  | b :: bs, 0, q, q', off, h => by simp [descend] at h
  | b :: bs, n + 1, q, q', off, h => by
    simp [descend] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q1 =>
      simp [hs] at h
      cases hd : descend m n q1 bs with
      | none => simp [hd] at h
      | some p =>
        rcases p with ⟨last, off1⟩
        simp [hd] at h
        rcases h with ⟨rfl, rfl⟩
        have ih := descend_bound m bs n q1 last off1 hd
        rcases ih with ⟨hlen, hbound⟩
        constructor
        · simp; omega
        · have hsub : n + 1 - (b :: bs).length = n - bs.length := by simp; omega
          rw [hsub, count_succ]
          cases b with
          | false =>
            simp [lowCount, hs] at hbound ⊢
            omega
          | true =>
            simp [lowCount, hiCount, hs] at hbound ⊢
            omega

private theorem isPrefixOf_cons_cons (b : Bool) (bs xs : List Bool) :
    (b :: bs).isPrefixOf (b :: xs) = bs.isPrefixOf xs := by
  simp [List.isPrefixOf]

private theorem not_isPrefixOf_cons_cons_diff (b c : Bool) (bs xs : List Bool) (h : b ≠ c) :
    (b :: bs).isPrefixOf (c :: xs) = false := by
  cases b <;> cases c <;> simp [List.isPrefixOf] at h ⊢

theorem prefixRange_generic (m : Machine) : prefixRange m := by
  intro n q pre q' off r hdesc hr
  induction pre generalizing n q q' off r with
  | nil =>
    simp [descend] at hdesc
    rcases hdesc with ⟨rfl, rfl⟩
    simp [List.isPrefixOf]
    have hs := unrank_some_of_lt m n q r hr
    simp [hs, hr]
  | cons b bs ih =>
    cases n with
    | zero => simp [descend] at hdesc
    | succ n =>
      simp [descend] at hdesc
      cases hs : m.step q b with
      | none => simp [hs] at hdesc
      | some q1 =>
        simp [hs] at hdesc
        cases hd : descend m n q1 bs with
        | none => simp [hd] at hdesc
        | some p =>
          rcases p with ⟨last, off1⟩
          simp [hd] at hdesc
          rcases hdesc with ⟨rfl, rfl⟩
          have hdb := descend_bound m bs n q1 last off1 hd
          have hsub : n + 1 - (b :: bs).length = n - bs.length := by simp; omega
          have hres : count m (n + 1 - (b :: bs).length) last = count m (n - bs.length) last := by rw [hsub]
          rw [count_succ] at hr
          cases b with
          | false =>
            simp only [Nat.add_zero, hres]
            by_cases hlt : r < lowCount m n q
            · have hdec : decide (lowCount m n q ≤ r) = false := by simpa [decide_eq_false_iff_not]
              simp only [unrank, hdec]
              cases hs0 : m.step q false with
              | none => simp [lowCount, hs0] at hlt
              | some q0 =>
                have hq0 : q0 = q1 := by simp [hs0] at hs; exact hs.symm
                subst hq0
                simp [lowCount, hs0] at hlt
                have ih' := ih n q1 last off1 r hd hlt
                cases hu : unrank m n q1 r with
                | none =>
                  simp [hu] at ih' ⊢
                  simpa using ih'
                | some tail =>
                  simp [hu, isPrefixOf_cons_cons] at ih' ⊢
                  exact ih'
            · have hle : lowCount m n q ≤ r := by omega
              have hdec : decide (lowCount m n q ≤ r) = true := by simpa
              simp only [unrank, hdec]
              have hout : ¬ (off1 ≤ r ∧ r < off1 + count m (n - bs.length) last) := by
                intro ⟨_, hr2⟩
                have hbnd := hdb.2
                simp [lowCount, hs] at hbnd
                omega
              have hdec2 : decide (off1 ≤ r ∧ r < off1 + count m (n - bs.length) last) = false := by
                simpa [decide_eq_false_iff_not]
              cases hs1 : m.step q true with
              | none => simp [hdec2]
              | some q1t =>
                cases hu : unrank m n q1t (r - lowCount m n q) with
                | none => simp [hdec2]
                | some tail =>
                  simp [not_isPrefixOf_cons_cons_diff false true bs tail (by decide), hdec2]
          | true =>
            simp only [hres]
            by_cases hle : lowCount m n q ≤ r
            · have hdec : decide (lowCount m n q ≤ r) = true := by simpa
              simp only [unrank, hdec]
              cases hs1 : m.step q true with
              | none =>
                simp [hiCount, hs1] at hr
                omega
              | some q1t =>
                have hq1t : q1t = q1 := by simp [hs1] at hs; exact hs.symm
                subst hq1t
                have hr' : r - lowCount m n q < count m n q1 := by
                  simp [hiCount, hs1] at hr; omega
                have ih' := ih n q1 last off1 (r - lowCount m n q) hd hr'
                have hcond : (off1 + lowCount m n q ≤ r ∧ r < off1 + lowCount m n q + count m (n - bs.length) last) ↔
                             (off1 ≤ r - lowCount m n q ∧ r - lowCount m n q < off1 + count m (n - bs.length) last) := by
                  constructor <;> intro ⟨h1, h2⟩ <;> constructor <;> omega
                have hdec_eq : decide (off1 + lowCount m n q ≤ r ∧ r < off1 + lowCount m n q + count m (n - bs.length) last) =
                               decide (off1 ≤ r - lowCount m n q ∧ r - lowCount m n q < off1 + count m (n - bs.length) last) := by
                  by_cases hh : (off1 ≤ r - lowCount m n q ∧ r - lowCount m n q < off1 + count m (n - bs.length) last)
                  · have hh2 : (off1 + lowCount m n q ≤ r ∧ r < off1 + lowCount m n q + count m (n - bs.length) last) := hcond.mpr hh
                    simp [hh, hh2]
                  · have hh2 : ¬ (off1 + lowCount m n q ≤ r ∧ r < off1 + lowCount m n q + count m (n - bs.length) last) := by
                      intro hc; exact hh (hcond.mp hc)
                    simp [hh, hh2]
                cases hu : unrank m n q1 (r - lowCount m n q) with
                | none =>
                  simp [hu] at ih' ⊢
                  rw [hdec_eq]
                  exact ih'
                | some tail =>
                  simp [hu, isPrefixOf_cons_cons] at ih' ⊢
                  rw [hdec_eq]
                  exact ih'
            · have hlt : r < lowCount m n q := by omega
              have hdec : decide (lowCount m n q ≤ r) = false := by simpa [decide_eq_false_iff_not]
              simp only [unrank, hdec]
              have hout : ¬ (off1 + lowCount m n q ≤ r ∧ r < off1 + lowCount m n q + count m (n - bs.length) last) := by omega
              have hdec2 : decide (off1 + lowCount m n q ≤ r ∧ r < off1 + lowCount m n q + count m (n - bs.length) last) = false := by
                simpa [decide_eq_false_iff_not]
              cases hs0 : m.step q false with
              | none => simp [hdec2]
              | some q0 =>
                cases hu : unrank m n q0 r with
                | none => simp [hdec2]
                | some tail =>
                  simp [not_isPrefixOf_cons_cons_diff true false bs tail (by decide), hdec2]

/-! ## WireLookup -/

theorem wireLookup_generic (m : Machine) : wireLookup m := by
  intro q xs r hrank
  have hbound := rank_bound m xs q r hrank
  have hcb : r < 2 ^ codeBits m xs.length q := codeBits_bound m xs.length q r hbound
  rw [get_pack_singleton _ _ hcb]
  exact decoder_generic m q xs r hrank

/-! ## Per-model theorems -/

-- FixedPopcount
theorem sound_FixedPopcount_Ordinal : FixedPopcount.Ordinal.Goal := ordinal_generic FixedPopcount.model
theorem sound_FixedPopcount_Decoder : FixedPopcount.Decoder.Goal := decoder_generic FixedPopcount.model
theorem sound_FixedPopcount_RejectSlack : FixedPopcount.RejectSlack.Goal := rejectSlack_generic FixedPopcount.model
theorem sound_FixedPopcount_PrefixRange : FixedPopcount.PrefixRange.Goal := prefixRange_generic FixedPopcount.model
theorem sound_FixedPopcount_SplitCode : FixedPopcount.SplitCode.Goal := splitCode_generic FixedPopcount.model
theorem sound_FixedPopcount_WireLookup : FixedPopcount.WireLookup.Goal := wireLookup_generic FixedPopcount.model
theorem sound_FixedPopcount_SpaceTest : FixedPopcount.SpaceTest.Goal := spaceTest_generic FixedPopcount.model
theorem sound_FixedPopcount_Cofactor : FixedPopcount.Cofactor.Goal := cofactor_generic FixedPopcount.model

-- AtMostHits
theorem sound_AtMostHits_Ordinal : AtMostHits.Ordinal.Goal := ordinal_generic AtMostHits.model
theorem sound_AtMostHits_Decoder : AtMostHits.Decoder.Goal := decoder_generic AtMostHits.model
theorem sound_AtMostHits_RejectSlack : AtMostHits.RejectSlack.Goal := rejectSlack_generic AtMostHits.model
theorem sound_AtMostHits_PrefixRange : AtMostHits.PrefixRange.Goal := prefixRange_generic AtMostHits.model
theorem sound_AtMostHits_SplitCode : AtMostHits.SplitCode.Goal := splitCode_generic AtMostHits.model
theorem sound_AtMostHits_WireLookup : AtMostHits.WireLookup.Goal := wireLookup_generic AtMostHits.model
theorem sound_AtMostHits_SpaceTest : AtMostHits.SpaceTest.Goal := spaceTest_generic AtMostHits.model
theorem sound_AtMostHits_Cofactor : AtMostHits.Cofactor.Goal := cofactor_generic AtMostHits.model

-- IndependentHits
theorem sound_IndependentHits_Ordinal : IndependentHits.Ordinal.Goal := ordinal_generic IndependentHits.model
theorem sound_IndependentHits_Decoder : IndependentHits.Decoder.Goal := decoder_generic IndependentHits.model
theorem sound_IndependentHits_RejectSlack : IndependentHits.RejectSlack.Goal := rejectSlack_generic IndependentHits.model
theorem sound_IndependentHits_PrefixRange : IndependentHits.PrefixRange.Goal := prefixRange_generic IndependentHits.model
theorem sound_IndependentHits_SplitCode : IndependentHits.SplitCode.Goal := splitCode_generic IndependentHits.model
theorem sound_IndependentHits_WireLookup : IndependentHits.WireLookup.Goal := wireLookup_generic IndependentHits.model
theorem sound_IndependentHits_SpaceTest : IndependentHits.SpaceTest.Goal := spaceTest_generic IndependentHits.model
theorem sound_IndependentHits_Cofactor : IndependentHits.Cofactor.Goal := cofactor_generic IndependentHits.model

-- PairedHoles
theorem sound_PairedHoles_Ordinal : PairedHoles.Ordinal.Goal := ordinal_generic PairedHoles.model
theorem sound_PairedHoles_Decoder : PairedHoles.Decoder.Goal := decoder_generic PairedHoles.model
theorem sound_PairedHoles_RejectSlack : PairedHoles.RejectSlack.Goal := rejectSlack_generic PairedHoles.model
theorem sound_PairedHoles_PrefixRange : PairedHoles.PrefixRange.Goal := prefixRange_generic PairedHoles.model
theorem sound_PairedHoles_SplitCode : PairedHoles.SplitCode.Goal := splitCode_generic PairedHoles.model
theorem sound_PairedHoles_WireLookup : PairedHoles.WireLookup.Goal := wireLookup_generic PairedHoles.model
theorem sound_PairedHoles_SpaceTest : PairedHoles.SpaceTest.Goal := spaceTest_generic PairedHoles.model
theorem sound_PairedHoles_Cofactor : PairedHoles.Cofactor.Goal := cofactor_generic PairedHoles.model

-- PrefixOnes
theorem sound_PrefixOnes_Ordinal : PrefixOnes.Ordinal.Goal := ordinal_generic PrefixOnes.model
theorem sound_PrefixOnes_Decoder : PrefixOnes.Decoder.Goal := decoder_generic PrefixOnes.model
theorem sound_PrefixOnes_RejectSlack : PrefixOnes.RejectSlack.Goal := rejectSlack_generic PrefixOnes.model
theorem sound_PrefixOnes_PrefixRange : PrefixOnes.PrefixRange.Goal := prefixRange_generic PrefixOnes.model
theorem sound_PrefixOnes_SplitCode : PrefixOnes.SplitCode.Goal := splitCode_generic PrefixOnes.model
theorem sound_PrefixOnes_WireLookup : PrefixOnes.WireLookup.Goal := wireLookup_generic PrefixOnes.model
theorem sound_PrefixOnes_SpaceTest : PrefixOnes.SpaceTest.Goal := spaceTest_generic PrefixOnes.model
theorem sound_PrefixOnes_Cofactor : PrefixOnes.Cofactor.Goal := cofactor_generic PrefixOnes.model

-- EvenParity
theorem sound_EvenParity_Ordinal : EvenParity.Ordinal.Goal := ordinal_generic EvenParity.model
theorem sound_EvenParity_Decoder : EvenParity.Decoder.Goal := decoder_generic EvenParity.model
theorem sound_EvenParity_RejectSlack : EvenParity.RejectSlack.Goal := rejectSlack_generic EvenParity.model
theorem sound_EvenParity_PrefixRange : EvenParity.PrefixRange.Goal := prefixRange_generic EvenParity.model
theorem sound_EvenParity_SplitCode : EvenParity.SplitCode.Goal := splitCode_generic EvenParity.model
theorem sound_EvenParity_WireLookup : EvenParity.WireLookup.Goal := wireLookup_generic EvenParity.model
theorem sound_EvenParity_SpaceTest : EvenParity.SpaceTest.Goal := spaceTest_generic EvenParity.model
theorem sound_EvenParity_Cofactor : EvenParity.Cofactor.Goal := cofactor_generic EvenParity.model

-- BalancedNesting
theorem sound_BalancedNesting_Ordinal : BalancedNesting.Ordinal.Goal := ordinal_generic BalancedNesting.model
theorem sound_BalancedNesting_Decoder : BalancedNesting.Decoder.Goal := decoder_generic BalancedNesting.model
theorem sound_BalancedNesting_RejectSlack : BalancedNesting.RejectSlack.Goal := rejectSlack_generic BalancedNesting.model
theorem sound_BalancedNesting_PrefixRange : BalancedNesting.PrefixRange.Goal := prefixRange_generic BalancedNesting.model
theorem sound_BalancedNesting_SplitCode : BalancedNesting.SplitCode.Goal := splitCode_generic BalancedNesting.model
theorem sound_BalancedNesting_WireLookup : BalancedNesting.WireLookup.Goal := wireLookup_generic BalancedNesting.model
theorem sound_BalancedNesting_SpaceTest : BalancedNesting.SpaceTest.Goal := spaceTest_generic BalancedNesting.model
theorem sound_BalancedNesting_Cofactor : BalancedNesting.Cofactor.Goal := cofactor_generic BalancedNesting.model

-- RunLimit
theorem sound_RunLimit_Ordinal : RunLimit.Ordinal.Goal := ordinal_generic RunLimit.model
theorem sound_RunLimit_Decoder : RunLimit.Decoder.Goal := decoder_generic RunLimit.model
theorem sound_RunLimit_RejectSlack : RunLimit.RejectSlack.Goal := rejectSlack_generic RunLimit.model
theorem sound_RunLimit_PrefixRange : RunLimit.PrefixRange.Goal := prefixRange_generic RunLimit.model
theorem sound_RunLimit_SplitCode : RunLimit.SplitCode.Goal := splitCode_generic RunLimit.model
theorem sound_RunLimit_WireLookup : RunLimit.WireLookup.Goal := wireLookup_generic RunLimit.model
theorem sound_RunLimit_SpaceTest : RunLimit.SpaceTest.Goal := spaceTest_generic RunLimit.model
theorem sound_RunLimit_Cofactor : RunLimit.Cofactor.Goal := cofactor_generic RunLimit.model

-- SingleIsland
theorem sound_SingleIsland_Ordinal : SingleIsland.Ordinal.Goal := ordinal_generic SingleIsland.model
theorem sound_SingleIsland_Decoder : SingleIsland.Decoder.Goal := decoder_generic SingleIsland.model
theorem sound_SingleIsland_RejectSlack : SingleIsland.RejectSlack.Goal := rejectSlack_generic SingleIsland.model
theorem sound_SingleIsland_PrefixRange : SingleIsland.PrefixRange.Goal := prefixRange_generic SingleIsland.model
theorem sound_SingleIsland_SplitCode : SingleIsland.SplitCode.Goal := splitCode_generic SingleIsland.model
theorem sound_SingleIsland_WireLookup : SingleIsland.WireLookup.Goal := wireLookup_generic SingleIsland.model
theorem sound_SingleIsland_SpaceTest : SingleIsland.SpaceTest.Goal := spaceTest_generic SingleIsland.model
theorem sound_SingleIsland_Cofactor : SingleIsland.Cofactor.Goal := cofactor_generic SingleIsland.model

-- Cooldown
theorem sound_Cooldown_Ordinal : Cooldown.Ordinal.Goal := ordinal_generic Cooldown.model
theorem sound_Cooldown_Decoder : Cooldown.Decoder.Goal := decoder_generic Cooldown.model
theorem sound_Cooldown_RejectSlack : Cooldown.RejectSlack.Goal := rejectSlack_generic Cooldown.model
theorem sound_Cooldown_PrefixRange : Cooldown.PrefixRange.Goal := prefixRange_generic Cooldown.model
theorem sound_Cooldown_SplitCode : Cooldown.SplitCode.Goal := splitCode_generic Cooldown.model
theorem sound_Cooldown_WireLookup : Cooldown.WireLookup.Goal := wireLookup_generic Cooldown.model
theorem sound_Cooldown_SpaceTest : Cooldown.SpaceTest.Goal := spaceTest_generic Cooldown.model
theorem sound_Cooldown_Cofactor : Cooldown.Cofactor.Goal := cofactor_generic Cooldown.model

-- FixedPopcount model-specific

private theorem fp_count_succ (n q : Nat) :
    count FixedPopcount.model (n + 1) q =
      count FixedPopcount.model n q + if q = 0 then 0 else count FixedPopcount.model n (q - 1) := by
  rw [count_succ]
  simp [lowCount, hiCount, FixedPopcount.model]

private theorem fp_count_zero_right (n : Nat) : count FixedPopcount.model n 0 = 1 := by
  induction n with
  | zero => simp [count, FixedPopcount.model]
  | succ n ih => rw [fp_count_succ, ih]; simp

private theorem fp_count_gt (n q : Nat) (h : n < q) : count FixedPopcount.model n q = 0 := by
  induction n generalizing q with
  | zero =>
    simp [count, FixedPopcount.model]
    omega
  | succ n ih =>
    rw [fp_count_succ]
    have h1 : count FixedPopcount.model n q = 0 := ih q (by omega)
    have h2 : (if q = 0 then 0 else count FixedPopcount.model n (q - 1)) = 0 := by
      split
      · rfl
      · rename_i hq
        exact ih (q - 1) (by omega)
    omega

private theorem fp_count_symm (n k : Nat) (hk : k ≤ n) :
    count FixedPopcount.model n k = count FixedPopcount.model n (n - k) := by
  induction n generalizing k with
  | zero =>
    have : k = 0 := by omega
    subst this; simp
  | succ n ih =>
    by_cases hk0 : k = 0
    · subst hk0
      simp
      rw [fp_count_zero_right]
      have hnn : count FixedPopcount.model (n + 1) (n + 1) = 1 := by
        rw [fp_count_succ]
        have h1 : count FixedPopcount.model n (n + 1) = 0 := fp_count_gt n (n + 1) (by omega)
        have h2 : count FixedPopcount.model n n = 1 := by
          have := ih n (Nat.le_refl n)
          simp at this
          rw [this, fp_count_zero_right]
        simp [h1, h2]
      exact hnn.symm
    · by_cases hkn : k = n + 1
      · subst hkn
        simp
        rw [fp_count_zero_right]
        have hnn : count FixedPopcount.model (n + 1) (n + 1) = 1 := by
          rw [fp_count_succ]
          have h1 : count FixedPopcount.model n (n + 1) = 0 := fp_count_gt n (n + 1) (by omega)
          have h2 : count FixedPopcount.model n n = 1 := by
            have := ih n (Nat.le_refl n)
            simp at this
            rw [this, fp_count_zero_right]
          simp [h1, h2]
        exact hnn
      · have hk' : k ≤ n := by omega
        rw [fp_count_succ]
        have hterm : (if k = 0 then 0 else count FixedPopcount.model n (k - 1)) = count FixedPopcount.model n (k - 1) := by
          simp [hk0]
        rw [hterm]
        have hnk : n + 1 - k ≠ 0 := by omega
        rw [fp_count_succ]
        have hterm2 : (if n + 1 - k = 0 then 0 else count FixedPopcount.model n (n + 1 - k - 1)) = count FixedPopcount.model n (n - k) := by
          simp [hnk]
          congr 1; omega
        rw [hterm2]
        have ih1 := ih k hk'
        have ih2 := ih (k - 1) (by omega)
        have hsub : n - (k - 1) = n + 1 - k := by omega
        rw [hsub] at ih2
        omega

theorem sound_FixedPopcount_ComplementSymmetry : FixedPopcount.ComplementSymmetry.Goal := by
  intro n k hk
  simp [FixedPopcount.ComplementSymmetry.Goal, codeBits]
  rw [fp_count_symm n k hk]

private theorem fp_count_eq_choose (n k : Nat) :
    count FixedPopcount.model n k = Samples.choose n k := by
  induction n generalizing k with
  | zero =>
    cases k with
    | zero => simp [count, FixedPopcount.model, Samples.choose]
    | succ k => simp [count, FixedPopcount.model, Samples.choose]
  | succ n ih =>
    cases k with
    | zero =>
      rw [fp_count_succ]
      simp [ih, Samples.choose, fp_count_zero_right]
    | succ k =>
      rw [fp_count_succ]
      simp [ih, Samples.choose]
      omega

private theorem fp_legal_count (xs : List Bool) :
    legal FixedPopcount.model (xs.count true) xs = true := by
  induction xs with
  | nil => simp [legal, run, FixedPopcount.model]
  | cons b bs ih =>
    cases b with
    | false =>
      simp [legal, run_cons, FixedPopcount.model, ih]
    | true =>
      simp [List.count_cons]
      have hstep : FixedPopcount.model.step (bs.count true + 1) true = some (bs.count true) := by
        simp [FixedPopcount.model]
      simp [legal, run_cons, hstep, ih]

theorem sound_FixedPopcount_PopulationFrame : FixedPopcount.PopulationFrame.Goal := by
  intro xs _hlen
  simp only [FixedPopcount.PopulationFrame.Goal]
  have hleg : legal FixedPopcount.model (xs.count true) xs = true := fp_legal_count xs
  have hrank := ordinal_generic FixedPopcount.model (xs.count true) xs hleg
  cases hr : rank FixedPopcount.model (xs.count true) xs with
  | none => simp [hrank] at hr
  | some r =>
    simp only [hr, Option.getD_some, List.head?_cons, List.drop_succ_cons, List.drop_zero]
    have hdec := decoder_generic FixedPopcount.model (xs.count true) xs r hr
    have hcount : count FixedPopcount.model xs.length (xs.count true) =
        Samples.choose xs.length (xs.count true) := fp_count_eq_choose xs.length (xs.count true)
    have hcb : codeBits FixedPopcount.model xs.length (xs.count true) =
        width (Samples.choose xs.length (xs.count true) - 1) := by
      simp [codeBits, hcount]
    constructor
    · rw [hcb, fromLittle_little]
      have hcbound : r < 2 ^ codeBits FixedPopcount.model xs.length (xs.count true) :=
        codeBits_bound FixedPopcount.model xs.length (xs.count true) r (by rwa [← hcount])
      have hbc : codeBits FixedPopcount.model xs.length (xs.count true) ≤
          8 * byteCount (codeBits FixedPopcount.model xs.length (xs.count true)) := by
        simp [byteCount]; omega
      have hpow : 256 ^ byteCount (codeBits FixedPopcount.model xs.length (xs.count true)) =
          2 ^ (8 * byteCount (codeBits FixedPopcount.model xs.length (xs.count true))) := by
        have : (256 : Nat) = 2 ^ 8 := by decide
        rw [this, ← Nat.pow_mul]
      have hmod : r % 256 ^ byteCount (codeBits FixedPopcount.model xs.length (xs.count true)) = r := by
        rw [hpow]
        exact Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le hcbound (Nat.pow_le_pow_right (by decide) hbc))
      simp [hmod, hcb]
      exact hdec
    · simp [little, List.length_cons, List.length_map, List.length_range, hcb]

-- AtMostHits model-specific

private theorem am_count_zero (q : Nat) : count AtMostHits.model 0 q = 1 := by
  simp [count, AtMostHits.model]

private theorem am_count_succ (n q : Nat) :
    count AtMostHits.model (n + 1) q =
      count AtMostHits.model n q + if q = 0 then 0 else count AtMostHits.model n (q - 1) := by
  rw [count_succ]
  simp [lowCount, hiCount, AtMostHits.model]

private theorem am_count_budget0 (n : Nat) : count AtMostHits.model n 0 = 1 := by
  induction n with
  | zero => simp [am_count_zero]
  | succ n ih => rw [am_count_succ, ih]; simp

theorem sound_AtMostHits_OneOutlier : AtMostHits.OneOutlier.Goal := by
  intro n
  induction n with
  | zero => simp [am_count_zero]
  | succ n ih => rw [am_count_succ, am_count_budget0]; omega

theorem sound_AtMostHits_UnrestrictedBudget : AtMostHits.UnrestrictedBudget.Goal := by
  intro n q hq
  induction n generalizing q with
  | zero => simp [am_count_zero]
  | succ n ih =>
    have hq1 : n ≤ q := by omega
    have hq2 : n ≤ q - 1 := by omega
    rw [am_count_succ]
    have hqne : q ≠ 0 := by omega
    simp [hqne, ih q hq1, ih (q - 1) hq2, Nat.pow_succ]
    omega

-- IndependentHits model-specific

private theorem ih_count_succ_0 (n : Nat) :
    count IndependentHits.model (n + 1) 0 =
      count IndependentHits.model n 0 + count IndependentHits.model n 1 := by
  rw [count_succ]; simp [lowCount, hiCount, IndependentHits.model]

private theorem ih_count_succ_1 (n : Nat) :
    count IndependentHits.model (n + 1) 1 = count IndependentHits.model n 0 := by
  rw [count_succ]; simp [lowCount, hiCount, IndependentHits.model]

theorem sound_IndependentHits_CheapConcatenation : IndependentHits.CheapConcatenation.Goal := by
  intro q a b hla hlb
  simp [legal, IndependentHits.model] at hla hlb ⊢
  rw [run_append, run_append]
  cases ha : run IndependentHits.model q a with
  | none => simp [ha] at hla
  | some qa =>
    simp [ha]
    simp [run, List.foldlM, IndependentHits.model]
    cases hb : run IndependentHits.model 0 b with
    | none => simp [hb] at hlb
    | some qb => simp [hb]

theorem sound_IndependentHits_FibonacciCount : IndependentHits.FibonacciCount.Goal := by
  intro n
  rw [ih_count_succ_0]
  congr 1
  rw [ih_count_succ_1]

-- PairedHoles model-specific

private theorem ph_run_ff0 : run PairedHoles.model 0 [false, false] = some 0 := by
  simp [run, List.foldlM, PairedHoles.model]

private theorem ph_run_ff1 (q : Nat) : run PairedHoles.model (q + 1) [false, false] = some (q + 1) := by
  simp [run, List.foldlM, PairedHoles.model]
  cases q <;> simp [Nat.sub_sub]

theorem sound_PairedHoles_EraseZeroPair : PairedHoles.EraseZeroPair.Goal := by
  intro a b
  simp only [PairedHoles.EraseZeroPair.Goal, legal]
  rw [run_append, run_append]
  cases ha : run PairedHoles.model 0 a with
  | none => simp
  | some qa =>
    simp only [Option.bind_some]
    cases qa with
    | zero =>
      have h1 : run PairedHoles.model 0 ([false, false] ++ b) = run PairedHoles.model 0 b := by
        rw [run_append, ph_run_ff0]; simp
      rw [h1]
    | succ qn =>
      have h1 : run PairedHoles.model (qn + 1) ([false, false] ++ b) = run PairedHoles.model (qn + 1) b := by
        rw [run_append, ph_run_ff1 qn]; simp
      rw [h1]

private theorem ph_run_replicate_false : ∀ n q, q < 2 →
    run PairedHoles.model q (List.replicate n false) = some ((q + n) % 2)
  | 0, q, _ => by simp [run]
  | n + 1, q, hq => by
    rw [List.replicate_succ, run_append]
    have hstep : run PairedHoles.model q [false] = some ((1 - q) % 2 + q % 2 - q % 2 + (1-q)) := by
      simp [run, List.foldlM, PairedHoles.model]
    -- simpler: compute directly
    have hstep' : run PairedHoles.model q [false] = some (1 - q) := by
      simp [run, List.foldlM, PairedHoles.model]
    rw [hstep']
    simp only [Option.bind_some]
    have hq1 : (1 - q) < 2 := by omega
    have ih := ph_run_replicate_false n (1 - q) hq1
    rw [ih]
    congr 1
    omega

theorem sound_PairedHoles_EvenTail : PairedHoles.EvenTail.Goal := by
  intro n
  simp only [PairedHoles.EvenTail.Goal, legal]
  rw [ph_run_replicate_false n 0 (by omega)]
  simp [PairedHoles.model]
  cases h : n % 2 <;> simp [h]

-- PrefixOnes model-specific

private theorem po_count_succ_0 (n : Nat) :
    count PrefixOnes.model (n + 1) 0 = count PrefixOnes.model n 1 + count PrefixOnes.model n 0 := by
  rw [count_succ]; simp [lowCount, hiCount, PrefixOnes.model]

private theorem po_count_succ_1 (n : Nat) :
    count PrefixOnes.model (n + 1) 1 = count PrefixOnes.model n 1 := by
  rw [count_succ]; simp [lowCount, hiCount, PrefixOnes.model]

private theorem po_count_1 (n : Nat) : count PrefixOnes.model n 1 = 1 := by
  induction n with
  | zero => simp [count, PrefixOnes.model]
  | succ n ih => rw [po_count_succ_1, ih]

theorem sound_PrefixOnes_CutCount : PrefixOnes.CutCount.Goal := by
  intro n
  induction n with
  | zero => simp [count, PrefixOnes.model]
  | succ n ih => rw [po_count_succ_0, po_count_1, ih]; omega

private theorem po_rank_ones_zeros : ∀ k m, k ≤ m →
    rank PrefixOnes.model 0 (List.replicate k true ++ List.replicate (m - k) false) = some k
  | 0, m, _ => by
    simp [List.replicate]
    induction m with
    | zero => simp [rank, PrefixOnes.model]
    | succ m ih =>
      rw [List.replicate_succ]
      simp [rank, PrefixOnes.model]
      have h1 : rank PrefixOnes.model 1 (List.replicate m false) = some 0 := by
        induction m with
        | zero => simp [rank, PrefixOnes.model]
        | succ m ih2 => rw [List.replicate_succ]; simp [rank, PrefixOnes.model, ih2, lowCount]
      simp [h1, lowCount]
  | k + 1, m, hm => by
    have hlist : List.replicate (k + 1) true ++ List.replicate (m - (k + 1)) false =
                 true :: (List.replicate k true ++ List.replicate ((m - 1) - k) false) := by
      simp [List.replicate_succ]; congr 1; congr 1; omega
    rw [hlist]
    simp [rank, PrefixOnes.model]
    have ih' := po_rank_ones_zeros k (m - 1) (by omega)
    rw [ih']
    simp [lowCount, PrefixOnes.model, po_count_1]

theorem sound_PrefixOnes_CutOrdinal : PrefixOnes.CutOrdinal.Goal := by
  intro n k hk
  exact po_rank_ones_zeros k n hk

-- EvenParity model-specific

private theorem ep_count_succ (n q : Nat) (hq : q < 2) :
    count EvenParity.model (n + 1) q = count EvenParity.model n 0 + count EvenParity.model n 1 := by
  rw [count_succ]
  simp [lowCount, hiCount, EvenParity.model]
  have hq0 : q % 2 = q := Nat.mod_eq_of_lt hq
  have hq1 : (q + 1) % 2 = 1 - q := by omega
  simp [hq0, hq1]
  omega

private theorem ep_count_sum (n : Nat) : count EvenParity.model n 0 + count EvenParity.model n 1 = 2 ^ n := by
  induction n with
  | zero => simp [count, EvenParity.model]
  | succ n ih =>
    have h0 := ep_count_succ n 0 (by omega)
    have h1 := ep_count_succ n 1 (by omega)
    rw [h0, h1, ih]
    simp [Nat.pow_succ]
    omega

theorem sound_EvenParity_ImpliedLastBit : EvenParity.ImpliedLastBit.Goal := by
  intro n q hq
  rw [ep_count_succ n q hq]
  exact ep_count_sum n

private theorem ep_count_q (n q : Nat) (hq : q < 2) (hn : 0 < n) :
    count EvenParity.model n q = 2 ^ (n - 1) := by
  cases n with
  | zero => omega
  | succ n =>
    simp
    rw [ep_count_succ n q hq, ep_count_sum]

private theorem ep_lowCount (n q : Nat) (hq : q < 2) :
    lowCount EvenParity.model n q = count EvenParity.model n q := by
  simp [lowCount, EvenParity.model, Nat.mod_eq_of_lt hq]

private theorem ep_legal_iff (q : Nat) (xs : List Bool) (hq : q < 2) :
    legal EvenParity.model q xs = true ↔ (q + xs.foldl (fun a b => a + b.toNat) 0) % 2 = 0 := by
  induction xs generalizing q with
  | nil =>
    simp [legal, run, EvenParity.model]
    omega
  | cons b bs ih =>
    simp [legal, run_cons, EvenParity.model]
    have hq1 : (q + b.toNat) % 2 < 2 := Nat.mod_lt _ (by decide)
    rw [ih ((q + b.toNat) % 2) hq1]
    constructor <;> intro h <;> { simp [List.foldl] at h ⊢; omega }

theorem sound_EvenParity_ComplementOrder : EvenParity.ComplementOrder.Goal := by
  intro xs hlen hleg
  have hgen : ∀ (xs : List Bool) (q : Nat), q < 2 →
      legal EvenParity.model q xs = true →
      (rank EvenParity.model q xs).getD 0 +
        (rank EvenParity.model ((q + xs.length) % 2) (xs.map (! ·))).getD 0 =
        count EvenParity.model xs.length q - 1 := by
    intro xs
    induction xs with
    | nil =>
      intro q hq hleg
      simp [legal, run, EvenParity.model] at hleg
      subst hleg
      simp [rank, count, EvenParity.model]
    | cons b bs ih =>
      intro q hq hleg
      have hqb : (q + b.toNat) % 2 < 2 := Nat.mod_lt _ (by decide)
      have hlegb : legal EvenParity.model ((q + b.toNat) % 2) bs = true := by
        simp [legal, run_cons, EvenParity.model] at hleg; exact hleg
      have ih' := ih ((q + b.toNat) % 2) hqb hlegb
      have hr1 : rank EvenParity.model q (b :: bs) =
          (rank EvenParity.model ((q + b.toNat) % 2) bs).map (· + if b then lowCount EvenParity.model bs.length q else 0) := by
        simp [rank, EvenParity.model]
      have hr2 : rank EvenParity.model ((q + (b :: bs).length) % 2) ((!b) :: bs.map (! ·)) =
          (rank EvenParity.model ((((q + (b :: bs).length) % 2) + (!b).toNat) % 2) (bs.map (! ·))).map
            (· + if !b then lowCount EvenParity.model bs.length ((q + (b :: bs).length) % 2) else 0) := by
        simp [rank, EvenParity.model, List.map]
      have hstate : (((q + (b :: bs).length) % 2) + (!b).toNat) % 2 = (((q + b.toNat) % 2) + bs.length) % 2 := by
        simp [List.length_cons]
        cases b <;> simp <;> omega
      rw [hstate] at hr2
      cases hrb : rank EvenParity.model ((q + b.toNat) % 2) bs with
      | none =>
        have ho := ordinal_generic EvenParity.model ((q + b.toNat) % 2) bs hlegb
        simp [hrb] at ho
      | some rb =>
        have hlegc_bs : legal EvenParity.model ((((q + b.toNat) % 2) + bs.length) % 2) (bs.map (! ·)) = true := by
          rw [ep_legal_iff _ _ (Nat.mod_lt _ (by decide))]
          rw [ep_legal_iff _ _ hqb] at hlegb
          have hsum : bs.foldl (fun a b => a + b.toNat) 0 + (bs.map (! ·)).foldl (fun a b => a + b.toNat) 0 = bs.length := by
            have hgen2 : ∀ (xs : List Bool) (acc acc2 : Nat),
                xs.foldl (fun a b => a + b.toNat) acc + (xs.map (! ·)).foldl (fun a b => a + b.toNat) acc2 =
                acc + acc2 + xs.length := by
              intro xs acc acc2
              induction xs generalizing acc acc2 with
              | nil => simp
              | cons c cs ih2 =>
                simp [List.foldl, List.map]
                have hc : c.toNat + (!c).toNat = 1 := by cases c <;> simp
                have ih3 := ih2 (acc + c.toNat) (acc2 + (!c).toNat)
                omega
            have := hgen2 bs 0 0
            simpa using this
          omega
        cases hrc : rank EvenParity.model ((((q + b.toNat) % 2) + bs.length) % 2) (bs.map (! ·)) with
        | none =>
          have ho := ordinal_generic EvenParity.model ((((q + b.toNat) % 2) + bs.length) % 2) (bs.map (! ·)) hlegc_bs
          simp [hrc] at ho
        | some rc =>
          simp [hr1, hr2, hrb, hrc]
          simp [hrb, hrc] at ih'
          have hlq : lowCount EvenParity.model bs.length q = count EvenParity.model bs.length q := ep_lowCount bs.length q hq
          have hqcl : (q + (b :: bs).length) % 2 < 2 := Nat.mod_lt _ (by decide)
          have hlqc : lowCount EvenParity.model bs.length ((q + (b :: bs).length) % 2) = count EvenParity.model bs.length ((q + (b :: bs).length) % 2) :=
            ep_lowCount bs.length _ hqcl
          have hcount_eq : count EvenParity.model bs.length ((q + (b :: bs).length) % 2) = count EvenParity.model bs.length q := by
            by_cases hn : bs.length = 0
            · subst hn; simp [count, EvenParity.model]; omega
            · rw [ep_count_q bs.length q hq (Nat.pos_of_ne_zero hn),
                  ep_count_q bs.length _ hqcl (Nat.pos_of_ne_zero hn)]
          rw [hlq, hlqc, hcount_eq]
          have hcount_len : count EvenParity.model (b :: bs).length q = count EvenParity.model bs.length q + count EvenParity.model bs.length q := by
            rw [show (b :: bs).length = bs.length + 1 by simp, count_succ]
            simp [lowCount, hiCount, EvenParity.model, Nat.mod_eq_of_lt hq]
            have h1 : (q + 1) % 2 < 2 := Nat.mod_lt _ (by decide)
            by_cases hn : bs.length = 0
            · subst hn; simp [count, EvenParity.model]; omega
            · rw [ep_count_q bs.length q hq (Nat.pos_of_ne_zero hn),
                  ep_count_q bs.length ((q+1)%2) h1 (Nat.pos_of_ne_zero hn)]
          rw [hcount_len]
          cases b <;> simp at ih' ⊢ <;> omega
  have hres := hgen xs 0 (by omega) hleg
  simp [hlen] at hres
  exact hres

-- BalancedNesting model-specific

private theorem bn_count_gt (n q : Nat) (h : n < q) : count BalancedNesting.model n q = 0 := by
  induction n generalizing q with
  | zero =>
    simp [count, BalancedNesting.model]
    omega
  | succ n ih =>
    rw [count_succ]
    have hlow : lowCount BalancedNesting.model n q = 0 := by
      simp [lowCount, BalancedNesting.model]
      split
      · rfl
      · rename_i hq
        have : n < q - 1 := by omega
        exact ih (q - 1) this
    have hhi : hiCount BalancedNesting.model n q = 0 := by
      simp [hiCount, BalancedNesting.model]
      exact ih n (q + 1) (by omega)
    omega

theorem sound_BalancedNesting_DepthCap : BalancedNesting.DepthCap.Goal := by
  intro n q h
  exact bn_count_gt n q h

private theorem bn_count_odd (n : Nat) : count BalancedNesting.model (2 * n + 1) 0 = 0 := by
  -- odd length can't return to depth 0
  -- prove by showing: count m (2*n+1) 0 = 0 and also more generally count at odd parity
  -- simpler: strong induction on n
  induction n with
  | zero =>
    simp [count, lowCount, hiCount, BalancedNesting.model]
  | succ n ih =>
    -- count (2*(n+1)+1) 0 = count (2*n+3) 0
    -- = lowCount (2*n+2) 0 + hiCount (2*n+2) 0
    -- lowCount: step 0 false = none, so 0
    -- hiCount: step 0 true = some 1, so count (2*n+2) 1
    -- count (2*n+2) 1 = lowCount (2*n+1) 1 + hiCount (2*n+1) 1
    -- lowCount: step 1 false = some 0, count (2*n+1) 0 = 0 by ih
    -- hiCount: step 1 true = some 2, count (2*n+1) 2
    -- need count (2*n+1) 2 = 0 ... this gets complicated.
    -- Use general lemma: count m k q = 0 if k%2 != q%2? Or (k+q) odd?
    -- Balanced nesting: moving by 1 changes depth by ±1, so parity of (depth) == parity of (remaining steps) needed.
    -- Actually count m k q > 0 implies (k+q) is even. Contrapositive: (k+q) odd -> count=0.
    -- (2*n+1 + 0) is odd. Yes.
    have hgen : ∀ k q, (k + q) % 2 = 1 → count BalancedNesting.model k q = 0 := by
      intro k
      induction k with
      | zero =>
        intro q h
        simp [count, BalancedNesting.model]
        omega
      | succ k ih2 =>
        intro q h
        rw [count_succ]
        have hlow : lowCount BalancedNesting.model k q = 0 := by
          simp [lowCount, BalancedNesting.model]
          split
          · rfl
          · rename_i hq
            apply ih2
            have : q - 1 + k = k + q - 1 := by omega
            omega
        have hhi : hiCount BalancedNesting.model k q = 0 := by
          simp [hiCount, BalancedNesting.model]
          apply ih2
          omega
        omega
    exact hgen (2 * n + 1) 0 (by omega)

theorem sound_BalancedNesting_OddLengthEmpty : BalancedNesting.OddLengthEmpty.Goal := by
  intro n
  exact bn_count_odd n

-- RunLimit model-specific

theorem sound_RunLimit_EraseSeparator : RunLimit.EraseSeparator.Goal := by
  intro a b hla hlb
  simp [legal, RunLimit.model] at hla hlb ⊢
  rw [run_append, run_append]
  cases ha : run RunLimit.model 0 a with
  | none => simp [ha] at hla
  | some qa =>
    simp [ha]
    have hstep : RunLimit.model.step qa false = some 0 := by simp [RunLimit.model]
    simp [run, List.foldlM, hstep]
    cases hb : run RunLimit.model 0 b with
    | none => simp [hb] at hlb
    | some qb => simp [hb]

private theorem rl_count_succ_0 (n : Nat) :
    count RunLimit.model (n + 1) 0 = count RunLimit.model n 0 + count RunLimit.model n 1 := by
  rw [count_succ]; simp [lowCount, hiCount, RunLimit.model]

private theorem rl_count_succ_1 (n : Nat) :
    count RunLimit.model (n + 1) 1 = count RunLimit.model n 0 + count RunLimit.model n 2 := by
  rw [count_succ]; simp [lowCount, hiCount, RunLimit.model]

private theorem rl_count_succ_2 (n : Nat) :
    count RunLimit.model (n + 1) 2 = count RunLimit.model n 0 := by
  rw [count_succ]; simp [lowCount, hiCount, RunLimit.model]

theorem sound_RunLimit_TribonacciCount : RunLimit.TribonacciCount.Goal := by
  intro n
  -- count(n+3,0) = count(n+2,0)+count(n+2,1) = count(n+2,0)+count(n+1,0)+count(n+1,2) = count(n+2,0)+count(n+1,0)+count(n,0)
  rw [rl_count_succ_0, rl_count_succ_1, rl_count_succ_2]
  omega

-- SingleIsland model-specific

theorem sound_SingleIsland_EmptyFirst : SingleIsland.EmptyFirst.Goal := by
  intro n
  induction n with
  | zero => simp [unrank, SingleIsland.model, count]
  | succ n ih =>
    simp [unrank, List.replicate_succ]
    -- lowCount m n 0 = count m n 0 (step 0 false = some 0)
    have hlow : lowCount SingleIsland.model n 0 = count SingleIsland.model n 0 := by
      simp [lowCount, SingleIsland.model]
    have hpos : 0 < count SingleIsland.model n 0 := by
      -- count is always >= 1 because empty word is always legal
      have hleg : legal SingleIsland.model 0 (List.replicate n false) = true := by
        induction n with
        | zero => simp [legal, run, SingleIsland.model]
        | succ n ih2 =>
          rw [List.replicate_succ]
          simp [legal, run_cons, SingleIsland.model, ih2]
      have hrank := ordinal_generic SingleIsland.model 0 (List.replicate n false) hleg
      cases hr : rank SingleIsland.model 0 (List.replicate n false) with
      | none => simp [hrank] at hr
      | some r =>
        have hb := rank_bound SingleIsland.model _ _ _ hr
        simp [List.length_replicate] at hb
        omega
    have hdec : decide (lowCount SingleIsland.model n 0 ≤ 0) = false := by
      rw [hlow]
      simp [Nat.not_le.mpr hpos]
    simp [hdec, SingleIsland.model, ih]

private theorem si_count_succ_0 (n : Nat) :
    count SingleIsland.model (n + 1) 0 = count SingleIsland.model n 0 + count SingleIsland.model n 1 := by
  rw [count_succ]; simp [lowCount, hiCount, SingleIsland.model]

private theorem si_count_succ_1 (n : Nat) :
    count SingleIsland.model (n + 1) 1 = count SingleIsland.model n 2 + count SingleIsland.model n 1 := by
  rw [count_succ]; simp [lowCount, hiCount, SingleIsland.model]

private theorem si_count_succ_2 (n : Nat) :
    count SingleIsland.model (n + 1) 2 = count SingleIsland.model n 2 := by
  rw [count_succ]; simp [lowCount, hiCount, SingleIsland.model]

private theorem si_count_2 (n : Nat) : count SingleIsland.model n 2 = 1 := by
  induction n with
  | zero => simp [count, SingleIsland.model]
  | succ n ih => rw [si_count_succ_2, ih]

private theorem si_count_1 (n : Nat) : count SingleIsland.model n 1 = n + 1 := by
  induction n with
  | zero => simp [count, SingleIsland.model]
  | succ n ih => rw [si_count_succ_1, si_count_2, ih]; omega

theorem sound_SingleIsland_IntervalsAndEmpty : SingleIsland.IntervalsAndEmpty.Goal := by
  intro n
  induction n with
  | zero => simp [count, SingleIsland.model]
  | succ n ih =>
    rw [si_count_succ_0, si_count_1, ih]
    -- goal: 1 + n*(n+1)/2 + (n+1) = 1 + (n+1)*(n+2)/2
    have h1 : n * (n + 1) / 2 + (n + 1) = (n + 1) * (n + 2) / 2 := by
      have heven : n * (n + 1) % 2 = 0 := by omega
      have heven2 : (n + 1) * (n + 2) % 2 = 0 := by omega
      have hmul1 : n * (n + 1) / 2 * 2 = n * (n + 1) := by
        have := Nat.div_add_mod (n * (n + 1)) 2
        omega
      have hmul2 : (n + 1) * (n + 2) / 2 * 2 = (n + 1) * (n + 2) := by
        have := Nat.div_add_mod ((n + 1) * (n + 2)) 2
        omega
      have h2 : n * (n + 1) + 2 * (n + 1) = (n + 1) * (n + 2) := by
        have h3 : n * (n + 1) + 2 * (n + 1) = (n + 2) * (n + 1) := by
          rw [Nat.add_mul]
        rw [h3, Nat.mul_comm]
      -- both sides * 2 equal
      have hlhs : (n * (n + 1) / 2 + (n + 1)) * 2 = (n + 1) * (n + 2) := by
        rw [Nat.add_mul, hmul1]
        omega
      have hrhs : (n + 1) * (n + 2) / 2 * 2 = (n + 1) * (n + 2) := hmul2
      omega
    omega

-- Cooldown model-specific

theorem sound_Cooldown_CheckpointConvergence : Cooldown.CheckpointConvergence.Goal := by
  intro q hq
  match q with
  | 0 => simp [run, List.foldlM, Cooldown.model]
  | 1 => simp [run, List.foldlM, Cooldown.model]
  | 2 => simp [run, List.foldlM, Cooldown.model]
  | n + 3 => omega

private theorem cd_count_succ_0 (n : Nat) :
    count Cooldown.model (n + 1) 0 = count Cooldown.model n 0 + count Cooldown.model n 2 := by
  rw [count_succ]; simp [lowCount, hiCount, Cooldown.model, Nat.sub_zero]

private theorem cd_count_succ_1 (n : Nat) :
    count Cooldown.model (n + 1) 1 = count Cooldown.model n 0 := by
  rw [count_succ]; simp [lowCount, hiCount, Cooldown.model]

private theorem cd_count_succ_2 (n : Nat) :
    count Cooldown.model (n + 1) 2 = count Cooldown.model n 1 := by
  rw [count_succ]; simp [lowCount, hiCount, Cooldown.model]

theorem sound_Cooldown_SpacingRecurrence : Cooldown.SpacingRecurrence.Goal := by
  intro n
  -- count(n+3,0) = count(n+2,0) + count(n+2,2) = count(n+2,0) + count(n+1,1) = count(n+2,0) + count(n,0)
  rw [cd_count_succ_0, cd_count_succ_2, cd_count_succ_1]

end TinylambOpt.ResearchProofs.StorageExecution.RankedMasks
