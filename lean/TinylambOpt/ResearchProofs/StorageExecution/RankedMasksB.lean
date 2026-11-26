import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

namespace TinylambOpt.ResearchProofs.StorageExecution.RankedMasksB

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Generic finite-state ranking facts. -/

private theorem filter_false {α : Type} (xs : List α) :
    xs.filter (fun _ => false) = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.filter_cons, ih]

private theorem sum_map_zero {α : Type} (xs : List α) :
    (xs.map (fun _ => (0 : Nat))).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := rfl

private theorem run_nil (m : Machine) (q : Nat) : run m q [] = some q := by
  simp [run]

private theorem legal_nil (m : Machine) (q : Nat) : legal m q [] = m.accept q := by
  simp [legal, run]

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  simp [run, List.foldlM]

private theorem run_append (m : Machine) (q : Nat) (a b : List Bool) :
    run m q (a ++ b) = (run m q a).bind (fun q' => run m q' b) := by
  induction a generalizing q with
  | nil => simp [run]
  | cons x xs ih => simp [run_cons, ih, Option.bind_assoc]

private theorem legal_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    legal m q (b :: bs) =
      (((m.step q b).map (fun q' => legal m q' bs)).getD false) := by
  simp [legal, run_cons]
  cases m.step q b <;> simp

private theorem rank_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    rank m q (b :: bs) = (m.step q b).bind
      (fun q' => (rank m q' bs).map
        (fun r => r + if b then lowCount m bs.length q else 0)) := by
  cases h : m.step q b with
  | none => simp [rank, h]
  | some q' =>
    cases hr : rank m q' bs with
    | none => simp [rank, h, hr]
    | some r => simp [rank, h, hr]

private theorem words_length : ∀ n, (words n).length = 2 ^ n
  | 0 => by simp [words]
  | n + 1 => by simp [words, words_length n, Nat.pow_succ]; omega

private theorem mem_words_of_length : ∀ {xs : List Bool}, xs ∈ words xs.length
  | [] => by simp [words]
  | b :: bs => by
    have ih := mem_words_of_length (xs := bs)
    simp only [List.length_cons, words, List.mem_append, List.mem_map]
    cases b
    · exact Or.inl ⟨bs, ih, rfl⟩
    · exact Or.inr ⟨bs, ih, rfl⟩

private theorem count_eq_filter_length (m : Machine) : ∀ (n q : Nat),
    count m n q = ((words n).filter (legal m q)).length
  | 0, q => by
    cases ha : m.accept q <;> simp [count, words, legal, run, ha]
  | n + 1, q => by
    rw [count_succ, words, List.filter_append, List.length_append]
    have hl : (((words n).map (false :: ·)).filter (legal m q)).length =
        lowCount m n q := by
      rw [List.filter_map]
      simp only [List.length_map, Function.comp_def, legal_cons]
      cases hs : m.step q false with
      | none => simp [lowCount, hs, filter_false]
      | some q' => simp [lowCount, hs, count_eq_filter_length m n q']
    have hr : (((words n).map (true :: ·)).filter (legal m q)).length =
        hiCount m n q := by
      rw [List.filter_map]
      simp only [List.length_map, Function.comp_def, legal_cons]
      cases hs : m.step q true with
      | none => simp [hiCount, hs, filter_false]
      | some q' => simp [hiCount, hs, count_eq_filter_length m n q']
    rw [hl, hr]

private theorem lowCount_eq_filter_length (m : Machine) (n q : Nat) :
    lowCount m n q =
      (((words n).map (false :: ·)).filter (legal m q)).length := by
  rw [List.filter_map]
  simp only [List.length_map, Function.comp_def, legal_cons]
  cases hs : m.step q false with
  | none => simp [lowCount, hs, filter_false]
  | some q' => simp [lowCount, hs, count_eq_filter_length m n q']

private theorem hiCount_eq_filter_length (m : Machine) (n q : Nat) :
    hiCount m n q =
      (((words n).map (true :: ·)).filter (legal m q)).length := by
  rw [List.filter_map]
  simp only [List.length_map, Function.comp_def, legal_cons]
  cases hs : m.step q true with
  | none => simp [hiCount, hs, filter_false]
  | some q' => simp [hiCount, hs, count_eq_filter_length m n q']

private theorem filter_cons_words (m : Machine) (q : Nat) (b : Bool) (n : Nat) :
    (((words n).map (b :: ·)).filter (legal m q)) =
      ((words n).filter
        (fun xs => ((m.step q b).map (fun q' => legal m q' xs)).getD false)).map
        (b :: ·) := by
  rw [List.filter_map]
  simp only [Function.comp_def, legal_cons]

private theorem rank_total (m : Machine) : ∀ (xs : List Bool) (q : Nat),
    legal m q xs = true → ∃ r, rank m q xs = some r
  | [], q, h => by
    have hacc : m.accept q = true := by simpa [legal, run] using h
    exact ⟨0, by simp [rank, hacc]⟩
  | b :: bs, q, h => by
    simp [legal_cons] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      obtain ⟨r, hr⟩ := rank_total m bs q' h
      exact ⟨r + if b then lowCount m bs.length q else 0,
        by simp [rank_cons, hs, hr]⟩

private theorem rank_nil_iff (m : Machine) (q r : Nat) :
    rank m q [] = some r ↔ r = 0 ∧ m.accept q = true := by
  constructor
  · intro h
    simp only [rank] at h
    split at h
    · rename_i ha
      simp only [Option.some.injEq] at h
      exact ⟨h.symm, ha⟩
    · simp at h
  · rintro ⟨rfl, ha⟩
    simp [rank, ha]

private theorem rank_bound (m : Machine) : ∀ (xs : List Bool) (q r : Nat),
    rank m q xs = some r → r < count m xs.length q
  | [], q, r, h => by
    rcases (rank_nil_iff m q r).mp h with ⟨rfl, hacc⟩
    simp [count, hacc]
  | b :: bs, q, r, h => by
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, count_succ]
    simp [rank_cons] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some tail =>
        simp [hr] at h
        subst r
        have ih := rank_bound m bs q' tail hr
        cases b with
        | false => simp [lowCount, hs] at ih ⊢; omega
        | true => simp [lowCount, hiCount, hs] at ih ⊢; omega

private theorem idxOf_cons_map (xs : List (List Bool)) (b : Bool)
    (ys : List Bool) (h : ys ∈ xs) :
    (xs.map (b :: ·)).idxOf (b :: ys) = xs.idxOf ys := by
  induction xs with
  | nil => simp at h
  | cons z zs ih =>
    simp only [List.map_cons, List.idxOf_cons]
    by_cases hzy : z = ys
    · subst z
      simp
    · have hbeq : (z == ys) = false := by simp [hzy]
      have htail : ys ∈ zs := by
        rcases List.mem_cons.mp h with heq | hmem
        · exact False.elim (hzy heq.symm)
        · exact hmem
      simp [hbeq, ih htail]

private theorem ordinal_generic (m : Machine) : ordinal m := by
  intro q xs
  induction xs generalizing q with
  | nil =>
    intro hlegal
    have hacc : m.accept q = true := by simpa [legal, run] using hlegal
    simp only [rank, words, List.length_nil, List.filter_cons, legal, run,
      Option.map_some, Option.getD_some, hacc, ite_true, List.filter_nil]
    rfl
  | cons b bs ih =>
    intro hlegal
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, words, List.filter_append, filter_cons_words, filter_cons_words]
    simp [legal_cons] at hlegal
    cases hs : m.step q b with
    | none => simp [hs] at hlegal
    | some q' =>
      simp [hs] at hlegal
      have htail := ih q' hlegal
      have hmem' : bs ∈ (words bs.length).filter (legal m q') :=
        List.mem_filter.mpr ⟨mem_words_of_length, hlegal⟩
      cases b with
      | false =>
        simp only [rank_cons, hs, Bool.false_eq_true, ite_false, Nat.add_zero,
          Option.bind_some, Option.map_some, Option.some.injEq, htail]
        simp only [Bool.false_eq_true, ite_false, Option.map_some, Option.getD_some]
        have hmem : false :: bs ∈
            ((words bs.length).filter (legal m q')).map (false :: ·) := by
          exact List.mem_map.mpr ⟨bs, hmem', rfl⟩
        rw [List.idxOf_append, ite_eq_left hmem,
          idxOf_cons_map (xs := (words bs.length).filter (legal m q')) false bs hmem']
      | true =>
        simp only [rank_cons, hs, ite_true, Option.bind_some, Option.map_some,
          Option.some.injEq, htail]
        simp only [Bool.false_eq_true, ite_false, Option.map_some, Option.getD_some]
        have hleft_len :
            (((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·)).length = lowCount m bs.length q := by
          rw [← filter_cons_words m q false bs.length]
          exact (lowCount_eq_filter_length m bs.length q).symm
        have hnot : true :: bs ∉
            ((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·) := by
          simp [List.mem_map]
        rw [List.idxOf_append, ite_eq_right hnot, hleft_len]
        rw [idxOf_cons_map (xs := (words bs.length).filter (legal m q')) true bs hmem']

private theorem unrank_total (m : Machine) : ∀ (n q r : Nat),
    r < count m n q → ∃ xs, unrank m n q r = some xs
  | 0, q, r, h => by
    cases ha : m.accept q with
    | false => simp [count, ha] at h
    | true =>
      simp [count, ha] at h
      have hr : r = 0 := by omega
      subst r
      exact ⟨[], by simp [unrank, ha]⟩
  | n + 1, q, r, h => by
    rw [count_succ] at h
    by_cases hl : r < lowCount m n q
    · have hd : decide (lowCount m n q ≤ r) = false := by simp [hl]
      cases hs : m.step q false with
      | none => simp [lowCount, hs] at hl
      | some qf =>
        obtain ⟨bs, hbs⟩ := unrank_total m n qf r (by simpa [lowCount, hs] using hl)
        exact ⟨false :: bs, by simp [unrank, hd, hs, hbs]⟩
    · have hle : lowCount m n q ≤ r := by omega
      have hd : decide (lowCount m n q ≤ r) = true := by simp [hle]
      cases hs : m.step q true with
      | none => simp [hiCount, hs] at h; omega
      | some qt =>
        simp [hiCount, hs] at h
        obtain ⟨bs, hbs⟩ := unrank_total m n qt (r - lowCount m n q) (by omega)
        exact ⟨true :: bs, by simp [unrank, hd, hs, hbs]⟩

private theorem unrank_nil_iff (m : Machine) (q r : Nat) (xs : List Bool) :
    unrank m 0 q r = some xs ↔ xs = [] ∧ r = 0 ∧ m.accept q = true := by
  simp [unrank, Bool.and_eq_true, and_assoc, and_left_comm, and_comm]

private theorem unrank_succ_iff (m : Machine) (n q r : Nat) (xs : List Bool) :
    unrank m (n + 1) q r = some xs ↔
      (r < lowCount m n q ∧ ∃ q' bs,
        m.step q false = some q' ∧ unrank m n q' r = some bs ∧ xs = false :: bs) ∨
      (lowCount m n q ≤ r ∧ ∃ q' bs,
        m.step q true = some q' ∧ unrank m n q' (r - lowCount m n q) = some bs ∧
          xs = true :: bs) := by
  by_cases hl : r < lowCount m n q
  · have hb : decide (lowCount m n q ≤ r) = false := by simp [hl]
    simp only [unrank, hb, Bool.false_eq_true, ite_false]
    constructor
    · intro h
      simp [Option.bind_eq_some_iff, Option.some.injEq] at h
      rcases h with ⟨next, hs, bs, hu, hbs⟩
      cases hst : m.step q false with
      | none => simp [hst] at hs
      | some q' =>
        simp [hst] at hs
        subst next
        exact Or.inl ⟨hl, q', bs, rfl, hu, hbs.symm⟩
    · rintro (⟨_, q', bs, hs, hu, rfl⟩ | ⟨hge, _⟩)
      · exact Option.bind_eq_some_iff.mpr ⟨q', hs,
          Option.bind_eq_some_iff.mpr ⟨bs, hu, by simp⟩⟩
      · exact False.elim (by omega)
  · have hle : lowCount m n q ≤ r := by omega
    have hb : decide (lowCount m n q ≤ r) = true := by simp [hle]
    simp only [unrank, hb, ite_true]
    constructor
    · intro h
      simp [Option.bind_eq_some_iff, Option.some.injEq] at h
      rcases h with ⟨next, hs, bs, hu, hbs⟩
      cases hst : m.step q true with
      | none => simp [hst] at hs
      | some q' =>
        simp [hst] at hs
        subst next
        exact Or.inr ⟨hle, q', bs, rfl, hu, hbs.symm⟩
    · rintro (⟨hlt, _⟩ | ⟨_, q', bs, hs, hu, rfl⟩)
      · exact False.elim (by omega)
      · exact Option.bind_eq_some_iff.mpr ⟨q', hs,
          Option.bind_eq_some_iff.mpr ⟨bs, hu, by simp⟩⟩

private theorem unrank_length (m : Machine) : ∀ (n q r : Nat) (xs : List Bool),
    unrank m n q r = some xs → xs.length = n
  | 0, q, r, xs, h => by
    rcases (unrank_nil_iff m q r xs).mp h with ⟨rfl, _, _⟩
    rfl
  | n + 1, q, r, xs, h => by
    rcases (unrank_succ_iff m n q r xs).mp h with
      ⟨_, q', bs, _, hu, rfl⟩ | ⟨_, q', bs, _, hu, rfl⟩
    · simp [unrank_length m n q' r bs hu]
    · simp [unrank_length m n q' (r - lowCount m n q) bs hu]

private theorem unrank_sound (m : Machine) : ∀ (n q r : Nat) (xs : List Bool),
    unrank m n q r = some xs → rank m q xs = some r ∧ r < count m n q
  | 0, q, r, xs, h => by
    rcases (unrank_nil_iff m q r xs).mp h with ⟨rfl, rfl, ha⟩
    simp [rank, count, ha]
  | n + 1, q, r, xs, h => by
    simp only [List.length_cons, count_succ]
    rcases (unrank_succ_iff m n q r xs).mp h with
      ⟨hlt, q', bs, hs, hu, rfl⟩ | ⟨hle, q', bs, hs, hu, rfl⟩
    · obtain ⟨hrank, hbound⟩ := unrank_sound m n q' r bs hu
      constructor
      · simp [rank_cons, hs, hrank]
      · simp [lowCount, hs] at hlt hbound ⊢
        omega
    · obtain ⟨hrank, hbound⟩ := unrank_sound m n q' (r - lowCount m n q) bs hu
      constructor
      · simp only [rank_cons, hs, hrank, Option.bind_some, Option.map_some, ite_true]
        have hlen : bs.length = n := unrank_length m n q' (r - lowCount m n q) bs hu
        rw [hlen]
        exact congrArg some (Nat.sub_add_cancel hle)
      · simp [lowCount, hiCount, hs] at hbound ⊢
        omega

private theorem unrank_none_ge (m : Machine) : ∀ (n q r : Nat),
    count m n q ≤ r → unrank m n q r = none
  | 0, q, r, h => by
    cases ha : m.accept q with
    | false => simp [unrank, count, ha]
    | true =>
      simp [unrank, count, ha] at h ⊢
      omega
  | n + 1, q, r, h => by
    rw [count_succ] at h
    have hle : lowCount m n q ≤ r := by omega
    have hd : decide (lowCount m n q ≤ r) = true := by simp [hle]
    simp [unrank, hd]
    cases hs : m.step q true with
    | none => simp [hs]
    | some q' =>
      simp [hiCount, hs] at h
      have hle' : count m n q' ≤ r - lowCount m n q := by omega
      simp [unrank_none_ge m n q' (r - lowCount m n q) hle']

private theorem rejectSlack_generic (m : Machine) : rejectSlack m := by
  intro n q r
  cases h : unrank m n q r with
  | none =>
    simp
    by_cases hc : count m n q ≤ r
    · simp [hc]
    · obtain ⟨xs, hx⟩ := unrank_total m n q r (by omega)
      simp [hx] at h
  | some xs =>
    simp
    have ⟨_, hbound⟩ := unrank_sound m n q r xs h
    omega

private theorem decoder_generic (m : Machine) : decoder m := by
  intro q xs r hrank
  induction xs generalizing q r with
  | nil =>
    rcases (rank_nil_iff m q r).mp hrank with ⟨rfl, hacc⟩
    simp [unrank, hacc]
  | cons b bs ih =>
    simp [rank_cons] at hrank
    cases hs : m.step q b with
    | none => simp [hs] at hrank
    | some q' =>
      simp [hs] at hrank
      cases hr : rank m q' bs with
      | none => simp [hr] at hrank
      | some tail =>
        simp [hr] at hrank
        subst r
        have htail := ih q' tail hr
        have hbound := rank_bound m bs q' tail hr
        cases b with
        | false =>
          have hlt : ¬ lowCount m bs.length q ≤ tail := by
            simpa [lowCount, hs] using Nat.not_le_of_gt hbound
          have hd : decide (lowCount m bs.length q ≤ tail) = false := by simp [hlt]
          simp [unrank, hd, hs, htail]
        | true =>
          have hle : lowCount m bs.length q ≤ tail + lowCount m bs.length q := by omega
          have hd : decide (lowCount m bs.length q ≤ tail + lowCount m bs.length q) = true := by
            simp [hle]
          simp [unrank, hd, hs, htail, Nat.add_sub_cancel_left]

private theorem fromLittle_append_singleton (xs : List Nat) (b : Nat) :
    fromLittle (xs ++ [b]) = fromLittle xs + 256 ^ xs.length * b := by
  induction xs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
    simp only [List.cons_append, fromLittle, List.length_cons]
    rw [ih, Nat.pow_succ, Nat.mul_add, Nat.mul_assoc, Nat.mul_comm,
      Nat.mul_left_comm]
    omega

private theorem fromLittle_little (n k : Nat) :
    fromLittle (little n k) = n % 256 ^ k := by
  induction k with
  | zero => simp [little, fromLittle, Nat.mod_one]
  | succ k ih =>
    have hsplit : little n (k + 1) = little n k ++ [(n / 256 ^ k) % 256] := by
      simp [little, List.range_succ]
    rw [hsplit, fromLittle_append_singleton, ih]
    have hlen : (little n k).length = k := by simp [little]
    rw [hlen, Nat.pow_succ]
    exact (Nat.mod_mul).symm

private theorem two_pow_pos (w : Nat) : 0 < 2 ^ w := Nat.pow_pos (by decide)

private theorem width_lt (n : Nat) : n < 2 ^ width n := by
  simp only [width]
  split
  · rename_i h
    simp only [beq_iff_eq] at h
    subst n
    decide
  · rename_i h
    have hn : n ≠ 0 := by simpa using h
    exact Nat.lt_log2_self

private theorem codeBits_bound (m : Machine) (n q r : Nat) (hr : r < count m n q) :
    r < 2 ^ codeBits m n q := by
  simp only [codeBits]
  have hlt : count m n q - 1 < 2 ^ width (count m n q - 1) := width_lt _
  omega

private theorem get_pack_singleton (w r : Nat) (hw : r < 2 ^ w) :
    TinylambOpt.ResearchGoals.Compression.Wire.get (pack w [r]) w 0 = r := by
  have hword : word w [r] = r % 2 ^ w := by simp [word]
  have hb : w ≤ 8 * byteCount w := by simp only [byteCount]; omega
  have hpow : 256 ^ byteCount w = 2 ^ (8 * byteCount w) := by
    rw [show (256 : Nat) = 2 ^ 8 from rfl, ← Nat.pow_mul]
  have hraw : fromLittle (little (r % 2 ^ w) (byteCount w)) = r := by
    rw [fromLittle_little]
    have hmod : (r % 2 ^ w) % 256 ^ byteCount w = r % 2 ^ w := by
      rw [hpow]
      exact Nat.mod_eq_of_lt
        (Nat.lt_of_lt_of_le (Nat.mod_lt r (two_pow_pos w))
          (Nat.pow_le_pow_right (by decide) hb))
    rw [hmod, Nat.mod_eq_of_lt hw]
  have hfrom : fromLittle (pack w [r]) = r := by
    simpa [pack, word] using hraw
  simp only [TinylambOpt.ResearchGoals.Compression.Wire.get, hfrom, Nat.zero_mul,
    Nat.pow_zero, Nat.div_one, Nat.mod_eq_of_lt hw]

private theorem wireLookup_generic (m : Machine) : wireLookup m := by
  intro q xs r hr
  have hbound := rank_bound m xs q r hr
  rw [get_pack_singleton _ _ (codeBits_bound m xs.length q r hbound)]
  exact decoder_generic m q xs r hr

private theorem byteCount_lt_iff (bits cap : Nat) (hcap : 0 < cap) :
    byteCount bits < cap ↔ bits ≤ 8 * (cap - 1) := by
  simp only [byteCount]
  omega

private theorem spaceTest_generic (m : Machine) : spaceTest m := by
  intro n q pages h _hpages _hcount
  have hcap : h + byteCount (pages * codeBits m n q) < pages * byteCount n ↔
      byteCount (pages * codeBits m n q) < pages * byteCount n - h := by omega
  rw [hcap]
  constructor
  · intro hlt
    have hc : 0 < pages * byteCount n - h := by omega
    constructor
    · omega
    · have := (byteCount_lt_iff (pages * codeBits m n q)
        (pages * byteCount n - h) hc).mp hlt
      omega
  · rintro ⟨hh, hbits⟩
    have hc : 0 < pages * byteCount n - h := by omega
    exact (byteCount_lt_iff (pages * codeBits m n q)
      (pages * byteCount n - h) hc).mpr (by omega)

private theorem sum_map_bind_step (m : Machine) (q : Nat) (b : Bool) (n k : Nat) :
    (((words n).map (fun pre =>
      (((m.step q b).bind (fun q' => run m q' pre)).map (count m k)).getD 0)).sum) =
    (((m.step q b).map (fun q' =>
      ((words n).map (fun pre => ((run m q' pre).map (count m k)).getD 0)).sum)).getD 0) := by
  cases h : m.step q b with
  | none => simp [h, List.map_const, sum_map_zero]
  | some q' => simp [h]

private theorem cofactor_generic (m : Machine) : cofactor m := by
  intro q a b
  induction a generalizing q with
  | zero => simp [words, run, cofactor]
  | succ a ih =>
    have hsplit : count m (a + 1 + b) q = count m (a + b + 1) q := by
      congr 1
      omega
    rw [hsplit, count_succ]
    simp only [words, List.map_append, List.sum_append, List.map_map, Function.comp_def]
    have hl : ((words a).map (fun pre =>
        ((run m q (false :: pre)).map (count m b)).getD 0)).sum =
        lowCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (false :: pre)).map (count m b)).getD 0 =
          (((m.step q false).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre
        rw [run_cons]
      simp only [h1]
      rw [sum_map_bind_step]
      simp [lowCount]
      cases h : m.step q false with
      | none => simp
      | some q' => simp [ih q']
    have hr : ((words a).map (fun pre =>
        ((run m q (true :: pre)).map (count m b)).getD 0)).sum =
        hiCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (true :: pre)).map (count m b)).getD 0 =
          (((m.step q true).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre
        rw [run_cons]
      simp only [h1]
      rw [sum_map_bind_step]
      simp [hiCount]
      cases h : m.step q true with
      | none => simp
      | some q' => simp [ih q']
    rw [hl, hr]

end TinylambOpt.ResearchProofs.StorageExecution.RankedMasksB
