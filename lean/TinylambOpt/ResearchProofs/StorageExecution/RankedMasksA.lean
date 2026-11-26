import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

namespace TinylambOpt.ResearchProofs.StorageExecution.RankedMasksA

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Generic facts about the finite-state ranking model. -/

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

private theorem fromLittle_append (xs : List Nat) (b : Nat) :
    fromLittle (xs ++ [b]) = fromLittle xs + 256 ^ xs.length * b := by
  induction xs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
    simp only [List.cons_append, fromLittle, List.length_cons]
    rw [ih, Nat.pow_succ, Nat.mul_add, Nat.mul_assoc, Nat.mul_comm,
      Nat.mul_left_comm]
    omega

private theorem fromLittle_little (n k : Nat) : fromLittle (little n k) = n % 256 ^ k := by
  induction k with
  | zero => simp [little, fromLittle, Nat.mod_one]
  | succ k ih =>
    have hsplit : little n (k + 1) =
        little n k ++ [(n / 256 ^ k) % 256] := by
      simp [little, List.range_succ]
    rw [hsplit, fromLittle_append, ih]
    have hlen : (little n k).length = k := by simp [little]
    rw [hlen, Nat.pow_succ]
    exact (Nat.mod_mul).symm

private theorem get_pack_singleton (w r : Nat) (hw : r < 2 ^ w) :
    TinylambOpt.ResearchGoals.Compression.Wire.get
      (TinylambOpt.ResearchGoals.Compression.Wire.pack w [r]) w 0 = r := by
  have hword : TinylambOpt.ResearchGoals.Compression.Wire.word w [r] = r % 2 ^ w := by
    simp [TinylambOpt.ResearchGoals.Compression.Wire.word]
  have hb : w ≤ 8 * byteCount w := by simp only [byteCount]; omega
  have hpow : 256 ^ byteCount w = 2 ^ (8 * byteCount w) := by
    rw [show (256 : Nat) = 2 ^ 8 from rfl, ← Nat.pow_mul]
  have hraw : fromLittle
      (little (r % 2 ^ w) (byteCount w)) = r := by
    rw [fromLittle_little]
    have hmod : (r % 2 ^ w) % 256 ^ byteCount w = r % 2 ^ w := by
      rw [hpow]
      exact Nat.mod_eq_of_lt
        (Nat.lt_of_lt_of_le (Nat.mod_lt r (Nat.pow_pos (by decide)))
          (Nat.pow_le_pow_right (by decide) hb))
    rw [hmod, Nat.mod_eq_of_lt hw]
  have hfrom : fromLittle
      (TinylambOpt.ResearchGoals.Compression.Wire.pack w [r]) = r := by
    simpa [TinylambOpt.ResearchGoals.Compression.Wire.pack,
      TinylambOpt.ResearchGoals.Compression.Wire.word] using hraw
  simp only [TinylambOpt.ResearchGoals.Compression.Wire.get, hfrom,
    Nat.zero_mul, Nat.pow_zero, Nat.div_one, Nat.mod_eq_of_lt hw]

private theorem width_lt (n : Nat) : n < 2 ^ width n := by
  simp only [width]
  split
  · rename_i h; simp at h; subst h; decide
  · rename_i h
    have hn : 0 < n := Nat.pos_of_ne_zero (by simpa using h)
    exact Nat.lt_log2_self

private theorem codeBits_bound (m : Machine) (n q r : Nat) (hr : r < count m n q) :
    r < 2 ^ codeBits m n q := by
  simp only [codeBits]
  have hlt : count m n q - 1 < 2 ^ width (count m n q - 1) := width_lt _
  omega

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
    · have := (byteCount_lt_iff (pages * codeBits m n q) (pages * byteCount n - h) hc).mp hlt
      omega
  · intro ⟨hh, hbits⟩
    have hc : 0 < pages * byteCount n - h := by omega
    exact (byteCount_lt_iff (pages * codeBits m n q) (pages * byteCount n - h) hc).mpr (by omega)

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
  | cons x xs ih => simp [run_cons, ih, Option.bind_assoc]

private theorem legal_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    legal m q (b :: bs) = (((m.step q b).map (fun q' => legal m q' bs)).getD false) := by
  simp [legal, run_cons]
  cases m.step q b <;> simp

private theorem legal_run_some (m : Machine) : ∀ (xs : List Bool) (q : Nat),
    legal m q xs = true → ∃ q', run m q xs = some q'
  | [], q, h => by simp [legal, run] at h; exact ⟨q, rfl⟩
  | b :: bs, q, h => by
    simp [legal_cons] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      obtain ⟨q'', hrun⟩ := legal_run_some m bs q' h
      exact ⟨q'', by simp [run_cons, hs, hrun]⟩

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

private theorem count_eq_filter_length (m : Machine) : ∀ (n q : Nat),
    count m n q = ((words n).filter (legal m q)).length
  | 0, q => by
    cases ha : m.accept q <;> simp [count, words, legal, run, ha]
  | n + 1, q => by
    rw [count_succ]
    simp only [words, List.filter_append, List.filter_map, List.length_append,
      List.length_map, Function.comp_def, legal_cons]
    cases hf : m.step q false with
    | none =>
      cases ht : m.step q true with
      | none => simp [lowCount, hiCount, hf, ht, filter_false]
      | some qt =>
        simp [lowCount, hiCount, hf, ht, filter_false]
        rw [← count_eq_filter_length m n qt]
    | some qf =>
      cases ht : m.step q true with
      | none =>
        simp [lowCount, hiCount, hf, ht, filter_false]
        rw [← count_eq_filter_length m n qf]
      | some qt =>
        simp [lowCount, hiCount, hf, ht]
        rw [← count_eq_filter_length m n qf, ← count_eq_filter_length m n qt]

private theorem lowCount_eq_filter_length (m : Machine) (n q : Nat) :
    lowCount m n q = (((words n).map (false :: ·)).filter (legal m q)).length := by
  rw [List.filter_map]
  simp only [List.length_map, Function.comp_def, legal_cons]
  cases hs : m.step q false with
  | none => simp [lowCount, hs, filter_false]
  | some q' => simp [lowCount, hs, count_eq_filter_length]

private theorem hiCount_eq_filter_length (m : Machine) (n q : Nat) :
    hiCount m n q = (((words n).map (true :: ·)).filter (legal m q)).length := by
  rw [List.filter_map]
  simp only [List.length_map, Function.comp_def, legal_cons]
  cases hs : m.step q true with
  | none => simp [hiCount, hs, filter_false]
  | some q' => simp [hiCount, hs, count_eq_filter_length]

private theorem idxOf_cons_map : ∀ (xs : List (List Bool)) (b : Bool) (ys : List Bool),
    ys ∈ xs → (xs.map (b :: ·)).idxOf (b :: ys) = xs.idxOf ys
  | [], b, ys, h => by simp at h
  | z :: zs, b, ys, h => by
    simp only [List.mem_cons] at h
    by_cases hzy : z = ys
    · subst z; simp
    · have hmem : ys ∈ zs := by
        rcases h with heq | hmem
        · exact False.elim (hzy heq.symm)
        · exact hmem
      simp only [List.map_cons, List.idxOf_cons]
      simp [hzy, idxOf_cons_map zs b ys hmem]

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
      exact ⟨r + if b then lowCount m bs.length q else 0, by simp [rank, hs, hr]⟩

private theorem rank_nil_iff (m : Machine) (q r : Nat) :
    rank m q [] = some r ↔ r = 0 ∧ m.accept q = true := by
  constructor
  · intro h
    by_cases ha : m.accept q = true
    · simp [rank, ha] at h
      exact ⟨h.symm, ha⟩
    · have hf : m.accept q = false := by
        cases hac : m.accept q <;> simp_all
      simp [rank, hf] at h
  · rintro ⟨rfl, ha⟩
    simp [rank, ha]

private theorem rank_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    rank m q (b :: bs) = (m.step q b).bind
      (fun q' => (rank m q' bs).map
        (fun r => r + if b then lowCount m bs.length q else 0)) := by
  cases hs : m.step q b with
  | none => simp [rank, hs]
  | some q' =>
    cases hr : rank m q' bs with
    | none => simp [rank, hs, hr]
    | some r => simp [rank, hs, hr]

private theorem filter_cons_words (m : Machine) (q : Nat) (b : Bool) (n : Nat) :
    (((words n).map (b :: ·)).filter (legal m q)) =
      ((words n).filter
        (fun xs => ((m.step q b).map (fun q' => legal m q' xs)).getD false)).map
        (b :: ·) := by
  rw [List.filter_map]
  simp only [Function.comp_def, legal_cons]

private theorem rank_some_legal (m : Machine) : ∀ (xs : List Bool) (q r : Nat),
    rank m q xs = some r → legal m q xs = true
  | [], q, r, h => by
    rcases (rank_nil_iff m q r).mp h with ⟨rfl, hacc⟩
    simp [legal, run, hacc]
  | b :: bs, q, r, h => by
    simp [rank] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some tail =>
        simp [hr] at h
        have ih := rank_some_legal m bs q' tail hr
        simp [legal_cons, hs, ih]

private theorem rank_bound (m : Machine) : ∀ (xs : List Bool) (q r : Nat),
    rank m q xs = some r → r < count m xs.length q
  | [], q, r, h => by
    rcases (rank_nil_iff m q r).mp h with ⟨rfl, hacc⟩
    simp [count, hacc]
  | b :: bs, q, r, h => by
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, count_succ]
    simp [rank] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some tail =>
        simp [hr] at h
        subst h
        have ih := rank_bound m bs q' tail hr
        cases b with
        | false => simp [lowCount, hs] at ih ⊢; omega
        | true => simp [lowCount, hiCount, hs] at ih ⊢; omega

private theorem ordinal_generic (m : Machine) : ordinal m := by
  intro q xs
  induction xs generalizing q with
  | nil =>
    intro hlegal
    have hacc : m.accept q = true := by simpa [legal, run] using hlegal
    cases ha : m.accept q <;> simp_all [rank, words, legal, run, ha]
  | cons b bs ih =>
    intro hlegal
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, words, List.filter_append, filter_cons_words, filter_cons_words]
    have hleg' : (((m.step q b).map (fun q' => legal m q' bs)).getD false) = true := by
      simpa [legal_cons] using hlegal
    cases hs : m.step q b with
    | none => simp [hs] at hleg'
    | some q' =>
      simp [hs] at hleg'
      have htail := ih q' hleg'
      have hmem' : bs ∈ (words bs.length).filter (legal m q') :=
        List.mem_filter.mpr ⟨mem_words_of_length, hleg'⟩
      cases b with
      | false =>
        simp only [rank_cons, hs, Bool.false_eq_true, ite_false, Nat.add_zero,
          Option.bind_some, Option.map_some, Option.some.injEq, htail,
          Option.map_some, Option.getD_some]
        have hmem : false :: bs ∈
            ((words bs.length).filter (legal m q')).map (false :: ·) :=
          List.mem_map.mpr ⟨bs, hmem', rfl⟩
        rw [List.idxOf_append, if_pos hmem, idxOf_cons_map _ _ _ hmem']
      | true =>
        simp only [rank_cons, hs, ite_true, Option.bind_some, Option.map_some,
          Option.some.injEq, htail, Option.map_some, Option.getD_some]
        have hleft_len :
            (((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·)).length = lowCount m bs.length q := by
          calc
            (((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·)).length =
                (((words bs.length).map (false :: ·)).filter (legal m q)).length := by
                  rw [filter_cons_words]
            _ = lowCount m bs.length q := (lowCount_eq_filter_length m bs.length q).symm
        have hnot : true :: bs ∉
            ((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·) := by
          simp [List.mem_map]
        rw [List.idxOf_append, if_neg hnot, hleft_len,
          idxOf_cons_map ((words bs.length).filter (legal m q')) true bs hmem']

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
    · cases hs : m.step q false with
      | none => simp [lowCount, hs, filter_false] at hl
      | some qf =>
        obtain ⟨bs, hbs⟩ := unrank_total m n qf r (by simpa [lowCount, hs] using hl)
        have hb : decide (lowCount m n q ≤ r) = false := by simp [hl]
        exact ⟨false :: bs, by simp [unrank, hb, hs, hbs]⟩
    · have hle : lowCount m n q ≤ r := by omega
      cases hs : m.step q true with
      | none => simp [hiCount, hs] at h; omega
      | some qt =>
        simp [hiCount, hs] at h
        obtain ⟨bs, hbs⟩ := unrank_total m n qt (r - lowCount m n q) (by omega)
        exact ⟨true :: bs, by simp [unrank, hle, hs, hbs]⟩

private theorem unrank_nil_iff (m : Machine) (q r : Nat) (xs : List Bool) :
    unrank m 0 q r = some xs ↔ xs = [] ∧ r = 0 ∧ m.accept q = true := by
  simp [unrank, and_comm, and_assoc, and_left_comm]

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
    rw [count_succ]
    rcases (unrank_succ_iff m n q r xs).mp h with
      ⟨hlt, q', bs, hs, hu, rfl⟩ | ⟨hle, q', bs, hs, hu, rfl⟩
    · obtain ⟨hrank, hbound⟩ := unrank_sound m n q' r bs hu
      have hlen : bs.length = n := by simpa using unrank_length m n q' r bs hu
      constructor
      · simp [rank, hs, hrank]
      · simp [lowCount, hs, hlen] at hlt hbound ⊢
        omega
    · obtain ⟨hrank, hbound⟩ := unrank_sound m n q' (r - lowCount m n q) bs hu
      have hlen : bs.length = n := by
        simpa using unrank_length m n q' (r - lowCount m n q) bs hu
      constructor
      · simp only [rank_cons, hs, Option.bind_some, hrank, Option.map_some,
          Option.some.injEq, ite_true, hlen]
        exact Nat.sub_add_cancel hle
      · simp [lowCount, hiCount, hs, hlen] at hbound ⊢
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
    have hb : decide (lowCount m n q ≤ r) = true := by simp [hle]
    simp only [unrank, hb, ite_true]
    cases hs : m.step q true with
    | none => simp [hiCount, hs] at h ⊢
    | some q' =>
      simp [hiCount, hs] at h
      have hle' : count m n q' ≤ r - lowCount m n q := by omega
      simp [unrank, hb, hs, unrank_none_ge m n q' (r - lowCount m n q) hle']

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
    simp [rank] at hrank
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
          have hlt : tail < lowCount m bs.length q := by simpa [lowCount, hs] using hbound
          have hb : decide (lowCount m bs.length q ≤ tail) = false := by simp [hlt]
          simp [unrank, hb, hs, htail]
        | true =>
          have hb : decide (lowCount m bs.length q ≤ tail + lowCount m bs.length q) = true := by
            simp
          simp [unrank, hb, hs, htail, Nat.add_sub_cancel]

private theorem wireLookup_generic (m : Machine) : wireLookup m := by
  intro q xs r hr
  have hbound := rank_bound m xs q r hr
  rw [get_pack_singleton _ _ (codeBits_bound m xs.length q r hbound)]
  exact decoder_generic m q xs r hr

private theorem sum_map_bind_step (m : Machine) (q : Nat) (b : Bool) (n k : Nat) :
    (((words n).map (fun pre =>
      (((m.step q b).bind (fun q' => run m q' pre)).map (count m k)).getD 0)).sum) =
    (((m.step q b).map (fun q' =>
      ((words n).map (fun pre => ((run m q' pre).map (count m k)).getD 0)).sum)).getD 0) := by
  cases h : m.step q b with
  | none => simp [h, sum_map_zero]
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
    simp only [words, List.map_append, List.sum_append, List.map_map]
    have hl : ((words a).map (fun pre => ((run m q (false :: pre)).map (count m b)).getD 0)).sum =
        lowCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (false :: pre)).map (count m b)).getD 0 =
          (((m.step q false).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre; rw [run_cons]
      simp only [h1]
      rw [sum_map_bind_step]
      simp [lowCount]
      cases h : m.step q false with
      | none => simp
      | some q' => simp [ih q']
    have hr : ((words a).map (fun pre => ((run m q (true :: pre)).map (count m b)).getD 0)).sum =
        hiCount m (a + b) q := by
      have h1 : ∀ pre, ((run m q (true :: pre)).map (count m b)).getD 0 =
          (((m.step q true).bind (fun q' => run m q' pre)).map (count m b)).getD 0 := by
        intro pre; rw [run_cons]
      simp only [h1]
      rw [sum_map_bind_step]
      simp [hiCount]
      cases h : m.step q true with
      | none => simp
      | some q' => simp [ih q']
    change lowCount m (a + b) q + hiCount m (a + b) q =
      ((words a).map (fun pre => ((run m q (false :: pre)).map (count m b)).getD 0)).sum +
      ((words a).map (fun pre => ((run m q (true :: pre)).map (count m b)).getD 0)).sum
    rw [hl, hr]

private theorem splitCode_generic (m : Machine) : splitCode m := by
  intro q a b q' off hdesc
  induction a generalizing q q' off with
  | nil =>
    simp [descend] at hdesc
    rcases hdesc with ⟨rfl, rfl⟩
    simp [rank]
  | cons x xs ih =>
    have hlen : (x :: xs).length + b.length = (xs.length + b.length) + 1 := by
      simp; omega
    rw [hlen] at hdesc
    simp only [descend] at hdesc
    cases hs : m.step q x with
    | none => simp [hs] at hdesc
    | some q1 =>
      simp only [hs, Option.bind_some] at hdesc
      cases hd : descend m (xs.length + b.length) q1 xs with
      | none => simp [hd] at hdesc
      | some p =>
        rcases p with ⟨last, off1⟩
        simp [hd] at hdesc
        rcases hdesc with ⟨rfl, rfl⟩
        have ih' := ih q1 last off1 hd
        simp only [List.cons_append, rank_cons, hs, Option.bind_some,
          List.length_append, ih']
        cases hr : rank m last b with
        | none => simp
        | some rb =>
          simp only [Option.map_some]
          cases x <;> simp only [Bool.false_eq_true, ite_false, ite_true,
            Nat.add_zero, Option.some.injEq] <;> omega

private theorem unrank_any_cons (m : Machine) (n q r : Nat) (b : Bool) (bs : List Bool) :
    Option.any (fun xs => (b :: bs).isPrefixOf xs) (unrank m (n + 1) q r) =
      if decide (lowCount m n q ≤ r) then
        (if b then ((m.step q true).map (fun q' =>
          Option.any (fun xs => bs.isPrefixOf xs)
            (unrank m n q' (r - lowCount m n q)))).getD false else false)
      else
        (if b then false else ((m.step q false).map (fun q' =>
          Option.any (fun xs => bs.isPrefixOf xs) (unrank m n q' r))).getD false) := by
  cases b with
  | false =>
    by_cases hle : lowCount m n q ≤ r
    · have hd : decide (lowCount m n q ≤ r) = true := by simp [hle]
      cases hs : m.step q true with
      | none => simp [unrank, hd, hs]
      | some qt =>
        cases hu : unrank m n qt (r - lowCount m n q) with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]
    · have hd : decide (lowCount m n q ≤ r) = false := by simp [hle]
      cases hs : m.step q false with
      | none => simp [unrank, hd, hs]
      | some qf =>
        cases hu : unrank m n qf r with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]
  | true =>
    by_cases hle : lowCount m n q ≤ r
    · have hd : decide (lowCount m n q ≤ r) = true := by simp [hle]
      cases hs : m.step q true with
      | none => simp [unrank, hd, hs]
      | some qt =>
        cases hu : unrank m n qt (r - lowCount m n q) with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]
    · have hd : decide (lowCount m n q ≤ r) = false := by simp [hle]
      cases hs : m.step q false with
      | none => simp [unrank, hd, hs]
      | some qf =>
        cases hu : unrank m n qf r with
        | none => simp [unrank, hd, hs, hu]
        | some tail => simp [unrank, hd, hs, hu, List.isPrefixOf]

private theorem descend_bound (m : Machine) : ∀ (pre : List Bool) (n q q' off : Nat),
    descend m n q pre = some (q', off) →
    pre.length ≤ n ∧ off + count m (n - pre.length) q' ≤ count m n q
  | [], n, q, q', off, h => by
    simp [descend] at h
    rcases h with ⟨rfl, rfl⟩
    simp
  | b :: bs, 0, q, q', off, h => by simp [descend] at h
  | b :: bs, n + 1, q, q', off, h => by
    simp only [descend] at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q1 =>
      simp only [hs, Option.bind_some] at h
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
        · have hsub : n + 1 - (b :: bs).length = n - bs.length := by simp
          rw [hsub, count_succ]
          cases b with
          | false => simp [lowCount, hs] at hbound ⊢; omega
          | true => simp [lowCount, hiCount, hs] at hbound ⊢; omega

private theorem prefixRange_generic (m : Machine) : prefixRange m := by
  intro n q pre q' off r hdesc hr
  induction pre generalizing n q q' off r with
  | nil =>
    simp [descend] at hdesc
    rcases hdesc with ⟨rfl, rfl⟩
    obtain ⟨xs, hx⟩ := unrank_total m n q r hr
    simp [hx, hr, List.isPrefixOf]
  | cons b bs ih =>
    cases n with
    | zero => simp [descend] at hdesc
    | succ n =>
      simp only [descend] at hdesc
      cases hs : m.step q b with
      | none => simp [hs] at hdesc
      | some q1 =>
        simp only [hs, Option.bind_some] at hdesc
        cases hd : descend m n q1 bs with
        | none => simp [hd] at hdesc
        | some p =>
          rcases p with ⟨last, off1⟩
          simp [hd] at hdesc
          have hdb := descend_bound m bs n q1 last off1 hd
          have hsub : n + 1 - (b :: bs).length = n - bs.length := by simp
          rw [count_succ] at hr
          rw [hsub]
          cases b with
          | false =>
            simp only [Bool.false_eq_true, ite_false, Nat.add_zero] at hdesc
            rcases hdesc with ⟨rfl, rfl⟩
            by_cases hl : r < lowCount m n q
            · have hddec : decide (lowCount m n q ≤ r) = false := by simp [hl]
              rw [unrank_any_cons]
              simp only [hddec, Bool.false_eq_true, ite_false, hs,
                Option.map_some, Option.getD_some]
              have hr' : r < count m n q1 := by
                simpa [lowCount, hs] using hl
              exact ih n q1 last off1 r hd hr'
            · have hle : lowCount m n q ≤ r := by omega
              have hout : ¬ (off1 ≤ r ∧ r < off1 + count m (n - bs.length) last) := by
                have hb := hdb.2
                have hlow : lowCount m n q = count m n q1 := by simp [lowCount, hs]
                omega
              rw [unrank_any_cons]
              simp [hle, hout]
          | true =>
            simp only [ite_true] at hdesc
            rcases hdesc with ⟨rfl, rfl⟩
            by_cases hle : lowCount m n q ≤ r
            · have hddec : decide (lowCount m n q ≤ r) = true := by simp [hle]
              rw [unrank_any_cons]
              simp only [hddec, ite_true, hs, Option.map_some, Option.getD_some]
              have hr' : r - lowCount m n q < count m n q1 := by
                simp [hiCount, hs] at hr
                omega
              have hcond :
                  (off1 + lowCount m n q ≤ r ∧
                    r < off1 + lowCount m n q + count m (n - bs.length) last) ↔
                  (off1 ≤ r - lowCount m n q ∧
                    r - lowCount m n q < off1 + count m (n - bs.length) last) := by
                constructor <;> rintro ⟨h1, h2⟩ <;> constructor <;> omega
              have hdec : decide
                  (off1 + lowCount m n q ≤ r ∧
                    r < off1 + lowCount m n q + count m (n - bs.length) last) =
                  decide
                  (off1 ≤ r - lowCount m n q ∧
                    r - lowCount m n q < off1 + count m (n - bs.length) last) := by
                by_cases hh : off1 ≤ r - lowCount m n q ∧
                    r - lowCount m n q < off1 + count m (n - bs.length) last
                · have hh2 := hcond.mpr hh
                  simp [hh, hh2]
                · have hh2 : ¬ (off1 + lowCount m n q ≤ r ∧
                    r < off1 + lowCount m n q + count m (n - bs.length) last) := by
                    intro hc
                    exact hh (hcond.mp hc)
                  simp [hh, hh2]
              rw [hdec]
              exact ih n q1 last off1 (r - lowCount m n q) hd hr'
            · have hl : r < lowCount m n q := by omega
              have hout : ¬ (off1 + lowCount m n q ≤ r ∧
                  r < off1 + lowCount m n q + count m (n - bs.length) last) := by omega
              rw [unrank_any_cons]
              have hddec : decide (lowCount m n q ≤ r) = false := by simp [hl]
              simp [hddec, hout]

/-! The eight model-independent goals, instantiated for the five requested models. -/

theorem sound_FixedPopcount_Ordinal :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.Ordinal.Goal :=
  ordinal_generic FixedPopcount.model

theorem sound_FixedPopcount_Decoder :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.Decoder.Goal :=
  decoder_generic FixedPopcount.model

theorem sound_FixedPopcount_RejectSlack :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.RejectSlack.Goal :=
  rejectSlack_generic FixedPopcount.model

theorem sound_FixedPopcount_PrefixRange :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.PrefixRange.Goal :=
  prefixRange_generic FixedPopcount.model

theorem sound_FixedPopcount_SplitCode :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.SplitCode.Goal :=
  splitCode_generic FixedPopcount.model

theorem sound_FixedPopcount_WireLookup :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.WireLookup.Goal :=
  wireLookup_generic FixedPopcount.model

theorem sound_FixedPopcount_SpaceTest :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.SpaceTest.Goal :=
  spaceTest_generic FixedPopcount.model

theorem sound_FixedPopcount_Cofactor :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.Cofactor.Goal :=
  cofactor_generic FixedPopcount.model

theorem sound_AtMostHits_Ordinal :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.Ordinal.Goal :=
  ordinal_generic AtMostHits.model

theorem sound_AtMostHits_Decoder :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.Decoder.Goal :=
  decoder_generic AtMostHits.model

theorem sound_AtMostHits_RejectSlack :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.RejectSlack.Goal :=
  rejectSlack_generic AtMostHits.model

theorem sound_AtMostHits_PrefixRange :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.PrefixRange.Goal :=
  prefixRange_generic AtMostHits.model

theorem sound_AtMostHits_SplitCode :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.SplitCode.Goal :=
  splitCode_generic AtMostHits.model

theorem sound_AtMostHits_WireLookup :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.WireLookup.Goal :=
  wireLookup_generic AtMostHits.model

theorem sound_AtMostHits_SpaceTest :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.SpaceTest.Goal :=
  spaceTest_generic AtMostHits.model

theorem sound_AtMostHits_Cofactor :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.Cofactor.Goal :=
  cofactor_generic AtMostHits.model

theorem sound_IndependentHits_Ordinal :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.Ordinal.Goal :=
  ordinal_generic IndependentHits.model

theorem sound_IndependentHits_Decoder :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.Decoder.Goal :=
  decoder_generic IndependentHits.model

theorem sound_IndependentHits_RejectSlack :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.RejectSlack.Goal :=
  rejectSlack_generic IndependentHits.model

theorem sound_IndependentHits_PrefixRange :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.PrefixRange.Goal :=
  prefixRange_generic IndependentHits.model

theorem sound_IndependentHits_SplitCode :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.SplitCode.Goal :=
  splitCode_generic IndependentHits.model

theorem sound_IndependentHits_WireLookup :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.WireLookup.Goal :=
  wireLookup_generic IndependentHits.model

theorem sound_IndependentHits_SpaceTest :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.SpaceTest.Goal :=
  spaceTest_generic IndependentHits.model

theorem sound_IndependentHits_Cofactor :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.Cofactor.Goal :=
  cofactor_generic IndependentHits.model

theorem sound_PairedHoles_Ordinal :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.Ordinal.Goal :=
  ordinal_generic PairedHoles.model

theorem sound_PairedHoles_Decoder :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.Decoder.Goal :=
  decoder_generic PairedHoles.model

theorem sound_PairedHoles_RejectSlack :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.RejectSlack.Goal :=
  rejectSlack_generic PairedHoles.model

theorem sound_PairedHoles_PrefixRange :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.PrefixRange.Goal :=
  prefixRange_generic PairedHoles.model

theorem sound_PairedHoles_SplitCode :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.SplitCode.Goal :=
  splitCode_generic PairedHoles.model

theorem sound_PairedHoles_WireLookup :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.WireLookup.Goal :=
  wireLookup_generic PairedHoles.model

theorem sound_PairedHoles_SpaceTest :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.SpaceTest.Goal :=
  spaceTest_generic PairedHoles.model

theorem sound_PairedHoles_Cofactor :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.Cofactor.Goal :=
  cofactor_generic PairedHoles.model

theorem sound_PrefixOnes_Ordinal :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.Ordinal.Goal :=
  ordinal_generic PrefixOnes.model

theorem sound_PrefixOnes_Decoder :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.Decoder.Goal :=
  decoder_generic PrefixOnes.model

theorem sound_PrefixOnes_RejectSlack :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.RejectSlack.Goal :=
  rejectSlack_generic PrefixOnes.model

theorem sound_PrefixOnes_PrefixRange :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.PrefixRange.Goal :=
  prefixRange_generic PrefixOnes.model

theorem sound_PrefixOnes_SplitCode :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.SplitCode.Goal :=
  splitCode_generic PrefixOnes.model

theorem sound_PrefixOnes_WireLookup :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.WireLookup.Goal :=
  wireLookup_generic PrefixOnes.model

theorem sound_PrefixOnes_SpaceTest :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.SpaceTest.Goal :=
  spaceTest_generic PrefixOnes.model

theorem sound_PrefixOnes_Cofactor :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.Cofactor.Goal :=
  cofactor_generic PrefixOnes.model

/-! Model-specific counting and framing facts. -/

private theorem fp_count_succ (n q : Nat) :
    count FixedPopcount.model (n + 1) q =
      count FixedPopcount.model n q +
        if q = 0 then 0 else count FixedPopcount.model n (q - 1) := by
  rw [count_succ]
  cases q with
  | zero => simp [lowCount, hiCount, FixedPopcount.model]
  | succ q => simp [lowCount, hiCount, FixedPopcount.model]

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
    subst this
    simp
  | succ n ih =>
    by_cases hk0 : k = 0
    · subst k
      simp only [Nat.sub_zero, fp_count_succ, if_pos rfl, Nat.add_zero]
      rw [fp_count_zero_right]
      have hnn : count FixedPopcount.model (n + 1) (n + 1) = 1 := by
        rw [fp_count_succ]
        have h1 : count FixedPopcount.model n (n + 1) = 0 := fp_count_gt n (n + 1) (by omega)
        have h2 : count FixedPopcount.model n n = 1 := by
          calc
            count FixedPopcount.model n n = count FixedPopcount.model n (n - n) :=
              ih n (Nat.le_refl n)
            _ = count FixedPopcount.model n 0 := by rw [Nat.sub_self]
            _ = 1 := fp_count_zero_right n
        simp [h1, h2]
      exact hnn.symm
    · by_cases hkn : k = n + 1
      · subst k
        simp only [Nat.sub_self, fp_count_succ, if_pos rfl, Nat.add_zero]
        rw [fp_count_zero_right]
        have hnn : count FixedPopcount.model (n + 1) (n + 1) = 1 := by
          rw [fp_count_succ]
          have h1 : count FixedPopcount.model n (n + 1) = 0 := fp_count_gt n (n + 1) (by omega)
          have h2 : count FixedPopcount.model n n = 1 := by
            calc
              count FixedPopcount.model n n = count FixedPopcount.model n (n - n) :=
                ih n (Nat.le_refl n)
              _ = count FixedPopcount.model n 0 := by rw [Nat.sub_self]
              _ = 1 := fp_count_zero_right n
          simp [h1, h2]
        exact hnn
      · have hk' : k ≤ n := by omega
        rw [fp_count_succ, fp_count_succ]
        have hterm : (if k = 0 then 0 else count FixedPopcount.model n (k - 1)) =
            count FixedPopcount.model n (k - 1) := by simp [hk0]
        rw [hterm]
        have hnk : n + 1 - k ≠ 0 := by omega
        have hterm2 : (if n + 1 - k = 0 then 0 else
            count FixedPopcount.model n (n + 1 - k - 1)) =
            count FixedPopcount.model n (n - k) := by
          simp [hnk]
          congr 1
          omega
        rw [hterm2]
        have ih1 := ih k hk'
        have ih2 := ih (k - 1) (by omega)
        have hsub : n - (k - 1) = n + 1 - k := by omega
        rw [hsub] at ih2
        omega

theorem sound_FixedPopcount_ComplementSymmetry :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.ComplementSymmetry.Goal := by
  intro n k hk
  simp only [TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.ComplementSymmetry.Goal,
    codeBits]
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
    | zero => rw [fp_count_succ, fp_count_zero_right]; simp [Samples.choose]
    | succ k =>
      rw [fp_count_succ]
      simp [ih, Samples.choose]
      omega

private theorem fp_run_count (xs : List Bool) :
    run FixedPopcount.model (xs.count true) xs = some 0 := by
  induction xs with
  | nil => simp [run, FixedPopcount.model]
  | cons b bs ih =>
    cases b with
    | false =>
      simp only [List.count_cons, Bool.false_eq_true, ite_false, Bool.toNat_false, Nat.sub_zero]
      change run FixedPopcount.model (bs.count true) (false :: bs) = some 0
      rw [run_cons]
      simp only [FixedPopcount.model, Bool.false_eq_true, ite_false, Option.bind_some]
      exact ih
    | true =>
      simp only [List.count_cons, ite_true, Bool.toNat_true]
      change run FixedPopcount.model (bs.count true + 1) (true :: bs) = some 0
      rw [run_cons]
      simp only [FixedPopcount.model, ite_true, Bool.and_self,
        Nat.add_one_ne_zero, Bool.false_eq_true, ite_false, Nat.add_sub_cancel,
        Option.bind_some]
      exact ih

private theorem fp_legal_count (xs : List Bool) :
    legal FixedPopcount.model (xs.count true) xs = true := by
  rw [legal, fp_run_count]
  rfl

theorem sound_FixedPopcount_PopulationFrame :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.PopulationFrame.Goal := by
  intro xs _hlen
  simp only [TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount.PopulationFrame.Goal]
  have hleg : legal FixedPopcount.model (xs.count true) xs = true := fp_legal_count xs
  have hrank := ordinal_generic FixedPopcount.model (xs.count true) xs hleg
  cases hr : rank FixedPopcount.model (xs.count true) xs with
  | none => simp [hrank] at hr
  | some r =>
    simp only [hr, Option.getD_some, List.cons_append, List.nil_append,
      List.head?_cons, List.drop_succ_cons, List.drop_zero]
    have hdec := decoder_generic FixedPopcount.model (xs.count true) xs r hr
    have hcount : count FixedPopcount.model xs.length (xs.count true) =
        Samples.choose xs.length (xs.count true) := fp_count_eq_choose xs.length (xs.count true)
    have hcb : codeBits FixedPopcount.model xs.length (xs.count true) =
        width (Samples.choose xs.length (xs.count true) - 1) := by
      simp [codeBits, hcount]
    rw [hcb, fromLittle_little]
    have hbound := rank_bound FixedPopcount.model xs (xs.count true) r hr
    have hcbound : r < 2 ^ codeBits FixedPopcount.model xs.length (xs.count true) :=
      codeBits_bound FixedPopcount.model xs.length (xs.count true) r
        (by simpa [hcount] using hbound)
    have hbc : codeBits FixedPopcount.model xs.length (xs.count true) ≤
        8 * byteCount (codeBits FixedPopcount.model xs.length (xs.count true)) := by
      simp [byteCount]
      omega
    have hpow : 256 ^ byteCount (codeBits FixedPopcount.model xs.length (xs.count true)) =
        2 ^ (8 * byteCount (codeBits FixedPopcount.model xs.length (xs.count true))) := by
      rw [show (256 : Nat) = 2 ^ 8 from rfl, ← Nat.pow_mul]
    have hmod : r % 256 ^ byteCount (codeBits FixedPopcount.model xs.length (xs.count true)) = r := by
      rw [hpow]
      exact Nat.mod_eq_of_lt
        (Nat.lt_of_lt_of_le hcbound (Nat.pow_le_pow_right (by decide) hbc))
    rw [← hcb, hmod]
    constructor
    · exact hdec
    · simp [little, hcb, Nat.add_comm]

private theorem am_count_succ (n q : Nat) :
    count AtMostHits.model (n + 1) q =
      count AtMostHits.model n q +
        if q = 0 then 0 else count AtMostHits.model n (q - 1) := by
  rw [count_succ]
  cases q with
  | zero => simp [lowCount, hiCount, AtMostHits.model]
  | succ q => simp [lowCount, hiCount, AtMostHits.model]

private theorem am_count_budget0 (n : Nat) : count AtMostHits.model n 0 = 1 := by
  induction n with
  | zero => simp [count, AtMostHits.model]
  | succ n ih => rw [am_count_succ, ih]; simp

theorem sound_AtMostHits_OneOutlier :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.OneOutlier.Goal := by
  intro n
  induction n with
  | zero => simp [count, AtMostHits.model]
  | succ n ih => rw [am_count_succ, am_count_budget0]; simp; omega

theorem sound_AtMostHits_UnrestrictedBudget :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.UnrestrictedBudget.Goal := by
  intro n q hq
  induction n generalizing q with
  | zero => simp [count, AtMostHits.model]
  | succ n ih =>
    have hq1 : n ≤ q := by omega
    have hq2 : n ≤ q - 1 := by omega
    rw [am_count_succ]
    have hqne : q ≠ 0 := by omega
    simp [hqne, ih q hq1, ih (q - 1) hq2, Nat.pow_succ]
    omega

private theorem ih_count_succ_0 (n : Nat) :
    count IndependentHits.model (n + 1) 0 =
      count IndependentHits.model n 0 + count IndependentHits.model n 1 := by
  rw [count_succ]
  simp [lowCount, hiCount, IndependentHits.model]

private theorem ih_count_succ_1 (n : Nat) :
    count IndependentHits.model (n + 1) 1 = count IndependentHits.model n 0 := by
  rw [count_succ]
  simp [lowCount, hiCount, IndependentHits.model]

theorem sound_IndependentHits_CheapConcatenation :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.CheapConcatenation.Goal := by
  intro q a b hla hlb
  obtain ⟨qa, ha⟩ := legal_run_some IndependentHits.model a q hla
  obtain ⟨qb, hb⟩ := legal_run_some IndependentHits.model b 0 hlb
  have hsep : run IndependentHits.model qa [false] = some 0 := by
    cases qa with
    | zero => simp [run, List.foldlM, IndependentHits.model]
    | succ qn => simp [run, List.foldlM, IndependentHits.model]
  have hleft : legal IndependentHits.model q (a ++ [false] ++ b) = true := by
    rw [legal, run_append, run_append, ha]
    simp only [Option.bind_some]
    rw [hsep, hb]
    rfl
  exact hleft.trans hlb.symm

theorem sound_IndependentHits_FibonacciCount :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits.FibonacciCount.Goal := by
  intro n
  rw [ih_count_succ_0, ih_count_succ_1]

private theorem ph_run_state : ∀ (xs : List Bool) (q : Nat), q < 2 →
    run PairedHoles.model q xs = none ∨
      ∃ q', q' < 2 ∧ run PairedHoles.model q xs = some q'
  | [], q, hq => Or.inr ⟨q, hq, by simp [run]⟩
  | b :: bs, q, hq => by
    cases b with
    | false =>
      have hs : PairedHoles.model.step q false = some (1 - q) := by
        simp [PairedHoles.model]
      rw [run_cons, hs]
      simp only [Option.bind_some]
      exact ph_run_state bs (1 - q) (by omega)
    | true =>
      cases q with
      | zero =>
        have hs : PairedHoles.model.step 0 true = some 0 := by
          simp [PairedHoles.model]
        rw [run_cons, hs]
        simp only [Option.bind_some]
        exact ph_run_state bs 0 (by decide)
      | succ qn =>
        have hqn : qn = 0 := by omega
        subst qn
        simp [run_cons, PairedHoles.model]

private theorem ph_run_ff (q : Nat) (hq : q < 2) :
    run PairedHoles.model q [false, false] = some q := by
  cases q with
  | zero => simp [run, List.foldlM, PairedHoles.model]
  | succ qn =>
    have hqn : qn = 0 := by omega
    subst qn
    simp [run, List.foldlM, PairedHoles.model]

theorem sound_PairedHoles_EraseZeroPair :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.EraseZeroPair.Goal := by
  intro a b
  cases ha : run PairedHoles.model 0 a with
  | none => simp [legal, run_append, ha]
  | some qa =>
    rcases ph_run_state a 0 (by decide) with hnone | ⟨q', hq', hrun⟩
    · simp [hnone] at ha
    · have hqa : q' = qa := by simpa [hrun] using ha
      subst q'
      have hrun' : run PairedHoles.model 0 (a ++ [false, false] ++ b) =
          run PairedHoles.model 0 (a ++ b) := by
        rw [run_append, run_append, ha]
        simp only [Option.bind_some]
        rw [run_append, ph_run_ff qa hq']
        simp
      simp only [legal, hrun']

private theorem ph_run_replicate_false : ∀ (n q : Nat), q < 2 →
    run PairedHoles.model q (List.replicate n false) = some ((q + n) % 2)
  | 0, q, _ => by simp [run]
  | n + 1, q, hq => by
    rw [List.replicate_succ, run_cons]
    have hs : PairedHoles.model.step q false = some (1 - q) := by
      simp [PairedHoles.model]
    rw [hs]
    simp only [Option.bind_some]
    rw [ph_run_replicate_false n (1 - q) (by omega)]
    congr 1
    omega

theorem sound_PairedHoles_EvenTail :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.EvenTail.Goal := by
  intro n
  simp only [TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles.EvenTail.Goal, legal]
  rw [ph_run_replicate_false n 0 (by decide)]
  simp [PairedHoles.model]

private theorem po_count_succ_0 (n : Nat) :
    count PrefixOnes.model (n + 1) 0 =
      count PrefixOnes.model n 1 + count PrefixOnes.model n 0 := by
  rw [count_succ]
  simp [lowCount, hiCount, PrefixOnes.model]

private theorem po_count_succ_1 (n : Nat) :
    count PrefixOnes.model (n + 1) 1 = count PrefixOnes.model n 1 := by
  rw [count_succ]
  simp [lowCount, hiCount, PrefixOnes.model]

private theorem po_count_1 (n : Nat) : count PrefixOnes.model n 1 = 1 := by
  induction n with
  | zero => simp [count, PrefixOnes.model]
  | succ n ih => rw [po_count_succ_1, ih]

theorem sound_PrefixOnes_CutCount :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.CutCount.Goal := by
  intro n
  induction n with
  | zero => simp [count, PrefixOnes.model]
  | succ n ih => rw [po_count_succ_0, po_count_1, ih]; omega

private theorem po_rank_zeros_1 (n : Nat) :
    rank PrefixOnes.model 1 (List.replicate n false) = some 0 := by
  induction n with
  | zero => simp [rank, PrefixOnes.model]
  | succ n ih =>
    rw [List.replicate_succ, rank_cons]
    have hs : PrefixOnes.model.step 1 false = some 1 := by simp [PrefixOnes.model]
    simp [hs, ih, lowCount, PrefixOnes.model]

private theorem po_rank_zeros_0 (n : Nat) :
    rank PrefixOnes.model 0 (List.replicate n false) = some 0 := by
  induction n with
  | zero => simp [rank, PrefixOnes.model]
  | succ n ih =>
    rw [List.replicate_succ, rank_cons]
    have hs : PrefixOnes.model.step 0 false = some 1 := by simp [PrefixOnes.model]
    simp [hs, po_rank_zeros_1, lowCount, PrefixOnes.model]

private theorem po_rank_ones_zeros : ∀ (k n : Nat), k ≤ n →
    rank PrefixOnes.model 0
      (List.replicate k true ++ List.replicate (n - k) false) = some k
  | 0, n, _ => by simpa using po_rank_zeros_0 n
  | k + 1, n, hn => by
    have hlist : List.replicate (k + 1) true ++ List.replicate (n - (k + 1)) false =
        true :: (List.replicate k true ++ List.replicate ((n - 1) - k) false) := by
      simp [List.replicate_succ]
      congr 1
      congr 1
      omega
    rw [hlist, rank_cons]
    have hs : PrefixOnes.model.step 0 true = some 0 := by simp [PrefixOnes.model]
    rw [hs]
    have ih' := po_rank_ones_zeros k (n - 1) (by omega)
    rw [ih']
    simp [lowCount, PrefixOnes.model, po_count_1]

theorem sound_PrefixOnes_CutOrdinal :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes.CutOrdinal.Goal := by
  intro n k hk
  exact po_rank_ones_zeros k n hk

end TinylambOpt.ResearchProofs.StorageExecution.RankedMasksA
