import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Named results include `not_eq_true`, `beq_eq_false`, `run_nil`.
-/
set_option maxHeartbeats 0
set_option linter.unusedVariables false

/-! Generic kernel: eight structural properties hold for EVERY machine. -/
namespace TinylambOpt.ResearchProofs.StorageExecution.SE_SK
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.Compression.Wire

variable {m : Machine}

theorem not_eq_true {b : Bool} (h : ¬ (b = true)) : b = false := by
  cases b <;> simp_all

theorem beq_eq_false {α : Type} [BEq α] [LawfulBEq α] [DecidableEq α] {x y : α} (h : ¬ x = y) :
    (x == y) = false := by
  by_cases hd : (x == y) = true
  · exact absurd ((beq_iff_eq (α := α)).mp hd) h
  · exact not_eq_true hd

/-! ### run / legal basics -/

theorem run_nil (q : Nat) : run m q [] = some q := rfl

theorem run_cons (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun s => run m s bs) := rfl

theorem run_append (q : Nat) (xs ys : List Bool) :
    run m q (xs ++ ys) = (run m q xs).bind (fun s => run m s ys) := by
  induction xs generalizing q with
  | nil => simp only [List.nil_append, run_nil, Option.bind_some]
  | cons b bs ih =>
    rw [List.cons_append, run_cons, run_cons]
    cases h : m.step q b with
    | none => simp only [h, Option.bind_none]
    | some s => rw [Option.bind_some, Option.bind_some, ih]

theorem legal_nil (q : Nat) : legal m q [] = m.accept q := by
  simp only [legal, run_nil, Option.map_some, Option.getD_some]

theorem legal_cons_some (q q' : Nat) (b : Bool) (bs : List Bool) (h : m.step q b = some q') :
    legal m q (b :: bs) = legal m q' bs := by
  simp only [legal, run_cons, h, Option.bind_some]

theorem legal_cons_none (q : Nat) (b : Bool) (bs : List Bool) (h : m.step q b = none) :
    legal m q (b :: bs) = false := by
  simp only [legal, run_cons, h, Option.bind_none, Option.map_none, Option.getD_none]

/-! ### words facts -/

theorem bs_in_words (xs : List Bool) : xs ∈ words xs.length := by
  induction xs with
  | nil => exact List.Mem.head _
  | cons b bs ih =>
    have h2 : b :: bs ∈ words (Nat.succ bs.length) := by
      cases b with
      | false =>
        exact List.mem_append.mpr
          (Or.inl (List.mem_map_of_mem (f := fun x => false :: x) ih))
      | true =>
        exact List.mem_append.mpr
          (Or.inr (List.mem_map_of_mem (f := fun x => true :: x) ih))
    exact h2

theorem filter_false (l : List (List Bool)) : l.filter (fun _ => false) = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp

/-! ### branch decompositions of filtered cons -/

theorem filter_cons_map_some (n q : Nat) (b : Bool) (s : Nat) (h : m.step q b = some s) :
    ((words n).map (b :: ·)).filter (legal m q) =
      ((words n).filter (legal m s)).map (b :: ·) := by
  have key : legal m q ∘ (fun x => b :: x) = legal m s := by
    funext x; exact legal_cons_some q s b x h
  rw [List.filter_map, key]

theorem filter_cons_map_none (n q : Nat) (b : Bool) (h : m.step q b = none) :
    ((words n).map (b :: ·)).filter (legal m q) = [] := by
  have key : legal m q ∘ (fun x => b :: x) = fun _ => false := by
    funext x; exact legal_cons_none q b x h
  rw [List.filter_map, key, filter_false, List.map_nil]

/-! ### count = cardinality of accepted words -/

theorem card_count : ∀ n q : Nat,
    ((words n).filter (legal m q)).length = count m n q := by
  intro n
  induction n with
  | zero =>
    intro q
    cases hq : m.accept q
    · rw [words, List.filter_cons, List.filter_nil, legal_nil, hq]
      rw [show count m 0 q = 0 from by show (m.accept q).toNat = 0; rw [hq]; rfl]
      rfl
    · rw [words, List.filter_cons, List.filter_nil, legal_nil, hq]
      rw [show count m 0 q = 1 from by show (m.accept q).toNat = 1; rw [hq]; rfl]
      rfl
  | succ n ih =>
    intro q
    rw [words, List.filter_append, List.length_append]
    rw [show count m (n + 1) q = lowCount m n q +
        ((m.step q true).map (count m n)).getD 0 from rfl]
    cases h0 : m.step q false with
    | none =>
      rw [filter_cons_map_none n q false h0, List.length_nil]
      cases h1 : m.step q true with
      | none =>
        rw [filter_cons_map_none n q true h1, List.length_nil]
        rw [lowCount, h0]
        simp only [Option.map_none, Option.getD_none]
      | some s1 =>
        rw [filter_cons_map_some n q true s1 h1]
        rw [List.length_map, ih s1]
        rw [lowCount, h0]
        simp only [Option.map_none, Option.getD_none, Option.map_some, Option.getD_some]
    | some s0 =>
      rw [filter_cons_map_some n q false s0 h0, List.length_map, ih s0]
      cases h1 : m.step q true with
      | none =>
        rw [filter_cons_map_none n q true h1, List.length_nil]
        rw [lowCount, h0]
        simp only [Option.map_none, Option.getD_none, Option.map_some, Option.getD_some]
      | some s1 =>
        rw [filter_cons_map_some n q true s1 h1, List.length_map, ih s1]
        rw [lowCount, h0]
        simp only [Option.map_some, Option.getD_some]

/-! ### lowCount = cardinality of the false branch -/

theorem lowCount_eq_card (n q : Nat) :
    lowCount m n q = (((words n).map (false :: ·)).filter (legal m q)).length := by
  cases h : m.step q false with
  | none => rw [filter_cons_map_none n q false h]; simp only [lowCount, h, Option.map_none,
      Option.getD_none, List.length_nil]
  | some s =>
    rw [filter_cons_map_some n q false s h, List.length_map, card_count, lowCount, h]
    simp only [Option.map_some, Option.getD_some]

/-! ### idxOf helpers -/

theorem idxOf_map_false (l : List (List Bool)) (bs : List Bool) :
    (l.map (false :: ·)).idxOf (false :: bs) = l.idxOf bs := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.map_cons, List.idxOf_cons, List.idxOf_cons]
    by_cases h : x = bs
    · subst h; simp
    · have h2 : ((false :: x) == (false :: bs)) = false :=
        beq_eq_false (fun he => h (by simpa using he))
      have h3 : (x == bs) = false := beq_eq_false h
      simp only [h2, h3, Bool.false_eq_true, reduceIte, ih]

theorem idxOf_map_true (l : List (List Bool)) (bs : List Bool) :
    (l.map (true :: ·)).idxOf (true :: bs) = l.idxOf bs := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.map_cons, List.idxOf_cons, List.idxOf_cons]
    by_cases h : x = bs
    · subst h; simp
    · have h2 : ((true :: x) == (true :: bs)) = false :=
        beq_eq_false (fun he => h (by simpa using he))
      have h3 : (x == bs) = false := beq_eq_false h
      simp only [h2, h3, Bool.false_eq_true, reduceIte, ih]

theorem idxOf_unique {x y : List Bool} {l : List (List Bool)}
    (hx : x ∈ l) (hy : y ∈ l) (h : l.idxOf x = l.idxOf y) : x = y := by
  induction l with
  | nil => simp at hx
  | cons a as ih =>
    rw [List.idxOf_cons, List.idxOf_cons] at h
    by_cases hxa : (a == x) = true
    · rw [if_pos hxa] at h
      by_cases hay : (a == y) = true
      · rw [if_pos hay] at h
        have h1 : a = x := (beq_iff_eq (α := List Bool)).mp hxa
        have h2 : a = y := (beq_iff_eq (α := List Bool)).mp hay
        exact h1.symm.trans h2
      · rw [if_neg hay] at h
        exact absurd h (by omega)
    · rw [if_neg hxa] at h
      by_cases hay : (a == y) = true
      · rw [if_pos hay] at h
        exact absurd h (by omega)
      · rw [if_neg hay] at h
        cases hx with
        | head as' => exact absurd (beq_self_eq_true (α := List Bool) _) hxa
        | tail b' hm =>
          cases hy with
          | head as'' => exact absurd (beq_self_eq_true (α := List Bool) _) hay
          | tail b'' hm2 => exact ih hm hm2 (Nat.succ.inj h)

/-! ### rank equation forms -/

theorem rank_nil (q : Nat) : rank m q [] = if m.accept q then some 0 else none := by
  cases h : m.accept q <;> simp [rank, h]

theorem rank_cons (q : Nat) (b : Bool) (bs : List Bool) :
    rank m q (b :: bs) =
      (m.step q b).bind (fun s =>
        (rank m s bs).map (fun t => t + (if b then lowCount m bs.length q else 0))) := by
  cases h : m.step q b with
  | none => simp [rank, h]
  | some s =>
    cases hs : rank m s bs with
    | none => simp [rank, h, hs]
    | some t => simp [rank, h, hs]

/-! ### master lemma: rank = lex index -/

theorem rank_some_info : ∀ (xs : List Bool) (q r : Nat), rank m q xs = some r →
    legal m q xs ∧
    ((words xs.length).filter (legal m q)).idxOf xs = r ∧
    r < count m xs.length q := by
  intro xs
  induction xs with
  | nil =>
    intro q r hr
    rw [rank_nil] at hr
    cases hacc : m.accept q
    · rw [hacc] at hr
      simp at hr
    · rw [hacc] at hr
      simp at hr
      rw [← hr]
      refine ⟨by rw [legal_nil]; exact hacc, ?_, ?_⟩
      · show ((List.filter (legal m q) [[]] : List (List Bool)).idxOf []) = 0
        rw [List.filter_cons, List.filter_nil,
          show legal m q [] = true from by rw [legal_nil]; exact hacc]
        rfl
      · rw [List.length_nil]
        rw [show count m 0 q = 1 from by show (m.accept q).toNat = 1; rw [hacc]; rfl]
        exact Nat.zero_lt_one
  | cons b bs ih =>
    intro q r hr
    simp only [List.length_cons]
    rw [rank_cons] at hr
    cases b
    · simp only [Bool.false_eq_true, reduceIte, ite_false, Nat.add_zero] at hr
      cases hstep : m.step q false with
      | none => rw [hstep, Option.bind_none] at hr; exact absurd hr (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at hr
        cases hs : rank m s bs with
        | none => rw [hs] at hr; exact absurd hr (by simp)
        | some t =>
          rw [hs, Option.map_some] at hr
          have hrt : r = t := Option.some.inj hr.symm
          obtain ⟨legal_s, idx_h, lt_h⟩ := ih s t hs
          refine ⟨by rw [legal_cons_some q s false bs hstep]; exact legal_s, ?_, ?_⟩
          · have hm : (false :: bs) ∈
              ((words bs.length).map (false :: ·)).filter (legal m q) := by
              refine List.mem_filter.mpr
                ⟨List.mem_map_of_mem (f := fun x => false :: x) (bs_in_words bs), ?_⟩
              rw [legal_cons_some q s false bs hstep]
              exact legal_s
            rw [words, List.filter_append, List.idxOf_append, if_pos hm,
              filter_cons_map_some bs.length q false s hstep, idxOf_map_false, idx_h]
            exact hrt.symm
          · rw [show count m (bs.length + 1) q = lowCount m bs.length q +
                ((m.step q true).map (count m bs.length)).getD 0 from rfl]
            rw [show lowCount m bs.length q = count m bs.length s from by
              show ((m.step q false).map (count m bs.length)).getD 0 = _
              rw [hstep]; rfl]
            omega
    · cases hstep : m.step q true with
      | none => rw [hstep, Option.bind_none] at hr; exact absurd hr (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at hr
        cases hs : rank m s bs with
        | none => rw [hs] at hr; exact absurd hr (by simp)
        | some t =>
          rw [hs, Option.map_some] at hr
          obtain rfl : r = t + lowCount m bs.length q := Option.some.inj hr.symm
          obtain ⟨legal_s, idx_h, lt_h⟩ := ih s t hs
          refine ⟨by rw [legal_cons_some q s true bs hstep]; exact legal_s, ?_, ?_⟩
          · have hn : ¬ (true :: bs) ∈
              ((words bs.length).map (false :: ·)).filter (legal m q) := by
              intro hh
              obtain ⟨hmem, _⟩ := List.mem_filter.mp hh
              obtain ⟨y, _, hy⟩ := List.mem_map.mp hmem
              exact absurd hy (by simp)
            rw [words, List.filter_append, List.idxOf_append, if_neg hn,
              lowCount_eq_card bs.length q,
              filter_cons_map_some bs.length q true s hstep, idxOf_map_true, idx_h]
          · rw [show count m (bs.length + 1) q = lowCount m bs.length q +
                ((m.step q true).map (count m bs.length)).getD 0 from rfl]
            rw [show ((m.step q true).map (count m bs.length)).getD 0 =
                count m bs.length s from by rw [hstep]; rfl]
            omega

theorem rank_some_of_legal : ∀ (xs : List Bool) (q : Nat), legal m q xs →
    ∃ r, rank m q xs = some r := by
  intro xs
  induction xs with
  | nil =>
    intro q h
    rw [rank_nil]
    rw [legal_nil] at h
    exact ⟨0, if_pos h⟩
  | cons b bs ih =>
    intro q h
    rw [rank_cons]
    cases hstep : m.step q b with
    | none =>
      rw [legal_cons_none q b bs hstep] at h
      exact absurd h (by simp)
    | some s =>
      rw [Option.bind_some]
      rw [legal_cons_some q s b bs hstep] at h
      obtain ⟨t, ht⟩ := ih s h
      refine ⟨t + (if b then lowCount m bs.length q else 0), ?_⟩
      rw [ht, Option.map_some]

theorem ordinal_gen : ordinal m := by
  intro q xs hlegal
  obtain ⟨r, hr⟩ := rank_some_of_legal xs q hlegal
  have idx_eq := (rank_some_info xs q r hr).2.1
  rw [hr, idx_eq]

theorem rank_lt_count (q : Nat) (xs : List Bool) (r : Nat) (hr : rank m q xs = some r) :
    r < count m xs.length q := (rank_some_info xs q r hr).2.2

theorem rank_some_imp_legal (q : Nat) (xs : List Bool) (r : Nat) (hr : rank m q xs = some r) :
    legal m q xs := (rank_some_info xs q r hr).1

theorem rank_inj {q : Nat} {xs ys : List Bool} (hx : legal m q xs) (hy : legal m q ys)
    (hlen : xs.length = ys.length) (h : rank m q xs = rank m q ys) : xs = ys := by
  obtain ⟨rx, hrx⟩ := rank_some_of_legal xs q hx
  obtain ⟨ry, hry⟩ := rank_some_of_legal ys q hy
  have hidx_x : List.idxOf xs ((words xs.length).filter (legal m q)) = rx :=
    (rank_some_info xs q rx hrx).2.1
  have hidx_y : List.idxOf ys ((words ys.length).filter (legal m q)) = ry :=
    (rank_some_info ys q ry hry).2.1
  rw [hrx, hry] at h
  obtain rfl : rx = ry := Option.some.inj h
  rw [← hlen] at hidx_y
  have hx' : xs ∈ (words xs.length).filter (legal m q) :=
    List.mem_filter.mpr ⟨bs_in_words xs, hx⟩
  have hy' : ys ∈ (words xs.length).filter (legal m q) :=
    List.mem_filter.mpr ⟨by rw [hlen]; exact bs_in_words ys, hy⟩
  exact idxOf_unique hx' hy' (hidx_x.trans hidx_y.symm)

/-! ### decoder -/

theorem unrank_zero (q r : Nat) :
    unrank m 0 q r = if r == 0 && m.accept q then some [] else none := by
  cases h : (r == 0 && m.accept q) <;> simp [unrank, h]

theorem unrank_succ (n q r : Nat) :
    unrank m (n + 1) q r =
      (m.step q (decide (lowCount m n q ≤ r))).bind (fun next =>
        (unrank m n next (if decide (lowCount m n q ≤ r) then r - lowCount m n q else r)).bind
          (fun bs => some (decide (lowCount m n q ≤ r) :: bs))) := rfl

theorem decoder_gen : decoder m := by
  intro q xs r hr
  induction xs generalizing q r with
  | nil =>
    rw [rank_nil] at hr
    cases hacc : m.accept q
    · rw [hacc] at hr
      simp at hr
    · rw [hacc] at hr
      simp at hr
      rw [← hr]
      simp only [List.length_nil]
      rw [unrank_zero, hacc]
      simp
  | cons b bs ih =>
    rw [List.length_cons]
    rw [rank_cons] at hr
    cases b
    · simp only [Bool.false_eq_true, reduceIte, ite_false, Nat.add_zero] at hr
      cases hstep : m.step q false with
      | none => rw [hstep, Option.bind_none] at hr; exact absurd hr (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at hr
        cases hs : rank m s bs with
        | none => rw [hs] at hr; exact absurd hr (by simp)
        | some t =>
          rw [hs, Option.map_some] at hr
          have hrt : r = t := Option.some.inj hr.symm
          have key := ih s t hs
          have lt_s := (rank_some_info bs s t hs).2.2
          rw [unrank_succ]
          have hb : decide (lowCount m bs.length q ≤ t) = false := by
            rw [show lowCount m bs.length q = count m bs.length s from by
              show ((m.step q false).map (count m bs.length)).getD 0 = _
              rw [hstep]; rfl]
            exact decide_eq_false_iff_not.mpr (by omega)
          rw [hrt, hb]
          simp only [hstep, Option.bind_some, Bool.false_eq_true, reduceIte, ite_false, key]
    · cases hstep : m.step q true with
      | none => rw [hstep, Option.bind_none] at hr; exact absurd hr (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at hr
        cases hs : rank m s bs with
        | none => rw [hs] at hr; exact absurd hr (by simp)
        | some t =>
          rw [hs, Option.map_some] at hr
          have hrt : r = t + lowCount m bs.length q := Option.some.inj hr.symm
          have key := ih s t hs
          rw [unrank_succ]
          rw [hrt]
          rw [show decide (lowCount m bs.length q ≤ t + lowCount m bs.length q) = true from
            decide_eq_true_eq.mpr (Nat.le_add_left _ _)]
          simp only [hstep, Option.bind_some, reduceIte, ite_true, Nat.add_sub_cancel, key]

/-! ### rejectSlack / completeness / correctness of unrank -/

theorem unrank_correct : ∀ (n q r : Nat) (bs : List Bool), unrank m n q r = some bs →
    legal m q bs ∧ rank m q bs = some r ∧ bs.length = n := by
  intro n
  induction n with
  | zero =>
    intro q r bs h
    rw [unrank_zero] at h
    by_cases hz0 : r = 0
    · by_cases hacc : m.accept q = true
      · have hc : (r == 0 && m.accept q) = true := by
          subst hz0
          simp [hacc]
        rw [hc] at h
        simp at h
        subst h
        refine ⟨by rw [legal_nil]; exact hacc, ?_, rfl⟩
        rw [rank_nil, hacc]
        simp only [reduceIte, ite_true]
        rw [hz0]
      · have hc : (r == 0 && m.accept q) = false := by simp [hacc]
        rw [hc] at h
        exact absurd h (by simp)
    · have hc : (r == 0 && m.accept q) = false := by
        have hbeq : (r == 0) = false := by
          cases r with
          | zero => exact absurd rfl hz0
          | succ r' => rfl
        simp [hbeq]
      rw [hc] at h
      exact absurd h (by simp)
  | succ n ih =>
    intro q r bs h
    rw [unrank_succ] at h
    by_cases hle : lowCount m n q ≤ r
    · have hb : decide (lowCount m n q ≤ r) = true := decide_eq_true_eq.mpr hle
      rw [hb] at h
      simp only [reduceIte, ite_true] at h
      cases hstep : m.step q true with
      | none => rw [hstep, Option.bind_none] at h; exact absurd h (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at h
        cases hun : unrank m n s (r - lowCount m n q) with
        | none => rw [hun, Option.bind_none] at h; exact absurd h (by simp)
        | some bs' =>
          rw [hun, Option.bind_some] at h
          obtain rfl : bs = true :: bs' := by simpa using h.symm
          obtain ⟨legal', rank', len'⟩ := ih s (r - lowCount m n q) bs' hun
          refine ⟨by rw [legal_cons_some q s true bs' hstep]; exact legal', ?_, ?_⟩
          · have hl : lowCount m bs'.length q = lowCount m n q := by rw [len']
            rw [rank_cons, hstep, Option.bind_some, rank', hl]
            simp only [Option.map_some]
            rw [show (if True then lowCount m n q else 0) = lowCount m n q from rfl]
            rw [Nat.sub_add_cancel hle]
          · rw [List.length_cons, len']
    · have hb : decide (lowCount m n q ≤ r) = false := decide_eq_false_iff_not.mpr hle
      rw [hb] at h
      simp only [Bool.false_eq_true, reduceIte, ite_false] at h
      cases hstep : m.step q false with
      | none => rw [hstep, Option.bind_none] at h; exact absurd h (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at h
        cases hun : unrank m n s r with
        | none => rw [hun, Option.bind_none] at h; exact absurd h (by simp)
        | some bs' =>
          rw [hun, Option.bind_some] at h
          obtain rfl : bs = false :: bs' := by simpa using h.symm
          obtain ⟨legal', rank', len'⟩ := ih s r bs' hun
          refine ⟨by rw [legal_cons_some q s false bs' hstep]; exact legal', ?_, ?_⟩
          · rw [rank_cons, hstep, Option.bind_some, rank']
            simp
          · rw [List.length_cons, len']

theorem unrank_complete : ∀ n q r : Nat, r < count m n q →
    ∃ bs, unrank m n q r = some bs := by
  intro n
  induction n with
  | zero =>
    intro q r h
    by_cases hacc : m.accept q = true
    · have hc : count m 0 q = 1 := by show (m.accept q).toNat = 1; simp [hacc]
      rw [hc] at h
      have : r = 0 := by omega
      subst this
      rw [unrank_zero]
      exact ⟨[], by simp [hacc]⟩
    · have hc : count m 0 q = 0 := by show (m.accept q).toNat = 0; simp [not_eq_true hacc]
      rw [hc] at h
      exact absurd h (by omega)
  | succ n ih =>
    intro q r h
    rw [unrank_succ]
    by_cases hle : lowCount m n q ≤ r
    · have hb : decide (lowCount m n q ≤ r) = true := decide_eq_true_eq.mpr hle
      rw [hb]
      simp only [reduceIte, ite_true]
      cases hstep : m.step q true with
      | none =>
        have hcount : count m (n + 1) q = lowCount m n q := by
          rw [show count m (n + 1) q = lowCount m n q +
              ((m.step q true).map (count m n)).getD 0 from rfl, hstep]
          rfl
        rw [hcount] at h
        exact absurd h (by omega)
      | some s =>
        simp only [Option.bind_some]
        have hcount : count m (n + 1) q = lowCount m n q + count m n s := by
          rw [show count m (n + 1) q = lowCount m n q +
              ((m.step q true).map (count m n)).getD 0 from rfl, hstep]
          simp only [Option.map_some, Option.getD_some]
        rw [hcount] at h
        obtain ⟨bs, hbs⟩ := ih s (r - lowCount m n q) (by omega)
        exact ⟨true :: bs, by rw [hbs, Option.bind_some]⟩
    · have hb : decide (lowCount m n q ≤ r) = false := decide_eq_false_iff_not.mpr hle
      rw [hb]
      simp only [Bool.false_eq_true, reduceIte, ite_false]
      cases hstep : m.step q false with
      | none =>
        have hz : lowCount m n q = 0 := by simp [lowCount, hstep]
        rw [hz] at hle
        exact absurd (Nat.zero_le r) hle
      | some s =>
        simp only [Option.bind_some]
        have hlow : lowCount m n q = count m n s := by
          show ((m.step q false).map (count m n)).getD 0 = _
          rw [hstep]; rfl
        obtain ⟨bs, hbs⟩ := ih s r (by rw [← hlow]; omega)
        exact ⟨false :: bs, by rw [hbs, Option.bind_some]⟩

theorem unrank_isNone : ∀ n q r : Nat,
    (unrank m n q r).isNone = decide (count m n q ≤ r) := by
  intro n
  induction n with
  | zero =>
    intro q r
    by_cases hacc : m.accept q = true
    · rw [show count m 0 q = 1 from by show (m.accept q).toNat = 1; simp [hacc], unrank_zero]
      rw [hacc]
      rcases r with (_ | _ | r) <;> simp
    · rw [show count m 0 q = 0 from by
        show (m.accept q).toNat = 0
        simp [not_eq_true hacc], unrank_zero]
      rw [not_eq_true hacc]
      rcases r with (_ | _ | r) <;> simp
  | succ n ih =>
    intro q r
    rw [unrank_succ]
    have hc : count m (n + 1) q = lowCount m n q +
        ((m.step q true).map (count m n)).getD 0 := rfl
    rw [hc]
    by_cases hle : lowCount m n q ≤ r
    · have hb : decide (lowCount m n q ≤ r) = true := decide_eq_true_eq.mpr hle
      rw [hb]
      simp only [reduceIte, ite_true]
      cases hstep : m.step q true with
      | none =>
        rw [Option.map_none, Option.getD_none, Option.bind_none, Option.isNone_none]
        exact ((decide_eq_true_eq (p := lowCount m n q + 0 ≤ r)).mpr (by omega)).symm
      | some s =>
        rw [Option.map_some, Option.getD_some, Option.bind_some]
        cases hun : unrank m n s (r - lowCount m n q) with
        | none =>
          have h1 := ih s (r - lowCount m n q)
          rw [hun, Option.isNone_none] at h1
          rw [Option.bind_none, Option.isNone_none]
          have hge : count m n s ≤ r - lowCount m n q := decide_eq_true_eq.mp h1.symm
          exact ((decide_eq_true_eq (p := lowCount m n q + count m n s ≤ r)).mpr
            (by omega)).symm
        | some t =>
          have h1 := ih s (r - lowCount m n q)
          rw [hun, Option.isNone_some] at h1
          rw [Option.bind_some, Option.isNone_some]
          have hnp : ¬ (count m n s ≤ r - lowCount m n q) :=
            (decide_eq_false_iff_not (p := count m n s ≤ r - lowCount m n q)).mp h1.symm
          exact ((decide_eq_false_iff_not
            (p := lowCount m n q + count m n s ≤ r)).mpr
            (fun hh => hnp (by omega))).symm
    · have hb : decide (lowCount m n q ≤ r) = false := decide_eq_false_iff_not.mpr hle
      rw [hb]
      simp only [Bool.false_eq_true, reduceIte, ite_false]
      cases hstep : m.step q false with
      | none =>
        have hz : lowCount m n q = 0 := by simp [lowCount, hstep]
        rw [hz] at hle
        exact absurd (Nat.zero_le r) hle
      | some s =>
        simp only [Option.bind_some]
        have hlow : lowCount m n q = count m n s := by
          show ((m.step q false).map (count m n)).getD 0 = _
          rw [hstep]; rfl
        cases hun : unrank m n s r with
        | none =>
          have h1 := ih s r
          rw [hun, Option.isNone_none] at h1
          rw [Option.bind_none, Option.isNone_none]
          have hge : count m n s ≤ r := decide_eq_true_eq.mp h1.symm
          rw [← hlow] at hge
          exact ((decide_eq_true_eq (p := lowCount m n q +
              ((m.step q true).map (count m n)).getD 0 ≤ r)).mpr (by omega)).symm
        | some t =>
          have h1 := ih s r
          rw [hun, Option.isNone_some] at h1
          rw [Option.bind_some, Option.isNone_some]
          have hnp : ¬ (count m n s ≤ r) :=
            (decide_eq_false_iff_not (p := count m n s ≤ r)).mp h1.symm
          exact ((decide_eq_false_iff_not (p := lowCount m n q +
              ((m.step q true).map (count m n)).getD 0 ≤ r)).mpr
            (fun hh => hnp (by rw [← hlow]; exact Nat.le_trans (Nat.le_add_right _ _) hh))).symm

theorem rejectSlack_gen : rejectSlack m := unrank_isNone

/-! ### descend / splitCode / prefixRange -/

theorem descend_nil (n q : Nat) : descend m n q [] = some (q, 0) := by simp [descend]

theorem descend_succ (n q : Nat) (b : Bool) (bs : List Bool) :
    descend m (n + 1) q (b :: bs) =
      (m.step q b).bind (fun q1 =>
        (descend m n q1 bs).bind (fun p =>
          some (p.1, p.2 + (if b then lowCount m n q else 0)))) := rfl

theorem descend_bound_length : ∀ n q (pre : List Bool) (x : Nat × Nat),
    descend m n q pre = some x → pre.length ≤ n := by
  intro n
  induction n with
  | zero =>
    intro q pre x hd
    cases pre with
    | nil => exact Nat.zero_le _
    | cons b bs =>
      have : descend m 0 q (b :: bs) = none := by simp [descend]
      rw [this] at hd; exact absurd hd (by simp)
  | succ n ih =>
    intro q pre x hd
    cases pre with
    | nil => exact Nat.zero_le _
    | cons b bs =>
      rw [descend_succ] at hd
      cases hstep : m.step q b with
      | none => rw [hstep, Option.bind_none] at hd; exact absurd hd (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at hd
        cases hd2 : descend m n s bs with
        | none => rw [hd2] at hd; exact absurd hd (by simp)
        | some p =>
          have := ih s bs p hd2
          rw [List.length_cons]
          omega

theorem descend_run : ∀ (n q : Nat) (pre : List Bool) (q' off : Nat),
    descend m n q pre = some (q', off) → run m q pre = some q' := by
  intro n
  induction n with
  | zero =>
    intro q pre q' off hd
    cases pre with
    | nil =>
      rw [descend_nil] at hd
      obtain ⟨hq, _⟩ := Prod.mk.inj (Option.some.inj hd)
      subst hq
      exact run_nil _
    | cons b bs =>
      have : descend m 0 q (b :: bs) = none := by simp [descend]
      rw [this] at hd; exact absurd hd (by simp)
  | succ n ih =>
    intro q pre q' off hd
    cases pre with
    | nil =>
      rw [descend_nil] at hd
      obtain ⟨hq, _⟩ := Prod.mk.inj (Option.some.inj hd)
      subst hq
      exact run_nil _
    | cons b bs =>
      rw [descend_succ] at hd
      rw [run_cons]
      cases hstep : m.step q b with
      | none => rw [hstep, Option.bind_none] at hd; exact absurd hd (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at hd
        cases hd2 : descend m n s bs with
        | none => rw [hd2] at hd; exact absurd hd (by simp)
        | some p =>
          rw [hd2, Option.bind_some] at hd
          obtain ⟨hq, _⟩ := Prod.mk.inj (Option.some.inj hd)
          subst hq
          rw [Option.bind_some, ih s bs p.1 p.2 hd2]

theorem descend_bound : ∀ (n q : Nat) (pre : List Bool) (q' off : Nat),
    descend m n q pre = some (q', off) →
      off + count m (n - pre.length) q' ≤ count m n q := by
  intro n
  induction n with
  | zero =>
    intro q pre q' off hd
    cases pre with
    | nil =>
      rw [descend_nil] at hd
      obtain ⟨hq, ho⟩ := Prod.mk.inj (Option.some.inj hd)
      subst hq; subst ho
      simp only [List.length_nil, Nat.sub_zero]
      omega
    | cons b bs =>
      have : descend m 0 q (b :: bs) = none := by simp [descend]
      rw [this] at hd; exact absurd hd (by simp)
  | succ n ih =>
    intro q pre q' off hd
    cases pre with
    | nil =>
      rw [descend_nil] at hd
      obtain ⟨hq, ho⟩ := Prod.mk.inj (Option.some.inj hd)
      subst hq; subst ho
      simp only [List.length_nil, Nat.sub_zero]
      omega
    | cons b bs =>
      rw [descend_succ] at hd
      cases hstep : m.step q b with
      | none => rw [hstep, Option.bind_none] at hd; exact absurd hd (by simp)
      | some s =>
        rw [hstep, Option.bind_some] at hd
        cases hd2 : descend m n s bs with
        | none => rw [hd2] at hd; exact absurd hd (by simp)
        | some p =>
          rw [hd2, Option.bind_some] at hd
          obtain ⟨hq, hoo⟩ := Prod.mk.inj (Option.some.inj hd)
          subst hq; subst hoo
          have blen : bs.length ≤ n := descend_bound_length n s bs p hd2
          have hihs := ih s bs p.1 p.2 hd2
          have hcount : count m (n + 1) q = lowCount m n q +
              ((m.step q true).map (count m n)).getD 0 := rfl
          rw [show (b :: bs).length = bs.length + 1 from rfl, Nat.add_sub_add_right] at ⊢
          cases b
          · have hlow : lowCount m n q = count m n s := by
              show ((m.step q false).map (count m n)).getD 0 = _
              rw [hstep]; rfl
            simp only [Bool.false_eq_true, reduceIte, ite_false, Nat.add_zero] at ⊢
            have h0 : 0 ≤ ((m.step q true).map (count m n)).getD 0 := Nat.zero_le _
            omega
          · have hi : ((m.step q true).map (count m n)).getD 0 = count m n s := by
              rw [hstep]; rfl
            rw [show (if true = true then lowCount m n q else 0) = lowCount m n q from rfl] at ⊢
            omega

theorem splitCode_gen : splitCode m := by
  intro q a
  induction a generalizing q with
  | nil =>
    intro b q' off hd
    rw [descend_nil] at hd
    obtain ⟨hq, ho⟩ := Prod.mk.inj (Option.some.inj hd)
    subst hq; subst ho
    cases hr : rank m q b with
    | none => simp [hr]
    | some v => simp [hr]
  | cons c as ih =>
    intro b q' off hd
    rw [show ((c :: as).length + b.length : Nat) = as.length + b.length + 1 from by
      rw [List.length_cons]; omega] at hd
    rw [descend_succ] at hd
    cases hstep : m.step q c with
    | none => rw [hstep, Option.bind_none] at hd; exact absurd hd (by simp)
    | some s =>
      rw [hstep, Option.bind_some] at hd
      cases hd2 : descend m (as.length + b.length) s as with
      | none => rw [hd2] at hd; exact absurd hd (by simp)
      | some p =>
        rw [hd2, Option.bind_some] at hd
        obtain ⟨hq, hoo⟩ := Prod.mk.inj (Option.some.inj hd)
        subst hq; subst hoo
        have hlenab : (as ++ b).length = as.length + b.length := List.length_append
        rw [List.cons_append, rank_cons, hstep, Option.bind_some, hlenab,
          ih s b p.1 p.2 hd2]
        cases hr : rank m p.1 b with
        | none => simp [hr]
        | some v =>
          rw [Option.map_some, Option.map_some, Option.map_some]
          cases c
          · rw [show (if (false : Bool) = true then lowCount m (as.length + b.length) q else 0) =
                (0 : Nat) from rfl, Nat.add_zero, Nat.add_zero]
          · rw [show (if (true : Bool) = true then lowCount m (as.length + b.length) q else 0) =
                lowCount m (as.length + b.length) q from rfl]
            exact congrArg some (by omega)

theorem prefix_exists {pre ys : List Bool} (h : pre.isPrefixOf ys = true) :
    ∃ tail, ys = pre ++ tail := by
  induction pre generalizing ys with
  | nil => exact ⟨ys, rfl⟩
  | cons c cs ih =>
    cases ys with
    | nil => exact absurd h (by rw [show (c :: cs).isPrefixOf ([] : List Bool) = false from rfl]; simp)
    | cons d ds =>
      have h' : (c == d && cs.isPrefixOf ds) = true := h
      rw [Bool.and_eq_true] at h'
      obtain ⟨h1, h2⟩ := h'
      have hc : c = d := (beq_iff_eq (α := Bool)).mp h1
      subst hc
      obtain ⟨t, ht⟩ := ih h2
      exact ⟨t, by rw [ht]; rfl⟩

theorem prefixRange_gen : prefixRange m := by
  intro n q pre q' off r hd hlt
  obtain ⟨ys, hys⟩ := unrank_complete n q r hlt
  obtain ⟨legal_y, rank_y, hleny⟩ := unrank_correct n q r ys hys
  have hle_pre : pre.length ≤ n := descend_bound_length n q pre _ hd
  rw [hys, Option.any_some]
  have hrange : off ≤ r → r < off + count m (n - pre.length) q' →
      (pre.isPrefixOf ys) = true := by
    intro h1 h2
    have tlt : r - off < count m (n - pre.length) q' := by omega
    obtain ⟨bs, hbs⟩ := unrank_complete (n - pre.length) q' (r - off) tlt
    obtain ⟨legal_b, rank_b, hlenbs⟩ := unrank_correct (n - pre.length) q' (r - off) bs hbs
    have hpl : pre.length + bs.length = n := by omega
    have hd' : descend m (pre.length + bs.length) q pre = some (q', off) := by
      rw [hpl]; exact hd
    have hsc := splitCode_gen q pre bs q' off hd'
    rw [rank_b] at hsc
    simp only [Option.map_some] at hsc
    have hleg : legal m q (pre ++ bs) := rank_some_imp_legal q (pre ++ bs) _ hsc
    have heq : ys = pre ++ bs :=
      rank_inj legal_y hleg (by rw [List.length_append, hleny, hpl])
        (by rw [rank_y, hsc, Nat.add_sub_cancel' h1])
    have hpf : ∀ (pre : List Bool), (pre.isPrefixOf (pre ++ bs)) = true := by
      intro pre
      induction pre with
      | nil => simp [List.isPrefixOf]
      | cons c cs ih =>
        rw [List.cons_append, List.isPrefixOf_cons_cons, beq_self_eq_true, Bool.true_and]
        exact ih
    rw [heq]
    exact hpf pre
  have hconv : (pre.isPrefixOf ys) = true →
      off ≤ r ∧ r < off + count m (n - pre.length) q' := by
    intro hpre
    have hpx : pre.isPrefixOf ys := by simpa using hpre
    obtain ⟨tail, ht⟩ := prefix_exists hpx
    have tlen : pre.length + tail.length = n := by
      rw [← List.length_append, ← ht, hleny]
    have tlen2 : tail.length = n - pre.length := by omega
    have hd' : descend m (pre.length + tail.length) q pre = some (q', off) := by
      rw [tlen]; exact hd
    have hrun : run m q pre = some q' := descend_run n q pre q' off hd
    have hleg : legal m q' tail := by
      have hrun2 : run m q ys = run m q' tail := by
        rw [ht, run_append, hrun, Option.bind_some]
      show ((run m q' tail).map m.accept).getD false
      rw [← hrun2]
      exact legal_y
    obtain ⟨v, hv⟩ := rank_some_of_legal tail q' hleg
    have hsc := splitCode_gen q pre tail q' off hd'
    rw [← ht, hv] at hsc
    simp only [Option.map_some] at hsc
    obtain rfl : r = off + v := Option.some.inj (rank_y.symm.trans hsc)
    have hltv := rank_lt_count q' tail v hv
    rw [tlen2] at hltv
    exact ⟨by omega, by omega⟩
  by_cases hc : off ≤ r ∧ r < off + count m (n - pre.length) q'
  · rw [show (decide _) = true from by simp [hc]]
    exact hrange hc.1 hc.2
  · rw [show (decide _) = false from by simp [hc]]
    by_cases hp : pre.isPrefixOf ys
    · exact absurd (hconv hp) hc
    · exact not_eq_true hp

/-! ### wire helpers -/

theorem width_gt (n : Nat) : n < 2 ^ width n := by
  by_cases h : n = 0
  · subst h; simp [width]
  · have hw : width n = n.log2 + 1 := by
      cases n with
      | zero => exact absurd rfl h
      | succ n' => rfl
    rw [hw]
    exact Nat.lt_log2_self

theorem eight_byteCover (w : Nat) : w ≤ 8 * byteCount w := by
  have e := Nat.div_add_mod (w + 7) 8
  have m := Nat.mod_lt (w + 7) (by decide : (0 : Nat) < 8)
  unfold byteCount at *
  omega

theorem fromLittle_append (xs ys : List Nat) :
    fromLittle (xs ++ ys) = fromLittle xs + 256 ^ xs.length * fromLittle ys := by
  induction xs generalizing ys with
  | nil => simp [fromLittle]
  | cons x xs ih =>
    have hp : 256 * (256 ^ xs.length * fromLittle ys) = 256 ^ (xs.length + 1) * fromLittle ys := by
      rw [Nat.pow_succ, ← Nat.mul_assoc, Nat.mul_comm 256 (256 ^ xs.length)]
    show x + 256 * fromLittle (xs ++ ys) = _
    rw [ih, Nat.mul_add, hp,
      show fromLittle (x :: xs) = x + 256 * fromLittle xs from rfl,
      show (256 : Nat) ^ (x :: xs).length = 256 ^ (xs.length + 1) from rfl,
      ← Nat.add_assoc]

theorem little_succ (v k : Nat) :
    little v (k + 1) = little v k ++ [v / 256 ^ k % 256] := by
  unfold little
  rw [List.range_succ, List.map_append]
  congr 1

theorem mod_split (v A : Nat) (hA : 0 < A) :
    v % A + A * (v / A % 256) = v % (A * 256) := by
  have h2 := Nat.div_add_mod (v / A) 256
  have hm : v / A % 256 < 256 := Nat.mod_lt _ (by decide)
  have ha : v % A < A := Nat.mod_lt _ hA
  have h2a : A * (256 * (v / A / 256)) + A * (v / A % 256) = A * (v / A) := by
    rw [← Nat.mul_add, h2]
  have h1 := Nat.div_add_mod v A
  have hAq : 256 * A * (v / A / 256) = A * (256 * (v / A / 256)) := by
    rw [Nat.mul_assoc, Nat.mul_left_comm 256 A (v / A / 256)]
  have key : 256 * A * (v / A / 256) + (v % A + A * (v / A % 256)) = v := by
    rw [hAq]
    omega
  have bound : v % A + A * (v / A % 256) < 256 * A := by
    have h3 : (v / A % 256 + 1) * A ≤ 256 * A := Nat.mul_le_mul_right A (by omega)
    rw [Nat.add_mul, Nat.one_mul] at h3
    rw [Nat.mul_comm]
    omega
  rw [Nat.mul_comm A 256]
  rw [show v % (256 * A) =
        (256 * A * (v / A / 256) + (v % A + A * (v / A % 256))) % (256 * A) from
      congrArg (fun t => t % (256 * A)) key.symm]
  rw [Nat.mul_add_mod, Nat.mod_eq_of_lt bound]

theorem fromLittle_little (v k : Nat) : fromLittle (little v k) = v % 256 ^ k := by
  induction k generalizing v with
  | zero => simp [little, fromLittle, Nat.mod_one]
  | succ k ih =>
    rw [little_succ, fromLittle_append, ih]
    have hl : (little v k).length = k := by
      unfold little
      rw [List.length_map, List.length_range]
    have hd : fromLittle [v / 256 ^ k % 256] = v / 256 ^ k % 256 := by
      show v / 256 ^ k % 256 + 256 * fromLittle [] = _
      simp [fromLittle]
    rw [hl, hd]
    rw [show (256 : Nat) ^ (k + 1) = 256 ^ k * 256 from Nat.pow_succ 256 k]
    exact mod_split v (256 ^ k) (Nat.pow_pos (by decide : (0 : Nat) < 256))

theorem bc_le (x k : Nat) : byteCount x ≤ k ↔ x ≤ 8 * k := by
  unfold byteCount
  rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 8)]
  constructor <;> intro h <;> omega

theorem get_pack_single (w r : Nat) (h : r < 2 ^ w) : get (pack w [r]) w 0 = r := by
  show (fromLittle (little (word w [r]) (byteCount ([r].length * w))) /
      2 ^ (0 * w)) % 2 ^ w = r
  have hw : [r].length * w = w := by simp
  rw [hw, Nat.zero_mul, Nat.pow_zero, Nat.div_one]
  have h1 : word w [r] = r % 2 ^ w := by
    show r % 2 ^ w + 2 ^ w * word w [] = _
    have hw0 : word w [] = 0 := rfl
    rw [hw0, Nat.mul_zero, Nat.add_zero]
  rw [h1, fromLittle_little]
  have h2 : r % 2 ^ w < 256 ^ byteCount w := by
    have : r % 2 ^ w < 2 ^ w := Nat.mod_lt _ (Nat.pow_pos (by decide))
    refine Nat.lt_of_lt_of_le this ?_
    show 2 ^ w ≤ 256 ^ byteCount w
    rw [show (256 : Nat) = 2 ^ 8 from rfl, ← Nat.pow_mul]
    exact Nat.pow_le_pow_right (by decide) (eight_byteCover w)
  rw [Nat.mod_eq_of_lt h2, Nat.mod_eq_of_lt h, Nat.mod_eq_of_lt h]

theorem wireLookup_gen : wireLookup m := by
  intro q xs r hr
  have key := decoder_gen q xs r hr
  have lt := rank_lt_count q xs r hr
  have w : r < 2 ^ codeBits m xs.length q := by
    show r < 2 ^ width (count m xs.length q - 1)
    have h : r ≤ count m xs.length q - 1 := by omega
    exact Nat.lt_of_le_of_lt h (width_gt _)
  rw [get_pack_single (codeBits m xs.length q) r w]
  exact key

/-! ### spaceTest -/

theorem spaceTest_gen : spaceTest m := by
  intro n q pages h hpos hc
  constructor
  · intro hlt
    refine ⟨by omega, ?_⟩
    have h1 : byteCount (pages * codeBits m n q) ≤ pages * byteCount n - h - 1 := by omega
    rw [bc_le] at h1
    exact h1
  · rintro ⟨h1, h2⟩
    have := (bc_le (pages * codeBits m n q) (pages * byteCount n - h - 1)).mpr h2
    omega

/-! ### cofactor -/

theorem sum_zero_map {α : Type} (l : List α) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [List.map_cons, List.sum_cons, ih]

theorem cofactor_gen : cofactor m := by
  intro q a b
  induction a generalizing q with
  | zero =>
    show count m (0 + b) q =
        ((words 0).map (fun pre => ((run m q pre).map (count m b)).getD 0)).sum
    simp [words, run_nil]
  | succ n ih =>
    rw [show ((n + 1) + b : Nat) = (n + b) + 1 from by omega]
    show lowCount m (n + b) q +
        ((m.step q true).map (count m (n + b))).getD 0 = _
    rw [words, List.map_append, List.sum_append, List.map_map, List.map_map]
    simp only [Function.comp_def]
    have hf2 : (words n).map (fun x => ((run m q (false :: x)).map (count m b)).getD 0) =
        (words n).map (fun pre => match m.step q false with
          | none => (0 : Nat)
          | some s => ((run m s pre).map (count m b)).getD 0) := by
      refine congrArg (fun f => (words n).map f) (funext fun pre => ?_)
      rw [run_cons]
      cases h : m.step q false <;> simp [h]
    have ht2 : (words n).map (fun x => ((run m q (true :: x)).map (count m b)).getD 0) =
        (words n).map (fun pre => match m.step q true with
          | none => (0 : Nat)
          | some s => ((run m s pre).map (count m b)).getD 0) := by
      refine congrArg (fun f => (words n).map f) (funext fun pre => ?_)
      rw [run_cons]
      cases h : m.step q true <;> simp [h]
    rw [hf2, ht2, lowCount]
    cases hf : m.step q false with
    | none =>
      rw [Option.map_none, Option.getD_none]
      rw [show (words n).map (fun pre => match (none : Option Nat) with
            | none => (0 : Nat)
            | some s => ((run m s pre).map (count m b)).getD 0) =
            (words n).map (fun _ => 0) from rfl,
        show ((words n).map (fun _ => (0 : Nat))).sum = 0 from sum_zero_map (words n)]
      cases ht : m.step q true with
      | none =>
        rw [Option.map_none, Option.getD_none]
        rw [show (words n).map (fun pre => match (none : Option Nat) with
              | none => (0 : Nat)
              | some s => ((run m s pre).map (count m b)).getD 0) =
              (words n).map (fun _ => 0) from rfl,
          show ((words n).map (fun _ => (0 : Nat))).sum = 0 from sum_zero_map (words n)]
      | some s1 =>
        rw [Option.map_some, Option.getD_some]
        rw [show (words n).map (fun pre => match (some s1 : Option Nat) with
              | none => (0 : Nat)
              | some s => ((run m s pre).map (count m b)).getD 0) =
              (words n).map (fun pre => ((run m s1 pre).map (count m b)).getD 0) from rfl,
          ← ih s1]
    | some s0 =>
      rw [Option.map_some, Option.getD_some]
      rw [show (words n).map (fun pre => match (some s0 : Option Nat) with
            | none => (0 : Nat)
            | some s => ((run m s pre).map (count m b)).getD 0) =
            (words n).map (fun pre => ((run m s0 pre).map (count m b)).getD 0) from rfl,
        ← ih s0]
      cases ht : m.step q true with
      | none =>
        rw [Option.map_none, Option.getD_none]
        rw [show (words n).map (fun pre => match (none : Option Nat) with
              | none => (0 : Nat)
              | some s => ((run m s pre).map (count m b)).getD 0) =
              (words n).map (fun _ => 0) from rfl,
          show ((words n).map (fun _ => (0 : Nat))).sum = 0 from sum_zero_map (words n)]
      | some s1 =>
        rw [Option.map_some, Option.getD_some]
        rw [show (words n).map (fun pre => match (some s1 : Option Nat) with
              | none => (0 : Nat)
              | some s => ((run m s pre).map (count m b)).getD 0) =
              (words n).map (fun pre => ((run m s1 pre).map (count m b)).getD 0) from rfl,
          ← ih s1]

theorem run_some_of_legal : ∀ (xs : List Bool) (q : Nat), legal m q xs →
    ∃ s, run m q xs = some s := by
  intro xs
  induction xs with
  | nil =>
    intro q h
    exact ⟨q, run_nil q⟩
  | cons b bs ih =>
    intro q h
    rw [run_cons]
    cases hstep : m.step q b with
    | none =>
      rw [legal, run_cons, hstep, Option.bind_none] at h
      simp [legal] at h
    | some s =>
      rw [legal, run_cons, hstep, Option.bind_some] at h
      obtain ⟨t, ht⟩ := ih s h
      exact ⟨t, ht⟩
end SE_SK