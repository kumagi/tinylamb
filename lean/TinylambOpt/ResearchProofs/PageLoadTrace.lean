import TinylambOpt.ResearchGoals.PageLoadTrace

namespace TinylambOpt.ResearchProofs.PageLoadTrace
open ResearchGoals.PageLoadTrace
open List

/-! ## Generic helpers -/

private theorem dtrue {p : Prop} [inst : Decidable p] (h : p) : decide p = true := by
  cases inst with
  | isTrue _ => rfl
  | isFalse hn => exact absurd h hn

private theorem dfalse {p : Prop} [inst : Decidable p] (h : ¬p) : decide p = false := by
  cases inst with
  | isTrue hp => exact absurd hp h
  | isFalse _ => rfl

private theorem decide_congr' {p q : Prop} [Decidable p] [Decidable q] (h : p ↔ q) :
    decide p = decide q := by
  rcases Classical.em p with hp | hp
  · exact (dtrue hp).trans (dtrue (h.mp hp)).symm
  · exact (dfalse hp).trans (dfalse (mt h.mpr hp)).symm

private theorem drop_append_le {α : Type} (u v : List α) (n : Nat) (h : n ≤ u.length) :
    drop n (u ++ v) = drop n u ++ v := by
  rw [List.drop_append, Nat.sub_eq_zero_of_le h, List.drop_zero]

private theorem filterMap_singleton {α β : Type} (f : α → Option β) (a : α) :
    [a].filterMap f = (f a).toList := by
  cases h : f a with
  | none => rw [List.filterMap_cons, h]; rfl
  | some b => rw [List.filterMap_cons, h]; rfl

private theorem flatMap_congr' {α β : Type} (f g : α → List β) :
    ∀ (l : List α), (∀ x ∈ l, f x = g x) → l.flatMap f = l.flatMap g := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a l ih =>
      intro h
      rw [List.flatMap_cons, h a List.mem_cons_self,
        ih (fun x hx => h x (List.mem_cons_of_mem a hx)), List.flatMap_cons]

private theorem take_filterMap (α : Type) (l : List α) :
    ∀ w : Nat, l.take w = (List.range w).filterMap (fun i => l[i]?) := by
  intro w
  induction w with
  | zero => rfl
  | succ k ih =>
    rw [List.range_succ, List.filterMap_append, ← ih, filterMap_singleton, List.take_succ]

private theorem filterMap_snd (l : List Nat) (g : Nat → Option UInt8) (h : Nat → Nat) :
    ((l.filterMap fun i => (g i).map fun b => (h i, b))).map Prod.snd = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    cases hgx : g x with
    | none => simp only [List.filterMap_cons, hgx, Option.map_none, ih]
    | some b => simp only [List.filterMap_cons, hgx, Option.map_some, List.map_cons, ih]

private theorem filterMap_fst (l : List Nat) (g : Nat → Option UInt8) (h : Nat → Nat) :
    ((l.filterMap fun i => (g i).map fun b => (h i, b))).map Prod.fst =
      (l.filter fun i => (g i).isSome).map h := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    cases hgx : g x with
    | none =>
      simp only [List.filterMap_cons, List.filter_cons, hgx, Option.map_none, Option.isSome_none,
        Bool.false_eq_true, reduceIte, ih]
    | some b =>
      simp only [List.filterMap_cons, List.filter_cons, hgx, Option.map_some, Option.isSome_some,
        reduceIte, List.map_cons, ih]

/-! ## Range filter decomposition -/

private theorem filter_range_split (P : Nat → Bool) (N k : Nat) (hk : k ≤ N) :
    (range N).filter P =
      (range k).filter P ++ map (fun x => k + x) (filter (fun i => P (k + i)) (range (N - k))) := by
  have hN : N = k + (N - k) := (Nat.add_sub_cancel' hk).symm
  rw [hN, List.range_add, List.filter_append, List.filter_map, Nat.add_sub_cancel_left,
    show (P ∘ fun x => k + x) = (fun i => P (k + i)) from rfl]

private theorem filter_range_false_after (P : Nat → Bool) (k N : Nat) (hk : k ≤ N)
    (h : ∀ b, k ≤ b → b < N → P b = false) :
    (range N).filter P = (range k).filter P := by
  have hN : N = k + (N - k) := (Nat.add_sub_cancel' hk).symm
  rw [hN, List.range_add, List.filter_append, List.filter_map]
  have h2 : (List.range (N - k)).filter (P ∘ fun x => k + x) = [] := by
    rw [List.filter_eq_nil_iff]
    intro i hi hcon
    rw [List.mem_range] at hi
    simp only [Function.comp_apply] at hcon
    rw [h (k + i) (by omega) (by omega)] at hcon
    exact absurd hcon (by decide)
  rw [h2, List.map_nil, List.append_nil]

private theorem filter_range_lt (M N : Nat) :
    (range N).filter (fun l => decide (l < M)) = range (min N M) := by
  have h1 : (range (min N M)).filter (fun l => decide (l < M)) = range (min N M) :=
    (List.filter_eq_self).mpr fun a ha =>
      dtrue (Nat.lt_of_lt_of_le (List.mem_range.mp ha) (Nat.min_le_right _ _))
  have h2 : (range (N - min N M)).filter (fun i => decide (min N M + i < M)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro i hi hcon
    rw [List.mem_range] at hi
    rw [dfalse (by omega : ¬(min N M + i < M))] at hcon
    exact absurd hcon (by decide)
  calc (range N).filter (fun l => decide (l < M))
      = (range (min N M)).filter (fun l => decide (l < M))
        ++ map (fun x => min N M + x)
          (filter (fun i => decide (min N M + i < M)) (range (N - min N M))) :=
          filter_range_split _ _ _ (Nat.min_le_left _ _)
    _ = range (min N M) ++ map (fun x => min N M + x) [] := by rw [h1, h2]
    _ = range (min N M) := by rw [List.map_nil, List.append_nil]

/-! ## Prefix (`<+:`) helpers.  `List.IsPrefix a b` unfolds to `∃ t, a ++ t = b`. -/

private theorem ip_length {needle data : List UInt8} (h : needle <+: data) :
    needle.length ≤ data.length := by
  obtain ⟨t, ht⟩ := h
  rw [← ht, List.length_append]
  exact Nat.le_add_right _ _

private theorem ip_append_right {needle as bs : List UInt8} (h : needle <+: as) :
    needle <+: as ++ bs := by
  obtain ⟨t, ht⟩ := h
  exact ⟨t ++ bs, by rw [← List.append_assoc, ht]⟩

private theorem ip_of_append {needle u v : List UInt8} {n : Nat}
    (h : needle <+: drop n (u ++ v)) (hn : n ≤ u.length)
    (hl : needle.length ≤ (drop n u).length) :
    needle <+: drop n u := by
  obtain ⟨t, ht⟩ := h
  have h2 : drop n (u ++ v) = drop n u ++ v := drop_append_le u v n hn
  rw [h2] at ht
  have h4 : take needle.length (drop n u) = needle := by
    rw [← List.take_append_of_le_length hl, ← ht,
      List.take_append_of_le_length (Nat.le_refl needle.length), List.take_length]
  refine ⟨drop needle.length (drop n u), ?_⟩
  exact (congrArg (fun x => x ++ drop needle.length (drop n u)) h4.symm).trans
    (List.take_append_drop needle.length (drop n u))

/-! ## The window decomposition of `reference` -/

private theorem ref_window_eq (needle : List UInt8) (hm : 0 < needle.length)
    (u v : List UInt8) :
    reference needle (u ++ v)
      = reference needle u
        ++ List.map (fun x => (u.length - min u.length (needle.length - 1)) + x)
          (reference needle (u.drop (u.length - min u.length (needle.length - 1)) ++ v)) := by
  have hKle : u.length - min u.length (needle.length - 1) ≤ u.length := Nat.sub_le _ _
  have hsub : u.length - (u.length - min u.length (needle.length - 1))
      = min u.length (needle.length - 1) := Nat.sub_sub_self (Nat.min_le_left _ _)
  have hwl : (u.drop (u.length - min u.length (needle.length - 1)) ++ v).length + 1
      = (u ++ v).length + 1 - (u.length - min u.length (needle.length - 1)) := by
    rw [List.length_append, List.length_drop, List.length_append]
    omega
  -- no hit starts in `[K, u.length]`
  have hcut : ∀ g, u.length - min u.length (needle.length - 1) ≤ g → g ≤ u.length →
      ¬ (needle <+: u.drop g) := by
    intro g h1 h2 hnp
    have h3 : needle.length ≤ (u.drop g).length := ip_length hnp
    rw [List.length_drop] at h3
    have h4 : u.length - g ≤ u.length - (u.length - min u.length (needle.length - 1)) :=
      Nat.sub_le_sub_left h1 _
    rw [hsub] at h4
    have h5 : min u.length (needle.length - 1) ≤ needle.length - 1 := Nat.min_le_right _ _
    omega
  -- below the cutoff the predicates agree
  have hlow : ∀ g, g < u.length - min u.length (needle.length - 1) →
      ((needle <+: (u ++ v).drop g) ↔ (needle <+: u.drop g)) := by
    intro g hg
    refine ⟨fun hhp => ip_of_append hhp (by omega) ?_, fun hhp => ?_⟩
    · by_cases h : u.length ≤ needle.length - 1
      · have hK0 : u.length - min u.length (needle.length - 1) = 0 := by
          rw [Nat.min_eq_left h, Nat.sub_self]
        rw [hK0] at hg
        exact absurd hg (Nat.not_lt_zero g)
      · have hmin : min u.length (needle.length - 1) = needle.length - 1 :=
          Nat.min_eq_right (Nat.le_of_not_le h)
        rw [hmin] at hg
        rw [List.length_drop]
        omega
    · have h1 : needle <+: u.drop g ++ v := ip_append_right hhp
      rw [← drop_append_le u v g (by omega)] at h1
      exact h1
  have hmain : List.filter (fun g => decide (needle <+: (u ++ v).drop g))
        (List.range ((u ++ v).length + 1))
      = List.filter (fun g => decide (needle <+: u.drop g)) (List.range (u.length + 1))
        ++ List.map (fun x => (u.length - min u.length (needle.length - 1)) + x)
          (List.filter (fun i => decide (needle <+:
              (u.drop (u.length - min u.length (needle.length - 1)) ++ v).drop i))
            (List.range ((u.drop (u.length - min u.length (needle.length - 1)) ++ v).length + 1))) := by
    have hidx : ∀ i : Nat,
        (u.drop (u.length - min u.length (needle.length - 1)) ++ v).drop i
          = (u ++ v).drop ((u.length - min u.length (needle.length - 1)) + i) := by
      intro i
      rw [← drop_append_le u v (u.length - min u.length (needle.length - 1)) hKle,
        List.drop_drop, Nat.add_comm]
    have hKN : u.length - min u.length (needle.length - 1) ≤ (u ++ v).length + 1 :=
      Nat.le_trans (Nat.sub_le _ _) (by rw [List.length_append]; omega)
    rw [filter_range_split (fun g => decide (needle <+: (u ++ v).drop g)) _ _ hKN]
    rw [show List.filter (fun g => decide (needle <+: (u ++ v).drop g))
          (List.range (u.length - min u.length (needle.length - 1)))
        = List.filter (fun g => decide (needle <+: u.drop g)) (List.range (u.length + 1)) from
      ((List.filter_congr (fun g hg => decide_congr' (hlow g (List.mem_range.mp hg)))).trans
        ((filter_range_false_after (fun g => decide (needle <+: u.drop g))
          (u.length - min u.length (needle.length - 1)) (u.length + 1) (by omega)
          (fun b hb1 hb2 => dfalse (hcut b hb1 (by omega)))).symm))]
    rw [show List.filter (fun i => decide (needle <+: (u ++ v).drop
            ((u.length - min u.length (needle.length - 1)) + i)))
          (List.range ((u ++ v).length + 1 - (u.length - min u.length (needle.length - 1))))
        = List.filter (fun i => decide (needle <+:
              (u.drop (u.length - min u.length (needle.length - 1)) ++ v).drop i))
          (List.range ((u.drop (u.length - min u.length (needle.length - 1)) ++ v).length + 1)) from by
      rw [← hwl]
      refine List.filter_congr (fun i _ =>
        (congrArg (fun x => decide (needle <+: x)) (hidx i)).symm)]
  unfold reference
  exact hmain

/-! ## The chunk fold -/

private def step (needle : List UInt8) (s : Scan) (c : List (Nat × UInt8)) : Scan :=
  { pos := s.pos + c.length
    carry := (s.carry ++ c.map Prod.snd).drop ((s.carry ++ c.map Prod.snd).length
      - (needle.length - 1))
    hits := s.hits ++ List.map (s.pos - s.carry.length + ·)
      (List.filter (fun i => s.carry.length < i + needle.length)
        (reference needle (s.carry ++ c.map Prod.snd)))
    reads := s.reads ++ c.map Prod.fst }

private theorem step_eq (needle : List UInt8) (s : Scan) (c : List (Nat × UInt8)) :
    scanChunk s needle c = step needle s c := rfl

private theorem fold_reads (needle : List UInt8) :
    ∀ (cs : List (List (Nat × UInt8))) (s : Scan),
      (cs.foldl (fun a c => step needle a c) s).reads
        = s.reads ++ cs.flatMap (fun c => c.map Prod.fst) := by
  intro cs
  induction cs with
  | nil =>
      intro s
      rw [List.flatMap_nil, List.append_nil]
      rfl
  | cons c cs ih =>
      intro s
      have h1 : (step needle s c).reads = s.reads ++ c.map Prod.fst := rfl
      rw [List.foldl_cons, ih, h1, List.flatMap_cons, List.append_assoc]

private theorem ref_nil (needle : List UInt8) (hm : 0 < needle.length) :
    reference needle [] = [] := by
  unfold reference
  rw [List.filter_eq_nil_iff]
  intro i hi hcon
  have h1 : i < 1 := List.mem_range.mp hi
  have hp := of_decide_eq_true hcon
  have h3 : needle.length ≤ (List.drop i ([] : List UInt8)).length := ip_length hp
  rw [List.length_drop, List.length_nil, Nat.zero_sub] at h3
  omega

private theorem drop_append_left {α : Type} (t s : List α) (n : Nat) :
    (t ++ s).drop (t.length + n) = s.drop n := by
  rw [List.drop_append,
    List.drop_of_length_le (show t.length ≤ t.length + n from Nat.le_add_right _ _),
    Nat.add_sub_cancel_left, List.nil_append]

private theorem carry_step {u v : List UInt8} (m : Nat) (hm : 0 < m) (C : List UInt8)
    (hC : C = u.drop (u.length - min u.length (m - 1))) :
    (C ++ v).drop ((C ++ v).length - (m - 1))
      = (u ++ v).drop ((u ++ v).length - min (m - 1) (u ++ v).length) := by
  by_cases hge : m - 1 ≤ u.length
  · have hmin : min u.length (m - 1) = m - 1 := Nat.min_eq_right hge
    have hcl : C.length = m - 1 := by
      rw [hC, hmin, List.length_drop]
      omega
    obtain ⟨t, ht⟩ : ∃ t, u = t ++ C :=
      ⟨u.take (u.length - (m - 1)), by rw [hC, hmin, List.take_append_drop]⟩
    rw [ht]
    have hmu : min (m - 1) (t ++ C ++ v).length = m - 1 :=
      Nat.min_eq_left (by rw [List.length_append, List.length_append]; omega)
    have hL : (C ++ v).length - (m - 1) = v.length := by
      rw [List.length_append, hcl]
      omega
    have hR : (t ++ C ++ v).length - min (m - 1) (t ++ C ++ v).length
        = t.length + v.length := by
      rw [hmu, List.length_append, List.length_append, hcl]
      omega
    rw [hL, hR, List.append_assoc t C v]
    exact (drop_append_left t (C ++ v) v.length).symm
  · have hK : u.length - min u.length (m - 1) = 0 := by
      have hmin : min u.length (m - 1) = u.length :=
        Nat.min_eq_left (Nat.le_of_not_le (by omega))
      rw [hmin, Nat.sub_self]
    rw [hC, hK, List.drop_zero]
    by_cases hh : (u ++ v).length ≤ m - 1
    · rw [Nat.sub_eq_zero_of_le hh, Nat.min_eq_right hh, Nat.sub_self, List.drop_zero]
    · rw [Nat.min_eq_left (by omega : m - 1 ≤ (u ++ v).length)]

private theorem fold_pos_carry_hits (needle : List UInt8) (hm : 0 < needle.length) :
    ∀ (cs : List (List (Nat × UInt8))) (s : Scan) (Ds : List UInt8),
      s.pos = Ds.length →
      s.carry = Ds.drop (Ds.length - min (needle.length - 1) Ds.length) →
      s.hits = reference needle Ds →
      (cs.foldl (fun a c => step needle a c) s).pos
        = (Ds ++ cs.flatMap (fun c => c.map Prod.snd)).length ∧
      (cs.foldl (fun a c => step needle a c) s).carry
        = (Ds ++ cs.flatMap (fun c => c.map Prod.snd)).drop
            ((Ds ++ cs.flatMap (fun c => c.map Prod.snd)).length
              - min (needle.length - 1) (Ds ++ cs.flatMap (fun c => c.map Prod.snd)).length) ∧
      (cs.foldl (fun a c => step needle a c) s).hits
        = reference needle (Ds ++ cs.flatMap (fun c => c.map Prod.snd)) := by
  intro cs
  induction cs with
  | nil =>
      intro s Ds hp hc hh
      rw [show List.foldl (fun a c => step needle a c) s [] = s from rfl,
        List.flatMap_nil, List.append_nil]
      exact ⟨hp, hc, hh⟩
  | cons c cs ih =>
      intro s Ds hp hc hh
      have hKc : Ds.length - min Ds.length (needle.length - 1)
          = Ds.length - min (needle.length - 1) Ds.length := by rw [Nat.min_comm]
      have hcarry : s.carry = Ds.drop (Ds.length - min Ds.length (needle.length - 1)) := by
        rw [hKc]; exact hc
      have hclen : s.carry.length = min (needle.length - 1) Ds.length := by
        rw [hc, List.length_drop]; omega
      have hoff : s.pos - s.carry.length
          = Ds.length - min (needle.length - 1) Ds.length := by
        rw [hp, hclen]
      have hfilt : List.filter (fun i => s.carry.length < i + needle.length)
          (reference needle (s.carry ++ c.map Prod.snd))
          = reference needle (s.carry ++ c.map Prod.snd) := by
        rw [List.filter_eq_self]
        intro i hi
        rw [hclen]
        exact dtrue (by omega)
      have hhits : reference needle (Ds ++ c.map Prod.snd)
          = s.hits ++ List.map (s.pos - s.carry.length + ·)
            (List.filter (fun i => s.carry.length < i + needle.length)
              (reference needle (s.carry ++ c.map Prod.snd))) := by
        rw [hh]
        have H := ref_window_eq needle hm Ds (c.map Prod.snd)
        rw [← hcarry] at H
        rw [hKc] at H
        rw [hfilt, hoff, ← H]
      have hstep_pos : (step needle s c).pos = (Ds ++ c.map Prod.snd).length := by
        rw [show (step needle s c).pos = s.pos + c.length from rfl, hp,
          List.length_append, List.length_map]
      have hstep_carry : (step needle s c).carry
          = (Ds ++ c.map Prod.snd).drop ((Ds ++ c.map Prod.snd).length
              - min (needle.length - 1) (Ds ++ c.map Prod.snd).length) := by
        rw [show (step needle s c).carry
            = (s.carry ++ c.map Prod.snd).drop ((s.carry ++ c.map Prod.snd).length
              - (needle.length - 1)) from rfl]
        exact carry_step (needle.length) hm s.carry hcarry
      have hstep_hits : (step needle s c).hits
          = reference needle (Ds ++ c.map Prod.snd) := by
        rw [show (step needle s c).hits
            = s.hits ++ List.map (s.pos - s.carry.length + ·)
              (List.filter (fun i => s.carry.length < i + needle.length)
                (reference needle (s.carry ++ c.map Prod.snd))) from rfl]
        exact hhits.symm
      rw [List.foldl_cons, List.flatMap_cons, ← List.append_assoc]
      exact ih (step needle s c) (Ds ++ c.map Prod.snd) hstep_pos hstep_carry hstep_hits

/-! ## Page loads -/

private theorem load_snd (p : Page) (off w : Nat) :
    (load p off w).map Prod.snd = (p.bytes.drop off).take w := by
  unfold load
  rw [show (fun lane => (p.bytes[off + lane]?).map fun b => (p.base + off + lane, b))
      = (fun lane => ((p.bytes.drop off)[lane]?).map fun b => (p.base + off + lane, b)) from
    funext fun lane => congrArg (fun o : Option UInt8 => o.map (fun b => (p.base + off + lane, b)))
      (List.getElem?_drop (xs := p.bytes) (i := off) (j := lane)).symm]
  rw [filterMap_snd, ← take_filterMap]

private theorem drop_range' (n : Nat) :
    ∀ i : Nat, (List.range n).drop i = (List.range (n - i)).map (fun x => i + x) := by
  induction n with
  | zero =>
      intro i
      rw [Nat.zero_sub, List.range_zero, List.map_nil, List.drop_nil]
  | succ n ih =>
      intro i
      cases i with
      | zero =>
          rw [Nat.sub_zero, List.drop_zero,
            show (fun x => 0 + x) = (fun x => x) from funext fun x => Nat.zero_add x]
          exact (List.map_id _).symm
      | succ i =>
          rw [show List.range (n + 1) = List.range n ++ [n] from by rw [List.range_succ],
            List.drop_append, ih (i + 1)]
          have hz : n + 1 - (i + 1) = n - i := Nat.succ_sub_succ_eq_sub n i
          by_cases h : i + 1 ≤ n
          · have h1 : n - i = n - (i + 1) + 1 := by omega
            rw [hz, h1, List.range_succ, List.map_append, List.map_singleton,
              List.length_range, Nat.sub_eq_zero_of_le (by omega : i + 1 ≤ n), List.drop_zero,
              Nat.add_sub_cancel' (by omega : i + 1 ≤ n)]
          · have h2 : n - i = 0 := by omega
            have h3 : n - (i + 1) = 0 := by omega
            have hlen : ([n] : List Nat).length = 1 := rfl
            rw [hz, h2, h3, List.length_range, List.range_zero, List.map_nil,
              List.drop_of_length_le (by rw [hlen]; omega : [n].length ≤ i + 1 - n),
              List.append_nil]

private theorem take_range_chunk (n off w : Nat) :
    ((List.range n).drop off).take w
      = (List.range (min w (n - off))).map (fun x => off + x) := by
  rw [drop_range', ← List.map_take, List.take_range]

private theorem load_fst (p : Page) (off w : Nat) :
    (load p off w).map Prod.fst =
      (List.range (min w (p.bytes.length - off))).map (fun l => p.base + off + l) := by
  unfold load
  rw [show (fun lane => (p.bytes[off + lane]?).map fun b => (p.base + off + lane, b))
      = (fun lane => ((p.bytes.drop off)[lane]?).map fun b => (p.base + off + lane, b)) from
    funext fun lane => congrArg (fun o : Option UInt8 => o.map (fun b => (p.base + off + lane, b)))
      (List.getElem?_drop (xs := p.bytes) (i := off) (j := lane)).symm]
  rw [filterMap_fst]
  have hsel : ∀ lane : Nat,
      ((p.bytes.drop off)[lane]? : Option UInt8).isSome = decide (lane < p.bytes.length - off) := by
    intro lane
    cases h : (p.bytes.drop off)[lane]? with
    | some b =>
      have hlen : lane < (p.bytes.drop off).length := by
        rcases Nat.lt_or_ge lane (p.bytes.drop off).length with hlt | hge
        · exact hlt
        · rw [List.getElem?_eq_none_iff.mpr (by omega : (p.bytes.drop off).length ≤ lane)] at h
          exact absurd h (by simp)
      rw [List.length_drop] at hlen
      rw [dtrue hlen, Option.isSome_some]
    | none =>
      have hlen := List.getElem?_eq_none_iff.mp h
      rw [List.length_drop] at hlen
      rw [dfalse (by omega : ¬(lane < p.bytes.length - off)), Option.isSome_none]
  rw [List.filter_congr (fun lane _ => hsel lane)]
  rw [filter_range_lt]

private theorem chunk_takes {α : Type} (w : Nat) (hw : 0 < w) :
    ∀ (n : Nat) (bs : List α), bs.length = n →
      (List.range ((n + w - 1) / w)).flatMap (fun i => (bs.drop (i * w)).take w) = bs := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      cases n with
      | zero =>
          intro bs hb
          rw [show (0 + w - 1) / w = 0 from Nat.div_eq_of_lt (by omega),
            List.range_zero, List.flatMap_nil]
          cases bs with
          | nil => rfl
          | cons b bs' => exact absurd hb (by simp)
      | succ n =>
          intro bs hb
          by_cases hle : n + 1 ≤ w
          · have hnc : (n + 1 + w - 1) / w = 1 := by
              have h1 : n + 1 + w - 1 = n + w := by omega
              rw [h1, Nat.add_comm, Nat.add_div hw, Nat.div_self hw,
                Nat.div_eq_of_lt (by omega), Nat.mod_self, Nat.zero_add]
              have hmod := Nat.mod_lt n hw
              rw [if_neg (by omega : ¬(w ≤ n % w))]
            rw [hnc, List.range_one, List.flatMap_cons, List.flatMap_nil, List.append_nil,
              Nat.zero_mul, List.drop_zero, List.take_of_length_le (by rw [hb]; omega)]
          · have hge : w ≤ n + 1 := by omega
            have hlen : (bs.drop w).length = n + 1 - w := by rw [List.length_drop, hb]
            have hsub : (bs.drop w).length + w - 1 = n := by rw [hlen]; omega
            have hnc : (n + 1 + w - 1) / w = 1 + ((bs.drop w).length + w - 1) / w := by
              rw [hsub]
              have h2 : n + 1 + w - 1 = n + w := by omega
              have hmod := Nat.mod_lt n hw
              rw [h2, Nat.add_comm, Nat.add_div hw, Nat.div_self hw, Nat.mod_self,
                Nat.zero_add, if_neg (by omega : ¬(w ≤ n % w))]
              omega
            have hih : (List.range (((bs.drop w).length + w - 1) / w)).flatMap
                (fun i => ((bs.drop w).drop (i * w)).take w) = bs.drop w :=
              ih ((bs.drop w).length) (by rw [hlen]; omega) (bs.drop w) rfl
            rw [hnc, List.range_add, List.flatMap_append,
              List.flatMap_map (fun x => 1 + x) (fun x => (bs.drop (x * w)).take w)
                (List.range (((bs.drop w).length + w - 1) / w))]
            rw [flatMap_congr'
                  (fun x => (bs.drop ((1 + x) * w)).take w)
                  (fun a => ((bs.drop w).drop (a * w)).take w) (List.range _)
                  (fun a _ => by
                    show (bs.drop ((1 + a) * w)).take w = ((bs.drop w).drop (a * w)).take w
                    rw [List.drop_drop, Nat.add_mul, Nat.one_mul])]
            rw [hih, List.range_one, List.flatMap_cons, List.flatMap_nil, List.append_nil]
            rw [show (bs.drop (0 * w)).take w = bs.take w from by
              rw [Nat.zero_mul, List.drop_zero]]
            exact List.take_append_drop w bs

private theorem chunk_bytes (p : Page) (w : Nat) (hw : 0 < w) :
    (List.range ((p.bytes.length + w - 1) / w)).flatMap
        (fun i => (load p (i * w) w).map Prod.snd) = p.bytes := by
  refine (flatMap_congr' _ _ _ (fun i _ => load_snd p (i * w) w)).trans
    (chunk_takes w hw p.bytes.length p.bytes rfl)

private theorem flatMap_flatMap' (f : α → List β) (g : β → List γ) :
    ∀ (l : List α), (l.flatMap f).flatMap g = l.flatMap (fun a => (f a).flatMap g) := by
  intro l
  induction l with
  | nil => rfl
  | cons a l ih =>
      rw [List.flatMap_cons, List.flatMap_append, ih, List.flatMap_cons]

private theorem chunk_addrs (p : Page) (w : Nat) (hw : 0 < w) :
    (List.range ((p.bytes.length + w - 1) / w)).flatMap
        (fun i => (load p (i * w) w).map Prod.fst)
      = (List.range p.bytes.length).map (fun x => p.base + x) := by
  have h1 : ∀ i : Nat,
      (List.range (min w (p.bytes.length - i * w))).map (fun l => p.base + i * w + l)
        = List.map (fun x => p.base + x)
          (((List.range p.bytes.length).drop (i * w)).take w) := by
    intro i
    rw [take_range_chunk, List.map_map]
    refine congrArg (fun f => List.map f (List.range (min w (p.bytes.length - i * w)))) ?_
    refine funext fun l => ?_
    exact Nat.add_assoc _ _ _
  have h2 : ∀ i : Nat, (load p (i * w) w).map Prod.fst
      = List.map (fun x => p.base + x) (((List.range p.bytes.length).drop (i * w)).take w) := by
    intro i
    rw [load_fst, h1]
  refine (flatMap_congr' _ _ _ (fun i _ => h2 i)).trans ?_
  rw [← List.map_flatMap]
  refine congrArg (List.map (fun x => p.base + x)) ?_
  exact chunk_takes w hw p.bytes.length (List.range p.bytes.length) List.length_range

private theorem foldl_map (β α : Type) (f : β → α → β) (g : Nat → α) :
    ∀ (l : List Nat) (s : β), l.foldl (fun a i => f a (g i)) s = (l.map g).foldl f s := by
  intro l
  induction l with
  | nil => intro s; rfl
  | cons a l ih =>
      intro s
      rw [List.foldl_cons, List.map_cons, List.foldl_cons]
      exact ih _

private theorem foldl_load (needle : List UInt8) (w : Nat) :
    ∀ (ps : List Page) (s : Scan),
      List.foldl (fun a p => List.foldl (fun c i => step needle c (load p (i * w) w)) a
          (List.range ((p.bytes.length + w - 1) / w))) s ps
        = List.foldl (fun c ch => step needle c ch) s
            (ps.flatMap fun p =>
              (List.range ((p.bytes.length + w - 1) / w)).map (fun i => load p (i * w) w)) := by
  intro ps
  induction ps with
  | nil => intro s; rfl
  | cons p ps ih =>
      intro s
      rw [List.foldl_cons, List.flatMap_cons, List.foldl_append, ih, foldl_map]

theorem sound : Goal := by
  intro pages needle width hw
  show (scan pages needle width).hits = reference needle (pages.flatMap Page.bytes) ∧
    (scan pages needle width).reads =
      (if needle.isEmpty then []
        else pages.flatMap fun p => (List.range p.bytes.length).map (fun x => p.base + x))
  cases needle with
  | nil =>
      have hscan : scan pages [] width
          = { hits := List.range ((pages.map fun p => p.bytes.length).sum + 1) } := rfl
      have hlen : (pages.flatMap Page.bytes).length
          = (pages.map (fun p => p.bytes.length)).sum := List.length_flatMap
      have href : reference ([] : List UInt8) (pages.flatMap Page.bytes)
          = List.range ((pages.flatMap Page.bytes).length + 1) := by
        unfold reference
        refine (List.filter_eq_self).mpr fun a ha =>
          dtrue ⟨List.drop a (pages.flatMap Page.bytes), List.nil_append _⟩
      constructor
      · rw [hscan, ← hlen, href]
      · rfl
  | cons a as =>
      have hm : 0 < ((a :: as : List UInt8)).length := Nat.succ_pos _
      have hnif : ¬((a :: as : List UInt8).isEmpty = true) := by simp
      have hscan : scan pages (a :: as) width
          = List.foldl (fun s c => step (a :: as) s c) {}
              (pages.flatMap fun p =>
                (List.range ((p.bytes.length + width - 1) / width)).map
                  (fun i => load p (i * width) width)) := by
        unfold scan
        rw [if_neg hnif]
        refine ((congrArg (fun F => List.foldl F {} pages)
          (funext fun s => funext fun p =>
            congrArg (fun f => List.foldl f s
              (List.range ((p.bytes.length + width - 1) / width)))
              (funext fun c => funext fun i => step_eq (a :: as) c _)))).trans
          (foldl_load (a :: as) width _ _)
      obtain ⟨_, _, hhitsF⟩ :=
        fold_pos_carry_hits (a :: as) hm
          ((pages.flatMap fun p =>
              (List.range ((p.bytes.length + width - 1) / width)).map
                (fun i => load p (i * width) width))) {} []
            rfl (by simp) (ref_nil _ hm).symm
      have hD : List.flatMap (fun c => c.map Prod.snd)
          (pages.flatMap fun p =>
            (List.range ((p.bytes.length + width - 1) / width)).map
              (fun i => load p (i * width) width))
          = pages.flatMap Page.bytes := by
        rw [flatMap_flatMap']
        refine flatMap_congr' _ _ _ (fun p _ => ?_)
        refine ((List.flatMap_map (fun i => load p (i * width) width)
            (fun c => c.map Prod.snd)
            (List.range ((p.bytes.length + width - 1) / width)))).trans ?_
        exact chunk_bytes p width hw
      have hR : List.flatMap (fun c => c.map Prod.fst)
          (pages.flatMap fun p =>
            (List.range ((p.bytes.length + width - 1) / width)).map
              (fun i => load p (i * width) width))
          = pages.flatMap fun p => (List.range p.bytes.length).map (fun x => p.base + x) := by
        rw [flatMap_flatMap']
        refine flatMap_congr' _ _ _ (fun p _ => ?_)
        refine ((List.flatMap_map (fun i => load p (i * width) width)
            (fun c => c.map Prod.fst)
            (List.range ((p.bytes.length + width - 1) / width)))).trans ?_
        exact chunk_addrs p width hw
      constructor
      · rw [hscan, hhitsF, List.nil_append, hD]
      · rw [hscan, fold_reads, hR, List.nil_append]
        rw [if_neg hnif]

end TinylambOpt.ResearchProofs.PageLoadTrace
