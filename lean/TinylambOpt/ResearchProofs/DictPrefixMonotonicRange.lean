import TinylambOpt.ResearchGoals.DictPrefixMonotonicRange

/-!
# `LIKE 'prefix%'` on a strictly-sorted dictionary is a binary-search range

`prefixRange dict pref` computes `(cLo, cHi)` where `cLo = bsLower dict pref` and
`cHi = bsLower dict (nextPrefix pref)` (or `dict.length` when `nextPrefix pref = none`,
i.e. `pref` is all `0xFF` bytes so no carry bound exists).  We prove `Goal`:

* `lexLt` is a strict total order on byte strings (trichotomy, asymmetry,
  transitivity) — proved from scratch by structural induction;
* prefix membership is an interval cut: `startsWith pref w = true` iff
  `lexLt w pref = false ∧ lexLt w (nextPrefix pref) = true` (for defined
  `nextPrefix`), resp. just the first conjunct for all-`0xFF` prefixes;
* `bsLower` (fuelled binary search) locates the first index whose element is
  `¬ lexLt · key`, via interval induction on the fuel.
-/

namespace TinylambOpt.ResearchProofs.DictPrefixMonotonicRange

open TinylambOpt.ResearchGoals.DictPrefixMonotonicRange

private theorem bconf {p : Bool} (h1 : p = true) (h2 : p = false) {C : Prop} : C := by
  cases p
  · exact absurd h1 (by simp)
  · exact absurd h2 (by simp)

private theorem and_eq_true {p q : Bool} (h : (p && q) = true) : p = true ∧ q = true := by
  cases p <;> cases q <;> simp_all

/-! ### `UInt8` order facts, reduced to `Nat` -/

private theorem u8_beq_iff (a b : UInt8) : ((a == b) = true) ↔ a = b := by
  rw [show (a == b) = decide (a = b) from rfl]
  exact ⟨of_decide_eq_true, decide_eq_true⟩

private theorem u8_beq_self (a : UInt8) : ((a == a) = true) :=
  (u8_beq_iff a a).mpr rfl

private theorem u8_beq_ne {a b : UInt8} (h : ¬ a = b) : ((a == b) = false) := by
  match hab : a == b with
  | true => exact absurd ((u8_beq_iff a b).mp hab) h
  | false => exact rfl

private theorem u8_lt_irrefl (a : UInt8) : ¬ a < a := by
  rw [UInt8.lt_iff_toNat_lt]; omega

private theorem u8_lt_asymm {a b : UInt8} (h : a < b) : ¬ b < a := by
  rw [UInt8.lt_iff_toNat_lt] at h ⊢; omega

private theorem u8_lt_trans {a b c : UInt8} (h1 : a < b) (h2 : b < c) : a < c := by
  rw [UInt8.lt_iff_toNat_lt] at h1 h2 ⊢; omega

private theorem u8_trich (a b : UInt8) : a < b ∨ a = b ∨ b < a := by
  rcases Nat.lt_trichotomy a.toNat b.toNat with h | h | h
  · exact Or.inl (UInt8.lt_iff_toNat_lt.mpr h)
  · exact Or.inr (Or.inl (UInt8.toNat_inj.mp h))
  · exact Or.inr (Or.inr (UInt8.lt_iff_toNat_lt.mpr h))

private theorem u8_lt_succ {b : UInt8} (hb : b.toNat ≠ 255) : b < b + 1 := by
  have hb' : b.toNat < 256 := UInt8.toNat_lt_size b
  have hadd : (b + 1).toNat = (b.toNat + 1) % 256 := by
    rw [UInt8.toNat_add, show (1 : UInt8).toNat = 1 from rfl]
  rw [UInt8.lt_iff_toNat_lt, hadd]
  omega

private theorem u8_not_lt_succ {a b : UInt8} (hab : a < b) : ¬ b < a + 1 := by
  have ha' : a.toNat < 256 := UInt8.toNat_lt_size a
  have hb' : b.toNat < 256 := UInt8.toNat_lt_size b
  have hab' : a.toNat < b.toNat := UInt8.lt_iff_toNat_lt.mp hab
  have hadd : (a + 1).toNat = (a.toNat + 1) % 256 := by
    rw [UInt8.toNat_add, show (1 : UInt8).toNat = 1 from rfl]
  rw [UInt8.lt_iff_toNat_lt, hadd]
  omega

private theorem u8_255_max (a : UInt8) : ¬ (255 : UInt8) < a := by
  have ha : a.toNat < 256 := UInt8.toNat_lt_size a
  have h255 : (255 : UInt8).toNat = 255 := rfl
  rw [UInt8.lt_iff_toNat_lt, h255]
  omega

private theorem u8_eq_255_of_not_lt (a : UInt8) (h : ¬ a < 255) : a = 255 := by
  have ha : a.toNat < 256 := UInt8.toNat_lt_size a
  have h255 : (255 : UInt8).toNat = 255 := rfl
  rw [UInt8.lt_iff_toNat_lt] at h
  rw [← UInt8.toNat_inj, h255]
  rw [h255] at h
  omega

private theorem u8_lt_succ' {c : UInt8} (hc : ((c == 255) = false)) : c < c + 1 :=
  u8_lt_succ (fun heq => bconf ((u8_beq_iff c 255).mpr (UInt8.toNat_inj.mp heq)) hc)

/-! ### `getElem?` helper reductions (definitional) -/

private theorem get?_nil {α : Type} (i : Nat) : ([] : List α)[i]? = none := rfl

private theorem get?_cons_zero {α : Type} (a : α) (as : List α) : (a :: as)[0]? = some a := rfl

private theorem get?_cons_succ {α : Type} (a : α) (as : List α) (i : Nat) :
    (a :: as)[i+1]? = as[i]? := rfl

private theorem lt_length_of_get?_some {α : Type} {as : List α} {i : Nat} {a : α}
    (h : as[i]? = some a) : i < as.length := by
  induction as generalizing i with
  | nil => rw [get?_nil] at h; exact absurd h (by simp)
  | cons b bs ih =>
      cases i with
      | zero => exact Nat.zero_lt_succ _
      | succ k =>
          rw [get?_cons_succ] at h
          have hik := ih h
          simp only [List.length_cons]
          omega

private theorem get?_some_of_lt_length {α : Type} (as : List α) :
    ∀ {i : Nat}, i < as.length → ∃ a, as[i]? = some a := by
  induction as with
  | nil => intro i h; exact absurd h (by simp)
  | cons b bs ih =>
      intro i h
      cases i with
      | zero => exact ⟨b, get?_cons_zero b bs⟩
      | succ k =>
          have hk : k < bs.length := by
            simp only [List.length_cons] at h; omega
          obtain ⟨a, ha⟩ := ih hk
          exact ⟨a, by rw [get?_cons_succ, ha]⟩

private theorem mem_of_get?_some {α : Type} {as : List α} {i : Nat} {a : α}
    (h : as[i]? = some a) : a ∈ as := by
  induction as generalizing i with
  | nil => rw [get?_nil] at h; exact absurd h (by simp)
  | cons b bs ih =>
      cases i with
      | zero =>
          rw [get?_cons_zero] at h
          cases h
          exact List.mem_cons_self
      | succ k =>
          rw [get?_cons_succ] at h
          exact List.mem_cons_of_mem b (ih h)

/-! ### `lexLt` is a strict total order -/

private theorem lexLt_nil_right (ws : List UInt8) : lexLt ws [] = false := by
  cases ws <;> rfl

private theorem lexLt_nil_left (a : UInt8) (as : List UInt8) : lexLt [] (a :: as) = true := rfl

private theorem lexLt_cons (a b : UInt8) (as bs : List UInt8) :
    lexLt (a :: as) (b :: bs) =
      if a < b then true else if (a == b) = true then lexLt as bs else false := rfl

private theorem lexLt_cons_lt (a b : UInt8) (as bs : List UInt8) (h : a < b) :
    lexLt (a :: as) (b :: bs) = true := by
  rw [lexLt_cons, if_pos h]

private theorem lexLt_cons_eq (a : UInt8) {as bs : List UInt8} (h : lexLt as bs = true) :
    lexLt (a :: as) (a :: bs) = true := by
  rw [lexLt_cons, if_neg (u8_lt_irrefl a), if_pos (u8_beq_self a)]
  exact h

private theorem lexLt_trichotomy :
    ∀ as bs : List UInt8, lexLt as bs = true ∨ as = bs ∨ lexLt bs as = true := by
  intro as
  induction as with
  | nil =>
      intro bs
      cases bs with
      | nil => exact Or.inr (Or.inl rfl)
      | cons b bs => exact Or.inl (lexLt_nil_left b bs)
  | cons a as ih =>
      intro bs
      cases bs with
      | nil => exact Or.inr (Or.inr (lexLt_nil_left a as))
      | cons b bs =>
          rcases u8_trich a b with hlt | hab | hgt
          · exact Or.inl (lexLt_cons_lt a b as bs hlt)
          · subst hab
            rcases ih bs with h | h | h
            · exact Or.inl (lexLt_cons_eq a h)
            · exact Or.inr (Or.inl (congrArg (fun t => a :: t) h))
            · exact Or.inr (Or.inr (lexLt_cons_eq a h))
          · exact Or.inr (Or.inr (lexLt_cons_lt b a bs as hgt))

private theorem lexLt_asymm :
    ∀ {as bs : List UInt8}, lexLt as bs = true → lexLt bs as = false := by
  intro as
  induction as with
  | nil =>
      intro bs h
      cases bs with
      | nil => rw [lexLt_nil_right] at h; exact absurd h (by simp)
      | cons b bs => exact lexLt_nil_right (b :: bs)
  | cons a as ih =>
      intro bs h
      cases bs with
      | nil =>
          rw [lexLt_nil_right] at h
          exact absurd h (by simp)
      | cons b bs =>
          rw [lexLt_cons] at h ⊢
          by_cases hlt : a < b
          · rw [if_neg (u8_lt_asymm hlt)]
            have hne : ¬ ((b == a) = true) :=
              fun heq => absurd hlt (by rw [(u8_beq_iff b a).mp heq]; exact u8_lt_irrefl a)
            rw [if_neg hne]
          · rw [if_neg hlt] at h
            by_cases hab : (a == b) = true
            · rw [if_pos hab] at h
              have heq : a = b := (u8_beq_iff a b).mp hab
              subst heq
              rw [if_neg (u8_lt_irrefl a), if_pos (u8_beq_self a)]
              exact ih h
            · rw [if_neg hab] at h
              exact absurd h (by simp)

private theorem lexLt_trans :
    ∀ {as bs cs : List UInt8}, lexLt as bs = true → lexLt bs cs = true → lexLt as cs = true := by
  intro as
  induction as with
  | nil =>
      intro bs cs h1 h2
      cases bs with
      | nil => rw [lexLt_nil_right] at h1; exact absurd h1 (by simp)
      | cons b bs =>
          cases cs with
          | nil => rw [lexLt_nil_right] at h2; exact absurd h2 (by simp)
          | cons c cs => exact lexLt_nil_left c cs
  | cons a as ih =>
      intro bs cs h1 h2
      cases bs with
      | nil => rw [lexLt_nil_right] at h1; exact absurd h1 (by simp)
      | cons b bs' =>
          cases cs with
          | nil => rw [lexLt_nil_right] at h2; exact absurd h2 (by simp)
          | cons c cs' =>
              rw [lexLt_cons] at h1 h2 ⊢
              rcases u8_trich a b with hlt | hab | hgt
              · -- a < b: goal `if a < c then true else ... = true`; split on a vs c
                rcases u8_trich a c with hlt2 | hac | hgt2
                · exact if_pos hlt2
                · -- a = c: `lexLt (b::bs') (a::cs') = true` is impossible since a < b
                  subst hac
                  rw [if_neg (u8_lt_irrefl a), if_pos (u8_beq_self a)]
                  rw [if_neg (u8_lt_asymm hlt)] at h2
                  have hne : ¬ ((b == a) = true) :=
                    fun heq => absurd hlt (by rw [(u8_beq_iff b a).mp heq]; exact u8_lt_irrefl a)
                  rw [if_neg hne] at h2
                  exact absurd h2 (by simp)
                · -- c < a, with a < b: impossible for `lexLt (b::bs') (c::cs') = true`
                  have hbgt : ¬ (b < c) := u8_lt_asymm (u8_lt_trans hgt2 hlt)
                  rw [if_neg hbgt] at h2
                  have hcne : ¬ ((b == c) = true) :=
                    fun heq => absurd (u8_lt_trans hgt2 hlt)
                      (by rw [← (u8_beq_iff b c).mp heq]; exact u8_lt_irrefl b)
                  rw [if_neg hcne] at h2
                  exact absurd h2 (by simp)
              · -- a = b
                have hnb : ¬ (a < b) := by rw [← hab]; exact u8_lt_irrefl a
                rw [if_neg hnb] at h1
                by_cases hab1 : (a == b) = true
                · rw [if_pos hab1] at h1
                  have heq : a = b := (u8_beq_iff a b).mp hab1
                  subst heq
                  by_cases hc : a < c
                  · rw [if_pos hc]
                  · rw [if_neg hc]
                    by_cases hc2 : (a == c) = true
                    · rw [if_pos hc2]
                      rw [if_neg hc, if_pos hc2] at h2
                      exact ih h1 h2
                    · rw [if_neg hc, if_neg hc2] at h2
                      exact absurd h2 (by simp)
                · rw [if_neg hab1] at h1
                  exact absurd h1 (by simp)
              · -- b < a
                rw [if_neg (u8_lt_asymm hgt)] at h1
                by_cases hab1 : (a == b) = true
                · rw [if_pos hab1] at h1
                  have heq : a = b := (u8_beq_iff a b).mp hab1
                  exact absurd (heq ▸ hgt) (u8_lt_irrefl b)
                · rw [if_neg hab1] at h1
                  exact absurd h1 (by simp)

private theorem lexLt_append_suffix (as bs : List UInt8) : lexLt (as ++ bs) as = false := by
  induction as generalizing bs with
  | nil => exact lexLt_nil_right bs
  | cons a as ih =>
      rw [List.cons_append, lexLt_cons, if_neg (u8_lt_irrefl a), if_pos (u8_beq_self a)]
      exact ih bs

/-! ### sorted dictionary: `lexLt` of earlier vs. later elements -/

private theorem lexLt_get?_lt {dict : List (List UInt8)} (sort : StrictlySorted dict) :
    ∀ {i j : Nat} {u v : List UInt8},
      dict[i]? = some u → dict[j]? = some v → i < j → lexLt u v = true := by
  induction dict with
  | nil =>
      intro i j u v h _ _
      rw [get?_nil] at h
      exact absurd h (by simp)
  | cons a t ih =>
      intro i j u v hi hj hij
      obtain ⟨hhead, htail⟩ := List.pairwise_cons.mp sort
      cases i with
      | zero =>
          rw [get?_cons_zero] at hi
          cases hi
          cases j with
          | zero => exact absurd hij (by omega)
          | succ k =>
              rw [get?_cons_succ] at hj
              exact hhead v (mem_of_get?_some hj)
      | succ m =>
          cases j with
          | zero => exact absurd hij (by omega)
          | succ n =>
              rw [get?_cons_succ] at hi hj
              exact ih htail hi hj (by omega)

/-! ### `startsWith` lemmas -/

private theorem startsWith_nil (s : List UInt8) : startsWith [] s = true := by
  unfold startsWith
  simp

private theorem startsWith_cons (a b : UInt8) (p s : List UInt8) :
    startsWith (a :: p) (b :: s) = (a == b && startsWith p s) := by
  unfold startsWith
  by_cases hab : a = b
  · subst hab
    simp [List.length_cons, List.take_cons, Nat.succ_le_succ_iff, List.beq_cons_cons]
  · have hb1 : ((b == a) = false) := u8_beq_ne (fun h => hab (h.symm))
    have hb2 : ((a == b) = false) := u8_beq_ne hab
    simp [hab, hb1, hb2, List.length_cons, List.take_cons]

private theorem startsWith_iff_exists (p w : List UInt8) :
    startsWith p w = true ↔ ∃ t, w = p ++ t := by
  constructor
  · intro h
    induction p generalizing w with
    | nil => exact ⟨w, by simp⟩
    | cons a p ih =>
        cases w with
        | nil =>
            have h2 : startsWith (a :: p) ([] : List UInt8) = false := by
              unfold startsWith
              simp
            rw [h2] at h
            exact absurd h (by simp)
        | cons b ws =>
            rw [startsWith_cons] at h
            have hboth := and_eq_true h
            have ha : a = b := (u8_beq_iff a b).mp hboth.1
            subst ha
            obtain ⟨t, ht⟩ := ih ws hboth.2
            exact ⟨t, by rw [List.cons_append, ht]⟩
  · intro ⟨t, ht⟩
    subst ht
    induction p generalizing t with
    | nil => exact startsWith_nil t
    | cons a p ih =>
        rw [List.cons_append, startsWith_cons, u8_beq_self, Bool.true_and]
        exact ih t

private theorem startsWith_self (p : List UInt8) : startsWith p p = true :=
  (startsWith_iff_exists p p).mpr ⟨[], by rw [List.append_nil]⟩

/-! ### `revNext` / `nextPrefix` shape lemmas -/

private theorem revNext_cons_255 (xs : List UInt8) : revNext (255 :: xs) = revNext xs := rfl

private theorem revNext_cons_ne {c : UInt8} {xs : List UInt8} (hc : ((c == 255) = false)) :
    revNext (c :: xs) = some ((c + 1) :: xs) := by
  show (if (c == 255) = true then revNext xs else some ((c + 1) :: xs)) = _
  rw [if_neg (by simpa using hc)]

private theorem revNext_some_shape :
    ∀ {xs y : List UInt8}, revNext xs = some y →
      ∃ a c rest, xs = a ++ (c :: rest) ∧ y = (c + 1) :: rest ∧ (∀ z ∈ a, z = 255) ∧
        ((c == 255) = false) := by
  intro xs
  induction xs with
  | nil => intro y h; exact absurd h (by simp [revNext])
  | cons b bs ih =>
      intro y h
      match hb : b == 255 with
      | true =>
          have hbs : revNext (b :: bs) = revNext bs := by
            show (if (b == 255) = true then revNext bs else some ((b + 1) :: bs)) = _
            rw [if_pos hb]
          have hbs2 : revNext bs = some y := by rw [← hbs]; exact h
          obtain ⟨a, c, rest, h1, h2, h3, h4⟩ := ih hbs2
          refine ⟨b :: a, c, rest, ?_, h2, ?_, h4⟩
          · rw [List.cons_append, ← h1]
          · intro z hz
            rcases List.mem_cons.mp hz with rfl | hz'
            · exact (u8_beq_iff z 255).mp hb
            · exact h3 z hz'
      | false =>
          have hbs : revNext (b :: bs) = some ((b + 1) :: bs) := by
            show (if (b == 255) = true then revNext bs else some ((b + 1) :: bs)) = _
            rw [if_neg (by simpa using hb)]
          have he : some ((b + 1) :: bs) = some y := by rw [← hbs]; exact h
          obtain rfl : y = (b + 1) :: bs := (Option.some.inj he).symm
          refine ⟨[], b, bs, by simp, rfl, fun z hz => absurd hz (by simp), ?_⟩
          exact hb

private theorem revNext_eq_none_of_all255 :
    ∀ xs : List UInt8, (∀ z ∈ xs, z = 255) → revNext xs = none := by
  intro xs
  induction xs with
  | nil => intro _; rfl
  | cons b bs ih =>
      intro h
      have hb : b = 255 := h b List.mem_cons_self
      subst hb
      rw [revNext_cons_255]
      exact ih (fun z hz => h z (List.mem_cons_of_mem 255 hz))

private theorem all255_of_revNext_eq_none :
    ∀ xs : List UInt8, revNext xs = none → ∀ z ∈ xs, z = 255 := by
  intro xs
  induction xs with
  | nil => intro _ z hz; exact absurd hz (by simp)
  | cons b bs ih =>
      intro h z hz
      rcases List.mem_cons.mp hz with rfl | hz'
      · match hb : z == 255 with
        | true => exact (u8_beq_iff z 255).mp hb
        | false =>
            rw [revNext_cons_ne hb] at h
            exact absurd h (by simp)
      · match hb : b == 255 with
        | true =>
            have hb' : b = 255 := (u8_beq_iff b 255).mp hb
            subst hb'
            rw [revNext_cons_255] at h
            exact ih h z hz'
        | false =>
            rw [revNext_cons_ne hb] at h
            exact absurd h (by simp)

private theorem nextPrefix_nil : nextPrefix ([] : List UInt8) = none := rfl

private theorem nextPrefix_snoc_ne255 {p : List UInt8} {c : UInt8}
    (hc : ((c == 255) = false)) : nextPrefix (p ++ [c]) = some (p ++ [c + 1]) := by
  have hrev : (p ++ [c]).reverse = c :: p.reverse := by
    rw [List.reverse_append]
    rfl
  unfold nextPrefix
  rw [hrev, revNext_cons_ne hc]
  have hmap : Option.map List.reverse (some ((c + 1) :: p.reverse)) =
      some (List.reverse ((c + 1) :: p.reverse)) := rfl
  rw [hmap, List.reverse_cons, List.reverse_reverse]

private theorem nextPrefix_snoc_255 {p : List UInt8} :
    nextPrefix (p ++ [255]) = nextPrefix p := by
  have hrev : (p ++ [255]).reverse = 255 :: p.reverse := by
    rw [List.reverse_append]
    rfl
  unfold nextPrefix
  rw [hrev, revNext_cons_255]

private theorem nextPrefix_some_shape :
    ∀ {p hi : List UInt8}, nextPrefix p = some hi →
      ∃ q c zs, p = q ++ [c] ++ zs ∧ hi = q ++ [c + 1] ∧ (∀ z ∈ zs, z = 255) ∧
        ((c == 255) = false) := by
  intro p hi hnp
  have hmap : (revNext p.reverse).map List.reverse = some hi := hnp
  match hr : revNext p.reverse with
  | none => rw [hr] at hmap; exact absurd hmap (by simp)
  | some r =>
      rw [hr] at hmap
      have hmap' : some (List.reverse r) = some hi := hmap
      obtain rfl : hi = List.reverse r := Option.some.inj hmap'.symm
      obtain ⟨a, c, rest, h1, h2, h3, h4⟩ := revNext_some_shape hr
      refine ⟨rest.reverse, c, a.reverse, ?_, ?_, ?_, h4⟩
      · rw [← List.reverse_reverse p, h1, List.reverse_append, List.reverse_cons]
      · rw [h2, List.reverse_cons]
      · intro z hz
        exact h3 z (List.mem_reverse.mp hz)

private theorem nextPrefix_eq_none_of_all255 :
    ∀ p : List UInt8, (∀ z ∈ p, z = 255) → nextPrefix p = none := by
  intro p hp
  unfold nextPrefix
  rw [revNext_eq_none_of_all255 p.reverse (fun z hz => hp z (List.mem_reverse.mp hz))]
  rfl

private theorem all255_of_nextPrefix_eq_none :
    ∀ p : List UInt8, nextPrefix p = none → ∀ z ∈ p, z = 255 := by
  intro p hnp z hz
  have hnone : revNext p.reverse = none := by
    unfold nextPrefix at hnp
    match hr : revNext p.reverse with
    | none => rfl
    | some r =>
        rw [hr] at hnp
        exact absurd hnp (by simp)
  exact all255_of_revNext_eq_none p.reverse hnone z (List.mem_reverse.mpr hz)

/-! ### the interval characterization -/

private theorem prefix_between_lt (q : List UInt8) : ∀ (c : UInt8) (zs t : List UInt8),
    ((c == 255) = false) → lexLt (q ++ [c] ++ zs ++ t) (q ++ [c + 1]) = true := by
  induction q with
  | nil =>
      intro c zs t hc
      exact lexLt_cons_lt c (c + 1) (zs ++ t) [] (u8_lt_succ' hc)
  | cons x xs ih =>
      intro c zs t hc
      have e1 : (x :: xs) ++ [c] ++ zs ++ t = x :: (xs ++ [c] ++ zs ++ t) := rfl
      have e2 : (x :: xs) ++ [c + 1] = x :: (xs ++ [c + 1]) := rfl
      rw [e1, e2, lexLt_cons, if_neg (u8_lt_irrefl x), if_pos (u8_beq_self x)]
      exact ih c zs t hc

private theorem lexLt_nextPrefix {p hi : List UInt8} (hnp : nextPrefix p = some hi) :
    lexLt p hi = true := by
  obtain ⟨q, c, zs, hpq, hhi, hzs, hc⟩ := nextPrefix_some_shape hnp
  rw [hpq, hhi]
  have h := prefix_between_lt q c zs [] hc
  rw [List.append_nil] at h
  exact h

private theorem startsWith_lt_nextPrefix {p hi w : List UInt8} (hnp : nextPrefix p = some hi)
    (h : startsWith p w = true) : lexLt w hi = true := by
  obtain ⟨t, ht⟩ := (startsWith_iff_exists p w).mp h
  obtain ⟨q, c, zs, hpq, hhi, hzs, hc⟩ := nextPrefix_some_shape hnp
  rw [hpq] at ht
  rw [ht, hhi]
  exact prefix_between_lt q c zs t hc

private theorem all255_ge_startsWith : ∀ (p w : List UInt8), (∀ z ∈ p, z = 255) →
    lexLt w p = false → startsWith p w = true := by
  intro p
  induction p with
  | nil => intro w _ _; exact startsWith_nil _
  | cons d ds ih =>
      intro w h hw
      have hd : d = 255 := h d List.mem_cons_self
      subst hd
      cases w with
      | nil =>
          rw [lexLt_nil_left] at hw
          exact absurd hw (by simp)
      | cons b ws =>
          rw [lexLt_cons] at hw
          by_cases hb : b < 255
          · rw [if_pos hb] at hw
            exact absurd hw (by simp)
          · have hb' : b = 255 := u8_eq_255_of_not_lt b hb
            subst hb'
            rw [if_neg (u8_255_max 255), if_pos (u8_beq_self 255)] at hw
            rw [startsWith_cons, u8_beq_self, Bool.true_and]
            exact ih ws (fun z hz => h z (List.mem_cons_of_mem 255 hz)) hw

/-- Core induction for the interval characterization: a word sandwiched
    between `q ++ [c] ++ zs` (all-`255` tail `zs`, `c ≠ 255`) and the carry
    bound `q ++ [c + 1]` must start with `q ++ [c] ++ zs`. -/
private theorem between_aux' : ∀ (q : List UInt8),
    ∀ (w : List UInt8) (c : UInt8) (zs : List UInt8),
      (∀ z ∈ zs, z = 255) → ((c == 255) = false) → lexLt w (q ++ [c] ++ zs) = false →
        lexLt w (q ++ [c + 1]) = true → startsWith (q ++ [c] ++ zs) w = true := by
  intro q
  induction q with
  | nil =>
      intro w c zs hzs hc h1 h2
      have e1 : ([] : List UInt8) ++ [c] ++ zs = c :: zs := rfl
      have e2 : ([] : List UInt8) ++ [c + 1] = (c + 1) :: [] := rfl
      rw [e1] at h1 ⊢
      rw [e2] at h2
      cases w with
      | nil =>
          rw [lexLt_nil_left] at h1
          exact absurd h1 (by simp)
      | cons b ws =>
          rcases u8_trich c b with hlt | hab | hgtb
          · -- c < b: then `¬ (b < c + 1)`, and `b ≠ c+1` or `lexLt ws []`
            rw [lexLt_cons] at h2
            rw [if_neg (u8_not_lt_succ hlt)] at h2
            by_cases hb : (b == c + 1) = true
            · rw [if_pos hb] at h2
              rw [lexLt_nil_right] at h2
              exact absurd h2 (by simp)
            · rw [if_neg hb] at h2
              exact absurd h2 (by simp)
          · -- c == b: reduces to the all-255 tail claim
            subst hab
            rw [lexLt_cons] at h1
            rw [if_neg (u8_lt_irrefl c), if_pos (u8_beq_self c)] at h1
            rw [startsWith_cons, u8_beq_self c, Bool.true_and]
            exact all255_ge_startsWith zs ws hzs h1
          · -- b < c: `lexLt (b :: ws) (c :: zs)` would be true, contradicting h1
            rw [lexLt_cons] at h1
            rw [if_pos hgtb] at h1
            exact absurd h1 (by simp)
  | cons x qs ih =>
      intro w c zs hzs hc h1 h2
      have e1 : (x :: qs) ++ [c] ++ zs = x :: (qs ++ [c] ++ zs) := rfl
      have e2 : (x :: qs) ++ [c + 1] = x :: (qs ++ [c + 1]) := rfl
      rw [e1] at h1 ⊢
      rw [e2] at h2
      cases w with
      | nil =>
          rw [lexLt_nil_left] at h1
          exact absurd h1 (by simp)
      | cons b ws =>
          rcases u8_trich x b with hlt | hab | hgtb
          · -- x < b: h2 needs `b < x` or `b == x`, both impossible
            rw [lexLt_cons] at h2
            rw [if_neg (u8_lt_asymm hlt)] at h2
            by_cases hb : (b == x) = true
            · rw [if_pos hb] at h2
              exact absurd hlt (by rw [(u8_beq_iff b x).mp hb]; exact u8_lt_irrefl x)
            · rw [if_neg hb] at h2
              exact absurd h2 (by simp)
          · -- x == b: peel and recurse
            subst hab
            rw [lexLt_cons] at h1 h2
            rw [if_neg (u8_lt_irrefl x), if_pos (u8_beq_self x)] at h1 h2
            have h := ih ws c zs hzs hc h1 h2
            rw [startsWith_cons, u8_beq_self x, Bool.true_and]
            exact h
          · -- b < x: `lexLt (b :: ws) (x :: ...)` would be true, contradicting h1
            rw [lexLt_cons] at h1
            rw [if_pos hgtb] at h1
            exact absurd h1 (by simp)

private theorem startsWith_of_between {p hi w : List UInt8} (hnp : nextPrefix p = some hi)
    (hgt : lexLt w hi = true) (hge : lexLt w p = false) : startsWith p w = true := by
  obtain ⟨q, c, zs, hpq, hhi, hzs, hc⟩ := nextPrefix_some_shape hnp
  rw [hhi] at hgt
  rw [hpq] at hge ⊢
  exact between_aux' q w c zs hzs hc hge hgt

private theorem startsWith_iff_between {p hi w : List UInt8} (hnp : nextPrefix p = some hi) :
    startsWith p w = true ↔ (lexLt w p = false ∧ lexLt w hi = true) := by
  constructor
  · intro h
    exact ⟨by
      obtain ⟨t, ht⟩ := (startsWith_iff_exists p w).mp h
      rw [ht]
      exact lexLt_append_suffix p t,
      startsWith_lt_nextPrefix hnp h⟩
  · intro ⟨hge, hgt⟩
    exact startsWith_of_between hnp hgt hge

/-! ### binary search: `PAt` and fuelled interval induction -/

private def PAt (dict : List (List UInt8)) (key : List UInt8) (i : Nat) : Bool :=
  match dict[i]? with
  | some w => lexLt w key
  | none => false

private theorem PAt_eq_of_get? {dict : List (List UInt8)} {key : List UInt8} {i : Nat}
    {w : List UInt8} (h : dict[i]? = some w) : PAt dict key i = lexLt w key := by
  simp only [PAt, h]

private theorem PAt_mono {dict : List (List UInt8)} (sort : StrictlySorted dict)
    (key : List UInt8) :
    ∀ {i j : Nat}, i ≤ j → PAt dict key j = true → PAt dict key i = true := by
  intro i j hij hj
  match hjd : dict[j]? with
  | none =>
      rw [PAt, hjd] at hj
      exact absurd hj (by simp)
  | some v =>
      have hjl : j < dict.length := lt_length_of_get?_some hjd
      have hvk : lexLt v key = true := by
        rw [PAt, hjd] at hj
        exact hj
      match hid : dict[i]? with
      | none =>
          obtain ⟨u, hu⟩ := get?_some_of_lt_length dict (by omega : i < dict.length)
          rw [hid] at hu
          exact absurd hu (by simp)
      | some u =>
          rcases Nat.eq_or_lt_of_le hij with rfl | hlt
          · have huveq : u = v := Option.some.inj (hid ▸ hjd)
            rw [PAt, hid, huveq]
            exact hvk
          · rw [PAt, hid]
            exact lexLt_trans (lexLt_get?_lt sort hid hjd hlt) hvk

private theorem bsLower_go_spec {dict : List (List UInt8)} (sort : StrictlySorted dict)
    (key : List UInt8) :
    ∀ (fuel l r : Nat), l ≤ r → r ≤ dict.length → r - l + 1 ≤ fuel →
      (∀ i, i < l → PAt dict key i = true) →
      (∀ i, r ≤ i → i < dict.length → PAt dict key i = false) →
      (∀ i, i < bsLower.go dict key l r fuel → PAt dict key i = true) ∧
      (∀ i, bsLower.go dict key l r fuel ≤ i → i < dict.length → PAt dict key i = false) ∧
      bsLower.go dict key l r fuel ≤ dict.length := by
  intro fuel
  induction fuel with
  | zero =>
      intro l r hlr hr hf h1 h2
      have hgo : bsLower.go dict key l r 0 = l := rfl
      have heq : l = r := by omega
      subst heq
      rw [hgo]
      exact ⟨h1, h2, hr⟩
  | succ fuel ih =>
      intro l r hlr hr hf h1 h2
      by_cases hle : r ≤ l
      · have hgo : bsLower.go dict key l r (fuel + 1) = l := by
          simp only [bsLower.go, if_pos hle]
        rw [hgo]
        exact ⟨h1, fun i hil hil2 => h2 i (by omega) hil2, by omega⟩
      · have hlt : l < r := by omega
        have hm1 : l ≤ (l + r) / 2 := by omega
        have hm2 : (l + r) / 2 < r := by omega
        have hmid_lt : (l + r) / 2 < dict.length := by omega
        match hw : dict[(l + r) / 2]? with
        | none =>
            obtain ⟨w, hw'⟩ := get?_some_of_lt_length dict hmid_lt
            rw [hw'] at hw
            exact absurd hw (by simp)
        | some w =>
            have hPmid : PAt dict key ((l + r) / 2) = lexLt w key := PAt_eq_of_get? hw
            by_cases hx : lexLt w key = true
            · have hgo : bsLower.go dict key l r (fuel + 1) =
                  bsLower.go dict key ((l + r) / 2 + 1) r fuel := by
                simp only [bsLower.go, hw, if_neg hle, if_pos hx]
              rw [hgo]
              obtain ⟨c1, c2, c3⟩ := ih ((l + r) / 2 + 1) r (by omega) hr (by omega)
                (fun i hi => PAt_mono sort key (by omega : i ≤ (l + r) / 2)
                  (by rw [hPmid]; exact hx))
                (fun i hi hil => h2 i (by omega) hil)
              exact ⟨c1, c2, c3⟩
            · have hgo : bsLower.go dict key l r (fuel + 1) =
                  bsLower.go dict key l ((l + r) / 2) fuel := by
                simp only [bsLower.go, hw, if_neg hle, if_neg hx]
              rw [hgo]
              obtain ⟨c1, c2, c3⟩ := ih l ((l + r) / 2) (by omega) (by omega) (by omega)
                h1
                (fun i hi hil =>
                  match hiP : PAt dict key i with
                  | true =>
                      absurd
                        (show lexLt w key = true from
                          by rw [← hPmid];
                             exact PAt_mono sort key hi hiP) hx
                  | false => rfl)
              exact ⟨c1, c2, c3⟩

private theorem bsLower_spec {dict : List (List UInt8)} (sort : StrictlySorted dict)
    (key : List UInt8) :
    (∀ i, i < bsLower dict key → PAt dict key i = true) ∧
    (∀ i, bsLower dict key ≤ i → i < dict.length → PAt dict key i = false) ∧
    bsLower dict key ≤ dict.length := by
  have hunfold : bsLower dict key = bsLower.go dict key 0 dict.length (dict.length + 1) := by
    unfold bsLower
    rfl
  rw [hunfold]
  refine bsLower_go_spec sort key (dict.length + 1) 0 dict.length (by omega) (by omega)
    (by omega) (fun i hi => absurd hi (by omega)) (fun i hi hil => absurd hil (by omega))

/-! ### assembling `Goal` -/

theorem sound : Goal := by
  intro dict pref sort
  obtain ⟨bLo1, bLo2, bLo3⟩ := bsLower_spec sort pref
  show bsLower dict pref ≤ (match nextPrefix pref with
        | none => dict.length
        | some hi => bsLower dict hi) ∧
      (match nextPrefix pref with
        | none => dict.length
        | some hi => bsLower dict hi) ≤ dict.length ∧
      ∀ i w, dict[i]? = some w →
        startsWith pref w =
          decide (bsLower dict pref ≤ i ∧
            i < match nextPrefix pref with
                | none => dict.length
                | some hi => bsLower dict hi)
  match hnp : nextPrefix pref with
  | none =>
      simp only
      have hall : ∀ z ∈ pref, z = 255 := all255_of_nextPrefix_eq_none pref hnp
      refine ⟨bLo3, Nat.le_refl _, ?_⟩
      intro i w hw
      have hiff : (startsWith pref w = true) ↔ (bsLower dict pref ≤ i ∧ i < dict.length) := by
        constructor
        · intro hsw
          refine ⟨?_, lt_length_of_get?_some hw⟩
          by_cases hble : bsLower dict pref ≤ i
          · exact hble
          · have hlt' := bLo1 i (by omega)
            rw [PAt_eq_of_get? hw] at hlt'
            obtain ⟨t, ht⟩ := (startsWith_iff_exists pref w).mp hsw
            rw [ht, lexLt_append_suffix pref t] at hlt'
            exact absurd hlt' (by simp)
        · intro ⟨hle, _⟩
          match h : lexLt w pref with
          | false => exact all255_ge_startsWith pref w hall h
          | true =>
              exfalso
              have h2 := bLo2 i hle (lt_length_of_get?_some hw)
              rw [PAt_eq_of_get? hw] at h2
              exact bconf h h2
      match hb : startsWith pref w with
      | true => exact (decide_eq_true (hiff.1 hb)).symm
      | false => exact (decide_eq_false (fun hp => bconf (hiff.2 hp) hb)).symm
  | some hi =>
      simp only
      obtain ⟨bHi1, bHi2, bHi3⟩ := bsLower_spec sort hi
      refine ⟨?_, bHi3, ?_⟩
      · by_cases hble : bsLower dict pref ≤ bsLower dict hi
        · exact hble
        · have hP := bLo1 (bsLower dict hi) (by omega)
          obtain ⟨w, hw⟩ :=
            get?_some_of_lt_length dict (by omega : bsLower dict hi < dict.length)
          have hwp : lexLt w pref = true := by
            rw [← PAt_eq_of_get? hw]
            exact hP
          have hwh : lexLt w hi = false := by
            have h2 := bHi2 (bsLower dict hi) (Nat.le_refl _) (lt_length_of_get?_some hw)
            rw [PAt_eq_of_get? hw] at h2
            exact h2
          exact bconf (lexLt_trans hwp (lexLt_nextPrefix hnp)) hwh
      · intro i w hw
        have hiff : (startsWith pref w = true) ↔
            (bsLower dict pref ≤ i ∧ i < bsLower dict hi) := by
          rw [startsWith_iff_between hnp]
          constructor
          · intro ⟨hge, hgt⟩
            refine ⟨?_, ?_⟩
            · by_cases hble : bsLower dict pref ≤ i
              · exact hble
              · have h2 := bLo1 i (by omega)
                rw [PAt_eq_of_get? hw] at h2
                exact bconf h2 hge
            · by_cases hbit : bsLower dict hi ≤ i
              · exfalso
                have h2 := bHi2 i hbit (lt_length_of_get?_some hw)
                rw [PAt_eq_of_get? hw] at h2
                exact bconf hgt h2
              · omega
          · intro ⟨hle, hlt⟩
            refine ⟨?_, ?_⟩
            · have h2 := bLo2 i hle (lt_length_of_get?_some hw)
              rw [PAt_eq_of_get? hw] at h2
              exact h2
            · have h2 := bHi1 i hlt
              rw [PAt_eq_of_get? hw] at h2
              exact h2
        match hb : startsWith pref w with
        | true => exact (decide_eq_true (hiff.1 hb)).symm
        | false => exact (decide_eq_false (fun hp => bconf (hiff.2 hp) hb)).symm

-- C++ references to this model or its proof.
-- table/pax_chunk/pax_chunk.cpp
