import TinylambOpt.ResearchGoals.Compression.TemporalText

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Compression.TemporalText

open TinylambOpt.ResearchGoals.Compression.TemporalText
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Compression.Text (Words raw)

/-! Soundness and counterexamples for the twelve TemporalText goals.

The parser `digits layout` is injective on the fixed 22-byte spelling and the
decimal value is a bijection between its fourteen digits and `[0, 10^14)`.
Everything else follows by fold algebra, mapM bookkeeping and bit-splice
arithmetic.  The last goal is false: the UTC suffix is literal and an
arbitrary three-byte replacement is not the original spelling. -/

/-! ## helpers: the ASCII parser -/

private theorem digits_spec (fmt : List (Option Nat)) (s : Bytes) (ds : List Nat)
    (h : digits fmt s = some ds) :
    s.length = fmt.length ∧ ds.length = (fmt.filter (fun x => x.isNone)).length ∧
      ∀ d ∈ ds, d < 10 := by
  induction fmt generalizing s ds with
  | nil =>
      cases s with
      | nil =>
          simp [digits] at h
          subst ds
          simp
      | cons b bs => simp [digits] at h
  | cons f fmt ih =>
      cases s with
      | nil => simp [digits] at h
      | cons b bs =>
          cases f with
          | none =>
              simp [digits] at h
              rcases h with ⟨hcond, ds', hd, rfl⟩
              have ih' := ih bs ds' hd
              simp only [List.length_cons, List.filter_cons, Option.isNone_none, ite_true,
                List.length_cons]
              constructor
              · omega
              constructor
              · omega
              · intro d hdmem
                simp only [List.mem_cons] at hdmem
                rcases hdmem with rfl | hdmem
                · omega
                · exact ih'.2.2 d hdmem
          | some t =>
              simp [digits] at h
              rcases h with ⟨ht, hd⟩
              have ih' := ih bs ds hd
              simpa [Option.isNone_some] using ih'

private theorem digits_inj (fmt : List (Option Nat)) :
    ∀ s₁ s₂ ds, digits fmt s₁ = some ds → digits fmt s₂ = some ds → s₁ = s₂ := by
  induction fmt with
  | nil =>
      intro s₁ s₂ ds h₁ h₂
      cases s₁ <;> cases s₂ <;> simp [digits] at h₁ h₂ ⊢
  | cons f fmt ih =>
      intro s₁ s₂ ds h₁ h₂
      cases s₁ with
      | nil => simp [digits] at h₁
      | cons b₁ bs₁ =>
          cases s₂ with
          | nil => simp [digits] at h₂
          | cons b₂ bs₂ =>
              cases f with
              | none =>
                  simp [digits] at h₁ h₂
                  rcases h₁ with ⟨hb₁, ds₁, hd₁, heq₁⟩
                  rcases h₂ with ⟨hb₂, ds₂, hd₂, heq₂⟩
                  have hhead : b₁ - 48 = b₂ - 48 := by
                    have := congrArg List.head? (heq₁.trans heq₂.symm)
                    simpa using this
                  have htail : ds₁ = ds₂ := by
                    have := congrArg List.tail? (heq₁.trans heq₂.symm)
                    simpa using this
                  have hb : b₁ = b₂ := by omega
                  rw [hb, ih bs₁ bs₂ ds₁ hd₁ (by simpa [htail] using hd₂)]
              | some t =>
                  simp [digits] at h₁ h₂
                  rcases h₁ with ⟨hb₁, hd₁⟩
                  rcases h₂ with ⟨hb₂, hd₂⟩
                  rw [← hb₁, ← hb₂]
                  rw [ih bs₁ bs₂ ds hd₁ hd₂]

private theorem digits_paint (fmt : List (Option Nat)) (ds : List Nat)
    (hlen : ds.length = (fmt.filter (fun x => x.isNone)).length)
    (hlt : ∀ d ∈ ds, d < 10) : digits fmt (paint fmt ds) = some ds := by
  induction fmt generalizing ds with
  | nil =>
      have hdnil : ds = [] := List.eq_nil_of_length_eq_zero (by simpa using hlen)
      subst ds
      rfl
  | cons f fmt ih =>
      cases f with
      | none =>
          cases ds with
          | nil => simp at hlen
          | cons d ds' =>
              have ih' : digits fmt (paint fmt ds') = some ds' := by
                apply ih ds'
                · simpa using hlen
                · intro x hx; exact hlt x (by simp [hx])
              simp [digits, paint, ih']
              have hdlt := hlt d (by simp)
              omega
      | some t =>
          simp [digits, paint]
          apply ih ds
          · simpa using hlen
          · exact hlt

private theorem digits_spell (v : Nat) : digits layout (spell v) =
    some ((List.range 14).reverse.map (fun i => v / 10 ^ i % 10)) := by
  unfold spell
  apply digits_paint
  · simp [layout]
  · intro d hdm
    rcases List.mem_map.mp hdm with ⟨i, hi, rfl⟩
    exact Nat.mod_lt _ (by decide : 0 < 10)

/-! ## helpers: decimal value -/

private theorem valueAux_eq (ds : List Nat) (acc : Nat) :
    ds.foldl (fun n d => 10 * n + d) acc = acc * 10 ^ ds.length + value ds := by
  induction ds generalizing acc with
  | nil =>
      change acc = acc * 1 + 0
      omega
  | cons d ds ih =>
      rw [List.foldl_cons]
      unfold value
      rw [List.foldl_cons]
      change ds.foldl (fun n x => 10 * n + x) (10 * acc + d) =
        acc * 10 ^ (ds.length + 1) + ds.foldl (fun n x => 10 * n + x) (10 * 0 + d)
      simp only [Nat.mul_zero, Nat.zero_add]
      rw [ih (10 * acc + d), ih d]
      rw [Nat.pow_succ']
      simp only [Nat.add_mul, Nat.mul_assoc]
      ac_rfl

private theorem value_cons (d : Nat) (ds : List Nat) :
    value (d :: ds) = d * 10 ^ ds.length + value ds := by
  change ds.foldl (fun n d => 10 * n + d) (10 * 0 + d) = d * 10 ^ ds.length + value ds
  simp only [Nat.mul_zero, Nat.zero_add]
  rw [valueAux_eq]

private theorem value_lt_pow (ds : List Nat) (hlt : ∀ d ∈ ds, d < 10) :
    value ds < 10 ^ ds.length := by
  induction ds with
  | nil => simp [value]
  | cons d ds ih =>
      have hd : d < 10 := hlt d (by simp)
      have hi := ih (fun x hx => hlt x (by simp [hx]))
      rw [value_cons]
      calc
        d * 10 ^ ds.length + value ds < d * 10 ^ ds.length + 10 ^ ds.length :=
          Nat.add_lt_add_left hi _
        _ = (d + 1) * 10 ^ ds.length := by rw [Nat.succ_mul]
        _ ≤ 10 * 10 ^ ds.length := Nat.mul_le_mul_right _ (Nat.succ_le_of_lt hd)
        _ = 10 ^ (ds.length + 1) := by rw [Nat.pow_succ']

private def lval : List Nat → Nat
  | [] => 0
  | d :: ds => d + 10 * lval ds

private theorem lval_append_singleton (l : List Nat) (x : Nat) :
    lval (l ++ [x]) = lval l + 10 ^ l.length * x := by
  induction l with
  | nil => simp [lval]
  | cons a as ih =>
      simp only [List.cons_append, lval, List.length_cons, ih]
      rw [Nat.mul_add, Nat.pow_succ']
      ac_rfl

private theorem value_rev (ds : List Nat) : value ds = lval ds.reverse := by
  induction ds with
  | nil => rfl
  | cons d ds ih =>
      rw [value_cons, ih]
      rw [List.reverse_cons]
      rw [lval_append_singleton]
      simp only [List.length_reverse]
      ac_rfl

private theorem lval_digits_lt (v n : Nat) :
    lval ((List.range n).map (fun i => v / 10 ^ i % 10)) = v % 10 ^ n := by
  induction n with
  | zero => simp [lval, Nat.mod_one]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.map_singleton, lval_append_singleton, ih]
      simp only [List.length_map, List.length_range]
      rw [Nat.pow_succ']
      let p := 10 ^ n
      let d := v / p % 10
      let q := v / p / 10
      have hp : 0 < p := Nat.pow_pos (by decide : 0 < 10)
      have hr : v % p < p := Nat.mod_lt _ hp
      have hdlt : d < 10 := Nat.mod_lt _ (by decide : 0 < 10)
      have hlt : v % p + p * d < 10 * p := by
        have hdle : d ≤ 9 := by omega
        have hpdle : p * d ≤ p * 9 := Nat.mul_le_mul_left p hdle
        calc
          v % p + p * d < p + p * 9 := Nat.add_lt_add_of_lt_of_le hr hpdle
          _ = 10 * p := by
            rw [Nat.mul_comm p 9, Nat.add_comm, ← Nat.succ_mul]
      have hv1 : v = v % p + p * (v / p) := (Nat.mod_add_div v p).symm
      have hv2 : v / p = d + 10 * q := by
        dsimp [d, q]
        exact (Nat.mod_add_div (v / p) 10).symm
      have hrewrite : v = (v % p + p * d) + (10 * p) * q := by
        calc
          v = v % p + p * (v / p) := hv1
          _ = v % p + p * (d + 10 * q) := by rw [hv2]
          _ = (v % p + p * d) + (10 * p) * q := by
            rw [Nat.mul_add]
            ac_rfl
      calc
        v % p + p * d = (v % p + p * d) % (10 * p) := (Nat.mod_eq_of_lt hlt).symm
        _ = ((v % p + p * d) + (10 * p) * q) % (10 * p) := by
          rw [Nat.add_mul_mod_self_left]
        _ = v % (10 * p) := by rw [← hrewrite]

private theorem value_decDigits (v : Nat) (hv : v < 10 ^ 14) :
    value ((List.range 14).reverse.map (fun i => v / 10 ^ i % 10)) = v := by
  rw [value_rev]
  have hrev : ((List.range 14).reverse.map (fun i => v / 10 ^ i % 10)).reverse =
      (List.range 14).map (fun i => v / 10 ^ i % 10) := by
    rw [List.map_reverse, List.reverse_reverse]
  rw [hrev, lval_digits_lt]
  exact Nat.mod_eq_of_lt hv

private theorem lval_digit (l : List Nat) (hlt : ∀ d ∈ l, d < 10) :
    ∀ i, i < l.length → lval l / 10 ^ i % 10 = l[i]?.getD 0 := by
  induction l with
  | nil => simp
  | cons d rest ih =>
      intro i hi
      cases i with
      | zero =>
          simp only [lval, List.getElem?_cons_zero, Option.getD_some, Nat.pow_zero, Nat.div_one]
          rw [Nat.add_mul_mod_self_left]
          exact Nat.mod_eq_of_lt (hlt d (by simp))
      | succ i =>
          have hi' : i < rest.length := by simpa using hi
          have hd : d < 10 := hlt d (by simp)
          have hdiv10 : (d + 10 * lval rest) / 10 = lval rest := by
            rw [Nat.mul_comm 10 (lval rest)]
            rw [Nat.add_mul_div_right _ _ (by decide : 0 < 10)]
            rw [Nat.div_eq_of_lt hd]
            omega
          rw [Nat.pow_succ']
          rw [← Nat.div_div_eq_div_mul]
          change ((d + 10 * lval rest) / 10) / 10 ^ i % 10 = (d :: rest)[i + 1]?.getD 0
          rw [hdiv10]
          simpa [List.getElem?_cons_succ] using
            ih (fun x hx => hlt x (by simp [hx])) i hi'

private theorem map_range_getD (l : List Nat) :
    (List.range l.length).map (fun i => l[i]?.getD 0) = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.length_cons, List.range_succ_eq_map, List.map_cons, List.map_map]
      simp only [List.getElem?_cons_zero, Option.getD_some, List.cons.injEq]
      constructor
      · trivial
      · have hmap :
            List.map ((fun i => (a :: as)[i]?.getD 0) ∘ Nat.succ) (List.range as.length) =
              List.map (fun i => as[i]?.getD 0) (List.range as.length) := by
            apply List.map_congr_left
            intro i hi
            simp only [Function.comp_apply, List.getElem?_cons_succ]
        rw [hmap, ih]

private theorem value_digits (ds : List Nat) (hlt : ∀ d ∈ ds, d < 10) :
    ((List.range ds.length).reverse.map (fun i => value ds / 10 ^ i % 10)) = ds := by
  let l := ds.reverse
  have hl : ∀ d ∈ l, d < 10 := by
    intro d hd
    exact hlt d (by simpa [l] using hd)
  calc
    (List.range ds.length).reverse.map (fun i => value ds / 10 ^ i % 10)
        = (List.range l.length).reverse.map (fun i => lval l / 10 ^ i % 10) := by
            simp only [l, List.length_reverse]
            congr 2
            funext i
            rw [value_rev]
    _ = (List.range l.length).reverse.map (fun i => l[i]?.getD 0) := by
            apply List.map_congr_left
            intro i hi
            have hiR : i ∈ List.range l.length := by simpa using (List.mem_reverse.mp hi)
            exact lval_digit l hl i (List.mem_range.mp hiR)
    _ = ((List.range l.length).map (fun i => l[i]?.getD 0)).reverse := by
            rw [List.map_reverse]
    _ = l.reverse := by rw [map_range_getD]
    _ = ds := by simp [l]

/-! ## LexicalInverse -/

theorem sound_LexicalInverse : LexicalInverse.Goal := by
  intro w v hcode
  unfold code at hcode
  cases hdig : digits layout w with
  | none => simp [hdig] at hcode
  | some ds =>
      simp [hdig, Option.some.injEq] at hcode
      subst v
      have hspec := digits_spec layout w ds hdig
      have hlen : ds.length = (layout.filter (fun x => x.isNone)).length := hspec.2.1
      have hcount : (layout.filter (fun x => x.isNone)).length = 14 := by simp [layout]
      have hds14 : ds.length = 14 := hlen.trans hcount
      have hdigit := hspec.2.2
      have hpaint : digits layout (paint layout ds) = some ds :=
        digits_paint layout ds (by simpa [hcount] using hlen) hdigit
      have hw : w = paint layout ds := digits_inj layout w (paint layout ds) ds hdig hpaint
      rw [spell, ← hds14, value_digits ds hdigit, hw]

/-! ## CodeDomain -/

theorem sound_CodeDomain : CodeDomain.Goal := by
  intro v
  constructor
  · rintro ⟨w, hcode⟩
    unfold code at hcode
    cases hdig : digits layout w with
    | none => simp [hdig] at hcode
    | some ds =>
        simp [hdig, Option.some.injEq] at hcode
        subst v
        have hspec := digits_spec layout w ds hdig
        have hlt := hspec.2.2
        have hpow := value_lt_pow ds hlt
        have h14 : ds.length ≤ 14 := by
          have := hspec.2.1
          simp [layout] at this
          omega
        exact Nat.lt_of_lt_of_le hpow (Nat.pow_le_pow_right (by decide : 0 < 10) h14)
  · intro hv
    refine ⟨spell v, ?_⟩
    unfold code
    rw [digits_spell]
    simp only [Option.map_some]
    rw [value_decDigits v hv]

/-! ## helpers: lexicographic order of the fixed layout -/

private theorem lex_cons_cons (a b : Nat) (l₁ l₂ : List Nat) :
    ((a :: l₁ : List Nat) < (b :: l₂)) ↔ a < b ∨ (a = b ∧ l₁ < l₂) := by
  constructor
  · intro h
    change List.Lex (fun x y : Nat => x < y) (a :: l₁) (b :: l₂) at h
    cases h with
    | rel hlt => exact Or.inl hlt
    | cons hlex => exact Or.inr ⟨rfl, hlex⟩
  · rintro (hlt | ⟨hab, hlex⟩)
    · exact List.Lex.rel hlt
    · subst b
      exact List.Lex.cons hlex

private theorem paint_layout_lex (fmt : List (Option Nat)) (ds es : List Nat)
    (hdl : ds.length = (fmt.filter (fun x => x.isNone)).length)
    (hel : es.length = (fmt.filter (fun x => x.isNone)).length)
    (hdd : ∀ d ∈ ds, d < 10) (hde : ∀ e ∈ es, e < 10) :
    (paint fmt ds < paint fmt es) ↔ ds < es := by
  induction fmt generalizing ds es with
  | nil =>
      have hdn : ds = [] := List.eq_nil_of_length_eq_zero (by simpa using hdl)
      have hen : es = [] := List.eq_nil_of_length_eq_zero (by simpa using hel)
      subst ds
      subst es
      rfl
  | cons f fmt ih =>
      cases f with
      | some t =>
          simp only [paint]
          rw [lex_cons_cons]
          simp at hdl hel ⊢
          exact ih ds es hdl hel hdd hde
      | none =>
          cases ds with
          | nil =>
              simp at hdl
          | cons d ds' =>
              cases es with
              | nil =>
                  simp at hel
              | cons e es' =>
                  simp only [paint]
                  rw [lex_cons_cons, lex_cons_cons]
                  have ih' := ih ds' es' (by simpa using hdl) (by simpa using hel)
                    (fun x hx => hdd x (by simp [hx])) (fun x hx => hde x (by simp [hx]))
                  by_cases hdeq : d = e
                  · subst e
                    simp [ih']
                  · rcases Nat.lt_or_gt_of_ne hdeq with hdlt | hedlt
                    · have hlt : 48 + d < 48 + e := by omega
                      simp [hlt, hdlt, hdeq]
                    · have hlt : ¬ (48 + d < 48 + e) := by omega
                      have hnd : ¬ d < e := by omega
                      simp [hlt, hnd, hdeq]

private theorem value_lt_of_lt_head (d e : Nat) (ds es : List Nat) (hde : d < e)
    (hb : value ds < 10 ^ ds.length) (hlen : ds.length = es.length) :
    value (d :: ds) < value (e :: es) := by
  rw [value_cons, value_cons]
  let p := 10 ^ ds.length
  have hp : 0 < p := Nat.pow_pos (by decide : 0 < 10)
  have hdp : d * p < e * p := Nat.mul_lt_mul_of_pos_right hde hp
  have hsucc : (d + 1) * p ≤ e * p := Nat.mul_le_mul_right p (Nat.succ_le_of_lt hde)
  have hadd : d * p + p ≤ e * p := by simpa [Nat.succ_mul] using hsucc
  calc
    d * p + value ds < d * p + p := Nat.add_lt_add_left hb _
    _ ≤ e * p := hadd
    _ = e * 10 ^ es.length := by
      change e * 10 ^ ds.length = e * 10 ^ es.length
      rw [hlen]
    _ ≤ e * 10 ^ es.length + value es := Nat.le_add_right _ _

private theorem digits_lex_value (ds es : List Nat) (hlen : ds.length = es.length)
    (hdd : ∀ d ∈ ds, d < 10) (hde : ∀ e ∈ es, e < 10) :
    (ds < es ↔ value ds < value es) := by
  induction ds generalizing es with
  | nil =>
      have hen : es = [] := List.eq_nil_of_length_eq_zero (by simpa using hlen.symm)
      subst es
      simp
  | cons d ds ih =>
      cases es with
      | nil => simp at hlen
      | cons e es =>
          have htail : ds.length = es.length := by simpa using hlen
          have hdd' : ∀ x ∈ ds, x < 10 := fun x hx => hdd x (by simp [hx])
          have hde' : ∀ x ∈ es, x < 10 := fun x hx => hde x (by simp [hx])
          by_cases hdeq : d = e
          · subst e
            constructor
            · intro hlex
              rw [lex_cons_cons] at hlex
              have htaillex : ds < es := by
                rcases hlex with hlt | ⟨_, h⟩
                · omega
                · exact h
              have hv := (ih es htail hdd' hde').mp htaillex
              rw [value_cons, value_cons, htail]
              omega
            · intro hv
              have hv' : value ds < value es := by
                rw [value_cons, value_cons, htail] at hv
                omega
              have htaillex := (ih es htail hdd' hde').mpr hv'
              rw [lex_cons_cons]
              exact Or.inr ⟨rfl, htaillex⟩
          · rcases Nat.lt_or_gt_of_ne hdeq with hdlt | hedlt
            · have hvlt := value_lt_of_lt_head d e ds es hdlt (value_lt_pow ds hdd') htail
              constructor
              · intro _
                exact hvlt
              · intro _
                rw [lex_cons_cons]
                exact Or.inl hdlt
            · have hvgt := value_lt_of_lt_head e d es ds hedlt (value_lt_pow es hde') htail.symm
              constructor
              · intro hlex
                rw [lex_cons_cons] at hlex
                rcases hlex with hlt | ⟨heq, _⟩
                · omega
                · subst d
                  omega
              · intro hv
                omega

/-! ## LexicalOrder -/

theorem sound_LexicalOrder : LexicalOrder.Goal := by
  intro a b x y hca hcb
  have ha := sound_LexicalInverse a x hca
  have hb := sound_LexicalInverse b y hcb
  subst a
  subst b
  let da := (List.range 14).reverse.map (fun i => x / 10 ^ i % 10)
  let db := (List.range 14).reverse.map (fun i => y / 10 ^ i % 10)
  have hxlt : x < 10 ^ 14 := (sound_CodeDomain x).mp ⟨spell x, hca⟩
  have hylt : y < 10 ^ 14 := (sound_CodeDomain y).mp ⟨spell y, hcb⟩
  have hdlt : ∀ d ∈ da, d < 10 := by
    intro d hd
    rcases List.mem_map.mp hd with ⟨i, hi, rfl⟩
    exact Nat.mod_lt _ (by decide : 0 < 10)
  have hedlt : ∀ e ∈ db, e < 10 := by
    intro d hd
    rcases List.mem_map.mp hd with ⟨i, hi, rfl⟩
    exact Nat.mod_lt _ (by decide : 0 < 10)
  have hlen : da.length = db.length := by simp [da, db]
  have hcount : (layout.filter (fun c => c.isNone)).length = 14 := by simp [layout]
  have hlda : da.length = (layout.filter (fun c => c.isNone)).length := by simp [hcount, da]
  have hldb : db.length = (layout.filter (fun c => c.isNone)).length := by simp [hcount, db]
  rw [show spell x = paint layout da by rfl, show spell y = paint layout db by rfl]
  rw [paint_layout_lex layout da db hlda hldb hdlt hedlt, digits_lex_value da db hlen hdlt hedlt]
  rw [value_decDigits x hxlt, value_decDigits y hylt]

/-! ## helpers: mapM over Option -/

private theorem mapM_length {α β : Type} (f : α → Option β) :
    ∀ (l : List α) (vs : List β), l.mapM f = some vs → vs.length = l.length := by
  intro l
  induction l with
  | nil =>
      intro vs h
      simp at h
      subst vs
      rfl
  | cons a as ih =>
      intro vs h
      simp only [List.mapM_cons] at h
      cases hf : f a with
      | none => simp [hf] at h
      | some x =>
          cases hm : as.mapM f with
          | none => simp [hf, hm] at h
          | some xs =>
              simp [hf, hm] at h
              subst vs
              simp [ih xs hm]

private theorem mapM_get {α β : Type} (f : α → Option β) :
    ∀ (l : List α) (vs : List β), l.mapM f = some vs →
      ∀ i : Nat, (l[i]?).map f = (vs[i]?).map some := by
  intro l
  induction l with
  | nil =>
      intro vs h i
      simp at h
      subst vs
      simp
  | cons a as ih =>
      intro vs h i
      simp only [List.mapM_cons] at h
      cases hf : f a with
      | none => simp [hf] at h
      | some x =>
          cases hm : as.mapM f with
          | none => simp [hf, hm] at h
          | some xs =>
              simp [hf, hm] at h
              subst vs
              cases i with
              | zero => simp [hf]
              | succ i => simpa [List.getElem?_cons_succ] using ih xs hm i

private theorem mapM_of_get {α β : Type} (f : α → Option β) :
    ∀ (l : List α) (vs : List β), vs.length = l.length →
      (∀ i : Nat, (l[i]?).map f = (vs[i]?).map some) → l.mapM f = some vs := by
  intro l
  induction l with
  | nil =>
      intro vs hlen hget
      have : vs = [] := List.eq_nil_of_length_eq_zero (by simpa using hlen)
      subst vs
      rfl
  | cons a as ih =>
      intro vs hlen hget
      cases vs with
      | nil => simp at hlen
      | cons v vs =>
          have h0 := hget 0
          simp only [List.getElem?_cons_zero, Option.map_some] at h0
          cases hf : f a with
          | none => simp [hf] at h0
          | some x =>
              have hx : x = v := by simpa [hf] using h0
              subst x
              have hrest : ∀ i : Nat, (as[i]?).map f = (vs[i]?).map some := by
                intro i
                simpa [List.getElem?_cons_succ] using hget (i + 1)
              rw [List.mapM_cons, ih vs (by simpa using hlen) hrest]
              simp [hf]

private theorem mapM_append {α β : Type} (f : α → Option β) (l₁ l₂ : List α)
    (v₁ v₂ : List β) (h₁ : l₁.mapM f = some v₁) (h₂ : l₂.mapM f = some v₂) :
    (l₁ ++ l₂).mapM f = some (v₁ ++ v₂) := by
  simp [List.mapM_append, h₁, h₂]

/-! ## helpers: ULEB charge -/

private theorem log2_div_128 (n : Nat) (hn : 128 ≤ n) :
    Nat.log2 n = Nat.log2 (n / 128) + 7 := by
  let q := n / 128
  have hq : q ≠ 0 := by dsimp [q]; omega
  have hlow : 2 ^ (Nat.log2 q + 7) ≤ n := by
    rw [Nat.pow_add]
    change 2 ^ Nat.log2 q * 2 ^ 7 ≤ n
    rw [show (2 : Nat) ^ 7 = 128 by decide]
    have hqle : 2 ^ Nat.log2 q ≤ q := Nat.log2_self_le hq
    have hmul : q * 128 ≤ n := by dsimp [q]; exact Nat.div_mul_le_self n 128
    have hle : 2 ^ Nat.log2 q * 128 ≤ q * 128 := Nat.mul_le_mul_right 128 hqle
    exact Nat.le_trans hle hmul
  have hloglow : Nat.log2 q + 7 ≤ Nat.log2 n :=
    (Nat.le_log2 (by omega : n ≠ 0)).mpr hlow
  have hhigh : n < 2 ^ (Nat.log2 q + 8) := by
    have hqbound : q < 2 ^ (Nat.log2 q + 1) := Nat.lt_log2_self
    have hq1 : q + 1 ≤ 2 ^ (Nat.log2 q + 1) := Nat.succ_le_of_lt hqbound
    have hnlt : n < 128 * (q + 1) := by
      dsimp [q]
      simpa [Nat.mul_comm] using Nat.lt_mul_div_succ n (by decide : 0 < 128)
    have hmul : 128 * (q + 1) ≤ 128 * 2 ^ (Nat.log2 q + 1) :=
      Nat.mul_le_mul_left 128 hq1
    have hp : 128 * 2 ^ (Nat.log2 q + 1) = 2 ^ (Nat.log2 q + 8) := by
      calc
        128 * 2 ^ (Nat.log2 q + 1) = 2 ^ 7 * 2 ^ (Nat.log2 q + 1) := by
          rw [show (128 : Nat) = 2 ^ 7 by decide]
        _ = 2 ^ (7 + (Nat.log2 q + 1)) := (Nat.pow_add 2 7 (Nat.log2 q + 1)).symm
        _ = 2 ^ (Nat.log2 q + 8) := by
          congr 1
          omega
    exact Nat.lt_of_lt_of_le hnlt (by simpa [hp] using hmul)
  have hloghigh : Nat.log2 n < Nat.log2 q + 8 :=
    (Nat.log2_lt (by omega : n ≠ 0)).mpr (by simpa [q] using hhigh)
  change Nat.log2 n = Nat.log2 q + 7
  omega

private theorem uleb_length (n : Nat) : (uleb n).length = ubytes n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
        unfold ubytes width
        by_cases hn : n == 0
        · simp [hn]
        · simp [hn]
          have hn0 : n ≠ 0 := by simpa using hn
          have hloglt : Nat.log2 n < 7 :=
            (Nat.log2_lt hn0).mpr (by
              change n < 2 ^ 7
              rw [show (2 : Nat) ^ 7 = 128 by decide]
              exact hlt)
          omega
      · have hn128 : 128 ≤ n := by omega
        have hdivlt : n / 128 < n := by omega
        have hdivlen : (uleb (n / 128)).length = ubytes (n / 128) := ih (n / 128) hdivlt
        rw [uleb]
        simp [hlt, hdivlen]
        unfold ubytes width
        have hn0 : n ≠ 0 := by omega
        have hnq : n / 128 ≠ 0 := by omega
        simp [hn0, hnq]
        have hlogeq := log2_div_128 n hn128
        rw [hlogeq]
        omega

/-! ## FixedWidthCharge -/

private theorem flatten_length_all (ws : Words) (n : Nat) (h : ∀ w ∈ ws, w.length = n) :
    ws.flatten.length = n * ws.length := by
  induction ws with
  | nil => simp
  | cons w ws ih =>
      have hw : w.length = n := h w (by simp)
      have ht : ∀ x ∈ ws, x.length = n := fun x hx => h x (by simp [hx])
      have ih' := ih ht
      simp only [List.flatten_cons, List.length_append, List.length_cons]
      rw [hw, ih']
      rw [Nat.mul_succ]
      ac_rfl

theorem sound_FixedWidthCharge : FixedWidthCharge.Goal := by
  intro ws bs hstream
  unfold stream at hstream
  cases hm : ws.mapM code with
  | none => simp [hm] at hstream
  | some vs =>
      have hbs : pack 47 vs = bs := by simpa [hm] using hstream
      have hlen : vs.length = ws.length := mapM_length code ws vs hm
      have hwordlen : ∀ w ∈ ws, w.length = 22 := by
        intro w hw
        rcases List.mem_iff_getElem.mp hw with ⟨i, hi, rfl⟩
        have hvi : i < vs.length := by omega
        have hcopt := mapM_get code ws vs hm i
        have hc : code ws[i] = some vs[i] := by
          simpa [List.getElem?_eq_getElem hi, List.getElem?_eq_getElem hvi] using hcopt
        cases hcd : digits layout ws[i] with
        | none => simp [code, hcd] at hc
        | some ds =>
            have hspec := digits_spec layout ws[i] ds hcd
            have hlay : layout.length = 22 := by simp [layout]
            omega
      have hflat : ws.flatten.length = 22 * ws.length := flatten_length_all ws 22 hwordlen
      constructor
      · unfold raw
        rw [hflat]
        omega
      · rw [← hbs]
        simp only [pack, little, List.length_map, List.length_range]
        rw [hlen]
        congr 1
        omega

/-! ## ExceptionCharge -/

theorem sound_ExceptionCharge : ExceptionCharge.Goal := by
  intro ws h
  have hmask : (masked ws).length = byteCount (47 * ws.length) := by
    simp [masked, pack, little, List.length_map, List.length_range]
    rw [Nat.mul_comm]
  have hbody : (exceptionBody ws).length =
      ((exceptions ws).map (fun (w, i) => ubytes i + ubytes w.length + w.length)).sum := by
    unfold exceptionBody
    rw [List.length_flatMap]
    congr 1
    apply List.map_congr_left
    intro e he
    rcases e with ⟨w, i⟩
    simp [uleb_length]
    omega
  rw [hmask, hbody]

/-! ## helpers: mapM derived facts -/

private theorem mapM_tail {α β : Type} (f : α → Option β) (a : α) (l : List α)
    (x : β) (xs : List β) : (a :: l).mapM f = some (x :: xs) → l.mapM f = some xs := by
  intro h
  simp only [List.mapM_cons] at h
  cases hf : f a with
  | none => simp [hf] at h
  | some y =>
      cases hm : l.mapM f with
      | none => simp [hf, hm] at h
      | some ys =>
          simp [hf, hm] at h
          have hyxs : ys = xs := h.2
          subst xs
          rfl

private theorem mapM_code_at (l : Words) (vs : List Nat) (h : l.mapM code = some vs)
    (i : Nat) (hi : i < l.length) (hvi : i < vs.length) : code l[i] = some vs[i] := by
  have hopt := mapM_get code l vs h i
  simpa [List.getElem?_eq_getElem hi, List.getElem?_eq_getElem hvi] using hopt

private theorem code_le_iff {w lo : Bytes} {v l : Nat}
    (hc : code w = some v) (hlo : code lo = some l) : (lo ≤ w ↔ l ≤ v) := by
  have hlt2 := sound_LexicalOrder w lo v l hc hlo
  constructor
  · intro hle
    by_cases hvl : l ≤ v
    · exact hvl
    · exfalso
      have hvlt : v < l := Nat.lt_of_not_ge hvl
      have hwlt : w < lo := hlt2.mpr hvlt
      exact (List.not_le).2 hwlt hle
  · intro hle
    by_cases hwl : lo ≤ w
    · exact hwl
    · exfalso
      have hwlt : w < lo := (List.not_le).1 hwl
      have hvlt : v < l := hlt2.mp hwlt
      omega

/-! ## CrossColumnDate -/

theorem sound_CrossColumnDate : CrossColumnDate.Goal := by
  intro a b xs ys ha hb hday
  have hxa : xs.length = a.length := mapM_length code a xs ha
  have hyb : ys.length = b.length := mapM_length code b ys hb
  have hxy : xs.length = ys.length := by
    have := congrArg List.length hday
    simpa using this
  apply List.ext_getElem?
  intro i
  by_cases hi : i < ys.length
  · have hxi : i < xs.length := by omega
    have hbi : i < b.length := by omega
    have hcb := mapM_code_at b ys hb i hbi hi
    have hbspell : spell ys[i] = b[i] := sound_LexicalInverse b[i] ys[i] hcb
    have hdi : xs[i] / 10 ^ 6 = ys[i] / 10 ^ 6 := by
      have h := congrArg (fun l : List Nat => l[i]?) hday
      simpa [List.getElem?_map, List.getElem?_eq_getElem hxi,
        List.getElem?_eq_getElem hi] using h
    have hval : (xs[i] / 10 ^ 6) * 10 ^ 6 + ys[i] % 10 ^ 6 = ys[i] := by
      rw [hdi]
      simpa [Nat.mul_comm] using Nat.div_add_mod (ys[i]) (10 ^ 6)
    have hxopt : xs[i]? = some xs[i] := List.getElem?_eq_getElem hxi
    have hyopt : ys[i]? = some ys[i] := List.getElem?_eq_getElem hi
    rw [List.getElem?_zipWith, hxopt, hyopt]
    simp only [Option.some.injEq]
    rw [hval, hbspell, List.getElem?_eq_getElem hbi]
  · have hyo : ys[i]? = none := by
      exact List.getElem?_eq_none (by omega)
    have hbo : b[i]? = none := by
      exact List.getElem?_eq_none (by omega)
    rw [List.getElem?_zipWith, hyo, hbo]
    simp

/-! ## RangeScan -/

/-! ## byte/pack helpers (ported from the Nullable proof module) -/

private theorem fromLittle_append_singleton (l : List Nat) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256 ^ l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m * 256) = v % m + m * ((v / m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256 * t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m * q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hsum_lt : r + m * s < m * 256 := by
    have hm_le : m * s ≤ m * 255 := Nat.mul_le_mul_left m hs_le
    omega
  have hrewrite : v = (r + m * s) + (m * 256) * t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m * 256) = r + m * s := by
    calc
      v % (m * 256) = ((r + m * s) + (m * 256) * t) % (m * 256) := by rw [hrewrite]
      _ = (r + m * s) % (m * 256) := by rw [Nat.add_mul_mod_self_left]
      _ = r + m * s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v / m) % 256) = r + m * s := by rfl
  exact hleft.trans hright.symm

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hmod : (bits + 7) % 8 < 8 := Nat.mod_lt _ (by decide)
  have hdiv := Nat.mod_add_div (bits + 7) 8
  omega

private theorem byteCount_le_of_le (bits b : Nat) (h : bits ≤ 8 * b) : byteCount bits ≤ b := by
  unfold byteCount
  have hmod : (bits + 7) % 8 < 8 := Nat.mod_lt _ (by decide)
  have hdiv := Nat.mod_add_div (bits + 7) 8
  omega

private theorem word_lt_pow (w : Nat) (xs : List Nat) :
    word w xs < 2 ^ (xs.length * w) := by
  induction xs with
  | nil => simp [word]
  | cons x xs ih =>
      have hxmod : x % 2 ^ w < 2 ^ w := Nat.mod_lt x (Nat.two_pow_pos w)
      have hsum : x % 2 ^ w + 2 ^ w * word w xs < 2 ^ w * (word w xs + 1) := by
        rw [Nat.mul_succ, Nat.add_comm (2 ^ w * word w xs) (2 ^ w)]
        exact Nat.add_lt_add_right hxmod _
      have hle : 2 ^ w * (word w xs + 1) ≤ 2 ^ w * 2 ^ (xs.length * w) :=
        Nat.mul_le_mul_left (2 ^ w) (Nat.succ_le_of_lt ih)
      have hp_exp : (xs.length + 1) * w = w + xs.length * w := by
        rw [Nat.add_mul, Nat.one_mul]
        omega
      have hp : 2 ^ ((xs.length + 1) * w) = 2 ^ w * 2 ^ (xs.length * w) := by
        rw [hp_exp, Nat.pow_add]
      rw [word]
      change x % 2 ^ w + 2 ^ w * word w xs < 2 ^ ((xs.length + 1) * w)
      rw [hp]
      exact Nat.lt_of_lt_of_le hsum hle

private theorem fromLittle_little (v k : Nat) :
    fromLittle ((List.range k).map (fun j => (v / 256 ^ j) % 256)) = v % 256 ^ k := by
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 v (256 ^ k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem fromLittle_pack (w : Nat) (xs : List Nat) :
    fromLittle (pack w xs) = word w xs := by
  unfold pack little
  rw [fromLittle_little]
  have hlt := word_lt_pow w xs
  have hb := bits_le_byteCount (xs.length * w)
  have hpow : 256 ^ byteCount (xs.length * w) = 2 ^ (8 * byteCount (xs.length * w)) := by
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, Nat.pow_mul]
  apply Nat.mod_eq_of_lt
  rw [hpow]
  exact Nat.lt_of_lt_of_le hlt (Nat.pow_le_pow_right (by decide : 0 < 2) hb)

private theorem word_append (w : Nat) (a b : List Nat) :
    word w (a ++ b) = word w a + 2 ^ (w * a.length) * word w b := by
  induction a with
  | nil => simp [word]
  | cons x xs ih =>
      rw [List.cons_append, word, ih, word]
      have hp_exp : w * (xs.length + 1) = w + w * xs.length := by
        rw [Nat.mul_add, Nat.mul_one, Nat.add_comm]
      have hp : 2 ^ (w * (xs.length + 1)) = 2 ^ w * 2 ^ (w * xs.length) := by
        rw [hp_exp, Nat.pow_add]
      rw [List.length_cons, hp]
      simp only [Nat.mul_add, Nat.add_assoc]
      ac_rfl

private theorem little_succ_append (v k : Nat) :
    little v (k + 1) = little v k ++ [(v / 256 ^ k) % 256] := by
  unfold little
  rw [List.range_succ, List.map_append]
  simp

private theorem little_cons (v k : Nat) :
    little v (k + 1) = (v % 256) :: little (v / 256) k := by
  unfold little
  rw [List.range_succ_eq_map, List.map_cons, List.map_map]
  simp only [Nat.pow_zero, Nat.div_one]
  congr 1
  apply List.map_congr_left
  intro i hi
  simp only [Function.comp_apply]
  have hp : 256 ^ Nat.succ i = 256 * 256 ^ i := Nat.pow_succ'
  rw [hp, ← Nat.div_div_eq_div_mul]

private theorem little_concat (ba bb A B : Nat)
    (hA : A < 256 ^ ba) (hB : B < 256 ^ bb) :
    little (A + 256 ^ ba * B) (ba + bb) = little A ba ++ little B bb := by
  induction ba generalizing bb A B with
  | zero =>
      have hA0 : A = 0 := by
        have hlt : A < 1 := by simpa using hA
        exact Nat.lt_one_iff.mp hlt
      subst A
      simp [little]
  | succ ba ih =>
      rw [show ba + 1 + bb = ba + bb + 1 from by omega]
      have hL : little (A + 256 ^ (ba + 1) * B) (ba + bb + 1) =
          (A + 256 ^ (ba + 1) * B) % 256 ::
            little ((A + 256 ^ (ba + 1) * B) / 256) (ba + bb) := little_cons _ _
      rw [hL]
      have hR : little A (ba + 1) = A % 256 :: little (A / 256) ba := little_cons _ _
      rw [hR, List.cons_append]
      have hNmod : (A + 256 ^ (ba + 1) * B) % 256 = A % 256 := by
        have hpow : 256 ^ (ba + 1) = 256 * 256 ^ ba := Nat.pow_succ'
        rw [hpow]
        have hassoc : (256 * 256 ^ ba) * B = 256 * (256 ^ ba * B) := by
          rw [Nat.mul_assoc]
        rw [hassoc]
        exact Nat.add_mul_mod_self_left A 256 (256 ^ ba * B)
      rw [hNmod]
      congr 1
      have hNdiv : (A + 256 ^ (ba + 1) * B) / 256 = A / 256 + 256 ^ ba * B := by
        have hpow : 256 ^ (ba + 1) = 256 * 256 ^ ba := Nat.pow_succ'
        rw [hpow]
        have hassoc : (256 * 256 ^ ba) * B = 256 * (256 ^ ba * B) := by
          rw [Nat.mul_assoc]
        rw [hassoc]
        exact Nat.add_mul_div_left A (256 ^ ba * B) (by decide : 0 < 256)
      rw [hNdiv]
      have hA' : A / 256 < 256 ^ ba := by
        have hpow : 256 ^ (ba + 1) = 256 ^ ba * 256 := Nat.pow_succ 256 ba
        rw [hpow] at hA
        exact (Nat.div_lt_iff_lt_mul (by decide : 0 < 256)).mpr hA
      exact ih _ _ _ hA' hB

private theorem little_drop (v s k : Nat) :
    (little v k).drop s = little (v / 256 ^ s) (k - s) := by
  induction s generalizing v k with
  | zero => simp [little]
  | succ s ih =>
      cases k with
      | zero => simp [little]
      | succ k =>
          rw [little_cons, List.drop_succ_cons, ih]
          have hp : (v / 256) / 256 ^ s = v / 256 ^ (s + 1) := by
            rw [Nat.div_div_eq_div_mul, Nat.pow_succ']
          rw [hp]
          congr 1
          omega

private theorem little_take_le (v k m : Nat) (hm : m ≤ k) :
    (little v k).take m = little v m := by
  induction k generalizing m with
  | zero =>
      have : m = 0 := by omega
      subst m
      simp [little]
  | succ k ih =>
      by_cases hmk : m ≤ k
      · rw [little_succ_append, List.take_append_of_le_length]
        · exact ih m hmk
        · simpa [little] using hmk
      · have hm_eq : m = k + 1 := by omega
        subst m
        simpa [little] using (List.take_length (l := little v (k + 1)))

private theorem word_shift (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w) :
    word w xs / 2 ^ w = word w (List.drop 1 xs) := by
  cases xs with
  | nil => simp [word]
  | cons x xs =>
      have hx : x < 2 ^ w := h x (by simp)
      have hxmod : x % 2 ^ w = x := Nat.mod_eq_of_lt hx
      have hxdiv : x / 2 ^ w = 0 := Nat.div_eq_of_lt hx
      rw [word, hxmod]
      rw [Nat.add_comm]
      rw [Nat.mul_add_div (Nat.two_pow_pos w) (word w xs) x, hxdiv, Nat.add_zero]
      simp

private theorem word_digit (w i : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w)
    (hi : i < xs.length) : (word w xs / 2 ^ (i * w)) % 2 ^ w = xs[i] := by
  induction i generalizing xs with
  | zero =>
      simp only [Nat.zero_mul, Nat.pow_zero, Nat.div_one]
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have hx : x < 2 ^ w := h x (by simp)
          simp [word, Nat.mod_eq_of_lt hx]
  | succ i ih =>
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have ht : ∀ y ∈ xs, y < 2 ^ w := by
            intro y hy
            exact h y (by simp [hy])
          have hp : 2 ^ ((i + 1) * w) = 2 ^ w * 2 ^ (i * w) := by
            have hmul : (i + 1) * w = w + i * w := by
              rw [Nat.succ_mul, Nat.add_comm]
            rw [hmul, Nat.pow_add]
          rw [hp, ← Nat.div_div_eq_div_mul, word_shift w (x :: xs) h]
          have hi' : i < xs.length := Nat.lt_of_succ_lt_succ (by simpa using hi)
          exact ih xs ht hi'

private theorem takeAt_pack (w i : Nat) (xs : List Nat) (hbound : ∀ x ∈ xs, x < 2 ^ w)
    (hi : i < xs.length) : takeAt (pack w xs) w i = xs[i] := by
  by_cases hw : w = 0
  · subst w
    have hxi : xs[i] = 0 := by
      have hb := hbound (xs[i]) (List.getElem_mem hi)
      simpa using hb
    simp [takeAt, pack, byteCount, hxi, fromLittle]
  · let v := word w xs
    let k := byteCount (xs.length * w)
    let start := i * w / 8
    let shift := i * w % 8
    let m := byteCount (shift + w)
    have hk_bits : xs.length * w ≤ 8 * k := by
      simpa [k] using bits_le_byteCount (xs.length * w)
    have hiw_le : i * w ≤ xs.length * w := Nat.mul_le_mul_right w (Nat.le_of_lt hi)
    have hiw1_le : (i + 1) * w ≤ xs.length * w :=
      Nat.mul_le_mul_right w (Nat.succ_le_of_lt hi)
    have hstep : (i + 1) * w = i * w + w := by rw [Nat.succ_mul]
    have hstart_le_k : start ≤ k := by
      have hdiv : i * w / 8 ≤ (8 * k) / 8 := Nat.div_le_div_right (by omega)
      have hcancel : (8 * k) / 8 = k := by omega
      simpa [start, hcancel] using hdiv
    have hshiftlt : shift < 8 := by
      dsimp [shift]
      exact Nat.mod_lt _ (by decide)
    have hiweq : i * w = 8 * start + shift := by
      dsimp [start, shift]
      have hmad := Nat.div_add_mod (i * w) 8
      omega
    have hmulsub : 8 * (k - start) = 8 * k - 8 * start := by omega
    have hwindow : shift + w ≤ 8 * (k - start) := by
      rw [hmulsub]
      omega
    have hm_le : m ≤ k - start := by
      dsimp [m]
      exact byteCount_le_of_le (shift + w) (k - start) hwindow
    have hfl : fromLittle ((little v k).drop start |>.take m) =
        (v / 256 ^ start) % 256 ^ m := by
      rw [little_drop, little_take_le _ _ _ hm_le]
      simpa [little] using (fromLittle_little (v / 256 ^ start) m)
    change (fromLittle ((little v k).drop start |>.take m) / 2 ^ shift) % 2 ^ w = xs[i]
    rw [hfl]
    have h8m_ge_shift : shift ≤ 8 * m := by
      have hb := bits_le_byteCount (shift + w)
      dsimp [m] at hb ⊢
      omega
    have h8m_ge_w : w ≤ 8 * m - shift := by
      have hb := bits_le_byteCount (shift + w)
      dsimp [m] at hb ⊢
      omega
    have hpow256 : 256 ^ m = 2 ^ (8 * m) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, Nat.pow_mul]
    let q := v / 256 ^ start
    let r := q % 256 ^ m
    let t := q / 256 ^ m
    have hq : q = r + 256 ^ m * t := by
      dsimp [r, t, q]
      exact (Nat.mod_add_div (v / 256 ^ start) (256 ^ m)).symm
    have hdivq : q / 2 ^ shift = r / 2 ^ shift + 2 ^ (8 * m - shift) * t := by
      rw [hq, hpow256]
      have hfactor : 2 ^ (8 * m) = 2 ^ shift * 2 ^ (8 * m - shift) := by
        rw [← Nat.pow_add]
        congr 1
        omega
      rw [hfactor, Nat.mul_assoc]
      exact Nat.add_mul_div_left r (2 ^ (8 * m - shift) * t) (Nat.two_pow_pos shift)
    have hmod_eq : (q / 2 ^ shift) % 2 ^ w = (r / 2 ^ shift) % 2 ^ w := by
      rw [hdivq]
      have hfactor : 2 ^ (8 * m - shift) * t =
          2 ^ w * (2 ^ (8 * m - shift - w) * t) := by
        have hexp : 8 * m - shift = w + (8 * m - shift - w) := by omega
        rw [hexp, Nat.pow_add, Nat.mul_assoc]
        simp
      rw [hfactor]
      exact Nat.add_mul_mod_self_left (r / 2 ^ shift) (2 ^ w)
        (2 ^ (8 * m - shift - w) * t)
    have h256start : 256 ^ start = 2 ^ (8 * start) := by
      have h256 : (256 : Nat) = 2 ^ 8 := by decide
      rw [h256, Nat.pow_mul]
    have hqv : q / 2 ^ shift = v / 2 ^ (i * w) := by
      dsimp [q]
      rw [h256start, Nat.div_div_eq_div_mul]
      have hp : 2 ^ (8 * start) * 2 ^ shift = 2 ^ (i * w) := by
        rw [← Nat.pow_add]
        congr 1
        omega
      rw [hp]
    rw [← hmod_eq, hqv]
    exact word_digit w i xs hbound hi

/-! ### Variable-length charges and flip positions -/


private theorem code_lt_2pow47 (w : Bytes) (v : Nat) (h : code w = some v) : v < 2 ^ 47 := by
  have hv : v < 10 ^ 14 := (sound_CodeDomain v).1 ⟨w, h⟩
  have hp : 10 ^ 14 < 2 ^ 47 := by decide
  exact Nat.lt_trans hv hp

private theorem mapM_code_bound : ∀ (ws : Words) (vs : List Nat),
    ws.mapM code = some vs → ∀ x ∈ vs, x < 2 ^ 47 := by
  intro ws
  induction ws with
  | nil =>
      intro vs h x hx
      simp at h
      subst vs
      simp at hx
  | cons w ws ih =>
      intro vs h x hx
      simp only [List.mapM_cons] at h
      cases hf : code w with
      | none => simp [hf] at h
      | some v =>
          cases hm : ws.mapM code with
          | none => simp [hf, hm] at h
          | some vs' =>
              simp [hf, hm] at h
              cases h
              simp at hx
              cases hx with
              | inl heq =>
                  subst x
                  exact code_lt_2pow47 w v hf
              | inr hxmem =>
                  exact ih vs' hm x hxmem

theorem sound_PackedSeek : PackedSeek.Goal := by
  intro ws bs i hstream hi
  unfold stream at hstream
  cases hm : ws.mapM code with
  | none => simp [hm] at hstream
  | some vs =>
      simp [hm] at hstream
      subst bs
      have hlen : vs.length = ws.length := mapM_length code ws vs hm
      have hbound : ∀ x ∈ vs, x < 2 ^ 47 := mapM_code_bound ws vs hm
      have hcode := mapM_code_at ws vs hm i hi (by omega)
      have hspell : spell vs[i] = ws[i] := sound_LexicalInverse ws[i] vs[i] hcode
      have htake : takeAt (pack 47 vs) 47 i = vs[i] :=
        takeAt_pack 47 i vs hbound (by omega)
      simp [extract, htake, hspell, List.getElem?_eq_getElem hi]

theorem sound_SharedDay : SharedDay.Goal := by
  intro ws vs day hws hday
  have hlen := mapM_length code ws vs hws
  let codes := vs.map (fun x => x % 10 ^ 6)
  have hbound : ∀ x ∈ codes, x < 2 ^ 20 := by
    intro x hx
    rcases List.mem_map.mp hx with ⟨v, hv, rfl⟩
    have hlt : v % 10 ^ 6 < 10 ^ 6 := Nat.mod_lt _ (by decide : 0 < 10 ^ 6)
    have hp : 10 ^ 6 < 2 ^ 20 := by decide
    exact Nat.lt_trans hlt hp
  apply List.ext_getElem?
  intro i
  by_cases hi : i < vs.length
  · have hci : i < codes.length := by simp [codes, hi]
    have htake : takeAt (pack 20 codes) 20 i = codes[i] :=
      takeAt_pack 20 i codes hbound hci
    have hcati : codes[i] = vs[i] % 10 ^ 6 := by simp [codes, hi]
    have hcode := mapM_code_at ws vs hws i (by omega) hi
    have hspell : spell vs[i] = ws[i] := sound_LexicalInverse ws[i] vs[i] hcode
    have hdi : vs[i] / 10 ^ 6 = day := hday vs[i] (List.getElem_mem hi)
    have hval : day * 10 ^ 6 + vs[i] % 10 ^ 6 = vs[i] := by
      rw [← hdi]
      simpa [Nat.mul_comm] using Nat.div_add_mod (vs[i]) (10 ^ 6)
    simp [List.getElem?_map, List.getElem?_range (by simpa using hi),
      htake, hcati, hval, hspell, codes]
  · have hi' : vs.length ≤ i := by omega
    have hwi : ws.length ≤ i := by omega
    have hrange : (List.range vs.length)[i]? = none :=
      List.getElem?_eq_none (l := List.range vs.length) (by simpa using hi')
    have hwsnone : ws[i]? = none := List.getElem?_eq_none hwi
    simp [List.getElem?_map, hrange, hwsnone]

private theorem find_mem {α : Type} (p : α → Bool) :
    ∀ (l : List α) (a : α), l.find? p = some a → a ∈ l := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a h
      by_cases hp : p x = true
      · simp [hp] at h
        subst a
        simp
      · simp [hp] at h
        exact List.mem_cons_of_mem x (ih a h)

private theorem find_true {α : Type} (p : α → Bool) :
    ∀ (l : List α) (a : α), l.find? p = some a → p a = true := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a h
      by_cases hp : p x = true
      · simp [hp] at h
        subst a
        exact hp
      · simp [hp] at h
        exact ih a h

private theorem zipIdx_mem_from {α : Type} (ws : List α) (k i : Nat)
    (hi : i < ws.length) : (ws[i], k + i) ∈ ws.zipIdx k := by
  induction ws generalizing i k with
  | nil => simp at hi
  | cons w ws ih =>
      cases i with
      | zero => simp
      | succ i =>
          simp at hi ⊢
          simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (k + 1) i hi

private theorem zipIdx_mem {α : Type} (ws : List α) (i : Nat) (hi : i < ws.length) :
    (ws[i], i) ∈ ws.zipIdx := by
  simpa using zipIdx_mem_from ws 0 i hi

private theorem getElem?_mem {α : Type} : ∀ (l : List α) (i : Nat) (a : α),
    l[i]? = some a → a ∈ l := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro i a h
      cases i with
      | zero => simp at h; subst a; simp
      | succ i => simp at h; exact List.mem_cons_of_mem x (ih i a h)

private theorem codeMaskBound (w : Bytes) : (code w).getD 0 < 2 ^ 47 := by
  cases hc : code w with
  | none => simp
  | some v =>
      simp [hc]
      exact code_lt_2pow47 w v hc

theorem sound_MixedSpelling : MixedSpelling.Goal := by
  intro ws i hi
  dsimp
  cases hfind : (exceptions ws).find? (fun e => e.2 == i) with
  | none =>
      have hnone := (List.find?_eq_none).1 hfind
      cases hc : code ws[i] with
      | none =>
          have hzmem : (ws[i], i) ∈ ws.zipIdx := zipIdx_mem ws i hi
          have hmem : (ws[i], i) ∈ exceptions ws := by
            simp [exceptions, List.mem_filter, hzmem, hc]
          exact (hnone (ws[i], i) hmem (by simp)).elim
      | some v =>
          have hbound : ∀ x ∈ ws.map (fun w => (code w).getD 0), x < 2 ^ 47 := by
            intro x hx
            rcases List.mem_map.mp hx with ⟨w, hw, rfl⟩
            exact codeMaskBound w
          have himap : i < (ws.map (fun w => (code w).getD 0)).length := by
            simpa using hi
          have htake : takeAt (pack 47 (ws.map (fun w => (code w).getD 0))) 47 i =
              (ws.map (fun w => (code w).getD 0))[i] :=
            takeAt_pack 47 i (ws.map (fun w => (code w).getD 0)) hbound himap
          have hval : (ws.map (fun w => (code w).getD 0))[i] = v := by
            simp [hc]
          have hspell : spell v = ws[i] := sound_LexicalInverse ws[i] v hc
          simp [hfind, masked, extract, htake, hval, hspell,
            List.getElem?_eq_getElem hi]
  | some e =>
      rcases e with ⟨w, j⟩
      have hmem : (w, j) ∈ exceptions ws :=
        find_mem (fun e : Bytes × Nat => e.2 == i) (exceptions ws) (w, j) hfind
      have hpred := find_true (fun e : Bytes × Nat => e.2 == i) (exceptions ws) (w, j) hfind
      have hji : j = i := beq_iff_eq.mp hpred
      have hz := (List.mem_filter).1 hmem
      have hz' := List.mem_zipIdx (x := w) (i := j) (xs := ws) (k := 0) hz.1
      have hw : w = ws[i] := by
        simpa [hji] using hz'.2.2
      simp [hfind, hw, List.getElem?_eq_getElem hi]

/-- DiscardSuffix is false: the three suffix bytes are part of the original
    spelling. For `x = 1` and `off = [0,0,0]`, `(spell 1).take 19 ++ [0,0,0]`
    is not `spell 1` (the real suffix is `[43,48,48]`, i.e. `+00`). -/
private theorem discard_suffix_counterexample :
    (spell 1).take 19 ++ [0,0,0] ≠ spell 1 := by
  decide

theorem refutation_DiscardSuffix :
    ¬ TinylambOpt.ResearchGoals.Compression.TemporalText.DiscardSuffix.Goal := by
  intro h
  exact discard_suffix_counterexample (h 1 [0,0,0] (by decide) (by decide))

theorem sound_RangeScan : RangeScan.Goal := by
  intro lo hi l h ws vs hlo hhi hws
  induction ws generalizing vs with
  | nil =>
      simp at hws
      subst vs
      simp
  | cons w ws ih =>
      cases vs with
      | nil =>
          cases hc : code w with
          | none => simp [hc] at hws
          | some x =>
              cases hm : ws.mapM code with
              | none => simp [hc, hm] at hws
              | some ys => simp [hc, hm] at hws
      | cons v vs =>
          have htail : ws.mapM code = some vs := mapM_tail code w ws v vs hws
          have hc : code w = some v := by
            have hopt := mapM_get code (w :: ws) (v :: vs) hws 0
            simpa using hopt
          have hspell : spell v = w := sound_LexicalInverse w v hc
          have hle := code_le_iff hc hlo
          have hlt := sound_LexicalOrder w hi v h hc hhi
          have htest : (lo ≤ w && w < hi) = (l ≤ v && v < h) := by
            simp [hle, hlt]
          cases hb : (lo ≤ w && w < hi)
          · have hv : (l ≤ v && v < h) = false := by rw [← htest, hb]
            simp [hb, hv, ih vs htail]
          · have hv : (l ≤ v && v < h) = true := by rw [← htest, hb]
            simp [hb, hv, hspell, ih vs htail]

end TinylambOpt.ResearchProofs.Compression.TemporalText
