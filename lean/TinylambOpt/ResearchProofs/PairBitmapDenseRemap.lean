import TinylambOpt.ResearchGoals.PairBitmapDenseRemap

namespace TinylambOpt.ResearchProofs.PairBitmapDenseRemap

open TinylambOpt.ResearchGoals.PairBitmapDenseRemap

/-! ### Basic facts about `normPair` and list membership -/

private theorem normPair_self (s : Bool) (p : Int × Int) : normPair s (normPair s p) = p := by
  cases s <;> rfl

private theorem normPair_eq_iff (s : Bool) (a b : Int × Int) :
    normPair s a = normPair s b ↔ a = b := by
  constructor
  · intro h
    have hh := congrArg (normPair s) h
    simpa [normPair_self] using hh
  · intro h
    rw [h]

private theorem contains_eq_true_iff (l : List (Int × Int)) (a : Int × Int) :
    l.contains a = true ↔ ∃ x ∈ l, x = a := by
  rw [List.contains_eq_mem]
  simp

private theorem contains_norm_iff (s : Bool) (l : List (Int × Int)) (q : Int × Int) :
    l.contains q = true ↔ ∃ p ∈ l, normPair s p = normPair s q := by
  rw [contains_eq_true_iff]
  constructor
  · rintro ⟨p, hp, rfl⟩
    exact ⟨p, hp, rfl⟩
  · rintro ⟨p, hp, h⟩
    exact ⟨p, hp, (normPair_eq_iff s p q).mp h⟩

/-! ### Minimum / maximum of the normalized inner values -/

private def stepMin (acc : Option Int) (v : Int) : Option Int :=
  match acc with
  | none => some v
  | some m => some (if v < m then v else m)

private def stepMax (acc : Option Int) (v : Int) : Option Int :=
  match acc with
  | none => some v
  | some m => some (if m < v then v else m)

private theorem foldMin_spec (l : List (Int × Int)) (s : Bool) (a : Int) :
    ∃ m, l.foldl (fun acc p => stepMin acc (normPair s p).2) (some a) = some m ∧
      m ≤ a ∧ ∀ p ∈ l, m ≤ (normPair s p).2 := by
  induction l generalizing a with
  | nil =>
      exact ⟨a, rfl, by omega, by simp⟩
  | cons p t ih =>
      let a' := if (normPair s p).2 < a then (normPair s p).2 else a
      rcases ih a' with ⟨m, hm, hle_init, hle_all⟩
      refine ⟨m, ?_, ?_, ?_⟩
      · rw [List.foldl_cons]
        rw [show stepMin (some a) (normPair s p).2 = some a' by simp [stepMin, a']]
        exact hm
      · have ha' : a' ≤ a := by
          by_cases h : (normPair s p).2 < a <;> simp [a', h] <;> omega
        omega
      · intro r hr
        simp only [List.mem_cons] at hr
        rcases hr with hr_self | hr
        · subst r
          have ha' : a' ≤ (normPair s p).2 := by
            by_cases h : (normPair s p).2 < a <;> simp [a', h] <;> omega
          omega
        · exact hle_all r hr

private theorem foldMax_spec (l : List (Int × Int)) (s : Bool) (a : Int) :
    ∃ m, l.foldl (fun acc p => stepMax acc (normPair s p).2) (some a) = some m ∧
      a ≤ m ∧ ∀ p ∈ l, (normPair s p).2 ≤ m := by
  induction l generalizing a with
  | nil =>
      exact ⟨a, rfl, by omega, by simp⟩
  | cons p t ih =>
      let a' := if a < (normPair s p).2 then (normPair s p).2 else a
      rcases ih a' with ⟨m, hm, hge_init, hge_all⟩
      refine ⟨m, ?_, ?_, ?_⟩
      · rw [List.foldl_cons]
        rw [show stepMax (some a) (normPair s p).2 = some a' by simp [stepMax, a']]
        exact hm
      · have ha' : a ≤ a' := by
          by_cases h : a < (normPair s p).2 <;> simp [a', h] <;> omega
        omega
      · intro r hr
        simp only [List.mem_cons] at hr
        rcases hr with hr_self | hr
        · subst r
          have ha' : (normPair s p).2 ≤ a' := by
            by_cases h : a < (normPair s p).2 <;> simp [a', h] <;> omega
          omega
        · exact hge_all r hr

private theorem innerMin_cons_fold (p : Int × Int) (t : List (Int × Int)) (s : Bool) :
    innerMin? (p :: t) s =
      t.foldl (fun acc z => stepMin acc (normPair s z).2) (some (normPair s p).2) := by
  rfl

private theorem innerMax_cons_fold (p : Int × Int) (t : List (Int × Int)) (s : Bool) :
    innerMax? (p :: t) s =
      t.foldl (fun acc z => stepMax acc (normPair s z).2) (some (normPair s p).2) := by
  rfl

private theorem innerMin_some (p : Int × Int) (t : List (Int × Int)) (s : Bool) :
    ∃ mn, innerMin? (p :: t) s = some mn := by
  rcases foldMin_spec t s (normPair s p).2 with ⟨m, hm, _, _⟩
  exact ⟨m, by rw [innerMin_cons_fold, hm]⟩

private theorem innerMax_some (p : Int × Int) (t : List (Int × Int)) (s : Bool) :
    ∃ mx, innerMax? (p :: t) s = some mx := by
  rcases foldMax_spec t s (normPair s p).2 with ⟨m, hm, _, _⟩
  exact ⟨m, by rw [innerMax_cons_fold, hm]⟩

private theorem innerMin_le (p : Int × Int) (t : List (Int × Int)) (s : Bool) (mn : Int)
    (h : innerMin? (p :: t) s = some mn) :
    ∀ r ∈ (p :: t), mn ≤ (normPair s r).2 := by
  rw [innerMin_cons_fold] at h
  rcases foldMin_spec t s (normPair s p).2 with ⟨m, hm, hle_init, hle_all⟩
  have hm_eq : m = mn := by
    rw [hm] at h
    exact Option.some.inj h
  subst mn
  intro r hr
  simp only [List.mem_cons] at hr
  rcases hr with rfl | hr
  · exact hle_init
  · exact hle_all r hr

private theorem innerMax_ge (p : Int × Int) (t : List (Int × Int)) (s : Bool) (mx : Int)
    (h : innerMax? (p :: t) s = some mx) :
    ∀ r ∈ (p :: t), (normPair s r).2 ≤ mx := by
  rw [innerMax_cons_fold] at h
  rcases foldMax_spec t s (normPair s p).2 with ⟨m, hm, hge_init, hge_all⟩
  have hm_eq : m = mx := by
    rw [hm] at h
    exact Option.some.inj h
  subst mx
  intro r hr
  simp only [List.mem_cons] at hr
  rcases hr with rfl | hr
  · exact hge_init
  · exact hge_all r hr

/-! ### Dense outer keys: `findIdx?` gets the unique stored key -/

private theorem foldl_congr_step {α β : Type} (l : List α) (f g : β → α → β)
    (h : ∀ b a, f b a = g b a) (init : β) : l.foldl f init = l.foldl g init := by
  induction l generalizing init with
  | nil => rfl
  | cons a t ih =>
      rw [List.foldl_cons, List.foldl_cons, h, ih]

private theorem findIdx?_getElem {l : List Int} {a : Int} {i : Nat}
    (h : l.findIdx? (fun x => x == a) = some i) :
    l[i]'((List.findIdx?_eq_some_iff_findIdx_eq.mp h).1) = a := by
  have hlt : i < l.length := (List.findIdx?_eq_some_iff_findIdx_eq.mp h).1
  have hf : l.findIdx (fun x => x == a) = i := (List.findIdx?_eq_some_iff_findIdx_eq.mp h).2
  have hw : l.findIdx (fun x => x == a) < l.length := by rw [hf]; exact hlt
  have hp := List.findIdx_getElem (xs := l) (p := fun x => x == a) (w := hw)
  have hp' : (l[i]'hlt == a) = true := by
    simpa [hf, hw] using hp
  simpa using hp'

private theorem findIdx?_getElem? {l : List Int} {a : Int} {i : Nat}
    (h : l.findIdx? (fun x => x == a) = some i) : l[i]? = some a := by
  rw [List.getElem?_eq_getElem (List.findIdx?_eq_some_iff_findIdx_eq.mp h).1]
  exact congrArg some (findIdx?_getElem h)

/-! ### Bit-level facts -/

private theorem shift_mod_two_eq_testBit (mask k : Nat) :
    (mask >>> k % 2 == 1) = mask.testBit k := by
  rw [show (mask >>> k % 2 == 1) = (mask >>> k).testBit 0 by
    rw [Nat.testBit_zero, Bool.beq_eq_decide_eq]]
  rw [Nat.testBit_shiftRight]
  simp

private theorem foldl_or_option_testBit {α : Type} (l : List α) (f : α → Option Nat)
    (idx : Nat) (acc : Nat) :
    (l.foldl (fun m a => match f a with | some b => m ||| (1 <<< b) | none => m) acc).testBit idx =
      (acc.testBit idx || decide (∃ a ∈ l, f a = some idx)) := by
  induction l generalizing acc with
  | nil => simp
  | cons a t ih =>
      rw [List.foldl_cons]
      cases h : f a with
      | none =>
          simp only
          rw [ih acc]
          simp [h]
      | some b =>
          simp only
          rw [ih (acc ||| (1 <<< b))]
          by_cases hb : b = idx
          · subst b
            rw [Nat.testBit_or, Nat.one_shiftLeft, Nat.testBit_two_pow]
            simp [h]
          · have hshift : (1 <<< b).testBit idx = false := by
              rw [Nat.one_shiftLeft, Nat.testBit_two_pow]
              simp [hb]
            rw [Nat.testBit_or, hshift]
            simp [h, hb]

private theorem bitIndex_inj {mn mx : Int} {span : Nat} (hspan : span = (mx - mn).toNat + 1)
    {oi1 oi2 : Nat} {iv1 iv2 : Int}
    (hlo1 : mn ≤ iv1) (hhi1 : iv1 ≤ mx) (hlo2 : mn ≤ iv2) (hhi2 : iv2 ≤ mx)
    (h : bitIndex oi1 mn span iv1 = bitIndex oi2 mn span iv2) :
    oi1 = oi2 ∧ iv1 = iv2 := by
  let d1 := (iv1 - mn).toNat
  let d2 := (iv2 - mn).toNat
  have hspanpos : 0 < span := by omega
  have hspan_int : (span : Int) = mx - mn + 1 := by
    rw [hspan]
    have hnn : 0 ≤ mx - mn := by omega
    simp [Int.toNat_of_nonneg hnn]
  have hd1lt : d1 < span := by
    dsimp [d1]
    rw [Int.toNat_lt (by omega : 0 ≤ iv1 - mn)]
    omega
  have hd2lt : d2 < span := by
    dsimp [d2]
    rw [Int.toNat_lt (by omega : 0 ≤ iv2 - mn)]
    omega
  have h' : oi1 * span + d1 = oi2 * span + d2 := by
    simpa [bitIndex, d1, d2] using h
  have hmod1 : (oi1 * span + d1) % span = d1 := Nat.mul_add_mod_of_lt hd1lt
  have hmod2 : (oi2 * span + d2) % span = d2 := Nat.mul_add_mod_of_lt hd2lt
  have hd : d1 = d2 := by
    rw [← hmod1, ← hmod2, h']
  have hdiv1 : (oi1 * span + d1) / span = oi1 := by
    rw [Nat.mul_comm oi1 span, Nat.mul_add_div hspanpos, Nat.div_eq_of_lt hd1lt, Nat.add_zero]
  have hdiv2 : (oi2 * span + d2) / span = oi2 := by
    rw [Nat.mul_comm oi2 span, Nat.mul_add_div hspanpos, Nat.div_eq_of_lt hd2lt, Nat.add_zero]
  have ho : oi1 = oi2 := by
    have hh := congrArg (fun n : Nat => n / span) h'
    rw [hdiv1, hdiv2] at hh
    exact hh
  refine ⟨ho, ?_⟩
  have h1nn : 0 ≤ iv1 - mn := by omega
  have h2nn : 0 ≤ iv2 - mn := by omega
  have hcast := congrArg (fun n : Nat => (n : Int)) hd
  rw [Int.toNat_of_nonneg h1nn, Int.toNat_of_nonneg h2nn] at hcast
  omega

/-! ### Main soundness proof -/

theorem sound : TinylambOpt.ResearchGoals.PairBitmapDenseRemap.Goal := by
  intro pairs swap q
  cases pairs with
  | nil =>
      simp [probePairBitmap, innerMin?, innerMax?]
  | cons p t =>
      rcases innerMin_some p t swap with ⟨mn, hmn⟩
      rcases innerMax_some p t swap with ⟨mx, hmx⟩
      let okeys := outerKeys (p :: t) swap
      let span := (mx - mn).toNat + 1
      let f : Int × Int → Option Nat := fun r =>
        match okeys.findIdx? (fun o => o == (normPair swap r).1) with
        | some oi => some (bitIndex oi mn span (normPair swap r).2)
        | none => none
      let mask := (p :: t).foldl
        (fun m r => match f r with | some b => m ||| (1 <<< b) | none => m) 0
      have hbuild : buildBitmap (p :: t) swap = mask := by
        unfold buildBitmap
        rw [hmn, hmx]
        change (p :: t).foldl
            (fun m r => match okeys.findIdx? (fun x => x == (normPair swap r).1) with
              | some oi => m ||| (1 <<< bitIndex oi mn span (normPair swap r).2)
              | none => m) 0 = mask
        dsimp [mask, okeys, span]
        have hinit :
            (match okeys.findIdx? (fun x => x == (normPair swap p).1) with
              | some oi => 0 ||| (1 <<< bitIndex oi mn span (normPair swap p).2)
              | none => 0) =
            (match f p with | some b => 0 ||| (1 <<< b) | none => 0) := by
          cases hfp : okeys.findIdx? (fun x => x == (normPair swap p).1) <;> simp [f, hfp]
        rw [hinit]
        apply foldl_congr_step
        intro m r
        cases hfr : okeys.findIdx? (fun x => x == (normPair swap r).1) <;> simp [f, hfr, span]
      let oq := (normPair swap q).1
      let iq := (normPair swap q).2
      have hprobe : probePairBitmap (p :: t) swap q =
          match okeys.findIdx? (fun o => o == oq) with
          | none => false
          | some oi => decide (mn ≤ iq ∧ iq ≤ mx) && mask.testBit (bitIndex oi mn span iq) := by
        unfold probePairBitmap
        simp [hmn, hmx, okeys, span, hbuild, oq, iq]
        cases okeys.findIdx? (fun o => o == oq) with
        | none => simp
        | some _ =>
            simp [shift_mod_two_eq_testBit]
      apply Bool.eq_iff_iff.mpr
      constructor
      · intro hpr
        have hpr' : (match okeys.findIdx? (fun o => o == oq) with
            | none => false
            | some oi => decide (mn ≤ iq ∧ iq ≤ mx) && mask.testBit (bitIndex oi mn span iq)) = true := by
          simpa [hprobe] using hpr
        cases hfindq : okeys.findIdx? (fun o => o == oq) with
        | none =>
            simp [hfindq] at hpr'
        | some oiq =>
            by_cases hrange : mn ≤ iq ∧ iq ≤ mx
            · have hrangeDec : decide (mn ≤ iq ∧ iq ≤ mx) = true := by simp [hrange]
              have hbit : mask.testBit (bitIndex oiq mn span iq) = true := by
                simpa [hfindq, hrangeDec] using hpr'
              rw [show mask = (p :: t).foldl
                  (fun m r => match f r with | some b => m ||| (1 <<< b) | none => m) 0 from rfl] at hbit
              rw [foldl_or_option_testBit] at hbit
              have hex : ∃ r ∈ (p :: t), f r = some (bitIndex oiq mn span iq) := by
                simpa using hbit
              rcases hex with ⟨r, hr, hfr⟩
              dsimp [f] at hfr
              split at hfr
              · rename_i oir hfindr
                have hidx : bitIndex oir mn span (normPair swap r).2 =
                    bitIndex oiq mn span iq := Option.some.inj hfr
                have hlo_r := innerMin_le p t swap mn hmn r hr
                have hhi_r := innerMax_ge p t swap mx hmx r hr
                have hinj := bitIndex_inj (show span = (mx - mn).toNat + 1 from rfl)
                  hlo_r hhi_r hrange.1 hrange.2 hidx
                have hv_eq : (normPair swap r).2 = iq := by simpa [iq] using hinj.2
                have hr_outer? := findIdx?_getElem? hfindr
                have hq_outer? := findIdx?_getElem? hfindq
                have ho_eq : (normPair swap r).1 = oq := by
                  have hh := hr_outer?
                  rw [hinj.1] at hh
                  rw [hq_outer?] at hh
                  exact (Option.some.inj hh).symm
                exact (contains_norm_iff swap (p :: t) q).mpr ⟨r, hr, by
                  ext
                  · exact ho_eq
                  · exact hv_eq⟩
              · simp at hfr
            · have hrangeDec : decide (mn ≤ iq ∧ iq ≤ mx) = false := by simp [hrange]
              simp [hfindq, hrangeDec] at hpr'
      · intro hcont
        rcases (contains_norm_iff swap (p :: t) q).mp hcont with ⟨r, hr, hnorm⟩
        have hqeq : r = q := (normPair_eq_iff swap r q).mp hnorm
        subst q
        have hmemq : oq ∈ okeys := by
          unfold okeys outerKeys
          rw [List.mem_eraseDups]
          rw [List.mem_map]
          exact ⟨r, hr, rfl⟩
        have hany : okeys.any (fun o => o == oq) = true := by
          rw [List.any_eq_true]
          exact ⟨oq, hmemq, by simp⟩
        have hisSome : (okeys.findIdx? (fun o => o == oq)).isSome = true := by
          rw [List.findIdx?_isSome]
          exact hany
        cases hfindq : okeys.findIdx? (fun o => o == oq) with
        | none =>
            simp [hfindq] at hisSome
        | some oiq =>
            have hrange : mn ≤ iq ∧ iq ≤ mx := by
              constructor
              · simpa [iq] using innerMin_le p t swap mn hmn r hr
              · simpa [iq] using innerMax_ge p t swap mx hmx r hr
            have hbit : mask.testBit (bitIndex oiq mn span iq) = true := by
              rw [show mask = (p :: t).foldl
                  (fun m r => match f r with | some b => m ||| (1 <<< b) | none => m) 0 from rfl]
              rw [foldl_or_option_testBit]
              have hex : ∃ z ∈ (p :: t), f z = some (bitIndex oiq mn span iq) := by
                refine ⟨r, hr, ?_⟩
                dsimp [f]
                rw [hfindq]
              rw [show decide (∃ z ∈ (p :: t), f z = some (bitIndex oiq mn span iq)) = true from
                decide_eq_true_eq.mpr hex]
              simp
            simpa [hprobe, hfindq, hrange] using hbit

end TinylambOpt.ResearchProofs.PairBitmapDenseRemap
