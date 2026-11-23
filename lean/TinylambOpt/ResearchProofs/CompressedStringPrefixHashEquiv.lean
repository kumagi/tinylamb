import TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv

namespace TinylambOpt.ResearchProofs.CompressedStringPrefixHashEquiv

set_option maxRecDepth 100000
set_option maxHeartbeats 2000000

private theorem beWord7Go_nil_zero (rem : Nat) :
    TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go rem [] 0 = 0 := by
  induction rem with
  | zero => simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
  | succ k ih => simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go, ih]

private theorem beWord7Go_acc (rem : Nat) (s : List UInt8) (acc : Nat) :
    TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go rem s acc =
      acc * 256 ^ rem +
        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go rem s 0 := by
  induction rem generalizing s acc with
  | zero => simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
  | succ k ih =>
      cases s with
      | nil =>
          simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
          rw [ih]
          congr 1
          rw [Nat.pow_succ]
          ac_rfl
      | cons a as =>
          simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
          rw [ih as (acc * 256 + a.toNat)]
          rw [ih as a.toNat]
          rw [Nat.add_mul, Nat.pow_succ]
          ac_rfl

private theorem beWord7Go_succ_cons (k : Nat) (a : UInt8) (as : List UInt8) :
    TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go (k+1) (a::as) 0 =
      a.toNat * 256^k +
        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 := by
  simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
  rw [beWord7Go_acc]

private theorem beWord7Go_lt_pow (rem : Nat) (s : List UInt8) (hs : s.length ≤ rem) :
    TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go rem s 0 < 256 ^ rem := by
  induction rem generalizing s with
  | zero =>
      cases s with
      | nil => simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
      | cons a as => simp at hs
  | succ k ih =>
      cases s with
      | nil =>
          simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
          exact Nat.lt_trans (ih [] (by simp))
            (Nat.pow_lt_pow_right (by decide) (Nat.lt_succ_self k))
      | cons a as =>
          have has : as.length ≤ k := by simpa using hs
          have ha : a.toNat < 256 := by
            have h := a.toNat_lt
            have h2 : (2:Nat)^8 = 256 := by decide
            omega
          have hr := ih as has
          rw [beWord7Go_succ_cons]
          rw [Nat.pow_succ]
          have hstep :
              a.toNat * 256^k +
                  TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 <
                (a.toNat + 1) * 256^k := by
            rw [Nat.succ_mul]
            exact Nat.add_lt_add_left hr (a.toNat * 256^k)
          have hle : (a.toNat + 1) * 256^k ≤ 256 * 256^k :=
            Nat.mul_le_mul_right _ (Nat.succ_le_of_lt ha)
          rw [← Nat.mul_comm 256]
          exact Nat.lt_of_lt_of_le hstep hle

private def key (rem : Nat) (s : List UInt8) : Nat :=
  TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go rem s 0 * 256 + s.length

private theorem key_succ_cons_eq (k : Nat) (a : UInt8) (as : List UInt8) :
    key (k+1) (a::as) =
      (a.toNat * 256^k +
        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0) * 256 +
          (as.length + 1) := by
  unfold key
  rw [beWord7Go_succ_cons]
  simp

private theorem key_mod (rem : Nat) (hrem : rem ≤ 7) (s : List UInt8) (hs : s.length ≤ rem) :
    key rem s % 256 = s.length := by
  unfold key
  have hlen : s.length < 256 := by omega
  rw [Nat.mul_comm
    (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go rem s 0) 256,
    Nat.mul_add_mod, Nat.mod_eq_of_lt hlen]

private theorem mul_add_lt_add (a b p r s : Nat) (hab : a + 1 ≤ b) (hr : r < p) :
    a * p + r < b * p + s := by
  have h1 : a * p + r < a * p + p := Nat.add_lt_add_left hr (a*p)
  rw [← Nat.succ_mul] at h1
  have h2 : (a+1)*p ≤ b*p := Nat.mul_le_mul_right p hab
  exact Nat.lt_of_lt_of_le h1 (Nat.le_trans h2 (Nat.le_add_right _ _))

private theorem shift_key_lt (a p ra rb n m : Nat) :
    (a*p + ra)*256 + (n+1) < (a*p + rb)*256 + (m+1) ↔ ra*256+n < rb*256+m := by
  rw [Nat.add_mul, Nat.add_mul]
  omega

private theorem key_inj (rem : Nat) (hrem : rem ≤ 7) :
    ∀ {s t : List UInt8}, s.length ≤ rem → t.length ≤ rem →
      key rem s = key rem t → s = t := by
  induction rem with
  | zero =>
      intro s t hs ht h
      cases s with
      | nil =>
          cases t with
          | nil => rfl
          | cons b bs => simp at ht
      | cons a as => simp at hs
  | succ k ih =>
      intro s t hs ht h
      have hk : k ≤ 7 := by omega
      have hlen : s.length = t.length := by
        have hmod := congrArg (fun z => z % 256) h
        rw [key_mod (k+1) (by omega) s hs, key_mod (k+1) (by omega) t ht] at hmod
        exact hmod
      cases s with
      | nil =>
          cases t with
          | nil => rfl
          | cons b bs => simp at hlen
      | cons a as =>
          cases t with
          | nil => simp at hlen
          | cons b bs =>
              have has : as.length ≤ k := by simpa using hs
              have hbs : bs.length ≤ k := by simpa using ht
              have hlenTail : as.length = bs.length := by simpa using hlen
              unfold key at h
              rw [beWord7Go_succ_cons, beWord7Go_succ_cons] at h
              have hmain :
                  a.toNat * 256^k +
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 =
                    b.toNat * 256^k +
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 := by
                omega
              have hab : a.toNat = b.toNat := by
                rcases Nat.lt_trichotomy a.toNat b.toNat with hlt | heq | hgt
                · have hr :
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 <
                        256^k := beWord7Go_lt_pow k as has
                  have contra :=
                    mul_add_lt_add a.toNat b.toNat (256^k)
                      (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0)
                      (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0)
                      (Nat.succ_le_of_lt hlt) hr
                  omega
                · exact heq
                · have hr :
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 <
                        256^k := beWord7Go_lt_pow k bs hbs
                  have contra :=
                    mul_add_lt_add b.toNat a.toNat (256^k)
                      (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0)
                      (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0)
                      (Nat.succ_le_of_lt hgt) hr
                  omega
              have hraeq :
                  TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 =
                    TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 := by
                have hm := hmain
                rw [hab] at hm
                exact Nat.add_left_cancel hm
              have habs : key k as = key k bs := by
                unfold key
                rw [hraeq, hlenTail]
              have htails : as = bs := ih hk has hbs habs
              rw [UInt8.toNat_inj.mp hab, htails]

private theorem key_lt_iff (rem : Nat) (hrem : rem ≤ 7) :
    ∀ {s t : List UInt8}, s.length ≤ rem → t.length ≤ rem →
      (key rem s < key rem t ↔
        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt s t = true) := by
  induction rem with
  | zero =>
      intro s t hs ht
      cases s with
      | nil =>
          cases t with
          | nil => simp [key, TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt,
              TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go]
          | cons b bs => simp at ht
      | cons a as => simp at hs
  | succ k ih =>
      intro s t hs ht
      have hk : k ≤ 7 := by omega
      cases s with
      | nil =>
          cases t with
          | nil => simp [key, TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt,
              beWord7Go_nil_zero]
          | cons b bs =>
              have hkeyNil : key (k+1) ([] : List UInt8) = 0 := by
                simp [key, beWord7Go_nil_zero]
              have hpos : 0 < key (k+1) (b::bs) := by
                rw [key_succ_cons_eq]
                have hp : 0 < bs.length + 1 := Nat.succ_pos bs.length
                exact Nat.lt_of_lt_of_le hp
                  (Nat.le_add_left (bs.length+1)
                    ((b.toNat * 256^k +
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0) * 256))
              constructor
              · intro _; simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt]
              · intro _; rw [hkeyNil]; exact hpos
      | cons a as =>
          cases t with
          | nil =>
              have hkeyNil : key (k+1) ([] : List UInt8) = 0 := by
                simp [key, beWord7Go_nil_zero]
              have hpos : 0 < key (k+1) (a::as) := by
                rw [key_succ_cons_eq]
                have hp : 0 < as.length + 1 := Nat.succ_pos as.length
                exact Nat.lt_of_lt_of_le hp
                  (Nat.le_add_left (as.length+1)
                    ((a.toNat * 256^k +
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0) * 256))
              constructor
              · intro hlt
                rw [hkeyNil] at hlt
                omega
              · intro htrue
                cases htrue
          | cons b bs =>
              have has : as.length ≤ k := by simpa using hs
              have hbs : bs.length ≤ k := by simpa using ht
              rcases Nat.lt_trichotomy a.toNat b.toNat with hlt | heq | hgt
              · have hab : a.toNat + 1 ≤ b.toNat := Nat.succ_le_of_lt hlt
                have hr :
                    TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 <
                      256^k := beWord7Go_lt_pow k as has
                have hbig :
                    a.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 <
                      b.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 :=
                  mul_add_lt_add a.toNat b.toNat (256^k)
                    (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0)
                    (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0)
                    hab hr
                have hsle :
                    a.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 + 1 ≤
                      b.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 :=
                  Nat.succ_le_of_lt hbig
                have hmul := Nat.mul_le_mul_right 256 hsle
                have hstep :
                    (a.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0) * 256 +
                        256 ≤
                      (b.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0) * 256 := by
                  simpa [Nat.add_mul] using hmul
                have hn : as.length + 1 ≤ 255 := by omega
                have hltkey : key (k+1) (a::as) < key (k+1) (b::bs) := by
                  rw [key_succ_cons_eq, key_succ_cons_eq]
                  omega
                have hltU : a < b := hlt
                constructor
                · intro _; simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt, hltU]
                · intro _; exact hltkey
              · have habU : a = b := UInt8.toNat_inj.mp heq
                have hred :
                    key (k+1) (a::as) < key (k+1) (b::bs) ↔ key k as < key k bs := by
                  rw [key_succ_cons_eq, key_succ_cons_eq, habU]
                  unfold key
                  exact shift_key_lt b.toNat (256^k)
                    (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0)
                    (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0)
                    as.length bs.length
                constructor
                · intro hklt
                  have hlex :
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt as bs = true :=
                    (ih hk has hbs).mp (hred.mp hklt)
                  simpa [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt, habU] using hlex
                · intro hlex
                  have hlexTail :
                      TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt as bs = true := by
                    simpa [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt, habU] using hlex
                  exact hred.mpr ((ih hk has hbs).mpr hlexTail)
              · have hab : b.toNat + 1 ≤ a.toNat := Nat.succ_le_of_lt hgt
                have hr :
                    TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 <
                      256^k := beWord7Go_lt_pow k bs hbs
                have hbig :
                    b.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 <
                      a.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 :=
                  mul_add_lt_add b.toNat a.toNat (256^k)
                    (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0)
                    (TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0)
                    hab hr
                have hsle :
                    b.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0 + 1 ≤
                      a.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0 :=
                  Nat.succ_le_of_lt hbig
                have hmul := Nat.mul_le_mul_right 256 hsle
                have hstep :
                    (b.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k bs 0) * 256 +
                        256 ≤
                      (a.toNat * 256^k +
                        TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7Go k as 0) * 256 := by
                  simpa [Nat.add_mul] using hmul
                have hn : bs.length + 1 ≤ 255 := by omega
                have hltkey : key (k+1) (b::bs) < key (k+1) (a::as) := by
                  rw [key_succ_cons_eq, key_succ_cons_eq]
                  omega
                have hnot : ¬ key (k+1) (a::as) < key (k+1) (b::bs) :=
                  Nat.not_lt_of_ge (Nat.le_of_lt hltkey)
                have hgtU : b < a := hgt
                constructor
                · intro hklt
                  exact False.elim (hnot hklt)
                · intro htrue
                  simp [TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.byteLexLt, hgtU] at htrue
                  exact False.elim ((Nat.lt_asymm hgtU) htrue)

theorem sound : TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.Goal := by
  constructor
  · intro s1 s2 hs1 hs2
    constructor
    · constructor
      · intro hEq
        have hk : key 7 s1 = key 7 s2 := by
          unfold TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.pack7 at hEq
          unfold TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7 at hEq
          unfold key
          exact hEq
        exact key_inj 7 (by decide) hs1 hs2 hk
      · intro hst
        subst s2
        rfl
    · constructor
      · intro hlt
        have hk : key 7 s1 < key 7 s2 := by
          unfold TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.pack7 at hlt
          unfold TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7 at hlt
          unfold key
          exact hlt
        exact (key_lt_iff 7 (by decide) hs1 hs2).mp hk
      · intro hlex
        have hk : key 7 s1 < key 7 s2 := (key_lt_iff 7 (by decide) hs1 hs2).mpr hlex
        unfold key at hk
        unfold TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.pack7
        unfold TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.beWord7
        exact hk
  · constructor <;> decide

end TinylambOpt.ResearchProofs.CompressedStringPrefixHashEquiv
