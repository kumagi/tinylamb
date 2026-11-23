import TinylambOpt.ResearchGoals.ExecutionStrategies.IndexPreimages

namespace TinylambOpt.ResearchProofs.Wave7IndexPreimages

open TinylambOpt.ResearchGoals.IndexPreimages

private theorem tdiv_of_nonneg {x d : Int} (hx : 0 ≤ x) (hd : 0 < d) : x.tdiv d = x / d := by
  cases x with
  | ofNat xm =>
      cases d with
      | ofNat dm => rfl
      | negSucc dm => omega
  | negSucc xm =>
      have : ¬ (0 ≤ (Int.negSucc xm : Int)) := by omega
      exact False.elim (this hx)


private theorem tdiv_of_neg {x d : Int} (hx : x < 0) (hd : 0 < d) : x.tdiv d = -((-x)/d) := by
  cases x with
  | ofNat xm =>
      have hnon : 0 ≤ (Int.ofNat xm : Int) := Int.natCast_nonneg xm
      omega
  | negSucc xm =>
      cases d with
      | ofNat dm => rfl
      | negSucc dm => omega


private theorem ediv_eq_iff_range (x d k : Int) (hx : 0 ≤ x) (hd : 0 < d) :
    x / d = k ↔ k*d ≤ x ∧ x < k*d + d := by
  constructor
  · intro h
    subst k
    constructor
    · have hle : (x/d)*d ≤ x := (Int.le_ediv_iff_mul_le hd).mp (by omega)
      simpa [Int.mul_comm] using hle
    · have hlt : x/d < x/d + 1 := by omega
      have hmul := (Int.ediv_lt_iff_lt_mul hd).mp hlt
      rw [Int.add_mul] at hmul
      omega
  · intro h
    apply Int.le_antisymm
    · have hlt : x < (k+1)*d := by
        rw [Int.add_mul]
        omega
      have hlt' : x/d < k+1 := (Int.ediv_lt_iff_lt_mul hd).mpr hlt
      omega
    · exact (Int.le_ediv_iff_mul_le hd).mpr (by simpa [Int.mul_comm] using h.1)


private theorem div_nonneg (x d : Int) (hx : 0 ≤ x) (hd : 0 < d) : 0 ≤ x/d := by
  exact (Int.le_ediv_iff_mul_le hd).mpr (by simpa using hx)


private theorem natAbs_nonneg_eq {v : Int} (hv : 0 ≤ v) : (v.natAbs : Int) = v :=
  Int.natAbs_of_nonneg hv


private theorem natAbs_neg_eq {v : Int} (hv : v < 0) : (v.natAbs : Int) = -v := by
  have h := Int.natAbs_of_nonneg (a := -v) (by omega)
  simpa [Int.natAbs_neg] using h


private theorem parity_succ_eq (q : Int) : (q+1) % 2 = 0 ↔ q % 2 != 0 := by
  have hq0 : 0 ≤ q % 2 := Int.emod_nonneg q (by omega)
  have hq1 : q % 2 < 2 := Int.emod_lt_of_pos q (by omega)
  have hrel : (q+1) % 2 = (q % 2 + 1) % 2 := by
    rw [Int.add_emod]
    simp
  rw [hrel]
  constructor
  · intro h
    simp
    intro hz
    rw [hz] at h
    have h1 : (1:Int) % 2 = 1 := by decide
    simp [h1] at h
  · intro h
    have hz : q % 2 = 1 := by
      have h' : q % 2 ≠ 0 := by simpa using h
      omega
    rw [hz]
    have h2 : (2:Int) % 2 = 0 := by decide
    simp [h2]


private theorem dvd_of_emod_eq_one {m a inv : Int} (hm : 1 < m) (h : (a*inv)%m = 1) : m ∣ a*inv - 1 := by
  have h1 : (1 : Int) % m = 1 := Int.emod_eq_of_lt (by omega) (by omega)
  have hz : (a*inv - 1) % m = 0 :=
    (Int.emod_eq_emod_iff_emod_sub_eq_zero (m:=a*inv) (n:=m) (k:=1)).mp (by simpa [h1] using h)
  exact (Int.dvd_iff_emod_eq_zero).mpr hz


private theorem dvd_emod_sub_self (m n : Int) : m ∣ n % m - n := by
  apply (Int.dvd_iff_emod_eq_zero).mpr
  rw [← Int.emod_eq_emod_iff_emod_sub_eq_zero]
  exact Int.emod_emod n m


private theorem emod_eq_of_dvd_sub (m a b : Int) (h : m ∣ a - b) : a % m = b % m := by
  exact (Int.emod_eq_emod_iff_emod_sub_eq_zero (m:=a) (n:=m) (k:=b)).mpr ((Int.dvd_iff_emod_eq_zero).mp h)


private theorem natAbs_le_iff (v rad : Int) : (v.natAbs : Int) ≤ rad ↔ -rad ≤ v ∧ v ≤ rad := by
  by_cases hv : 0 ≤ v
  · have hvabs : (v.natAbs : Int) = v := Int.natAbs_of_nonneg hv
    rw [hvabs]
    constructor
    · intro h
      constructor <;> omega
    · intro h
      exact h.2
  · have hvabs : (v.natAbs : Int) = -v := by
      have h := Int.natAbs_of_nonneg (a := -v) (by omega)
      simpa [Int.natAbs_neg] using h
    rw [hvabs]
    constructor
    · intro h
      constructor <;> omega
    · intro h
      omega


private theorem le_natAbs_iff (v l : Int) (hl : 0 ≤ l) : l ≤ (v.natAbs : Int) ↔ v ≤ -l ∨ l ≤ v := by
  by_cases hv : 0 ≤ v
  · have hvabs : (v.natAbs : Int) = v := Int.natAbs_of_nonneg hv
    rw [hvabs]
    constructor
    · intro h
      exact Or.inr h
    · intro h
      rcases h with h | h <;> omega
  · have hvabs : (v.natAbs : Int) = -v := by
      have h := Int.natAbs_of_nonneg (a := -v) (by omega)
      simpa [Int.natAbs_neg] using h
    rw [hvabs]
    constructor
    · intro h
      exact Or.inl (by omega)
    · intro h
      rcases h with h | h <;> omega


private theorem perm_cons_append {α : Type} (a : α) (ys zs : List α) :
    (ys ++ a :: zs).Perm (a :: (ys ++ zs)) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have h2 := ih.cons y
      have h3 := List.Perm.swap a y (ys ++ zs)
      exact h2.trans h3


private theorem filter_perm_or {α : Type} (p q : α → Bool)
    (hdisj : ∀ a, ¬ (p a = true ∧ q a = true)) : ∀ xs : List α,
    (xs.filter p ++ xs.filter q).Perm (xs.filter (fun a => p a || q a)) := by
  intro xs
  induction xs with
  | nil => simp
  | cons a xs ih =>
      by_cases hp : p a = true
      · have hq : q a = false := by
          cases hqa : q a with
          | false => rfl
          | true => exact False.elim (hdisj a ⟨hp, hqa⟩)
        simpa [hp, hq] using ih.cons a
      · by_cases hqtrue : q a = true
        · have hswap := perm_cons_append a (xs.filter p) (xs.filter q)
          have h1 := hswap.trans (ih.cons a)
          simpa [hp, hqtrue] using h1
        · have hq : q a = false := by
            cases hqa : q a with
            | false => rfl
            | true => exact False.elim (hqtrue hqa)
          simpa [hp, hq] using ih


private theorem compare_lt_bool (a b : Int) : (compare a b == .lt) = decide (a < b) := by
  by_cases h : a < b
  · have hc : compare a b = .lt := Int.compare_eq_lt.mpr h
    simp [h, hc]
  · simp [h, Int.compare_eq_lt]


private theorem compare_gt_bool (a b : Int) : (compare a b == .gt) = decide (b < a) := by
  by_cases h : b < a
  · have hc : compare a b = .gt := Int.compare_eq_gt.mpr h
    simp [h, hc]
  · simp [h, Int.compare_eq_gt]


private theorem cmpKey_lt_char (a b : Key) : (cmpKey a b == .lt) =
    (match a.1, b.1 with
     | none, none => decide (b.2 < a.2)
     | none, some _ => false
     | some _, none => true
     | some x, some y => decide (x < y || (x == y && b.2 < a.2))) := by
  rcases a with ⟨a1,a2⟩
  rcases b with ⟨b1,b2⟩
  cases a1 with
  | none =>
      cases b1 with
      | none => simp [cmpKey, compare_lt_bool]
      | some byv => simp [cmpKey]
  | some ax =>
      cases b1 with
      | none => simp [cmpKey]
      | some byv =>
          by_cases hxy : ax = byv
          · simp [cmpKey, hxy, compare_lt_bool]
          · simp [cmpKey, hxy, compare_lt_bool]


private theorem cmpKey_gt_char (a b : Key) : (cmpKey a b == .gt) =
    (match a.1, b.1 with
     | none, none => decide (a.2 < b.2)
     | none, some _ => true
     | some _, none => false
     | some x, some y => decide (y < x || (x == y && a.2 < b.2))) := by
  rcases a with ⟨a1,a2⟩
  rcases b with ⟨b1,b2⟩
  cases a1 with
  | none =>
      cases b1 with
      | none => simp [cmpKey, compare_gt_bool]
      | some byv => simp [cmpKey]
  | some ax =>
      cases b1 with
      | none => simp [cmpKey]
      | some byv =>
          by_cases hxy : ax = byv
          · simp [cmpKey, hxy, compare_gt_bool]
          · simp [cmpKey, hxy, compare_gt_bool]


private def peer (cur r : Key) : Bool := r.1 == cur.1 && r.2 < cur.2


private def later (cur r : Key) : Bool :=
  match cur.1, r.1 with
  | some x, some y => decide (x < y)
  | some _, none => true
  | _,_ => false


private theorem peer_after (cur r : Key) (h : peer cur r = true) : cmpKey cur r == .lt := by
  rw [cmpKey_lt_char]
  rcases cur with ⟨c1,c2⟩
  rcases r with ⟨r1,r2⟩
  simp [peer] at h ⊢
  rcases h with ⟨h1, h2⟩
  subst r1
  cases c1 with
  | none => simp [h2]
  | some cx => simp [h2]


private theorem later_after (cur r : Key) (h : later cur r = true) : cmpKey cur r == .lt := by
  rw [cmpKey_lt_char]
  rcases cur with ⟨c1,c2⟩
  rcases r with ⟨r1,r2⟩
  simp [later] at h ⊢
  cases c1 with
  | none => simp at h
  | some cx =>
      cases r1 with
      | none => simp
      | some ry => simp [h]


private theorem after_peer_or_later (cur r : Key) (h : cmpKey cur r == .lt) :
    peer cur r = true ∨ later cur r = true := by
  rw [cmpKey_lt_char] at h
  rcases cur with ⟨c1,c2⟩
  rcases r with ⟨r1,r2⟩
  simp [peer, later] at h ⊢
  cases c1 with
  | none =>
      cases r1 with
      | none =>
          left
          exact ⟨rfl, of_decide_eq_true h⟩
      | some ry => simp at h
  | some cx =>
      cases r1 with
      | none => right; rfl
      | some ry =>
          by_cases hxy : cx = ry
          · subst ry
            simp at h
            left
            exact ⟨rfl, h⟩
          · right
            simpa [hxy] using h


private theorem peer_later_disjoint (cur r : Key) : ¬ (peer cur r = true ∧ later cur r = true) := by
  rcases cur with ⟨c1,c2⟩
  rcases r with ⟨r1,r2⟩
  simp [peer, later]
  cases c1 with
  | none =>
      cases r1 with
      | none => simp
      | some ry => simp
  | some cx =>
      cases r1 with
      | none => simp
      | some ry =>
          intro h1 h2
          have h1' : ry = cx := by injection h1
          simp [h1']


private theorem no_peer_after_later (cur a b : Key)
    (hla : later cur a = true) (hab : (cmpKey a b != .gt) = true) : peer cur b = false := by
  have hnotgt : ¬ ((cmpKey a b == .gt) = true) := by
    simpa using hab
  rw [cmpKey_gt_char] at hnotgt
  unfold later at hla
  unfold peer
  rcases cur with ⟨c1,c2⟩
  rcases a with ⟨a1,a2⟩
  rcases b with ⟨b1,b2⟩
  simp at hla hnotgt ⊢
  cases c1 with
  | none =>
      cases a1 with
      | none => simp at hla
      | some ay => simp at hla
  | some cx =>
      cases a1 with
      | none =>
          cases b1 with
          | none =>
              simp at hnotgt
              simp
          | some byv => simp at hnotgt
      | some ay =>
          have hlax : cx < ay := by simpa using hla
          cases b1 with
          | none => simp
          | some byv =>
              by_cases hab_eq : ay = byv
              · subst byv
                simp at hnotgt
                intro heq
                have heq' : ay = cx := by injection heq
                omega
              · by_cases hcxcy : cx = ay
                · subst ay
                  omega
                · simp [hcxcy] at hnotgt
                  intro heq
                  have heq' : byv = cx := by injection heq
                  omega


private theorem filter_nil_of_false {α : Type} (p : α → Bool) {xs : List α}
    (h : ∀ x ∈ xs, p x = false) : xs.filter p = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ y ∈ xs, p y = false := by intro y hy; exact h y (by simp [hy])
      simp [List.filter_cons, hx, ih ht]


private theorem morton_top (n x y : Nat) :
    morton (n+1) x y = morton n x y + ((x.testBit n).toNat + 2*(y.testBit n).toNat) * 4^n := by
  simp [morton, List.range_succ]


private theorem pow4_succ (n : Nat) : 4^(n+1) = 4^n * 4 := by
  rw [show n+1 = Nat.succ n by omega]
  rw [Nat.pow_succ]


private theorem morton_bound (b x y : Nat) : morton b x y ≤ 4^b - 1 := by
  induction b with
  | zero => simp [morton]
  | succ b ih =>
      rw [morton_top]
      rw [pow4_succ]
      have hbitx : (x.testBit b).toNat ≤ 1 := by
        cases x.testBit b <;> simp
      have hbity : (y.testBit b).toNat ≤ 1 := by
        cases y.testBit b <;> simp
      have hdig : (x.testBit b).toNat + 2*(y.testBit b).toNat ≤ 3 := by omega
      have hmul : ((x.testBit b).toNat + 2*(y.testBit b).toNat) * 4^b ≤ 3 * 4^b :=
        Nat.mul_le_mul_right _ hdig
      rw [show 4^b * 4 = 4^b + 3*4^b by omega]
      omega


private theorem morton_lt (b x y : Nat) : morton b x y < 4^b := by
  have h := morton_bound b x y
  have hp : 0 < 4^b := Nat.pow_pos (by omega)
  omega


private theorem morton_split (b k x y : Nat) :
    morton (b+k) x y = morton b x y + 4^b * morton k (x/2^b) (y/2^b) := by
  induction k with
  | zero => simp [morton]
  | succ k ih =>
      rw [Nat.add_succ, morton_top]
      rw [ih]
      rw [morton_top]
      have hbit : (x/2^b).testBit k = x.testBit (b+k) := by
        simpa [Nat.add_comm] using (Nat.testBit_add x k b).symm
      have hbity : (y/2^b).testBit k = y.testBit (b+k) := by
        simpa [Nat.add_comm] using (Nat.testBit_add y k b).symm
      rw [← hbit, ← hbity]
      rw [Nat.pow_add]
      grind


private theorem morton_one (x y : Nat) : morton 1 x y = x%2 + 2*(y%2) := by
  simp [morton, Nat.testBit_zero]
  have hx : x % 2 < 2 := Nat.mod_lt _ (by omega)
  have hy : y % 2 < 2 := Nat.mod_lt _ (by omega)
  have hxdec : (decide (x % 2 = 1)).toNat = x % 2 := by
    by_cases h : x % 2 = 1 <;> simp [h] <;> omega
  have hydec : (decide (y % 2 = 1)).toNat = y % 2 := by
    by_cases h : y % 2 = 1 <;> simp [h] <;> omega
  omega


private theorem morton_split_one (k x y : Nat) :
    morton (1+k) x y = x%2 + 2*(y%2) + 4*morton k (x/2) (y/2) := by
  rw [morton_split, morton_one]


private theorem morton_inj (k q r q' r' : Nat)
    (hq : q < 2^k) (hr : r < 2^k) (hq' : q' < 2^k) (hr' : r' < 2^k)
    (h : morton k q r = morton k q' r') : q = q' ∧ r = r' := by
  induction k generalizing q r q' r' with
  | zero =>
      have hq0 : q = 0 := by omega
      have hq0' : q' = 0 := by omega
      have hr0 : r = 0 := by omega
      have hr0' : r' = 0 := by omega
      omega
  | succ k ih =>
      rw [Nat.pow_succ] at hq hr hq' hr'
      rw [show k + 1 = 1 + k by omega] at h
      rw [morton_split_one, morton_split_one] at h
      have hlow : q%2 + 2*(r%2) = q'%2 + 2*(r'%2) := by
        have hq2 : q%2 < 2 := Nat.mod_lt _ (by omega)
        have hr2 : r%2 < 2 := Nat.mod_lt _ (by omega)
        have hq2' : q'%2 < 2 := Nat.mod_lt _ (by omega)
        have hr2' : r'%2 < 2 := Nat.mod_lt _ (by omega)
        omega
      have hhigh : morton k (q/2) (r/2) = morton k (q'/2) (r'/2) := by omega
      have hq2 : q%2 < 2 := Nat.mod_lt _ (by omega)
      have hr2 : r%2 < 2 := Nat.mod_lt _ (by omega)
      have hq2' : q'%2 < 2 := Nat.mod_lt _ (by omega)
      have hr2' : r'%2 < 2 := Nat.mod_lt _ (by omega)
      have hqr2 : q%2 = q'%2 ∧ r%2 = r'%2 := by omega
      have hhigh' := ih (q/2) (r/2) (q'/2) (r'/2) (by omega) (by omega) (by omega) (by omega) hhigh
      constructor <;> omega


private theorem div_bound (d b x : Nat) (hbd : b ≤ d) (hx : x < 2^d) : x/2^b < 2^(d-b) := by
  have hpow := Nat.pow_sub_mul_pow 2 hbd
  have hbpos : 0 < 2^b := Nat.pow_pos (by omega)
  rw [← hpow] at hx
  exact (Nat.div_lt_iff_lt_mul hbpos).mpr (by simpa [Nat.mul_comm] using hx)


private theorem first_bounds (lo period phase : Int) (hp : 0 < period) :
    lo ≤ lo + (phase-lo)%period ∧ lo + (phase-lo)%period < lo + period := by
  have hpne : period ≠ 0 := by omega
  constructor
  · have := Int.emod_nonneg (phase-lo) hpne
    omega
  · have := Int.emod_lt_of_pos (phase-lo) hp
    omega


private theorem first_mod (lo period phase : Int) (hp : 0 < period)
    (hph0 : 0 ≤ phase) (hphp : phase < period) :
    (lo + (phase-lo)%period) % period = phase := by
  have hdvd : period ∣ (lo + (phase-lo)%period) - phase := by
    have hself := dvd_emod_sub_self period (phase-lo)
    have heq : lo + (phase-lo)%period - phase = (phase-lo)%period - (phase-lo) := by grind
    rw [heq]
    exact hself
  have hmod := emod_eq_of_dvd_sub period _ _ hdvd
  exact hmod.trans (Int.emod_eq_of_lt hph0 hphp)


private theorem first_minimal (lo period phase x : Int) (hp : 0 < period)
    (hph0 : 0 ≤ phase) (hphp : phase < period) (hlo : lo ≤ x)
    (hxl : x < lo + (phase-lo)%period) : (x % period != phase) = true := by
  have hne : x % period ≠ phase := by
    intro hxprop
    have hfmod := first_mod lo period phase hp hph0 hphp
    have hdvd : period ∣ (lo + (phase-lo)%period) - x := by
      apply (Int.dvd_iff_emod_eq_zero).mpr
      rw [← Int.emod_eq_emod_iff_emod_sub_eq_zero]
      rw [hfmod, hxprop]
    have hdiffpos : 0 < (lo + (phase-lo)%period) - x := by omega
    have hb := first_bounds lo period phase hp
    have hdiff_lt : (lo + (phase-lo)%period) - x < period := by omega
    have hq0 : ((lo + (phase-lo)%period) - x) / period = 0 :=
      Int.ediv_eq_zero_of_lt (by omega) hdiff_lt
    have hdecomp := Int.ediv_mul_add_emod ((lo + (phase-lo)%period) - x) period
    rw [hq0] at hdecomp
    simp at hdecomp
    have hmodzero : ((lo + (phase-lo)%period) - x) % period = 0 := (Int.dvd_iff_emod_eq_zero).mp hdvd
    omega
  simp [hne]


private theorem neg_count_zero (a p : Int) (hp : 0 < p) (ha : a < 0) : (a/p + 1).toNat = 0 := by
  have hlt : a/p < 0 := (Int.ediv_lt_iff_lt_mul hp).mpr (by simpa using ha)
  have hle : a/p + 1 ≤ 0 := by omega
  exact Int.toNat_of_nonpos hle


private theorem nonneg_count (a p : Int) (hp : 0 < p) (ha : 0 ≤ a) : (a/p + 1).toNat = a/p + 1 := by
  have hq : 0 ≤ a/p := (Int.le_ediv_iff_mul_le hp).mpr (by simpa using ha)
  rw [Int.toNat_of_nonneg]
  omega


private theorem prev_div_of_mod_zero (a p : Int) (hp : 0 < p) (hr : a % p = 0) :
    (a - 1) / p = a / p - 1 := by
  have hdecomp := Int.ediv_mul_add_emod a p
  rw [hr] at hdecomp
  simp at hdecomp
  let q := a / p
  have hrew : a - 1 = (p - 1) + p * (q - 1) := by
    dsimp [q]
    grind
  rw [hrew]
  rw [Int.add_mul_ediv_left]
  · have hsmall : (p - 1) / p = 0 := Int.ediv_eq_zero_of_lt (by omega) (by omega)
    rw [hsmall]
    omega
  · omega


private theorem prev_div_of_mod_nezero (a p : Int) (hp : 0 < p) (ha : 0 ≤ a) (hr : (a % p != 0) = true) :
    (a - 1) / p = a / p := by
  let r := a % p
  have hpne : p ≠ 0 := by omega
  have hrpos : 0 < r := by
    have hnon := Int.emod_nonneg a hpne
    have hneprop : a % p ≠ 0 := by
      have htmp := hr
      simp at htmp
      exact htmp
    omega
  have hrlt : r < p := Int.emod_lt_of_pos a hp
  have hdecomp := Int.ediv_mul_add_emod a p
  have hdecomp' : p * (a/p) + a%p = a := by simpa [Int.mul_comm] using hdecomp
  have hrew : a - 1 = (r - 1) + p * (a/p) := by
    dsimp [r] at *
    grind
  rw [hrew]
  rw [Int.add_mul_ediv_left]
  · have hsmall : (r - 1) / p = 0 := Int.ediv_eq_zero_of_lt (by omega) (by omega)
    rw [hsmall]
    omega
  · omega


private theorem seek_aux (lo period phase : Int) (hp : 0 < period)
    (hph0 : 0 ≤ phase) (hphp : phase < period) : ∀ L : Nat,
    let hi := lo + (L:Int)
    let f := lo + (phase-lo)%period
    let n := ((hi-1-f)/period+1).toNat
    (List.range n).map (fun i => f+(Int.ofNat i)*period) =
      ((List.range L).map (fun i => lo+(Int.ofNat i))).filter (fun x => x%period == phase) := by
  intro L
  induction L with
  | zero =>
      dsimp
      have hb := first_bounds lo period phase hp
      have hneg : lo - 1 - (lo + (phase-lo)%period) < 0 := by omega
      have hn := neg_count_zero (lo-1-(lo+(phase-lo)%period)) period hp hneg
      simp [hn]
  | succ L ih =>
      dsimp at ih ⊢
      let hi := lo + (L:Int)
      let f := lo + (phase-lo)%period
      let a := hi - f
      have hb := first_bounds lo period phase hp
      have hlohi : lo ≤ hi := by dsimp [hi]; omega
      have hnext : lo + ((L : Int) + 1) = hi + 1 := by dsimp [hi]; omega
      have hsimp1 : hi + 1 - 1 - (lo + (phase-lo)%period) = hi - f := by dsimp [f]; omega
      have hsimp2 : lo + (L:Int) - 1 - (lo + (phase-lo)%period) = hi - 1 - f := by dsimp [hi, f]
      rw [hnext]
      rw [hsimp1]
      rw [hsimp2] at ih
      by_cases haneg : a < 0
      · have hnnew : (((hi - f)/period+1).toNat) = 0 :=
          neg_count_zero (hi-f) period hp (by simpa [a] using haneg)
        have hnoldarg : hi - 1 - f < 0 := by dsimp [a] at haneg; omega
        have hnold : (((hi-1-f)/period+1).toNat) = 0 := neg_count_zero (hi-1-f) period hp hnoldarg
        have hmodnew : (hi % period != phase) = true := by
          apply first_minimal lo period phase hi hp hph0 hphp hlohi
          dsimp [a, f] at haneg
          omega
        rw [hnold] at ih
        simp only [List.range_zero, List.map_nil] at ih
        rw [hnnew]
        rw [List.range_succ]
        have hmodnew' : ((lo + (L:Int)) % period == phase) = false := by
          have ht : ((lo + (L:Int)) % period != phase) = true := by simpa [hi] using hmodnew
          simp at ht
          simp [ht]
        rw [List.map_append, List.map_singleton, List.filter_append]
        simp [hmodnew', ih]
      · have hanonneg : 0 ≤ a := by omega
        have hqnonneg : 0 ≤ a / period := (Int.le_ediv_iff_mul_le hp).mpr (by simpa using hanonneg)
        by_cases hEq : hi % period == phase
        · have hfmod := first_mod lo period phase hp hph0 hphp
          have hEqProp : hi % period = phase := of_decide_eq_true hEq
          have hfmod' : f % period = phase := by simpa [f] using hfmod
          have hdvdaf : period ∣ a := by
            apply (Int.dvd_iff_emod_eq_zero).mpr
            rw [← Int.emod_eq_emod_iff_emod_sub_eq_zero]
            rw [hfmod', hEqProp]
          have hr0 : a % period = 0 := (Int.dvd_iff_emod_eq_zero).mp hdvdaf
          have hnnew : (((hi - f)/period+1).toNat) = a/period + 1 :=
            nonneg_count (hi-f) period hp (by simpa [a] using hanonneg)
          have hnold : (((hi-1-f)/period+1).toNat) = a/period := by
            have hprev := prev_div_of_mod_zero a period hp hr0
            have harg : hi - 1 - f = a - 1 := by dsimp [a]; omega
            rw [harg, hprev]
            rw [show a/period - 1 + 1 = a/period by omega]
            exact Int.toNat_of_nonneg hqnonneg
          have hlast : f + (a/period : Int)*period = hi := by
            have hdecomp := Int.ediv_mul_add_emod a period
            rw [hr0] at hdecomp
            simp at hdecomp
            dsimp [a] at *
            grind
          have hnoldNat : (((hi-1-f)/period+1).toNat) = (a/period).toNat := by omega
          have hnnewNat : (((hi-f)/period+1).toNat) = (a/period+1).toNat := by omega
          rw [hnoldNat] at ih
          rw [hnnewNat]
          have htoNat : (a/period+1).toNat = (a/period).toNat + 1 := by omega
          rw [htoNat, List.range_succ]
          rw [List.map_append, List.map_singleton]
          rw [ih]
          have hlast' : lo + (phase-lo)%period + (a/period : Int) * period = lo + (L:Int) := by
            simpa [f, hi] using hlast
          have hEq' : ((lo + (L:Int)) % period == phase) = true := by
            simpa [hi] using hEq
          rw [List.range_succ, List.map_append, List.map_singleton, List.filter_append]
          have hmax : max (a/period) 0 = a/period := Int.max_eq_left hqnonneg
          simp [hmax, hlast', hEq']
        · have hneprop : a % period ≠ 0 := by
            intro hzprop
            have hfmod := first_mod lo period phase hp hph0 hphp
            have hfmod' : f % period = phase := by simpa [f] using hfmod
            have hEqProp : hi % period ≠ phase := by
              have ht := hEq
              simp at ht
              exact ht
            have hzp : a % period = 0 := hzprop
            have hdecomp := Int.ediv_mul_add_emod a period
            rw [hzp] at hdecomp
            simp at hdecomp
            have hmodeq : hi % period = f % period := by
              apply emod_eq_of_dvd_sub
              have hd : period ∣ a := (Int.dvd_iff_emod_eq_zero).mpr hzp
              simpa [a] using hd
            rw [hfmod'] at hmodeq
            exact hEqProp hmodeq
          have hrne : (a % period != 0) = true := by simp [hneprop]
          have hprev := prev_div_of_mod_nezero a period hp hanonneg hrne
          have hnnew : (((hi - f)/period+1).toNat) = a/period + 1 :=
            nonneg_count (hi-f) period hp (by simpa [a] using hanonneg)
          have hargEq : hi - 1 - f = a - 1 := by dsimp [a]; omega
          have heqx : ((hi-1-f)/period+1) = a/period+1 := by rw [hargEq, hprev]
          have hnon1 : 0 ≤ ((hi-1-f)/period+1) := by omega
          have hnon2 : 0 ≤ a/period+1 := by omega
          have hnoldNat : (((hi-1-f)/period+1).toNat) = (a/period+1).toNat := by
            have hcast := congrArg (fun z : Int => z.toNat) heqx
            simpa [Int.toNat_of_nonneg hnon1, Int.toNat_of_nonneg hnon2] using hcast
          have hnnewNat : (((hi-f)/period+1).toNat) = (a/period+1).toNat := by
            have hq : ((hi-f)/period+1).toNat = (a/period+1).toNat := by
              have hweq : ((hi-f)/period+1) = a/period+1 := by simpa [a]
              have hc := congrArg (fun z : Int => z.toNat) hweq
              simpa [Int.toNat_of_nonneg (by omega), Int.toNat_of_nonneg hnon2] using hc
            exact hq
          rw [hnoldNat] at ih
          rw [hnnewNat]
          have hEqf : ((lo + (L:Int)) % period == phase) = false := by
            have ht : (hi % period == phase) = false := by simp [hEq]
            simpa [hi] using ht
          rw [List.range_succ]
          rw [List.map_append, List.map_singleton, List.filter_append]
          simp [hEqf, ih]


theorem TruncBucket_sound : TinylambOpt.ResearchGoals.IndexPreimages.TruncBucket.Goal := by
  intro x d q hd
  by_cases hx : 0 ≤ x
  · rw [tdiv_of_nonneg hx hd]
    by_cases hqneg : q < 0
    · have hdiv : 0 ≤ x/d := div_nonneg x d hx hd
      have hprod : q*d < 0 := Int.mul_neg_of_neg_of_pos hqneg hd
      simp [truncBounds, hqneg]
      constructor <;> intro h <;> omega
    · have hq0 : 0 ≤ q := by omega
      have hrange := ediv_eq_iff_range x d q hx hd
      by_cases hqz : q = 0
      · subst q
        simp [truncBounds] at *
        constructor
        · intro h
          have hb := hrange.mp h
          omega
        · intro h
          apply hrange.mpr
          omega
      · have hqpos : 0 < q := by omega
        simp [truncBounds, hqneg, hqz] at *
        constructor
        · intro h
          have hb := hrange.mp h
          omega
        · intro h
          apply hrange.mpr
          omega
  · have hxneg : x < 0 := by omega
    rw [tdiv_of_neg hxneg hd]
    have hy : 0 ≤ -x := by omega
    by_cases hqpos : 0 < q
    · have hdiv : 0 ≤ -x/d := div_nonneg (-x) d hy hd
      have hprod : 0 < q*d := Int.mul_pos hqpos hd
      have hqneg' : ¬ q < 0 := by omega
      have hqz' : q ≠ 0 := by omega
      simp [truncBounds, hqneg', hqz'] at *
      constructor <;> intro h <;> omega
    · have hqnonpos : q ≤ 0 := by omega
      have hk : 0 ≤ -q := by omega
      have hrange := ediv_eq_iff_range (-x) d (-q) hy hd
      by_cases hqz : q = 0
      · subst q
        simp [truncBounds] at *
        constructor
        · intro h
          have hb := hrange.mp (by omega)
          omega
        · intro h
          apply hrange.mpr
          omega
      · have hqneg : q < 0 := by omega
        simp [truncBounds, hqneg, hqz] at *
        have hmulneg : q*d = -((-q)*d) := by grind
        constructor
        · intro h
          have hkEq : (-x)/d = -q := by omega
          have hb := hrange.mp hkEq
          omega
        · intro h
          have hkEq : (-x)/d = -q := by
            apply hrange.mpr
            omega
          omega

theorem RoundedBucket_sound : TinylambOpt.ResearchGoals.IndexPreimages.RoundedBucket.Goal := by
  intro n d k hd
  dsimp
  let q := n / d
  let r := n % d
  let twice := 2 * r
  let C : Prop := twice < d ∨ (twice = d ∧ q % 2 = 0)
  have hrnon : 0 ≤ r := Int.emod_nonneg n (by omega)
  have hrlt : r < d := Int.emod_lt_of_pos n hd
  have htw0 : 0 ≤ twice := by dsimp [twice]; omega
  have htw2 : twice < 2*d := by dsimp [twice]; omega
  have hn : n = q*d + r := by
    have h := Int.ediv_mul_add_emod n d
    dsimp [q, r]
    grind
  have hcond : ((decide (twice < d) || (twice == d && q % 2 == 0)) = true) ↔ C := by
    dsimp [C]
    constructor
    · intro h
      simp at h
      rcases h with h | ⟨h, hq⟩
      · exact Or.inl h
      · exact Or.inr ⟨h, hq⟩
    · intro h
      rcases h with h | ⟨h, hq⟩
      · simp [h]
      · simp [h, hq]
  have hnearest : nearestEven n d = (if (decide (twice < d) || (twice == d && q % 2 == 0)) = true then q else q+1) := by
    simp [nearestEven, q, r, twice]
  have hneqsucc : q+1 ≠ q := by omega
  have hneqsucc' : q ≠ q+1 := by omega
  have hnq : (nearestEven n d = q) ↔ C := by
    rw [hnearest]
    by_cases hc : (decide (twice < d) || (twice == d && q % 2 == 0)) = true
    · have hcprop : C := hcond.mp hc
      simp [hc, hcprop]
    · have hcprop : ¬ C := by intro h; exact hc (hcond.mpr h)
      simp [hc, hcprop, hneqsucc]
  have hnq1 : (nearestEven n d = q+1) ↔ ¬ C := by
    rw [hnearest]
    by_cases hc : (decide (twice < d) || (twice == d && q % 2 == 0)) = true
    · have hcprop : C := hcond.mp hc
      simp [hc, hcprop, hneqsucc']
    · have hcprop : ¬ C := by intro h; exact hc (hcond.mpr h)
      simp [hc, hcprop]
  have hdist_eq (k : Int) : (2*n - 2*k*d).natAbs = (2*(q-k)*d + twice).natAbs := by
    congr 1
    rw [hn]
    dsimp [twice]
    grind
  have hfarpos : k ≤ q-1 → d < ((2*n-2*k*d).natAbs : Int) := by
    intro hk
    have hqk : 1 ≤ q-k := by omega
    have hmul : d ≤ (q-k)*d := by
      have htmp := Int.mul_le_mul_of_nonneg_right hqk (by omega : 0 ≤ d)
      simpa using htmp
    have hmul2 : 2*d ≤ 2*(q-k)*d := by
      have htmp := Int.mul_le_mul_of_nonneg_left hmul (by omega : 0 ≤ (2:Int))
      grind
    have hv : 0 ≤ 2*(q-k)*d + twice := by omega
    rw [hdist_eq, natAbs_nonneg_eq hv]
    omega
  have hfarneg : q+2 ≤ k → d < ((2*n-2*k*d).natAbs : Int) := by
    intro hk
    have hqk : q-k ≤ -2 := by omega
    have hmul : (q-k)*d ≤ (-2)*d := Int.mul_le_mul_of_nonneg_right hqk (by omega : 0 ≤ d)
    have hmul2 : 2*(q-k)*d ≤ (-4)*d := by
      have htmp := Int.mul_le_mul_of_nonneg_left hmul (by omega : 0 ≤ (2:Int))
      grind
    have hv : 2*(q-k)*d + twice < 0 := by omega
    rw [hdist_eq, natAbs_neg_eq hv]
    omega
  have hkcases : k ≤ q-1 ∨ k = q ∨ k = q+1 ∨ q+2 ≤ k := by omega
  rcases hkcases with hklow | hkq | hkq1 | hkhigh
  · have hfar := hfarpos hklow
    have hneneq : ¬ (nearestEven n d = k) := by
      intro h
      by_cases hc : nearestEven n d = q
      · have : q = k := hc ▸ h
        omega
      · have hs : nearestEven n d = q+1 := by
          have hcprop : ¬ C := by intro hC; exact hc (hnq.mpr hC)
          exact hnq1.mpr hcprop
        have : q+1 = k := hs ▸ h
        omega
    constructor
    · intro h; exact False.elim (hneneq h)
    · intro h; rcases h with hlt | heq <;> omega
  · subst k
    have hdist : ((2*n-2*q*d).natAbs : Int) = twice := by
      rw [hdist_eq]
      simp
      exact natAbs_nonneg_eq htw0
    rw [hdist]
    constructor
    · intro h
      exact hnq.mp h
    · intro hC
      exact hnq.mpr hC
  · subst k
    have hv : 2*(q-(q+1))*d + twice < 0 := by omega
    have hdist : ((2*n-2*(q+1)*d).natAbs : Int) = 2*d - twice := by
      rw [hdist_eq, natAbs_neg_eq hv]
      omega
    rw [hdist]
    have hrhs : (2*d - twice < d ∨ (2*d - twice = d ∧ (q+1) % 2 = 0)) ↔ ¬ C := by
      dsimp [C]
      constructor
      · intro h
        rcases h with hlt | ⟨heq1, hpar⟩
        · intro hc
          rcases hc with hc | ⟨hc, _⟩ <;> omega
        · have hz : twice = d := by omega
          have hqodd : q % 2 != 0 := (parity_succ_eq q).mp hpar
          have hqoddprop : q % 2 ≠ 0 := by simpa using hqodd
          intro hc
          rcases hc with hc | ⟨hc, hqeven⟩ <;> omega
      · intro h
        have hcase : twice < d ∨ twice = d ∨ d < twice := by omega
        rcases hcase with hlt | heq | hgt
        · exact False.elim (h (Or.inl hlt))
        · right
          constructor
          · omega
          · have hqeven : ¬ q % 2 = 0 := by
              intro hq
              exact h (Or.inr ⟨heq, hq⟩)
            have hqodd : q % 2 != 0 := by simp [hqeven]
            exact (parity_succ_eq q).mpr hqodd
        · left
          omega
    constructor
    · intro h
      exact hrhs.mpr (hnq1.mp h)
    · intro h
      exact hnq1.mpr (hrhs.mp h)
  · have hfar := hfarneg hkhigh
    have hneneq : ¬ (nearestEven n d = k) := by
      intro h
      by_cases hc : nearestEven n d = q
      · have : q = k := hc ▸ h
        omega
      · have hs : nearestEven n d = q+1 := by
          have hcprop : ¬ C := by intro hC; exact hc (hnq.mpr hC)
          exact hnq1.mpr hcprop
        have : q+1 = k := hs ▸ h
        omega
    constructor
    · intro h; exact False.elim (hneneq h)
    · intro h; rcases h with hlt | heq <;> omega

theorem ModularLookup_sound : TinylambOpt.ResearchGoals.IndexPreimages.ModularLookup.Goal := by
  intro m a inv bias x target hm hmul hx hxt ht htt
  have hmpos : 0 < m := by omega
  have htmod : target % m = target := Int.emod_eq_of_lt ht htt
  have hxmod : x % m = x := Int.emod_eq_of_lt hx hxt
  have hdvd_inv : m ∣ a*inv - 1 := dvd_of_emod_eq_one hm hmul
  constructor
  · intro h
    have hdvd_h : m ∣ (a*x + bias) - target := by
      have hz : ((a*x + bias) - target) % m = 0 := by
        rw [← Int.emod_eq_emod_iff_emod_sub_eq_zero]
        simpa [htmod] using h
      exact (Int.dvd_iff_emod_eq_zero).mpr hz
    have hdvd1 : m ∣ (a*inv - 1) * x := Int.dvd_mul_of_dvd_left hdvd_inv
    have hdvd2 : m ∣ -inv * ((a*x + bias) - target) := Int.dvd_mul_of_dvd_right hdvd_h
    have hdvd3 : m ∣ (a*inv - 1)*x + (-inv)*((a*x + bias) - target) := Int.dvd_add hdvd1 hdvd2
    have heq : (a*inv - 1)*x + (-inv)*((a*x + bias) - target) = inv*(target-bias) - x := by grind
    have hdvd : m ∣ inv*(target-bias) - x := by simpa [heq] using hdvd3
    have hmod := emod_eq_of_dvd_sub m _ _ hdvd
    rw [hxmod] at hmod
    exact hmod.symm
  · intro h
    rw [h]
    have hdvd1 : m ∣ (a*inv - 1)*(target-bias) := Int.dvd_mul_of_dvd_left hdvd_inv
    have hdvd2 : m ∣ (inv*(target-bias)) % m - inv*(target-bias) := dvd_emod_sub_self m (inv*(target-bias))
    have hdvd3 : m ∣ a*((inv*(target-bias)) % m - inv*(target-bias)) := Int.dvd_mul_of_dvd_right hdvd2
    have hdvd4 : m ∣ (a*inv - 1)*(target-bias) + a*((inv*(target-bias)) % m - inv*(target-bias)) := Int.dvd_add hdvd1 hdvd3
    have heq : (a*inv - 1)*(target-bias) + a*((inv*(target-bias)) % m - inv*(target-bias)) =
        a*((inv*(target-bias)) % m) + bias - target := by grind
    have hmod := emod_eq_of_dvd_sub m _ _ (by simpa [heq] using hdvd4)
    simpa [htmod] using hmod

theorem DescendingBand_sound : TinylambOpt.ResearchGoals.IndexPreimages.DescendingBand.Goal := by
  intro x lo hi bias d hd
  constructor
  · intro h
    have h' : lo ≤ bias - x*d ∧ bias - x*d < hi := by simpa [Int.mul_comm] using h
    constructor
    · rw [Int.add_one_le_iff]
      apply (Int.ediv_lt_iff_lt_mul hd).mpr
      omega
    · apply (Int.le_ediv_iff_mul_le hd).mpr
      omega
  · intro h
    have hxle : x*d ≤ bias-lo := (Int.le_ediv_iff_mul_le hd).mp h.2
    have hxgt : bias-hi < x*d := by
      have hlt : (bias-hi)/d < x := by omega
      exact (Int.ediv_lt_iff_lt_mul hd).mp hlt
    have h'' : lo ≤ bias - x*d ∧ bias - x*d < hi := by omega
    simpa [Int.mul_comm] using h''

theorem CheckedDistanceBand_sound : TinylambOpt.ResearchGoals.IndexPreimages.CheckedDistanceBand.Goal := by
  intro rows m l h center rad hbounds hlo hhi
  induction rows with
  | nil => simp
  | cons x xs ih =>
      have hx_bounds := hbounds x (by simp)
      have hxs_bounds : ∀ y ∈ xs, l ≤ y ∧ y ≤ h := by
        intro y hy
        exact hbounds y (by simp [hy])
      have ih' := ih hxs_bounds
      have hsafe : -m < x - center ∧ x - center < m := by
        constructor <;> omega
      have hcd : checkedDistance m x center = some (x - center).natAbs := by
        simp [checkedDistance]
        constructor <;> omega
      have hdist : decide ((x - center).natAbs ≤ rad) =
          decide (center - rad ≤ x ∧ x ≤ center + rad) := by
        have hpq : ((x - center).natAbs ≤ rad) ↔ (center - rad ≤ x ∧ x ≤ center + rad) := by
          rw [natAbs_le_iff]
          omega
        by_cases hp : (x - center).natAbs ≤ rad
        · have hq := hpq.mp hp
          simp [hp, hq]
        · have hq : ¬ (center - rad ≤ x ∧ x ≤ center + rad) := by
            intro hq
            exact hp (hpq.mpr hq)
          simp [hp, hq]
      simp [List.mapM_cons, hcd, hdist, ih']

theorem DistanceRing_sound : TinylambOpt.ResearchGoals.IndexPreimages.DistanceRing.Goal := by
  intro rows o l h hl
  let p : Int → Bool := fun x => o-h ≤ x && x ≤ o - max 1 l
  let q : Int → Bool := fun x => o+l ≤ x && x ≤ o+h
  let r : Int → Bool := fun x => let d : Int := (x-o).natAbs; l ≤ d && d ≤ h
  have hdisj : ∀ x, ¬ (p x = true ∧ q x = true) := by
    intro x hx
    have hxp : o-h ≤ x ∧ x ≤ o - max 1 l := by simpa [p] using hx.1
    have hxq : o+l ≤ x ∧ x ≤ o+h := by simpa [q] using hx.2
    have hmax : 1 ≤ max 1 l := Int.le_max_left 1 l
    omega
  have hpr : ∀ x, (p x || q x) = r x := by
    intro x
    have hiff : ((p x = true) ∨ (q x = true)) ↔ (r x = true) := by
      constructor
      · intro hx
        rcases hx with hpq | hqq
        · have hpProp : o-h ≤ x ∧ x ≤ o - max 1 l := by simpa [p] using hpq
          have hmaxl : l ≤ max 1 l := Int.le_max_right 1 l
          have hmax1 : 1 ≤ max 1 l := Int.le_max_left 1 l
          have hvlow : l ≤ (x-o).natAbs := (le_natAbs_iff (x-o) l hl).mpr (Or.inl (by omega))
          have hvhigh : (x-o).natAbs ≤ h := (natAbs_le_iff (x-o) h).mpr (by constructor <;> omega)
          simpa [r] using (show l ≤ (x-o).natAbs ∧ (x-o).natAbs ≤ h from ⟨hvlow, hvhigh⟩)
        · have hqProp : o+l ≤ x ∧ x ≤ o+h := by simpa [q] using hqq
          have hvlow : l ≤ (x-o).natAbs := (le_natAbs_iff (x-o) l hl).mpr (Or.inr (by omega))
          have hvhigh : (x-o).natAbs ≤ h := (natAbs_le_iff (x-o) h).mpr (by constructor <;> omega)
          simpa [r] using (show l ≤ (x-o).natAbs ∧ (x-o).natAbs ≤ h from ⟨hvlow, hvhigh⟩)
      · intro hr
        have hr' : l ≤ (x-o).natAbs ∧ (x-o).natAbs ≤ h := by simpa [r] using hr
        have hhi := (natAbs_le_iff (x-o) h).mp hr'.2
        by_cases hneg : x - o < 0
        · apply Or.inl
          have hvabs : ((x-o).natAbs : Int) = -(x-o) := by
            have hz := Int.natAbs_of_nonneg (a := -(x-o)) (by omega)
            simpa [Int.natAbs_neg] using hz
          have hvle : x - o ≤ -l := by
            have := hr'.1
            rw [hvabs] at this
            omega
          have hmax1 : 1 ≤ max 1 l := Int.le_max_left 1 l
          have hmaxl : l ≤ max 1 l := Int.le_max_right 1 l
          have hxupper : x ≤ o - max 1 l := by
            by_cases hl0 : l = 0
            · simp [hl0]
              omega
            · have hl1 : 1 ≤ l := by omega
              have hmaxeq : max 1 l = l := Int.max_eq_right hl1
              rw [hmaxeq]
              omega
          have hpProp : o-h ≤ x ∧ x ≤ o - max 1 l := by omega
          simpa [p] using hpProp
        · apply Or.inr
          have hqProp : o+l ≤ x ∧ x ≤ o+h := by omega
          simpa [q] using hqProp
    by_cases hp : p x = true
    · have hq : q x = false := by
        cases hqa : q x with
        | false => rfl
        | true => exact False.elim (hdisj x ⟨hp, hqa⟩)
      have hr := hiff.mp (Or.inl hp)
      simp [hp, hq, hr]
    · by_cases hq : q x = true
      · have hr := hiff.mp (Or.inr hq)
        simp [hp, hq, hr]
      · have hr : r x = false := by
          cases hrv : r x with
          | false => rfl
          | true =>
              have hpq : p x = true ∨ q x = true := hiff.mpr hrv
              rcases hpq with hp' | hq'
              · exact False.elim (hp hp')
              · exact False.elim (hq hq')
        simp [hp, hq, hr]
  have hfilt := filter_perm_or p q hdisj rows
  have hcongr : rows.filter (fun a => p a || q a) = rows.filter r := by
    apply List.filter_congr
    intro x hx
    exact hpr x
  rw [hcongr] at hfilt
  change (rows.filter p ++ rows.filter q).Perm (rows.filter r)
  exact hfilt

theorem CursorRanges_sound : TinylambOpt.ResearchGoals.IndexPreimages.CursorRanges.Goal := by
  intro rows cur
  dsimp
  intro hpair
  change rows.filter (peer cur) ++ rows.filter (later cur) = rows.filter (fun r => cmpKey cur r == .lt)
  induction rows with
  | nil => simp
  | cons a as ih =>
      have htail : as.Pairwise (fun a b => cmpKey a b ≠ .gt) := List.Pairwise.of_cons hpair
      have harel : ∀ b ∈ as, cmpKey a b ≠ .gt := (List.pairwise_cons.mp hpair).1
      by_cases hpa : peer cur a = true
      · have hla : later cur a = false := by
          cases ht : later cur a with
          | false => rfl
          | true => exact False.elim (peer_later_disjoint cur a ⟨hpa, ht⟩)
        have haa : cmpKey cur a == .lt := peer_after cur a hpa
        simp [List.filter_cons, hpa, hla, haa, ih htail]
      · by_cases hla : later cur a = true
        · have haa : cmpKey cur a == .lt := later_after cur a hla
          have hnoPeer : as.filter (peer cur) = [] := by
            apply filter_nil_of_false
            intro b hb
            exact no_peer_after_later cur a b hla (by simpa using harel b hb)
          have hcongr : as.filter (fun r => cmpKey cur r == .lt) = as.filter (later cur) := by
            apply List.filter_congr
            intro b hb
            have hnpeer : peer cur b = false := no_peer_after_later cur a b hla (by simpa using harel b hb)
            cases hltb : later cur b with
            | false =>
                have hnafter : (cmpKey cur b == .lt) = false := by
                  cases hafter : cmpKey cur b == .lt with
                  | false => rfl
                  | true =>
                      rcases after_peer_or_later cur b hafter with hp | hl
                      · have : False := by rw [hp] at hnpeer; contradiction
                        exact False.elim this
                      · have : False := by rw [hl] at hltb; contradiction
                        exact False.elim this
                simp [hnafter, hltb]
            | true =>
                have hafter : cmpKey cur b == .lt := later_after cur b hltb
                simp [hafter, hltb]
          simp [List.filter_cons, hpa, hla, haa, hnoPeer, hcongr]
        · have haa : (cmpKey cur a == .lt) = false := by
            cases h' : cmpKey cur a == .lt with
            | false => rfl
            | true =>
                rcases after_peer_or_later cur a h' with hp | hl
                · exact False.elim (hpa hp)
                · exact False.elim (hla hl)
          simpa [List.filter_cons, hpa, hla, haa] using ih htail

theorem MortonSquare_sound : TinylambOpt.ResearchGoals.IndexPreimages.MortonSquare.Goal := by
  intro d b x y u v hbd hx hy hu hv
  dsimp
  let k := d - b
  let q := x / 2^b
  let r := y / 2^b
  let low := morton b x y
  let Mq := morton k q r
  let Mu := morton k u v
  let start := Mu * 4^b
  have hd : d = b + k := by dsimp [k]; omega
  have hsplit : morton d x y = low + Mq * 4^b := by
    dsimp [low, Mq, q, r]
    rw [hd, morton_split]
    rw [Nat.mul_comm]
  have hlowlt : low < 4^b := by
    dsimp [low]
    exact morton_lt b x y
  have h4pos : 0 < 4^b := Nat.pow_pos (by omega)
  have hqb : q < 2^k := by
    dsimp [q, k]
    exact div_bound d b x hbd hx
  have hrb : r < 2^k := by
    dsimp [r, k]
    exact div_bound d b y hbd hy
  have hstart_eq : morton (d-b) u v * 4^b = start := by rfl
  constructor
  · intro h
    rcases h with ⟨hq, hr⟩
    have hMq : Mq = Mu := by
      dsimp [Mq, Mu, q, r, k]
      rw [hq, hr]
    have hm : morton d x y = low + start := by rw [hsplit, hMq]
    constructor
    · rw [hm]
      omega
    · rw [hm]
      omega
  · intro h
    have hle : start ≤ morton d x y := by
      rw [← hstart_eq]
      exact h.1
    have hlt : morton d x y < start + 4^b := by
      rw [← hstart_eq]
      exact h.2
    rw [hsplit] at hle hlt
    have hleMq : Mu * 4^b < (Mq+1) * 4^b := by
      rw [Nat.add_mul]
      omega
    have hltStart : Mq * 4^b < (Mu+1) * 4^b := by
      rw [Nat.add_mul]
      omega
    have hltMu : Mu < Mq+1 := Nat.lt_of_mul_lt_mul_right hleMq
    have hltMq : Mq < Mu+1 := Nat.lt_of_mul_lt_mul_right hltStart
    have hMqMu : Mq = Mu := by omega
    have hinj := morton_inj k q r u v hqb hrb hu hv (by simpa [Mq, Mu] using hMqMu)
    simpa [q, r] using hinj

theorem PeriodicSeek_sound : TinylambOpt.ResearchGoals.IndexPreimages.PeriodicSeek.Goal := by
  intro lo hi period phase hp hph0 hphp
  dsimp
  have hcases : hi < lo ∨ lo ≤ hi := by omega
  rcases hcases with hlt | hge
  · have hzero : (hi-lo).toNat = 0 := by exact Int.toNat_of_nonpos (by omega)
    have hn : (((hi-1-(lo+(phase-lo)%period))/period+1).toNat) = 0 := by
      have hb := first_bounds lo period phase hp
      have hneg : hi-1-(lo+(phase-lo)%period) < 0 := by omega
      exact neg_count_zero _ _ hp hneg
    simp [hn, hzero]
  · have hL : hi = lo + ((hi-lo).toNat : Int) := by
      rw [Int.toNat_of_nonneg (by omega)]
      omega
    have h := seek_aux lo period phase hp hph0 hphp ((hi-lo).toNat)
    dsimp at h
    rw [← hL] at h
    simpa using h

end TinylambOpt.ResearchProofs.Wave7IndexPreimages
