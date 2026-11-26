import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Named results include `perm_sum`, `perm_prod`, `foldl_mul_acc`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-! Generic facts about the retraction contract, plus small list algebra lemmas
shared by the ten model instantiations. -/

theorem perm_sum {a b : List Nat} (p : a.Perm b) : a.sum = b.sum := by
  induction p with
  | nil => rfl
  | cons x p ih => simp [ih]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂
  | swap x y l => simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem perm_prod {a b : List Nat} (p : a.Perm b) : a.prod = b.prod := by
  induction p with
  | nil => rfl
  | cons x p ih => simp [ih]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂
  | swap x y l => simp [Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc]

theorem foldl_mul_acc (a : List Nat) (acc : Nat) :
    a.foldl (fun x y => x * y) acc = acc * a.prod := by
  induction a generalizing acc with
  | nil => simp
  | cons y ys ih =>
      simp only [List.foldl]
      rw [ih (acc * y)]
      simp [Nat.mul_assoc]

theorem foldl_or_acc (a : List Nat) (acc : Nat) :
    a.foldl (fun x y => x ||| y) acc = acc ||| a.foldl (fun x y => x ||| y) 0 := by
  induction a generalizing acc with
  | nil => simp
  | cons y ys ih =>
      simp only [List.foldl]
      rw [ih (acc ||| y), ih (0 ||| y)]
      simp [Nat.or_assoc]

theorem foldl_or_append (a b : List Nat) :
    (a ++ b).foldl (fun x y => x ||| y) 0 =
      a.foldl (fun x y => x ||| y) 0 ||| b.foldl (fun x y => x ||| y) 0 := by
  rw [List.foldl_append, foldl_or_acc]

theorem foldl_xor_acc (a : List Nat) (acc : Nat) :
    a.foldl (fun x y => x ^^^ y) acc = acc ^^^ a.foldl (fun x y => x ^^^ y) 0 := by
  induction a generalizing acc with
  | nil => simp
  | cons y ys ih =>
      simp only [List.foldl]
      rw [ih (acc ^^^ y), ih (0 ^^^ y)]
      simp [Nat.xor_assoc]

theorem foldl_xor_append (a b : List Nat) :
    (a ++ b).foldl (fun x y => x ^^^ y) 0 =
      a.foldl (fun x y => x ^^^ y) 0 ^^^ b.foldl (fun x y => x ^^^ y) 0 := by
  rw [List.foldl_append, foldl_xor_acc]

theorem perm_foldl_or {a b : List Nat} (p : a.Perm b) :
    a.foldl (fun x y => x ||| y) 0 = b.foldl (fun x y => x ||| y) 0 := by
  induction p with
  | nil => rfl
  | cons x p ih =>
      have h1 := congrArg (fun z => (0 ||| x) ||| z) ih
      rw [← foldl_or_acc _ (0 ||| x), ← foldl_or_acc _ (0 ||| x)] at h1
      simpa [List.foldl] using h1
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂
  | swap x y l => simp [Nat.or_comm, Nat.or_assoc]

theorem perm_foldl_xor {a b : List Nat} (p : a.Perm b) :
    a.foldl (fun x y => x ^^^ y) 0 = b.foldl (fun x y => x ^^^ y) 0 := by
  induction p with
  | nil => rfl
  | cons x p ih =>
      have h1 := congrArg (fun z => (0 ^^^ x) ^^^ z) ih
      rw [← foldl_xor_acc _ (0 ^^^ x), ← foldl_xor_acc _ (0 ^^^ x)] at h1
      simpa [List.foldl] using h1
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂
  | swap x y l => simp [Nat.xor_comm, Nat.xor_assoc]

theorem min_eq_getD {xs : List Nat} (h : xs ≠ []) : xs.min? = some (xs.min?.getD 0) := by
  cases hm : xs.min? with
  | none =>
      cases xs with
      | nil => contradiction
      | cons y ys => simp at hm
  | some z => simp [hm]

theorem max_eq_getD {xs : List Nat} (h : xs ≠ []) : xs.max? = some (xs.max?.getD 0) := by
  cases hm : xs.max? with
  | none =>
      cases xs with
      | nil => contradiction
      | cons y ys => simp at hm
  | some z => simp [hm]

theorem perm_min {a b : List Nat} (p : a.Perm b) : a.min?.getD 0 = b.min?.getD 0 := by
  by_cases ha : a = []
  · subst a
    have hb : b = [] := by simpa using p.symm
    subst b
    rfl
  · have hb : b ≠ [] := by
      intro h; subst b
      have : a = [] := by simpa using p
      contradiction
    have ha' := (List.min?_eq_some_iff (xs := a)).mp (min_eq_getD ha)
    have hb' := (List.min?_eq_some_iff (xs := b)).mp (min_eq_getD hb)
    exact Nat.le_antisymm (ha'.2 _ ((p.symm.mem_iff).mp hb'.1))
      (hb'.2 _ ((p.mem_iff).mp ha'.1))

theorem perm_max {a b : List Nat} (p : a.Perm b) : a.max?.getD 0 = b.max?.getD 0 := by
  by_cases ha : a = []
  · subst a
    have hb : b = [] := by simpa using p.symm
    subst b
    rfl
  · have hb : b ≠ [] := by
      intro h; subst b
      have : a = [] := by simpa using p
      contradiction
    have ha' := (List.max?_eq_some_iff (xs := a)).mp (max_eq_getD ha)
    have hb' := (List.max?_eq_some_iff (xs := b)).mp (max_eq_getD hb)
    exact Nat.le_antisymm (hb'.2 _ ((p.mem_iff).mp ha'.1))
      (ha'.2 _ ((p.symm.mem_iff).mp hb'.1))

theorem filterMap_id_erase_some {rows : List (Option Nat)} {x : Nat} (hx : some x ∈ rows) :
    (rows.erase (some x)).filterMap id = (rows.filterMap id).erase x := by
  induction rows with
  | nil => simp at hx
  | cons r rs ih =>
      cases r with
      | none =>
          have hxrs : some x ∈ rs := by simpa using hx
          simp [ih hxrs]
      | some b =>
          by_cases hb : b == x
          · have hbx : b = x := by simpa using hb
            subst b
            simp
          · have hbx : b ≠ x := by intro h; subst x; simp at hb
            have hxb : x ≠ b := hbx.symm
            have hxrs : some x ∈ rs := by simpa [hxb] using hx
            simp [hbx, List.erase_cons_tail, ih hxrs]

theorem isEmpty_false_of_ne_nil {α : Type} {l : List α} (h : l ≠ []) : l.isEmpty = false := by
  cases he : l.isEmpty with
  | false => rfl
  | true => exact False.elim (h (List.isEmpty_iff.mp he))

theorem removeAll_eq_nil_of_subset {l ys : List Nat} (h : l ⊆ ys) : l.removeAll ys = [] := by
  unfold List.removeAll
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : a ∈ ys := h (by simp)
      have hsub : as ⊆ ys := by
        intro z hz
        exact h (by simp [hz])
      have ih' := ih hsub
      simp [ha, ih']
      exact hsub

theorem xor_or_of_subset {a b : Nat} (h : a ||| b = b) : (b ^^^ a) ||| a = b := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_or, Nat.testBit_xor]
  have hb := congrArg (fun n => n.testBit i) h
  rw [Nat.testBit_or] at hb
  cases ha : a.testBit i <;> cases hbi : b.testBit i <;> simp_all

theorem xor_ne_self_of_subset_ne_zero {a b : Nat} (ha : a ≠ 0) (h : a ||| b = b) :
    b ^^^ a ≠ b := by
  let i := a.log2
  have hai : a.testBit i = true := by exact Nat.testBit_log2 ha
  have hbi : b.testBit i = true := by
    have hb := congrArg (fun n => n.testBit i) h
    rw [Nat.testBit_or] at hb
    cases hbit : b.testBit i <;> simp_all
  intro heq
  have ht := congrArg (fun n => n.testBit i) heq
  rw [Nat.testBit_xor, hai, hbi] at ht
  simp at ht

theorem or_fold_map_const (l : List Nat) (c : Nat) :
    (l.map (fun _ => c)).foldl (fun a b => a ||| b) 0 = if l = [] then 0 else c := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.foldl]
      rw [foldl_or_acc, ih]
      cases as with
      | nil => simp
      | cons b bs => simp [Nat.or_assoc, Nat.or_self]

theorem testBit_or_fold_false (l : List Nat) (i : Nat)
    (h : ∀ z, z ∈ l → z.testBit i = false) :
    (l.foldl (fun a b => a ||| b) 0).testBit i = false := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : a.testBit i = false := h a (by simp)
      have has : ∀ z, z ∈ as → z.testBit i = false := by
        intro z hz
        exact h z (by simp [hz])
      simp only [List.foldl]
      rw [foldl_or_acc, Nat.zero_or, Nat.testBit_or, ha, ih has]
      simp

theorem mem_le_sum {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ xs.sum := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
      simp at hx
      rcases hx with rfl | hx
      · simp
      · have := ih hx
        simp
        omega

theorem sum_erase_of_mem {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    (xs.erase x).sum = xs.sum - x := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
      by_cases hy : y == x
      · have hyx : y = x := by simpa using hy
        subst y
        simp
      · have hyx : y ≠ x := by intro h; subst x; simp at hy
        have hxy : x ≠ y := hyx.symm
        have hxys : x ∈ ys := by simpa [hxy] using hx
        have hle := mem_le_sum hxys
        simp [List.erase_cons_tail, hy, ih hxys]
        omega

theorem prod_eq_mul_prod_erase {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    xs.prod = x * (xs.erase x).prod := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
      by_cases hy : y == x
      · have hyx : y = x := by simpa using hy
        subst y
        simp
      · have hyx : y ≠ x := by intro h; subst x; simp at hy
        have hxy : x ≠ y := hyx.symm
        have hxys : x ∈ ys := by simpa [hxy] using hx
        simp [List.erase_cons_tail, hy, ih hxys, Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc]

theorem xor_erase_of_mem {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    xs.foldl (fun a b => a ^^^ b) 0 = x ^^^ (xs.erase x).foldl (fun a b => a ^^^ b) 0 := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
      by_cases hy : y == x
      · have hyx : y = x := by simpa using hy
        subst y
        simp only [List.foldl, List.erase_cons_head]
        rw [foldl_xor_acc]
        simp
      · have hyx : y ≠ x := by intro h; subst x; simp at hy
        have hxy : x ≠ y := hyx.symm
        have hxys : x ∈ ys := by simpa [hxy] using hx
        rw [List.erase_cons_tail hy]
        simp only [List.foldl, Nat.zero_xor]
        rw [foldl_xor_acc ys y, foldl_xor_acc (ys.erase x) y, ih hxys]
        ac_rfl

theorem or_erase_of_mem {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    xs.foldl (fun a b => a ||| b) 0 = x ||| (xs.erase x).foldl (fun a b => a ||| b) 0 := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
      by_cases hy : y == x
      · have hyx : y = x := by simpa using hy
        subst y
        simp only [List.foldl, List.erase_cons_head]
        rw [foldl_or_acc]
        simp
      · have hyx : y ≠ x := by intro h; subst x; simp at hy
        have hxy : x ≠ y := hyx.symm
        have hxys : x ∈ ys := by simpa [hxy] using hx
        rw [List.erase_cons_tail hy]
        simp only [List.foldl, Nat.zero_or]
        rw [foldl_or_acc ys y, foldl_or_acc (ys.erase x) y, ih hxys]
        ac_rfl

theorem min_erase_of_lt {xs : List Nat} {x : Nat} (hx : x ∈ xs)
    (hlt : xs.min?.getD 0 < x) : (xs.erase x).min?.getD 0 = xs.min?.getD 0 := by
  have hne : xs ≠ [] := by intro h; subst xs; simp at hx
  have ha := (List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD hne)
  let a := xs.min?.getD 0
  have ha_ne : a ≠ x := by omega
  have hamerase : a ∈ xs.erase x := (List.mem_erase_of_ne ha_ne).2 ha.1
  have hmineq : (xs.erase x).min? = some a := (List.min?_eq_some_iff).2 ⟨hamerase, by
    intro b hb
    exact ha.2 b (List.mem_of_mem_erase hb)⟩
  simp [a, hmineq]

theorem max_erase_of_lt {xs : List Nat} {x : Nat} (hx : x ∈ xs)
    (hlt : x < xs.max?.getD 0) : (xs.erase x).max?.getD 0 = xs.max?.getD 0 := by
  have hne : xs ≠ [] := by intro h; subst xs; simp at hx
  have ha := (List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD hne)
  let a := xs.max?.getD 0
  have ha_ne : a ≠ x := by omega
  have hamerase : a ∈ xs.erase x := (List.mem_erase_of_ne ha_ne).2 ha.1
  have hmaxeq : (xs.erase x).max? = some a := (List.max?_eq_some_iff).2 ⟨hamerase, by
    intro b hb
    exact ha.2 b (List.mem_of_mem_erase hb)⟩
  simp [a, hmaxeq]

theorem min_eq_of_mem_of_not_lt {xs : List Nat} {x : Nat} (hx : x ∈ xs)
    (h : ¬ xs.min?.getD 0 < x) : xs.min?.getD 0 = x := by
  have hne : xs ≠ [] := by intro h'; subst xs; simp at hx
  have ha := (List.min?_eq_some_iff (xs := xs)).mp (min_eq_getD hne)
  exact Nat.le_antisymm (ha.2 x hx) (Nat.not_lt.mp h)

theorem max_eq_of_mem_of_not_lt {xs : List Nat} {x : Nat} (hx : x ∈ xs)
    (h : ¬ x < xs.max?.getD 0) : xs.max?.getD 0 = x := by
  have hne : xs ≠ [] := by intro h'; subst xs; simp at hx
  have ha := (List.max?_eq_some_iff (xs := xs)).mp (max_eq_getD hne)
  exact Nat.le_antisymm (Nat.not_lt.mp h) (ha.2 x hx)

theorem erase_erase_perm (xs : List Nat) (x y : Nat) (hx : x ∈ xs) (hy : y ∈ xs.erase x) :
    ((xs.erase x).erase y).Perm ((xs.erase y).erase x) := by
  induction xs with
  | nil => simp at hx
  | cons z zs ih =>
      by_cases hz : z == x
      · have hzx : z = x := by simpa using hz
        subst x
        simp only [List.erase_cons_head] at hy ⊢
        by_cases hyz : y == z
        · have hyz' : y = z := by simpa using hyz
          subst y
          simp
        · have hzy : ¬ z == y := by
            cases h : z == y <;> simp_all
          rw [List.erase_cons_tail hzy]
          simp
      · have hzx : z ≠ x := by intro h; subst x; simp at hz
        have hxt : x ∈ zs := by
          simp only [List.mem_cons] at hx
          cases hx with
          | inl h => exact False.elim (hzx h.symm)
          | inr h => exact h
        by_cases hyz : y == z
        · have hyz' : y = z := by simpa using hyz
          subst y
          rw [List.erase_cons_tail hz]
          simp
        · rw [List.erase_cons_tail hz] at hy
          have hyt : y ∈ zs.erase x := by
            simp only [List.mem_cons] at hy
            cases hy with
            | inl h =>
                have hyne : y ≠ z := by intro hh; subst y; simp at hyz
                exact False.elim (hyne h)
            | inr h => exact h
          have ihp := ih hxt hyt
          have hzy : ¬ z == y := by
            cases h : z == y <;> simp_all
          simp only [List.erase_cons_tail hz, List.erase_cons_tail hzy]
          exact ihp.cons z

/-! Generic contract implications. -/

theorem fastRetraction_of_remove {m : Model}
    (h : ∀ xs x, x ∈ xs → 1 < xs.length →
      m.remove xs.length (m.result xs) x = some (m.result (xs.erase x))) :
    fastRetraction m := by
  intro xs x out hx hd
  by_cases hlen : xs.length = 1
  · have hxs : xs = [x] := by
      cases xs with
      | nil => simp at hlen
      | cons y ys =>
          simp at hlen
          subst ys
          have hxy : x = y := by simpa using hx
          subst y
          rfl
    subst xs
    simp [dropOne, value] at hd ⊢
    exact hd
  · have hgt : 1 < xs.length := by
      have hpos : 0 < xs.length := List.length_pos_of_mem hx
      omega
    have hlist : xs.erase x ≠ [] := by
      intro hnil
      have hl := congrArg List.length hnil
      rw [List.length_erase_of_mem hx] at hl
      simp at hl
      omega
    have hd' : out = some (m.result (xs.erase x)) := by
      simp [dropOne, hlen, h xs x hx hgt] at hd
      exact hd.symm
    subst out
    have his : (xs.erase x).isEmpty = false := by
      cases he : (xs.erase x).isEmpty with
      | false => rfl
      | true =>
          have hnil : xs.erase x = [] := List.isEmpty_iff.mp he
          exact False.elim (hlist hnil)
    simp [value, his]

theorem fallbackNecessary_vacuous {m : Model}
    (h : ∀ n v x, m.remove n v x ≠ none) : fallbackNecessary m := by
  intro xs x hx hd
  by_cases hlen : xs.length = 1
  · simp [dropOne, hlen] at hd
  · simp [dropOne, hlen, h] at hd

theorem combineFrames_of {m : Model}
    (hcombine : ∀ a b, a ≠ [] → b ≠ [] →
      m.combine a.length (m.result a) b.length (m.result b) = some (m.result (a ++ b))) :
    combineFrames m := by
  intro a b out ho
  by_cases ha : a = []
  · subst a
    injection ho
  by_cases hb : b = []
  · subst b
    simpa [merged, ha] using ho
  · have hc := hcombine a b ha hb
    have ho1 : some (some (m.result (a ++ b))) = some out := by
      simpa [merged, ha, hb, hc] using ho
    injection ho1 with hout
    have happ : a ++ b ≠ [] := by simp [ha]
    simpa [value, happ] using hout

theorem orderIndependent_of {m : Model}
    (hres : ∀ a b, a.Perm b → m.result a = m.result b) : orderIndependent m := by
  intro a b x hp
  constructor
  · by_cases ha : a = []
    · subst a
      have hb : b = [] := by simpa using hp.symm
      subst b
      rfl
    · have hb : b ≠ [] := by
        intro h; subst b
        have : a = [] := by simpa using hp
        contradiction
      simp [value, ha, hb, hres a b hp]
  · have hlen : a.length = b.length := hp.length_eq
    have hr : m.result a = m.result b := hres a b hp
    by_cases ha : a.length = 1
    · have hb : b.length = 1 := by omega
      cases a with
      | nil => simp at ha
      | cons y ys =>
          simp at ha
          subst ys
          cases b with
          | nil => simp at hb
          | cons z zs =>
              simp at hb
              subst zs
              simp [dropOne]
    · simp [dropOne, ha, hlen, hr]

theorem physicalCount_refute {m : Model} (x out : Nat)
    (hne : m.empty ≠ some out) (h : m.remove 3 (m.result [x]) x = some out) :
    ¬ physicalCount m := by
  intro hp
  have hmem : some x ∈ ([some x, none, none] : List (Option Nat)) := by simp
  have hh := hp [some x, none, none] x out hmem (by simp) h
  simp [value, hne] at hh

theorem duplicateFrame_refute {m : Model} (x : Nat)
    (h : m.result [x, x] ≠ m.result [x]) : ¬ duplicateFrame m := by
  intro hd
  have hh := hd [x]
  simp [value, h] at hh

theorem alwaysInvertible_refute {m : Model} (xs ys : List Nat) (x : Nat)
    (hlen : xs.length = ys.length) (hv : value m xs = value m ys)
    (hx : x ∈ xs) (hy : x ∈ ys)
    (hd : value m (xs.erase x) ≠ value m (ys.erase x)) :
    ¬ alwaysInvertible m := by
  intro ha
  exact hd (ha xs ys x hx hy hlen hv)

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common
