import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Mapping a scalar function over an output respects pointwise equality.

Reading a concrete value cell yields that value.

Named results include `mapV_congr`, `get_eq_val`, `get_eq_hole`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

/-- Mapping a scalar function over an output respects pointwise equality. -/
theorem mapV_congr {f g : Int → Int} (h : ∀ x, f x = g x) : ∀ o : Out, mapV f o = mapV g o := by
  intro o
  cases o <;> simp [mapV, h]

/-- Reading a concrete value cell yields that value. -/
theorem get_eq_val (r : Row) (i : Nat) (x : Int) (h : r[i]? = some (some x)) :
    @TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r i = Out.val x := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  rw [h]

/-- Reading a NULL cell yields NULL. -/
theorem get_eq_hole (r : Row) (i : Nat) (h : r[i]? = some none) :
    @TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r i = Out.null := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  rw [h]

/-- Reading past the end of the row faults. -/
theorem get_eq_missing (r : Row) (i : Nat) (h : r[i]? = none) :
    @TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r i = Out.fault := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  rw [h]

/-- A one-element row exposes its value at index `0`. -/
theorem get_singleton_val (L : Int) : @TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [some L] 0 = Out.val L := rfl

/-- A missing index reads as a fault. -/
theorem get_eq_fault (r : Row) (i : Nat) (h : r.length ≤ i) :
    @TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r i = Out.fault :=
  get_eq_missing r i (List.getElem?_eq_none_iff.mpr h)

/-- A row that pads `s` holes then one value exposes the value at index `s`. -/
theorem get_row_val (s : Nat) (x : Int) :
    @TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate s none ++ [some x]) s = Out.val x :=
  get_eq_val _ _ _ (by
    rw [List.getElem?_append]
    simp only [List.length_replicate]
    rw [if_neg (by omega : ¬ (s < s)), Nat.sub_self, List.getElem?_singleton,
      if_pos (show 0 = 0 from rfl)])

/-- After `s` holes, a value and `d ≥ 1` further holes, index `s + d` reads NULL. -/
theorem get_row_null (s d : Nat) (x : Int) (hd : 1 ≤ d) :
    @TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate s none ++ [some x] ++ List.replicate d none) (s + d) = Out.null :=
  get_eq_hole _ _ (by
    rw [List.getElem?_append]
    have hlen : List.length (List.replicate s none ++ [some x]) = s + 1 := by
      rw [List.length_append]
      simp
    rw [hlen]
    rw [if_neg (by omega : ¬ (s + d < s + 1)), List.getElem?_replicate]
    rw [if_pos (by omega : s + d - (s + 1) < d)])

/-- A Bool-equality test against a value output reflects propositional equality. -/
theorem beq_val {o : Out} {n : Int} (h : (o == .val n) = true) : o = .val n := by
  cases o
  · have hne : (Out.null == .val n) = false := rfl
    exact absurd (hne ▸ h) Bool.false_ne_true
  · have hne : (Out.fault == .val n) = false := rfl
    exact absurd (hne ▸ h) Bool.false_ne_true
  · rename_i m
    have h1 : (Out.val m == Out.val n) = (m == n) := rfl
    rw [h1] at h
    exact congrArg Out.val (beq_iff_eq.mp h)

/-- Low-bit masking agrees with Euclidean remainder on nonnegative arguments. -/
theorem mask_nat (a k : Nat) : a &&& (2^k - 1) = a % 2^k := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_and, Nat.testBit_two_pow_sub_one, Nat.testBit_mod_two_pow]
  exact Bool.and_comm _ _

theorem mask_emod (x : Int) (k : Nat) (hx : 0 ≤ x) :
    ((x.toNat &&& (2^k - 1) : Nat) : Int) = x % (2^k : Int) := by
  rw [mask_nat]
  obtain ⟨n, rfl⟩ : ∃ n : Nat, x = n := Int.eq_ofNat_of_zero_le hx
  rfl

/-- A value inside the checked INT64 window survives the wrapping word kernel. -/
theorem word_of_fits (v : Int) (h : fits 64 v) : word v = v := by
  unfold word
  simp only [fits, Bool.and_eq_true, decide_eq_true_eq] at h
  have hlim : limit 64 = 9223372036854775808 := by native_decide
  rw [hlim] at h
  apply BitVec.toInt_ofInt_eq_self (by native_decide)
  · simpa using h.1
  · simpa using h.2

/-- Duplicate removal and filtering by absence commute in length. -/
theorem zipIdx_find_value (t : List Int) (x : Nat) :
    (t.zipIdx.find? (fun p => p.2 == x)).map (fun p => p.1) = t[x]? := by
  by_cases hx : x < t.length
  · have hfind : t.zipIdx.find? (fun p => p.2 == x) = some (t[x], x) := by
      rw [List.find?_eq_some_iff_getElem]
      refine ⟨?_, x, ?_, ?_, ?_⟩
      · simp
      · simpa using hx
      · simp
      · intro j hj
        simp [Nat.ne_of_lt hj]
    simp [hfind]
  · have hx' : t.length ≤ x := by omega
    have hfind : t.zipIdx.find? (fun p => p.2 == x) = none := by
      simp only [List.find?_eq_none]
      rintro ⟨v, i⟩ hp
      have hget : t[i]? = some v := List.mk_mem_zipIdx_iff_getElem?.mp hp
      have hi : i < t.length := by
        rw [List.getElem?_eq_some_iff] at hget
        exact hget.1
      have hix : i ≠ x := by omega
      simp [hix]
    simp [hfind, hx']

/-- The generated positional load agrees with the dictionary lookup. -/
theorem load_eq_reference (t : List Int) (x : Int) :
    (if x < 0 then .null else ((t.toArray[x.toNat]?).map Out.val).getD .null) =
    (((t.zipIdx.find? (fun p => (p.2 : Int) == x)).map (fun p => Out.val p.1)).getD .null) := by
  by_cases hx : x < 0
  · have hfind : t.zipIdx.find? (fun p => (p.2 : Int) == x) = none := by
      simp only [List.find?_eq_none]
      rintro ⟨v, i⟩ hp
      have hget : t[i]? = some v := List.mk_mem_zipIdx_iff_getElem?.mp hp
      have hne : (i : Int) ≠ x := by omega
      simp [hne]
    simp [hx, hfind]
  · have hx0 : 0 ≤ x := by omega
    have hxn : (x.toNat : Int) = x := by omega
    by_cases hlen : x.toNat < t.length
    · have hfind : t.zipIdx.find? (fun p => (p.2 : Int) == x) = some (t[x.toNat], x.toNat) := by
        rw [List.find?_eq_some_iff_getElem]
        refine ⟨?_, x.toNat, ?_, ?_, ?_⟩
        · simp [hxn]
        · simpa using hlen
        · simp
        · intro j hj
          have : (j : Int) ≠ x := by omega
          simp [this]
      simp [hx, hfind, hlen]
    · have hlen' : t.length ≤ x.toNat := by omega
      have hfind : t.zipIdx.find? (fun p => (p.2 : Int) == x) = none := by
        simp only [List.find?_eq_none]
        rintro ⟨v, i⟩ hp
        have hget : t[i]? = some v := List.mk_mem_zipIdx_iff_getElem?.mp hp
        have hi : i < t.length := by
          rw [List.getElem?_eq_some_iff] at hget
          exact hget.1
        have hne : (i : Int) ≠ x := by omega
        simp [hne]
      have hget : t.toArray[x.toNat]? = none := by
        simp [hlen']
      simp [hx, hfind, hget]

/-- Compatibility is equality of specialization keys. -/
theorem compatible_iff_eq (s : Shape) (a b : Cfg) : compatible s a b ↔ s.key a = s.key b := by
  rw [compatible, beq_iff_eq]

/-- A cached entry found by `find?` is compatible with the caller. -/
theorem compatible_of_lookup (s : Shape) (pool : List Cfg) (c held : Cfg)
    (h : lookup s pool c = some held) : compatible s held c := by
  have h' : compatible s c held := by simpa [lookup] using List.find?_some h
  rw [compatible] at h' ⊢
  rw [beq_iff_eq] at h' ⊢
  exact h'.symm

/-- No compatible entry exists when the lookup misses. -/
theorem no_compatible_of_lookup_none (s : Shape) (pool : List Cfg) (c : Cfg)
    (h : lookup s pool c = none) : ∀ x, x ∈ pool → s.key x ≠ s.key c := by
  intro x hx hxc
  have hn := (List.find?_eq_none.mp (by simpa [lookup] using h)) x hx
  apply hn
  rw [compatible, beq_iff_eq]
  exact hxc.symm

/-- Every cached call agrees with the reference when both generic contracts hold. -/
theorem call_eq_reference (s : Shape) (hone : oneVersion s) (hsu : substitute s)
    (pool : List Cfg) (c : Cfg) (r : Row) : call s pool c r = s.reference c r := by
  unfold call
  by_cases hacc : s.accepts c r = true
  · simp [hacc]
    cases hl : lookup s pool c with
    | none => exact hone c r hacc
    | some held => exact hsu held c r (compatible_of_lookup s pool c held hl) hacc
  · have hfalse : s.accepts c r = false := by
      cases h : s.accepts c r <;> simp_all
    simp [hfalse]

/-- Generic batch replay contract. -/
theorem replayBatch_of (s : Shape) (h : ∀ pool c r, call s pool c r = s.reference c r) : replayBatch s := by
  intro pool c rs
  have hmap : rs.map (call s pool c) = rs.map (s.reference c) := by
    induction rs with
    | nil => simp
    | cons r rs ih => simp [ih, h pool c r]
  simp [hmap]

/-- Generic retained-artifact contract. -/
theorem evictWhileHeld_of (s : Shape) (h : ∀ pool c r, call s pool c r = s.reference c r)
    (hsu : substitute s) : evictWhileHeld s := by
  intro pool c held r hl hacc
  rw [h [] c r]
  exact hsu held c r (compatible_of_lookup s pool c held hl) hacc

/-- Generic publication-order contract. -/
theorem publishRace_of (s : Shape) (h : ∀ pool c r, call s pool c r = s.reference c r) : publishRace s := by
  intro a b pool c r _
  rw [h (a :: b :: pool) c r, h (b :: a :: pool) c r]

/-- Generic substitution contract from key-determined code and one-version correctness. -/
theorem substitute_of (s : Shape) (hone : oneVersion s)
    (hcode : ∀ a b, s.key a = s.key b → ∀ r, s.code a r = s.code b r) : substitute s := by
  intro a b r hcomp hacc
  have hk : s.key a = s.key b := (compatible_iff_eq s a b).mp hcomp
  rw [hcode a b hk r]
  exact hone b r hacc

/-- A false strong hypothesis: two different keys that agree on all mutually eligible rows. -/
theorem not_keyMinimal_of (s : Shape) (a b : Cfg)
    (hkey : s.key a ≠ s.key b)
    (hsame : ∀ r, s.accepts a r → s.accepts b r → s.code a r = s.code b r) : ¬ keyMinimal s := by
  intro hk
  obtain ⟨r, ha, hb, hne⟩ := hk a b hkey
  exact hne (hsame r ha hb)

/-- A false key-shrinking hypothesis: an explicit distinguishing witness. -/
theorem not_dropLastField_of (s : Shape) (a b : Cfg) (r : Row)
    (hkey : (s.key a).dropLast = (s.key b).dropLast)
    (hacc : s.accepts b r = true)
    (hne : s.code a r ≠ s.reference b r) : ¬ dropLastField s := by
  intro h
  exact hne (h a b r hkey hacc)

/-! ## Compile-once accounting (abstract: dedup by key) -/

/-- Key-level warm-up step. -/
def wKey {α : Type} [DecidableEq α] (l : List α) (k : α) : List α := if k ∈ l then l else k :: l

theorem wKey_length {α : Type} [DecidableEq α] (l : List α) (k : α) :
    (wKey l k).length = l.length + (if k ∈ l then 0 else 1) := by
  by_cases h : k ∈ l <;> simp [wKey, h]

theorem wKey_contains {α : Type} [DecidableEq α] (l : List α) (k x : α) :
    x ∈ wKey l k ↔ x ∈ l ∨ x = k := by
  by_cases h : k ∈ l
  · by_cases hx : x = k
    · subst x
      simp [wKey, h]
    · simp [wKey, h, hx]
  · simp [wKey, h, eq_comm, or_comm]

/-- Number of insertions performed by the abstract fold. -/
theorem fold_wKey_length {α : Type} [DecidableEq α] [BEq α] [LawfulBEq α] (l : List α) (ks : List α) :
    (ks.foldl wKey l).length =
      l.length + (ks.filter (fun k => decide (k ∉ l))).eraseDups.length := by
  induction ks generalizing l with
  | nil => simp
  | cons k ks ih =>
      rw [List.foldl_cons]
      by_cases hk : k ∈ l
      · rw [show wKey l k = l by simp [wKey, hk]]
        rw [ih l]
        simp [hk]
      · rw [show wKey l k = k :: l by simp [wKey, hk]]
        rw [ih (k :: l)]
        have hfk : List.filter (fun x => decide (x ∉ l)) (k :: ks) =
            k :: List.filter (fun x => decide (x ∉ l)) ks := by
          simp [hk]
        rw [hfk]
        have hfilter : ks.filter (fun x => decide (x ∉ k :: l)) =
            ks.filter (fun x => decide (x ∉ l) && decide (x ≠ k)) := by
          apply List.filter_congr
          intro x hx
          by_cases hxl : x ∈ l <;> by_cases hxk : x = k <;> simp [hxl, hxk, eq_comm]
        rw [hfilter, ← List.filter_filter]
        rw [List.eraseDups_cons]
        have hcomm : (ks.filter (fun x => decide (x ≠ k))).filter (fun x => decide (x ∉ l)) =
            (ks.filter (fun x => decide (x ∉ l))).filter (fun x => decide (x ≠ k)) := by
          rw [List.filter_filter, List.filter_filter]
          apply List.filter_congr
          intro x hx
          by_cases hxl : x ∈ l <;> by_cases hxk : x = k <;> simp [hxl, hxk, eq_comm]
        rw [hcomm]
        have hpred : List.filter (fun b => !b == k) (List.filter (fun x => decide (x ∉ l)) ks) =
            List.filter (fun x => decide (x ≠ k)) (List.filter (fun x => decide (x ∉ l)) ks) := by
          apply List.filter_congr
          intro a ha
          by_cases hak : a = k <;> simp [hak, beq_iff_eq]
        rw [hpred]
        simp only [List.length_cons]
        omega

/-- Filtering before or after duplicate removal keeps the same number of entries. -/
theorem filter_eraseDups_length {α : Type} [DecidableEq α] [BEq α] [LawfulBEq α]
    (l : List α) (ks : List α) :
    (ks.filter (fun k => decide (k ∉ l))).eraseDups.length =
      (ks.eraseDups.filter (fun k => decide (k ∉ l))).length := by
  induction hlen : ks.length using Nat.strongRecOn generalizing ks with
  | ind n ih =>
      cases ks with
      | nil => simp
      | cons a tl =>
          have hbne : (fun b : α => !b == a) = (fun b : α => decide (b ≠ a)) := by
            funext b
            by_cases hba : b = a <;> simp [hba]
          have hn : (a :: tl).length = n := by simpa using hlen
          have hlt : (tl.filter fun x => decide (x ≠ a)).length < n := by
            have := List.length_filter_le (fun x => decide (x ≠ a)) tl
            simp only [List.length_cons] at hn
            omega
          by_cases ha : a ∈ l
          · have hfilter : tl.filter (fun x => decide (x ∉ l)) =
                (tl.filter (fun x => decide (x ≠ a))).filter (fun x => decide (x ∉ l)) := by
              rw [List.filter_filter]
              apply List.filter_congr
              intro x hx
              by_cases hxa : x = a
              · subst x
                simp [ha]
              · by_cases hxl : x ∈ l <;> simp [hxa, hxl]
            have hfleft : List.filter (fun k => decide (k ∉ l)) (a :: tl) =
                List.filter (fun k => decide (k ∉ l)) tl := by
              simp [ha]
            have hfright : List.filter (fun k => decide (k ∉ l)) (a :: tl).eraseDups =
                List.filter (fun k => decide (k ∉ l)) (tl.filter (fun b => decide (b ≠ a))).eraseDups := by
              rw [List.eraseDups_cons, hbne]
              simp [ha]
            rw [hfleft, hfright, hfilter]
            exact ih _ hlt _ rfl
          · have hfleft : List.filter (fun k => decide (k ∉ l)) (a :: tl) =
                a :: List.filter (fun k => decide (k ∉ l)) tl := by
              simp [ha]
            have hfright : List.filter (fun k => decide (k ∉ l)) (a :: tl).eraseDups =
                a :: List.filter (fun k => decide (k ∉ l)) (tl.filter (fun b => decide (b ≠ a))).eraseDups := by
              rw [List.eraseDups_cons, hbne]
              simp [ha]
            rw [hfleft, hfright]
            rw [List.eraseDups_cons, hbne]
            simp only [List.length_cons]
            have hcomm : (List.filter (fun x => decide (x ≠ a)) (List.filter (fun x => decide (x ∉ l)) tl)) =
                List.filter (fun x => decide (x ∉ l)) (List.filter (fun x => decide (x ≠ a)) tl) := by
              rw [List.filter_filter, List.filter_filter]
              apply List.filter_congr
              intro x hx
              by_cases hxa : x = a <;> by_cases hxl : x ∈ l <;> simp [hxa, hxl]
            rw [hcomm]
            exact congrArg (fun n => n + 1) (ih _ hlt _ rfl)

/-- A lookup hits exactly when the key occurs in the pool keys. -/
theorem lookup_isSome_iff_key_mem (s : Shape) (pool : List Cfg) (c : Cfg) :
    (lookup s pool c).isSome = true ↔ s.key c ∈ pool.map s.key := by
  induction pool with
  | nil => simp [lookup]
  | cons x pool ih =>
      by_cases hcomp : compatible s c x = true
      · have hk : s.key c = s.key x := (compatible_iff_eq s c x).mp hcomp
        simp [lookup, hcomp, hk]
      · have hk : s.key c ≠ s.key x := by
          intro he
          apply hcomp
          rw [compatible, beq_iff_eq]
          exact he
        rw [show lookup s (x :: pool) c = lookup s pool c by simp [lookup, hcomp]]
        simpa [hk] using ih

/-- The pool keys after warm-up are obtained by key-level warm-up. -/
theorem keys_of_fold_warm (s : Shape) (pool cs : List Cfg) :
    (cs.foldl (warm s) pool).map s.key =
      (cs.map s.key).foldl wKey (pool.map s.key) := by
  induction cs generalizing pool with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons, List.map_cons, List.foldl_cons]
      by_cases hsome : (lookup s pool c).isSome = true
      · have hmem : s.key c ∈ pool.map s.key := (lookup_isSome_iff_key_mem s pool c).mp hsome
        rw [show wKey (pool.map s.key) (s.key c) = pool.map s.key by simp [wKey, hmem]]
        simp [warm, hsome, ih]
      · have hmem : s.key c ∉ pool.map s.key := by
          intro h
          exact hsome ((lookup_isSome_iff_key_mem s pool c).mpr h)
        rw [show wKey (pool.map s.key) (s.key c) = s.key c :: pool.map s.key by simp [wKey, hmem]]
        simp [warm, hsome, ih]

/-- Generic compile-once contract. -/
theorem compileOnce (s : Shape) : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.compileOnce s := by
  intro pool cs
  have hkeys := keys_of_fold_warm s pool cs
  have hlen := congrArg List.length hkeys
  rw [fold_wKey_length, filter_eraseDups_length] at hlen
  have hpred : (List.map s.key cs).eraseDups.filter (fun k => !(List.map s.key pool).contains k) =
      (List.map s.key cs).eraseDups.filter (fun k => decide (k ∉ List.map s.key pool)) := by
    apply List.filter_congr
    intro k hk
    rw [List.contains_eq_mem]
    by_cases hmem : k ∈ List.map s.key pool <;> simp [hmem]
  rw [hpred]
  simpa using hlen

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
