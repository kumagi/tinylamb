import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

/-! Generic machinery for the PrefixKernels research batch.  All eight shared
contracts are proved once for an arbitrary affine kernel; model files only
instantiate these theorems or prove their two model-specific goals. -/

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

section Basic

theorem limit_pos (w : Nat) : 0 < limit w := by
  unfold limit
  exact Int.pow_pos (by decide : (0 : Int) < 2)

theorem fits_zero (w : Nat) : fits w 0 = true := by
  simp only [fits, Bool.and_eq_true]
  have hle : -limit w ≤ (0 : Int) := by have := limit_pos w; omega
  exact ⟨decide_eq_true_eq.mpr hle, decide_eq_true_eq.mpr (limit_pos w)⟩

theorem fits_true (w : Nat) (v : Int) : fits w v = true ↔ -limit w ≤ v ∧ v < limit w := by
  simp [fits, Bool.and_eq_true]

theorem apply_compose (later early : Aff) (s : Int) :
    apply (compose later early) s = apply later (apply early s) := by
  cases later
  cases early
  simp only [apply, compose, Prod.fst, Prod.snd]
  rw [Int.mul_add, Int.mul_assoc, ← Int.add_assoc]

theorem compose_id_right (f : Aff) : compose f (1, 0) = f := by
  cases f
  simp [compose]

theorem compose_id_left (f : Aff) : compose (1, 0) f = f := by
  cases f
  simp [compose]

theorem compose_assoc (a b c : Aff) : compose a (compose b c) = compose (compose a b) c := by
  cases a
  cases b
  cases c
  simp [compose, Int.mul_add, Int.add_mul, Int.mul_assoc, Int.add_assoc]

theorem foldl_compose (A : Aff) (fs : List Aff) :
    fs.foldl (fun f g => compose g f) A =
      compose (fs.foldl (fun f g => compose g f) (1, 0)) A := by
  induction fs generalizing A with
  | nil =>
      rw [List.foldl_nil, List.foldl_nil]
      exact (compose_id_left A).symm
  | cons g gs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      rw [ih (compose g A)]
      rw [compose_id_right]
      rw [ih g]
      exact compose_assoc _ _ _

theorem whole_append (k : Kernel) (a b : List Row) :
    whole k (a ++ b) = compose (whole k b) (whole k a) := by
  unfold whole
  rw [List.map_append, List.foldl_append]
  rw [foldl_compose]

theorem whole_cons (k : Kernel) (r : Row) (rs : List Row) :
    whole k (r :: rs) = compose (whole k rs) (k r) := by
  unfold whole
  rw [List.map_cons, List.foldl_cons]
  rw [foldl_compose]
  rw [compose_id_right]

theorem preimage_spec (w : Nat) (a b s : Int) :
    preimage w (a, b) s = fits w (a * s + b) := by
  unfold preimage fits
  dsimp only
  by_cases h0 : a == 0
  · have ha : a = 0 := beq_iff_eq.mp h0
    subst a
    simp
  · have ha : a ≠ 0 := by intro he; exact h0 (beq_iff_eq.mpr he)
    simp only [h0, Bool.false_eq_true, ↓reduceIte]
    by_cases hp : 0 < a
    · simp only [hp, ↓reduceIte]
      apply Bool.eq_iff_iff.mpr
      constructor <;> intro h
      · simp only [Bool.and_eq_true] at h ⊢
        constructor
        · have hleft : -((-(-limit w - b)) / a) ≤ s := of_decide_eq_true h.1
          have hleft' : -s ≤ (-(-limit w - b)) / a := by omega
          rw [Int.le_ediv_iff_mul_le hp] at hleft'
          rw [Int.neg_mul] at hleft'
          have hneg := Int.neg_le_neg hleft'
          rw [Int.neg_neg] at hneg
          rw [Int.mul_comm s a] at hneg
          apply decide_eq_true_eq.mpr
          omega
        · have hright : s ≤ (limit w - 1 - b) / a := of_decide_eq_true h.2
          rw [Int.le_ediv_iff_mul_le hp] at hright
          rw [Int.mul_comm s a] at hright
          apply decide_eq_true_eq.mpr
          omega
      · simp only [Bool.and_eq_true] at h ⊢
        constructor
        · have hlow : -limit w ≤ a * s + b := of_decide_eq_true h.1
          apply decide_eq_true_eq.mpr
          have htmp : -s * a ≤ -(-limit w - b) := by
            rw [Int.neg_mul, Int.mul_comm s a]
            omega
          rw [← Int.le_ediv_iff_mul_le hp] at htmp
          omega
        · have hup : a * s + b < limit w := of_decide_eq_true h.2
          apply decide_eq_true_eq.mpr
          have htmp : s * a ≤ limit w - 1 - b := by
            rw [Int.mul_comm s a]
            omega
          rwa [← Int.le_ediv_iff_mul_le hp] at htmp
    · have hn : a < 0 := by omega
      have hd : 0 < -a := by omega
      simp only [hp, ↓reduceIte]
      apply Bool.eq_iff_iff.mpr
      constructor <;> intro h
      · simp only [Bool.and_eq_true] at h ⊢
        constructor
        · have hright : s ≤ (-(-limit w - b)) / (-a) := of_decide_eq_true h.2
          rw [Int.le_ediv_iff_mul_le hd] at hright
          rw [Int.mul_comm s (-a), Int.neg_mul] at hright
          have hneg := Int.neg_le_neg hright
          rw [Int.neg_neg] at hneg
          apply decide_eq_true_eq.mpr
          omega
        · have hleft : -s ≤ (limit w - 1 - b) / (-a) := by
            have htmp : -((limit w - 1 - b) / (-a)) ≤ s := of_decide_eq_true h.1
            omega
          rw [Int.le_ediv_iff_mul_le hd] at hleft
          rw [Int.neg_mul, Int.mul_neg, Int.neg_neg] at hleft
          rw [Int.mul_comm s a] at hleft
          apply decide_eq_true_eq.mpr
          omega
      · simp only [Bool.and_eq_true] at h ⊢
        constructor
        · have hup : a * s + b < limit w := of_decide_eq_true h.2
          apply decide_eq_true_eq.mpr
          have htmp : -s * (-a) ≤ limit w - 1 - b := by
            rw [Int.neg_mul, Int.mul_neg, Int.neg_neg, Int.mul_comm s a]
            omega
          rw [← Int.le_ediv_iff_mul_le hd] at htmp
          omega
        · have hlow : -limit w ≤ a * s + b := of_decide_eq_true h.1
          apply decide_eq_true_eq.mpr
          have htmp : s * (-a) ≤ -(-limit w - b) := by
            rw [Int.mul_comm s (-a), Int.neg_mul]
            omega
          rw [← Int.le_ediv_iff_mul_le hd] at htmp
          omega

end Basic

section States

/-- All accumulator values seen by the loop, starting with the incoming one. -/
def states (k : Kernel) (s : Int) : List Row → List Int
  | [] => [s]
  | r :: rs => s :: states k (apply (k r) s) rs

theorem states_nonempty (k : Kernel) (s : Int) (rs : List Row) : states k s rs ≠ [] := by
  cases rs <;> simp [states]

theorem states_cons (k : Kernel) (s : Int) (r : Row) (rs : List Row) :
    states k s (r :: rs) = s :: states k (apply (k r) s) rs := by
  rfl

theorem states_eq_cons (k : Kernel) (s : Int) (rs : List Row) :
    ∃ t, states k s rs = s :: t := by
  cases rs with
  | nil => exact ⟨[], rfl⟩
  | cons r rs => exact ⟨states k (apply (k r) s) rs, rfl⟩

theorem getLast?_cons_eq {α : Type} (s v : α) (l : List α) (h : l ≠ []) :
    (s :: l).getLast? = some v ↔ l.getLast? = some v := by
  cases l with
  | nil => contradiction
  | cons x xs =>
      rw [List.getLast?_cons, List.getLast?_cons]
      simp only [Option.getD_some, Option.some.injEq]

theorem states_append (k : Kernel) (s : Int) (a b : List Row) :
    states k s (a ++ b) = states k s a ++ (states k (apply (whole k a) s) b).drop 1 := by
  induction a generalizing s with
  | nil =>
      cases b <;> simp [states, whole, apply]
  | cons r as ih =>
      rw [List.cons_append, states_cons, ih]
      rw [whole_cons, apply_compose]
      simp only [states_cons, List.cons_append]

theorem scanl_apply_from (fs : List Aff) (A : Aff) (s : Int) :
    (fs.scanl (fun f g => compose g f) A).map (fun f => apply f s) =
      fs.scanl (fun x f => apply f x) (apply A s) := by
  induction fs generalizing A s with
  | nil => simp
  | cons f fs ih =>
      rw [List.scanl_cons, List.map_cons, List.scanl_cons]
      congr 1
      rw [ih (compose f A) s]
      rw [apply_compose]

theorem states_eq_scanl (k : Kernel) (s : Int) (rs : List Row) :
    states k s rs = (rs.map k).scanl (fun x f => apply f x) s := by
  induction rs generalizing s with
  | nil => simp [states]
  | cons r rs ih =>
      rw [states_cons, List.map_cons, List.scanl_cons, ih]

theorem cert_states (k : Kernel) (w : Nat) (s : Int) (rs : List Row) :
    cert k w s rs = ((states k s rs).drop 1).all (fits w) := by
  unfold cert prefixes
  have h := scanl_apply_from (rs.map k) (1, 0) s
  have hdrop := congrArg (List.drop 1) h
  rw [← List.map_drop] at hdrop
  change (List.drop 1 ((rs.map k).scanl (fun f g => compose g f) (1, 0))).all
      (fits w ∘ fun f => apply f s) =
    ((states k s rs).drop 1).all (fits w)
  rw [← List.all_map]
  rw [hdrop]
  rw [states_eq_scanl]
  simp [apply, compose_id_right]

theorem scanl_getLast? {α β : Type} (f : β → α → β) (b : β) :
    ∀ l : List α, (l.scanl f b).getLast? = some (l.foldl f b) := by
  intro l
  induction l generalizing b with
  | nil => simp
  | cons a t ih =>
      rw [List.scanl_cons, List.foldl_cons]
      cases t with
      | nil => simp
      | cons x xs =>
          rw [List.getLast?_cons]
          have h := ih (f b a)
          rw [h]
          simp

theorem foldl_apply_eq (fs : List Aff) (s : Int) :
    fs.foldl (fun x f => apply f x) s =
      apply (fs.foldl (fun f g => compose g f) (1, 0)) s := by
  induction fs generalizing s with
  | nil =>
      rw [List.foldl_nil, List.foldl_nil]
      simp [apply, compose_id_right]
  | cons f fs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      rw [ih (apply f s)]
      rw [compose_id_right]
      have hcomp := foldl_compose f fs
      rw [hcomp]
      rw [apply_compose]

theorem states_getLast? (k : Kernel) (s : Int) (rs : List Row) :
    (states k s rs).getLast? = some (apply (whole k rs) s) := by
  rw [states_eq_scanl]
  rw [scanl_getLast?]
  simp [whole, foldl_apply_eq]

theorem execute_ok_iff (k : Kernel) (w : Nat) (i : Nat) (s : Int) (rs : List Row) (v : Int) :
    execute k w i s rs = .ok v ↔
      ((states k s rs).drop 1).all (fits w) ∧ (states k s rs).getLast? = some v := by
  induction rs generalizing i s with
  | nil =>
      simp [execute, states]
  | cons r rs ih =>
      unfold execute
      dsimp only
      rw [states_cons, List.drop_cons (by decide : 0 < 1)]
      simp only [Nat.sub_self, List.drop_zero]
      have ih' := ih (i + 1) (apply (k r) s)
      rcases states_eq_cons k (apply (k r) s) rs with ⟨t, ht⟩
      rw [ht] at ih' ⊢
      simp only [List.drop_cons (by decide : 0 < 1), Nat.sub_self, List.drop_zero] at ih'
      by_cases hf : fits w (apply (k r) s) = true
      · simp only [hf, ↓reduceIte, List.all_cons, Bool.true_and]
        exact ih'
      · have hf' : fits w (apply (k r) s) = false := Bool.eq_false_of_ne_true hf
        simp [hf']

theorem findIdx?_congr {α : Type} (p q : α → Bool) :
    ∀ l : List α, (∀ x ∈ l, p x = q x) → l.findIdx? p = l.findIdx? q := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a t ih =>
      intro h
      rw [List.findIdx?_cons, List.findIdx?_cons, h a List.mem_cons_self]
      rw [ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

theorem execute_error_first (k : Kernel) (w : Nat) (i : Nat) (s : Int) (rs : List Row) (j : Nat) :
    execute k w i s rs = .error j ↔
      ∃ n, ((states k s rs).drop 1).findIdx? (fun x => !(fits w x)) = some n ∧ j = i + n := by
  induction rs generalizing i s with
  | nil =>
      simp [execute, states]
  | cons r rs ih =>
      unfold execute
      dsimp only
      rw [states_cons, List.drop_cons (by decide : 0 < 1)]
      simp only [Nat.sub_self, List.drop_zero]
      by_cases hf : fits w (apply (k r) s) = true
      · simp only [hf, ↓reduceIte]
        rw [ih]
        rcases states_eq_cons k (apply (k r) s) rs with ⟨t, ht⟩
        constructor
        · rintro ⟨n, hn, hj⟩
          refine ⟨n + 1, ?_, ?_⟩
          rw [ht] at hn ⊢
          simp only [List.drop_cons (by decide : 0 < 1), Nat.sub_self, List.drop_zero] at hn
          rw [List.findIdx?_cons]
          simp [hf, hn]
          omega
        · rintro ⟨n, hn, hj⟩
          rw [ht, List.findIdx?_cons] at hn
          simp [hf] at hn
          rcases hn with ⟨m, hm, hmn⟩
          refine ⟨m, ?_, ?_⟩
          · rw [ht]
            simp only [List.drop_cons (by decide : 0 < 1), Nat.sub_self, List.drop_zero]
            exact hm
          · omega
      · have hf' : fits w (apply (k r) s) = false := Bool.eq_false_of_ne_true hf
        simp only [hf', Bool.false_eq_true, ↓reduceIte, Except.error.injEq]
        constructor
        · intro hj
          subst j
          refine ⟨0, ?_, by omega⟩
          rcases states_eq_cons k (apply (k r) s) rs with ⟨t, ht⟩
          rw [ht, List.findIdx?_cons]
          simp [hf']
        · rintro ⟨n, hn, hj⟩
          rcases states_eq_cons k (apply (k r) s) rs with ⟨t, ht⟩
          rw [ht, List.findIdx?_cons] at hn
          simp [hf'] at hn
          omega

end States

section Contracts

theorem execute_cert_ok (k : Kernel) (w : Nat) (s : Int) (rs : List Row) (h : cert k w s rs = true) :
    execute k w 0 s rs = .ok (apply (whole k rs) s) := by
  rw [execute_ok_iff (v := apply (whole k rs) s)]
  rw [cert_states] at h
  exact ⟨h, states_getLast? k s rs⟩

theorem not_fits_limit (w : Nat) : fits w (limit w) = false := by
  apply Bool.eq_false_of_ne_true
  intro h
  rw [fits_true] at h
  omega

theorem execute_ok_index (k : Kernel) (w : Nat) (i j : Nat) (s : Int) (rs : List Row) (v : Int) :
    execute k w i s rs = .ok v ↔ execute k w j s rs = .ok v := by
  induction rs generalizing i j s with
  | nil => simp [execute]
  | cons r rs ih =>
      unfold execute
      dsimp only
      by_cases hf : fits w (apply (k r) s) = true
      · simp only [hf, ↓reduceIte]
        exact ih (i + 1) (j + 1) (apply (k r) s)
      · have hf' : fits w (apply (k r) s) = false := Bool.eq_false_of_ne_true hf
        simp only [hf', Bool.false_eq_true, ↓reduceIte]
        simp

theorem execute_ok_cert (k : Kernel) (w : Nat) (i : Nat) (s : Int) (rs : List Row) (v : Int)
    (h : execute k w i s rs = .ok v) : cert k w s rs = true := by
  rw [cert_states]
  exact ((execute_ok_iff k w i s rs v).mp h).1

theorem states_drop_eq_map (k : Kernel) (s : Int) (rs : List Row) :
    (states k s rs).drop 1 = ((prefixes k rs).drop 1).map (fun f => apply f s) := by
  unfold prefixes
  have h := scanl_apply_from (rs.map k) (1, 0) s
  have hdrop := congrArg (List.drop 1) h
  rw [← List.map_drop] at hdrop
  rw [states_eq_scanl]
  simpa only [compose_id_right, apply, Int.one_mul, Int.zero_add, Int.add_zero] using hdrop.symm

theorem prefix_findIdx_eq_states (k : Kernel) (w : Nat) (s : Int) (rs : List Row) :
    ((prefixes k rs).drop 1).findIdx? (fun f => !(fits w (apply f s))) =
      ((states k s rs).drop 1).findIdx? (fun x => !(fits w x)) := by
  rw [states_drop_eq_map, List.findIdx?_map]
  apply findIdx?_congr
  intro f hf
  rfl

theorem cert_cons (k : Kernel) (w : Nat) (s : Int) (r : Row) (rs : List Row) :
    cert k w s (r :: rs) = (fits w (apply (k r) s) && cert k w (apply (k r) s) rs) := by
  rw [cert_states k w s (r :: rs), states_cons,
    List.drop_cons (by decide : 0 < 1)]
  simp only [Nat.sub_self, List.drop_zero]
  rw [cert_states k w (apply (k r) s) rs]
  rcases states_eq_cons k (apply (k r) s) rs with ⟨t, ht⟩
  rw [ht, List.all_cons]
  simp only [List.drop_cons (by decide : 0 < 1), Nat.sub_self, List.drop_zero]

theorem compose_one (a b : Int) : compose (1, b) (1, a) = (1, a + b) := by
  simp [compose]

theorem whole_slope_one (k : Kernel) (hslope : ∀ r, (k r).1 = 1) :
    ∀ rs : List Row, whole k rs = (1, (rs.map (fun r => (k r).2)).sum) := by
  intro rs
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [whole_cons, ih]
      rcases h : k r with ⟨a, b⟩
      have ha : a = 1 := by simpa [h] using hslope r
      subst a
      rw [compose_one]
      simp [h]

theorem drop_one_append {α : Type} (l₁ l₂ : List α) (h : l₁ ≠ []) :
    (l₁ ++ l₂).drop 1 = l₁.drop 1 ++ l₂ := by
  cases l₁ with
  | nil => contradiction
  | cons a t => rfl

theorem gen_exactEntryGuard (k : Kernel) : exactEntryGuard k := by
  intro w s rs
  have hall : ((prefixes k rs).drop 1).all (fun f => preimage w f s) =
      ((prefixes k rs).drop 1).all (fun f => fits w (apply f s)) := by
    exact List.all_congr rfl (fun f => by
      rcases f with ⟨a, b⟩
      exact preimage_spec w a b s)
  rw [hall]
  change cert k w s rs = (match execute k w 0 s rs with | .ok _ => true | .error _ => false)
  by_cases h : cert k w s rs = true
  · rw [h]
    rw [execute_cert_ok k w s rs h]
  · have hf : cert k w s rs = false := Bool.eq_false_of_ne_true h
    rw [hf]
    cases he : execute k w 0 s rs with
    | ok v =>
        have hc := execute_ok_cert k w 0 s rs v he
        exact (h hc).elim
    | error i => rfl

theorem gen_uncheckedFold (k : Kernel) : uncheckedFold k := by
  intro w s rs h
  exact execute_cert_ok k w s rs h

theorem gen_failingLane (k : Kernel) : failingLane k := by
  intro w s rs i
  rw [execute_error_first k w 0 s rs i]
  rw [← prefix_findIdx_eq_states k w s rs]
  constructor
  · rintro ⟨n, hn, hi⟩
    have hin : i = n := by omega
    subst i
    simpa using hn
  · intro h
    refine ⟨i, ?_, by omega⟩
    simpa using h

theorem gen_joinCertificates (k : Kernel) : joinCertificates k := by
  intro w s a b
  rw [cert_states k w s (a ++ b), cert_states k w s a,
    cert_states k w (apply (whole k a) s) b]
  rw [states_append]
  rw [drop_one_append (states k s a) ((states k (apply (whole k a) s) b).drop 1)
    (states_nonempty k s a)]
  rw [List.all_append]

theorem fits_between (w : Nat) (A B lo hi s : Int) (hlo : lo ≤ s) (hhi : s ≤ hi)
    (ha : -limit w ≤ A * lo + B ∧ A * lo + B < limit w)
    (hb : -limit w ≤ A * hi + B ∧ A * hi + B < limit w) :
    -limit w ≤ A * s + B ∧ A * s + B < limit w := by
  by_cases hA : 0 ≤ A
  · have h1 : A * lo ≤ A * s := Int.mul_le_mul_of_nonneg_left hlo hA
    have h2 : A * s ≤ A * hi := Int.mul_le_mul_of_nonneg_left hhi hA
    omega
  · have hAn : A ≤ 0 := by omega
    have h1 : A * hi ≤ A * s := Int.mul_le_mul_of_nonpos_left hAn hhi
    have h2 : A * s ≤ A * lo := Int.mul_le_mul_of_nonpos_left hAn hlo
    omega

theorem gen_twoEntryChecks (k : Kernel) : twoEntryChecks k := by
  intro w a b s rs has hsb hca hcb
  unfold cert at hca hcb ⊢
  rw [List.all_eq] at hca hcb ⊢
  simp only [decide_eq_true_eq] at hca hcb ⊢
  intro f hf
  have ha := hca f hf
  have hb := hcb f hf
  rcases f with ⟨A, B⟩
  simp only [apply, Prod.fst, Prod.snd] at ha hb ⊢
  rw [fits_true] at ha hb ⊢
  exact fits_between w A B a b s has hsb ha hb

theorem sum_nonneg : ∀ l : List Int, (∀ x ∈ l, 0 ≤ x) → 0 ≤ l.sum := by
  intro l
  induction l with
  | nil => intro _; simp
  | cons a t ih =>
      intro h
      rw [List.sum_cons]
      have ha : 0 ≤ a := h a List.mem_cons_self
      have ht : 0 ≤ t.sum := ih (fun x hx => h x (List.mem_cons_of_mem a hx))
      omega

theorem cert_slope_one_nonneg (k : Kernel) (w : Nat)
    (hslope : ∀ r, (k r).1 = 1) (hinc : ∀ r, 0 ≤ (k r).2) :
    ∀ (s : Int) (rs : List Row), fits w s = true →
      cert k w s rs = fits w (s + (rs.map (fun r => (k r).2)).sum) := by
  intro s rs
  induction rs generalizing s with
  | nil =>
      intro hs
      change true = fits w (s + 0)
      simpa using hs.symm
  | cons r rs ih =>
      intro hs
      rw [cert_cons]
      let rest := (rs.map (fun t => (k t).2)).sum
      have hi : 0 ≤ (k r).2 := hinc r
      have hrest : 0 ≤ rest := by
        dsimp only [rest]
        apply sum_nonneg
        intro x hx
        rcases List.mem_map.mp hx with ⟨t, ht, hxt⟩
        subst x
        exact hinc t
      have happly : apply (k r) s = s + (k r).2 := by
        simp [apply, hslope r]
      by_cases hf : fits w (apply (k r) s) = true
      · rw [hf]
        rw [ih (apply (k r) s) hf]
        rw [happly]
        simp [rest, Int.add_assoc]
      · have hf' : fits w (apply (k r) s) = false := Bool.eq_false_of_ne_true hf
        rw [hf']
        have hfinal : fits w (s + (k r).2 + rest) = false := by
          apply Bool.eq_false_of_ne_true
          intro hfin
          have hsL := (fits_true w s).mp hs
          have hfL := (fits_true w (s + (k r).2 + rest)).mp hfin
          have hb := fits_between w 1 0 s (s + (k r).2 + rest) (s + (k r).2)
            (by omega) (by omega) (by simpa using hsL) (by simpa using hfL)
          have hb' : -limit w ≤ s + (k r).2 ∧ s + (k r).2 < limit w := by simpa using hb
          have hok : fits w (s + (k r).2) = true := (fits_true w (s + (k r).2)).mpr hb'
          rw [← happly] at hok
          exact hf hok
        simpa [rest, Int.add_assoc] using hfinal

theorem gen_mergeOrder (k : Kernel) : mergeOrder k := by
  intro w s a b hcomm hcertab hcertba
  have hw : whole k (a ++ b) = whole k (b ++ a) := by
    rw [whole_append, whole_append, hcomm]
  rw [execute_cert_ok k w s (a ++ b) hcertab]
  rw [execute_cert_ok k w s (b ++ a) hcertba]
  rw [hw]

end Contracts

section Masked

def selP (p : Nat → Bool) : List (Row × Nat) → List Row
  | [] => []
  | (r, i) :: xs => if p i then r :: selP p xs else selP p xs

def lanesP (k : Kernel) (p : Nat → Bool) : List (Row × Nat) → List Aff
  | [] => []
  | (r, i) :: xs => (if p i then k r else (1, 0)) :: lanesP k p xs

def pathP (k : Kernel) (p : Nat → Bool) (s : Int) : List (Row × Nat) → List Int
  | [] => [s]
  | (r, i) :: xs =>
      let f := if p i then k r else (1, 0)
      s :: pathP k p (apply f s) xs

theorem pathP_eq_cons (k : Kernel) (p : Nat → Bool) (s : Int) (xs : List (Row × Nat)) :
    ∃ t, pathP k p s xs = s :: t := by
  cases xs with
  | nil => exact ⟨[], rfl⟩
  | cons x xs =>
      rcases x with ⟨r, i⟩
      exact ⟨pathP k p (apply (if p i then k r else (1, 0)) s) xs, rfl⟩

theorem pathP_nonempty (k : Kernel) (p : Nat → Bool) (s : Int) (xs : List (Row × Nat)) :
    pathP k p s xs ≠ [] := by
  rcases pathP_eq_cons k p s xs with ⟨t, ht⟩
  rw [ht]
  simp

theorem pathP_eq_scanl (k : Kernel) (p : Nat → Bool) (s : Int) :
    ∀ xs : List (Row × Nat),
      pathP k p s xs = (lanesP k p xs).scanl (fun x f => apply f x) s := by
  intro xs
  induction xs generalizing s with
  | nil => simp [pathP, lanesP]
  | cons x xs ih =>
      rcases x with ⟨r, i⟩
      by_cases hp : p i = true
      · simp [pathP, lanesP, hp, ih]
      · have hp' : p i = false := Bool.eq_false_of_ne_true hp
        simp [pathP, lanesP, hp', ih]

theorem selP_eq_filterMap (p : Nat → Bool) :
    ∀ xs : List (Row × Nat),
      selP p xs = xs.filterMap (fun (r, i) => if p i then some r else none) := by
  intro xs
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rcases x with ⟨r, i⟩
      by_cases hp : p i = true
      · simp [selP, hp, ih]
      · have hp' : p i = false := Bool.eq_false_of_ne_true hp
        simp [selP, hp', ih]

theorem lanesP_eq_map (k : Kernel) (p : Nat → Bool) :
    ∀ xs : List (Row × Nat),
      lanesP k p xs = xs.map (fun (r, i) => if p i then k r else (1, 0)) := by
  intro xs
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rcases x with ⟨r, i⟩
      by_cases hp : p i = true
      · simp [lanesP, hp, ih]
      · have hp' : p i = false := Bool.eq_false_of_ne_true hp
        simp [lanesP, hp', ih]

theorem masked_core (k : Kernel) (w : Nat) (p : Nat → Bool) :
    ∀ (i : Nat) (s : Int) (xs : List (Row × Nat)) (v : Int),
      fits w s = true →
      (execute k w i s (selP p xs) = .ok v ↔
        (pathP k p s xs).all (fits w) = true ∧ (pathP k p s xs).getLast? = some v) := by
  intro i s xs
  induction xs generalizing i s with
  | nil =>
      intro v hs
      simp [execute, selP, pathP, hs]
  | cons x xs ih =>
      rcases x with ⟨r, j⟩
      intro v hs
      by_cases hp : p j = true
      · have hsel : selP p ((r, j) :: xs) = r :: selP p xs := by simp [selP, hp]
        have hpath : pathP k p s ((r, j) :: xs) = s :: pathP k p (apply (k r) s) xs := by
          simp [pathP, hp]
        rw [hsel, hpath]
        unfold execute
        dsimp only
        by_cases hf : fits w (apply (k r) s) = true
        · simp only [hf, ↓reduceIte, List.all_cons, hs, Bool.true_and]
          rw [getLast?_cons_eq _ _ _ (pathP_nonempty k p (apply (k r) s) xs)]
          exact ih (i + 1) (apply (k r) s) v hf
        · have hf' : fits w (apply (k r) s) = false := Bool.eq_false_of_ne_true hf
          simp only [hf', Bool.false_eq_true, ↓reduceIte]
          rcases pathP_eq_cons k p (apply (k r) s) xs with ⟨t, ht⟩
          rw [ht]
          simp [hf']
      · have hp' : p j = false := Bool.eq_false_of_ne_true hp
        have hsel : selP p ((r, j) :: xs) = selP p xs := by simp [selP, hp']
        have hpath : pathP k p s ((r, j) :: xs) = s :: pathP k p s xs := by
          simp [pathP, hp', apply]
        rw [hsel, hpath]
        simp only [List.all_cons, hs, Bool.true_and]
        rw [getLast?_cons_eq _ _ _ (pathP_nonempty k p s xs)]
        exact ih i s v hs

theorem gen_dropMasked (k : Kernel) : dropMasked k := by
  intro w s mask rs v hs
  let p : Nat → Bool := fun i => mask[i]?.getD false
  have hsel : selected mask rs = selP p (rs.zipIdx) := by
    unfold selected
    rw [selP_eq_filterMap]
  have hlanes : rs.zipIdx.map (fun (r, i) => if mask[i]?.getD false then k r else (1, 0)) =
      lanesP k p (rs.zipIdx) := by
    rw [lanesP_eq_map]
  have hpath : (lanesP k p (rs.zipIdx)).scanl
        (fun x f => apply f x) s = pathP k p s (rs.zipIdx) := by
    exact (pathP_eq_scanl k p s (rs.zipIdx)).symm
  rw [hsel]
  dsimp only
  rw [hlanes, hpath]
  exact masked_core k w p 0 s (rs.zipIdx) v hs

end Masked



end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
