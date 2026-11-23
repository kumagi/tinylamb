import TinylambOpt.ResearchGoals.OverlapPairCount

/-!
# Soundness of the interval-join page-pair classification

`count` classifies a pair of pages by the interval endpoints stored in their
headers:

* if `minLo a ≥ maxHi b` or `minLo b ≥ maxHi a`, no half-open span of `a`
  overlaps any span of `b`, so the pair contributes `0` hits;
* else if `maxLo a < minHi b` and `maxLo b < minHi a`, every span of `a`
  overlaps every span of `b`, so the count is the product of the sizes;
* otherwise the nested-loop `reference` must run (flag `true`), and in this
  case the endpoint order guarantees a non-matching pair exists.

The fold `bounds` is characterized by `fold_spec`/`bounds_spec`: the running
minima/maxima bound every span component-wise and are attained by spans of
the list (the `B.minX = b.minX ∨ ∃ r ∈ xs, ...` disjuncts).  The reference
count collapses to `0` (`ref_eq_zero`) when no pair hits and to the product
(`ref_all`) when every pair hits.
-/

namespace TinylambOpt.ResearchProofs.OverlapPairCount

open TinylambOpt.ResearchGoals.OverlapPairCount

/-! ### The `bounds` fold -/

private def step (s : Bounds) (y : Span) : Bounds :=
  ⟨min s.minLo y.lo, max s.maxLo y.lo, min s.minHi y.hi, max s.maxHi y.hi⟩

private def foldB (b : Bounds) (xs : List Span) : Bounds := xs.foldl step b

private theorem bounds_cons (x : Span) (xs : List Span) :
    bounds (x :: xs) = some (foldB ⟨x.lo, x.lo, x.hi, x.hi⟩ xs) := rfl

private theorem step_minLo (s : Bounds) (y : Span) : (step s y).minLo = min s.minLo y.lo := rfl
private theorem step_maxLo (s : Bounds) (y : Span) : (step s y).maxLo = max s.maxLo y.lo := rfl
private theorem step_minHi (s : Bounds) (y : Span) : (step s y).minHi = min s.minHi y.hi := rfl
private theorem step_maxHi (s : Bounds) (y : Span) : (step s y).maxHi = max s.maxHi y.hi := rfl

/-- `Spec b B xs`: the accumulator `B` reached from seed `b` by folding `step`
over `xs` component-wise dominates (for `max*`) / is dominated by (for `min*`)
the seed, bounds every span of `xs`, and each of its four components is either
the seed's component or attained by a span of `xs`. -/
private def Spec (b B : Bounds) (xs : List Span) : Prop :=
  B.minLo ≤ b.minLo ∧ b.maxLo ≤ B.maxLo ∧ B.minHi ≤ b.minHi ∧ b.maxHi ≤ B.maxHi ∧
  (∀ r ∈ xs, B.minLo ≤ r.lo ∧ r.lo ≤ B.maxLo ∧ B.minHi ≤ r.hi ∧ r.hi ≤ B.maxHi) ∧
  (B.minLo = b.minLo ∨ ∃ r ∈ xs, r.lo = B.minLo) ∧
  (B.maxLo = b.maxLo ∨ ∃ r ∈ xs, r.lo = B.maxLo) ∧
  (B.minHi = b.minHi ∨ ∃ r ∈ xs, r.hi = B.minHi) ∧
  (B.maxHi = b.maxHi ∨ ∃ r ∈ xs, r.hi = B.maxHi)

private theorem fold_spec (b : Bounds) (xs : List Span) : Spec b (foldB b xs) xs := by
  induction xs generalizing b with
  | nil =>
      show Spec b b []
      exact ⟨by omega, by omega, by omega, by omega,
        fun r hr => absurd hr List.not_mem_nil, Or.inl rfl, Or.inl rfl, Or.inl rfl,
        Or.inl rfl⟩
  | cons y ys ih =>
      show Spec b (foldB (step b y) ys) (y :: ys)
      obtain ⟨h1, h2, h3, h4, hbnd, d1, d2, d3, d4⟩ := ih (step b y)
      refine ⟨Int.le_trans h1 ?_, Int.le_trans ?_ h2, Int.le_trans h3 ?_, Int.le_trans ?_ h4,
        ?_, ?_, ?_, ?_, ?_⟩
      · rw [step_minLo, Int.min_def]; split <;> omega
      · rw [step_maxLo, Int.max_def]; split <;> omega
      · rw [step_minHi, Int.min_def]; split <;> omega
      · rw [step_maxHi, Int.max_def]; split <;> omega
      · intro r hr
        rcases List.mem_cons.mp hr with rfl | hr
        · refine ⟨Int.le_trans h1 ?_, Int.le_trans ?_ h2, Int.le_trans h3 ?_, Int.le_trans ?_ h4⟩
          · rw [step_minLo, Int.min_def]; split <;> omega
          · rw [step_maxLo, Int.max_def]; split <;> omega
          · rw [step_minHi, Int.min_def]; split <;> omega
          · rw [step_maxHi, Int.max_def]; split <;> omega
        · exact hbnd r hr
      · rcases d1 with he | ⟨r, hr, rhe⟩
        · rw [step_minLo] at he
          by_cases hle : b.minLo ≤ y.lo
          · left; rw [he, Int.min_def, ite_eq_left hle]
          · right; exact ⟨y, List.Mem.head _, by rw [he, Int.min_def, ite_eq_right hle]⟩
        · right; exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩
      · rcases d2 with he | ⟨r, hr, rhe⟩
        · rw [step_maxLo] at he
          by_cases hle : b.maxLo ≤ y.lo
          · right; exact ⟨y, List.Mem.head _, by rw [he, Int.max_def, ite_eq_left hle]⟩
          · left; rw [he, Int.max_def, ite_eq_right hle]
        · right; exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩
      · rcases d3 with he | ⟨r, hr, rhe⟩
        · rw [step_minHi] at he
          by_cases hle : b.minHi ≤ y.hi
          · left; rw [he, Int.min_def, ite_eq_left hle]
          · right; exact ⟨y, List.Mem.head _, by rw [he, Int.min_def, ite_eq_right hle]⟩
        · right; exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩
      · rcases d4 with he | ⟨r, hr, rhe⟩
        · rw [step_maxHi] at he
          by_cases hle : b.maxHi ≤ y.hi
          · right; exact ⟨y, List.Mem.head _, by rw [he, Int.max_def, ite_eq_left hle]⟩
          · left; rw [he, Int.max_def, ite_eq_right hle]
        · right; exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩

private theorem bounds_spec {xs : List Span} {B : Bounds} (h : bounds xs = some B) :
    (∀ r ∈ xs, B.minLo ≤ r.lo ∧ r.lo ≤ B.maxLo ∧ B.minHi ≤ r.hi ∧ r.hi ≤ B.maxHi) ∧
    (∃ r ∈ xs, r.lo = B.minLo) ∧ (∃ r ∈ xs, r.lo = B.maxLo) ∧
    (∃ r ∈ xs, r.hi = B.minHi) ∧ (∃ r ∈ xs, r.hi = B.maxHi) := by
  cases xs with
  | nil => exact absurd h (by simp [bounds])
  | cons x ys =>
      rw [bounds_cons] at h
      obtain rfl : foldB ⟨x.lo, x.lo, x.hi, x.hi⟩ ys = B := Option.some.inj h
      obtain ⟨c1, c2, c3, c4, hbnd, d1, d2, d3, d4⟩ := fold_spec ⟨x.lo, x.lo, x.hi, x.hi⟩ ys
      refine ⟨?_, ?_, ?_, ?_, ?_⟩
      · intro r hr
        rcases List.mem_cons.mp hr with rfl | hr
        · exact ⟨c1, c2, c3, c4⟩
        · exact hbnd r hr
      · rcases d1 with he | ⟨r, hr, rhe⟩
        · exact ⟨x, List.Mem.head _, he.symm⟩
        · exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩
      · rcases d2 with he | ⟨r, hr, rhe⟩
        · exact ⟨x, List.Mem.head _, he.symm⟩
        · exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩
      · rcases d3 with he | ⟨r, hr, rhe⟩
        · exact ⟨x, List.Mem.head _, he.symm⟩
        · exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩
      · rcases d4 with he | ⟨r, hr, rhe⟩
        · exact ⟨x, List.Mem.head _, he.symm⟩
        · exact ⟨r, List.mem_cons_of_mem _ hr, rhe⟩

private theorem bounds_eq_none {xs : List Span} (h : bounds xs = none) : xs = [] := by
  cases xs with
  | nil => rfl
  | cons x ys => exact absurd h (by simp [bounds])

/-! ### Collapsing the double loop -/

/-! ### Reference-count collapse lemmas -/

private theorem and_false_left {p q : Bool} (hp : p = false) : (p && q) = false := by
  cases hp; rfl

private theorem and_false_right {p q : Bool} (hq : q = false) : (p && q) = false := by
  cases hq; cases p <;> rfl

private theorem and_true {p q : Bool} (hp : p = true) (hq : q = true) : (p && q) = true := by
  cases hp; cases hq; rfl

private theorem filter_nil_of_false {p : α → Bool} :
    ∀ l : List α, (∀ a ∈ l, p a = false) → List.filter p l = []
  | [], _ => rfl
  | a :: as, h => by
      rw [List.filter_cons, h a (List.Mem.head _)]
      exact filter_nil_of_false as (fun b hb => h b (List.Mem.tail _ hb))

private theorem ref_eq_zero {ys : List Span} :
    ∀ xs : List Span, (∀ r ∈ xs, ∀ s ∈ ys, hit r s = false) → reference xs ys = 0
  | [], _ => rfl
  | x :: xs, h => by
      show (List.filter (hit x) ys).length + reference xs ys = 0
      rw [filter_nil_of_false ys (fun s hs => h x (List.Mem.head _) s hs)]
      rw [List.length_nil, Nat.zero_add]
      exact ref_eq_zero xs (fun r hr => h r (List.Mem.tail _ hr))

private theorem ref_all {ys : List Span} :
    ∀ xs : List Span, (∀ r ∈ xs, ∀ s ∈ ys, hit r s = true) →
      reference xs ys = xs.length * ys.length
  | [], _ => by simp [reference]
  | x :: xs, h => by
      show (List.filter (hit x) ys).length + reference xs ys = (xs.length + 1) * ys.length
      have hf : List.filter (hit x) ys = ys := by
        rw [List.filter_eq_self]
        intro s hs
        exact h x (List.Mem.head _) s hs
      rw [hf, ref_all xs (fun r hr => h r (List.Mem.tail _ hr))]
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_comm _ _

/-! ### Main theorem -/

theorem sound : Goal := by
  intro a b va vb
  obtain ⟨hah, has, _⟩ := va
  obtain ⟨hbh, hbs, _⟩ := vb
  rcases a with ⟨ah, asz, ad⟩
  rcases b with ⟨bh, bsz, bd⟩
  dsimp only at hah has hbh hbs ⊢
  rcases ah with _ | X
  · -- missing header on the left: no pair is opened and `ad = []`
    have hnil : ad = [] := bounds_eq_none hah.symm
    subst hnil
    have hc : count ⟨none, asz, []⟩ ⟨bh, bsz, bd⟩ = (0, false) := rfl
    rw [hc]
    exact ⟨by simp [reference], fun hf => absurd hf (by decide)⟩
  · have hX : bounds ad = some X := hah.symm
    obtain ⟨hbndA, wAminLo, wAmaxLo, wAminHi, wAmaxHi⟩ := bounds_spec hX
    rcases bh with _ | Y
    · -- missing header on the right: `bd = []`
      have hnil : bd = [] := bounds_eq_none hbh.symm
      subst hnil
      have hc : count ⟨some X, asz, ad⟩ ⟨none, bsz, []⟩ = (0, false) := rfl
      rw [hc]
      exact ⟨(ref_eq_zero ad (fun _ _ _ hs => by cases hs)).symm,
        fun hf => absurd hf (by decide)⟩
    · have hY : bounds bd = some Y := hbh.symm
      obtain ⟨hbndB, wBminLo, wBmaxLo, wBminHi, wBmaxHi⟩ := bounds_spec hY
      have hc : count ⟨some X, asz, ad⟩ ⟨some Y, bsz, bd⟩ =
          if X.minLo ≥ Y.maxHi || Y.minLo ≥ X.maxHi then (0, false)
          else if X.maxLo < Y.minHi && Y.maxLo < X.minHi then (asz * bsz, false)
          else (reference ad bd, true) := rfl
      rw [hc, has, hbs]
      by_cases h1 : X.minLo ≥ Y.maxHi || Y.minLo ≥ X.maxHi
      · -- no-overlap class: the reference sum is empty
        rw [ite_eq_left h1]
        obtain h1' : X.minLo ≥ Y.maxHi ∨ Y.minLo ≥ X.maxHi := by
          simp only [Bool.or_eq_true, decide_eq_true_eq] at h1
          exact h1
        refine ⟨(ref_eq_zero ad fun r hr s hs => ?_).symm, fun hf => absurd hf (by decide)⟩
        obtain ⟨hAr1, _, _, hAr4⟩ := hbndA r hr
        obtain ⟨hBs1, _, _, hBs4⟩ := hbndB s hs
        simp only [hit]
        rcases h1' with hge | hge
        · apply and_false_left
          rw [decide_eq_false_iff_not]
          omega
        · apply and_false_right
          rw [decide_eq_false_iff_not]
          omega
      · rw [ite_eq_right h1]
        by_cases h2 : X.maxLo < Y.minHi && Y.maxLo < X.minHi
        · -- full-containment class: every pair hits
          rw [ite_eq_left h2]
          obtain h2' : X.maxLo < Y.minHi ∧ Y.maxLo < X.minHi := by
            simp only [Bool.and_eq_true, decide_eq_true_eq] at h2
            exact h2
          obtain ⟨h21, h22⟩ := h2'
          refine ⟨(ref_all ad fun r hr s hs => ?_).symm, by simp⟩
          obtain ⟨_, hr2, hr3, _⟩ := hbndA r hr
          obtain ⟨_, hs2, hs3, _⟩ := hbndB s hs
          simp only [hit]
          apply and_true <;> rw [decide_eq_true_eq] <;> omega
        · -- residual class: the opened pair contains a non-matching pair
          rw [ite_eq_right h2]
          refine ⟨rfl, fun _ => ?_⟩
          by_cases hltA : X.maxLo < Y.minHi
          · by_cases hltB : Y.maxLo < X.minHi
            · exfalso
              exact absurd (by simp [hltA, hltB]) h2
            · obtain ⟨s, hs, hsl⟩ := wBmaxLo
              obtain ⟨r, hr, hri⟩ := wAminHi
              exact ⟨r, hr, s, hs, by
                simp only [hit]
                apply and_false_right
                rw [decide_eq_false_iff_not, hsl, hri]
                exact hltB⟩
          · obtain ⟨r, hr, hrl⟩ := wAmaxLo
            obtain ⟨s, hs, hsh⟩ := wBminHi
            exact ⟨r, hr, s, hs, by
              simp only [hit]
              apply and_false_left
              rw [decide_eq_false_iff_not, hrl, hsh]
              exact hltA⟩

end TinylambOpt.ResearchProofs.OverlapPairCount
