import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

/-!
This module closes the remaining scan-cache resource-policy goals.  It models
SQL block reads as immutable weighted keys in an MRU-first, byte-bounded cache.
The proofs allow capacities, speculative reads, and one-pass scans to change,
while preserving the modeled hit, inclusion, reuse-distance, and pinning
results.  No execution outside this abstract cache model is changed.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ScanCacheRandom.RemainingGoals

open TinylambOpt.ResearchGoals.ResourcePolicies.ScanCache

set_option maxHeartbeats 0

/-- Insert a key at the front of an unbounded recency stack. -/
private def push (xs : List Nat) (k : Nat) : List Nat := k :: xs.erase k

/-- The unbounded recency stack produced by a trace. -/
private def stack (trace : List Nat) : List Nat := trace.foldl push []

/-- Removing a key from a larger cache preserves the smaller cache prefix. -/
private theorem erase_prefix {xs ys : List Nat} {k : Nat} (h : xs <+: ys) :
    xs.erase k <+: ys.erase k := by
  rcases h with ⟨rest, rfl⟩
  by_cases hk : k ∈ xs
  · rw [List.erase_append_left _ hk]
    exact List.prefix_append _ _
  · rw [List.erase_append_right _ hk, List.erase_of_not_mem hk]
    exact List.prefix_append _ _

/-- Fitting two lists with the same ceiling preserves their prefix order. -/
private theorem fit_prefix_same (bytes : Nat → Nat) (cap : Nat)
    {xs ys : List Nat} (h : xs <+: ys) :
    fit bytes cap xs <+: fit bytes cap ys := by
  rcases h with ⟨rest, rfl⟩
  induction xs generalizing rest cap with
  | nil => simp [fit]
  | cons x xs ih =>
      by_cases hx : bytes x ≤ cap
      · simp only [List.cons_append, fit, hx, ↓reduceIte]
        exact List.cons_prefix_cons.mpr ⟨rfl, ih (cap - bytes x) rest⟩
      · simp [fit, hx]

/-- Raising a byte ceiling preserves every resident prefix. -/
private theorem fit_prefix_cap (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b)
    (xs : List Nat) : fit bytes a xs <+: fit bytes b xs := by
  induction xs generalizing a b with
  | nil => simp [fit]
  | cons x xs ih =>
      by_cases hx : bytes x ≤ a
      · have hxb : bytes x ≤ b := Nat.le_trans hx hab
        simp only [fit, hx, hxb, ↓reduceIte]
        exact List.cons_prefix_cons.mpr ⟨rfl, ih (Nat.sub_le_sub_right hab _)⟩
      · simp [fit, hx]

/-- Touching equal keys preserves prefix inclusion between two caches. -/
private theorem touch_prefix (bytes : Nat → Nat) (cap : Nat)
    {xs ys : List Nat} (h : xs <+: ys) (k : Nat) :
    touch bytes cap xs k <+: touch bytes cap ys k := by
  exact fit_prefix_same bytes cap (List.cons_prefix_cons.mpr ⟨rfl, erase_prefix h⟩)

/-- Touching equal keys preserves prefix inclusion after a capacity increase. -/
private theorem touch_prefix_cap (bytes : Nat → Nat) {a b : Nat} (hab : a ≤ b)
    (xs : List Nat) (k : Nat) :
    touch bytes a xs k <+: touch bytes b xs k := by
  exact fit_prefix_cap bytes hab _

/-- Running the same trace preserves prefix inclusion between two caches. -/
private theorem run_prefix (bytes : Nat → Nat) (cap : Nat)
    {xs ys : List Nat} (h : xs <+: ys) (trace : List Nat) :
    run bytes cap xs trace <+: run bytes cap ys trace := by
  induction trace generalizing xs ys with
  | nil => exact h
  | cons k ks ih => exact ih (touch_prefix bytes cap h k)

/-- A cache with a larger resident prefix cannot incur more misses. -/
private theorem demandMisses_le_of_prefix (bytes : Nat → Nat) (cap : Nat)
    {xs ys : List Nat} (h : ys <+: xs) (trace : List Nat) :
    demandMisses bytes cap xs trace ≤ demandMisses bytes cap ys trace := by
  induction trace generalizing xs ys with
  | nil => simp [demandMisses]
  | cons k ks ih =>
      simp only [demandMisses, List.contains_eq_mem, decide_eq_true_eq]
      by_cases hy : k ∈ ys
      · have hx := h.mem hy
        simp [hx, hy, ih (touch_prefix bytes cap h k)]
      · by_cases hx : k ∈ xs
        · have hi := ih (touch_prefix bytes cap h k)
          simp only [hx, hy, ite_false, ite_true, Nat.zero_add]
          omega
        · simp [hx, hy, ih (touch_prefix bytes cap h k)]

/-- Every fitted list is a prefix of its input. -/
private theorem fit_prefix_self (bytes : Nat → Nat) (cap : Nat)
    (xs : List Nat) : fit bytes cap xs <+: xs := by
  induction xs generalizing cap with
  | nil => simp [fit]
  | cons x xs ih =>
      by_cases hx : bytes x ≤ cap
      · simp only [fit, hx, ↓reduceIte]
        exact List.cons_prefix_cons.mpr ⟨rfl, ih (cap - bytes x)⟩
      · simp [fit, hx]

/-- Fitting never exceeds the byte ceiling. -/
private theorem fit_sum_le (bytes : Nat → Nat) (cap : Nat) :
    ∀ xs : List Nat, ((fit bytes cap xs).map bytes).sum ≤ cap
  | [] => by simp [fit]
  | x :: xs => by
      by_cases hx : bytes x ≤ cap
      · simp only [fit, hx, ↓reduceIte, List.map_cons, List.sum_cons]
        have ih := fit_sum_le bytes (cap - bytes x) xs
        omega
      · simp [fit, hx]

/-- A list whose bytes fit is retained in full. -/
private theorem fit_eq_self_of_sum_le (bytes : Nat → Nat) (cap : Nat) :
    ∀ xs : List Nat, (xs.map bytes).sum ≤ cap → fit bytes cap xs = xs
  | [] => by simp [fit]
  | x :: xs => by
      intro hsum
      by_cases hx : bytes x ≤ cap
      · simp only [fit, hx, ↓reduceIte, List.map_cons, List.sum_cons] at hsum ⊢
        rw [fit_eq_self_of_sum_le bytes (cap - bytes x) xs (by omega)]
      · simp only [List.map_cons, List.sum_cons] at hsum
        exfalso
        omega

/-- Fitting is idempotent. -/
private theorem fit_idempotent (bytes : Nat → Nat) (cap : Nat)
    (xs : List Nat) : fit bytes cap (fit bytes cap xs) = fit bytes cap xs :=
  fit_eq_self_of_sum_le bytes cap _ (fit_sum_le bytes cap xs)

/-- Erasing a key that fits the original ceiling commutes with the residual fit. -/
private theorem fit_erase (bytes : Nat → Nat) (cap k : Nat) (hk : bytes k ≤ cap) :
    ∀ xs : List Nat,
      fit bytes (cap - bytes k) (xs.erase k) =
        fit bytes (cap - bytes k) ((fit bytes cap xs).erase k)
  | [] => by simp [fit]
  | x :: xs => by
      by_cases hkx : k = x
      · subst k
        by_cases hx : bytes x ≤ cap
        · simp [fit, hx, fit_idempotent]
        · exfalso
          omega
      · have hxk : x ≠ k := Ne.symm hkx
        have hbool : ¬(x == k) = true := by simpa using hxk
        by_cases hx : bytes x ≤ cap
        · simp only [List.erase_cons_tail hbool, fit, hx, ↓reduceIte]
          by_cases hxd : bytes x ≤ cap - bytes k
          · simp only [fit, hxd, ↓reduceIte]
            have hk' : bytes k ≤ cap - bytes x := by omega
            have ih := fit_erase bytes (cap - bytes x) k hk' xs
            have hcap : cap - bytes k - bytes x = cap - bytes x - bytes k := by omega
            exact congrArg (x :: ·) (by rw [hcap]; exact ih)
          · simp only [hxd, ite_false]
        · have hxd : ¬bytes x ≤ cap - bytes k := by omega
          simp only [List.erase_cons_tail hbool]
          simp [fit, hx, hxd]

/-- Moving a key before a fitted prefix commutes with fitting that prefix. -/
private theorem fit_push (bytes : Nat → Nat) (cap : Nat) (k : Nat)
    (xs : List Nat) : fit bytes cap (push xs k) = touch bytes cap (fit bytes cap xs) k := by
  by_cases hk : bytes k ≤ cap
  · simp only [push, touch, fit, hk, ↓reduceIte]
    exact congrArg (k :: ·) (fit_erase bytes cap k hk xs)
  · simp [push, touch, fit, hk]

/-- A trace starting from an arbitrary stack agrees with running its fitted cache. -/
private theorem run_eq_fit_stack_any (bytes : Nat → Nat) (cap : Nat)
    (cache trace : List Nat) :
    run bytes cap (fit bytes cap cache) trace = fit bytes cap (trace.foldl push cache) := by
  induction trace generalizing cache with
  | nil => rfl
  | cons k ks ih =>
      change run bytes cap (touch bytes cap (fit bytes cap cache) k) ks =
        fit bytes cap (ks.foldl push (push cache k))
      rw [← ih (push cache k), fit_push bytes cap k cache]

/-- The concrete cache is the fitted unbounded recency stack of its trace. -/
private theorem run_eq_fit_stack (bytes : Nat → Nat) (cap : Nat)
    (cache trace : List Nat) (hc : fit bytes cap cache = cache) :
    run bytes cap cache trace = fit bytes cap (trace.foldl push cache) := by
  calc
    run bytes cap cache trace = run bytes cap (fit bytes cap cache) trace := by rw [hc]
    _ = fit bytes cap (trace.foldl push cache) := run_eq_fit_stack_any bytes cap cache trace

/-- Membership in a recency-stack push is exactly the new key or an old key. -/
private theorem mem_push {xs : List Nat} {k a : Nat} :
    a ∈ push xs k ↔ a = k ∨ a ∈ xs := by
  by_cases hak : a = k
  · subst a
    simp [push]
  · simp [push, List.mem_erase_of_ne hak]

/-- A recency-stack fold retains exactly its initial and traced keys. -/
private theorem mem_fold_push (trace xs : List Nat) (a : Nat) :
    a ∈ trace.foldl push xs ↔ a ∈ xs ∨ a ∈ trace := by
  induction trace generalizing xs with
  | nil => simp
  | cons k ks ih =>
      rw [List.foldl_cons, ih, mem_push]
      simp only [List.mem_cons]
      simp only [or_assoc, or_left_comm, or_comm]

/-- The unbounded stack retains exactly the keys mentioned by the trace. -/
private theorem mem_stack {trace : List Nat} {a : Nat} :
    a ∈ stack trace ↔ a ∈ trace := by
  simp [stack, mem_fold_push]

/-- Pushing preserves duplicate-freedom of a recency stack. -/
private theorem nodup_push {xs : List Nat} (h : xs.Nodup) (k : Nat) :
    (push xs k).Nodup := by
  exact List.nodup_cons.mpr ⟨h.not_mem_erase, h.erase k⟩

/-- A recency-stack fold preserves duplicate-freedom. -/
private theorem nodup_fold_push (trace : List Nat) {xs : List Nat} (h : xs.Nodup) :
    (trace.foldl push xs).Nodup := by
  induction trace generalizing xs with
  | nil => exact h
  | cons k ks ih => exact ih (nodup_push h k)

/-- The unbounded recency stack has no duplicate keys. -/
private theorem nodup_stack (trace : List Nat) : (stack trace).Nodup :=
  nodup_fold_push trace List.nodup_nil

/-- Deduplicating a list preserves duplicate-freedom. -/
private theorem nodup_eraseDups (xs : List Nat) : xs.eraseDups.Nodup := by
  match xs with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hx
        have hmem := List.mem_eraseDups.mp hx
        have hf := List.mem_filter.mp hmem
        simp at hf
      · exact nodup_eraseDups _
termination_by xs.length
decreasing_by
  have hlen := List.length_filter_le (fun b => !b == x) xs
  simpa using Nat.lt_succ_of_le hlen

/-- A key occurs once in a duplicate-free list that contains it. -/
private theorem count_eq_one_of_nodup {xs : List Nat} {a : Nat}
    (h : xs.Nodup) (ha : a ∈ xs) : xs.count a = 1 := by
  induction xs generalizing a with
  | nil => simp at ha
  | cons x xs ih =>
      rcases List.nodup_cons.mp h with ⟨hx, hxs⟩
      by_cases hxa : x = a
      · subst x
        simp [List.count_cons, List.count_eq_zero_of_not_mem hx]
      · have haxs : a ∈ xs := by
          rcases List.mem_cons.mp ha with hhead | htail
          · exact False.elim (hxa (Eq.symm hhead))
          · exact htail
        simp [List.count_cons, hxa, ih hxs haxs]

/-- The recency stack is a permutation of the trace's distinct keys. -/
private theorem stack_perm_eraseDups (trace : List Nat) :
    (stack trace).Perm trace.eraseDups := by
  apply List.perm_iff_count.mpr
  intro a
  by_cases ha : a ∈ trace
  · have hs : a ∈ stack trace := mem_stack.mpr ha
    have hd : a ∈ trace.eraseDups := List.mem_eraseDups.mpr ha
    rw [count_eq_one_of_nodup (nodup_stack trace) hs,
      count_eq_one_of_nodup (nodup_eraseDups trace) hd]
  · have hs : a ∉ stack trace := fun h => ha (mem_stack.mp h)
    have hd : a ∉ trace.eraseDups := fun h => ha (List.mem_eraseDups.mp h)
    rw [List.count_eq_zero_of_not_mem hs, List.count_eq_zero_of_not_mem hd]

/-- Recency-stack weights equal the weights of the trace's distinct keys. -/
private theorem stack_sum (bytes : Nat → Nat) (trace : List Nat) :
    ((stack trace).map bytes).sum = (trace.eraseDups.map bytes).sum :=
  ((stack_perm_eraseDups trace).map bytes).sum_nat

/-- Pushing a key different from a pinned tail key preserves that tail key. -/
private theorem push_append_singleton {xs : List Nat} {a k : Nat} (hak : a ≠ k) :
    push (xs ++ [k]) a = push xs a ++ [k] := by
  by_cases ha : a ∈ xs
  · unfold push
    rw [List.erase_append_left _ ha]
    rfl
  · unfold push
    rw [List.erase_append_right _ ha]
    have hk : a ∉ [k] := by simpa using hak
    rw [List.erase_of_not_mem hk, List.erase_of_not_mem ha]
    simp only [List.cons_append]

/-- A trace that avoids a pinned key leaves it at the tail of the stack. -/
private theorem fold_push_pin (trace xs : List Nat) (k : Nat) (hk : k ∉ trace) :
    trace.foldl push (xs ++ [k]) = (trace.foldl push xs) ++ [k] := by
  induction trace generalizing xs with
  | nil => simp
  | cons a as ih =>
      have hak : a ≠ k := by
        intro h
        apply hk
        simp [h]
      simp only [List.foldl_cons]
      rw [push_append_singleton hak]
      exact ih (push xs a) (fun h => hk (by simp [h]))

/-- An absent target remains at the tail after all intervening references. -/
private theorem stack_pin {k : Nat} {between : List Nat} (hk : k ∉ between) :
    stack (k :: between) = stack between ++ [k] := by
  change between.foldl push (push [] k) = between.foldl push [] ++ [k]
  exact fold_push_pin between [] k hk

/-- A nonduplicated final key is resident exactly when its whole prefix fits. -/
private theorem mem_fit_append_of_not_mem (bytes : Nat → Nat) (cap : Nat)
    (pre : List Nat) (k : Nat) (hk : k ∉ pre) :
    k ∈ fit bytes cap (pre ++ [k]) ↔ (pre.map bytes).sum + bytes k ≤ cap := by
  induction pre generalizing cap with
  | nil => simp [fit]
  | cons x xs ih =>
      have hkx : k ≠ x := by
        intro h
        apply hk
        simp [h]
      have hkxs : k ∉ xs := fun h => hk (by simp [h])
      by_cases hx : bytes x ≤ cap
      · simp only [List.cons_append, fit, hx, ↓reduceIte, List.mem_cons, hkx,
          false_or]
        rw [ih (cap - bytes x) hkxs]
        simp only [List.map_cons, List.sum_cons]
        omega
      · simp only [List.cons_append, fit, hx, ite_false, List.not_mem_nil, false_iff]
        simp only [List.map_cons, List.sum_cons]
        omega

/-- Charge distinct, more-recent keys plus the target for an exact reuse test after its last touch. -/
theorem weightedReuseDistance : WeightedReuseDistance.Goal := by
  intro bytes cap k between hk
  rw [run_eq_fit_stack bytes cap [] (k :: between) (by simp [fit])]
  change k ∈ fit bytes cap (stack (k :: between)) ↔ _
  have hnot : k ∉ stack between := fun h => hk (mem_stack.mp h)
  rw [stack_pin hk, mem_fit_append_of_not_mem bytes cap (stack between) k hnot,
    stack_sum]
  omega

/-- Remove every key belonging to an irrelevant scan from a recency stack. -/
private def project (scan xs : List Nat) : List Nat :=
  xs.filter (fun x => !scan.contains x)

/-- The byte weight of keys removed from a stack by scan projection. -/
private def discarded (bytes : Nat → Nat) (scan xs : List Nat) : Nat :=
  ((xs.filter (fun x => scan.contains x)).map bytes).sum

/-- Projection removes exactly the scan keys and preserves all other keys. -/
private theorem mem_project {scan xs : List Nat} {a : Nat} :
    a ∈ project scan xs ↔ a ∈ xs ∧ a ∉ scan := by
  simp [project, List.mem_filter, List.contains_eq_mem]

/-- Projection agrees with the library's repeated removal operation. -/
private theorem project_eq_removeAll (scan xs : List Nat) :
    project scan xs = xs.removeAll scan := by
  induction scan generalizing xs with
  | nil => rfl
  | cons k ks ih =>
      rw [List.removeAll_cons, ← ih (xs.filter (fun x => !x == k))]
      simp only [project, List.contains_eq_mem, Bool.not_or, List.filter_filter]
      congr 1
      funext a
      by_cases h : a = k
      · subst a
        simp
      · have hb : (a == k) = false := by simp [h]
        simp [h, hb]

/-- Erasing a key commutes with projection. -/
private theorem project_erase (scan xs : List Nat) (k : Nat) :
    project scan (xs.erase k) = (project scan xs).erase k := by
  exact (List.erase_filter (a := k) (f := fun x : Nat => !scan.contains x)
    (l := xs)).symm

/-- A fitted prefix can be separated before fitting the remaining suffix. -/
private theorem fit_append_of_sum_le (bytes : Nat → Nat) (cap : Nat) :
    ∀ (pre post : List Nat), (pre.map bytes).sum ≤ cap →
      fit bytes cap (pre ++ post) = pre ++ fit bytes (cap - (pre.map bytes).sum) post
  | [], post, _ => by simp [fit]
  | x :: xs, post, hsum => by
      have hsum' : bytes x + (xs.map bytes).sum ≤ cap := by
        simpa only [List.map_cons, List.sum_cons] using hsum
      have hx : bytes x ≤ cap := by omega
      simp only [List.cons_append, fit, hx, ↓reduceIte, List.map_cons,
        List.sum_cons]
      have ih := fit_append_of_sum_le bytes (cap - bytes x) xs post (by omega)
      have hcap : cap - bytes x - (xs.map bytes).sum =
          cap - (bytes x + (xs.map bytes).sum) := by omega
      rw [ih, hcap]

/-- A resident key in a fitted prefix stays resident when a suffix is appended. -/
private theorem mem_fit_append_left (bytes : Nat → Nat) (cap : Nat)
    (pre post : List Nat) {k : Nat} (hk : k ∈ pre) :
    k ∈ fit bytes cap (pre ++ post) → k ∈ fit bytes cap pre := by
  induction pre generalizing cap with
  | nil => simp at hk
  | cons x xs ih =>
      intro hhit
      by_cases hkx : k = x
      · subst k
        have hx : bytes x ≤ cap := by
          apply Nat.le_of_not_gt
          intro hnot
          have hnot' : ¬bytes x ≤ cap := Nat.not_le.mpr hnot
          simp [fit, hnot'] at hhit
        simp only [fit, hx, ↓reduceIte]
        exact List.mem_cons_self
      · have hkxs : k ∈ xs := by
          rcases List.mem_cons.mp hk with h | h
          · exact False.elim (hkx h)
          · exact h
        by_cases hx : bytes x ≤ cap
        · have hhit' : k ∈ fit bytes (cap - bytes x) (xs ++ post) := by
            simpa only [List.cons_append, fit, hx, ↓reduceIte, List.mem_cons,
              hkx, false_or] using hhit
          simp only [fit, hx, ↓reduceIte]
          exact List.mem_cons_of_mem x (ih (cap - bytes x) hkxs hhit')
        · simp [fit, hx] at hhit

/-- A resident suffix key certifies that the whole preceding prefix fits. -/
private theorem mem_fit_append_right (bytes : Nat → Nat) (cap : Nat)
    (pre post : List Nat) {k : Nat} (hk : k ∉ pre) :
    k ∈ fit bytes cap (pre ++ post) →
      (pre.map bytes).sum ≤ cap ∧
        k ∈ fit bytes (cap - (pre.map bytes).sum) post := by
  induction pre generalizing cap with
  | nil =>
      intro hhit
      simpa [fit] using hhit
  | cons x xs ih =>
      intro hhit
      have hkx : k ≠ x := by
        intro h
        apply hk
        simp [h]
      have hkxs : k ∉ xs := fun h => hk (by simp [h])
      by_cases hx : bytes x ≤ cap
      · have hhit' : k ∈ fit bytes (cap - bytes x) (xs ++ post) := by
          simpa only [List.cons_append, fit, hx, ↓reduceIte, List.mem_cons,
            hkx, false_or] using hhit
        have htail := ih (cap - bytes x) hkxs hhit'
        constructor
        · simp only [List.map_cons, List.sum_cons]
          omega
        · simp only [List.map_cons, List.sum_cons]
          have hcap : cap - (bytes x + (xs.map bytes).sum) =
              cap - bytes x - (xs.map bytes).sum := by omega
          simpa only [hcap] using htail.2
      · simp [fit, hx] at hhit

/-- Nonnegative mapped weights are monotone under sublists. -/
private theorem sum_map_sublist_le (bytes : Nat → Nat) {xs ys : List Nat}
    (h : xs.Sublist ys) : (xs.map bytes).sum ≤ (ys.map bytes).sum := by
  induction h with
  | slnil => simp
  | cons _ ih => simp only [List.map_cons, List.sum_cons]; omega
  | cons_cons _ ih => simp only [List.map_cons, List.sum_cons]; omega

/-- Erasing a key cannot increase the discarded scan weight. -/
private theorem discarded_erase_le (bytes : Nat → Nat) (scan xs : List Nat)
    (k : Nat) : discarded bytes scan (xs.erase k) ≤ discarded bytes scan xs := by
  unfold discarded
  have hs : ((xs.erase k).filter (fun x : Nat => scan.contains x)).Sublist
      (xs.filter (fun x : Nat => scan.contains x)) := by
    exact (List.erase_sublist (a := k) (l := xs)).filter
      (fun x : Nat => scan.contains x)
  exact sum_map_sublist_le bytes hs

/-- Removing at most the spare byte budget cannot hide a projected cache hit. -/
private theorem mem_fit_project (bytes : Nat → Nat) (cap : Nat) (scan : List Nat) :
    ∀ (xs : List Nat) (k : Nat), k ∉ scan → discarded bytes scan xs ≤ cap →
      k ∈ fit bytes (cap - discarded bytes scan xs) (project scan xs) →
      k ∈ fit bytes cap xs
  | [], k, _hk, _hd, hhit => by simp [project, fit] at hhit
  | x :: xs, k, hk, hd, hhit => by
      by_cases hxs : x ∈ scan
      · have hdiscard : discarded bytes scan (x :: xs) =
            bytes x + discarded bytes scan xs := by
          simp [discarded, List.filter_cons, hxs]
        have hproj : project scan (x :: xs) = project scan xs := by
          simp [project, List.filter_cons, List.contains_eq_mem, hxs]
        have hxcap : bytes x ≤ cap := by rw [hdiscard] at hd; omega
        have hdtail : discarded bytes scan xs ≤ cap - bytes x := by
          rw [hdiscard] at hd
          omega
        have hsmall : cap - (bytes x + discarded bytes scan xs) =
            (cap - bytes x) - discarded bytes scan xs := by omega
        have htail : k ∈ fit bytes ((cap - bytes x) - discarded bytes scan xs)
            (project scan xs) := by
          simpa only [hdiscard, hproj, hsmall] using hhit
        have hi := mem_fit_project bytes (cap - bytes x) scan xs k hk hdtail htail
        simp only [fit, hxcap, ↓reduceIte]
        exact List.mem_cons_of_mem x hi
      · have hdiscard : discarded bytes scan (x :: xs) = discarded bytes scan xs := by
          simp [discarded, List.filter_cons, hxs]
        have hproj : project scan (x :: xs) = x :: project scan xs := by
          simp [project, List.filter_cons, List.contains_eq_mem, hxs]
        by_cases hkx : k = x
        · subst k
          have hxsmall : bytes x ≤ cap - discarded bytes scan xs := by
            apply Nat.le_of_not_gt
            intro hnot
            simp [hproj, hdiscard, fit] at hhit
            omega
          have hxcap : bytes x ≤ cap := by omega
          simp only [fit, hxcap, ↓reduceIte]
          exact List.mem_cons_self
        · have hxsmall : bytes x ≤ cap - discarded bytes scan xs := by
            apply Nat.le_of_not_gt
            intro hnot
            simp [hproj, hdiscard, fit] at hhit
            omega
          have hparts : k ∈ fit bytes (cap - discarded bytes scan xs - bytes x)
              (project scan xs) := by
            simpa only [hproj, hdiscard, fit, hxsmall, ↓reduceIte, List.mem_cons,
              hkx, false_or] using hhit
          have hxcap : bytes x ≤ cap := by omega
          have hdtail : discarded bytes scan xs ≤ cap - bytes x := by omega
          have hsmall : cap - bytes x - discarded bytes scan xs =
              cap - discarded bytes scan xs - bytes x := by omega
          have htail : k ∈ fit bytes (cap - bytes x - discarded bytes scan xs)
              (project scan xs) := by simpa only [hsmall] using hparts
          have hi := mem_fit_project bytes (cap - bytes x) scan xs k hk hdtail htail
          simp only [fit, hxcap, ↓reduceIte]
          exact List.mem_cons_of_mem x hi

/-- Raising the capacity preserves the complete resident prefix. -/
theorem weightedCacheStackInclusion : WeightedCacheStackInclusion.Goal := by
  intro bytes a b trace hab
  rw [run_eq_fit_stack bytes a [] trace (by simp [fit])]
  rw [run_eq_fit_stack bytes b [] trace (by simp [fit])]
  exact fit_prefix_cap bytes hab _

/-- Shrinking a larger final cache agrees with running at the smaller ceiling. -/
theorem onlineCacheShrink : OnlineCacheShrink.Goal := by
  intro bytes a b trace hab
  rw [run_eq_fit_stack bytes b [] trace (by simp [fit])]
  rw [run_eq_fit_stack bytes a [] trace (by simp [fit])]
  apply List.IsPrefix.eq_of_length
  · exact fit_prefix_same bytes a (fit_prefix_self bytes b (stack trace))
  · apply Nat.le_antisymm
    · exact (fit_prefix_same bytes a (fit_prefix_self bytes b (stack trace))).length_le
    · have hp := fit_prefix_same bytes a (fit_prefix_cap bytes hab (stack trace))
      rw [fit_idempotent] at hp
      exact hp.length_le

/-- An oversized demand empties the cache and cannot increase later misses. -/
theorem oversizeReadBypass : OversizeReadBypass.Goal := by
  intro bytes cap cache k tail hlarge
  have ht : touch bytes cap cache k = [] := by simp [touch, fit, Nat.not_le.mpr hlarge]
  rw [ht]
  exact demandMisses_le_of_prefix bytes cap List.nil_prefix tail

/-- Erasing one occurrence accounts for exactly its weight when it is present. -/
private theorem sum_erase_add (bytes : Nat → Nat) (xs : List Nat) (k : Nat) :
    ((xs.erase k).map bytes).sum + (if k ∈ xs then bytes k else 0) =
      (xs.map bytes).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hkx : k = x
      · subst k
        simp
        omega
      · have hxk : ¬(x == k) = true := by simpa using Ne.symm hkx
        simp only [List.erase_cons_tail hxk, List.map_cons, List.sum_cons,
          List.mem_cons, hkx, false_or, ite_false]
        rw [Nat.add_assoc, ih]

/-- Inserting a key adds at most its own weight to the resident byte sum. -/
private theorem sum_push_le (bytes : Nat → Nat) (xs : List Nat) (k : Nat) :
    ((push xs k).map bytes).sum ≤ (xs.map bytes).sum + bytes k := by
  have h := sum_erase_add bytes xs k
  simp only [push, List.map_cons, List.sum_cons]
  by_cases hk : k ∈ xs
  · rw [if_pos hk] at h
    omega
  · rw [if_neg hk] at h
    omega

/-- Touching a duplicate-free cache leaves it duplicate-free. -/
private theorem touch_nodup (bytes : Nat → Nat) (cap : Nat)
    {xs : List Nat} (h : xs.Nodup) (k : Nat) : (touch bytes cap xs k).Nodup := by
  exact List.Sublist.nodup (fit_prefix_self bytes cap (push xs k)).sublist
    (nodup_push h k)

/-- If all incoming bytes and the old prefix fit, every old key stays pinned. -/
private theorem run_pins_of_budget (bytes : Nat → Nat) (cap : Nat) :
    ∀ (incoming cache : List Nat), cache.Nodup →
      (cache.map bytes).sum + (incoming.map bytes).sum ≤ cap →
      ∀ k, k ∈ cache → k ∈ run bytes cap cache incoming
  | [], cache, hn, _hbudget, k, hk => hk
  | x :: xs, cache, hn, hbudget, k, hk => by
      have hpush : ((push cache x).map bytes).sum ≤ cap := by
        have hle := sum_push_le bytes cache x
        simp only [List.map_cons, List.sum_cons] at hbudget
        omega
      have hfit : touch bytes cap cache x = push cache x :=
        fit_eq_self_of_sum_le bytes cap _ hpush
      have hnext : ((touch bytes cap cache x).map bytes).sum + (xs.map bytes).sum ≤ cap := by
        rw [hfit]
        have hle := sum_push_le bytes cache x
        simp only [List.map_cons, List.sum_cons] at hbudget
        omega
      have hk' : k ∈ touch bytes cap cache x := by
        rw [hfit]
        by_cases hkx : k = x
        · subst k
          simp [push]
        · simp [push, List.mem_erase_of_ne hkx, hk]
      exact run_pins_of_budget bytes cap xs (touch bytes cap cache x)
        (touch_nodup bytes cap hn x) hnext k hk'

/-- The next probe after speculative prefetch must still hit if the prefetch and the entire old prefix fit together. -/
theorem prefetchPinAdmission : PrefetchPinAdmission.Goal := by
  intro bytes cap cache incoming k hk _hnodup hbudget
  exact run_pins_of_budget bytes cap incoming cache _hnodup hbudget k hk

end TinylambOpt.ResearchProofs.ResourcePolicies.ScanCacheRandom.RemainingGoals
