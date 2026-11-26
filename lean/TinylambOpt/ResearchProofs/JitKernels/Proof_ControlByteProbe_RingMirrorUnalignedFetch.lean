import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# RingMirrorUnalignedFetch (proven)

A hash table keeps a mirror copy `xs ++ xs` so a SIMD probe can load a
contiguous window of `n ≤ xs.length` lanes starting at any home slot `h`
without wrapping the load.  This proof shows the contiguous window
`((xs ++ xs).drop h).take n` fetches exactly the ring-read semantics
`(ring xs h).take n`: both are the `n`-element list of slots
`xs[(h+i) % xs.length]`, so the engine may replace modular gather logic with
one unaligned sequential read. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_RingMirrorUnalignedFetch
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private theorem filterMap_congr {α β : Type} (l : List α) {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [List.filterMap_cons, List.filterMap_cons, h a (List.mem_cons_self),
        ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

private theorem filterMap_const_none (α β : Type) (l : List α) :
    l.filterMap (fun _ => (none : Option β)) = [] := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [List.filterMap_cons, ih]

private theorem getElem?_some_of_lt {α : Type} : (l : List α) →
    ∀ i, i < l.length → ∃ b, l[i]? = some b
  | [], _, h => absurd h (Nat.not_lt_zero _)
  | a :: as, 0, _ => ⟨a, List.getElem?_cons_zero⟩
  | a :: as, i+1, h => by
      obtain ⟨b, hb⟩ := getElem?_some_of_lt as i (by simpa using h)
      exact ⟨b, by rw [List.getElem?_cons_succ, hb]⟩

private theorem take_eq_filterMap {α : Type} : (m : List α) →
    ∀ n, m.take n = (List.range n).filterMap (fun i => m[i]?)
  | [], _ => by
      rw [List.take_nil,
        show (fun i => ([] : List α)[i]?) = fun _ => none from funext fun _ => List.getElem?_nil,
        filterMap_const_none]
  | a :: as, n => by
      cases n with
      | zero => rfl
      | succ n =>
        rw [List.take_succ_cons]
        simp only [List.range_succ_eq_map, List.filterMap_cons, List.getElem?_cons_zero,
          List.filterMap_map]
        show a :: as.take n = a :: (List.range n).filterMap (fun x => (a :: as)[x+1]?)
        rw [show (fun x => (a :: as)[x+1]?) = (fun x => as[x]?) from
              funext fun x => List.getElem?_cons_succ,
            take_eq_filterMap as n]

private theorem getElem?_drop {α : Type} : (l : List α) →
    ∀ (h i : Nat), (l.drop h)[i]? = l[h+i]?
  | [], h, _ => by cases h <;> rfl
  | a :: as, 0, i => by
      rw [show (a :: as).drop 0 = a :: as from rfl, Nat.zero_add]
  | a :: as, h+1, i => by
      rw [show (a :: as).drop (h+1) = as.drop h from rfl, getElem?_drop as h i,
        show (h+1) + i = (h+i)+1 by omega, List.getElem?_cons_succ]

private theorem take_range_filterMap {β : Type} (f : Nat → Option β) :
    ∀ (m : Nat), (∀ i, i < m → ∃ b, f i = some b) → ∀ n, n ≤ m →
      ((List.range m).filterMap f).take n = (List.range n).filterMap f
  | 0, _, n, hn => by
      obtain rfl : n = 0 := by omega
      rw [List.range_zero, List.filterMap_nil, List.take_zero]
  | m+1, tot, 0, _ => by rw [List.take_zero, List.range_zero, List.filterMap_nil]
  | m+1, tot, n+1, hn => by
      obtain ⟨b0, hb0⟩ := tot 0 (by omega)
      have hcomp : ∀ i, i < m → ∃ b, (f ∘ Nat.succ) i = some b := by
        intro i hi
        obtain ⟨b, hb⟩ := tot (i+1) (by omega)
        exact ⟨b, hb⟩
      have lhs : ((List.range (m+1)).filterMap f).take (n+1)
          = b0 :: (((List.range m).filterMap (f ∘ Nat.succ)).take n) := by
        simp only [List.range_succ_eq_map, List.filterMap_cons, hb0,
          List.take_succ_cons, List.filterMap_map]
      have rhs : (List.range (n+1)).filterMap f
          = b0 :: (List.range n).filterMap (f ∘ Nat.succ) := by
        simp only [List.range_succ_eq_map, List.filterMap_cons, hb0, List.filterMap_map]
      rw [lhs, rhs, take_range_filterMap (f ∘ Nat.succ) m hcomp n (by omega)]

private theorem mod_mirror {k L : Nat} (h1 : L ≤ k) (h2 : k < 2 * L) :
    k % L = k - L := by
  have hpos : 0 < L := by omega
  have hdiv : k / L = 1 := Nat.div_eq_of_lt_le (by omega) (by omega)
  have h3 := Nat.mod_add_div k L
  rw [hdiv, Nat.mul_one] at h3
  omega

theorem sound : RingMirrorUnalignedFetch.Goal := by
  intro xs h n hlt hn
  have H1 : (ring xs h).take n = (List.range n).filterMap (fun i => xs[(h+i)%xs.length]?) := by
    have tot : ∀ i, i < xs.length → ∃ b, xs[(h+i)%xs.length]? = some b := by
      intro i hi
      exact getElem?_some_of_lt xs ((h+i)%xs.length) (Nat.mod_lt _ (by omega))
    show ((List.range xs.length).filterMap (fun i => xs[(h+i)%xs.length]?)).take n
        = (List.range n).filterMap (fun i => xs[(h+i)%xs.length]?)
    rw [take_range_filterMap (fun i => xs[(h+i)%xs.length]?) xs.length tot n hn]
  have H2 : ((xs++xs).drop h).take n
      = (List.range n).filterMap (fun i => (xs++xs)[h+i]?) := by
    rw [take_eq_filterMap, filterMap_congr]
    intro i _
    exact getElem?_drop (xs++xs) h i
  rw [H1, H2]
  refine filterMap_congr (List.range n) (fun i hi => ?_)
  have hi' : i < n := List.mem_range.mp hi
  by_cases hc : h + i < xs.length
  · rw [Nat.mod_eq_of_lt hc, List.getElem?_append_left hc]
  · rw [List.getElem?_append_right (by omega),
        mod_mirror (by omega) (by omega)]
