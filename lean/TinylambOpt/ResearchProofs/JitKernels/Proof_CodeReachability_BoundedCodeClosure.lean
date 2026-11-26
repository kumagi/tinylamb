import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# BoundedCodeClosure (proved)

Claim: one edge-expansion round more than the heap size changes nothing:
`mark heap roots (heap.length + 1) = mark heap roots heap.length`, i.e. after
`heap.length` grow rounds the live set is already closed under direct calls, so
re-marking the resident graph never needs an extra round.

Proof sketch.  `mark` lists stay duplicate-free (`mark_nodup`).  A private
`Rev` predicate records reverse call paths: a root starts the path `[r]`, and
`Rev y (y :: xs)` extends a path through the reverse edge `y ∈ edges heap x`
(where `x` is the previous head).  Membership in `mark` and existence of a path
of length `≤ fuel + 1` coincide (`Rev.mem_mark`, `mark_rev`, by induction using
the characterization `k ∈ grow h l ↔ k ∈ l ∨ ∃ x ∈ l, k ∈ edges h x`).  Every
reachable key has a *duplicate-free* path: `Rev.sub` turns a path member into
the sub-path `xs.drop (xs.idxOf y)`, and one splices out each repeated head
(`Rev.short`, with `nodup_drop`).  On a duplicate-free path every non-last node
carries an outgoing edge, hence occurs in `heap.map Obj.key`; the pigeonhole
bound `Nodup.length_le_keys` then gives path length `≤ heap.length + 1`, so
every key of `grow heap (mark heap roots heap.length)` has distance
`≤ heap.length` and was already marked (fuel monotonicity).  Finally, appending
keys that are all already members of a duplicate-free list is a no-op for
`eraseDups` (`append_eraseDups_eq_self`, via `eraseDups_append`/`removeAll`). -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_BoundedCodeClosure
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

private theorem grow_mem (heap : List Obj) (live : List Key) (k : Key) :
    k ∈ grow heap live ↔ k ∈ live ∨ ∃ x ∈ live, k ∈ edges heap x := by
  show k ∈ (live ++ live.flatMap (edges heap)).eraseDups ↔ _
  rw [List.mem_eraseDups, List.mem_append, List.mem_flatMap]

private theorem mark_succ (heap : List Obj) (roots : List Key) (n : Nat) :
    mark heap roots (n+1) = grow heap (mark heap roots n) := rfl

private theorem mark_step (heap : List Obj) (roots : List Key) (n : Nat) (k : Key) :
    k ∈ mark heap roots n → k ∈ mark heap roots (n+1) :=
  fun hk => (grow_mem ..).mpr (Or.inl hk)

private theorem mark_mono_fuel (heap : List Obj) (roots : List Key) (k : Key) :
    ∀ m, ∀ n ≤ m, k ∈ mark heap roots n → k ∈ mark heap roots m := by
  intro m
  induction m with
  | zero =>
      intro n hn h
      cases Nat.eq_zero_of_le_zero hn
      exact h
  | succ m ih =>
      intro n hn h
      cases Nat.lt_or_ge n (m+1) with
      | inl hlt => exact mark_step heap roots m k (ih n (Nat.le_of_lt_succ hlt) h)
      | inr hge =>
          have heq : n = m+1 := Nat.le_antisymm hn hge
          rw [heq] at h; exact h

private theorem nodup_eraseDups : (l : List Key) → l.eraseDups.Nodup
  | [] => by rw [List.eraseDups_nil]; exact List.nodup_nil
  | a :: as => by
      rw [List.eraseDups_cons]
      refine List.nodup_cons.mpr ⟨?_, nodup_eraseDups (List.filter (fun b => !b == a) as)⟩
      rw [List.mem_eraseDups, List.mem_filter]
      exact fun ⟨_, h2⟩ => by simpa using h2
  termination_by l => l.length
  decreasing_by
    show (List.filter (fun b => !b == a) as).length < (a :: as).length
    have hle := List.length_filter_le (fun b => !b == a) as
    rw [List.length_cons]
    omega

private theorem mark_nodup (heap : List Obj) (roots : List Key) :
    (n : Nat) → (mark heap roots n).Nodup
  | 0 => nodup_eraseDups roots
  | n+1 => by
      show (grow heap (mark heap roots n)).Nodup
      show ((mark heap roots n ++ (mark heap roots n).flatMap (edges heap)).eraseDups).Nodup
      exact nodup_eraseDups _

private theorem filter_all_false {α : Type} (p : α → Bool) :
    (l : List α) → (∀ x ∈ l, p x = false) → l.filter p = []
  | [], _ => rfl
  | a :: as, h => by
      have hneg : ¬ p a := by
        intro he
        rw [h a List.mem_cons_self] at he
        cases he
      rw [List.filter_cons_of_neg hneg,
        filter_all_false p as (fun x hx => h x (List.Mem.tail a hx))]

private theorem filter_eq_self {α : Type} (p : α → Bool) :
    (l : List α) → (∀ x ∈ l, p x = true) → l.filter p = l
  | [], _ => rfl
  | a :: as, h => by
      have hpos : p a = true := h a List.mem_cons_self
      rw [List.filter_cons_of_pos hpos,
        filter_eq_self p as (fun x hx => h x (List.Mem.tail a hx))]

private theorem eraseDups_of_nodup : (l : List Key) → l.Nodup → l.eraseDups = l
  | [], _ => rfl
  | a :: as, h => by
      obtain ⟨hnm, hnd⟩ := List.nodup_cons.mp h
      rw [List.eraseDups_cons,
        filter_eq_self (fun b => !(b == a)) as
          (fun b hb => by
            rw [not_true_eq_false
              (show ¬((b == a) = true) from
                fun he => absurd ((LawfulBEq.eq_of_beq he : b = a) ▸ hb) hnm)]
            exact Bool.not_false),
        eraseDups_of_nodup as hnd]

private theorem append_eraseDups_eq_self (l s : List Key) (hl : l.Nodup)
    (hs : ∀ x ∈ s, x ∈ l) : (l ++ s).eraseDups = l := by
  rw [List.eraseDups_append, eraseDups_of_nodup l hl]
  have hrm : s.removeAll l = [] := by
    show s.filter (fun x => !l.elem x) = []
    refine filter_all_false (fun x => !l.elem x) s (fun x hx => ?_)
    have : l.elem x = true := by
      rw [List.elem_eq_contains, List.contains_eq_mem, decide_eq_true (hs x hx)]
    rw [this]
    exact Bool.not_true
  rw [hrm, List.eraseDups_nil, List.append_nil]

/-- A reverse call path: the head is the current key, links go backwards along
`edges heap`, and the last element is a root. -/
private inductive Rev (heap : List Obj) (roots : List Key) : Key → List Key → Prop where
  | root {r} (hr : r ∈ roots) : Rev heap roots r [r]
  | ext {x y xs} (hx : Rev heap roots x xs) (he : y ∈ edges heap x) :
      Rev heap roots y (y :: xs)

private theorem Rev.no_nil (heap : List Obj) (roots : List Key) (x : Key)
    (h : Rev heap roots x []) : False := by cases h

private theorem Rev.mem_mark (heap : List Obj) (roots : List Key) (k : Key)
    (xs : List Key) (h : Rev heap roots k xs) :
    k ∈ mark heap roots (xs.length - 1) := by
  cases h with
  | root hr => exact List.mem_eraseDups.mpr hr
  | @ext x _y tl hx he =>
      cases tl with
      | nil => exact False.elim (Rev.no_nil heap roots x hx)
      | cons x' xs' =>
          have ih := Rev.mem_mark heap roots x (x' :: xs') hx
          have hg : k ∈ grow heap (mark heap roots ((x' :: xs').length - 1)) :=
            (grow_mem ..).mpr (Or.inr ⟨x, ih, he⟩)
          rw [show ((k :: x' :: xs').length - 1) = ((x' :: xs').length - 1) + 1 from by
                rw [List.length_cons, List.length_cons]; omega, mark_succ]
          exact hg

private theorem mark_rev (heap : List Obj) (roots : List Key) :
    (n : Nat) → (k : Key) → k ∈ mark heap roots n →
      ∃ xs, Rev heap roots k xs ∧ xs.length ≤ n + 1
  | 0, k, hk => ⟨[k], Rev.root (List.mem_eraseDups.mp hk), by simp⟩
  | n+1, k, hk => by
      rw [mark_succ, grow_mem] at hk
      cases hk with
      | inl h =>
          obtain ⟨xs, hxs, hlen⟩ := mark_rev heap roots n k h
          exact ⟨xs, hxs, by omega⟩
      | inr h =>
          obtain ⟨x, hx, he⟩ := h
          obtain ⟨xs, hxs, hlen⟩ := mark_rev heap roots n x hx
          exact ⟨k :: xs, Rev.ext hxs he, by rw [List.length_cons]; omega⟩

private theorem drop_mem {α : Type} (n : Nat) : (l : List α) →
    ∀ x, x ∈ l.drop n → x ∈ l
  | [], x, hx => by
      rw [List.drop_nil] at hx
      exact absurd hx List.not_mem_nil
  | a :: as, x, hx => by
      cases n with
      | zero => exact hx
      | succ n' => exact List.Mem.tail a (drop_mem n' as x hx)

private theorem nodup_drop {α : Type} : (l : List α) → l.Nodup →
    ∀ (n : Nat), (l.drop n).Nodup
  | [], _, _ => by rw [List.drop_nil]; exact List.nodup_nil
  | a :: as, h, n => by
      obtain ⟨hnm, hnd⟩ := List.nodup_cons.mp h
      cases n with
      | zero => rw [List.drop_zero]; exact h
      | succ n =>
          rw [List.drop_cons (Nat.succ_pos n)]
          exact nodup_drop as hnd n

private theorem Rev.sub (heap : List Obj) (roots : List Key) (k : Key)
    (xs : List Key) (h : Rev heap roots k xs) :
    ∀ y, y ∈ xs → Rev heap roots y (xs.drop (xs.idxOf y)) := by
  cases h with
  | root hr =>
      intro y hy
      obtain rfl := List.mem_singleton.mp hy
      rw [List.idxOf_cons, if_pos (beq_self_eq_true y), List.drop_zero]
      exact Rev.root hr
  | @ext x _y tl hx he =>
      intro z hz
      by_cases hcz : (k == z) = true
      · rw [List.idxOf_cons, if_pos hcz, List.drop_zero]
        obtain rfl := LawfulBEq.eq_of_beq hcz
        exact Rev.ext hx he
      · rw [List.idxOf_cons, if_neg hcz,
            List.drop_cons (Nat.succ_pos (tl.idxOf z))]
        refine Rev.sub heap roots x tl hx z ?_
        have hcase : z = k ∨ z ∈ tl := List.mem_cons.mp hz
        cases hcase with
        | inl hzk =>
            subst hzk
            exact (hcz (beq_self_eq_true z)).elim
        | inr hz' => exact hz'

private theorem Rev.short (heap : List Obj) (roots : List Key) (k : Key)
    (xs : List Key) (h : Rev heap roots k xs) :
    ∃ ys, Rev heap roots k ys ∧ ys.Nodup := by
  cases h with
  | root hr => exact ⟨[k], Rev.root hr, by simp⟩
  | @ext x _y tl hx he =>
      obtain ⟨ys, hys, hnd⟩ := Rev.short heap roots x tl hx
      by_cases hky : k ∈ ys
      · exact ⟨ys.drop (ys.idxOf k), Rev.sub heap roots x ys hys k hky,
          nodup_drop ys hnd (ys.idxOf k)⟩
      · exact ⟨k :: ys, Rev.ext hys he, List.nodup_cons.mpr ⟨hky, hnd⟩⟩

private theorem mem_map_key_of_edge (heap : List Obj) (x y : Key)
    (h : y ∈ edges heap x) : x ∈ heap.map Obj.key := by
  cases h' : heap.find? (fun o => o.key == x) with
  | none =>
      have he : edges heap x = [] := by unfold edges; rw [h']; rfl
      rw [he] at h; exact absurd h List.not_mem_nil
  | some o =>
      obtain ⟨hp, as, bs, heq, _⟩ := List.find?_eq_some_iff_append.mp h'
      have ho : o ∈ heap := by
        rw [heq]; exact List.mem_append.mpr (Or.inr List.mem_cons_self)
      exact List.mem_map.mpr ⟨o, ho, LawfulBEq.eq_of_beq hp⟩

private theorem Nodup.length_le_keys : (l ks : List Key) → l.Nodup →
    (∀ x ∈ l, x ∈ ks) → l.length ≤ ks.length
  | [], _, _, _ => Nat.zero_le _
  | a :: as, ks, h, hall => by
      obtain ⟨hnm, hnd⟩ := List.nodup_cons.mp h
      obtain ⟨as1, bs1, _, heq, herase⟩ :=
        List.exists_erase_eq (hall a List.mem_cons_self)
      have hsub : ∀ x ∈ as, x ∈ as1 ++ bs1 := by
        intro x hx
        have hmem : x ∈ ks := hall x (List.Mem.tail a hx)
        rw [heq, List.mem_append] at hmem
        cases hmem with
        | inl h1 => exact List.mem_append.mpr (Or.inl h1)
        | inr h2 =>
            cases List.mem_cons.mp h2 with
            | inl he => exact absurd (he ▸ hx) hnm
            | inr h3 => exact List.mem_append.mpr (Or.inr h3)
      have hlen := Nodup.length_le_keys as (as1 ++ bs1) hnd hsub
      have hcnt : (as1 ++ bs1).length = ks.length - 1 := by
        rw [← herase, List.length_erase_of_mem (hall a List.mem_cons_self)]
      have hks : 0 < ks.length := List.length_pos_of_mem (hall a List.mem_cons_self)
      rw [List.length_cons]
      omega

private theorem Rev.head_eq (heap : List Obj) (roots : List Key) (x : Key)
    (tl : List Key) (h : Rev heap roots x tl) : ∃ rest, tl = x :: rest := by
  cases h with
  | root _hr => exact ⟨[], rfl⟩
  | @ext p _y tl2 _hx _he => exact ⟨tl2, rfl⟩

private theorem Rev.in_map_key (heap : List Obj) (roots : List Key) (k : Key)
    (tl : List Key) (h : Rev heap roots k tl) :
    ∀ z ∈ tl, z = k ∨ z ∈ heap.map Obj.key := by
  cases h with
  | root _hr =>
      intro z hz
      exact Or.inl (List.mem_singleton.mp hz)
  | @ext x _y tl2 hx he =>
      intro z hz
      cases List.mem_cons.mp hz with
      | inl hzk => exact Or.inl hzk
      | inr hz' =>
          cases Rev.in_map_key heap roots x tl2 hx z hz' with
          | inl hzx => exact Or.inr (hzx ▸ mem_map_key_of_edge heap x k he)
          | inr hz'' => exact Or.inr hz''

private theorem Rev.length_bound (heap : List Obj) (roots : List Key) (k : Key)
    (xs : List Key) (h : Rev heap roots k xs) (hnd : xs.Nodup) :
    xs.length ≤ heap.length + 1 := by
  cases h with
  | root _hr => exact Nat.succ_le_succ (Nat.zero_le _)
  | @ext x _y tl hx he =>
      obtain ⟨_hknm, htnd⟩ := List.nodup_cons.mp hnd
      have hxmap := mem_map_key_of_edge heap x k he
      have hkeys : ∀ z ∈ tl, z = x ∨ z ∈ heap.map Obj.key :=
        Rev.in_map_key heap roots x tl hx
      have hall : ∀ z ∈ tl, z ∈ heap.map Obj.key := by
        intro z hz
        cases hkeys z hz with
        | inl hzx => exact hzx ▸ hxmap
        | inr hz' => exact hz'
      have hlen := Nodup.length_le_keys tl (heap.map Obj.key) htnd hall
      rw [List.length_map] at hlen
      rw [List.length_cons]
      exact Nat.succ_le_succ hlen

theorem sound : BoundedCodeClosure.Goal := by
  intro heap roots
  have hn := mark_nodup heap roots heap.length
  let s := (mark heap roots heap.length).flatMap (edges heap)
  refine append_eraseDups_eq_self (mark heap roots heap.length) s hn ?_
  intro x hx
  rw [List.mem_flatMap] at hx
  obtain ⟨y, hy, hxy⟩ := hx
  obtain ⟨xs, hxs, _hxlen⟩ := mark_rev heap roots heap.length y hy
  obtain ⟨ys, hys, _hnd⟩ := Rev.short heap roots _ _ hxs
  obtain ⟨ws, hws, wnd⟩ := Rev.short heap roots _ _ (Rev.ext hys hxy)
  have hlen := Rev.length_bound heap roots _ _ hws wnd
  have hmem := Rev.mem_mark heap roots _ _ hws
  exact mark_mono_fuel heap roots x heap.length (ws.length - 1) (by omega) hmem

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_BoundedCodeClosure
