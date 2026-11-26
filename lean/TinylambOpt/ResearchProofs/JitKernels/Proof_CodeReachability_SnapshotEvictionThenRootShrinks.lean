import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# SnapshotEvictionThenRootShrinks (proved)

Claim: after the first collection retains `kept := retain heap (mark heap a
heap.length)` and roots shrink to `b ⊆ a`, a second mark-and-retain on the
trimmed heap coincides with marking the original heap:
`retain kept (mark kept b kept.length) = retain heap (mark heap b heap.length)`.
So the already-trimmed compilation cache can be reused for the next collection.

Proof sketch.  Write `T := mark heap a heap.length`.  By the bounded-closure
argument (duplicate-free `Rev` call paths, pigeonhole through `heap.map
Obj.key`) `grow heap T = T`, so `T` is closed under edges; and `find?_retain`
gives `edges kept k = edges heap k` for every `k ∈ T`.  Roots shrink (`b ⊆ a`)
and marking from `b` stays inside `T` (closure, again via duplicate-free paths),
so a simultaneous induction on fuel shows `mark kept b n = mark heap b n`
(edge tables agree on the marked keys).  Since `kept` only keeps `T`-live
objects, every non-root node of a duplicate-free path from `b` lies in
`kept.map Obj.key`, so the pigeonhole bound improves to `kept.length + 1` and
`mark heap b` already saturates after `kept.length ≤ heap.length` rounds;
filtering `T` out of a `T`-subset (`retain_of_subset`, via `filter_filter`)
then finishes. -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_SnapshotEvictionThenRootShrinks
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private theorem not_true_eq_false {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | false => rfl
  | true => exact absurd rfl h

private theorem contains_of_key_mem (live : List Key) (o : Obj)
    (h : o.key ∈ live) : live.contains o.key = true := by
  rw [List.contains_eq_mem]; exact decide_eq_true h

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

private theorem grow_eq_append (heap : List Obj) (live : List Key) (hl : live.Nodup) :
    ∃ s, grow heap live = live ++ s := by
  refine ⟨((live.flatMap (edges heap)).removeAll live).eraseDups, ?_⟩
  show (live ++ live.flatMap (edges heap)).eraseDups = live ++ _
  rw [List.eraseDups_append, eraseDups_of_nodup live hl]

private theorem mark_prefix (heap : List Obj) (roots : List Key) :
    ∀ (n : Nat), ∀ m ≤ n, ∃ s, mark heap roots n = mark heap roots m ++ s := by
  intro n
  induction n with
  | zero =>
      intro m hm
      cases Nat.eq_zero_of_le_zero hm
      exact ⟨[], (List.append_nil _).symm⟩
  | succ n ih =>
      intro m hm
      cases Nat.lt_or_ge m (n+1) with
      | inr hge =>
          have heq : m = n+1 := Nat.le_antisymm hm hge
          subst heq
          exact ⟨[], (List.append_nil _).symm⟩
      | inl hlt =>
          obtain ⟨s, hs⟩ := ih m (Nat.le_of_lt_succ hlt)
          obtain ⟨t, ht⟩ := grow_eq_append heap (mark heap roots n) (mark_nodup heap roots n)
          rw [← mark_succ, hs] at ht
          exact ⟨s ++ t, ht.trans (List.append_assoc _ _ _ )⟩

/-- A reverse call path: the head is the current key, links go backwards along
`edges heap`, and the last element is a root. -/
private inductive Rev (heap : List Obj) (roots : List Key) : Key → List Key → Prop where
  | root {r} (hr : r ∈ roots) : Rev heap roots r [r]
  | ext {x y xs} (hx : Rev heap roots x xs) (he : y ∈ edges heap x) :
      Rev heap roots y (y :: xs)

private theorem Rev.no_nil (heap : List Obj) (roots : List Key) (x : Key)
    (h : Rev heap roots x []) : False := by cases h

private theorem Rev.head_eq (heap : List Obj) (roots : List Key) (x : Key)
    (tl : List Key) (h : Rev heap roots x tl) : ∃ rest, tl = x :: rest := by
  cases h with
  | root _hr => exact ⟨[], rfl⟩
  | @ext p _y tl2 _hx _he => exact ⟨tl2, rfl⟩

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

private theorem flatMap_congr {f g : Key → List Key} :
    (l : List Key) → (∀ x ∈ l, f x = g x) → l.flatMap f = l.flatMap g
  | [], _ => rfl
  | a :: as, h => by
      rw [List.flatMap_cons, List.flatMap_cons, h a List.mem_cons_self,
        flatMap_congr as (fun x hx => h x (List.Mem.tail a hx))]

private theorem find?_retain (live : List Key) (p : Obj → Bool)
    (h : ∀ o, p o = true → o.key ∈ live) :
    ∀ heap : List Obj, (heap.filter (fun o => live.contains o.key)).find? p = heap.find? p
  | [] => rfl
  | o :: os => by
      by_cases hc : live.contains o.key = true
      · rw [List.filter_cons, if_pos hc, List.find?_cons, List.find?_cons]
        by_cases hp : p o = true
        · rw [hp]
        · rw [not_true_eq_false hp, find?_retain live p h os]
      · have hpnf : p o = false := not_true_eq_false
          (fun hp => hc (contains_of_key_mem live o (h o hp)))
        rw [List.filter_cons, if_neg hc, find?_retain live p h os, List.find?_cons, hpnf]

private theorem retain_of_subset (heap : List Obj) (T S : List Key)
    (hS : ∀ k ∈ S, k ∈ T) :
    retain (retain heap T) S = retain heap S := by
  simp only [retain]
  rw [List.filter_filter]
  refine List.filter_congr (fun o _ => ?_)
  by_cases hc : S.contains o.key = true
  · have hm2 : decide (o.key ∈ S) = true := by
      rw [← List.contains_eq_mem]; exact hc
    have hT : T.contains o.key = true :=
      contains_of_key_mem T o (hS o.key (of_decide_eq_true hm2))
    rw [hc, hT]
    rfl
  · rw [not_true_eq_false hc]
    rfl

theorem sound : SnapshotEvictionThenRootShrinks.Goal := by
  intro heap a b hab kept
  have hkf : kept = heap.filter (fun o => (mark heap a heap.length).contains o.key) := rfl
  -- the first live set is closed under edges
  have hTclosed : grow heap (mark heap a heap.length) = mark heap a heap.length := by
    have hn := mark_nodup heap a heap.length
    refine append_eraseDups_eq_self (mark heap a heap.length)
      ((mark heap a heap.length).flatMap (edges heap)) hn ?_
    intro x hx
    rw [List.mem_flatMap] at hx
    obtain ⟨y, hy, hxy⟩ := hx
    obtain ⟨xs, hxs, _⟩ := mark_rev heap a heap.length y hy
    obtain ⟨ys, hys, _⟩ := Rev.short heap a y xs hxs
    obtain ⟨ws, hws, wnd⟩ := Rev.short heap a x (x :: ys) (Rev.ext hys hxy)
    have hlen := Rev.length_bound heap a x ws hws wnd
    have hmem := Rev.mem_mark heap a x ws hws
    exact mark_mono_fuel heap a x heap.length (ws.length - 1) (by omega) hmem
  -- retained heap is no larger
  have keep_len : kept.length ≤ heap.length := by
    rw [hkf]
    exact List.length_filter_le _ heap
  -- edge tables agree on live keys
  have hedges_same : ∀ k ∈ mark heap a heap.length, edges kept k = edges heap k := by
    intro k hk
    simp only [edges, retain, hkf]
    rw [find?_retain (mark heap a heap.length) (fun o => o.key == k)
      (fun o ho => show o.key ∈ mark heap a heap.length from
        by rw [LawfulBEq.eq_of_beq ho]; exact hk) heap]
  -- marking agrees between `kept` and `heap`, and stays inside the first live set
  have claim : (n : Nat) →
      mark kept b n = mark heap b n ∧ (∀ k ∈ mark heap b n, k ∈ mark heap a heap.length) := by
    intro n
    induction n with
    | zero =>
        constructor
        · show (b.eraseDups : List Key) = b.eraseDups
          rfl
        · intro k hk
          exact mark_mono_fuel heap a k heap.length 0 (by omega)
            (List.mem_eraseDups.mpr (hab k (List.mem_eraseDups.mp hk)))
    | succ n ih =>
        obtain ⟨heq, hmem⟩ := ih
        constructor
        · rw [mark_succ, mark_succ, heq]
          refine congrArg (fun l => (mark heap b n ++ l).eraseDups)
            (flatMap_congr (mark heap b n) (fun x hx => hedges_same x (hmem x hx)))
        · intro k hk
          rw [mark_succ, grow_mem] at hk
          cases hk with
          | inl h => exact hmem k h
          | inr h =>
              obtain ⟨x, hx, he⟩ := h
              have hT : k ∈ grow heap (mark heap a heap.length) :=
                (grow_mem ..).mpr (Or.inr ⟨x, hmem x hx, he⟩)
              rw [hTclosed] at hT
              exact hT
  -- transfer the left-hand side to the original heap
  rw [((claim kept.length).1)]
  show retain (retain heap (mark heap a heap.length)) (mark heap b kept.length) =
    retain heap (mark heap b heap.length)
  refine Eq.trans (retain_of_subset heap (mark heap a heap.length)
    (mark heap b kept.length) (fun k hk => (claim kept.length).2 k hk)) ?_
  -- marking with roots b saturates within `kept.length` rounds
  have hmark : mark heap b kept.length = mark heap b heap.length := by
    obtain ⟨s, hs⟩ := mark_prefix heap b heap.length kept.length keep_len
    have hsn : ∀ z ∈ s, z ∈ mark heap b kept.length := by
      intro z hz
      have hfull : z ∈ mark heap b heap.length := by
        rw [hs]; exact List.mem_append.mpr (Or.inr hz)
      have hzT : z ∈ mark heap a heap.length := (claim heap.length).2 z hfull
      obtain ⟨xs, hxs, _⟩ := mark_rev heap b heap.length z hfull
      obtain ⟨ws, hws, wnd⟩ := Rev.short heap b z xs hxs
      obtain ⟨rest, hw⟩ := Rev.head_eq heap b z ws hws
      subst hw
      obtain ⟨hznr, hrnd⟩ := List.nodup_cons.mp wnd
      have hkeys : ∀ y ∈ (z :: rest : List Key), y = z ∨ y ∈ heap.map Obj.key :=
        Rev.in_map_key heap b z (z :: rest) hws
      have hrest_heap : ∀ y ∈ rest, y ∈ heap.map Obj.key := by
        intro y hy
        cases hkeys y (List.mem_cons.mpr (Or.inr hy)) with
        | inl hyz => exact absurd (hyz ▸ hy) hznr
        | inr h => exact h
      have hlen_h : rest.length ≤ heap.length := by
        have h := Nodup.length_le_keys rest (heap.map Obj.key) hrnd hrest_heap
        rw [List.length_map] at h
        exact h
      have hpath_mem : ∀ y ∈ rest, y ∈ mark heap a heap.length := by
        intro y hy
        have hpath : Rev heap b y ((z :: rest).drop ((z :: rest).idxOf y)) :=
          Rev.sub heap b z (z :: rest) hws y (List.mem_cons.mpr (Or.inr hy))
        refine (claim heap.length).2 y
          (mark_mono_fuel heap b y heap.length
            (((z :: rest).drop ((z :: rest).idxOf y)).length - 1) ?_
            (Rev.mem_mark heap b y ((z :: rest).drop ((z :: rest).idxOf y)) hpath))
        rw [List.length_drop, List.length_cons]
        omega
      have hrest : ∀ y ∈ rest, y ∈ kept.map Obj.key := by
        intro y hy
        obtain ⟨o, ho, hok⟩ := List.mem_map.mp (hrest_heap y hy)
        have ho' : o ∈ kept := by
          rw [hkf, List.mem_filter]
          refine ⟨ho, contains_of_key_mem _ _ ?_⟩
          rw [hok]
          exact hpath_mem y hy
        exact List.mem_map.mpr ⟨o, ho', hok⟩
      have hlen := Nodup.length_le_keys rest (kept.map Obj.key) hrnd hrest
      rw [List.length_map] at hlen
      exact mark_mono_fuel heap b z kept.length ((z :: rest).length - 1)
        (by rw [List.length_cons]; omega) (Rev.mem_mark heap b z (z :: rest) hws)
    have hempty : s = [] := by
      rcases s with _|⟨z, zs⟩
      · rfl
      · have hnd := mark_nodup heap b heap.length
        rw [hs] at hnd
        obtain ⟨_, _, hd⟩ := List.nodup_append.mp hnd
        exfalso
        have hcs : z ∈ (z :: zs : List Key) := List.mem_cons_self
        exact (hd z (hsn z hcs) z hcs) rfl
    rw [hempty, List.append_nil] at hs
    exact hs.symm
  rw [hmark]

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_SnapshotEvictionThenRootShrinks
