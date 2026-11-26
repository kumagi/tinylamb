import TinylambOpt.ResearchGoals.ClickstreamLayout.Execution

/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Named results include `sound`.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickstreamLayout.DefaultGroupComplement

open TinylambOpt.ResearchGoals.ClickstreamLayout.Execution
open DefaultGroups

private theorem nat_beq_self : ∀ (n : Nat), (n == n) = true := by
  intro n
  induction n with
  | zero => rfl
  | succ m ih =>
    rw [show (Nat.succ m == Nat.succ m) = (m == m) from rfl]
    exact ih

private theorem intval : ∀ (a b : Int), ((a == b) = true) ↔ a = b := by
  intro a b
  cases a with
  | ofNat n =>
    cases b with
    | ofNat m => simp
    | negSucc m => simp
  | negSucc n =>
    cases b with
    | ofNat m => simp
    | negSucc m => simp

private theorem optval : ∀ (a b : Val), ((a == b) = true) ↔ a = b := by
  intro a b
  rcases a with _|u
  · rcases b with _|v
    · exact ⟨fun _ => rfl, fun _ => rfl⟩
    · simp
  · rcases b with _|v
    · simp
    · rw [show ((some u == some v) = (u == v)) from rfl]
      exact ⟨fun h => congrArg some ((intval u v).mp h),
        fun h => (intval u v).mpr (Option.some.inj h)⟩

private theorem listval : ∀ (a b : List Val), ((a == b) = true) ↔ a = b := by
  intro a
  induction a with
  | nil =>
    intro b
    cases b with
    | nil => exact ⟨fun _ => rfl, fun _ => rfl⟩
    | cons y ys =>
      refine ⟨fun h => ?_, fun h => ?_⟩
      · have hq : (([] : List Val) == y :: ys) = false := rfl
        rw [hq] at h
        exact absurd h (by decide)
      · have h2 := congrArg List.length h
        rw [List.length_nil, List.length_cons] at h2
        omega
  | cons x xs ih =>
    intro b
    cases b with
    | nil =>
      refine ⟨fun h => ?_, fun h => ?_⟩
      · have hq : ((x :: xs : List Val) == []) = false := rfl
        rw [hq] at h
        exact absurd h (by decide)
      · have h2 := congrArg List.length h
        rw [List.length_cons, List.length_nil] at h2
        omega
    | cons y ys =>
      rw [show ((x :: xs == y :: ys) = ((x == y) && (xs == ys))) from rfl]
      rw [Bool.and_eq_true, optval, ih]
      exact ⟨fun ⟨h1, h2⟩ => by rw [h1, h2], List.cons.inj⟩

private theorem lookup_not_mem : ∀ (β : Type) (es : List (Nat × β)) (i : Nat),
    ¬(i ∈ es.map Prod.fst) → es.lookup i = none := by
  intro β es
  induction es with
  | nil => intro i _; rfl
  | cons p es ih =>
    intro i h
    rw [List.lookup_cons]
    have hneg : ¬((i == p.1) = true) := by
      intro hh
      have heq : i = p.1 := Nat.eq_of_beq_eq_true hh
      apply h
      rw [heq]
      exact List.Mem.head (as := List.map Prod.fst es)
    cases h' : (i == p.1) with
    | true => exact False.elim (absurd h' hneg)
    | false => exact ih i (fun hh => h (List.Mem.tail _ hh))

private theorem tupleAt_default : ∀ (lanes : List Lane) (i : Nat),
    ¬(i ∈ touched lanes) → tupleAt lanes i = lanes.map Prod.fst := by
  intro lanes
  induction lanes with
  | nil => intro i _; rfl
  | cons l ls ih =>
    rcases l with ⟨d, es⟩
    intro i h
    have hfull : ¬(i ∈ List.flatMap (fun (x : Lane) => x.2.map Prod.fst) ((d, es) :: ls)) := by
      intro hh
      exact h ((List.mem_eraseDups (l := _)).mpr hh)
    have h1 : ¬(i ∈ es.map Prod.fst) := by
      intro hh
      exact hfull (List.mem_flatMap.mpr ⟨(d, es), List.Mem.head _, hh⟩)
    have h2 : ¬(i ∈ touched ls) := by
      intro hh
      exact hfull (List.mem_append.mpr (Or.inr ((List.mem_eraseDups (l := _)).mp hh)))
    show (es.lookup i).getD d :: tupleAt ls i = d :: ls.map Prod.fst
    rw [lookup_not_mem _ es i h1, ih i h2]
    rfl

private theorem count_filter_compl {α : Type} [BEq α] : ∀ (p : Nat → Bool) (xs : List Nat),
    ∀ (g : Nat → α) (key : α),
      (xs.map g).count key =
        ((xs.filter p).map g).count key + ((xs.filter (fun x => !p x)).map g).count key := by
  intro p xs
  induction xs with
  | nil => intro g key; rfl
  | cons x xs ih =>
    intro g key
    by_cases h : p x = true
    · rw [List.filter_cons_of_pos h,
        List.filter_cons_of_neg (fun hh => by
          simp only [h] at hh
          exact absurd hh (by decide))]
      rw [List.map_cons, List.count_cons, List.map_cons, List.count_cons, ih g key]
      omega
    · rw [List.filter_cons_of_neg h,
        List.filter_cons_of_pos (by
          cases hpx : p x with
          | true => exact absurd hpx h
          | false => rfl)]
      rw [List.map_cons, List.count_cons, List.map_cons, List.count_cons, ih g key]
      omega

private theorem length_filter_compl : ∀ (p : Nat → Bool) (xs : List Nat),
    (xs.filter p).length + (xs.filter (fun x => !p x)).length = xs.length := by
  intro p xs
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    by_cases h : p x = true
    · rw [List.filter_cons_of_pos h,
        List.filter_cons_of_neg (fun hh => by
          simp only [h] at hh
          exact absurd hh (by decide))]
      simp only [List.length_cons]
      omega
    · rw [List.filter_cons_of_neg h,
        List.filter_cons_of_pos (by
          cases hpx : p x with
          | true => exact absurd hpx h
          | false => rfl)]
      simp only [List.length_cons]
      omega

private theorem nodup_filter : ∀ (p : Nat → Bool) (xs : List Nat),
    xs.Nodup → (xs.filter p).Nodup := by
  intro p xs
  induction xs with
  | nil => intro _; exact List.nodup_nil
  | cons x xs ih =>
    intro hnd
    obtain ⟨hxnn, hxs'⟩ := List.nodup_cons.mp hnd
    by_cases h : p x = true
    · rw [List.filter_cons_of_pos h]
      rw [List.nodup_cons]
      refine ⟨fun hh => hxnn (List.mem_filter.mp hh).1, ih hxs'⟩
    · rw [List.filter_cons_of_neg h]
      exact ih hxs'

private theorem commute : ∀ (as : List Nat) (p : Nat → Bool),
    (as.filter p).eraseDups = as.eraseDups.filter p := by
  intro as
  induction as with
  | nil => intro p; rfl
  | cons a l ih =>
    intro p
    by_cases hp : p a = true
    · simp only [List.filter_cons_of_pos hp, List.eraseDups_cons, List.filter_filter,
        ih (fun b => !b == a && p b), ih (fun b => !b == a), ih p]
      refine congrArg (fun t => a :: t) (List.filter_congr (fun b _ => ?_))
      exact Bool.and_comm _ _
    · have hx : p a = false := by
        cases hpx : p a with
        | true => exact absurd hpx hp
        | false => rfl
      simp only [List.filter_cons_of_neg hp, List.eraseDups_cons,
        ih (fun b => !b == a), ih p, List.filter_filter]
      refine List.filter_congr (fun b _ => ?_)
      by_cases hba : (b == a) = true
      · have hbeq : b = a := Nat.eq_of_beq_eq_true hba
        rw [hbeq]
        have hq : !a == a = false := by
          rw [nat_beq_self a]
          rfl
        rw [hq, Bool.and_false, hx]
      · have hq : !b == a = true := by
          rw [Bool.not_eq_true] at hba
          rw [hba]
          rfl
        rw [hq, Bool.and_true]

private theorem nodup_eraseDups : ∀ (as : List Nat), as.eraseDups.Nodup := by
  intro as
  induction as with
  | nil => exact List.nodup_nil
  | cons b bs ih =>
    rw [List.eraseDups_cons, List.nodup_cons]
    refine ⟨?_, ?_⟩
    · intro hh
      rw [List.mem_eraseDups, List.mem_filter] at hh
      have h2 : !(b == b) = true := hh.2
      rw [nat_beq_self] at h2
      exact absurd h2 (by decide)
    · rw [commute]
      exact nodup_filter _ _ ih

private theorem count_erase_head : ∀ (ids : List Nat), ids.Nodup → ∀ (b : Nat), b ∈ ids →
    ∀ {α : Type} [BEq α] (f : Nat → α) (key : α),
      (ids.map f).count key =
        (if (f b == key) = true then 1 else 0) + ((ids.erase b).map f).count key := by
  intro ids
  induction ids with
  | nil =>
    intro hnd b hb α _ f key
    exact absurd hb List.not_mem_nil
  | cons x xs ih =>
    intro hnd b hb α _ f key
    rw [List.map_cons, List.count_cons]
    obtain hbx | ht := List.mem_cons.mp hb
    · rw [hbx]
      rw [List.erase_cons, if_pos (nat_beq_self x)]
      omega
    · obtain ⟨hxnn, hxs'⟩ := List.nodup_cons.mp hnd
      have hxne : ¬(x = b) := fun hh => hxnn (hh ▸ ht)
      have hif : (if (x == b) = true then xs else x :: xs.erase b) = x :: xs.erase b :=
        if_neg (fun hh => hxne (Nat.eq_of_beq_eq_true hh))
      rw [List.erase_cons, hif]
      rw [ih hxs' b ht f key]
      rw [List.map_cons, List.count_cons]
      omega

private theorem count_nodup_same_elems {α : Type} [BEq α] :
    ∀ (xs : List Nat), xs.Nodup → ∀ (ids : List Nat), ids.Nodup →
      (∀ i ∈ xs, i ∈ ids) → (∀ i ∈ ids, i ∈ xs) →
      ∀ (f : Nat → α) (key : α), (xs.map f).count key = (ids.map f).count key := by
  intro xs
  induction xs with
  | nil =>
    intro _ ids hids h1 h2 f key
    have hid : ids = [] := by
      cases ids with
      | nil => rfl
      | cons a as => exact absurd (h2 a (List.Mem.head _)) List.not_mem_nil
    rw [hid]
  | cons x xs ih =>
    intro hxs ids hids h1 h2 f key
    obtain ⟨hxnn, hxs'⟩ := List.nodup_cons.mp hxs
    rw [List.map_cons, List.count_cons]
    rw [count_erase_head ids hids x (h1 x (List.Mem.head _)) f key]
    have h1' : ∀ i ∈ xs, i ∈ ids.erase x := by
      intro i hi
      have hii : i ∈ ids := h1 i (List.Mem.tail _ hi)
      have hix : ¬(i = x) := fun hh => hxnn (hh ▸ hi)
      rw [List.Nodup.mem_erase_iff hids]
      exact ⟨hix, hii⟩
    have h2' : ∀ i ∈ ids.erase x, i ∈ xs := by
      intro i hi
      rw [List.Nodup.mem_erase_iff hids] at hi
      have hmem := h2 i hi.2
      rw [List.mem_cons] at hmem
      rcases hmem with hix' | hxt
      · exact (hi.1 hix').elim
      · exact hxt
    rw [ih hxs' (ids.erase x) (List.Nodup.erase x hids) h1' h2' f key]
    omega

private theorem all_ones_map : ∀ (ys : List Nat),
    ∀ a ∈ ys.map (fun _ => (1 : Nat)), a = 1 := by
  intro ys a ha
  rcases List.mem_map.mp ha with ⟨j, _, rfl⟩
  rfl

private theorem count_all_ones : ∀ (l : List Nat), (∀ a ∈ l, a = 1) → l.count 1 = l.length := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a as ih =>
    intro h
    rw [List.count_cons]
    have ha : a = 1 := h a (List.Mem.head _)
    rw [ha, if_pos (nat_beq_self 1)]
    have h' : ∀ j ∈ as, j = 1 := fun j hj => h j (List.Mem.tail _ hj)
    rw [ih h', List.length_cons]

private theorem count_map_default : ∀ (f : Nat → List Val) (d key : List Val),
    ∀ (ys : List Nat), (∀ y ∈ ys, f y = d) →
      (ys.map f).count key = if (key == d) = true then ys.length else 0 := by
  intro f d key ys
  induction ys with
  | nil =>
    intro _
    by_cases h : (key == d) = true
    · rw [if_pos h]
      rfl
    · rw [if_neg h]
      rfl
  | cons y t ih =>
    intro h
    rw [List.map_cons, List.count_cons,
      ih (fun j hj => h j (List.Mem.tail _ hj)), List.length_cons]
    have hfy : f y = d := h y (List.Mem.head _)
    rw [hfy]
    by_cases hk : (key == d) = true
    · have hd : (d == key) = true :=
        (listval d key).mpr ((listval key d).mp hk).symm
      simp only [if_pos hk, if_pos hd]
    · have hd : ¬((d == key) = true) := fun hh =>
        hk ((listval key d).mpr ((listval d key).mp hh).symm)
      simp only [if_neg hk, if_neg hd]

private theorem main : ∀ (lanes : List Lane) (n : Nat) (key : List Val),
    (∀ l ∈ lanes, ∀ e ∈ l.2, e.1 < n) →
    ((List.range n).map (tupleAt lanes)).count key =
      ((touched lanes).map (tupleAt lanes)).count key +
        (if key == lanes.map Prod.fst then n - (touched lanes).length else 0) := by
  intro lanes n key h
  have hnod : (touched lanes).Nodup := nodup_eraseDups _
  have hlt : ∀ i ∈ touched lanes, i ∈ List.range n := by
    intro i hi
    rw [List.mem_range]
    rw [DefaultGroups.touched] at hi
    rw [List.mem_eraseDups] at hi
    rcases List.mem_flatMap.mp hi with ⟨l, hl, hie⟩
    rcases List.mem_map.mp hie with ⟨e, he, hie'⟩
    rw [← hie']
    exact h l hl e he
  have hcnt_split := count_filter_compl (fun i => (i ∈ touched lanes))
    (List.range n) (tupleAt lanes) key
  have hlen_split := length_filter_compl (fun i => (i ∈ touched lanes)) (List.range n)
  have hAtd : ∀ i ∈ (List.range n).filter (fun i => (i ∈ touched lanes)),
      i ∈ touched lanes := by
    intro i hi
    exact of_decide_eq_true (List.mem_filter.mp hi).2
  have htdA : ∀ i ∈ touched lanes,
      i ∈ (List.range n).filter (fun i => (i ∈ touched lanes)) := by
    intro i hi
    rw [List.mem_filter]
    exact ⟨hlt i hi, decide_eq_true hi⟩
  have hAnod : ((List.range n).filter (fun i => (i ∈ touched lanes))).Nodup :=
    nodup_filter _ _ List.nodup_range
  have hBdef : ∀ y ∈ (List.range n).filter
      (fun x => !((fun i => (i ∈ touched lanes)) x)),
      tupleAt lanes y = lanes.map Prod.fst := by
    intro y hy
    obtain ⟨_, hy'⟩ := List.mem_filter.mp hy
    have hny : ¬(y ∈ touched lanes) := by
      intro hh
      have hd : decide (y ∈ touched lanes) = true := decide_eq_true hh
      rw [hd] at hy'
      exact absurd hy' (by decide)
    exact tupleAt_default lanes y hny
  have hcntA := count_nodup_same_elems
    ((List.range n).filter (fun i => (i ∈ touched lanes))) hAnod
    (touched lanes) hnod hAtd htdA (tupleAt lanes) key
  have h1 : ((List.range n).filter (fun i => (i ∈ touched lanes))
      |>.map fun _ => (1 : Nat)).count 1 =
      ((List.range n).filter (fun i => (i ∈ touched lanes))).length := by
    rw [← List.length_map (fun _ => (1 : Nat))]
    exact count_all_ones _ (all_ones_map _)
  have h2 : ((touched lanes).map fun _ => (1 : Nat)).count 1 = (touched lanes).length := by
    rw [← List.length_map (fun _ => (1 : Nat))]
    exact count_all_ones _ (all_ones_map _)
  have hlenA : ((List.range n).filter (fun i => (i ∈ touched lanes))).length =
      (touched lanes).length := by
    have hc := count_nodup_same_elems
      ((List.range n).filter (fun i => (i ∈ touched lanes))) hAnod
      (touched lanes) hnod hAtd htdA (fun _ => (1 : Nat)) 1
    rw [← h1, ← h2, hc]
  have hlenB : ((List.range n).filter
      (fun x => !(decide (x ∈ touched lanes)))).length =
      n - (touched lanes).length := by
    rw [List.length_range] at hlen_split
    rw [hlenA] at hlen_split
    omega
  have hcntB := count_map_default (tupleAt lanes) (lanes.map Prod.fst) key _ hBdef
  rw [hcnt_split, hcntA, hcntB, hlenB]

theorem sound : Goal := by
  intro lanes n key h
  exact main lanes n key h

end TinylambOpt.ResearchProofs.ClickstreamLayout.DefaultGroupComplement

-- C++ references to this model or its proof.
-- executor/detail/engine_decisions.hpp
