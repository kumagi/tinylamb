import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc

namespace TinylambOpt.ResearchProofs.Wave8Ldbc

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc
open TinylambOpt.ResearchGoals.RecursiveDelta (index)

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  rcases a with ⟨ac, an, as⟩
  rcases b with ⟨bc, bn, bs⟩
  rcases c with ⟨cc, cn, cs⟩
  simp [plus]
  <;> omega

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  rcases a with ⟨ac, an, as⟩
  simp [plus, zero]

private theorem foldl_summarize (acc : Totals) (xs : List (Option Int)) :
    xs.foldl (fun a v => plus a (weighted 1 v)) acc = plus acc (summarize xs) := by
  induction xs generalizing acc with
  | nil =>
      rcases acc with ⟨ac, an, as⟩
      simp [summarize, plus, zero]
  | cons x xs ih =>
      rw [List.foldl_cons, ih]
      change plus (plus acc (weighted 1 x)) (summarize xs) =
        plus acc (List.foldl (fun a v => plus a (weighted 1 v)) (plus zero (weighted 1 x)) xs)
      rw [plus_zero_left, ih, plus_assoc]

private theorem summarize_append (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  change (xs ++ ys).foldl (fun a v => plus a (weighted 1 v)) zero = _
  rw [List.foldl_append, foldl_summarize]
  rfl

private theorem summarize_replicate (n : Nat) (v : Option Int) :
    summarize (List.replicate n v) = weighted n v := by
  induction n with
  | zero => simp [summarize, weighted, zero]
  | succ n ih =>
      rw [List.replicate_succ]
      change summarize (v :: List.replicate n v) = weighted (n + 1) v
      rw [summarize, List.foldl_cons, plus_zero_left, foldl_summarize, ih]
      cases v with
      | none =>
          simp [weighted, plus]
          omega
      | some z =>
          simp [weighted, plus]
          rw [Int.add_mul, Int.one_mul, Int.add_comm]
          constructor <;> omega

-- TODO(ActiveFriends): unresolved; HashMap fold-over-histogram bookkeeping needs further lemmas.

-- L2 -----------------------------------------------------------------------

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  rcases a with ⟨ac, an, as⟩
  simp [plus, zero]

private theorem weighted_zero (v : Option Int) : weighted 0 v = zero := by
  rcases v with _ | z <;> simp [weighted, zero]

private def carryStep (members : Nat → Nat)
    (s : Option Nat × Nat × Std.HashMap (Nat × Int) Totals × Nat) (r : Message) :=
  let (prev, w, st, n) := s
  let (f, y, v) := r
  let same := prev == some f
  let weight := if same then w else members f
  let updated := if weight == 0 then st else
    st.insert (f, y) (plus (st.getD (f, y) zero) (weighted weight v))
  (some f, weight, updated, n + (!same).toNat)

private theorem carried_eq (members : Nat → Nat) (rows : List Message) :
    carried members rows =
      (let (_,_,out,n) := rows.foldl (carryStep members) (none, 0, ({} : Std.HashMap (Nat × Int) Totals), 0)
       (out, n)) := by
  rfl

private def wOf (members : Nat → Nat) (prev : Option Nat) : Nat :=
  match prev with
  | none => 0
  | some f => members f

private def refRuns (members : Nat → Nat) (rows : List Message) (key : Nat × Int) : Totals :=
  summarize (rows.flatMap fun (f, y, v) =>
    if (f, y) == key then List.replicate (members f) v else [])

private theorem refRuns_nil (members : Nat → Nat) (key : Nat × Int) :
    refRuns members [] key = zero := by
  simp [refRuns, summarize, zero]

private theorem refRuns_cons (members : Nat → Nat) (f : Nat) (y : Int) (v : Option Int)
    (rows : List Message) (key : Nat × Int) :
    refRuns members ((f, y, v) :: rows) key =
      plus (if (f, y) == key then weighted (members f) v else zero)
        (refRuns members rows key) := by
  unfold refRuns
  rw [List.flatMap_cons]
  by_cases h : (f, y) == key
  · simp [h, summarize_append, summarize_replicate]
  · simp [h]
    rw [plus_zero_left]

private theorem step_getD (key : Nat × Int) (st : Std.HashMap (Nat × Int) Totals)
    (f : Nat) (y : Int) (v : Option Int) (weight : Nat) :
  (let updated := if weight == 0 then st else
      st.insert (f, y) (plus (st.getD (f, y) zero) (weighted weight v))
   updated.getD key zero) =
    plus (st.getD key zero) (if (f, y) == key then weighted weight v else zero) := by
  by_cases hw0 : weight == 0
  · have hw0' : weight = 0 := by simpa using hw0
    subst weight
    simp [hw0, weighted_zero, plus_zero_right]
  · by_cases hkey : (f, y) == key
    · have heq : (f, y) = key := by simpa using hkey
      subst key
      simp [hw0, hkey, Std.HashMap.getD_insert]
    · simp [hw0, hkey, Std.HashMap.getD_insert, plus_zero_right]

private theorem carried_groups_aux (members : Nat → Nat) (rows : List Message)
    (key : Nat × Int) (st : Std.HashMap (Nat × Int) Totals) (prev : Option Nat) (w n : Nat)
    (hw : w = wOf members prev) :
  ((rows.foldl (carryStep members) (prev, w, st, n)).2.2.1).getD key zero =
    plus (st.getD key zero) (refRuns members rows key) := by
  induction rows generalizing st prev w n with
  | nil => simp [refRuns_nil, plus_zero_right]
  | cons row rows ih =>
      rcases row with ⟨f, y, v⟩
      simp only [carryStep, List.foldl_cons]
      let same := prev == some f
      let weight := if same then w else members f
      let updated := if weight == 0 then st else
        st.insert (f, y) (plus (st.getD (f, y) zero) (weighted weight v))
      have hweight : weight = members f := by
        by_cases hs : same
        · have hp : prev = some f := by simpa [same] using hs
          simp [weight, hs, hw, wOf, hp]
        · simp [weight, hs]
      have hw' : weight = wOf members (some f) := by simp [wOf, hweight]
      rw [ih (st := updated) (prev := some f) (w := weight) (n := n + (!same).toNat) hw']
      have hstep := step_getD key st f y v weight
      rw [hstep]
      rw [refRuns_cons]
      rw [hweight]
      by_cases hkey : (f, y) == key
      · simp [hkey, plus_assoc]
      · simp [hkey, plus_assoc, plus_zero_left]

private theorem zipIdx_add (rows : List Message) (start : Nat) :
    rows.zipIdx (start + 1) = (rows.zipIdx start).map (fun p => (p.1, p.2 + 1)) := by
  induction rows generalizing start with
  | nil => rfl
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, ih]
      simp only [List.map_cons]

private theorem countP_map {α β : Type} (f : α → β) (p : β → Bool) (l : List α) :
    (l.map f).countP p = l.countP (fun x => p (f x)) := by
  induction l with
  | nil => rfl
  | cons x xs ih => rw [List.map_cons, List.countP_cons, List.countP_cons, ih]

private theorem countP_congr {α : Type} {p q : α → Bool} {l : List α}
    (h : ∀ x ∈ l, p x = q x) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons, h a (by simp),
        ih (by intro x hx; exact h x (by simp [hx]))]

private theorem cons_getElem_map_fst (row : Message) (rows : List Message) (i : Nat) :
    ((row :: rows)[i]?.map Prod.fst) =
      (if i == 0 then some row.1 else rows[i - 1]?.map Prod.fst) := by
  cases i <;> simp

private def probeAux (prev : Option Nat) (rows : List Message) : Nat :=
  rows.zipIdx.countP fun (r, i) =>
    (if i == 0 then prev else rows[i - 1]?.map Prod.fst) != some r.1

private theorem probeAux_cons (prev : Option Nat) (row : Message) (rows : List Message) :
    probeAux prev (row :: rows) =
      (if prev != some row.1 then 1 else 0) + probeAux (some row.1) rows := by
  unfold probeAux
  rw [List.zipIdx_cons, List.countP_cons]
  simp only [Nat.zero_eq, Nat.zero_sub, if_pos rfl, List.getElem?_cons_zero,
    Option.map_some]
  rw [zipIdx_add rows 0, countP_map, Nat.add_comm]
  apply congrArg (fun t => (if prev != some row.1 then 1 else 0) + t)
  apply countP_congr
  intro x hx
  rcases x with ⟨r, i⟩
  simp [cons_getElem_map_fst]

private theorem probeFold_aux (prev : Option Nat) (rows : List Message) (n : Nat) :
    (rows.foldl (fun (p, m) (f, _, _) => (some f, m + (!(p == some f)).toNat)) (prev, n)).2 =
      n + probeAux prev rows := by
  induction rows generalizing prev n with
  | nil => simp [probeAux]
  | cons row rows ih =>
      rcases row with ⟨f, y, v⟩
      simp only [List.foldl_cons]
      change (rows.foldl (fun (p, m) (f, _, _) => (some f, m + (!(p == some f)).toNat))
        (some f, n + (!(prev == some f)).toNat)).2 = _
      rw [ih (prev := some f) (n := n + (!(prev == some f)).toNat), probeAux_cons]
      simp only [Prod.fst]
      have h : (!(prev == some f)).toNat = (if prev != some f then 1 else 0) := by
        by_cases hpf : prev == some f
        · have hp : prev = some f := by simpa using hpf
          simp [hpf, hp]
        · have hp : prev ≠ some f := by intro heq; exact hpf (by simp [heq])
          simp [hpf, hp]
      rw [h]
      omega

private theorem carried_probes_fold (members : Nat → Nat) (rows : List Message)
    (st : Std.HashMap (Nat × Int) Totals) (prev : Option Nat) (w n : Nat) :
  (rows.foldl (carryStep members) (prev, w, st, n)).2.2.2 =
    (rows.foldl (fun (p, m) (f, _, _) => (some f, m + (!(p == some f)).toNat)) (prev, n)).2 := by
  induction rows generalizing st prev w n with
  | nil => rfl
  | cons row rows ih =>
      rcases row with ⟨f, y, v⟩
      simp only [carryStep, List.foldl_cons]
      apply ih

private theorem probeAux_none (rows : List Message) :
    probeAux none rows =
      rows.zipIdx.countP (fun (r, i) => i == 0 || rows[i - 1]?.map Prod.fst != some r.1) := by
  unfold probeAux
  apply countP_congr
  intro x hx
  rcases x with ⟨r, i⟩
  by_cases hi : i == 0 <;> simp [hi]

theorem ForumRuns_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc.ForumRuns.Goal := by
  intro members rows key
  rw [carried_eq]
  constructor
  · dsimp
    rw [carried_groups_aux members rows key ({} : Std.HashMap (Nat × Int) Totals) none 0 0]
    · simp [Std.HashMap.getD_empty, plus_zero_left, refRuns]
    · simp [wOf]
  · dsimp
    rw [carried_probes_fold]
    rw [probeFold_aux none rows 0, probeAux_none]
    simp

private def passes (m : ForumMessage) (f : Nat) : Bool :=
  m.forum == some f && m.year == 2012 && m.parent.isSome

private def block (ms : List ForumMessage) (f : Nat) : List (Option Int) :=
  (ms.filter (fun m => passes m f)).map ForumMessage.score

private theorem lateForumWeight_aux (fs : List Nat) (ms : List ForumMessage) (k : Nat) :
    summarize (fs.flatMap fun f => if f != k then [] else block ms f) =
      let a := summarize (block ms k)
      (⟨a.cnt * fs.count k, a.nn * fs.count k, a.sum * fs.count k⟩ : Totals) := by
  induction fs with
  | nil => simp [summarize, zero]
  | cons f fs ih =>
      by_cases hf : f == k
      · have hfeq : f = k := by simpa using hf
        subst f
        have hne : (k != k) = false := by simp
        simp only [List.flatMap_cons, List.count_cons, if_pos rfl, hne, Bool.false_eq_true,
          ↓reduceIte]
        rw [summarize_append, ih]
        simp only [hf, ↓reduceIte]
        rcases summarize (block ms k) with ⟨ac, an, as⟩
        change (⟨ac + ac * List.count k fs, an + an * List.count k fs,
          as + as * (List.count k fs : Int)⟩ : Totals) =
          ⟨ac * (List.count k fs + 1), an * (List.count k fs + 1),
            as * ((List.count k fs + 1 : Nat) : Int)⟩
        have hcast : ((List.count k fs + 1 : Nat) : Int) = (List.count k fs : Int) + 1 := by simp
        rw [hcast, Int.mul_add, Int.mul_one]
        simp [Nat.mul_add, Nat.mul_one, Int.add_comm] <;> omega
      · have hne : (f != k) = true := by simpa using hf
        rw [List.flatMap_cons, List.count_cons, if_neg hf, Nat.add_zero, if_pos hne,
          List.nil_append]
        exact ih

theorem LateForumWeight_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc.LateForumWeight.Goal := by
  intro fs ms k
  unfold afterGrouping
  change (let a := summarize (block ms k)
      (⟨a.cnt * fs.count k, a.nn * fs.count k, a.sum * fs.count k⟩ : Totals)) = _
  rw [← lateForumWeight_aux]
  rfl

-- L5 -----------------------------------------------------------------------

private theorem histogram_getD_general_nat (m : Std.HashMap Nat Nat) (xs : List Nat) (k : Nat) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 =
      m.getD k 0 + xs.count k := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih, Std.HashMap.getD_insert]
      by_cases h : x == k
      · have hx : x = k := by simpa using h
        subst x
        rw [if_pos h]
        simp [List.count_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · rw [if_neg h]
        simp [h, List.count_cons]

private theorem histogram_getD_nat (xs : List Nat) (k : Nat) :
    (histogram xs).getD k 0 = xs.count k := by
  simpa [histogram] using histogram_getD_general_nat ({} : Std.HashMap Nat Nat) xs k

private theorem count_map_fst (es : List Edge) (i : Nat) :
    (es.map Prod.fst).count i = (es.filter fun e => e.1 == i).length := by
  induction es with
  | nil => simp
  | cons e es ih =>
      rcases e with ⟨u, v⟩
      simp only [List.map_cons, List.count_cons, List.filter_cons]
      by_cases h : u == i <;> simp [h, ih, Nat.add_comm]

private def deg (es : List Edge) := histogram (es.map Prod.fst)

private def pref (es : List Edge) (i : Nat) : Nat :=
  (List.range i).foldl (fun n j => n + (deg es).getD j 0) 0

private theorem offsets_get (es : List Edge) (width i : Nat) (h : i ≤ width) :
    (offsets es width)[i]?.getD 0 = pref es i := by
  unfold offsets
  rw [List.getElem?_toArray, List.getElem?_scanl]
  simp only [List.length_range]
  rw [if_pos h, Option.getD_some]
  congr 1
  rw [List.take_range, Nat.min_eq_left h]

private theorem pref_succ (es : List Edge) (i : Nat) :
    pref es (i + 1) = pref es i + (es.filter fun e => e.1 == i).length := by
  unfold pref
  rw [List.range_succ]
  rw [List.foldl_append]
  simp only [List.foldl_cons, List.foldl_nil]
  rw [show (deg es).getD i 0 = (es.filter fun e => e.1 == i).length by
    simp [deg, histogram_getD_nat, count_map_fst]]

private theorem offsets_list_get (es : List Edge) (width i : Nat) :
    (offsets es width)[i]? =
      (List.scanl (fun n j => n + (deg es).getD j 0) 0 (List.range width))[i]? := by
  simp [offsets, deg]

private theorem offsets_diff (es : List Edge) (width id : Nat)
    (hall : ∀ e ∈ es, e.1 < width) :
    (offsets es width)[id + 1]?.getD 0 - (offsets es width)[id]?.getD 0 =
      (es.filter fun e => e.1 == id).length := by
  by_cases hid : id < width
  · rw [offsets_get es width (id + 1) (by omega), offsets_get es width id (by omega),
      pref_succ]
    rw [← List.countP_eq_length_filter]
    omega
  · have hfilt : es.filter (fun e => e.1 == id) = [] := by
      induction es with
      | nil => rfl
      | cons e es ih =>
          have he : (e.1 == id) = false := by
            by_cases h : e.1 == id
            · have hw := hall e (by simp)
              have heq : e.fst = id := by simpa using h
              omega
            · simpa using h
          simp [he, ih (by intro z hz; exact hall z (by simp [hz]))]
    rw [hfilt]
    simp
    have hnext : (offsets es width)[id + 1]? = none := by
      rw [offsets_list_get]
      apply List.getElem?_eq_none_iff.mpr
      simp
      omega
    rw [hnext]
    simp

private theorem filter_map_const_eq_replicate (es : List Edge) (id : Nat) (age : Option Int) :
    (es.filter (fun e => e.1 == id)).map (fun _ => age) =
      List.replicate ((es.filter fun e => e.1 == id).length) age := by
  induction es with
  | nil => simp
  | cons e es ih =>
      by_cases h : e.1 == id
      · simp [h]
        rw [ih]
        rw [show (List.filter (fun e => e.fst == id) es).length + 1 =
            Nat.succ (List.filter (fun e => e.fst == id) es).length by omega,
          List.replicate_succ]
      · simp [h, ih]

private theorem degreeAvg_fold (dir : Array Nat) (label : String) (acc : Totals) :
    ∀ ps : List Resident,
    ps.foldl (fun st (i, c, age) => if c != label then st else
      plus st (weighted (dir[i + 1]?.getD 0 - dir[i]?.getD 0) age)) acc =
      plus acc (summarize (ps.flatMap fun (id, c, age) =>
        if c == label then List.replicate (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age else [])) := by
  intro ps
  induction ps generalizing acc with
  | nil => simp [summarize, plus_zero_right]
  | cons p ps ih =>
      rcases p with ⟨id, c, age⟩
      simp only [List.foldl_cons, List.flatMap_cons]
      by_cases hcl : c == label
      · have hcl' : c = label := by simpa using hcl
        rw [show (if c != label then acc else
              plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age)) =
              plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age) by simp [hcl']]
        rw [ih (acc := plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age))]
        simp [hcl, summarize_append, summarize_replicate, plus_assoc]
      · have hcl' : c ≠ label := by intro heq; exact hcl (by simp [heq])
        rw [show (if c != label then acc else
              plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age)) = acc by simp [hcl']]
        rw [ih (acc := acc)]
        simp [hcl, plus_assoc, plus_zero_left]

theorem DirectoryDegree_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc.DirectoryDegree.Goal := by
  intro es width ps label hall
  unfold degreeAvg
  rw [degreeAvg_fold]
  simp only [plus_zero_left]
  congr 1
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨id, c, age⟩
      simp only [List.flatMap_cons]
      by_cases hcl : c == label
      · rw [if_pos hcl, if_pos hcl, ih, offsets_diff es width id hall,
          filter_map_const_eq_replicate]
      · rw [if_neg hcl, if_neg hcl, ih]

-- L3 -----------------------------------------------------------------------

private def pos (v : Option Int) : Bool := v.any fun x => 0 < x
private def rng (y : Int) : Bool := decide (2010 ≤ y) && decide (y ≤ 2014)

private theorem rng_true_iff (y : Int) : rng y = true ↔ 2010 ≤ y ∧ y ≤ 2014 := by
  simp [rng]

private def refP (pre : List Reply) (y : Int) : Totals :=
  summarize (pre.flatMap fun a =>
    if a.year == y && rng y then
      (pre.filter fun b => b.parent == some a.id && pos b.score).map Reply.score
    else [])

private def pendP (pre : List Reply) (id : Nat) : Totals :=
  if pre.any (fun a => a.id == id) then zero else
    summarize ((pre.filter fun b => b.parent == some id && pos b.score).map Reply.score)

private def Inv (pre : List Reply) (s : State) : Prop :=
  (∀ id, s.years[id]? = (pre.find? fun a => a.id == id).map Reply.year) ∧
  (∀ id, s.pending.getD id zero = pendP pre id) ∧
  (∀ y, s.groups.getD y zero = refP pre y)

private theorem arrive_years (s : State) (r : Reply) :
    (arrive s r).years = s.years.insert r.id r.year := by
  simp only [arrive, Id.run]
  repeat' (first | split | simp [credit] | rfl)

private theorem find_append_single (pre : List Reply) (r : Reply) (id : Nat) :
    (pre ++ [r]).find? (fun a => a.id == id) =
      match pre.find? (fun a => a.id == id) with
      | some a => some a
      | none => if r.id == id then some r else none := by
  induction pre with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x.id == id
      · simp [h]
      · simp [h, ih]

private theorem find_none_of_not_mem {pre : List Reply} {id : Nat}
    (h : id ∉ pre.map Reply.id) : (pre.find? fun a => a.id == id) = none := by
  induction pre with
  | nil => simp
  | cons x xs ih =>
      have hx : x.id ≠ id := by
        intro heq
        exact h (by simp [heq])
      have ht : id ∉ xs.map Reply.id := by
        intro hm
        exact h (by simp [hm])
      by_cases hbe : x.id == id
      · have : x.id = id := by simpa using hbe
        exact (hx this).elim
      · simp [hbe, ih ht]

private theorem find_some_mem {pre : List Reply} {id : Nat} {a : Reply}
    (h : (pre.find? fun x => x.id == id) = some a) : a ∈ pre := by
  induction pre with
  | nil => simp at h
  | cons x xs ih =>
      by_cases hx : x.id == id
      · simp [hx] at h
        cases h
        simp
      · simp [hx] at h
        simpa using (Or.inr (ih h) : a = x ∨ a ∈ xs)

-- TODO(ForwardReplies): unresolved; the arrival-state invariant (years/pending/groups)
-- needs a full forward/backward/self-link case analysis not completed here.

end TinylambOpt.ResearchProofs.Wave8Ldbc
