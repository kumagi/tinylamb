import TinylambOpt.ResearchGoals.LazyBlockCacheReuse

/-!
Proved

- `Goal` — C++ `ReuseLazyDecodedBlock` (`table/tlcs_file.cpp`): bumping
  `LayoutGeneration` invalidates stale cache entries lazily without eagerly flushing
  the per-block cache, while per-key `evict` drops only that key.

The reference model is defined in `TinylambOpt.ResearchGoals.LazyBlockCacheReuse`.
-/

namespace TinylambOpt.ResearchProofs.LazyBlockCacheReuse

open TinylambOpt.ResearchGoals.LazyBlockCacheReuse

/-! ### Basic helpers -/

private theorem not_eq_true_of_eq_false {c : Bool} (h : c = false) : ¬(c = true) := by
  rw [h]
  exact by decide

private theorem nat_beq_comm (m n : Nat) : (m == n) = (n == m) := by
  by_cases h : m = n
  · subst h
    rfl
  · rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr (fun hh => h hh.symm)]

private theorem key_beq_comm (a b : Key) : (a == b) = (b == a) := by
  cases a with
  | mk a₁ a₂ =>
      cases b with
      | mk b₁ b₂ =>
          obtain h1 := nat_beq_comm a₁ b₁
          obtain h2 := nat_beq_comm a₂ b₂
          show (a₁ == b₁ && a₂ == b₂) = (b₁ == a₁ && b₂ == a₂)
          rw [h1, h2]

private theorem all_helper {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ a ∈ l, p a = true) : l.all p = true := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.all_cons, h a (List.Mem.head _), Bool.true_and]
      exact ih (fun x hx => h x (List.Mem.tail _ hx))

private theorem any_and_const {α : Type} (l : List α) (f : α → Bool) (c : Bool) :
    l.any (fun x => f x && c) = (l.any f && c) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.any_cons, ih, List.any_cons]
      cases c <;> simp

private theorem any_false {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, f a = false) : l.any f = false := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.any_cons, h a (List.Mem.head _),
        ih (fun x hx => h x (List.Mem.tail _ hx))]
      rfl

private theorem any_congr_helper {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ a ∈ l, p a = q a) : l.any p = l.any q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.any_cons, h a (List.Mem.head _),
        ih (fun x hx => h x (List.Mem.tail _ hx)), List.any_cons]

/-! ### lookupCache / eraseKey helpers -/

private theorem lookupCache_cons (k : Key) (e : Key × Nat × Nat)
    (l : List (Key × Nat × Nat)) :
    lookupCache k (e :: l) = if e.1 == k then some e.2 else lookupCache k l := rfl

private theorem lookupCache_some_mem {k : Key} {p : Nat × Nat}
    {l : List (Key × Nat × Nat)} (h : lookupCache k l = some p) : (k, p) ∈ l := by
  induction l with
  | nil => cases h
  | cons e es ih =>
      cases e with
      | mk ek ep =>
          by_cases hd : (ek == k)
          · have h1 : lookupCache k ((ek, ep) :: es) = some ep := by
              rw [lookupCache_cons, if_pos hd]
            rw [h1] at h
            have hep : ep = p := Option.some.inj h
            subst hep
            have hek : ek = k := (beq_iff_eq).mp hd
            subst hek
            exact List.Mem.head _
          · have hd' : (ek == k) = false := Bool.eq_false_of_ne_true hd
            rw [lookupCache_cons, if_neg (not_eq_true_of_eq_false hd')] at h
            exact List.Mem.tail _ (ih h)

private theorem lookupCache_eraseKey_none (k : Key) (l : List (Key × Nat × Nat)) :
    lookupCache k (eraseKey k l) = none := by
  induction l with
  | nil => rfl
  | cons e es ih =>
      cases e with
      | mk ek ep =>
          by_cases hd : (ek == k)
          · have hne : ¬((ek != k) = true) := by
              show ¬(!(ek == k) = true)
              rw [hd]
              decide
            rw [eraseKey]
            rw [List.filter_cons_of_neg (p := fun q => q.1 != k) (a := (ek, ep)) hne]
            exact ih
          · have hd' : (ek == k) = false := Bool.eq_false_of_ne_true hd
            have hp : (ek != k) = true := by
              show !(ek == k) = true
              rw [hd']
              rfl
            rw [eraseKey]
            rw [List.filter_cons_of_pos (p := fun q => q.1 != k) (a := (ek, ep)) hp,
              lookupCache_cons, if_neg (not_eq_true_of_eq_false hd')]
            exact ih

private theorem lookupCache_eraseKey_ne {k k₀ : Key} (hk : (k₀ == k) = false)
    (l : List (Key × Nat × Nat)) :
    lookupCache k (eraseKey k₀ l) = lookupCache k l := by
  induction l with
  | nil => rfl
  | cons e es ih =>
      cases e with
      | mk ek ep =>
          by_cases h1 : (ek == k₀)
          · obtain hek : ek = k₀ := (beq_iff_eq).mp h1
            have hkn : (ek == k) = false := by rw [hek]; exact hk
            have hne : ¬((ek != k₀) = true) := by
              show ¬(!(ek == k₀) = true)
              rw [h1]
              decide
            rw [eraseKey]
            rw [List.filter_cons_of_neg (p := fun q => q.1 != k₀) (a := (ek, ep)) hne]
            show lookupCache k (eraseKey k₀ es) = lookupCache k ((ek, ep) :: es)
            rw [ih, lookupCache_cons, if_neg (not_eq_true_of_eq_false hkn)]
          · have hp : (ek != k₀) = true := by
              have h1' : (ek == k₀) = false := Bool.eq_false_of_ne_true h1
              show !(ek == k₀) = true
              rw [h1']
              rfl
            rw [eraseKey]
            rw [List.filter_cons_of_pos (p := fun q => q.1 != k₀) (a := (ek, ep)) hp]
            show lookupCache k ((ek, ep) :: eraseKey k₀ es) =
              lookupCache k ((ek, ep) :: es)
            rw [lookupCache_cons, lookupCache_cons, ih]

/-! ### Machine definitions -/

private def updMiss (st : CacheState) (k : Key) : CacheState :=
  { st with cache := (k, (st.curGen, st.storage k)) :: st.cache }

private def nextRead (st : CacheState) (k : Key) : CacheState :=
  match lookupCache k st.cache with
  | some (g, v) => if (g == st.curGen) = true then st else updMiss st k
  | none => updMiss st k

private def hitBool (st : CacheState) (k : Key) : Bool :=
  match lookupCache k st.cache with
  | some (g, v) => g == st.curGen
  | none => false

private def next (st : CacheState) : Op → CacheState
  | .read k => nextRead st k
  | .evict k => { st with cache := eraseKey k st.cache }
  | .mutate f => { st with curGen := st.curGen + 1, storage := f }

private def nextAll (st : CacheState) : List Op → CacheState
  | [] => st
  | op :: rest => nextAll (next st op) rest

private def optRun2 : CacheState → List Op → CacheState × List (Nat × Bool)
  | st, [] => (st, [])
  | st, .read k :: rest =>
      match lookupCache k st.cache with
      | some (g, v) =>
          if (g == st.curGen) = true then
            let r := optRun2 st rest
            (r.1, (v, false) :: r.2)
          else
            let r := optRun2 (updMiss st k) rest
            (r.1, (st.storage k, true) :: r.2)
      | none =>
          let r := optRun2 (updMiss st k) rest
          (r.1, (st.storage k, true) :: r.2)
  | st, .evict k :: rest => optRun2 { st with cache := eraseKey k st.cache } rest
  | st, .mutate f :: rest => optRun2 { st with curGen := st.curGen + 1, storage := f } rest

private def flagsFrom : CacheState → List Op → List Bool
  | _, [] => []
  | st, .read k :: rest => (!hitBool st k) :: flagsFrom (next st (.read k)) rest
  | st, .evict k :: rest => flagsFrom (next st (.evict k)) rest
  | st, .mutate f :: rest => flagsFrom (next st (.mutate f)) rest

private def refFlags (full : List Op) : List Op → Nat → List Bool
  | [], _ => []
  | .read k :: rest, idx => (!shouldHitAt full idx k) :: refFlags full rest (idx + 1)
  | .evict _ :: rest, idx => refFlags full rest (idx + 1)
  | .mutate _ :: rest, idx => refFlags full rest (idx + 1)

private def Inv (full : List Op) (st : CacheState) (idx : Nat) : Prop :=
  (∀ k, hitBool st k = shouldHitAt full idx k) ∧
  (∀ e ∈ st.cache, e.2.1 ≤ st.curGen) ∧
  (∀ e ∈ st.cache, e.2.1 = st.curGen → st.storage e.1 = e.2.2)

/-! ### `refRun` step facts -/

private theorem refRun_read (st : Key → Nat) (k : Key) (as : List Op) :
    refRun st (Op.read k :: as) = st k :: refRun st as := rfl

private theorem refRun_evict (st : Key → Nat) (k : Key) (as : List Op) :
    refRun st (Op.evict k :: as) = refRun st as := rfl

private theorem refRun_mutate (st : Key → Nat) (f : Key → Nat) (as : List Op) :
    refRun st (Op.mutate f :: as) = refRun f as := rfl

/-! ### `hitBool` and `next` step facts -/

private theorem hitBool_hit (st : CacheState) (k : Key) (g v : Nat)
    (h : lookupCache k st.cache = some (g, v)) (hd : (g == st.curGen) = true) :
    hitBool st k = true := by
  unfold hitBool
  rw [h]
  show (g == st.curGen) = true
  exact hd

private theorem hitBool_miss (st : CacheState) (k : Key) (g v : Nat)
    (h : lookupCache k st.cache = some (g, v)) (hd : ¬((g == st.curGen) = true)) :
    hitBool st k = false := by
  unfold hitBool
  rw [h]
  show (g == st.curGen) = false
  exact Bool.eq_false_of_ne_true hd

private theorem hitBool_none (st : CacheState) (k : Key)
    (h : lookupCache k st.cache = none) : hitBool st k = false := by
  unfold hitBool
  rw [h]

private theorem nextRead_hit (st : CacheState) (k : Key) (g v : Nat)
    (h : lookupCache k st.cache = some (g, v)) (hd : (g == st.curGen) = true) :
    nextRead st k = st := by
  unfold nextRead
  rw [h]
  show (if (g == st.curGen) = true then st else updMiss st k) = st
  rw [if_pos hd]

private theorem nextRead_none (st : CacheState) (k : Key)
    (h : lookupCache k st.cache = none) : nextRead st k = updMiss st k := by
  unfold nextRead
  rw [h]

private theorem nextRead_stale (st : CacheState) (k : Key) (g v : Nat)
    (h : lookupCache k st.cache = some (g, v)) (hd : ¬((g == st.curGen) = true)) :
    nextRead st k = updMiss st k := by
  unfold nextRead
  rw [h]
  show (if (g == st.curGen) = true then st else updMiss st k) = updMiss st k
  rw [if_neg hd]

private theorem hitBool_upd_miss (st : CacheState) (k₀ k : Key) :
    hitBool (updMiss st k₀) k = (hitBool st k || (k₀ == k)) := by
  show (match lookupCache k ((k₀, (st.curGen, st.storage k₀)) :: st.cache) with
    | some (g, v) => g == st.curGen
    | none => false) = (hitBool st k || (k₀ == k))
  rw [lookupCache_cons]
  by_cases hd : (k₀ == k)
  · obtain heq : k₀ = k := (beq_iff_eq).mp hd
    subst heq
    rw [if_pos (beq_self_eq_true (a := k₀))]
    show (st.curGen == st.curGen) = (hitBool st k₀ || (k₀ == k₀))
    rw [beq_self_eq_true (a := st.curGen), beq_self_eq_true (a := k₀), Bool.or_true]
  · have hd' : (k₀ == k) = false := Bool.eq_false_of_ne_true hd
    rw [if_neg (not_eq_true_of_eq_false hd'), hd', Bool.or_false]
    rfl

private theorem hitBool_next_read (st : CacheState) (k₀ k : Key) :
    hitBool (next st (Op.read k₀)) k = (hitBool st k || (k₀ == k)) := by
  by_cases hb0 : hitBool st k₀
  · have hupd : next st (Op.read k₀) = st := by
      cases hlook : lookupCache k₀ st.cache with
      | none =>
          rw [hitBool_none st k₀ hlook] at hb0
          exact absurd hb0 (by decide)
      | some p =>
          cases p with
          | mk g v =>
              have hd : (g == st.curGen) = true := by
                unfold hitBool at hb0
                rw [hlook] at hb0
                exact hb0
              show nextRead st k₀ = st
              rw [nextRead, hlook]
              show (if (g == st.curGen) = true then st else updMiss st k₀) = st
              rw [if_pos hd]
    rw [hupd]
    by_cases hd : (k₀ == k)
    · obtain heq : k₀ = k := (beq_iff_eq).mp hd
      subst heq
      rw [beq_self_eq_true (a := k₀), Bool.or_true]
      exact hb0
    · rw [Bool.eq_false_of_ne_true hd, Bool.or_false]
  · have hupd : next st (Op.read k₀) = updMiss st k₀ := by
      cases hlook : lookupCache k₀ st.cache with
      | none =>
          show nextRead st k₀ = updMiss st k₀
          rw [nextRead, hlook]
      | some p =>
          cases p with
          | mk g v =>
              have hd : ¬((g == st.curGen) = true) := by
                unfold hitBool at hb0
                rw [hlook] at hb0
                exact hb0
              show nextRead st k₀ = updMiss st k₀
              rw [nextRead, hlook]
              show (if (g == st.curGen) = true then st else updMiss st k₀) = updMiss st k₀
              rw [if_neg hd]
    rw [hupd, hitBool_upd_miss]

private theorem next_evict_eq (st : CacheState) (k : Key) :
    next st (Op.evict k) = { st with cache := eraseKey k st.cache } := rfl

private theorem next_mutate_eq (st : CacheState) (f : Key → Nat) :
    next st (Op.mutate f) = { st with curGen := st.curGen + 1, storage := f } := rfl

private theorem hitBool_next_evict (st : CacheState) (k₀ k : Key) :
    hitBool (next st (Op.evict k₀)) k = (hitBool st k && (k₀ != k)) := by
  show hitBool { st with cache := eraseKey k₀ st.cache } k = (hitBool st k && (k₀ != k))
  by_cases hd : (k₀ == k)
  · obtain heq : k₀ = k := (beq_iff_eq).mp hd
    subst heq
    unfold hitBool
    rw [lookupCache_eraseKey_none]
    have hx : (k₀ != k₀) = false := by
      rw [show (k₀ != k₀) = !((k₀ == k₀)) from rfl, beq_self_eq_true (a := k₀)]
      rfl
    rw [hx, Bool.and_false]
  · have hd' : (k₀ == k) = false := Bool.eq_false_of_ne_true hd
    unfold hitBool
    rw [lookupCache_eraseKey_ne hd']
    have hx : (k₀ != k) = true := by
      show !(k₀ == k) = true
      rw [hd']
      rfl
    rw [hx, Bool.and_true]

private theorem hitBool_next_mutate (st : CacheState) (f : Key → Nat) (k : Key)
    (hbound : ∀ e ∈ st.cache, e.2.1 ≤ st.curGen) :
    hitBool (next st (Op.mutate f)) k = false := by
  show (match lookupCache k st.cache with
    | some (g, v) => g == st.curGen + 1
    | none => false) = false
  cases hlook : lookupCache k st.cache with
  | none => rfl
  | some p =>
      cases p with
      | mk g v =>
          show (g == st.curGen + 1) = false
          refine Bool.eq_false_of_ne_true (fun hd => ?_)
          have hge : g = st.curGen + 1 := (beq_iff_eq).mp hd
          have hle : g ≤ st.curGen := hbound (k, (g, v)) (lookupCache_some_mem hlook)
          omega

/-! ### `noResetBetween` / `shouldHitAt` step facts -/

private theorem noResetBetween_succ_read (full : List Op) (j i : Nat) (k k₀ : Key)
    (hget : full[i]? = some (Op.read k₀)) :
    noResetBetween full j (i + 1) k = noResetBetween full j i k := by
  unfold noResetBetween
  rw [List.range_succ, List.all_append, List.all_cons, List.all_nil, Bool.and_true]
  have hF : (if j < i then
      match full[i]? with
      | some (Op.mutate _) => false
      | some (Op.evict k') => k' != k
      | _ => true
    else true) = true := by
    rw [hget]
    by_cases h : j < i
    · rw [if_pos h]
    · rw [if_neg h]
  exact ((congrArg (fun x => ((List.range i).all (fun m => if j < m then match full[m]? with
      | some (Op.mutate _) => false
      | some (Op.evict k') => k' != k
      | _ => true
      else true)) && x) hF).trans (Bool.and_true _))

private theorem shouldHitAt_succ_read (full : List Op) (k₀ : Key) (i : Nat)
    (hget : full[i]? = some (Op.read k₀)) (k : Key) :
    shouldHitAt full (i + 1) k = (shouldHitAt full i k || (k₀ == k)) := by
  unfold shouldHitAt
  rw [List.range_succ, List.any_append]
  have hA : (List.range i).any (fun j =>
      match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) =
      (List.range i).any (fun j =>
        match full[j]? with
        | some (Op.read k') => (k' == k) && noResetBetween full j i k
        | _ => false) := by
    refine any_congr_helper (List.range i) _ _ (fun j hj => ?_)
    show (match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) =
      (match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j i k
      | _ => false)
    rw [noResetBetween_succ_read full j i k k₀ hget]
  have hB : ([i].any fun j =>
      match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) = (k₀ == k) := by
    rw [List.any_cons, List.any_nil, Bool.or_false, hget]
    show ((k₀ == k) && noResetBetween full i (i + 1) k) = (k₀ == k)
    rw [noResetBetween_succ_read full i i k k₀ hget]
    have hn : noResetBetween full i i k = true := by
      unfold noResetBetween
      refine all_helper (List.range i) _ (fun m hm => ?_)
      rw [List.mem_range] at hm
      rw [if_neg (by omega : ¬(i < m))]
    rw [hn, Bool.and_true]
  exact ((congrArg (fun x => (List.range i).any (fun j =>
      match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) || x) hB).trans
    (congrArg (fun x => x || (k₀ == k)) hA))

private theorem noResetBetween_succ_evict (full : List Op) (j i : Nat) (k k₀ : Key)
    (hget : full[i]? = some (Op.evict k₀)) (hji : j < i) :
    noResetBetween full j (i + 1) k =
      (noResetBetween full j i k && (k₀ != k)) := by
  unfold noResetBetween
  rw [List.range_succ, List.all_append, List.all_cons, List.all_nil, Bool.and_true]
  have hF : (if j < i then
      match full[i]? with
      | some (Op.mutate _) => false
      | some (Op.evict k') => k' != k
      | _ => true
    else true) = (k₀ != k) := by
    rw [if_pos hji, hget]
  exact ((congrArg (fun x => ((List.range i).all (fun m => if j < m then match full[m]? with
      | some (Op.mutate _) => false
      | some (Op.evict k') => k' != k
      | _ => true
      else true)) && x) hF).trans rfl)

private theorem shouldHitAt_succ_evict (full : List Op) (k₀ : Key) (i : Nat)
    (hget : full[i]? = some (Op.evict k₀)) (k : Key) :
    shouldHitAt full (i + 1) k = (shouldHitAt full i k && (k₀ != k)) := by
  unfold shouldHitAt
  rw [List.range_succ, List.any_append]
  have hS : ([i].any fun j =>
      match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) = false := by
    rw [List.any_cons, List.any_nil, Bool.or_false, hget]
  have hA0 : (List.range i).any (fun j =>
      match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) =
      (List.range i).any (fun j =>
        (match full[j]? with
        | some (Op.read k') => (k' == k) && noResetBetween full j i k
        | _ => false) && (k₀ != k)) := by
    refine any_congr_helper (List.range i) _ _ (fun j hj => ?_)
    rw [List.mem_range] at hj
    show (match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) =
      ((match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j i k
      | _ => false) && (k₀ != k))
    cases hopt : full[j]? with
    | none => rfl
    | some o =>
        cases o with
        | read kk =>
            show ((kk == k) && noResetBetween full j (i + 1) k) =
              (((kk == k) && noResetBetween full j i k) && (k₀ != k))
            rw [noResetBetween_succ_evict full j i k k₀ hget hj, Bool.and_assoc]
        | evict kk => rfl
        | mutate ff => rfl
  have hA : (List.range i).any (fun j =>
      match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) =
      ((List.range i).any (fun j =>
        match full[j]? with
        | some (Op.read k') => (k' == k) && noResetBetween full j i k
        | _ => false) && (k₀ != k)) := by
    rw [hA0, any_and_const]
  exact ((congrArg (fun x => ((List.range i).any (fun j => match full[j]? with
        | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
        | _ => false)) || x) hS).trans (Bool.or_false _)).trans hA

private theorem shouldHitAt_succ_mutate (full : List Op) (f : Key → Nat) (i : Nat)
    (hget : full[i]? = some (Op.mutate f)) (k : Key) :
    shouldHitAt full (i + 1) k = false := by
  unfold shouldHitAt
  refine any_false (List.range (i + 1)) _ ?_
  intro j hj
  rw [List.mem_range] at hj
  by_cases hjt : j < i
  · have hn : noResetBetween full j (i + 1) k = false := by
      unfold noResetBetween
      rw [List.range_succ, List.all_append, List.all_cons, List.all_nil, Bool.and_true]
      have hF : (if j < i then
          match full[i]? with
          | some (Op.mutate _) => false
          | some (Op.evict k') => k' != k
          | _ => true
        else true) = false := by
        rw [if_pos hjt, hget]
      exact ((congrArg (fun x => ((List.range i).all (fun m => if j < m then match full[m]? with
          | some (Op.mutate _) => false
          | some (Op.evict k') => k' != k
          | _ => true
          else true)) && x) hF).trans (Bool.and_false _))
    show (match full[j]? with
      | some (Op.read k') => (k' == k) && noResetBetween full j (i + 1) k
      | _ => false) = false
    cases hopt : full[j]? with
    | none => rfl
    | some o =>
        cases o with
        | read kk =>
            rw [hn]
            exact Bool.and_false _
        | evict kk => rfl
        | mutate ff => rfl
  · have hj' : i = j := by omega
    subst hj'
    show (match full[i]? with
      | some (Op.read k') => (k' == k) && noResetBetween full i (i + 1) k
      | _ => false) = false
    rw [hget]

/-! ### `Inv` evolution -/

private theorem next_read_eq (st : CacheState) (k : Key) :
    next st (Op.read k) = nextRead st k := rfl

private theorem next_read_none (st : CacheState) (k : Key)
    (h : lookupCache k st.cache = none) : next st (Op.read k) = updMiss st k := by
  rw [next_read_eq, nextRead_none st k h]

private theorem next_read_hit (st : CacheState) (k : Key) (g v : Nat)
    (h : lookupCache k st.cache = some (g, v)) (hd : (g == st.curGen) = true) :
    next st (Op.read k) = st := by
  rw [next_read_eq, nextRead_hit st k g v h hd]

private theorem next_read_stale (st : CacheState) (k : Key) (g v : Nat)
    (h : lookupCache k st.cache = some (g, v)) (hd : ¬((g == st.curGen) = true)) :
    next st (Op.read k) = updMiss st k := by
  rw [next_read_eq, nextRead_stale st k g v h hd]

private theorem Inv_init (full : List Op) (initStorage : Key → Nat) :
    Inv full ⟨1, initStorage, []⟩ 0 := by
  refine ⟨?_, ?_, ?_⟩
  · intro k
    unfold hitBool shouldHitAt
    rw [List.range_zero, List.any_nil]
    rfl
  · intro e he
    exact absurd he (by simp)
  · intro e he
    exact absurd he (by simp)

private theorem Inv_step_read (full : List Op) (st : CacheState) (k₀ : Key) (idx : Nat)
    (hInv : Inv full st idx) (hget : full[idx]? = some (Op.read k₀)) :
    Inv full (next st (Op.read k₀)) (idx + 1) := by
  refine ⟨?_, ?_, ?_⟩
  · intro k
    rw [hitBool_next_read, shouldHitAt_succ_read full k₀ idx hget k, hInv.1 k]
  · intro e he
    cases hlook : lookupCache k₀ st.cache with
    | none =>
        rw [next_read_none st k₀ hlook] at he ⊢
        cases List.mem_cons.mp he with
        | inl h =>
            subst h
            exact Nat.le_refl _
        | inr htail =>
            exact hInv.2.1 e htail
    | some p =>
        cases p with
        | mk g v =>
            by_cases hd : (g == st.curGen)
            · rw [next_read_hit st k₀ g v hlook hd] at he ⊢
              exact hInv.2.1 e he
            · rw [next_read_stale st k₀ g v hlook hd] at he ⊢
              cases List.mem_cons.mp he with
              | inl h =>
                  subst h
                  exact Nat.le_refl _
              | inr htail =>
                  exact hInv.2.1 e htail
  · intro e he hst
    cases hlook : lookupCache k₀ st.cache with
    | none =>
        rw [next_read_none st k₀ hlook] at he hst ⊢
        cases List.mem_cons.mp he with
        | inl h =>
            subst h
            rfl
        | inr htail =>
            exact hInv.2.2 e htail hst
    | some p =>
        cases p with
        | mk g v =>
            by_cases hd : (g == st.curGen)
            · rw [next_read_hit st k₀ g v hlook hd] at he hst ⊢
              exact hInv.2.2 e he hst
            · rw [next_read_stale st k₀ g v hlook hd] at he hst ⊢
              cases List.mem_cons.mp he with
              | inl h =>
                  subst h
                  rfl
              | inr htail =>
                  exact hInv.2.2 e htail hst

private theorem Inv_step_evict (full : List Op) (st : CacheState) (k₀ : Key) (idx : Nat)
    (hInv : Inv full st idx) (hget : full[idx]? = some (Op.evict k₀)) :
    Inv full (next st (Op.evict k₀)) (idx + 1) := by
  refine ⟨?_, ?_, ?_⟩
  · intro k
    rw [hitBool_next_evict, shouldHitAt_succ_evict full k₀ idx hget k, hInv.1 k]
  · intro e he
    rw [next_evict_eq] at he ⊢
    unfold eraseKey at he
    rw [List.mem_filter] at he
    obtain ⟨hmem, _⟩ := he
    exact hInv.2.1 e hmem
  · intro e he hst
    rw [next_evict_eq] at he hst ⊢
    unfold eraseKey at he
    rw [List.mem_filter] at he
    obtain ⟨hmem, _⟩ := he
    exact hInv.2.2 e hmem hst

private theorem Inv_step_mutate (full : List Op) (st : CacheState) (f : Key → Nat) (idx : Nat)
    (hInv : Inv full st idx) (hget : full[idx]? = some (Op.mutate f)) :
    Inv full (next st (Op.mutate f)) (idx + 1) := by
  refine ⟨?_, ?_, ?_⟩
  · intro k
    rw [shouldHitAt_succ_mutate full f idx hget k,
      hitBool_next_mutate st f k hInv.2.1]
  · intro e he
    have hle := hInv.2.1 e he
    exact Nat.le_trans hle (Nat.le_succ _)
  · intro e he hst
    have hle := hInv.2.1 e he
    rw [hst] at hle
    have hno : ¬((next st (Op.mutate f)).curGen ≤ st.curGen) := by
      show ¬(st.curGen + 1 ≤ st.curGen)
      exact Nat.not_succ_le_self st.curGen
    exact absurd hle hno

/-! ### Step equations for `optRun2` -/

private theorem optRun2_fst_read_none (st : CacheState) (k : Key) (as : List Op)
    (h : lookupCache k st.cache = none) :
    (optRun2 st (Op.read k :: as)).1 = (optRun2 (updMiss st k) as).1 := by
  simp [optRun2, h]

private theorem optRun2_snd_read_none (st : CacheState) (k : Key) (as : List Op)
    (h : lookupCache k st.cache = none) :
    (optRun2 st (Op.read k :: as)).2 =
      (st.storage k, true) :: (optRun2 (updMiss st k) as).2 := by
  simp [optRun2, h]

private theorem optRun2_fst_read_hit (st : CacheState) (k : Key) (g v : Nat) (as : List Op)
    (h : lookupCache k st.cache = some (g, v)) (hd : (g == st.curGen) = true) :
    (optRun2 st (Op.read k :: as)).1 = (optRun2 st as).1 := by
  simp [optRun2, h, hd]

private theorem optRun2_snd_read_hit (st : CacheState) (k : Key) (g v : Nat) (as : List Op)
    (h : lookupCache k st.cache = some (g, v)) (hd : (g == st.curGen) = true) :
    (optRun2 st (Op.read k :: as)).2 = (v, false) :: (optRun2 st as).2 := by
  simp [optRun2, h, hd]

private theorem optRun2_fst_read_stale (st : CacheState) (k : Key) (g v : Nat) (as : List Op)
    (h : lookupCache k st.cache = some (g, v)) (hd : ¬((g == st.curGen) = true)) :
    (optRun2 st (Op.read k :: as)).1 = (optRun2 (updMiss st k) as).1 := by
  simp [optRun2, h, hd]

private theorem optRun2_snd_read_stale (st : CacheState) (k : Key) (g v : Nat) (as : List Op)
    (h : lookupCache k st.cache = some (g, v)) (hd : ¬((g == st.curGen) = true)) :
    (optRun2 st (Op.read k :: as)).2 =
      (st.storage k, true) :: (optRun2 (updMiss st k) as).2 := by
  simp [optRun2, h, hd]

private theorem optRun2_read_evict (st : CacheState) (k : Key) (as : List Op) :
    optRun2 st (Op.evict k :: as) = optRun2 (next st (Op.evict k)) as := rfl

private theorem optRun2_read_mutate (st : CacheState) (f : Key → Nat) (as : List Op) :
    optRun2 st (Op.mutate f :: as) = optRun2 (next st (Op.mutate f)) as := rfl

/-! ### Step equations for the spec `optRun` -/

private theorem optRun_read_none (st : CacheState) (k : Key) (as : List Op)
    (h : lookupCache k st.cache = none) :
    optRun st (Op.read k :: as) =
      (st.storage k, true) :: optRun (updMiss st k) as := by
  simp [optRun, updMiss, h]

private theorem optRun_read_hit (st : CacheState) (k : Key) (g v : Nat) (as : List Op)
    (h : lookupCache k st.cache = some (g, v)) (hd : (g == st.curGen) = true) :
    optRun st (Op.read k :: as) = (v, false) :: optRun st as := by
  simp [optRun, updMiss, h, hd]

private theorem optRun_read_stale (st : CacheState) (k : Key) (g v : Nat) (as : List Op)
    (h : lookupCache k st.cache = some (g, v)) (hd : ¬((g == st.curGen) = true)) :
    optRun st (Op.read k :: as) =
      (st.storage k, true) :: optRun (updMiss st k) as := by
  simp [optRun, updMiss, h, hd]

private theorem optRun_evict (st : CacheState) (k : Key) (as : List Op) :
    optRun st (Op.evict k :: as) = optRun (next st (Op.evict k)) as := rfl

private theorem optRun_mutate (st : CacheState) (f : Key → Nat) (as : List Op) :
    optRun st (Op.mutate f :: as) = optRun (next st (Op.mutate f)) as := rfl

/-! ### Bridging `optRun2` to `optRun` -/

private theorem optRun2_snd : ∀ (st : CacheState) (ops : List Op),
    (optRun2 st ops).2 = optRun st ops := by
  intro st ops
  induction ops generalizing st with
  | nil => rfl
  | cons a as ih =>
      cases a with
      | read k =>
          cases hlook : lookupCache k st.cache with
          | none =>
              rw [optRun2_snd_read_none st k as hlook]
              show (st.storage k, true) :: (optRun2 (updMiss st k) as).2 = _
              rw [ih, optRun_read_none st k as hlook]
          | some p =>
              cases p with
              | mk g v =>
                  by_cases hd : (g == st.curGen)
                  · rw [optRun2_snd_read_hit st k g v as hlook hd]
                    show (v, false) :: (optRun2 st as).2 = _
                    rw [ih, optRun_read_hit st k g v as hlook hd]
                  · rw [optRun2_snd_read_stale st k g v as hlook hd]
                    show (st.storage k, true) :: (optRun2 (updMiss st k) as).2 = _
                    rw [ih, optRun_read_stale st k g v as hlook hd]
      | evict k =>
          rw [optRun2_read_evict st k as, ih, optRun_evict st k as]
      | mutate f =>
          rw [optRun2_read_mutate st f as, ih, optRun_mutate st f as]

/-! ### Step equations for `flagsFrom` / `refFlags` -/

private theorem flagsFrom_read (st : CacheState) (k : Key) (as : List Op) :
    flagsFrom st (Op.read k :: as) =
      (!hitBool st k) :: flagsFrom (next st (Op.read k)) as := rfl

private theorem flagsFrom_evict (st : CacheState) (k : Key) (as : List Op) :
    flagsFrom st (Op.evict k :: as) = flagsFrom (next st (Op.evict k)) as := rfl

private theorem flagsFrom_mutate (st : CacheState) (f : Key → Nat) (as : List Op) :
    flagsFrom st (Op.mutate f :: as) = flagsFrom (next st (Op.mutate f)) as := rfl

private theorem refFlags_read (full : List Op) (k : Key) (as : List Op) (idx : Nat) :
    refFlags full (Op.read k :: as) idx =
      (!shouldHitAt full idx k) :: refFlags full as (idx + 1) := rfl

private theorem refFlags_evict (full : List Op) (k : Key) (as : List Op) (idx : Nat) :
    refFlags full (Op.evict k :: as) idx = refFlags full as (idx + 1) := rfl

private theorem refFlags_mutate (full : List Op) (f : Key → Nat) (as : List Op) (idx : Nat) :
    refFlags full (Op.mutate f :: as) idx = refFlags full as (idx + 1) := rfl

private theorem nextAll_cons (st : CacheState) (op : Op) (as : List Op) :
    nextAll st (op :: as) = nextAll (next st op) as := rfl

/-! ### `refFlags` vs `filterMap` -/

private theorem refFlags_eq_filterMap (full : List Op) :
    ∀ (ops : List Op) (idx : Nat),
      refFlags full ops idx =
        (ops.zipIdx idx).filterMap fun (op, idx) =>
          match op with
          | .read k => some (!shouldHitAt full idx k)
          | _ => none := by
  intro ops
  induction ops with
  | nil => intro idx; rfl
  | cons a as ih =>
      intro idx
      cases a with
      | read k =>
          rw [refFlags_read, List.zipIdx_cons, List.filterMap_cons, ← ih (idx + 1)]
      | evict k =>
          rw [refFlags_evict, List.zipIdx_cons, List.filterMap_cons, ← ih (idx + 1)]
      | mutate f =>
          rw [refFlags_mutate, List.zipIdx_cons, List.filterMap_cons, ← ih (idx + 1)]

/-! ### Main induction -/

private theorem main (full : List Op) :
    ∀ (post : List Op) (pre : List Op) (st : CacheState),
      full = pre ++ post →
      Inv full st pre.length →
      (optRun2 st post).1 = nextAll st post ∧
      ((optRun2 st post).2).map Prod.fst = refRun st.storage post ∧
      ((optRun2 st post).2).map Prod.snd = flagsFrom st post ∧
      flagsFrom st post = refFlags full post pre.length ∧
      Inv full (nextAll st post) (pre.length + post.length) := by
  intro post
  induction post with
  | nil =>
      intro pre st hfull hInv
      refine ⟨rfl, rfl, rfl, rfl, ?_⟩
      rw [List.length_nil, Nat.add_zero]
      exact hInv
  | cons a as ih =>
      intro pre st hfull hInv
      have hlen : (pre ++ [a]).length = pre.length + 1 := by
        rw [List.length_append, List.length_singleton]
      have hlenS : ∀ (op : Op),
          pre.length + (op :: as).length = (pre.length + 1) + as.length := by
        intro op
        rw [show (op :: as).length = as.length + 1 from rfl]
        omega
      have hget : full[pre.length]? = some a := by
        rw [hfull, List.getElem?_append_right (Nat.le_refl pre.length)]
        rw [Nat.sub_self, List.getElem?_cons_zero]
      have hfull' : full = (pre ++ [a]) ++ as := by
        rw [List.append_assoc]
        exact hfull
      cases a with
      | read k =>
          have hInv' : Inv full (next st (Op.read k)) (pre.length + 1) :=
            Inv_step_read full st k pre.length hInv hget
          have hInv'' : Inv full (next st (Op.read k)) (pre ++ [Op.read k]).length := by
            rw [hlen]
            exact hInv'
          have hi := ih (pre ++ [Op.read k]) (next st (Op.read k)) hfull' hInv''
          rw [hlen] at hi
          cases hlook : lookupCache k st.cache with
          | none =>
              have hns : next st (Op.read k) = updMiss st k := next_read_none st k hlook
              rw [hns] at hi
              refine ⟨?_, ?_, ?_, ?_, ?_⟩
              · rw [optRun2_fst_read_none st k as hlook, nextAll_cons, hns]
                exact hi.1
              · rw [optRun2_snd_read_none st k as hlook, List.map_cons, refRun_read]
                exact congrArg (st.storage k :: ·) hi.2.1
              · rw [optRun2_snd_read_none st k as hlook, List.map_cons, flagsFrom_read,
                  hns, hitBool_none st k hlook, Bool.not_false]
                exact congrArg (true :: ·) hi.2.2.1
              · rw [flagsFrom_read, hns, refFlags_read, hInv.1 k]
                rw [hi.2.2.2.1]
              · rw [nextAll_cons, hns, hlenS (Op.read k)]
                exact hi.2.2.2.2
          | some p =>
              cases p with
              | mk g v =>
                  by_cases hd : (g == st.curGen)
                  · have hns : next st (Op.read k) = st :=
                      next_read_hit st k g v hlook hd
                    rw [hns] at hi
                    refine ⟨?_, ?_, ?_, ?_, ?_⟩
                    · rw [optRun2_fst_read_hit st k g v as hlook hd, nextAll_cons, hns]
                      exact hi.1
                    · have hv : st.storage k = v :=
                        hInv.2.2 (k, (g, v)) (lookupCache_some_mem hlook)
                          ((beq_iff_eq).mp hd)
                      rw [optRun2_snd_read_hit st k g v as hlook hd, List.map_cons,
                        refRun_read, ← hv]
                      rw [hi.2.1]
                    · rw [optRun2_snd_read_hit st k g v as hlook hd, List.map_cons,
                        flagsFrom_read, hns, hitBool_hit st k g v hlook hd, Bool.not_true]
                      exact congrArg (false :: ·) hi.2.2.1
                    · rw [flagsFrom_read, hns, refFlags_read, ← hInv.1 k,
                        hitBool_hit st k g v hlook hd, Bool.not_true]
                      rw [hi.2.2.2.1]
                    · rw [nextAll_cons, hns, hlenS (Op.read k)]
                      exact hi.2.2.2.2
                  · have hns : next st (Op.read k) = updMiss st k :=
                      next_read_stale st k g v hlook hd
                    rw [hns] at hi
                    refine ⟨?_, ?_, ?_, ?_, ?_⟩
                    · rw [optRun2_fst_read_stale st k g v as hlook hd, nextAll_cons, hns]
                      exact hi.1
                    · rw [optRun2_snd_read_stale st k g v as hlook hd, List.map_cons,
                        refRun_read]
                      exact congrArg (st.storage k :: ·) hi.2.1
                    · rw [optRun2_snd_read_stale st k g v as hlook hd, List.map_cons,
                        flagsFrom_read, hns, hitBool_miss st k g v hlook hd, Bool.not_false]
                      exact congrArg (true :: ·) hi.2.2.1
                    · rw [flagsFrom_read, hns, refFlags_read, hInv.1 k]
                      rw [hi.2.2.2.1]
                    · rw [nextAll_cons, hns, hlenS (Op.read k)]
                      exact hi.2.2.2.2
      | evict k =>
          have hInv' : Inv full (next st (Op.evict k)) (pre.length + 1) :=
            Inv_step_evict full st k pre.length hInv hget
          have hInv'' : Inv full (next st (Op.evict k)) (pre ++ [Op.evict k]).length := by
            rw [hlen]
            exact hInv'
          have hi := ih (pre ++ [Op.evict k]) (next st (Op.evict k)) hfull' hInv''
          rw [hlen] at hi
          refine ⟨?_, ?_, ?_, ?_, ?_⟩
          · rw [optRun2_read_evict st k as, nextAll_cons]
            exact hi.1
          · rw [optRun2_read_evict st k as, hi.2.1, refRun_evict]; rfl
          · rw [optRun2_read_evict st k as, hi.2.2.1, flagsFrom_evict]
          · rw [flagsFrom_evict, refFlags_evict]
            exact hi.2.2.2.1
          · rw [nextAll_cons, hlenS (Op.evict k)]
            exact hi.2.2.2.2
      | mutate f =>
          have hInv' : Inv full (next st (Op.mutate f)) (pre.length + 1) :=
            Inv_step_mutate full st f pre.length hInv hget
          have hInv'' : Inv full (next st (Op.mutate f)) (pre ++ [Op.mutate f]).length := by
            rw [hlen]
            exact hInv'
          have hi := ih (pre ++ [Op.mutate f]) (next st (Op.mutate f)) hfull' hInv''
          rw [hlen] at hi
          refine ⟨?_, ?_, ?_, ?_, ?_⟩
          · rw [optRun2_read_mutate st f as, nextAll_cons]
            exact hi.1
          · rw [optRun2_read_mutate st f as, hi.2.1, refRun_mutate]; rfl
          · rw [optRun2_read_mutate st f as, hi.2.2.1, flagsFrom_mutate]
          · rw [flagsFrom_mutate, refFlags_mutate]
            exact hi.2.2.2.1
          · rw [nextAll_cons, hlenS (Op.mutate f)]
            exact hi.2.2.2.2

/-! ### The goal -/

theorem sound : Goal := by
  intro initStorage ops
  have h := main ops ops [] ⟨1, initStorage, []⟩ rfl (Inv_init ops initStorage)
  refine ⟨?_, ?_⟩
  · rw [← optRun2_snd ⟨1, initStorage, []⟩ ops, h.2.1]
  · rw [← optRun2_snd ⟨1, initStorage, []⟩ ops, h.2.2.1, h.2.2.2.1]
    exact (refFlags_eq_filterMap ops ops 0).trans rfl

end TinylambOpt.ResearchProofs.LazyBlockCacheReuse
