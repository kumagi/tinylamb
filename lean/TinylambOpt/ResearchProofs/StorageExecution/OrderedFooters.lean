import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Generic fold/merge laws -/

namespace Generic

variable {σ : Type} (f : Fold σ)

theorem stitch_nil_left (h : stitch f) (a : List Nat) :
    f.join (f.reference []) (f.reference a) = f.reference a := by
  simpa [List.nil_append] using h [] a

theorem stitch_nil_right (h : stitch f) (a : List Nat) :
    f.join (f.reference a) (f.reference []) = f.reference a := by
  simpa [List.append_nil] using h a []

theorem stitch_assoc (h : stitch f) (a b c : List Nat) :
    f.join (f.join (f.reference a) (f.reference b)) (f.reference c) =
      f.join (f.reference a) (f.join (f.reference b) (f.reference c)) := by
  rw [h a b, h (a ++ b) c, h b c, h a (b ++ c), List.append_assoc]

theorem combine_eq_reference_flatten (h : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  unfold combine
  have haux : ∀ (l : List (List Nat)) (acc : List Nat),
      List.foldl f.join (f.reference acc) (l.map f.reference) =
        f.reference (acc ++ l.flatten) := by
    intro l
    induction l with
    | nil =>
      intro acc
      simp
    | cons p ps ih =>
      intro acc
      simp only [List.map_cons, List.foldl_cons, List.flatten_cons]
      rw [h acc p]
      have ih' := ih (acc ++ p)
      rw [ih']
      congr 1
      simp [List.append_assoc]
  have h0 := haux ps []
  simpa [List.nil_append] using h0

theorem treeReduce_of_stitch (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b ihA ihB =>
    simp only [rootState, plain, ihA, ihB]
    exact h (plain a) (plain b)

private theorem replicate_add_eq {α : Type} (n m : Nat) (a : α) :
    List.replicate (n + m) a = List.replicate n a ++ List.replicate m a := by
  induction n with
  | zero => simp
  | succ n ih =>
    have h1 : n + 1 + m = n + m + 1 := by omega
    rw [h1, List.replicate_succ, ih, List.replicate_succ]
    simp

theorem repeatPower_of_stitch (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ n ih =>
    unfold repeatState
    by_cases hn : n = 0
    · subst hn; simp
    · have hpos : 0 < n := Nat.pos_of_ne_zero hn
      have hlt : n / 2 < n := Nat.div_lt_self hpos (by decide)
      have ihHalf := ih (n / 2) hlt
      have hjoin : f.join (f.reference (List.replicate (n / 2) xs).flatten)
          (f.reference (List.replicate (n / 2) xs).flatten) =
          f.reference (List.replicate (n / 2 * 2) xs).flatten := by
        rw [h]
        rw [show (List.replicate (n / 2) xs).flatten ++ (List.replicate (n / 2) xs).flatten = (List.replicate (n / 2 * 2) xs).flatten from by
          rw [show n / 2 * 2 = n / 2 + n / 2 by omega, replicate_add_eq, List.flatten_append]]
      have hif : (if hn0 : n = 0 then f.reference []
          else let a := repeatState f (f.reference xs) (n / 2)
               let sq := f.join a a
               if n % 2 == 0 then sq else f.join sq (f.reference xs)) =
          f.reference (List.replicate n xs).flatten := by
        simp only [hn, ↓reduceDIte, ihHalf]
        by_cases hmod : n % 2 == 0
        · have hmod' : n % 2 = 0 := by simpa using hmod
          have hn2 : n / 2 * 2 = n := by omega
          simp only [hmod, ↓reduceIte]
          rw [hjoin, hn2]
        · have hmod' : n % 2 ≠ 0 := by simpa using hmod
          have hmod1 : n % 2 = 1 := by omega
          have hn2 : n / 2 * 2 + 1 = n := by omega
          have hne : (n % 2 == 0) = false := by cases hc : (n % 2 == 0) <;> simp_all
          simp only [hne, Bool.false_eq_true, ↓reduceIte]
          rw [hjoin, h]
          congr 1
          rw [show (List.replicate n xs).flatten = (List.replicate (n / 2 * 2 + 1) xs).flatten from by rw [hn2]]
          rw [show List.replicate (n / 2 * 2 + 1) xs = List.replicate (n / 2 * 2) xs ++ List.replicate 1 xs from replicate_add_eq _ _ _]
          simp [List.flatten_append, List.replicate, List.flatten]
      exact hif


theorem frameSlice_of_stitch (h : stitch f) : frameSlice f := by
  intro a ps z lo hi hlo
  have hcomb := combine_eq_reference_flatten f h ps
  have hlist : (((a ++ ps.flatten ++ z).drop lo).take (a.length - lo + ps.flatten.length + hi)) =
      a.drop lo ++ ps.flatten ++ z.take hi := by
    have h1 : (a ++ ps.flatten ++ z).drop lo = a.drop lo ++ ps.flatten ++ z := by
      rw [show a ++ ps.flatten ++ z = a ++ (ps.flatten ++ z) by simp [List.append_assoc]]
      rw [List.drop_append_of_le_length hlo]
      simp [List.append_assoc]
    rw [h1]
    have hz : z = z.take hi ++ z.drop hi := (List.take_append_drop hi z).symm
    rw [hz]
    simp only [List.append_assoc]
    by_cases hhi : hi ≤ z.length
    · have hlen : a.length - lo + ps.flatten.length + hi ≤ (a.drop lo ++ ps.flatten ++ z.take hi).length := by
        simp [List.length_append, List.length_drop, List.length_take, Nat.min_eq_left hhi]
        omega
      have htake := List.take_append_of_le_length (l₁ := a.drop lo ++ ps.flatten ++ z.take hi) (l₂ := z.drop hi) hlen
      have hlen2 : (a.drop lo ++ ps.flatten ++ z.take hi).length ≤ a.length - lo + ps.flatten.length + hi := by
        simp [List.length_append, List.length_drop, List.length_take, Nat.min_eq_left hhi]
        omega
      rw [List.take_of_length_le hlen2] at htake
      simpa [List.append_assoc] using htake
    · have hhi' : z.length ≤ hi := Nat.le_of_not_ge hhi
      rw [List.take_of_length_le hhi', List.drop_eq_nil_of_le hhi']
      simp only [List.append_nil, List.append_assoc]
      have hlen : (a.drop lo ++ ps.flatten ++ z).length ≤ a.length - lo + ps.flatten.length + hi := by
        simp [List.length_append, List.length_drop]; omega
      simp only [List.append_assoc] at hlen ⊢
      rw [List.take_of_length_le hlen]
      simp [List.take_of_length_le hhi']
  rw [hcomb, h ps.flatten (z.take hi), h (a.drop lo) (ps.flatten ++ z.take hi), hlist]
  simp [List.append_assoc]


theorem prefixCheckpoint_of_stitch (h : stitch f) : prefixCheckpoint f := by
  intro init ps
  have haux : ∀ (l : List (List Nat)) (acc : σ) (pref : List Nat),
      acc = f.reference pref →
      List.scanl f.join acc (l.map f.reference) =
        (List.range (l.length + 1)).map (fun i => f.reference (pref ++ (l.take i).flatten)) := by
    intro l
    induction l with
    | nil =>
      intro acc pref hacc
      subst hacc
      simp
    | cons p ps ih =>
      intro acc pref hacc
      subst hacc
      simp only [List.map_cons, List.scanl_cons, List.length_cons, List.range_succ_eq_map, List.map_cons,
        Nat.zero_add, List.take_zero, List.flatten_nil, List.append_nil, List.take_succ_cons, List.flatten_cons]
      congr 1
      have hstep : f.join (f.reference pref) (f.reference p) = f.reference (pref ++ p) := h _ _
      have ih' := ih (f.reference (pref ++ p)) (pref ++ p) rfl
      rw [hstep, ih']
      simp [List.map_map, Function.comp, List.flatten_cons, List.append_assoc, List.take_succ_cons, List.range_succ_eq_map]
  exact haux ps (f.reference init) init rfl


theorem amendLeaf_of_stitch (h : stitch f) : amendLeaf f := by
  intro ps i xs hi
  have hc1 := combine_eq_reference_flatten f h (ps.take i)
  have hc2 := combine_eq_reference_flatten f h (ps.drop (i + 1))
  have hc3 := combine_eq_reference_flatten f h (ps.set i xs)
  have hflat : (ps.set i xs).flatten = (ps.take i).flatten ++ xs ++ (ps.drop (i + 1)).flatten := by
    have hdecomp : ps.set i xs = ps.take i ++ [xs] ++ ps.drop (i + 1) := by
      conv => lhs; rw [show ps.set i xs = (ps.set i xs).take i ++ (ps.set i xs).drop i from (List.take_append_drop i (ps.set i xs)).symm]
      rw [List.take_set_of_le (by omega)]
      have hdropi : (ps.set i xs).drop i = xs :: ps.drop (i + 1) := by
        rw [List.drop_eq_getElem_cons (by simpa [List.length_set] using hi : i < (ps.set i xs).length)]
        simp only [List.getElem_set_self]
        congr 1
        apply List.ext_getElem
        · simp [List.length_drop, List.length_set]
        · intro j hj1 hj2
          simp only [List.getElem_drop, List.getElem_set, List.length_set]
          have hne : i + 1 + j ≠ i := by omega
          have hne' : i = i + 1 + j ↔ False := by
            constructor
            · intro h'; exact hne (h'.symm)
            · intro h'; exact h'.elim
          simp [hne']
      rw [hdropi]
      simp [List.append_assoc]
    rw [hdecomp]
    simp [List.flatten_append, List.append_assoc]
  rw [hc1, hc2, hc3, hflat]
  rw [h xs ((ps.drop (i + 1)).flatten), h ((ps.take i).flatten) (xs ++ ((ps.drop (i + 1)).flatten))]
  simp [List.append_assoc]


theorem runInput_of_stitch (h : stitch f) : runInput f := by
  intro rs
  have haux : ∀ (l : List (Nat × Nat)) (acc : σ) (pref : List Nat),
      acc = f.reference pref →
      List.foldl f.join acc (l.map (fun (v, n) => repeatState f (f.reference [v]) n)) =
        f.reference (pref ++ l.flatMap (fun (v, n) => List.replicate n v)) := by
    intro l
    induction l with
    | nil =>
      intro acc pref hacc
      subst hacc
      simp
    | cons p ps ih =>
      intro acc pref hacc
      obtain ⟨v, n⟩ := p
      subst hacc
      simp only [List.map_cons, List.foldl_cons, List.flatMap_cons]
      have hrep := repeatPower_of_stitch f h [v] n
      have hrep' : repeatState f (f.reference [v]) n = f.reference (List.replicate n v) := by
        have : (List.replicate n [v]).flatten = List.replicate n v := by
          induction n with
          | zero => simp
          | succ n ih => simp [List.replicate_succ, ih]
        simpa [this] using hrep
      rw [hrep', h _ _]
      have ih' := ih (f.reference (pref ++ List.replicate n v)) (pref ++ List.replicate n v) rfl
      simpa [List.append_assoc] using ih'
  simpa using haux rs (f.reference []) []

theorem reorderNeighbors_of_stitch (h : stitch f) : reorderNeighbors f := by
  intro pre a b suf hcomm
  have h3 : f.reference (pre ++ a ++ b ++ suf) =
      f.join (f.join (f.join (f.reference pre) (f.reference a)) (f.reference b)) (f.reference suf) := by
    calc f.reference (pre ++ a ++ b ++ suf)
        = f.join (f.reference (pre ++ a ++ b)) (f.reference suf) := by rw [← h (pre ++ a ++ b) suf]
      _ = f.join (f.join (f.reference (pre ++ a)) (f.reference b)) (f.reference suf) := by rw [← h (pre ++ a) b]
      _ = f.join (f.join (f.join (f.reference pre) (f.reference a)) (f.reference b)) (f.reference suf) := by rw [← h pre a]
  have h6 : f.reference (pre ++ b ++ a ++ suf) =
      f.join (f.join (f.join (f.reference pre) (f.reference b)) (f.reference a)) (f.reference suf) := by
    calc f.reference (pre ++ b ++ a ++ suf)
        = f.join (f.reference (pre ++ b ++ a)) (f.reference suf) := by rw [← h (pre ++ b ++ a) suf]
      _ = f.join (f.join (f.reference (pre ++ b)) (f.reference a)) (f.reference suf) := by rw [← h (pre ++ b) a]
      _ = f.join (f.join (f.join (f.reference pre) (f.reference b)) (f.reference a)) (f.reference suf) := by rw [← h pre b]
  rw [h3, h6, stitch_assoc f h pre a b, hcomm, ← stitch_assoc f h pre b a]

end Generic


open Generic

private theorem List_min?_append (a b : List Nat) :
    (a ++ b).min? = match a.min?, b.min? with
      | none, y => y
      | x, none => x
      | some x, some y => some (min x y) := by
  induction a with
  | nil => simp [List.min?]
  | cons x xs ih =>
    simp only [List.cons_append, List.min?_cons, ih]
    cases hxs : xs.min? <;> cases hxb : b.min? <;> simp [hxs, hxb, Nat.min_assoc, Nat.min_left_comm, Nat.min_comm]

private theorem List_max?_append (a b : List Nat) :
    (a ++ b).max? = match a.max?, b.max? with
      | none, y => y
      | x, none => x
      | some x, some y => some (max x y) := by
  induction a with
  | nil => simp [List.max?]
  | cons x xs ih =>
    simp only [List.cons_append, List.max?_cons, ih]
    cases hxs : xs.max? <;> cases hxb : b.max? <;> simp [hxs, hxb, Nat.max_assoc, Nat.max_left_comm, Nat.max_comm]

/-! ## LongestHole -/

namespace LongestHole
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole

private theorem takeWhile_append_true {p : Nat → Bool} (a b : List Nat)
    (h : ∀ x ∈ a, p x = true) : (a ++ b).takeWhile p = a ++ b.takeWhile p := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    simp only [List.cons_append, List.takeWhile_cons]
    have hx : p x = true := h x (by simp)
    simp [hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_all_true {p : Nat → Bool} (a : List Nat)
    (h : ∀ x ∈ a, p x = true) : a.takeWhile p = a := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    have hx : p x = true := h x (by simp)
    simp [List.takeWhile_cons, hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_append_false {p : Nat → Bool} (a b : List Nat)
    (h : ∃ x ∈ a, p x = false) : (a ++ b).takeWhile p = a.takeWhile p := by
  induction a with
  | nil => rcases h with ⟨x, hx, _⟩; simp at hx
  | cons x xs ih =>
    simp only [List.cons_append, List.takeWhile_cons]
    by_cases hx : p x = true
    · simp [hx]
      have h' : ∃ y ∈ xs, p y = false := by
        rcases h with ⟨y, hy, hfy⟩
        simp at hy
        rcases hy with rfl | hy
        · simp [hx] at hfy
        · exact ⟨y, hy, hfy⟩
      exact ih h'
    · have hx' : p x = false := by cases hp : p x <;> simp_all
      simp [hx']

private theorem mem_takeWhile_prop {p : Nat → Bool} {l : List Nat} {x : Nat}
    (h : x ∈ l.takeWhile p) : p x = true := by
  induction l with
  | nil => simp at h
  | cons a as ih =>
    simp only [List.takeWhile_cons] at h
    split at h
    · simp at h
      rcases h with rfl | h
      · assumption
      · exact ih h
    · simp at h

private theorem takeWhile_eq_take_length {p : Nat → Bool} (l : List Nat) :
    l.takeWhile p = l.take (l.takeWhile p).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
    by_cases hp : p a = true
    · have hk : (as.takeWhile p).length ≤ as.length := (List.takeWhile_sublist (p := p) (l := as)).length_le
      simp only [List.takeWhile_cons, hp, ite_true, ih, List.length_cons, List.take_cons_succ, Nat.min_eq_left hk]
    · have hp' : p a = false := by cases h : p a <;> simp_all
      simp only [List.takeWhile_cons, hp', Bool.false_eq_true, ite_false, List.take_zero, List.length_nil]

private theorem le_takeWhile_length {p : Nat → Bool} {l : List Nat} {n : Nat}
    (hn : n ≤ l.length) (hall : ∀ i (hi : i < n), p (l[i]'(by omega)) = true) :
    n ≤ (l.takeWhile p).length := by
  induction l generalizing n with
  | nil => simp only [List.length_nil] at hn; omega
  | cons a as ih =>
    cases n with
    | zero => simp
    | succ n =>
      have ha : p a = true := by simpa using hall 0 (Nat.zero_lt_succ n)
      have htail : ∀ i (hi : i < n), p (as[i]'(by simp only [List.length_cons] at hn; omega)) = true := by
        intro i hi
        have hi' : i + 1 < n + 1 := Nat.succ_lt_succ hi
        simpa using hall (i + 1) hi'
      have hlen : n ≤ (as.takeWhile p).length := ih (by simp only [List.length_cons] at hn; omega) htail
      simp [List.takeWhile_cons, ha, List.length_cons]
      omega


private theorem getD_max_le {l : List Nat} {b : Nat} (h : ∀ x ∈ l, x ≤ b) : l.max?.getD 0 ≤ b := by
  cases hm : l.max? with
  | none => simp [Option.getD]
  | some m =>
    simp only [Option.getD]
    have hmem : m ∈ l := List.max?_mem hm
    exact h m hmem

private theorem le_getD_max {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  cases hm : l.max? with
  | none => simp [List.max?_eq_none_iff.mp hm] at hx
  | some m =>
    simp only [Option.getD]
    exact (List.max?_le_iff (xs := l) (x := m) hm).mp (Nat.le_refl m) x hx

private theorem takeWhile_length_le_drop {p : Nat → Bool} (l : List Nat) (i : Nat) :
    ((l.drop i).takeWhile p).length ≤ (l.drop i).length := by
  exact List.Sublist.length_le (List.takeWhile_sublist (p := p) (l := l.drop i))

theorem sound_BridgeRun : BridgeRun.Goal := by
  intro a b
  -- unfold the goal's let-bindings and the reference longestZero
  simp only [longestZero, TinylambOpt.ResearchGoals.Compression.Wire.peak]
  apply Nat.le_antisymm
  · -- ≤ direction: every window in a++b is bounded by max of a-windows, b-windows, bridge
    apply getD_max_le
    intro x hx
    simp only [List.mem_map, List.mem_range] at hx
    rcases hx with ⟨i, hi, rfl⟩
    by_cases hia : i ≤ a.length
    · have hdrop : (a ++ b).drop i = a.drop i ++ b := by
        rw [List.drop_append]; simp [Nat.not_lt.mpr hia, Nat.sub_eq_zero_of_le hia]
      by_cases hall : ∀ y ∈ a.drop i, (y == 0) = true
      · rw [hdrop, takeWhile_append_true _ _ hall]
        have htail : a.length - i ≤ (a.reverse.takeWhile (· == 0)).length := by
          have hrev : (a.drop i).reverse = a.reverse.take (a.length - i) := by
            simpa [Nat.sub_sub_self hia] using (List.reverse_drop (l := a) (i := i))
          have hallr : ∀ y ∈ (a.drop i).reverse, (y == 0) = true := by
            intro y hy; exact hall y (List.mem_reverse.mp hy)
          have hallr' : ∀ j (hj : j < a.length - i), (a.reverse[j]'(by rw [List.length_reverse]; omega)) == 0 = true := by
            intro j hj
            have hmem : a.reverse[j]'(by rw [List.length_reverse]; omega) ∈ a.reverse.take (a.length - i) := by
              apply List.getElem_mem_take
              simp [List.length_reverse]; omega
            rw [← hrev] at hmem
            exact hallr _ hmem
          exact le_takeWhile_length (p := (· == 0)) (by rw [List.length_reverse]; omega) (fun j hj => by simpa using hallr' j hj)

        have hle : a.length - i + (b.takeWhile (· == 0)).length ≤
            max (max ((List.map (fun j => ((a.drop j).takeWhile (· == 0)).length) (List.range (a.length + 1))).max?.getD 0)
                     ((List.map (fun j => ((b.drop j).takeWhile (· == 0)).length) (List.range (b.length + 1))).max?.getD 0))
                ((a.reverse.takeWhile (· == 0)).length + (b.takeWhile (· == 0)).length) :=
          Nat.le_trans (by omega) (Nat.le_max_right _ _)
        simp only [List.length_append, List.length_drop]
        exact hle
      · have ⟨y, hy, hfy⟩ : ∃ y ∈ a.drop i, (y == 0) = false := by
          have hne : (a.drop i).any (fun x => !(x == 0)) = true := by
            cases hany : (a.drop i).any (fun x => !(x == 0)) with
            | true => rfl
            | false =>
              exfalso; apply hall; intro z hz
              have := List.any_eq_false.mp hany z hz
              simp at this
              cases hv : (z == 0) <;> simp_all
          rcases List.any_eq_true.mp hne with ⟨y, hy, hfy⟩
          exact ⟨y, hy, by simpa using hfy⟩
        rw [hdrop, takeWhile_append_false _ _ ⟨y, hy, hfy⟩]
        have hmem : ((a.drop i).takeWhile (· == 0)).length ∈
            (List.map (fun j => ((a.drop j).takeWhile (· == 0)).length) (List.range (a.length + 1))) :=
          List.mem_map.mpr ⟨i, by simp [List.mem_range]; omega, rfl⟩
        exact Nat.le_trans (le_getD_max hmem) (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
    · have hia' : a.length < i := Nat.lt_of_not_ge hia
      have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
        have hge : a.length ≤ i := Nat.le_of_lt hia'
        rw [List.drop_append, if_neg (Nat.not_lt.mpr hge)]
      rw [hdrop]
      have hmem : ((b.drop (i - a.length)).takeWhile (· == 0)).length ∈
          (List.map (fun j => ((b.drop j).takeWhile (· == 0)).length) (List.range (b.length + 1))) :=
        List.mem_map.mpr ⟨i - a.length, by simp [List.mem_range, List.length_append] at hi ⊢; omega, rfl⟩
      exact Nat.le_trans (le_getD_max hmem) (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · -- ≥ direction
    refine Nat.max_le.mpr ⟨?_, ?_⟩
    · refine Nat.max_le.mpr ⟨?_, ?_⟩
      · apply getD_max_le
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨i, hi, rfl⟩
        have hia : i ≤ a.length := by omega
        have hdrop : (a ++ b).drop i = a.drop i ++ b := by
          rw [List.drop_append]; simp [Nat.not_lt.mpr hia, Nat.sub_eq_zero_of_le hia]
        by_cases hall : ∀ y ∈ a.drop i, (y == 0) = true
        · have htw : ((a.drop i) ++ b).takeWhile (· == 0) = (a.drop i) ++ b.takeWhile (· == 0) := takeWhile_append_true _ _ hall
          have hmem : (((a ++ b).drop i).takeWhile (· == 0)).length ∈
              (List.map (fun j => (((a ++ b).drop j).takeWhile (· == 0)).length) (List.range ((a ++ b).length + 1))) :=
            List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
          have hle' := le_getD_max hmem
          have htw2 : ((a ++ b).drop i).takeWhile (· == 0) = (a.drop i) ++ b.takeWhile (· == 0) := by rw [hdrop, htw]
          have htw3 : (a.drop i).takeWhile (· == 0) = a.drop i := takeWhile_all_true _ hall
          rw [htw3]
          have hlen_le : (a.drop i).length ≤ (((a ++ b).drop i).takeWhile (· == 0)).length := by
            rw [htw2]; simp [List.length_append]
          exact Nat.le_trans hlen_le hle'
        · have ⟨y, hy, hfy⟩ : ∃ y ∈ a.drop i, (y == 0) = false := by
            have hne : (a.drop i).any (fun x => !(x == 0)) = true := by
              cases hany : (a.drop i).any (fun x => !(x == 0)) with
              | true => rfl
              | false =>
                exfalso; apply hall; intro z hz
                have := List.any_eq_false.mp hany z hz
                simp at this
                cases hv : (z == 0) <;> simp_all
            rcases List.any_eq_true.mp hne with ⟨y, hy, hfy⟩
            exact ⟨y, hy, by simpa using hfy⟩
          have hmem : (((a ++ b).drop i).takeWhile (· == 0)).length ∈
              (List.map (fun j => (((a ++ b).drop j).takeWhile (· == 0)).length) (List.range ((a ++ b).length + 1))) :=
            List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
          have hle' := le_getD_max hmem
          have htw : ((a ++ b).drop i).takeWhile (· == 0) = (a.drop i).takeWhile (· == 0) := by
            rw [hdrop, takeWhile_append_false _ _ ⟨y, hy, hfy⟩]
          rw [htw] at hle'
          exact hle' 
      · apply getD_max_le
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨j, hj, rfl⟩
        have hdrop : (a ++ b).drop (a.length + j) = b.drop j := by simp [List.drop_append]
        have hmem : (((a ++ b).drop (a.length + j)).takeWhile (· == 0)).length ∈
            (List.map (fun k => (((a ++ b).drop k).takeWhile (· == 0)).length) (List.range ((a ++ b).length + 1))) :=
          List.mem_map.mpr ⟨a.length + j, by simp [List.mem_range, List.length_append]; omega, rfl⟩
        have hle := le_getD_max hmem
        simpa [hdrop] using hle
    · -- bridge
      let s := (a.reverse.takeWhile (· == 0)).length
      let p := (b.takeWhile (· == 0)).length
      have hs : s ≤ a.length := by
        have : s ≤ a.reverse.length := List.Sublist.length_le (List.takeWhile_sublist (p := (· == 0)) (l := a.reverse))
        simpa [s] using this
      let i := a.length - s
      have hi_le : i ≤ a.length := Nat.sub_le _ _
      have hdrop : (a ++ b).drop i = a.drop i ++ b := by
        rw [List.drop_append]; simp [Nat.not_lt.mpr hi_le, Nat.sub_eq_zero_of_le hi_le]
      have hall : ∀ y ∈ a.drop i, (y == 0) = true := by
        intro y hy
        have hrev_drop : (a.drop i).reverse = a.reverse.take (a.length - i) := by
          simpa [Nat.sub_sub_self hi_le] using (List.reverse_drop (l := a) (i := i))
        have h1 : y ∈ (a.drop i).reverse := List.mem_reverse.mpr hy
        rw [hrev_drop] at h1
        have hmem_rev : y ∈ a.reverse.take s := by simpa [show a.length - i = s from by omega] using h1
        have ht_eq : a.reverse.takeWhile (· == 0) = a.reverse.take s := by
          simpa [s] using (takeWhile_eq_take_length a.reverse)
        have hmem_rev' : y ∈ a.reverse.takeWhile (· == 0) := by
          rw [ht_eq]; exact hmem_rev
        exact mem_takeWhile_prop (p := (· == 0)) (l := a.reverse) hmem_rev'
      have hmem : (((a ++ b).drop i).takeWhile (· == 0)).length ∈
          (List.map (fun k => (((a ++ b).drop k).takeWhile (· == 0)).length) (List.range ((a ++ b).length + 1))) :=
        List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
      have hle := le_getD_max hmem
      have htw : ((a ++ b).drop i).takeWhile (· == 0) = (a.drop i) ++ b.takeWhile (· == 0) := by
        rw [hdrop, takeWhile_append_true _ _ hall]
      rw [htw] at hle
      simp only [List.length_append, List.length_drop] at hle ⊢
      have hlen : a.length - i = s := by omega
      omega

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, List.length_append]
  refine Prod.ext rfl (Prod.ext ?_ (Prod.ext ?_ ?_))
  · by_cases ha : ∀ x ∈ a, (x == 0) = true
    · have ht : (a ++ b).takeWhile (· == 0) = a ++ b.takeWhile (· == 0) := takeWhile_append_true a b ha
      have hta : a.takeWhile (· == 0) = a := takeWhile_all_true a ha
      simp [ht, hta, List.length_append]
    · have ⟨x, hx, hfx⟩ : ∃ x ∈ a, (x == 0) = false := by
        have hne : a.any (fun x => !(x == 0)) = true := by
          cases hany : a.any (fun x => !(x == 0)) with
          | true => rfl
          | false =>
            exfalso; apply ha; intro y hy
            have := List.any_eq_false.mp hany y hy
            simp at this
            cases hv : (y == 0) <;> simp_all
        rcases List.any_eq_true.mp hne with ⟨x, hx, hfx⟩
        exact ⟨x, hx, by simpa using hfx⟩
      have ht : (a ++ b).takeWhile (· == 0) = a.takeWhile (· == 0) := takeWhile_append_false a b ⟨x, hx, hfx⟩
      simp only [ht]
      have hne : (a.takeWhile (· == 0)).length ≠ a.length := by
        intro heq
        have hall : a.takeWhile (· == 0) = a := List.Sublist.eq_of_length (List.takeWhile_sublist (p := (· == 0)) (l := a)) heq
        have hxmem : x ∈ a.takeWhile (· == 0) := by rw [hall]; exact hx
        exact absurd (mem_takeWhile_prop (l := a) hxmem) (by simp [hfx])
      simp [hne]
  · by_cases hb : ∀ x ∈ b, (x == 0) = true
    · have hrb : b.reverse.takeWhile (· == 0) = b.reverse := takeWhile_all_true b.reverse (by intro y hy; exact hb y (List.mem_reverse.mp hy))
      have htab : (a ++ b).reverse.takeWhile (· == 0) = b.reverse ++ a.reverse.takeWhile (· == 0) := by
        rw [List.reverse_append]; exact takeWhile_append_true b.reverse a.reverse (by intro y hy; exact hb y (List.mem_reverse.mp hy))
      have hbeq : (b.length == b.length) = true := by simp
      simp only [htab, hrb, List.length_append, List.length_reverse, hbeq, ite_true]
    · have ⟨x, hx, hfx⟩ : ∃ x ∈ b, (x == 0) = false := by
        have hne : b.any (fun x => !(x == 0)) = true := by
          cases hany : b.any (fun x => !(x == 0)) with
          | true => rfl
          | false =>
            exfalso; apply hb; intro y hy
            have := List.any_eq_false.mp hany y hy
            simp at this
            cases hv : (y == 0) <;> simp_all
        rcases List.any_eq_true.mp hne with ⟨x, hx, hfx⟩
        exact ⟨x, hx, by simpa using hfx⟩
      have hxr : x ∈ b.reverse := List.mem_reverse.mpr hx
      have htab : (a ++ b).reverse.takeWhile (· == 0) = b.reverse.takeWhile (· == 0) := by
        rw [List.reverse_append]; exact takeWhile_append_false b.reverse a.reverse ⟨x, hxr, hfx⟩
      have hne : (b.reverse.takeWhile (· == 0)).length ≠ b.length := by
        intro heq
        have heq' : (b.reverse.takeWhile (· == 0)).length = b.reverse.length := by simpa using heq
        have hall : b.reverse.takeWhile (· == 0) = b.reverse := List.Sublist.eq_of_length (List.takeWhile_sublist (p := (· == 0)) (l := b.reverse)) heq'
        have hxmem : x ∈ b.reverse.takeWhile (· == 0) := by rw [hall]; exact hxr
        exact absurd (mem_takeWhile_prop (p := (· == 0)) (l := b.reverse) hxmem) (by simp [hfx])
      have hcond : ((List.takeWhile (fun x => x == 0) b.reverse).length == b.length) = false := by
        cases h : ((List.takeWhile (fun x => x == 0) b.reverse).length == b.length) with
        | false => rfl
        | true => exact absurd (eq_of_beq h) hne
      simp only [hcond, Bool.false_eq_true, ite_false]; rw [← List.reverse_append, htab]
  · have h := sound_BridgeRun a b
    dsimp at h
    exact h.symm

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

/-- The all-zero block has zero runs on both ends and inside. -/
theorem sound_ZeroFill : ZeroFill.Goal := by
  intro n
  unfold agg
  simp only [longestZero, TinylambOpt.ResearchGoals.Compression.Wire.peak]
  have htw : (List.replicate n 0).takeWhile (· == 0) = List.replicate n 0 := by
    apply takeWhile_all_true
    intro x hx
    have := List.eq_of_mem_replicate hx
    simp [this]
  have hrev : (List.replicate n 0).reverse = List.replicate n 0 := by simp
  have htr : (List.replicate n 0).reverse.takeWhile (· == 0) = List.replicate n 0 := by rw [hrev, htw]
  refine Prod.ext (by simp) (Prod.ext ?_ (Prod.ext ?_ ?_))
  · simp [htw]
  · simp [htr]
  · apply Nat.le_antisymm
    · apply getD_max_le
      intro x hx
      simp only [List.mem_map, List.mem_range] at hx
      rcases hx with ⟨i, hi, rfl⟩
      have hdrop : (List.replicate n 0).drop i = List.replicate (n - i) 0 := by
        simp [List.drop_replicate]
      rw [hdrop]
      have hall : ∀ y ∈ List.replicate (n - i) 0, (y == 0) = true := by
        intro y hy; have := List.eq_of_mem_replicate hy; simp [this]
      rw [takeWhile_all_true _ hall]
      simp; omega
    · have hmem : (List.replicate n 0).length ∈
          (List.map (fun i => (((List.replicate n 0).drop i).takeWhile (· == 0)).length) (List.range (n + 1))) :=
        List.mem_map.mpr ⟨0, by simp [List.mem_range], by simp [htw]⟩
      simpa using le_getD_max hmem

end LongestHole

namespace Drawdown
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.Drawdown

private theorem min?_append_getD (a b : List Nat) :
    (a ++ b).min?.getD 0 = match a.min?, b.min? with
      | none, none => 0
      | some x, none => x
      | none, some y => y
      | some x, some y => min x y := by
  rw [List_min?_append]
  cases a.min? <;> cases b.min? <;> simp [Option.getD]

private theorem max?_append_getD (a b : List Nat) :
    (a ++ b).max?.getD 0 = match a.max?, b.max? with
      | none, none => 0
      | some x, none => x
      | none, some y => y
      | some x, some y => max x y := by
  rw [List_max?_append]
  cases a.max? <;> cases b.max? <;> simp [Option.getD]

private theorem max?_getD_le {l : List Nat} {b : Nat} (h : ∀ x ∈ l, x ≤ b) : l.max?.getD 0 ≤ b := by
  cases hm : l.max? with
  | none => simp [Option.getD]
  | some m => simp only [Option.getD]; exact h m (List.max?_mem hm)

private theorem le_max?_getD {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  cases hm : l.max? with
  | none => simp [List.max?_eq_none_iff.mp hm] at hx
  | some m => simp only [Option.getD]; exact (List.max?_le_iff (xs := l) (x := m) hm).mp (Nat.le_refl m) x hx

private theorem min?_getD_le {l : List Nat} {x : Nat} (hx : x ∈ l) : l.min?.getD x ≤ x := by
  cases hm : l.min? with
  | none => simp [Option.getD]
  | some m =>
    simp only [Option.getD]
    exact (List.min?_le_iff (xs := l) (x := m) hm).mp (Nat.le_refl m) x hx

private theorem min?_mem_getD (l : List Nat) (d : Nat) (hne : l.min?.isSome) :
    l.min?.getD d ∈ l := by
  cases hm : l.min? with
  | none => simp [hm] at hne
  | some m => simp [Option.getD]; exact List.min?_mem hm

private theorem head?_append_some {α : Type} {a b : List α} {v : α} (ha : a.head? = some v) :
    (a ++ b).head? = some v := by
  cases a <;> simp_all

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  cases ha : a.head? with
  | none =>
    have ha' : a = [] := by cases a <;> simp_all
    subst ha'; simp [optionalJoin]
  | some va =>
    cases hb : b.head? with
    | none =>
      have hb' : b = [] := by cases b <;> simp_all
      subst hb'; simp [optionalJoin]
    | some vb =>
      simp only [ha, hb, Option.map_some, optionalJoin]
      have hhead : (a ++ b).head? = some va := head?_append_some ha
      simp only [hhead]
      congr 1
      · -- min
        rw [min?_append_getD]
        cases hma : a.min? <;> cases hmb : b.min? <;> simp [hma, hmb, Option.getD]
      · congr 1
        · -- peak
          unfold TinylambOpt.ResearchGoals.Compression.Wire.peak
          rw [max?_append_getD]
          cases hpa : a.max? <;> cases hpb : b.max? <;> simp [hpa, hpb, Option.getD]
        · -- drawdown = peak (xs.zipIdx.map ...)
          unfold TinylambOpt.ResearchGoals.Compression.Wire.peak
          apply Nat.le_antisymm
          · apply max?_getD_le
            intro x hx
            simp only [List.mem_map] at hx
            rcases hx with ⟨⟨v, i⟩, hvi, rfl⟩
            simp only [List.mem_zipIdx_iff_getElem?] at hvi
            rcases hvi with ⟨hiv, hget⟩
            by_cases hi : i < a.length
            · have hv : v = a[i] := by
                have := hget
                simp [List.getElem?_append_left hi] at this
                exact this.symm
              subst hv
              have hdrop : (a ++ b).drop i = a.drop i ++ b := by
                rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (Nat.le_of_lt hi)]
              rw [hdrop]
              have hmin : ((a.drop i ++ b).min?.getD a[i]) = min ((a.drop i).min?.getD a[i]) (b.min?.getD a[i]) := by
                rw [min?_append_getD]
                cases hda : (a.drop i).min? <;> cases hdb : b.min? <;> simp [hda, hdb, Option.getD]
              rw [hmin]
              have hda2 : a[i] - (a.drop i).min?.getD a[i] ≤ ((a.zipIdx.map (fun (x, j) => x - ((a.drop j).min?.getD x))).max?.getD 0) := by
                have hmem2 : (a[i] - ((a.drop i).min?.getD a[i])) ∈ (a.zipIdx.map (fun (x, j) => x - ((a.drop j).min?.getD x))) := by
                  apply List.mem_map.mpr
                  refine ⟨(a[i], i), ?_, rfl⟩
                  simp [List.mem_zipIdx_iff_getElem?, hi]
                exact le_max?_getD hmem2
              have hv_le : a[i] ≤ a.max?.getD 0 := by
                have hmem_ai : a[i] ∈ a := List.getElem_mem hi
                exact le_max?_getD hmem_ai
              have hdb2 : a[i] - b.min?.getD a[i] ≤ a.max?.getD 0 - b.min?.getD 0 := by omega
              have hle1 : a[i] - min ((a.drop i).min?.getD a[i]) (b.min?.getD a[i]) ≤
                  Nat.max ((a.zipIdx.map (fun (x, j) => x - ((a.drop j).min?.getD x))).max?.getD 0) (a.max?.getD 0 - b.min?.getD 0) := by
                omega
              exact Nat.le_trans hle1 (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
            · have hi' : a.length ≤ i := Nat.le_of_not_gt hi
              have hj : i - a.length < b.length := by
                have := hiv; simp [List.length_append] at this; omega
              have hv : v = b[i - a.length] := by
                have := hget
                simp [List.getElem?_append_right hi'] at this
                exact this.symm
              subst hv
              have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
                rw [List.drop_append]
                have : ¬ i < a.length := Nat.not_lt.mpr hi'
                simp [this]
              rw [hdrop]
              have hdb2 : b[i - a.length] - ((b.drop (i - a.length)).min?.getD b[i - a.length]) ≤
                  ((b.zipIdx.map (fun (x, j) => x - ((b.drop j).min?.getD x))).max?.getD 0) := by
                have hmem2 : (b[i - a.length] - ((b.drop (i - a.length)).min?.getD b[i - a.length])) ∈
                    (b.zipIdx.map (fun (x, j) => x - ((b.drop j).min?.getD x))) := by
                  apply List.mem_map.mpr
                  refine ⟨(b[i - a.length], i - a.length), ?_, rfl⟩
                  simp [List.mem_zipIdx_iff_getElem?, hj]
                exact le_max?_getD hmem2
              exact Nat.le_trans hdb2 (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
          · refine Nat.max_le ?_ ?_
            · refine Nat.max_le ?_ ?_
              · -- Da
                apply max?_getD_le
                intro x hx
                simp only [List.mem_map] at hx
                rcases hx with ⟨⟨v, i⟩, hvi, rfl⟩
                simp only [List.mem_zipIdx_iff_getElem?] at hvi
                rcases hvi with ⟨hi, hget⟩
                have hv : v = a[i] := by have := hget; simp at this; exact this.symm
                subst hv
                have hmem_ab : (a[i] - (((a ++ b).drop i).min?.getD a[i])) ∈
                    ((a ++ b).zipIdx.map (fun (x, j) => x - (((a ++ b).drop j).min?.getD x))) := by
                  apply List.mem_map.mpr
                  refine ⟨(a[i], i), ?_, rfl⟩
                  simp [List.mem_zipIdx_iff_getElem?, List.length_append, hi, List.getElem?_append_left hi]
                have hle_ab := le_max?_getD hmem_ab
                have hdrop : (a ++ b).drop i = a.drop i ++ b := by
                  rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (Nat.le_of_lt hi)]
                rw [hdrop, min?_append_getD] at hle_ab
                cases hda3 : (a.drop i).min? <;> cases hdb3 : b.min? <;> simp [hda3, hdb3, Option.getD] at hle_ab ⊢ <;> omega
              · -- Db
                apply max?_getD_le
                intro x hx
                simp only [List.mem_map] at hx
                rcases hx with ⟨⟨v, j⟩, hvj, rfl⟩
                simp only [List.mem_zipIdx_iff_getElem?] at hvj
                rcases hvj with ⟨hj, hget⟩
                have hv : v = b[j] := by have := hget; simp at this; exact this.symm
                subst hv
                let ii := a.length + j
                have hii : ii < (a ++ b).length := by simp [ii, List.length_append]; omega
                have hmem_ab : (b[j] - (((a ++ b).drop ii).min?.getD b[j])) ∈
                    ((a ++ b).zipIdx.map (fun (x, k) => x - (((a ++ b).drop k).min?.getD x))) := by
                  apply List.mem_map.mpr
                  refine ⟨(b[j], ii), ?_, rfl⟩
                  have hgetab : (a ++ b)[ii]? = some b[j] := by
                    simp [ii, List.getElem?_append_right (Nat.le_add_right a.length j)]
                  simp [List.mem_zipIdx_iff_getElem?, hii, hgetab]
                have hle_ab := le_max?_getD hmem_ab
                have hdrop : (a ++ b).drop ii = b.drop j := by simp [ii, List.drop_append]
                rw [hdrop] at hle_ab
                exact hle_ab
            · -- peak a - min b
              cases hpa : a.max? with
              | none => simp [Option.getD]
              | some pa =>
                have hmem_pa : pa ∈ a := List.max?_mem hpa
                rcases List.getElem_of_mem hmem_pa with ⟨i, hi, hvi⟩
                have hvi' : a[i] = pa := hvi
                have hdrop : (a ++ b).drop i = a.drop i ++ b := by
                  rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (Nat.le_of_lt hi)]
                have hmem_dd : (a[i] - (((a ++ b).drop i).min?.getD a[i])) ∈
                    ((a ++ b).zipIdx.map (fun (x, k) => x - (((a ++ b).drop k).min?.getD x))) := by
                  apply List.mem_map.mpr
                  refine ⟨(a[i], i), ?_, rfl⟩
                  simp [List.mem_zipIdx_iff_getElem?, List.length_append, hi, List.getElem?_append_left hi]
                have hle_ab := le_max?_getD hmem_dd
                rw [hdrop, min?_append_getD] at hle_ab
                rw [hvi'] at hle_ab
                simp only [Option.getD, hpa]
                cases hda : (a.drop i).min? <;> cases hdb : b.min? <;> simp [hda, hdb, Option.getD] at hle_ab ⊢ <;> omega

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_PeakRecovery : PeakRecovery.Goal := by
  intro a b ha hb
  have hst := stitch_agg a b
  simp only [stitch, agg] at hst
  cases hha : a.head? with
  | none => cases a <;> simp_all
  | some va =>
    cases hhb : b.head? with
    | none => cases b <;> simp_all
    | some vb =>
      simp only [hha, hhb, Option.map_some, optionalJoin] at hst ⊢
      have hhead : (a ++ b).head? = some va := head?_append_some hha
      simp only [hhead, Option.map_some, Option.getD_some]
      -- hst gives equality of full tuples; extract 3rd component
      have h3 : (((a ++ b).zipIdx.map (fun (x, j) => x - (((a ++ b).drop j).min?.getD x))).max?.getD 0) =
          Nat.max (Nat.max (((a.zipIdx.map (fun (x, j) => x - ((a.drop j).min?.getD x))).max?.getD 0))
                           (((b.zipIdx.map (fun (x, j) => x - ((b.drop j).min?.getD x))).max?.getD 0)))
                  ((a.max?.getD 0) - (b.min?.getD 0)) := by
        -- from hst
        have := congrArg (fun x => match x with | some r => r.2.2 | none => 0) hst
        simpa [hhead, Option.getD_some, peak] using this
      rw [h3]
      exact Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_right _ _)

theorem sound_AscendingPage : AscendingPage.Goal := by
  intro xs hpair
  simp only [agg]
  cases h : xs.head? with
  | none => simp
  | some v =>
    simp only [Option.all_some, decide_eq_true_eq]
    unfold TinylambOpt.ResearchGoals.Compression.Wire.peak
    apply Nat.eq_zero_of_le_zero
    apply max?_getD_le
    intro x hx
    simp only [List.mem_map] at hx
    rcases hx with ⟨⟨xv, i⟩, hxi, rfl⟩
    simp only [List.mem_zipIdx_iff_getElem?] at hxi
    rcases hxi with ⟨hi, hget⟩
    have hxv_eq : xv = xs[i] := by have := hget; simp at this; exact this.symm
    subst hxv_eq
    have hmin_ge : xs[i] ≤ ((xs.drop i).min?.getD xs[i]) := by
      cases hm2 : (xs.drop i).min? with
      | none => simp [List.min?_eq_none_iff.mp hm2] at hi
      | some mn =>
        simp only [Option.getD, hm2]
        have hmem_m : mn ∈ xs.drop i := List.min?_mem hm2
        rw [List.mem_iff_getElem] at hmem_m
        rcases hmem_m with ⟨j, hj, hmj⟩
        have hij : i ≤ i + j := Nat.le_add_right i j
        have hjlen : i + j < xs.length := by
          have := hj; simp [List.length_drop] at this; omega
        have hle_ij := (List.pairwise_iff_get.mp hpair) ⟨i, hi⟩ ⟨i + j, hjlen⟩ (by simp; exact Nat.le_add_right i j)
        simp only [List.getElem_drop] at hmj
        have : xs[i] ≤ xs[i + j] := by simpa using hle_ij
        simpa [hmj] using this
    omega

end Drawdown

/-! ## MonotoneBreaks -/

namespace MonotoneBreaks
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.MonotoneBreaks

private theorem head?_append_some {α : Type} {a b : List α} {v : α} (ha : a.head? = some v) :
    (a ++ b).head? = some v := by
  cases a <;> simp_all

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  cases ha : a.head? with
  | none =>
    have : a = [] := by cases a <;> simp_all
    subst this; simp [optionalJoin]
  | some va =>
    cases hb : b.head? with
    | none =>
      have : b = [] := by cases b <;> simp_all
      subst this; simp [optionalJoin]
    | some vb =>
      simp only [ha, hb, Option.map_some, optionalJoin]
      have hhead : (a ++ b).head? = some va := head?_append_some ha
      have hlast : (a ++ b).getLast? = b.getLast? := by
        cases b with
        | nil => simp at hb
        | cons x xs => simp [List.getLast?_append_cons]
      simp only [hhead, hlast]
      congr 1
      have hlastA : a.getLast? = some (a.getLast (by cases a <;> simp_all)) := by simp [List.getLast?_eq_getLast]
      simp only [hlastA, Option.getD_some]
      have hvb : vb = b.head (by cases b <;> simp_all) := by cases b <;> simp_all
      have hedges : edges (a ++ b) = edges a ++ ((a.getLast (by cases a <;> simp_all), vb) :: edges b) := by
        cases a with
        | nil => simp at ha
        | cons x xs =>
          cases b with
          | nil => simp at hb
          | cons y ys =>
            simp [edges, List.zip_append, List.drop_append, List.getLast_cons, List.zip_cons_cons]
      rw [hedges]
      simp [List.countP_append, List.countP_cons, hvb]

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_ParallelMergeFree : ParallelMergeFree.Goal := by
  intro a b hpa hpb hab
  simp only [agg]
  cases ha : a.head? with
  | none => simp
  | some va =>
    cases hb : b.head? with
    | none => simp
    | some vb =>
      simp only [ha, hb, Option.map_some, optionalJoin, Option.all_some, decide_eq_true_eq]
      have hba : (edges a).countP (fun (x, y) => y < x) = 0 := by
        rw [List.countP_eq_zero]
        intro ⟨x, y⟩ hxy
        simp only [edges, List.mem_zip] at hxy
        rcases hxy with ⟨i, hi, hx, hy⟩
        have hi' : i < a.length - 1 := by
          have h1 : i < (a.zip (a.drop 1)).length := hi
          simp [List.length_zip, List.length_drop] at h1
          omega
        have hle : x ≤ y := by
          have := (List.pairwise_iff_get.mp hpa) ⟨i, by omega⟩ ⟨i + 1, by omega⟩ (by simp)
          simp only [List.getElem_cons_drop, List.getElem_drop] at this
          -- hx : a[i]? or get from zip; easier use get
          have hx' : x = a[i]'(by omega) := by
            have := hx; simp [List.getElem?_eq_getElem (by omega : i < a.length)] at this; exact this.symm
          have hy' : y = a[i+1]'(by omega) := by
            have := hy; simp [List.getElem?_eq_getElem (by omega : i < (a.drop 1).length), List.getElem_drop] at this; exact this.symm
          subst hx'; subst hy'; simpa using this
        simp; omega
      have hbb : (edges b).countP (fun (x, y) => y < x) = 0 := by
        rw [List.countP_eq_zero]
        intro ⟨x, y⟩ hxy
        simp only [edges, List.mem_zip] at hxy
        rcases hxy with ⟨i, hi, hx, hy⟩
        have hi' : i < b.length - 1 := by
          have h1 : i < (b.zip (b.drop 1)).length := hi
          simp [List.length_zip, List.length_drop] at h1
          omega
        have hle : x ≤ y := by
          have := (List.pairwise_iff_get.mp hpb) ⟨i, by omega⟩ ⟨i + 1, by omega⟩ (by simp)
          have hx' : x = b[i]'(by omega) := by
            have := hx; simp [List.getElem?_eq_getElem (by omega : i < b.length)] at this; exact this.symm
          have hy' : y = b[i+1]'(by omega) := by
            have := hy; simp [List.getElem?_eq_getElem (by omega : i < (b.drop 1).length), List.getElem_drop] at this; exact this.symm
          subst hx'; subst hy'; simpa using this
        simp; omega
      have hlastA : a.getLast (by cases a <;> simp_all) ∈ a := List.getLast_mem (by cases a <;> simp_all)
      have hheadB : vb ∈ b := by cases b <;> simp_all
      have hboundary : ¬ (vb < a.getLast (by cases a <;> simp_all)) := by
        have := hab _ hlastA _ hheadB; omega
      have hst := stitch_agg a b
      simp only [stitch, agg, ha, hb, Option.map_some, optionalJoin] at hst
      have hhead : (a ++ b).head? = some va := head?_append_some ha
      have hlastb : b.getLast? = some (b.getLast (by cases b <;> simp_all)) := by simp [List.getLast?_eq_getLast]
      have := congrArg (fun x => match x with | some r => r.2.2 | none => 0) hst
      simp only [hhead, Option.getD_some, hlastb, Option.getD_some] at this
      -- this gives count equality
      have hcount : (edges (a ++ b)).countP (fun (x, y) => y < x) =
          (edges a).countP (fun (x, y) => y < x) + (edges b).countP (fun (x, y) => y < x) +
          (if vb < a.getLast (by cases a <;> simp_all) then 1 else 0) := by
        -- derive from this
        simpa [hvb_eq : vb = b.head (by cases b <;> simp_all)] using this
      rw [hcount, hba, hbb]
      simp [hboundary]

theorem sound_SortingCertificate : SortingCertificate.Goal := by
  intro xs
  simp only [agg]
  cases h : xs.head? with
  | none => simp [List.Pairwise.nil]
  | some v =>
    simp only [Option.map_some, Option.getD_some]
    constructor
    · intro hzero
      rw [List.pairwise_iff_get]
      intro i j hij
      have hedge : (edges xs).countP (fun (x, y) => y < x) = 0 := hzero
      rw [List.countP_eq_zero] at hedge
      have hstep : ∀ k (hk : k + 1 < xs.length), xs[k] ≤ xs[k + 1] := by
        intro k hk
        have hmem : (xs[k], xs[k+1]) ∈ edges xs := by
          simp only [edges, List.mem_zip]
          refine ⟨k, ?_, ?_, ?_⟩
          · simp [List.length_zip, List.length_drop]; omega
          · simp [List.getElem?_eq_getElem (by omega : k < xs.length)]
          · simp [List.getElem?_eq_getElem (by omega : k < (xs.drop 1).length), List.getElem_drop]
        have := hedge _ hmem
        simp at this; omega
      -- chain
      have : ∀ k, i.1 + k ≤ j.1 → xs[i.1] ≤ xs[i.1 + k] := by
        intro k hk
        induction k with
        | zero => exact Nat.le_refl _
        | succ k ih =>
          have hk1 : i.1 + k < xs.length := by omega
          have h1 := ih (by omega)
          have h2 := hstep (i.1 + k) (by omega)
          exact Nat.le_trans h1 h2
      have hk : i.1 + (j.1 - i.1) = j.1 := Nat.add_sub_cancel' (Nat.le_of_lt hij)
      have := this (j.1 - i.1) (by omega)
      simpa [hk] using this
    · intro hpair
      have hedge : (edges xs).countP (fun (x, y) => y < x) = 0 := by
        rw [List.countP_eq_zero]
        intro ⟨x, y⟩ hxy
        simp only [edges, List.mem_zip] at hxy
        rcases hxy with ⟨i, hi, hx, hy⟩
        have hi' : i < xs.length - 1 := by
          have h1 : i < (xs.zip (xs.drop 1)).length := hi
          simp [List.length_zip, List.length_drop] at h1
          omega
        have hx' : x = xs[i]'(by omega) := by
          have := hx; simp [List.getElem?_eq_getElem (by omega : i < xs.length)] at this; exact this.symm
        have hy' : y = xs[i+1]'(by omega) := by
          have := hy; simp [List.getElem?_eq_getElem (by omega : i < (xs.drop 1).length), List.getElem_drop] at this; exact this.symm
        have hle : x ≤ y := by
          have := (List.pairwise_iff_get.mp hpair) ⟨i, by omega⟩ ⟨i + 1, by omega⟩ (by simp)
          subst hx'; subst hy'; simpa using this
        simp; omega
      simp [hedge]

end MonotoneBreaks

/-! ## DeltaBytes -/

namespace DeltaBytes
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes

private theorem head?_append_some {α : Type} {a b : List α} {v : α} (ha : a.head? = some v) :
    (a ++ b).head? = some v := by
  cases a <;> simp_all

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  cases ha : a.head? with
  | none =>
    have : a = [] := by cases a <;> simp_all
    subst this; simp [optionalJoin]
  | some va =>
    cases hb : b.head? with
    | none =>
      have : b = [] := by cases b <;> simp_all
      subst this; simp [optionalJoin]
    | some vb =>
      simp only [ha, hb, Option.map_some, optionalJoin]
      have hhead : (a ++ b).head? = some va := head?_append_some ha
      have hlast : (a ++ b).getLast? = b.getLast? := by
        cases b with
        | nil => simp at hb
        | cons x xs => simp [List.getLast?_append_cons]
      simp only [hhead, hlast]
      congr 1
      have hlastA : a.getLast? = some (a.getLast (by cases a <;> simp_all)) := by simp [List.getLast?_eq_getLast]
      simp only [hlastA, Option.getD_some]
      have hvb : vb = b.head (by cases b <;> simp_all) := by cases b <;> simp_all
      have hedges : edges (a ++ b) = edges a ++ ((a.getLast (by cases a <;> simp_all), vb) :: edges b) := by
        cases a with
        | nil => simp at ha
        | cons x xs =>
          cases b with
          | nil => simp at hb
          | cons y ys =>
            simp [edges, List.zip_append, List.drop_append, List.getLast_cons, List.zip_cons_cons]
      rw [hedges]
      simp [List.map_append, List.sum_append, List.map_cons, List.sum_cons, hvb]

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_ReverseResidualCliff : ReverseResidualCliff.Goal := by
  intro xs
  simp only [agg]
  cases h : xs.reverse.head? with
  | none =>
    have : xs = [] := by cases xs <;> simp_all [List.reverse_eq_nil_iff]
    simp [this]
  | some vr =>
    cases h2 : xs.head? with
    | none =>
      have : xs = [] := by cases xs <;> simp_all
      simp [this] at h
    | some v =>
      simp only [Option.map_some, Option.getD_some]
      have hrev : (edges xs.reverse).map (fun (x, y) => sbytes ((y : Int) - x)) =
          ((edges xs).map (fun (x, y) => sbytes ((x : Int) - y))).reverse := by
        simp [edges, List.map_reverse, List.zip_reverse, List.reverse_reverse, Function.comp]
      rw [hrev, List.sum_reverse]
      have hbound : ∀ (p : Nat × Nat), sbytes ((p.1 : Int) - p.2) ≤ sbytes ((p.2 : Int) - p.1) + 1 := by
        intro p
        simp only [sbytes, sbytes, ubytes, ubytes, zig, zig, width, width]
        omega
      calc (((edges xs).map (fun (x, y) => sbytes ((x : Int) - y))).sum)
          ≤ (((edges xs).map (fun (x, y) => sbytes ((y : Int) - x) + 1)).sum) := by
            apply List.sum_le_sum
            intro z hz
            simp only [List.mem_map] at hz
            rcases hz with ⟨⟨a, b⟩, _, rfl⟩
            exact hbound (a, b)
        _ = ((edges xs).map (fun (x, y) => sbytes ((y : Int) - x))).sum + (edges xs).length := by
            have hmap : ((edges xs).map (fun (x, y) => sbytes ((y : Int) - x) + 1)) =
                ((edges xs).map (fun (x, y) => sbytes ((y : Int) - x))).map (· + 1) := by
              simp [List.map_map, Function.comp]
            rw [hmap, List.sum_map_add]
            simp
        _ ≤ ((edges xs).map (fun (x, y) => sbytes ((y : Int) - x))).sum + (xs.length - 1) := by
            have hlen : (edges xs).length ≤ xs.length - 1 := by
              simp [edges, List.length_zip, List.length_drop]
              omega
            omega

theorem sound_RepeatedConstant : RepeatedConstant.Goal := by
  intro x n
  simp only [agg]
  simp [List.head?, List.getLast?_cons, List.getLast?_replicate, edges, List.zip_cons_cons]
  induction n with
  | zero => simp
  | succ n ih =>
    simp [List.replicate_succ, edges, List.zip_cons_cons, ih]

end DeltaBytes

/-! ## PairSubsequence -/

namespace PairSubsequence
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

private def cross (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => if v == 1 then (xs.drop (i + 1)).count 2 else 0)).sum

private theorem cross_cons (x : Nat) (xs : List Nat) :
    cross (x :: xs) = (if x == 1 then xs.count 2 else 0) + cross xs := by
  simp [cross, List.zipIdx_cons, List.map_cons, List.map_map, Function.comp, Nat.add_comm]

private theorem cross_append (a b : List Nat) :
    cross (a ++ b) = cross a + cross b + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [cross]
  | cons x xs ih =>
    simp only [List.cons_append, cross_cons, List.count_cons, ih, List.count_append]
    by_cases hx : x == 1 <;> simp [hx] <;> omega

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, List.count_append]
  refine Prod.ext rfl (Prod.ext rfl ?_)
  exact cross_append a b

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_ReversalComplement : ReversalComplement.Goal := by
  intro xs
  simp only [agg]
  induction xs with
  | nil => simp [cross]
  | cons x xs ih =>
    rw [cross_cons]
    have hrev : (x :: xs).reverse = xs.reverse ++ [x] := by simp
    rw [hrev, cross_append]
    have hcx : cross [x] = 0 := by simp [cross]
    rw [hcx]
    simp only [List.count_reverse, Nat.add_zero, List.count_cons]
    have hc1 : [x].count 2 = if x == 2 then 1 else 0 := by simp [List.count_cons]
    rw [hc1]
    by_cases h1 : x == 1 <;> by_cases h2 : x == 2 <;> simp_all <;> omega

theorem sound_BatchedMarkers : BatchedMarkers.Goal := by
  intro a b
  simp only [agg]
  refine Prod.ext ?_ (Prod.ext ?_ ?_)
  · rw [List.count_append]; simp [List.count_replicate]
  · rw [List.count_append]; simp [List.count_replicate]
  · rw [cross_append]
    have hc1 : cross (List.replicate a 1) = 0 := by
      induction a with
      | zero => simp [cross]
      | succ n ih => rw [List.replicate_succ, cross_cons]; simp [List.count_replicate, ih]
    have hc2 : cross (List.replicate b 2) = 0 := by
      induction b with
      | zero => simp [cross]
      | succ n ih => rw [List.replicate_succ, cross_cons]; simp [ih]
    simp [hc1, hc2, List.count_replicate]

end PairSubsequence

/-! ## RollingFingerprint -/

namespace RollingFingerprint
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

private def M : Nat := 65521
private def B : Nat := 257
private def hpow (xs : List Nat) : Nat := B ^ xs.length % M
private def hval (xs : List Nat) : Nat := xs.foldl (fun h x => (B * h + x) % M) 0

private theorem hpow_append (a b : List Nat) : hpow (a ++ b) = hpow a * hpow b % M := by
  simp [hpow, List.length_append, Nat.pow_add, Nat.mul_mod]

private theorem hval_snoc (xs : List Nat) (x : Nat) : hval (xs ++ [x]) = (B * hval xs + x) % M := by
  simp [hval, List.foldl_append]

private theorem hpow_snoc (xs : List Nat) (x : Nat) : hpow (xs ++ [x]) = hpow xs * (B % M) % M := by
  simp [hpow, List.length_append, Nat.pow_add, Nat.pow_one, Nat.mul_mod, Nat.mul_comm]

private theorem hM_pos : 0 < M := by decide

private theorem hval_lt (xs : List Nat) : hval xs < M := by
  unfold hval
  suffices ∀ (l : List Nat) (acc : Nat), acc < M → List.foldl (fun h x => (B * h + x) % M) acc l < M from
    this xs 0 (by decide)
  intro l
  induction l with
  | nil => intro acc h; exact h
  | cons x xs ih => intro acc h; exact ih _ (Nat.mod_lt _ hM_pos)

private theorem mod_add_right_cancel (a b c : Nat) : (a + c) % M = (b + c) % M → a % M = b % M := by
  intro h
  have h1 : (a + c % M) % M = (b + c % M) % M := by
    simpa [Nat.add_mod, Nat.mod_mod] using h
  -- use Nat.add_mod_right / omega on mod
  omega

private theorem hval_append (a b : List Nat) : hval (a ++ b) = (hval a * hpow b % M + hval b) % M := by
  induction b using List.reverseRecOn with
  | nil =>
    simp [hval, hpow]
    rw [Nat.mod_eq_of_lt (hval_lt a)]
  | append_singleton bs x ih =>
    rw [List.append_assoc, hval_snoc, ih, hval_snoc, hpow_snoc]
    -- show equality mod M
    apply Nat.eq_of_testBit_eq
    intro i
    -- easier: use Nat.ModEq-like reasoning via calc mod M
    have hgoal : (B * ((hval a * hpow bs % M + hval bs) % M) + x) % M =
                 (hval a * (hpow bs * (B % M) % M) % M + (B * hval bs + x) % M) % M := by
      -- normalize both sides mod M
      -- LHS = (B * (hval a * hpow bs + hval bs) + x) % M
      -- RHS = (hval a * (hpow bs * B) + B * hval bs + x) % M
      have lhs : (B * ((hval a * hpow bs % M + hval bs) % M) + x) % M =
                 (B * (hval a * hpow bs % M + hval bs) + x) % M := by
        rw [Nat.add_mod, Nat.mul_mod, Nat.mod_mod, ← Nat.mul_mod, ← Nat.add_mod]
      have rhs : (hval a * (hpow bs * (B % M) % M) % M + (B * hval bs + x) % M) % M =
                 (hval a * (hpow bs * B) + (B * hval bs + x)) % M := by
        calc (hval a * (hpow bs * (B % M) % M) % M + (B * hval bs + x) % M) % M
            = (hval a * (hpow bs * (B % M) % M) + (B * hval bs + x)) % M := by
                rw [Nat.add_mod, Nat.mod_mod, Nat.mod_mod, ← Nat.add_mod]
          _ = (hval a * (hpow bs * B) + (B * hval bs + x)) % M := by
                congr 1
                rw [Nat.mul_mod, Nat.mul_mod hval a, Nat.mod_mod]
                congr 1
                rw [Nat.mul_mod (hpow bs), Nat.mod_mod, ← Nat.mul_mod]
      rw [lhs, rhs]
      -- now (B*(hval a*hpow bs%M + hval bs) + x) % M = (hval a*(hpow bs*B) + (B*hval bs+x)) % M
      -- simplify LHS
      calc (B * (hval a * hpow bs % M + hval bs) + x) % M
          = (B * (hval a * hpow bs % M) + B * hval bs + x) % M := by rw [Nat.mul_add]; ac_rfl
        _ = (B * (hval a * hpow bs % M) % M + (B * hval bs + x) % M) % M := by
            rw [Nat.add_mod, Nat.add_mod (B * (hval a * hpow bs % M)), Nat.mod_mod, ← Nat.add_mod]
        _ = (B * (hval a * hpow bs) % M + (B * hval bs + x) % M) % M := by
            congr 2
            rw [Nat.mul_mod, Nat.mul_mod B (hval a * hpow bs), Nat.mod_mod]
        _ = (hval a * hpow bs * B % M + (B * hval bs + x) % M) % M := by
            congr 2
            have : B * (hval a * hpow bs) = hval a * hpow bs * B := by
              calc B * (hval a * hpow bs) = B * hval a * hpow bs := by rw [Nat.mul_assoc]
                _ = hval a * B * hpow bs := by rw [Nat.mul_comm B (hval a)]
                _ = hval a * (B * hpow bs) := by rw [Nat.mul_assoc]
                _ = hval a * (hpow bs * B) := by rw [Nat.mul_comm B (hpow bs)]
                _ = hval a * hpow bs * B := by rw [← Nat.mul_assoc]
            rw [this]
        _ = (hval a * (hpow bs * B) % M + (B * hval bs + x) % M) % M := by
            congr 2; rw [Nat.mul_assoc]
        _ = (hval a * (hpow bs * B) + (B * hval bs + x)) % M := by
            rw [Nat.add_mod, Nat.mod_mod, Nat.mod_mod, ← Nat.add_mod]
    exact hgoal

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  exact Prod.ext (hpow_append a b) (hval_append a b)

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_EqualWindowResidues : EqualWindowResidues.Goal := by
  intro a b pre z hlen hres
  have ha := stitch_agg pre a
  have hb := stitch_agg pre b
  have haz := stitch_agg (pre ++ a) z
  have hbz := stitch_agg (pre ++ b) z
  simp only [stitch, agg] at ha hb haz hbz
  have hlen1 : (agg.reference a).1 = (agg.reference b).1 := by
    simp only [agg, hpow, hlen]
  have hpa_eq : agg.reference (pre ++ a) = agg.reference (pre ++ b) := by
    apply Prod.ext
    · -- fst
      have := congrArg Prod.fst ha
      have := congrArg Prod.fst hb
      simp only [agg, hpow, List.length_append, hlen] at this ⊢
      -- (B^(pre+a)%M) = (B^pre%M * B^a%M)%M
      simp [hlen]
    · -- snd
      have hsnda := congrArg Prod.snd ha
      have hsndb := congrArg Prod.snd hb
      simp only [agg, hpow, hval, List.length_append, hlen] at hsnda hsndb ⊢
      -- both sides: (hval pre * hpow a %M + hval a) %M
      rw [hsnda, hsndb]
      -- hlen means hpow a = hpow b
      have hpow_eq : B ^ a.length % M = B ^ b.length % M := by rw [hlen]
      -- hres: snd of agg.reference a = b
      simp only [agg, hval] at hres
      rw [hpow_eq, hres]
  calc agg.reference (pre ++ a ++ z)
      = agg.reference ((pre ++ a) ++ z) := by simp [List.append_assoc]
    _ = agg.join (agg.reference (pre ++ a)) (agg.reference z) := (stitch_agg (pre ++ a) z).symm
    _ = agg.join (agg.reference (pre ++ b)) (agg.reference z) := by rw [hpa_eq]
    _ = agg.reference ((pre ++ b) ++ z) := stitch_agg (pre ++ b) z
    _ = agg.reference (pre ++ b ++ z) := by simp [List.append_assoc]

theorem sound_CollisionBoundary : CollisionBoundary.Goal := by
  decide

end RollingFingerprint

/-! ## BalanceFloor -/

namespace BalanceFloor
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def step (x : Nat) : Int := if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int)
private def steps (xs : List Nat) : List Int := xs.map step

private theorem steps_append (a b : List Nat) : steps (a ++ b) = steps a ++ steps b := by
  simp [steps, List.map_append]

private theorem scanl_add_append (a b : List Int) (init : Int) :
    (a ++ b).scanl (· + ·) init = a.scanl (· + ·) init ++ (b.scanl (· + ·) (init + a.sum)).tail := by
  induction a generalizing init with
  | nil => simp
  | cons x xs ih =>
    simp only [List.cons_append, List.scanl, List.sum_cons, List.tail_cons]
    rw [ih (init + x)]
    congr 1
    congr 1
    omega

private theorem foldl_min_eq_min?_getD (l : List Int) (init : Int) :
    l.foldl min init = (l.min?.getD init) := by
  induction l generalizing init with
  | nil => simp [Option.getD]
  | cons x xs ih =>
    simp only [List.foldl, List.min?_cons]
    rw [ih (min init x)]
    cases h : xs.min? <;> simp [h, Option.getD, min_assoc, min_left_comm, min_comm]

private theorem scanl_foldl_min_shift (l : List Int) (s m : Int) :
    (l.scanl (· + ·) s).tail.foldl min m = min m (s + ((l.scanl (· + ·) 0).foldl min 0)) := by
  induction l generalizing s m with
  | nil => simp
  | cons x xs ih =>
    simp only [List.scanl, List.tail_cons, List.foldl]
    rw [ih (s + x) (min m (s + x))]
    have ih0 := ih x (min 0 x)
    simp only [List.scanl, List.tail_cons, List.foldl, zero_add] at ih0
    -- min (min m (s+x)) (s+x + foldl min 0 (scanl xs 0)) = min m (s + min x (x + foldl...))
    -- use ih0: min (min 0 x) (x + foldl...) = min 0 (x + foldl0...)
    omega

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, steps_append, List.sum_append]
  refine Prod.ext ?_ ?_
  · omega
  · -- min of scanl
    rw [scanl_add_append]
    -- min? of appended scanl
    -- (scanl_a ++ tail(scanl_b shifted)).min?.getD 0
    -- = min (scanl_a.min?.getD 0) (tail(scanl_b shifted).min?.getD ...)
    -- easier to work with foldl min
    have h1 : ((steps a).scanl (· + ·) 0 ++ ((steps b).scanl (· + ·) (0 + (steps a).sum)).tail).min?.getD 0 =
              ((steps a).scanl (· + ·) 0 ++ ((steps b).scanl (· + ·) ((steps a).sum)).tail).foldl min 0 := by
      simp [foldl_min_eq_min?_getD]
    rw [h1]
    rw [List.foldl_append]
    -- foldl min (foldl min 0 scanl_a) (tail scanl_b shifted)
    simp only [zero_add]
    rw [scanl_foldl_min_shift (steps b) ((steps a).sum) (((steps a).scanl (· + ·) 0).foldl min 0)]
    -- goal: min (foldl min 0 scanl_a) ((steps a).sum + foldl min 0 (scanl b 0)) = min (...) (...)
    simp [foldl_min_eq_min?_getD]

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_ShardAdmission : ShardAdmission.Goal := by
  intro a b credit
  have hst := stitch_agg a b
  simp only [stitch, agg] at hst
  obtain ⟨hs, hm⟩ := hst
  rw [hm]
  constructor
  · intro h; constructor <;> omega
  · intro ⟨h1, h2⟩; omega

private theorem scanl_mem_prefix_sum (l : List Int) (init : Int) (j : Nat) (hj : j ≤ l.length) :
    init + (l.take j).sum ∈ l.scanl (· + ·) init := by
  induction j generalizing l init with
  | zero => simp
  | succ j ih =>
    cases l with
    | nil => simp at hj
    | cons x xs =>
      simp only [List.take_succ_cons, List.sum_cons, List.scanl, List.mem_cons]
      right
      have := ih xs (init + x) (by simp at hj; omega)
      simpa [add_assoc, add_comm, add_left_comm] using this

private theorem foldl_min_le_of_mem_scanl (l : List Int) (init : Int) (x : Int)
    (hx : x ∈ l.scanl (· + ·) init) : (l.scanl (· + ·) init).foldl min init ≤ x := by
  induction l generalizing init with
  | nil =>
    simp at hx; simp [hx]
  | cons y ys ih =>
    simp only [List.scanl, List.mem_cons] at hx
    rcases hx with rfl | hx
    · simp [List.foldl]
      exact Int.min_le_left _ _
    · simp only [List.foldl]
      exact Int.le_trans (Int.min_le_right _ _) (ih _ _ hx)

theorem sound_CreditLimit : CreditLimit.Goal := by
  intro xs credit
  simp only [agg]
  constructor
  · intro h i hi
    -- min of scanl >= -credit
    have hmin_eq : ((steps xs).scanl (· + ·) 0).min?.getD 0 = ((steps xs).scanl (· + ·) 0).foldl min 0 := by
      rw [foldl_min_eq_min?_getD]
    have hmin : ((steps xs).scanl (· + ·) 0).foldl min 0 ≥ -credit := by
      rw [← hmin_eq]; omega
    have hmem : (steps xs |>.take i).sum ∈ (steps xs).scanl (· + ·) 0 := by
      have := scanl_mem_prefix_sum (steps xs) 0 i (by simp [steps]; omega)
      simpa using this
    have hle := foldl_min_le_of_mem_scanl (steps xs) 0 _ hmem
    have hsum : (List.map (fun x => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int)) (List.take i xs)).sum =
                ((steps xs).take i).sum := by
      simp [steps, List.map_take]
    rw [hsum]
    omega
  · intro hall
    -- all prefix sums >= -credit => min >= -credit
    have hmin_eq : ((steps xs).scanl (· + ·) 0).min?.getD 0 = ((steps xs).scanl (· + ·) 0).foldl min 0 := by
      rw [foldl_min_eq_min?_getD]
    rw [hmin_eq]
    have hbound : ∀ (l : List Int) (init : Int),
        (∀ i ≤ l.length, init + (l.take i).sum ≥ -credit) →
        (l.scanl (· + ·) init).foldl min init ≥ -credit := by
      intro l init hall2
      induction l generalizing init with
      | nil =>
        simp
        have := hall2 0 (by simp)
        simp at this
        omega
      | cons x xs ih =>
        simp only [List.scanl, List.foldl]
        apply ih
        intro i hi
        have := hall2 (i + 1) (by simp at hi ⊢; omega)
        simp [List.take_succ_cons, List.sum_cons] at this ⊢
        omega
    apply hbound
    intro i hi
    have hi' : i ≤ xs.length := by simp [steps] at hi; omega
    have := hall i hi'
    simp only [agg, steps] at this
    have hsum : (List.map (fun x => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int)) (List.take i xs)).sum =
                ((steps xs).take i).sum := by
      simp [steps, List.map_take]
    have hsum_eq : (List.map (fun x => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int)) (List.take i xs)).sum = ((steps xs).take i).sum := hsum
    omega

end BalanceFloor

/-! ## BitOccupancy -/

namespace BitOccupancy
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

private def mask (xs : List Nat) : Nat := xs.foldl (fun a x => a ||| 2 ^ (x % 64)) 0

private theorem mask_append (a b : List Nat) : mask (a ++ b) = mask a ||| mask b := by
  simp only [mask, List.foldl_append]
  have hgen : ∀ (l : List Nat) (init : Nat),
      List.foldl (fun a x => a ||| 2 ^ (x % 64)) init l =
      init ||| List.foldl (fun a x => a ||| 2 ^ (x % 64)) 0 l := by
    intro l
    induction l with
    | nil => intro init; simp
    | cons x xs ih =>
      intro init
      simp only [List.foldl]
      rw [ih (init ||| 2 ^ (x % 64)), ih (0 ||| 2 ^ (x % 64))]
      simp [Nat.or_assoc]
  rw [hgen b (mask a)]

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, List.length_append]
  exact Prod.ext rfl (mask_append a b)

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

private theorem testBit_or_two_pow (n i j : Nat) :
    (n ||| 2 ^ j).testBit i = (n.testBit i || (decide (i = j))) := by
  simp [Nat.testBit_or, Nat.testBit_two_pow]

private theorem foldl_or_testBit (l : List Nat) (init : Nat) (b : Nat) :
    (List.foldl (fun a x => a ||| 2 ^ (x % 64)) init l).testBit b =
    (init.testBit b || l.any (fun x => decide (x % 64 = b))) := by
  induction l generalizing init with
  | nil => simp
  | cons x xs ih =>
    simp only [List.foldl, List.any_cons]
    rw [ih]
    simp [Nat.testBit_or, Nat.testBit_two_pow, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]

theorem sound_FoldInto32 : FoldInto32.Goal := by
  intro xs k hk
  simp only [agg, mask]
  rw [foldl_or_two_pow_testBit_k, foldl_or_two_pow_testBit_k32]
  · -- combine
    -- goal: (0.testBit k || xs.any (decide (x%64=k))) || (0.testBit (k+32) || xs.any (decide (x%64=k+32)))
    --     = xs.any (fun x => x%32 == k)
    simp only [Nat.testBit_zero, Bool.false_or]
    -- relate any over %64==k or %64==k+32 to %32==k
    have hiff : ∀ x, (x % 64 = k ∨ x % 64 = k + 32) ↔ x % 32 = k := by
      intro x
      constructor
      · intro h
        rcases h with h | h <;> omega
      · intro h
        have hmod : x % 64 % 32 = x % 32 := by omega
        have : x % 64 < 64 := Nat.mod_lt _ (by decide)
        have hk32 : k + 32 < 64 := by omega
        -- x%64 is either k or k+32
        omega
    -- convert
    rw [Bool.eq_iff_iff]
    simp only [Bool.or_eq_true, List.any_eq_true, decide_eq_true_eq, beq_iff_eq]
    constructor
    · intro h
      rcases h with ⟨x, hx, hxk⟩ | ⟨x, hx, hxk⟩
      · exact ⟨x, hx, (hiff x).mp (Or.inl hxk)⟩
      · exact ⟨x, hx, (hiff x).mp (Or.inr hxk)⟩
    · intro h
      rcases h with ⟨x, hx, hxk⟩
      rcases (hiff x).mpr hxk with hk1 | hk2
      · exact Or.inl ⟨x, hx, hk1⟩
      · exact Or.inr ⟨x, hx, hk2⟩
  · intro l init
    rw [foldl_or_testBit]
  · intro l init
    rw [foldl_or_testBit]
where
  foldl_or_two_pow_testBit_k (l : List Nat) (init : Nat) :
      (List.foldl (fun a x => a ||| 2 ^ (x % 64)) init l).testBit k =
      (init.testBit k || l.any (fun x => decide (x % 64 = k))) := foldl_or_testBit l init k
  foldl_or_two_pow_testBit_k32 (l : List Nat) (init : Nat) :
      (List.foldl (fun a x => a ||| 2 ^ (x % 64)) init l).testBit (k + 32) =
      (init.testBit (k + 32) || l.any (fun x => decide (x % 64 = k + 32))) := foldl_or_testBit l init (k + 32)

theorem sound_NegativeTest : NegativeTest.Goal := by
  intro xs x hnot hmem
  simp only [agg, mask] at hnot
  have hset : (List.foldl (fun a y => a ||| 2 ^ (y % 64)) 0 xs).testBit (x % 64) = true := by
    have hgen : ∀ (l : List Nat) (init : Nat),
        ∀ y ∈ l, (List.foldl (fun a z => a ||| 2 ^ (z % 64)) init l).testBit (y % 64) = true := by
      intro l init y hy
      induction l generalizing init with
      | nil => simp at hy
      | cons z zs ih =>
        simp only [List.foldl]
        simp only [List.mem_cons] at hy
        rcases hy with rfl | hy
        · -- y == z, bit is set in (init ||| 2^(y%64))
          have hbit : (init ||| 2 ^ (y % 64)).testBit (y % 64) = true := by
            simp [Nat.testBit_or, Nat.testBit_two_pow]
          -- once set stays set
          clear ih hy
          have hmono : ∀ (l : List Nat) (n : Nat) (b : Nat),
              n.testBit b = true →
              (List.foldl (fun a z => a ||| 2 ^ (z % 64)) n l).testBit b = true := by
            intro l n b hb
            induction l generalizing n with
            | nil => exact hb
            | cons w ws ih2 =>
              simp only [List.foldl]
              apply ih2
              simp [Nat.testBit_or, hb]
          exact hmono zs _ _ hbit
        · exact ih (init ||| 2 ^ (z % 64)) hy
    exact hgen xs 0 x hmem
  simp at hnot
  rw [hnot] at hset
  cases hset

end BitOccupancy

/-! ## FirstLastChange -/

namespace FirstLastChange
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

private theorem head?_append_some {α : Type} {a b : List α} {v : α} (ha : a.head? = some v) :
    (a ++ b).head? = some v := by
  cases a <;> simp_all

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  cases ha : a.head? with
  | none =>
    have : a = [] := by cases a <;> simp_all
    subst this; simp [optionalJoin]
  | some va =>
    cases hb : b.head? with
    | none =>
      have : b = [] := by cases b <;> simp_all
      subst this; simp [optionalJoin]
    | some vb =>
      simp only [ha, hb, Option.map_some, optionalJoin]
      have hhead : (a ++ b).head? = some va := head?_append_some ha
      have hlast : (a ++ b).getLast? = b.getLast? := by
        cases b with
        | nil => simp at hb
        | cons x xs => simp [List.getLast?_append_cons]
      simp only [hhead, hlast]
      congr 1
      have hlastA : a.getLast? = some (a.getLast (by cases a <;> simp_all)) := by simp [List.getLast?_eq_getLast]
      simp only [hlastA, Option.getD_some]
      have hvb : vb = b.head (by cases b <;> simp_all) := by cases b <;> simp_all
      have hedges : edges (a ++ b) = edges a ++ ((a.getLast (by cases a <;> simp_all), vb) :: edges b) := by
        cases a with
        | nil => simp at ha
        | cons x xs =>
          cases b with
          | nil => simp at hb
          | cons y ys =>
            simp [edges, List.zip_append, List.drop_append, List.getLast_cons, List.zip_cons_cons]
      rw [hedges]
      simp [List.countP_append, List.countP_cons, hvb]

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_DuplicateBlock : DuplicateBlock.Goal := by
  intro xs k hne hk
  have hxs : xs ≠ [] := hne
  induction k with
  | zero => omega
  | succ k ih =>
    by_cases hk0 : k = 0
    · subst hk0
      simp [List.replicate, List.flatten]
    · have hk_pos : 0 < k := Nat.pos_of_ne_zero hk0
      have ih' := ih hk_pos
      simp only [agg] at ih' ⊢
      cases hx : xs.head? with
      | none => cases xs <;> simp_all
      | some vx =>
        simp only [hx, Option.map_some, Option.getD_some] at ih' ⊢
        have h1 : (List.replicate (k + 1) xs).flatten = xs ++ (List.replicate k xs).flatten := by
          simp [List.replicate_succ]
        have hhead2 : (List.replicate k xs).flatten.head? = some vx := by
          cases k with
          | zero => omega
          | succ k2 => simp [List.replicate_succ, hx]
        have hst := stitch_agg xs (List.replicate k xs).flatten
        simp only [stitch, agg, hx, hhead2, Option.map_some, optionalJoin] at hst
        have hhead_ab : (xs ++ (List.replicate k xs).flatten).head? = some vx := by
          cases xs <;> simp_all
        have hlastA : xs.getLast? = some (xs.getLast (by cases xs <;> simp_all)) := by simp [List.getLast?_eq_getLast]
        have hlast_rep : (List.replicate k xs).flatten.getLast? = xs.getLast? := by
          cases k with
          | zero => omega
          | succ k2 =>
            simp [List.replicate_succ, List.getLast?_append]
            cases hxr : (List.replicate k2 xs).flatten with
            | nil =>
              have : xs = [] := by
                -- flatten of replicate k2 xs is nil means k2==0 and xs==[] or similar
                simp [List.flatten_eq_nil_iff] at hxr
                cases hxr with
                | inl h => simp at h; omega
                | inr h => have := h xs (by simp [List.mem_replicate]); exact this
              simp_all
            | cons y ys => simp [List.getLast?_cons_cons]
        have hcount_eq : (1 + (edges (xs ++ (List.replicate k xs).flatten)).countP (fun (x, y) => x != y)) =
            (1 + (edges xs).countP (fun (x, y) => x != y)) + (1 + (edges (List.replicate k xs).flatten).countP (fun (x, y) => x != y)) -
            (if (xs.getLast (by cases xs <;> simp_all)) == vx then 1 else 0) := by
          have := congrArg (fun x => match x with | some r => r.2.2 | none => 0) hst
          simp only [hhead_ab, Option.getD_some, hlastA, Option.getD_some] at this
          -- vb = head of replicate = vx
          have hvb : vx = (List.replicate k xs).flatten.head (by { cases k with | zero => omega | succ k2 => simp [List.replicate_succ] }) := by
            cases k with
            | zero => omega
            | succ k2 => simp [List.replicate_succ, hx]
          simpa [hvb] using this
        rw [h1]
        simp only [Option.map_some, Option.getD_some]
        rw [hcount_eq]
        -- simplify using ih' and boundary condition
        have hbound : ((xs.getLast (by cases xs <;> simp_all)) == vx) = (xs.head? == xs.getLast?) := by
          simp [hx, List.getLast?_eq_getLast]
        rw [hbound] at hcount_eq ⊢
        simp only [ih']
        by_cases hhl : xs.head? == xs.getLast? <;> simp [hhl] <;> omega

theorem sound_ReverseCount : ReverseCount.Goal := by
  intro xs
  simp only [agg]
  cases h : xs.head? with
  | none =>
    have : xs = [] := by cases xs <;> simp_all
    simp [this]
  | some v =>
    have hrev_head : xs.reverse.head? = xs.getLast? := by simp [List.head?_reverse]
    simp only [h, Option.map_some, hrev_head]
    -- countP of edges in reverse = countP of edges
    have hcount : (edges xs.reverse).countP (fun (x, y) => x != y) = (edges xs).countP (fun (x, y) => x != y) := by
      simp only [edges]
      -- (xs.reverse.zip (xs.reverse.drop 1)) relates to (xs.zip (xs.drop 1)).reverse.map Prod.swap
      have hzip : xs.reverse.zip (xs.reverse.drop 1) = (xs.zip (xs.drop 1)).reverse.map Prod.swap := by
        -- use List.zip_reverse: (l.reverse.zip l'.reverse) = (l.zip l').reverse
        -- and reverse_drop: (l.drop n).reverse = l.reverse.take (l.length - n)
        -- and reverse_take: (l.take n).reverse = l.reverse.drop (l.length - n)
        have hrd : (xs.drop 1).reverse = xs.reverse.take (xs.length - 1) := by
          rw [List.reverse_drop]; congr 1
        have hrt : (xs.reverse.take (xs.length - 1)) = (xs.reverse.drop 1).reverse.reverse.reverse.take (xs.length - 1) := by simp
        -- easier: use List.reverse_eq_iff
        rw [← List.reverse_eq_iff, List.reverse_map, List.reverse_reverse]
        -- need: (xs.reverse.zip (xs.reverse.drop 1)).reverse = (xs.zip (xs.drop 1)).map Prod.swap? No wait.
        -- reverse_eq_iff: a.reverse = b ↔ a = b.reverse. So goal becomes (xs.reverse.zip ...).reverse = ((...).reverse.map Prod.swap) ... 
        -- Better: just show directly
        have h2 : (xs.reverse.zip (xs.reverse.drop 1)).reverse = (xs.zip (xs.drop 1)).map Prod.swap := by
          calc (xs.reverse.zip (xs.reverse.drop 1)).reverse
              = xs.reverse.reverse.zip (xs.reverse.drop 1).reverse := by
                  rw [← List.zip_reverse]
            _ = xs.zip ((xs.reverse.drop 1).reverse) := by simp
            _ = xs.zip (xs.take (xs.length - 1)) := by
                  rw [List.reverse_drop]; simp [List.reverse_reverse]
            _ = (xs.zip (xs.drop 1)).map Prod.swap := by
                  -- show xs.zip (xs.take (len-1)) = (xs.zip (xs.drop 1)).map Prod.swap
                  apply List.ext_getElem
                  · simp [List.length_zip, List.length_take, List.length_drop, List.length_map]
                    omega
                  · intro i hi1 hi2
                    simp only [List.length_zip, List.length_take, List.length_drop, List.length_map] at hi1 hi2
                    simp [List.getElem_zip, List.getElem_map, List.getElem_take, List.getElem_drop]
                    <;> omega
        rw [← h2, List.countP_reverse]
    rw [hcount]

end FirstLastChange

/-! ## IndexWeighted -/

namespace IndexWeighted
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

private def wsum (xs : List Nat) : Nat := (xs.zipIdx.map (fun (v, i) => i * v)).sum

private theorem wsum_append (a b : List Nat) : wsum (a ++ b) = wsum a + wsum b + a.length * b.sum := by
  simp only [wsum, List.zipIdx_append, List.map_append, List.sum_append]
  congr 1
  rw [List.map_map]
  simp [Function.comp]

theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, List.length_append, List.sum_append]
  refine Prod.ext rfl (Prod.ext rfl ?_)
  exact wsum_append a b

theorem sound_Stitch : Stitch.Goal := stitch_agg
theorem sound_TreeReduce : TreeReduce.Goal := treeReduce_of_stitch _ stitch_agg
theorem sound_RepeatPower : RepeatPower.Goal := repeatPower_of_stitch _ stitch_agg
theorem sound_FrameSlice : FrameSlice.Goal := frameSlice_of_stitch _ stitch_agg
theorem sound_PrefixCheckpoint : PrefixCheckpoint.Goal := prefixCheckpoint_of_stitch _ stitch_agg
theorem sound_AmendLeaf : AmendLeaf.Goal := amendLeaf_of_stitch _ stitch_agg
theorem sound_RunInput : RunInput.Goal := runInput_of_stitch _ stitch_agg
theorem sound_ReorderNeighbors : ReorderNeighbors.Goal := reorderNeighbors_of_stitch _ stitch_agg

theorem sound_ReplaceAt : ReplaceAt.Goal := by
  intro xs i x hi
  simp only [agg]
  have hsplit : xs = xs.take i ++ xs[i] :: xs.drop (i + 1) := by
    have htd := List.take_append_drop i xs
    -- rebuild
    conv => lhs; rw [← htd]
    have hcd : xs.drop i = xs[i] :: xs.drop (i + 1) := List.drop_eq_getElem_cons hi
    rw [hcd]
  have hset : xs.set i x = xs.take i ++ x :: xs.drop (i + 1) := by
    rw [List.set_eq_take_append_cons_drop, if_pos hi]
  have hlen_t : (xs.take i).length = i := List.length_take_of_le (Nat.le_of_lt hi)
  have hw_cons : ∀ (v : Nat) (tail : List Nat),
      wsum (v :: tail) = wsum tail + tail.sum := by
    intro v tail
    simp only [wsum, List.zipIdx_cons, List.map_cons, List.sum_cons, Nat.zero_mul, Nat.zero_add]
    rw [List.map_map]
    simp only [Function.comp]
    have hshift : (List.map (fun a => (a.2 + 1) * a.1) (List.zipIdx tail)).sum =
        (List.map (fun a => a.2 * a.1) (List.zipIdx tail)).sum + tail.sum := by
      have h1 : (List.map (fun a => (a.2 + 1) * a.1) (List.zipIdx tail)) =
          (List.map (fun a => a.2 * a.1 + a.1) (List.zipIdx tail)) := by
        congr 1; ext ⟨a1, a2⟩; simp [Nat.add_mul, Nat.one_mul]
      rw [h1, ← List.sum_map_add]
      congr 1
      · simp [List.zipIdx_map_fst]
    omega
  have hget : xs[i]? = some xs[i] := List.getElem?_eq_getElem hi
  simp only [hget, Option.getD_some]
  rw [hset]
  -- compute LHS wsum
  rw [wsum_append, hw_cons, hlen_t, List.sum_cons]
  -- compute RHS: wsum xs = wsum (take i ++ xs[i] :: drop (i+1))
  conv => rhs; rw [← hsplit]
  rw [wsum_append, hw_cons, hlen_t, List.sum_cons]
  omega

theorem sound_ReverseMoment : ReverseMoment.Goal := by
  intro xs
  simp only [agg, wsum]
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp only [List.reverse_cons, List.length_append, List.length_singleton, List.sum_append,
      List.sum_cons, List.sum_singleton, List.length_reverse, List.length_cons, List.sum_reverse]
    rw [wsum_append]
    simp only [wsum, List.zipIdx_singleton, List.map_cons, List.sum_cons, Nat.zero_mul, Nat.zero_add,
      List.length_reverse]
    have hshift : (List.map (fun a => (a.2 + 1) * a.1) (List.zipIdx xs)).sum =
        (List.map (fun a => a.2 * a.1) (List.zipIdx xs)).sum + xs.sum := by
      have h1 : (List.map (fun a => (a.2 + 1) * a.1) (List.zipIdx xs)) =
          (List.map (fun a => a.2 * a.1 + a.1) (List.zipIdx xs)) := by
        congr 1; ext ⟨a1, a2⟩; simp [Nat.add_mul, Nat.one_mul]
      rw [h1, ← List.sum_map_add]
      congr 1
      · simp [List.zipIdx_map_fst]
    simp only [wsum] at ih
    -- ih : (map (fun a => a.2*a.1) (zipIdx xs.reverse)).sum + (map (fun a => a.2*a.1) (zipIdx xs)).sum = (xs.length-1)*xs.sum
    omega

end IndexWeighted

/-! ## Top-level aliases matching requested names -/

theorem sound_LongestHole_Stitch : LongestHole.Stitch.Goal := LongestHole.sound_Stitch
theorem sound_LongestHole_TreeReduce : LongestHole.TreeReduce.Goal := LongestHole.sound_TreeReduce
theorem sound_LongestHole_RepeatPower : LongestHole.RepeatPower.Goal := LongestHole.sound_RepeatPower
theorem sound_LongestHole_FrameSlice : LongestHole.FrameSlice.Goal := LongestHole.sound_FrameSlice
theorem sound_LongestHole_PrefixCheckpoint : LongestHole.PrefixCheckpoint.Goal := LongestHole.sound_PrefixCheckpoint
theorem sound_LongestHole_BridgeRun : LongestHole.BridgeRun.Goal := LongestHole.sound_BridgeRun
theorem sound_LongestHole_AmendLeaf : LongestHole.AmendLeaf.Goal := LongestHole.sound_AmendLeaf
theorem sound_LongestHole_RunInput : LongestHole.RunInput.Goal := LongestHole.sound_RunInput
theorem sound_LongestHole_ReorderNeighbors : LongestHole.ReorderNeighbors.Goal := LongestHole.sound_ReorderNeighbors
theorem sound_LongestHole_ZeroFill : LongestHole.ZeroFill.Goal := LongestHole.sound_ZeroFill
theorem sound_Drawdown_Stitch : Drawdown.Stitch.Goal := Drawdown.sound_Stitch
theorem sound_Drawdown_TreeReduce : Drawdown.TreeReduce.Goal := Drawdown.sound_TreeReduce
theorem sound_Drawdown_RepeatPower : Drawdown.RepeatPower.Goal := Drawdown.sound_RepeatPower
theorem sound_Drawdown_FrameSlice : Drawdown.FrameSlice.Goal := Drawdown.sound_FrameSlice
theorem sound_Drawdown_PrefixCheckpoint : Drawdown.PrefixCheckpoint.Goal := Drawdown.sound_PrefixCheckpoint
theorem sound_Drawdown_PeakRecovery : Drawdown.PeakRecovery.Goal := Drawdown.sound_PeakRecovery
theorem sound_Drawdown_AmendLeaf : Drawdown.AmendLeaf.Goal := Drawdown.sound_AmendLeaf
theorem sound_Drawdown_RunInput : Drawdown.RunInput.Goal := Drawdown.sound_RunInput
theorem sound_Drawdown_ReorderNeighbors : Drawdown.ReorderNeighbors.Goal := Drawdown.sound_ReorderNeighbors
theorem sound_Drawdown_AscendingPage : Drawdown.AscendingPage.Goal := Drawdown.sound_AscendingPage
theorem sound_MonotoneBreaks_Stitch : MonotoneBreaks.Stitch.Goal := MonotoneBreaks.sound_Stitch
theorem sound_MonotoneBreaks_TreeReduce : MonotoneBreaks.TreeReduce.Goal := MonotoneBreaks.sound_TreeReduce
theorem sound_MonotoneBreaks_RepeatPower : MonotoneBreaks.RepeatPower.Goal := MonotoneBreaks.sound_RepeatPower
theorem sound_MonotoneBreaks_FrameSlice : MonotoneBreaks.FrameSlice.Goal := MonotoneBreaks.sound_FrameSlice
theorem sound_MonotoneBreaks_PrefixCheckpoint : MonotoneBreaks.PrefixCheckpoint.Goal := MonotoneBreaks.sound_PrefixCheckpoint
theorem sound_MonotoneBreaks_ParallelMergeFree : MonotoneBreaks.ParallelMergeFree.Goal := MonotoneBreaks.sound_ParallelMergeFree
theorem sound_MonotoneBreaks_AmendLeaf : MonotoneBreaks.AmendLeaf.Goal := MonotoneBreaks.sound_AmendLeaf
theorem sound_MonotoneBreaks_RunInput : MonotoneBreaks.RunInput.Goal := MonotoneBreaks.sound_RunInput
theorem sound_MonotoneBreaks_ReorderNeighbors : MonotoneBreaks.ReorderNeighbors.Goal := MonotoneBreaks.sound_ReorderNeighbors
theorem sound_MonotoneBreaks_SortingCertificate : MonotoneBreaks.SortingCertificate.Goal := MonotoneBreaks.sound_SortingCertificate
theorem sound_DeltaBytes_Stitch : DeltaBytes.Stitch.Goal := DeltaBytes.sound_Stitch
theorem sound_DeltaBytes_TreeReduce : DeltaBytes.TreeReduce.Goal := DeltaBytes.sound_TreeReduce
theorem sound_DeltaBytes_RepeatPower : DeltaBytes.RepeatPower.Goal := DeltaBytes.sound_RepeatPower
theorem sound_DeltaBytes_FrameSlice : DeltaBytes.FrameSlice.Goal := DeltaBytes.sound_FrameSlice
theorem sound_DeltaBytes_PrefixCheckpoint : DeltaBytes.PrefixCheckpoint.Goal := DeltaBytes.sound_PrefixCheckpoint
theorem sound_DeltaBytes_ReverseResidualCliff : DeltaBytes.ReverseResidualCliff.Goal := DeltaBytes.sound_ReverseResidualCliff
theorem sound_DeltaBytes_AmendLeaf : DeltaBytes.AmendLeaf.Goal := DeltaBytes.sound_AmendLeaf
theorem sound_DeltaBytes_RunInput : DeltaBytes.RunInput.Goal := DeltaBytes.sound_RunInput
theorem sound_DeltaBytes_ReorderNeighbors : DeltaBytes.ReorderNeighbors.Goal := DeltaBytes.sound_ReorderNeighbors
theorem sound_DeltaBytes_RepeatedConstant : DeltaBytes.RepeatedConstant.Goal := DeltaBytes.sound_RepeatedConstant
theorem sound_PairSubsequence_Stitch : PairSubsequence.Stitch.Goal := PairSubsequence.sound_Stitch
theorem sound_PairSubsequence_TreeReduce : PairSubsequence.TreeReduce.Goal := PairSubsequence.sound_TreeReduce
theorem sound_PairSubsequence_RepeatPower : PairSubsequence.RepeatPower.Goal := PairSubsequence.sound_RepeatPower
theorem sound_PairSubsequence_FrameSlice : PairSubsequence.FrameSlice.Goal := PairSubsequence.sound_FrameSlice
theorem sound_PairSubsequence_PrefixCheckpoint : PairSubsequence.PrefixCheckpoint.Goal := PairSubsequence.sound_PrefixCheckpoint
theorem sound_PairSubsequence_ReversalComplement : PairSubsequence.ReversalComplement.Goal := PairSubsequence.sound_ReversalComplement
theorem sound_PairSubsequence_AmendLeaf : PairSubsequence.AmendLeaf.Goal := PairSubsequence.sound_AmendLeaf
theorem sound_PairSubsequence_RunInput : PairSubsequence.RunInput.Goal := PairSubsequence.sound_RunInput
theorem sound_PairSubsequence_ReorderNeighbors : PairSubsequence.ReorderNeighbors.Goal := PairSubsequence.sound_ReorderNeighbors
theorem sound_PairSubsequence_BatchedMarkers : PairSubsequence.BatchedMarkers.Goal := PairSubsequence.sound_BatchedMarkers
theorem sound_RollingFingerprint_Stitch : RollingFingerprint.Stitch.Goal := RollingFingerprint.sound_Stitch
theorem sound_RollingFingerprint_TreeReduce : RollingFingerprint.TreeReduce.Goal := RollingFingerprint.sound_TreeReduce
theorem sound_RollingFingerprint_RepeatPower : RollingFingerprint.RepeatPower.Goal := RollingFingerprint.sound_RepeatPower
theorem sound_RollingFingerprint_FrameSlice : RollingFingerprint.FrameSlice.Goal := RollingFingerprint.sound_FrameSlice
theorem sound_RollingFingerprint_PrefixCheckpoint : RollingFingerprint.PrefixCheckpoint.Goal := RollingFingerprint.sound_PrefixCheckpoint
theorem sound_RollingFingerprint_EqualWindowResidues : RollingFingerprint.EqualWindowResidues.Goal := RollingFingerprint.sound_EqualWindowResidues
theorem sound_RollingFingerprint_AmendLeaf : RollingFingerprint.AmendLeaf.Goal := RollingFingerprint.sound_AmendLeaf
theorem sound_RollingFingerprint_RunInput : RollingFingerprint.RunInput.Goal := RollingFingerprint.sound_RunInput
theorem sound_RollingFingerprint_ReorderNeighbors : RollingFingerprint.ReorderNeighbors.Goal := RollingFingerprint.sound_ReorderNeighbors
theorem sound_RollingFingerprint_CollisionBoundary : RollingFingerprint.CollisionBoundary.Goal := RollingFingerprint.sound_CollisionBoundary
theorem sound_BalanceFloor_Stitch : BalanceFloor.Stitch.Goal := BalanceFloor.sound_Stitch
theorem sound_BalanceFloor_TreeReduce : BalanceFloor.TreeReduce.Goal := BalanceFloor.sound_TreeReduce
theorem sound_BalanceFloor_RepeatPower : BalanceFloor.RepeatPower.Goal := BalanceFloor.sound_RepeatPower
theorem sound_BalanceFloor_FrameSlice : BalanceFloor.FrameSlice.Goal := BalanceFloor.sound_FrameSlice
theorem sound_BalanceFloor_PrefixCheckpoint : BalanceFloor.PrefixCheckpoint.Goal := BalanceFloor.sound_PrefixCheckpoint
theorem sound_BalanceFloor_ShardAdmission : BalanceFloor.ShardAdmission.Goal := BalanceFloor.sound_ShardAdmission
theorem sound_BalanceFloor_AmendLeaf : BalanceFloor.AmendLeaf.Goal := BalanceFloor.sound_AmendLeaf
theorem sound_BalanceFloor_RunInput : BalanceFloor.RunInput.Goal := BalanceFloor.sound_RunInput
theorem sound_BalanceFloor_ReorderNeighbors : BalanceFloor.ReorderNeighbors.Goal := BalanceFloor.sound_ReorderNeighbors
theorem sound_BalanceFloor_CreditLimit : BalanceFloor.CreditLimit.Goal := BalanceFloor.sound_CreditLimit
theorem sound_BitOccupancy_Stitch : BitOccupancy.Stitch.Goal := BitOccupancy.sound_Stitch
theorem sound_BitOccupancy_TreeReduce : BitOccupancy.TreeReduce.Goal := BitOccupancy.sound_TreeReduce
theorem sound_BitOccupancy_RepeatPower : BitOccupancy.RepeatPower.Goal := BitOccupancy.sound_RepeatPower
theorem sound_BitOccupancy_FrameSlice : BitOccupancy.FrameSlice.Goal := BitOccupancy.sound_FrameSlice
theorem sound_BitOccupancy_PrefixCheckpoint : BitOccupancy.PrefixCheckpoint.Goal := BitOccupancy.sound_PrefixCheckpoint
theorem sound_BitOccupancy_FoldInto32 : BitOccupancy.FoldInto32.Goal := BitOccupancy.sound_FoldInto32
theorem sound_BitOccupancy_AmendLeaf : BitOccupancy.AmendLeaf.Goal := BitOccupancy.sound_AmendLeaf
theorem sound_BitOccupancy_RunInput : BitOccupancy.RunInput.Goal := BitOccupancy.sound_RunInput
theorem sound_BitOccupancy_ReorderNeighbors : BitOccupancy.ReorderNeighbors.Goal := BitOccupancy.sound_ReorderNeighbors
theorem sound_BitOccupancy_NegativeTest : BitOccupancy.NegativeTest.Goal := BitOccupancy.sound_NegativeTest
theorem sound_FirstLastChange_Stitch : FirstLastChange.Stitch.Goal := FirstLastChange.sound_Stitch
theorem sound_FirstLastChange_TreeReduce : FirstLastChange.TreeReduce.Goal := FirstLastChange.sound_TreeReduce
theorem sound_FirstLastChange_RepeatPower : FirstLastChange.RepeatPower.Goal := FirstLastChange.sound_RepeatPower
theorem sound_FirstLastChange_FrameSlice : FirstLastChange.FrameSlice.Goal := FirstLastChange.sound_FrameSlice
theorem sound_FirstLastChange_PrefixCheckpoint : FirstLastChange.PrefixCheckpoint.Goal := FirstLastChange.sound_PrefixCheckpoint
theorem sound_FirstLastChange_DuplicateBlock : FirstLastChange.DuplicateBlock.Goal := FirstLastChange.sound_DuplicateBlock
theorem sound_FirstLastChange_AmendLeaf : FirstLastChange.AmendLeaf.Goal := FirstLastChange.sound_AmendLeaf
theorem sound_FirstLastChange_RunInput : FirstLastChange.RunInput.Goal := FirstLastChange.sound_RunInput
theorem sound_FirstLastChange_ReorderNeighbors : FirstLastChange.ReorderNeighbors.Goal := FirstLastChange.sound_ReorderNeighbors
theorem sound_FirstLastChange_ReverseCount : FirstLastChange.ReverseCount.Goal := FirstLastChange.sound_ReverseCount
theorem sound_IndexWeighted_Stitch : IndexWeighted.Stitch.Goal := IndexWeighted.sound_Stitch
theorem sound_IndexWeighted_TreeReduce : IndexWeighted.TreeReduce.Goal := IndexWeighted.sound_TreeReduce
theorem sound_IndexWeighted_RepeatPower : IndexWeighted.RepeatPower.Goal := IndexWeighted.sound_RepeatPower
theorem sound_IndexWeighted_FrameSlice : IndexWeighted.FrameSlice.Goal := IndexWeighted.sound_FrameSlice
theorem sound_IndexWeighted_PrefixCheckpoint : IndexWeighted.PrefixCheckpoint.Goal := IndexWeighted.sound_PrefixCheckpoint
theorem sound_IndexWeighted_ReplaceAt : IndexWeighted.ReplaceAt.Goal := IndexWeighted.sound_ReplaceAt
theorem sound_IndexWeighted_AmendLeaf : IndexWeighted.AmendLeaf.Goal := IndexWeighted.sound_AmendLeaf
theorem sound_IndexWeighted_RunInput : IndexWeighted.RunInput.Goal := IndexWeighted.sound_RunInput
theorem sound_IndexWeighted_ReorderNeighbors : IndexWeighted.ReorderNeighbors.Goal := IndexWeighted.sound_ReorderNeighbors
theorem sound_IndexWeighted_ReverseMoment : IndexWeighted.ReverseMoment.Goal := IndexWeighted.sound_ReverseMoment

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters
