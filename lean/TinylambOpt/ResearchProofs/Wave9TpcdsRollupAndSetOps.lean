import TinylambOpt.ResearchGoals.Wave9.TpcdsRollupAndSetOps

namespace TinylambOpt.ResearchProofs.Wave9TpcdsRollupAndSetOps

open TinylambOpt.ResearchGoals.AnalyticsWorkloads (Totals zero weighted plus summarize histogram)
open TinylambOpt.ResearchGoals.Wave9.TpcdsRollupAndSetOps

/-! ## Shared algebra for `Totals` -/

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a; cases b; cases c
  simp [plus]
  omega

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  cases a; cases b
  simp [plus]
  omega

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  cases a
  simp [plus, zero]

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  cases a
  simp [plus, zero]

private theorem foldl_add_left {α : Type} (xs : List α) (g : α → Totals)
    (acc a : Totals) :
    xs.foldl (fun s x => plus s (g x)) (plus a acc) =
      plus a (xs.foldl (fun s x => plus s (g x)) acc) := by
  induction xs generalizing acc with
  | nil => simp
  | cons x xs ih =>
      change xs.foldl (fun s y => plus s (g y)) (plus (plus a acc) (g x)) =
        plus a (xs.foldl (fun s y => plus s (g y)) (plus acc (g x)))
      rw [show plus (plus a acc) (g x) = plus a (plus acc (g x)) by
        simpa [plus_assoc]]
      exact ih (acc := plus acc (g x))

private theorem foldl_add_sum {α : Type} (xs : List α) (g : α → Totals) (acc : Totals) :
    xs.foldl (fun s x => plus s (g x)) acc =
      plus acc (xs.foldl (fun s x => plus s (g x)) zero) := by
  simpa [plus_zero_right] using foldl_add_left xs g zero acc

private theorem summarize_cons (v : Option Int) (vs : List (Option Int)) :
    summarize (v :: vs) = plus (weighted 1 v) (summarize vs) := by
  change vs.foldl (fun s x => plus s (weighted 1 x)) (plus zero (weighted 1 v)) =
    plus (weighted 1 v) (vs.foldl (fun s x => plus s (weighted 1 x)) zero)
  rw [show plus zero (weighted 1 v) = plus (weighted 1 v) zero by exact plus_comm _ _]
  exact foldl_add_left vs (fun x : Option Int => weighted 1 x) zero (weighted 1 v)

private theorem summarize_append (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  unfold summarize
  rw [List.foldl_append]
  exact foldl_add_sum ys (fun v : Option Int => weighted 1 v)
    (xs.foldl (fun s v => plus s (weighted 1 v)) zero)

/-! ## Generic histogram lookup -/

private theorem histogram_foldl_getD {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    [LawfulBEq α] (xs : List α) (m : Std.HashMap α Nat) (k : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 =
      m.getD k 0 + (xs.filter (fun x => x == k)).length := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih (m.insert x (m.getD x 0 + 1))]
      rw [Std.HashMap.getD_insert]
      by_cases h : (x == k) = true
      · rw [if_pos h]
        have hxk : x = k := LawfulBEq.eq_of_beq h
        subst k
        simp [ih]
        omega
      · rw [if_neg h]
        simp [h]

private theorem hist_getD_count {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    [LawfulBEq α] (xs : List α) (k : α) :
    (histogram xs).getD k 0 = (xs.filter (fun x => x == k)).length := by
  simpa [histogram] using histogram_foldl_getD xs ({} : Std.HashMap α Nat) k

/-! ## G2: SurrogateIntersectSetCount -/

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List α), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length ?_ _ rfl)
            · have hb' := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
              have hba : b ≠ a := by
                simpa using hb'
              exact fun heq => hba heq.symm
            · rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
  exact key l.length l rfl

private theorem contains_iff_mem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.contains a = true ↔ a ∈ l := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem proj_perm {α β : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β) (l : List α) :
    List.Perm (l.eraseDups.map f).eraseDups (l.map f).eraseDups := by
  apply List.perm_iff_count.mpr
  intro k
  have h1 := (List.nodup_iff_count_eq_ite.mp (nodup_eraseDups (l.eraseDups.map f))) k
  have h2 := (List.nodup_iff_count_eq_ite.mp (nodup_eraseDups (l.map f))) k
  rw [h1, h2]
  have hmem : k ∈ (l.eraseDups.map f).eraseDups ↔ k ∈ (l.map f).eraseDups := by
    constructor
    · intro hk
      have hk' : k ∈ l.eraseDups.map f := List.mem_eraseDups.mp hk
      rcases List.mem_map.mp hk' with ⟨a, ha, rfl⟩
      apply List.mem_eraseDups.mpr
      exact List.mem_map.mpr ⟨a, List.mem_eraseDups.mp ha, rfl⟩
    · intro hk
      have hk' : k ∈ l.map f := List.mem_eraseDups.mp hk
      rcases List.mem_map.mp hk' with ⟨a, ha, rfl⟩
      apply List.mem_eraseDups.mpr
      exact List.mem_map.mpr ⟨a, List.mem_eraseDups.mpr ha, rfl⟩
  by_cases hp : k ∈ (l.eraseDups.map f).eraseDups
  · have hq : k ∈ (l.map f).eraseDups := hmem.mp hp
    simp [hp, hq]
  · have hq : k ∉ (l.map f).eraseDups := by
      intro hk
      exact hp (hmem.mpr hk)
    simp [hp, hq]

private theorem contains_of_perm {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (p : List.Perm l₁ l₂) (a : α) : l₁.contains a = l₂.contains a := by
  apply Bool.eq_iff_iff.mpr
  rw [contains_iff_mem, contains_iff_mem, p.mem_iff]

theorem SurrogateIntersectSetCount_sound : SurrogateIntersectSetCount.Goal := by
  intro c1 c2 c3 nameOf dateOf
  let f : Nat × Nat → Nat × Nat := fun (c, d) => (nameOf c, dateOf d)
  let a1 := (c1.eraseDups.map f).eraseDups
  let a2 := (c2.eraseDups.map f).eraseDups
  let a3 := (c3.eraseDups.map f).eraseDups
  let b1 := (c1.map f).eraseDups
  let b2 := (c2.map f).eraseDups
  let b3 := (c3.map f).eraseDups
  have h1 : List.Perm a1 b1 := by simpa [a1, b1] using proj_perm f c1
  have h2 : List.Perm a2 b2 := by simpa [a2, b2] using proj_perm f c2
  have h3 : List.Perm a3 b3 := by simpa [a3, b3] using proj_perm f c3
  unfold surrogateIntersect refIntersect
  dsimp only
  change (a1.filter (fun k => a2.contains k && a3.contains k)).length =
    (b1.filter (fun k => b2.contains k && b3.contains k)).length
  rw [← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
  refine h1.countP_congr ?_
  intro k hk
  rw [contains_of_perm h2 k, contains_of_perm h3 k]

/-! ## G3: DisjointRangeBucketHistogram -/

private theorem diffBucket_row (sh so : Option Int) (bin : Fin 5) :
    (diffBucket sh so == some bin) =
      match sh, so with
      | some s, some o =>
        let d := s - o
        match bin.val with
        | 0 => d ≤ 30
        | 1 => 30 < d && d ≤ 60
        | 2 => 60 < d && d ≤ 90
        | 3 => 90 < d && d ≤ 120
        | _ => 120 < d
      | _, _ => false := by
  rcases bin with ⟨bv, hbv⟩
  cases sh with
  | none => simp [diffBucket]
  | some s =>
      cases so with
      | none => simp [diffBucket]
      | some o =>
          have hcases : bv = 0 ∨ bv = 1 ∨ bv = 2 ∨ bv = 3 ∨ bv = 4 := by omega
          rcases hcases with rfl | rfl | rfl | rfl | rfl
          all_goals
            by_cases h30 : s - o ≤ 30
            <;> by_cases h60 : s - o ≤ 60
            <;> by_cases h90 : s - o ≤ 90
            <;> by_cases h120 : s - o ≤ 120
            <;> simp [diffBucket, h30, h60, h90, h120]
            <;> omega

theorem DisjointRangeBucketHistogram_sound : DisjointRangeBucketHistogram.Goal := by
  intro rows bin
  unfold bucketHist refCaseSum
  apply List.countP_congr
  intro r hr
  rcases r with ⟨sh, so⟩
  simp only
  rw [diffBucket_row sh so bin]
  simp only
  rfl

/-! ## G4: LeftJoinNullFlagSplitCount -/

private def pcAdd (a b : PromoCounts) : PromoCounts :=
  ⟨a.noPromo + b.noPromo, a.promo + b.promo, a.total + b.total⟩

private def pcZero : PromoCounts := ⟨0, 0, 0⟩

private theorem pcAdd_assoc (a b c : PromoCounts) : pcAdd (pcAdd a b) c = pcAdd a (pcAdd b c) := by
  cases a; cases b; cases c
  simp [pcAdd]
  omega

private theorem pcAdd_comm (a b : PromoCounts) : pcAdd a b = pcAdd b a := by
  cases a; cases b
  simp [pcAdd]
  omega

private theorem pcAdd_zero_left (a : PromoCounts) : pcAdd pcZero a = a := by
  cases a
  simp [pcAdd, pcZero]

private theorem pcAdd_zero_right (a : PromoCounts) : pcAdd a pcZero = a := by
  cases a
  simp [pcAdd, pcZero]

private theorem pc_foldl_add (xs : List (Option Nat)) (g : Option Nat → PromoCounts) (acc : PromoCounts) :
    xs.foldl (fun s x => pcAdd s (g x)) acc =
      pcAdd acc (xs.foldl (fun s x => pcAdd s (g x)) pcZero) := by
  induction xs generalizing acc with
  | nil => simp [pcAdd_zero_right]
  | cons x xs ih =>
      change xs.foldl (fun s y => pcAdd s (g y)) (pcAdd acc (g x)) =
        pcAdd acc (xs.foldl (fun s y => pcAdd s (g y)) (pcAdd pcZero (g x)))
      rw [ih (acc := pcAdd acc (g x))]
      rw [show pcAdd pcZero (g x) = g x by exact pcAdd_zero_left (g x)]
      rw [ih (acc := g x)]
      exact pcAdd_assoc acc (g x) _

private def pcOf (l : List (Option Nat)) : PromoCounts :=
  ⟨l.countP Option.isNone, l.countP Option.isSome, l.length⟩

private theorem pcOf_append (l₁ l₂ : List (Option Nat)) :
    pcOf (l₁ ++ l₂) = pcAdd (pcOf l₁) (pcOf l₂) := by
  simp [pcOf, pcAdd, List.countP_append]

private theorem pcOf_flatMap (xs : List (Option Nat)) (g : Option Nat → List (Option Nat)) :
    pcOf (xs.flatMap g) = xs.foldl (fun acc x => pcAdd acc (pcOf (g x))) pcZero := by
  induction xs with
  | nil => simp [pcOf, pcZero]
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      rw [pcOf_append, ih]
      rw [List.foldl_cons]
      rw [show pcAdd pcZero (pcOf (g x)) = pcOf (g x) by exact pcAdd_zero_left (pcOf (g x))]
      symm
      rw [pc_foldl_add]

private theorem count_filterMap_id (promos : List (Option Nat)) (k : Nat) :
    ((promos.filterMap id).filter (fun n => n == k)).length =
      (promos.filter (fun p : Option Nat => p == some k)).length := by
  induction promos with
  | nil => simp
  | cons p ps ih =>
      cases p with
      | none => simp [ih]
      | some n =>
          by_cases h : n == k
          · have hnk : n = k := by simpa using h
            subst k
            simp [ih]
          · have hne : n ≠ k := by simpa using h
            simp [h, ih, hne]

private theorem countP_none_filter (promos : List (Option Nat)) (k : Nat) :
    (promos.filter (fun p : Option Nat => p == some k)).countP Option.isNone = 0 := by
  induction promos with
  | nil => simp
  | cons p ps ih =>
      by_cases h : p == some k
      · have hp : p = some k := LawfulBEq.eq_of_beq h
        subst p
        simp [h, ih]
      · simp [h, ih]

private theorem countP_some_filter (promos : List (Option Nat)) (k : Nat) :
    (promos.filter (fun p : Option Nat => p == some k)).countP Option.isSome =
      (promos.filter (fun p : Option Nat => p == some k)).length := by
  induction promos with
  | nil => simp
  | cons p ps ih =>
      by_cases h : p == some k
      · have hp : p = some k := LawfulBEq.eq_of_beq h
        subst p
        simp [h, ih]
      · simp [h, ih]

private def refStep (promos : List (Option Nat)) (fk : Option Nat) : List (Option Nat) :=
  match fk with
  | none => [none]
  | some k =>
    let hits := promos.filter (fun p : Option Nat => p == some k)
    if hits.isEmpty then [none] else hits

private def collapsedStep (promos : List (Option Nat)) (fk : Option Nat) : PromoCounts :=
  let mult := histogram (promos.filterMap id)
  let m := fk.elim 0 (fun k => mult.getD k 0)
  if m == 0 then ⟨1, 0, 1⟩ else ⟨0, m, m⟩

private theorem step_eq (promos : List (Option Nat)) (fk : Option Nat) :
    collapsedStep promos fk = pcOf (refStep promos fk) := by
  cases fk with
  | none => simp [collapsedStep, refStep, pcOf]
  | some k =>
      let hits := promos.filter (fun p : Option Nat => p == some k)
      let m := (histogram (promos.filterMap id)).getD k 0
      have hm : m = hits.length := by
        dsimp [m, hits]
        rw [hist_getD_count]
        exact count_filterMap_id promos k
      change (if m == 0 then (⟨1, 0, 1⟩ : PromoCounts) else ⟨0, m, m⟩) =
        pcOf (if hits.isEmpty then [none] else hits)
      by_cases hm0 : m == 0
      · have hmz : m = 0 := by simpa using hm0
        have hlen0 : hits.length = 0 := by omega
        have hisEmpty : hits.isEmpty = true := by
          exact List.isEmpty_iff_length_eq_zero.mpr hlen0
        simp [hm0, hisEmpty, pcOf]
      · have hne : m ≠ 0 := by simpa using hm0
        have hlenpos : 0 < hits.length := by omega
        have hisEmpty : hits.isEmpty = false := by
          by_cases he : hits.isEmpty = true
          · have : hits.length = 0 := List.isEmpty_iff_length_eq_zero.mp he
            omega
          · simpa using he
        have hn : hits.countP Option.isNone = 0 := by
          simpa [hits] using countP_none_filter promos k
        have hs : hits.countP Option.isSome = hits.length := by
          simpa [hits] using countP_some_filter promos k
        have hhnil : hits ≠ [] := by
          intro hnil
          have : hits.length = 0 := by rw [hnil]; simp
          omega
        simp [hm0, hisEmpty, pcOf, hn, hs, hm, hhnil]

private def collapsedStepAcc (promos : List (Option Nat)) (acc : PromoCounts) (fk : Option Nat) : PromoCounts :=
  let mult := histogram (promos.filterMap id)
  let m := fk.elim 0 (fun k => mult.getD k 0)
  if m == 0 then ⟨acc.noPromo + 1, acc.promo, acc.total + 1⟩
  else ⟨acc.noPromo, acc.promo + m, acc.total + m⟩

private theorem stepAcc_eq_pcAdd (promos : List (Option Nat)) (acc : PromoCounts) (fk : Option Nat) :
    collapsedStepAcc promos acc fk = pcAdd acc (collapsedStep promos fk) := by
  dsimp [collapsedStepAcc, collapsedStep]
  cases acc
  by_cases h : (fk.elim 0 fun k => (histogram (List.filterMap id promos)).getD k 0) == 0 <;>
    simp [h, pcAdd]

private theorem collapsed_fold_eq_acc (facts promos : List (Option Nat)) (acc : PromoCounts) :
    facts.foldl (collapsedStepAcc promos) acc =
      facts.foldl (fun acc fk => pcAdd acc (collapsedStep promos fk)) acc := by
  induction facts generalizing acc with
  | nil => rfl
  | cons fk fs ih =>
      simp only [List.foldl_cons]
      rw [stepAcc_eq_pcAdd]
      exact ih (acc := pcAdd acc (collapsedStep promos fk))

private theorem collapsed_fold_eq (facts promos : List (Option Nat)) :
    facts.foldl (collapsedStepAcc promos) pcZero =
      facts.foldl (fun acc fk => pcAdd acc (collapsedStep promos fk)) pcZero :=
  collapsed_fold_eq_acc facts promos pcZero

theorem LeftJoinNullFlagSplitCount_sound : LeftJoinNullFlagSplitCount.Goal := by
  intro facts promos
  unfold collapsedLeftPromo refLeftPromo
  change facts.foldl (collapsedStepAcc promos) pcZero =
    pcOf (facts.flatMap (refStep promos))
  rw [pcOf_flatMap, collapsed_fold_eq]
  induction facts with
  | nil => rfl
  | cons fk fs ih =>
      simp only [List.foldl_cons]
      rw [step_eq]
      rw [pc_foldl_add (xs := fs) (g := fun x => collapsedStep promos x)
        (acc := pcAdd pcZero (pcOf (refStep promos fk)))]
      rw [pc_foldl_add (xs := fs) (g := fun x => pcOf (refStep promos x))
        (acc := pcAdd pcZero (pcOf (refStep promos fk)))]
      rw [ih]

/-! ## G1: SurrogateKeyPreAggRollup -/

private def skValues (facts : List (Nat × Option Int)) (sk : Nat) : List (Option Int) :=
  (facts.filter fun (s, _) => s == sk).map fun (_, v) => v

private theorem summarize_cnt (vs : List (Option Int)) : (summarize vs).cnt = vs.length := by
  induction vs with
  | nil => rfl
  | cons v vs ih =>
      simp [summarize_cons, weighted, plus]
      omega

private theorem fold_facts_getD (facts : List (Nat × Option Int))
    (m : Std.HashMap Nat Totals) (sk : Nat) :
    (facts.foldl (fun mm (s, v) => mm.insert s (plus (mm.getD s zero) (weighted 1 v))) m).getD sk zero =
      plus (m.getD sk zero) (summarize (skValues facts sk)) := by
  induction facts generalizing m with
  | nil => simp [skValues, summarize, plus_zero_right]
  | cons f facts ih =>
      rcases f with ⟨s, v⟩
      simp only [List.foldl_cons]
      rw [ih]
      rw [Std.HashMap.getD_insert]
      by_cases h : (s == sk) = true
      · rw [if_pos h]
        have hsk : s = sk := LawfulBEq.eq_of_beq h
        subst sk
        have hsv : skValues ((s, v) :: facts) s = v :: skValues facts s := by
          simp [skValues]
        rw [hsv, summarize_cons, plus_assoc]
      · rw [if_neg h]
        have hne : s ≠ sk := by
          intro he
          apply h
          simpa [he]
        have hsv : skValues ((s, v) :: facts) sk = skValues facts sk := by
          simp [skValues, hne]
        rw [hsv]

private theorem bySk_getD (facts : List (Nat × Option Int)) (sk : Nat) :
    (facts.foldl (fun mm (s, v) => mm.insert s (plus (mm.getD s zero) (weighted 1 v)))
      ({} : Std.HashMap Nat Totals)).getD sk zero = summarize (skValues facts sk) := by
  simpa [plus_zero_left] using fold_facts_getD facts ({} : Std.HashMap Nat Totals) sk

private def stOf (facts : List (Nat × Option Int)) (sk : Nat) : Totals :=
  summarize (skValues facts sk)

private theorem stOf_update (facts : List (Nat × Option Int)) (sk : Nat) (v : Option Int) (k : Nat) :
    stOf ((sk, v) :: facts) k =
      if k = sk then plus (weighted 1 v) (stOf facts k) else stOf facts k := by
  by_cases h : k = sk
  · subst k
    simp [stOf, skValues, summarize_cons]
  · have hbe : (sk == k) = false := beq_eq_false_iff_ne.mpr (by intro he; exact h he.symm)
    simp [stOf, skValues, h, hbe]

private theorem stOf_cnt_zero (facts : List (Nat × Option Int)) (sk : Nat) :
    (stOf facts sk).cnt == 0 → stOf facts sk = zero := by
  intro h
  have hz : (stOf facts sk).cnt = 0 := by simpa using h
  have hc : (skValues facts sk).length = 0 := by
    have hcnt := summarize_cnt (skValues facts sk)
    dsimp [stOf] at hz
    omega
  have hnil : skValues facts sk = [] := List.eq_nil_of_length_eq_zero hc
  simp [stOf, hnil, summarize]

private def prefixAdd (key : RollupKey) (ps : List RollupKey) (st : Totals) : Totals :=
  ps.foldl (fun a rk => if rk = key then plus a st else a) zero

private theorem prefixAdd_cons (key : RollupKey) (rk : RollupKey) (ps : List RollupKey) (st : Totals) :
    prefixAdd key (rk :: ps) st =
      ps.foldl (fun a x => if x = key then plus a st else a) (if rk = key then plus zero st else zero) := rfl

private theorem prefixAdd_acc (key : RollupKey) (ps : List RollupKey) (st acc : Totals) :
    ps.foldl (fun a rk => if rk = key then plus a st else a) acc = plus acc (prefixAdd key ps st) := by
  induction ps generalizing acc with
  | nil => simp [prefixAdd, plus_zero_right]
  | cons rk ps ih =>
      simp only [List.foldl_cons]
      by_cases h : rk = key
      · subst rk
        rw [prefixAdd_cons]
        simp only [if_true]
        rw [show plus zero st = st by exact plus_zero_left st]
        rw [ih (acc := plus acc st)]
        rw [ih (acc := st)]
        rw [plus_assoc]
      · rw [prefixAdd_cons]
        simp [h]
        rw [show ps.foldl (fun a x => if x = key then plus a st else a) zero = prefixAdd key ps st by rfl]
        rw [ih (acc := acc)]

private theorem prefixAdd_zero (key : RollupKey) (ps : List RollupKey) :
    prefixAdd key ps zero = zero := by
  induction ps with
  | nil => rfl
  | cons rk ps ih =>
      rw [prefixAdd_cons]
      by_cases h : rk = key
      · subst rk
        simp only [if_true]
        rw [show plus zero zero = (zero : Totals) by exact plus_zero_left zero]
        rw [show ps.foldl (fun a x => if x = key then plus a zero else a) zero = prefixAdd key ps zero by rfl]
        exact ih
      · simp [h]
        rw [show ps.foldl (fun a x => if x = key then plus a zero else a) zero = prefixAdd key ps zero by rfl]
        exact ih

private theorem prefixAdd_plus (key : RollupKey) (ps : List RollupKey) (a b : Totals) :
    prefixAdd key ps (plus a b) = plus (prefixAdd key ps a) (prefixAdd key ps b) := by
  induction ps with
  | nil => simp [prefixAdd, plus_zero_left]
  | cons rk ps ih =>
      by_cases h : rk = key
      · subst rk
        rw [prefixAdd_cons, prefixAdd_cons, prefixAdd_cons]
        simp only [if_true]
        rw [show plus zero (plus a b) = plus a b by exact plus_zero_left (plus a b)]
        rw [show plus zero a = a by exact plus_zero_left a]
        rw [show plus zero b = b by exact plus_zero_left b]
        rw [prefixAdd_acc key ps (plus a b) (plus a b)]
        rw [prefixAdd_acc key ps a a]
        rw [prefixAdd_acc key ps b b]
        rw [ih]
        generalize hA : prefixAdd key ps a = A
        generalize hB : prefixAdd key ps b = B
        cases a; cases b; cases A; cases B
        simp [plus]
        omega
      · rw [prefixAdd_cons, prefixAdd_cons, prefixAdd_cons]
        simp [h]
        exact ih

private theorem prefixAdd_update (key : RollupKey) (ps : List RollupKey)
    (w old : Totals) (c : Prop) [Decidable c] :
    prefixAdd key ps (if c then plus w old else old) =
      plus (if c then prefixAdd key ps w else zero) (prefixAdd key ps old) := by
  by_cases hc : c <;> simp [hc, prefixAdd_plus, plus_zero_left]

private def P (dims : List ItemDim) (key : RollupKey) (st : Nat → Totals) : Totals :=
  dims.foldl (fun acc it => plus acc (prefixAdd key (rollupPrefixes it) (st it.sk))) zero

private theorem P_update (dims : List ItemDim) (key : RollupKey) (st : Nat → Totals)
    (sk : Nat) (w : Totals) :
    P dims key (fun k => if k = sk then plus w (st k) else st k) =
      plus (P dims key (fun k => if k = sk then w else zero)) (P dims key st) := by
  induction dims with
  | nil => simp [P, plus_zero_left]
  | cons it dims ih =>
      simp only [P, List.foldl_cons]
      rw [show plus zero (prefixAdd key (rollupPrefixes it)
          ((fun k => if k = sk then plus w (st k) else st k) it.sk)) =
          prefixAdd key (rollupPrefixes it)
            ((fun k => if k = sk then plus w (st k) else st k) it.sk) by exact plus_zero_left _]
      rw [show plus zero (prefixAdd key (rollupPrefixes it)
          ((fun k => if k = sk then w else zero) it.sk)) =
          prefixAdd key (rollupPrefixes it)
            ((fun k => if k = sk then w else zero) it.sk) by exact plus_zero_left _]
      rw [show plus zero (prefixAdd key (rollupPrefixes it) (st it.sk)) =
          prefixAdd key (rollupPrefixes it) (st it.sk) by exact plus_zero_left _]
      rw [foldl_add_sum]
      rw [foldl_add_sum]
      rw [foldl_add_sum]
      rw [prefixAdd_update]
      simp only [plus_zero_left, plus_zero_right]
      rw [show (dims.foldl (fun acc it => plus acc (prefixAdd key (rollupPrefixes it) (if it.sk = sk then w else zero)))
          (prefixAdd key (rollupPrefixes it) (if it.sk = sk then w else zero))) =
          plus (prefixAdd key (rollupPrefixes it) (if it.sk = sk then w else zero))
            (P dims key (fun k => if k = sk then w else zero)) by
            simpa [P] using foldl_add_sum dims
              (fun it => prefixAdd key (rollupPrefixes it) (if it.sk = sk then w else zero))
              (prefixAdd key (rollupPrefixes it) (if it.sk = sk then w else zero))]
      rw [show (dims.foldl (fun acc it => plus acc (prefixAdd key (rollupPrefixes it) (st it.sk)))
          (prefixAdd key (rollupPrefixes it) (st it.sk))) =
          plus (prefixAdd key (rollupPrefixes it) (st it.sk)) (P dims key st) by
            simpa [P] using foldl_add_sum dims
              (fun it => prefixAdd key (rollupPrefixes it) (st it.sk))
              (prefixAdd key (rollupPrefixes it) (st it.sk))]
      rw [show (dims.foldl (fun s x => plus s (prefixAdd key (rollupPrefixes x)
          (if x.sk = sk then plus w (st x.sk) else st x.sk))) zero) =
          P dims key (fun k => if k = sk then plus w (st k) else st k) by rfl]
      rw [ih]
      by_cases hit : it.sk = sk
      · simp [hit]
        generalize hA : prefixAdd key (rollupPrefixes it) w = A
        generalize hB : prefixAdd key (rollupPrefixes it) (st it.sk) = B
        generalize hC : P dims key (fun k => if k = sk then w else zero) = C
        generalize hD : P dims key st = D
        cases A; cases B; cases C; cases D
        simp [plus]
        omega
      · simp [hit, prefixAdd_zero]
        generalize hB : prefixAdd key (rollupPrefixes it) (st it.sk) = B
        generalize hC : P dims key (fun k => if k = sk then w else zero) = C
        generalize hD : P dims key st = D
        cases B; cases C; cases D
        simp [plus]
        omega

private theorem P_zero (dims : List ItemDim) (key : RollupKey) :
    P dims key (fun _ => zero) = zero := by
  induction dims with
  | nil => rfl
  | cons it dims ih =>
      simp only [P, List.foldl_cons]
      rw [prefixAdd_zero, plus_zero_right]
      change P dims key (fun _ => zero) = zero
      exact ih

private theorem P_cons_of_zero (dims : List ItemDim) (it : ItemDim) (key : RollupKey)
    (st : Nat → Totals) (h : st it.sk = zero) :
    P (it :: dims) key st = P dims key st := by
  simp only [P, List.foldl_cons]
  rw [h, prefixAdd_zero, plus_zero_left]

private def preStep (facts : List (Nat × Option Int)) (key : RollupKey)
    (ac : Totals) (it : ItemDim) : Totals :=
  let st := (facts.foldl (fun mm (s, v) => mm.insert s (plus (mm.getD s zero) (weighted 1 v)))
    ({} : Std.HashMap Nat Totals)).getD it.sk zero
  if st.cnt == 0 then ac else
    (rollupPrefixes it).foldl (fun a rk => if rk == key then plus a st else a) ac

private theorem preAgg_fold_acc (facts : List (Nat × Option Int)) (dims : List ItemDim)
    (key : RollupKey) (acc : Totals) :
    dims.foldl (preStep facts key) acc = plus acc (P dims key (stOf facts)) := by
  induction dims generalizing acc with
  | nil => simp [P, plus_zero_right]
  | cons it dims ih =>
      simp only [List.foldl_cons, preStep]
      rw [bySk_getD facts it.sk]
      change dims.foldl (preStep facts key)
          (if (stOf facts it.sk).cnt == 0 then acc else
            (rollupPrefixes it).foldl (fun a rk => if rk == key then plus a (stOf facts it.sk) else a) acc) =
        plus acc (P (it :: dims) key (stOf facts))
      by_cases hz : (stOf facts it.sk).cnt == 0
      · rw [if_pos hz]
        have hst : stOf facts it.sk = zero := stOf_cnt_zero facts it.sk hz
        rw [P_cons_of_zero dims it key (stOf facts) hst]
        exact ih (acc := acc)
      · rw [if_neg hz]
        have hfold : (rollupPrefixes it).foldl
              (fun a rk => if rk == key then plus a (stOf facts it.sk) else a) acc =
            plus acc (prefixAdd key (rollupPrefixes it) (stOf facts it.sk)) := by
          simpa using prefixAdd_acc key (rollupPrefixes it) (stOf facts it.sk) acc
        rw [hfold]
        rw [ih (acc := plus acc (prefixAdd key (rollupPrefixes it) (stOf facts it.sk)))]
        rw [show P (it :: dims) key (stOf facts) =
            plus (prefixAdd key (rollupPrefixes it) (stOf facts it.sk))
              (P dims key (stOf facts)) by
              simp only [P, List.foldl_cons]
              rw [show plus zero (prefixAdd key (rollupPrefixes it) (stOf facts it.sk)) =
                  prefixAdd key (rollupPrefixes it) (stOf facts it.sk) by exact plus_zero_left _]
              rw [foldl_add_sum]]
        rw [plus_assoc]

private theorem preAgg_to_P (facts : List (Nat × Option Int)) (dims : List ItemDim)
    (key : RollupKey) :
    preAggRollup facts dims key = P dims key (stOf facts) := by
  unfold preAggRollup
  change dims.foldl (preStep facts key) zero = P dims key (stOf facts)
  simpa [plus_zero_left] using preAgg_fold_acc facts dims key zero

private theorem summarize_flatMap {α : Type} (xs : List α) (g : α → List (Option Int)) :
    summarize (xs.flatMap g) = xs.foldl (fun acc x => plus acc (summarize (g x))) zero := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      rw [summarize_append, ih]
      rw [List.foldl_cons]
      rw [show xs.foldl (fun acc x => plus acc (summarize (g x)))
              (plus zero (summarize (g x))) =
            plus (summarize (g x)) (xs.foldl (fun acc x => plus acc (summarize (g x))) zero) by
              simpa [plus_zero_left] using
                foldl_add_sum xs (fun x => summarize (g x)) (summarize (g x))]

private theorem summarize_filterMap_prefix_eq (ps : List RollupKey) (key : RollupKey) (v : Option Int) :
    summarize (ps.filterMap fun rk => if rk = key then some v else none) =
      prefixAdd key ps (weighted 1 v) := by
  induction ps with
  | nil => rfl
  | cons rk ps ih =>
      by_cases h : rk = key
      · subst rk
        simp only [List.filterMap_cons, if_true]
        rw [summarize_cons]
        rw [prefixAdd_cons]
        simp only [if_true]
        rw [show plus zero (weighted 1 v) = weighted 1 v by exact plus_zero_left (weighted 1 v)]
        have hacc : ps.foldl (fun a rk => if rk = key then plus a (weighted 1 v) else a) (weighted 1 v) =
            plus (weighted 1 v) (prefixAdd key ps (weighted 1 v)) :=
          prefixAdd_acc key ps (weighted 1 v) (weighted 1 v)
        rw [hacc, ih]
      · rw [prefixAdd_cons]
        simp [h]
        rw [show ps.foldl (fun a rk => if rk = key then plus a (weighted 1 v) else a) zero =
            prefixAdd key ps (weighted 1 v) by rfl]
        exact ih

private theorem summarize_filterMap_prefix (ps : List RollupKey) (key : RollupKey) (v : Option Int) :
    summarize (ps.filterMap fun rk => if rk == key then some v else none) =
      prefixAdd key ps (weighted 1 v) := by
  simpa using summarize_filterMap_prefix_eq ps key v

private def contribTot (dims : List ItemDim) (key : RollupKey) (sk : Nat) (w : Totals) : Totals :=
  dims.foldl (fun acc it => plus acc
    (if it.sk = sk then prefixAdd key (rollupPrefixes it) w else zero)) zero

private theorem prefixAdd_if (key : RollupKey) (ps : List RollupKey) (w : Totals)
    (c : Prop) [Decidable c] :
    prefixAdd key ps (if c then w else zero) = if c then prefixAdd key ps w else zero := by
  by_cases h : c <;> simp [h, prefixAdd_zero]

private def contribFact (dims : List ItemDim) (key : RollupKey) (sk : Nat) (v : Option Int) : Totals :=
  contribTot dims key sk (weighted 1 v)

private theorem filter_foldl_acc {α : Type} (xs : List α) (p : α → Bool)
    (f : Totals → α → Totals) (acc : Totals) :
    (xs.filter p).foldl f acc = xs.foldl (fun a x => if p x then f a x else a) acc := by
  induction xs generalizing acc with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons, List.foldl_cons]
      by_cases hp : p x
      · simp [hp]
        exact ih (acc := f acc x)
      · simp [hp]
        exact ih (acc := acc)

private theorem summarize_filtered_dims (dims : List ItemDim) (key : RollupKey)
    (sk : Nat) (v : Option Int) :
    (dims.filter fun it => it.sk == sk).foldl
        (fun acc it => plus acc
          (summarize ((rollupPrefixes it).filterMap fun rk => if rk == key then some v else none))) zero =
      contribFact dims key sk v := by
  have hfun : (fun (acc : Totals) (it : ItemDim) => plus acc
        (summarize ((rollupPrefixes it).filterMap fun rk => if rk == key then some v else none))) =
      (fun (acc : Totals) (it : ItemDim) => plus acc
        (prefixAdd key (rollupPrefixes it) (weighted 1 v))) := by
    funext acc it
    rw [summarize_filterMap_prefix]
  rw [hfun]
  rw [filter_foldl_acc]
  have hfun2 : (fun (a : Totals) (x : ItemDim) =>
        if x.sk == sk then plus a (prefixAdd key (rollupPrefixes x) (weighted 1 v)) else a) =
      (fun (a : Totals) (x : ItemDim) =>
        plus a (if x.sk = sk then prefixAdd key (rollupPrefixes x) (weighted 1 v) else zero)) := by
    funext a x
    by_cases hx : x.sk == sk
    · have heq : x.sk = sk := LawfulBEq.eq_of_beq hx
      simp [hx, heq]
    · have hne : x.sk ≠ sk := by
        intro heq
        apply hx
        simpa [heq]
      simp [hx, hne, plus_zero_right]
  rw [hfun2]
  unfold contribFact contribTot
  rfl

private theorem inner_sum (dims : List ItemDim) (key : RollupKey) (sk : Nat) (v : Option Int) :
    summarize ((dims.filter fun it => it.sk == sk).flatMap fun it =>
      (rollupPrefixes it).filterMap fun rk => if rk == key then some v else none) =
      contribFact dims key sk v := by
  rw [summarize_flatMap]
  exact summarize_filtered_dims dims key sk v

private theorem ref_to_fold (facts : List (Nat × Option Int)) (dims : List ItemDim)
    (key : RollupKey) :
    refRollup facts dims key =
      facts.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero := by
  unfold refRollup
  induction facts with
  | nil => rfl
  | cons f fs ih =>
      simp only [List.flatMap_cons]
      rw [summarize_append, ih, inner_sum]
      change plus (contribFact dims key f.1 f.2)
          (List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero fs) =
        List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero (f :: fs)
      rw [List.foldl_cons]
      rw [show plus zero (contribFact dims key f.1 f.2) = contribFact dims key f.1 f.2 by exact plus_zero_left _]
      rw [show List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2))
              (contribFact dims key f.1 f.2) fs =
            plus (contribFact dims key f.1 f.2)
              (List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero fs) by
              exact foldl_add_sum fs (fun f => contribFact dims key f.1 f.2) (contribFact dims key f.1 f.2)]

private theorem P_contrib (dims : List ItemDim) (key : RollupKey) (sk : Nat) (w : Totals) :
    P dims key (fun k => if k = sk then w else zero) = contribTot dims key sk w := by
  unfold P contribTot
  have hfun : (fun (acc : Totals) (x : ItemDim) =>
        plus acc (prefixAdd key (rollupPrefixes x) ((fun k => if k = sk then w else zero) x.sk))) =
      (fun (acc : Totals) (x : ItemDim) =>
        plus acc (if x.sk = sk then prefixAdd key (rollupPrefixes x) w else zero)) := by
    funext acc x
    rw [show ((fun k => if k = sk then w else zero) x.sk) = (if x.sk = sk then w else zero) by rfl]
    rw [prefixAdd_if]
  rw [hfun]

private theorem P_facts (facts : List (Nat × Option Int)) (dims : List ItemDim)
    (key : RollupKey) :
    P dims key (stOf facts) =
      facts.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero := by
  induction facts with
  | nil =>
      have hs : stOf [] = (fun _ => zero) := by
        funext k
        simp [stOf, skValues, summarize]
      rw [hs]
      exact P_zero dims key
  | cons f fs ih =>
      rcases f with ⟨sk, v⟩
      have hs : stOf ((sk, v) :: fs) =
          (fun k => if k = sk then plus (weighted 1 v) (stOf fs k) else stOf fs k) := by
        funext k
        exact stOf_update fs sk v k
      rw [hs]
      rw [P_update]
      rw [P_contrib]
      rw [ih]
      change plus (contribTot dims key sk (weighted 1 v))
          (List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero fs) =
        List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero ((sk, v) :: fs)
      rw [List.foldl_cons]
      rw [show plus zero (contribFact dims key sk v) = contribFact dims key sk v by exact plus_zero_left _]
      rw [show List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2))
              (contribFact dims key sk v) fs =
            plus (contribFact dims key sk v)
              (List.foldl (fun acc f => plus acc (contribFact dims key f.1 f.2)) zero fs) by
              exact foldl_add_sum fs (fun f => contribFact dims key f.1 f.2) (contribFact dims key sk v)]
      rfl

theorem SurrogateKeyPreAggRollup_sound : SurrogateKeyPreAggRollup.Goal := by
  intro facts dims key hnodup
  rw [preAgg_to_P, P_facts, ref_to_fold]

end TinylambOpt.ResearchProofs.Wave9TpcdsRollupAndSetOps
