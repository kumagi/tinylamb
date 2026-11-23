import TinylambOpt.HighRiskRulesAudit

/-!
# Left-side pre-filtering for FETCH WITH TIES (`executor/topn.cpp`)

The reference plan joins the whole left input and then applies
`FETCH FIRST K ROWS WITH TIES` ordered by a left-side key.  The optimized
plan keeps, *before* the join, every left row tied with the K-th left row and
then applies the very same FETCH WITH TIES to the joined stream.

The key observation is a counting lower bound: because every left row has at
least one joined output, the first `K` joined outputs all come from the first
`K` left rows.  Hence the join frontier is never larger than the left
frontier, and every joined row that can pass the final tie filter comes from
a left row that the pre-filter kept.  A naive `take K` of the left side is
not sound: a boundary-key row with several matches is dropped.

C++ checks: `executor/topn.cpp` keeps the whole equal-key run when
`with_ties_` is set (`TopN::Consume` / the `with_ties_` branch); the caller
supplies no OFFSET, an ordered left stream, the same left-only comparator on
both sides, and no intervening filter. SQL NULL ordering for a signed INT64
column is represented by ranks outside its value range. Adding a unique ID
to the tie test changes the comparator.
-/

namespace TinylambOpt.OuterFetchTies

/-- The joined bag (in left order): each left row emits its matching rows. -/
def join {L R : Type} (on : L → R → Bool) (Ls : List L) (Rs : List R) : List (L × R) :=
  Ls.flatMap (fun l => (Rs.filter (fun r => on l r)).map (fun r => (l, r)))

/-- `FETCH FIRST K ROWS WITH TIES` on an already key-ordered stream: when
    fewer than `K` rows exist nothing is cut; otherwise keep every row tied
    with the key of the last row of the first `K`. -/
def fetchTies {L R : Type} (key : L → Int) (K : Nat) (J : List (L × R)) : List (L × R) :=
  if J.length < K then J else
    match (J.take K).getLast? with
    | none => []
    | some (l0, _) => J.filter (fun (l, _) => key l ≤ key l0)

/-- The analogous left-side frontier and pre-filter. -/
def leftTies {L : Type} (key : L → Int) (K : Nat) (Ls : List L) : List L :=
  if Ls.length < K then Ls else
    match (Ls.take K).getLast? with
    | none => []
    | some l0 => Ls.takeWhile (fun l => key l ≤ key l0)

private theorem mem_of_getLast?_eq_some {α : Type} :
    ∀ (L : List α) (x : α), L.getLast? = some x → x ∈ L
  | [], _, h => by simp at h
  | [a], x, h => by
      have hx : x = a := by simpa using h.symm
      simp [hx]
  | _ :: b :: bs, x, h => by
      have h' : (b :: bs).getLast? = some x := by
        simpa [List.getLast?_cons_cons] using h
      exact List.mem_cons_of_mem _ (mem_of_getLast?_eq_some (b :: bs) x h')

private theorem take_getLast_some {α : Type} (L : List α) (K : Nat)
    (hK : 0 < K) (hKL : K ≤ L.length) : ∃ x, (L.take K).getLast? = some x := by
  cases hgl : (L.take K).getLast? with
  | none =>
      have hnil : L.take K = [] := List.getLast?_eq_none_iff.mp hgl
      have hlen0 : (L.take K).length = 0 := by simp [hnil]
      rw [List.length_take] at hlen0
      omega
  | some x => exact ⟨x, rfl⟩

private theorem filter_false_nil {α : Type} (p : α → Bool) :
    ∀ (L : List α), (∀ x ∈ L, p x = false) → L.filter p = [] := by
  intro L h
  induction L with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ y ∈ xs, p y = false := fun y hy => h y (by simp [hy])
      simp [hx, ih ht]

private theorem join_append {L R : Type} (on : L → R → Bool)
    (A B : List L) (Rs : List R) :
    join on (A ++ B) Rs = join on A Rs ++ join on B Rs := by
  simp [join]

private theorem join_length_ge {L R : Type} (on : L → R → Bool) (Rs : List R) :
    ∀ (Ls : List L), (∀ l ∈ Ls, ∃ r ∈ Rs, on l r = true) →
      Ls.length ≤ (join on Ls Rs).length := by
  intro Ls hlive
  induction Ls with
  | nil => simp [join]
  | cons l ls ih =>
      have htail : ∀ x ∈ ls, ∃ r ∈ Rs, on x r = true := fun x hx => hlive x (by simp [hx])
      have ih' := ih htail
      obtain ⟨r, hr, hon⟩ := hlive l (by simp)
      have hmem : r ∈ Rs.filter (fun r' => on l r') := by simp [hr, hon]
      have hpos : 0 < (Rs.filter (fun r' => on l r')).length := List.length_pos_of_mem hmem
      simp only [join, List.flatMap_cons, List.length_append, List.length_map,
        List.length_cons] at ih' ⊢
      omega

/-- The first `K` joined outputs lie inside the first `K` left rows' blocks. -/
private theorem join_take {L R : Type} (on : L → R → Bool) (Rs : List R) :
    ∀ (Ls : List L) (K : Nat), (∀ l ∈ Ls, ∃ r ∈ Rs, on l r = true) →
      (join on Ls Rs).take K = (join on (Ls.take K) Rs).take K := by
  intro Ls K hlive
  by_cases hKL : K ≤ Ls.length
  · have hdecomp : join on Ls Rs = join on (Ls.take K) Rs ++ join on (Ls.drop K) Rs := by
      calc
        join on Ls Rs = join on (Ls.take K ++ Ls.drop K) Rs := by rw [List.take_append_drop]
        _ = join on (Ls.take K) Rs ++ join on (Ls.drop K) Rs := join_append on _ _ _
    have hlenA : K ≤ (join on (Ls.take K) Rs).length := by
      have hlentake : (Ls.take K).length = K := by rw [List.length_take, Nat.min_eq_left hKL]
      have hge := join_length_ge on Rs (Ls.take K) (fun l hl => hlive l (List.mem_of_mem_take hl))
      omega
    rw [hdecomp, List.take_append]
    have hzero : K - (join on (Ls.take K) Rs).length = 0 := by omega
    simp [hzero]
  · have htake : Ls.take K = Ls := List.take_of_length_le (by omega)
    rw [htake]

private theorem pairwise_le_lastKey {α : Type} (key : α → Int) :
    ∀ (L : List α), L.Pairwise (fun a b => key a ≤ key b) →
      ∀ x ∈ L, key x ≤ key (L.getLast?.getD x) := by
  intro L hp
  induction L with
  | nil => intro x hx; simp at hx
  | cons a as ih =>
      rw [List.pairwise_cons] at hp
      rcases hp with ⟨ha, htail⟩
      cases as with
      | nil =>
          intro x hx
          simp at hx
          subst x
          simp
      | cons b bs =>
          intro x hx
          have hlas : (a :: b :: bs).getLast? = (b :: bs).getLast? := by simp
          rcases List.mem_cons.mp hx with hxa | hxas
          · subst x
            have hne : (b :: bs).getLast? ≠ none := by
              intro hnone
              have hnil : b :: bs = [] := List.getLast?_eq_none_iff.mp hnone
              simp at hnil
            have hlast : (b :: bs).getLast?.getD a = (b :: bs).getLast?.getD b := by
              cases hgl : (b :: bs).getLast? with
              | none => exact False.elim (hne hgl)
              | some y => simp
            rw [hlas, hlast]
            have ha_b : key a ≤ key b := ha b (by simp)
            have hb := ih htail b (by simp)
            omega
          · simpa [hlas] using ih htail x hxas

private theorem length_filter_ge_take {α : Type} (q : α → Bool) :
    ∀ (L : List α) (K : Nat), K ≤ L.length →
      (∀ x ∈ L.take K, q x = true) → K ≤ (L.filter q).length := by
  intro L K
  revert L
  induction K with
  | zero =>
      intro L hlen hq
      simp
  | succ k ih =>
      intro L hlen hq
      cases L with
      | nil => simp at hlen
      | cons a as =>
          have hkas : k ≤ as.length := by simpa using hlen
          have hqa : q a = true := hq a (by simp)
          have hqas : ∀ x ∈ as.take k, q x = true := by
            intro x hx
            exact hq x (by simp [hx])
          have ih' : k ≤ (as.filter q).length := ih as hkas hqas
          simp [hqa]
          omega

private theorem takeWhile_eq_filter_of_sorted {α : Type} (key : α → Int) :
    ∀ (L : List α), L.Pairwise (fun a b => key a ≤ key b) →
      ∀ b : Int, L.takeWhile (fun x => key x ≤ b) = L.filter (fun x => key x ≤ b) := by
  intro L hp
  induction L with
  | nil => intro b; rfl
  | cons a as ih =>
      rw [List.pairwise_cons] at hp
      rcases hp with ⟨ha, htail⟩
      intro b
      by_cases hab : key a ≤ b
      · have hdec : decide (key a ≤ b) = true := decide_eq_true hab
        simp [hdec, ih htail b]
      · have hdec : decide (key a ≤ b) = false := decide_eq_false hab
        have hnone : (as.filter (fun x => key x ≤ b)) = [] := by
          apply filter_false_nil
          intro x hx
          apply decide_eq_false
          intro hxb
          have hax : key a ≤ key x := ha x hx
          omega
        simp [hdec, hnone]

private theorem mem_takeWhile_of_sorted {α : Type} (key : α → Int) :
    ∀ (L : List α), L.Pairwise (fun a b => key a ≤ key b) →
      ∀ (b : Int) (x : α), x ∈ L → key x ≤ b →
        x ∈ L.takeWhile (fun y => key y ≤ b) := by
  intro L hp b x hx hkx
  rw [takeWhile_eq_filter_of_sorted key L hp b]
  simp [hx, hkx]

private theorem dropWhile_all_gt {α : Type} (key : α → Int) :
    ∀ (L : List α), L.Pairwise (fun a b => key a ≤ key b) →
      ∀ (b : Int) (x : α), x ∈ L.dropWhile (fun y => key y ≤ b) → b < key x := by
  intro L hp
  induction L with
  | nil => intro b x hx; simp at hx
  | cons a as ih =>
      rw [List.pairwise_cons] at hp
      rcases hp with ⟨ha, htail⟩
      intro b x hx
      rw [List.dropWhile_cons] at hx
      by_cases hab : key a ≤ b
      · have hdec : decide (key a ≤ b) = true := decide_eq_true hab
        simp [hdec] at hx
        exact ih htail b x hx
      · have hdec : decide (key a ≤ b) = false := decide_eq_false hab
        simp [hdec] at hx
        rcases hx with hxa | hxas
        · subst x
          omega
        · have hax : key a ≤ key x := ha x hxas
          omega

/-- ✅ **`outer_fetch_ties_sound`**: with `K > 0`, no OFFSET, a single
    non-NULL left sort key using the same order on both sides, every left row
    producing at least one joined row, and no post-FETCH filter, the
    left-side tie-preserving pre-filter commutes with the join and the final
    FETCH WITH TIES.  (A plain `take K` of the left side would be wrong.) -/
theorem outer_fetch_ties_sound {L R : Type} (on : L → R → Bool) (key : L → Int)
    (K : Nat) (Ls : List L) (Rs : List R)
    (hK : 0 < K)
    (hsorted : Ls.Pairwise (fun a b => key a ≤ key b))
    (hlive : ∀ l ∈ Ls, ∃ r ∈ Rs, on l r = true) :
    fetchTies key K (join on (leftTies key K Ls) Rs) =
      fetchTies key K (join on Ls Rs) := by
  by_cases hKL : K ≤ Ls.length
  · obtain ⟨l0, hgl⟩ := take_getLast_some Ls K hK hKL
    let bL := key l0
    let P := Ls.takeWhile (fun l => key l ≤ bL)
    let T := Ls.dropWhile (fun l => key l ≤ bL)
    have hnotLen : ¬ Ls.length < K := by omega
    have hleft : leftTies key K Ls = P := by
      unfold leftTies
      simp [hnotLen, hgl, bL, P]
    have hdecompL : Ls = P ++ T := by
      dsimp [P, T]
      exact (List.takeWhile_append_dropWhile (p := fun l => key l ≤ bL) (l := Ls)).symm
    have hJdecomp : join on Ls Rs = join on P Rs ++ join on T Rs := by
      rw [hdecompL, join_append]
    let J := join on Ls Rs
    let Jp := join on P Rs
    have hP_eq : P = Ls.filter (fun l => key l ≤ bL) := by
      dsimp [P]
      exact takeWhile_eq_filter_of_sorted key Ls hsorted bL
    have hPsize : K ≤ P.length := by
      rw [hP_eq]
      apply length_filter_ge_take (fun l => key l ≤ bL) Ls K hKL
      intro l hl
      have hlast := pairwise_le_lastKey key (Ls.take K) hsorted.take l hl
      have hlast_eq : (Ls.take K).getLast?.getD l = l0 := by simp [hgl]
      have : key l ≤ bL := by simpa [hlast_eq, bL] using hlast
      exact decide_eq_true this
    have hPsub : ∀ l ∈ P, l ∈ Ls := by
      intro l hl
      have hl' : l ∈ Ls ∧ key l ≤ bL := by simpa [hP_eq] using hl
      exact hl'.1
    have hJpLen : K ≤ Jp.length := by
      dsimp [Jp]
      have hge := join_length_ge on Rs P (fun l hl => hlive l (hPsub l hl))
      omega
    have hJLen : K ≤ J.length := by
      dsimp [J]
      have hge := join_length_ge on Rs Ls hlive
      omega
    have hJtake : J.take K = Jp.take K := by
      dsimp [J]
      rw [hJdecomp, List.take_append]
      have hzero : K - Jp.length = 0 := by omega
      simp [hzero, Jp]
    obtain ⟨j0, hgJ⟩ := take_getLast_some J K hK hJLen
    obtain ⟨jp0, hgJp⟩ := take_getLast_some Jp K hK hJpLen
    have hlast_eq : j0 = jp0 := by
      have heq : (J.take K).getLast? = (Jp.take K).getLast? := by rw [hJtake]
      simpa [hgJ, hgJp] using heq
    have hj0mem : j0 ∈ Jp := by
      have hmemtake : j0 ∈ J.take K := mem_of_getLast?_eq_some (J.take K) j0 hgJ
      rw [hJtake] at hmemtake
      exact List.mem_of_mem_take hmemtake
    have hb_le_bL : key j0.1 ≤ bL := by
      rcases List.mem_flatMap.mp hj0mem with ⟨l, hlP, hblk⟩
      rcases List.mem_map.mp hblk with ⟨r, hr, hrfl⟩
      have hlfilter : l ∈ Ls.filter (fun z => key z ≤ bL) := by simpa [hP_eq] using hlP
      have hkle : key l ≤ bL := of_decide_eq_true (List.mem_filter.mp hlfilter).2
      rw [← hrfl]
      exact hkle
    have hJnot : ¬ J.length < K := by omega
    have hJpnot : ¬ Jp.length < K := by omega
    have hTfilter : (join on T Rs).filter (fun (l, _) => key l ≤ key jp0.1) = [] := by
      apply filter_false_nil
      intro x hx
      rcases x with ⟨l, r⟩
      rcases List.mem_flatMap.mp hx with ⟨l', hl'T, hblk⟩
      rcases List.mem_map.mp hblk with ⟨r', hr', hpair⟩
      injection hpair with hl_eq hr_eq
      subst l'
      apply decide_eq_false
      intro hkle
      have hgt : bL < key l := by
        exact dropWhile_all_gt key Ls hsorted bL l (by simpa [T] using hl'T)
      have hj : key jp0.1 = key j0.1 := by rw [hlast_eq]
      omega
    rw [hleft]
    change fetchTies key K Jp = fetchTies key K J
    simp [fetchTies, hJnot, hJpnot, hgJ, hgJp, hlast_eq]
    dsimp [J]
    rw [hJdecomp, List.filter_append, hTfilter, List.append_nil]
  · have hlt : Ls.length < K := by omega
    have hleft : leftTies key K Ls = Ls := by
      unfold leftTies
      simp [hlt]
    rw [hleft]

/-- Represent the NULL-padded partner by one extra candidate. Its predicate
    succeeds exactly when no real candidate makes SQL ON true. -/
private def paddedOn (on : L → R → SqlBool) (rs : List R) (l : L) : Option R → Bool
  | none => (rs.filter fun r => (on l r).inWhere).isEmpty
  | some r => (on l r).inWhere

private theorem join_padded (on : L → R → SqlBool) (ls : List L) (rs : List R) :
    join (paddedOn on rs) ls (none :: rs.map some) = leftOuterJoin on ls rs := by
  unfold join leftOuterJoin
  apply congrArg (fun f => ls.flatMap f)
  funext l
  simp only [List.filter_cons, List.filter_map, Function.comp_def, paddedOn]
  cases h : rs.filter (fun r => (on l r).inWhere) <;> simp [Function.comp_def]

private theorem paddedOn_live (on : L → R → SqlBool) (rs : List R) (l : L) :
    ∃ r ∈ none :: rs.map some, paddedOn on rs l r = true := by
  cases h : rs.filter (fun r => (on l r).inWhere) with
  | nil => exact ⟨none, by simp, by simp [paddedOn, h]⟩
  | cons r rest =>
    have hr : r ∈ rs.filter (fun r => (on l r).inWhere) := h ▸ List.mem_cons_self ..
    obtain ⟨hr, hp⟩ := List.mem_filter.mp hr
    exact ⟨some r, by simp [hr], hp⟩

/-- Order encoding for one nullable signed INT64 column, using mathematical
    integers for sentinels so neither can collide with a real key. -/
def nullableRank (nullsFirst : Bool) : Option Int64 → Int
  | none => if nullsFirst then -(2^63)-1 else 2^63
  | some x => x.toInt

theorem null_rank_extreme (x : Int64) :
    nullableRank true none < x.toInt ∧ x.toInt < nullableRank false none := by
  have lo := x.le_toInt
  have hi := x.toInt_lt
  simp [nullableRank]
  omega

theorem sql_outer_fetch_ties_sound (on : L → R → SqlBool) (key : L → Int)
    (K : Nat) (ls : List L) (rs : List R)
    (hsorted : ls.Pairwise (fun a b => key a ≤ key b)) :
    fetchTies key K (leftOuterJoin on (leftTies key K ls) rs) =
      fetchTies key K (leftOuterJoin on ls rs) := by
  by_cases hK : 0 < K
  · simpa only [join_padded] using outer_fetch_ties_sound (paddedOn on rs) key K ls
      (none :: rs.map some) hK hsorted (fun l _ => paddedOn_live on rs l)
  · have : K = 0 := by omega
    simp [this, fetchTies]

theorem unmatched_null_ties_regression :
    let ls : List (Option Int64) := [none, none, some 2]
    let key := nullableRank true
    fetchTies key 1 (leftOuterJoin (fun _ (_ : Nat) => .tNull) (leftTies key 1 ls) [0]) =
      [(none, none), (none, none)] := by decide

/-- The boundary contains both a matched row (twice) and an unmatched row.
    The last left row lies beyond the NULLS LAST frontier. -/
theorem mixed_boundary_regression :
    let ls : List (Nat × Option Int64) := [(0,some 1),(1,some 1),(2,none)]
    let onRow := fun (l : Nat × Option Int64) (r : Nat) =>
      if l.1 = r then SqlBool.tTrue else SqlBool.tFalse
    let key := fun (l : Nat × Option Int64) => nullableRank false l.2
    fetchTies key 1 (leftOuterJoin onRow (leftTies key 1 ls) [0,0]) =
      [((0,some 1),some 0), ((0,some 1),some 0), ((1,some 1),none)] := by decide

end TinylambOpt.OuterFetchTies
