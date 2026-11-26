import Std

/-!
Left-deep chain evaluation `((A ⋈_{k1} B) ⋈_{k2} C) ⋈_{k3} D` retaining all intermediate
columns.

Bushy join plan with immediate dead-key projection pruning: `AB := π_{outA, k2}(A ⋈_{k1}
B)` and `CD := π_{k2, outD}(C ⋈_{k3} D)`, dropping `k1` and `k3` before computing `AB
⋈_{k2} CD`.

Modeled claims

- `Goal` — Bushy join associativity with dead-key column pruning on 4-table chain joins
  (LDBC L1 / STATS-CEB C2 / JOB J4 / DictionaryJoinsAndLateLabels B3): reassociating
  `((A ⋈ B) ⋈ C) ⋈ D` into `(A ⋈ B) ⋈ (C ⋈ D)` while immediately dropping dead join
  keys (`k1`, `k3`) from intermediate schemas preserves exact multiset equality
  across duplicates and NULL join keys.
-/

namespace TinylambOpt.ResearchGoals.BushyJoinAssociativityWithDeadKeyPrune

structure ARow where
  outA : Int
  k1 : Option Int
  deriving DecidableEq

structure BRow where
  k1 : Option Int
  k2 : Option Int
  deriving DecidableEq

structure CRow where
  k2 : Option Int
  k3 : Option Int
  deriving DecidableEq

structure DRow where
  k3 : Option Int
  outD : Int
  deriving DecidableEq

/-- Left-deep chain evaluation `((A ⋈_{k1} B) ⋈_{k2} C) ⋈_{k3} D` retaining
    all intermediate columns. -/
def refLeftDeep (aRows : List ARow) (bRows : List BRow)
    (cRows : List CRow) (dRows : List DRow) : List (Int × Int) :=
  let ab :=
    aRows.flatMap fun a =>
      bRows.filterMap fun b =>
        match a.k1, b.k1 with
        | some ak1, some bk1 => if ak1 == bk1 then some (a.outA, b.k2) else none
        | _, _ => none
  let abc :=
    ab.flatMap fun (outA, bk2) =>
      cRows.filterMap fun c =>
        match bk2, c.k2 with
        | some k2b, some k2c => if k2b == k2c then some (outA, c.k3) else none
        | _, _ => none
  abc.flatMap fun (outA, ck3) =>
    dRows.filterMap fun d =>
      match ck3, d.k3 with
      | some k3c, some k3d => if k3c == k3d then some (outA, d.outD) else none
      | _, _ => none

/-- Bushy join plan with immediate dead-key projection pruning:
    `AB := π_{outA, k2}(A ⋈_{k1} B)` and `CD := π_{k2, outD}(C ⋈_{k3} D)`,
    dropping `k1` and `k3` before computing `AB ⋈_{k2} CD`. -/
def optBushyPruned (aRows : List ARow) (bRows : List BRow)
    (cRows : List CRow) (dRows : List DRow) : List (Int × Int) :=
  let ab : List (Int × Option Int) :=
    aRows.flatMap fun a =>
      bRows.filterMap fun b =>
        match a.k1, b.k1 with
        | some ak1, some bk1 => if ak1 == bk1 then some (a.outA, b.k2) else none
        | _, _ => none
  let cd : List (Option Int × Int) :=
    cRows.flatMap fun c =>
      dRows.filterMap fun d =>
        match c.k3, d.k3 with
        | some k3c, some k3d => if k3c == k3d then some (c.k2, d.outD) else none
        | _, _ => none
  ab.flatMap fun (outA, k2ab) =>
    cd.filterMap fun (k2cd, outD) =>
      match k2ab, k2cd with
      | some l2, some r2 => if l2 == r2 then some (outA, outD) else none
      | _, _ => none

/-- Bushy join associativity with dead-key column pruning on 4-table chain joins
    (LDBC L1 / STATS-CEB C2 / JOB J4 / DictionaryJoinsAndLateLabels B3): reassociating
    `((A ⋈ B) ⋈ C) ⋈ D` into `(A ⋈ B) ⋈ (C ⋈ D)` while immediately dropping dead
    join keys (`k1`, `k3`) from intermediate schemas preserves exact multiset
    equality across duplicates and NULL join keys. -/
def Goal : Prop :=
  ∀ (aRows : List ARow) (bRows : List BRow) (cRows : List CRow)
    (dRows : List DRow) (p : Int × Int),
    (optBushyPruned aRows bRows cRows dRows).count p =
      (refLeftDeep aRows bRows cRows dRows).count p

example :
    let aRows : List ARow :=
      [{ outA := 1, k1 := some 10 }, { outA := 1, k1 := some 10 }, { outA := 2, k1 := none }]
    let bRows : List BRow :=
      [{ k1 := some 10, k2 := some 20 }, { k1 := some 10, k2 := some 21 }, { k1 := some 10, k2 := none }]
    let cRows : List CRow :=
      [{ k2 := some 20, k3 := some 30 }, { k2 := some 20, k3 := some 30 }, { k2 := none, k3 := some 30 }]
    let dRows : List DRow :=
      [{ k3 := some 30, outD := 99 }, { k3 := some 30, outD := 100 }, { k3 := none, outD := 99 }]
    (optBushyPruned aRows bRows cRows dRows).count (1, 99) =
      (refLeftDeep aRows bRows cRows dRows).count (1, 99) ∧
    (optBushyPruned aRows bRows cRows dRows).count (1, 99) = 4 ∧
    (optBushyPruned aRows bRows cRows dRows).count (2, 99) = 0 := by
  decide

example :
    let aRows : List ARow := [{ outA := 5, k1 := some 1 }]
    let bRows : List BRow := [{ k1 := some 1, k2 := some 1 }]
    let cRows : List CRow := [{ k2 := some 1, k3 := some 1 }]
    let dRows : List DRow := []
    optBushyPruned aRows bRows cRows dRows = [] ∧
    refLeftDeep aRows bRows cRows dRows = [] := by
  decide

end TinylambOpt.ResearchGoals.BushyJoinAssociativityWithDeadKeyPrune
