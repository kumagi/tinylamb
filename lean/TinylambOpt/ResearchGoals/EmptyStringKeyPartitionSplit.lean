import Std

/-!
Split 3-way partition aggregation: hot `""` and `NULL` partitions aggregate over `Int`
(`uid`) keys alone with zero string hashing/comparison, then merge their Top-K with the
non-empty `(uid, s)` partition.

Modeled claims

- `Goal` — Empty-string partition split + cheap predicate reorder + exact int AVG
  (ClickBench Q17/Q22/Q31/Q32): (1) partitioning `GROUP BY (uid, phrase)` into
  `phrase = ""`, `phrase = NULL`, and `phrase ≠ ""` avoids string hashing on the hot
  empty partition and agrees with unified Top-K, (2) checking `phrase ≠ ""` before
  expensive `URL LIKE` preserves total boolean filtering while bounding substring
  checks to `phrase ≠ ""` rows, and (3) integer `(sum, cnt)` accumulation preserves
  `AVG(CAST(int AS FLOAT))`.
-/

namespace TinylambOpt.ResearchGoals.EmptyStringKeyPartitionSplit

structure HitRow where
  uid : Int
  phrase : Option String
  url : String
  val : Option Int
  deriving DecidableEq

inductive PhraseKey where
  | emptyKey (uid : Int)
  | nonEmptyKey (uid : Int) (s : String)
  | nullKey (uid : Int)
  deriving DecidableEq

def classifyKey (uid : Int) : Option String → PhraseKey
  | none => .nullKey uid
  | some s => if s == "" then .emptyKey uid else .nonEmptyKey uid s

def addCount (m : List (PhraseKey × Nat)) (k : PhraseKey) :
    List (PhraseKey × Nat) :=
  match m with
  | [] => [(k, 1)]
  | (k', c) :: rest =>
      if k' == k then (k', c + 1) :: rest
      else (k', c) :: addCount rest k

def keyTag : PhraseKey → Nat
  | .emptyKey _ => 0
  | .nonEmptyKey _ _ => 1
  | .nullKey _ => 2

def leEntry (a b : PhraseKey × Nat) : Bool :=
  if a.2 != b.2 then decide (b.2 ≤ a.2)
  else if keyTag a.1 != keyTag b.1 then decide (keyTag a.1 ≤ keyTag b.1)
  else match a.1, b.1 with
       | .emptyKey u1, .emptyKey u2 => decide (u1 ≤ u2)
       | .nullKey u1, .nullKey u2 => decide (u1 ≤ u2)
       | .nonEmptyKey u1 s1, .nonEmptyKey u2 s2 =>
           if u1 != u2 then decide (u1 ≤ u2) else decide (s1 ≤ s2)
       | _, _ => true

def insertSortedE (x : PhraseKey × Nat) :
    List (PhraseKey × Nat) → List (PhraseKey × Nat)
  | [] => [x]
  | y :: ys => if leEntry x y then x :: y :: ys else y :: insertSortedE x ys

def sortEntries : List (PhraseKey × Nat) → List (PhraseKey × Nat)
  | [] => []
  | x :: xs => insertSortedE x (sortEntries xs)

def refGroupTopK (k : Nat) (rows : List HitRow) : List (PhraseKey × Nat) :=
  let m := rows.foldl (fun acc r => addCount acc (classifyKey r.uid r.phrase)) []
  (sortEntries m).take k

def addIntCount (m : List (Int × Nat)) (u : Int) : List (Int × Nat) :=
  match m with
  | [] => [(u, 1)]
  | (u', c) :: rest => if u' == u then (u', c + 1) :: rest else (u', c) :: addIntCount rest u

/-- Split 3-way partition aggregation: hot `""` and `NULL` partitions aggregate
    over `Int` (`uid`) keys alone with zero string hashing/comparison, then merge
    their Top-K with the non-empty `(uid, s)` partition. -/
def optSplitTopK (k : Nat) (rows : List HitRow) : List (PhraseKey × Nat) :=
  let mEmpty :=
    (rows.filter (fun r => r.phrase == some "")).foldl (fun acc r => addIntCount acc r.uid) []
  let mNull :=
    (rows.filter (fun r => r.phrase.isNone)).foldl (fun acc r => addIntCount acc r.uid) []
  let mStr :=
    (rows.filter (fun r => match r.phrase with | some s => s != "" | none => false)).foldl
      (fun acc r => addCount acc (classifyKey r.uid r.phrase)) []
  let all :=
    mEmpty.map (fun (u, c) => (PhraseKey.emptyKey u, c)) ++
    mNull.map (fun (u, c) => (PhraseKey.nullKey u, c)) ++
    mStr
  (sortEntries all).take k

def isNonEmptyPhrase : Option String → Bool
  | some s => s != ""
  | none => false

def evalPredNaive (containsUrl : String → Bool) (r : HitRow) : Bool :=
  containsUrl r.url && isNonEmptyPhrase r.phrase

def evalPredReordered (containsUrl : String → Bool) (r : HitRow) : Bool :=
  isNonEmptyPhrase r.phrase && containsUrl r.url

def avgStateOfOpts (xs : List (Option Int)) : Int × Nat :=
  xs.foldl
    (fun (s, c) ov => match ov with | none => (s, c) | some v => (s + v, c + 1))
    (0, 0)

/-- Empty-string partition split + cheap predicate reorder + exact int AVG
    (ClickBench Q17/Q22/Q31/Q32):
    (1) partitioning `GROUP BY (uid, phrase)` into `phrase = ""`, `phrase = NULL`,
    and `phrase ≠ ""` avoids string hashing on the hot empty partition and
    agrees with unified Top-K,
    (2) checking `phrase ≠ ""` before expensive `URL LIKE` preserves total
    boolean filtering while bounding substring checks to `phrase ≠ ""` rows, and
    (3) integer `(sum, cnt)` accumulation preserves `AVG(CAST(int AS FLOAT))`. -/
def Goal : Prop :=
  (∀ (k : Nat) (rows : List HitRow),
    optSplitTopK k rows = refGroupTopK k rows) ∧
  (∀ (containsUrl : String → Bool) (r : HitRow),
    evalPredReordered containsUrl r = evalPredNaive containsUrl r) ∧
  (∀ (xs ys : List (Option Int)),
    let (s1, c1) := avgStateOfOpts xs
    let (s2, c2) := avgStateOfOpts ys
    avgStateOfOpts (xs ++ ys) = (s1 + s2, c1 + c2))

example :
    let rows : List HitRow :=
      [ { uid := 1, phrase := some "", url := "a", val := some 10 }
      , { uid := 1, phrase := some "", url := "b", val := none }
      , { uid := 2, phrase := some "hi", url := "c", val := some 20 }
      , { uid := 1, phrase := none, url := "d", val := some 30 }
      , { uid := 1, phrase := some "", url := "e", val := some 40 }
      ]
    optSplitTopK 2 rows = refGroupTopK 2 rows ∧
    optSplitTopK 2 rows = [(.emptyKey 1, 3), (.nonEmptyKey 2 "hi", 1)] := by
  decide

example :
    avgStateOfOpts [none, some (-5), some 15] = (10, 2) ∧
    (let p := fun u : String => u == "hit"
     evalPredReordered p { uid := 1, phrase := some "", url := "hit", val := none } = false) := by
  decide

end TinylambOpt.ResearchGoals.EmptyStringKeyPartitionSplit
