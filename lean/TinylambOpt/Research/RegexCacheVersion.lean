import Std.Tactic

/-! Versioned dispatch and caching of verified pattern code
    (C++ counterpart: `common/regex_guard.hpp`).

    A small but genuine regular-expression engine is modelled: patterns are
    parsed into a grammar (`Regex`), backtracking matching produces captures
    in priority order (leftmost alternative first), and an unsupported pattern
    falls back to literal-substring search.  A zero-width match advances by one
    position before the next search (`findAll`).

    Two backends share the same search but differ in what they preserve:

      * `existence` -- Thompson-style existence search: only the start
        position is kept; capture groups are *not* available;
      * `capturing` -- priority-ordered capturing search: unmatched groups are
        represented explicitly by `Elem.unmatched` when a replacement is
        rendered.

    The cache key contains the pattern syntax, flags, collation and
    `needsCapture`.  `requestKey` derives the key from the call mode, so a
    replacement request always asks for captures.  Results for any sequence of
    hits and misses equal the reference API.  Sharing the existence code with
    a replacement turns unmatched groups into empty strings; that regression is
    proved as a counterexample, as is one for keys that omit flags/collation.
-/

namespace TinylambOpt.Research.RegexCacheVersion

/-- The supported regex grammar.  `bad` marks patterns handled by fallback. -/
inductive Regex where
  | eps
  | chr (c : Nat)
  | seq (r s : Regex)
  | alt (r s : Regex)
  | grp (i : Nat) (r : Regex)
  | bad
  deriving DecidableEq, Repr

structure Flags where
  ignoreCase : Bool := false
  deriving DecidableEq, Repr

inductive Collation where | binary | nocase
  deriving DecidableEq, Repr

structure Key where
  pattern : List Nat
  flags : Flags
  collation : Collation
  needsCapture : Bool
  deriving DecidableEq, Repr

/-- Pattern markers used by the tiny parser. -/
def mLit : Nat := 0
def mAlt : Nat := 1
def mGrp : Nat := 2

/-- The parser from the supported pattern string to generated code's grammar. -/
def parse : List Nat → Regex
  | [] => Regex.eps
  | [0, c] => Regex.chr c
  | [0, a, 0, b] => Regex.seq (Regex.chr a) (Regex.chr b)
  | [0, 2, i, c] => Regex.grp i (Regex.chr c)
  | [1, a, b] => Regex.alt (Regex.chr a) (Regex.chr b)
  | [1, 2, a, b] => Regex.alt (Regex.grp 1 (Regex.chr a)) (Regex.chr b)
  | _ => Regex.bad

def isBad : Regex → Bool
  | Regex.bad => true
  | _ => false

def groupIds : Regex → List Nat
  | Regex.eps | Regex.chr _ | Regex.bad => []
  | Regex.seq a b => groupIds a ++ groupIds b
  | Regex.alt a b => groupIds a ++ groupIds b
  | Regex.grp i a => i :: groupIds a

/-- Flag/collation-normalised character comparison. -/
def norm (f : Flags) (co : Collation) (c : Nat) : Nat :=
  let x := if f.ignoreCase ∧ 65 ≤ c ∧ c ≤ 90 then c + 32 else c
  match co with
  | Collation.binary => x
  | Collation.nocase => x % 128

def charEq (f : Flags) (co : Collation) (a b : Nat) : Bool :=
  norm f co a == norm f co b

/-- Capture map: group id -> (start, end).  Later writes win. -/
abbrev Caps := List (Nat × (Nat × Nat))

def setCap (i s e : Nat) (caps : Caps) : Caps :=
  (i, (s, e)) :: caps.filter (fun p => p.1 != i)

def getCap (i : Nat) (caps : Caps) : Option (Nat × Nat) :=
  (caps.find? (fun p => p.1 = i)).map (fun p => p.2)

/-- Backtracking matcher: the list of (end, captures) pairs is in priority
    order.  Alternatives are tried left-to-right; a capture map is attached
    only when its group actually participated. -/
def matchHere (r : Regex) (f : Flags) (co : Collation)
    (text : List Nat) (pos : Nat) (caps : Caps) : List (Nat × Caps) :=
  match r with
  | Regex.eps => [(pos, caps)]
  | Regex.chr c =>
      if pos < text.length then
        if charEq f co c (text.getD pos 0) then [(pos + 1, caps)] else []
      else []
  | Regex.seq a b =>
      List.flatMap (fun p => matchHere b f co text p.1 p.2)
        (matchHere a f co text pos caps)
  | Regex.alt a b =>
      matchHere a f co text pos caps ++ matchHere b f co text pos caps
  | Regex.grp i a =>
      (matchHere a f co text pos caps).map (fun p => (p.1, setCap i pos p.1 p.2))
  | Regex.bad => []

structure Match where
  start : Nat
  finish : Nat
  caps : Caps

/-- Scan start positions in increasing order.  After a zero-width match the
    next search starts at `pos + 1`, so empty matches make progress.  `fuel`
    is an explicit bound making the search structurally recursive. -/
def findAllFuel (r : Regex) (f : Flags) (co : Collation) (text : List Nat)
    (fuel pos : Nat) : List Match :=
  match fuel with
  | 0 => []
  | n + 1 =>
      if pos > text.length then []
      else
        match (matchHere r f co text pos []).head? with
        | none => findAllFuel r f co text n (pos + 1)
        | some p =>
            let next := if p.1 = pos then pos + 1 else p.1
            { start := pos, finish := p.1, caps := p.2 } ::
              findAllFuel r f co text n next

def findAll (r : Regex) (f : Flags) (co : Collation) (text : List Nat) : List Match :=
  findAllFuel r f co text (text.length + 1) 0

/-- Fallback for unsupported patterns: literal substring search. -/
def literalPrefix (pat suf : List Nat) : Bool :=
  decide (pat = suf.take pat.length)

def fallbackFuel (pat text : List Nat) (fuel pos : Nat) : List Match :=
  match fuel with
  | 0 => []
  | n + 1 =>
      if pos > text.length then []
      else if literalPrefix pat (text.drop pos) then
        { start := pos, finish := pos + pat.length, caps := [] } ::
          fallbackFuel pat text n (pos + pat.length + 1)
      else fallbackFuel pat text n (pos + 1)

def fallbackFindAll (pat text : List Nat) : List Match :=
  fallbackFuel pat text (text.length + 1) 0

def findFirst (key : Key) (text : List Nat) : Option Match :=
  let r := parse key.pattern
  if isBad r then (fallbackFindAll key.pattern text).head?
  else (findAll r key.flags key.collation text).head?

/-- Replacement tokens and the observable output alphabet. -/
inductive Token where | lit (c : Nat) | group (i : Nat)
  deriving DecidableEq, Repr

inductive Elem where | ch (c : Nat) | unmatched
  deriving DecidableEq, Repr

inductive Err where | invalidGroup
  deriving DecidableEq, Repr

structure APIResult where
  pos : Option Nat
  output : List Elem
  err : Option Err
  deriving DecidableEq, Repr

inductive Mode where
  | existence
  | replace (tmpl : List Token)

def templateOk (r : Regex) (tmpl : List Token) : Bool :=
  tmpl.all (fun t => match t with
    | Token.lit _ => true
    | Token.group i => decide (i ∈ groupIds r))

/-- Rendering a replacement.  `useCaptures` decides whether a capturing code
    path is used; when it is false, group tokens expand to the empty string
    (the bug in the regression).  For unsupported patterns there are no
    captures at all. -/
def render (key : Key) (text : List Nat) (tmpl : List Token) (useCaptures : Bool) : APIResult :=
  if !templateOk (parse key.pattern) tmpl then
    { pos := none, output := [], err := some Err.invalidGroup }
  else
    match findFirst key text with
    | none => { pos := none, output := text.map Elem.ch, err := none }
    | some m =>
        let capsOpt : Option Caps :=
          if isBad (parse key.pattern) then none
          else if useCaptures then some m.caps else none
        let rep :=
          match capsOpt with
          | none => []
          | some caps =>
              List.flatMap (fun t =>
                match t with
                | Token.lit c => [Elem.ch c]
                | Token.group i =>
                    match getCap i caps with
                    | none => [Elem.unmatched]
                    | some (s, e) => (text.drop s).take (e - s) |>.map Elem.ch) tmpl
        { pos := some m.start
        , output := (text.take m.start).map Elem.ch ++ rep ++ (text.drop m.finish).map Elem.ch
        , err := none }

/-- The original API: existence search returns a position only; replacement
    always uses the capturing code path. -/
def refAPI (key : Key) (text : List Nat) (mode : Mode) : APIResult :=
  match mode with
  | Mode.existence =>
      { pos := (findFirst key text).map Match.start, output := [], err := none }
  | Mode.replace tmpl => render key text tmpl true

/-- The two compiled backends.  They share the search; only the treatment of
    captures differs. -/
inductive Backend where | existence | capturing | fallback
  deriving DecidableEq, Repr

def compile (key : Key) : Backend :=
  if isBad (parse key.pattern) then Backend.fallback
  else if key.needsCapture then Backend.capturing
  else Backend.existence

def execBackend (b : Backend) (key : Key) (text : List Nat) (mode : Mode) : APIResult :=
  match mode with
  | Mode.existence =>
      { pos := (findFirst key text).map Match.start, output := [], err := none }
  | Mode.replace tmpl =>
      let use := match b with | Backend.capturing => true | _ => false
      render key text tmpl use

/-- Derive the cache key from the call mode: replacements need captures. -/
def requestKey (base : Key) (mode : Mode) : Key :=
  { base with needsCapture := match mode with | Mode.replace _ => true | Mode.existence => false }

/-- **Parser-to-code bridge.** The compiled backend selected from the parsed
    grammar and the mode reproduces the reference API exactly. -/
theorem parser_to_code_bridge (key : Key) (text : List Nat) (mode : Mode) :
    execBackend (compile (requestKey key mode)) key text mode = refAPI key text mode := by
  cases mode with
  | existence =>
      by_cases hbad : isBad (parse key.pattern) <;>
        simp [execBackend, refAPI, compile, requestKey, hbad, render]
  | replace tmpl =>
      by_cases hbad : isBad (parse key.pattern) <;>
        simp [execBackend, refAPI, compile, requestKey, hbad, render]

theorem compile_correct (key : Key) (text : List Nat) (mode : Mode) :
    execBackend (compile (requestKey key mode)) key text mode = refAPI key text mode :=
  parser_to_code_bridge key text mode

/-! ## Cache with arbitrary hit/miss sequences -/

abbrev Cache := List (Key × Backend)

def lookupCache : Cache → Key → Option Backend
  | [], _ => none
  | (k', b) :: cs, k => if k' = k then some b else lookupCache cs k

def CacheWF (c : Cache) : Prop := ∀ p ∈ c, p.2 = compile p.1

theorem lookupCache_sound {c : Cache} {k : Key} {b : Backend}
    (hwf : CacheWF c) (h : lookupCache c k = some b) : b = compile k := by
  induction c with
  | nil => simp [lookupCache] at h
  | cons p ps ih =>
      by_cases hp : p.1 = k
      · simp [lookupCache, hp] at h
        subst b
        rw [← hp]
        exact hwf p (by simp)
      · simp [lookupCache, hp] at h
        exact ih (fun q hq => hwf q (by simp [hq])) h

def stepCache (c : Cache) (base : Key) (text : List Nat) (mode : Mode) : APIResult × Cache :=
  let k := requestKey base mode
  let hit := lookupCache c k
  let b := match hit with | some b' => b' | none => compile k
  let c' := match hit with | some _ => c | none => (k, b) :: c
  (execBackend b k text mode, c')

abbrev Request := Key × List Nat × Mode

def runCache (c : Cache) : List Request → List APIResult
  | [] => []
  | (base, text, mode) :: rest =>
      let r := stepCache c base text mode
      r.1 :: runCache r.2 rest

def refResults : List Request → List APIResult
  | [] => []
  | (base, text, mode) :: rest =>
      refAPI (requestKey base mode) text mode :: refResults rest

/-- **MAIN THEOREM.** For any well-formed initial cache and any sequence of
    cache hits and misses, the dispatched results equal the reference API
    results (match position, replacement output and error). -/
theorem regex_cache_version_sound (c0 : Cache) (hwf0 : CacheWF c0)
    (reqs : List Request) : runCache c0 reqs = refResults reqs := by
  induction reqs generalizing c0 with
  | nil => rfl
  | cons req rest ih =>
      rcases req with ⟨base, text, mode⟩
      dsimp [runCache, refResults]
      let k := requestKey base mode
      let hit := lookupCache c0 k
      cases hhit : hit with
      | none =>
          let c' : Cache := (k, compile k) :: c0
          have hwf' : CacheWF c' := by
            intro p hp
            simp [c'] at hp
            rcases hp with rfl | hp
            · rfl
            · exact hwf0 p hp
          have hres : execBackend (compile k) k text mode =
              refAPI (requestKey base mode) text mode :=
            parser_to_code_bridge base text mode
          have hstep : stepCache c0 base text mode =
              (refAPI (requestKey base mode) text mode, c') := by
            simp [stepCache, k, hit, hhit, c', hres]
          rw [hstep]
          simpa using ih c' hwf'
      | some b =>
          have hb : b = compile k := lookupCache_sound hwf0 (by simpa [hit] using hhit)
          let c' : Cache := c0
          have hwf' : CacheWF c' := by simpa [c'] using hwf0
          have hres : execBackend b k text mode =
              refAPI (requestKey base mode) text mode := by
            rw [hb]
            exact parser_to_code_bridge base text mode
          have hstep : stepCache c0 base text mode =
              (refAPI (requestKey base mode) text mode, c') := by
            simp [stepCache, k, hit, hhit, c', hres]
          rw [hstep]
          simpa using ih c' hwf'

/-! ## Counterexamples -/

def capturePattern : Key :=
  { pattern := [1, 2, 97, 98]
  , flags := {}
  , collation := Collation.binary
  , needsCapture := false }

def captureText : List Nat := [98]

def captureTemplate : List Token := [Token.group 1]

/-- The regression: pattern `[1,2,97,98]` is `alt (grp 1 'a') 'b'`; on text
    `b` group 1 does not participate.  The correct capturing backend renders
    `Elem.unmatched`, but the existence code shared with the replacement
    renders an empty string. -/
theorem unmatched_capture_sharing_counterexample :
    (execBackend (compile (requestKey capturePattern (Mode.replace captureTemplate)))
        capturePattern captureText (Mode.replace captureTemplate)).output =
      [Elem.unmatched] ∧
    (execBackend (compile capturePattern)
        capturePattern captureText (Mode.replace captureTemplate)).output = [] := by
  native_decide

/-- A key that omits flags changes the match position for case-insensitive
    matching, so flags must be part of the cache key. -/
theorem key_flags_counterexample :
    let k : Key :=
      { pattern := [0, 65], flags := { ignoreCase := true }
      , collation := Collation.binary, needsCapture := false }
    let kBad := { k with flags := { ignoreCase := false } }
    (findFirst k [97]).map Match.start = some 0 ∧
      (findFirst kBad [97]).map Match.start = none := by
  native_decide

/-- A key that omits collation changes the result for `nocase`. -/
theorem key_collation_counterexample :
    let k : Key :=
      { pattern := [0, 200], flags := {}
      , collation := Collation.nocase, needsCapture := false }
    let kBad := { k with collation := Collation.binary }
    (findFirst k [72]).map Match.start = some 0 ∧
      (findFirst kBad [72]).map Match.start = none := by
  native_decide

/-- Zero-width matches make progress: `epsilon` on two characters yields
    three matches at positions 0, 1 and 2. -/
theorem empty_match_progress :
    let k : Key := { pattern := [], flags := {}, collation := Collation.binary, needsCapture := false }
    (findAll (parse k.pattern) k.flags k.collation [1, 2]).map Match.start = [0, 1, 2] := by
  native_decide

/-- Unsupported patterns take the fallback path and still agree with the
    reference API. -/
theorem unsupported_fallback_agrees :
    let k : Key := { pattern := [9, 9], flags := {}, collation := Collation.binary, needsCapture := true }
    isBad (parse k.pattern) = true ∧
    compile k = Backend.fallback ∧
    execBackend (compile k) k [9, 9, 8] Mode.existence = refAPI k [9, 9, 8] Mode.existence := by
  native_decide

end TinylambOpt.Research.RegexCacheVersion
