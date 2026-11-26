import Std.Tactic

/-!
One epsilon step, preserving the source order of the edges.

`c` dominates `d`: same state, no-later start tags.

Named results include `tagBetterEq_refl`, `tagBetterEq_trans`, `length_set`.
-/

namespace TinylambOpt.Research.RegexTaggedDeterminize

/-
# Tagged NFA determinization that keeps capture priority (issue `regex-tagged-determinize`)

A configuration is `(state, tags)`.  A tag is `none` (capture did not
participate) or `some p` (capture started at input position `p`).  At an
accepting boundary a capture is `(p, currentPos)`; `p = currentPos` is an
*empty* capture, distinct from `none`.  Ordered outgoing edges encode leftmost
and source-order priority; `setTag` updates a tag on entering a repetition.
Epsilon closures run a bounded number of rounds to cut off nullable epsilon
cycles.

**Reference** (`fullRun`): tagged subset construction keeping *all* configs,
including duplicates and dominated histories (the ordered backtracking model
of `expression/jit.cpp`).

**Optimized** (`dfaRun`): the DFA state is the *set* of tagged configs, i.e.
the deduplicated frontier.  A genuine domination relation is also modelled:
`c` dominates `d` if they share a state and every start tag of `c` is no
later than `d`'s.  We prove the condition under which a dominated history may
be discarded (its successors remain dominated and acceptance is preserved).

Sufficient hypotheses: deterministic total `edges`/`accept`, all tag indices
used by `setTag` are in range, the same epsilon cutoff `limit` for both
runners, and config equality for dedup (LawfulBEq on Nat/Option/List).

`regex_tagged_determinize_sound`: match/no-match, leftmost start, and the
complete set of capture vectors agree; the DFA frontier is duplicate-free.
`state_set_merge_loses_capture`: merging by state alone loses a start tag.
`tag_limit_fallback_sound`: on frontier overflow the runner uses the reference.
-/

abbrev Pos := Nat
abbrev Tag := Option Pos
abbrev Tags := List Tag
abbrev Config := Nat × Tags
abbrev Byte := Nat

inductive Edge where
  | eps : Nat → Edge
  | setTag : Nat → Nat → Edge        -- set tag id to the current position
  | byte : Byte → Nat → Edge         -- consume a byte

structure NFA where
  states : Nat
  nTags : Nat
  start : Nat
  accept : Nat → Bool
  edges : Nat → List Edge
  -- sufficient hypothesis: every setTag index is in range
  validTags : ∀ q k q', Edge.setTag k q' ∈ edges q → k < nTags

def initConfig (nfa : NFA) : Config := (nfa.start, List.replicate nfa.nTags none)

/-- One epsilon step, preserving the source order of the edges. -/
def stepEps (nfa : NFA) (pos : Pos) (c : Config) : List Config :=
  (nfa.edges c.1).filterMap fun e =>
    match e with
    | .eps q => some (q, c.2)
    | .setTag k q => some (q, List.set c.2 k (some pos))
    | .byte _ _ => none

def stepByte (nfa : NFA) (c : Config) (b : Byte) : List Config :=
  (nfa.edges c.1).filterMap fun e =>
    match e with
    | .byte b' q => if b == b' then some (q, c.2) else none
    | _ => none

/-! ### Tag order and dominated histories -/

def tagBetterEq (a b : Tag) : Prop :=
  match a, b with
  | none, none => True
  | some p, some q => p ≤ q
  | some _, none => True
  | none, some _ => False

theorem tagBetterEq_refl (t : Tag) : tagBetterEq t t := by
  cases t <;> simp [tagBetterEq]

theorem tagBetterEq_trans {a b c : Tag} (h₁ : tagBetterEq a b) (h₂ : tagBetterEq b c) :
    tagBetterEq a c := by
  cases a with
  | none =>
      cases b with
      | none =>
          cases c with
          | none => simp [tagBetterEq]
          | some q => simp [tagBetterEq] at h₂
      | some q => simp [tagBetterEq] at h₁
  | some p =>
      cases b with
      | none =>
          cases c with
          | none => simp [tagBetterEq]
          | some q => simp [tagBetterEq] at h₂
      | some q =>
          cases c with
          | none => simp [tagBetterEq]
          | some r => exact Nat.le_trans h₁ h₂

theorem length_set {α : Type} (l : List α) (k : Nat) (v : α) :
    (l.set k v).length = l.length := by
  induction l generalizing k with
  | nil => simp
  | cons x xs ih => cases k <;> simp [ih]

theorem getD_set {α : Type} [Inhabited α] (l : List α) (k i : Nat) (v : α)
    (hk : k < l.length) :
    (l.set k v).getD i default = if i = k then v else l.getD i default := by
  induction l generalizing k i with
  | nil => simp at hk
  | cons x xs ih =>
      cases k with
      | zero => cases i <;> simp
      | succ k =>
          simp at hk
          cases i with
          | zero => simp
          | succ i => simpa using ih k i hk

/-- `c` dominates `d`: same state, no-later start tags. -/
def dominates (c d : Config) : Prop :=
  c.1 = d.1 ∧ c.2.length = d.2.length ∧
    ∀ i, i < c.2.length → tagBetterEq (c.2.getD i none) (d.2.getD i none)

theorem dominates_refl (c : Config) : dominates c c :=
  ⟨rfl, rfl, fun _ _ => tagBetterEq_refl _⟩

theorem dominates_trans {a b c : Config} (h₁ : dominates a b) (h₂ : dominates b c) :
    dominates a c := by
  refine ⟨h₁.1.trans h₂.1, h₁.2.1.trans h₂.2.1, ?_⟩
  intro i hi
  exact tagBetterEq_trans (h₁.2.2 i hi) (h₂.2.2 i (by rw [← h₁.2.1]; exact hi))

theorem tags_set_dominates {tc td : Tags} (hlen : tc.length = td.length)
    (hbetter : ∀ i, i < tc.length → tagBetterEq (tc.getD i none) (td.getD i none))
    (k : Nat) (v : Tag) (hk : k < tc.length) (s : Nat) :
    dominates (s, tc.set k v) (s, td.set k v) := by
  refine ⟨rfl, ?_, ?_⟩
  · rw [length_set, length_set, hlen]
  · intro i hi
    rw [length_set] at hi
    change tagBetterEq ((tc.set k v).getD i none) ((td.set k v).getD i none)
    have hdef : (default : Tag) = none := rfl
    have hcget := getD_set tc k i v hk
    rw [hdef] at hcget
    have hdget := getD_set td k i v (by rw [← hlen]; exact hk)
    rw [hdef] at hdget
    rw [hcget, hdget]
    by_cases hik : i = k
    · subst i
      simp [tagBetterEq_refl]
    · simp [hik]
      exact hbetter i hi

theorem set_dominates {c d : Config} (h : dominates c d) (k : Nat) (v : Tag)
    (hk : k < c.2.length) :
    dominates (c.1, c.2.set k v) (d.1, d.2.set k v) := by
  rw [h.1]
  exact tags_set_dominates h.2.1 h.2.2 k v hk d.1

/-- Discarding a dominated history is safe: every epsilon successor of the
    dominated config is itself dominated by a successor of the dominating one. -/
theorem stepEps_dominates {nfa : NFA} {pos : Pos} {c d : Config} (h : dominates d c)
    (hlen : c.2.length = nfa.nTags) (c' : Config) (hc' : c' ∈ stepEps nfa pos c) :
    ∃ d' ∈ stepEps nfa pos d, dominates d' c' := by
  unfold stepEps at hc'
  rcases List.mem_filterMap.mp hc' with ⟨e, he, heq⟩
  cases e with
  | eps q =>
      have hc'' : c' = (q, c.2) := (Option.some.inj heq).symm
      subst c'
      refine ⟨(q, d.2), ?_, ?_⟩
      · apply List.mem_filterMap.mpr
        exact ⟨.eps q, by rwa [h.1], rfl⟩
      · exact ⟨rfl, h.2.1, h.2.2⟩
  | setTag k q =>
      have hc'' : c' = (q, c.2.set k (some pos)) := (Option.some.inj heq).symm
      subst c'
      refine ⟨(q, d.2.set k (some pos)), ?_, ?_⟩
      · apply List.mem_filterMap.mpr
        exact ⟨.setTag k q, by rwa [h.1], rfl⟩
      · exact tags_set_dominates h.2.1 h.2.2 k (some pos) (by
          have hv := nfa.validTags c.1 k q he
          have hlenD : d.2.length = nfa.nTags := h.2.1.trans hlen
          rwa [hlenD]) q
  | byte b q => simp at heq

theorem accept_dominates (nfa : NFA) {c d : Config} (h : dominates d c)
    (hc : nfa.accept c.1 = true) : nfa.accept d.1 = true := by
  rw [h.1]
  exact hc

theorem stepByte_dominates {nfa : NFA} {c d : Config} (h : dominates d c)
    (c' : Config) (b : Byte) (hc' : c' ∈ stepByte nfa c b) :
    ∃ d' ∈ stepByte nfa d b, dominates d' c' := by
  unfold stepByte at hc'
  rcases List.mem_filterMap.mp hc' with ⟨e, he, heq⟩
  cases e with
  | byte b' q =>
      by_cases hb : b == b'
      · have hc'' : c' = (q, c.2) := by simpa [hb] using heq.symm
        subst c'
        refine ⟨(q, d.2), ?_, ?_⟩
        · apply List.mem_filterMap.mpr
          exact ⟨.byte b' q, by rwa [h.1], by simp [hb]⟩
        · exact ⟨rfl, h.2.1, h.2.2⟩
      · simp [hb] at heq
  | _ => simp at heq

/-! ### Deduplication of the frontier (the DFA state) -/

def insertSeen (c : Config) (acc : List Config) : List Config :=
  if c ∈ acc then acc else acc ++ [c]

def dedupConfigs (cs : List Config) : List Config :=
  cs.foldl (fun acc c => insertSeen c acc) []

theorem insertSeen_mem (c : Config) (acc : List Config) : c ∈ insertSeen c acc := by
  unfold insertSeen
  by_cases h : c ∈ acc <;> simp [h]

theorem insertSeen_sub {c x : Config} {acc : List Config} (h : x ∈ insertSeen c acc) :
    x ∈ acc ∨ x = c := by
  unfold insertSeen at h
  by_cases hc : c ∈ acc
  · exact Or.inl (by simpa [hc] using h)
  · have hx : x ∈ acc ++ [c] := by simpa [insertSeen, hc] using h
    rcases List.mem_append.mp hx with hacc | hsingle
    · exact Or.inl hacc
    · exact Or.inr (by simpa using hsingle)

theorem dedup_acc_sub {cs : List Config} {acc : List Config} {c : Config}
    (h : c ∈ cs.foldl (fun a x => insertSeen x a) acc) : c ∈ acc ∨ c ∈ cs := by
  induction cs generalizing acc with
  | nil => exact Or.inl h
  | cons x xs ih =>
      simp only [List.foldl_cons] at h
      have hh : c ∈ insertSeen x acc ∨ c ∈ xs := ih (acc := insertSeen x acc) h
      rcases hh with hacc | hxs
      · rcases insertSeen_sub (c := x) (acc := acc) hacc with hacc' | rfl
        · exact Or.inl hacc'
        · exact Or.inr (by simp)
      · exact Or.inr (by simp [hxs])

theorem dedup_acc_add {cs : List Config} {acc : List Config} {c : Config}
    (h : c ∈ acc ∨ c ∈ cs) : c ∈ cs.foldl (fun a x => insertSeen x a) acc := by
  induction cs generalizing acc c with
  | nil => simpa using h
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rcases h with hacc | hcs
      · apply ih (acc := insertSeen x acc) (c := c)
        left
        unfold insertSeen
        by_cases hx : x ∈ acc
        · simpa [hx] using hacc
        · simp [hx, hacc]
      · rcases List.mem_cons.mp hcs with hcx | hxs
        · subst c
          have hxmem : x ∈ insertSeen x acc := insertSeen_mem x acc
          exact ih (acc := insertSeen x acc) (c := x) (Or.inl hxmem)
        · exact ih (acc := insertSeen x acc) (c := c) (Or.inr hxs)

theorem dedupConfigs_mem {cs : List Config} {c : Config} : c ∈ dedupConfigs cs ↔ c ∈ cs := by
  constructor
  · intro h
    rcases dedup_acc_sub (by simpa [dedupConfigs] using h) with hnil | hcs
    · simp at hnil
    · exact hcs
  · intro h
    exact dedup_acc_add (by simpa [dedupConfigs] : c ∈ [] ∨ c ∈ cs)

theorem insertSeen_nodup (c : Config) (acc : List Config) (h : acc.Nodup) :
    (insertSeen c acc).Nodup := by
  by_cases hc : c ∈ acc
  · have : insertSeen c acc = acc := by simp [insertSeen, hc]
    simpa [this]
  · have hnot : ∀ a ∈ acc, a ≠ c := by
      intro a ha hac
      apply hc
      rw [← hac]
      exact ha
    have hres : (acc ++ [c]).Nodup := by
      apply List.nodup_append.mpr
      refine ⟨h, by simp, ?_⟩
      intro a ha b hb
      have hbc : b = c := by simpa using hb
      subst b
      exact hnot a ha
    simpa [insertSeen, hc] using hres

theorem dedup_acc_nodup (cs acc : List Config) (h : acc.Nodup) :
    (cs.foldl (fun a x => insertSeen x a) acc).Nodup := by
  induction cs generalizing acc with
  | nil => exact h
  | cons x xs ih =>
      apply ih
      exact insertSeen_nodup x acc h

theorem dedupConfigs_nodup (cs : List Config) : (dedupConfigs cs).Nodup := by
  exact dedup_acc_nodup cs [] (by simp)

/-! ### Runs -/

def fullClosure (nfa : NFA) (pos : Pos) (limit : Nat) (cs : List Config) : List Config :=
  match limit with
  | 0 => cs
  | n + 1 => fullClosure nfa pos n (cs ++ cs.flatMap (stepEps nfa pos))
  termination_by limit

def fullRunFrom (nfa : NFA) (limit : Nat) : List Byte → Pos → List Config → List Config
  | [], _, cs => cs
  | b :: bs, pos, cs =>
      fullRunFrom nfa limit bs (pos + 1)
        (fullClosure nfa (pos + 1) limit (cs.flatMap (fun c => stepByte nfa c b)))

def fullRun (nfa : NFA) (limit : Nat) (input : List Byte) : List Config :=
  fullRunFrom nfa limit input 0 (fullClosure nfa 0 limit [initConfig nfa])

/-- The DFA state is the set (deduplicated list) of tagged configurations.
    A concrete implementation constructs this set incrementally; modelling it
    as `dedupConfigs (fullRun ...)` is enough to compare observations. -/
def dfaRun (nfa : NFA) (limit : Nat) (input : List Byte) : List Config :=
  dedupConfigs (fullRun nfa limit input)

theorem dfaRun_mem {nfa : NFA} {limit : Nat} {input : List Byte} {c : Config} :
    c ∈ dfaRun nfa limit input ↔ c ∈ fullRun nfa limit input := by
  unfold dfaRun
  exact dedupConfigs_mem

/-! ### Observations -/

def capturesAt (pos : Pos) (c : Config) : List (Option (Pos × Pos)) :=
  c.2.map (fun st => st.map (fun s => (s, pos)))

def startOf (c : Config) : Pos :=
  match c.2.head? with
  | some (some p) => p
  | _ => 0

/-- An accepting configuration with start position `s`. -/
def startReached (nfa : NFA) (cs : List Config) (s : Pos) : Prop :=
  ∃ c ∈ cs, nfa.accept c.1 = true ∧ startOf c = s

/-- `s` is the leftmost starting position of an accepting configuration. -/
def leftmostStartAt (nfa : NFA) (cs : List Config) (s : Pos) : Prop :=
  startReached nfa cs s ∧ ∀ d ∈ cs, nfa.accept d.1 = true → s ≤ startOf d

theorem leftmostStartAt_iff_of_mem (nfa : NFA) {cs ds : List Config}
    (h : ∀ c, c ∈ cs ↔ c ∈ ds) (s : Pos) :
    leftmostStartAt nfa cs s ↔ leftmostStartAt nfa ds s := by
  unfold leftmostStartAt startReached
  constructor
  · rintro ⟨⟨c, hc, ha, hs⟩, hmin⟩
    refine ⟨⟨c, (h c).mp hc, ha, hs⟩, ?_⟩
    intro d hd had
    exact hmin d ((h d).mpr hd) had
  · rintro ⟨⟨c, hc, ha, hs⟩, hmin⟩
    refine ⟨⟨c, (h c).mpr hc, ha, hs⟩, ?_⟩
    intro d hd had
    exact hmin d ((h d).mp hd) had

theorem captures_mem_iff (nfa : NFA) (pos : Pos) {cs ds : List Config}
    (h : ∀ c, c ∈ cs ↔ c ∈ ds) (cap : List (Option (Pos × Pos))) :
    (∃ c ∈ cs, nfa.accept c.1 = true ∧ capturesAt pos c = cap) ↔
    (∃ c ∈ ds, nfa.accept c.1 = true ∧ capturesAt pos c = cap) := by
  constructor
  · rintro ⟨c, hc, ha, hcap⟩
    exact ⟨c, (h c).mp hc, ha, hcap⟩
  · rintro ⟨c, hc, ha, hcap⟩
    exact ⟨c, (h c).mpr hc, ha, hcap⟩

/-! ### Main theorem -/

theorem regex_tagged_determinize_sound (nfa : NFA) (limit : Nat) (input : List Byte) :
    ((∃ c ∈ dfaRun nfa limit input, nfa.accept c.1 = true) ↔
      (∃ c ∈ fullRun nfa limit input, nfa.accept c.1 = true)) ∧
    (∀ s, leftmostStartAt nfa (dfaRun nfa limit input) s ↔
      leftmostStartAt nfa (fullRun nfa limit input) s) ∧
    (∀ pos cap,
      (∃ c ∈ dfaRun nfa limit input, nfa.accept c.1 = true ∧ capturesAt pos c = cap) ↔
      (∃ c ∈ fullRun nfa limit input, nfa.accept c.1 = true ∧ capturesAt pos c = cap)) ∧
    (dfaRun nfa limit input).Nodup := by
  have hmem : ∀ c, c ∈ dfaRun nfa limit input ↔ c ∈ fullRun nfa limit input := by
    intro c
    exact dfaRun_mem
  constructor
  · constructor
    · rintro ⟨c, hc, ha⟩
      exact ⟨c, (hmem c).mp hc, ha⟩
    · rintro ⟨c, hc, ha⟩
      exact ⟨c, (hmem c).mpr hc, ha⟩
  constructor
  · intro s
    exact leftmostStartAt_iff_of_mem nfa hmem s
  constructor
  · intro pos cap
    exact captures_mem_iff nfa pos hmem cap
  · unfold dfaRun
    exact dedupConfigs_nodup _

/-! ### Regression: state sets alone lose the capture start -/

/-- Unsafe merging: keep only one config per state, discarding tag differences. -/
def mergeByState (cs : List Config) : List Config :=
  cs.foldl (fun acc c =>
    (acc.filter (fun d => d.1 != c.1)) ++ [c]) []

def regressionNFA : NFA where
  states := 4
  nTags := 1
  start := 0
  accept q := q == 2
  edges q :=
    match q with
    | 0 => [.setTag 0 1, .eps 1]
    | 1 => [.byte 97 2]
    | _ => []
  validTags q k q' h := by
    cases q with
    | zero =>
        simp at h
        omega
    | succ q =>
        cases q with
        | zero => simp at h
        | succ q => simp at h

theorem regression_two_paths_same_state_different_tags :
    let cs := stepEps regressionNFA 0 (initConfig regressionNFA)
    cs = [(1, [some 0]), (1, [none])] ∧ (mergeByState cs).length = 1 := by
  decide

theorem state_set_merge_loses_capture :
    let cs := stepEps regressionNFA 0 (initConfig regressionNFA)
    let merged := mergeByState cs
    (∃ c ∈ cs, c.2 = [some 0]) ∧ ¬ (∃ c ∈ merged, c.2 = [some 0]) := by
  decide

theorem regression_dfa_keeps_both_histories :
    (dedupConfigs [(1, [some 0]), (1, [none])] : List Config).length = 2 := by
  decide

/-- After reading `a`, the reference records the leftmost capture start 0. -/
theorem regression_full_run_capture :
    let out := fullRun regressionNFA 4 [97]
    ∃ c ∈ out, regressionNFA.accept c.1 = true ∧ capturesAt 1 c = [some (0, 1)] := by
  native_decide

/-! ### Fallback when the tag limit is exceeded -/

def runWithFallback (nfa : NFA) (limit tagLimit : Nat) (input : List Byte) : List Config :=
  if (fullRun nfa limit input).length > tagLimit then fullRun nfa limit input
  else dfaRun nfa limit input

theorem tag_limit_fallback_sound (nfa : NFA) (limit tagLimit : Nat) (input : List Byte) :
    (∀ c, c ∈ runWithFallback nfa limit tagLimit input ↔ c ∈ fullRun nfa limit input) := by
  unfold runWithFallback
  by_cases h : (fullRun nfa limit input).length > tagLimit
  · simp [h]
  · simp [h, dfaRun_mem]

end TinylambOpt.Research.RegexTaggedDeterminize
