import Std.Tactic
import TinylambOpt.LikeUnderscore

/-!
# Skipping LIKE blocks whose mandatory trigram is absent from a block summary

This module models the "required trigram" pre-filter used by a native LIKE
scan over a block (row group) of byte strings.

Reference evaluator.  After escape interpretation the pattern is a list of
literal segments separated by `%` (so a *single* segment never contains a
wildcard).  `ChainMatch s segs` is `%seg1%...%segN%` over binary collation:
every segment must occur as a contiguous byte run, in order, and the runs do
not overlap (the suffix after one segment is where the next one is searched).

Optimized decision procedure.  From every literal segment of length at least
three we take all of its consecutive three-byte windows.  Any such trigram is
*mandatory*: a matching row must contain it.  The block summary is an
over-approximation of the trigrams of all visible rows (Bloom collisions only
cost extra reads).  If a mandatory trigram is missing from the summary, the
whole block can be skipped.

Sufficient hypotheses (checked by the C++ implementation):
* binary collation — byte equality, modelled by equality on `Byte = Nat`;
* valid byte strings — every byte is `< 256` (no embedded NUL/escape bytes
  after escape interpretation);
* the summary is a superset of `trigrams3` of every *visible* row in the
  block (deleted rows need not be represented);
* short literals (`length < 3`, hence no trigram) never enter this test.

C++ realization: `common/literal_like_pattern.hpp` (`Compile` strips
consecutive `%` and rejects backslash escapes, so segments are wildcard-free),
the trigram extractor over `literal`, and the block-level trigram/Bloom
summary of the scan.  NULL/collation/visibility are decided by the caller
before this byte-level test.
-/

namespace TinylambOpt.TrigramSkip

abbrev Byte := Nat

/-- `p` is a prefix of `s`, expressed with an explicit suffix witness. -/
def IsPrefix (p s : List Byte) : Prop := ∃ suf, p ++ suf = s

/-- Valid byte string under binary collation. -/
def ValidByteString (s : List Byte) : Prop := ∀ b ∈ s, b < 256

/-- All contiguous length-three windows of a byte string. -/
def trigrams3 : List Byte → List (List Byte)
  | [] => []
  | [_] => []
  | [_, _] => []
  | a :: b :: c :: rest => [a, b, c] :: trigrams3 (b :: c :: rest)

/-- The reference LIKE semantics for `%seg1%...%segN%` with binary collation. -/
def ChainMatch : List Byte → List (List Byte) → Prop
  | _, [] => True
  | s, seg :: rest => ∃ pre suf, s = pre ++ seg ++ suf ∧ ChainMatch suf rest

/-- Adding one byte in front cannot destroy an existing trigram. -/
theorem trigrams3_cons_head (x : Byte) (xs : List Byte) :
    ∀ t ∈ trigrams3 xs, t ∈ trigrams3 (x :: xs) := by
  cases xs with
  | nil => simp [trigrams3]
  | cons y ys =>
      cases ys with
      | nil => simp [trigrams3]
      | cons z zs =>
          intro t h
          simp [trigrams3] at h ⊢
          exact Or.inr h

/-- Appending a prefix cannot destroy an existing trigram. -/
theorem trigrams3_append_left (pre seg : List Byte) :
    ∀ t ∈ trigrams3 seg, t ∈ trigrams3 (pre ++ seg) := by
  induction pre with
  | nil => intro t h; simpa using h
  | cons x pre' ih =>
      intro t h
      exact trigrams3_cons_head x (pre' ++ seg) t (ih t h)

/-- Appending a suffix cannot destroy an existing trigram. -/
theorem trigrams3_append_right (seg suf : List Byte) :
    ∀ t ∈ trigrams3 seg, t ∈ trigrams3 (seg ++ suf) := by
  induction seg with
  | nil => simp [trigrams3]
  | cons a tail ih =>
      intro t hmem
      cases tail with
      | nil => simp [trigrams3] at hmem
      | cons b tail' =>
          cases tail' with
          | nil => simp [trigrams3] at hmem
          | cons c rest =>
              simp [trigrams3] at hmem ⊢
              rcases hmem with hEq | hTail
              · exact Or.inl hEq
              · exact Or.inr (ih t hTail)

/-- A trigram of an infix is a trigram of the whole string. -/
theorem trigrams3_of_infix (pre seg suf : List Byte) :
    ∀ t ∈ trigrams3 seg, t ∈ trigrams3 (pre ++ seg ++ suf) := by
  intro t ht
  have ht1 : t ∈ trigrams3 (seg ++ suf) := trigrams3_append_right seg suf t ht
  simpa [List.append_assoc] using trigrams3_append_left pre (seg ++ suf) t ht1

/-- Every mandatory trigram of a matching row is a trigram of that row. -/
theorem mandatory_trigram_present (s : List Byte) (segs : List (List Byte))
    (h : ChainMatch s segs) (seg : List Byte) (hmem : seg ∈ segs)
    (t : List Byte) (ht : t ∈ trigrams3 seg) : t ∈ trigrams3 s := by
  induction segs generalizing s seg t with
  | nil => simp at hmem
  | cons seg0 rest ih =>
      rcases h with ⟨pre, suf, hs, hrest⟩
      simp at hmem
      rcases hmem with hEq | hmem
      · subst seg
        rw [hs]
        simpa [List.append_assoc] using trigrams3_of_infix pre seg0 suf t ht
      · have ht_suf : t ∈ trigrams3 suf := ih suf hrest seg hmem t ht
        rw [hs]
        exact trigrams3_append_left (pre ++ seg0) suf t ht_suf

/-- Block soundness: a missing mandatory trigram excludes every visible row. -/
theorem trigram_skip_sound
    (block : List (List Byte)) (summary : List (List Byte))
    (segs : List (List Byte))
    (_hValidBlock : ∀ row ∈ block, ValidByteString row)
    (_hValidSegs : ∀ seg ∈ segs, ValidByteString seg)
    (hSummary : ∀ row ∈ block, ∀ t ∈ trigrams3 row, t ∈ summary)
    {tri : List Byte}
    (htri_mandatory : ∃ seg ∈ segs, tri ∈ trigrams3 seg)
    (htri_missing : tri ∉ summary) :
    ∀ row ∈ block, ¬ ChainMatch row segs := by
  intro row hrow hmatch
  rcases htri_mandatory with ⟨seg, hseg, htseg⟩
  have htrow : tri ∈ trigrams3 row :=
    mandatory_trigram_present row segs hmatch seg hseg tri htseg
  exact htri_missing (hSummary row hrow tri htrow)

/-- Compute the signature from the visible physical rows. Multiple trigrams
    may hash to one bit, which only causes extra scans. -/
theorem built_trigram_skip (block : List (List Byte)) (visible : List Byte → Bool)
    (hash : List Byte → Nat) (segs : List (List Byte)) {tri : List Byte}
    (mand : ∃ seg ∈ segs, tri ∈ trigrams3 seg)
    (miss : hash tri ∉ ((block.filter visible).flatMap trigrams3).map hash) :
    ∀ row ∈ block, visible row = true → ¬ ChainMatch row segs := by
  intro row hr hv hm
  obtain ⟨seg,hs,ht⟩ := mand
  apply miss
  exact List.mem_map_of_mem (List.mem_flatMap.mpr
    ⟨row, List.mem_filter.mpr ⟨hr,hv⟩, mandatory_trigram_present row segs hm seg hs tri ht⟩)

open LikeUnderscore in
private theorem literal_prefix (lit row : List Byte) (post : List Tok)
    (h : ByteLikeRef row (lit.map Tok.lit ++ post) = true) :
    ∃ suffix, row = lit ++ suffix := by
  induction lit generalizing row with
  | nil => exact ⟨row,rfl⟩
  | cons c lit ih =>
    cases row with
    | nil => simp [ByteLikeRef] at h
    | cons b bs =>
      simp [ByteLikeRef] at h
      obtain ⟨s,rfl⟩ := ih _ h.2
      exact ⟨s,by simp [h.1]⟩

open LikeUnderscore in
private theorem matched_segment (row lit : List Byte) (pre post : List Tok)
    (h : ByteLikeRef row (pre ++ lit.map Tok.lit ++ post) = true) :
    ∃ a b, row = a ++ lit ++ b := by
  cases hp : pre with
  | nil =>
    obtain ⟨s,hs⟩ := literal_prefix lit row post (by simpa [hp] using h)
    exact ⟨[],s,hs⟩
  | cons t ts =>
    have liftDrop (n : Nat) (hsub : ∃ a b, row.drop n = a ++ lit ++ b) :
        ∃ a b, row = a ++ lit ++ b := by
      obtain ⟨a,b,eq⟩ := hsub
      exact ⟨row.take n ++ a,b,by simpa [List.append_assoc, ← eq] using
        (List.take_append_drop n row).symm⟩
    cases ht : t with
    | lit c =>
      cases hr : row with
      | nil => simp [hp, ht, hr, ByteLikeRef] at h
      | cons b bs =>
        simp [hp, ht, hr, ByteLikeRef] at h
        obtain ⟨a,z,rfl⟩ := matched_segment bs lit ts post (by simpa [List.append_assoc] using h.2)
        exact ⟨b::a,z,rfl⟩
    | under =>
      cases hr : row with
      | nil => simp [hp, ht, hr, ByteLikeRef] at h
      | cons b bs =>
        apply (hr ▸ liftDrop) (charWidth b bs)
        exact matched_segment _ _ ts post (by simpa [hp,ht,hr,ByteLikeRef] using h)
    | percent =>
      cases hr : row with
      | nil =>
        exact matched_segment [] lit ts post (by simpa [hp,ht,hr,ByteLikeRef] using h)
      | cons b bs =>
        simp only [hp,ht,hr,List.cons_append,ByteLikeRef, Bool.or_eq_true] at h
        rcases h with h | h
        · exact matched_segment _ lit ts post h
        · apply (hr ▸ liftDrop) (1 + (charWidth b bs - 1))
          simpa [Nat.add_comm 1, List.drop_succ_cons] using
            matched_segment (bs.drop (charWidth b bs - 1)) lit (.percent :: ts) post h
termination_by (pre.length,row.length)
decreasing_by all_goals simp_all [Prod.lex_def] <;> omega

open LikeUnderscore in
theorem raw_trigram_skip (rows : List (List Byte)) (pattern lit tri : List Byte)
    (pre post : List Tok) (split : tokenize pattern = pre ++ lit.map Tok.lit ++ post)
    (ht : tri ∈ trigrams3 lit) (hash : List Byte → Nat)
    (absent : hash tri ∉ (rows.flatMap trigrams3).map hash) :
    ∀ row ∈ rows, ByteLikeRef row (tokenize pattern) = false := by
  intro row hr
  cases h : ByteLikeRef row (tokenize pattern) with
  | false => rfl
  | true =>
    rw [split] at h
    obtain ⟨a,b,rfl⟩ := matched_segment _ _ _ _ h
    exact False.elim (absent (List.mem_map_of_mem (List.mem_flatMap.mpr
      ⟨_,hr,trigrams3_of_infix a lit b tri ht⟩)))

end TinylambOpt.TrigramSkip

-- C++ references to this model or its proof.
-- executor/zone_map.hpp
