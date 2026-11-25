import TinylambOpt.ResearchGoals.Compression.PhraseTables

namespace TinylambOpt.ResearchProofs.Compression.PhraseTables

open TinylambOpt.ResearchGoals.Compression.PhraseTables
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Proofs and counterexamples for PhraseTables goals.

`Bytes = List Nat`, `Words = List Bytes`.  The DP `parse` considers the literal
path first and every matching dictionary word (with an index below 255), and
`shortest` picks a minimum-length candidate.  Most positive goals follow from
three facts: every candidate decodes back to the input, the chosen candidate is
a member of the candidate list, and no well-formed stream can be beaten by
another well-formed stream. -/

private abbrev B := TinylambOpt.ResearchGoals.Compression.Wire.Bytes
private abbrev W := TinylambOpt.ResearchGoals.Compression.Text.Words

/-! ## `shortest` -/

private theorem shortest_mem_of_ne_nil {cands : List B} (h : cands ≠ []) :
    shortest cands ∈ cands := by
  unfold shortest
  let le := fun a b : B => decide (a.length ≤ b.length)
  have hp := List.mergeSort_perm cands le
  cases hms : cands.mergeSort le with
  | nil =>
      have hlen := hp.length_eq
      have hpos : 0 < cands.length := by
        cases cands with
        | nil => exact (h rfl).elim
        | cons y ys => simp
      simp [hms] at hlen
      omega
  | cons x xs =>
      have hxsort : x ∈ cands.mergeSort le := by rw [hms]; simp
      have hx : x ∈ cands := by simpa using hxsort
      simp [hx]

private theorem shortest_le_of_mem {cands : List B} {c : B} (hc : c ∈ cands) :
    (shortest cands).length ≤ c.length := by
  unfold shortest
  let le := fun a b : B => decide (a.length ≤ b.length)
  have htrans : ∀ a b d : B, le a b → le b d → le a d := by
    intro a b d hab hbd
    simp [le] at hab hbd ⊢
    omega
  have htotal : ∀ a b : B, le a b || le b a := by
    intro a b
    simp [le]
    omega
  have hsorted := List.pairwise_mergeSort htrans htotal cands
  have hc' : c ∈ cands.mergeSort le := List.mem_mergeSort.mpr hc
  cases hms : cands.mergeSort le with
  | nil =>
      have : c ∈ ([] : List B) := by simp [hms] at hc'
      simp at this
  | cons x xs =>
      have hs : (x :: xs).Pairwise (fun a b => le a b = true) := by
        simpa [hms] using hsorted
      have hxle : ∀ b ∈ xs, le x b = true := (List.pairwise_cons.mp hs).1
      have hxc : x.length ≤ c.length := by
        rcases List.mem_cons.mp (by simpa [hms] using hc' : c ∈ x :: xs) with rfl | hcx
        · exact Nat.le_refl _
        · have hle := hxle c hcx
          simpa [le] using hle
      simpa [hms] using hxc

/-! ## Well-formed token streams -/

private inductive Good : B → Prop where
  | nil : Good []
  | lit {x : Nat} {rest : B} : Good rest → Good (255 :: x :: rest)
  | code {i : Nat} {rest : B} : i < 255 → Good rest → Good (i :: rest)

private theorem good_append : ∀ {a : B}, Good a → ∀ b : B, Good b → Good (a ++ b)
  | [], _, b, hb => by simpa using hb
  | 255 :: x :: rest, Good.lit h, b, hb => by
      change Good (255 :: x :: (rest ++ b))
      exact Good.lit (good_append h b hb)
  | i :: rest, Good.code hi h, b, hb => by
      change Good (i :: (rest ++ b))
      exact Good.code hi (good_append h b hb)

private theorem good_nil {x : Nat} : Good [x] ↔ x < 255 := by
  constructor
  · intro h
    cases h with
    | code hi _ => exact hi
  · intro h
    exact Good.code h Good.nil

private theorem expand_cons_255 (d : W) (x : Nat) (t : B) :
    expand d (255 :: x :: t) = (expand d t).map (fun tail => x :: tail) := by
  rfl

private theorem expand_cons_ne255 (d : W) (c : Nat) (t : B) (hc : (c == 255) = false) :
    expand d (c :: t) = (d[c]?).bind (fun w => (expand d t).map (fun tail => w ++ tail)) := by
  have hne : c ≠ 255 := by
    intro h
    subst c
    simp at hc
  cases t with
  | nil => simp [expand, hne]
  | cons x xs =>
      simp [expand, hne]
      cases hw : d[c]? <;> cases ht : expand d (x :: xs) <;> simp

private theorem expand_append_good : ∀ (d : W) {a : B}, Good a → ∀ b : B,
    expand d (a ++ b) = (expand d a).bind (fun sa => (expand d b).map (fun sb => sa ++ sb))
  | d, [], _, b => by simp [expand]
  | d, 255 :: x :: rest, Good.lit h, b => by
      have hrec := expand_append_good d h b
      rw [show (255 :: x :: rest) ++ b = 255 :: x :: (rest ++ b) by rfl]
      rw [expand_cons_255, expand_cons_255]
      rw [hrec]
      cases hx : expand d rest with
      | none => simp
      | some sr =>
          cases hb : expand d b with
          | none => simp
          | some sb => simp
  | d, i :: rest, Good.code hi h, b => by
      have hrec := expand_append_good d h b
      rw [show (i :: rest) ++ b = i :: (rest ++ b) by rfl]
      have hi255 : (i == 255) = false := by
        have : i ≠ 255 := by omega
        exact beq_eq_false_iff_ne.mpr this
      rw [expand_cons_ne255 d i (rest ++ b) hi255, expand_cons_ne255 d i rest hi255]
      cases hw : d[i]? with
      | none => simp
      | some w =>
          rw [hrec]
          cases hx : expand d rest with
          | none => simp
          | some sr =>
              cases hb : expand d b with
              | none => simp
              | some sb => simp [List.append_assoc]

private theorem expand_literals (d : W) : ∀ s : B, expand d (literals s) = some s := by
  intro s
  induction s with
  | nil => rfl
  | cons x xs ih =>
      change expand d (255 :: x :: literals xs) = some (x :: xs)
      simp [expand, ih]

private theorem drop_drop_len (w s : B) (n : Nat) :
    (s.drop w.length).drop n = s.drop (w.length + n) := by
  rw [List.drop_drop]

/-! ## Prefix and lookup helpers -/

private theorem prefix_append_drop {w s : B} (h : w.isPrefixOf s) :
    w ++ s.drop w.length = s :=
  List.prefix_iff_eq_append.mp (List.isPrefixOf_iff_prefix.mp h)

private theorem prefix_length_eq {w s : B} (h : w.isPrefixOf s) :
    w.length + (s.drop w.length).length = s.length := by
  simpa [List.length_append] using congrArg List.length (prefix_append_drop h)

private theorem length_pos_of_ne_nil {α : Type} {l : List α} (h : l ≠ []) : 0 < l.length := by
  cases l with
  | nil => exact (h rfl).elim
  | cons x xs => simp

private theorem getElem_append_left (d new : W) (i : Nat) (hi : i < d.length) :
    (d ++ new)[i]? = d[i]? := by
  simp [List.getElem?_append, hi]

private theorem getElem_take_255 (d : W) (i : Nat) (hi : i < 255) :
    (d.take 255)[i]? = d[i]? := by
  simp [hi]

private theorem mem_choices_of (d : W) (s w : B) (i : Nat)
    (hi : i < 255) (hw : d[i]? = some w) (hne : w ≠ []) (hpfx : w.isPrefixOf s) :
    (w, i) ∈ choices d s := by
  unfold choices
  apply List.mem_filter.mpr
  constructor
  · apply List.mk_mem_zipIdx_iff_getElem?.mpr
    simpa [getElem_take_255 d i hi] using hw
  · simp [hne, hpfx]


/-! ## Candidates and DP round-trip -/

private theorem choices_data {d : W} {s w : B} {i : Nat} (h : (w, i) ∈ choices d s) :
    w ≠ [] ∧ w.isPrefixOf s = true ∧ i < 255 ∧ d[i]? = some w := by
  unfold choices at h
  rcases List.mem_filter.mp h with ⟨hz, hcond⟩
  have hcond' : (!w.isEmpty = true) ∧ (w.isPrefixOf s = true) := by
    simpa [Bool.and_eq_true] using hcond
  have hwne : w ≠ [] := by
    cases w with
    | nil => simp at hcond'
    | cons x xs => simp
  have hget : (d.take 255)[i]? = some w := List.mk_mem_zipIdx_iff_getElem?.mp hz
  have hiTake : i < (d.take 255).length := (List.getElem?_eq_some_iff.mp hget).1
  have hi : i < 255 := by
    have hle : (d.take 255).length ≤ 255 := List.length_take_le _ _
    omega
  exact ⟨hwne, hcond'.2, hi, by simpa [getElem_take_255 d i hi] using hget⟩

private theorem good_literals : ∀ s : B, Good (literals s)
  | [] => by simp [literals]; exact Good.nil
  | x :: xs => by
      rw [show literals (x :: xs) = 255 :: x :: literals xs by rfl]
      exact Good.lit (good_literals xs)

private theorem parse_length_le_raw (d : W) (fuel : Nat) (x : Nat) (xs : B) :
    (parse d (fuel + 1) (x :: xs)).length ≤ 2 + (parse d fuel xs).length := by
  dsimp [parse]
  let cands := ([255, x] ++ parse d fuel xs) ::
    ((choices d (x :: xs)).map (fun (w, i) => i :: parse d fuel ((x :: xs).drop w.length)))
  have hraw : ([255, x] ++ parse d fuel xs) ∈ cands := by simp [cands]
  have hle := shortest_le_of_mem (cands := cands) hraw
  have hle' : (shortest cands).length ≤ 2 + (parse d fuel xs).length := by
    simp only [List.length_append, List.length_cons, List.length_nil] at hle
    omega
  simpa [cands] using hle'

private theorem parse_length_le_choice (d : W) (fuel : Nat) (s : B) (w : B) (i : Nat)
    (h : (w, i) ∈ choices d s) :
    (parse d (fuel + 1) s).length ≤ 1 + (parse d fuel (s.drop w.length)).length := by
  cases s with
  | nil => simp [choices] at h
  | cons y ys =>
      dsimp [parse]
      let s : B := y :: ys
      let cands := ([255, y] ++ parse d fuel ys) ::
        ((choices d s).map (fun (w, i) => i :: parse d fuel (s.drop w.length)))
      have hcand : (i :: parse d fuel (s.drop w.length)) ∈ cands := by
        apply List.mem_cons_of_mem
        exact List.mem_map.mpr ⟨(w, i), by simpa [s] using h, rfl⟩
      have hle := shortest_le_of_mem (cands := cands) hcand
      have hle' : (shortest cands).length ≤ (parse d fuel (s.drop w.length)).length + 1 := by
        simpa only [List.length_cons] using hle
      simpa [cands, s, Nat.add_comm] using hle'

private theorem parse_expand (d : W) : ∀ (fuel : Nat) (s : B),
    s.length ≤ fuel → expand d (parse d fuel s) = some s
  | 0, s, hf => by
      have hlen0 : s.length = 0 := Nat.eq_zero_of_le_zero hf
      have hs : s = [] := by
        cases s with
        | nil => rfl
        | cons x xs => simp at hlen0
      subst s
      rfl
  | fuel + 1, [], hf => rfl
  | fuel + 1, x :: xs, hf => by
      dsimp [parse]
      let s : B := x :: xs
      let cands := ([255, x] ++ parse d fuel xs) ::
        ((choices d s).map (fun (w, i) => i :: parse d fuel (s.drop w.length)))
      have hcNon : cands ≠ [] := by simp [cands]
      have hmem := shortest_mem_of_ne_nil (cands := cands) hcNon
      have hall : ∀ c ∈ cands, expand d c = some (x :: xs) := by
        intro c hc
        simp only [cands, List.mem_cons, List.mem_map] at hc
        rcases hc with hraw | ⟨⟨w, i⟩, hwi, rfl⟩
        · subst c
          rw [show [255, x] ++ parse d fuel xs = 255 :: x :: parse d fuel xs by rfl]
          rw [expand_cons_255]
          have hi := parse_expand d fuel xs (by simp at hf ⊢; omega)
          simp [hi]
        · rcases choices_data hwi with ⟨hwne, hp, hi, hdi⟩
          have hdrop : (s.drop w.length).length ≤ fuel := by
            have hlen := prefix_length_eq hp
            have hlen' : w.length + ((x :: xs).drop w.length).length = (x :: xs).length := by
              simpa [s] using hlen
            have hwpos : 0 < w.length := length_pos_of_ne_nil hwne
            have hxlen : xs.length ≤ fuel := by
              simp only [List.length_cons] at hf
              omega
            have hsdrop : s.drop w.length = (x :: xs).drop w.length := by simp [s]
            rw [hsdrop]
            omega
          have hit := parse_expand d fuel (s.drop w.length) hdrop
          have hi255 : (i == 255) = false := beq_eq_false_iff_ne.mpr (by omega)
          rw [expand_cons_ne255 d i _ hi255]
          rw [hdi, hit]
          congr 1
          simpa [s] using prefix_append_drop hp
      have hres := hall (shortest cands) hmem
      simpa [cands, s] using hres

private theorem expand_encode (d : W) (s : B) : expand d (encode d s) = some s := by
  unfold encode
  exact parse_expand d s.length s (Nat.le_refl _)

private theorem good_parse (d : W) : ∀ (fuel : Nat) (s : B), Good (parse d fuel s)
  | 0, s => by
      simp [parse]
      exact good_literals s
  | fuel + 1, [] => by
      simp [parse]
      exact Good.nil
  | fuel + 1, x :: xs => by
      dsimp [parse]
      let s : B := x :: xs
      let cands := ([255, x] ++ parse d fuel xs) ::
        ((choices d s).map (fun (w, i) => i :: parse d fuel (s.drop w.length)))
      have hcNon : cands ≠ [] := by simp [cands]
      have hmem := shortest_mem_of_ne_nil (cands := cands) hcNon
      have hall : ∀ c ∈ cands, Good c := by
        intro c hc
        simp only [cands, List.mem_cons, List.mem_map] at hc
        rcases hc with hraw | ⟨⟨w, i⟩, hwi, rfl⟩
        · subst c
          rw [show [255, x] ++ parse d fuel xs = 255 :: x :: parse d fuel xs by rfl]
          exact Good.lit (good_parse d fuel xs)
        · rcases choices_data hwi with ⟨hwne, hp, hi, hdi⟩
          exact Good.code hi (good_parse d fuel (s.drop w.length))
      have hres := hall (shortest cands) hmem
      simpa [cands, s] using hres

private theorem parse_minimal_good (d : W) (stream : B) (hgood : Good stream) :
    ∀ (fuel : Nat) (s : B), s.length ≤ fuel → expand d stream = some s →
      (parse d fuel s).length ≤ stream.length := by
  cases hgood with
  | nil =>
      intro fuel s hf he
      simp [expand] at he
      subst s
      cases fuel with
      | zero => exact Nat.le_refl _
      | succ f => exact Nat.le_refl _
  | lit hrest =>
      rename_i x rest
      intro fuel s hf he
      rw [expand_cons_255] at he
      cases hx : expand d rest with
      | none => simp [hx] at he
      | some xs =>
          simp [hx] at he
          subst s
          cases fuel with
          | zero =>
              have hpos : 0 < (x :: xs).length := by simp
              omega
          | succ f =>
              have hxf : xs.length ≤ f := by
                simp only [List.length_cons] at hf
                omega
              have hih := parse_minimal_good d rest hrest f xs hxf hx
              have hle := parse_length_le_raw d f x xs
              simp only [List.length_cons]
              omega
  | code hi hrest =>
      rename_i i rest
      intro fuel s hf he
      have hi255 : (i == 255) = false := beq_eq_false_iff_ne.mpr (by omega)
      rw [expand_cons_ne255 d i rest hi255] at he
      cases hw : d[i]? with
      | none => simp [hw] at he
      | some w =>
          cases hx : expand d rest with
          | none => simp [hw, hx] at he
          | some xs =>
              simp [hw, hx] at he
              by_cases hwe : w = []
              · subst w
                simp at he
                subst s
                have hih := parse_minimal_good d rest hrest fuel xs hf hx
                simp only [List.length_cons]
                omega
              · have hpfx : w.isPrefixOf s := by
                  rw [← he]
                  simp
                have hdrop : s.drop w.length = xs := by
                  rw [← he]
                  simp
                have hlen2 : w.length + xs.length = s.length := by
                  simpa [List.length_append] using congrArg List.length he
                have hxsle : xs.length ≤ fuel := by
                  have hwpos : 0 < w.length := length_pos_of_ne_nil hwe
                  omega
                cases fuel with
                | zero =>
                    have hpos : 0 < s.length := by
                      have hwpos : 0 < w.length := length_pos_of_ne_nil hwe
                      omega
                    omega
                | succ f =>
                    have hihf : xs.length ≤ f := by
                      have hwpos : 0 < w.length := length_pos_of_ne_nil hwe
                      omega
                    have hih := parse_minimal_good d rest hrest f xs hihf hx
                    have hcand : (w, i) ∈ choices d s :=
                      mem_choices_of d s w i hi hw hwe hpfx
                    have hle := parse_length_le_choice d f s w i hcand
                    have hih' : (parse d f (s.drop w.length)).length ≤ rest.length := by
                      rw [hdrop]
                      exact hih
                    simp only [List.length_cons]
                    omega
termination_by stream.length


/-! ## Stream transformations -/

private theorem good_of_expand (d : W) (hd : d.length ≤ 255) (stream : B) :
    ∀ s : B, expand d stream = some s → Good stream := by
  induction hlen : stream.length using Nat.strongRecOn generalizing stream with
  | ind n ih =>
      intro s h
      cases stream with
      | nil => exact Good.nil
      | cons c rest =>
          by_cases hc : c == 255
          · have hceq : c = 255 := beq_iff_eq.mp hc
            subst c
            cases rest with
            | nil => simp [expand] at h
            | cons x xs =>
                rw [expand_cons_255] at h
                cases hx : expand d xs with
                | none => simp [hx] at h
                | some ys =>
                    simp [hx] at h
                    subst s
                    have hlt : xs.length < n := by rw [← hlen]; simp only [List.length_cons]; omega
                    exact Good.lit (ih xs.length hlt xs rfl ys hx)
          · have hcf : (c == 255) = false := Bool.eq_false_of_ne_true hc
            rw [expand_cons_ne255 d c rest hcf] at h
            cases hw : d[c]? with
            | none => simp [hw] at h
            | some w =>
                cases hx : expand d rest with
                | none => simp [hw, hx] at h
                | some ys =>
                    simp [hw, hx] at h
                    have hclen : c < d.length := (List.getElem?_eq_some_iff.mp hw).1
                    have hc255 : c < 255 := by omega
                    subst s
                    have hlt : rest.length < n := by rw [← hlen]; simp only [List.length_cons]; omega
                    exact Good.code hc255 (ih rest.length hlt rest rfl ys hx)

private theorem expand_append_dict (d new : W) (stream : B) (hgood : Good stream) :
    ∀ s : B, expand d stream = some s → expand (d ++ new) stream = some s := by
  revert hgood
  induction hlen : stream.length using Nat.strongRecOn generalizing stream with
  | ind n ih =>
      intro hgood s he
      cases hgood with
      | nil =>
          simpa [expand] using he
      | lit hrest =>
          rename_i x rest
          rw [expand_cons_255] at he ⊢
          cases hx : expand d rest with
          | none => simp [hx] at he
          | some xs =>
              simp [hx] at he
              subst s
              have hlt : rest.length < n := by rw [← hlen]; simp only [List.length_cons]; omega
              have htail := ih rest.length hlt rest rfl hrest xs hx
              simp [htail]
      | code hi hrest =>
          rename_i i rest
          have hi255 : (i == 255) = false := beq_eq_false_iff_ne.mpr (by omega)
          rw [expand_cons_ne255 d i rest hi255] at he
          rw [expand_cons_ne255 (d ++ new) i rest hi255]
          cases hw : d[i]? with
          | none => simp [hw] at he
          | some w =>
              have hilen : i < d.length := (List.getElem?_eq_some_iff.mp hw).1
              have hget : (d ++ new)[i]? = some w := by simpa [getElem_append_left d new i hilen] using hw
              rw [hget]
              cases hx : expand d rest with
              | none => simp [hw, hx] at he
              | some xs =>
                  simp [hw, hx] at he
                  subst s
                  have hlt : rest.length < n := by rw [← hlen]; simp only [List.length_cons]; omega
                  have htail := ih rest.length hlt rest rfl hrest xs hx
                  simp [htail]

private theorem getElem_idxOf_eq_some (l : W) (w : B) (hw : w ∈ l) :
    l[l.idxOf w]? = some w := by
  have hlt := List.idxOf_lt_length_of_mem hw
  exact (List.getElem?_eq_some_iff).mpr ⟨hlt, List.getElem_idxOf hlt⟩

private theorem remap_stream (d e : W) (f : Nat → Nat)
    (hf : ∀ i, i < d.length → e[f i]? = d[i]? ∧ f i < 255)
    (stream : B) (hgood : Good stream) :
    ∀ s : B, expand d stream = some s →
      ∃ stream' : B, Good stream' ∧ stream'.length = stream.length ∧
        expand e stream' = some s := by
  revert hgood
  induction hlen : stream.length using Nat.strongRecOn generalizing stream with
  | ind n ih =>
      intro hgood s he
      cases hgood with
      | nil =>
          exact ⟨[], Good.nil, by simp [hlen], by simpa [expand] using he⟩
      | lit hrest =>
          rename_i x rest
          rw [expand_cons_255] at he
          cases hx : expand d rest with
          | none => simp [hx] at he
          | some xs =>
              simp [hx] at he
              subst s
              have hlt : rest.length < n := by rw [← hlen]; simp only [List.length_cons]; omega
              rcases ih rest.length hlt rest rfl hrest xs hx with ⟨t, ht, hlenT, heT⟩
              refine ⟨255 :: x :: t, Good.lit ht, by simpa [hlenT] using hlen, ?_⟩
              rw [expand_cons_255]
              simp [heT]
      | code hi hrest =>
          rename_i i rest
          have hi255 : (i == 255) = false := beq_eq_false_iff_ne.mpr (by omega)
          rw [expand_cons_ne255 d i rest hi255] at he
          cases hw : d[i]? with
          | none => simp [hw] at he
          | some w =>
              cases hx : expand d rest with
              | none => simp [hw, hx] at he
              | some xs =>
                  simp [hw, hx] at he
                  have hilen : i < d.length := (List.getElem?_eq_some_iff.mp hw).1
                  have hfi := hf i hilen
                  have hfiw : e[f i]? = some w := by simpa [hw] using hfi.1
                  have hlt : rest.length < n := by rw [← hlen]; simp only [List.length_cons]; omega
                  rcases ih rest.length hlt rest rfl hrest xs hx with ⟨t, ht, hlenT, heT⟩
                  refine ⟨f i :: t, Good.code hfi.2 ht, by simpa [hlenT] using hlen, ?_⟩
                  have hf255 : (f i == 255) = false := beq_eq_false_iff_ne.mpr (by omega)
                  rw [expand_cons_ne255 e (f i) t hf255]
                  rw [hfiw, heT]
                  simpa using he

/-! ## Numeric wire helpers -/

private theorem width_mono {n m : Nat} (h : n ≤ m) : width n ≤ width m := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · have hm : m ≠ 0 := by omega
    have hlog : n.log2 ≤ m.log2 := by
      apply (Nat.le_log2 hm).mpr
      exact Nat.le_trans (Nat.log2_self_le hn) h
    simp [width, hn, hm]
    omega

private theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  unfold ubytes
  have hw := width_mono h
  have hd : (width n + 6) / 7 ≤ (width m + 6) / 7 := Nat.div_le_div_right (by omega)
  exact Nat.max_le.mpr ⟨Nat.le_max_left 1 _, Nat.le_trans hd (Nat.le_max_right 1 _)⟩

private theorem uleb_bound : ∀ n : Nat, n < 128 ^ (uleb n).length := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
      · rw [uleb]
        simp [hlt]
        have hdivlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        have hrec := ih (n / 128) hdivlt
        rw [Nat.pow_succ]
        have hc' : n = (n / 128) * 128 + n % 128 := by
          rw [Nat.mul_comm]
          exact (Nat.div_add_mod n 128).symm
        have hmul : (n / 128) * 128 < 128 ^ (uleb (n / 128)).length * 128 :=
          Nat.mul_lt_mul_of_pos_right hrec (by decide)
        have hmod : n % 128 < 128 := Nat.mod_lt _ (by decide)
        omega

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  by_cases hlt : n < 128
  · rw [uleb]
    simp [hlt]
  · rw [uleb]
    simp [hlt]

private theorem uleb_len_le_of_bound : ∀ (n k : Nat), 0 < k → n < 128 ^ k → (uleb n).length ≤ k := by
  intro n k
  induction k generalizing n with
  | zero => intro hk; omega
  | succ k ih =>
      intro hk hnk
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
      · rw [uleb]
        simp [hlt]
        have hdiv : n / 128 < 128 ^ k := by
          apply Nat.div_lt_of_lt_mul
          simpa [Nat.pow_succ, Nat.mul_comm] using hnk
        have hkpos : 0 < k := by
          by_cases hk0 : k = 0
          · subst k
            simp at hnk
            omega
          · omega
        have hrec := ih (n / 128) hkpos hdiv
        omega

private theorem uleb_len_mono {n m : Nat} (h : n ≤ m) : (uleb n).length ≤ (uleb m).length :=
  uleb_len_le_of_bound n (uleb m).length (uleb_len_pos m) (Nat.lt_of_le_of_lt h (uleb_bound m))


/-! ## Dictionary cost helpers -/

private def wlengths (d : W) : B := TinylambOpt.ResearchGoals.Compression.Text.lengths d

private theorem wlengths_append (d new : W) : wlengths (d ++ new) = wlengths d ++ wlengths new := by
  unfold wlengths TinylambOpt.ResearchGoals.Compression.Text.lengths
  simp [List.flatMap_append]

private theorem wlengths_cons_length (a : B) (as : W) :
    (wlengths (a :: as)).length = (uleb a.length ++ a).length + (wlengths as).length := by
  simp [wlengths, TinylambOpt.ResearchGoals.Compression.Text.lengths, List.length_append]
  omega

private theorem wlengths_sublist_le {l₁ l₂ : W} (h : l₁.Sublist l₂) :
    (wlengths l₁).length ≤ (wlengths l₂).length := by
  induction h with
  | slnil => simp [wlengths, TinylambOpt.ResearchGoals.Compression.Text.lengths]
  | cons a h ih =>
      simp only [wlengths_cons_length]
      omega
  | cons_cons a h ih =>
      rw [wlengths_cons_length, wlengths_cons_length]
      exact Nat.add_le_add_left ih _

private theorem eraseDups_sublist (l : W) : l.eraseDups.Sublist l := by
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons a as =>
          rw [List.eraseDups_cons]
          have hle : (as.filter (fun b : B => !b == a)).length ≤ as.length :=
            List.length_filter_le _ _
          have hlt : (as.filter (fun b : B => !b == a)).length < n := by
            rw [← hlen]
            simp
            omega
          have hsub := ih (as.filter (fun b : B => !b == a)).length hlt
            (as.filter (fun b : B => !b == a)) rfl
          exact (List.Sublist.trans hsub List.filter_sublist).cons_cons a

private theorem tableBytes_append_le (d new : W) : tableBytes d ≤ tableBytes (d ++ new) := by
  change ubytes d.length + (wlengths d).length ≤ ubytes (d ++ new).length + (wlengths (d ++ new)).length
  rw [wlengths_append]
  simp only [List.length_append]
  have hu : ubytes d.length ≤ ubytes (d.length + new.length) := ubytes_mono (by omega)
  omega

private theorem wlengths_filter_le : ∀ (p : B → Bool) (l : W),
    (wlengths (l.filter p)).length ≤ (wlengths l).length := by
  intro p l
  induction l with
  | nil => simp [wlengths, TinylambOpt.ResearchGoals.Compression.Text.lengths]
  | cons a as ih =>
      by_cases hp : p a = true
      · simp [hp]
        rw [wlengths_cons_length, wlengths_cons_length]
        omega
      · have hp' : p a = false := Bool.eq_false_of_ne_true hp
        simp [hp']
        rw [wlengths_cons_length]
        omega

private theorem wlengths_eraseDups_le (d : W) :
    (wlengths d.eraseDups).length ≤ (wlengths d).length :=
  wlengths_sublist_le (eraseDups_sublist d)

private theorem length_eraseDups_le (d : W) : d.eraseDups.length ≤ d.length :=
  (eraseDups_sublist d).length_le

private theorem tableBytes_eraseDups_le (d : W) : tableBytes d.eraseDups ≤ tableBytes d := by
  change ubytes d.eraseDups.length + (wlengths d.eraseDups).length ≤ ubytes d.length + (wlengths d).length
  have hu : ubytes d.eraseDups.length ≤ ubytes d.length := ubytes_mono (length_eraseDups_le d)
  have hl := wlengths_eraseDups_le d
  omega

private theorem perm_map_sum {α : Type} (f : α → Nat) {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp [Nat.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem wlengths_length_eq_of_perm {d e : W} (p : d.Perm e) :
    (wlengths d).length = (wlengths e).length := by
  unfold wlengths TinylambOpt.ResearchGoals.Compression.Text.lengths
  simp only [List.length_flatMap]
  exact perm_map_sum (fun w : B => (TinylambOpt.ResearchGoals.Compression.Wire.uleb w.length ++ w).length) p

private theorem tableBytes_eq_of_perm {d e : W} (p : d.Perm e) : tableBytes d = tableBytes e := by
  change ubytes d.length + (wlengths d).length = ubytes e.length + (wlengths e).length
  have hl := p.length_eq
  have hw := wlengths_length_eq_of_perm p
  have hu : ubytes d.length = ubytes e.length := by rw [hl]
  omega

private theorem rowFrame_len_le_append (d new : W) (s : B) :
    (rowFrame (d ++ new) s).length ≤ (rowFrame d s).length := by
  unfold rowFrame
  rw [List.length_append, List.length_append]
  have he1 := expand_encode d s
  have he2 := expand_append_dict d new (encode d s) (good_parse d s.length s) s he1
  have hmin := parse_minimal_good (d ++ new) (encode d s) (good_parse d s.length s)
    s.length s (Nat.le_refl _) he2
  unfold encode at hmin
  have hlen : (encode (d ++ new) s).length ≤ (encode d s).length := hmin
  have huleb := uleb_len_mono hlen
  omega


/-! ## readU, length ceilings and reversal -/

private theorem readU_uleb_aux (n fuel : Nat) (tail : B) (hf : n < 128 ^ fuel) :
    readU (uleb n ++ tail) = some (n, tail) := by
  induction fuel generalizing n tail with
  | zero =>
      have : n = 0 := by omega
      subst this
      rw [uleb]
      simp [readU]
  | succ fuel ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt, readU]
      · have hdiv : n / 128 < 128 ^ fuel := by
          rw [Nat.div_lt_iff_lt_mul (by omega)]
          simpa [Nat.pow_succ, Nat.mul_comm] using hf
        have hrec := ih (n / 128) tail hdiv
        rw [uleb]
        simp only [hlt, ite_false, List.cons_append, readU]
        have hnotlt : ¬ (128 + n % 128 < 128) := by omega
        have hmod : 128 + n % 128 < 256 := by omega
        simp only [hnotlt, ite_false, hmod, ite_true]
        rw [hrec]
        simp only [Option.map_some, Option.some.injEq]
        refine Prod.ext ?_ ?_
        · simp
          omega
        · rfl

private theorem readU_uleb (n : Nat) (tail : B) : readU (uleb n ++ tail) = some (n, tail) := by
  have hf : n < 128 ^ (n + 1) := by
    have h2 : n < 128 ^ n := by
      calc n < 2 ^ n := Nat.lt_two_pow_self
        _ ≤ 128 ^ n := Nat.pow_le_pow_left (by omega) _
    calc n < 128 ^ n := h2
      _ ≤ 128 ^ n * 128 := Nat.le_mul_of_pos_right _ (by omega)
      _ = 128 ^ (n + 1) := by rw [Nat.pow_succ]
  exact readU_uleb_aux n (n + 1) tail hf

private theorem literals_length : ∀ s : B, (literals s).length = 2 * s.length := by
  intro s
  induction s with
  | nil => rfl
  | cons x xs ih =>
      rw [show literals (x :: xs) = 255 :: x :: literals xs by rfl]
      simp only [List.length_cons]
      rw [ih]
      omega

private theorem parse_length_le_twice (d : W) : ∀ (fuel : Nat) (s : B),
    (parse d fuel s).length ≤ 2 * s.length := by
  intro fuel
  induction fuel with
  | zero =>
      intro s
      change (literals s).length ≤ 2 * s.length
      rw [literals_length]
      omega
  | succ fuel ih =>
      intro s
      cases s with
      | nil => simp [parse]
      | cons x xs =>
          have hle := parse_length_le_raw d fuel x xs
          have hih := ih xs
          simp only [List.length_cons] at hle ⊢
          omega

private theorem len_le_peak_of_mem {l : W} {w : B} (hw : w ∈ l) :
    w.length ≤ peak (l.map List.length) := by
  unfold peak
  have hwmem : w.length ∈ l.map List.length := List.mem_map.mpr ⟨w, hw, rfl⟩
  cases hmax : (l.map List.length).max? with
  | none =>
      have hnil : l.map List.length = [] := List.max?_eq_none_iff.mp hmax
      rw [hnil] at hwmem
      simp at hwmem
  | some m =>
      have hle : ∀ b ∈ l.map List.length, b ≤ m := (List.max?_le_iff hmax).1 (Nat.le_refl _)
      have hb := hle w.length hwmem
      simpa [hmax] using hb

private theorem add_one_le_mul_succ (a b P : Nat) (hP : 1 ≤ P) (h : a ≤ b * P) :
    a + 1 ≤ (b + 1) * P := by
  rw [Nat.add_mul]
  omega

private theorem add_le_mul_succ (a c b P : Nat) (h : a ≤ b * P) (hc : c ≤ P) :
    a + c ≤ (b + 1) * P := by
  rw [Nat.add_mul]
  omega

private theorem parse_floor (d : W) : ∀ (fuel : Nat) (s : B),
    s.length ≤ (parse d fuel s).length * max 1 (peak ((d.take 255).map List.length)) := by
  let P := max 1 (peak ((d.take 255).map List.length))
  have hP : 1 ≤ P := Nat.le_max_left _ _
  intro fuel
  induction fuel with
  | zero =>
      intro s
      rw [show parse d 0 s = literals s by rfl, literals_length]
      have hle1 : s.length ≤ 2 * s.length := by omega
      have hle2 : 2 * s.length ≤ (2 * s.length) * P := Nat.le_mul_of_pos_right _ hP
      exact Nat.le_trans hle1 hle2
  | succ fuel ih =>
      intro s
      cases s with
      | nil => simp [parse]
      | cons x xs =>
          dsimp [parse]
          let s : B := x :: xs
          let cands := ([255, x] ++ parse d fuel xs) ::
            ((choices d s).map (fun (w, i) => i :: parse d fuel (s.drop w.length)))
          have hcNon : cands ≠ [] := by simp [cands]
          have hmem := shortest_mem_of_ne_nil (cands := cands) hcNon
          have hall : ∀ c ∈ cands, (x :: xs).length ≤ c.length * P := by
            intro c hc
            simp only [cands, List.mem_cons, List.mem_map] at hc
            rcases hc with hraw | ⟨⟨w, i⟩, hwi, rfl⟩
            · subst c
              have hih := ih xs
              dsimp [P] at hih ⊢
              let Q := max 1 (peak ((d.take 255).map List.length))
              have hih' : xs.length ≤ (List.length (parse d fuel xs) + 1) * Q := by
                have : xs.length ≤ (List.length (parse d fuel xs)) * Q := by simpa [Q] using hih
                exact Nat.le_trans this (Nat.mul_le_mul_right Q (by omega))
              have hstep := add_one_le_mul_succ xs.length (List.length (parse d fuel xs) + 1) Q (by dsimp [Q]; omega) hih'
              dsimp [Q] at hstep
              omega
            · rcases choices_data hwi with ⟨hwne, hp, hi, hdi⟩
              have hz : (w, i) ∈ (d.take 255).zipIdx := by
                unfold choices at hwi
                exact (List.mem_filter.mp hwi).1
              have hget : (d.take 255)[i]? = some w := List.mk_mem_zipIdx_iff_getElem?.mp hz
              rcases List.getElem?_eq_some_iff.mp hget with ⟨hwpos, hwidx⟩
              have hwmem : w ∈ d.take 255 := by
                rw [← hwidx]
                exact List.getElem_mem hwpos
              have hwpeak : w.length ≤ peak ((d.take 255).map List.length) := len_le_peak_of_mem hwmem
              have hwP : w.length ≤ P := Nat.le_trans hwpeak (Nat.le_max_right _ _)
              have hlen := prefix_length_eq hp
              have hlen' : w.length + (s.drop w.length).length = xs.length + 1 := by
                simpa [s] using hlen
              have hih := ih (s.drop w.length)
              dsimp [P] at hih hwP ⊢
              have hstep := add_le_mul_succ (s.drop w.length).length w.length
                (List.length (parse d fuel (s.drop w.length))) P hih hwP
              dsimp [P] at hstep
              rw [← hlen']
              simpa [Nat.add_comm] using hstep
          have hres := hall (shortest cands) hmem
          simpa [cands, s, P] using hres

private def revStream : B → B
  | [] => []
  | 255 :: x :: rest => revStream rest ++ [255, x]
  | c :: rest => revStream rest ++ [c]

private theorem revStream_cons_255 (x : Nat) (rest : B) :
    revStream (255 :: x :: rest) = revStream rest ++ [255, x] := by
  rfl

private theorem revStream_cons_of_ne (c : Nat) (rest : B) (hc : (c == 255) = false) :
    revStream (c :: rest) = revStream rest ++ [c] := by
  cases rest with
  | nil => simp [revStream]
  | cons x xs =>
      have hne : c ≠ 255 := by
        intro hEq
        subst c
        simp at hc
      simp [revStream, hne]

private theorem revStream_length (s : B) : (revStream s).length = s.length := by
  induction hlen : s.length using Nat.strongRecOn generalizing s with
  | ind n ih =>
      cases s with
      | nil => simpa [revStream] using hlen
      | cons c rest =>
          by_cases hc : c == 255
          · cases rest with
            | nil =>
                have hceq : c = 255 := beq_iff_eq.mp hc
                subst c
                simpa [revStream] using hlen
            | cons x xs =>
                have hceq : c = 255 := beq_iff_eq.mp hc
                subst c
                rw [revStream_cons_255]
                have hlt : xs.length < n := by
                  rw [← hlen]
                  simp
                  omega
                have hrec := ih xs.length hlt xs rfl
                rw [List.length_append, hrec]
                simpa using hlen
          · rw [revStream_cons_of_ne c rest (Bool.eq_false_of_ne_true hc)]
            have hlt : rest.length < n := by
              rw [← hlen]
              simp
            have hrec := ih rest.length hlt rest rfl
            rw [List.length_append, hrec]
            simpa using hlen

private theorem good_revStream : ∀ (s : B), Good s → Good (revStream s) := by
  intro s h
  induction h with
  | nil => change Good []; exact Good.nil
  | lit h ih =>
      rename_i x rest
      rw [show revStream (255 :: x :: rest) = revStream rest ++ [255, x] by rfl]
      exact good_append ih [255, x] (Good.lit Good.nil)
  | code hi h ih =>
      rename_i i rest
      rw [revStream_cons_of_ne i rest (beq_eq_false_iff_ne.mpr (by omega))]
      exact good_append ih [i] (Good.code hi Good.nil)

private theorem expand_revStream (d : W) : ∀ (s stream : B), Good stream →
    expand d stream = some s → expand (d.map List.reverse) (revStream stream) = some s.reverse := by
  intro s stream hgood
  induction hgood generalizing s with
  | nil =>
      intro he
      simp [revStream, expand] at he ⊢
      subst s
      rfl
  | lit h ih =>
      rename_i x rest
      intro he
      rw [expand_cons_255] at he
      cases hx : expand d rest with
      | none => simp [hx] at he
      | some xs =>
          simp [hx] at he
          subst s
          have hrec := expand_append_good (d.map List.reverse) (good_revStream rest h) [255, x]
          have hih := ih xs hx
          rw [show revStream (255 :: x :: rest) = revStream rest ++ [255, x] by rfl]
          change expand (d.map List.reverse) (revStream rest ++ [255, x]) = some (x :: xs).reverse
          rw [hrec, hih]
          simp [expand, List.reverse_cons]
  | code hi h ih =>
      rename_i i rest
      intro he
      have hi255 : (i == 255) = false := beq_eq_false_iff_ne.mpr (by omega)
      rw [expand_cons_ne255 d i rest hi255] at he
      cases hw : d[i]? with
      | none => simp [hw] at he
      | some w =>
          cases hx : expand d rest with
          | none => simp [hw, hx] at he
          | some xs =>
              simp [hw, hx] at he
              have hrec := expand_append_good (d.map List.reverse) (good_revStream rest h) [i]
              have hih := ih xs hx
              rw [revStream_cons_of_ne i rest hi255]
              rw [← he]
              change expand (d.map List.reverse) (revStream rest ++ [i]) = some (w ++ xs).reverse
              rw [hrec, hih]
              rw [expand_cons_ne255 (d.map List.reverse) i [] hi255]
              simp [hw, List.reverse_append, expand]

private theorem rev_length_le (d : W) (s : B) :
    (encode (d.map List.reverse) s.reverse).length ≤ (encode d s).length := by
  let st := encode d s
  have hgood := good_parse d s.length s
  have he := expand_encode d s
  have hrg := good_revStream st hgood
  have hre := expand_revStream d s st hgood he
  have hmin := parse_minimal_good (d.map List.reverse) (revStream st) hrg
    s.reverse.length s.reverse (Nat.le_refl _) hre
  change (encode (d.map List.reverse) s.reverse).length ≤ (revStream st).length at hmin
  simpa [st, revStream_length] using hmin


/-! ## Theorems -/

private theorem sum_le_sum_of_forall (f g : B → Nat) : ∀ l : W,
    (∀ s ∈ l, f s ≤ g s) → (l.map f).sum ≤ (l.map g).sum := by
  intro l
  induction l with
  | nil => intro h; simp
  | cons a as ih =>
      intro h
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have htail := ih (by intro x hx; exact h x (by simp [hx]))
      omega

theorem sound_OptimalInverse : TinylambOpt.ResearchGoals.Compression.PhraseTables.OptimalInverse.Goal := by
  intro d s hs
  exact expand_encode d s

theorem sound_OptimalParse : TinylambOpt.ResearchGoals.Compression.PhraseTables.OptimalParse.Goal := by
  intro d s stream hd hs hexp
  have hg := good_of_expand d hd stream s hexp
  exact parse_minimal_good d stream hg s.length s (Nat.le_refl _) hexp

theorem sound_TableExtension : TinylambOpt.ResearchGoals.Compression.PhraseTables.TableExtension.Goal := by
  intro d new s hlen
  have hd : d.length ≤ 255 := by
    have : d.length ≤ d.length + new.length := Nat.le_add_right _ _
    simpa [List.length_append] using Nat.le_trans this hlen
  have hdn : (d ++ new).length ≤ 255 := by simpa [List.length_append] using hlen
  have hg := good_parse d s.length s
  have he1 := expand_encode d s
  have he2 := expand_append_dict d new (encode d s) hg s he1
  exact parse_minimal_good (d ++ new) (encode d s) hg s.length s (Nat.le_refl _) he2

theorem sound_AliasPrune : TinylambOpt.ResearchGoals.Compression.PhraseTables.AliasPrune.Goal := by
  intro d ws hd
  let e : W := d.eraseDups
  let f : Nat → Nat := fun i => e.idxOf (d[i]?.getD [])
  have heLen : e.length ≤ 255 := by
    simpa [e] using Nat.le_trans (length_eraseDups_le d) hd
  have hf : ∀ i, i < d.length → e[f i]? = d[i]? ∧ f i < 255 := by
    intro i hi
    let w : B := d[i]'hi
    have hdi : d[i]? = some w := by simp [w]
    have hwmem : w ∈ d := by
      dsimp [w]
      exact List.getElem_mem hi
    have hwmemE : w ∈ e := by simpa [e] using hwmem
    have hlt : f i < e.length := by
      dsimp [f]
      simpa [w, hdi] using List.idxOf_lt_length_of_mem hwmemE
    have hget : e[f i]? = some w := by
      dsimp [f]
      have hgetd : d[i]?.getD [] = w := by simp [hdi]
      rw [hgetd]
      exact getElem_idxOf_eq_some e w hwmemE
    constructor
    · simpa [hdi] using hget
    · exact Nat.lt_of_lt_of_le hlt heLen
  have hrow : ∀ s ∈ ws, (rowFrame e s).length ≤ (rowFrame d s).length := by
    intro s hs
    have hg := good_parse d s.length s
    have he := expand_encode d s
    rcases remap_stream d e f hf (encode d s) hg s he with ⟨t, ht, hlenT, heT⟩
    have hmin := parse_minimal_good e t ht s.length s (Nat.le_refl _) heT
    change (encode e s).length ≤ t.length at hmin
    have hlenE : (encode e s).length ≤ (encode d s).length := by
      simpa [hlenT] using hmin
    unfold rowFrame
    rw [List.length_append, List.length_append]
    have hu := uleb_len_mono hlenE
    omega
  unfold charge
  have htbl := tableBytes_eraseDups_le d
  have hsum := sum_le_sum_of_forall (fun s : B => (rowFrame e s).length)
    (fun s : B => (rowFrame d s).length) ws hrow
  change tableBytes e + (ws.map (fun s => (rowFrame e s).length)).sum ≤
    tableBytes d + (ws.map (fun s => (rowFrame d s).length)).sum
  exact Nat.add_le_add htbl hsum

theorem sound_ReorderTable : TinylambOpt.ResearchGoals.Compression.PhraseTables.ReorderTable.Goal := by
  intro d e ws hp hd
  let f : Nat → Nat := fun i => e.idxOf (d[i]?.getD [])
  have heLen : e.length ≤ 255 := by rw [← hp.length_eq]; exact hd
  have hf : ∀ i, i < d.length → e[f i]? = d[i]? ∧ f i < 255 := by
    intro i hi
    let w : B := d[i]'hi
    have hdi : d[i]? = some w := by simp [w]
    have hwmem : w ∈ d := by
      dsimp [w]
      exact List.getElem_mem hi
    have hwmemE : w ∈ e := (hp.mem_iff).mp hwmem
    have hlt : f i < e.length := by
      dsimp [f]
      simpa [w, hdi] using List.idxOf_lt_length_of_mem hwmemE
    have hget : e[f i]? = some w := by
      dsimp [f]
      have hgetd : d[i]?.getD [] = w := by simp [hdi]
      rw [hgetd]
      exact getElem_idxOf_eq_some e w hwmemE
    constructor
    · simpa [hdi] using hget
    · exact Nat.lt_of_lt_of_le hlt heLen
  have hrow : ∀ s ∈ ws, (rowFrame e s).length = (rowFrame d s).length := by
    intro s hs
    have hg := good_parse d s.length s
    have he := expand_encode d s
    rcases remap_stream d e f hf (encode d s) hg s he with ⟨t, ht, hlenT, heT⟩
    have hmin := parse_minimal_good e t ht s.length s (Nat.le_refl _) heT
    change (encode e s).length ≤ t.length at hmin
    have hle1 : (encode e s).length ≤ (encode d s).length := by simpa [hlenT] using hmin
    have phe : e.Perm d := hp.symm
    let g : Nat → Nat := fun i => d.idxOf (e[i]?.getD [])
    have hgf : ∀ i, i < e.length → d[g i]? = e[i]? ∧ g i < 255 := by
      intro i hi
      let w : B := e[i]'hi
      have hei : e[i]? = some w := by simp [w]
      have hwmem : w ∈ e := by dsimp [w]; exact List.getElem_mem hi
      have hwmemD : w ∈ d := (phe.mem_iff).mp hwmem
      have hlt : g i < d.length := by
        dsimp [g]
        simpa [w, hei] using List.idxOf_lt_length_of_mem hwmemD
      have hget : d[g i]? = some w := by
        dsimp [g]
        have hgete : e[i]?.getD [] = w := by simp [hei]
        rw [hgete]
        exact getElem_idxOf_eq_some d w hwmemD
      exact ⟨by simpa [hei] using hget, Nat.lt_of_lt_of_le hlt hd⟩
    have hgGood := good_parse e s.length s
    have heE := expand_encode e s
    rcases remap_stream e d g hgf (encode e s) hgGood s heE with ⟨u, huG, hlenU, heU⟩
    have hmin2 := parse_minimal_good d u huG s.length s (Nat.le_refl _) heU
    change (encode d s).length ≤ u.length at hmin2
    have hle2 : (encode d s).length ≤ (encode e s).length := by simpa [hlenU] using hmin2
    have hlenEq : (encode e s).length = (encode d s).length := Nat.le_antisymm hle1 hle2
    unfold rowFrame
    rw [List.length_append, List.length_append]
    have hu := congrArg uleb hlenEq
    have huleb : (uleb (encode e s).length).length = (uleb (encode d s).length).length :=
      congrArg List.length hu
    omega
  unfold charge
  have htbl := tableBytes_eq_of_perm hp
  have hsum : (ws.map (fun s => (rowFrame d s).length)).sum =
      (ws.map (fun s => (rowFrame e s).length)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro s hs
    exact (hrow s hs).symm
  rw [htbl, hsum]

private theorem charge_concat (d x y : W) :
    charge d (x ++ y) + tableBytes d = charge d x + charge d y := by
  unfold charge
  simp [List.map_append, List.sum_append]
  omega

theorem sound_ExternalTable : TinylambOpt.ResearchGoals.Compression.PhraseTables.ExternalTable.Goal := by
  intro d blocks hne
  induction blocks with
  | nil => exact (hne rfl).elim
  | cons b bs ih =>
      cases bs with
      | nil => simp [charge]
      | cons b' bs' =>
          have ih' := ih (by simp)
          have hcat := charge_concat d b (b' :: bs').flatten
          simp only [List.map_cons, List.sum_cons] at ih' ⊢
          rw [ih']
          simp only [List.flatten_cons] at hcat ih' ⊢
          have hlen : (b :: b' :: bs').length - 1 = (b' :: bs').length := by simp
          rw [hlen]
          have hmul : ((b' :: bs').length - 1) * tableBytes d + tableBytes d =
              (b' :: bs').length * tableBytes d := by
            obtain ⟨n, hn⟩ : ∃ n, (b' :: bs').length = n + 1 := by
              refine ⟨(b' :: bs').length - 1, ?_⟩
              have hpos : 0 < (b' :: bs').length := by simp
              omega
            rw [hn]
            simp [Nat.add_mul]
          rw [← Nat.add_assoc]
          rw [← hcat]
          rw [Nat.add_assoc]
          rw [Nat.add_comm (tableBytes d) (((b' :: bs').length - 1) * tableBytes d)]
          rw [hmul]


theorem sound_RowBoundary : TinylambOpt.ResearchGoals.Compression.PhraseTables.RowBoundary.Goal := by
  intro d w tail
  unfold rowFrame
  simpa [List.append_assoc] using readU_uleb (encode d w).length (encode d w ++ tail)

theorem sound_LiteralCeiling : TinylambOpt.ResearchGoals.Compression.PhraseTables.LiteralCeiling.Goal := by
  intro d s
  exact parse_length_le_twice d s.length s

theorem sound_TokenFloor : TinylambOpt.ResearchGoals.Compression.PhraseTables.TokenFloor.Goal := by
  intro d s
  exact parse_floor d s.length s

theorem sound_JoinFragments : TinylambOpt.ResearchGoals.Compression.PhraseTables.JoinFragments.Goal := by
  intro d a b
  have ha := expand_encode d a
  have hb := expand_encode d b
  have hab : expand d (encode d a ++ encode d b) = some (a ++ b) := by
    unfold encode at ha ⊢
    have hb' : expand d (parse d b.length b) = some b := by simpa [encode] using hb
    rw [expand_append_good d (good_parse d a.length a) (parse d b.length b)]
    simp [ha, hb']
  have hg := good_append (good_parse d a.length a) (parse d b.length b) (good_parse d b.length b)
  have hmin := parse_minimal_good d (parse d a.length a ++ parse d b.length b) hg
    (a ++ b).length (a ++ b) (Nat.le_refl _) hab
  unfold encode
  simpa [List.length_append] using hmin

theorem sound_ReverseParsing : TinylambOpt.ResearchGoals.Compression.PhraseTables.ReverseParsing.Goal := by
  intro d s
  have h1 := rev_length_le d s
  have h2 := rev_length_le (d.map List.reverse) s.reverse
  have h2' : (encode d s).length ≤ (encode (d.map List.reverse) s.reverse).length := by
    simpa [Function.comp_def] using h2
  exact Nat.le_antisymm h1 h2'

-- Greedy longest-match parsing is not optimal: with dictionary [1,2],[1],[2,3,4]
-- and input [1,2,3,4] greedy emits 5 bytes ([0,255,3,255,4]) while the DP
-- emits 2 bytes ([1,2] = [1],[2,3,4]).
theorem refutation_LongestIsOptimal :
    ¬ TinylambOpt.ResearchGoals.Compression.PhraseTables.LongestIsOptimal.Goal := by
  intro h
  have hh := h ([[1, 2], [1], [2, 3, 4]] : W) [1, 2, 3, 4]
  have hc : ¬ (greedy ([[1, 2], [1], [2, 3, 4]] : W) 4 [1, 2, 3, 4]).length =
      (encode ([[1, 2], [1], [2, 3, 4]] : W) [1, 2, 3, 4]).length := by
    native_decide
  exact hc hh

-- Training on the empty prefix favors the empty dictionary (charge 1 < 3),
-- but two rows [1,1] then cost 11 under [] versus 9 under [[1]].
theorem refutation_TrainOnPrefix :
    ¬ TinylambOpt.ResearchGoals.Compression.PhraseTables.TrainOnPrefix.Goal := by
  intro h
  have hh := h ([] : W) ([[1]] : W) ([] : W) ([[1, 1], [1, 1]] : W)
  have hpre : charge ([] : W) ([] : W) ≤ charge ([[1]] : W) ([] : W) := by native_decide
  have hc : ¬ charge ([] : W) ([[1, 1], [1, 1]] : W) ≤
      charge ([[1]] : W) ([[1, 1], [1, 1]] : W) := by native_decide
  exact hc (hh hpre)


/-! ## UnusedSuffix -/

private theorem zipIdx_append_one (d : W) (w : B) :
    (d ++ [w]).zipIdx = d.zipIdx ++ [(w, d.length)] := by
  apply List.ext_getElem?
  intro i
  by_cases hi : i < d.length
  · simp [List.getElem?_append, List.getElem?_zipIdx, hi]
  · by_cases h0 : i - d.length = 0
    · have hiEq : i = d.length := by omega
      simp [hiEq]
    · simp [List.getElem?_append, List.getElem?_zipIdx, hi, h0]

private theorem choices_append_eq (d : W) (w : B) (s : B) (hd : d.length < 255)
    (hno : ∀ i < s.length, !(w.isPrefixOf (s.drop i)) = true) :
    choices (d ++ [w]) s = choices d s := by
  unfold choices
  have hdtake : d.take 255 = d := List.take_of_length_le (by omega)
  have htake : (d ++ [w]).take 255 = d ++ [w] := by
    rw [List.take_append, hdtake]
    rw [show List.take (255 - d.length) [w] = [w] by
      apply List.take_of_length_le
      simp
      omega]
  rw [htake, hdtake, zipIdx_append_one]
  have hbad : (!w.isEmpty && w.isPrefixOf s) = false := by
    by_cases hwne : w = []
    · simp [hwne]
    · by_cases hspos : s.length = 0
      · have hs : s = [] := by cases s <;> simp at hspos ⊢
        subst s
        cases w with
        | nil => exact (hwne rfl).elim
        | cons a as => simp
      · have hslenpos : 0 < s.length := by omega
        have hn := hno 0 hslenpos
        simp only [List.drop_zero] at hn
        have hp : (w.isPrefixOf s) = false := Bool.eq_false_of_ne_true (fun ht => by simp [ht] at hn)
        simp [hp]
  simp [hbad]

private theorem parse_append_eq (d : W) (w : B) (s : B) (hd : d.length < 255)
    (hno : ∀ k i, i < (s.drop k).length → !(w.isPrefixOf ((s.drop k).drop i)) = true) :
    ∀ fuel, parse (d ++ [w]) fuel s = parse d fuel s
  | 0 => rfl
  | fuel + 1 => by
      cases s with
      | nil => rfl
      | cons x xs =>
          dsimp [parse]
          have hc0 : choices (d ++ [w]) (x :: xs) = choices d (x :: xs) := by
            apply choices_append_eq d w (x :: xs) hd
            intro i hi
            simpa using hno 0 i hi
          rw [hc0]
          have hnoTail : ∀ k j, j < (xs.drop k).length →
              !(w.isPrefixOf ((xs.drop k).drop j)) = true := by
            intro k j hj
            have hj' : j < ((x :: xs).drop (k + 1)).length := by simpa using hj
            have hres := hno (k + 1) j hj'
            simpa using hres
          have htailRaw := parse_append_eq d w xs hd hnoTail fuel
          have hmap : ((choices d (x :: xs)).map (fun (wu, i) => i :: parse (d ++ [w]) fuel ((x :: xs).drop wu.length))) =
              ((choices d (x :: xs)).map (fun (wu, i) => i :: parse d fuel ((x :: xs).drop wu.length))) := by
            apply List.map_congr_left
            intro p hp
            rcases p with ⟨wu, i⟩
            change i :: parse (d ++ [w]) fuel ((x :: xs).drop wu.length) =
              i :: parse d fuel ((x :: xs).drop wu.length)
            have hno' : ∀ k j, j < (((x :: xs).drop wu.length).drop k).length →
                !(w.isPrefixOf ((((x :: xs).drop wu.length).drop k).drop j)) = true := by
              intro k j hj
              have hj' : j < ((x :: xs).drop (wu.length + k)).length := by
                simpa [List.drop_drop] using hj
              have hres := hno (wu.length + k) j hj'
              simpa [List.drop_drop, Nat.add_assoc] using hres
            have htail := parse_append_eq d w ((x :: xs).drop wu.length) hd hno' fuel
            rw [htail]
          apply congrArg shortest
          rw [htailRaw, hmap]

theorem sound_UnusedSuffix : TinylambOpt.ResearchGoals.Compression.PhraseTables.UnusedSuffix.Goal := by
  intro d w ws hd hno
  have hrow : ∀ s ∈ ws, (rowFrame (d ++ [w]) s).length = (rowFrame d s).length := by
    intro s hs
    have hno' : ∀ k i, i < (s.drop k).length →
        !(w.isPrefixOf ((s.drop k).drop i)) = true := by
      intro k i hi
      have hlen : (s.drop k).length = s.length - k := by simp
      have hiTot : k + i < s.length := by omega
      have hres := hno s hs (k + i) hiTot
      simpa [List.drop_drop, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using hres
    have hpe := congrArg List.length (parse_append_eq d w s hd hno' s.length)
    have hlenEq : (encode (d ++ [w]) s).length = (encode d s).length := by
      simpa [encode] using hpe
    unfold rowFrame
    rw [List.length_append, List.length_append]
    have hu := congrArg uleb hlenEq
    have huleb : (uleb (encode (d ++ [w]) s).length).length =
        (uleb (encode d s).length).length := congrArg List.length hu
    omega
  unfold charge
  have htbl := tableBytes_append_le d [w]
  have hsum : (ws.map (fun s => (rowFrame (d ++ [w]) s).length)).sum =
      (ws.map (fun s => (rowFrame d s).length)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro s hs
    exact hrow s hs
  omega

private theorem sum_sub_eq_sub_sum_of_forall (f g : B → Nat) : ∀ l : W,
    (∀ s ∈ l, g s ≤ f s) →
    (l.map (fun s => f s - g s)).sum = (l.map f).sum - (l.map g).sum := by
  intro l
  induction l with
  | nil => simp
  | cons a as ih =>
      intro h
      simp only [List.map_cons, List.sum_cons]
      have ha := h a (by simp)
      have htail := ih (by intro x hx; exact h x (by simp [hx]))
      have hle : (as.map g).sum ≤ (as.map f).sum := sum_le_sum_of_forall g f as (by
        intro x hx
        exact h x (by simp [hx]))
      omega

private theorem nat_add_lt_iff_sub_lt {a b c d : Nat} (h1 : a ≤ b) (h2 : c ≤ d) :
    b + c < a + d ↔ b - a < d - c := by
  omega

theorem sound_AmortizeSymbol : TinylambOpt.ResearchGoals.Compression.PhraseTables.AmortizeSymbol.Goal := by
  intro d w ws hd
  have hrowle : ∀ s ∈ ws, (rowFrame (d ++ [w]) s).length ≤ (rowFrame d s).length := by
    intro s hs
    exact rowFrame_len_le_append d [w] s
  have htbl := tableBytes_append_le d [w]
  have hsumle := sum_le_sum_of_forall (fun s : B => (rowFrame (d ++ [w]) s).length)
    (fun s : B => (rowFrame d s).length) ws hrowle
  have hdiff := sum_sub_eq_sub_sum_of_forall (fun s : B => (rowFrame d s).length)
    (fun s : B => (rowFrame (d ++ [w]) s).length) ws hrowle
  unfold charge at *
  constructor
  · intro hlt
    rw [hdiff]
    exact (nat_add_lt_iff_sub_lt htbl hsumle).mp hlt
  · intro hlt
    rw [hdiff] at hlt
    exact (nat_add_lt_iff_sub_lt htbl hsumle).mpr hlt

end TinylambOpt.ResearchProofs.Compression.PhraseTables
