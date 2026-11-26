import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

/-!
# SparseOffsetsWithLocalLengthLookup

A stride anchor and the lengths since that anchor identify the exact byte range for an
in-bounds URL. A column reader can fetch a single URL using sparse offsets instead of
storing every absolute offset.
-/


set_option linter.unusedSimpArgs false
set_option linter.deprecated false
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_SparseOffsetsWithLocalLengthLookup

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

theorem flatten_drop_prefix (xs : List (List Nat)) :
    ∀ ys, ((xs ++ ys).flatten).drop ((xs.map List.length).sum) = ys.flatten := by
  induction xs with
  | nil => intro ys; simp
  | cons h t ih =>
    intro ys
    show ((h ++ (t ++ ys).flatten).drop (h.length + (t.map List.length).sum))
        = ys.flatten
    rw [← List.drop_drop, List.drop_append, List.drop_length, Nat.sub_self,
      List.drop_zero, List.nil_append]
    exact ih ys

theorem drop_eq_nil (xs : List α) : ∀ i, xs.drop i = [] → xs.length ≤ i := by
  induction xs with
  | nil => intro i _; simp
  | cons a as ih =>
    intro i h
    cases i with
    | zero => simpa using h
    | succ j =>
      simp only [List.length_cons]
      have := ih j (by simpa using h)
      omega

theorem take_app_len (x y : List α) : (x ++ y).take x.length = x := by
  rw [List.take_append, List.take_length]
  simp

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs.SparseOffsetsWithLocalLengthLookup.Goal := by
  intro rows stride i hstride hi
  intro lengths anchor offset
  have hanch : anchor ≤ i := by
    dsimp only [anchor]
    exact Nat.div_mul_le_self i stride
  have hlt : lengths.take anchor = (rows.take anchor).map List.length := by
    dsimp only [lengths]; rw [List.map_take]
  have hld : lengths.drop anchor = (rows.drop anchor).map List.length := by
    dsimp only [lengths]; rw [List.map_drop]
  have hstep1 : (rows.flatten).drop ((lengths.take anchor).sum)
      = (rows.drop anchor).flatten := by
    rw [hlt]
    have hc : (rows.take anchor ++ rows.drop anchor).flatten = rows.flatten := by
      rw [List.take_append_drop]
    rw [← hc]
    exact flatten_drop_prefix (rows.take anchor) (rows.drop anchor)
  have hstep2 : List.drop
      (List.sum (List.take (i - anchor) (List.drop anchor lengths)))
      (List.flatten (List.drop anchor rows))
      = List.flatten (List.drop (i - anchor) (List.drop anchor rows)) := by
    rw [hld, ← List.map_take]
    have hc : ((rows.drop anchor).take (i - anchor)
        ++ (rows.drop anchor).drop (i - anchor)).flatten
        = (rows.drop anchor).flatten := by
      rw [List.take_append_drop]
    rw [← hc]
    exact flatten_drop_prefix ((rows.drop anchor).take (i - anchor))
      ((rows.drop anchor).drop (i - anchor))
  have hdrop : rows.flatten.drop offset = (rows.drop i).flatten := by
    show (rows.flatten).drop
        ((lengths.take anchor).sum + ((lengths.drop anchor).take (i - anchor)).sum)
        = (rows.drop i).flatten
    rw [← List.drop_drop, hstep1, hstep2, List.drop_drop]
    rw [show anchor + (i - anchor) = i from by omega]
  rw [hdrop]
  cases hrd : rows.drop i with
  | nil =>
    have hlen : rows.length ≤ i := drop_eq_nil rows i hrd
    omega
  | cons r rest =>
    have hv : rows[i]? = some r := by
      have h0 : (rows.drop i)[0]? = some r := by
        rw [hrd]
        simp
      rw [List.getElem?_drop] at h0
      simpa using h0
    have hl : lengths[i]? = some r.length := by
      dsimp only [lengths]
      rw [List.getElem?_map, hv]
      simp
    rw [List.flatten_cons, hv, hl]
    simp only [Option.getD_some]
    rw [take_app_len]

end TinylambOpt.ResearchProofs.QueryRewrites.ColumnCodecs.Goal_SparseOffsetsWithLocalLengthLookup


-- C++ references to this model or its proof.
-- table/rematch_storage_codecs.hpp
