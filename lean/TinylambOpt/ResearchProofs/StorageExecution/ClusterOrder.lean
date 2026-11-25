import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.StorageExecution.ClusterOrder

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! ## Common helpers -/

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  unfold width
  by_cases h : n == 0
  · simp at h; simp [h]
  · simp [h]
    have hn : n ≠ 0 := by intro h0; simp [h0] at h
    have hlog : n.log2 < n.log2 + 1 := Nat.lt_succ_self _
    exact Nat.lt_of_le_of_lt (Nat.log2_self_le hn) (by
      have : 2 ^ (n.log2 + 1) = 2 * 2 ^ n.log2 := by ring
      rw [this]
      have : n < 2 * 2 ^ n.log2 := by
        have h1 : n < 2 ^ (n.log2 + 1) := Nat.lt_log2_self
        simpa [Nat.pow_succ, Nat.mul_comm] using h1
      exact this)

private theorem mem_perm_lt (key : Key) (rs : List Row) {i : Nat}
    (hi : i ∈ perm key rs) : i < rs.length := by
  unfold perm at hi
  simp only [List.mem_map, Prod.exists, exists_and_right, exists_eq_right] at hi
  obtain ⟨r, hr⟩ := hi
  have := List.mem_zipIdx hr.1
  simpa using this

private theorem perm_length (key : Key) (rs : List Row) : (perm key rs).length = rs.length := by
  unfold perm
  simp

private theorem perm_bound (key : Key) (rs : List Row) :
    ∀ x ∈ perm key rs, x < 2 ^ width (rs.length - 1) := by
  intro x hx
  have hxlt : x < rs.length := mem_perm_lt key rs hx
  have : rs.length - 1 < 2 ^ width (rs.length - 1) := lt_two_pow_width (rs.length - 1)
  omega

private theorem pack_bytes_lt (w : Nat) (xs : List Nat) :
    ∀ b ∈ pack w xs, b < 256 := by
  intro b hb
  unfold pack little at hb
  simp only [List.mem_map, List.mem_range] at hb
  obtain ⟨i, _, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide)

private theorem ordinal_eq_get (key : Key) (rs : List Row) (i : Nat)
    (hi : i < rs.length) :
    ordinal key rs i = (perm key rs)[i]?.getD 0 := by
  unfold ordinal side
  set w := width (rs.length - 1)
  set xs := perm key rs
  have hlen : xs.length = rs.length := perm_length key rs
  have hbound : ∀ x ∈ xs, x < 2 ^ w := perm_bound key rs
  have hbs : ∀ b ∈ pack w xs, b < 256 := pack_bytes_lt w xs
  rw [sound_LocalRead (pack w xs) w i hbs]
  have hpi := sound_PackedInverse w xs hbound
  have hmap : (List.range xs.length).map (get (pack w xs) w) = xs := hpi
  have hget : get (pack w xs) w i = xs[i]?.getD 0 := by
    have hi' : i < xs.length := by omega
    have := congrArg (fun l => l[i]?.getD 0) hmap
    simp only [List.getElem?_map, List.getElem?_range, hi', ↓reduceIte, Option.map_some,
      Option.getD_some, List.getElem?_eq_getElem hi', Option.getD_some] at this
    simpa using this
  exact hget

private theorem perm_mem_range (key : Key) (rs : List Row) :
    ∀ i ∈ perm key rs, i < rs.length := mem_perm_lt key rs

private theorem physical_length (key : Key) (rs : List Row) :
    (physical key rs).length = rs.length := by
  unfold physical
  simp [perm_length]

