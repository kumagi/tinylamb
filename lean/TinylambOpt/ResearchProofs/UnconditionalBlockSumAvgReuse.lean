import TinylambOpt.ResearchGoals.UnconditionalBlockSumAvgReuse

namespace TinylambOpt.ResearchProofs.UnconditionalBlockSumAvgReuse

open TinylambOpt.ResearchGoals.UnconditionalBlockSumAvgReuse

/-! ## Small list helpers -/

private theorem filter_all_true {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = true) : l.filter p = l := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      have ha : p a = true := h a (by simp)
      have hl : ∀ x ∈ l, p x = true := fun x hx => h x (by simp [hx])
      simp [ha, ih hl]

private theorem filter_all_false {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  rw [List.filter_eq_nil_iff]
  intro x hx hp
  rw [h x hx] at hp
  simp at hp

private theorem foldl_if_filter {α β : Type} (p : α → Bool) (f : β → α → β) :
    ∀ (l : List α) (init : β),
      l.foldl (fun acc x => if p x then f acc x else acc) init = (l.filter p).foldl f init := by
  intro l
  induction l with
  | nil => intro init; rfl
  | cons a l ih =>
      intro init
      cases hp : p a <;> simp [hp, ih]

/-! ## Header reasoning -/

private theorem skip_rows_fail (cutoff : Int) (b : Block) (hb : ValidBlockHeader b)
    (hcut : cutoff < b.minDate) :
    ∀ r ∈ b.rows, datePasses cutoff r.shipdate = false := by
  intro r hr
  cases hrs : r.shipdate with
  | none => simp [datePasses]
  | some d =>
      have hbnd := hb r hr
      simp [hrs] at hbnd
      have hnd : ¬ d ≤ cutoff := by omega
      simp [datePasses, hnd]

private theorem cover_rows_pass (cutoff : Int) (b : Block) (hb : ValidBlockHeader b)
    (hnfalse : b.hasNullDate = false) (hmax : b.maxDate ≤ cutoff) :
    ∀ r ∈ b.rows, datePasses cutoff r.shipdate = true := by
  intro r hr
  cases hrs : r.shipdate with
  | none =>
      have hnull : b.hasNullDate = true := by simpa [hrs] using hb r hr
      rw [hnfalse] at hnull
      simp at hnull
  | some d =>
      have hbnd := hb r hr
      simp [hrs] at hbnd
      have hd : d ≤ cutoff := by omega
      simp [datePasses, hd]

/-! ## Aggregate fold commutes with the independent projections -/

private theorem foldl_stepRow_eq (rs : List Row) (acc : AggOut) :
    rs.foldl stepRow acc =
      { sumQty := rs.foldl (fun a r => match r.qty with | none => a | some q => addOptInt a q) acc.sumQty
        cntQty := acc.cntQty + (rs.filter (fun r => r.qty.isSome)).length
        sumDiscPrice := rs.foldl (fun a r => addOptInt a (r.price * (100 - r.disc))) acc.sumDiscPrice
        sumCharge := rs.foldl (fun a r => addOptInt a ((r.price * (100 - r.disc)) * (100 + r.tax))) acc.sumCharge
        cntStar := acc.cntStar + rs.length } := by
  induction rs generalizing acc with
  | nil => simp
  | cons r rs ih =>
      cases hq : r.qty with
      | none => simp [List.foldl_cons, ih, stepRow, hq] <;> omega
      | some q => simp [List.foldl_cons, ih, stepRow, hq] <;> omega

/-! ## Per-block behaviour -/

private def blockStep (cutoff : Int) (st : ScanResult) (b : Block) : ScanResult :=
  if cutoff < b.minDate then st
  else if !b.hasNullDate && b.maxDate ≤ cutoff then
    { out := b.rows.foldl stepRow st.out, dateChecks := st.dateChecks }
  else
    let out' :=
      b.rows.foldl
        (fun acc r => if datePasses cutoff r.shipdate then stepRow acc r else acc) st.out
    { out := out', dateChecks := st.dateChecks + b.rows.length }

private theorem block_out (cutoff : Int) (b : Block) (hb : ValidBlockHeader b) (st : ScanResult) :
    (blockStep cutoff st b).out =
      (b.rows.filter (fun r => datePasses cutoff r.shipdate)).foldl stepRow st.out := by
  by_cases hskip : cutoff < b.minDate
  · have hf : b.rows.filter (fun r => datePasses cutoff r.shipdate) = [] :=
      filter_all_false _ _ (skip_rows_fail cutoff b hb hskip)
    simp [blockStep, hskip, hf]
  · by_cases hcover : (!b.hasNullDate && b.maxDate ≤ cutoff) = true
    · have hnfalse : b.hasNullDate = false := by
        cases hbn : b.hasNullDate with
        | true => simp [hbn] at hcover
        | false => rfl
      have hmax : b.maxDate ≤ cutoff := by
        simpa [hnfalse] using hcover
      have hf : b.rows.filter (fun r => datePasses cutoff r.shipdate) = b.rows :=
        filter_all_true _ _ (cover_rows_pass cutoff b hb hnfalse hmax)
      simp [blockStep, hskip, hcover, hf]
    · have hif := foldl_if_filter (fun r => datePasses cutoff r.shipdate) stepRow b.rows st.out
      simp [blockStep, hskip, hcover, hif]

/-! ## Whole-scan fold invariants -/

private theorem flatMap_pass (cutoff : Int) :
    ∀ bs : List Block,
      (bs.flatMap Block.rows).filter (fun r => datePasses cutoff r.shipdate) =
        bs.flatMap (fun b => b.rows.filter (fun r => datePasses cutoff r.shipdate)) := by
  intro bs
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [List.flatMap_cons, List.filter_append, ih]

private theorem blocks_fold_inv (cutoff : Int) :
    ∀ (bs : List Block) (st : ScanResult),
      (∀ b ∈ bs, ValidBlockHeader b) →
      (bs.foldl (blockStep cutoff) st).out =
        (bs.flatMap (fun b => b.rows.filter (fun r => datePasses cutoff r.shipdate))).foldl
          stepRow st.out := by
  intro bs
  induction bs with
  | nil => intro st _; simp
  | cons b bs ih =>
      intro st hv
      have hb : ValidBlockHeader b := hv b (by simp)
      have hrest : ∀ x ∈ bs, ValidBlockHeader x := fun x hx => hv x (by simp [hx])
      rw [List.foldl_cons]
      rw [ih (blockStep cutoff st b) hrest]
      rw [block_out cutoff b hb st]
      simp [List.flatMap_cons, List.foldl_append]

private theorem dateChecks_zero (cutoff : Int) :
    ∀ (bs : List Block) (st : ScanResult),
      (∀ b ∈ bs, b.hasNullDate = false ∧ b.maxDate ≤ cutoff) →
      (bs.foldl (blockStep cutoff) st).dateChecks = st.dateChecks := by
  intro bs
  induction bs with
  | nil => intro st _; simp
  | cons b bs ih =>
      intro st hv
      have hb := hv b (by simp)
      have hrest : ∀ x ∈ bs, x.hasNullDate = false ∧ x.maxDate ≤ cutoff :=
        fun x hx => hv x (by simp [hx])
      have hcond : (!b.hasNullDate && b.maxDate ≤ cutoff) = true := by
        simp [hb.1, hb.2]
      have hd : (blockStep cutoff st b).dateChecks = st.dateChecks := by
        by_cases hskip : cutoff < b.minDate
        · simp [blockStep, hskip]
        · simp [blockStep, hskip, hcond]
      rw [List.foldl_cons]
      rw [ih (blockStep cutoff st b) hrest]
      exact hd

/-! ## Main theorem -/

theorem sound : TinylambOpt.ResearchGoals.UnconditionalBlockSumAvgReuse.Goal := by
  intro cutoff bs hval
  dsimp only
  constructor
  · change (bs.foldl (blockStep cutoff) { out := {}, dateChecks := 0 }).out = refScan cutoff bs
    have hinv := blocks_fold_inv cutoff bs { out := {}, dateChecks := 0 } hval
    rw [hinv]
    rw [← flatMap_pass cutoff bs]
    rw [foldl_stepRow_eq]
    simp [refScan]
    rfl
  · intro hcover
    change (bs.foldl (blockStep cutoff) { out := {}, dateChecks := 0 }).dateChecks = 0
    exact dateChecks_zero cutoff bs { out := {}, dateChecks := 0 } hcover

end TinylambOpt.ResearchProofs.UnconditionalBlockSumAvgReuse
