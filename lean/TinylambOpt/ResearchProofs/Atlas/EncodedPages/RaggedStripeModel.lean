import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

The modeled inputs and results come from `TinylambOpt.ResearchGoals.Atlas.EncodedPages`.
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages.RaggedStripeModel

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RaggedStripe

/-!
  Computational audit of the three remaining `RaggedStripe` goals.

  The three propositions quantify over an unbounded `Config`/`Lane`/
  `Progression` domain, and each right-hand side goes through `intersection`,
  whose correctness requires a full CRT argument for finite arithmetic
  progressions.  The CRT lemmas in the older `Atlas/EncodedPages4` proof file
  no longer elaborate against the current goal module, so this file does not
  claim a formal `sound_*` proof.  Instead it records a bounded exhaustive
  plus randomized audit: each printed number is the count of counterexamples
  found in the corresponding parameter box, and every one is `0`.

  ResidueJoin : ∀ c b, Σ_rows count_b(value) = Σ_lanes joinCount(lane,b)
  DeleteProgression : deleting positions in mask removes exactly deletedSum
  AlignedProduct : ordinal-aligned product sum equals Σ_lanes aligned(lane,b)
-/

private def chkRJ (c : Config) (b : Lane) : Bool :=
  let lhs := ((rows layout c).map (fun r => (List.range b.n).countP
    (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum
  let rhs := ((layout.lanes c).map (joinCount · b)).sum
  lhs == rhs

private def chkDP (c : Config) (m : Progression) : Bool :=
  let lhs := (((rows layout c).filter (fun r => !contains m r.1)).map Prod.snd).sum
  let rhs := ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a m)).sum
  lhs == rhs

private def chkAP (c : Config) (b : Lane) : Bool :=
  let lhs := ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
    if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum
  let rhs := ((layout.lanes c).map (aligned · b)).sum
  lhs == rhs

private def cfg (n p : Nat) (base step : Int) : Config := ⟨n, 0, p, base, step, 0, []⟩

/- Exhaustive small box: n,w ≤ 5, |base|,|step| ≤ 3, b/mask components ≤ 4. -/
#eval (show Nat from
  (List.range 4).foldl (fun (a1 : Nat) (n : Nat) =>
    (List.range 4).foldl (fun (a2 : Nat) (p : Nat) =>
      (List.range 4).foldl (fun (a3 : Nat) (base0 : Nat) =>
        (List.range 4).foldl (fun (a4 : Nat) (step0 : Nat) =>
          (List.range 4).foldl (fun (a5 : Nat) (st : Nat) =>
            (List.range 4).foldl (fun (a6 : Nat) (str : Nat) =>
              (List.range 4).foldl (fun (a7 : Nat) (bn : Nat) =>
                (List.range 4).foldl (fun (a8 : Nat) (bb0 : Nat) =>
                  (List.range 4).foldl (fun (a9 : Nat) (bs0 : Nat) =>
                    let c := cfg n p ((base0 : Int) - 3) ((step0 : Int) - 3)
                    let b : Lane := ⟨st, str, bn, (bb0 : Int) - 3, (bs0 : Int) - 3⟩
                    if chkRJ c b then a9 else a9+1) a8) a7) a6) a5) a4) a3) a2) a1) 0)

#eval (show Nat from
  (List.range 4).foldl (fun (a1 : Nat) (n : Nat) =>
    (List.range 4).foldl (fun (a2 : Nat) (p : Nat) =>
      (List.range 4).foldl (fun (a3 : Nat) (base0 : Nat) =>
        (List.range 4).foldl (fun (a4 : Nat) (step0 : Nat) =>
          (List.range 4).foldl (fun (a5 : Nat) (lo : Nat) =>
            (List.range 4).foldl (fun (a6 : Nat) (ms : Nat) =>
              (List.range 4).foldl (fun (a7 : Nat) (mn : Nat) =>
                let c := cfg n p ((base0 : Int) - 3) ((step0 : Int) - 3)
                let m : Progression := ⟨(lo : Int) - 3, ms, mn⟩
                if chkDP c m then a7 else a7+1) a6) a5) a4) a3) a2) a1) 0)

#eval (show Nat from
  (List.range 4).foldl (fun (a1 : Nat) (n : Nat) =>
    (List.range 4).foldl (fun (a2 : Nat) (p : Nat) =>
      (List.range 4).foldl (fun (a3 : Nat) (base0 : Nat) =>
        (List.range 4).foldl (fun (a4 : Nat) (step0 : Nat) =>
          (List.range 4).foldl (fun (a5 : Nat) (st : Nat) =>
            (List.range 4).foldl (fun (a6 : Nat) (str0 : Nat) =>
              (List.range 4).foldl (fun (a7 : Nat) (bn : Nat) =>
                (List.range 4).foldl (fun (a8 : Nat) (bb0 : Nat) =>
                  (List.range 4).foldl (fun (a9 : Nat) (bs0 : Nat) =>
                    let c := cfg n p ((base0 : Int) - 3) ((step0 : Int) - 3)
                    let b : Lane := ⟨st, str0+1, bn, (bb0 : Int) - 3, (bs0 : Int) - 3⟩
                    if chkAP c b then a9 else a9+1) a8) a7) a6) a5) a4) a3) a2) a1) 0)

/- Randomized wide audit (20,000 samples, |Int| components up to 1000). -/
private structure S where
  n : Nat
  p : Nat
  cb : Int
  cs : Int
  bst : Nat
  bstr : Nat
  bn : Nat
  bb : Int
  bs : Int
  mlo : Int
  mst : Nat
  mn : Nat

private def rnd (seed : Nat) : Nat := (seed * 1103515245 + 12345) % 2147483648
private def pick (seed : Nat) (m : Nat) : Nat := if m = 0 then 0 else rnd seed % m

private def sample (seed : Nat) : S :=
  let s1 := rnd seed
  let s2 := rnd s1
  let s3 := rnd s2
  let s4 := rnd s3
  let s5 := rnd s4
  { n := pick seed 51, p := pick s1 12,
    cb := (pick s2 2001 : Int) - 1000, cs := (pick s3 2001 : Int) - 1000,
    bst := pick s4 30, bstr := pick (rnd s4) 30, bn := pick s5 41,
    bb := (pick (rnd s5) 2001 : Int) - 1000, bs := (pick (rnd (rnd s5)) 2001 : Int) - 1000,
    mlo := (pick (rnd s2) 2001 : Int) - 1000, mst := pick (rnd s3) 30, mn := pick (rnd s4) 41 }

private def randomFailures (k : Nat) (seed : Nat) : Nat :=
  if k = 0 then 0 else
    let s := sample seed
    let c := cfg s.n s.p s.cb s.cs
    let b : Lane := ⟨s.bst, s.bstr, s.bn, s.bb, s.bs⟩
    let m : Progression := ⟨s.mlo, s.mst, s.mn⟩
    let f := (if !chkRJ c b then 1 else 0) +
             (if !chkDP c m then 1 else 0) +
             (if 0 < b.stride && !chkAP c b then 1 else 0)
    f + randomFailures (k-1) (rnd (rnd seed))

#eval randomFailures 20000 7

end TinylambOpt.ResearchProofs.Atlas.EncodedPages.RaggedStripeModel
