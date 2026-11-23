import TinylambOpt.Research.RegexTaggedDeterminize

namespace TinylambOpt.ResearchGoals.OrderedTagFrontier

open Research.RegexTaggedDeterminize

-- Edges do not inspect tags. At equal states identical continuations remain
-- available. Keep the earliest history in the reference's frontier order.
def firstStates (cs : List Config) : List Config :=
  cs.foldl (fun out c => if out.any (fun d => d.1 == c.1) then out else out ++ [c]) []

def close (nfa : NFA) (pos : Nat) : Nat → List Config → List Config
  | 0, cs => firstStates cs
  | k+1, cs =>
    let live := firstStates cs
    close nfa pos k (live ++ live.flatMap (stepEps nfa pos))

def walk (nfa : NFA) (limit pos : Nat) (cs : List Config) : List Byte → List Config
  | [] => cs
  | b :: bs =>
    let next := close nfa (pos+1) limit (cs.flatMap fun c => stepByte nfa c b)
    walk nfa limit (pos+1) next bs

def frontier (nfa : NFA) (limit : Nat) (input : List Byte) :=
  walk nfa limit 0 (close nfa 0 limit [initConfig nfa]) input

def winner (nfa : NFA) (pos : Nat) (cs : List Config) :=
  (cs.find? fun c => nfa.accept c.1).map (capturesAt pos)

def Goal : Prop := ∀ nfa limit input,
  winner nfa input.length (frontier nfa limit input) =
    winner nfa input.length (fullRun nfa limit input) ∧
  ((frontier nfa limit input).map Prod.fst).Nodup

example : winner regressionNFA 1 (frontier regressionNFA 2 [97]) =
    some [some (0,1)] := by cbv
example : winner regressionNFA 1 (fullRun regressionNFA 2 [97]) =
    some [some (0,1)] := by cbv

end TinylambOpt.ResearchGoals.OrderedTagFrontier
