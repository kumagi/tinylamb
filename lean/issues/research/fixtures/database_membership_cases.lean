import TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership

/-! Emit SQL and expected CLI output for the same DOUBLE IN inputs. Expected
results use the row-wise three-valued specification, independently of the hash
representation and the execution engine. Project IN and NOT IN independently
so one predicate cannot mask an erroneous result from the other. -/
open TinylambOpt.ResearchGoals.DatabaseCorrectness.NullableMembership

namespace MembershipCases
structure Sample where
  sql : String
  value : Cell

def nullValue : Sample := ⟨"CAST(NULL AS DOUBLE)",none⟩
def nanValue : Sample := ⟨"CAST('NaN' AS DOUBLE)",some (Float.ofBits 0x7ff8000000000000)⟩
def plusZero : Sample := ⟨"CAST(0.0 AS DOUBLE)",some 0⟩
def minusZero : Sample := ⟨"CAST('-0.0' AS DOUBLE)",some (Float.ofBits 0x8000000000000000)⟩
def pos : Sample := ⟨"CAST(1.5 AS DOUBLE)",some 1.5⟩
def neg : Sample := ⟨"CAST(-2.0 AS DOUBLE)",some (-2.0)⟩
def posInf : Sample := ⟨"CAST('Infinity' AS DOUBLE)",some (Float.ofBits 0x7ff0000000000000)⟩
def negInf : Sample := ⟨"CAST('-Infinity' AS DOUBLE)",some (Float.ofBits 0xfff0000000000000)⟩
def tiny : Sample := ⟨"CAST('2.2250738585072014e-308' AS DOUBLE)",some (Float.ofBits 0x0010000000000000)⟩
def probes := [nullValue,nanValue,plusZero,minusZero,pos,neg,posInf,negInf,tiny]
def bags := [[],[nullValue],[nanValue],[plusZero],[minusZero,minusZero],
  [nullValue,nanValue],[nullValue,pos,pos],[neg,nullValue,nanValue,pos],[nanValue,plusZero,neg],
  [posInf,negInf],[negInf,nanValue,nullValue],[tiny,minusZero]]

def setup : IO Unit := do
  IO.println "CREATE TABLE probes (id INT64, v DOUBLE);"
  IO.println "CREATE TABLE rhs (g INT64, v DOUBLE);"
  for (x,i) in probes.zipIdx do
    IO.println s!"INSERT INTO probes VALUES ({i}, {x.sql});"
  for (bag,g) in bags.zipIdx do
    for x in bag do IO.println s!"INSERT INTO rhs VALUES ({g}, {x.sql});"

def queries : IO Unit := do
  for (_,g) in bags.zipIdx do
    let sub := s!"(SELECT r.v FROM rhs AS r WHERE r.g = {g})"
    IO.println s!"SELECT {g} AS g, p.id, p.v IN {sub} AS member, p.v NOT IN {sub} AS nonmember FROM probes AS p ORDER BY p.id;"

def expected : IO Unit := do
  for (bag,g) in bags.zipIdx do
    for (x,i) in probes.zipIdx do
      let truth := rescan x.value (bag.map Sample.value)
      let render := fun (v : Truth) => match v with
        | some true => "1" | some false => "0" | none => "NULL"
      IO.println s!"[{g}, {i}, {render truth}, {render (truth.map Bool.not)}]"
end MembershipCases

def main : List String → IO Unit
  | ["setup"] => MembershipCases.setup
  | ["queries"] => MembershipCases.queries
  | ["expected"] => MembershipCases.expected
  | _ => throw (IO.userError "expected one argument: setup, queries, or expected")
