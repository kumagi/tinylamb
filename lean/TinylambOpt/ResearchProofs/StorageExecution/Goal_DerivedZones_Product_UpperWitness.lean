import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_UpperWitness

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

/-- For the product kernel the upper bound of the derived interval is
    exactly the score at the upper corner `(a.hi, b.hi)`: both reduce to
    `a.hi * b.hi`. -/
theorem sound_UpperWitness : Product.UpperWitness.Goal := by
  intro a b
  rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_UpperWitness
