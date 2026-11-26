import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
#eval QuadraticGap.kernel.bounds 0 ⟨100, 1⟩ ⟨2, 3⟩   -- expect ⟨9409, 2⟩?
#eval QuadraticGap.kernel.score 0 (1, 2)             -- expect 1
#eval (QuadraticGap.kernel.bounds 0 ⟨100, 1⟩ ⟨2, 3⟩).lo == QuadraticGap.kernel.score 0 (1, 2)
