-- Proof aggregation for S3Durability (History / Origin / Barrier / Pipeline)
import TinylambOpt.ResearchProofs.S3Durability.History
import TinylambOpt.ResearchProofs.S3Durability.Origin
import TinylambOpt.ResearchProofs.S3Durability.Barrier
import TinylambOpt.ResearchProofs.S3Durability.Pipeline

/-!
This module collects the proofs for the Primary + S3 design
(`docs/primary-s3.md`): the WAL history a restore reads from S3, the retained
origin a restore or seal scan may start from, the reply barrier that decides
when a SQL reply may leave the server, and the uploader throughput model that
sizes WAL chunks and in-flight PUTs.
-/
