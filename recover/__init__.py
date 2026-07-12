"""
Pacote de recuperação e tolerância a falhas.

Componentes:
- AtomicCheckpointStore: persistência local de checkpoints com escrita atômica,
  checksum e cópia de segurança.
- FailureDetector: detecção de falhas baseada em heartbeats.
"""

from .checkpoint import (
    AtomicCheckpointStore,
    CheckpointCorruptedError,
    CheckpointError,
    load_checkpoint,
    save_checkpoint,
)
from .failure_detector import FailureDetector, NodeStatus

__all__ = [
    "AtomicCheckpointStore",
    "CheckpointCorruptedError",
    "CheckpointError",
    "FailureDetector",
    "NodeStatus",
    "load_checkpoint",
    "save_checkpoint",
]
