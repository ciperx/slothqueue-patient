"""slothqueue — a patient, local-first job queue (Python standard library only)."""

from .core import SlothQueue, SlothQueueError

__all__ = ["SlothQueue", "SlothQueueError"]
__version__ = "0.1.0"
