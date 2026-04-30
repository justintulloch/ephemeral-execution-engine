import os 
import logging
import numpy as np
from core.constants import WORKER_MEMORY_LIMIT_MB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ember.shared_memory")

DEFAULT_WEIGHTS_PATH = "/tmp/ember_model_weights.nmap"

class SharedModelWeights:
    """shared memory region for model weights"""

    def __init__(
        self,
        path: str = DEFAULT_WEIGHTS_PATH,
        size_mb: int = WORKER_MEMORY_LIMIT_MB,
    ) -> None:
        self.path = path
        self.size_mb = size_mb
        # float32 = 4 bytes, so elements = (size_mb * 1024 * 1024) / 4
        self.num_elements = (size_mb * 1024 * 1024) // 4
        self._mmap: np.memmap | None = None

    def create(self) -> np.memmap:
        """
        Create and populate the shared weight file.

        Called once by the host process at boot time.
        Fills with deterministic data so readers can verify integrity.
        """
        logger.info(
            "[SHM] Creating %dMB shared weight file: %s (%d float32 elements)",
            self.size_mb,
            self.path,
            self.num_elements,
        )

        self._mmap = np.memmap(
            self.path,
            dtype="float32",
            mode="w+",
            shape=(self.num_elements,),
        )

        # Fill with deterministic pattern for integrity checks
        # First 1000 elements get sequential values, rest filled with 1.0
        seq_count = min(1000, self.num_elements)
        self._mmap[:seq_count] = np.arange(seq_count, dtype="float32")
        if self.num_elements > seq_count:
            self._mmap[seq_count:] = 1.0
        self._mmap.flush()

        actual_size_mb = os.path.getsize(self.path) / (1024 * 1024)
        logger.info("[SHM] Created: %.1fMB on disk", actual_size_mb)

        return self._mmap


    def attach_readonly(self) -> np.memmap:

        if not os.path.exists(self.path):
            raise FileNotFoundError(
                f"Shared weight file not found: {self.path}. "
                "Host must call create() before workers attach." 
            )

        mmap = np.memmap(
            self.path,
            dtype="float32",
            mode="r",
            shape=(self.num_elements,),
        )

        return mmap

    def verify_integrity(self, mmap: np.memmap, check_count: int = 100) -> bool:

        """Checks the first `check_count` sequential values written by create()."""

        expected = np.arange(check_count, dtype="float32")
        actual = np.array(mmap[:check_count])
        return np.array_equal(expected, actual)

    def cleanup(self) -> None:

        if self._mmap is not None:
            del self._mmap
            self._mmap = None

        if os.path.exists(self.path):
            os.remove(self.path)
            logger.info("[SHM] Clean up: %s", self.path)
