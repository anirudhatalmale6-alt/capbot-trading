import os
from pathlib import Path
from typing import Optional


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)  # does not kill; just checks existence/permission
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # process exists but we cannot signal it; treat as alive
        return True
    except Exception:
        return False


class InstanceLock:
    """
    Simple PID lock file.

    - Creates lock file with O_EXCL.
    - If lock exists, reads PID and checks if process alive:
        - if not alive -> remove stale lock and retry acquire once
        - if alive -> raise
    """

    def __init__(self, path: Path, stale_timeout_sec: int = 0):
        self.path = Path(path)
        self.fd: Optional[int] = None
        self.stale_timeout_sec = stale_timeout_sec

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        def _try_create() -> None:
            self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.write(self.fd, str(os.getpid()).encode("utf-8"))
            os.fsync(self.fd)

        try:
            _try_create()
            return
        except FileExistsError:
            # check stale
            try:
                txt = self.path.read_text(encoding="utf-8").strip()
                pid = int(txt) if txt else -1
            except Exception:
                pid = -1

            if pid > 0 and _pid_is_alive(pid):
                raise RuntimeError(f"Active lock: {self.path}. PID {pid} is still alive.")

            # stale lock -> remove and retry once
            try:
                self.path.unlink()
            except Exception:
                pass

            # retry acquire
            try:
                _try_create()
                return
            except FileExistsError:
                raise RuntimeError(f"Active lock: {self.path}. Another process won the race.")

    def release(self) -> None:
        try:
            if self.fd is not None:
                os.close(self.fd)
        finally:
            self.fd = None
            try:
                if self.path.exists():
                    self.path.unlink()
            except Exception:
                pass
