import importlib
from typing import Any

def load_strategy(spec: str) -> Any:
    if ":" not in spec:
        raise ValueError("strategy.module debe ser 'module.path:ClassName'")
    mod, cls = spec.split(":", 1)
    m = importlib.import_module(mod)
    C = getattr(m, cls)
    return C()
