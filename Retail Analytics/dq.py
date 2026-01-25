from typing import List, Dict, Any, Tuple

def require_non_empty(rows: List[Dict[str, Any]], key: str) -> Tuple[bool, str]:
    missing = sum(1 for r in rows if not str(r.get(key, "")).strip())
    ok = missing == 0
    return ok, f"{key}: missing={missing}"

def require_unique(rows: List[Dict[str, Any]], key: str) -> Tuple[bool, str]:
    vals = [r.get(key) for r in rows]
    ok = len(vals) == len(set(vals))
    return ok, f"{key}: unique={ok} (rows={len(vals)})"
