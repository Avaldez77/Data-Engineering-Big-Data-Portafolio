\
import re
from typing import Optional, Dict

RE_SUCCESS_WEB = re.compile(r"Operación\s+(?P<op>\d+)\s+ejecutada\s+en\s+la\s+aplicación\s+web", re.I)
RE_SUCCESS_DB  = re.compile(r"Operación\s+(?P<op>\d+)\s+ejecutada\s+en\s+la\s+base\s+de\s+datos", re.I)
RE_ERR_WEB     = re.compile(r"Error\s+de\s+la\s+aplicación\s+web\s+de\s+tipo\s+(?P<code>\d+)\s+en\s+la\s+operación\s+(?P<op>\d+)", re.I)
RE_ERR_DB      = re.compile(r"Error\s+de\s+la\s+base\s+de\s+datos\s+en\s+la\s+operación\s+(?P<op>\d+)", re.I)

def parse_line(line: str) -> Optional[Dict]:
    """
    Parse a single raw log line into a structured event dictionary.
    Returns None for empty lines.
    """
    line = (line or "").strip()
    if not line:
        return None

    m = RE_SUCCESS_WEB.search(line)
    if m:
        return {"operation_id": int(m["op"]), "layer": "web", "status": "success",
                "http_code": None, "error_type": None, "raw_line": line}

    m = RE_SUCCESS_DB.search(line)
    if m:
        return {"operation_id": int(m["op"]), "layer": "db", "status": "success",
                "http_code": None, "error_type": None, "raw_line": line}

    m = RE_ERR_WEB.search(line)
    if m:
        code = int(m["code"])
        return {"operation_id": int(m["op"]), "layer": "web", "status": "error",
                "http_code": code, "error_type": f"web_{code}", "raw_line": line}

    m = RE_ERR_DB.search(line)
    if m:
        return {"operation_id": int(m["op"]), "layer": "db", "status": "error",
                "http_code": None, "error_type": "db_error", "raw_line": line}

    # Unknown / unparsed line
    return {"operation_id": None, "layer": "unknown", "status": "unknown",
            "http_code": None, "error_type": "unparsed", "raw_line": line}
