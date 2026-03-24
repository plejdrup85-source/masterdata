"""Persistent job storage for matching and anbud jobs.

Jobs are stored as individual JSON files in data/jobs/.
Each job file contains full metadata + serialized results.
Supports 90-day retention, locking, and resume.
"""

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

JOBS_DIR = Path(os.environ.get("JOBS_DIR", "data/jobs"))
JOBS_DIR.mkdir(parents=True, exist_ok=True)

RETENTION_DAYS = 90
RETENTION_SECONDS = RETENTION_DAYS * 24 * 60 * 60

ADMIN_PASSWORD_ENV = "ADMIN_PASSWORD"


def _hash_password(password: str) -> str:
    """Hash a password with salt for storage."""
    salt = os.urandom(16).hex()
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{h}"


def _verify_password(password: str, stored: str) -> bool:
    """Verify a password against stored hash."""
    if ":" not in stored:
        return False
    salt, h = stored.split(":", 1)
    return hashlib.sha256((salt + password).encode()).hexdigest() == h


def _verify_admin(password: str) -> bool:
    """Verify admin password from environment."""
    admin_pw = os.environ.get(ADMIN_PASSWORD_ENV)
    if not admin_pw:
        return False
    return password == admin_pw


def _job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def save_job(job_data: dict) -> None:
    """Save job metadata and results to disk."""
    job_id = job_data["job_id"]
    job_data["updated_at"] = time.time()
    job_data["last_activity_at"] = time.time()

    # Serialize MatchResult objects if present
    serializable = _make_serializable(job_data)

    path = _job_path(job_id)
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(serializable, ensure_ascii=False, default=str), encoding="utf-8")
        tmp.replace(path)
    except OSError as e:
        logger.error(f"Failed to save job {job_id}: {e}")
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def load_job(job_id: str) -> Optional[dict]:
    """Load job from disk. Returns None if not found."""
    path = _job_path(job_id)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to load job {job_id}: {e}")
        return None


def delete_job(job_id: str) -> bool:
    """Delete a job from disk."""
    path = _job_path(job_id)
    if path.exists():
        path.unlink()
        return True
    return False


def list_jobs() -> list[dict]:
    """List all jobs with summary metadata (no full results)."""
    jobs = []
    now = time.time()
    for p in JOBS_DIR.glob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            # Check retention
            last_activity = data.get("last_activity_at", data.get("created_at", 0))
            if now - last_activity > RETENTION_SECONDS:
                logger.info(f"Removing expired job {p.stem}")
                p.unlink()
                continue

            jobs.append({
                "job_id": data.get("job_id", p.stem),
                "job_name": data.get("job_name", ""),
                "created_by": data.get("created_by", ""),
                "created_at": data.get("created_at", 0),
                "updated_at": data.get("updated_at", 0),
                "last_activity_at": data.get("last_activity_at", 0),
                "job_type": data.get("job_type", "standard"),
                "match_mode": data.get("match_mode", "standard"),
                "status": data.get("status", "unknown"),
                "total": data.get("total", 0),
                "processed": data.get("processed", 0),
                "source_filename": data.get("source_filename", ""),
                "locked": data.get("locked", False),
                "summary": data.get("summary"),
            })
        except (json.JSONDecodeError, OSError):
            continue

    jobs.sort(key=lambda j: j.get("last_activity_at", 0), reverse=True)
    return jobs


def update_job_activity(job_id: str) -> None:
    """Touch last_activity_at without loading full job."""
    data = load_job(job_id)
    if data:
        data["last_activity_at"] = time.time()
        save_job(data)


def lock_job(job_id: str, password: str) -> bool:
    """Lock a job with a password. Returns True on success."""
    data = load_job(job_id)
    if not data:
        return False
    data["locked"] = True
    data["lock_hash"] = _hash_password(password)
    save_job(data)
    return True


def unlock_job(job_id: str, password: str) -> tuple[bool, str]:
    """Unlock a job. Accepts job password or admin password.
    Returns (success, message).
    """
    data = load_job(job_id)
    if not data:
        return False, "Jobb ikke funnet"
    if not data.get("locked"):
        return True, "Jobben er ikke låst"

    lock_hash = data.get("lock_hash", "")

    # Try admin password first
    if _verify_admin(password):
        data["locked"] = False
        data.pop("lock_hash", None)
        save_job(data)
        return True, "Låst opp med admin-tilgang"

    # Try job password
    if _verify_password(password, lock_hash):
        data["locked"] = False
        data.pop("lock_hash", None)
        save_job(data)
        return True, "Låst opp"

    return False, "Feil passord"


def verify_job_access(job_id: str, password: Optional[str] = None) -> tuple[bool, str]:
    """Check if a job is accessible. Returns (can_edit, message)."""
    data = load_job(job_id)
    if not data:
        return False, "Jobb ikke funnet"
    if not data.get("locked"):
        return True, "OK"
    if not password:
        return False, "Jobben er låst"

    lock_hash = data.get("lock_hash", "")
    if _verify_admin(password) or _verify_password(password, lock_hash):
        return True, "OK"

    return False, "Feil passord"


def cleanup_expired_jobs() -> int:
    """Remove jobs inactive for more than RETENTION_DAYS. Returns count removed."""
    now = time.time()
    removed = 0
    for p in JOBS_DIR.glob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            last_activity = data.get("last_activity_at", data.get("created_at", 0))
            if now - last_activity > RETENTION_SECONDS:
                p.unlink()
                removed += 1
        except (json.JSONDecodeError, OSError):
            continue
    return removed


def _make_serializable(data: dict) -> dict:
    """Convert job data to JSON-serializable form."""
    out = {}
    for k, v in data.items():
        if k == "results" and isinstance(v, list):
            out[k] = []
            for item in v:
                if hasattr(item, "to_dict"):
                    out[k].append(item.to_dict())
                elif isinstance(item, dict):
                    out[k].append(item)
                else:
                    out[k].append(str(item))
        else:
            out[k] = v
    return out
