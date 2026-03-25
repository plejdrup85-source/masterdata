"""Saved jobs service — persist and resume analysis jobs.

Jobs are password-protected JSON files on disk.
Passwords are stored as bcrypt-compatible hashes (using hashlib for zero-dependency).
Jobs expire after 60 days of inactivity.
"""

import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SAVED_JOBS_DIR = Path(os.environ.get("SAVED_JOBS_DIR", "/tmp/masterdata_output/saved_jobs"))
SAVED_JOBS_DIR.mkdir(parents=True, exist_ok=True)

JOB_EXPIRY_DAYS = 60
JOB_EXPIRY_SECONDS = JOB_EXPIRY_DAYS * 24 * 3600

ADMIN_PASSWORD_ENV = "ANALYSIS_JOB_ADMIN_PASSWORD"


def _hash_password(password: str) -> str:
    """Hash password with salt using SHA-256."""
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{h}"


def _verify_password(password: str, stored_hash: str) -> bool:
    """Verify password against stored hash."""
    if ":" not in stored_hash:
        return False
    salt, expected = stored_hash.split(":", 1)
    actual = hashlib.sha256((salt + password).encode()).hexdigest()
    return hmac.compare_digest(actual, expected)


def _is_admin_password(password: str) -> bool:
    """Check if password matches admin env var."""
    admin_pw = os.environ.get(ADMIN_PASSWORD_ENV)
    if not admin_pw:
        return False
    return hmac.compare_digest(password, admin_pw)


def _job_path(job_id: str) -> Path:
    return SAVED_JOBS_DIR / f"{job_id}.json"


def _load_job(job_id: str) -> Optional[dict]:
    path = _job_path(job_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to load saved job {job_id}: {e}")
        return None


def _persist_job(job_id: str, data: dict) -> None:
    try:
        _job_path(job_id).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        logger.error(f"Failed to persist saved job {job_id}: {e}")


def save_job(
    job_id: str,
    job_name: str,
    password: str,
    module: str,
    state_payload: dict,
) -> dict:
    """Save a job with password protection."""
    if len(password) < 4:
        raise ValueError("Passord må være minst 4 tegn")
    if not job_name.strip():
        raise ValueError("Jobbnavn er påkrevd")

    # Check if job already exists
    existing = _load_job(job_id)
    if existing:
        raise ValueError(f"Jobb {job_id} er allerede lagret. Bruk oppdater i stedet.")

    now = time.time()
    data = {
        "job_id": job_id,
        "job_name": job_name.strip(),
        "module": module,
        "password_hash": _hash_password(password),
        "created_at": now,
        "updated_at": now,
        "status": "saved",
        "state_payload": state_payload,
    }
    _persist_job(job_id, data)
    logger.info(f"Job {job_id} saved: '{job_name}' ({module})")
    return {"job_id": job_id, "job_name": job_name, "status": "saved"}


def update_job(job_id: str, password: str, state_payload: dict) -> dict:
    """Update an existing saved job (refreshes activity timer)."""
    data = _load_job(job_id)
    if not data:
        raise ValueError("Jobb ikke funnet")

    if not _verify_password(password, data["password_hash"]) and not _is_admin_password(password):
        raise ValueError("Feil passord")

    data["updated_at"] = time.time()
    data["state_payload"] = state_payload
    data["status"] = "saved"
    _persist_job(job_id, data)
    return {"job_id": job_id, "status": "updated"}


def unlock_job(job_id: str, password: str) -> dict:
    """Verify password and return the job state."""
    data = _load_job(job_id)
    if not data:
        raise ValueError("Jobb ikke funnet")

    # Check expiry
    last_activity = data.get("updated_at", data.get("created_at", 0))
    if time.time() - last_activity > JOB_EXPIRY_SECONDS:
        # Delete expired job
        _job_path(job_id).unlink(missing_ok=True)
        raise ValueError("Jobben er utløpt og har blitt slettet (60 dager uten aktivitet)")

    is_admin = _is_admin_password(password)
    if not _verify_password(password, data["password_hash"]) and not is_admin:
        raise ValueError("Feil passord")

    # Refresh activity timer
    data["updated_at"] = time.time()
    _persist_job(job_id, data)

    return {
        "job_id": data["job_id"],
        "job_name": data["job_name"],
        "module": data["module"],
        "status": data["status"],
        "created_at": data["created_at"],
        "updated_at": data["updated_at"],
        "state_payload": data["state_payload"],
        "unlocked_by_admin": is_admin,
    }


def list_saved_jobs() -> list[dict]:
    """List all saved jobs (metadata only, no passwords or state)."""
    jobs = []
    for path in sorted(SAVED_JOBS_DIR.glob("*.json"), reverse=True):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            last_activity = data.get("updated_at", data.get("created_at", 0))
            is_expired = time.time() - last_activity > JOB_EXPIRY_SECONDS
            if is_expired:
                # Clean up expired on list
                path.unlink(missing_ok=True)
                continue
            jobs.append({
                "job_id": data.get("job_id", path.stem),
                "job_name": data.get("job_name", ""),
                "module": data.get("module", ""),
                "created_at": data.get("created_at", 0),
                "updated_at": data.get("updated_at", 0),
                "status": data.get("status", "saved"),
                "is_saved_job": True,
            })
        except Exception:
            pass
    return jobs[:50]


def delete_job(job_id: str, password: str) -> dict:
    """Delete a saved job (requires password or admin)."""
    data = _load_job(job_id)
    if not data:
        raise ValueError("Jobb ikke funnet")

    if not _verify_password(password, data["password_hash"]) and not _is_admin_password(password):
        raise ValueError("Feil passord")

    _job_path(job_id).unlink(missing_ok=True)
    return {"job_id": job_id, "deleted": True}


def cleanup_expired() -> int:
    """Remove jobs older than 60 days. Returns count of removed jobs."""
    removed = 0
    for path in SAVED_JOBS_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            last_activity = data.get("updated_at", data.get("created_at", 0))
            if time.time() - last_activity > JOB_EXPIRY_SECONDS:
                path.unlink(missing_ok=True)
                removed += 1
                logger.info(f"Cleaned up expired saved job: {path.stem}")
        except Exception:
            pass
    return removed
