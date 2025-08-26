# scripts/utils_io.py
from __future__ import annotations

import os
import shutil
import stat
import tempfile
import time
from pathlib import Path
from typing import Optional, Union

PathLike = Union[str, Path]

def timestamp() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


# ---------- Guards ----------

def assert_not_readonly(path: PathLike) -> None:
    """
    On Windows, raise if the target file exists and has the Read-only attribute set.
    On non-Windows, this silently passes.
    """
    p = Path(path)
    if not p.exists():
        return
    try:
        # Windows-only attribute; will raise AttributeError elsewhere
        if p.stat().st_file_attributes & stat.FILE_ATTRIBUTE_READONLY:  # type: ignore[attr-defined]
            raise PermissionError(
                f"{p} is Read-only. Clear it and retry.\n"
                f'PowerShell: attrib -R "{p}"'
            )
    except AttributeError:
        # Non-Windows platforms: nothing special to check
        return


# ---------- Snapshots ----------

def snapshot_csv(path: PathLike, *, suffix: str = "prewrite",
                 snapshots_dir: Optional[PathLike] = None) -> Optional[Path]:
    """
    If `path` exists, copy it to a sibling `_snapshots` folder with a timestamped name:
    e.g., foo.csv -> _snapshots/foo_prewrite_YYYYMMDD_HHMMSS.csv
    Returns the snapshot path if created, else None.
    """
    src = Path(path)
    if not src.exists():
        return None

    snap_dir = Path(snapshots_dir) if snapshots_dir else src.parent / "_snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)

    stem, ext = src.stem, (src.suffix or ".csv")
    snap_name = f"{stem}_{suffix}_{timestamp()}{ext}"
    dst = snap_dir / snap_name
    shutil.copy2(src, dst)
    return dst


# ---------- Atomic CSV write ----------

def write_csv_atomic(df, path: PathLike, *,
                     index: bool = False,
                     encoding: str = "utf-8") -> Path:
    """
    Write CSV atomically:
      1) Write to a temp file in the same directory
      2) os.replace() onto the final destination (atomic on Windows)
    Raises a friendly PermissionError if the file is locked/open.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Friendly lock/Read-only check
    assert_not_readonly(target)

    tmp: Optional[tempfile.NamedTemporaryFile] = None
    tmp_name: Optional[str] = None
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, dir=target.parent, suffix=".tmp")
        tmp_name = tmp.name
        tmp.close()  # allow pandas to open it on Windows

        df.to_csv(tmp_name, index=index, encoding=encoding)
        os.replace(tmp_name, target)  # atomic on Windows/Posix
        return target
    except PermissionError as e:
        raise PermissionError(
            f"Permission denied writing {target}.\n"
            "Is it open in Excel or marked Read-only? Close/unlock and retry."
        ) from e
    finally:
        if tmp_name and os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass


# ---------- Convenience wrapper (keeps your original API) ----------

def safe_write_csv(df, path: PathLike, *,
                   backups_dir: Optional[PathLike] = None,
                   index: bool = False,
                   encoding: str = "utf-8",
                   tag: Optional[str] = None) -> Path:
    """
    Atomic write + optional snapshot of the previous file to `backups_dir`.
    If `backups_dir` is None, no extra snapshot is made (use snapshot_csv separately).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if backups_dir and path.exists():
        backups = Path(backups_dir)
        backups.mkdir(parents=True, exist_ok=True)
        stem, ext = path.stem, (path.suffix or ".csv")
        tag_part = f"_{tag}" if tag else ""
        snap_name = f"{stem}_prewrite_{timestamp()}{tag_part}{ext}"
        shutil.copy2(path, backups / snap_name)

    return write_csv_atomic(df, path, index=index, encoding=encoding)
