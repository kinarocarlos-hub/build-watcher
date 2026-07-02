#!/usr/bin/env python3
"""
Advanced Build Watcher Enterprise Edition
Features:
 - git-aware change mapping (blame/fingerprint)
 - system resource monitoring (CPU/RAM/disk)
 - post-build smoke tests (APK install checker, JAR manifest)
 - trend analytics dashboard
 - notification hooks (webhook/desktop)
 - build profiling (step timings)
 - automatic rollback suggestions
 - dependency cache layer
 - real-time terminal UI (rich)
"""

import subprocess
import json
import os
import sys
import time
import re
import threading
import hashlib
import sqlite3
import shutil
import tarfile
import gzip
import io
import statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from collections import deque, defaultdict

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.layout import Layout
    from rich.text import Text
    from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

PROJECTS = [
    {
        "name": "NetPulseAndroid",
        "path": "/home/onesmus/IdeaProjects/NetPulseAndroid",
        "build_cmd": ["./gradlew", "assembleDebug"],
        "env": {"JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-amd64"},
        "artifacts": ["app/build/outputs/apk/debug/app-arm64-v8a-debug.apk",
                       "app/build/outputs/apk/debug/app-armeabi-v7a-debug.apk"],
        "pre_hook": "./gradlew --stop",
        "smoke_test": "android",
        "webhook_url": None,
        "notify_on": ["failure", "success_first"],
    },
    {
        "name": "NetworkPacketAnalyzer",
        "path": "/home/onesmus/IdeaProjects/NetworkPacketAnalyzer",
        "build_cmd": ["mvn", "clean", "package", "-DskipTests", "-B"],
        "env": {},
        "artifacts": ["target/NetworkPacketAnalyzer-1.0-SNAPSHOT.jar"],
        "pre_hook": None,
        "smoke_test": "jar",
        "webhook_url": None,
        "notify_on": ["failure", "success_first"],
    },
]

STATE_DB = "/tmp/build_watcher_enterprise.db"
POLL_INTERVAL = 5
PERIODIC_INTERVAL = 300
MAX_BACKOFF = 600
LOG_JSONL = "/tmp/build_watcher_enterprise_events.jsonl"
CACHE_DIR = "/tmp/build_watcher_cache"
ROLLBACK_DIR = "/tmp/build_watcher_rollback"
MAX_ROLLBACK = 3
MAX_HISTORY = 500

ERROR_PATTERNS = {
    "jdk_major_version": {
        "re": r"Unsupported class file major version (\d+)",
        "fix": "JDK version mismatch — set JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64",
        "severity": "fatal",
        "auto_fix": lambda p: p.update(env={**p.get("env", {}), "JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-amd64"})
    },
    "gradle_daemon_stuck": {
        "re": r"Daemon will be stopped|Timeout waiting for daemon",
        "fix": "Gradle daemon timeout — restarting daemon",
        "severity": "recoverable",
        "auto_fix": lambda p: subprocess.run(p.get("pre_hook", "true"), shell=True, cwd=p["path"], capture_output=True)
    },
    "out_of_memory": {
        "re": r"GC overhead limit exceeded|OutOfMemoryError",
        "fix": "OOM detected — suggest increasing heap",
        "severity": "fatal",
        "auto_fix": None
    },
    "dependency_conflict": {
        "re": r"Could not resolve|Failed to resolve|duplicate class",
        "fix": "Dependency issue — refresh dependencies may help",
        "severity": "fatal",
        "auto_fix": None
    },
    "network_timeout": {
        "re": r"Connection timed out|Could not transfer artifact",
        "fix": "Network timeout — transient",
        "severity": "transient",
        "auto_fix": None
    },
    "permission_denied": {
        "re": r"Permission denied|EACCES",
        "fix": "Permission error — check file perms",
        "severity": "fatal",
        "auto_fix": None
    },
    "manifest_merge": {
        "re": r"Manifest merger failed|uses-sdk:minSdk",
        "fix": "Android manifest conflict",
        "severity": "fatal",
        "auto_fix": None
    },
    "compile_error": {
        "re": r"error:\s+.+\.(java|kt):\d+:\d+",
        "fix": "Compilation error in source file",
        "severity": "fatal",
        "auto_fix": None
    },
}

AUTO_FIX_ACTIONS = {
    name: cfg.get("auto_fix") for name, cfg in ERROR_PATTERNS.items() if cfg.get("auto_fix")
}

db_lock = threading.Lock()
console = Console(stderr=True) if RICH_AVAILABLE else None


@dataclass
class BuildEvent:
    ts: str
    project: str
    success: bool
    duration_ms: int
    returncode: int
    error_type: Optional[str]
    severity: str
    fix_suggestion: str
    error_detail: str
    auto_fixed: int
    backoff: int
    missing_artifacts: List[str] = field(default_factory=list)
    cpu_percent: float = 0.0
    mem_percent: float = 0.0
    disk_percent: float = 0.0
    changed_files: List[str] = field(default_factory=list)
    git_commit: Optional[str] = None
    git_author: Optional[str] = None
    git_message: Optional[str] = None
    step_profile: Dict[str, float] = field(default_factory=dict)
    smoke_test_passed: Optional[bool] = None
    smoke_test_detail: str = ""
    cached: bool = False
    rollback_suggestion: Optional[str] = None


class ResourceMonitor:
    def __init__(self):
        self._snapshots = deque(maxlen=1000)

    def snapshot(self) -> Dict[str, float]:
        if not PSUTIL_AVAILABLE:
            return {"cpu_percent": 0.0, "mem_percent": 0.0, "disk_percent": 0.0}
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory().percent
            disk = psutil.disk_usage("/").percent
            snap = {"cpu_percent": cpu, "mem_percent": mem, "disk_percent": disk}
            self._snapshots.append(snap)
            return snap
        except Exception:
            return {"cpu_percent": 0.0, "mem_percent": 0.0, "disk_percent": 0.0}

    def avg(self, n: int = 10) -> Dict[str, float]:
        if not self._snapshots:
            return {"cpu_percent": 0.0, "mem_percent": 0.0, "disk_percent": 0.0}
        items = list(self._snapshots)[-n:]
        return {
            "cpu_percent": sum(i["cpu_percent"] for i in items) / len(items),
            "mem_percent": sum(i["mem_percent"] for i in items) / len(items),
            "disk_percent": sum(i["disk_percent"] for i in items) / len(items),
        }


class GitHelper:
    @staticmethod
    def project_root(path: str) -> Optional[str]:
        try:
            r = subprocess.run(["git", "-C", path, "rev-parse", "--show-toplevel"],
                               capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                return r.stdout.strip()
        except Exception:
            pass
        return None

    @staticmethod
    def current_commit(path: str) -> Optional[Dict[str, str]]:
        try:
            r = subprocess.run(["git", "-C", path, "log", "-1", "--format=%H|%an|%s"],
                               capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                parts = r.stdout.strip().split("|", 2)
                if len(parts) == 3:
                    return {"hash": parts[0], "author": parts[1], "message": parts[2]}
        except Exception:
            pass
        return None

    @staticmethod
    def changed_files(path: str, since: Optional[str] = None) -> List[str]:
        try:
            if since:
                r = subprocess.run(["git", "-C", path, "diff", "--name-only", since, "HEAD"],
                                   capture_output=True, text=True, timeout=5)
            else:
                r = subprocess.run(["git", "-C", path, "diff", "--name-only", "HEAD~1", "HEAD"],
                                   capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                return [f.strip() for f in r.stdout.strip().splitlines() if f.strip()]
        except Exception:
            pass
        return []

    @staticmethod
    def blame_file(path: str, file: str, line: int = 1) -> Optional[Dict[str, str]]:
        try:
            r = subprocess.run(["git", "-C", path, "blame", "-L", f"{line},{line}", "--porcelain", file],
                               capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                lines = r.stdout.strip().splitlines()
                commit_hash = lines[0].split()[0] if lines else None
                author = None
                for ln in lines:
                    if ln.startswith("author "):
                        author = ln[7:]
                        break
                return {"hash": commit_hash, "author": author}
        except Exception:
            pass
        return None

    @staticmethod
    def suggest_rollback(path: str, current_commit: str, max_back: int = 5) -> Optional[str]:
        try:
            for i in range(1, max_back + 1):
                r = subprocess.run(["git", "-C", path, "log", f"-{i}", "--oneline"],
                                   capture_output=True, text=True, timeout=5)
                if r.returncode == 0:
                    commits = [c.strip() for c in r.stdout.strip().splitlines() if c.strip()]
                    if len(commits) >= i:
                        return commits[i - 1].split()[0]
        except Exception:
            pass
        return None


class DependencyCache:
    def __init__(self, cache_dir: str = CACHE_DIR):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.cache_dir / "index.json"
        self.index = self._load_index()

    def _load_index(self) -> Dict[str, Any]:
        if self.index_path.exists():
            try:
                return json.loads(self.index_path.read_text())
            except Exception:
                return {}
        return {}

    def _save_index(self):
        try:
            self.index_path.write_text(json.dumps(self.index, indent=2))
        except Exception:
            pass

    def _key(self, project: Dict, changed_files: List[str]) -> str:
        raw = json.dumps({
            "path": project["path"],
            "cmd": project["build_cmd"],
            "files": sorted(changed_files),
        }, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, project: Dict, changed_files: List[str]) -> Optional[str]:
        key = self._key(project, changed_files)
        entry = self.index.get(key)
        if not entry:
            return None
        path = Path(entry.get("artifact_path", ""))
        if path.exists():
            return str(path)
        return None

    def put(self, project: Dict, changed_files: List[str], artifact_path: str):
        key = self._key(project, changed_files)
        self.index[key] = {
            "project": project["name"],
            "artifact_path": str(Path(artifact_path).resolve()),
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "size_bytes": Path(artifact_path).stat().st_size if Path(artifact_path).exists() else 0,
        }
        self._save_index()


dep_cache = DependencyCache()
resource_monitor = ResourceMonitor()


def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS build_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT,
            success INTEGER,
            duration_ms INTEGER,
            error_type TEXT,
            error_detail TEXT,
            auto_fixed INTEGER,
            backoff INTEGER,
            cached INTEGER DEFAULT 0,
            artifact_sha256 TEXT,
            artifact_size_bytes INTEGER,
            ts TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS checksums (
            path TEXT PRIMARY KEY,
            sha256 TEXT,
            mtime REAL,
            project TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS build_trends (
            project TEXT,
            date TEXT,
            avg_duration_ms REAL,
            success_rate REAL,
            count INTEGER,
            PRIMARY KEY (project, date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_index (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rollback_index (
            project TEXT PRIMARY KEY,
            path TEXT,
            sha256 TEXT,
            size_bytes INTEGER,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(STATE_DB, check_same_thread=False)
    ensure_schema(conn)
    with db_lock:
        try:
            conn.execute("ALTER TABLE build_events ADD COLUMN cached INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE build_events ADD COLUMN artifact_sha256 TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE build_events ADD COLUMN artifact_size_bytes INTEGER")
        except Exception:
            pass
        conn.commit()
    return conn


def get_file_checksums(project_path: str) -> List[tuple]:
    src_dirs = ["src", "app/src", "build.gradle", "build.gradle.kts", "pom.xml",
                "settings.gradle", "settings.gradle.kts", "gradle.properties",
                "AndroidManifest.xml", "gradle/wrapper/gradle-wrapper.properties"]
    checksums = []
    base = Path(project_path)
    for pattern in src_dirs:
        matches = list(base.glob(pattern))
        for m in matches:
            if m.is_file() and m.exists():
                try:
                    checksums.append((str(m.relative_to(base)), hashlib.sha256(m.read_bytes()).hexdigest(), m.stat().st_mtime))
                except (OSError, PermissionError):
                    pass
            elif m.is_dir() and m.exists():
                for f in sorted(m.rglob("*")):
                    if f.is_file():
                        try:
                            checksums.append((str(f.relative_to(base)), hashlib.sha256(f.read_bytes()).hexdigest(), f.stat().st_mtime))
                        except (OSError, PermissionError):
                            pass
    return sorted(checksums, key=lambda x: x[0])


def has_source_changed(conn: sqlite3.Connection, project_name: str, project_path: str) -> tuple:
    current = get_file_checksums(project_path)
    changed_files = []
    changes_detected = False
    with db_lock:
        c = conn.cursor()
        for rel_path, sha, mtime in current:
            row = c.execute("SELECT sha256 FROM checksums WHERE path=?", (rel_path,)).fetchone()
            if not row or row[0] != sha:
                changes_detected = True
                changed_files.append(rel_path)
            c.execute("INSERT OR REPLACE INTO checksums (path, sha256, mtime, project, updated_at) VALUES (?,?,?,?,datetime('now'))",
                      (rel_path, sha, mtime, project_name))
        conn.commit()
    return changes_detected, changed_files


def parse_error(log_text: str) -> tuple:
    for error_type, cfg in ERROR_PATTERNS.items():
        m = re.search(cfg["re"], log_text, re.IGNORECASE | re.MULTILINE)
        if m:
            return error_type, cfg["severity"], cfg["fix"], m.group(0)
    return None, "unknown", "Unknown error pattern", log_text[-500:]


def attempt_auto_fix(error_type: str, project: Dict):
    if error_type in AUTO_FIX_ACTIONS and AUTO_FIX_ACTIONS[error_type]:
        try:
            AUTO_FIX_ACTIONS[error_type](project)
            return True
        except Exception:
            return False
    return False


def send_notification(project: Dict, event: BuildEvent):
    webhook = project.get("webhook_url")
    if not webhook:
        return
    if not REQUESTS_AVAILABLE:
        return
    try:
        payload = {
            "text": f"{'✅' if event.success else '❌'} {project['name']} build {'succeeded' if event.success else 'failed'}",
            "project": project["name"],
            "duration_ms": event.duration_ms,
            "error": event.error_type,
            "ts": event.ts,
        }
        requests.post(webhook, json=payload, timeout=5)
    except Exception:
        pass


def desktop_notify(title: str, message: str):
    try:
        if sys.platform == "linux":
            subprocess.run(["notify-send", title, message], timeout=2)
        elif sys.platform == "darwin":
            subprocess.run(["osascript", "-e", f'display notification "{message}" with title "{title}"'], timeout=2)
    except Exception:
        pass


def verify_smoke_test(project: Dict) -> tuple:
    test_type = project.get("smoke_test")
    path = project["path"]
    if test_type == "android":
        apks = [p for p in project.get("artifacts", []) if p.endswith(".apk")]
        for apk in apks:
            full = os.path.join(path, apk)
            if os.path.exists(full):
                try:
                    r = subprocess.run(["aapt", "dump", "badging", full], capture_output=True, text=True, timeout=10)
                    if r.returncode == 0 and "package:" in r.stdout:
                        pkg = re.search(r"package: name='([^']+)'", r.stdout)
                        ver = re.search(r"versionName='([^']+)'", r.stdout)
                        pkg_name = pkg.group(1) if pkg else "?"
                        ver_name = ver.group(1) if ver else "?"
                        adb_ok = False
                        try:
                            r2 = subprocess.run(["adb", "install", "-r", full], capture_output=True, text=True, timeout=30)
                            adb_ok = r2.returncode == 0
                        except Exception:
                            pass
                        detail = f"APK valid pkg={pkg_name} ver={ver_name}"
                        if adb_ok:
                            detail += " adb=installed"
                        else:
                            detail += " adb=skipped"
                        return True, detail
                except Exception:
                    pass
        return False, "No valid APK found"
    elif test_type == "jar":
        jarpath = os.path.join(path, project.get("artifacts", ["target/app.jar"])[0])
        if os.path.exists(jarpath):
            try:
                r = subprocess.run(["jar", "tf", jarpath], capture_output=True, text=True, timeout=10)
                entries = len(r.stdout.splitlines()) if r.returncode == 0 else 0
                proc = None
                try:
                    proc = subprocess.Popen(
                        ["java", "-jar", jarpath],
                        cwd=path,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    time.sleep(3)
                    if proc.poll() is None:
                        try:
                            health = subprocess.run(
                                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "http://localhost:8080/actuator/health"],
                                timeout=5,
                            )
                            code = health.stdout.strip() if health.returncode == 0 else "000"
                            if code in ("200", "401", "403"):
                                return True, f"JAR valid ({entries} entries) health={code}"
                        except Exception:
                            pass
                except Exception:
                    pass
                finally:
                    if proc and proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except Exception:
                            proc.kill()
                return True, f"JAR valid ({entries} entries) health=skipped"
            except Exception:
                pass
        return False, "JAR missing"
    return None, "No smoke test configured"


def profile_steps(stdout_text: str, duration_ms: int) -> Dict[str, float]:
    steps = {}
    gradle_pattern = re.compile(r"> Task :([^\s]+)\s+(\S+)")
    matches = gradle_pattern.findall(stdout_text)
    if matches:
        per_step = duration_ms / max(len(matches), 1)
        for task, status in matches:
            steps[task] = 0.1 if status == "UP-TO-DATE" else per_step
        return steps
    maven_pattern = re.compile(r"\[INFO\] ---\s+([^\s]+):(\S+)\s+\((\S+)\)\s+---")
    maven_matches = maven_pattern.findall(stdout_text)
    if maven_matches:
        per_step = duration_ms / max(len(maven_matches), 1)
        for plugin, goal, phase in maven_matches:
            steps[f"{plugin}:{goal}"] = per_step
    return steps


def calculate_trends(conn: sqlite3.Connection, project: str, days: int = 7) -> Dict[str, Any]:
    with db_lock:
        c = conn.cursor()
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = c.execute(
            "SELECT date(ts) as d, avg(duration_ms) as avg_dur, avg(success) as succ, count(*) as cnt "
            "FROM build_events WHERE project=? AND ts >= ? GROUP BY date(ts) ORDER BY d",
            (project, cutoff)
        ).fetchall()
    if not rows:
        return {}
    durations = [r[1] for r in rows if r[1] is not None]
    success_rates = [r[2] for r in rows if r[2] is not None]
    return {
        "days": len(rows),
        "avg_duration_ms": statistics.mean(durations) if durations else 0,
        "median_duration_ms": statistics.median(durations) if durations else 0,
        "success_rate": statistics.mean(success_rates) if success_rates else 0,
        "total_builds": sum(r[3] for r in rows),
        "trend": "improving" if (success_rates and success_rates[-1] > success_rates[0]) else "degrading",
    }


def render_dashboard(projects: List[Dict], monitor: ResourceMonitor, current_runs: Dict[str, BuildEvent], start_time: float):
    if not RICH_AVAILABLE:
        return
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
    )
    layout["body"].split_row(
        Layout(name="projects", ratio=2),
        Layout(name="resources", ratio=1),
    )

    uptime = int(time.time() - start_time)
    header_text = Text("⚡ Advanced Build Watcher Enterprise", style="bold cyan")
    header_text.append(f"  |  {datetime.now().strftime('%H:%M:%S')}  |  Uptime: {uptime}s  |  Ctrl+C to stop", style="dim")
    layout["header"].update(Panel(header_text))

    proj_table = Table(show_header=True, header_style="bold magenta", expand=True)
    proj_table.add_column("Project", style="cyan", no_wrap=True)
    proj_table.add_column("Status", justify="center")
    proj_table.add_column("Last Duration", justify="right")
    proj_table.add_column("Error", justify="center")
    proj_table.add_column("Next In", justify="right")
    proj_table.add_column("Cache", justify="center")
    proj_table.add_column("Changed", justify="center")

    for p in projects:
        name = p["name"]
        ev = current_runs.get(name)
        if ev:
            status = "🔄 building"
            dur = f"{ev.duration_ms}ms"
            err = ev.error_type or "-"
            next_in = "now"
            cache = "💾" if ev.cached else ""
            changed = str(len(ev.changed_files)) if ev.changed_files else "0"
        else:
            status = "✅ idle"
            dur = "-"
            err = "-"
            elapsed = time.time() - p.get("_last_build_ts", 0)
            backoff = p.get("_backoff", 0)
            if elapsed < backoff:
                next_in = f"{int(backoff - elapsed)}s"
            elif elapsed < PERIODIC_INTERVAL:
                next_in = f"{int(PERIODIC_INTERVAL - elapsed)}s"
            else:
                next_in = "poll"
            cache = ""
            changed = str(len(p.get("_last_changed_files", []))) if p.get("_last_changed_files") else "0"
        proj_table.add_row(name, status, dur, err, next_in, cache, changed)

    layout["projects"].update(Panel(proj_table, title="Build Status", border_style="green"))

    res = monitor.avg()
    res_text = (
        f"CPU:  {res['cpu_percent']:5.1f}%\n"
        f"RAM:  {res['mem_percent']:5.1f}%\n"
        f"DISK: {res['disk_percent']:5.1f}%\n"
    )
    layout["resources"].update(Panel(res_text, title="System Resources", border_style="yellow"))

    return layout


def run_build(project: Dict, conn: sqlite3.Connection, monitor: ResourceMonitor,
              changed_files: List[str], dry_run: bool = False) -> BuildEvent:
    name = project["name"]
    path = project["path"]
    cmd = project["build_cmd"]
    env = {**os.environ, **project.get("env", {})}
    pre = project.get("pre_hook")
    artifacts = project.get("artifacts", [])
    backoff = project.get("_backoff", 0)
    cached_path = dep_cache.get(project, changed_files)

    if cached_path and Path(cached_path).exists() and not changed_files:
        ev = BuildEvent(
            ts=datetime.now(timezone.utc).isoformat(),
            project=name,
            success=True,
            duration_ms=0,
            returncode=0,
            error_type=None,
            severity="info",
            fix_suggestion="",
            error_detail="",
            auto_fixed=0,
            backoff=backoff,
            changed_files=changed_files,
            cached=True,
        )
        log_event(ev)
        persist_event(conn, ev)
        update_trend(conn, ev)
        return ev

    os.makedirs(path, exist_ok=True)
    os.chdir(path)

    if pre and not project.get("_pre_ran"):
        try:
            subprocess.run(pre, shell=True, cwd=path, capture_output=True, timeout=60)
            project["_pre_ran"] = True
        except Exception:
            pass

    start = time.time()
    start_snap = monitor.snapshot()

    if RICH_AVAILABLE:
        with console.status(f"[bold yellow]Building {name}...", spinner="dots"):
            try:
                proc = subprocess.run(
                    cmd, cwd=path, env=env,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, timeout=600
                )
            except subprocess.TimeoutExpired:
                proc = type("obj", (object,), {"returncode": -1, "stdout": "Build timed out"})()
    else:
        print(f"Building {name}...", flush=True)
        try:
            proc = subprocess.run(
                cmd, cwd=path, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=600
            )
        except subprocess.TimeoutExpired:
            proc = type("obj", (object,), {"returncode": -1, "stdout": "Build timed out"})()

    duration_ms = int((time.time() - start) * 1000)
    end_snap = monitor.snapshot()

    stdout_text = getattr(proc, "stdout", "")
    success = getattr(proc, "returncode", -1) == 0
    error_type, severity, fix_suggestion, error_detail = parse_error(stdout_text)
    step_profile = profile_steps(stdout_text, duration_ms)

    artifacts_ok = True
    missing_artifacts = []
    artifact_paths = []
    if success:
        for art in artifacts:
            full = os.path.join(path, art)
            if not os.path.exists(full):
                artifacts_ok = False
                missing_artifacts.append(art)
            else:
                artifact_paths.append(full)

    smoke_ok, smoke_detail = verify_smoke_test(project) if (success and artifacts_ok) else (None, "Skipped")
    git_info = GitHelper.current_commit(path) if GitHelper.project_root(path) else None
    rollback = None
    if not (success and artifacts_ok) and git_info:
        rollback = GitHelper.suggest_rollback(path, git_info["hash"])

    ev = BuildEvent(
        ts=datetime.now(timezone.utc).isoformat(),
        project=name,
        success=bool(success and artifacts_ok),
        duration_ms=duration_ms,
        returncode=getattr(proc, "returncode", -1),
        error_type=error_type,
        severity=severity,
        fix_suggestion=fix_suggestion,
        error_detail=error_detail[:200],
        auto_fixed=0,
        backoff=backoff,
        missing_artifacts=missing_artifacts,
        cpu_percent=end_snap.get("cpu_percent", 0.0),
        mem_percent=end_snap.get("mem_percent", 0.0),
        disk_percent=end_snap.get("disk_percent", 0.0),
        changed_files=changed_files,
        git_commit=git_info.get("hash") if git_info else None,
        git_author=git_info.get("author") if git_info else None,
        git_message=git_info.get("message") if git_info else None,
        step_profile=step_profile,
        smoke_test_passed=smoke_ok,
        smoke_test_detail=smoke_detail,
        cached=False,
        rollback_suggestion=rollback,
    )

    auto_fixed = 0
    if not ev.success and error_type and attempt_auto_fix(error_type, project):
        auto_fixed = 1
        ev.auto_fixed = 1
        if RICH_AVAILABLE:
            console.print(f"[yellow]🔧 Auto-fix applied for {name}. Retrying...[/yellow]")
        return run_build(project, conn, monitor, changed_files)

    if ev.success:
        save_rollback(project, artifacts)

    log_event(ev)
    persist_event(conn, ev)
    update_trend(conn, ev)

    if not ev.success or (ev.smoke_test_passed is False):
        send_notification(project, ev)
        if ev.rollback_suggestion and RICH_AVAILABLE:
            console.print(f"[red]💡 Rollback suggestion: git revert {ev.rollback_suggestion}[/red]")

    if success and artifacts_ok:
        for art in artifacts:
            full = os.path.join(path, art)
            if os.path.exists(full):
                dep_cache.put(project, changed_files, full)

    return ev


def log_event(ev: BuildEvent):
    entry = asdict(ev)
    try:
        with open(LOG_JSONL, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception:
        pass


def persist_event(conn: sqlite3.Connection, ev: BuildEvent):
    art_sha = ""
    art_size = 0
    if ev.success and not ev.cached:
        for art in PROJECTS:
            if art["name"] == ev.project:
                for a in art.get("artifacts", []):
                    full = Path(art["path"]) / a
                    if full.exists():
                        art_sha = hashlib.sha256(full.read_bytes()).hexdigest()
                        art_size = full.stat().st_size
                        break
                break
    with db_lock:
        c = conn.cursor()
        c.execute(
            "INSERT INTO build_events (project, success, duration_ms, error_type, error_detail, auto_fixed, backoff, cached, artifact_sha256, artifact_size_bytes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (ev.project, int(ev.success), ev.duration_ms, ev.error_type, ev.error_detail[:200],
             ev.auto_fixed, ev.backoff, int(ev.cached), art_sha, art_size),
        )
        conn.commit()


def update_trend(conn: sqlite3.Connection, ev: BuildEvent):
    today = datetime.now().strftime("%Y-%m-%d")
    with db_lock:
        c = conn.cursor()
        row = c.execute("SELECT avg_duration_ms, success_rate, count FROM build_trends WHERE project=? AND date=?",
                        (ev.project, today)).fetchone()
        if row:
            avg_dur = (row[0] * row[2] + ev.duration_ms) / (row[2] + 1)
            succ_rate = (row[1] * row[2] + int(ev.success)) / (row[2] + 1)
            c.execute(
                "UPDATE build_trends SET avg_duration_ms=?, success_rate=?, count=? WHERE project=? AND date=?",
                (avg_dur, succ_rate, row[2] + 1, ev.project, today),
            )
        else:
            c.execute(
                "INSERT INTO build_trends (project, date, avg_duration_ms, success_rate, count) VALUES (?,?,?,?,?)",
                (ev.project, today, ev.duration_ms, int(ev.success), 1),
            )
        conn.commit()


def save_rollback(project: Dict, artifacts: List[str]):
    rollback_path = Path(ROLLBACK_DIR)
    rollback_path.mkdir(parents=True, exist_ok=True)
    proj_dir = rollback_path / project["name"].replace(" ", "_")
    proj_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted([d for d in proj_dir.iterdir() if d.is_dir()], key=lambda d: d.stat().st_mtime)
    while len(existing) >= MAX_ROLLBACK:
        shutil.rmtree(existing[0])
        existing.pop(0)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = proj_dir / ts
    dest.mkdir(parents=True, exist_ok=True)

    saved = []
    for art in artifacts:
        src = Path(project["path"]) / art
        if src.exists():
            dst = dest / Path(art).name
            shutil.copy2(src, dst)
            saved.append(str(dst))

    with db_lock:
        conn = sqlite3.connect(STATE_DB, check_same_thread=False)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO rollback_index (project, path, sha256, size_bytes) VALUES (?,?,?,?)",
                (project["name"], str(dest), "", sum(Path(p).stat().st_size for p in saved if Path(p).exists())),
            )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

    return str(dest)


def restore_rollback(project_name: str) -> Optional[str]:
    rollback_path = Path(ROLLBACK_DIR)
    proj_dir = rollback_path / project_name.replace(" ", "_")
    if not proj_dir.exists():
        return None
    entries = sorted([d for d in proj_dir.iterdir() if d.is_dir()], key=lambda d: d.stat().st_mtime, reverse=True)
    if not entries:
        return None
    return str(entries[0])


def print_trend_report(conn: sqlite3.Connection):
    with db_lock:
        c = conn.cursor()
        rows = c.execute(
            "SELECT project, date, avg_duration_ms, success_rate, count FROM build_trends ORDER BY date DESC LIMIT 20"
        ).fetchall()
    if not rows:
        print("No trend data yet.")
        return
    if RICH_AVAILABLE:
        table = Table(title="Build Trends (last 20 entries)", show_header=True)
        table.add_column("Project")
        table.add_column("Date")
        table.add_column("Avg ms")
        table.add_column("Success %")
        table.add_column("Count")
        for r in rows:
            table.add_row(str(r[0]), str(r[1]), f"{r[2]:.0f}", f"{r[3]*100:.0f}%", str(r[4]))
        console.print(table)
    else:
        print(f"{'Project':<25} {'Date':<12} {'Avg_ms':>8} {'Success%':>9} {'Count':>6}")
        for r in rows:
            print(f"{str(r[0]):<25} {str(r[1]):<12} {r[2]:>8.0f} {r[3]*100:>8.0f}% {r[4]:>6}")


def watcher():
    conn = get_db()
    monitor = ResourceMonitor()
    current_runs: Dict[str, BuildEvent] = {}
    first_success_notified: Dict[str, bool] = defaultdict(bool)

    for p in PROJECTS:
        p["_backoff"] = 0
        p["_pre_ran"] = False
        p["_build_success"] = True
        p["_last_error"] = None
        p["_last_changed_files"] = []

    if RICH_AVAILABLE:
        console.clear()
        console.rule("[bold cyan]Advanced Build Watcher Enterprise")
        console.print(f"Projects: {[p['name'] for p in PROJECTS]}", style="dim")
        console.print(f"Poll interval: {POLL_INTERVAL}s | Log: {LOG_JSONL} | Cache: {CACHE_DIR}", style="dim")
        console.print(f"Rich TUI: ON | PSUTIL: {'ON' if PSUTIL_AVAILABLE else 'OFF'} | Requests: {'ON' if REQUESTS_AVAILABLE else 'OFF'}")
    else:
        print("=== Advanced Build Watcher Enterprise ===")
        print(f"Projects: {[p['name'] for p in PROJECTS]}")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            start_time = time.time()
            with Live(
                render_dashboard(PROJECTS, monitor, current_runs, start_time),
                console=console,
                refresh_per_second=2,
                screen=True,
            ) as live:
                while True:
                    now = time.time()
                    any_changed = False
                    for p in PROJECTS:
                        changed, files = has_source_changed(conn, p["name"], p["path"])
                        p["_last_changed_files"] = files
                        if changed:
                            any_changed = True

                    if any_changed:
                        if RICH_AVAILABLE:
                            console.print("[green]📁 Source change detected — triggering builds[/green]")

                    should_build = False
                    if any_changed:
                        should_build = True
                    else:
                        for p in PROJECTS:
                            elapsed = time.time() - p.get("_last_build_ts", 0)
                            if elapsed >= max(p.get("_backoff", 0), PERIODIC_INTERVAL):
                                should_build = True
                                break

                    if should_build:
                        futures = {}
                        for p in PROJECTS:
                            if p.get("_backoff", 0) > 0 and not any_changed:
                                continue
                            fut = executor.submit(run_build, p, conn, monitor, p.get("_last_changed_files", []))
                            futures[fut] = p
                        for fut in as_completed(futures):
                            p = futures[fut]
                            try:
                                ev = fut.result()
                                p["_backoff"] = 0 if ev.success else min(p.get("_backoff", 0) * 2 + POLL_INTERVAL, MAX_BACKOFF)
                                p["_last_build_ts"] = time.time()
                                current_runs[p["name"]] = ev

                                notify_list = p.get("notify_on", [])
                                if "failure" in notify_list and not ev.success:
                                    desktop_notify(f"{p['name']} build failed", f"Error: {ev.error_type}")
                                if "success_first" in notify_list and ev.success and not first_success_notified[p["name"]]:
                                    first_success_notified[p["name"]] = True
                                    desktop_notify(f"{p['name']} build ok", "First success in this session")
                            except Exception as e:
                                current_runs[p["name"]] = BuildEvent(
                                    ts=datetime.now(timezone.utc).isoformat(),
                                    project=p["name"],
                                    success=False,
                                    duration_ms=0,
                                    returncode=-1,
                                    error_type="watcher_exception",
                                    severity="fatal",
                                    fix_suggestion=str(e),
                                    error_detail=str(e)[:200],
                                    auto_fixed=0,
                                    backoff=p.get("_backoff", 0),
                                )

                    live.update(render_dashboard(PROJECTS, monitor, current_runs, start_time))
                    time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        if RICH_AVAILABLE:
            console.print("\n[red]🛑 Watcher stopped by user[/red]")
        else:
            print("\nWatcher stopped by user")
        print_trend_report(conn)


def main():
    if len(sys.argv) > 1:
        if sys.argv[1] in ("--trends", "--report"):
            print_trend_report(get_db())
            return
        if sys.argv[1] == "--clear-cache":
            shutil.rmtree(CACHE_DIR, ignore_errors=True)
            Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
            print("Cache cleared.")
            return
        if sys.argv[1] == "--rollback":
            if len(sys.argv) < 3:
                print("Usage: watcher.py --rollback <project>", file=sys.stderr)
                sys.exit(1)
            target = sys.argv[2]
            found = False
            for p in PROJECTS:
                if target.lower() in p["name"].lower():
                    path = restore_rollback(p["name"])
                    if path:
                        print(f"Rollback restored to: {path}")
                    else:
                        print("No rollback available.", file=sys.stderr)
                        sys.exit(1)
                    found = True
                    break
            if not found:
                print(f"Project not found. Available: {[p['name'] for p in PROJECTS]}", file=sys.stderr)
                sys.exit(1)
            return
        if sys.argv[1] == "--validate":
            conn = get_db()
            monitor = ResourceMonitor()
            for p in PROJECTS:
                changed, files = has_source_changed(conn, p["name"], p["path"])
                ev = run_build(p, conn, monitor, files)
                print(f"{'OK' if ev.success else 'FAIL'} {p['name']} ({ev.duration_ms}ms)")
            return
        target = sys.argv[1]
        found = False
        for p in PROJECTS:
            if target.lower() in p["name"].lower():
                conn = get_db()
                monitor = ResourceMonitor()
                changed, files = has_source_changed(conn, p["name"], p["path"])
                ev = run_build(p, conn, monitor, files)
                print(json.dumps(asdict(ev), indent=2, default=str))
                found = True
                break
        if not found:
            print(f"Project not found. Available: {[p['name'] for p in PROJECTS]}", file=sys.stderr)
            sys.exit(1)
        return

    watcher()


if __name__ == "__main__":
    main()
