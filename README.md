# Build Watcher Enterprise

Advanced build watcher with smart change detection, concurrent builds, error parsing, auto-fixes, exponential backoff, artifact verification, structured logging, SQLite state tracking, git-aware change mapping, system resource monitoring, post-build smoke tests, trend analytics, notification hooks, build profiling, automatic rollback suggestions, dependency cache layer, and real-time terminal UI.

## Usage

```bash
# Run watcher (rebuilds on changes + every 5 minutes)
python3 watcher.py

# One-shot build for a specific project
python3 watcher.py NetPulseAndroid

# Show trend report
python3 watcher.py --trends

# Rollback last good build
python3 watcher.py --rollback NetPulseAndroid

# Clear build cache
python3 watcher.py --clear-cache

# Run validation only
python3 watcher.py --validate
```

## Features

- **Smart rebuilds**: SHA-256 source checksums, only builds on actual changes
- **Concurrent builds**: ThreadPoolExecutor for parallel project builds
- **Error parsing**: 8 common error patterns with auto-fix capabilities
- **Exponential backoff**: Up to 600s on repeated failures
- **Artifact verification**: Confirms APK/JAR exists post-build
- **Structured logging**: JSONL + SQLite for all build events
- **Git integration**: Commit blame, changed files, rollback suggestions
- **Resource monitoring**: CPU/RAM/Disk snapshots via psutil
- **Smoke tests**: APK manifest validation, JAR entry checks
- **Dependency cache**: Avoids redundant rebuilds when nothing changed
- **Rich TUI**: Real-time dashboard with build status and resources
- **Notifications**: Desktop + webhook on failures/successes
