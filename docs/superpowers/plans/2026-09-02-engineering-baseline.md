# Engineering Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `mktdata-pipeline` a reproducible uv workflow, standard CLI entry points, and automated quality checks without changing its market-data behavior.

**Architecture:** Move the existing command parsing and orchestration into `mktdata.cli:main`. Make the installed `mktdata` command, `python -m mktdata`, and the legacy script delegate to that one implementation. Lock dependencies with uv and run the same test/lint/type-check commands locally and in GitHub Actions.

**Tech Stack:** Python 3.10+, argparse, uv, pytest, Ruff, mypy, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-02-engineering-baseline-design.md`

## Global Constraints

- Keep `requires-python = ">=3.10"`.
- Add no runtime dependency for CLI parsing; use the existing standard-library `argparse` approach.
- Preserve all existing CLI flags and modes: `sim`, `live`, `kline`, `--symbol`, `--n`, `--interval`, `--out`, `--batch-size`.
- The installed command, module command, and legacy script must all call `mktdata.cli:main`.
- Submit `uv.lock`; CI must use Python 3.10 and 3.12.

---

## File Structure

- Create: `src/mktdata/cli.py` — argument parsing and pipeline orchestration, moved from the legacy script.
- Create: `src/mktdata/__main__.py` — module launcher that calls `cli.main`.
- Modify: `scripts/run_pipeline.py` — compatibility shim that calls `cli.main`.
- Modify: `pyproject.toml` — `mktdata` console-script entry point and uv dev dependency group.
- Modify: `tests/test_pipeline.py` — CLI behavior and compatibility tests.
- Create: `.github/workflows/quality.yml` — test, lint, and type-check workflow.
- Modify: `README.md` — uv-first installation and command documentation.
- Create: `uv.lock` — resolved, reproducible dependency graph.

### Task 1: Establish the testable CLI entry point

**Files:**
- Create: `src/mktdata/cli.py`
- Create: `src/mktdata/__main__.py`
- Modify: `scripts/run_pipeline.py`
- Modify: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `mktdata.sources.sim_stream`, `live_stream`, `kline_stream`; `mktdata.sink.write_parquet`, `write_klines`, `ParquetBatchWriter`; `mktdata.analyze.report`, `report_klines`.
- Produces: `mktdata.cli.main() -> None`, callable by all three entry points.

- [ ] **Step 1: Write failing CLI tests**

Append tests that execute the installed/module-equivalent interface with a temporary output path:

```python
def test_cli_sim_writes_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["mktdata", "--mode", "sim", "--n", "3", "--out", str(tmp_path / "q.parquet")])
    main()
    assert (tmp_path / "q.parquet").exists()

def test_module_launcher_exports_main():
    from mktdata.__main__ import main as module_main
    assert module_main is main
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run: `pytest tests/test_pipeline.py -k 'cli_sim or module_launcher' -v`

Expected: collection failure because `mktdata.cli` and `mktdata.__main__` do not exist.

- [ ] **Step 3: Implement the shared CLI**

Move the current `main`, `collect_live`, and `_collect` implementations from `scripts/run_pipeline.py` into `src/mktdata/cli.py` without changing argument defaults or mode behavior. Add:

```python
# src/mktdata/__main__.py
from mktdata.cli import main

if __name__ == "__main__":
    main()
```

Replace the script body with:

```python
from mktdata.cli import main

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the focused tests and verify they pass**

Run: `pytest tests/test_pipeline.py -k 'cli_sim or module_launcher' -v`

Expected: PASS; the simulated command creates a three-row Parquet file.

- [ ] **Step 5: Commit the CLI task**

```bash
git add src/mktdata/cli.py src/mktdata/__main__.py scripts/run_pipeline.py tests/test_pipeline.py
git commit -m "feat: add mktdata command entry point"
```

### Task 2: Add uv metadata, lockfile, and installed command verification

**Files:**
- Modify: `pyproject.toml`
- Create: `uv.lock`
- Modify: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `mktdata.cli.main() -> None` from Task 1.
- Produces: console script `mktdata` mapped to `mktdata.cli:main`; an uv environment reproducible from `uv.lock`.

- [ ] **Step 1: Write the failing packaging assertion**

Add a test that reads `pyproject.toml` and requires the entry point:

```python
def test_project_declares_mktdata_console_script():
    data = tomllib.loads(Path("pyproject.toml").read_text())
    assert data["project"]["scripts"]["mktdata"] == "mktdata.cli:main"
```

- [ ] **Step 2: Run the assertion and verify it fails**

Run: `pytest tests/test_pipeline.py::test_project_declares_mktdata_console_script -v`

Expected: FAIL with missing `scripts` metadata.

- [ ] **Step 3: Declare and synchronize project tooling**

Add to `pyproject.toml`:

```toml
[project.scripts]
mktdata = "mktdata.cli:main"

[dependency-groups]
dev = ["pytest>=8", "ruff>=0.4", "mypy>=1.10"]
```

Remove the now-redundant `project.optional-dependencies.dev` declaration, then run `uv lock` to create `uv.lock`.

- [ ] **Step 4: Verify metadata and the installed command**

Run:

```bash
uv sync --all-groups
uv run pytest tests/test_pipeline.py::test_project_declares_mktdata_console_script -v
uv run mktdata --mode sim --n 3 --out /tmp/mktdata-cli-check.parquet
```

Expected: all commands exit 0 and `/tmp/mktdata-cli-check.parquet` exists.

- [ ] **Step 5: Commit the uv task**

```bash
git add pyproject.toml uv.lock tests/test_pipeline.py
git commit -m "build: add uv workflow and console script"
```

### Task 3: Document and automate the quality baseline

**Files:**
- Create: `.github/workflows/quality.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: `uv.lock`, `mktdata` console script, pytest, Ruff, and mypy configured by Tasks 1-2.
- Produces: GitHub Actions checks for Python 3.10 and 3.12, plus documented local commands.

- [ ] **Step 1: Write failing documentation/configuration tests**

Add tests that assert the checked-in workflow and README advertise the intended commands:

```python
def test_quality_workflow_targets_supported_python_versions():
    workflow = Path(".github/workflows/quality.yml").read_text()
    assert '"3.10"' in workflow
    assert '"3.12"' in workflow
    assert "uv run pytest" in workflow
    assert "uv run ruff check ." in workflow
    assert "uv run mypy src" in workflow
```

- [ ] **Step 2: Run the new test and verify it fails**

Run: `pytest tests/test_pipeline.py::test_quality_workflow_targets_supported_python_versions -v`

Expected: FAIL because the workflow file is absent.

- [ ] **Step 3: Add GitHub Actions and refresh README**

Create a workflow triggered by `push` and `pull_request`; use `astral-sh/setup-uv`, matrix Python 3.10/3.12, `uv sync --all-groups --locked`, then run pytest, Ruff, and mypy. Replace README setup commands with the uv-first commands from the approved design, retaining an explicit `venv + pip` fallback.

- [ ] **Step 4: Run the documentation/configuration test**

Run: `uv run pytest tests/test_pipeline.py::test_quality_workflow_targets_supported_python_versions -v`

Expected: PASS.

- [ ] **Step 5: Commit the quality baseline**

```bash
git add .github/workflows/quality.yml README.md tests/test_pipeline.py
git commit -m "ci: add Python quality workflow"
```

### Task 4: Verify the completed project

**Files:**
- Verify only: all files changed by Tasks 1-3.

**Interfaces:**
- Consumes: all three entry points, uv lockfile, test suite, and CI workflow.
- Produces: verified engineering baseline.

- [ ] **Step 1: Run format/lint/type/test checks**

Run:

```bash
uv run pytest
uv run ruff check .
uv run mypy src
```

Expected: each command exits 0.

- [ ] **Step 2: Exercise all runtime entry points**

Run:

```bash
uv run mktdata --mode sim --n 5 --out /tmp/mktdata-command.parquet
uv run python -m mktdata --mode sim --n 5 --out /tmp/mktdata-module.parquet
uv run python scripts/run_pipeline.py --mode sim --n 5 --out /tmp/mktdata-legacy.parquet
```

Expected: each command exits 0 and creates its corresponding Parquet file.

- [ ] **Step 3: Check the final diff and history**

Run:

```bash
git status --short
git log --oneline main..HEAD
git diff --check main...HEAD
```

Expected: no whitespace errors; history contains the design, CLI, build, and CI commits.
