# 工程化基础改造设计

## 目标

将行情数据管道整理为可复现安装、可发现运行、可自动验证的学习项目标准版，同时保持现有采集、落库和分析行为不变。

## 范围

- 使用 `uv` 管理虚拟环境、依赖同步和锁文件。
- 注册标准库 `argparse` 实现的 `mktdata` 命令行入口。
- 支持 `python -m mktdata` 作为等价入口。
- 保留 `scripts/run_pipeline.py`，使现有脚本调用继续可用。
- 固化测试、Ruff、mypy 和 GitHub Actions 质量检查。
- 更新项目文档与忽略规则。

不包含 Docker、服务化、监控、交易所扩展或行情业务逻辑变更。

## 架构

命令行解析与调度从脚本移动到 `mktdata.cli`。`cli.main()` 是唯一的命令实现，调用既有 `sources`、`sink` 与 `analyze` 模块；`__main__` 和历史脚本只转发到该函数，防止多入口逻辑分叉。

```text
uv run mktdata / uv run python -m mktdata / scripts/run_pipeline.py
                         |
                   mktdata.cli:main
                         |
        sources -> schema -> sink (Parquet) -> analyze (DuckDB)
```

## 接口与兼容性

- 分发命令：`mktdata = "mktdata.cli:main"`。
- 模块入口：`python -m mktdata` 调用同一个 `main()`。
- 兼容入口：`python scripts/run_pipeline.py` 调用同一个 `main()`。
- 继续支持 `--mode sim|live|kline`、`--symbol`、`--n`、`--interval`、`--out` 与 `--batch-size`。

## 开发工作流

```bash
uv sync --all-groups
uv run mktdata --mode sim --n 100
uv run pytest
uv run ruff check .
uv run mypy src
```

`uv.lock` 必须提交，以让开发机和 CI 使用相同的依赖解析结果。项目最低 Python 版本保持 `3.10`；CI 测试 Python `3.10` 与 `3.12`。

## 质量保障

- 为三个入口添加测试，验证它们的帮助命令可运行，并验证 CLI 的 `sim` 模式能生成 Parquet。
- GitHub Actions 在 push 与 pull request 中安装 uv、同步锁定依赖，并执行 pytest、Ruff 与 mypy。
- `.gitignore` 忽略本地环境、缓存、生成的数据和项目内 worktree。

## 错误处理

保留当前的 argparse 参数校验和数据源错误处理。CLI 不吞掉异常：依赖、网络或数据写入错误应以非零退出状态暴露给本地用户和 CI。
