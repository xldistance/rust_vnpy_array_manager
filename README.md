## 速度能提升一些，不如用rust重构因子速度提升的多，所以首先请用rust重构你策略中用到的因子

## 前置要求

### 安装 Rust

```bash
# Linux/macOS
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Windows
# 下载并运行 rustup-init.exe
# https://rustup.rs/
```

安装完成后，重启终端并验证安装：

```bash
rustc --version
cargo --version
```
## 安装步骤

```bash
pip install maturin
```
**生产构建：**
```bash
maturin build --release
pip install target/wheels/*.whl
```
## 使用示例
```
# buffer_size缓存数组数量,additional_status 计算额外因子状态,只使用ohlc因子是设置为False
from rust_array_manager import ArrayManager
am = ArrayManager(buffer_size=100, additional_status=False)
am.update_bar(bar)
```
