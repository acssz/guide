# 使用说明
克隆Git仓库

```
git clone https://github.com/acssz/guide
cd guide
```

安装Python包，推荐使用virtualenv

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

设置环境变量

```
export LARK_APP_ID=<app-id>
export LARK_APP_SECRET=<app-secret>
```

```
python main_async.py
```
