# 销售数据仪表板

自动化销售数据分析工具 —— 原始数据 → 智能清洗 → 描述型分析 → 交互式仪表板

## 快速开始

```bash
# 1. 安装依赖
pip3 install -r requirements.txt

# 2. 创建管理员账号
python3 admin.py add-user --name admin --password your_password

# 3. 启动服务
python3 main.py
```

浏览器访问 `http://localhost:8000`

## 功能

- **字段智能识别**：支持中英文表头自动映射，规则表覆盖 80% 常见格式
- **数据清洗**：去重、日期标准化、金额格式清理、空值处理
- **4 大分析模块**：
  - 销售趋势与节奏（日/周/月自动识别 + 环比）
  - 经销商贡献与集中度（TOP 排名 + 帕累托分析）
  - 产品/品类表现（ABC 分类 + 价格带分布）
  - 订单特征（客单价分布 + 异常单标注）
- **交互式仪表板**：深色主题、图表导出（SVG/PNG）、筛选联动
- **认证系统**：bcrypt 密码哈希 + HttpOnly Cookie 会话管理

## 管理员工具

```bash
# 列出所有用户
python3 admin.py list-users

# 禁用用户
python3 admin.py disable-user --name 张三

# 启用用户
python3 admin.py enable-user --name 张三

# 重置密码
python3 admin.py reset-password --name 张三 --password newpass
```

## 生成示例数据

```bash
python3 generate_sample.py
```

## 技术栈

- **后端**: Python 3.14 + FastAPI + Polars
- **前端**: HTML/JS + ECharts 5.5
- **认证**: bcrypt + itsdangerous (HttpOnly Cookie)
- **数据**: SQLite (用户存储) + Polars (数据处理)

## 项目结构

```
sales-dashboard/
├── main.py              # FastAPI 入口、路由
├── auth.py              # 认证模块
├── admin.py             # 管理员工具
├── generate_sample.py   # 示例数据生成
├── requirements.txt     # 依赖清单
├── core/
│   ├── config.py        # 字段映射表、阈值配置
│   ├── field_mapper.py  # 字段识别 & 映射
│   ├── cleaner.py       # 数据清洗管道
│   └── analyzer.py      # 聚合计算（Day 2）
├── frontend/
│   ├── index.html       # 登录页
│   └── dashboard.html   # 仪表板
├── uploads/             # 临时上传目录
└── tests/
```

## 开发里程碑

- [x] Day 1: 字段映射 + 数据清洗 + 认证核心 + 前端框架
- [ ] Day 2: 分析引擎（4 大模块聚合 + 描述生成）
- [ ] Day 3: 端到端联调 + 图表渲染
- [ ] Day 4: 图表导出 + 筛选联动
- [ ] Day 5: 性能优化 + 细节打磨
