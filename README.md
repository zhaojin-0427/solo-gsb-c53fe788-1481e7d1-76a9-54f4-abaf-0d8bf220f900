# 本地容器制品仓库 API

一个使用 **Python + FastAPI + SQLite + 本地文件存储** 实现的、遵循
[OCI Distribution Spec](https://github.com/opencontainers/distribution-spec)
风格的本地容器制品（blob / manifest）仓库。支持 OCI 风格的多级仓库命名空间
（如 `acme/team/web`）、分块上传与断点续传、按摘要全局去重、不可变 manifest、
标签 CAS（compare-and-swap）移动，以及带可达图、可复核 dry-run 的安全垃圾回收。

> 单进程、零外部依赖（SQLite + 本地磁盘卷），适合本地开发、内网制品暂存与教学。

---

## 1. 快速开始：Docker Compose 一键启动

```bash
docker compose up -d --build
```

服务监听 `http://localhost:8080`，所有持久化数据位于命名卷 `registry-data`
（容器内挂载到 `/data`）：

```
/data
├── registry.db            # SQLite 元数据（WAL 模式）
├── blobs/sha256/<ab>/…    # 内容寻址（CAS）存储，全局去重
├── uploads/<uuid>.part    # 进行中的分块上传
└── staging/               # 单请求体的暂存文件
```

健康检查与版本握手：

```bash
curl -fsS http://localhost:8080/healthz
# {"status":"ok"}

curl -fsS http://localhost:8080/v2/
# {}
```

停止 / 清理：

```bash
docker compose down          # 停止并删除容器（保留数据卷）
docker compose down -v       # 同时删除数据卷（清空仓库）
```

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REGISTRY_DATA_DIR` | `/data` | 数据根目录（blob 文件 + SQLite） |
| `REGISTRY_DB_PATH` | `$REGISTRY_DATA_DIR/registry.db` | SQLite 数据库路径 |
| `REGISTRY_UPLOAD_TTL_SECONDS` | `86400` | 上传/提交租约有效期；过期会话可回收，且不再作为 GC 根 |
| `REGISTRY_GC_PLAN_TTL_SECONDS` | `7200` | dry-run 计划的有效期 |
| `REGISTRY_GC_TOKEN` | _(空)_ | 设置后 `POST /gc` 需携带 `Authorization: Bearer <token>` |

### 不用容器直接运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
REGISTRY_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8080
```

---

## 2. API 总览

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /v2/{repo}/blobs/uploads/` | 创建上传会话（支持跨仓库 `mount`、单请求整包上传） |
| `PATCH /v2/{repo}/blobs/uploads/{uuid}` | 追加一个数据块（可指定 `Content-Range`） |
| `PUT /v2/{repo}/blobs/uploads/{uuid}?digest=<d>` | 完成上传并按摘要校验、去重落盘 |
| `GET /v2/{repo}/blobs/uploads/{uuid}` | 查询上传进度（断点续传） |
| `HEAD/GET /v2/{repo}/blobs/{digest}` | 查询/拉取 blob（GET 支持 `Range`） |
| `PUT /v2/{repo}/manifests/{ref}` | 发布不可变 manifest（`ref` 为 tag 或 digest） |
| `HEAD/GET /v2/{repo}/manifests/{ref}` | 拉取 manifest |
| `GET /v2/{repo}/tags/list` | 标签列表 |
| `POST /gc` | **默认 dry-run** 生成可复核计划；`{"execute":true,"plan_id":...}` 执行 |
| `GET /gc/plans/{id}` | 取回 dry-run 计划 |
| `GET /healthz` | 健康检查 |

错误响应统一为 OCI 风格 JSON：

```json
{"errors":[{"code":"DIGEST_INVALID","message":"...","detail":{...}}]}
```

---

## 3. curl 示例：分块上传 → 发布 → 拉取

设仓库为 `acme/team/web`，服务地址为 `http://localhost:8080`。

### 3.1 计算摘要并准备数据

```bash
REPO=acme/team/web
BASE=http://localhost:8080

# 一个“配置” blob 和一个“层” blob
echo '{"architecture":"amd64","os":"linux"}' > config.json
head -c 200000 /dev/urandom | gzip -1 > layer.tar.gz

CONFIG_DIGEST=sha256:$(sha256sum config.json | cut -d' ' -f1)
LAYER_DIGEST=sha256:$(sha256sum layer.tar.gz | cut -d' ' -f1)
echo "$CONFIG_DIGEST"; echo "$LAYER_DIGEST"
```

### 3.2 创建上传会话

```bash
curl -fsS -X POST "$BASE/v2/$REPO/blobs/uploads/" -i
# HTTP/1.1 202 Accepted
# Location: /v2/acme/team/web/blobs/uploads/3f9c…
# Docker-Upload-UUID: 3f9c…
# Upload-Expires-At: 2026-09-30T12:00:00Z

UPLOAD='<Location 的值>'
```

### 3.3 分块 PATCH 上传（断点续传）

```bash
# 切成 64KiB 的块依次 PATCH；Content-Range 为字节闭区间
split -b 65536 -d layer.tar.gz chunk-
offset=0; size=$(stat -c%s layer.tar.gz)
for c in chunk-*; do
  len=$(stat -c%s "$c")
  end=$((offset + len - 1))
  curl -fsS -X PATCH "$BASE$UPLOAD" \
    --data-binary "@$c" \
    -H "Content-Range: bytes $offset-$end/$size" -i | grep -Ei '^(HTTP|Range|Location)'
  offset=$((end + 1))
done
```

如果连接中断或某块没传对：

```bash
# 查询已确认的偏移量（Range: 0-<offset-1>），随后从该偏移继续 PATCH
curl -fsS "$BASE$UPLOAD" -i | grep -i range
```

偏移不匹配返回 `416 RANGE_INVALID`，未被确认的字节会在服务端回滚，
客户端按返回的当前位置续传即可（见 `GET uploads/{uuid}`）。

### 3.4 PUT 完成上传（强制摘要校验 + 去重）

```bash
curl -fsS -X PUT "$BASE$UPLOAD?digest=$LAYER_DIGEST" -i
# HTTP/1.1 201 Created
# Location: /v2/acme/team/web/blobs/sha256:…
# Docker-Content-Digest: sha256:…
```

摘要不一致 → `400 DIGEST_INVALID`，坏数据绝不会进入 CAS。
配置文件很小，可以用**单请求整包上传**：

```bash
curl -fsS -X POST "$BASE/v2/$REPO/blobs/uploads/?digest=$CONFIG_DIGEST" \
  --data-binary @config.json -i | grep -Ei '^(HTTP|Docker)'
```

跨仓库复用（去重挂载）：

```bash
curl -fsS -X POST \
  "$BASE/v2/other/repo/blobs/uploads/?mount=$LAYER_DIGEST&from=$REPO" -i
# 201 Created：同一物理文件，仅新增一行仓库归属关系
```

### 3.5 校验并发布不可变 manifest

```bash
cat > manifest.json <<EOF
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {"mediaType":"application/vnd.oci.image.config.v1+json",
             "size": $(stat -c%s config.json), "digest": "$CONFIG_DIGEST"},
  "layers": [
    {"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip",
     "size": $(stat -c%s layer.tar.gz), "digest": "$LAYER_DIGEST"}
  ]
}
EOF

MT=application/vnd.oci.image.manifest.v1+json
curl -fsS -X PUT "$BASE/v2/$REPO/manifests/v1.0.0" \
  -H "Content-Type: $MT" --data-binary @manifest.json -i
# HTTP/1.1 201 Created
# Docker-Content-Digest: sha256:<manifest digest>
```

发布时服务端在同一事务内校验：

* 每个 config / layer 描述符的 **blob 已存在于本仓库**；
* 描述符 `size` 与实际内容一致；
* image index 的子 manifest、`subject` 引用均存在。

任一引用缺失 → `400 BLOB_UNKNOWN` / `MANIFEST_UNKNOWN`，发布整体失败，
仓库中**不可能出现悬空引用**。同一摘要的内容不可变；按 digest 重复 PUT 是幂等的。

### 3.6 拉取

```bash
M=$(curl -fsS "$BASE/v2/$REPO/manifests/v1.0.0" \
      -H 'Accept: application/vnd.oci.image.manifest.v1+json')
echo "$M"

# 按摘要拉取 blob，支持断点 Range
curl -fsS "$BASE/v2/$REPO/blobs/$LAYER_DIGEST" \
  -H 'Range: bytes=100-199' -o part.bin -i | grep -Ei '^(HTTP|Content-Range)'

curl -fsS "$BASE/v2/$REPO/tags/list"   # {"name":"acme/team/web","tags":["v1.0.0"]}
```

### 3.7 移动标签：必须携带期望摘要（CAS）

标签已存在时，直接 PUT 会被拒绝：

```bash
curl -X PUT "$BASE/v2/$REPO/manifests/v1.0.0" \
  -H "Content-Type: $MT" --data-binary @new-manifest.json -i
# HTTP/1.1 409 Conflict
# Docker-Content-Digest: sha256:<当前 digest>
# {"errors":[{"code":"TAG_CONFLICT","detail":{"CurrentDigest":"…","ExpectedDigest":"…"}}]}
```

先拉取当前摘要，放入 `If-Match` 完成 compare-and-swap：

```bash
CURRENT=$(curl -fsS -I "$BASE/v2/$REPO/manifests/v1.0.0" \
  | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}')

curl -fsS -X PUT "$BASE/v2/$REPO/manifests/v1.0.0" \
  -H "Content-Type: $MT" -H "If-Match: \"$CURRENT\"" \
  --data-binary @new-manifest.json -i
# 200 OK（标签移动）；若期间被别人改过 → 409 TAG_CONFLICT，需重新拉取后重试
```

`If-None-Match: *` 可要求“标签必须尚不存在”。

---

## 4. 垃圾回收（POST /gc）：先复核，再安全清理

GC **默认只做 dry-run**，返回一份带 `id` 的可复核计划（可通过
`GET /gc/plans/{id}` 取回）：

```bash
PLAN=$(curl -fsS -X POST "$BASE/gc")
echo "$PLAN" | python3 -m json.tool
```

计划示例（节选）：

```json
{
  "id": "9c33…",
  "dry_run": true,
  "roots": {
    "tags": 3,
    "uploadLeases": [{"repo":"acme/team/web","digest":"sha256:…",
                      "expires_at":1759200000.0}]
  },
  "reachable": {"manifests": [["acme/team/web","sha256:…"]],
                "blobs": ["sha256:…"]},
  "unreachable": {
    "blobs": ["sha256:ab12…"],
    "manifests": [["acme/team/web","sha256:old…"]],
    "expiredUploads": ["44de…"]
  },
  "reclaimableBytes": 12345,
  "warnings": [],
  "review": "2 blob(s) and 1 manifest(s) unreachable; … POST /gc with ... to execute."
}
```

复核无误后，用该计划执行：

```bash
PID=$(echo "$PLAN" | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -fsS -X POST "$BASE/gc" \
  -H 'Content-Type: application/json' \
  -d "{\"execute\": true, \"plan_id\": \"$PID\"}" | python3 -m json.tool
```

返回 `deleted`（实际删除）与 `skippedBecauseReachable`（dry-run 时不可达、
执行时已重新可达而被跳过）两部分。

### GC 如何保证“禁止悬空引用、禁止误删”

1. **可达图根集合**：所有标签当前指向的 manifest，以及所有**未过期的上传租约**
   （刚 finalize、manifest 尚未发布的 blob 在推送窗口内受保护）。
2. **边为 manifest 引用闭包**：image manifest → config/layers；
   image index → 子 manifest；以及 `subject`/artifact manifest 的引用，逐跳 BFS。
3. **执行时二次取快照并取交集**：执行 GC 时在写事务内**重新计算**当前可达图，
   只删除“旧计划 ∩ 新图”仍不可达的对象。dry-run 与执行之间发生的发布天然被排除
   （出现在 `skippedBecauseReachable`）。
4. **发布与 GC 串行化**：发布与 GC 均使用 SQLite `BEGIN IMMEDIATE` 写事务，
   引用校验与删除不可能交叉提交；标签→manifest 外键带 `ON DELETE RESTRICT`，
   事务提交前还有一次“无悬空标签”断言，任何不变量被破坏都会整体回滚。
5. **先提交元数据，后删文件**：删除在数据库事务提交后才 `unlink`。POSIX 下已在拉取
   （已打开文件句柄）的并发请求读完整个 inode 不受影响。
6. **过期上传会话**及其分片文件一并回收；未过期会话绝不触碰。

过期/已消费的计划 ID 返回 `404 PLAN_UNKNOWN`；重复执行已消费的计划返回
`409 PLAN_STALE`（可用 `{"force": true}` 强制按最新快照重新交集执行）。

### 重启后恢复上传与数据

服务启动（lifespan）时自动执行对账（`reconcile_on_startup`）：

* **进行中的分块上传**：按数据库中“最后已确认偏移”截断分片文件——已确认的字节保留，
  没落库的尾巴丢弃；随后客户端用 `GET uploads/{uuid}` 拿到偏移即可继续 PUT/PATCH。
* **崩溃在“硬链接已建、DB 未提交”之间的 CAS 文件**：重新校验摘要后认养入库；
  内容与路径摘要不符的损坏文件才删除。
* **有元数据、文件缺失**的 blob/manifest：清理对应行与受影响标签（修复已损坏状态），
  保证恢复后无悬空引用。
* 上次运行中被打断的 GC 标记为 `failed`；`staging/` 临时文件清空。

因此上传会话、blob 与 manifest 均可跨进程重启恢复。

---

## 5. 设计要点速览

* **CAS 去重**：blob 按 `sha256` 分片存于 `blobs/sha256/<ab>/<62hex>`，提交时
  `link(2)` 原子落位；同摘要无论推送到多少仓库只占一个文件，仓库归属是
  `repo_blobs` 关系行。
* **上传租约**：`uploads` 记录 `uploading → finalized` 状态与 `expires_at`；
  finalize 后的短租约作为 GC 根，闭合“先传 blob、后发 manifest”的窗口。
* **不可变内容**：manifest 本身也以 CAS 存储；标签只是指向摘要的可变指针，
  移动必须经 `If-Match` 条件，冲突返回 `409` 并带回当前摘要（CAS）。
* **作用域**：blob 必须在目标仓库“可见”（本仓库成员或成功 mount）才能被该仓库
  manifest 引用，跨仓库不会隐式暴露私有内容。

---

## 6. 运行测试

```bash
pip install -r requirements.txt
python3 -m pytest -q
```

测试覆盖：分块/整包上传、错误偏移回滚与续传、摘要/尺寸校验、全局去重、mount、
manifest 引用校验、不可变性与标签 CAS、GC dry-run/交集/租约保护/悬空防护、
重启恢复（孤儿 CAS 认养、分片截断），以及一个使用真实 uvicorn + 多线程
（发布 ×1 / 拉取 ×2 / GC ×1）的**并发安全**端到端测试。
