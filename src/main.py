"""
学术报告团购 Agent — 3人成团，拼团购买学术报告
"""

import os
import json
import uuid
import asyncio
import redis
from typing import AsyncGenerator
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()

# ── Redis 连接 (L1 state: 平台 Redis) ──────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None
REDIS_TLS = os.environ.get("REDIS_TLS") == "true"
REDIS_KEY_PREFIX = os.environ.get("REDIS_KEY_PREFIX", "shop:104:")

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=REDIS_TLS,
    decode_responses=True,
)
PFX = REDIS_KEY_PREFIX

# ── 平台 API 配置 ───────────────────────────────────────────────
A2H_API_BASE = os.environ.get("A2H_API_BASE", "https://demo.a2hmarket.ai")
A2H_TOKEN = os.environ.get("A2H_TOKEN", "")
A2H_SHOP_ID = os.environ.get("A2H_SHOP_ID", "104")
A2H_SELLER_ID = os.environ.get("A2H_SELLER_ID", "")

# 商品 worksId — 通过环境变量或硬编码配置
WORKS_ID = os.environ.get("WORKS_ID", "")

# 团购配置
GROUP_SIZE = 3
GROUP_TTL = 3 * 24 * 3600  # 72 小时
PRICE_CENTS = 990  # 9.90 CNY
CURRENCY = "CNY"

# ── 学术报告主题列表 ────────────────────────────────────────────
TOPICS = [
    ("人工智能", "ai_report"),
    ("气候变化", "climate_report"),
    ("量子计算", "quantum_report"),
    ("生物医药", "biotech_report"),
    ("新能源", "energy_report"),
    ("区块链", "blockchain_report"),
    ("太空探索", "space_report"),
    ("基因编辑", "gene_report"),
]


def sse_frame(event_type: str, payload: dict) -> str:
    """生成一个 SSE 数据帧"""
    return f"data: {json.dumps({'type': event_type, **payload})}\n\n"


def platform_get(path: str) -> dict:
    """调用平台 API (GET)"""
    import urllib.request
    url = f"{A2H_API_BASE}{path}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {A2H_TOKEN}")
    req.add_header("X-Gateway-Bypass", "true")
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def platform_post(path: str, body: dict) -> dict:
    """调用平台 API (POST)"""
    import urllib.request
    url = f"{A2H_API_BASE}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {A2H_TOKEN}")
    req.add_header("X-Gateway-Bypass", "true")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def find_or_create_group(buyer_id: str, topic_id: str) -> tuple:
    """找到有位置的团购组，或创建新组。返回 (group_id, current_count, is_new)"""
    # 扫描现有组
    pattern = f"{PFX}group:*:{topic_id}"
    keys = list(r.scan_iter(match=pattern, count=100))

    for key in keys:
        count = r.scard(key)
        if count < GROUP_SIZE and not r.sismember(key, buyer_id):
            return key.split(":")[1], count, False

    # 创建新组
    group_id = uuid.uuid4().hex[:8]
    group_key = f"{PFX}group:{group_id}:{topic_id}"
    r.sadd(group_key, buyer_id)
    r.expire(group_key, GROUP_TTL)
    return group_id, 1, True


def join_group(buyer_id: str, group_id: str, topic_id: str) -> tuple:
    """加入指定团购组。返回 (success, current_count)"""
    group_key = f"{PFX}group:{group_id}:{topic_id}"
    if r.sismember(group_key, buyer_id):
        return False, r.scard(group_key)
    count = r.sadd(group_key, buyer_id)
    r.expire(group_key, GROUP_TTL)
    return True, r.scard(group_key)


def get_group_members(group_id: str, topic_id: str) -> list:
    """获取团购组成员列表"""
    group_key = f"{PFX}group:{group_id}:{topic_id}"
    return list(r.smembers(group_key))


def get_group_ttl(group_id: str, topic_id: str) -> int:
    """获取团购组剩余时间(秒)"""
    group_key = f"{PFX}group:{group_id}:{topic_id}"
    return r.ttl(group_key)


def get_topic_name(topic_id: str) -> str:
    for name, tid in TOPICS:
        if tid == topic_id:
            return name
    return topic_id


async def handle_chat(request: Request) -> AsyncGenerator[str, None]:
    """处理聊天请求，返回 SSE 流"""
    body = await request.json()
    api_version = body.get("apiVersion", "")
    session_id = body.get("session_id", "")
    shop_id = body.get("shop_id", "")
    buyer = body.get("buyer", {})
    buyer_id = buyer.get("id", "")
    buyer_nickname = buyer.get("nickname", "同学")
    message = body.get("message", {})
    text = (message.get("text") or "").strip()
    event = body.get("event")

    # ── 处理系统事件 ──────────────────────────────────
    if event:
        event_type = event.get("type", "")
        if event_type == "payment.succeeded":
            order_id = event.get("order_id", "")
            yield sse_frame("text", {"text": "🎉 支付成功！正在为您生成学术报告..."})
            # 检查是否全组成团
            yield sse_frame("text", {"text": "您的报告正在生成中，完成后将发送给您。感谢您的参与！"})
            yield sse_frame("done", {})
            return
        elif event_type == "payment.failed":
            yield sse_frame("text", {"text": "支付未成功，别担心，您可以重新发起支付。"})
            yield sse_frame("done", {})
            return
        elif event_type == "session.resumed":
            yield sse_frame("text", {"text": f"欢迎回来，{buyer_nickname}！继续团购学术报告吗？"})
            yield sse_frame("done", {})
            return

    # ── 无消息时返回问候 ──────────────────────────────
    if not text:
        greeting = (
            f"👋 你好，{buyer_nickname}！欢迎来到学术报告团购！\n\n"
            "📚 3人成团，即可获取高质量学术报告，每人仅需 ¥9.90\n\n"
            "🔬 可选主题：\n"
        )
        for i, (name, tid) in enumerate(TOPICS, 1):
            greeting += f"  {i}. {name}\n"
        greeting += "\n请告诉我你想购买哪个主题的报告，回复数字或名称即可！"
        yield sse_frame("text", {"text": greeting})
        yield sse_frame("done", {})
        return

    # ── 解析用户意图 ──────────────────────────────────
    # 检查是否选择了主题
    selected_topic = None
    text_lower = text.lower()
    for i, (name, tid) in enumerate(TOPICS, 1):
        if text == str(i) or name in text or tid in text_lower:
            selected_topic = (name, tid)
            break

    # 检查是否包含团购关键词
    is_group_query = any(kw in text for kw in ["团购", "拼团", "成团", "加入", "组团"])

    if selected_topic:
        topic_name, topic_id = selected_topic
        # 检查用户是否已在某个组中
        pattern = f"{PFX}group:*:{topic_id}"
        existing_key = None
        for key in r.scan_iter(match=pattern, count=100):
            if r.sismember(key, buyer_id):
                existing_key = key
                break

        if existing_key:
            gid = existing_key.split(":")[1]
            count = r.scard(existing_key)
            if count >= GROUP_SIZE:
                yield sse_frame("text", {"text": f"🎉 「{topic_name}」团购已成团！等待支付后即可获取报告。"})
                yield sse_frame("done", {})
                return
            ttl_hours = r.ttl(existing_key) // 3600
            yield sse_frame("text", {"text": f"你已加入「{topic_name}」的团购（编号 {gid}），当前 {count}/{GROUP_SIZE} 人，剩余约 {ttl_hours} 小时。等待更多同学加入即可成团！"})
            yield sse_frame("done", {})
            return

        # 找组或创建新组
        group_id, count, is_new = find_or_create_group(buyer_id, topic_id)
        if is_new:
            yield sse_frame("text", {"text": f"🆕 你发起了「{topic_name}」的新团购！\n\n📋 团购编号：{group_id}\n👥 当前人数：1/{GROUP_SIZE}\n⏰ 有效时间：72小时\n\n分享给同学，3人即可成团，每人 ¥9.90！"})
        else:
            current_count = r.scard(f"{PFX}group:{group_id}:{topic_id}")
            if current_count >= GROUP_SIZE:
                yield sse_frame("text", {"text": f"🎉 太棒了！「{topic_name}」团购已满员，即将成团！"})
                yield sse_frame("ui", {
                    "action": "open_order",
                    "params": {
                        "works_id": WORKS_ID,
                        "title": f"学术报告团购 - {topic_name}（团购 {group_id}）",
                        "prefill": json.dumps({
                            "topic": topic_name,
                            "topic_id": topic_id,
                            "group_id": group_id,
                        })
                    }
                })
                yield sse_frame("done", {})
                return
            yield sse_frame("text", {"text": f"✅ 成功加入「{topic_name}」团购！\n\n📋 团购编号：{group_id}\n👥 当前人数：{current_count}/{GROUP_SIZE}\n⏰ 还差 {GROUP_SIZE - current_count} 人即可成团\n\n继续邀请同学加入吧！"})

        # 如果当前组刚好满员，触发下单
        if count >= GROUP_SIZE:
            yield sse_frame("text", {"text": f"🎉 恭喜！「{topic_name}」团购已满员，可以下单了！"})
            if WORKS_ID:
                yield sse_frame("ui", {
                    "action": "open_order",
                    "params": {
                        "works_id": WORKS_ID,
                        "title": f"学术报告团购 - {topic_name}（团购 {group_id}）",
                        "prefill": json.dumps({
                            "topic": topic_name,
                            "topic_id": topic_id,
                            "group_id": group_id,
                        })
                    }
                })
            else:
                yield sse_frame("text", {"text": "📌 团购已满！商品配置中，请稍候..."})
        yield sse_frame("done", {})
        return

    # ── 处理加入指定团购组的请求 ────────────────────────
    # 格式: "加入 XXXXXXXX" 或 "加入 报告"
    join_parts = text.split()
    if len(join_parts) >= 2 and join_parts[0] in ("加入", "join"):
        code = join_parts[1]
        # 尝试匹配 topic
        matched_topic = None
        for name, tid in TOPICS:
            if name in text or tid in text:
                matched_topic = (name, tid)
                break
        if matched_topic:
            _, topic_id = matched_topic
            success, count = join_group(buyer_id, code, topic_id)
            if success and count >= GROUP_SIZE:
                yield sse_frame("text", {"text": f"🎉 成功加入！「{get_topic_name(topic_id)}」团购已满员，即将成团！"})
                yield sse_frame("done", {})
                return
            yield sse_frame("text", {"text": f"✅ 已加入团购 {code}，当前 {count}/{GROUP_SIZE} 人。"})
        else:
            yield sse_frame("text", {"text": "请告诉我你想加入哪个主题的团购，比如「加入 ABC123 人工智能」。"})
        yield sse_frame("done", {})
        return

    # ── 通用回复 ───────────────────────────────────────
    reply = (
        f"好的，{buyer_nickname}！请从以下主题中选择一个：\n\n"
    )
    for i, (name, tid) in enumerate(TOPICS, 1):
        reply += f"  {i}. {name}\n"
    reply += "\n回复数字或名称即可开始团购！"
    yield sse_frame("text", {"text": reply})
    yield sse_frame("done", {})
    return


@app.post("/chat")
async def chat(request: Request):
    return StreamingResponse(
        handle_chat(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "shop_id": A2H_SHOP_ID}
