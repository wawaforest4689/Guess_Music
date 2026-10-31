# -*- coding: utf-8 -*-
import os
import re
import time
import uuid
import random
import json
import base64
import socket
import asyncio
import threading
from html import escape
from pathlib import Path
from typing import Optional, Set, List
from collections import deque

import wave
import websocket
from pydub import AudioSegment
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from nicegui import ui, app, background_tasks

# ---------------- FFmpeg 路径 ----------------
AudioSegment.converter = r"D:\Applications\ffmpeg\bin\ffmpeg.exe"

# ---------------- 基础配置 ----------------
BASE_DIR = r"d:\wf200\Music\长途车载音乐"
MUSIC_DIR = os.path.join(BASE_DIR, "音乐数据库-精简版")
STATIC_DIR = os.path.join(str(Path(__file__).resolve().parent), "static_audio")
os.makedirs(STATIC_DIR, exist_ok=True)

CLIP_DURATION_MS = 15 * 1000

# 难度配置
DIFFICULTY_CONFIG = {
    "easy": {"max_time": 40, "options": 4, "label": "简单 (40s, 4选项)"},
    "medium": {"max_time": 30, "options": 6, "label": "中等 (30s, 6选项)"},
    "hard": {"max_time": 20, "options": 8, "label": "困难 (20s, 8选项)"}
}

REALTIME_WS_URL = ("wss://llm-vu7gcr4lzmmf6vy4.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime")

ai_semaphore = asyncio.Semaphore(5)  # 最多同时允许5个请求去连阿里云，其他的请求需排队等待


# =====================================================================
#  WebSocket 连接管理器
# =====================================================================
class WSManager:
    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._lock = threading.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        with self._lock:
            self._connections.add(ws)

    def disconnect(self, ws: WebSocket):
        with self._lock:
            self._connections.discard(ws)

    async def broadcast(self, message: dict):
        data = json.dumps(message, ensure_ascii=False)
        with self._lock:
            conns = list(self._connections)
        dead = []
        for ws in conns:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        if dead:
            with self._lock:
                for ws in dead:
                    self._connections.discard(ws)


# =====================================================================
#  全局共享状态：API 配置 + 大屏有效轮次（所有终端共用）
# =====================================================================
class SharedState:
    def __init__(self):
        self.api_key: str = ""
        self.realtime_url: str = REALTIME_WS_URL
        self.model_name: str = "qwen-audio-3.1-realtime-flash"

        self.round_id = 0  # 大屏有效轮次，跨终端全局递增
        self.last_result: List[Optional[dict]] = deque(maxlen=10)
        self.lock = threading.Lock()

    def set_config(self, api_key: Optional[str] = None, realtime_url: Optional[str] = None,
                   model_name: Optional[str] = None):
        if api_key: self.api_key = api_key
        if realtime_url: self.realtime_url = realtime_url
        if model_name: self.model_name = model_name

    def next_round_id(self) -> int:
        with self.lock:
            self.round_id += 1
            return self.round_id


ws_manager = WSManager()
shared = SharedState()


# =====================================================================
#  核心业务类：每个打开的游戏页面（每台平板 / 每个标签页）各持有一个实例，互不干扰
# =====================================================================
class Music_Guesser:
    def __init__(self, level: str, player: str = ""):
        self.player = player  # 终端标识（客户端 IP），用于大屏区分
        self.difficulty = level
        self.max_time = DIFFICULTY_CONFIG[level]["max_time"]
        self.options_count = DIFFICULTY_CONFIG[level]["options"]

        self.question_id = 0
        self.correct_song = ""
        self.options = []
        self.clip_url_path = ""
        self.clip_path = ""
        self._clips = []  # 本会话生成过的片段文件，换题/离开页面时删除
        self.human_start_time = 0.0
        self.drawing = False

        self.round_states = {}
        self.lock = threading.Lock()

        self.selected_letter: Optional[str] = None
        self._last_synced_round = 0
        self._last_sidebar_html = None
        self.option_btns = {}
        self.option_labels = {}

    def _realtime_ask(self, prompt: str, clip_path: str) -> str:
        temp_wav = os.path.join(str(STATIC_DIR), f"tempclip_{uuid.uuid4().hex}_16k.wav")
        realtime_url, model_name, api_key = shared.realtime_url, shared.model_name, shared.api_key
        try:
            seg = AudioSegment.from_file(clip_path).set_frame_rate(16000).set_channels(1).set_sample_width(2)
            seg.export(temp_wav, format="wav")

            url = f"{realtime_url}?model={model_name}" if "?" not in realtime_url else f"{realtime_url}&model={model_name}"
            headers = [f"Authorization: Bearer {api_key}"]

            ws = None
            for attempt in range(2):
                try:
                    ws = websocket.create_connection(url, header=headers, timeout=15)
                    break
                except Exception as e:
                    if attempt == 1: raise RuntimeError(f"无法连接: {e}")
                    time.sleep(1)

            try:
                ws.send(json.dumps({
                    "type": "session.update",
                    "session": {"modalities": ["text"], "instructions": prompt, "turn_detection": None,
                                "audio": {"input": {"format": {"type": "pcm", "sample_rate": 16000}}}}
                }))

                with wave.open(temp_wav, 'rb') as wf:
                    while True:
                        data = wf.readframes(3200)
                        if not data: break
                        ws.send(
                            json.dumps({"type": "input_audio_buffer.append", "audio": base64.b64encode(data).decode()}))
                        time.sleep(0.02)

                ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
                ws.send(json.dumps({"type": "response.create", "response": {"modalities": ["text"]}}))

                text_parts, final_text = [], ""
                while True:
                    resp = ws.recv()
                    if not resp: break
                    ev = json.loads(resp)
                    t = ev.get("type")
                    if t == "error":
                        raise RuntimeError(f"Realtime error: {ev.get('error')}")
                    elif t in ("response.text.delta", "response.audio_transcript.delta"):
                        text_parts.append(ev.get("delta", ""))
                    elif t in ("response.done", "response.completed"):
                        resp_data = ev.get("response", {})
                        for item in resp_data.get("output", []):
                            for c in item.get("content", []):
                                if c.get("text"): final_text = c["text"]
                        break
                return "".join(text_parts).strip() or final_text.strip()
            finally:
                if ws: ws.close()
        except Exception as e:
            print(f"\n[AI 调用失败] {e}\n")
            return f"AI调用失败: {e}"
        finally:
            if os.path.exists(temp_wav):
                try:
                    os.remove(temp_wav)
                except:
                    pass

    async def _process_ai_task(self, q_id: int, attempt: int, prompt: str, clip_path: str):
        start_wall = time.monotonic()
        try:
            async with ai_semaphore:  # 多并发访问限流
                content = await asyncio.wait_for(asyncio.to_thread(self._realtime_ask, prompt, clip_path),
                                                timeout=self.max_time)
        except asyncio.TimeoutError:
            content = f"API 调用超时（{int(self.max_time)}秒未返回）"
        except Exception as e:
            content = f"API 调用失败：{e}"
        elapsed = time.monotonic() - start_wall

        match = re.findall(r"答案[：:]\s*([A-Z])", content.upper())
        ai_choice = match[-1] if match else None
        if ai_choice not in [chr(65 + i) for i in range(self.options_count)]:
            letters = re.findall(r"\b([A-Z])\b", content.upper())
            # 从后往前找合法的选项字母
            for l in reversed(letters):
                if l in [chr(65 + i) for i in range(self.options_count)]:
                    ai_choice = l
                    break

        with self.lock:
            state = self.round_states.get(q_id)
            # attempt 不一致说明本题已被重置/重新开始，丢弃过期的 AI 结果
            if not state or state.get("attempt") != attempt or "ai_reasoning" in state: return
            state["ai_choice"] = ai_choice
            state["ai_time"] = round(elapsed, 2)
            state["ai_reasoning"] = content

        self._try_finalize(q_id)

    def build_result_payload(self, q_id: int) -> Optional[dict]:
        with self.lock:
            state = self.round_states.get(q_id)
            if not state: return None

            valid_rid = state.get("valid_round_id", 1)
            correct = state.get("correct_letter", "A")
            h_choice = state.get("human_choice")
            h_time = state.get("human_time")
            timed_out = bool(state.get("timed_out"))

            # 核心修复：只有选了合法的 ABCD... 且没有超时，才算对
            human_correct = bool(
                h_choice and h_choice in [chr(65 + i) for i in range(self.options_count)]
                and h_choice == correct and not timed_out and (h_time if h_time else 9999) <= self.max_time
            )

            a_choice = state.get("ai_choice")
            a_time = state.get("ai_time")
            ai_correct = bool(a_choice and a_choice == correct and a_time is not None and a_time <= self.max_time)

            return {
                "round_id": valid_rid, "correct_song": state.get("song", ""),
                "human_choice": h_choice, "human_time": h_time, "human_correct": human_correct,
                "ai_choice": a_choice, "ai_time": a_time, "ai_correct": ai_correct, "difficulty": self.difficulty,
                "player": self.player
            }

    def _notify_result(self, r_id: int):
        payload = self.build_result_payload(r_id)
        if payload:
            shared.last_result.append(payload)
            background_tasks.create(ws_manager.broadcast(payload), name="broadcast_result")

    # 计时器挂在事件循环上（而非 threading.Timer），超时回调和大屏广播都在同一事件循环里执行
    def _start_human_timer(self, r_id: int):
        timer = asyncio.get_running_loop().call_later(self.max_time, self._on_human_timeout, r_id)
        with self.lock:
            if self.round_states.get(r_id): self.round_states[r_id]["timer"] = timer

    def _on_human_timeout(self, q_id: int):
        with self.lock:
            if q_id != self.question_id: return
            state = self.round_states.get(q_id)
            if not state or not state.get("started") or state.get("submitted"): return
            state["submitted"] = True
            state["timed_out"] = True
            state["human_choice"] = "-"
            state["human_time"] = self.max_time
            state.pop("timer", None)
        self._try_finalize(q_id)

    def _start_ai_timer(self, r_id: int):
        timer = asyncio.get_running_loop().call_later(self.max_time, self._on_ai_timeout, r_id)
        with self.lock:
            if self.round_states.get(r_id): self.round_states[r_id]["ai_timer"] = timer

    def _on_ai_timeout(self, q_id: int):
        with self.lock:
            if q_id != self.question_id: return
            state = self.round_states.get(q_id)
            if not state or "ai_reasoning" in state: return
            state["ai_choice"] = None
            state["ai_time"] = float(self.max_time)
            state["ai_reasoning"] = f"⏰ AI 作答超时（{self.max_time}秒），直接判错"
            state.pop("ai_timer", None)
        self._try_finalize(q_id)

    def get_ui_state(self):
        with self.lock:
            state = self.round_states.get(self.question_id, {})
            options = state.get("options", [])
            started, submitted, timed_out = bool(state.get("started")), bool(state.get("submitted")), bool(
                state.get("timed_out"))
            status = "done" if "ai_reasoning" in state else (
                "thinking" if submitted and started else ("playing" if started else "idle"))
            ai_choice, ai_time, correct_letter = state.get("ai_choice"), state.get("ai_time"), state.get(
                "correct_letter")
            ai_correct = (ai_choice == correct_letter and ai_time <= self.max_time) if (
                        ai_choice is not None and ai_time is not None) else None
            return {
                "round_id": self.question_id, "options": list(options), "started": started, "submitted": submitted,
                "timed_out": timed_out,
                "status": status, "reasoning": state.get("ai_reasoning"), "choice": ai_choice, "ai_time": ai_time,
                "ai_correct": ai_correct,
                "human_choice": state.get("human_choice"), "human_time": state.get("human_time"),
                "correct_letter": correct_letter,
            }

    def choose_music(self):
        exts = {"mp3", "wav", "flac", "ogg", "m4a"}
        songs = [os.path.basename(p) for p in os.listdir(MUSIC_DIR) if p.split(".")[-1].lower() in exts]
        if len(songs) < self.options_count: raise RuntimeError(f"音乐库中至少需要 {self.options_count} 首音乐")

        correct_path = random.sample(songs, k=1)[0]
        correct_song = correct_path.split(".")[0]
        others = [p for p in songs if p != correct_path]
        options = [correct_song] + random.sample([p.split(".")[0] for p in others], k=self.options_count - 1)
        random.shuffle(options)

        audio = AudioSegment.from_file(os.path.join(MUSIC_DIR, correct_path))
        clip = audio if len(audio) <= CLIP_DURATION_MS else audio[random.randint(0, len(audio) - CLIP_DURATION_MS):][
            :CLIP_DURATION_MS]

        # uuid 命名：多终端同时抽题不会撞名，也无法猜到别人的片段地址
        clip_name = f"clip_{uuid.uuid4().hex}.wav"
        clip_path = Path(STATIC_DIR) / clip_name
        clip.export(str(clip_path), format="wav")

        with self.lock:
            self.correct_song, self.options = correct_song, options
            self.clip_path = str(clip_path)
            self.clip_url_path = f"/audio/{clip_name}"
            self._clips.append(self.clip_path)

            self.question_id += 1
            self.round_states[self.question_id] = {
                "song": self.correct_song, "options": list(self.options),
                "correct_letter": chr(65 + self.options.index(self.correct_song)),
                "clip_path": self.clip_path,
                "started": False, "submitted": False, "timed_out": False,
            }

    def start_round_api(self, q_id):
        with self.lock:
            if q_id != self.question_id: return {"ok": False}
            state = self.round_states.get(self.question_id)
            if not state or state.get("started") or state.get("submitted"): return {"ok": False}

            state["started"] = True
            state["submitted"] = False
            state["timed_out"] = False
            for k in ["human_choice", "human_time", "ai_choice", "ai_time", "ai_reasoning"]: state.pop(k, None)
            state["attempt"] = state.get("attempt", 0) + 1

            self.human_start_time = time.monotonic()
            current_rid, options = self.question_id, list(state["options"])
            attempt, clip_path = state["attempt"], state["clip_path"]

        self._start_human_timer(current_rid)
        self._start_ai_timer(current_rid)

        opts_text = "\n".join(f"{chr(65 + i)}. {opt}" for i, opt in enumerate(options))
        prompt = f"听这段15秒的音乐片段，从以下选项中选出最匹配的歌曲，限时{self.max_time}秒：\n{opts_text}\n先给50字内推理，最后输出格式为'答案：A'"
        print(f"[{self.player}] Turn {current_rid}: {prompt}")

        background_tasks.create(self._process_ai_task(current_rid, attempt, prompt, clip_path), name="ai_answer")
        return {"ok": True}

    def submit_api(self, q_id, letter):
        with self.lock:
            if q_id != self.question_id: return {"ok": False}
            state = self.round_states.get(self.question_id)
            if not state or not state.get("started") or state.get("submitted"): return {"ok": False}
            # 核心修复：如果没有选项或选项不合法，直接拒绝
            if letter not in [chr(65 + i) for i in range(self.options_count)]: return {"ok": False}

            elapsed = time.monotonic() - self.human_start_time
            state["timed_out"] = elapsed > self.max_time
            state["human_choice"] = "Z" if state["timed_out"] else letter
            state["human_time"] = self.max_time if state["timed_out"] else round(elapsed, 2)
            state["submitted"] = True
            timer = state.pop("timer", None)

        if timer: timer.cancel()
        ui.notify(f"✅️正确答案是 {state.get("correct_letter","")}:{self.correct_song}!",type="info",position="top",timeout=5000)
        self._try_finalize(q_id)

        return {"ok": True}

    def quit_api(self, q_id):
        with self.lock:
            if q_id != self.question_id: return {"ok": False}
            state = self.round_states.get(self.question_id)
            if not state: return {"ok": False}

            # 优雅地取出并清理计时器
            timer = state.pop("timer", None)
            ai_timer = state.pop("ai_timer", None)

            if timer: timer.cancel()
            if ai_timer: ai_timer.cancel()

            # 原地重置作答状态（attempt +1 让仍在路上的 AI 结果作废）
            state["started"] = False
            state["submitted"] = False
            state["timed_out"] = False
            state["attempt"] = state.get("attempt", 0) + 1
            state.pop("human_choice", None)
            state.pop("human_time", None)
            state.pop("ai_choice", None)
            state.pop("ai_time", None)
            state.pop("ai_reasoning", None)

            # 清空结算标记
            state.pop("finalized", None)
            state.pop("valid_round_id", None)

        return {"ok": True}

    def _try_finalize(self, q_id: int):
        """检查人类和 AI 是否都完成了，如果都完成且是首次，则大屏有效轮次 +1 并广播"""
        is_finalize=False
        with self.lock:
            state = self.round_states.get(q_id)
            if not state: return

            # 如果这局已经结算过了，直接退出
            if state.get("finalized"): return

            # 人类是否完成（提交或超时）
            human_done = state.get("submitted", False)
            # AI 是否完成（正常返回有 reasoning 或 超时有 reasoning）
            ai_done = "ai_reasoning" in state

            # 双方都完成了，才进行结算
            if human_done and ai_done:
                state["finalized"] = True  # 标记已结算
                state["valid_round_id"] = shared.next_round_id()  # 👈 大屏有效轮次 +1（全局）
                is_finalize=True

        if is_finalize:
            # 结算完成后，立刻向大屏广播这唯一的一次最终结果
            self._notify_result(q_id)

    def _cleanup_clips(self, keep: str = ""):
        remaining = []
        for p in self._clips:
            if p == keep:
                remaining.append(p)
                continue
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
            except OSError:
                remaining.append(p)  # Windows 下文件可能仍被占用，下次再删
        self._clips = remaining

    def dispose(self):
        """页面关闭/离开时调用：取消本会话所有计时器并删除音频片段"""
        with self.lock:
            for state in self.round_states.values():
                for key in ("timer", "ai_timer"):
                    t = state.pop(key, None)
                    if t: t.cancel()
        self._cleanup_clips()


# =====================================================================
#  NiceGUI 挂载 FastAPI 路由
# =====================================================================
app.mount("/audio", StaticFiles(directory=str(STATIC_DIR)), name="audio")


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        if shared.last_result!=[]:
            for rec in shared.last_result:
                await websocket.send_text(json.dumps(rec, ensure_ascii=False))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)


# =====================================================================
#  前端 UI 构建
# =====================================================================
WAVE_BG_HTML = '''
<div id="wave-bg">
  <style>
    #wave-bg { position: fixed; inset: 0; z-index: -1; background: radial-gradient(circle at 50% 50%, #1a0028, #050008 70%); overflow: hidden; }
    #wave-bg .bar { position: absolute; bottom: 0; width: 20px; background: linear-gradient(to top, #b388ff, #6a3f9a); border-radius: 4px 4px 0 0; opacity: 0.8; will-change: height; }
  </style>
  <script>
  (function() {
    const root = document.getElementById('wave-bg');
    if (!root) return;
    const count = 40; const bars = [];
    for (let i = 0; i < count; i++) {
      const el = document.createElement('div'); el.className = 'bar'; el.style.left = (i * (100 / count)) + '%'; root.appendChild(el);
      bars.push({ el: el, phase: Math.random() * Math.PI * 2, freq: 0.4 + Math.random() * 0.9, amp: 100 + Math.random() * 200, base: 60 + Math.random() * 80 });
    }
    let t = 0;
    function tick() {
      t += 0.025;
      for (const b of bars) { const h = b.base + b.amp * (0.6 * Math.sin(t * b.freq + b.phase) + 0.4 * Math.sin(t * b.freq * 1.7 + b.phase * 2)); b.el.style.height = Math.max(4, h) + 'px'; }
      requestAnimationFrame(tick);
    }
    tick();
  })();
  </script>
</div>
'''
HEAD_HTML = '''
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<style>
body { margin: 0; }
.nicegui-content { background: transparent !important; }
/* 触屏：去掉双击缩放和点击延迟 */
.q-btn, #music_player { touch-action: manipulation; }
/* 强制覆盖 Quasar 按钮底色，实现透明背景 */
.circle-btn {
    background: transparent !important;
    box-shadow: none !important;
    border: 3px solid #fff !important;
    color: #fff !important;
    font-size: 30px !important;
    font-weight: bold !important;
    transition: all 0.2s;
}
.circle-btn.active {
    border-color: #000 !important;
    background: #b388ff !important;
    color: #000 !important;
}
/* 开始作答前锁住播放器，防止提前试听 */
#music_player.locked { pointer-events: none; opacity: 0.35; }

/* AI 助手面板：全局统一固定悬浮在右上角 */
.ai-panel {
    position: fixed !important;
    right: 20px;
    top: 20px;
    width: min(380px, calc(100vw - 40px)); /* 窄屏自适应，最宽380px */
    max-height: 80vh;
    overflow-y: auto;
    background: rgba(20,0,35,.95); /* 稍微加深一点背景，防止透明度过高看不清 */
    border: 1px solid #6a3f9a;
    border-radius: 16px;
    padding: 16px;
    color: #e8ddff;
    font-size: 14px;
    z-index: 100;
    box-sizing: border-box;
    box-shadow: 0 8px 32px rgba(0,0,0,0.6); /* 加个阴影，更有悬浮感 */
    backdrop-filter: blur(8px);
}

/* 针对小屏幕（手机）特别适配：距离边缘更近，防止遮挡过多 */
@media (max-width: 600px) {
    .ai-panel {
        right: 10px;
        top: 10px;
        width: calc(100vw - 20px);
        max-height: 60vh; /* 手机屏幕小，限制最大高度，可滚动 */
    }
}
</style>
'''

# 播放必须在按钮的浏览器点击事件里直接触发：iPad/iOS Safari 只允许用户手势调用 play()，
# 经服务端转发的 run_javascript 不算手势，会被静默拦截
PLAY_CLIP_JS = '''() => {
  const p = document.getElementById('music_player');
  if (!p || !p.getAttribute('src')) return;
  p.classList.remove('locked');
  try { p.currentTime = 0; } catch (e) {}
  const r = p.play(); if (r) r.catch(function(){});
}'''

RESET_CLIP_JS = '''(function() {
  const p = document.getElementById('music_player');
  if (p) { p.pause(); try { p.currentTime = 0; } catch (e) {} p.classList.add('locked'); }
})();'''


def _load_clip_js(url: str) -> str:
    return f"""(function() {{
        const p = document.getElementById('music_player');
        if (p) {{ p.pause(); p.classList.add('locked'); p.src = {json.dumps(url)}; p.load(); }}
    }})();"""


def _client_host() -> str:
    req = ui.context.client.request
    return req.client.host if req.client else ""


def _is_local_client(host: str) -> bool:
    """只有服务端笔记本本机访问时才显示 API 配置，避免平板玩家误改全局配置"""
    if host in ("127.0.0.1", "::1", "localhost"): return True
    try:
        return host in socket.gethostbyname_ex(socket.gethostname())[2]
    except OSError:
        return False


@ui.page("/")
def index_page():
    ui.add_head_html(HEAD_HTML)
    ui.add_body_html(WAVE_BG_HTML)

    with ui.column().style("width:100%;height:100vh;align-items:center;justify-content:center;gap:24px;"):
        ui.label("🎵 AI vs 人类 · 听歌识曲对抗").style(
            "font-size:42px;font-weight:800;color:#e8ddff;text-shadow:0 0 20px #b388ff;margin-bottom:20px;")

        for level, cfg in DIFFICULTY_CONFIG.items():
            ui.button(cfg["label"], on_click=lambda l=level: ui.navigate.to(f"/game/{l}")).style(
                "width:300px;height:60px;border-radius:999px;background:#6a3f9a;color:#fff;font-size:20px;font-weight:bold;"
            )

        if _is_local_client(_client_host()):
            with ui.expansion("⚙️ 配置 API 接口").style("position:fixed;left:20px;bottom:20px;width:360px;background:rgba(15,0,25,.92);border:1px solid #4a2a6a;border-radius:12px;color:#d8c8ff;"):
                key_input = ui.input("API Key", password=True, password_toggle_button=True).style("width:100%;")
                url_input = ui.input("Realtime WS URL", value=shared.realtime_url).style("width:100%;")
                model_input = ui.input("Model Setting", value=shared.model_name).style("width:100%;")

                def _on_save_config():
                    shared.set_config(api_key=key_input.value or "", realtime_url=url_input.value or "",
                                      model_name=model_input.value or "")
                    ui.notify("✅ 配置已保存", color="positive", position="bottom-left")

                ui.button("保存", on_click=_on_save_config).style("width:100%;background:#6a3f9a;color:#fff;")


@ui.page("/game/{level}")
def game_page(level: str):
    if level not in DIFFICULTY_CONFIG:
        ui.navigate.to("/")
        return

    # 每次打开游戏页都新建独立会话，多台平板同时游玩互不干扰；页面销毁时自动清理
    g = Music_Guesser(level, player=_client_host())
    ui.context.client.on_delete(g.dispose)

    def _go_back():
        g.dispose()
        ui.navigate.to('/')

    ui.add_head_html(HEAD_HTML)
    ui.add_body_html(WAVE_BG_HTML)

    # 左上角返回按钮
    ui.button(icon='arrow_back', on_click=_go_back).props('flat color=white').style(
        "position:fixed;left:20px;top:20px;font-size:24px;z-index:50;background:rgba(106,63,154,0.5);border-radius:50%;"
    )

    with ui.column().style("width:100%;align-items:center;"):
        ui.label(f"🎵 AI vs 人类 · {DIFFICULTY_CONFIG[level]['label']}").style(
            "font-size:34px;font-weight:800;color:#e8ddff;margin-top:26px;text-shadow:0 0 18px #b388ff;")

        with ui.row().style("margin-top:22px;gap:24px;justify-content:center;"):
            g.btn_draw = ui.button("🎲 抽取并播放音乐", on_click=g._on_draw).style(
                "background:#6a3f9a;color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;")
            g.btn_start = ui.button("▶ 开始作答", on_click=g._on_start).style(
                "color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;background:#3a3a3a;opacity:0.55;")
            g.btn_start.on('click', js_handler=PLAY_CLIP_JS)

        g.audio_html = ui.html(
            '<audio id="music_player" class="locked" controls preload="auto" '
            'style="width:min(680px, calc(100vw - 32px));height:56px;margin-top:24px;"></audio>')

        # 动态生成 A, B, C... 按钮（按钮与歌名成组排列，窄屏换行时依然对齐）
        letters = [chr(65 + i) for i in range(g.options_count)]
        gap_style = "gap:30px;" if g.options_count > 4 else "gap:56px;"

        with ui.row().style(f"margin-top:34px;{gap_style} justify-content:center;"):
            for letter in letters:
                with ui.column().style("align-items:center;gap:14px;"):
                    # 使用 props('flat') 强制移除 Quasar 蓝色底，由 CSS 统一接管
                    btn = ui.button(letter, on_click=lambda l=letter: g._on_select(l)).props('flat').classes('circle-btn')
                    btn.style("width:80px;height:80px;opacity:0.35;")
                    g.option_btns[letter] = btn
                    g.option_labels[letter] = ui.label("—").style(
                        "width:80px;text-align:center;color:#cbb7ee;font-size:15px;")

        with ui.row().style("margin-top:36px;gap:24px;justify-content:center;"):
            g.btn_submit = ui.button("✅ 提交作答", on_click=g._on_submit).style(
                "color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;background:#3a3a3a;opacity:0.55;")
            g.btn_quit = ui.button("↺ 重置本题", on_click=g._on_quit).style(
                "color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;background:#3a3a3a;opacity:0.55;")

        g.ai_panel = ui.html("").classes("ai-panel")
        g.ai_panel.set_visibility(False)

    ui.timer(0.4, g._sync_ui)


# =====================================================================
#  事件处理扩展 (注入到类中)
# =====================================================================
def _style_btn(btn, enabled, ready_color):
    btn.style(f"background:{ready_color};opacity:1;" if enabled else "background:#3a3a3a;opacity:0.55;")


async def _on_draw(self):
    self.drawing = True
    self.btn_draw.disable()
    try:
        # 解码/剪切音频比较耗时，放到线程里做，避免卡住其他终端
        await asyncio.to_thread(self.choose_music)
    except Exception as e:
        ui.notify(f"抽题失败：{e}", type="negative", position="top")
        return
    finally:
        self.drawing = False
        self.btn_draw.enable()
    self._cleanup_clips(keep=self.clip_path)
    ui.run_javascript(_load_clip_js(self.clip_url_path))
    self._sync_ui()


def _on_select(self, letter):
    st = self.get_ui_state()
    if not st["started"] or st["submitted"]: return
    self.selected_letter = None if self.selected_letter == letter else letter
    self._sync_ui()


def _on_start(self):
    # 音频由按钮上的 PLAY_CLIP_JS 在浏览器端直接播放，这里只负责开局计时和启动 AI 作答
    self.start_round_api(self.question_id)
    self._sync_ui()


def _on_submit(self):
    if self.selected_letter:
        self.submit_api(self.question_id, self.selected_letter)
    self._sync_ui()


def _on_quit(self):
    self.quit_api(self.question_id)
    self.selected_letter = None
    ui.run_javascript(RESET_CLIP_JS)
    self._sync_ui()


def _update_sidebar(self, st: dict):
    if not st.get("submitted"):
        self.ai_panel.set_visibility(False)
        return
    html = '<div><b>🤖 AI 助手</b></div><hr style="border-color:#3d2055;">'
    if st.get("timed_out"):
        html += '<div style="color:#ff8a80;font-weight:bold;">⏰ 人类超时，回答错误</div><hr style="border-color:#3d2055;">'
    if st["status"] == "thinking":
        html += "AI 做题中... ⏳"
    elif st["status"] == "done":
        html += f'<b>思考过程：</b><br><span style="color:#cfc0ff">{escape(st.get("reasoning") or "无")}</span><hr style="border-color:#3d2055;">'
        html += f'<b>最终选择：</b><span style="color:#ffd54f;font-size:20px"> {st.get("choice") or "无"} </span>'
        if st.get("ai_time") is not None: html += f'<br><span style="color:#aaa;">AI 用时：{st["ai_time"]} 秒</span>'
        if st.get("ai_correct") is False:
            html += '<br><span style="color:#ff8a80;">AI 本题判错</span>'
        elif st.get("ai_correct") is True:
            html += '<br><span style="color:#81c784;">AI 本题答对</span>'
    # 内容没变就不重复推送，减轻多终端同时在线时的网络压力
    if html != self._last_sidebar_html:
        self._last_sidebar_html = html
        self.ai_panel.content = html
    self.ai_panel.set_visibility(True)


def _sync_ui(self):
    st = self.get_ui_state()
    if st["round_id"] != self._last_synced_round:
        self._last_synced_round = st["round_id"]
        self.selected_letter = None
    options, started, submitted = st.get("options", []), st["started"], st["submitted"]
    can_select = started and not submitted

    letters = [chr(65 + i) for i in range(self.options_count)]
    for i, letter in enumerate(letters):
        if letter not in self.option_btns: continue
        btn = self.option_btns[letter]
        if i < len(options): self.option_labels[letter].set_text(options[i])

        # 核心修复：切换 CSS 类，而不是用内联 style，防止 Quasar 覆盖
        if self.selected_letter == letter:
            btn.classes(add='active')
            btn.style("opacity:1;")
        else:
            btn.classes(remove='active')
            btn.style("opacity:1;" if can_select else "opacity:0.35;")

    has_q = st["round_id"] > 0 and len(options) > 0
    can_start = has_q and not started and not submitted and not self.drawing
    can_submit = has_q and started and not submitted and bool(self.selected_letter)

    self.btn_start.enabled = can_start
    self.btn_submit.enabled = can_submit
    self.btn_quit.enabled = has_q
    _style_btn(self.btn_start, can_start, "#6a3f9a")
    _style_btn(self.btn_submit, can_submit, "#2e7d32")
    _style_btn(self.btn_quit, has_q, "#c62828")
    self._update_sidebar(st)


Music_Guesser._style_btn = _style_btn
Music_Guesser._on_draw = _on_draw
Music_Guesser._on_select = _on_select
Music_Guesser._on_start = _on_start
Music_Guesser._on_submit = _on_submit
Music_Guesser._on_quit = _on_quit
Music_Guesser._update_sidebar = _update_sidebar
Music_Guesser._sync_ui = _sync_ui


if __name__ in {"__main__", "__mp_main__"}:
    os.environ["NO_PROXY"] = "127.0.0.1,localhost"
    # host=0.0.0.0 监听所有网卡：同一局域网的平板访问 http://<笔记本局域网IP>:8000 即可
    # reconnect_timeout 放宽到 15 秒：平板短暂锁屏/切后台/Wi-Fi 抖动后回来，当前对局仍然保留
    ui.run(host="0.0.0.0", port=8000, title="AI vs 人类 · 听歌识曲", show=False, dark=True, reconnect_timeout=15)


