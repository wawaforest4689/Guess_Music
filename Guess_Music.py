# -*- coding: utf-8 -*-
import os
import re
import time
import random
import json
import base64
import asyncio
import threading
from pathlib import Path
from typing import Optional, Set
import traceback
import wave

import websocket
from pydub import AudioSegment
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from nicegui import ui, app

# ---------------- FFmpeg 路径 ----------------
AudioSegment.converter = r"D:\Applications\ffmpeg\bin\ffmpeg.exe"

# ---------------- 基础配置 ----------------
BASE_DIR = r"d:\wf200\Music\长途车载音乐"
MUSIC_DIR = os.path.join(BASE_DIR, "2026")
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


ws_manager = WSManager()
guesser_instance = None


# =====================================================================
#  核心业务类
# =====================================================================
class Music_Guesser:
    def __init__(self):
        self.api_key: str = ""
        self.realtime_url: str = REALTIME_WS_URL
        self.model_name: str = "qwen-audio-3.1-realtime-flash"

        self.difficulty = "easy"
        self.max_time = 60
        self.options_count = 4

        self.round_id = 0
        self.question_id = 0
        self.correct_song = ""
        self.options = []
        self.clip_url_path = ""
        self.clip_path = ""
        self.human_start_time = 0.0

        self.round_states = {}
        self.lock = threading.Lock()

        self.selected_letter: Optional[str] = None
        self._last_synced_round = 0
        self.option_btns = {}
        self.option_labels = {}

    def set_difficulty(self, level: str):
        self.difficulty = level
        self.max_time = DIFFICULTY_CONFIG[level]["max_time"]
        self.options_count = DIFFICULTY_CONFIG[level]["options"]
        # 重置当前状态
        self.question_id=0
        self.options = []
        with self.lock:
            # 遍历并彻底清理
            for state in self.round_states.values():
                timer = state.pop("timer", None)
                ai_timer = state.pop("ai_timer", None)
                if timer: timer.cancel()
                if ai_timer: ai_timer.cancel()
            self.round_states.clear()

    def set_config(self, api_key: Optional[str] = None, realtime_url: Optional[str] = None,
                   model_name: Optional[str] = None):
        if api_key: self.api_key = api_key
        if realtime_url: self.realtime_url = realtime_url
        if model_name: self.model_name = model_name


    def _realtime_ask(self, prompt: str) -> str:
        temp_wav = os.path.join(str(STATIC_DIR), f"tempclip_{int(time.process_time())}_16k.wav")
        try:
            seg = AudioSegment.from_file(self.clip_path).set_frame_rate(16000).set_channels(1).set_sample_width(2)
            seg.export(temp_wav, format="wav")

            url = f"{self.realtime_url}?model={self.model_name}" if "?" not in self.realtime_url else f"{self.realtime_url}&model={self.model_name}"
            headers = [f"Authorization: Bearer {self.api_key}"]

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

    async def _process_ai_task(self, q_id: int, prompt: str):
        start_wall = time.monotonic()
        try:
            content = await asyncio.wait_for(asyncio.to_thread(self._realtime_ask, prompt), timeout=self.max_time)
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
            if not state or "ai_reasoning" in state: return
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
                "ai_choice": a_choice, "ai_time": a_time, "ai_correct": ai_correct,"difficulty":self.difficulty
            }

    def _notify_result(self, r_id: int):
        payload = self.build_result_payload(r_id)
        if payload:
            asyncio.create_task(ws_manager.broadcast(payload))

    def _start_human_timer(self, r_id: int):
        timer = threading.Timer(self.max_time, self._on_human_timeout, args=(r_id,))
        timer.daemon = True
        with self.lock:
            if self.round_states.get(r_id): self.round_states[r_id]["timer"] = timer
        timer.start()

    def _on_human_timeout(self, q_id: int):
        with self.lock:
            if q_id != self.question_id: return
            state = self.round_states.get(q_id)
            if not state or not state.get("started") or state.get("submitted"): return
            state["submitted"] = True
            state["timed_out"] = True
            state["human_choice"] = "Z"
            state["human_time"] = self.max_time
            state.pop("timer", None)
        self._try_finalize(q_id)

    def _start_ai_timer(self, r_id: int):
        timer = threading.Timer(self.max_time, self._on_ai_timeout, args=(r_id,))
        timer.daemon = True
        with self.lock:
            if self.round_states.get(r_id): self.round_states[r_id]["ai_timer"] = timer
        timer.start()

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
        self.correct_song = correct_path.split(".")[0]
        others = [p for p in songs if p != correct_path]
        self.options = [self.correct_song] + random.sample([p.split(".")[0] for p in others], k=self.options_count - 1)
        random.shuffle(self.options)

        audio = AudioSegment.from_file(os.path.join(MUSIC_DIR, correct_path))
        clip = audio if len(audio) <= CLIP_DURATION_MS else audio[random.randint(0, len(audio) - CLIP_DURATION_MS):][
            :CLIP_DURATION_MS]

        clip_name = f"clip_{self.question_id}_{int(time.process_time())}.wav"
        clip_path = Path(STATIC_DIR) / clip_name
        clip.export(str(clip_path), format="wav")

        self.clip_path = str(clip_path)
        self.clip_url_path = f"/audio/{clip_name}"

        self.question_id += 1
        current_qid = self.question_id
        with self.lock:
            self.round_states[current_qid] = {
                "song": self.correct_song, "options": list(self.options),
                "correct_letter": chr(65 + self.options.index(self.correct_song)),
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

            self.human_start_time = time.monotonic()
            current_rid, options = self.question_id, list(state["options"])

        self._start_human_timer(current_rid)
        self._start_ai_timer(current_rid)

        opts_text = "\n".join(f"{chr(65 + i)}. {opt}" for i, opt in enumerate(options))
        prompt = f"听这段15秒的音乐片段，从以下选项中选出最匹配的歌曲，限时{self.max_time}秒：\n{opts_text}\n先给50字内推理，最后输出格式为'答案：A'"
        print(f"Turn {current_rid}: {prompt}")

        asyncio.create_task(self._process_ai_task(current_rid, prompt))
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

            # 原地重置作答状态
            state["started"] = False
            state["submitted"] = False
            state["timed_out"] = False
            state.pop("human_choice", None)
            state.pop("human_time", None)
            state.pop("ai_choice", None)
            state.pop("ai_time", None)
            state.pop("ai_reasoning", None)

            # 清空结算标记
            state.pop("finalized", None)
            state.pop("valid_round_id", None)

        return {"ok": True}

    def _on_save_config(self):
        self.set_config(
            api_key=self.key_input.value or "",
            realtime_url=self.url_input.value or "",
            model_name=self.model_input.value or "")
        ui.notify("✅ 配置已保存", color="positive", position="bottom-left")

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
                self.round_id += 1  # 👈 大屏有效轮次 +1
                state["finalized"] = True  # 标记已结算
                state["valid_round_id"] = self.round_id
                is_finalize=True

        if is_finalize:
            # 结算完成后，立刻向大屏广播这唯一的一次最终结果
            self._notify_result(q_id)


# =====================================================================
#  NiceGUI 挂载 FastAPI 路由
# =====================================================================
app.mount("/audio", StaticFiles(directory=str(STATIC_DIR)), name="audio")


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        if guesser_instance and guesser_instance.round_id > 0:
            payload = guesser_instance.build_result_payload(guesser_instance.round_id)
            if payload:
                await websocket.send_text(json.dumps(payload, ensure_ascii=False))
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
SIDEBAR_HTML = '''
<div id="ai_sidebar_wrapper" style="display:none;">
  <div style="position:fixed;right:20px;top:20px;width:400px;max-height:80vh;overflow-y:auto;background:rgba(20,0,35,.92);border:1px solid #6a3f9a;border-radius:16px;padding:16px;color:#e8ddff;font-size:14px;z-index:100;">
    <div><b>🤖 AI 助手</b></div><hr style="border-color:#3d2055;"><div id="ai_content">等待提交作答...</div>
  </div>
</div>
'''
HEAD_CSS = '''
<style>
body { margin: 0; } 
.nicegui-content { background: transparent !important; }
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
</style>
'''

def _on_save_config(self):
    self.set_config(api_key=self.key_input.value or "", realtime_url=self.url_input.value or "",
                    model_name=self.model_input.value or "")
    ui.notify("✅ 配置已保存", color="positive", position="bottom-left")


@ui.page("/")
def index_page():
    ui.add_head_html(HEAD_CSS)
    ui.add_body_html(WAVE_BG_HTML)

    with ui.column().style("width:100%;height:100vh;align-items:center;justify-content:center;gap:24px;"):
        ui.label("🎵 AI vs 人类 · 听歌识曲对抗").style(
            "font-size:42px;font-weight:800;color:#e8ddff;text-shadow:0 0 20px #b388ff;margin-bottom:20px;")

        for level, cfg in DIFFICULTY_CONFIG.items():
            ui.button(cfg["label"], on_click=lambda l=level: ui.navigate.to(f"/game/{l}")).style(
                "width:300px;height:60px;border-radius:999px;background:#6a3f9a;color:#fff;font-size:20px;font-weight:bold;"
            )
        global guesser_instance
        g=guesser_instance
        with ui.expansion("⚙️ 配置 API 接口").style("position:fixed;left:20px;bottom:20px;width:360px;background:rgba(15,0,25,.92);border:1px solid #4a2a6a;border-radius:12px;color:#d8c8ff;"):
            g.key_input=ui.input("API Key", password=True, password_toggle_button=True).style("width:100%;")
            g.url_input=ui.input("Realtime WS URL", value=g.realtime_url).style("width:100%;")
            g.model_input=ui.input("Model Setting", value=g.model_name).style("width:100%;")
            ui.button("保存", on_click=g._on_save_config).style("width:100%;background:#6a3f9a;color:#fff;")


@ui.page("/game/{level}")
def game_page(level: str):
    if level not in DIFFICULTY_CONFIG:
        ui.navigate.to("/")
        return

    global guesser_instance
    guesser_instance.set_difficulty(level)
    g = guesser_instance

    ui.add_head_html(HEAD_CSS)
    ui.add_body_html(WAVE_BG_HTML)
    ui.add_body_html(SIDEBAR_HTML)

    # 左上角返回按钮
    ui.button(icon='arrow_back', on_click=lambda: ui.navigate.to('/')).props('flat color=white').style(
        "position:fixed;left:20px;top:20px;font-size:24px;z-index:50;background:rgba(106,63,154,0.5);border-radius:50%;"
    )

    with ui.column().style("width:100%;align-items:center;"):
        ui.label(f"🎵 AI vs 人类 · {DIFFICULTY_CONFIG[level]['label']}").style(
            "font-size:34px;font-weight:800;color:#e8ddff;margin-top:26px;text-shadow:0 0 18px #b388ff;")

        with ui.row().style("margin-top:22px;gap:24px;"):
            g.btn_draw = ui.button("🎲 抽取并播放音乐", on_click=g._on_draw).style(
                "background:#6a3f9a;color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;")
            g.btn_start = ui.button("▶ 开始作答", on_click=g._on_start).style(
                "color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;background:#3a3a3a;opacity:0.55;")

        g.audio_html = ui.html(
            '<audio id="music_player" controls style="width:680px;height:56px;margin-top:24px;"></audio>')

        # 动态生成 A, B, C... 按钮
        letters = [chr(65 + i) for i in range(g.options_count)]
        gap_style = "gap:30px;" if g.options_count > 4 else "gap:56px;"

        with ui.row().style(f"margin-top:34px;{gap_style} justify-content:center;"):
            for letter in letters:
                # 使用 props('flat') 强制移除 Quasar 蓝色底，由 CSS 统一接管
                btn = ui.button(letter, on_click=lambda l=letter: g._on_select(l)).props('flat').classes('circle-btn')
                btn.style("width:80px;height:80px;opacity:0.35;")
                g.option_btns[letter] = btn

        with ui.row().style(f"margin-top:14px;{gap_style} justify-content:center;"):
            for letter in letters:
                g.option_labels[letter] = ui.label("—").style(
                    "width:80px;text-align:center;color:#cbb7ee;font-size:15px;")

        with ui.row().style("margin-top:36px;gap:24px;"):
            g.btn_submit = ui.button("✅ 提交作答", on_click=g._on_submit).style(
                "color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;background:#3a3a3a;opacity:0.55;")
            g.btn_quit = ui.button("↺ 重置本题", on_click=g._on_quit).style(
                "color:#fff;font-size:16px;font-weight:bold;border-radius:999px;padding:14px 28px;background:#3a3a3a;opacity:0.55;")

    ui.timer(0.4, g._sync_ui)


# =====================================================================
#  事件处理扩展 (注入到类中)
# =====================================================================
def _style_btn(btn, enabled, ready_color):
    btn.style(f"background:{ready_color};opacity:1;" if enabled else "background:#3a3a3a;opacity:0.55;")


def _on_draw(self):
    self.choose_music()
    for i, letter in enumerate([chr(65 + i) for i in range(self.options_count)]):
        self.option_labels[letter].set_text(self.options[i])
    self._sync_ui()


def _on_select(self, letter):
    st = self.get_ui_state()
    if not st["started"] or st["submitted"]: return
    self.selected_letter = None if self.selected_letter == letter else letter
    self._sync_ui()


def _on_start(self):
    self.start_round_api(self.question_id)
    full_url = self.clip_url_path
    ui.run_javascript(f"""
        (function() {{
            const p = document.getElementById('music_player');
            if (p) {{ p.src = {json.dumps(full_url)}; p.currentTime = 0; const r = p.play(); if (r) r.catch(function(){{}}); }}
        }})();
    """)
    self._sync_ui()


def _on_submit(self):
    if self.selected_letter:
        self.submit_api(self.question_id, self.selected_letter)
    self._sync_ui()


def _on_quit(self):
    self.quit_api(self.question_id)
    self.selected_letter = None
    self._sync_ui()


def _update_sidebar(self, st: dict):
    if not st.get("submitted"):
        ui.run_javascript("document.getElementById('ai_sidebar_wrapper').style.display='none';")
        return
    html = ""
    if st.get("timed_out"):
        html += '<div style="color:#ff8a80;font-weight:bold;">⏰ 人类超时，回答错误</div><hr style="border-color:#3d2055;">'
    if st["status"] == "thinking":
        html += "AI 做题中... ⏳"
    elif st["status"] == "done":
        html += f'<b>思考过程：</b><br><span style="color:#cfc0ff">{st.get("reasoning") or "无"}</span><hr style="border-color:#3d2055;">'
        html += f'<b>最终选择：</b><span style="color:#ffd54f;font-size:20px"> {st.get("choice") or "无"} </span>'
        if st.get("ai_time") is not None: html += f'<br><span style="color:#aaa;">AI 用时：{st["ai_time"]} 秒</span>'
        if st.get("ai_correct") is False:
            html += '<br><span style="color:#ff8a80;">AI 本题判错</span>'
        elif st.get("ai_correct") is True:
            html += '<br><span style="color:#81c784;">AI 本题答对</span>'
    ui.run_javascript(f"""
        (function() {{
            const w = document.getElementById('ai_sidebar_wrapper'), c = document.getElementById('ai_content');
            if (w && c) {{ w.style.display = 'block'; c.innerHTML = {json.dumps(html)}; }}
        }})();
    """)


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
    can_start = has_q and not started and not submitted
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
    guesser_instance = Music_Guesser()
    ui.run(host="0.0.0.0", port=8000, title="AI vs 人类 · 听歌识曲", show=False, dark=True)


