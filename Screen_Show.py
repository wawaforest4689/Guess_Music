import json
import threading
import dash
from dash import dcc, html, Input, Output, ctx  # 引入 ctx 用于判断哪个按钮被点击
import plotly.graph_objs as go
import websockets
import asyncio
import random
from collections import Counter

OPERATOR_WS_URL = "ws://127.0.0.1:8000/ws"
DASH_PORT = 8050


class Panel:
    def __init__(self):
        self.rounds_dict = {}
        self.connected = False
        self._lock = threading.Lock()
        self.song_colors = {}

        # 👈 新增：撤销历史栈
        self.undo_stack = []

        self.app = dash.Dash(__name__)
        self._setup_layout()

    def connect_server(self) -> bool:
        def _listen():
            async def _loop():
                while True:
                    try:
                        async with websockets.connect(OPERATOR_WS_URL) as ws:
                            self.connected = True
                            async for message in ws:
                                self._on_message(message)
                    except Exception:
                        self.connected = False
                        await asyncio.sleep(2)

            asyncio.run(_loop())

        threading.Thread(target=_listen, daemon=True).start()
        return True

    def _on_message(self, message: str):
        try:
            data = json.loads(message)
            with self._lock:
                self.rounds_dict[data["round_id"]] = data
        except Exception:
            pass

    def _recalculate_stats(self):
        if not self.rounds_dict:
            return None

        sorted_rounds = [self.rounds_dict[k] for k in sorted(self.rounds_dict.keys())]

        stats = {
            "total": len(sorted_rounds),
            "h_correct": 0, "a_correct": 0,
            "h_times": [], "a_times": [],
            "h_streak": 0, "h_max_streak": 0,
            "a_streak": 0, "a_max_streak": 0,
            "h_acc_history": [], "a_acc_history": [],
            "round_ids": []
        }

        h_cum_correct = 0
        a_cum_correct = 0

        for i, r in enumerate(sorted_rounds):
            stats["round_ids"].append(r["round_id"])

            if r.get("human_correct"):
                stats["h_correct"] += 1
                h_cum_correct += 1
                stats["h_streak"] += 1
                stats["h_max_streak"] = max(stats["h_max_streak"], stats["h_streak"])
            else:
                stats["h_streak"] = 0

            if r.get("ai_correct"):
                stats["a_correct"] += 1
                a_cum_correct += 1
                stats["a_streak"] += 1
                stats["a_max_streak"] = max(stats["a_max_streak"], stats["a_streak"])
            else:
                stats["a_streak"] = 0

            stats["h_acc_history"].append(h_cum_correct / (i + 1) * 100)
            stats["a_acc_history"].append(a_cum_correct / (i + 1) * 100)

            if r.get("human_time") is not None:
                stats["h_times"].append(r["human_time"])
            if r.get("ai_time") is not None:
                stats["a_times"].append(r["ai_time"])

        return stats, sorted_rounds

    def _setup_layout(self):
        self.app.layout = html.Div(
            style={"minHeight": "100vh", "background": "radial-gradient(circle at 30% 20%, #1a0028, #050008 70%)",
                   "color": "#e8ddff", "fontFamily": "sans-serif", "padding": "24px"},
            children=[
                html.H1("🎵 AI vs 人类 · 实时数据面板", style={"textAlign": "center"}),
                html.Div(id="conn-status", style={"textAlign": "center", "marginBottom": "20px"}),
                html.Div(id="metric-cards",
                         style={"display": "grid", "gridTemplateColumns": "repeat(4, 1fr)", "gap": "16px"}),
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px", "marginTop": "24px"},
                    children=[
                        dcc.Graph(id="accuracy-chart"),
                        dcc.Graph(id="time-chart"),
                        dcc.Graph(id="streak-chart"),
                        dcc.Graph(id="song-cloud"),
                    ]),

                # 👈 数据表格容器 + 操作按钮
                html.Div(
                    style={"marginTop": "24px"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                                   "marginBottom": "10px"},
                            children=[
                                html.H3("历史记录", style={"margin": "0", "fontSize": "18px", "color": "#a58bc9"}),
                                html.Div(
                                    style={"display": "flex", "gap": "15px"},
                                    children=[
                                        html.Button("↺ 撤销", id="btn-undo", n_clicks=0, style={
                                            "background": "transparent", "border": "1px solid #69f0ae",
                                            "color": "#69f0ae", "borderRadius": "8px", "cursor": "pointer",
                                            "padding": "6px 12px", "fontSize": "14px", "fontWeight": "bold"
                                        }),
                                        html.Button("🗑️ 删除", id="btn-delete", n_clicks=0, style={
                                            "background": "transparent", "border": "1px solid #ff8a80",
                                            "color": "#ff8a80", "borderRadius": "8px", "cursor": "pointer",
                                            "padding": "6px 12px", "fontSize": "14px", "fontWeight": "bold"
                                        })
                                    ]
                                )
                            ]
                        ),
                        html.Div(id="recent-table")
                    ]
                ),
                dcc.Interval(id="tick", interval=1000, n_intervals=0),
            ],
        )

        @self.app.callback(
            [Output("metric-cards", "children"), Output("accuracy-chart", "figure"),
             Output("time-chart", "figure"), Output("streak-chart", "figure"),
             Output("song-cloud", "figure"), Output("recent-table", "children"),
             Output("conn-status", "children")],
            [Input("tick", "n_intervals"), Input("btn-delete", "n_clicks"), Input("btn-undo", "n_clicks")],
        )
        def _refresh(n_intervals, n_del, n_undo):
            with self._lock:
                # 👈 处理删除操作
                if ctx.triggered_id == "btn-delete":
                    if self.rounds_dict:
                        max_id = max(self.rounds_dict.keys())
                        deleted_record = self.rounds_dict.pop(max_id)
                        self.undo_stack.append(deleted_record)  # 压入撤销栈

                # 👈 处理撤销操作
                elif ctx.triggered_id == "btn-undo":
                    if self.undo_stack:
                        record = self.undo_stack.pop()
                        self.rounds_dict[record["round_id"]] = record  # 恢复记录

                return self._render()

    def _render(self):
        status = "🟢 已连接操作端" if self.connected else "🔴 等待操作端连接……"
        res = self._recalculate_stats()

        if not res:
            return [html.Div("暂无数据")], go.Figure(), go.Figure(), go.Figure(), go.Figure(), html.Div(
                "暂无历史记录"), status

        stats, sorted_rounds = res

        # --- 1. 指标卡 ---
        h_acc = stats["h_acc_history"][-1] if stats["h_acc_history"] else 0
        a_acc = stats["a_acc_history"][-1] if stats["a_acc_history"] else 0
        h_avg = sum(stats["h_times"]) / len(stats["h_times"]) if stats["h_times"] else 0
        a_avg = sum(stats["a_times"]) / len(stats["a_times"]) if stats["a_times"] else 0

        def card(t, v, c):
            return html.Div(
                style={"background": "rgba(30,10,50,.85)", "border": f"1px solid {c}", "borderRadius": "14px",
                       "padding": "16px", "textAlign": "center"}, children=[
                    html.Div(t, style={"fontSize": "13px", "color": "#a58bc9"}),
                    html.Div(str(v), style={"fontSize": "28px", "fontWeight": "700", "color": c})
                ])

        cards = [card("总轮次", stats["total"], "#b388ff"),
                 card("当前人类准确率", f"{h_acc:.1f}%", "#69f0ae"),
                 card("当前AI准确率", f"{a_acc:.1f}%", "#ffd54f"),
                 card("平均用时", f"{h_avg:.1f}s / {a_avg:.1f}s", "#80d8ff")]

        # --- 2. 准确率时序折线图 ---
        acc_fig = go.Figure()
        acc_fig.add_trace(go.Scatter(
            x=stats["round_ids"], y=stats["h_acc_history"], mode="lines+markers", name="人类准确率",
            line=dict(color="#69f0ae", width=3), marker=dict(size=8, color="#69f0ae")
        ))
        acc_fig.add_trace(go.Scatter(
            x=stats["round_ids"], y=stats["a_acc_history"], mode="lines+markers", name="AI 准确率",
            line=dict(color="#ffd54f", width=3), marker=dict(size=8, color="#ffd54f")
        ))
        acc_fig.update_layout(
            title="累计准确率时序统计（%）", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e8ddff", yaxis=dict(range=[0, 105], gridcolor="rgba(255,255,255,0.1)"),
            xaxis=dict(title="轮次", gridcolor="rgba(255,255,255,0.1)")
        )

        # --- 3. 用时散点箱线图 (Box Plot) ---
        time_fig = go.Figure()
        if stats["h_times"]:
            time_fig.add_trace(go.Box(
                y=stats["h_times"], name="人类", marker_color="#69f0ae", line_color="#69f0ae",
                boxpoints='all', jitter=0.5, pointpos=-1.8, marker=dict(size=8, opacity=0.8)
            ))
        if stats["a_times"]:
            time_fig.add_trace(go.Box(
                y=stats["a_times"], name="AI", marker_color="#ffd54f", line_color="#ffd54f",
                boxpoints='all', jitter=0.5, pointpos=1.8, marker=dict(size=8, opacity=0.8)
            ))
        time_fig.update_layout(
            title="作答用时分布（秒）", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e8ddff", yaxis=dict(gridcolor="rgba(255,255,255,0.1)")
        )

        # --- 4. 连对横向条形图 ---
        streak_y = ["人类最大连对", "AI 最大连对", "人类当前连对", "AI 当前连对"]
        streak_x = [stats["h_max_streak"], stats["a_max_streak"], stats["h_streak"], stats["a_streak"]]
        streak_colors = ["#69f0ae", "#ffd54f", "#69f0ae", "#ffd54f"]

        streak_fig = go.Figure(go.Bar(
            x=streak_x, y=streak_y, orientation='h', marker_color=streak_colors,
            text=streak_x, textposition='auto', textfont=dict(color="#000", weight="bold")
        ))
        streak_fig.update_layout(
            title="连对次数统计", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e8ddff", xaxis=dict(title="次数", gridcolor="rgba(255,255,255,0.1)"),
            yaxis=dict(autorange="reversed"), margin=dict(l=120)
        )

        # --- 5. 词云图 ---
        song_counter = Counter(r.get("correct_song", "?") for r in sorted_rounds)
        words = list(song_counter.keys())
        counts = list(song_counter.values())

        palette = ["#b388ff", "#69f0ae", "#ffd54f", "#80d8ff", "#ff8a80", "#b2ff59", "#e040fb", "#ffab40", "#64ffda"]

        fixed_colors = []
        for w in words:
            if w not in self.song_colors:
                self.song_colors[w] = random.choice(palette)
            fixed_colors.append(self.song_colors[w])

        cloud_fig = go.Figure(
            go.Scatter(
                x=[i % 6 for i in range(len(words))], y=[i // 6 for i in range(len(words))],
                mode="text", text=words, hoverinfo="text",
                textfont=dict(
                    size=[max(14, 12 + c * 8) for c in counts],
                    color=fixed_colors
                )
            )
        )
        cloud_fig.update_layout(
            title="正确答案歌名", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e8ddff", xaxis=dict(visible=False), yaxis=dict(visible=False)
        )

        # --- 6. 数据表格 ---
        header = html.Tr([html.Th(c) for c in ["轮次", "终端", "难度", "歌曲", "人类", "AI", "人类用时", "AI用时"]])
        rows = []
        for r in sorted_rounds[-8:][::-1]:
            rows.append(html.Tr(
                [
                    html.Td(str(r["round_id"])),
                    html.Td(r.get("player") or "—"),
                    html.Td(r.get("difficulty", "—").capitalize()),
                    html.Td(r.get("correct_song", "")),
                    html.Td(r.get("human_choice") or "—"),
                    html.Td(r.get("ai_choice") or "—"),
                    html.Td(f'{r.get("human_time", "—")}s'),
                    html.Td(f'{r.get("ai_time", "—")}s')
                ]))

        return cards, acc_fig, time_fig, streak_fig, cloud_fig, html.Table([html.Thead(header), html.Tbody(rows)],
                                                                           style={"width": "100%",
                                                                                  "fontSize": "13px"}), status

    def run(self):
        self.connect_server()
        self.app.run(host="0.0.0.0", port=DASH_PORT, debug=False)


if __name__ == "__main__":
    panel = Panel()
    panel.run()


