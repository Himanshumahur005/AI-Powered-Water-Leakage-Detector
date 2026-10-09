"""
Live Water Leak Detection - Streamlit dashboard (with username + password login)
* Works with VS Code's normal Run button (relaunches itself through Streamlit)
* Login screen: nobody can see the dashboard without the username and password
* Data source: MQTT (ESP32 / flow sensor via broker) or built-in Simulation
* Saves every reading to live_water_data.csv AND to a SQLite database (history)
* Live graph with leak threshold line and red X on leaks
* New leak => flashing alarm + LOOPING siren until ACKNOWLEDGE is pressed
* STOP button => one click stops the whole system
* HISTORY button => browse past days from the database
"""

# =====================================================================
# ONE-CLICK RUN SUPPORT (VS Code Run button)
# =====================================================================
import os
import sys
import subprocess
from pathlib import Path

if __name__ == "__main__" and os.environ.get("WATER_DASHBOARD_STREAMLIT") != "1":
    _env = os.environ.copy()
    _env["WATER_DASHBOARD_STREAMLIT"] = "1"
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(Path(__file__).resolve())],
        env=_env,
        check=False,
    )
    raise SystemExit

# =====================================================================
import hmac
import io
import json
import random
import sqlite3
import time
import wave
from collections import deque
from datetime import datetime, timedelta
from html import escape

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

# ============================ SETTINGS ============================
CSV_FILE = "live_water_data.csv"
DB_FILE = "water_history.db"
MAX_KEEP = 5000
DEFAULT_THRESHOLD = 7.0

DEFAULT_BROKER = "broker.hivemq.com"
DEFAULT_PORT = 1883
DEFAULT_TOPIC = "waterleak/CHANGE_ME_1234/meter1/flow"   # must match the ESP32 topic

# ---- LOGIN ----  (CHANGE THESE! You can also set DASH_USER / DASH_PASSWORD environment variables)
PLACEHOLDER_PASSWORD = "ChangeMe123"
LOGIN_USER = os.environ.get("DASH_USER", "water")
LOGIN_PASSWORD = os.environ.get("DASH_PASSWORD", "water@123")

st.set_page_config(page_title="Live Water Leak Detection", page_icon="💧", layout="wide")


# ============================ LOGIN ============================
def login_gate():
    """Blocks the whole dashboard until the correct username + password is entered."""
    if LOGIN_PASSWORD == PLACEHOLDER_PASSWORD or not LOGIN_PASSWORD:
        st.error("Set your own password first: change LOGIN_PASSWORD (and LOGIN_USER) "
                 "near the top of this file, or set the DASH_PASSWORD environment variable. "
                 "Then restart the dashboard.")
        st.stop()
    if st.session_state.get("authed"):
        return

    st.title("🔒 Water Leak Dashboard - Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")
    if st.button("Login"):
        ok_u = hmac.compare_digest(u.encode(), LOGIN_USER.encode())
        ok_p = hmac.compare_digest(p.encode(), LOGIN_PASSWORD.encode())
        if ok_u and ok_p:
            st.session_state["authed"] = True
            st.rerun()
        time.sleep(1)                          # slows down password guessing
        st.error("Wrong username or password")
    st.stop()


login_gate()


# ============================ DATA SOURCE: SIMULATION ============================
def read_flow():
    """Simulated sensor (used only when Data source = Simulation)."""
    flow = random.uniform(1.8, 3.0)
    if random.random() < 0.08:              # occasional leak
        flow = random.uniform(8.0, 10.0)
    return round(flow, 2)


# ============================ DATA SOURCE: MQTT ============================
class MqttReceiver:
    """Background MQTT subscriber. Readings wait in a queue until the dashboard collects them."""

    def __init__(self, host, port, topic, user, pwd, tls):
        self.topic = topic
        self.queue = deque(maxlen=10000)
        self.connected = False
        self.last_rx = None
        self.error = ""
        cid = f"streamlit-water-{random.randint(0, 999999)}"
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid)
        except AttributeError:                       # older paho-mqtt (1.x)
            self.client = mqtt.Client(client_id=cid)
        if user:
            self.client.username_pw_set(user, pwd)
        if tls:
            self.client.tls_set()
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(1, 30)
        try:
            self.client.connect_async(host, port, keepalive=30)
            self.client.loop_start()
        except Exception as e:
            self.error = str(e)

    def _on_connect(self, client, userdata, flags, rc, *extra):
        failed = rc.is_failure if hasattr(rc, "is_failure") else (rc != 0)
        if failed:
            self.connected = False
            self.error = f"connection refused ({rc})"
            return
        self.connected = True
        self.error = ""
        client.subscribe(self.topic, qos=1)          # re-subscribes after every reconnect

    def _on_disconnect(self, client, userdata, *args):
        self.connected = False

    def _on_message(self, client, userdata, msg):
        flow = self.parse(msg.payload)
        if flow is None:
            return
        self.queue.append((pd.Timestamp(datetime.now()), flow))
        self.last_rx = time.time()

    @staticmethod
    def parse(payload):
        """Accepts  2.5   or   {"flow_rate": 2.5}   or   {"flow": 2.5}   or   {"value": 2.5}"""
        try:
            obj = json.loads(payload.decode("utf-8", "ignore").strip())
        except ValueError:
            return None
        if isinstance(obj, dict):
            for key in ("flow_rate", "flow", "value"):
                if key in obj:
                    obj = obj[key]
                    break
            else:
                return None
        try:
            value = float(obj)
        except (TypeError, ValueError):
            return None
        return value if value >= 0 and value == value else None

    def drain(self):
        items = []
        while True:
            try:
                items.append(self.queue.popleft())
            except IndexError:
                break
        return items

    def close(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass


@st.cache_resource
def _mqtt_holder():
    return {"key": None, "rx": None}


def get_mqtt(host, port, topic, user, pwd, tls):
    """One shared MQTT connection; recreated only when the settings change."""
    holder = _mqtt_holder()
    key = (host, int(port), topic, user, pwd, bool(tls))
    if holder["key"] != key:
        if holder["rx"] is not None:
            holder["rx"].close()
        holder["rx"] = MqttReceiver(host, int(port), topic, user, pwd, tls)
        holder["key"] = key
    return holder["rx"]


# ============================ CSS ============================
st.markdown("""
<style>
.block-container {padding-top: 1rem; max-width: 1450px;}
[data-testid="stAudio"] {display: none;}

.hero {background: linear-gradient(120deg,#0b1220,#12305c 60%,#0e7490); border-radius: 18px;
       padding: 20px 26px; color: #e2e8f0; margin-bottom: 14px; display: flex;
       justify-content: space-between; align-items: center; border: 1px solid #1e3a5f;}
.hero h1 {margin: 0; padding: 0; font-size: 1.7rem; color: #f8fafc;}
.hero p {margin: 4px 0 0 0; color: #94a3b8; font-size: 0.92rem;}
.badge {padding: 6px 14px; border-radius: 999px; font-weight: 700; font-size: 0.82rem;
        background: #0f172a; color: #e2e8f0; border: 1px solid #334155;}
.dot {display: inline-block; width: 10px; height: 10px; border-radius: 50%;
      background: #22c55e; margin-right: 7px; animation: blink 1s infinite;}
@keyframes blink {0%,100% {opacity: 1;} 50% {opacity: .25;}}

.panel {background: #1e293b; color: #e2e8f0; border-radius: 14px; padding: 12px 14px;
        border: 1px solid #334155;}
.card {background: #1e293b; color: #e2e8f0; border-radius: 14px; padding: 14px 16px;
       border: 1px solid #334155; border-top: 4px solid #38bdf8;}
.card-title {font-size: .74rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .08em;}
.card-value {font-size: 1.6rem; font-weight: 800; line-height: 1.25; margin-top: 2px;}
.card-sub {font-size: .78rem; color: #94a3b8; margin-top: 2px;}

.alarm-panel {color: white; border-radius: 16px; padding: 22px; text-align: center;
              border: 3px solid #fecaca; animation: flash .9s infinite; margin-bottom: 8px;}
@keyframes flash {0%,100% {background: #ef4444;} 50% {background: #7f1d1d;}}
.alarm-title {font-size: 2rem; font-weight: 800;}
.alarm-sub {margin-top: 6px; font-size: 1.05rem;}
.strip {border-radius: 14px; padding: 14px; text-align: center; font-weight: 700;
        font-size: 1.3rem; color: white; margin-bottom: 8px;}
.strip-ok {background: #15803d;}
.strip-warn {background: #b45309;}
.strip-stop {background: #475569;}

.gauge-wrap {background: #1e293b; border: 1px solid #334155; border-radius: 14px;
             padding: 18px; color: #e2e8f0; height: 100%;}
.gauge-bar {position: relative; height: 26px; border-radius: 13px; background: #334155;
            margin: 18px 0 10px 0; overflow: hidden;}
.gauge-fill {height: 100%; border-radius: 13px;}
.gauge-limit {position: absolute; top: 0; bottom: 0; width: 3px; background: #f8fafc;}
.gauge-value {font-size: 2.1rem; font-weight: 800; text-align: center;}
.gauge-sub {text-align: center; color: #94a3b8; font-size: .85rem;}

.sect {font-size: 1.05rem; font-weight: 700; margin: 16px 0 6px 0;}
.tbl {width: 100%; border-collapse: collapse; font-size: .88rem; color: #e2e8f0;}
.tbl th {background: #334155; padding: 7px; text-align: center;}
.tbl td {padding: 6px; text-align: center; border-bottom: 1px solid #334155;}
.tbl tr.leak td {color: #f87171; font-weight: 700;}

.st-key-ack button {background: #f59e0b !important; color: #111827 !important;
                    font-weight: 800 !important; font-size: 1.1rem !important;
                    width: 100%; border: none !important; padding: .7rem 1rem;}

.st-key-stopbtn button {background: #dc2626 !important; color: white !important;
                        font-weight: 800 !important; font-size: 1.1rem !important;
                        width: 100%; border: 2px solid #fecaca !important; padding: .6rem 1rem;}
.st-key-startbtn button {background: #16a34a !important; color: white !important;
                         font-weight: 800 !important; font-size: 1.1rem !important;
                         width: 100%; border: none !important; padding: .6rem 1rem;}
.st-key-histbtn button {background: #2563eb !important; color: white !important;
                        font-weight: 800 !important; font-size: 1.1rem !important;
                        width: 100%; border: none !important; padding: .6rem 1rem;}
</style>
""", unsafe_allow_html=True)


# ============================ DATABASE (SQLite history) ============================
def init_db():
    con = sqlite3.connect(DB_FILE, timeout=10)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS readings (
                           id        INTEGER PRIMARY KEY AUTOINCREMENT,
                           timestamp TEXT NOT NULL,
                           flow_rate REAL NOT NULL,
                           status    TEXT NOT NULL)""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings(timestamp)")
        con.commit()
    finally:
        con.close()


def db_insert(ts, flow, status):
    try:
        con = sqlite3.connect(DB_FILE, timeout=10)
        try:
            con.execute("INSERT INTO readings (timestamp, flow_rate, status) VALUES (?, ?, ?)",
                        (f"{ts:%Y-%m-%d %H:%M:%S}", float(flow), status))
            con.commit()
        finally:
            con.close()
    except Exception as e:
        st.session_state["storage_msg"] = f"Database error: {e}"


def db_history(start_date, end_date):
    """All readings from start_date 00:00:00 to end_date 23:59:59."""
    con = sqlite3.connect(DB_FILE, timeout=10)
    try:
        return pd.read_sql_query(
            "SELECT timestamp, flow_rate, status FROM readings "
            "WHERE timestamp >= ? AND timestamp < ? ORDER BY timestamp",
            con, params=(f"{start_date} 00:00:00", f"{end_date + timedelta(days=1)} 00:00:00"))
    finally:
        con.close()


init_db()


def _history_body():
    today = datetime.now().date()
    rng = st.date_input("Select date range", value=(today, today), max_value=today,
                        key="hist_range")
    if not (isinstance(rng, (list, tuple)) and len(rng) == 2):
        st.info("Pick both a start date and an end date.")
        return
    h = db_history(rng[0], rng[1])
    if h.empty:
        st.info("No readings stored for this period.")
        return

    h["timestamp"] = pd.to_datetime(h["timestamp"])
    leaks = h[h["status"] == "POSSIBLE LEAK"]
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Readings", f"{len(h):,}")
    m2.metric("Average flow", f"{h['flow_rate'].mean():.2f} L/min")
    m3.metric("Peak flow", f"{h['flow_rate'].max():.2f} L/min")
    m4.metric("Leak readings", f"{len(leaks):,}")

    plot = h.set_index("timestamp")["flow_rate"]
    if len(plot) > 3000:                       # keep the chart light for long periods
        plot = plot.resample("1min").mean().dropna()
    st.line_chart(plot)

    tab_all, tab_leak = st.tabs(["All readings", "Leak events only"])
    with tab_all:
        st.dataframe(h.iloc[::-1], hide_index=True)
    with tab_leak:
        st.dataframe(leaks.iloc[::-1], hide_index=True)

    st.download_button("⬇ Download this period as CSV", h.to_csv(index=False).encode("utf-8"),
                       file_name="water_history.csv", mime="text/csv")


_dialog = getattr(st, "dialog", None) or getattr(st, "experimental_dialog", None)
if _dialog:
    @_dialog("📜 Water Flow History", width="large")
    def history_dialog():
        _history_body()
else:
    def history_dialog():
        st.warning("Please upgrade Streamlit:  pip install --upgrade streamlit")


# ============================ STATE + STORAGE ============================
def load_existing():
    rows = []
    if os.path.exists(CSV_FILE):
        try:
            df = pd.read_csv(CSV_FILE)
            for ts, fl in zip(df["timestamp"], df["flow_rate"]):
                rows.append((pd.to_datetime(ts), float(fl)))
        except Exception:
            pass
    return rows[-MAX_KEEP:]


def init_state():
    ss = st.session_state
    ss.setdefault("running", True)          # system ON/OFF (shared by checkbox + STOP button)
    if "data" not in ss:
        ss["data"] = load_existing()
        ss["alarm_latched"] = False
        ss["alarm_info"] = None
        ss["was_leak"] = False
        ss["alert_count"] = 0
        ss["storage_msg"] = "Storage: ready"


def save_row(ts, flow, limit):
    status = "POSSIBLE LEAK" if flow > limit else "NORMAL"
    db_insert(ts, flow, status)                       # save to the database (history)
    try:
        new_file = not os.path.exists(CSV_FILE)
        with open(CSV_FILE, "a", encoding="utf-8", newline="") as f:
            if new_file:
                f.write("timestamp,flow_rate,status\n")
            f.write(f"{ts:%Y-%m-%d %H:%M:%S},{flow},{status}\n")
        st.session_state["storage_msg"] = f"Saved to {CSV_FILE} and {DB_FILE}"
    except PermissionError:
        st.session_state["storage_msg"] = "File is OPEN in Excel - close it!"
    except Exception as e:
        st.session_state["storage_msg"] = f"Storage error: {e}"


def process_reading(ts, flow, limit):
    """Store one reading and latch the alarm on a NEW leak (normal -> leak). Returns True if alarmed."""
    ss = st.session_state
    ss["data"].append((ts, flow))
    save_row(ts, flow, limit)
    leak_now = flow > limit
    new_alarm = leak_now and not ss["was_leak"]
    if new_alarm:
        ss["alarm_latched"] = True
        ss["alarm_info"] = (ts.strftime("%Y-%m-%d %H:%M:%S"), flow)
        ss["alert_count"] += 1
    ss["was_leak"] = leak_now
    return new_alarm


def acknowledge():
    """The ONLY way to stop the buzzer (while the system is running)."""
    st.session_state["alarm_latched"] = False


def stop_system():
    """STOP button: one click stops data, saving, alarm sound and auto-refresh."""
    ss = st.session_state
    ss["running"] = False
    ss["alarm_latched"] = False
    ss["was_leak"] = False
    ss["storage_msg"] = "System stopped - no data is being recorded"


def start_system():
    st.session_state["running"] = True
    st.session_state["flush_queue"] = True            # ignore readings that arrived while stopped
    st.session_state["storage_msg"] = "System started"


def clear_data():
    """Clears the live graph + CSV. The database history is NOT deleted."""
    ss = st.session_state
    ss["data"] = []
    ss["was_leak"] = False
    ss["alarm_latched"] = False
    ss["alert_count"] = 0
    try:
        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)
        ss["storage_msg"] = "Storage: cleared (database history kept)"
    except PermissionError:
        ss["storage_msg"] = "Close Excel, then press Clear again"


init_state()


# ============================ SOUND ============================
@st.cache_data
def alarm_wav():
    """Two-tone siren (loops cleanly)."""
    rate = 44100

    def tone(freq, seconds):
        t = np.arange(int(rate * seconds)) / rate
        return np.sin(2 * np.pi * freq * t)

    samples = np.concatenate([tone(1000, 0.4), tone(1400, 0.4)]) * 0.6
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes((samples * 32767).astype(np.int16).tobytes())
    return buf.getvalue()


# ============================ SIDEBAR ============================
st.sidebar.title("⚙️ Settings")
threshold = st.sidebar.number_input("Leakage threshold (L/min)", 1.0, 50.0,
                                    DEFAULT_THRESHOLD, 0.5)
running = st.sidebar.checkbox("▶ Live data running", key="running")
refresh_seconds = st.sidebar.slider("Refresh interval (seconds)", 1, 5, 1)
show_points = st.sidebar.slider("Readings shown on graph", 10, 200, 30, 5)

st.sidebar.markdown("---")
st.sidebar.subheader("📡 Data source")
source = st.sidebar.radio("Source", ["Hardware", "Simulation"], index=0)
mqtt_host = st.sidebar.text_input("Broker host", DEFAULT_BROKER)
mqtt_port = st.sidebar.number_input("Broker port", 1, 65535, DEFAULT_PORT)
mqtt_topic = st.sidebar.text_input("Topic", DEFAULT_TOPIC)
mqtt_user = st.sidebar.text_input("Username (optional)")
mqtt_pass = st.sidebar.text_input("Password (optional)", type="password")
mqtt_tls = st.sidebar.checkbox("Use TLS (port 8883)", value=False)

st.sidebar.markdown("---")
sound_on = st.sidebar.checkbox("🔔 Alarm sound", value=True)
test_buzzer = st.sidebar.button("🔊 Test buzzer")
st.sidebar.caption("Browsers allow sound only after you click on the page. "
                   "Press 'Test buzzer' once to enable it.")
st.sidebar.markdown("---")
st.sidebar.button("🗑 Clear data", on_click=clear_data)

if st.sidebar.button("🔓 Log out"):
    st.session_state["authed"] = False
    st.rerun()

if test_buzzer:
    st.sidebar.audio(alarm_wav(), format="audio/wav", autoplay=True)

use_mqtt = source.startswith("Hardware")
rx = None
if use_mqtt:
    if mqtt is None:
        st.error("The paho-mqtt package is missing. Run:  pip install paho-mqtt")
        st.stop()
    rx = get_mqtt(mqtt_host.strip(), mqtt_port, mqtt_topic.strip(),
                  mqtt_user.strip(), mqtt_pass, mqtt_tls)


# ============================ HELPERS ============================
def info_card(title, value, sub, color):
    return (f'<div class="card" style="border-top-color:{color}">'
            f'<div class="card-title">{title}</div>'
            f'<div class="card-value" style="color:{color}">{value}</div>'
            f'<div class="card-sub">{sub}</div></div>')


def html_table(headers, rows):
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    if rows:
        body = "".join(f'<tr class="{cls}">' + "".join(f"<td>{escape(str(c))}</td>" for c in cells)
                       + "</tr>" for cells, cls in rows)
    else:
        body = f'<tr><td colspan="{len(headers)}">No entries yet</td></tr>'
    return (f'<div class="panel"><table class="tbl"><thead><tr>{head}</tr></thead>'
            f'<tbody>{body}</tbody></table></div>')


def gauge_html(value, limit, peak):
    top = max(limit * 1.6, peak * 1.15, 1.0)
    pct = min(value / top * 100, 100)
    lim = min(limit / top * 100, 100)
    leak = value > limit
    color = "#ef4444" if leak else "#22c55e"
    return (f'<div class="gauge-wrap"><div class="card-title">Flow gauge</div>'
            f'<div class="gauge-bar"><div class="gauge-fill" style="width:{pct:.1f}%;background:{color}"></div>'
            f'<div class="gauge-limit" style="left:{lim:.1f}%"></div></div>'
            f'<div class="gauge-value" style="color:{color}">{value:.2f} L/min</div>'
            f'<div class="gauge-sub">{"POSSIBLE LEAK" if leak else "NORMAL"} &nbsp;|&nbsp; '
            f'limit {limit:g} &nbsp;|&nbsp; scale 0-{top:.0f}</div></div>')


def show_chart(chart):
    try:
        st.altair_chart(chart, width="stretch")
    except TypeError:
        st.altair_chart(chart, use_container_width=True)


def make_chart(df, limit):
    plot = df.copy()
    plot["Status"] = np.where(plot["Flow"] > limit, "Leak", "Normal")
    ymax = float(max(12.0, limit + 1, plot["Flow"].max() + 1))
    ax = dict(labelColor="#94a3b8", titleColor="#94a3b8")

    x = alt.X("Time:T", title="Timestamp", axis=alt.Axis(format="%H:%M:%S", **ax))
    y = alt.Y("Flow:Q", title="Flow Rate (L/min)", scale=alt.Scale(domain=[0, ymax]),
              axis=alt.Axis(**ax))

    line = alt.Chart(plot).mark_line(color="#38bdf8", strokeWidth=2.5).encode(x=x, y=y)
    normal = alt.Chart(plot[plot["Status"] == "Normal"]).mark_point(
        filled=True, size=60, color="#38bdf8", opacity=1).encode(x=x, y=y)
    leaks = alt.Chart(plot[plot["Status"] == "Leak"]).mark_point(
        shape="cross", size=200, color="#ef4444", strokeWidth=3, opacity=1).encode(
        x=x, y=y, tooltip=[alt.Tooltip("Time:T"), alt.Tooltip("Flow:Q", format=".2f")])
    rule = alt.Chart(pd.DataFrame({"limit": [limit]})).mark_rule(
        color="#ef4444", strokeDash=[7, 5], strokeWidth=2).encode(
        y=alt.Y("limit:Q", scale=alt.Scale(domain=[0, ymax])))

    return (line + normal + leaks + rule).properties(height=320).configure(
        background="#1e293b").configure_view(strokeWidth=0).configure_axis(
        gridColor="#334155", domainColor="#475569")


# ============================ HEADER ============================
badge = ('<span class="badge"><span class="dot"></span>LIVE</span>' if running
         else '<span class="badge">⏹ STOPPED</span>')
st.markdown(
    '<div class="hero"><div><h1>💧 Live Water Leak Detection</h1>'
    '<p>Live flow graph &nbsp;•&nbsp; Looping alarm &nbsp;•&nbsp; '
    'Acknowledge button &nbsp;•&nbsp; MQTT hardware data &nbsp;•&nbsp; History database</p></div>'
    f'{badge}</div>', unsafe_allow_html=True)

# ---- STOP / START button (one click) + HISTORY button ----
b1, b2 = st.columns([3, 1])
with b1:
    if running:
        st.button("⏹ STOP ENTIRE SYSTEM", key="stopbtn", on_click=stop_system)
    else:
        st.button("▶ START SYSTEM", key="startbtn", on_click=start_system)
with b2:
    if st.button("📜 HISTORY", key="histbtn"):
        history_dialog()


# ============================ LIVE SECTION ============================
@st.fragment(run_every=refresh_seconds if running else None)
def live_dashboard():
    ss = st.session_state
    sound_slot = st.empty()

    # ---- 0. MQTT connection status ----
    if use_mqtt and running:
        age = None if rx.last_rx is None else time.time() - rx.last_rx
        if not rx.connected:
            st.warning(f"📡 Connecting to broker {mqtt_host}:{mqtt_port} ... {rx.error}")
        elif age is None:
            st.info(f"📡 Connected to broker. Waiting for the first message on topic: {mqtt_topic}")
        elif age > 10:
            st.warning(f"📡 No data from the hardware for {age:.0f} s - "
                       "check the ESP32, its Wi-Fi and that the topic matches.")

    # ---- 1. collect new readings, save them, detect a NEW leak ----
    if running:
        if use_mqtt:
            if ss.pop("flush_queue", False):
                rx.queue.clear()
            new_readings = rx.drain()
        else:
            new_readings = [(pd.Timestamp(datetime.now()), read_flow())]

        alarmed = False
        for ts, flow in new_readings:
            if process_reading(ts, flow, threshold):
                alarmed = True
        ss["data"] = ss["data"][-MAX_KEEP:]
        if alarmed:
            try:
                st.toast("Leak detected!", icon="🚨")
            except Exception:
                pass

    if not ss["data"]:
        if running:
            st.info("No data yet. Waiting for the first reading...")
        else:
            st.markdown('<div class="strip strip-stop">⏹ System stopped - press START SYSTEM</div>',
                        unsafe_allow_html=True)
        return

    df = pd.DataFrame(ss["data"], columns=["Time", "Flow"])
    latest = float(df["Flow"].iloc[-1])
    is_leak = latest > threshold

    # ---- 2. looping buzzer until acknowledged (never plays while stopped) ----
    if running and ss["alarm_latched"] and sound_on:
        try:
            sound_slot.audio(alarm_wav(), format="audio/wav", autoplay=True, loop=True)
        except TypeError:
            sound_slot.audio(alarm_wav(), format="audio/wav", autoplay=True)
    else:
        sound_slot.empty()

    # ---- 3. alert area ----
    if not running:
        st.markdown('<div class="strip strip-stop">⏹ SYSTEM STOPPED - monitoring, saving and '
                    'alarm are switched off. Press START SYSTEM to resume.</div>',
                    unsafe_allow_html=True)
    elif ss["alarm_latched"]:
        when, aflow = ss["alarm_info"]
        st.markdown(
            '<div class="alarm-panel"><div class="alarm-title">🚨 WATER LEAK DETECTED 🚨</div>'
            f'<div class="alarm-sub">Flow {aflow:.2f} L/min &nbsp;|&nbsp; Threshold {threshold:g} L/min '
            f'&nbsp;|&nbsp; Detected at {escape(when)}</div>'
            f'<div class="alarm-sub">{"🔊 Alarm sounds until you acknowledge" if sound_on else "🔇 Alarm sound is switched off in the sidebar"}'
            f' &nbsp;|&nbsp; Current reading: {latest:.2f} L/min</div></div>',
            unsafe_allow_html=True)
        st.button("🔕 ACKNOWLEDGE & SILENCE ALARM", key="ack", on_click=acknowledge)
    elif is_leak:
        st.markdown('<div class="strip strip-warn">⚠️ Leak still in progress - alarm was acknowledged</div>',
                    unsafe_allow_html=True)
    else:
        st.markdown('<div class="strip strip-ok">✅ System normal - no leak detected</div>',
                    unsafe_allow_html=True)

    # ---- 4. cards ----
    n_leak = int((df["Flow"] > threshold).sum())
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.markdown(info_card("Current flow", f"{latest:.2f} L/min",
                          f"Updated {df['Time'].iloc[-1]:%H:%M:%S}", "#38bdf8"), unsafe_allow_html=True)
    c2.markdown(info_card("Status", "POSSIBLE LEAK" if is_leak else "NORMAL",
                          f"Threshold {threshold:g} L/min", "#ef4444" if is_leak else "#22c55e"),
                unsafe_allow_html=True)
    c3.markdown(info_card("Average", f"{df['Flow'].mean():.2f} L/min", "all readings", "#f59e0b"),
                unsafe_allow_html=True)
    c4.markdown(info_card("Peak", f"{df['Flow'].max():.2f} L/min", f"{len(df):,} readings", "#a78bfa"),
                unsafe_allow_html=True)
    c5.markdown(info_card("Leak alerts", f"{n_leak}", f"🚨 {ss['alert_count']} alarm(s) this session",
                          "#ef4444" if n_leak else "#e2e8f0"), unsafe_allow_html=True)

    # ---- 5. live graph + gauge ----
    st.markdown('<div class="sect">📈 Live Water Flow Monitoring</div>', unsafe_allow_html=True)
    left, right = st.columns([2.6, 1])
    with left:
        show_chart(make_chart(df.tail(show_points), threshold))
    with right:
        st.markdown(gauge_html(latest, threshold, float(df["Flow"].max())), unsafe_allow_html=True)

    # ---- 6. tables ----
    t1, t2 = st.columns(2)
    with t1:
        st.markdown('<div class="sect">🖥️ Recent readings</div>', unsafe_allow_html=True)
        rows = [([f"{r.Time:%Y-%m-%d %H:%M:%S}", f"{r.Flow:.2f}",
                  "POSSIBLE LEAK" if r.Flow > threshold else "NORMAL"],
                 "leak" if r.Flow > threshold else "")
                for r in df.tail(8).iloc[::-1].itertuples()]
        st.markdown(html_table(["timestamp", "flow_rate", "status"], rows), unsafe_allow_html=True)
    with t2:
        st.markdown('<div class="sect">🚨 Alert log (leak events)</div>', unsafe_allow_html=True)
        rows = [([f"{r.Time:%Y-%m-%d %H:%M:%S}", f"{r.Flow:.2f}", "🚨 LEAK"], "leak")
                for r in df[df["Flow"] > threshold].tail(8).iloc[::-1].itertuples()]
        st.markdown(html_table(["time", "flow", "alert"], rows), unsafe_allow_html=True)

    st.caption(f"{ss['storage_msg']}  |  Source: {'MQTT ' + mqtt_topic if use_mqtt else 'Simulation'}  |  "
               f"Last update: {time.strftime('%H:%M:%S')}  |  Threshold: {threshold:g} L/min  |  "
               + (f"Refresh: every {refresh_seconds} s" if running else "Refresh: stopped"))


live_dashboard()