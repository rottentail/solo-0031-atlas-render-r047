#!/usr/bin/env python3
"""Atlas Render 有界队列与渲染预算容量验证。

通过真实 HTTP 进程施加可重复负载，验证：
  1. 渲染工作预算（4 千万图层像素）在接受前拦截，拒绝不产生收据/任务；
  2. 队列上限 64 个未结束任务：过载时 429、无收据任务、健康查询不被拖死；
  3. 取消（排队中/运行中）、重试（取消后、队列满、被拒键）语义；
  4. 重启后接受集合（收据）、任务终态与实际 PNG 结果逐一对得上。

仅使用 Python 标准库；服务端为仓库内 Go 服务。结果写入 JSON 报告与
控制台摘要，任一硬性断言失败则以非零码退出。

用法：
  python3 scripts/capacity_validation.py [--workdir DIR] [--report report.json]
"""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]

# ---- 固定负载参数（可重复；数值经 scripts 外的 probe 实测校准）-----------------
QUEUE_LIMIT = 64                 # 规格：最多 64 个未结束任务
BUDGET_LAYER_PIXELS = 40_000_000  # 规格：编译工作量上限
SATURATION_KEYS = 80             # 并发提交键数（>64，制造过载）
PROBE_COUNT = 40                 # 过载期间健康查询次数
HEALTH_DEADLINE_S = 5.0          # 健康查询单次延迟上限（实测空闲 <10ms）
DRAIN_DEADLINE_S = 240.0         # 全部任务排空上限


def rect(layer_id, size, opacity=0.8):
    return {"id": layer_id, "kind": "rectangle", "width": size, "height": size,
            "transform": {"scaleX": 1, "scaleY": 1}, "opacity": opacity,
            "fill": {"kind": "linear", "color": "#145A70", "endColor": "#E87E4F",
                     "from": {"x": 0, "y": 0}, "to": {"x": size, "y": size}}}


def document(width, height, layer_count):
    """全幅覆盖矩形层：每层覆盖整个画布，成本 ≈ layer_count * w * h。"""
    return {"width": width, "height": height, "background": "#20283C",
            "layers": [rect(f"L{i:03d}", max(width, height)) for i in range(layer_count)]}


# 场景组合：小场景、大覆盖图层、高倍输出、预算越界。
# 饱和负载选用 fill8（单任务实测约 0.85s）：突发在百毫秒内到达，远快于单任务渲染，
# 仍可稳定打满 64 槽；big16（实测约 3.2s）保留给取消/崩溃恢复等需要长窗口的实验。
SCENES = {
    # name: (document, 预期 scale1 成本, 预期 scale2 成本)
    "small": (document(320, 240, 2), 2 * 320 * 240, 2 * 640 * 480),
    "fill8": (document(1024, 1024, 8), 8 * 1024 * 1024, 8 * 2048 * 2048),
    "fill12": (document(1024, 1024, 12), 12 * 1024 * 1024, 12 * 2048 * 2048),
    "big16": (document(1500, 1500, 16), 16 * 1500 * 1500, 16 * 3000 * 3000),
    "over18": (document(1500, 1500, 18), 18 * 1500 * 1500, 18 * 3000 * 3000),
}
SATURATION_SCENE = "fill8"       # 队列饱和突发使用的场景
LONG_RUNNING_SCENE = "big16"    # 取消运行中/崩溃恢复使用的长任务场景


class CheckFailed(AssertionError):
    pass


def check(condition, message):
    if not condition:
        raise CheckFailed(message)


class Client:
    """单线程自用的 keep-alive HTTP 客户端。"""

    def __init__(self, port, timeout=30.0):
        self.port = port
        self.timeout = timeout
        self.conn = None

    def _connect(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
        conn.connect()
        return conn

    def call(self, method, path, body=None, headers=None):
        hdr = dict(headers or {})
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            hdr["Content-Type"] = "application/json"
        raw_err = None
        for attempt in range(2):  # 连接复用遇到关闭时重建一次
            if self.conn is None:
                self.conn = self._connect()
            try:
                self.conn.request(method, path, data, hdr)
                resp = self.conn.getresponse()
                payload = resp.read()
                ctype = resp.headers.get("Content-Type", "")
                parsed = json.loads(payload) if ctype.startswith("application/json") and payload else payload
                return resp.status, parsed
            except (http.client.RemoteDisconnected, ConnectionError, OSError) as e:
                raw_err = e
                try:
                    self.conn.close()
                finally:
                    self.conn = None
        raise RuntimeError(f"{method} {path} failed: {raw_err}")

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class Server:
    def __init__(self, binary, directory):
        self.binary = binary
        self.directory = Path(directory)
        self.log_path = self.directory / "server.log"
        self.state_path = self.directory / "state.json"
        self.proc = None
        self.port = None
        self.log = None

    def start(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.log = open(self.log_path, "ab")
        env = dict(os.environ, ATLAS_ADDR=f"127.0.0.1:{self.port}",
                   ATLAS_STATE=str(self.state_path))
        self.proc = subprocess.Popen([str(self.binary)], env=env,
                                     stdout=self.log, stderr=self.log)
        client = Client(self.port)
        try:
            for _ in range(150):
                try:
                    status, _ = client.call("GET", "/healthz")
                    if status == 200:
                        return
                except OSError:
                    if self.proc.poll() is not None:
                        raise RuntimeError(self.log_path.read_text())
                time.sleep(0.03)
        finally:
            client.close()
        raise RuntimeError("server startup deadline exceeded")

    def stop(self, abrupt=False):
        if self.proc and self.proc.poll() is None:
            (self.proc.kill if abrupt else self.proc.terminate)()
            self.proc.wait(timeout=15)
        if self.log:
            self.log.close()
            self.log = None

    def rss_mib(self):
        if not self.proc or self.proc.poll() is not None:
            return None
        try:
            for line in Path(f"/proc/{self.proc.pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
        except OSError:
            return None
        return None

    def cpu_seconds(self):
        if not self.proc or self.proc.poll() is not None:
            return None
        try:
            fields = Path(f"/proc/{self.proc.pid}/stat").read_text().split()
            # utime(14), stime(15)，clock ticks
            ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
            return (int(fields[13]) + int(fields[14])) / ticks
        except (OSError, IndexError, ValueError):
            return None

    def read_state(self):
        """状态文件可能正处于写入窗口，容忍瞬时读取失败。"""
        deadline = time.monotonic() + 5
        last = None
        while time.monotonic() < deadline:
            try:
                return json.loads(self.state_path.read_text())
            except (FileNotFoundError, json.JSONDecodeError) as e:
                last = e
                time.sleep(0.02)
        raise RuntimeError(f"cannot read persisted state: {last}")


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.samples_rss = []
        self.samples_state_bytes = []
        self.samples_cpu = []
        self.latency = {}
        self.stop_sampling = threading.Event()

    def record_latency(self, label, seconds):
        with self.lock:
            self.latency.setdefault(label, []).append(seconds)

    def sampler(self, server):
        start_cpu = server.cpu_seconds()
        wall0 = time.monotonic()
        while not self.stop_sampling.is_set():
            rss = server.rss_mib()
            cpu = server.cpu_seconds()
            try:
                size = server.state_path.stat().st_size
            except OSError:
                size = 0
            with self.lock:
                if rss is not None:
                    self.samples_rss.append(rss)
                self.samples_state_bytes.append(size)
                if cpu is not None and start_cpu is not None:
                    self.samples_cpu.append(((time.monotonic() - wall0), max(0.0, cpu - start_cpu)))
            time.sleep(0.05)

    def report(self):
        def pct(values, q):
            if not values:
                return None
            ordered = sorted(values)
            idx = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
            return ordered[idx]

        lat = {}
        for label, values in self.latency.items():
            lat[label] = {
                "count": len(values),
                "minMs": round(min(values) * 1000, 2),
                "avgMs": round(statistics.fmean(values) * 1000, 2),
                "p95Ms": round(pct(values, 0.95) * 1000, 2),
                "maxMs": round(max(values) * 1000, 2),
            }
        cpu_peak = None
        if self.samples_cpu:
            wall, cpu = self.samples_cpu[-1]
            cpu_peak = round(cpu / wall, 3) if wall > 0 else None
        return {
            "latency": lat,
            "peakRssMiB": round(max(self.samples_rss), 2) if self.samples_rss else None,
            "peakStateMiB": round(max(self.samples_state_bytes) / 1024 / 1024, 3)
            if self.samples_state_bytes else None,
            "cpuBusyRatio": cpu_peak,
            "rssSamples": len(self.samples_rss),
        }


def fingerprint(snapshot_id, scale):
    return hashlib.sha256(json.dumps([snapshot_id, scale], separators=(",", ":")).encode()).hexdigest()


def wait_terminal(client, job_id, deadline_s):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        status, job = client.call("GET", f"/v1/jobs/{job_id}")
        check(status == 200, f"GET job {job_id} status={status}")
        if job["state"] in ("succeeded", "failed", "cancelled"):
            return job
        time.sleep(0.05)
    raise RuntimeError(f"job {job_id} did not reach terminal state within {deadline_s}s")


def pending_from_status(client):
    status, summary = client.call("GET", "/v1/status")
    check(status == 200, f"status endpoint returned {status}")
    jobs = summary["jobs"]
    return jobs.get("queued", 0) + jobs.get("running", 0), summary


def assert_state_invariants(state, phase):
    """收据-任务-结果三角一致性：任何阶段都必须成立。"""
    jobs = state["jobs"]
    receipts = state["receipts"]
    artifacts = state["artifacts"]
    # 每张收据指向存在的任务，且指纹与任务语义匹配。
    for key, receipt in receipts.items():
        job = jobs.get(receipt["jobId"])
        check(job is not None, f"[{phase}] receipt {key} points to missing job {receipt['jobId']}")
        expect_fp = fingerprint(job["snapshotId"], job["scale"])
        check(receipt["fingerprint"] == expect_fp,
              f"[{phase}] receipt {key} fingerprint mismatch")
        check(receipt["key"] == key, f"[{phase}] receipt key field mismatch for {key}")
    # 每个任务都能反向找到至少一张收据（无收据任务不存在）。
    jobs_with_receipt = {r["jobId"] for r in receipts.values()}
    for job_id in jobs:
        check(job_id in jobs_with_receipt, f"[{phase}] job {job_id} has no receipt")
    # 成功任务必有 live/purged 结果且引用一致；非成功任务不得持有结果。
    for job_id, job in jobs.items():
        if job["state"] == "succeeded":
            art = artifacts.get(job["artifactId"])
            check(art is not None, f"[{phase}] succeeded job {job_id} missing artifact")
            check(art["jobId"] == job_id, f"[{phase}] artifact back-reference mismatch")
            check(art["state"] in ("live", "purged"), f"[{phase}] bad artifact state")
        else:
            check(not job.get("artifactId"), f"[{phase}] non-succeeded job {job_id} carries artifact")
    pending = sum(1 for j in jobs.values() if j["state"] in ("queued", "running"))
    check(pending <= QUEUE_LIMIT, f"[{phase}] pending {pending} exceeds queue limit {QUEUE_LIMIT}")
    return pending


def fill_to_limit(client, snap_id, key_prefix, rejected_keys, accepted, start_index=0):
    """持续补灌直到一次提交收到 429，以此证明队列此刻确实满载。

    返回 (accepted_count, rejected_key, next_index)。渲染期间槽位会自然释放，
    因此“观察到 64”并不可靠；只有“提交被容量拒绝”才是满载的充分证据。
    """
    i = start_index
    accepted_count = 0
    last_rejected = None
    while True:
        key = f"{key_prefix}-{i:03d}"
        i += 1
        status, body = client.call("POST", "/v1/jobs",
                                   {"snapshotId": snap_id, "scale": 1},
                                   {"Idempotency-Key": key})
        if status == 429:
            check(body["code"] == "capacity", f"{key_prefix} refusal wrong code: {body}")
            rejected_keys.append(key)
            last_rejected = key
            break
        check(status == 202, f"{key_prefix} {key} -> {status}")
        accepted.append((key, body["id"], snap_id, 1))
        accepted_count += 1
        if i > start_index + QUEUE_LIMIT + 10:
            raise CheckFailed(f"{key_prefix} never reached the queue limit")
    return accepted_count, last_rejected, i


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", help="保留工作目录（默认临时目录，结束删除）")
    parser.add_argument("--report", help="JSON 报告输出路径（默认 <workdir>/report.json）")
    args = parser.parse_args()

    workdir = Path(args.workdir) if args.workdir else Path(tempfile.mkdtemp(prefix="atlas-capacity-"))
    workdir.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report) if args.report else workdir / "report.json"
    result = {"phases": [], "measurements": {}, "passCriteria": [], "passed": False}

    def phase_record(name, detail):
        entry = {"phase": name, "detail": detail}
        result["phases"].append(entry)
        print(f"[phase] {name}: {json.dumps(detail, ensure_ascii=False)}")
        return entry

    binary = workdir / "atlas"
    print(f"[build] go build -> {binary}")
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/atlas"], cwd=ROOT,
                   check=True, timeout=120)

    server = Server(binary, workdir)
    metrics = Metrics()
    snapshots = {}
    rejected_budget_keys = []
    rejected_queue_keys = []
    accepted_burst = []      # (key, jobId, snapshotId, scale)
    canceled_running = None
    pre_restart = {}

    try:
        server.start()
        sampler_thread = threading.Thread(target=metrics.sampler, args=(server,), daemon=True)
        sampler_thread.start()
        client = Client(server.port)

        # ---- 阶段 0：建立场景与快照 ------------------------------------------
        revisions = {}
        for name, (doc, _, _) in SCENES.items():
            status, scene = client.call("PUT", f"/v1/scenes/{name}",
                                        {"name": name, "expectedRevision": 0, "document": doc})
            check(status == 200, f"create scene {name} -> {status}")
            revisions[name] = scene["revision"]
            status, snap = client.call("POST", f"/v1/scenes/{name}/snapshots",
                                       {"expectedRevision": revisions[name]})
            check(status == 201, f"freeze {name} -> {status}")
            snapshots[name] = snap
        baseline_pending, _ = pending_from_status(client)
        check(baseline_pending == 0, "fresh server should have zero pending jobs")
        phase_record("setup", {"scenes": list(SCENES), "snapshots":
                               {k: v["id"] for k, v in snapshots.items()}})

        # ---- 阶段 1：渲染预算在接受前拦截，拒绝不产生收据 ----------------------
        budget_detail = {"rejected": []}
        # over18 scale1 与 big16 scale2（高倍）均超 4 千万。
        budget_cases = [
            ("budget-over18-s1", snapshots["over18"]["id"], 1,
             SCENES["over18"][1]),
            ("budget-big16-s2", snapshots["big16"]["id"], 2,
             SCENES["big16"][2]),
            ("budget-fill12-s2", snapshots["fill12"]["id"], 2,
             SCENES["fill12"][2]),
        ]
        for key, snap_id, scale, expected_cost in budget_cases:
            check(expected_cost > BUDGET_LAYER_PIXELS,
                  f"test design error: {key} cost {expected_cost} not over budget")
            status, err = client.call("POST", "/v1/jobs",
                                      {"snapshotId": snap_id, "scale": scale},
                                      {"Idempotency-Key": key})
            check(status == 429, f"{key} expected 429, got {status}: {err}")
            check(err["code"] == "capacity", f"{key} wrong error code {err}")
            rejected_budget_keys.append(key)
            budget_detail["rejected"].append({"key": key, "status": status,
                                              "code": err["code"], "expectedCost": expected_cost})
        state = server.read_state()
        check(len(state["receipts"]) == 0, "budget rejections must not create receipts")
        check(len(state["jobs"]) == 0, "budget rejections must not create jobs")
        # 预算内对照：小场景 scale1/scale2 与 big16 scale1 必须接受。
        control = [("ctrl-small-s1", "small", 1), ("ctrl-small-s2", "small", 2),
                   ("ctrl-big16-s1", "big16", 1)]
        control_jobs = []
        for key, scene_name, scale in control:
            expect_cost = SCENES[scene_name][1] if scale == 1 else SCENES[scene_name][2]
            check(expect_cost <= BUDGET_LAYER_PIXELS, f"control {key} unexpectedly over budget")
            status, job = client.call("POST", "/v1/jobs",
                                      {"snapshotId": snapshots[scene_name]["id"], "scale": scale},
                                      {"Idempotency-Key": key})
            check(status == 202, f"control {key} expected 202, got {status}")
            control_jobs.append((key, job["id"], snapshots[scene_name]["id"], scale))
        # 被预算拒绝的键换成预算内请求（同键不同语义在“从未落收据”时应被接受）。
        replay_key = rejected_budget_keys[0]
        status, replay_job = client.call("POST", "/v1/jobs",
                                         {"snapshotId": snapshots["small"]["id"], "scale": 1},
                                         {"Idempotency-Key": replay_key})
        check(status == 202, f"rejected key reused for valid request -> {status}")
        control_jobs.append((replay_key, replay_job["id"], snapshots["small"]["id"], 1))
        budget_detail["rejectedKeyReplay"] = {"key": replay_key, "jobId": replay_job["id"]}
        # 等对照组排空，恢复零 pending，再进入饱和实验。
        for _, job_id, _, _ in control_jobs:
            terminal = wait_terminal(client, job_id, DRAIN_DEADLINE_S)
            check(terminal["state"] == "succeeded", f"control job {job_id} -> {terminal['state']}")
        pending, _ = pending_from_status(client)
        check(pending == 0, f"queue not drained before saturation: {pending}")
        phase_record("budget-guard", budget_detail)

        # ---- 阶段 2：并发突发 + 主动灌满，验证 64 槽硬边界 ----------------------
        # 单任务渲染期间队列会自然排空，突发接受数依赖时序；硬性依据是任何时刻
        # pending <= 64，且主动灌满后新请求必为 429、被拒请求不落收据。
        barrier = threading.Barrier(SATURATION_KEYS)
        burst_results = []
        burst_lock = threading.Lock()
        max_pending = {"value": 0}
        poll_stop = threading.Event()

        def poller():
            pc = Client(server.port)
            try:
                while not poll_stop.is_set():
                    try:
                        p, _ = pending_from_status(pc)
                        with burst_lock:
                            max_pending["value"] = max(max_pending["value"], p)
                    except RuntimeError:
                        pass
                    time.sleep(0.01)
            finally:
                pc.close()

        poll_thread = threading.Thread(target=poller, daemon=True)
        poll_thread.start()

        def burst_worker(index):
            sat_snap = snapshots[SATURATION_SCENE]["id"]
            key = f"burst-{index:03d}"
            pc = Client(server.port)
            try:
                barrier.wait()
                t0 = time.monotonic()
                status, body = pc.call("POST", "/v1/jobs",
                                       {"snapshotId": sat_snap, "scale": 1},
                                       {"Idempotency-Key": key})
                dt = time.monotonic() - t0
                metrics.record_latency("burstSubmit", dt)
                with burst_lock:
                    burst_results.append({"key": key, "status": status, "body": body})
            finally:
                pc.close()

        threads = [threading.Thread(target=burst_worker, args=(i,)) for i in range(SATURATION_KEYS)]
        t_start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        burst_elapsed = time.monotonic() - t_start
        check(len(burst_results) == SATURATION_KEYS, "not all burst requests returned")
        burst_statuses = {r["status"] for r in burst_results}
        check(burst_statuses <= {202, 429}, f"unexpected statuses in burst: {burst_statuses}")
        burst_accepted = [r for r in burst_results if r["status"] == 202]
        burst_refused = [r for r in burst_results if r["status"] == 429]
        for r in burst_results:
            if r["status"] == 429:
                check(r["body"]["code"] == "capacity", "refusal must carry capacity code")
                rejected_queue_keys.append(r["key"])
            else:
                accepted_burst.append((r["key"], r["body"]["id"],
                                       snapshots[SATURATION_SCENE]["id"], 1))
        # 主动补灌直到重新打满：只有收到 429 才证明此刻队列确实满载。
        refill_accepted, refill_refused_key, _ = fill_to_limit(
            client, snapshots[SATURATION_SCENE]["id"], "topoff",
            rejected_queue_keys, accepted_burst)
        refill_refused = 1
        # 灌满后继续打：全部 429，且不产生新收据。
        state_during = server.read_state()
        receipts_before = set(state_during["receipts"])
        extra = []
        for i in range(8):
            key = f"extra-{i:02d}"
            status, body = client.call("POST", "/v1/jobs",
                                       {"snapshotId": snapshots[SATURATION_SCENE]["id"], "scale": 1},
                                       {"Idempotency-Key": key})
            extra.append(status)
            check(status == 429, f"overload submit {key} got {status}")
            rejected_queue_keys.append(key)
        state_after = server.read_state()
        check(set(state_after["receipts"]) == receipts_before,
              "refused overload requests must not create receipts")
        pending_now, summary = pending_from_status(client)
        check(pending_now == QUEUE_LIMIT, f"pending after fill = {pending_now}")
        poll_stop.set()
        poll_thread.join(timeout=5)
        check(max_pending["value"] <= QUEUE_LIMIT,
              f"observed pending {max_pending['value']} breached queue limit")
        check(max_pending["value"] == QUEUE_LIMIT,
              f"queue never reached its limit; max observed {max_pending['value']}")
        accepted_ids = {item[1] for item in accepted_burst}
        check(len(accepted_ids) == len(accepted_burst), "duplicate job ids among accepts")
        assert_state_invariants(state_after, "saturation")
        phase_record("saturation", {
            "burstSubmitted": SATURATION_KEYS,
            "burstAccepted": len(burst_accepted), "burstRefused": len(burst_refused),
            "topoffAccepted": refill_accepted, "topoffRefused": refill_refused,
            "extraRefusedAtFullQueue": len(extra),
            "burstElapsedMs": round(burst_elapsed * 1000, 1),
            "maxObservedPending": max_pending["value"], "pendingNow": pending_now,
            "totalAcceptedSoFar": len(accepted_burst),
            "totalRejectedKeysSoFar": len(rejected_queue_keys),
            "statusSummary": summary["jobs"],
        })
        # 队列满载时刻已经由 pending_now==64 与追加 429 证明，无需在此保留长任务；
        # 阶段 4 会在排空后用空队列重新构造可控的取消场景。

        # ---- 阶段 3：队列满载时健康查询不被拖死；小场景请求同样被容量拒绝 --------
        health_latencies = []
        health_failures = 0
        # 探测期间持续补压，使队列在整个观测窗口保持接近满载。
        probe_stop = threading.Event()

        def pressure_worker():
            pc = Client(server.port)
            i = 0
            try:
                while not probe_stop.is_set():
                    try:
                        pc.call("POST", "/v1/jobs",
                                {"snapshotId": snapshots[SATURATION_SCENE]["id"], "scale": 1},
                                {"Idempotency-Key": f"pressure-{i:04d}"})
                    except (OSError, RuntimeError):
                        pass
                    i += 1
                    time.sleep(0.02)
            finally:
                pc.close()

        pressure = threading.Thread(target=pressure_worker, daemon=True)
        pressure.start()
        try:
            for i in range(PROBE_COUNT):
                t0 = time.monotonic()
                status, _ = client.call("GET", "/healthz")
                dt = time.monotonic() - t0
                metrics.record_latency("healthUnderLoad", dt)
                health_latencies.append(dt)
                if status != 200 or dt > HEALTH_DEADLINE_S:
                    health_failures += 1
                t0 = time.monotonic()
                status, _ = client.call("GET", "/v1/status")
                metrics.record_latency("statusUnderLoad", time.monotonic() - t0)
                if status != 200:
                    health_failures += 1
        finally:
            probe_stop.set()
            pressure.join(timeout=5)
        check(health_failures == 0, f"{health_failures} health/status failures under load")
        # 队列满载时小场景请求也被 429（队列满与渲染预算无关），键不被消费。
        # 先补灌到 429 证明满载，再用固定键提交小场景请求，消除排空竞态。
        fill_to_limit(client, snapshots[SATURATION_SCENE]["id"], "health-topoff",
                      rejected_queue_keys, accepted_burst)
        small_under_load_key = "small-under-load"
        status, body = client.call("POST", "/v1/jobs",
                                   {"snapshotId": snapshots["small"]["id"], "scale": 1},
                                   {"Idempotency-Key": small_under_load_key})
        check(status == 429 and body["code"] == "capacity",
              f"small job under overload -> {status}")
        rejected_queue_keys.append(small_under_load_key)
        phase_record("health-under-load", {
            "probes": PROBE_COUNT, "failures": health_failures,
            "maxHealthMs": round(max(health_latencies) * 1000, 2),
            "p95HealthMs": round(sorted(health_latencies)[int(0.95 * (len(health_latencies) - 1))] * 1000, 2),
        })

        # ---- 阶段 4：取消排队中与运行中任务，槽位立即归还 -----------------------
        def wait_pending(limit):
            dline = time.monotonic() + DRAIN_DEADLINE_S
            last = -1
            while time.monotonic() < dline:
                p, _ = pending_from_status(client)
                last = p
                if p <= limit:
                    return p
                time.sleep(0.1)
            raise RuntimeError(f"pending did not reach {limit} (last {last})")

        # 先让饱和积压排空，回到空队列，再构造确定性的取消场景。
        wait_pending(0)

        # 4a 排队中取消：提交两个任务，取消排在后面（仍 queued）的那个。
        key_q1, key_q2 = "queued-cancel-a", "queued-cancel-b"
        status, job_q1 = client.call("POST", "/v1/jobs",
                                     {"snapshotId": snapshots[SATURATION_SCENE]["id"], "scale": 1},
                                     {"Idempotency-Key": key_q1})
        check(status == 202, f"{key_q1} submit -> {status}")
        status, job_q2 = client.call("POST", "/v1/jobs",
                                     {"snapshotId": snapshots[SATURATION_SCENE]["id"], "scale": 1},
                                     {"Idempotency-Key": key_q2})
        check(status == 202, f"{key_q2} submit -> {status}")
        accepted_burst.append((key_q1, job_q1["id"], snapshots[SATURATION_SCENE]["id"], 1))
        accepted_burst.append((key_q2, job_q2["id"], snapshots[SATURATION_SCENE]["id"], 1))
        # 等待 q1 被领取（q2 此时必然仍 queued），再取消 q2。
        def wait_state(job_id, states, deadline_s=DRAIN_DEADLINE_S):
            dline = time.monotonic() + deadline_s
            while time.monotonic() < dline:
                state_now = server.read_state()
                got = state_now["jobs"].get(job_id, {}).get("state")
                if got in states:
                    return got
                time.sleep(0.03)
            raise RuntimeError(f"job {job_id} never reached {states}")

        wait_state(job_q1["id"], ("running", "succeeded"))
        state_now = server.read_state()
        check(state_now["jobs"][job_q2["id"]]["state"] == "queued",
              "q2 should still be queued")
        cancel_target_queued = job_q2["id"]
        status, cancelled = client.call("POST", f"/v1/jobs/{cancel_target_queued}/cancel")
        check(status == 200 and cancelled["state"] == "cancelled", "cancel queued failed")
        # 取消幂等：再次取消仍返回 cancelled。
        status, job = client.call("POST", f"/v1/jobs/{cancel_target_queued}/cancel")
        check(status == 200 and job["state"] == "cancelled", "cancel must be idempotent")
        # q1 正常成功；q2 永远不会被领取执行。
        terminal = wait_terminal(client, job_q1["id"], DRAIN_DEADLINE_S)
        check(terminal["state"] == "succeeded", "q1 should succeed after q2 cancel")
        status, job = client.call("GET", f"/v1/jobs/{cancel_target_queued}")
        check(job["state"] == "cancelled" and not job.get("artifactId"),
              "queued-cancelled job must stay cancelled without artifact")
        # 槽位释放：阶段 2 被拒的键立刻可接受（同键重试得到合理结果——新任务）。
        freed_key = rejected_queue_keys[0]
        status, freed_job = client.call("POST", "/v1/jobs",
                                        {"snapshotId": snapshots["small"]["id"], "scale": 1},
                                        {"Idempotency-Key": freed_key})
        check(status == 202, f"rejected key retry after slot freed -> {status}")
        accepted_burst.append((freed_key, freed_job["id"], snapshots["small"]["id"], 1))
        terminal = wait_terminal(client, freed_job["id"], DRAIN_DEADLINE_S)
        check(terminal["state"] == "succeeded", "freed-slot small job should succeed")

        # 4b 运行中取消：空队列提交 big16 长任务，它立即成为最老任务并被领取。
        def wait_running(ids, deadline_s):
            dline = time.monotonic() + deadline_s
            while time.monotonic() < dline:
                state_now = server.read_state()
                running = [jid for jid in ids
                           if state_now["jobs"].get(jid, {}).get("state") == "running"]
                if running:
                    return running[0]
                time.sleep(0.03)
            return None

        wait_pending(0)
        long_key = "long-running-cancel"
        status, long_job = client.call(
            "POST", "/v1/jobs",
            {"snapshotId": snapshots[LONG_RUNNING_SCENE]["id"], "scale": 1},
            {"Idempotency-Key": long_key})
        check(status == 202, f"long cancel probe submit -> {status}")
        accepted_burst.append((long_key, long_job["id"], snapshots[LONG_RUNNING_SCENE]["id"], 1))
        running_target = wait_running([long_job["id"]], DRAIN_DEADLINE_S)
        check(running_target is not None, "long cancel probe never reached running")
        status, cancelled = client.call("POST", f"/v1/jobs/{running_target}/cancel")
        check(status == 200, f"cancel running -> {status}")
        terminal = wait_terminal(client, running_target, DRAIN_DEADLINE_S)
        check(terminal["state"] == "cancelled",
              f"running cancel ended as {terminal['state']} (stale result overwrote?)")
        check(not terminal.get("artifactId"), "cancelled job must not carry artifact")
        canceled_running = running_target
        # 给执行器足够时间后复查，确保没有迟到成功覆盖。
        time.sleep(0.5)
        status, job = client.call("GET", f"/v1/jobs/{running_target}")
        check(job["state"] == "cancelled", "cancelled state regressed")
        phase_record("cancel", {"cancelledQueued": cancel_target_queued,
                                "cancelledRunning": canceled_running})

        # ---- 阶段 5：重试语义 ------------------------------------------------
        # 5a 重试被取消的 running 任务：attempt+1，沿用固定输入，最终成功。
        status, retried = client.call("POST", f"/v1/jobs/{canceled_running}/retry")
        check(status == 202 and retried["attempt"] == 2 and retried["state"] == "queued",
              f"retry semantics wrong: {retried}")
        # 成功任务不能重试。
        status, _ = client.call("POST", f"/v1/jobs/{freed_job['id']}/retry")
        check(status == 409, "retry of succeeded job must be 409")
        # 5b 取消 queued 的任务也可重试。
        status, retried_q = client.call("POST", f"/v1/jobs/{cancel_target_queued}/retry")
        check(status == 202 and retried_q["attempt"] == 2, "retry cancelled-queued failed")

        # 5c 队列再满时，重试同样收到 429 而不是破坏状态。
        # 先排空到剩余 pending 较少，再灌满，验证 Retry 路径的容量检查。
        detail_5c = {}
        wait_pending(QUEUE_LIMIT - 4)
        # 补灌到 429 为止，拿到一个满载队列。
        fill_to_limit(client, snapshots[SATURATION_SCENE]["id"], "refill",
                      rejected_queue_keys, accepted_burst)
        # 取一个已接受的 queued 任务作为 victim，取消后验证“队列满时重试得 429”。
        victim = accepted_burst[-1][1]
        status, _ = client.call("POST", f"/v1/jobs/{victim}/cancel")
        check(status == 200, "victim cancel failed")
        # victim 可能仍在排队；取消是持久提交，等待状态落定即可，不依赖 worker 时机。
        dline = time.monotonic() + DRAIN_DEADLINE_S
        while time.monotonic() < dline:
            status, terminal = client.call("GET", f"/v1/jobs/{victim}")
            if terminal["state"] == "cancelled":
                break
            check(terminal["state"] in ("queued", "running"),
                  f"victim unexpected state {terminal['state']}")
            time.sleep(0.1)
        check(terminal["state"] == "cancelled", "victim should be cancelled")
        # victim 释放一个槽位；补灌到再次 429，确保重试 victim 时队列确定满载。
        fill_to_limit(client, snapshots[SATURATION_SCENE]["id"], "filler",
                      rejected_queue_keys, accepted_burst)
        pending_full, _ = pending_from_status(client)
        check(pending_full == QUEUE_LIMIT, f"pending before retry-429 = {pending_full}")
        status, body = client.call("POST", f"/v1/jobs/{victim}/retry")
        check(status == 429 and body["code"] == "capacity",
              f"retry with full queue -> {status}")
        detail_5c = {"victim": victim, "retryStatus": status}
        # 槽位空出后重试 victim：应成功且 attempt 递增。
        wait_pending(QUEUE_LIMIT - 1)
        status, retried_victim = client.call("POST", f"/v1/jobs/{victim}/retry")
        check(status == 202, f"victim retry after drain -> {status}")
        phase_record("retry", {"runningRetryAttempt": retried["attempt"],
                               "queuedRetryAttempt": retried_q["attempt"],
                               "retryWhenFull": detail_5c})

        # ---- 阶段 6：全部排空，再重放被拒键，记录重启前事实 ---------------------
        # 先让既有积压完全排空，确保后续重试是在有容量的条件下进行。
        wait_pending(0)
        # 被预算拒绝的键从未落收据：用预算内的小场景请求重放，必须被接受。
        state_now = server.read_state()
        for key in rejected_budget_keys[1:]:
            check(key not in state_now["receipts"], f"budget key {key} unexpectedly consumed")
        # 队列满拒绝的键：仍未被消费的在容量恢复后同键重试，均应接受；
        # 已在前面阶段成功重试的键跳过（它们已持有收据，重放只会返回同一任务）。
        replayed = 0
        for key in rejected_budget_keys[1:] + rejected_queue_keys:
            state_now = server.read_state()
            if key in state_now["receipts"]:
                continue
            status, job = client.call("POST", "/v1/jobs",
                                      {"snapshotId": snapshots["small"]["id"], "scale": 1},
                                      {"Idempotency-Key": key})
            check(status == 202, f"rejected key {key} retry -> {status}")
            accepted_burst.append((key, job["id"], snapshots["small"]["id"], 1))
            replayed += 1
        # 等待重放任务也全部排空。
        wait_pending(0)
        pending, last_summary = pending_from_status(client)
        check(pending == 0, f"queue did not drain: {last_summary}")
        phase_replay = replayed
        state = server.read_state()
        assert_state_invariants(state, "pre-restart")
        # 记录每个任务的终态、attempt、结果摘要与 PNG 摘要。
        for job_id, job in state["jobs"].items():
            entry = {"state": job["state"], "attempt": job["attempt"],
                     "snapshotId": job["snapshotId"], "scale": job["scale"],
                     "artifactId": job.get("artifactId", "")}
            if job["state"] == "succeeded":
                art = state["artifacts"][job["artifactId"]]
                status, png = client.call("GET", f"/v1/artifacts/{job['artifactId']}")
                check(status == 200 and png[:8] == b"\x89PNG\r\n\x1a\n",
                      f"artifact {job['artifactId']} download failed")
                entry["pngDigest"] = hashlib.sha256(png).hexdigest()
                entry["pngBytes"] = len(png)
                entry["storedDigest"] = art["digest"]
                entry["dims"] = [art["width"], art["height"]]
                check(entry["pngDigest"] == art["digest"], "stored digest mismatch")
            pre_restart[job_id] = entry
        phase_record("drain", {"jobs": len(state["jobs"]),
                               "rejectedKeysReplayedAfterRecovery": phase_replay,
                               "terminalBreakdown": {s: sum(1 for j in state["jobs"].values()
                                                             if j["state"] == s)
                                                     for s in ("queued", "running", "succeeded",
                                                               "failed", "cancelled")},
                               "receipts": len(state["receipts"])})

        # ---- 阶段 7：强杀重启，恢复 running->queued，集合与结果对账 -------------
        client.close()
        server.stop(abrupt=True)
        server.start()
        client = Client(server.port)
        status, _ = client.call("GET", "/healthz")
        check(status == 200, "server did not return healthy after restart")
        state2 = server.read_state()
        assert_state_invariants(state2, "post-restart")
        # 收据集合完全一致。
        check(set(state2["receipts"]) == set(state["receipts"]),
              "receipt set changed across restart")
        for key, receipt in state["receipts"].items():
            check(state2["receipts"][key] == receipt, f"receipt {key} changed across restart")
        # 任务集合、终态、attempt 一致（重启时无 running，但恢复路径由下面的二次重启覆盖）。
        check(set(state2["jobs"]) == set(state["jobs"]), "job set changed across restart")
        for job_id, before in pre_restart.items():
            after = state2["jobs"][job_id]
            check(after["state"] == before["state"],
                  f"job {job_id} state {before['state']} -> {after['state']} across restart")
            check(after["attempt"] == before["attempt"], f"job {job_id} attempt changed")
            if before["state"] == "succeeded":
                check(after["artifactId"] == before["artifactId"], "artifact link changed")
                art = state2["artifacts"][before["artifactId"]]
                status, png = client.call("GET", f"/v1/artifacts/{before['artifactId']}")
                check(status == 200, "artifact missing after restart")
                digest = hashlib.sha256(png).hexdigest()
                check(digest == before["pngDigest"],
                      f"artifact {before['artifactId']} bytes changed across restart")
                check(art["digest"] == before["storedDigest"], "stored digest changed")
                check([art["width"], art["height"]] == before["dims"], "dims changed")
        # 重启后同键重放：返回原任务（202 + 同一 jobId），语义不变。
        replay_key_name = accepted_burst[0][0]
        replay_snap = accepted_burst[0][2]
        replay_scale = accepted_burst[0][3]
        status, replay = client.call("POST", "/v1/jobs",
                                     {"snapshotId": replay_snap, "scale": replay_scale},
                                     {"Idempotency-Key": replay_key_name})
        check(status == 202, "idempotent replay after restart failed")
        check(replay["id"] == accepted_burst[0][1], "replay returned different job id")
        # 同键不同语义仍 409。
        status, body = client.call("POST", "/v1/jobs",
                                   {"snapshotId": replay_snap, "scale": 3 - replay_scale},
                                   {"Idempotency-Key": replay_key_name})
        check(status == 409 and body["code"] == "conflict",
              f"same key different semantics -> {status}")

        # 7b 二次验证恢复路径：提交一个 big16 任务，running 时强杀，重启后必须回到 queued
        # 且 attempt 不增加，随后正常成功。
        status, recovery_job = client.call("POST", "/v1/jobs",
                                           {"snapshotId": snapshots[LONG_RUNNING_SCENE]["id"], "scale": 1},
                                           {"Idempotency-Key": "recovery-probe"})
        check(status == 202, "recovery probe submit failed")
        rid = recovery_job["id"]
        running_seen = False
        dline = time.monotonic() + 20
        while time.monotonic() < dline:
            status, job = client.call("GET", f"/v1/jobs/{rid}")
            if job["state"] == "running":
                running_seen = True
                break
            if job["state"] != "queued":
                break
            time.sleep(0.03)
        check(running_seen, "recovery probe never reached running")
        client.close()
        server.stop(abrupt=True)
        server.start()
        client = Client(server.port)
        status, job = client.call("GET", f"/v1/jobs/{rid}")
        check(status == 200, "job missing after crash restart")
        check(job["state"] == "queued", f"running job not reset to queued: {job['state']}")
        check(job["attempt"] == 1, f"recovery must not increment attempt: {job['attempt']}")
        recovered_events = [e["kind"] for e in server.read_state()["events"]]
        check("job.recovered" in recovered_events, "job.recovered event missing")
        terminal = wait_terminal(client, rid, DRAIN_DEADLINE_S)
        check(terminal["state"] == "succeeded", f"recovered job ended {terminal['state']}")
        final_state = server.read_state()
        assert_state_invariants(final_state, "final")
        phase_record("restart-reconcile", {
            "receiptsPreserved": len(final_state["receipts"]),
            "jobsPreserved": len(final_state["jobs"]),
            "recoveredRunningJob": rid,
            "recoveredAttempt": job["attempt"],
        })

        metrics.stop_sampling.set()
        sampler_thread.join(timeout=5)
        result["measurements"] = metrics.report()

        # ---- 通过依据（硬性不变量 + 资源门槛）-------------------------------
        m = result["measurements"]
        result["passCriteria"] = [
            {"name": "预算拒绝为 429/capacity 且零收据零任务", "passed": True},
            {"name": f"队列严格不超过 {QUEUE_LIMIT}（峰值观测 {max_pending['value']}，灌满后稳定 {pending_now}）",
             "passed": max_pending["value"] == QUEUE_LIMIT},
            {"name": "突发与灌满期间仅出现 202/429，满载追加请求全部拒绝且零新收据", "passed": True},
            {"name": "过载下健康/状态查询零失败，p95 见测量", "passed": True},
            {"name": "取消排队/运行任务均落 cancelled，无迟到结果覆盖", "passed": True},
            {"name": "重试 attempt 递增；队列满重试 429；成功任务重试 409", "passed": True},
            {"name": "被拒键在容量恢复后同键重试成功，收据唯一", "passed": True},
            {"name": "重启前后收据集合、任务终态、PNG 摘要完全一致", "passed": True},
            {"name": "running 任务崩溃重启后回 queued，attempt 不增并最终成功", "passed": True},
            {"name": f"峰值 RSS {m['peakRssMiB']} MiB ≤ 256 MiB",
             "passed": m["peakRssMiB"] is not None and m["peakRssMiB"] <= 256},
            {"name": f"峰值状态文件 {m['peakStateMiB']} MiB ≤ 16 MiB",
             "passed": m["peakStateMiB"] is not None and m["peakStateMiB"] <= 16},
        ]
        result["passed"] = all(c["passed"] for c in result["passCriteria"])
    except CheckFailed as e:
        result["error"] = f"assertion: {e}"
    except Exception as e:  # noqa: BLE001 - 顶层兜底，报告里保留失败原因
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        try:
            metrics.stop_sampling.set()
        except Exception:
            pass
        report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"[report] {report_path}")
        print(json.dumps(result["measurements"], ensure_ascii=False, indent=2))
        print("[criteria]")
        for c in result.get("passCriteria", []):
            print(f"  {'PASS' if c['passed'] else 'FAIL'}  {c['name']}")
        if result.get("error"):
            print(f"[error] {result['error']}", file=sys.stderr)
        server.stop()
        if not args.workdir and not result.get("passed"):
            print(f"[note] retained workdir for inspection: {workdir}")
        sys.exit(0 if result.get("passed") else 1)


if __name__ == "__main__":
    main()
