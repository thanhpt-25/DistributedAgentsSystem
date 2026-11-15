import json, sys, time, httpx
from concurrent.futures import ThreadPoolExecutor
N = int(sys.argv[2])
base = dict(base_url="http://127.0.0.1:18080", headers={"X-API-Key": "gemma-demo-api-key-0123456789"}, timeout=15)
c = httpx.Client(**base)
for _ in range(60):
    if len([a for a in c.get("/v1/agents").json() if a["status"] == "Available"]) >= 4: break
    time.sleep(2)
PROMPT = open(sys.argv[3]).read()
t0 = time.time()
def submit(i):
    with httpx.Client(**base) as cl:
        s = time.time(); r = cl.post("/v1/requests", json={"prompt": PROMPT}); r.raise_for_status()
        return r.json()["request_id"], round(time.time() - s, 3)
with ThreadPoolExecutor(N) as ex: subs = list(ex.map(submit, range(N)))
rids = [r for r, _ in subs]
print("submitted", N, "requests; submit latencies s:", [d for _, d in subs], flush=True)
done, last, bodies = {}, {}, {}
while len(done) < N and time.time() - t0 < 1500:
    for k, rid in enumerate(rids):
        if rid in done: continue
        b = c.get(f"/v1/requests/{rid}").json(); bodies[rid] = b
        st = (b["status"], tuple((j["job_id"][:12], j["state"]) for j in b.get("jobs", [])))
        if st != last.get(rid): print(f"[{time.time()-t0:6.1f}s] req{k}", st, flush=True); last[rid] = st
        if b["status"] in {"Completed", "Failed", "Cancelled"}: done[rid] = round(time.time() - t0, 1)
    time.sleep(3)
json.dump({"done_s": done, "bodies": bodies}, open(sys.argv[1], "w"), indent=1)
print("done_s", [done.get(r) for r in rids], "total_s", round(time.time() - t0, 1), flush=True)
