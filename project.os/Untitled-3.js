/* Multithreaded file downloader (browser only).
   Each "thread" is a Web Worker that downloads one byte range with fetch(). */

const DEMO_SIZE = 8 * 1024 * 1024;   // 8 MB simulated file
const DEMO_SPEED = 1024 * 1024;      // simulated 1 MB/s per connection

/* ---------- Worker code (runs in its own thread) ---------- */
function workerMain() {
  self.onmessage = async (e) => {
    const { id, url, start, end, useRange, demo, demoSpeed } = e.data;
    try {
      // Demo mode: simulate a server that limits speed per connection
      if (demo) {
        const total = end - start + 1;
        const buf = new Uint8Array(total);
        const step = 64 * 1024;
        const delay = (step / demoSpeed) * 1000;
        for (let off = 0; off < total; off += step) {
          const n = Math.min(step, total - off);
          for (let i = 0; i < n; i++) buf[off + i] = ((start + off + i) * 31) & 255;
          await new Promise((r) => setTimeout(r, delay));
          self.postMessage({ type: "progress", id, received: off + n });
        }
        self.postMessage({ type: "done", id, buffer: buf.buffer }, [buf.buffer]);
        return;
      }

      // Real download
      const headers = useRange ? { Range: `bytes=${start}-${end}` } : {};
      const res = await fetch(url, { headers });
      if (useRange && res.status !== 206) {
        throw new Error("Server ignored the Range request (status " + res.status + ").");
      }
      if (!res.ok) throw new Error("HTTP " + res.status);

      const reader = res.body.getReader();
      const chunks = [];
      let received = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        received += value.length;
        self.postMessage({ type: "progress", id, received });
      }
      const out = new Uint8Array(received);
      let pos = 0;
      for (const c of chunks) { out.set(c, pos); pos += c.length; }
      if (useRange && received !== end - start + 1) throw new Error("Part size mismatch.");
      self.postMessage({ type: "done", id, buffer: out.buffer }, [out.buffer]);
    } catch (err) {
      self.postMessage({ type: "error", id, message: err.message });
    }
  };
}
const workerURL = URL.createObjectURL(
  new Blob([`(${workerMain.toString()})()`], { type: "text/javascript" })
);

/* ---------- Helpers ---------- */
const $ = (id) => document.getElementById(id);
const pct = (done, total) => (total ? Math.min(100, (done / total) * 100) : 0);
const fmt = (s) => s.toFixed(2) + " s";
let finalBlob = null, finalName = "download.bin";

function showError(msg) {
  $("error").textContent = msg || "";
  $("error").classList.toggle("hidden", !msg);
}

/* Find file size and check that the server supports range requests */
async function probe(url) {
  let res;
  try {
    res = await fetch(url, { headers: { Range: "bytes=0-0" } });
  } catch {
    throw new Error("Could not reach the file. The server must allow CORS (Access-Control-Allow-Origin).");
  }
  if (res.body) res.body.cancel();
  if (res.status !== 206) throw new Error("This server does not support range requests (needed for multithreading).");
  let size = 0;
  const cr = res.headers.get("Content-Range");           // "bytes 0-0/12345"
  if (cr && cr.includes("/")) size = parseInt(cr.split("/")[1], 10);
  if (!size) {
    const head = await fetch(url, { method: "HEAD" });
    size = parseInt(head.headers.get("Content-Length") || "0", 10);
  }
  if (!size) throw new Error("Could not read the file size (server must expose Content-Range).");
  return size;
}

/* Run one download phase with n workers. Returns {time, buffers} */
function runPhase({ url, size, n, useRange, demo, onProgress }) {
  return new Promise((resolve, reject) => {
    const part = Math.floor(size / n);
    const received = new Array(n).fill(0);
    const buffers = new Array(n);
    const workers = [];
    let finished = 0;
    const t0 = performance.now();

    for (let i = 0; i < n; i++) {
      const start = i * part;
      const end = i === n - 1 ? size - 1 : start + part - 1;
      const w = new Worker(workerURL);
      workers.push(w);
      w.onmessage = (e) => {
        const m = e.data;
        if (m.type === "progress") {
          received[i] = m.received;
          onProgress(received.slice());
        } else if (m.type === "done") {
          buffers[i] = m.buffer;
          received[i] = end - start + 1;
          onProgress(received.slice());
          w.terminate();
          if (++finished === n) {
            // merge the parts, in order, into one Blob
            const blob = new Blob(buffers);
            resolve({ time: (performance.now() - t0) / 1000, blob });
          }
        } else if (m.type === "error") {
          workers.forEach((x) => x.terminate());
          reject(new Error(m.message));
        }
      };
      w.postMessage({ id: i, url, start, end, useRange, demo, demoSpeed: DEMO_SPEED });
    }
  });
}

async function blobsEqual(a, b) {
  if (a.size !== b.size) return false;
  const x = new Uint8Array(await a.arrayBuffer());
  const y = new Uint8Array(await b.arrayBuffer());
  for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return false;
  return true;
}

/* ---------- UI ---------- */
$("demo").onchange = (e) => { $("url").disabled = e.target.checked; };

$("start").onclick = async () => {
  showError("");
  const demo = $("demo").checked;
  const url = $("url").value.trim();
  const n = Math.max(1, Math.min(16, parseInt($("threads").value, 10) || 4));
  if (!demo && !/^https?:\/\//i.test(url)) return showError("Enter a valid http(s) URL.");

  $("start").disabled = true;
  $("results").classList.add("hidden");
  $("progress").classList.remove("hidden");
  $("s-fill").style.width = "0%"; $("m-fill").style.width = "0%";
  $("s-text").textContent = "waiting"; $("m-text").textContent = "waiting";
  $("m-title").textContent = `Multithreaded (${n} threads)`;
  $("parts").innerHTML = Array.from({ length: n }, (_, i) =>
    `<div class="part"><small>Part ${i + 1}</small><div class="bar"><div class="fill green" id="p${i}"></div></div></div>`
  ).join("");

  try {
    const size = demo ? DEMO_SIZE : await probe(url);

    // 1) single thread (plain GET, one worker)
    $("s-text").textContent = "downloading...";
    const single = await runPhase({
      url, size, n: 1, useRange: false, demo,
      onProgress: (r) => {
        const p = pct(r[0], size);
        $("s-fill").style.width = p + "%";
        $("s-text").textContent = p.toFixed(0) + "%";
      },
    });
    $("s-text").textContent = "100% · " + fmt(single.time);

    // 2) multithreaded
    $("m-text").textContent = "downloading...";
    const partSize = size / n;
    const multi = await runPhase({
      url, size, n, useRange: true, demo,
      onProgress: (r) => {
        const total = r.reduce((a, b) => a + b, 0);
        const p = pct(total, size);
        $("m-fill").style.width = p + "%";
        $("m-text").textContent = p.toFixed(0) + "%";
        r.forEach((d, i) => { $("p" + i).style.width = pct(d, partSize) + "%"; });
      },
    });
    $("m-text").textContent = "100% · " + fmt(multi.time);

    // 3) verify and show results
    const same = await blobsEqual(single.blob, multi.blob);
    finalBlob = multi.blob;
    finalName = demo ? "demo_file.bin" : (decodeURIComponent(new URL(url).pathname.split("/").pop()) || "download.bin");

    $("r-single").textContent = fmt(single.time);
    $("r-multi").textContent = fmt(multi.time);
    $("r-multi-l").textContent = `${n} threads`;
    $("r-speed").textContent = (single.time / multi.time).toFixed(2) + "x";
    $("r-msg").textContent = same ? "Both downloads are identical." : "Warning: the two files differ!";
    $("results").classList.remove("hidden");
  } catch (err) {
    showError(err.message);
  } finally {
    $("start").disabled = false;
  }
};

$("save").onclick = () => {
  if (!finalBlob) return;
  const a = document.createElement("a");
  a.href = URL.createObjectURL(finalBlob);
  a.download = finalName;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 10000);
};
