const PART_BYTES = 8 * 1024 * 1024;
const MAX_BYTES = 12_000_000_000;
const UPLOADS = "/v1/buckets/pilot/multipart-uploads";
const HEX_DIGEST = /^[0-9a-f]{64}$/i;

const form = document.querySelector("#upload-form");
const codeInput = document.querySelector("#upload-code");
const fileInput = document.querySelector("#file");
const uploadButton = document.querySelector("#upload");
const progress = document.querySelector("#progress");
const status = document.querySelector("#status");
const error = document.querySelector("#error");
const share = document.querySelector("#share");
const createLinkButton = document.querySelector("#create-link");
const copyLinkButton = document.querySelector("#copy-link");
const linkResult = document.querySelector("#link-result");
const linkInput = document.querySelector("#download-link");
const linkExpiry = document.querySelector("#link-expiry");

const savedSelect = document.querySelector("#saved-upload");
const recovery = document.querySelector("#recovery");
const hint = document.querySelector("#recovery-hint");
const metrics = document.querySelector("#metrics");
const receipt = document.querySelector("#receipt");
const STORAGE_PREFIX = "nebulafs.pilot.upload.v1.";
let completedUpload;

function persist(upload) {
  try {
    localStorage.setItem(STORAGE_PREFIX + upload.id, JSON.stringify(upload));
  } catch {
    throw new Error("Could not save recovery information. Check browser storage before continuing.");
  }
}

function loadSaved(id) {
  return JSON.parse(localStorage.getItem(STORAGE_PREFIX + id));
}

function showMetrics(upload) {
  metrics.hidden = false;
  metrics.textContent = `Payload bytes resent (browser-reported): ${upload.resent.toLocaleString()}.`;
}

function refreshSelection() {
  const upload = savedSelect.value && loadSaved(savedSelect.value);
  fileInput.required = !upload;
  uploadButton.textContent = upload ? "Check saved upload" : "Upload file";
  hint.textContent = upload?.completed ? "Enter your upload code to retrieve the verified receipt." :
    upload ? "Enter your upload code. Reselect the original file to send any missing parts." : "";
  if (upload) showMetrics(upload);
}

function addOption(upload) {
  let option = Array.from(savedSelect.options).find(item => item.value === upload.id);
  if (!option) {
    option = new Option("", upload.id);
    savedSelect.add(option);
  }
  option.textContent = `${upload.name} · ${upload.completed ? "Verified" : "In progress"} · ${new Date(upload.created).toLocaleString()}`;
  recovery.hidden = false;
}

try {
  const saved = Object.keys(localStorage).filter(key => key.startsWith(STORAGE_PREFIX))
    .map(key => loadSaved(key.slice(STORAGE_PREFIX.length))).filter(upload => upload?.id)
    .sort((a, b) => a.created - b.created);
  for (const upload of saved) {
    addOption(upload);
    savedSelect.value = upload.id;
  }
  refreshSelection();
} catch {
  showError("Browser storage is unavailable. Enable it to recover uploads in this browser.");
}

function clearResult() {
  error.hidden = true;
  status.hidden = true;
  progress.hidden = true;
  metrics.hidden = true;
  share.hidden = true;
  linkResult.hidden = true;
  linkInput.value = "";
  completedUpload = undefined;
}

savedSelect.addEventListener("change", () => {
  clearResult();
  fileInput.value = "";
  refreshSelection();
});

class ConnectionError extends Error {}

class RequestError extends Error {
  constructor(httpStatus, response) {
    super(response?.error?.message ?? `Request failed (HTTP ${httpStatus}).`);
    this.status = httpStatus;
  }
}

function setStatus(message, fraction) {
  status.textContent = message;
  status.hidden = false;
  progress.hidden = fraction === undefined;
  if (fraction !== undefined) {
    progress.value = fraction;
  }
}

function showError(message) {
  error.textContent = message;
  error.hidden = false;
}

function hashFile(file, onProgress) {
  return new Promise((resolve, reject) => {
    const worker = new Worker("/pilot/sha256-worker.js", { type: "module" });
    worker.onmessage = ({ data }) => {
      if (data.processed !== undefined) {
        onProgress(data.processed);
        return;
      }
      worker.terminate();
      if (data.error) {
        reject(new Error(data.error));
      } else {
        resolve(data.digest);
      }
    };
    worker.onerror = () => {
      worker.terminate();
      reject(new Error("Could not start file verification."));
    };
    worker.postMessage(file);
  });
}

async function request(path, code, method = "GET", body) {
  const headers = { "Authorization": `Bearer ${code}`, "Accept": "application/json" };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 60_000);
  try {
    const response = await fetch(path, {
      method, headers, signal: controller.signal,
      body: body === undefined ? undefined : JSON.stringify(body),
      credentials: "omit", cache: "no-store", redirect: "error",
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) throw new RequestError(response.status, payload);
    if (!payload) throw new ConnectionError("The server response could not be read.");
    return payload;
  } catch (failure) {
    if (failure instanceof RequestError) throw failure;
    throw new ConnectionError("The connection to the host was interrupted.");
  } finally {
    clearTimeout(timeout);
  }
}

function sendPart(path, code, chunk, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", path);
    xhr.responseType = "json";
    xhr.setRequestHeader("Authorization", `Bearer ${code}`);
    xhr.setRequestHeader("Content-Type", "application/octet-stream");
    let timer;
    function track(loaded) {
      clearTimeout(timer);
      timer = setTimeout(() => xhr.abort(), 120_000);
      try {
        onProgress(loaded);
      } catch (failure) {
        reject(failure);
        xhr.abort();
      }
    }
    xhr.upload.onprogress = event => track(event.loaded);
    xhr.upload.onload = event => track(event.loaded);
    xhr.onloadend = () => clearTimeout(timer);
    xhr.onload = () => {
      if (xhr.status < 200 || xhr.status >= 300) {
        reject(new RequestError(xhr.status, xhr.response));
      } else if (!HEX_DIGEST.test(xhr.response?.etag ?? "")) {
        reject(new ConnectionError("The server did not confirm the uploaded part."));
      } else {
        resolve(xhr.response.etag);
      }
    };
    xhr.onabort = xhr.onerror = () => reject(new ConnectionError("Connection lost during upload."));
    timer = setTimeout(() => xhr.abort(), 120_000);
    xhr.send(chunk);
  });
}

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

function partSize(upload, number) {
  return Math.min(upload.partBytes, upload.size - (number - 1) * upload.partBytes);
}

function confirmedParts(upload, listing) {
  const count = Math.ceil(upload.size / upload.partBytes);
  const confirmed = new Map();
  if (!Array.isArray(listing.parts)) throw new Error("The server did not list the saved parts.");
  for (const { part_number: n, size, etag } of listing.parts) {
    if (!Number.isSafeInteger(n) || n < 1 || n > count || confirmed.has(n) ||
        size !== partSize(upload, n) || !HEX_DIGEST.test(etag)) {
      throw new Error("The saved parts do not match this upload.");
    }
    confirmed.set(n, etag);
  }
  return confirmed;
}

async function sendMissing(path, code, upload, file, confirmed, onConfirmed) {
  let bytes = Array.from(confirmed.keys()).reduce((sum, n) => sum + partSize(upload, n), 0);
  const count = Math.ceil(upload.size / upload.partBytes);
  if (confirmed.size < count && !file) {
    throw new Error(`Reselect ${upload.name} to resume. ${confirmed.size} of ${count} parts are already on the host.`);
  }
  for (let n = 1; n <= count; n++) {
    if (confirmed.has(n)) continue;
    const offset = (n - 1) * upload.partBytes;
    const size = partSize(upload, n);
    const previous = upload.pending?.number === n ? upload.pending.sent : 0;
    let counted = 0;
    function track(loaded) {
      const resent = Math.min(previous, loaded);
      upload.resent += resent - counted;
      counted = resent;
      upload.pending = { number: n, sent: Math.max(previous, loaded) };
      persist(upload);
      showMetrics(upload);
      progress.value = (bytes + loaded) / upload.size;
    }
    setStatus(`Uploading part ${n} of ${count}…`, bytes / upload.size);
    const etag = await sendPart(`${path}/parts/${n}`, code, file.slice(offset, offset + size), track);
    track(size);
    confirmed.set(n, etag);
    bytes += size;
    delete upload.pending;
    persist(upload);
    onConfirmed();
  }
  return Array.from(confirmed.keys()).sort((a, b) => a - b)
    .map(n => ({ part_number: n, etag: confirmed.get(n) }));
}

function transient(failure) {
  return failure instanceof ConnectionError ||
    failure instanceof RequestError && [408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524].includes(failure.status);
}

async function transfer(upload, code, file) {
  const path = `${UPLOADS}/${encodeURIComponent(upload.id)}`;
  let retries = 0;
  let submitted = false;
  for (;;) {
    try {
      setStatus("Checking saved upload with the host…");
      let result = await request(`${path}/status`, code);
      if (result.state === "initiated" || result.state === "uploading") {
        if (submitted && result.error) throw new Error(result.error);
        const listing = await request(`${path}/parts`, code);
        const confirmed = confirmedParts(upload, listing);
        const parts = await sendMissing(path, code, upload, file, confirmed, () => { retries = 0; });
        setStatus("Verifying on the server…");
        submitted = true;
        result = await request(`${path}/complete`, code, "POST", { parts });
      }
      while (result.state === "finalizing") {
        submitted = true;
        setStatus("Verifying on the server…");
        await wait(1200);
        result = await request(`${path}/status`, code);
        retries = 0;
      }
      if (result.state !== "completed" || result.size !== upload.size || result.sha256 !== upload.sha256) {
        throw new Error(typeof result.error === "string" ? result.error : "The host has not verified this file.");
      }
      upload.completed = true;
      delete upload.pending;
      persist(upload);
      addOption(upload);
      refreshSelection();
      completedUpload = { code, path };
      receipt.textContent = `${result.size.toLocaleString()} bytes · SHA-256 ${result.sha256}`;
      if (Number.isSafeInteger(result.finalization_ms) && result.finalization_ms >= 0) {
        receipt.textContent += ` · Host verification and finalization: ${result.finalization_ms.toLocaleString()} ms`;
      }
      setStatus(`${upload.name} uploaded and verified.`);
      share.hidden = false;
      return;
    } catch (failure) {
      if (failure instanceof RequestError && [404, 410].includes(failure.status)) {
        throw new Error(`${failure.message}. Choose “Start a new upload” to send the file again.`);
      }
      if (!transient(failure)) throw failure;
      if (++retries > 5) throw new Error(`${failure.message} Check the saved upload to try again.`);
      const seconds = Math.min(2 ** (retries - 1), 16);
      setStatus(`Connection interrupted. Checking the host again in ${seconds} s (retry ${retries} of 5)…`);
      await wait(seconds * 1000);
    }
  }
}

form.addEventListener("submit", async event => {
  event.preventDefault();
  if (uploadButton.disabled) return;
  const code = codeInput.value.trim();
  const [file] = fileInput.files;
  clearResult();
  if (!HEX_DIGEST.test(code)) {
    showError("Enter the 64-character upload code from your host.");
    return;
  }
  if ((!file && !savedSelect.value) || (file && (file.size < 1 || file.size > MAX_BYTES))) {
    showError("Choose a non-empty file up to 12 GB.");
    return;
  }

  uploadButton.disabled = fileInput.disabled = codeInput.disabled = savedSelect.disabled = true;
  try {
    let upload = savedSelect.value && loadSaved(savedSelect.value);
    if (savedSelect.value && !upload) throw new Error("This saved upload is unavailable. Reload the page.");
    let digest;
    if (file) {
      setStatus("Checking file…", 0);
      digest = await hashFile(file, count => { progress.value = count / file.size; });
      if (upload && (digest !== upload.sha256 || file.size !== upload.size)) {
        throw new Error("The selected file does not match the original file’s SHA-256 or size. Choose the original file.");
      }
    }
    if (!upload) {
      // Fail before admitting a server session if this browser cannot save it.
      localStorage.setItem(STORAGE_PREFIX + "probe", "0");
      localStorage.removeItem(STORAGE_PREFIX + "probe");
      const session = await request(UPLOADS, code, "POST", { object: file.name, size: file.size, sha256: digest });
      if (typeof session.upload_id !== "string" || !session.upload_id) throw new Error("The host did not provide an upload session.");
      upload = { id: session.upload_id, name: file.name, size: file.size, sha256: digest,
        partBytes: PART_BYTES, created: Date.now(), resent: 0, completed: false };
      persist(upload);
      addOption(upload);
      savedSelect.value = upload.id;
    }
    showMetrics(upload);
    await transfer(upload, code, file);
    fileInput.value = codeInput.value = "";
  } catch (failure) {
    status.hidden = progress.hidden = true;
    showError(failure.message);
  } finally {
    uploadButton.disabled = fileInput.disabled = codeInput.disabled = savedSelect.disabled = false;
    refreshSelection();
  }
});

createLinkButton.addEventListener("click", async () => {
  const upload = completedUpload;
  if (!upload) {
    return;
  }
  error.hidden = true;
  createLinkButton.disabled = true;
  try {
    const result = await request(`${upload.path}/download-link`, upload.code, "POST");
    if (completedUpload !== upload) {
      return;
    }
    const expiry = new Date(result.expires_at * 1000);
    const pathPattern = /^\/d\/[A-Za-z0-9_-]+\.[0-9]+\.[0-9a-f]{64}$/;
    if (!pathPattern.test(result.url) || !Number.isSafeInteger(result.expires_at) ||
        Number.isNaN(expiry.valueOf())) {
      throw new Error("The server did not provide a valid download link.");
    }
    linkInput.value = new URL(result.url, location.origin).href;
    linkExpiry.textContent = `Expires ${expiry.toLocaleString()}. Anyone with the link can download the file.`;
    linkResult.hidden = false;
  } catch (failure) {
    if (completedUpload === upload) {
      showError(failure.message);
    }
  } finally {
    createLinkButton.disabled = false;
  }
});

copyLinkButton.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(linkInput.value);
    setStatus("Download link copied.");
  } catch {
    linkInput.select();
  }
});
