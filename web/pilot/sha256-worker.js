import { sha256 } from "./sha256.js";

const HASH_BLOCK_BYTES = 8 * 1024 * 1024;

self.onmessage = async ({ data: file }) => {
  try {
    const hash = sha256.create();
    let previousUpdate = performance.now();
    for (let offset = 0; offset < file.size; offset += HASH_BLOCK_BYTES) {
      const end = Math.min(offset + HASH_BLOCK_BYTES, file.size);
      const chunk = await file.slice(offset, end).arrayBuffer();
      hash.update(new Uint8Array(chunk));
      const now = performance.now();
      if (now - previousUpdate >= 250) {
        self.postMessage({ processed: end });
        previousUpdate = now;
      }
    }
    const digest = Array.from(hash.digest(), byte => byte.toString(16).padStart(2, "0")).join("");
    self.postMessage({ digest });
  } catch {
    self.postMessage({ error: "Could not read the selected file." });
  }
};
