/**
 * A Web Serial-like port over the MeshCore collector repeater's WebSocket (ws://<host>/ws).
 *
 * The node sends the same bytes over the WebSocket as over USB (CLI echo, "  -> " replies),
 * so SerialCLI runs unchanged on top of this: it only needs open(), close(), readable and writable.
 *
 * open() logs in with "auth <password>" before handing the stream over, so the echoed password
 * never reaches SerialCLI or the console.
 */

const toBytes = (data) => (data instanceof ArrayBuffer ? new Uint8Array(data) : new TextEncoder().encode(String(data)));

export class WebSocketPort {
  /**
   * @param {string} url e.g. ws://my-node.local/ws
   * @param {string} password the node's admin password
   */
  constructor(url, password) {
    this.url = url;
    this.password = password;
    this.ws = null;
    this.readable = null;
    this.writable = null;
  }

  async open() {
    const ws = new WebSocket(this.url);
    ws.binaryType = 'arraybuffer';
    this.ws = ws;

    await new Promise((resolve, reject) => {
      const decoder = new TextDecoder();
      let text = '';
      let settled = false;
      const done = (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (err) {
          ws.close();
          reject(err);
        } else {
          resolve();
        }
      };
      const timer = setTimeout(() => done(new Error(`No answer from ${this.url}`)), 8000);

      ws.onerror = () => done(new Error(`Cannot reach ${this.url}`));
      ws.onclose = () => done(new Error('Connection closed during login'));
      ws.onopen = () => ws.send(new TextEncoder().encode(`auth ${this.password}\r`));
      ws.onmessage = (e) => {
        text += decoder.decode(toBytes(e.data), { stream: true });
        if (text.includes('OK - authenticated')) done();
        else if (text.includes('Err - auth')) done(new Error('Wrong admin password'));
      };
    });
    this.password = '';

    this.readable = new ReadableStream({
      start: (controller) => {
        ws.onmessage = (e) => {
          try { controller.enqueue(toBytes(e.data)); } catch (err) { /* stream already closed */ }
        };
        ws.onclose = () => {
          try { controller.close(); } catch (err) { /* already closed */ }
        };
        ws.onerror = () => {
          try { controller.error(new Error('WebSocket error')); } catch (err) { /* already closed */ }
        };
      },
      cancel: () => ws.close(),
    });

    this.writable = new WritableStream({
      write: (chunk) => {
        if (ws.readyState !== WebSocket.OPEN) throw new Error('WebSocket closed');
        ws.send(chunk);
      },
      close: () => ws.close(),
      abort: () => ws.close(),
    });
  }

  async close() {
    if (this.ws && this.ws.readyState <= WebSocket.OPEN) this.ws.close();
  }

  getInfo() {
    return { url: this.url };
  }
}
