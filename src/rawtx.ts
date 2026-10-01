// Zero-copy parsing of raw transactions held as Buffers.
//
// Transaction.fromBinary takes number[], so loadTx spread every rawtx into a JS array
// (8+ bytes of heap per tx byte) and the SDK then copied each script into more arrays.
// A single 44MB inscription cost ~2GB of heap that way -- the ceiling for an api worker --
// and a crawler walking large inscriptions on 2026-10-01 took out every worker at once,
// then the box. Everything here returns subarray() views into the original Buffer instead.
//
// Short reads follow @bsv/sdk's Reader (missing bytes read as 0, slices truncate) so that
// results match what the SDK-based code returned for the same bytes.

import { Utils } from "@bsv/sdk";
import type { InscriptionData } from "./models/txo";

const OP_0 = 0x00;
const OP_PUSHDATA1 = 0x4c;
const OP_PUSHDATA2 = 0x4d;
const OP_PUSHDATA4 = 0x4e;
const OP_1 = 0x51;
const OP_IF = 0x63;
const OP_ENDIF = 0x68;

const B = Buffer.from('19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut');
const ORD = Buffer.from('ord');

class Cursor {
    pos = 0;
    constructor(private buf: Buffer) { }

    eof() {
        return this.pos >= this.buf.length;
    }

    u8() {
        const v = this.buf[this.pos] ?? 0;
        this.pos += 1;
        return v;
    }

    u16() {
        return this.u8() | (this.u8() << 8);
    }

    u32() {
        return (this.u8() | (this.u8() << 8) | (this.u8() << 16) | (this.u8() << 24)) >>> 0;
    }

    varint() {
        const first = this.u8();
        switch (first) {
            case 0xfd: return this.u16();
            case 0xfe: return this.u32();
            case 0xff: {
                const lo = this.u32();
                const hi = this.u32();
                return hi * 0x100000000 + lo;
            }
            default: return first;
        }
    }

    skip(len: number) {
        this.pos += len;
    }

    slice(len: number) {
        const start = Math.min(this.pos, this.buf.length);
        const end = Math.min(this.pos + len, this.buf.length);
        this.pos += len;
        return this.buf.subarray(start, end);
    }
}

// Yields each output's locking script, in order, as a view into rawtx.
export function* outputScripts(rawtx: Buffer): Generator<Buffer> {
    const c = new Cursor(rawtx);
    c.skip(4); // version
    const nIn = c.varint();
    for (let i = 0; i < nIn; i++) {
        c.skip(36); // prev txid + vout
        c.skip(c.varint()); // unlocking script
        c.skip(4); // sequence
    }
    const nOut = c.varint();
    for (let i = 0; i < nOut; i++) {
        c.skip(8); // satoshis
        yield c.slice(c.varint());
    }
}

export function outputScript(rawtx: Buffer, vout: number): Buffer | undefined {
    let i = 0;
    for (const script of outputScripts(rawtx)) {
        if (i++ === vout) return script;
    }
    return;
}

interface Chunk {
    op: number;
    data?: Buffer;
}

// Same chunking as @bsv/sdk 1.1.x Script.fromBinary: pushes carry data, everything
// else is a bare opcode. No OP_RETURN special case, matching that version.
export function scriptChunks(script: Buffer): Chunk[] {
    const c = new Cursor(script);
    const chunks: Chunk[] = [];
    while (!c.eof()) {
        const op = c.u8();
        if (op > 0 && op < OP_PUSHDATA1) {
            chunks.push({ op, data: c.slice(op) });
        } else if (op === OP_PUSHDATA1) {
            chunks.push({ op, data: c.slice(c.u8()) });
        } else if (op === OP_PUSHDATA2) {
            chunks.push({ op, data: c.slice(c.u16()) });
        } else if (op === OP_PUSHDATA4) {
            chunks.push({ op, data: c.slice(c.u32()) });
        } else {
            chunks.push({ op });
        }
    }
    return chunks;
}

const EMPTY = Buffer.alloc(0);
// Content types are a few bytes, so the SDK decoder costs nothing here, and it decodes
// invalid UTF-8 differently from Buffer's, which would change the Content-Type served.
const utf8 = (data?: Buffer) => Utils.toUTF8([...(data || EMPTY)]);
// Compares bytes rather than decoding: the old code utf8-decoded every chunk, payload
// included, so each 44MB inscription also became a 44MB string just to compare to 'ord'.
const is = (data: Buffer | undefined, tag: Buffer) => !!data && data.equals(tag);

// Buffer port of the former Txo.parseOutputScript; returned data is a view into script.
export function parseInscription(script: Buffer): InscriptionData | undefined {
    const chunks = scriptChunks(script);
    let opFalse = 0;
    let opIf = 0;
    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        if (chunk.op === OP_0) {
            opFalse = i;
        }
        if (chunk.op === OP_IF) {
            opIf = i;
        }
        if (is(chunk.data, ORD) && opFalse === i - 2 && opIf === i - 1) {
            const insData = {} as InscriptionData;
            for (let j = i + 1; j < chunks.length; j += 2) {
                switch (chunks[j].op) {
                    case OP_0:
                        insData.data = chunks[j + 1]?.data || EMPTY;
                        return insData;
                    case OP_1:
                        insData.type = utf8(chunks[j + 1]?.data);
                        break;
                    case OP_ENDIF:
                        break;
                }
            }
        }
        if (is(chunk.data, B)) {
            const insData = {} as InscriptionData;
            insData.data = chunks[i + 1]?.data || EMPTY;
            insData.type = utf8(chunks[i + 2]?.data);
            return insData;
        }
    }
    return;
}
