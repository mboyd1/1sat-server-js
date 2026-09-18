import { JungleBusClient } from "@gorillapool/js-junglebus";
import { NotFound } from 'http-errors';
import { Redis } from "ioredis";
import { Pool } from 'pg';
import { MerklePath, Transaction, Utils } from "@bsv/sdk";
import { BlockHeader } from "./models/block";

const { POSTGRES_FULL, POSTGRES_READ, BITCOIN_HOST, BITCOIN_PORT, JUNGLEBUS, REDISDB, REDISCACHE, HEADERS } = process.env;
export const jb = new JungleBusClient(JUNGLEBUS || 'https://junglebus.gorillapool.io');
const cparts = (REDISCACHE || '').split(':')

export let cache: Redis = new Redis({
    port: cparts[1] ? parseInt(cparts[1]) : 6379,
    host: cparts[0],
});

const rparts = (REDISDB || '').split(':')
export const redis = new Redis({
    port: rparts[1] ? parseInt(rparts[1]) : 6379,
    host: rparts[0],
});

const POSTGRES = POSTGRES_FULL
console.log("POSTGRES", POSTGRES)

// Fail fast instead of hanging indefinitely when the Postgres endpoint is briefly unavailable
// during a failover or a health-check flap. Without these limits a pool checkout or query blocks
// forever, so simple requests stall until the caller's own timeout fires. All values are
// env-overridable so they can be tuned without a redeploy.
const pgNum = (v: string | undefined, d: number) => (v && !isNaN(Number(v)) ? Number(v) : d);
export const pool = new Pool({
    connectionString: POSTGRES,
    max: pgNum(process.env.PG_POOL_MAX, 10),
    connectionTimeoutMillis: pgNum(process.env.PG_CONNECT_TIMEOUT_MS, 5000),
    idleTimeoutMillis: pgNum(process.env.PG_IDLE_TIMEOUT_MS, 30000),
    query_timeout: pgNum(process.env.PG_QUERY_TIMEOUT_MS, 60000),
    statement_timeout: pgNum(process.env.PG_STATEMENT_TIMEOUT_MS, 60000),
    keepAlive: true,
});

// Required: node-postgres emits 'error' on idle pooled clients whose connection is dropped,
// which is exactly what happens when the endpoint drops established sessions during a failover.
// With no listener the error is unhandled and takes down the whole worker.
pool.on('error', (err) => {
    console.error('pg pool error (idle client):', err.message);
});

// Dedicated pool for rare, deliberately-long background computes (e.g. collection stats over
// millions of rows). It has NO client-side query_timeout, so callers bound duration server-side via
// `SET LOCAL statement_timeout`; kept small so heavy queries can't starve the main pool. (pg 8.x
// ignores a per-query query_timeout unless you pass a Query object, hence a separate pool.)
export const longPool = new Pool({
    connectionString: POSTGRES,
    max: pgNum(process.env.PG_LONGPOOL_MAX, 4),
    connectionTimeoutMillis: pgNum(process.env.PG_CONNECT_TIMEOUT_MS, 5000),
    idleTimeoutMillis: pgNum(process.env.PG_IDLE_TIMEOUT_MS, 30000),
    keepAlive: true,
});
longPool.on('error', (err) => {
    console.error('pg longPool error (idle client):', err.message);
});

// Read-only pools pointed at a streaming replica, to keep analytical/browse traffic off the
// primary. Only endpoints that tolerate replication lag use these -- wallet-facing reads
// (UTXOs, balances, spend state, tx status) deliberately stay on the primary `pool`, since a
// lagged answer there surfaces as "insufficient funds" or a double-spend attempt.
//
// Two safety properties worth knowing:
//   1. Unset POSTGRES_READ falls back to the primary, so blanking the env var is a complete
//      kill-switch for this feature -- no redeploy needed, just restart the app.
//   2. The read endpoint is expected to fail back to the primary on its own when no replica is
//      available, so a dead or rebuilding replica degrades to primary reads rather than
//      erroring. That is why there is no application-level fallback wrapper here.
const POSTGRES_RO = POSTGRES_READ || POSTGRES;
console.log("POSTGRES_READ", POSTGRES_READ ? "replica" : "(unset; falling back to primary)");
export const readPool = new Pool({
    connectionString: POSTGRES_RO,
    max: pgNum(process.env.PG_READ_POOL_MAX, 10),
    connectionTimeoutMillis: pgNum(process.env.PG_CONNECT_TIMEOUT_MS, 5000),
    idleTimeoutMillis: pgNum(process.env.PG_IDLE_TIMEOUT_MS, 30000),
    query_timeout: pgNum(process.env.PG_QUERY_TIMEOUT_MS, 60000),
    statement_timeout: pgNum(process.env.PG_STATEMENT_TIMEOUT_MS, 60000),
    keepAlive: true,
});
readPool.on('error', (err) => {
    console.error('pg readPool error (idle client):', err.message);
});

// Replica twin of longPool: no client-side query_timeout, for the deliberately-long collection
// stats aggregate. Running it here means a 6-14 min scan costs the primary nothing -- including
// when a worker dies mid-compute and leaves the query running server-side until its timeout.
export const longReadPool = new Pool({
    connectionString: POSTGRES_RO,
    max: pgNum(process.env.PG_LONGPOOL_MAX, 4),
    connectionTimeoutMillis: pgNum(process.env.PG_CONNECT_TIMEOUT_MS, 5000),
    idleTimeoutMillis: pgNum(process.env.PG_IDLE_TIMEOUT_MS, 30000),
    keepAlive: true,
});
longReadPool.on('error', (err) => {
    console.error('pg longReadPool error (idle client):', err.message);
});

export async function getChainTip(): Promise<BlockHeader> {
    const resp = await fetch(`${HEADERS}/api/v1/chain/tip/longest`);
    if (!resp.ok) {
        throw new Error(`Failed to fetch chain tip: ${resp.status} ${resp.statusText}`);
    }
    const data = await resp.json();
    return {
        hash: data.header.hash,
        height: data.height,
        version: data.header.version,
        prevHash: data.header.prevBlockHash,
        merkleroot: data.header.merkleRoot,
        time: data.header.creationTimestamp,
        bits: data.header.difficultyTarget,
        nonce: data.header.nonce,
    } as BlockHeader;
}

export async function loadRawtx(txid: string): Promise<Buffer> {

    const cacheKey = `tx:${txid}`;
    let rawtx = await cache.getBuffer(cacheKey);

    if (!rawtx) {
        const url = `${JUNGLEBUS}/v1/transaction/get/${txid}/bin`
        console.log('JB fetch:', url)
        const resp = await fetch(url);
        if (resp.ok && resp.status == 200) {
            const buf = await resp.arrayBuffer();
            if (buf.byteLength > 0) {
                rawtx = Buffer.from(buf);
                await cache.setex(cacheKey, 600, rawtx);
            }
        } else {
            console.error('JB error:', txid, resp.status, resp.statusText, url)
        }
    }

    if (!rawtx && BITCOIN_HOST) {
        const url = `http://${BITCOIN_HOST}:${BITCOIN_PORT}/rest/tx/${txid}.bin`
        const resp = await fetch(url);
        if (resp.ok && resp.status == 200) {
            const buf = await resp.arrayBuffer();
            if (buf.byteLength > 0) {
                rawtx = Buffer.from(buf);
                await cache.setex(cacheKey, 600, rawtx);
            }
        } else console.error('Node error:', txid, resp.status, resp.statusText)
    }

    if (rawtx) return rawtx;

    throw new NotFound(`${txid} not found`);
}

export async function loadTx(txid: string): Promise<Transaction> {
    const rawtx = await loadRawtx(txid);
    return Transaction.fromBinary([...rawtx]);
}

export async function loadTxWithProof(txid: string): Promise<number[]> {
    const [rawtx, proof] = await Promise.all([
        loadRawtx(txid).then(rawtx => [...rawtx] as number[]),
        loadProof(txid).then(proof => [...proof] as number[]).catch(() => [] as number[])
    ])

    const writer = new Utils.Writer();
    writer.writeVarIntNum(rawtx.length)
    writer.write(rawtx)
    writer.writeVarIntNum(proof.length)
    writer.write(proof)
    const resp = writer.toArray();
    console.log('GET TX:', txid, rawtx.length, proof.length, JSON.stringify(resp.slice(0, 10)))
    return resp;
}


export async function loadProof(txid: string): Promise<Buffer> {
    const cacheKey = `prf:${txid}`;
    let proof = await cache.getBuffer(cacheKey);
    if (!proof) {
        const resp = await fetch(`${JUNGLEBUS}/v1/transaction/proof/${txid}/bin`);
        if (!resp.ok) {
            // throw createError(resp.status, resp.statusText)
            throw new NotFound(`${txid} not found`);
        }
        proof = Buffer.from(await resp.arrayBuffer())
        const merklePath = MerklePath.fromBinary([...proof]);
        const chaintip = await getChainTip();
        if (merklePath.blockHeight < chaintip.height - 5) {
            await cache.set(cacheKey, proof)
        } else {
            await cache.setex(cacheKey, 60, proof)
        }
    }

    if (!proof) {
        throw new NotFound(`${txid} not found`);
    }
    return proof;
}