import { NotFound, ServiceUnavailable } from 'http-errors';
import { Controller, Get, Path, Route } from "tsoa";
import { cache, readPool, longReadPool } from "../db";
import { Utils } from '@bsv/sdk';


@Route("api/collections")
export class CollectionsController extends Controller {
    // Collection stats are expensive (MAX+COUNT over all of a collection's txos, which can be
    // millions of rows). We cache them, serve stale-while-revalidate so a request never blocks on
    // the heavy query, and single-flight the recompute so the 12 cluster workers can't stampede the
    // same collection. The recompute runs with a longer per-query timeout than the global pool
    // default so it can actually finish and warm the cache (the global 60s statement_timeout, added
    // to fail fast on Postgres failovers, would otherwise cancel it and the cache could never warm).
    // It also runs on the replica (longReadPool), so a multi-minute scan -- including one left
    // running server-side by a worker that died mid-compute -- costs the primary nothing.
    private static readonly FRESH_MS = 30 * 60 * 1000;            // serve without refreshing for 30 min
    private static readonly HARD_TTL_S = 24 * 60 * 60;            // keep a (stale-servable) value up to 1 day
    private static readonly LOCK_TTL_S = 90;                      // short: a dead holder's lock frees itself fast
    private static readonly LOCK_RENEW_MS = 30 * 1000;            // live holder re-extends well before expiry
    private static readonly COMPUTE_TIMEOUT_MS = 15 * 60 * 1000;  // let the heavy background query finish
    private static readonly FAIL_BACKOFF_S = 30 * 60;             // after a failed compute, don't retry for 30 min

    // The lock TTL is deliberately far shorter than a worst-case compute; a live holder keeps
    // extending it (see the heartbeat in refreshCollectionStats). Both scripts compare a per-holder
    // token first so a holder whose lock already lapsed (long Redis stall, paused process) can never
    // extend or delete the lock a *successor* now owns. Redis runs each script atomically, so a
    // renewal racing a release can't resurrect a deleted key: it either extends before the DEL, or
    // finds the key gone and no-ops (PEXPIRE on a missing key does nothing).
    private static readonly RENEW_LUA =
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";
    private static readonly RELEASE_LUA =
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    @Get("{collectionId}/stats")
    public async getCollection(
        @Path() collectionId: string,
    ): Promise<{ count: number, max: number }> {
        const cacheKey = `stats:${collectionId}`;
        this.setHeader('Cache-Control', 'max-age=600')
        const cached = await cache.get(cacheKey);
        if (cached) {
            const p = JSON.parse(cached);
            // stale-while-revalidate: refresh in the background, never block the caller
            if (!p.ts || (Date.now() - p.ts) > CollectionsController.FRESH_MS) {
                this.refreshCollectionStats(collectionId).catch(() => {});
            }
            return { count: p.count, max: p.max };
        }
        // cold cache: recompute under a single-flight lock so only one worker runs the heavy query.
        // Race it against a short deadline: small collections return 200 immediately; a slow one is
        // left running in the background (it will warm the cache) and the caller gets told to retry
        // rather than blocking for minutes.
        if (await cache.exists(`stats:fail:${collectionId}`)) {
            throw new ServiceUnavailable('Collection stats are temporarily unavailable for this collection; please retry later.');
        }
        const TIMEOUT = Symbol('timeout');
        const refresh = this.refreshCollectionStats(collectionId);
        refresh.catch(() => {}); // detach: it may outlive this request
        const raced = await Promise.race([
            refresh,
            new Promise((r) => setTimeout(() => r(TIMEOUT), 5000)),
        ]);
        if (raced && raced !== TIMEOUT) {
            return raced as { count: number, max: number };
        }
        // either another worker is computing, or ours is still running in the background; poll briefly
        for (let i = 0; i < 12; i++) {
            await new Promise((r) => setTimeout(r, 500));
            const c = await cache.get(cacheKey);
            if (c) {
                const p = JSON.parse(c);
                return { count: p.count, max: p.max };
            }
        }
        throw new ServiceUnavailable('Collection stats are being computed; please retry shortly.');
    }

    // Runs the expensive aggregate with a per-query statement_timeout override (SET LOCAL inside a
    // transaction, so it auto-resets and never leaks the long timeout back to the pooled connection).
    private async computeCollectionStats(collectionId: string): Promise<{ count: number, max: number }> {
        const client = await longReadPool.connect();
        // A checked-out client has no pool 'error' listener. If the replica terminates the session
        // ("conflict with recovery"), pg emits 'error' on the client; unhandled, that is an
        // uncaughtException and pm2 takes down the whole worker. The pending query still rejects.
        let clientError: Error | undefined;
        const onClientError = (err: Error) => {
            clientError = err;
            console.error('pg longReadPool error (stats compute):', err.message);
        };
        client.on('error', onClientError);
        try {
            await client.query('BEGIN');
            await client.query(`SET LOCAL statement_timeout = ${CollectionsController.COMPUTE_TIMEOUT_MS}`);
            const { rows: [row] } = await client.query(`
        SELECT MAX((data->'map'->'subTypeData'->>'mintNumber')::BIGINT) as maxnum,
        COUNT(1)::INTEGER as count
        FROM txos
        WHERE data @> $1`,
                [JSON.stringify({ map: { subTypeData: { collectionId } } })],
            )
            await client.query('COMMIT');
            if (!row) throw new NotFound();
            return { count: row.count, max: row.maxnum };
        } catch (e) {
            if (!clientError) await client.query('ROLLBACK').catch(() => {});
            throw e;
        } finally {
            client.removeListener('error', onClientError);
            // Passing the error makes the pool discard a broken connection instead of reusing it.
            client.release(clientError);
        }
    }

    // Single-flight: only the worker that wins the Redis lock recomputes; others get null and fall
    // back to the (possibly stale) cached value.
    private async refreshCollectionStats(collectionId: string): Promise<{ count: number, max: number } | null> {
        const failKey = `stats:fail:${collectionId}`;
        if (await cache.exists(failKey)) return null;
        const lockKey = `stats:lock:${collectionId}`;
        // Unique per holder so renew/release can prove ownership before mutating the key.
        const token = `${process.pid}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
        const got = await cache.set(lockKey, token, 'EX', CollectionsController.LOCK_TTL_S, 'NX');
        if (!got) return null;

        // Heartbeat: keep extending our own lock for as long as this worker is alive to compute.
        // If it dies mid-compute (a `pm2 reload` landing on an in-flight query, a crash), nothing
        // renews and the lock lapses in <= LOCK_TTL_S -- so the next request recomputes instead of
        // the collection being stranded on 503s until the old fixed 20 min TTL expired.
        const heartbeat = setInterval(() => {
            cache.eval(
                CollectionsController.RENEW_LUA, 1,
                lockKey, token, String(CollectionsController.LOCK_TTL_S * 1000),
            ).catch(() => { }); // a dropped renewal isn't fatal; the next tick retries
        }, CollectionsController.LOCK_RENEW_MS);
        heartbeat.unref?.(); // never let the timer keep the event loop alive on shutdown

        try {
            const value = await this.computeCollectionStats(collectionId);
            await cache.set(
                `stats:${collectionId}`,
                JSON.stringify({ ...value, ts: Date.now() }),
                'EX', CollectionsController.HARD_TTL_S,
            );
            return value;
        } catch (e) {
            // Without this, every request after the lock lapses (<= LOCK_TTL_S) starts the same
            // doomed scan again -- one collection was retrying ~every 90s on the replica.
            await cache.set(failKey, (e as Error)?.message || 'compute failed', 'EX', CollectionsController.FAIL_BACKOFF_S).catch(() => { });
            throw e;
        } finally {
            clearInterval(heartbeat);
            await cache.eval(CollectionsController.RELEASE_LUA, 1, lockKey, token).catch(() => { });
        }
    }

    @Get("{collectionId}/holders")
    public async getCollectionHolders(
        @Path() collectionId: string,
    ): Promise<{ address: string, amt: string }[]> {
        // const cacheKey = `coll:${collectionId}:holders`

        // this.setHeader('Cache-Control', 'max-age=3600')
        // const status = await cache.get(cacheKey);
        // if (status) {
        //     return JSON.parse(status).slice(0, limit);
        // }

        const { rows } = await readPool.query(`
            SELECT t.pkhash, COUNT(1) as amt
            FROM txos t
            LEFT JOIN txos o ON o.outpoint = t.origin
            WHERE o.data @> $1 AND t.spend='\\x'
            GROUP BY t.pkhash
            ORDER BY amt DESC`,
            [JSON.stringify({ map: { subTypeData: { collectionId } } })],
        )

        // const { rows } = await pool.query(`
        //     SELECT pkhash, SUM(amt) as amt
        //     FROM bsv20_txos
        //     WHERE tick=$1 AND status=1 AND spend='\\x' and pkhash != '\\x'
        //     GROUP BY pkhash
        //     ORDER BY amt DESC`,
        //     [tick],
        // );
        const tokens = rows.map(r => ({
            address: Utils.toBase58Check([...r.pkhash]),
            amt: r.amt,
        }))
        // await cache.set(cacheKey, JSON.stringify(tokens), 'EX', 60);
        return tokens;
    }
}