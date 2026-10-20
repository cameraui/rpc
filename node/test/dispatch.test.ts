import { describe, expect, it } from 'vitest';

import { createBoundedDispatcher, createSerialDispatcher, sleep } from '../src/utils.js';

interface Msg {
  id: number;
  delayMs?: number;
  fail?: boolean;
}

function makeHandler(events: string[]) {
  return (msg: Msg): void | Promise<void> => {
    events.push(`start:${msg.id}`);
    if (msg.delayMs === undefined) {
      if (msg.fail) throw new Error(`sync-fail-${msg.id}`);
      events.push(`end:${msg.id}`);
      return;
    }
    return sleep(msg.delayMs).then(() => {
      if (msg.fail) throw new Error(`async-fail-${msg.id}`);
      events.push(`end:${msg.id}`);
    });
  };
}

describe('createSerialDispatcher', () => {
  it('runs purely synchronous handlers inline (no microtask deferral)', () => {
    const events: string[] = [];
    const { dispatch } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1 });
    dispatch({ id: 2 });

    // before any microtask runs
    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2']);
  });

  it('preserves strict dispatch order for mixed sync/async handlers', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1, delayMs: 15 });
    dispatch({ id: 2 });
    dispatch({ id: 3, delayMs: 1 });
    dispatch({ id: 4 });

    await flush();

    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2', 'start:3', 'end:3', 'start:4', 'end:4']);
  });

  it('re-arms the sync fast path once the chain drains', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1, delayMs: 1 });
    await flush();

    // idle again, so the sync handler runs inline
    dispatch({ id: 2 });
    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2']);
  });

  it('keeps ordering across interleaved dispatches from async context', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1, delayMs: 5 });
    await sleep(1);
    dispatch({ id: 2 });
    dispatch({ id: 3, delayMs: 5 });
    await sleep(20);
    dispatch({ id: 4 });

    await flush();

    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2', 'start:3', 'end:3', 'start:4', 'end:4']);
  });

  it('reports errors via onError and keeps dispatching subsequent messages', async () => {
    const events: string[] = [];
    const errors: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), (error) => errors.push((error as Error).message));

    dispatch({ id: 1, fail: true }); // sync throw on an idle chain
    dispatch({ id: 2, delayMs: 1, fail: true });
    dispatch({ id: 3, fail: true }); // sync throw on a busy chain
    dispatch({ id: 4 });

    await flush();

    expect(errors).toEqual(['sync-fail-1', 'async-fail-2', 'sync-fail-3']);
    expect(events).toEqual(['start:1', 'start:2', 'start:3', 'start:4', 'end:4']);
  });

  it('flush() resolves immediately when the chain is idle', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1 });

    let flushed = false;
    const p = flush().then(() => {
      flushed = true;
    });
    await Promise.resolve();
    expect(flushed).toBe(true);
    await p;
  });

  it('flush() waits for every handler dispatched so far', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1, delayMs: 10 });
    dispatch({ id: 2 });
    dispatch({ id: 3, delayMs: 5 });

    await flush();

    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2', 'start:3', 'end:3']);
  });
});

describe('createBoundedDispatcher', () => {
  function tracked(limitEvents: string[]) {
    let inFlight = 0;
    let maxInFlight = 0;
    const handler = (msg: Msg & { exclusive?: boolean }): void | Promise<void> => {
      limitEvents.push(`start:${msg.id}`);
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      if (msg.delayMs === undefined) {
        inFlight--;
        if (msg.fail) throw new Error(`sync-fail-${msg.id}`);
        limitEvents.push(`end:${msg.id}`);
        return;
      }
      return sleep(msg.delayMs).then(() => {
        inFlight--;
        if (msg.fail) throw new Error(`async-fail-${msg.id}`);
        limitEvents.push(`end:${msg.id}`);
      });
    };
    return { handler, max: () => maxInFlight };
  }

  const shared = (msg: { exclusive?: boolean }) => !msg.exclusive;

  it('runs up to the limit at once and starts in arrival order', async () => {
    const events: string[] = [];
    const t = tracked(events);
    const { dispatch, flush } = createBoundedDispatcher(t.handler, 3, shared, () => {});

    for (let id = 1; id <= 7; id++) dispatch({ id, delayMs: 10 - id });
    await flush();

    expect(t.max()).toBe(3);
    expect(events.filter((e) => e.startsWith('start')).map((e) => Number(e.slice(6)))).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(events.filter((e) => e.startsWith('end')).length).toBe(7);
  });

  it('cuts total time for slow handlers', async () => {
    const { dispatch, flush } = createBoundedDispatcher(
      () => sleep(30),
      8,
      () => true,
      () => {},
    );
    const started = Date.now();
    for (let i = 0; i < 8; i++) dispatch({});
    await flush();
    expect(Date.now() - started).toBeLessThan(150);
  });

  it('runs a non-shared message alone and keeps later messages behind it', async () => {
    const events: string[] = [];
    const t = tracked(events);
    const { dispatch, flush } = createBoundedDispatcher(t.handler, 4, shared, () => {});

    dispatch({ id: 1, delayMs: 10 });
    dispatch({ id: 2, delayMs: 10 });
    dispatch({ id: 3, delayMs: 5, exclusive: true } as Msg);
    dispatch({ id: 4, delayMs: 1 });
    await flush();

    expect(events.indexOf('start:3')).toBeGreaterThan(events.indexOf('end:1'));
    expect(events.indexOf('start:3')).toBeGreaterThan(events.indexOf('end:2'));
    expect(events.indexOf('start:4')).toBeGreaterThan(events.indexOf('end:3'));
  });

  it('runs synchronous handlers inline and reports errors', async () => {
    const events: string[] = [];
    const errors: string[] = [];
    const t = tracked(events);
    const { dispatch, flush } = createBoundedDispatcher(t.handler, 2, shared, (e) => errors.push((e as Error).message));

    dispatch({ id: 1 });
    dispatch({ id: 2, fail: true });
    expect(events).toEqual(['start:1', 'end:1', 'start:2']);

    dispatch({ id: 3, delayMs: 1, fail: true });
    dispatch({ id: 4, delayMs: 1 });
    await flush();

    expect(errors).toEqual(['sync-fail-2', 'async-fail-3']);
    expect(events).toContain('end:4');
  });

  it('keeps arrival order through a backlog of thousands of messages', async () => {
    const started: number[] = [];
    const errors: unknown[] = [];
    const { dispatch, flush } = createBoundedDispatcher(
      (msg: { id: number }) => {
        started.push(msg.id);
        return Promise.resolve();
      },
      2,
      () => true,
      (e) => errors.push(e),
    );

    for (let id = 0; id < 3000; id++) dispatch({ id });
    await flush();

    expect(started).toEqual(Array.from({ length: 3000 }, (_, i) => i));
    expect(errors).toEqual([]);
  });

  it('flush() resolves at once when idle and after the queue drains otherwise', async () => {
    const { dispatch, flush } = createBoundedDispatcher(
      () => sleep(5),
      1,
      () => true,
      () => {},
    );
    let done = false;
    await flush();
    dispatch({});
    dispatch({});
    const p = flush().then(() => {
      done = true;
    });
    await sleep(1);
    expect(done).toBe(false);
    await p;
    expect(done).toBe(true);
  });
});
