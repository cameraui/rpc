import { describe, expect, it } from 'vitest';

import { createSerialDispatcher, sleep } from '../src/utils.js';

interface Msg {
  id: number;
  /** When set, the handler is async and waits this long before finishing. */
  delayMs?: number;
  /** When set, the handler throws (sync) or rejects (async). */
  fail?: boolean;
}

function makeHandler(events: string[]) {
  return (msg: Msg): void | Promise<void> => {
    events.push(`start:${msg.id}`);
    if (msg.delayMs === undefined) {
      // Synchronous handler
      if (msg.fail) throw new Error(`sync-fail-${msg.id}`);
      events.push(`end:${msg.id}`);
      return;
    }
    // Asynchronous handler
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

    // Assert synchronously — before any microtask has a chance to run.
    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2']);
  });

  it('preserves strict dispatch order for mixed sync/async handlers', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1, delayMs: 15 }); // async, slow
    dispatch({ id: 2 }); // sync — must wait for 1
    dispatch({ id: 3, delayMs: 1 }); // async, fast — must still run after 2
    dispatch({ id: 4 }); // sync — must run last

    await flush();

    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2', 'start:3', 'end:3', 'start:4', 'end:4']);
  });

  it('re-arms the sync fast path once the chain drains', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1, delayMs: 1 });
    await flush();

    // Chain is idle again — sync handler must execute inline.
    dispatch({ id: 2 });
    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2']);
  });

  it('keeps ordering across interleaved dispatches from async context', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1, delayMs: 5 });
    await sleep(1); // chain still busy
    dispatch({ id: 2 });
    dispatch({ id: 3, delayMs: 5 });
    await sleep(20); // 1 and its followers settle; 3 done too
    dispatch({ id: 4 });

    await flush();

    expect(events).toEqual(['start:1', 'end:1', 'start:2', 'end:2', 'start:3', 'end:3', 'start:4', 'end:4']);
  });

  it('reports errors via onError and keeps dispatching subsequent messages', async () => {
    const events: string[] = [];
    const errors: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), (error) => errors.push((error as Error).message));

    dispatch({ id: 1, fail: true }); // sync throw on idle chain
    dispatch({ id: 2, delayMs: 1, fail: true }); // async reject
    dispatch({ id: 3, fail: true }); // sync throw while chain busy
    dispatch({ id: 4 });

    await flush();

    expect(errors).toEqual(['sync-fail-1', 'async-fail-2', 'sync-fail-3']);
    expect(events).toEqual(['start:1', 'start:2', 'start:3', 'start:4', 'end:4']);
  });

  it('flush() resolves immediately when the chain is idle', async () => {
    const events: string[] = [];
    const { dispatch, flush } = createSerialDispatcher(makeHandler(events), () => {});

    dispatch({ id: 1 }); // sync — done inline

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
