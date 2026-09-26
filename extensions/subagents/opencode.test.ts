import assert from "node:assert/strict";
import test from "node:test";
import { Effect } from "effect";
import { opencodeBackend } from "./src/backends/opencode.ts";
import type { ParentContext, SpawnTask } from "./src/domain.ts";
import { SubagentManager } from "./src/manager.ts";
import { createSubagentRuntime, runTool } from "./src/runtime.ts";

const parent: ParentContext = {
  parentCwd: process.cwd(),
  projectTrusted: false,
};

function task(prompt: string): SpawnTask {
  return {
    prompt,
    title: "live opencode test",
    cwd: process.cwd(),
    // Harness default, unless the runner pins one (e.g. a free-tier model).
    ...(process.env.OPENCODE_TEST_MODEL
      ? { model: process.env.OPENCODE_TEST_MODEL }
      : {}),
    parent,
  };
}

function deadline<A>(operation: Promise<A>, timeoutMs: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error(`Live opencode test exceeded ${timeoutMs}ms`)),
      timeoutMs,
    );
  });
  return Promise.race([operation, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

async function opencodeAvailable() {
  return Effect.runPromise(opencodeBackend.available);
}

test(
  "Opencode backend completes a live manager run",
  { timeout: 120_000 },
  async (t) => {
    if (!(await opencodeAvailable())) {
      t.skip("opencode executable is unavailable");
      return;
    }

    const runtime = createSubagentRuntime();
    try {
      const manager = await runtime.runPromise(SubagentManager);
      const spawned = await runTool(
        runtime,
        manager.spawn("opencode", task("Reply with exactly: hello opencode")),
      );

      await deadline(runTool(runtime, manager.waitFor([spawned.id])), 90_000);
      const done = manager.view.get(spawned.id);
      assert.equal(done?.status, "done");
      assert.match(done?.finalText ?? "", /hello opencode/i);
      assert.equal(done?.meta.backend, "opencode");
      assert.ok(done?.meta.nativeSessionId);
    } finally {
      await runtime.dispose();
    }
  },
);

test(
  "Opencode backend interrupt settles a live manager run",
  { timeout: 60_000 },
  async (t) => {
    if (!(await opencodeAvailable())) {
      t.skip("opencode executable is unavailable");
      return;
    }

    const runtime = createSubagentRuntime();
    try {
      const manager = await runtime.runPromise(SubagentManager);
      const spawned = await runTool(
        runtime,
        manager.spawn(
          "opencode",
          task("Run `sleep 60`, then reply with the word finished."),
        ),
      );

      await new Promise((resolve) => setTimeout(resolve, 5_000));
      const result = await deadline(
        runTool(runtime, manager.cancel([spawned.id])),
        15_000,
      );
      assert.equal(result[0]?.cancelled, true);
      assert.equal(manager.view.get(spawned.id)?.status, "error");
      assert.equal(manager.view.get(spawned.id)?.errorText, "Run was aborted");
    } finally {
      await runtime.dispose();
    }
  },
);

test(
  "Opencode backend continues the session on follow-up send",
  { timeout: 120_000 },
  async (t) => {
    if (!(await opencodeAvailable())) {
      t.skip("opencode executable is unavailable");
      return;
    }

    const runtime = createSubagentRuntime();
    try {
      const manager = await runtime.runPromise(SubagentManager);
      const spawned = await runTool(
        runtime,
        manager.spawn("opencode", task("Reply with exactly: first answer")),
      );
      await deadline(runTool(runtime, manager.waitFor([spawned.id])), 90_000);
      const first = manager.view.get(spawned.id);
      assert.equal(first?.status, "done");
      assert.match(first?.finalText ?? "", /first answer/i);
      assert.ok(first?.meta.nativeSessionId);

      await runTool(
        runtime,
        manager.send(spawned.id, "Reply with exactly: second answer"),
      );
      // The fresh run flips the status back to running...
      const flipDeadline = Date.now() + 10_000;
      while (
        manager.view.get(spawned.id)?.status !== "running" &&
        Date.now() < flipDeadline
      ) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      await deadline(runTool(runtime, manager.waitFor([spawned.id])), 90_000);
      const second = manager.view.get(spawned.id);
      assert.equal(second?.status, "done");
      assert.match(second?.finalText ?? "", /second answer/i);
      assert.equal(second?.meta.nativeSessionId, first?.meta.nativeSessionId);
      assert.equal(second?.turns, 2);
    } finally {
      await runtime.dispose();
    }
  },
);
