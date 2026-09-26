/**
 * Opencode backend — real implementation over `opencode run --format json`.
 *
 * Each run is one short-lived `opencode run` child process whose NDJSON
 * events (`text`, `reasoning`, `tool_use`, `step_start`, `step_finish`,
 * `error`) are translated into normalized SubagentEvents. The prompt travels
 * over stdin (avoiding shell quoting entirely); `--auto` gives the headless
 * child the same autonomous posture as the other CLI backends, and
 * `--session` continues the same opencode session for follow-up turns.
 * send() queues a follow-up while busy (no true mid-run steering, like the
 * codex backend), and interrupt terminates the active child with a local
 * deadline so a wedged process can never leave the manager stuck.
 */

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import type { Cause, Scope } from "effect";
import { Effect, Queue, Stream } from "effect";
import type { SubagentBackend, SubagentSession } from "../backend.ts";
import type {
  ReasoningEffort,
  RunOutcome,
  SpawnTask,
  SubagentEvent,
  SubagentMeta,
  TranscriptPart,
} from "../domain.ts";
import { SendError, SpawnError } from "../domain.ts";

const INTERRUPT_FALLBACK_MS = 1_500;
const FORCE_KILL_AFTER_MS = 2_000;
const PREVIEW_MAX_LENGTH = 1_024;
/** A protocol line larger than this without a newline means a broken peer. */
const STDOUT_BUFFER_MAX_BYTES = 4 * 1024 * 1024;

type JsonRecord = Record<string, unknown>;

// --- Binary + JSON helpers ---------------------------------------------------

let cachedOpencodeBinary: string | null | undefined;

function executable(file: string) {
  try {
    fs.accessSync(file, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/** Resolve once on first use; availability checks after that are allocation-only. */
function resolveOpencodeBinary() {
  if (cachedOpencodeBinary !== undefined)
    return cachedOpencodeBinary ?? undefined;
  const names =
    process.platform === "win32"
      ? ["opencode.exe", "opencode.cmd", "opencode"]
      : ["opencode"];
  for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
    if (!directory) continue;
    for (const name of names) {
      const candidate = path.join(directory, name);
      if (executable(candidate)) {
        cachedOpencodeBinary = candidate;
        return candidate;
      }
    }
  }
  cachedOpencodeBinary = null;
  return undefined;
}

function record(value: unknown): JsonRecord | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonRecord)
    : undefined;
}

function stringValue(value: unknown) {
  return typeof value === "string" ? value : undefined;
}

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}

function safeJson(value: unknown) {
  try {
    const text = JSON.stringify(value);
    return text === undefined ? undefined : text.slice(0, PREVIEW_MAX_LENGTH);
  } catch {
    return undefined;
  }
}

function firstLine(value: unknown) {
  if (typeof value !== "string") return undefined;
  const line = value.split("\n").find((candidate) => candidate.trim());
  return line?.trim().slice(0, PREVIEW_MAX_LENGTH);
}

function boundedError(error: unknown) {
  return (error instanceof Error ? error.message : String(error)).slice(
    0,
    4096,
  );
}

function errorMessage(value: unknown) {
  if (typeof value === "string") return value;
  const top = record(value);
  const message = stringValue(top?.message);
  if (message) return message;
  const nested = stringValue(record(top?.data)?.message);
  if (nested) return nested;
  return safeJson(value) ?? "opencode run failed";
}

/** opencode --variant is provider-specific; pass the shared scale through and
 * let the server resolve it. "off" has no equivalent, so it means default. */
function preferredOpencodeVariant(effort: ReasoningEffort | undefined) {
  switch (effort) {
    case undefined:
    case "off":
      return undefined;
    case "minimal":
      return "minimal";
    case "low":
      return "low";
    case "medium":
      return "medium";
    case "high":
      return "high";
    case "xhigh":
      return "xhigh";
    case "max":
      return "max";
  }
}

function outputPreview(value: unknown) {
  if (typeof value === "string") return firstLine(value);
  return safeJson(value);
}

// --- The session -------------------------------------------------------------

const makeOpencodeSession = (
  task: SpawnTask,
): Effect.Effect<SubagentSession, SpawnError, Scope.Scope> =>
  Effect.gen(function* () {
    const binary = resolveOpencodeBinary();
    if (!binary) {
      return yield* new SpawnError({
        message: "opencode executable was not found on PATH.",
      });
    }

    const events = yield* Queue.make<SubagentEvent, Cause.Done>();
    const emit = (event: SubagentEvent) => {
      Queue.offerUnsafe(events, event);
    };

    const variant = preferredOpencodeVariant(task.reasoningEffort);

    const state = {
      closed: false,
      closing: false,
      activeRun: false,
      interruptRequested: false,
      runSerial: 0,
      sessionId: undefined as string | undefined,
      runError: undefined as string | undefined,
      /** Last assistant text; `opencode run --format json` emits finished
       * parts only, so there are no text deltas to stream. */
      currentText: "",
      pendingPrompts: [] as string[],
      child: undefined as ChildProcessWithoutNullStreams | undefined,
      childExited: true,
      stderr: "",
      meta: {
        backend: "opencode",
        modelLabel: task.model,
      } satisfies SubagentMeta as SubagentMeta,
      interruptTimer: undefined as ReturnType<typeof setTimeout> | undefined,
    };

    const queuedView = () =>
      state.pendingPrompts.map((text) => ({
        text,
        kind: "follow-up" as const,
      }));

    const startNextQueued = () => {
      if (state.closed || state.activeRun) return;
      const next = state.pendingPrompts.shift();
      if (next === undefined) return;
      emit({ _tag: "QueueChanged", queued: queuedView() });
      startRun(next);
    };

    const settleRun = (outcome: RunOutcome, serial = state.runSerial) => {
      if (!state.activeRun || serial !== state.runSerial) return;
      if (state.interruptTimer) clearTimeout(state.interruptTimer);
      state.interruptTimer = undefined;
      state.activeRun = false;
      state.interruptRequested = false;
      state.child = undefined;
      emit({ _tag: "RunSettled", outcome });
      queueMicrotask(startNextQueued);
    };

    const partialText = () => state.currentText || undefined;

    function handleToolPart(part: JsonRecord) {
      const name = stringValue(part.tool) ?? "tool";
      const toolId =
        stringValue(part.callID) ??
        stringValue(part.id) ??
        `tool-${state.runSerial}`;
      const toolState = record(part.state);
      const status = stringValue(toolState?.status);
      const argsPreview =
        toolState && "input" in toolState
          ? safeJson(toolState.input)
          : undefined;
      const output = outputPreview(toolState?.output ?? toolState?.error);
      const toolPart: TranscriptPart = {
        type: "toolCall",
        toolId,
        name,
        argsPreview,
      };
      emit({ _tag: "AssistantMessage", parts: [toolPart] });
      emit({ _tag: "ToolStart", toolId, name, argsPreview });
      emit({
        _tag: "ToolEnd",
        toolId,
        name,
        isError: status === "error",
        outputPreview: output,
      });
    }

    function handleLine(line: string) {
      if (!line.trim()) return;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        emit({
          _tag: "BackendError",
          message: `Invalid opencode protocol line: ${line.slice(0, 512)}`,
        });
        return;
      }
      const message = record(parsed);
      if (!message) return;
      const type = stringValue(message.type);
      if (!type) return;

      const sessionID = stringValue(message.sessionID);
      if (sessionID && !state.sessionId) {
        state.sessionId = sessionID;
        state.meta = { ...state.meta, nativeSessionId: sessionID };
        emit({ _tag: "MetaChanged", meta: state.meta });
      }

      switch (type) {
        case "text": {
          const part = record(message.part);
          const text = stringValue(part?.text);
          if (text) {
            emit({ _tag: "AssistantMessage", parts: [{ type: "text", text }] });
            state.currentText = text;
          }
          break;
        }
        case "reasoning": {
          const part = record(message.part);
          const thinking = stringValue(part?.text);
          if (thinking) {
            emit({
              _tag: "AssistantMessage",
              parts: [{ type: "thinking", text: thinking }],
            });
          }
          break;
        }
        case "tool_use": {
          const part = record(message.part);
          if (part) handleToolPart(part);
          break;
        }
        case "step_start":
          break;
        case "step_finish": {
          const tokens = numberValue(
            record(record(message.part)?.tokens)?.total,
          );
          if (tokens !== undefined) emit({ _tag: "UsageChanged", tokens });
          break;
        }
        case "error": {
          const failure = boundedError(errorMessage(message.error));
          state.runError = failure;
          emit({ _tag: "BackendError", message: failure });
          break;
        }
        default:
          break;
      }
    }

    function watchChild(child: ChildProcessWithoutNullStreams, serial: number) {
      let stdoutBuffer = "";
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        stdoutBuffer += chunk;
        while (true) {
          const newline = stdoutBuffer.indexOf("\n");
          if (newline < 0) break;
          const line = stdoutBuffer.slice(0, newline).replace(/\r$/, "");
          stdoutBuffer = stdoutBuffer.slice(newline + 1);
          handleLine(line);
        }
        if (stdoutBuffer.length > STDOUT_BUFFER_MAX_BYTES) {
          // A frame this large with no newline is protocol corruption, and
          // an unbounded buffer is a memory leak. Session-fatal: the exit
          // handler settles any active run.
          stdoutBuffer = "";
          void terminateChild(child, () => state.childExited);
        }
      });
      child.stderr.setEncoding("utf8");
      child.stderr.on("data", (chunk: string) => {
        state.stderr = `${state.stderr}${chunk}`.slice(-4096);
      });
      child.once("error", (error) => {
        if (serial !== state.runSerial || !state.activeRun) return;
        settleRun(
          {
            _tag: "Failed",
            errorText: boundedError(`opencode run failed: ${error}`),
            partialText: partialText(),
          },
          serial,
        );
      });
      child.once("exit", (code, signal) => {
        state.childExited = true;
        if (serial !== state.runSerial || !state.activeRun) return;
        if (state.interruptRequested || signal !== null) {
          settleRun(
            { _tag: "Interrupted", partialText: partialText() },
            serial,
          );
          return;
        }
        if (code === 0 && !state.runError) {
          settleRun(
            { _tag: "Completed", finalText: state.currentText },
            serial,
          );
          return;
        }
        const suffix = firstLine(state.stderr);
        settleRun(
          {
            _tag: "Failed",
            errorText: boundedError(
              state.runError ??
                `opencode run exited (code ${code ?? "unknown"})${suffix ? `: ${suffix}` : ""}`,
            ),
            partialText: partialText(),
          },
          serial,
        );
      });
    }

    function startRun(text: string) {
      if (state.closed || state.activeRun) return;
      const exe = binary;
      if (!exe) {
        // Binary vanished mid-session; fail loudly rather than hanging.
        emit({ _tag: "UserMessage", text });
        emit({ _tag: "RunStarted" });
        settleRun({
          _tag: "Failed",
          errorText: "opencode executable was not found on PATH.",
        });
        return;
      }
      const serial = ++state.runSerial;
      state.activeRun = true;
      state.interruptRequested = false;
      state.runError = undefined;
      state.currentText = "";
      state.stderr = "";
      emit({ _tag: "UserMessage", text });
      emit({ _tag: "RunStarted" });

      const args = [
        "run",
        "--format",
        "json",
        "--dir",
        task.cwd,
        // Headless children cannot answer permission prompts. The caller
        // already chose to launch an autonomous subagent, so auto-approve
        // permissions that are not explicitly denied.
        "--auto",
        ...(task.title ? ["--title", task.title] : []),
        ...(task.model ? ["--model", task.model] : []),
        ...(variant ? ["--variant", variant] : []),
        // Continue the same opencode session for follow-up turns.
        ...(state.sessionId ? ["--session", state.sessionId] : []),
      ];
      let child: ChildProcessWithoutNullStreams;
      try {
        child = spawn(exe, args, {
          cwd: task.cwd,
          env: process.env,
          stdio: ["pipe", "pipe", "pipe"],
          windowsHide: true,
          // Own process group on POSIX so teardown can signal the whole
          // tree: a wedged run must not orphan a still-running tool child.
          detached: process.platform !== "win32",
        });
      } catch (error) {
        settleRun(
          {
            _tag: "Failed",
            errorText: boundedError(error),
            partialText: partialText(),
          },
          serial,
        );
        return;
      }
      state.child = child;
      state.childExited = false;
      watchChild(child, serial);
      // The prompt travels over stdin, so no shell quoting is involved and
      // newlines survive verbatim. `run` reads piped stdin once at startup.
      try {
        child.stdin.write(text);
        child.stdin.end();
      } catch (error) {
        if (serial === state.runSerial && state.activeRun) {
          settleRun(
            {
              _tag: "Failed",
              errorText: boundedError(error),
              partialText: partialText(),
            },
            serial,
          );
        }
      }
    }

    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        if (state.closing) return;
        state.closing = true;
        if (state.interruptTimer) clearTimeout(state.interruptTimer);
        // Settle before marking closed so the run gets the correct
        // "Interrupted" outcome instead of the pump's generic fallback.
        if (state.activeRun) {
          settleRun({ _tag: "Interrupted", partialText: partialText() });
        }
        state.closed = true;
        const child = state.child;
        state.child = undefined;
        if (child && !state.childExited) {
          await terminateChild(child, () => state.childExited);
        }
        Queue.endUnsafe(events);
      }),
    );

    emit({ _tag: "MetaChanged", meta: state.meta });
    startRun(task.prompt);

    return {
      meta: Effect.sync(() => state.meta),
      events: Stream.fromQueue(events),
      send: (text) =>
        Effect.suspend((): Effect.Effect<void, SendError> => {
          if (state.closed) {
            return new SendError({ message: "Subagent session is closed." });
          }
          if (state.activeRun) {
            state.pendingPrompts.push(text);
            emit({ _tag: "QueueChanged", queued: queuedView() });
            return Effect.void;
          }
          return Effect.sync(() => startRun(text));
        }),
      interrupt: Effect.promise(async () => {
        if (state.closed || !state.activeRun) return;
        const serial = state.runSerial;
        state.pendingPrompts = [];
        emit({ _tag: "QueueChanged", queued: [] });
        state.interruptRequested = true;
        const child = state.child;
        if (child && !state.childExited) {
          void terminateChild(child, () => state.childExited);
        }
        if (state.interruptTimer) clearTimeout(state.interruptTimer);
        state.interruptTimer = setTimeout(() => {
          if (state.activeRun && serial === state.runSerial) {
            // The process ignored SIGTERM; it cannot be trusted with further
            // work — the escalated SIGKILL in terminateChild covers it, and
            // the run settles here so cancel never hangs.
            settleRun(
              { _tag: "Interrupted", partialText: partialText() },
              serial,
            );
            const pending = state.child;
            state.child = undefined;
            if (pending && !state.childExited) {
              try {
                killTree(pending, "SIGKILL");
              } catch {
                // Process may already be gone.
              }
            }
          }
        }, INTERRUPT_FALLBACK_MS);
      }),
    } satisfies SubagentSession;
  });

/** Signal the whole process group on POSIX so tool descendants (shell
 * commands the run spawned) die with it; a wedged process must not orphan a
 * still-running command in the workspace. */
function killTree(
  child: ChildProcessWithoutNullStreams,
  signal: NodeJS.Signals,
) {
  if (process.platform === "win32" && child.pid) {
    try {
      const killer = spawn(
        "taskkill",
        [
          "/pid",
          String(child.pid),
          "/T",
          ...(signal === "SIGKILL" ? ["/F"] : []),
        ],
        { stdio: "ignore", windowsHide: true },
      );
      const killDirect = () => {
        try {
          child.kill(signal);
        } catch {
          // Process may already be gone.
        }
      };
      killer.once("error", killDirect);
      killer.once("exit", (code) => {
        if (code !== 0) killDirect();
      });
      killer.unref();
      return;
    } catch {
      // Fall through to a direct signal when taskkill cannot be launched.
    }
  }
  if (process.platform !== "win32" && child.pid) {
    try {
      process.kill(-child.pid, signal);
      return;
    } catch {
      // Group may already be gone; fall through to the direct signal.
    }
  }
  try {
    child.kill(signal);
  } catch {
    // Process may already be gone.
  }
}

/** SIGTERM is normally enough; the second deadline covers a wedged process. */
function terminateChild(
  child: ChildProcessWithoutNullStreams,
  exited: () => boolean,
) {
  if (exited()) return Promise.resolve();
  return new Promise<void>((resolve) => {
    let done = false;
    let forceTimer: ReturnType<typeof setTimeout> | undefined;
    let lastTimer: ReturnType<typeof setTimeout> | undefined;
    const finish = () => {
      if (done) return;
      done = true;
      if (forceTimer) clearTimeout(forceTimer);
      if (lastTimer) clearTimeout(lastTimer);
      resolve();
    };
    child.once("exit", finish);
    killTree(child, "SIGTERM");
    forceTimer = setTimeout(() => {
      if (!exited()) killTree(child, "SIGKILL");
    }, FORCE_KILL_AFTER_MS);
    lastTimer = setTimeout(finish, FORCE_KILL_AFTER_MS + 500);
  });
}

export const opencodeBackend: SubagentBackend = {
  name: "opencode",
  capabilities: {
    steering: false,
    modelSelection: true,
    reasoningEffort: true,
  },
  available: Effect.sync(() => resolveOpencodeBinary() !== undefined),
  spawn: makeOpencodeSession,
};
