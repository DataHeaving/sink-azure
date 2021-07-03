import * as common from "@data-heaving/common";
import test, { ExecutionContext } from "ava";
import * as spec from "../events";

// We must use serial here
test.serial(
  "Test that console logging event emitter works as expected when printing progress",
  (t) => {
    performConsoleLoggingTest(t, true, undefined);
  },
);

test.serial(
  "Test that console logging event emitter works as expected when NOT printing progress",
  (t) => {
    performConsoleLoggingTest(t, false, spec.createEventEmitterBuilder()); // We are creating event builder only to get 100% code coverage from events.ts :)
  },
);

const performConsoleLoggingTest = (
  t: ExecutionContext,
  printProgress: boolean,
  builderToUse:
    | common.EventEmitterBuilder<spec.VirtualBlobWriteEvents<string>>
    | undefined,
) => {
  const logsAndErrors: Record<"logs" | "errors", Array<string>> = {
    logs: [],
    errors: [],
  };
  const eventEmitter = spec
    .consoleLoggingEventEmitterBuilder<string>(
      (ctx) => ctx,
      undefined,
      builderToUse,
      {
        log: (msg) => logsAndErrors.logs.push(msg),
        error: (msg) => logsAndErrors.errors.push(msg),
      },
      printProgress,
    )
    .createEventEmitter();
  const context = "Context";
  const blobPath = "dummy";
  const bytesUploaded = 100;

  eventEmitter.emit("uploadStart", {
    context,
    blobPath,
  });
  const uploadStartMessage = `Initiating upload of ${context} to ${blobPath}.`;
  t.deepEqual(logsAndErrors, {
    logs: [uploadStartMessage],
    errors: [],
  });

  eventEmitter.emit("uploadProgress", {
    context,
    blobPath,
    bytesUploaded,
  });
  const uploadProgressMessage = printProgress
    ? [`For ${context} and path ${blobPath} uploaded ${bytesUploaded} bytes.`]
    : [];
  t.deepEqual(logsAndErrors, {
    logs: [uploadStartMessage, ...uploadProgressMessage],
    errors: [],
  });

  const uploadEndMessage = `Upload to ${blobPath} complete, size ${bytesUploaded}, completed successfully.`;
  eventEmitter.emit("uploadEnd", {
    context,
    blobPath,
    bytesUploaded,
  });
  t.deepEqual(logsAndErrors, {
    logs: [uploadStartMessage, ...uploadProgressMessage, uploadEndMessage],
    errors: [],
  });

  const error = "Error";
  const uploadEndWithErrorMessage = `Upload to ${blobPath} complete, size ${bytesUploaded}, completed with an error ${error}.`;
  eventEmitter.emit("uploadEnd", {
    context,
    blobPath,
    bytesUploaded,
    error,
  });
  t.deepEqual(logsAndErrors, {
    logs: [uploadStartMessage, ...uploadProgressMessage, uploadEndMessage],
    errors: [uploadEndWithErrorMessage],
  });
};
