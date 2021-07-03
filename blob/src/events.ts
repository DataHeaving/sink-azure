import * as common from "@data-heaving/common";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualBlobWriteEvents<TContext> {
  uploadStart: {
    context: TContext;
    blobPath: string;
  };
  uploadProgress: VirtualBlobWriteEvents<TContext>["uploadStart"] & {
    bytesUploaded: number;
  };
  uploadEnd: VirtualBlobWriteEvents<TContext>["uploadProgress"] & {
    error?: unknown;
  };
}

export type VirtualBlobWriteEventEmitter<TArg> = common.EventEmitter<
  VirtualBlobWriteEvents<TArg>
>;

export const createEventEmitterBuilder = <TContext>() =>
  new common.EventEmitterBuilder<VirtualBlobWriteEvents<TContext>>();

export const consoleLoggingEventEmitterBuilder = <TContext>(
  getContextDescription: (arg: TContext) => string,
  logMessagePrefix?: Parameters<typeof common.createConsoleLogger>[0],
  builder?: common.EventEmitterBuilder<VirtualBlobWriteEvents<TContext>>,
  consoleAbstraction?: common.ConsoleAbstraction,
  printProgress?: boolean,
) => {
  if (!builder) {
    builder = createEventEmitterBuilder();
  }

  const logger = common.createConsoleLogger(
    logMessagePrefix,
    consoleAbstraction,
  );

  builder.addEventListener("uploadStart", (arg) =>
    logger(
      `Initiating upload of ${getContextDescription(arg.context)} to ${
        arg.blobPath
      }.`,
    ),
  );
  if (printProgress === true) {
    builder.addEventListener("uploadProgress", (arg) =>
      logger(
        `For ${getContextDescription(arg.context)} and path ${
          arg.blobPath
        } uploaded ${arg.bytesUploaded} bytes.`,
      ),
    );
  }
  builder.addEventListener("uploadEnd", (arg) =>
    logger(
      `Upload to ${arg.blobPath} complete, size ${
        arg.bytesUploaded
      }, completed ${
        "error" in arg ? `with an error ${arg.error}` : "successfully"
      }.`,
      "error" in arg,
    ),
  );

  return builder;
};
