import * as utils from "@data-heaving/common";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualBlobWriteEvents<TArg> {
  uploadStart: {
    creationArg: TArg;
    blobPath: string;
  };
  uploadProgress: VirtualBlobWriteEvents<TArg>["uploadStart"] & {
    bytesUploaded: number;
  };
  uploadEnd: VirtualBlobWriteEvents<TArg>["uploadProgress"] & {
    error?: Error;
  };
}

export type VirtualBlobWriteEventEmitter<TArg> = utils.EventEmitter<
  VirtualBlobWriteEvents<TArg>
>;

export const createEventEmitterBuilder = <TArg>() =>
  new utils.EventEmitterBuilder<VirtualBlobWriteEvents<TArg>>();

export const consoleLoggingEventEmitterBuilder = <TArg>(
  getArgString: (arg: TArg) => string,
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<VirtualBlobWriteEvents<TArg>>,
  printProgress?: boolean,
) => {
  if (!builder) {
    builder = createEventEmitterBuilder();
  }

  const logger = utils.createConsoleLogger(logMessagePrefix);

  builder.addEventListener("uploadStart", (arg) =>
    logger(
      `Initiating upload of ${getArgString(arg.creationArg)} to ${
        arg.blobPath
      }.`,
    ),
  );
  if (printProgress === true) {
    builder.addEventListener("uploadProgress", (arg) =>
      logger(
        `For ${getArgString(arg.creationArg)} and path ${
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
