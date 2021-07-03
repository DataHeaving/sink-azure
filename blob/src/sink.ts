import * as blob from "@azure/storage-blob";
import * as stream from "stream";
import * as common from "@data-heaving/common";
import * as commonAzure from "@data-heaving/common-azure";
import * as events from "./events";

export interface AzureBlobStoringOptions<TContext> {
  getBlobID: (context: TContext) => string; // Doesn't need to be URL
  blobClientFactory: (
    blobID: string,
    existingCount: number,
    context: TContext,
  ) => {
    client: commonAzure.BlobClientOrInfo;
    maxSizeInKB?: number;
    blockSizeInKB?: number;
  };
  eventEmitter?: events.VirtualBlobWriteEventEmitter<TContext>;
}

export function toAzureBlobStorage<TContext>({
  getBlobID,
  blobClientFactory,
  eventEmitter,
}: AzureBlobStoringOptions<TContext>): () => common.DatumStoringFactory<
  TContext,
  Buffer,
  blob.BlobUploadCommonResponse
> {
  return () => {
    // TODO make test that existingCount does not persist between invocations of same pipeline
    const existingCount: { [blobURL: string]: number } = {};
    return (context, recreateSignal) => {
      const name = getBlobID(context);
      existingCount[name] = name in existingCount ? existingCount[name] + 1 : 0;
      const clientInfo = blobClientFactory(name, existingCount[name], context);
      const maxSize = (clientInfo.maxSizeInKB || 0) * 1024;
      const blockSize = (clientInfo.blockSizeInKB || 1024) * 1024;
      const client = clientInfo.client;
      const blockBlobClient =
        client instanceof blob.BlockBlobClient
          ? client
          : new blob.BlockBlobClient(client.url, client.credential);
      const eventArg: events.VirtualBlobWriteEvents<TContext>["uploadStart"] = {
        blobPath: blockBlobClient.url,
        context,
      };
      eventEmitter?.emit("uploadStart", eventArg);
      const readable = new stream.PassThrough({
        readableHighWaterMark: blockSize,
      });
      const promise = (async () => {
        let error: unknown = undefined;
        let bytesUploaded = 0;
        try {
          return await blockBlobClient.uploadStream(
            readable,
            blockSize,
            undefined,
            {
              onProgress: ({ loadedBytes }) => {
                bytesUploaded = loadedBytes;
                eventEmitter?.emit("uploadProgress", {
                  ...eventArg,
                  bytesUploaded: loadedBytes,
                });
              },
            },
          );
        } catch (e) {
          error = e;
          throw e;
        } finally {
          const endArg: events.VirtualBlobWriteEvents<TContext>["uploadEnd"] = {
            ...eventArg,
            bytesUploaded,
          };
          if (error) {
            endArg.error = error;
          }
          eventEmitter?.emit("uploadEnd", endArg);
        }
      })();
      let currentSize = 0;
      let recreateSignalSent = false;
      return {
        storing: {
          processor: (buffer) => {
            readable.write(buffer);
            if (maxSize > 0) {
              currentSize += buffer.byteLength;
              if (currentSize > maxSize && !recreateSignalSent) {
                // Signal to recreate the transformers and sink by source pipeline (but don't send it too many times)
                recreateSignal();
                recreateSignalSent = true;
              }
            }
          },
          end: () => {
            readable.end();
          },
        },
        promise,
      };
    };
  };
}
