import * as blob from "@azure/storage-blob";
import * as stream from "stream";
import * as common from "@data-heaving/common";
import * as commonAzure from "@data-heaving/common-azure";
import * as events from "./events";

export interface AzureBlobStoringOptions<TArg> {
  getBlobID: (arg: TArg) => string; // Doesn't need to be URL
  blobClientFactory: (
    blobID: string,
    existingCount: number,
    arg: TArg,
  ) => {
    client: commonAzure.BlobClientOrInfo;
    maxSizeInKB?: number;
    blockSizeInKB?: number;
  };
  eventEmitter?: events.VirtualBlobWriteEventEmitter<TArg>;
}

export function toAzureBlobStorage<TArg>({
  getBlobID,
  blobClientFactory,
  eventEmitter,
}: AzureBlobStoringOptions<TArg>): () => common.DatumStoringFactory<
  TArg,
  Buffer,
  blob.BlobUploadCommonResponse
> {
  console.log("ONCE PER LIFETIME");// eslint-disable-line
  return () => {
    console.log("ONCE PER PIPELINE");// eslint-disable-line
    const existingCount: { [blobURL: string]: number } = {};
    return (arg, recreateSignal) => {
      console.log("MULTIPLE PER PIPELINE"); // eslint-disable-line
      const name = getBlobID(arg);
      existingCount[name] = name in existingCount ? existingCount[name] + 1 : 0;
      const clientInfo = blobClientFactory(name, existingCount[name], arg);
      const maxSize = (clientInfo.maxSizeInKB || 0) * 1024;
      const blockSize = (clientInfo.blockSizeInKB || 1024) * 1024;
      const client = clientInfo.client;
      const blockBlobClient =
        client instanceof blob.BlockBlobClient
          ? client
          : new blob.BlockBlobClient(client.url, client.credential);
      const eventArg: events.VirtualBlobWriteEvents<TArg>["uploadStart"] = {
        blobPath: blockBlobClient.url,
        creationArg: arg,
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
          const endArg: events.VirtualBlobWriteEvents<TArg>["uploadEnd"] = {
            ...eventArg,
            bytesUploaded,
          };
          if (error) {
            endArg.error = error as Error;
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
        promises: [promise],
      };
    };
  };
}
