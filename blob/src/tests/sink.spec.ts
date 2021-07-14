import * as common from "@data-heaving/common";
import * as storage from "@azure/storage-blob";
import * as abi from "../tests-setup/interface";
import { ExecutionContext } from "ava";
import * as spec from "../sink";
import * as events from "../events";

abi.thisTest.serial(
  "Verify that blob storing works as intended in simple usecase",
  async (t) => {
    const { sinkFactory, getBlobSDKClient, eventTracker } = createSinkFactory(
      t,
    );
    const context = "Context";
    const { storing, promise } = sinkFactory(
      context,
      recreateSignalNotSupported,
    );
    const data = Buffer.from("This is test");
    storing.processor(data, controlFlowUsageNotSupported);
    storing.end();

    await promise;

    const blobPath = getBlobSDKClient(0).url;
    t.deepEqual(eventTracker, {
      uploadStart: [
        {
          eventIndex: 0,
          eventArg: {
            context,
            blobPath,
          },
        },
      ],
      uploadProgress: [
        {
          eventIndex: 1,
          eventArg: {
            context,
            blobPath,
            bytesUploaded: data.byteLength,
          },
        },
      ],
      uploadEnd: [
        {
          eventIndex: 2,
          eventArg: {
            context,
            blobPath,
            bytesUploaded: data.byteLength,
          },
        },
      ],
    });
    t.deepEqual(await getBlobSDKClient(0).downloadToBuffer(), data);
  },
);

abi.thisTest.serial(
  "Verify that error from blob service propagates",
  async (t) => {
    const sink = spec.toAzureBlobStorage({
      getBlobID: () => BLOB_ID,
      blobClientFactory: () => ({
        client: new storage.BlockBlobClient(
          `${t.context.blobInfo.containerURL.replace(
            t.context.blobInfo.containerName,
            "non-existing-container",
          )}/${BLOB_ID}.txt`,
          t.context.blobInfo.credential,
        ),
      }),
    })()("Context", recreateSignalNotSupported);
    sink.storing.processor(Buffer.from("Some data"));
    sink.storing.end();

    await t.throwsAsync(async () => await sink.promise, {
      instanceOf: storage.RestError,
    });
  },
);

abi.thisTest.serial(
  "Verify that splitting into multiple blobs works",
  async (t) => {
    const { sinkFactory, getBlobSDKClient, eventTracker } = createSinkFactory(
      t,
      1 / 1024,
    );
    let sink: ReturnType<typeof sinkFactory> | undefined = undefined;
    const promises: Array<Promise<unknown> | undefined> = [];
    const recreateSink = () => {
      if (sink) {
        sink.storing.end();
        promises.push(sink.promise);
        sink = undefined;
      }
    };
    const context = "Context";
    const getCurrentSink = () => {
      if (!sink) {
        sink = sinkFactory(context, recreateSink);
      }
      return sink;
    };
    const data1 = Buffer.from("This is first data");
    getCurrentSink().storing.processor(data1, controlFlowUsageNotSupported);
    const data2 = Buffer.from("This is second data");
    getCurrentSink().storing.processor(data2, controlFlowUsageNotSupported);
    recreateSink();

    t.deepEqual(promises.length, 2);
    await Promise.all(promises);

    const firstClient = getBlobSDKClient(0);
    const secondClient = getBlobSDKClient(1);

    t.deepEqual(eventTracker, {
      uploadStart: [
        {
          eventIndex: 0,
          eventArg: {
            context,
            blobPath: firstClient.url,
          },
        },
        {
          eventIndex: 1,
          eventArg: {
            context,
            blobPath: secondClient.url,
          },
        },
      ],
      uploadProgress: [
        {
          eventIndex: eventTracker.uploadProgress[0].eventIndex, // The order is indetermenistic because of IO,
          eventArg: {
            context,
            blobPath: eventTracker.uploadProgress[0].eventArg.blobPath,
            bytesUploaded: (eventTracker.uploadProgress[0].eventArg.blobPath ===
            firstClient.url
              ? data1
              : data2
            ).byteLength,
          },
        },
        {
          eventIndex: eventTracker.uploadProgress[1].eventIndex, // The order is indetermenistic because of IO
          eventArg: {
            context,
            blobPath: eventTracker.uploadProgress[1].eventArg.blobPath,
            bytesUploaded: (eventTracker.uploadProgress[1].eventArg.blobPath ===
            firstClient.url
              ? data1
              : data2
            ).byteLength,
          },
        },
      ],
      uploadEnd: [
        {
          eventIndex: eventTracker.uploadEnd[0].eventIndex, // The order is indetermenistic because of IO
          eventArg: {
            context,
            blobPath: eventTracker.uploadEnd[0].eventArg.blobPath,
            bytesUploaded: (eventTracker.uploadEnd[0].eventArg.blobPath ===
            firstClient.url
              ? data1
              : data2
            ).byteLength,
          },
        },
        {
          eventIndex: eventTracker.uploadEnd[1].eventIndex, // The order is indetermenistic because of IO
          eventArg: {
            context,
            blobPath: eventTracker.uploadEnd[1].eventArg.blobPath,
            bytesUploaded: (eventTracker.uploadEnd[1].eventArg.blobPath ===
            firstClient.url
              ? data1
              : data2
            ).byteLength,
          },
        },
      ],
    });
    t.deepEqual(await firstClient.downloadToBuffer(), data1);
    t.deepEqual(await secondClient.downloadToBuffer(), data2);
  },
);

const createSinkFactory = (
  t: ExecutionContext<abi.BlobSinkTestContext>,
  maxSizeInKB?: number,
  blobClientFactory?: spec.AzureBlobStoringOptions<unknown>["blobClientFactory"],
) => {
  const getBlobSDKClient = (existingCount: number) =>
    new storage.BlockBlobClient(
      `${t.context.blobInfo.containerURL}/${BLOB_ID}-${existingCount}.txt`,
      t.context.blobInfo.credential,
    );
  if (!blobClientFactory) {
    blobClientFactory = (_: string, existingCount: number) => ({
      client: getBlobSDKClient(existingCount),
      maxSizeInKB,
    });
  }
  const { eventEmitter, eventTracker } = createEventEmitterAndRecorder();
  const sinkOptions: spec.AzureBlobStoringOptions<unknown> = {
    getBlobID: () => BLOB_ID,
    blobClientFactory,
    eventEmitter,
  };
  return {
    sinkFactory: spec.toAzureBlobStorage(sinkOptions)(),
    getBlobSDKClient,
    eventTracker,
  };
};

const controlFlowUsageNotSupported: common.ControlFlow = {
  pause: () => {
    throw new Error("Control flow should never be utilized by blob sink");
  },
  resume: () => {
    throw new Error("Control flow should never be utilized by blob sink");
  },
};

const recreateSignalNotSupported = () => {
  throw new Error("Recreate signal should not be called in this test");
};

const BLOB_ID = "test";

const createEventEmitterAndRecorder = () => {
  const eventBuilder = events.createEventEmitterBuilder<string>();
  let eventIndex = 0;
  const eventTracker: Record<
    keyof events.VirtualBlobWriteEvents<string>,
    Array<{
      eventIndex: number;
      eventArg: events.VirtualBlobWriteEvents<string>[keyof events.VirtualBlobWriteEvents<string>];
    }>
  > = {
    uploadStart: [],
    uploadProgress: [],
    uploadEnd: [],
  };
  for (const evtName of Object.keys(eventTracker)) {
    const eventName = evtName as keyof events.VirtualBlobWriteEvents<string>;
    eventBuilder.addEventListener(eventName, (eventArg) => {
      eventTracker[eventName].push({
        eventIndex,
        eventArg,
      });
      ++eventIndex;
    });
  }

  return {
    eventEmitter: eventBuilder.createEventEmitter(),
    eventTracker,
  };
};
