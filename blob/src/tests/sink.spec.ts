import * as common from "@data-heaving/common";
import * as storage from "@azure/storage-blob";
import * as spec from "../sink";
import * as abi from "../tests-setup/interface";
import { ExecutionContext } from "ava";

abi.thisTest(
  "Verify that blob storing works as intended in simple usecase",
  async (t) => {
    const { sinkFactory, getBlobSDKClient } = createSinkFactory(t);
    const { storing, promise } = sinkFactory(
      "Context",
      recreateSignalNotSupported,
    );
    const data = Buffer.from("This is test");
    storing.processor(data, controlFlowUsageNotSupported);
    storing.end();

    await promise!;

    // Wait until metadata about uploaded blob syncs
    await common.sleep(1000);

    t.deepEqual(await getBlobSDKClient(0).downloadToBuffer(), data);
  },
);

abi.thisTest("Verify that error from blob service propagates", async (t) => {
  const sink = spec.toAzureBlobStorage({
    getBlobID: () => BLOB_ID,
    blobClientFactory: () => ({
      client: new storage.BlockBlobClient(
        `${t.context.blobInfo.containerURL.replace(
          t.context.blobInfo.containerName,
          "non-existing-blob",
        )}/${BLOB_ID}.txt`,
        t.context.blobInfo.credential,
      ),
    }),
  })()("Context", recreateSignalNotSupported);
  sink.storing.processor(Buffer.from("Some data"));
  sink.storing.end();

  await t.throwsAsync(() => sink.promise!, {
    instanceOf: storage.RestError,
  });
});

abi.thisTest("Verify that splitting into multiple blobs works", async (t) => {
  const { sinkFactory, getBlobSDKClient } = createSinkFactory(t, 1 / 1024);
  let sink: ReturnType<typeof sinkFactory> | undefined = undefined;
  const promises: Array<Promise<unknown>> = [];
  const recreateSink = () => {
    if (sink) {
      sink.storing.end();
      promises.push(sink.promise!);
      sink = undefined;
    }
  };
  const getCurrentSink = () => {
    if (!sink) {
      sink = sinkFactory("Context", recreateSink);
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

  // Wait until metadata about uploaded blob syncs
  await common.sleep(1000);

  t.deepEqual(await getBlobSDKClient(0).downloadToBuffer(), data1);
  t.deepEqual(await getBlobSDKClient(1).downloadToBuffer(), data2);
});

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
  const sinkOptions: spec.AzureBlobStoringOptions<unknown> = {
    getBlobID: () => BLOB_ID,
    blobClientFactory,
  };
  return {
    sinkFactory: spec.toAzureBlobStorage(sinkOptions)(),
    getBlobSDKClient,
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
