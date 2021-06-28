import * as storage from "@azure/storage-blob";
import { env } from "process";
import * as common from "@data-heaving/common";
import * as testSupport from "@data-heaving/common-test-support";
import * as abi from "./interface";

let port = 10000;

abi.thisTest.beforeEach("Start Azurite Container", async (t) => {
  const containerName = "test-container";
  const exposedPort = ++port; // We have to do this when GitHub action is running the tests via "real", non-Dockerized npm.
  const {
    containerID,
    containerHostName,
    checkIsReady,
  } = await testSupport.startContainerAsync({
    image: "mcr.microsoft.com/azure-storage/azurite:3.13.1",
    containerPorts: [
      {
        containerPort: port,
        exposedPort,
      },
    ],
    containerEnvironment: {},
    networkName: env.SQL_SERVER_DOCKER_NW,
  });
  t.context.containerID = containerID;
  const storageAccountName = "devstoreaccount1";
  const containerURL = `http://${containerHostName}:${exposedPort}/${storageAccountName}/${containerName}`;
  const credential = new storage.StorageSharedKeyCredential(
    "devstoreaccount1",
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
  ); // See https://github.com/Azure/Azurite for default credentials
  t.context.blobInfo = {
    containerName,
    containerURL,
    credential,
  };

  while (!(await checkIsReady())) {
    await common.sleep(1000);
  }

  // Create container to hold blobs
  await new storage.ContainerClient(containerURL, credential).create({
    access: "container",
  });
});

abi.thisTest.afterEach.always("Shut down Azurite Container", async (t) => {
  await testSupport.stopContainerAsync(t.context.containerID);
});
