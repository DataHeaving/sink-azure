import * as storage from "@azure/storage-blob";
import * as identity from "@azure/core-auth";
import test, { TestInterface } from "ava";

export const thisTest = test as TestInterface<BlobSinkTestContext>;

export interface TargetBlobInfo {
  containerName: string;
  containerURL: string;
  credential:
    | storage.StorageSharedKeyCredential
    | storage.AnonymousCredential
    | identity.TokenCredential;
}
export interface BlobSinkTestContext {
  blobInfo: TargetBlobInfo;
  containerID: string;
}
