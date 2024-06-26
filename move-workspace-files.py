#!/usr/bin/env python3

import argparse
import logging
import os
import pathlib
import sys
import tempfile
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from cosmotech_api import ApiClient, Configuration
from cosmotech_api.api.organization_api import OrganizationApi
from cosmotech_api.api.workspace_api import WorkspaceApi


logger = logging.getLogger(pathlib.Path(__file__).stem)


def copy_files(blobServiceClient: BlobServiceClient, workspaceApi: WorkspaceApi, organizationId: str, workspaceId: str):
  logger.info(f"Migrating {organizationId}/{workspaceId}")
  blobCount = 0

  containerName = organizationId.lower()
  # List matching containers and only try to migrate a perfact match
  for container in blobServiceClient.list_containers(containerName):
    if container.name == containerName:
      containerClient = blobServiceClient.get_container_client(containerName)

      workspacePrefix = workspaceId + "/"
      blobList = containerClient.list_blobs(name_starts_with=workspacePrefix)
      for blob in blobList:
        fileName = blob.name.removeprefix(workspacePrefix)
        logger.debug(f" - {fileName}")

        with tempfile.NamedTemporaryFile("wb") as blobData:
          containerClient.download_blob(blob.name).readinto(blobData)
          blobData.flush()
          workspaceApi.upload_workspace_file(organization_id=organizationId,
                                             workspace_id=workspaceId,
                                             destination=fileName,
                                             overwrite=False,
                                             file=blobData.name)

        blobCount += 1

  logger.info(f" -> Done {blobCount} file(s)")


def list_workspace_refs(organizationApi: OrganizationApi, workspaceApi: WorkspaceApi, input_refs: list[str]):
  workspace_refs = set()
  if not input_refs:
    input_refs = [organization.id for organization in organizationApi.find_all_organizations()]

  for ref in input_refs:
    ids = ref.split('/')
    idCount = len(ids)
    if idCount == 1:
      organizationId = ids[0]
      workspaces = workspaceApi.find_all_workspaces(organization_id=organizationId)
      for ws in workspaces:
        workspace_refs.add((organizationId, ws.id))
    elif idCount == 2:
      workspace_refs.add(tuple(ids))
    else:
      logger.error(f"Invalid workspace ref '{ref}'")
      sys.exit(1)

  logger.info(f"Migrating {len(workspace_refs)} workspace(s)")
  return sorted(workspace_refs)


if __name__ == "__main__":
  optionParser = argparse.ArgumentParser(description="""migrate files of one or more workspaces

workspace references can have the following pattern:
 <no refs>: pass nothing to migrate all workspaces of all organizations
 'o-###': to migrate all workspaces of a specific organization
 'o-###/w-###': to migrate a specific workspace

required environment variables:
 AZURE_STORAGE_CONNECTION_STRING: connection string to the source azure storage container
""", formatter_class=argparse.RawDescriptionHelpFormatter)
  optionParser.add_argument("-d", "--debug",
                            help="enable debug log level",
                            action="store_true")
  optionParser.add_argument("-u", "--target-api-url",
                            help="API URL where to write data",
                            required=True)
  optionParser.add_argument("-s", "--target-api-scope",
                            help="API URL scope, defaults to the API URL",
                            default=None)
  optionParser.add_argument("workspace_refs",
                            help="workspace refs to migrate, combination of multiple workspace references or nothing",
                            nargs="*")

  args = optionParser.parse_args()
  logging.basicConfig(level=logging.WARNING)
  logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
  targetApiScope = args.target_api_scope or args.target_api_url

  # Setup target Cosmo API client
  apiConfig = Configuration(host=args.target_api_url,
                            access_token=DefaultAzureCredential().get_token(targetApiScope).token)
  apiClient = ApiClient(apiConfig)
  organizationApi = OrganizationApi(apiClient)
  workspaceApi = WorkspaceApi(apiClient)

  # Expand incomplete workspace references
  workspace_refs = list_workspace_refs(organizationApi=organizationApi,
                                       workspaceApi=workspaceApi,
                                       input_refs=args.workspace_refs)

  # Setup source azure storage client
  storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
  if storage_connection_string == None:
    logger.error("Missing AZURE_STORAGE_CONNECTION_STRING environment variable for source storage location")
    sys.exit(1)
  blobServiceClient = BlobServiceClient.from_connection_string(storage_connection_string)

  # Migrate files
  for organizationId, workspaceId in workspace_refs:
    copy_files(blobServiceClient=blobServiceClient,
              workspaceApi=workspaceApi,
              organizationId=organizationId,
              workspaceId=workspaceId)
