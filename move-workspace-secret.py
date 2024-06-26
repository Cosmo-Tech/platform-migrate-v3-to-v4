#!/usr/bin/env python3

import argparse
import base64
import kubernetes
import logging
import pathlib
import sys
from azure.identity import DefaultAzureCredential
from cosmotech_api import ApiClient, Configuration
from cosmotech_api.api.workspace_api import WorkspaceApi, WorkspaceSecret


logger = logging.getLogger(pathlib.Path(__file__).stem)


if __name__ == "__main__":
  optionParser = argparse.ArgumentParser(description="""migrate a workspace secret
""", formatter_class=argparse.RawDescriptionHelpFormatter)
  optionParser.add_argument("-d", "--debug",
                            help="enable debug log level",
                            action="store_true")
  optionParser.add_argument("-c", "--source-k8s-context",
                            help="Kubernetes source cluster context name, as specified in your local .kube/config",
                            required=True)
  optionParser.add_argument("-n", "--source-k8s-namespace",
                            help="Kubernetes namespace of the source tenant",
                            required=True)
  optionParser.add_argument("-u", "--target-api-url",
                            help="API URL where to write data",
                            required=True)
  optionParser.add_argument("-s", "--target-api-scope",
                            help="API URL scope, defaults to the API URL",
                            default=None)
  optionParser.add_argument("workspace_ref",
                            help="workspace ref to migrate in the form of 'o-###/w-###'")

  args = optionParser.parse_args()
  logging.basicConfig(level=logging.WARNING)
  logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
  targetApiScope = args.target_api_scope or args.target_api_url

  ref_parts = args.workspace_ref.split('/')
  if len(ref_parts) != 2:
    logger.error(f"Invalid workspace ref {args.workspace_ref}")
    sys.exit(1)
  (organizationId, workspaceId) = ref_parts

  logger.info(f"Migrating {args.workspace_ref}")

  # Setup target Cosmo API client
  apiConfig = Configuration(host=args.target_api_url,
                            access_token=DefaultAzureCredential().get_token(targetApiScope).token)
  apiClient = ApiClient(apiConfig)
  workspaceApi = WorkspaceApi(apiClient)

  # Setup k8s client
  logger.debug(f"Using k8s source cluster {args.source_k8s_context} and namespace {args.source_k8s_namespace}")
  kubernetesApi = kubernetes.config.new_client_from_config(context=args.source_k8s_context)
  kubernetesClient = kubernetes.client.CoreV1Api(api_client=kubernetesApi)

  # Read content frow old secret
  workspace = workspaceApi.find_workspace_by_id(organization_id=organizationId,
                                                workspace_id=workspaceId)
  workspaceKey = workspace.key
  oldSecretName = f"{organizationId}-{workspaceKey}".lower()

  namespacedSecrets = kubernetesClient.list_namespaced_secret(args.source_k8s_namespace).items
  workspaceSecrets = list(filter(lambda secret: secret.metadata.name == oldSecretName, namespacedSecrets))

  if len(workspaceSecrets) == 0:
    logger.info("No secret to migrate")
    sys.exit(0)
  logger.debug(f"Found source secret {oldSecretName}")
  secretContent = base64.b64decode(base64.b64decode(workspaceSecrets[0].data['eventHubAccessKey'])).decode("utf-8")

  # Create new secret
  logger.debug("Creating new secret")
  workspaceApi.create_secret(organization_id=organizationId,
                             workspace_id=workspaceId,
                             workspace_secret=WorkspaceSecret(dedicatedEventHubKey=secretContent))

  # Delete old secret
  logger.debug("Removing old secret")
  kubernetesClient.delete_namespaced_secret(name=oldSecretName,
                                            namespace=args.source_k8s_namespace)

  logger.info("Secret migrated successfully")
