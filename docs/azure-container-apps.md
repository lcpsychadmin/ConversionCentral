# Deploying to Azure Container Apps

This guide outlines the steps to run ConversionCentral on Azure Container Apps (ACA) using Azure-managed databases. The local Docker Compose setup remains unchanged for development.

## 1. Prerequisites

- Azure CLI 2.53.0 or newer: `https://learn.microsoft.com/cli/azure/install-azure-cli`
- Logged in: `az login`
- Docker installed and authenticated (`docker login` if using Azure Container Registry).

Set a few shell variables for reuse (PowerShell example):

```powershell
$resourceGroup = "cc-rg"
$location = "eastus"
$acrName = "ccacr001"
$envName = "cc-env"
$backendImage = "$acrName.azurecr.io/conversioncentral-backend:latest"
$frontendImage = "$acrName.azurecr.io/conversioncentral-frontend:latest"
```

## 2. Provision shared resources

```powershell
az group create -n $resourceGroup -l $location
az acr create -n $acrName -g $resourceGroup --sku Basic
az acr login -n $acrName

$workspace = az monitor log-analytics workspace create `
  -g $resourceGroup -n "${envName}-law" --query id -o tsv
$key = az monitor log-analytics workspace get-shared-keys `
  -g $resourceGroup -n "${envName}-law" --query primarySharedKey -o tsv

az containerapp env create `
  -g $resourceGroup -n $envName `
  --logs-workspace-id $workspace --logs-workspace-key $key
```

## 3. Build and push container images

Run from the repo root:

```powershell
docker compose build backend frontend

docker tag conversioncentral-backend $backendImage
docker push $backendImage

docker tag conversioncentral-frontend $frontendImage
docker push $frontendImage
```

## 4. Configure databases

1. **PostgreSQL**: Create an Azure Database for PostgreSQL Flexible Server. Collect the SQLAlchemy URL (format `postgresql+psycopg://user:password@host:5432/dbname`).
2. **Databricks SQL warehouse**: Provision a SQL warehouse in your Databricks workspace (or reuse an existing one). Generate a personal access token (PAT) with permission to query the warehouse and note the following values:
  - Workspace host (for example `adb-123456789012345.7.azuredatabricks.net`)
  - HTTP path (for example `/sql/1.0/warehouses/<warehouse-id>`)
  - Optional catalog and schema defaults if you want the application to target a specific schema by default.
3. Apply the schema by running Alembic migrations during the backend startup (handled automatically by `docker/backend/start.sh`).

## 5. Deploy the backend container app

```powershell
$databaseUrl    = "postgresql+psycopg://..."            # PostgreSQL SQLAlchemy URL
$databricksHost = "adb-123456789012345.7.azuredatabricks.net"
$databricksPath = "/sql/1.0/warehouses/<warehouse-id>"
$databricksToken = "<databricks-personal-access-token>"

az containerapp create `
  -g $resourceGroup -n "cc-backend" `
  --environment $envName `
  --image $backendImage `
  --target-port 8000 `
  --ingress external `
  --registry-server "$acrName.azurecr.io" `
  --cpu 1 --memory 2Gi `
  --secrets `
      database-url=$databaseUrl `
      databricks-token=$databricksToken `
  --env-vars `
      DATABASE_URL=secretref:database-url `
      DATABRICKS_HOST=$databricksHost `
      DATABRICKS_HTTP_PATH=$databricksPath `
      DATABRICKS_TOKEN=secretref:databricks-token `
      APP_NAME="Conversion Central API" `
      FRONTEND_ORIGINS="https://cc-frontend.<region>.azurecontainerapps.io,https://localhost:3000"
```

Notes:
- Replace the sample `FRONTEND_ORIGINS` with the actual frontend hostname once it is known.
- If you configure optional catalog or schema defaults, set `DATABRICKS_CATALOG` / `DATABRICKS_SCHEMA` environment variables.

## 6. Deploy the frontend container app

Point the frontend to the backend’s HTTPS endpoint:

```powershell
$backendUrl = "https://cc-backend.<region>.azurecontainerapps.io/api"

az containerapp create `
  -g $resourceGroup -n "cc-frontend" `
  --environment $envName `
  --image $frontendImage `
  --target-port 80 `
  --ingress external `
  --registry-server "$acrName.azurecr.io" `
  --cpu 0.5 --memory 1Gi `
  --env-vars VITE_API_URL=$backendUrl
```

If the frontend needs environment variables beyond `VITE_API_URL`, set them here.

## 7. Updating deployments

- Rebuild and push new images, then run `az containerapp update -g <rg> -n <app> --image <new-image>`.
- For config-only changes, use `az containerapp update --set-env-vars ...` with the appropriate secret references.

## 8. Local development

No changes are required locally. Continue using `docker compose up` to start the Postgres-backed API and the frontend. Configure Databricks credentials through environment variables (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, etc.) when you want to test warehouse integrations from your local environment.

## 9. Cleanup

```powershell
az group delete -n $resourceGroup --yes
```

This removes all Azure resources created in the guide.
