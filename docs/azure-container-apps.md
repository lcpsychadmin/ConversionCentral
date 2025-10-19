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
2. **SQL Server ingestion**: Use Azure SQL Database/Managed Instance. Ensure the ODBC driver is accessible (driver `ODBC Driver 18 for SQL Server`). Collect the SQLAlchemy-style connection string you provide to the backend.
3. Apply the schema:
   - Run Alembic migrations through the backend app on first start (already executed in `start.sh`).
   - Execute `docker/sqlserver-init.sql` against the Azure SQL instance (e.g., using Azure Data Studio or `sqlcmd`).

## 5. Deploy the backend container app

```powershell
$databaseUrl = "postgresql+psycopg://..."            # Replace
$ingestionUrl = "mssql+pyodbc://..."                 # Replace
$ingestionPassword = "<ingestion-user-password>"     # Optional, if used elsewhere

az containerapp create `
  -g $resourceGroup -n "cc-backend" `
  --environment $envName `
  --image $backendImage `
  --target-port 8000 `
  --ingress external `
  --registry-server "$acrName.azurecr.io" `
  --cpu 1 --memory 2Gi `
  --secrets database-url=$databaseUrl ingestion-url=$ingestionUrl `
  --env-vars `
      DATABASE_URL=secretref:database-url `
      INGESTION_DATABASE_URL=secretref:ingestion-url `
      APP_NAME="Conversion Central API" `
      FRONTEND_ORIGINS="https://cc-frontend.<region>.azurecontainerapps.io,https://localhost:3000"
```

Notes:
- Replace the sample `FRONTEND_ORIGINS` with the actual frontend hostname once it is known.
- If you set additional secrets (e.g., ingestion password), reference them via `secretref` too.

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

No changes are required locally. Continue using `docker compose up` with the bundled Postgres and SQL Server containers. The new environment variables (`FRONTEND_ORIGINS`, Azure connection strings) only apply to the cloud deployment.

## 9. Cleanup

```powershell
az group delete -n $resourceGroup --yes
```

This removes all Azure resources created in the guide.
