# Deploying Conversion Central Containers to Heroku

Heroku's Container Runtime lets you push pre-built Docker images for individual process types (for example `web`). Because the platform runs one container per dyno, you will typically deploy the backend and frontend as separate Heroku apps. The steps below assume you want to keep the existing container images (`Dockerfile.backend` and `frontend/Dockerfile`).

> ⚠️ **PostgreSQL** – Provision a managed database (e.g. `heroku addons:create heroku-postgresql:mini`) and update the `DATABASE_URL` config on the backend app before releasing your image.

## 1. Install and Authenticate

```powershell
# Install the Heroku CLI if it is not already on PATH
winget install HerokuCLI

# Authenticate browser-based account
heroku login

# Authenticate the container registry
heroku container:login
```

## 2. Create Apps (Backend & Frontend)

```powershell
# Backend API
heroku create <backend-app-name> --stack=container

# React frontend
heroku create <frontend-app-name> --stack=container
```

If you prefer a single hostname, use Heroku Pipelines or custom domains to group the apps later.

## 3. Configure Backend Environment

```powershell
# Example configuration
heroku config:set \
  APP_NAME="Conversion Central API" \
  DATABASE_URL="postgresql+psycopg2://<user>:<password>@<host>:5432/<db>" \
  FRONTEND_ORIGINS="https://<frontend-app-name>.herokuapp.com" \
  DATABRICKS_HOST="" \
  DATABRICKS_HTTP_PATH="" \
  DATABRICKS_TOKEN="" \
  --app <backend-app-name>
```

## 4. Push & Release the Backend Image

```powershell
# From the repository root
heroku container:push web --app <backend-app-name> -f Dockerfile.backend
heroku container:release web --app <backend-app-name>
```

The backend container runs `docker/backend/start.sh`, which already binds to `$PORT` when present (Heroku injects this variable automatically).

## 5. Push & Release the Frontend Image

```powershell
# From the repository root
heroku container:push web --app <frontend-app-name> -f frontend/Dockerfile
heroku container:release web --app <frontend-app-name>
```

The frontend Dockerfile builds the React application and serves it via Nginx. Update any environment variables (for example, `VITE_API_URL`) on the frontend app so it points to the deployed backend URL:

```powershell
heroku config:set VITE_API_URL="https://<backend-app-name>.herokuapp.com/api" --app <frontend-app-name>
```

## 6. Optional: HTTPS for Custom Domains

Heroku automatically provisions TLS for `*.herokuapp.com` domains. If you map a custom domain, use `heroku certs:auto:enable --app <app-name>` to enable ACM-managed certificates.

## 7. Continuous Deployment

To automate deployments, script the `heroku container:push` and `heroku container:release` commands inside your CI/CD pipeline, or use Heroku GitHub integration to trigger a build whenever your `Dockerfile` changes.

---

For more complex scenarios—such as serving the React build and API from a single container—you can author a bespoke Dockerfile that builds the frontend in an intermediate stage and serves it via the FastAPI app or an embedded Nginx instance. The existing guides above keep parity with the current development setup (`docker-compose`) while staying aligned with Heroku's single-container-per-dyno model.
