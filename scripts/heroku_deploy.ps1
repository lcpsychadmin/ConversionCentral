$workspace = "C:\Users\jwesc\OneDrive\Documents\VSCode Workspace\ConversionCentral"
$frontend = Join-Path $workspace "frontend"
$pushLog = Join-Path $workspace "heroku_push_output.log"
$releaseLog = Join-Path $workspace "heroku_release_output.log"

Push-Location $frontend

$pushOutput = & heroku container:push web --app conversion-central-frontend --verbose 2>&1
$pushOutput | Set-Content $pushLog
$pushOutput | ForEach-Object { Write-Output $_ }

Pop-Location

$releaseOutput = & heroku container:release web --app conversion-central-frontend 2>&1
$releaseOutput | Set-Content $releaseLog
$releaseOutput | ForEach-Object { Write-Output $_ }
