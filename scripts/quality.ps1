$ErrorActionPreference = "Stop"

Write-Host "Running Ruff auto-fixes across the project..."
ruff check . --fix

Write-Host "Running Black formatter across the project..."
black .

Write-Host "Running Ruff formatter across the project..."
ruff format .

Write-Host "Quality checks completed."
