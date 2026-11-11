[CmdletBinding()]
param([switch]$Build)

$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    function Invoke-DockerChecked {
        & docker @args
        if ($LASTEXITCODE -ne 0) { throw "Docker validation failed (exit $LASTEXITCODE)." }
    }
    Invoke-DockerChecked compose config --quiet
    if ($Build) { Invoke-DockerChecked compose build }
    Invoke-DockerChecked compose up -d --wait --wait-timeout 300
    Invoke-DockerChecked compose ps --all
    Invoke-DockerChecked compose exec -T airflow-scheduler airflow dags list --output json
    Invoke-DockerChecked compose exec -T airflow-scheduler airflow dags list-import-errors --output json
    Invoke-DockerChecked compose exec -T airflow-scheduler dbt --version
    Invoke-DockerChecked compose exec -T airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v

    $hostHash = (Get-FileHash -LiteralPath 'airflow/dags/customer360_pipeline.py' -Algorithm SHA256).Hash.ToLowerInvariant()
    $hashCode = "import hashlib; from pathlib import Path; print(hashlib.sha256(Path('/opt/airflow/dags/customer360_pipeline.py').read_bytes()).hexdigest())"
    $containerHash = & docker compose exec -T airflow-scheduler python -c $hashCode
    if ($LASTEXITCODE -ne 0 -or $containerHash.Trim() -ne $hostHash) {
        throw 'The mounted DAG does not match the repository file.'
    }
    Write-Host 'Mounted DAG SHA256 matches the repository.'
    Invoke-DockerChecked compose logs --no-color --tail 60 airflow-scheduler
    Write-Host 'Local validation passed. No pipeline run or external connection test was started.'
} finally {
    Pop-Location
}
