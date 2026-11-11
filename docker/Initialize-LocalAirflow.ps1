# Idempotent local-only bootstrap. Does not read or copy integration credentials.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$localDirectory = Join-Path $repositoryRoot '.local/customer360'
$secretDirectory = Join-Path $localDirectory 'secrets'
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

foreach ($directory in @($secretDirectory, (Join-Path $localDirectory 'dbt'), (Join-Path $localDirectory 'keys'))) {
    [void](New-Item -ItemType Directory -Path $directory -Force)
}

# Remove inherited access only on this newly managed local directory. Preserve
# SYSTEM and Administrators so Docker Desktop and normal backup tools can access it.
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$allowedSids = @($identity, 'S-1-5-18', 'S-1-5-32-544')
# icacls updates the DACL without requiring the SACL/audit privileges that
# Set-Acl may request in Windows PowerShell 5.1.
& icacls $localDirectory /inheritance:r /grant:r "*${identity}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not protect the local runtime directory.' }
foreach ($rule in (Get-Acl -LiteralPath $localDirectory).Access) {
    $sid = $rule.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value
    if ($sid -notin $allowedSids) {
        & icacls $localDirectory /remove "*$sid" | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Could not remove an extra local runtime access rule.' }
    }
}

function New-RandomValue {
    $bytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($bytes) } finally { $generator.Dispose() }
    return [Convert]::ToBase64String($bytes).Replace('+', '-').Replace('/', '_')
}

function Write-SecretIfMissing([string]$Name, [string]$Value) {
    $path = Join-Path $secretDirectory $Name
    if (-not (Test-Path -LiteralPath $path)) {
        [System.IO.File]::WriteAllText($path, $Value, $utf8NoBom)
    }
    if ((Get-Item -LiteralPath $path).Length -eq 0) {
        throw "Existing local secret file is empty: $Name. Restore it before startup."
    }
}

Write-SecretIfMissing 'postgres_password' (New-RandomValue)
$postgresPassword = [System.IO.File]::ReadAllText((Join-Path $secretDirectory 'postgres_password'))
$encodedPassword = [Uri]::EscapeDataString($postgresPassword)
Write-SecretIfMissing 'sql_alchemy_conn' "postgresql+psycopg2://airflow:${encodedPassword}@postgres/airflow"
Write-SecretIfMissing 'fernet_key' (New-RandomValue)
Write-SecretIfMissing 'jwt_secret' (New-RandomValue)
Write-SecretIfMissing 'api_secret_key' (New-RandomValue)
Write-SecretIfMissing 'simple_auth_passwords.json' (@{ localadmin = (New-RandomValue) } | ConvertTo-Json -Compress)

$runtimeFile = Join-Path $localDirectory 'runtime.env'
if (-not (Test-Path -LiteralPath $runtimeFile)) {
    [System.IO.File]::WriteAllText($runtimeFile, "# Optional runtime variables; never commit this file.`n", $utf8NoBom)
}
Write-Host 'Local Airflow files are ready. Existing secrets were preserved; no secret values were printed.'
Write-Host 'UI user: localadmin. Open .local/customer360/secrets/simple_auth_passwords.json privately for the password.'
