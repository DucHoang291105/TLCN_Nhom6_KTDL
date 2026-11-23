param(
    [Parameter(Mandatory=$true)]
    [string]$App,
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$AppArgs
)

$ErrorActionPreference = "Stop"
$trinoStates = @(docker ps -a --filter "name=^/tlcn_trino$" --format "{{.State}}")
if ($LASTEXITCODE -ne 0) { throw "Cannot check the catalog write window: Docker is unavailable" }
foreach ($state in $trinoStates) {
    if ($state.Trim() -notin @("created", "exited", "dead")) {
        throw "Trino is active: stop tlcn_trino before manual Spark jobs; restart query/dashboard after the pipeline finishes"
    }
}

$containerPath = "/opt/project/" + ($App -replace "\\", "/")

$previousPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
try {
    docker exec -e PYTHONPATH=/opt/project tlcn_spark_master `
        python3 -m src.orchestration.publication ($App -replace "\\", "/") @AppArgs 2>&1 | ForEach-Object { "$_" }
    $jobExitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousPreference
}


exit $jobExitCode
