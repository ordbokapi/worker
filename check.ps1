param(
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$args
)

$featuresArg = "--all-features"

foreach ($arg in $args) {
    if ($arg -in @("-?", "-h", "--help", "help")) {
        Write-Host "Usage: .\check.ps1 [feature [feature …]]"
        Write-Host
        Write-Host "Runs clippy and fmt checks on the project."
        Write-Host
        Write-Host "  [feature]   Optional. Feature to enable for the build."
        Write-Host "              If not provided, all features are enabled."
        Write-Host "              ""none"" can be used to build without any features."
        Write-Host "  -h, --help  Show this help message."
        exit 0
    } elseif ($arg -eq "none") {
        $featuresArg = ""
    }
}

if ($featuresArg -ne "" -and $args.Length -gt 0) {
    $featuresArg = "--features " + ($args -join ",")
}

if ($Host.UI.SupportsVirtualTerminal -and -not [Console]::IsOutputRedirected) {
    $dim = "`e[2m"
    $reset = "`e[0m"
} else {
    $dim = ""
    $reset = ""
}

function Run($cmd) {
    Write-Host "`n$dim> $cmd$reset`n"
    $parts = $cmd.Split(" ")
    & $parts[0] $parts[1..($parts.Length - 1)]
}

$cmd = "cargo clippy"

if ($featuresArg) {
    $cmd += " $featuresArg"
}

$cmd += " -- -D clippy::suspicious -D clippy::style -D clippy::complexity -D clippy::perf -D clippy::dbg_macro -D clippy::todo -D clippy::unimplemented -D warnings"

Run $cmd
Run "cargo fmt -- --check"
