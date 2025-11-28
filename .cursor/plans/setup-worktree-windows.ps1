$ErrorActionPreference = 'Stop'

# 1) Identify which worktree we're in
$worktreePath = Get-Location
$worktreeName = Split-Path -Leaf $worktreePath

Write-Host "Setting up worktree: $worktreeName"

# 2) Map worktree folder -> conda env + branch
switch ($worktreeName) {
    "pos-pipeline-core-etl-exp1" {
        $envName = "pos-etl-exp1"
        $branch  = "cursor-exp-1"
    }
    "pos-pipeline-core-etl-exp2" {
        $envName = "pos-etl-exp2"
        $branch  = "cursor-exp-2"
    }
    "pos-pipeline-core-etl-exp3" {
        $envName = "pos-etl-exp3"
        $branch  = "cursor-exp-3"
    }
    default {
        Write-Host "Unknown worktree '$worktreeName'. Skipping env setup."
        exit 0
    }
}

Write-Host "Using conda env: $envName"
Write-Host "Using branch:    $branch"

# 3) Build python path for that env (adjust base path if your Anaconda lives elsewhere)
$condaEnvRoot = "C:\Users\mzenk\anaconda3\envs"
$pythonPath   = Join-Path (Join-Path $condaEnvRoot $envName) "python.exe"

# 4) Create/update .vscode/settings.json to point Cursor/VSCode at this env
$newSettings = @{
    "python.defaultInterpreterPath" = $pythonPath
}

New-Item -ItemType Directory -Force -Path ".vscode" | Out-Null
$newSettings | ConvertTo-Json -Depth 2 | Set-Content ".vscode\settings.json" -Encoding UTF8

Write-Host ".vscode/settings.json written with interpreter $pythonPath"

# 5) (Optional) Install branch-specific pos-core into that env
$repoUrl    = "https://github.com/ToxicFyre/pos-pipeline-core-etl.git"
$installUrl = "git+$repoUrl@$branch#egg=pos-core"

Write-Host "Uninstalling old pos_core from $envName..."
conda run -n $envName python -m pip uninstall pos-core pos_core -y

Write-Host "Installing pos_core from branch '$branch' into $envName..."
conda run -n $envName python -m pip install --force-reinstall --no-deps $installUrl

Write-Host "Worktree setup complete for $worktreeName."
