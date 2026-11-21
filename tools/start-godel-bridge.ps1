[CmdletBinding()]
param([ValidateRange(1,20)][int]$Minutes=20,[switch]$UntilClose,[switch]$CompatibilityCheck,[switch]$CheckOnly)
& (Join-Path $PSScriptRoot 'start-browser-comparison.ps1') -Source Godel @PSBoundParameters
