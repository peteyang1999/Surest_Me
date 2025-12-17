# ============================================================================
# Network Mismatch Job Configuration
# ============================================================================
# Customize these settings for your SQL Server Agent Job
# ============================================================================

# SQL Server Configuration
$ServerInstance = "plx-sqlprod\qcprod"
$Database = "qc_core"

# Script Paths
$SqlScriptPath = "c:\user-data\NetworkMissmatch\updatedNetworkMismatch1206.sql"
$PowerShellScriptPath = "c:\user-data\NetworkMissmatch\MismatchedNetworksQCPROD112025.ps1"

# QC Configuration
$QCWebServerName = "plx-qcprod"
$QCServiceUser = "bind\qcprod"
$AdjudicateClaim = $false  # Set to $true if you want to adjudicate claims after clearing

# Export Configuration
$OutputPath = "c:\user-data\NetworkMissmatch\NetworkMismatch_Results_$(Get-Date -Format 'yyyyMMdd').csv"
# For Excel: "c:\user-data\NetworkMissmatch\NetworkMismatch_Results_$(Get-Date -Format 'yyyyMMdd').xlsx"

# Email Configuration (Optional - leave empty to skip email)
$SmtpServer = ""  # e.g., "email-smtp.us-east-1.amazonaws.com" or "smtp.office365.com"
$SmtpPort = 587
$FromEmail = ""  # e.g., "sql-agent@yourdomain.com"
$ToEmail = ""  # e.g., "recipient@yourdomain.com"
$SmtpUsername = ""  # Leave empty if not needed
$SmtpPassword = ""  # Leave empty if not needed
$EmailSubject = "Network Mismatch Processing Results - $(Get-Date -Format 'yyyy-MM-dd')"

# Export Options
$ExportToExcel = $false  # Set to $true for Excel export
$UseSSL = $true

# ============================================================================
# Execute the Script
# ============================================================================
$ScriptPath = Join-Path $PSScriptRoot "NetworkMismatch_Job_Complete.ps1"

$params = @{
    ServerInstance = $ServerInstance
    Database = $Database
    SqlScriptPath = $SqlScriptPath
    PowerShellScriptPath = $PowerShellScriptPath
    QCWebServerName = $QCWebServerName
    QCServiceUser = $QCServiceUser
    OutputPath = $OutputPath
    ExportToExcel = $ExportToExcel
    UseSSL = $UseSSL
}

# Add optional parameters only if they're set
if ($AdjudicateClaim) { $params.AdjudicateClaim = $true }
if (-not [string]::IsNullOrWhiteSpace($SmtpServer)) { 
    $params.SmtpServer = $SmtpServer
    $params.SmtpPort = $SmtpPort
    $params.FromEmail = $FromEmail
    $params.ToEmail = $ToEmail
    if (-not [string]::IsNullOrWhiteSpace($SmtpUsername)) { 
        $params.SmtpUsername = $SmtpUsername 
        $params.SmtpPassword = $SmtpPassword
    }
    $params.EmailSubject = $EmailSubject
} else {
    $params.SkipEmail = $true
}

& $ScriptPath @params

