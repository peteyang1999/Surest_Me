# ============================================================================
# Network Mismatch Processing Job - Complete Workflow
# ============================================================================
# This script:
#   1. Executes the SQL script to identify and fix mismatched networks
#   2. Runs the PowerShell script to process claims via MSMQ
#   3. Exports processing results to CSV/Excel
#   4. Sends email notification with attachment
# ============================================================================
# Usage in SQL Server Agent Job:
#   Step 1: Run SQL script (updatedNetworkMismatch1206.sql)
#   Step 2: Run this PowerShell script
# ============================================================================

param(
    # SQL Server Configuration
    [Parameter(Mandatory=$false)]
    [string]$ServerInstance = "plx-sqlprod\qcprod",
    
    [Parameter(Mandatory=$false)]
    [string]$Database = "qc_core",
    
    # SQL Script Path
    [Parameter(Mandatory=$false)]
    [string]$SqlScriptPath = "c:\user-data\NetworkMissmatch\updatedNetworkMismatch1206.sql",
    
    # PowerShell Script Path
    [Parameter(Mandatory=$false)]
    [string]$PowerShellScriptPath = "c:\user-data\NetworkMissmatch\MismatchedNetworksQCPROD112025 (2).ps1",
    
    # QC Configuration (for PowerShell script)
    [Parameter(Mandatory=$false)]
    [string]$QCWebServerName = "plx-qcprod",
    
    [Parameter(Mandatory=$false)]
    [string]$QCServiceUser = "bind\qcprod",
    
    [Parameter(Mandatory=$false)]
    [switch]$AdjudicateClaim = $false,
    
    # Export and Email Configuration
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = "c:\user-data\NetworkMissmatch\NetworkMismatch_Results_$(Get-Date -Format 'yyyyMMdd').csv",
    
    [Parameter(Mandatory=$false)]
    [string]$SmtpServer,
    
    [Parameter(Mandatory=$false)]
    [int]$SmtpPort = 587,
    
    [Parameter(Mandatory=$false)]
    [string]$FromEmail,
    
    [Parameter(Mandatory=$false)]
    [string]$ToEmail,
    
    [Parameter(Mandatory=$false)]
    [string]$SmtpUsername,
    
    [Parameter(Mandatory=$false)]
    [string]$SmtpPassword,
    
    [Parameter(Mandatory=$false)]
    [string]$EmailSubject = "Network Mismatch Processing Results - $(Get-Date -Format 'yyyy-MM-dd')",
    
    [Parameter(Mandatory=$false)]
    [switch]$ExportToExcel = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$UseSSL = $true,
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipEmail = $false  # Set to true to skip email if not configured
)

# ============================================================================
# Error Handling Setup
# ============================================================================
$ErrorActionPreference = "Stop"
$script:ExitCode = 0

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Host $logMessage
    
    if ($env:SQLAGENT_ENABLED -eq "1") {
        Write-Output $logMessage
    }
}

function Exit-WithError {
    param([string]$ErrorMessage, [int]$ErrorCode = 1)
    Write-Log $ErrorMessage "ERROR"
    $script:ExitCode = $ErrorCode
    exit $ErrorCode
}

# ============================================================================
# Step 1: Execute SQL Script
# ============================================================================
Write-Log "============================================================================"
Write-Log "Step 1: Executing SQL Script to Identify and Fix Mismatched Networks"
Write-Log "============================================================================"

try {
    if (-not (Test-Path $SqlScriptPath)) {
        Exit-WithError "SQL script not found: $SqlScriptPath"
    }
    
    Write-Log "Reading SQL script from: $SqlScriptPath"
    $sqlQuery = Get-Content -Path $SqlScriptPath -Raw
    
    if ([string]::IsNullOrWhiteSpace($sqlQuery)) {
        Exit-WithError "SQL script is empty"
    }
    
    Write-Log "Connecting to SQL Server: $ServerInstance, Database: $Database"
    
    # Execute SQL script
    if (Get-Module -ListAvailable -Name SqlServer) {
        Write-Log "Using SqlServer PowerShell module"
        Import-Module SqlServer -Force
        
        $sqlResults = Invoke-Sqlcmd `
            -ServerInstance $ServerInstance `
            -Database $Database `
            -Query $sqlQuery `
            -QueryTimeout 600 `
            -ErrorAction Stop
        
        Write-Log "SQL script executed successfully"
    } else {
        Write-Log "Using .NET SqlClient"
        Add-Type -AssemblyName System.Data
        
        $connectionString = "Server=$ServerInstance;Database=$Database;Integrated Security=true;Connection Timeout=30;"
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        
        try {
            $connection.Open()
            $command = New-Object System.Data.SqlClient.SqlCommand($sqlQuery, $connection)
            $command.CommandTimeout = 600
            $command.ExecuteNonQuery() | Out-Null
            Write-Log "SQL script executed successfully"
        } finally {
            if ($connection.State -eq 'Open') {
                $connection.Close()
            }
        }
    }
    
} catch {
    Exit-WithError "Failed to execute SQL script: $($_.Exception.Message)"
}

# ============================================================================
# Step 2: Execute PowerShell Script to Process Claims
# ============================================================================
Write-Log "============================================================================"
Write-Log "Step 2: Executing PowerShell Script to Process Claims via MSMQ"
Write-Log "============================================================================"

try {
    if (-not (Test-Path $PowerShellScriptPath)) {
        Exit-WithError "PowerShell script not found: $PowerShellScriptPath"
    }
    
    Write-Log "Executing PowerShell script: $PowerShellScriptPath"
    
    # Set variables that the script expects (override defaults in the script)
    $QCServerName = $ServerInstance
    $QCDatabaseName = $Database
    $QCWebServerName = $QCWebServerName
    $QCServiceUser = $QCServiceUser
    $adjudicateClaim = $AdjudicateClaim
    
    # Execute the script in the current scope
    # The script will use our variables and execute its functions
    . $PowerShellScriptPath
    
    Write-Log "PowerShell script executed successfully"
    
} catch {
    Write-Log "WARNING: PowerShell script execution had issues: $($_.Exception.Message)" "WARNING"
    # Don't exit - continue to export results even if processing had issues
}

# ============================================================================
# Step 3: Query Results for Export
# ============================================================================
Write-Log "============================================================================"
Write-Log "Step 3: Querying Processing Results for Export"
Write-Log "============================================================================"

try {
    # Query to get the processing results
    $resultsQuery = @"
SELECT 
    claim_id,
    claim_ud,
    repriced_network,
    ark_network,
    arko_network,
    expected_network,
    to_process,
    created_user_name,
    created_date,
    modified_date,
    modified_user_name
FROM bind_mismatched_network_processing
WHERE created_date >= CAST(GETDATE() AS DATE)
ORDER BY created_date DESC, claim_id
"@
    
    Write-Log "Querying processing results from bind_mismatched_network_processing table"
    
    $results = $null
    
    if (Get-Module -ListAvailable -Name SqlServer) {
        $results = Invoke-Sqlcmd `
            -ServerInstance $ServerInstance `
            -Database $Database `
            -Query $resultsQuery `
            -QueryTimeout 300 `
            -ErrorAction Stop
    } else {
        Add-Type -AssemblyName System.Data
        
        $connectionString = "Server=$ServerInstance;Database=$Database;Integrated Security=true;Connection Timeout=30;"
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        
        try {
            $connection.Open()
            $command = New-Object System.Data.SqlClient.SqlCommand($resultsQuery, $connection)
            $command.CommandTimeout = 300
            
            $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($command)
            $dataset = New-Object System.Data.DataSet
            $adapter.Fill($dataset) | Out-Null
            
            $results = $dataset.Tables[0]
        } finally {
            if ($connection.State -eq 'Open') {
                $connection.Close()
            }
        }
    }
    
    if ($null -eq $results -or $results.Count -eq 0) {
        Write-Log "WARNING: No results found for today's processing" "WARNING"
        $results = @()
    } else {
        Write-Log "Found $($results.Count) records to export"
    }
    
} catch {
    Exit-WithError "Failed to query results: $($_.Exception.Message)"
}

# ============================================================================
# Step 4: Export to CSV or Excel
# ============================================================================
Write-Log "============================================================================"
Write-Log "Step 4: Exporting Results to File"
Write-Log "============================================================================"

try {
    # Ensure output directory exists
    $outputDir = Split-Path -Path $OutputPath -Parent
    if (-not (Test-Path $outputDir)) {
        Write-Log "Creating output directory: $outputDir"
        New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
    }
    
    if ($ExportToExcel) {
        Write-Log "Exporting to Excel format: $OutputPath"
        
        if (Get-Module -ListAvailable -Name ImportExcel) {
            Import-Module ImportExcel -Force
            $results | Export-Excel -Path $OutputPath -AutoSize -AutoFilter -FreezeTopRow -ErrorAction Stop
            Write-Log "Excel file created successfully"
        } else {
            Write-Log "ImportExcel module not found. Installing..." "WARNING"
            try {
                Install-Module -Name ImportExcel -Scope CurrentUser -Force -AllowClobber
                Import-Module ImportExcel -Force
                $results | Export-Excel -Path $OutputPath -AutoSize -AutoFilter -FreezeTopRow -ErrorAction Stop
                Write-Log "Excel file created successfully"
            } catch {
                Write-Log "Failed to install ImportExcel module. Falling back to CSV..." "WARNING"
                $OutputPath = $OutputPath -replace '\.xlsx$', '.csv'
                $ExportToExcel = $false
            }
        }
    }
    
    if (-not $ExportToExcel) {
        Write-Log "Exporting to CSV format: $OutputPath"
        
        if ($results -is [System.Data.DataTable]) {
            $csvData = @()
            foreach ($row in $results.Rows) {
                $obj = @{}
                foreach ($col in $results.Columns) {
                    $obj[$col.ColumnName] = $row[$col]
                }
                $csvData += [PSCustomObject]$obj
            }
            $csvData | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8
        } else {
            $results | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8
        }
        
        Write-Log "CSV file created successfully"
    }
    
    if (-not (Test-Path $OutputPath)) {
        Exit-WithError "Export file was not created: $OutputPath"
    }
    
    $fileSize = (Get-Item $OutputPath).Length
    Write-Log "File created: $OutputPath ($([math]::Round($fileSize/1KB, 2)) KB)"
    
} catch {
    Exit-WithError "Failed to export results: $($_.Exception.Message)"
}

# ============================================================================
# Step 5: Send Email with Attachment (if configured)
# ============================================================================
if (-not $SkipEmail -and -not [string]::IsNullOrWhiteSpace($SmtpServer) -and -not [string]::IsNullOrWhiteSpace($ToEmail)) {
    Write-Log "============================================================================"
    Write-Log "Step 5: Sending Email Notification with Attachment"
    Write-Log "============================================================================"
    
    try {
        $recordCount = if ($results -is [System.Data.DataTable]) { $results.Rows.Count } else { $results.Count }
        
        $emailBody = @"
Network Mismatch Processing Job Completed

Processing Date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
Records Processed: $recordCount

The attached file contains the results from today's network mismatch processing.

Job Steps Completed:
1. SQL Script: Identified and fixed mismatched networks
2. PowerShell Script: Processed claims via MSMQ
3. Results Exported: $OutputPath

Server: $ServerInstance
Database: $Database

Best regards,
SQL Server Agent
"@
        
        $mailMessage = New-Object System.Net.Mail.MailMessage
        $mailMessage.From = New-Object System.Net.Mail.MailAddress($FromEmail)
        $mailMessage.To.Add($ToEmail)
        $mailMessage.Subject = $EmailSubject
        $mailMessage.Body = $emailBody
        $mailMessage.IsBodyHtml = $false
        
        $attachment = New-Object System.Net.Mail.Attachment($OutputPath)
        $mailMessage.Attachments.Add($attachment)
        Write-Log "Attachment added: $OutputPath"
        
        $smtpClient = New-Object System.Net.Mail.SmtpClient($SmtpServer, $SmtpPort)
        $smtpClient.EnableSsl = $UseSSL
        $smtpClient.Timeout = 30000
        
        if (-not [string]::IsNullOrWhiteSpace($SmtpUsername) -and -not [string]::IsNullOrWhiteSpace($SmtpPassword)) {
            $smtpClient.Credentials = New-Object System.Net.NetworkCredential($SmtpUsername, $SmtpPassword)
            Write-Log "Using authenticated SMTP"
        } else {
            $smtpClient.UseDefaultCredentials = $true
            Write-Log "Using default credentials for SMTP"
        }
        
        Write-Log "Sending email to: $ToEmail"
        $smtpClient.Send($mailMessage)
        Write-Log "Email sent successfully"
        
        $attachment.Dispose()
        $smtpClient.Dispose()
        $mailMessage.Dispose()
        
    } catch {
        Write-Log "WARNING: Failed to send email: $($_.Exception.Message)" "WARNING"
        # Don't exit - job completed successfully even if email fails
    }
} else {
    Write-Log "Email notification skipped (not configured or SkipEmail flag set)"
}

# ============================================================================
# Success
# ============================================================================
Write-Log "============================================================================"
Write-Log "Job completed successfully"
Write-Log "============================================================================"
exit 0

