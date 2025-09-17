   <#
.SYNOPSIS
    Powershell script script used to mass process claims.
.DESCRIPTION
    Script that can be used to mass clear, adjudicate, or negate claims from a table in the qc_core database called bind_batch_claim_process.
.PARAMETER environment
    The environment that you want to process against. (Mandatory)
    Valid Options: claims, dream, stage, dev, daisy, dolly, prod
.PARAMETER batch_id
    Specifies the batch you want to process from the bind_claim_processing_batch table, specificically the bind_claim_processing_batch_id. (Mandatory)
.PARAMETER negate_claims
    Determines if you want the script to negate the claims flagged for negation the batch. (Default: false).
.PARAMETER clear_claims
    Determines if you want the script to clear adjudication on the claims flagged for adjudication in the batch. (Default: false).
.PARAMETER adj_claims
    Determines if you want the script to adjudicate the claims flagged for adjudication in the batch. (Default: false).
.PARAMETER run_adj_queue
    Determines if you want to just kick off an adjudication Queu run instead of dumping all claims in the adjudication Queue. 
.PARAMETER process_original_claim
    This flag is used to tell the script if you want to process the claims based on the claim_id from the bind_claim_processing table instead of the new_claim_id. This only applies to the clear and adjudicate claims process. 
.PARAMETER claim_limit
    Restricts the number of claims that will be processed by the script. This number is passed to the sql queries as a "Select Top claim_limit"
.PARAMETER logToConsole
    If this flag is set the script will write out actions to the console using "Write-host".
.PARAMETER logToFile
    If this flag is set the script will write out actions to a log file located at c:\logs\yyyyMMdd-mass_processing.log"
.PARAMETER timeout
    Sets the timeout for the script in minutes. (Default: 120)
.EXAMPLE
    C:\PS> 
    <Description of example>
.NOTES
    Author: Cody Parker
    Date:   April 19, 2022    
#>

param(
 [Parameter(Mandatory=$true)]
 [ValidateSet('claims','dream','stage','dev','perf','daisy','dolly','prod')]
 [string]$environment
,[parameter(Mandatory=$true)]
 [ValidateRange(1, [int]::MaxValue)]
 [int]$batch_id 
,[switch]$negate_claims
,[switch]$clear_claims
,[switch]$adj_claims
,[switch]$run_adj_queue
,[switch]$run_vendorOverride
,[bool]$process_original_claim = $false
,[string]$claim_limit = ''
,[switch]$logToConsole 
,[switch]$logToFile
,[string]$timeout = '120'
,[string]$batchserver
)

$debug_log = "c:\logs\$(get-date -Format 'yyyyMMdd')-mass_processing.log"

if (!(Test-Path "$debug_log"))
{
   New-Item -path $debug_log -type "file" -value "Running Mass Claim Processing Batch($batch_id) at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
   Write-Host "New Logging file created at $debug_log"
}
else
{
  Add-Content -path $debug_log -value "Running Mass Claim Processing Batch($batch_id) at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
  Write-Host "Logging file already exists. Appending job results to $debug_log"
}

function Write-Log {
    param (
        [Parameter()]
        [String]
        $text,
        [Parameter()]
        [switch]
        $force
    )
    if($logToConsole -eq $true -or $force -eq $true){
    Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') :: $text"
    }
    if($logToFile -eq $true -or $force -eq $true){
        Add-Content -path $debug_log -value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') :: $text"
    }
  }



##Environment Mapping
$envs = @(
    @{env="claims";dbserver="plx-sqldev04";dbInstance="qcclaims";username="bind\qcclaims";webserver="plx-qcclaims01";batchserver=""}
    @{env="dream";dbserver="plx-sqldev04";dbInstance="qcdream";username="bind\qcdream";webserver="plx-qcdream";batchserver=""}
    @{env="stage";dbserver="plx-sqldev";dbInstance="qcstage";username="bind\qcstage";webserver="plx-qcstage";batchserver=""}
    @{env="dev";dbserver="plx-sqldev";dbInstance="qcdev";username="bind\qcdev";webserver="plx-qcdev";batchserver=""}
    @{env="perf";dbserver="plx-sqlperf";dbInstance="qcperf";username="bind\qcperf";webserver="plx-qcperf";batchserver=""}
    @{env="daisy";dbserver="DaisySQL";dbInstance="qcdaisy";username="bind\qcdaisy";webserver="plx-qcdaisy02";batchserver=""}
    @{env="dolly";dbserver="plx-sqlprod03";dbInstance="qcdolly";username="bind\qcdolly";webserver="plx-qcdolly02";batchserver=""}
    @{env="prod";dbserver="plx-sqlprod";dbInstance="qcprod";username="bind\qcprod";webserver="plx-qcprod";batchserver="plx-batchadj01"}
    )

if ($envs.env.contains("$environment")) {
    Write-Host "Populating variables from environment mapping"
    Foreach ($Key in ($envs.GetEnumerator() | Where-Object {$_.env -eq "$environment"})) {
        $QCServiceUser = $Key.username
        $dbServerName = $Key.dbserver
        $instanceName = $Key.dbInstance
        $QCWebServerName = $Key.webserver
        $qcBatchServer = $Key.batchserver  

        $QCServerName = "$dbServerName\$instanceName"
        $QCDatabaseName = 'qc_core'
    }
}
else {
    Write-Log -text "Provided environment not found. Please review and try again. (See environment map within script for further details)" -force
    Write-Log -text "Environment Provided: $environment" -force
}

if ([string]::IsNullOrEmpty($qcBatchServer)) {
    set-variable -Name "qcBatchServer" -Value $QCWebServerName
}

Function Get-QCClaims{
        Param(
            [Parameter(Mandatory=$true)]
            [ValidateNotNullOrEmpty()]
            [string]$serverName,
            [Parameter(Mandatory=$true)]
            [ValidateNotNullOrEmpty()]
            [string]$databaseName,
            [bool]$process_orig_claim = $false,
            [string]$limit = '',
            [Parameter(Mandatory=$true)]
            [ValidateSet('adj','negate','clear')]
            [string]$claimType
    )
    <#
        .SYNOPSIS
          Will retrieve a list of claims in the qc_core database that have been loaded in the bind_claim_processing table, based on the claim type 
        .EXAMPLE
          Get-QCCalims -serverName "ServerName" -databaseName "DatabaseName" -claimType "adj"
          Get-QCClaims -serverName "ServerName" -databaseName "DatabaseName" -claimType "adj"  -process_orig_claim $true 
          Get-QCClaims -serverName "ServerName" -databaseName "DatabaseName" -claimType "adj"  -process_orig_claim $true -limit 100
    #>
    Try
	{
            $top = "";
            If (-not ([string]::IsNullOrWhitespace($limit))){$top = "top $limit";}

            $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
            $sqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $sqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
            $return = New-Object System.Data.Datatable
            $sqlConnection.ConnectionString = "Data Source=$serverName;Initial Catalog=$databaseName;Integrated Security=SSPI"

            $sqlConnection.Open()
            $sqlCommand.Connection = $sqlConnection
            if($claimType -ieq 'adj'){
                if($process_orig_claim -eq $true){
                    $sqlCommand.CommandText = "SELECT $top claim_id as claim_id FROM bind_claim_processing  WHERE to_adjudicate = 1 and bind_claim_processing_batch_id = $batch_id order by bind_claim_processing_id" 
                }
                else{
                    $sqlCommand.CommandText = "SELECT $top new_claim_id as claim_id FROM bind_claim_processing  WHERE to_adjudicate = 1 and bind_claim_processing_batch_id = $batch_id order by bind_claim_processing_id" 
                }
            }
            elseif($claimType -ieq 'clear'){
                if($process_orig_claim -eq $true){
                    $sqlCommand.CommandText = "SELECT $top claim_id as claim_id FROM bind_claim_processing  WHERE to_clear = 1 and bind_claim_processing_batch_id = $batch_id order by bind_claim_processing_id" 
                }
                else{
                    $sqlCommand.CommandText = "SELECT $top new_claim_id as claim_id FROM bind_claim_processing  WHERE to_clear = 1 and bind_claim_processing_batch_id = $batch_id order by bind_claim_processing_id" 
                }
            }
            elseif($claimType -ieq 'negate'){
                if($process_orig_claim -eq $true){
                    $sqlCommand.CommandText = "SELECT $top claim_id as claim_id FROM bind_claim_processing  WHERE to_negate = 1 and bind_claim_processing_batch_id = $batch_id order by bind_claim_processing_id" 
                }
                else{
                    $sqlCommand.CommandText = "SELECT $top new_claim_id as claim_id FROM bind_claim_processing  WHERE to_negate = 1 and bind_claim_processing_batch_id = $batch_id order by bind_claim_processing_id" 
                }            
            }
            Write-Log -text "$sqlCommand" 
            $sqlAdapter.SelectCommand = $sqlCommand

            [void]$sqlAdapter.fill($return)
            $sqlConnection.Close()
           
	}
    Catch
	    {
	    $ErrorMessage = $_.Exception.Message
        Return $ErrorMessage
	    }

    Return $return
}


Function Update-QCClaims{
        param(
            [Parameter(Mandatory=$true)]
            [psobject]$claims,
            [Parameter(Mandatory=$true)]
            [ValidateNotNullOrEmpty()]
            [string]$serverName,
            [Parameter(Mandatory=$true)]
            [ValidateNotNullOrEmpty()]
            [string]$databaseName,
            [bool]$process_orig_claim = $false,
            [Parameter(Mandatory=$true)]
            [ValidateSet('adj','negate','clear')]
            [string]$claimType
        )
    
    <#
        .SYNOPSIS
          . This script will update bind_claim_processing records based on a list of claims passed in. This scripts marks them as processed based upon the claimType passed in 
        .EXAMPLE
          . Update-QCClaims -serverName "plx-sqldev\qcdev" -databaseName "qc_core" -claimType 'adj' -claims $claimsGetAdj
    #>
    Try {
    foreach($i in $claims) {
            $claim_id = $i.claim_id
            
            $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
            $sqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $sqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
            $return = New-Object System.Data.Datatable
            $sqlConnection.ConnectionString = "Data Source=$serverName;Initial Catalog=$databaseName;Integrated Security=SSPI"

            $sqlConnection.Open()
            $sqlCommand.Connection = $sqlConnection
            if($claimType -ieq 'adj'){
                if($process_orig_claim -eq $true){
                    $sqlCommand.CommandText = "UPDATE bind_claim_processing SET to_adjudicate = 0, adjudicate_time = getdate() WHERE to_adjudicate = 1 and bind_claim_processing_batch_id = $batch_id and claim_id = $claim_id" 
                }
                else{
                    $sqlCommand.CommandText = "UPDATE bind_claim_processing SET to_adjudicate = 0, adjudicate_time = getdate() WHERE to_adjudicate = 1 and bind_claim_processing_batch_id = $batch_id and new_claim_id = $claim_id"  
                }
            }
            elseif($claimType -ieq 'clear'){
                if($process_orig_claim -eq $true){
                    $sqlCommand.CommandText = "UPDATE bind_claim_processing SET to_clear = 0, clear_time = getdate() WHERE to_adjudicate = 1 and bind_claim_processing_batch_id = $batch_id and claim_id = $claim_id" 
                }
                else{
                    $sqlCommand.CommandText = "UPDATE bind_claim_processing SET to_clear = 0, clear_time = getdate() WHERE to_adjudicate = 1 and bind_claim_processing_batch_id = $batch_id and new_claim_id = $claim_id"  
                }
            }
            elseif ($claimType -ieq 'negate'){
                if($process_orig_claim -eq $true){
                    $sqlCommand.CommandText = "UPDATE bind_claim_processing SET to_negate = 0, netgate_time = getdate() WHERE to_negate = 1 and bind_claim_processing_batch_id = $batch_id and claim_id = $claim_id" 
                }
                else{
                    $sqlCommand.CommandText = "UPDATE bind_claim_processing SET to_negate = 0, netgate_time = getdate() WHERE to_negate = 1 and bind_claim_processing_batch_id = $batch_id and new_claim_id = $claim_id"  
                }
            }
            $sqlAdapter.SelectCommand = $sqlCommand

            [void]$sqlAdapter.fill($return)
            $sqlConnection.Close()
         }  
	}
    Catch
	    {
	    $ErrorMessage = $_.Exception.Message
        Return $ErrorMessage
	    }

    Return $return
}



Function Start-QCMassClear{
    param(
        [Parameter(Mandatory=$true)]
        [psobject]$claims,
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$targetMachine,
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$user
    )
    <#
        .SYNOPSIS
          Will clear adjudicate a list of claims supplied to the function in the $claims paramater.
          $targetMachine will point to the desired QC application server, server running function must have MSMQ
          $user will be user processing claims, should be service account
        .EXAMPLE
          Start-QCMassClear -claims $claimList -targetMachine "ServerName" -user "UserName"
    #>

    #----------------------------------------------------------------------------
    # Set variables
    #----------------------------------------------------------------------------
    [Reflection.Assembly]::LoadWithPartialName("System.Messaging") | Out-Null
    $Success = 1

    Try
        {
        Foreach($i in $claims)
            {
            $claim_id = $i.claim_id

            #----------------------------------------------------------------------------
            # Loop thru each claim ID, build the MSMQ message, put it on the queue.
            #----------------------------------------------------------------------------
            $msg = new-object System.Messaging.Message
            $msgStream = new-object System.IO.MemoryStream
            $queue = New-Object System.Messaging.MessageQueue
            $queue = New-Object System.Messaging.MessageQueue "FormatName:DIRECT=OS:$targetMachine\private$\qc.service.claimadjudication.clearclaimfromadjudication.input"
            $utf8 = new-object System.Text.UTF8Encoding
            $tran = new-object System.Messaging.MessageQueueTransaction

            #--------------------
            # Clear Adjudication - Build msg.
            #-------------------- 
            [xml]$XML = "<?xml version=""1.0""?><ClearClaimFromAdjudication xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:baseType=""Plexis.Messages.ICommand"" xmlns:baseType1=""Plexis.Messages.IMessage"" xmlns:baseType2=""Plexis.Messages.IReference"" xmlns:baseType3=""Plexis.Messages.IInternalId"" xmlns:baseType4=""Plexis.Messages.IIdentifier"" xmlns:baseType5=""Plexis.Messages.ISendResponse"" xmlns=""http://plexisweb.com/Plexis.Messages.ClaimAdjudication.Commands""><InternalId>$claim_id</InternalId><SecurityUserCredential>marcusb</SecurityUserCredential><Username>$user</Username><SendResponse>false</SendResponse></ClearClaimFromAdjudication>"


            #--------------------
            # Put the message on the queue
            #--------------------
            $queueMessage = $XML.OuterXml
            $tran.Begin()    
            $msgBytes = $utf8.GetBytes($queueMessage)    
            $msgStream.Write($msgBytes, 0, $msgBytes.Length)    
            $msg.BodyStream = $msgStream
            $msg.Label = ""
            $msg.ResponseQueue = $queue
            $queue.Send($msg, $tran)    
            $tran.Commit()
            }
        }
     Catch
        {
        $ErrorMessage = $_.Exception.Message
        Return $ErrorMessage
        $Success = 0
        }
    Return $Success
    }

Function Start-QCMassAdjudicate{
        param(
            [Parameter(Mandatory=$true)]
            [psobject]$claims,
            [Parameter(Mandatory=$true)]
            [ValidateNotNullOrEmpty()]
            [string]$targetMachine,
            [Parameter(Mandatory=$true)]
            [ValidateNotNullOrEmpty()]
            [string]$user
        )
    <#
        .SYNOPSIS
          Will adjudicate a list of claims supplied to the function in the $claims paramater.
          $targetMachine will point to the desired QC application server, server running function must have MSMQ
          $user will be user processing claims, should be service account
        .EXAMPLE
          Start-QCMassAdjudicate -claims $claimList -targetMachine "ServerName" -user "UserName"
    #>

    #----------------------------------------------------------------------------
    # Set variables
    #----------------------------------------------------------------------------
    [Reflection.Assembly]::LoadWithPartialName("System.Messaging") | Out-Null
    $Success = 1

    Try
        {
        Foreach($i in $claims)
            {
            $claim_id = $i.claim_id

            #----------------------------------------------------------------------------
            # Loop thru each claim ID, build the MSMQ message, put it on the queue.
            #----------------------------------------------------------------------------
            $msg = new-object System.Messaging.Message
            $msgStream = new-object System.IO.MemoryStream
            $queue = New-Object System.Messaging.MessageQueue
            $queue = New-Object System.Messaging.MessageQueue "FormatName:DIRECT=OS:$targetMachine\private$\qc.service.claimadjudication.adjudicateclaim.input"
            $utf8 = new-object System.Text.UTF8Encoding
            $tran = new-object System.Messaging.MessageQueueTransaction

            #--------------------
            # Adjudicate Claim - Build msg.
            #-------------------- 
            [xml]$XML = "<?xml version=""1.0""?><AdjudicateClaim xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:baseType=""Plexis.Messages.ICommand"" xmlns:baseType1=""Plexis.Messages.IMessage"" xmlns:baseType2=""Plexis.Messages.IReference"" xmlns:baseTyp3=""Plexis.Messages.IInternalId"" xmlns:baseType4=""Plexis.Messages.IIdentifier"" xmlns:baseType5=""Plexis.Messages.ISendResponse"" xmlns=""http://plexisweb.com/Plexis.Messages.ClaimAdjudication.Commands""><InternalId>$claim_id</InternalId><SecurityUserCredential>$user</SecurityUserCredential><Username>$user</Username><SendResponse>false</SendResponse></AdjudicateClaim>"


            #--------------------
            # Put the message on the queue
            #--------------------
            $queueMessage = $XML.OuterXml
            $tran.Begin()    
            $msgBytes = $utf8.GetBytes($queueMessage)    
            $msgStream.Write($msgBytes, 0, $msgBytes.Length)    
            $msg.BodyStream = $msgStream
            $msg.Label = ""
            $msg.ResponseQueue = $queue
            $queue.Send($msg, $tran)    
            $tran.Commit()
            }
        }
     Catch
        {
        $ErrorMessage = $_.Exception.Message
        Return $ErrorMessage
        $Success = 0
        }
    Return $Success
    }

Function Start-adjudicationQueue{
    param(
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$targetMachine,
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$user
    )
    <#
        .SYNOPSIS
          Will put a message in the adjudication_queue queue that will triger the system to adjudicate all open claims in the system. 
          $targetMachine will point to the desired QC application server, server running function must have MSMQ
          $user will be user processing claims, should be service account
        .EXAMPLE
          Start-adjudicationQueue -targetMachine "ServerName" -user "UserName"
    #>

    #----------------------------------------------------------------------------
    # Set variables
    #----------------------------------------------------------------------------
    [Reflection.Assembly]::LoadWithPartialName("System.Messaging") | Out-Null
    $Success = 1

    Try
        {

            $msg = new-object System.Messaging.Message
            $msgStream = new-object System.IO.MemoryStream
            $queue = New-Object System.Messaging.MessageQueue
            $queue = New-Object System.Messaging.MessageQueue "FormatName:DIRECT=OS:$targetMachine\private$\qc.service.claimadjudication.adjudicatequeue.input"
            $utf8 = new-object System.Text.UTF8Encoding
            $tran = new-object System.Messaging.MessageQueueTransaction

            #--------------------
            # Adjudicate Queue - Build msg.
            #-------------------- 
            [xml]$XML = "<?xml version=""1.0""?><AdjudicateQueue xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:baseType=""Plexis.Messages.ICommand"" xmlns:baseType1=""Plexis.Messages.IMessage"" xmlns:baseType2=""Plexis.Messages.IReference"" xmlns:baseType3=""Plexis.Messages.IInternalId"" xmlns:baseType4=""Plexis.Messages.IIdentifier"" xmlns=""http://plexisweb.com/Plexis.Messages.ClaimAdjudication.Commands""><InternalId>2</InternalId><Username>$user</Username></AdjudicateQueue>"


            #--------------------
            # Put the message on the queue
            #--------------------
            $queueMessage = $XML.OuterXml
            $tran.Begin()    
            $msgBytes = $utf8.GetBytes($queueMessage)    
            $msgStream.Write($msgBytes, 0, $msgBytes.Length)    
            $msg.BodyStream = $msgStream
            $msg.Label = ""
            $msg.ResponseQueue = $queue
            $queue.Send($msg, $tran)    
            $tran.Commit()
        }
     Catch
        {
        $ErrorMessage = $_.Exception.Message
        Return $ErrorMessage
        $Success = 0
        }
    Return $Success
    }

Function Start-QCMassNegate{
    param(
        [Parameter(Mandatory=$true)]
        [psobject]$claims,
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$targetMachine,
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$user
    )
    <#
        .SYNOPSIS
          Will negate a list of claims supplied to the function in the $claims paramater.
          $targetMachine will point to the desired QC application server, server running function must have MSMQ
          $user will be user processing claims, should be service account
        .EXAMPLE
          Start-QCMassNegate -claims $claimList -targetMachine "ServerName" -user "UserName"
    #>

    #----------------------------------------------------------------------------
    # Set variables
    #----------------------------------------------------------------------------
    [Reflection.Assembly]::LoadWithPartialName("System.Messaging") | Out-Null
    $Success = 1

    Try
        {
        Foreach($i in $claims)
            {
            $claim_id = $i.claim_id

            #----------------------------------------------------------------------------
            # Loop thru each claim ID, build the MSMQ message, put it on the queue.
            #----------------------------------------------------------------------------
            $msg = new-object System.Messaging.Message
            $msgStream = new-object System.IO.MemoryStream
            $queue = New-Object System.Messaging.MessageQueue
            $queue = New-Object System.Messaging.MessageQueue "FormatName:DIRECT=OS:$targetMachine\private$\qc.service.claimadjudication.negateclaim.input"
            $utf8 = new-object System.Text.UTF8Encoding
            $tran = new-object System.Messaging.MessageQueueTransaction

            #--------------------
            # Negate Claim - Build msg.
            #-------------------- 
            [xml]$XML = "<?xml version=""1.0""?><NegateClaim xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:baseType=""Plexis.Messages.ICommand"" xmlns:baseType1=""Plexis.Messages.IMessage"" xmlns:baseType2=""Plexis.Messages.IReference"" xmlns:baseType3=""Plexis.Messages.IInternalId"" xmlns:baseType4=""Plexis.Messages.IIdentifier"" xmlns:baseType5=""Plexis.Messages.ISendResponse"" xmlns=""http://plexisweb.com/Plexis.Messages.ClaimAdjudication.Commands""><InternalId>$claim_id</InternalId><Procedures></Procedures><SecurityUserCredential>$user</SecurityUserCredential><Username>$user</Username><SendResponse>false</SendResponse></NegateClaim>"

            #--------------------
            # Put the message on the queue
            #--------------------
            $queueMessage = $XML.OuterXml
            $tran.Begin()    
            $msgBytes = $utf8.GetBytes($queueMessage)    
            $msgStream.Write($msgBytes, 0, $msgBytes.Length)    
            $msg.BodyStream = $msgStream
            $msg.Label = ""
            $msg.ResponseQueue = $queue
            $queue.Send($msg, $tran)    
            $tran.Commit()
            }
        }
     Catch
        {
        $ErrorMessage = $_.Exception.Message
        Return $ErrorMessage
        $Success = 0
        }
    Return $Success
    }

function execute-sqlagent-job {
    param (
        [Parameter()]
        [String]
        $jobName,
        [parameter()]
        [switch]
        $waitForCompletion
    )
    
    $jobRunning = 0

    #Build job status query
$query = @"
USE [master]


SELECT count(1) as is_running
FROM msdb.dbo.sysjobactivity AS sja
WHERE sja.start_execution_date IS NOT NULL
   AND sja.stop_execution_date IS NULL
   and sja.job_id = '$($job_id.job_id)'

GO

"@;
    

    $job_id = Invoke-Sqlcmd -ServerInstance $QCServerName -Database 'master' -QueryTimeout 0 -ConnectionTimeout 60 -Query "SELECT job_id from msdb.dbo.sysjobs where name = '$jobName'"
    If (-not ([string]::IsNullOrWhitespace($job_id.job_id))){
        $jobRunning = 1
    }
    else {
        write-host "Job not found"
    }

    Invoke-Sqlcmd -ServerInstance $QCServerName -Database 'master' -QueryTimeout 0 -ConnectionTimeout 60 -Query "exec msdb.dbo.sp_start_job @job_id = '$($job_id.job_id)';"

    if ($waitForCompletion) {
        while ($jobRunning -ge 1) {

        $jobRunning = (Invoke-Sqlcmd -ServerInstance $QCServerName -Database 'master' -QueryTimeout 0 -ConnectionTimeout 60 -query $query).is_running
        Write-Host "Job still running. Waiting 5s"
        sleep -Seconds 5
        }
    }


    write-host "$jobName executed"

}

### END FUNCTIONS ###

### Start Timer ###
$timer = [Diagnostics.Stopwatch]::StartNew()


if ($negate_claims){
    $negate_timer = [Diagnostics.Stopwatch]::StartNew()
    Write-Log -text "Negate claim process starting."
    $claimsGetNegate = Get-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'negate' -process_orig_claim $true -limit $claim_limit


    $negateCheck = Start-QCMassNegate -claims $claimsGetNegate -targetMachine $QCWebServerName -user $QCServiceUser
    $NegateQueueCount = (invoke-command -computerName $QCWebServerName -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.negateclaim.input" | Select-Object MessageCount}).MessageCount
    while ($NegateQueueCount -gt 0) {
        Write-Log -text "Current Negate Claim Queue Count at $NegateQueueCount. Waiting for Queue to hit 0 before proceeding"
        sleep 5
        $NegateQueueCount = (invoke-command -computerName $QCWebServerName -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.negateclaim.input" | Select-Object MessageCount}).MessageCount
        if ($timer.elapsed.totalminutes -gt $timeout){
            $timer.stop()
            Write-Log -text "While Negating Claims timeout of $timeout minutes exceeded, exiting script." -force
            exit 2
        }
    }
   
    If ($negateCheck -eq 1)
        {
        Update-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'negate' -process_orig_claim $true -claims $claimsGetNegate
        }
    $negate_timer.stop()
    Write-Log -text "Negate process completed. Total processing time in seconds: $($negate_timer.elapsed.totalseconds)"
}

if ($clear_claims){
    $clear_timer = [Diagnostics.Stopwatch]::StartNew()
    $claimsGetClear = Get-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'clear' -process_orig_claim $process_original_claim -limit $claim_limit
    Write-Host 
    Write-Log -text "Clear claim process starting."
    $clearCheck = Start-QCMassClear -claims $claimsGetClear -targetMachine $qcBatchServer -user $QCServiceUser
    $ClearQueueCount = (invoke-command -computerName $qcBatchServer -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.clearclaimfromadjudication.input" | Select-Object MessageCount}).MessageCount
    while ( $ClearQueueCount -gt 0) {
        Write-Log -text "Current Clear Claim Queue Count at $ClearQueueCount. Waiting for Queue to hit 0 before proceeding"
        sleep 60
        $ClearQueueCount = (invoke-command -computerName $qcBatchServer -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.clearclaimfromadjudication.input" | Select-Object MessageCount}).MessageCount
        if ($timer.elapsed.totalminutes -gt $timeout){
            $timer.stop()
            Write-Log -text "While Clearing claims timeout of $timeout minutes exceeded, exiting script." -force
            exit 2
        }

    }
    If ($clearCheck -eq 1)
    {
    Update-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'clear' -process_orig_claim $process_original_claim -claims $claimsGetClear
    }

        $clear_timer.stop()
        Write-Log -text "Clear claim process completed. Total processing time in seconds: $($clear_timer.elapsed.totalseconds)"
}


if ($run_vendorOverride){
    Write-Log -text "Running Vendor Override"
    Invoke-Sqlcmd -ServerInstance $QCServerName -Database $QCDatabaseName -Query "declare @job_status int EXEC qc_core.dbo.[bind_execute_agent_job_and_wait] @job_name = 'BIND-Network_Override', @job_result = @job_status output" -QueryTimeout 600 

}


if($adj_claims){
    $adj_timer = [Diagnostics.Stopwatch]::StartNew()
    $claimsGetAdj = Get-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'adj' -process_orig_claim $process_original_claim -limit $claim_limit
    Write-Log -text "Adjudicate claim process starting."

    $checkStart = Start-QCMassAdjudicate -claims $claimsGetAdj -targetMachine $qcBatchServer -user $QCServiceUser
    $AdjudQueueCount = (invoke-command -computerName $qcBatchServer -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.adjudicateclaim.input" | Select-Object MessageCount}).MessageCount
    while ( $AdjudQueueCount -gt 0) {
        Write-Log  -text "Current Adjudication Claim Queue Count at $AdjudQueueCount. Waiting for Queue to hit 0 before proceeding"
        sleep 5
        $AdjudQueueCount = (invoke-command -computerName $qcBatchServer -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.adjudicateclaim.input" | Select-Object MessageCount}).MessageCount
        if ($timer.elapsed.totalminutes -gt $timeout){
            $timer.stop()
            Write-Log -text "While adjudicating claims timeout of $timeout minutes exceeded, exiting script." -force
            exit 2
        }
    }
    If ($checkStart -eq 1)
        {
        Update-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'adj' -process_orig_claim $process_original_claim -claims $claimsGetAdj
        }

    $adj_timer.stop()
    Write-Log -text "Adjudicate claims process completed. Total processing time in seconds: $($adj_timer.elapsed.totalseconds)"
}

if($run_adj_queue){
    $queue_timer = [Diagnostics.Stopwatch]::StartNew()
    Write-Log -text "Triggering adjudication queue for claim process."
    $claimsGetAdj = Get-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'adj' -process_orig_claim $process_original_claim -limit $claim_limit

    $checkStart =  Start-adjudicationQueue -targetMachine $QCWebServerName -user $QCServiceUser
    sleep 30 ## This sleep here is to make sure that the we let the system start to load the claims in the queue before we start checking on them. If the below part fails the claims will still process in the queue even we exit.
    $AdjudQueueCount = (invoke-command -computerName $QCWebServerName -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.adjudicateclaim.input" | Select-Object MessageCount}).MessageCount
    while ( $AdjudQueueCount -gt 0) {
        Write-Log  -text "Current Adjudication Claim Queue Count at $AdjudQueueCount. Waiting for Queue to hit 0 before proceeding"
        sleep 5
        $AdjudQueueCount = (invoke-command -computerName $QCWebServerName -scriptBlock {Get-MsmqQueue -Name "qc.service.claimadjudication.adjudicateclaim.input" | Select-Object MessageCount}).MessageCount
        if ($timer.elapsed.totalminutes -gt $timeout){
            $timer.stop()
            Write-Log -text "While adjudicating claims timeout of $timeout minutes exceeded, exiting script." -force
            exit 2
        }
    }
    If ($checkStart -eq 1)
        {
        Update-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName -claimType 'adj' -process_orig_claim $process_original_claim -claims $claimsGetAdj
        }

    $queue_timer.stop()
    Write-Log -text "Adjudication queue completed. Total processing time in seconds: $($queue_timer.elapsed.totalseconds)"
}

$timer.stop()
Write-Log -text "Running Mass Claim Processing Batch($batch_id) completed. Total processing time in seconds: $($timer.elapsed.totalseconds)" -force 
 
 
