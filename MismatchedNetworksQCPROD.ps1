$QCServerName = 'plx-sqldev\qcprod'
$QCDatabaseName = 'qc_core'
$QCWebServerName = 'plx-qcprod'
$QCServiceUser = 'bind\qcprod'
 $adjudicateClaim = $false

Function Get-QCClaims ($serverName, $databaseName)
    {
    <#
        .SYNOPSIS
          Will retrieve a list of claims in the Medcore database that have been loaded into a custom table.
          The $adjType paramater is used to differentiate between Clear and Adjudication.
        .EXAMPLE
          Get-QCCalims -claimType "Clear" -serverName "ServerName" -databaseName "DatabaseName"
          Get-QCCalims -claimType "Adj" -serverName "ServerName" -databaseName "DatabaseName"
    #>
    Try
 {
            $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
            $sqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $sqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
            $return = New-Object System.Data.Datatable
            $sqlConnection.ConnectionString = "Data Source=$serverName;Initial Catalog=$databaseName;Integrated Security=SSPI"

            $sqlConnection.Open()
            $sqlCommand.Connection = $sqlConnection
            $sqlCommand.CommandText = "SELECT claim_id FROM bind_mismatched_network_processing WHERE to_process = 1" 
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

Function Start-QCMassClear($claims, $targetMachine, $user)
    {
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

Function Start-QCMassAdjudicate($claims, $targetMachine, $user)
    {
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
            $queue = New-Object System.Messaging.MessageQueue "FormatName:DIRECT=OS:$targetMachine\private$\qc.service.claimadjudication.adjudicateclaim.input"
            $utf8 = new-object System.Text.UTF8Encoding
            $tran = new-object System.Messaging.MessageQueueTransaction

            #--------------------
            # Clear Adjudication - Build msg.
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

Function Update-QCClaims ($serverName, $databaseName)
    {
    <#
        .SYNOPSIS
          Will retrieve a list of claims in the Medcore database that have been loaded into a custom table.
          The $adjType paramater is used to differentiate between Clear and Adjudication.
        .EXAMPLE
          Get-QCCalims -claimType "Clear" -serverName "ServerName" -databaseName "DatabaseName"
          Get-QCCalims -claimType "Adj" -serverName "ServerName" -databaseName "DatabaseName"
    #>
    Try
 {
            $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
            $sqlCommand = New-Object System.Data.SqlClient.SqlCommand
            $sqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
            $return = New-Object System.Data.Datatable
            $sqlConnection.ConnectionString = "Data Source=$serverName;Initial Catalog=$databaseName;Integrated Security=SSPI"

            $sqlConnection.Open()
            $sqlCommand.Connection = $sqlConnection
            $sqlCommand.CommandText = "UPDATE bind_mismatched_network_processing
SET to_process = 0
WHERE to_process = 1" 
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

$claimsGet = Get-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName

$ClaimcheckStart = Start-QCMassClear  -claims $claimsGet -targetMachine $QCWebServerName -user $QCServiceUser

if ($ClaimcheckStart -eq 1 -and $adjudicateClaim)
  {
    $Checkstart = start-QCMassAdjudicate -claims $claimsGet -targetMachine $QCWebServerName -user $QCServiceUser
  }



If ($checkStart -eq 1 -or $adjudicateClaim -eq $false)
    {
    Update-QCClaims -serverName $QCServerName -databaseName $QCDatabaseName
    } 