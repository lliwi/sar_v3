param(
    [string]$SourceDomain = "",
    [string]$TargetDomain = "AUDI"
)

Clear-Host
# Input and output files paths
$inputFile = "C:\Temp\BarcellonaADGroups\membershipList.csv"
$outputFile = "C:\Temp\BarcellonaADGroups\changeMembership.csv"
$logfile = "C:\Temp\BarcellonaADGroups\log.txt"

# First of all, we need to load QuestAD PS module and connect to the ARS endpoint
Write-Host "Loading QuestAD PowerShell module..."
"Loading QuestAD PowerShell module..." | Out-File $logfile -Append
#Import-Module Questad -DisableNameChecking
Import-Module ActiveRolesConfiguration -DisableNameChecking

# R99 cred
$User4 = "R99\W6TRHF1"
$PasswordFile4 = "C:\Temp\Password.txt" 
$KeyFile4 = "C:\Temp\AES.key"
$key4 = Get-Content $KeyFile4
$cred4 = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $User4, (Get-Content $PasswordFile4 | ConvertTo-SecureString -Key $key4)



# Import of SQL snippets
Import-Module SqlServer
cd SQLSERVER:\SQL\ITDGBASAX000004\DEFAULT




# Output file CSV delimiter and header
$csvDelimiter = ";"
$outputHeader = "UserName;ADGroup;VerifyOK"
$outputHeader | Out-File $outputFile


# Seconds to sleep before verifying membership changes
$secondsToSleep = 30

#Clear and initt log file
$date=date
"Executed on $date`r`n"| Out-File $logfile
 
# If input file exists, then we can proceed
if (Test-Path $inputFile){


    Write-Host "`r`nConnecting to ARS service endpoint..."
    "`r`nConnecting to ARS service endpoint..." | Out-File $logfile -Append 
    $null = Connect-QADService -Service 'appars.ads.vwgroup.com' -Credential $cred4 -Proxy


	# Then the input file is loaded and processed line-by-line
	Write-Host "`r`nLoading file $inputFile...`r`n"
    "`r`nLoading file $inputFile...`r`n" | Out-File $logfile -Append
	$csvContent = Import-CSV -Delimiter $csvDelimiter $inputFile


	foreach ($csvLine in $csvContent){

		# Username and group name are extracted from the current line
		if ($SourceDomain -and $SourceDomain -ne "") {
			$groupName = $csvLine.ADGroup.replace("$SourceDomain\\","$TargetDomain\\")
		} else {
			$groupName = $csvLine.ADGroup
		}
		$userName = "$TargetDomain\" + $csvLine.UserName
        $taskId = $csvLine.idTarea
        $actionId = $csvLine.idAccion
        $matriculaUsu = $csvLine.MatriculaUsu
        $idRecurso = $csvLine.idRecurso
        $idModo = $csvLine.idModo

		# Membership change operation, with execution result evaluation
		
        switch($actionId)
        {
		   1 { 
                Write-Host "Adding $userName to $groupName..."
                "Adding $userName to $groupName..." | Out-File $logfile -Append
                try{
			        $null = Add-QADGroupMember -Identity $groupName -Member $userName
		        }catch{
    			    Write-Host "Failure while trying to add $userName to $groupName"
                    "Failure while trying to add $userName to $groupName" | Out-File $logfile -Append
		        }
              }

            2 { 
                Write-Host "Removing $userName from $groupName..."
                "Removing $userName to $groupName..." | Out-File $logfile -Append
                try{
			        $null = Remove-QADGroupMember -Identity $groupName -Member $userName
		        }catch{
    			    Write-Host "Failure while trying to remove $userName to $groupName"
                    "Failure while trying to remove $userName to $groupName" | Out-File $logfile -Append
		        }
              }
            default {"Failed invalid action for task $taskId" | Out-File $logfile -Append} 
        }
	}
	
	# Sleeping for $secondsToSleep seconds, before performing membership change evaluation. A progress string informs about remaining time
    "Sleeping for $secondsToSleep seconds" | Out-File $logfile -Append
	foreach ($seconds in (1..$secondsToSleep)){
		Write-Progress -Activity "Sleeping $secondsToSleep seconds before verifying new memberships..." -Status "$($secondsToSleep - $seconds) seconds left"
		Start-Sleep -Seconds 1
	}

	# Re-reading input file line-by-line
	foreach ($csvLine in $csvContent){
		if ($SourceDomain -and $SourceDomain -ne "") {
			$groupName = $csvLine.ADGroup.replace("$SourceDomain\\","$TargetDomain\\")
		} else {
			$groupName = $csvLine.ADGroup
		}
		$userName = "$TargetDomain\" + $csvLine.UserName
        $taskId = $csvLine.idTarea
        $actionId = $csvLine.idAccion
        $matriculaUsu = $csvLine.MatriculaUsu
        $idRecurso = $csvLine.idRecurso
        $idModo = $csvLine.idModo

		Write-Host "`r`nVerifying new membership for $userName..."
        "`r`nVerifying new membership for $userName..." | Out-File $logfile -Append

		# Membership change evaluation, by splitting AD group into domain name and account name
		$groupData = $groupName.Split("\")
		$groupAccName = $groupData[1]
		$isMember = Get-QADMemberOf -Identity $userName -Name $groupAccName

        switch($actionId)
        {
		   1 {
		
		        if ($isMember -eq $null)
                {
			        Write-Host "Membership has not been verified"
                    "Membership has not been verified" | Out-File $logfile -Append
                    Invoke-Sqlcmd -Query "update [SAR_2].[dbo].[TAREAS]  set [idEstado] =2, [idTipo_crea]=1 where [idTarea] = $taskId"
			        $verifyOK = $false
		        }
        		else
                {
			        Write-Host "Memberhip has been verified"
                    "Membership has been verified" | Out-File $logfile -Append
                    Invoke-Sqlcmd -Query "update [SAR_2].[dbo].[TAREAS]  set [idEstado] =3 where [idTarea] =$taskId"
                    Invoke-Sqlcmd -Query "insert into [SAR_2].[dbo].[ACCESOS] ([MatriculaUsu] ,[IdRecurso]  ,[IdModo]  ,[Fecha_crea]) values ($matriculaUsu ,$idRecurso  ,$idModo  , CURRENT_TIMESTAMP)"
			        $verifyOK = $true
                }
		      }
            2 {
		
		        if ($isMember -eq $null)
                {
			        Write-Host "Membership has not been verified"
                   "Membership has  not been verified" | Out-File $logfile -Append
                   Invoke-Sqlcmd -Query "update [SAR_2].[dbo].[TAREAS]  set [idEstado] =3 where [idTarea] =$taskId"
                   Invoke-Sqlcmd -Query "delete from [SAR_2].[dbo].[ACCESOS] where [MatriculaUsu] = $matriculaUsu and [IdRecurso] = $idRecurso and [IdModo] = $idModo"
                 
			        $verifyOK = $true
		        }
        		else
                {
			        Write-Host "Memberhip has been verified"
                    "Membership has been verified" | Out-File $logfile -Append
                    Invoke-Sqlcmd -Query "update [SAR_2].[dbo].[TAREAS]  set [idEstado] =2, [idTipo_crea]=1 where [idTarea] = $taskId"

			        $verifyOK = $false
                }
		     }
        }

		# Results are dumped into the output file
		"$userName;$groupName;$verifyOK" | Out-File $outputFile -Append
	}

}
# If input file doesn't exist, we can't proceed
else{
	Write-Host "Missing input file $inputFile"
    "Missing input file $inputFile" | Out-File $logfile -Append
}
Write-Host "`r`nExecution completed`r`n"
"`r`nExecution completed`r`n" | Out-File $logfile -Append