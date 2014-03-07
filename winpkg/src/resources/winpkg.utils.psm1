### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.


### NOTE: This file is common across the Windows installer projects for Hadoop Core, Hive, and Pig.
### This dependency is currently managed by convention.
### If you find yourself needing to change something in this file, it's likely that you're
### either doing something that's more easily done outside this file or is a bigger change
### that likely has wider ramifications. Work intentionally.


param( [parameter( Position=0, Mandatory=$true)]
       [String]  $ComponentName )

function Write-Log ($message, $level, $pipelineObj )
{
    $message = SanitizeString $message
    switch($level)
    {
        "Failure" 
        {
            $message = "$ComponentName FAILURE: $message"
            Write-Error $message 
            break;
        }

        "Info"
        {
            $message = "${ComponentName}: $message"
            Write-Host $message
            break;
        }

        default
        {
            $message = "${ComponentName}: $message"
            Write-Host "$message"
        }
    }

    
    Out-File -FilePath $ENV:WINPKG_LOG -InputObject "$message" -Append -Encoding "UTF8"

    if( $pipelineObj -ne $null )
    {
        Out-File -FilePath $ENV:WINPKG_LOG -InputObject $pipelineObj.InvocationInfo.PositionMessage -Append -Encoding "UTF8"
    }
}

function Write-LogRecord( $source, $record )
{
    if( $record -is [Management.Automation.ErrorRecord])
    {
        $message = "$ComponentName-$source FAILURE: " + $record.Exception.Message
        $message = SanitizeString $message

        if( $message.EndsWith( [Environment]::NewLine ))
        {
            Write-Host $message -NoNewline
            [IO.File]::AppendAllText( "$ENV:WINPKG_LOG", "$message", [Text.Encoding]::UTF8 )
        }
        else
        {
            Write-Host $message
            Out-File -FilePath $ENV:WINPKG_LOG -InputObject $message -Append -Encoding "UTF8"
        }
    }
    else
    {
        $message = $record
        $message = SanitizeString $message
        Write-Host $message
        Out-File -FilePath $ENV:WINPKG_LOG -InputObject "$message" -Append -Encoding "UTF8"
    }
}

function SanitizeString($s)
{
    $s -replace "-password([a-z0-9]*) (\S*)", '-password$1 ****'
}

function Invoke-Cmd ($command)
{
    Write-Log $command
    $out = cmd.exe /C "$command" 2>&1
    $out | ForEach-Object { Write-LogRecord "CMD" $_ }
    return $out
}

function Invoke-CmdChk ($command)
{
    Write-Log $command
    $out = cmd.exe /C "$command" 2>&1
    $out | ForEach-Object { Write-LogRecord "CMD" $_ }
    if (-not ($LastExitCode  -eq 0))
    {
        throw "Command `"$out`" failed with exit code $LastExitCode "
    }
    return $out
}

function Invoke-Ps ($command)
{
    Write-Log $command
    $out = powershell.exe -InputFormat none -Command "$command" 2>&1
    #$out | ForEach-Object { Write-LogRecord "PS" $_ }
    return $out
}

function Invoke-PsChk ($command)
{
    Write-Log $command
    $out = powershell.exe -InputFormat none -Command $command 2>&1
    $captureExitCode = $LastExitCode
	$out | ForEach-Object { Write-LogRecord "PS" $_ }
	if (-not ($captureExitCode  -eq 0))
    {
        throw "Command `"$command`" failed with exit code  $captureExitCode. Output is  `"$out`" "
    }
    return $out
}

### Sets HADOOP_NODE_INSTALL_ROOT if unset
### Initializes Winpkg Environment (ENV:WINPKG_LOG and ENV:WINPKG_BIN)
### Tests for Admin

function Initialize-InstallationEnv( $scriptDir, $logFilename )
{
    $HDP_INSTALL_PATH = $scriptDir
    $HDP_RESOURCES_DIR = Resolve-Path "$HDP_INSTALL_PATH\..\resources"

    if( -not (Test-Path ENV:HADOOP_NODE_INSTALL_ROOT))
    {
        $ENV:HADOOP_NODE_INSTALL_ROOT = "c:\hadoop"
    }
	
    Check-Drive $ENV:HADOOP_NODE_INSTALL_ROOT "HADOOP_NODE_INSTALL_ROOT"

    if( -not (Test-Path ENV:WINPKG_LOG ))
    {
        throw "ENV:WINPKG_LOG not set"
    }
    else
    {
        Write-Log "Logging to existing log $ENV:WINPKG_LOG" "Info"
    }

    Write-Log "Logging to $ENV:WINPKG_LOG" "Info"
    Write-Log "HDP_INSTALL_PATH: $HDP_INSTALL_PATH"
    Write-Log "HDP_RESOURCES_DIR: $HDP_RESOURCES_DIR"

    $currentPrincipal = New-Object Security.Principal.WindowsPrincipal( [Security.Principal.WindowsIdentity]::GetCurrent( ) )
    if ( -not ($currentPrincipal.IsInRole( [Security.Principal.WindowsBuiltInRole]::Administrator ) ) )
    {
        throw "install script must be run elevated"
    }

    return $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR
}

function Test-JavaHome
{
    if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
    {
        throw "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist"
    }
}

### Add service control permissions to authenticated users.
### Reference:
### http://stackoverflow.com/questions/4436558/start-stop-a-windows-service-from-a-non-administrator-user-account 
### http://msmvps.com/blogs/erikr/archive/2007/09/26/set-permissions-on-a-specific-service-windows.aspx

function Set-ServiceAcl ($service)
{
    $cmd = "sc sdshow $service"
    $sd = Invoke-Cmd $cmd

    Write-Log "Current SD: $sd"

    ## A;; --- allow
    ## RP ---- SERVICE_START
    ## WP ---- SERVICE_STOP
    ## CR ---- SERVICE_USER_DEFINED_CONTROL    
    ## ;;;AU - AUTHENTICATED_USERS

    $sd = [String]$sd
    $sd = $sd.Replace( "S:(", "(A;;RPWPCR;;;AU)S:(" )
    Write-Log "Modifying SD to: $sd"

    $cmd = "sc sdset $service $sd"
    Invoke-Cmd $cmd
}

function Expand-String([string] $source)
{
    return $ExecutionContext.InvokeCommand.ExpandString((($source -replace '"', '`"') -replace '''', '`'''))
}

function Copy-XmlTemplate( [string][parameter( Position=1, Mandatory=$true)] $Source,
       [string][parameter( Position=2, Mandatory=$true)] $Destination,
       [hashtable][parameter( Position=3 )] $TemplateBindings = @{} )
{

    ### Import template bindings
    Push-Location Variable:

    foreach( $key in $TemplateBindings.Keys )
    {
        $value = $TemplateBindings[$key]
        New-Item -Path $key -Value $ExecutionContext.InvokeCommand.ExpandString( $value ) | Out-Null
    }

    Pop-Location

    ### Copy and expand
    $Source = Resolve-Path $Source

    if( Test-Path $Destination -PathType Container )
    {
        $Destination = Join-Path $Destination ([IO.Path]::GetFilename( $Source ))
    }

    $DestinationDir = Resolve-Path (Split-Path $Destination -Parent)
    $DestinationFilename = [IO.Path]::GetFilename( $Destination )
    $Destination = Join-Path $DestinationDir $DestinationFilename

    if( $Destination -eq $Source )
    {
        throw "Destination $Destination and Source $Source cannot be the same"
    }

    $template = [IO.File]::ReadAllText( $Source )
    $expanded = Expand-String( $template )
    ### Output xml files as ANSI files (same as original)
    write-output $expanded | out-file -encoding ascii $Destination
}

# Convenience method for processing command-line credential objects
# Assumes $credentialsHash is a hash with one of the following being true:
#  - keys "username" and "password"/"passwordBase64" are set to strings
#  - key "credentialFilePath" is set to the path of a serialized PSCredential object
function Get-HadoopUserCredentials($credentialsHash)
{
    if($credentialsHash["username"])
    {
        Write-Log "Using provided credentials for username $($credentialsHash["username"])" | Out-Null
        $username = $credentialsHash["username"]
        if($username -notlike "*\*")
        {
            $username = "$ENV:COMPUTERNAME\$username"
        }
        if($credentialsHash["passwordBase64"])
        {
            $base64Password = $credentialsHash["passwordBase64"]
            $decoded = [System.Convert]::FromBase64String($base64Password);
            $decodedPassword = [System.Text.Encoding]::UTF8.GetString($decoded);
            $securePassword = $decodedPassword | ConvertTo-SecureString -AsPlainText -Force
        }
        else
        {
            $securePassword = $credentialsHash["password"] | ConvertTo-SecureString -AsPlainText -Force
        }
    }
    else
    {
        Write-Log "Reading credentials from $($credentialsHash['credentialFilePath'])" | Out-Null
        $import = Import-Clixml -Path $credentialsHash["credentialFilePath"]
        $username = $import.Username
        $securePassword = $import.Password | ConvertTo-SecureString
    }

    $creds = New-Object System.Management.Automation.PSCredential $username, $securePassword
    return $creds
}

function Check-Drive($path, $message){
    if($path -cnotcontains ":"){
        Write-Warning "Target path doesn't contains drive identifier, checking skipped"
        return
    }
    $pathvolume = $path.Split(":")[0] + ":"
    if (! (Test-Path ($pathvolume + '\')) ) {
        throw "Target volume for $message $pathvolume doesn't exist"
    }
}

Export-ModuleMember -Function Copy-XmlTemplate
Export-ModuleMember -Function Get-HadoopUserCredentials
Export-ModuleMember -Function Initialize-WinpkgEnv
Export-ModuleMember -Function Initialize-InstallationEnv
Export-ModuleMember -Function Invoke-Cmd
Export-ModuleMember -Function Invoke-CmdChk
Export-ModuleMember -Function Invoke-Ps
Export-ModuleMember -Function Invoke-PsChk
Export-ModuleMember -Function Set-ServiceAcl
Export-ModuleMember -Function Test-JavaHome
Export-ModuleMember -Function Write-Log
