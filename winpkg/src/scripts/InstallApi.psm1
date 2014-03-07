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

###
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)

$FinalName = "storm-@storm.version@"

###############################################################################
###
### Installs storm.
###
### Arguments:
###     component: Component to be installed, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: Space separated list of roles that should be installed.
###           (for example, "jobtracker historyserver" for mapreduce)
###
###############################################################################

function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $roles
    )
{

	
    if ( $component -eq "storm" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
        Write-Log "Checking the JAVA Installation."
        if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
        {
            Write-Log "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist" "Failure"
            throw "Install: JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist."
        }

        Write-Log "Checking the Hadoop Installation."
        if( -not (Test-Path $ENV:HADOOP_HOME\bin\winutils.exe))
        {
          Write-Log "HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist" "Failure"
          throw "Install: HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist."
        }        

        ### $stormInstallPath: the name of the folder containing the application, after unzipping
        $stormInstallPath = Join-Path $nodeInstallRoot $FinalName
        $stormInstallToBin = Join-Path "$stormInstallPath" "bin"
		
	    Write-Log "Installing Apache $FinalName to $stormInstallPath"

        ### Create Node Install Root directory
        if( -not (Test-Path "$stormInstallPath"))
        {
            Write-Log "Creating Node Install Root directory: `"$stormInstallPath`""
            $cmd = "mkdir `"$stormInstallPath`""
            Invoke-CmdChk $cmd
        }

        $stormIntallPathParent = (Get-Item $stormInstallPath).parent.FullName
		
        ###
        ###  Unzip storm distribution from compressed archive
        ###
		
        Write-Log "Extracting $FinalName.zip to $stormIntallPathParent"
        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$FinalName.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$stormIntallPathParent`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$FinalName.zip")
            $destinationFolder = $shellApplication.NameSpace($stormInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }
		
        ###
        ### Set storm_HOME environment variable
        ###
        Write-Log "Setting the storm_HOME environment variable at machine scope to `"$stormInstallPath`""
        [Environment]::SetEnvironmentVariable("storm_HOME", $stormInstallPath, [EnvironmentVariableTarget]::Machine)
        $ENV:storm_HOME = "$stormInstallPath"
		
		if ($roles) { 

		###
		### Create storm Windows Services and grant user ACLS to start/stop
		###
		Write-Log "Node storm Role Services: $roles"

		### Verify that roles are in the supported set	
		CheckRole $roles @("supervisor" "nimbus")
		Write-Log "Role : $roles"
		foreach( $service in empty-null ($roles -Split('\s+')))
		{
			CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $stormInstallToBin $serviceCredential
		}
	  
 	     ### end of roles loop
        }
	    Write-Log "Finished installing Apache storm"
    }
    else
    {
        throw "Install: Unsupported component argument."
    }
}


###############################################################################
###
### Uninstalls Hadoop component.
###
### Arguments:
###     component: Component to be uninstalled, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################
        
function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "storm" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

	    Write-Log "Uninstalling Apache storm $FinalName"
	    $stormInstallPath = Join-Path $nodeInstallRoot $FinalName

        ### If Hadoop Core root does not exist exit early
        if ( -not (Test-Path $stormInstallPath) )
        {
            return
        }
		
        ### Stop and delete services
        ###
        foreach( $service in ("supervisor nimbus"))
        {
            StopAndDeleteHadoopService $service
        }

	    ###
	    ### Delete install dir
	    ###
	    $cmd = "rd /s /q `"$stormInstallPath`""
	    Invoke-Cmd $cmd

        ### Removing storm_HOME environment variable
        Write-Log "Removing the storm_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "storm_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Successfully uninstalled storm"
        
    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "storm" )
    {
        Write-Log "StartService: storm services"
		CheckRole $roles @("supervisor" "nimbus")

        foreach ( $role in $roles -Split("\s+") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    else
    {
        throw "StartService: Unsupported component argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "storm" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("supervisor" "nimbus")
        foreach ( $role in $roles -Split("\s+") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                    Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }

        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the storm component.
###
### Arguments:
###     component: Component to be configured, it should be "storm"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs: 
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{
    
    if ( $component -eq "storm" )
    {
        Write-Log "Configure: storm does not have any configurations"
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}


### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Gives full permissions on the folder to the given user 
function GiveFullPermissions(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $folder,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $username,
    [bool]
    [Parameter( Position=2, Mandatory=$false )]
    $recursive = $false)
{
    Write-Log "Giving user/group `"$username`" full permissions to `"$folder`""
    $cmd = "icacls `"$folder`" /grant ${username}:(OI)(CI)F"
    if ($recursive) {
        $cmd += " /T"
    }
    Invoke-CmdChk $cmd
}

### Checks if the given space separated roles are in the given array of
### supported roles.
function CheckRole(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $roles,
    [array]
    [parameter( Position=1, Mandatory=$true )]
    $supportedRoles
    )
{
    foreach ( $role in $roles.Split(" ") )
    {
        if ( -not ( $supportedRoles -contains $role ) )
        {
            throw "CheckRole: Passed in role `"$role`" is outside of the supported set `"$supportedRoles`""
        }
    }
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
		 Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$HDP_RESOURCES_DIR\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd
		
        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
        if ( $s -eq $null )
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= disabled"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, skipping service creation"
    }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue 

    if( $s -ne $null )
    {
        Stop-Service $service
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
