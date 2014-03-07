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


function Main( $scriptDir )
{
    Write-Log "Uninstalling Apache storm storm-@storm.version@"
    ###
    ### Stop and delete services
    ###
    
    foreach( $service in ("supervisor", "nimbus"))
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
    Uninstall "storm" $ENV:HADOOP_NODE_INSTALL_ROOT
    Write-Log "Finished Uninstalling Apache storm"
}

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("storm") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
finally
{
    if( $apiModule -ne $null )
    {        
        Remove-Module $apiModule
    }

    if( $utilsModule -ne $null )
    {        
        Remove-Module $utilsModule
    }
}
