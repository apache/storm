# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Resolve links - $PSCommandPath may be a softlink
$PRG = $PSCommandPath;

while((Get-Item $PRG).LinkType -eq "SymbolicLink") {
  $PRG = (Get-Item $PRG).Target;
}

# Check for Python version
$PythonVersion = (& python -V 2>&1)[0].ToString().Split(" ")[1];
$PythonMajor = [int]$PythonVersion.Split(".")[0];
$PythonMinor = [int]$PythonVersion.Split(".")[1];
$PythonNumVersion = $PythonMajor * 10 + $PythonMinor;
if($PythonNumVersion -le 26) {
  echo "Need python version > 2.6";
  exit 1;
}

$STORM_BIN_DIR = Split-Path -Parent $PRG;
$env:STORM_BASE_DIR = Split-Path -Parent $STORM_BIN_DIR;

# Check to see if the conf dir or file is given as an optional argument
if($args.Length -ge 1) {
  if("--config" -eq $args.get(0)) {
    $ConfFile = $args.get(1);
    if(-not (Test-Path $ConfFile)) {
      echo ("Error: Path {0} does not exist" -f $ConfFile);
      exit 1;
    }
    if((Get-Item $ConfFile).PsIsContainer) {
      $ConfFile=[io.path]::combine($ConfFile, "storm.yaml");
    }
    if(-not (Test-Path $ConfFile)) {
      echo ("Error: Path {0} does not exist" -f $ConfFile);
      exit 1;
    }
    $STORM_CONF_FILE = $ConfFile;
    $STORM_CONF_DIR = Split-Path -Parent $STORM_CONF_FILE;
  } 
}

$env:STORM_CONF_DIR = if($STORM_CONF_DIR -ne $null) { $STORM_CONF_DIR; } else { [io.path]::combine($env:STORM_BASE_DIR, "conf"); }
$env:STORM_CONF_FILE = if($STORM_CONF_FILE -ne $null) { $STORM_CONF_FILE; } else { [io.path]::combine($env:STORM_BASE_DIR, "conf", "storm.yaml"); }

$StormEnvPath = [io.path]::combine($env:STORM_CONF_DIR, "storm-env.ps1");
if(Test-Path $StormEnvPath) {
  . $StormEnvPath;
}

& ([io.path]::combine("$STORM_BIN_DIR", "storm.py")) $args;

exit $LastExitCode