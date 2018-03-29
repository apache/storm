#!/bin/bash

pwd=$(pwd)

echo "Cleaning up output folder..."
rm -rf ./output/*

echo "Building $1..."
if [[ "$1" = "adapter" || "$1" = "example" ]]; then
    dotnet publish $pwd/src/main/dotnet/Dotnet.Storm.Adapter/Dotnet.Storm.Adapter.csproj -o $pwd/output/resources/ -v m
fi
if [[ "$1" = "example" ]]; then
    dotnet publish $pwd/src/main/dotnet/Dotnet.Storm.Example/Dotnet.Storm.Example.csproj -o $pwd/output/resources/ -v m
fi
if [[ "$1" = "nuget" ]]; then
    dotnet pack $pwd/src/main/dotnet/Dotnet.Storm.Adapter/Dotnet.Storm.Adapter.csproj -o $pwd/output/resources/ -v m
fi
if [[ ! "$1" = "nuget" && ! "$1" = "adapter" && ! "$1" = "example" ]]; then
  echo "usage: build.sh [adapter|example|nuget]"
fi
