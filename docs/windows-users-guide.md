---
title: Windows Users Guide
layout: documentation
documentation: true
---

This page guides how to set up environment on Windows for Apache Storm.

## Symbolic Link

Starting at 1.0.0, Apache Storm utilizes `symbolic link` to aggregate log directory and resource directory into worker directory.
Unfortunately, `creating symbolic link` on Windows needs non-default privilege, so users should configure it manually to make sure Storm processes can create symbolic link on runtime.
Depending on the Windows version (i.e. non-professional), setting symbolic links privilege by a security policy is not possible since the tool is not installed.

When creating a symbolic link is not possible, the Supervisor process will stop as soon as it tries to start workers since the permission exception is considered a fatal error.

Below pages (MS technet) guide how to configure that policy to the account which Storm runs on.

* [How to Configure Security Policy Settings](https://technet.microsoft.com/en-us/library/dn452420.aspx)
* [Create symbolic links](https://technet.microsoft.com/en-us/library/dn221947.aspx)

One tricky point is, `administrator` group already has this privilege, but it's activated only process is run as `administrator` account.
So if your account belongs to `administrator` group (and you don't want to change it), you may want to open `command prompt` with `run as administrator` and execute processes within that console.
If you don't want to execute Storm processes directly (not on command prompt), please execute processes with `runas /user:administrator` to run as administrator account.

Starting with Windows 10 Creators Update, it will be possible to activate a Developer Mode that supports creating symbolic links without `run as administrator`
[Symlinks in Windows 10!](https://blogs.windows.com/buildingapps/2016/12/02/symlinks-windows-10/)
