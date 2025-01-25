#!/bin/sh

if ! [ -x "$(command -v pwsh)" ]; then
  echo >&2
  echo 'Error: PowerShell is not installed, but the bot registration script requires it to run.' >&2
  echo 'You can install PowerShell from' >&2
  echo >&2
  printf "  \033]8;;https://github.com/PowerShell/PowerShell/releases\ahttps://github.com/PowerShell/PowerShell/releases\033]8;;\a\n" >&2
  echo >&2
  echo 'For more information, see' >&2
  echo >&2
  printf "  \033]8;;https://learn.microsoft.com/powershell/scripting/install/installing-powershell\ahttps://learn.microsoft.com/powershell/scripting/install/installing-powershell\033]8;;\a\n" >&2
  echo >&2
  exit 1
fi

pwsh -File ./register-bot.ps1
