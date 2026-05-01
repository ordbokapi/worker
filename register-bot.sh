#!/bin/sh

# SPDX-FileCopyrightText: Copyright (C) 2026 Adaline Simonian
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Ordbok API.
#
# Ordbok API is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# Ordbok API is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Ordbok API. If not, see <https://www.gnu.org/licenses/>.

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
