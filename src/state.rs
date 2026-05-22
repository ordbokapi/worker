// SPDX-FileCopyrightText: Copyright (C) 2026 Adaline Simonian
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of Ordbok API.
//
// Ordbok API is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option) any
// later version.
//
// Ordbok API is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
// details.
//
// You should have received a copy of the GNU Affero General Public License
// along with Ordbok API. If not, see <https://www.gnu.org/licenses/>.

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Sync status for an entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    Idle,
    PendingFetch,
    PendingIndex,
}

impl SyncStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::PendingFetch => "pending_fetch",
            Self::PendingIndex => "pending_index",
        }
    }
}

impl std::str::FromStr for SyncStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "idle" => Ok(Self::Idle),
            "pending_fetch" => Ok(Self::PendingFetch),
            "pending_index" => Ok(Self::PendingIndex),
            _ => Err(()),
        }
    }
}

impl fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Dictionary variants for UiB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum UibDictionary {
    Bokmål,
    Nynorsk,
    NorskOrdbok,
}

impl UibDictionary {
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[Self::Bokmål, Self::Nynorsk, Self::NorskOrdbok]
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bokmål => "bm",
            Self::Nynorsk => "nn",
            Self::NorskOrdbok => "no",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "bm" => Some(Self::Bokmål),
            "nn" => Some(Self::Nynorsk),
            "no" => Some(Self::NorskOrdbok),
            _ => None,
        }
    }
}

impl ValueEnum for UibDictionary {
    fn value_variants<'a>() -> &'a [Self] {
        Self::all()
    }

    fn from_str(input: &str, _ignore_case: bool) -> Result<Self, String> {
        Self::parse(input).ok_or_else(|| format!("Unknown dictionary: {input}"))
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(clap::builder::PossibleValue::new(self.as_str()))
    }
}

impl fmt::Display for UibDictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
