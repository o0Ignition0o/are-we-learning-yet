use std::fmt::Display;

use anyhow::Result as AnyResult;
use chrono::{DateTime, Utc};
use crates_io_api::Crate;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{crates::CratesIo, github::RepoData};

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Topic {
    Communication,
    Drones,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "kind")]
pub enum InputCrateInfo {
    CratesIo(CratesIoInputCrateInfo),
    Manual(ManualCrateInfo),
}

impl Display for InputCrateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, from) = match &self {
            Self::CratesIo(c) => {
                if let Some(name) = &c.name {
                    (name.clone(), "crates.io")
                } else if let Some(repo) = &c.repository {
                    (repo.to_string(), "source code repository")
                } else {
                    (format!("Invalid entry: {:#?}", c), "")
                }
            }
            Self::Manual(m) => {
                let name = if let Some(k) = &m.krate {
                    k.name.clone()
                } else {
                    if let Some(repo) = &m.repo {
                        repo.name.clone()
                    } else {
                        "unknown crate name".to_string()
                    }
                };

                (name, "populated manually")
            }
        };

        write!(f, "{} from {}", name, from)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ManualCrateInfo {
    pub topics: Vec<Topic>,
    pub score: Option<u64>,
    #[serde(rename = "crate")]
    pub krate: Option<Crate>,
    pub repo: Option<RepoData>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CratesIoInputCrateInfo {
    pub name: Option<String>,
    pub topics: Vec<Topic>,

    //overridable crate fields
    pub documentation: Option<String>,
    pub repository: Option<Url>,
    pub license: Option<String>,
    pub description: Option<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct GeneratedCrateInfo {
    pub topics: Vec<Topic>,
    pub score: Option<u64>,

    #[serde(rename = "meta", skip_serializing_if = "Option::is_none")]
    pub krate: Option<Crate>,

    #[serde(rename = "repo", skip_serializing_if = "Option::is_none")]
    pub repo: Option<RepoData>,
}

impl GeneratedCrateInfo {
    pub async fn try_from(input: InputCrateInfo) -> AnyResult<Self> {
        match input {
            InputCrateInfo::CratesIo(input) => GeneratedCrateInfo::from_crates_io(input).await,
            InputCrateInfo::Manual(input) => Ok(GeneratedCrateInfo::fill_manually(input)),
        }
    }

    fn fill_manually(krate: ManualCrateInfo) -> Self {
        Self {
            topics: krate.topics,
            score: None,
            repo: krate.repo,
            krate: krate.krate,
        }
    }

    async fn from_crates_io(krate: CratesIoInputCrateInfo) -> AnyResult<Self> {
        let mut this = Self {
            topics: krate.topics,
            score: None,
            krate: None,
            repo: None,
        };

        if let Some(crate_name) = krate.name {
            let crates_io = CratesIo::new()?;

            match crates_io.get_crate_data(crate_name.as_str()).await {
                Ok(mut data) => {
                    data.license = data.license.or(krate.license);
                    data.documentation = data.documentation.or(krate.documentation);

                    if data.documentation.is_none() {
                        data.documentation = Some(format!("https://docs.rs/crate/{}", data.name));
                    }
                    data.repository = data
                        .repository
                        .or(krate.repository.map(|repo| repo.to_string()));
                    data.description = data.description.or(krate.description);

                    this.krate = Some(data);
                }
                Err(err) => {
                    eprintln!("Error getting crate data for {} - {}", crate_name, err);
                }
            }
        }

        Ok(this)
    }
}

impl GeneratedCrateInfo {
    //   In calculating last_activity, we only scrape last_commit for github-based crates
    //   so this is unfair to projects that host source elsewhere.
    //   This is slightly mitigated by falling back to the last crate publish date
    fn last_activity(&self) -> Option<DateTime<Utc>> {
        let mut last_activity = self.krate.as_ref().map(|k| k.updated_at);

        if let Some(last_commit) = self.repo.as_ref().map(|r| r.last_commit) {
            if Some(last_commit) > last_activity {
                last_activity = Some(last_commit);
            }
        }

        last_activity
    }

    pub fn update_score(&mut self) {
        if self.krate.is_none() {
            // crate is not published to crates.io
            self.score = Some(0);
        }

        let coefficient = match self.last_activity() {
            None => 0.1,
            Some(last_activity) => {
                let inactive_days = (Utc::now() - last_activity).num_days();

                // This is really simple, but basically calls any crate with activity in 6 months as maintained
                // trying to recognize that some crates may actually be stable enough to require infrequent changes
                // From 6-12 months, it's maintenance state is less certain, and after a year without activity, it's likely unmaintained
                if inactive_days <= 180 {
                    1.0
                } else if inactive_days <= 365 {
                    0.5
                } else {
                    0.1
                }
            }
        };

        let recent_downloads = self
            .krate
            .as_ref()
            .and_then(|k| k.recent_downloads)
            .unwrap_or(0);
        self.score = Some(f32::floor(coefficient * recent_downloads as f32) as u64);
    }
}
