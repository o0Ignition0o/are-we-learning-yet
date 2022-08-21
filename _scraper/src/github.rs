use crate::util::{cache_path, read_cache, write_cache};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use octocrab::Octocrab;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, Value};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoData {
    pub name: String,
    pub stargazers_count: u32,
    pub last_commit: DateTime<Utc>,
    pub contributor_count: Option<u32>,
    pub open_issues_count: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphqlError {
    pub path: Option<Vec<String>>,
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum GraphqlResponse {
    Error { errors: Vec<GraphqlError> },
    Value(Value),
}

impl RepoData {
    fn from_graphql_data(name: &str, val: &Value) -> Result<RepoData> {
        let repo = &val["data"]["repository"];
        let repo_data = RepoData {
            name: name.to_string(),
            stargazers_count: from_value(repo["stargazers"]["totalCount"].clone())?,
            last_commit: from_value(repo["pushedAt"].clone())?,
            contributor_count: from_value(repo["collaborators"]["totalCount"].clone())?,
            open_issues_count: from_value(repo["issues"]["totalCount"].clone())?,
        };
        Ok(repo_data)
    }
}

pub struct Github {
    client: Octocrab,
}

impl Github {
    pub fn new() -> Result<Github> {
        let token = env::var("GITHUB_TOKEN").context("GITHUB_TOKEN has not been set")?;
        let client = octocrab::OctocrabBuilder::new()
            .personal_token(token)
            .build()?;
        Ok(Github { client })
    }

    async fn fetch_remote_repo_data(&self, username: &str, repo: &str) -> Result<Value> {
        let query = format!(
            r#"query {{
              repository(owner:"{}", name:"{}") {{
                stargazers {{
                  totalCount
                }}
                collaborators {{
                    totalCount
                }}
                issues(states: OPEN) {{
                    totalCount
                }}
                pushedAt
              }}
            }}"#,
            username, repo
        );

        let response: GraphqlResponse = self.client.graphql(&query).await?;

        // Hopefully temporary: see https://github.com/XAMPPRocky/octocrab/issues/78
        match response {
            GraphqlResponse::Error { errors } => {
                Err(anyhow!("{}", errors[0].message.as_ref().unwrap()))
            }
            GraphqlResponse::Value(v) => Ok(v),
        }
    }

    // TODO: use cache where available
    pub async fn get_repo_data(&self, username: &str, repo: &str) -> Result<RepoData> {
        let cache_path = cache_path("github", &format!("{}--{}", username, repo))?;

        let data = match read_cache(&cache_path) {
            Ok(data) => data,
            Err(_) => {
                let data = self.fetch_remote_repo_data(username, repo).await?;
                let _ = write_cache(&cache_path, &data);
                data
            }
        };

        RepoData::from_graphql_data(&format!("{}/{}", username, repo), &data)
    }
}
