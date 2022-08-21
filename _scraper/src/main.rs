use anyhow::{bail, ensure, Context, Result};

mod crates;
mod data;
mod github;
mod util;

use data::{GeneratedCrateInfo, InputCrateInfo};
use github::Github;
use std::env;
use url::Url;
use util::{read_yaml, write_yaml};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args();
    let _ = args.next();
    let path = match args.next() {
        Some(arg) => arg,
        None => bail!("Usage: scraper <path_to_crates_yaml>"),
    };

    let gh = Github::new()?;

    let input: Vec<InputCrateInfo> =
        read_yaml(&path).with_context(|| format!("Error reading {}", path))?;
    let mut generated = Vec::new();
    for krate in input {
        println!("Processing {}", krate);

        let mut gen = GeneratedCrateInfo::try_from(krate).await?;
        // println!("{}", serde_yaml::to_string(&gen).unwrap());
        // Yes, we serialized in `override_crate_data` and then re-parse it as a Url,
        // but the upstream Crate type uses a String, and I still want to deserialize data.yaml as
        // a Url for input validation
        let repo_opt: Option<Url> = gen
            .krate
            .as_ref()
            .and_then(|k| k.repository.as_ref().map(|r| Url::parse(r).unwrap()))
            .clone();
        if let Some(repo) = repo_opt {
            if repo.host_str() == Some("github.com") {
                // split path including both '/' and '.', because `.git` is occasionally appended to git URLs
                let parts = repo.path().split(&['/', '.'][..]).collect::<Vec<_>>();
                ensure!(parts.len() >= 3);
                match gh.get_repo_data(parts[1], parts[2]).await {
                    Ok(data) => gen.repo = Some(data),
                    Err(err) => {
                        eprintln!(
                            "Error getting Github repo data for {}/{} - {}",
                            parts[1], parts[2], err
                        )
                    }
                }
            }
        }
        gen.update_score();
        generated.push(gen);
    }

    write_yaml("_data/crates_generated.yaml", generated)
}
