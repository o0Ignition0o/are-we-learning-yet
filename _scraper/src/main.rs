use anyhow::{bail, ensure, Context, Result as AnyResult};

mod crates;
mod data;
mod github;
mod util;

use chrono::{TimeZone, Utc};
use chrono_tz::Europe::London;
use crates_io_api::CrateLinks;
use data::{GeneratedCrateInfo, InputCrateInfo};
use github::Github;
use std::{
    collections::{BTreeSet, HashMap},
    env,
    fs::File,
    io::Write,
};
use url::Url;
use util::{read_yaml, write_yaml};

use crate::data::ManualCrateInfo;

static DB_DUMP: &str = "db-dump.tar.gz";

#[tokio::main]
async fn main() -> AnyResult<()> {
    let mut args = env::args();
    let _ = args.next();
    let path = match args.next() {
        Some(arg) => arg,
        None => bail!("Usage: scraper <path_to_crates_yaml>"),
    };

    let gh = Github::new()?;

    let input: Vec<InputCrateInfo> =
        read_yaml(&path).with_context(|| format!("Error reading {}", path))?;

    let (categories, input): (Vec<_>, Vec<_>) = input.into_iter().partition(|i| match i {
        InputCrateInfo::Category(_) => true,
        _ => false,
    });

    let requested_categories = categories
        .iter()
        .map(|ic| match ic {
            InputCrateInfo::Category(c) => c.name.clone(),
            _ => panic!("checked above"),
        })
        .collect::<Vec<_>>();

    let mut generated = Vec::new();

    for krate in input {
        eprintln!("Processing {}", krate);

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

    let mut crates = BTreeSet::new();
    let mut all_categories = BTreeSet::new();
    let mut crates_categories: HashMap<
        db_dump::categories::CategoryId,
        Vec<db_dump::crates::CrateId>,
    > = HashMap::new();
    let mut categories_crates: HashMap<
        db_dump::crates::CrateId,
        Vec<db_dump::categories::CategoryId>,
    > = HashMap::new();
    if !requested_categories.is_empty() {
        make_sure_db_is_available().await?;

        eprintln!("loading all categories from database");

        // let's deal with categories
        db_dump::Loader::new()
            .crates(|row| {
                crates.insert(row);
            })
            .categories(|row| {
                all_categories.insert(row);
            })
            .crates_categories(|row| {
                crates_categories
                    .entry(row.category_id)
                    .and_modify(|crates| crates.push(row.crate_id))
                    .or_insert(vec![row.crate_id]);

                categories_crates
                    .entry(row.crate_id)
                    .and_modify(|categories| categories.push(row.category_id))
                    .or_insert(vec![row.category_id]);
            })
            .load("./db-dump.tar.gz")?;

        let relevant_categories = all_categories
            .iter()
            .filter(|c| requested_categories.contains(&c.category))
            .collect::<Vec<_>>();

        let crates_by_categories: HashMap<&db_dump::categories::Row, _> = crates_categories
            .into_iter()
            .filter(|(c, _)| relevant_categories.iter().any(|cat| cat.id.0 == c.0))
            .map(|(category_id, crate_ids)| {
                let category = all_categories
                    .get(&category_id)
                    .expect("checked in the filter");

                let crates = crate_ids
                    .into_iter()
                    .map(|id| crates.get(&id).expect("all crates must be present").clone())
                    .collect::<Vec<_>>();

                (category.clone(), crates)
            })
            .collect();

        for (category, crates) in crates_by_categories.into_iter() {
            eprintln!(
                "Category {} has {} crates",
                category.category, category.crates_cnt
            );
            for krate in crates {
                eprintln!("Processing {}", krate.name);

                let current_crate_categories: Vec<_> = categories_crates
                    .get(&krate.id)
                    .expect("we should have all categories")
                    .into_iter()
                    .map(|category| {
                        all_categories
                            .get(category)
                            .expect("we should have all categories 2")
                            .description
                            .clone()
                    })
                    .collect();
                let info = InputCrateInfo::Manual(ManualCrateInfo {
                    topics: current_crate_categories.clone(),
                    krate: Some(crates_io_api::Crate {
                        id: krate.id.0.to_string(),
                        name: krate.name.clone(),
                        description: Some(krate.description.clone()),
                        license: None,
                        documentation: krate.documentation.clone(),
                        homepage: krate.homepage.clone(),
                        repository: krate.repository.clone(),
                        downloads: krate.downloads,
                        recent_downloads: None,
                        categories: Some(current_crate_categories),
                        keywords: None,
                        // TODO: versions can be found while iterating on the db
                        versions: None,
                        max_version: "unknown".to_string(),
                        links: CrateLinks {
                            owner_team: "unknown".to_string(),
                            owner_user: "unknown".to_string(),
                            owners: "unknown".to_string(),
                            reverse_dependencies: "unknown".to_string(),
                            version_downloads: "unknown".to_string(),
                            versions: None,
                        },
                        created_at: London
                            .from_local_datetime(&krate.created_at)
                            .unwrap()
                            .with_timezone(&Utc),
                        updated_at: London
                            .from_local_datetime(&krate.updated_at)
                            .unwrap()
                            .with_timezone(&Utc),
                        exact_match: None,
                    }),
                    score: None,
                    repo: None,
                });

                let mut gen = GeneratedCrateInfo::try_from(info).await?;
                gen.update_score();
                generated.push(gen);
            }
        }
    }

    println!("{}", serde_json::to_string(&generated).unwrap());
    write_yaml("_data/crates_generated.yaml", generated)
}

async fn make_sure_db_is_available() -> AnyResult<()> {
    let crates_io_db_dump = std::fs::metadata(DB_DUMP);

    let must_update = match crates_io_db_dump {
        Ok(file_metadata) => {
            if let Ok(created_at) = file_metadata.created() {
                // the crates-io dump is more than 24 hours old
                created_at
                    .elapsed()
                    .map(|elapsed| elapsed.as_secs() >= 24 * 60 * 60)
                    .unwrap_or(true)
            } else {
                // couldn't get created-at
                false
            }
        }
        // couldn't get file metadata
        _ => true,
    };

    if must_update {
        eprintln!("downloading crates io db dump, this will take a while...");

        let resp = reqwest::get("https://static.crates.io/db-dump.tar.gz")
            .await?
            .bytes()
            .await?;
        let mut out = File::create(DB_DUMP)?;

        out.write_all(&resp)?;
    }

    Ok(())
}
