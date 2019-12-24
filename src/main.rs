extern crate chrono;

#[macro_use] extern crate failure;
use failure::{Error, ResultExt};

#[macro_use] extern crate diesel;
use diesel::prelude::*;

use dotenv;

use log::{info, LevelFilter};

use std::env;
use std::path::PathBuf;
use std::time::Duration;


use structopt::StructOpt;

use url::Url;

mod models;
mod schema;
mod parser;
mod process;

const CHUNK_SIZE: usize = 1000;

#[derive(StructOpt, Debug)]
#[structopt(name = "hubber_xml")]
struct Opts {
    /// Update price, oldprice and currencyId fields
    #[structopt(long)]
    update_price: bool,
    /// Update available field
    #[structopt(long)]
    update_available: bool,
    /// Create new products
    #[structopt(long)]
    insert_new: bool,
    /// Mark products that not in file as unavailable
    #[structopt(long)]
    mark_missing_unavailable: bool,
    /// Do not render progress bar
    #[structopt(long)]
    no_progress: bool,
    /// XML file path to process
    #[structopt(name = "FILE_PATH", parse(from_os_str))]
    file_path: PathBuf,
}

#[derive(Default, Debug)]
struct ProcessedStat {
    pub total_offers: u32,
    pub ignored_offers: u32,
    pub parsed_offers: u32,
    pub updated_price: u32,
    pub updated_available: u32,
    pub inserted_products: u32,
    pub marked_as_unavailable: u32,
    pub total_duration: Duration,
    pub parse_duration: Duration,
}

fn main() -> Result<(), Error> {
    env_logger::builder()
        .filter(None, LevelFilter::Info)
        .init();

    let opts = Opts::from_args();

    let conn = establish_mysql_connection()?;

    let stat = parser::parse_offers(&opts, &conn)?;
    println!("Total offers: {}", stat.total_offers);
    println!("Ignored offers: {} (with errors or missing required fields)", stat.ignored_offers);
    println!("Parsed offers: {}", stat.parsed_offers);
    if opts.update_price {
        println!("Updated price: {}", stat.updated_price);
    } else {
        println!("Different price: {} (not_updated)", stat.updated_price);
    }
    if opts.update_available {
        println!("Updated available: {}", stat.updated_available);
    } else {
        println!("Different available: {} (not_updated)", stat.updated_available);
    }
    if opts.insert_new {
        println!("Inserted products: {}", stat.inserted_products);
    } else {
        println!("New products: {} (not inserted)", stat.inserted_products);
    }
    if opts.mark_missing_unavailable {
        println!("Marked as unavailable: {}", stat.marked_as_unavailable);
    }
    println!("Total time: {:?}", stat.total_duration);
    println!("Parse time: {:?}", stat.parse_duration);

    Ok(())
}

pub fn establish_mysql_connection() -> Result<MysqlConnection, Error> {
    dotenv::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .context("Environment variable DATABASE_URL must be set")?;
    let mut safe_url = Url::parse(&database_url)
        .context("Cannot parse DATABASE_URL environment variable")?;
    safe_url.set_password(Some("******")).ok();

    let conn = MysqlConnection::establish(&database_url)
        .context(format!("Error connecting to {}", &safe_url))?;
    info!("Successfully connected to {}", &safe_url);

    Ok(conn)
}
