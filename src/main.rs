extern crate chrono;
use chrono::prelude::*;

#[macro_use] extern crate failure;
use failure::{Error, ResultExt};

#[macro_use] extern crate diesel;
use diesel::prelude::*;
use diesel::mysql::MysqlConnection;

use dotenv;

use indicatif::{ProgressBar, ProgressStyle};

use log::{error, info, warn, LevelFilter};

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use structopt::StructOpt;

use quick_xml::Reader;
use quick_xml::events::Event;

use url::Url;

mod models;
mod schema;

use models::{AVAILABLE, NOT_AVAILABLE};
use schema::products;

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

    let stat = process_offers(&opts, &conn)?;
    println!("Total offers: {}", stat.total_offers);
    println!("Ignored offers (with errors or missing required fields): {}", stat.ignored_offers);
    println!("Parsed offers: {}", stat.parsed_offers);
    if opts.update_price {
        println!("Updated price: {}", stat.updated_price);
    } else {
        println!("Different price: {}", stat.updated_price);
    }
    if opts.update_available {
        println!("Updated available: {}", stat.updated_available);
    } else {
        println!("Different available: {}", stat.updated_available);
    }
    if opts.insert_new {
        println!("Inserted products: {}", stat.inserted_products);
    } else {
        println!("New products: {}", stat.inserted_products);
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

pub fn fetch_product() {

}

struct Offer {
    pub offer_id: String,
    pub available: i8,
    pub price: Option<f32>,
    pub old_price: Option<f32>,
    pub currency_id: Option<String>,
    pub category_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub vendor: Option<String>,
    pub vendor_code: Option<String>,
}

impl Offer {
    pub fn new(offer_id: String, available: i8) -> Offer {
        Offer {
            offer_id,
            available,
            price: None,
            old_price: None,
            currency_id: None,
            category_id: None,
            name: None,
            description: None,
            vendor: None,
            vendor_code: None,
        }
    }
}

enum OfferFields {
    None,
    Price,
    OldPrice,
    CurrencyId,
    CategoryId,
    Name,
    Description,
    Vendor,
    VendorCode,
}

fn process_offers(
    opts: &Opts, conn: &MysqlConnection,
) -> Result<ProcessedStat, Error> {
    let start_processing_at = Instant::now();
    let mut total_sync_duration = Duration::default();
    let file_path = opts.file_path.as_path();
    let file_size = fs::metadata(file_path)?.len();
    let update_progress_after_chunk = file_size / 100;
    let progress_bar = if !opts.no_progress {
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .progress_chars("#>-")
        );
        Some(pb)
    } else {
        None
    };
    let mut xml_reader = Reader::from_file(file_path)?;
    let mut buf = vec!();
    let mut offer_buf = vec!();
    let mut stat = ProcessedStat::default();

    let mut products_bucket = vec!();
    let mut all_offer_ids = HashSet::new();

    loop {
        match xml_reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) |
            Ok(Event::Empty(ref e)) => {
                match e.name() {
                    b"offer" => {
                        let mut offer_id = None;
                        let mut available = NOT_AVAILABLE;
                        for attr_res in e.attributes() {
                            let attr = attr_res?;
                            match attr.key {
                                b"id" => {
                                    offer_id = Some(String::from_utf8_lossy(&attr.value).to_string());
                                }
                                b"available" => {
                                    available = match attr.value.as_ref() {
                                        b"" => NOT_AVAILABLE,
                                        b"true" | b"1" => AVAILABLE,
                                        b"false" | b"0" => NOT_AVAILABLE,
                                        v => {
                                            return Err(format_err!(
                                                "Unknown \"available\" attribute: {}", String::from_utf8_lossy(v)
                                            ))
                                        }
                                    };
                                }
                                _ => {}
                            }
                        }
                        let mut offer = if let Some(offer_id) = offer_id {
                            Offer::new(offer_id, available)
                        } else {
                            warn!("An offer without id was found");
                            continue;
                        };
                        let mut offer_field = OfferFields::None;

                        loop {
                            match xml_reader.read_event(&mut offer_buf) {
                                Ok(Event::Start(ref offer_event)) => {
                                    match offer_event.name() {
                                        b"price" => {
                                            offer_field = OfferFields::Price;
                                        }
                                        b"oldprice" => {
                                            offer_field = OfferFields::OldPrice;
                                        }
                                        b"currencyId" => {
                                            offer_field = OfferFields::CurrencyId;
                                        }
                                        b"categoryId" => {
                                            offer_field = OfferFields::CategoryId;
                                        }
                                        b"name" => {
                                            offer_field = OfferFields::Name;
                                        }
                                        b"description" => {
                                            offer_field = OfferFields::Description;
                                        }
                                        b"vendor" => {
                                            offer_field = OfferFields::Vendor;
                                        }
                                        b"vendorCode" => {
                                            offer_field = OfferFields::VendorCode;
                                        }
                                        _ => {}
                                    }
                                }
                                Ok(Event::Text(ref v)) => {
                                    let value = String::from_utf8_lossy(v.escaped());
                                    match offer_field {
                                        OfferFields::Price => {
                                            if let Ok(price) = value.parse() {
                                                offer.price = Some(price);
                                            } else {
                                                warn!("{}: Cannot parse price: {}", offer.offer_id, value);
                                            }
                                        }
                                        OfferFields::OldPrice => {
                                            offer.old_price = value.parse().ok();
                                        }
                                        OfferFields::CurrencyId => {
                                            match value.as_ref() {
                                                "UAH" | "USD" | "EUR" | "RUB" | "BYR" | "KZT" => {
                                                    offer.currency_id = Some(value.to_string());
                                                }
                                                "" => {}
                                                _ => {
                                                    warn!("{}: Unknown currencyId: {}", offer.offer_id, value);
                                                }
                                            }
                                        }
                                        OfferFields::CategoryId => {
                                            if let Ok(cat_id) = value.parse() {
                                                offer.category_id = Some(cat_id);
                                            } else {
                                                warn!("{}: Cannot parse categoryId: {}", offer.offer_id, value);
                                            }
                                        }
                                        OfferFields::Name => {
                                            offer.name = Some(value.to_string());
                                        }
                                        OfferFields::Description => {
                                            offer.description = Some(value.to_string());
                                        }
                                        OfferFields::Vendor => {
                                            offer.vendor = Some(value.to_string());
                                        }
                                        OfferFields::VendorCode => {
                                            offer.vendor_code = Some(value.to_string());
                                        }
                                        _ => {}
                                    }
                                }
                                Ok(Event::End(ref e)) => {
                                    match e.name() {
                                        b"offer" => {
                                            break;
                                        }
                                        _ => {
                                            offer_field = OfferFields::None;
                                        }
                                    }
                                }
                                Ok(Event::Eof) => {
                                    unreachable!();
                                }
                                Err(e) => {
                                    error!("Error at position: {}", xml_reader.buffer_position());
                                    Err(e)?;
                                }
                                _ => {}
                            }

                            offer_buf.clear();
                        }

                        stat.total_offers += 1;
                        if let Some(product) = convert_offer_to_product(offer) {
                            if opts.mark_missing_unavailable {
                                all_offer_ids.insert(product.offer_id.clone());
                            }
                            products_bucket.push(product);
                            stat.parsed_offers += 1;
                        } else {
                            stat.ignored_offers += 1;
                        }
                        if products_bucket.len() == 1000 {
                            let processed_products_stat = sync_products_chunk(
                                conn, &products_bucket, opts
                            )?;
                            stat.updated_price += processed_products_stat.updated_price;
                            stat.updated_available += processed_products_stat.updated_available;
                            stat.inserted_products += processed_products_stat.inserted;
                            total_sync_duration += processed_products_stat.duration;
                            products_bucket.clear();
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => {
                break;
            }
            Err(e) => {
                error!("Error at position: {}", xml_reader.buffer_position());
                Err(e)?;
            }
            _ => {}
        }

        buf.clear();

        if let Some(ref pb) = progress_bar {
            let cur_file_position = xml_reader.buffer_position() as u64;
            if cur_file_position > pb.position() + update_progress_after_chunk {
                pb.set_position(cur_file_position);
            }
        };
    }

    if !products_bucket.is_empty() {
        let processed_products_stat = sync_products_chunk(
            conn, &products_bucket, opts
        )?;
        stat.updated_price += processed_products_stat.updated_price;
        stat.updated_available += processed_products_stat.updated_available;
        stat.inserted_products += processed_products_stat.inserted;
        total_sync_duration += processed_products_stat.duration;
    }

    if let Some(ref pb) = progress_bar {
        pb.finish();
    };

    if opts.mark_missing_unavailable {
        stat.marked_as_unavailable = mark_missing_as_unavailable(conn, &all_offer_ids, opts)?;
    }

    stat.total_duration = start_processing_at.elapsed();
    stat.parse_duration = stat.total_duration - total_sync_duration;

    Ok(stat)
}

fn convert_offer_to_product(offer: Offer) -> Option<models::NewProduct> {
    let name = if let Some(name) = offer.name {
        name
    } else {
        return None;
    };
    let category_id = if let Some(cat_id) = offer.category_id {
        cat_id
    } else {
        return None;
    };
    let price = if let Some(price) = offer.price {
        price
    } else {
        return None;
    };
    Some(models::NewProduct {
        offer_id: offer.offer_id.clone(),
        hub_stock_id: offer.offer_id.clone(),
        available: offer.available,
        categoryId: category_id,
        name,
        price,
        oldprice: offer.old_price,
        currencyId: offer.currency_id,
        description: offer.description,
    })
}

#[derive(Default)]
struct ProcessedProducts {
    pub updated_price: u32,
    pub updated_available: u32,
    pub inserted: u32,
    pub duration: Duration,
}

fn sync_products_chunk(
    conn: &MysqlConnection,
    parsed_products: &Vec<models::NewProduct>,
    opts: &Opts
) -> Result<ProcessedProducts, Error> {
    use schema::products::dsl::{products as products_table};

    let start_syncing_at = Instant::now();
    let mut processed_products_stat = ProcessedProducts::default();

    let offer_ids = parsed_products.iter()
        .map(|p| p.offer_id.as_str())
        .collect::<Vec<_>>();
    let found_products = products_table
        .filter(schema::products::hub_stock_id.eq_any(offer_ids))
        .load::<models::Product>(conn)?;
    let offer_id_to_found_product = found_products.iter()
        .filter_map(|p| {
            if let Some(ref hub_stock_id) = p.hub_stock_id {
                Some((hub_stock_id.as_str(), p))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    for p in parsed_products {
        match offer_id_to_found_product.get(p.hub_stock_id.as_str()) {
            Some(found_product) => {
                let mut should_update = false;
                let mut update_product = models::ModProduct::default();
                if p.available != found_product.available.unwrap_or(NOT_AVAILABLE) {
                    processed_products_stat.updated_available += 1;
                    if opts.update_available {
                        update_product.available = Some(&p.available);
                        should_update = true;
                    }
                }
                if p.price != found_product.price ||
                    p.oldprice != found_product.oldprice ||
                    p.currencyId != found_product.currencyId
                {
                    processed_products_stat.updated_price += 1;
                    if opts.update_price {
                        update_product.price = Some(&p.price);
                        update_product.oldprice = Some(p.oldprice.as_ref());
                        update_product.currencyId = Some(p.currencyId.as_deref());
                        should_update = true;
                    }
                }
                if should_update {
                    // println!("Updating product with offer_id={}: {:?}", p.offer_id, update_product);
                    let date_modified = Utc::now().naive_utc();
                    update_product.renew_date = Some(Some(&date_modified));
                    diesel::update(schema::products::table.find(found_product.id))
                        .set(&update_product)
                        .execute(conn)?;
                }
            }
            None => {}
        }
    }

    let insert_products = parsed_products.iter()
        .filter(|&p| {
            !offer_id_to_found_product.contains_key(p.hub_stock_id.as_str())
        })
        .collect::<Vec<_>>();
    processed_products_stat.inserted += insert_products.len() as u32;
    if !insert_products.is_empty() {
        if opts.insert_new {
            diesel::insert_into(products::table)
                .values(insert_products)
                .execute(conn)?;
        }
    }

    processed_products_stat.duration += start_syncing_at.elapsed();

    Ok(processed_products_stat)
}

fn mark_missing_as_unavailable(
    conn: &MysqlConnection,
    all_offer_ids: &HashSet<String>,
    opts: &Opts,
) -> Result<u32, Error> {
    use schema::products::dsl;

    let mut last_product_id = 0;
    let mut missing_offer_ids = Vec::with_capacity(CHUNK_SIZE);
    let mut marked_count: u32 = 0;

    let progress = if !opts.no_progress {
        let total_products = dsl::products.select(dsl::id)
            .filter(dsl::available.eq(AVAILABLE))
            .count()
            .get_result::<i64>(conn)? as u64;
        let pb = ProgressBar::new(total_products);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:6} ({eta})")
        );
        Some((pb, total_products / 100))
    } else {
        None
    };

    let mut total_processed: u64 = 0;
    loop {
        let db_offers = dsl::products.select((dsl::id, dsl::hub_stock_id))
            .filter(dsl::id.gt(last_product_id))
            .filter(dsl::available.eq(AVAILABLE))
            .order(dsl::id)
            .limit(CHUNK_SIZE as i64)
            .load::<(i32, Option<String>)>(conn)?;

        if db_offers.is_empty() {
            break;
        }

        total_processed += db_offers.len() as u64;
        last_product_id = db_offers.last().unwrap().0;

        for (_, db_offer_id) in db_offers {
            if let Some(db_offer_id) = db_offer_id {
                if !all_offer_ids.contains(&db_offer_id) {
                    missing_offer_ids.push(db_offer_id);
                }
            }
        }
        if !missing_offer_ids.is_empty() {
            diesel::update(dsl::products.filter(dsl::hub_stock_id.eq_any(&missing_offer_ids)))
                .set(dsl::available.eq(NOT_AVAILABLE))
                .execute(conn)?;
            marked_count += missing_offer_ids.len() as u32;
            missing_offer_ids.clear();
        }

        if let Some((ref pb, update_after_count)) = progress {
            if pb.position() + update_after_count < total_processed {
                pb.set_position(total_processed);
            }
        }
    }

    if let Some((pb, _)) = progress {
        pb.finish();
    }

    Ok(marked_count)
}
