#[macro_use] extern crate failure;
use failure::Error;

#[macro_use]
extern crate diesel;
use diesel::prelude::*;
use diesel::mysql::MysqlConnection;

use dotenv;

use log::{error, info, warn, LevelFilter};

use std::env;
use std::path::Path;

use quick_xml::Reader;
use quick_xml::events::Event;

mod models;
mod schema;

use schema::products;
use std::collections::HashMap;

#[derive(Default, Debug)]
struct ProcessedStat {
    pub total_offers: u32,
    pub ignored_offers: u32,
    pub parsed_offers: u32,
    pub updated_products: u32,
    pub inserted_products: u32,
}

fn main() -> Result<(), Error> {
    env_logger::builder()
        .filter(None, LevelFilter::Info)
        .init();

    let args: Vec<String> = env::args().collect();
    let file_path = match args.get(1) {
        Some(p) => Path::new(p),
        None => return Err(format_err!("Expected exactly 1 argument: FILE_PATH")),
    };

    let conn = establish_connection();

    let stat = process_offers(file_path, &conn)?;
    info!("{:?}", stat);

    Ok(())
}

pub fn establish_connection() -> MysqlConnection {
    dotenv::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let conn = MysqlConnection::establish(&database_url)
        .expect(&format!("Error connecting to {}", database_url));
    info!("Successfully connected to {}", database_url);

    conn
}

pub fn fetch_product() {

}

struct Offer {
    pub offer_id: String,
    pub available: Option<i8>,
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
    pub fn new(offer_id: String, available: Option<i8>) -> Offer {
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

fn process_offers(path: &Path, conn: &MysqlConnection) -> Result<ProcessedStat, Error> {
    let mut xml_reader = Reader::from_file(path)?;
    let mut buf = vec!();
    let mut offer_buf = vec!();
    let mut stat = ProcessedStat::default();

    let mut products_bucket = vec!();

    loop {
        match xml_reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) |
            Ok(Event::Empty(ref e)) => {
                match e.name() {
                    b"offer" => {
                        let mut offer_id = None;
                        let mut available = None;
                        for attr_res in e.attributes() {
                            let attr = attr_res?;
                            match attr.key {
                                b"id" => {
                                    offer_id = Some(String::from_utf8_lossy(&attr.value).to_string());
                                }
                                b"available" => {
                                    available = match attr.value.as_ref() {
                                        b"" => { None }
                                        b"true" | b"1" => { Some(1) }
                                        b"false" | b"0" => { Some(0) }
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
                        }

                        stat.total_offers += 1;
                        if let Some(product) = convert_offer_to_product(offer) {
                            products_bucket.push(product);
                            stat.parsed_offers += 1;
                        } else {
                            stat.ignored_offers += 1;
                        }
                        if products_bucket.len() == 1000 {
                            let (updated_count, inserted_count) = process_products_chunk(conn, &products_bucket)?;
                            stat.updated_products += updated_count;
                            stat.inserted_products += inserted_count;
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
    }

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
        offer_id: offer.offer_id,
        available: offer.available,
        categoryId: category_id,
        name,
        price,
        oldprice: offer.old_price,
        currencyId: offer.currency_id,
        description: offer.description,
    })
}

fn process_products_chunk(
    conn: &MysqlConnection, parsed_products: &Vec<models::NewProduct>
) -> Result<(u32, u32), Error> {
    use schema::products::dsl::{products as products_table};

    let mut updated_count = 0;
    let mut inserted_count = 0;

    let offer_ids = parsed_products.iter()
        .map(|p| p.offer_id.as_str())
        .collect::<Vec<_>>();
    let found_products = products_table
        .filter(schema::products::offer_id.eq_any(offer_ids))
        .load::<models::Product>(conn)?;
    let offer_id_to_found_product = found_products.iter()
        .map(|p| (p.offer_id.as_str(), p))
        .collect::<HashMap<_, _>>();

    for p in parsed_products {
        match offer_id_to_found_product.get(p.offer_id.as_str()) {
            Some(found_product) => {
                let mut should_update = false;
                let mut update_product = models::ModProduct::default();
                if p.available != found_product.available {
                    update_product.available = Some(p.available.as_ref());
                    should_update = true;
                }
                if p.price != found_product.price || p.currencyId != found_product.currencyId {
                    update_product.price = Some(&p.price);
                    update_product.currencyId = Some(p.currencyId.as_deref());
                    should_update = true;
                }
                if should_update {
                    // println!("Updating product with offer_id={}: {:?}", p.offer_id, update_product);
                    diesel::update(schema::products::table.find(found_product.id))
                        .set(&update_product)
                        .execute(conn)?;
                    updated_count += 1;
                }
            }
            None => {}
        }
    }

    let insert_products = parsed_products.iter()
        .filter(|&p| !offer_id_to_found_product.contains_key(p.offer_id.as_str()))
        .collect::<Vec<_>>();
    inserted_count += insert_products.len();
    if !insert_products.is_empty() {
        diesel::insert_into(products::table)
            .values(insert_products)
            .execute(conn)?;
    }
    Ok((updated_count, inserted_count as u32))
}
