#[macro_use] extern crate failure;
use failure::Error;

#[macro_use]
extern crate diesel;
use diesel::prelude::*;
use diesel::mysql::MysqlConnection;

use dotenv::dotenv;

use log::{info, warn};

use std::env;
use std::path::Path;

use quick_xml::Reader;
use quick_xml::events::Event;

mod models;
mod schema;

use schema::products;
//use schema::products::dsl::*;

fn main() -> Result<(), Error> {
    let conn = establish_connection();

    let (total_offers, processed_offers) = process_offers(Path::new("hubber.xml"), &conn)?;
    println!("Total offers: {}", total_offers);
    println!("Processed offers: {}", processed_offers);

    Ok(())
}

pub fn establish_connection() -> MysqlConnection {
    dotenv().ok();

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

fn process_offers(path: &Path, conn: &MysqlConnection) -> Result<(u32, u32), Error> {
    let mut xml_reader = Reader::from_file(path)?;
    let mut buf = vec!();
    let mut offer_buf = vec!();
    let mut total_offers = 0;
    let mut processed_offers = 0;

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
                                                warn!("Cannot parse price: {}", value);
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
                                                _ => {}
                                            }
                                        }
                                        OfferFields::CategoryId => {
                                            offer.category_id = value.parse().ok();
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
                                _ => {}
                            }
                        }

                        total_offers += 1;
                        if let Some(product) = convert_offer_to_product(offer) {
                            products_bucket.push(product);
                            processed_offers += 1;
                        }
                        if products_bucket.len() == 1000 {
                            process_products_chunk(conn, &products_bucket)?;
                            products_bucket.clear();
                            info!("Processed {} offers", total_offers)
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => {
                break;
            }
            Err(e) => {
                Err(e)?;
            }
            _ => {}
        }
    }

    Ok((total_offers, processed_offers))
}

fn convert_offer_to_product(offer: Offer) -> Option<models::NewProduct> {
    let name = if let Some(name) = offer.name {
        name
    } else {
        println!("no name");
        return None;
    };
    let category_id = if let Some(cat_id) = offer.category_id {
        cat_id
    } else {
        println!("no category");
        return None;
    };
    let price = if let Some(price) = offer.price {
        price
    } else {
        println!("no price");
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

fn process_products_chunk(conn: &MysqlConnection, products: &Vec<models::NewProduct>) -> Result<(), Error> {
//    diesel::insert_into(products::table)
//        .values(products)
//        .execute(conn)?;
    Ok(())
}

//fn parse_offer(xml_reader: &mut Reader<BufReader<File>>, buf: &mut Vec<u8>) {
//
//}
