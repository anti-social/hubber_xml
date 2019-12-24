use chrono::Utc;

use diesel::mysql::MysqlConnection;

use failure::Error;

use indicatif::{ProgressBar, ProgressStyle};

use log::{error, warn};

use quick_xml::Reader;
use quick_xml::events::Event;

use std::collections::HashSet;
use std::fs;
use std::time::{Duration, Instant};

use crate::{Opts, ProcessedStat};
use crate::models::{AVAILABLE, NOT_AVAILABLE};
use crate::process::{convert_offer_to_product, mark_missing_as_unavailable, sync_products_chunk};

pub(crate) struct Offer {
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

pub(crate) fn parse_offers(
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
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) parsing file and updating products")
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

    let date_modified = Utc::now().naive_utc();

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
                                                "" => {
                                                    offer.currency_id = Some("UAH".to_string());
                                                }
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
                                conn, &products_bucket, opts, &date_modified
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
            conn, &products_bucket, opts, &date_modified
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
