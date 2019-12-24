use chrono::NaiveDateTime;

use diesel::connection::SimpleConnection;
use diesel::mysql::MysqlConnection;
use diesel::prelude::*;

use failure::Error;

use indicatif::{ProgressBar, ProgressStyle};

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::{CHUNK_SIZE, Opts};
use crate::models::{self, AVAILABLE, NOT_AVAILABLE};
use crate::parser::Offer;
use crate::schema::{self, products};


pub(crate) fn convert_offer_to_product(offer: Offer) -> Option<models::NewProduct> {
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
pub(crate) struct ProcessedProducts {
    pub updated_price: u32,
    pub updated_available: u32,
    pub inserted: u32,
    pub duration: Duration,
}

pub(crate) fn sync_products_chunk(
    conn: &MysqlConnection,
    parsed_products: &Vec<models::NewProduct>,
    opts: &Opts,
    date_modified: &NaiveDateTime,
) -> Result<ProcessedProducts, Error> {
    use crate::schema::products::dsl::{products as products_table};

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

    let mut raw_update_queries = String::new();
    for p in parsed_products {
        match offer_id_to_found_product.get(p.hub_stock_id.as_str()) {
            Some(found_product) => {
                let mut should_update = false;
                let mut update_product = models::ModProduct::default();
                if Some(p.available) != found_product.available {
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
                    raw_update_queries.push_str("UPDATE `products` SET ");
                    if let Some(available) = update_product.available {
                        raw_update_queries.push_str(
                            &format!("`available` = {}, ", available.to_string())
                        );
                    }
                    if let Some(price) = update_product.price {
                        raw_update_queries.push_str(
                            &format!("`price` = {}, ", price.to_string())
                        );
                    }
                    if let Some(oldprice) = update_product.oldprice {
                        raw_update_queries.push_str(
                            &format!("`oldprice` = {}, ", optional_to_sql(oldprice))
                        );
                    }
                    if let Some(currency_id) = update_product.currencyId {
                        raw_update_queries.push_str(
                            &format!("`currencyId` = {}, ", optional_string_to_sql(currency_id))
                        );
                    }
                    raw_update_queries.push_str(&format!(
                        "`renew_date` = '{}', `to_renew` = 1 WHERE `id` = {};\n",
                        &date_modified, found_product.id
                    ));

//                    update_product.renew_date = Some(&date_modified);
//                    diesel::update(schema::products::table.find(found_product.id))
//                        .set(&update_product)
//                        .execute(conn)?;
                }
            }
            None => {}
        }
    }
    if !raw_update_queries.is_empty() {
        conn.batch_execute(&raw_update_queries)?;
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

fn optional_to_sql<T: ToString>(v: Option<&T>) -> String {
    return if let Some(v) = v {
        v.to_string()
    } else {
        "NULL".to_string()
    }
}

fn optional_string_to_sql(s: Option<&str>) -> String {
    return if let Some(s) = s {
        format!(
            "'{}'",
            s
            .replace(r"\", r"\\")
            .replace("'", r"\'")
            .replace(r#"""#, r#"\""#))
    } else {
        "NULL".to_string()
    }
}

pub(crate) fn mark_missing_as_unavailable(
    conn: &MysqlConnection,
    all_offer_ids: &HashSet<String>,
    opts: &Opts,
) -> Result<u32, Error> {
    use crate::schema::products::dsl;

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
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:6} ({eta}) searching missing products")
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

pub(crate) fn finilize_processing(conn: &MysqlConnection, date_processing: &NaiveDateTime) -> Result<(), Error> {
    // TODO: Create row if not exists
    conn.batch_execute(&format!(
        "UPDATE timestamps SET event_date='{}' WHERE event = 'hub_xml_update';", date_processing
    ))?;
    Ok(())
}
