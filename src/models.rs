#![allow(non_snake_case)]
use super::schema::products;

pub const AVAILABLE: i8 = 1;
pub const NOT_AVAILABLE: i8 = 0;

#[derive(Insertable)]
#[table_name="products"]
pub struct NewProduct {
    pub offer_id: String,
    pub hub_stock_id: String,
    pub categoryId: i32,
    pub name: String,
    pub price: f32,
    pub oldprice: Option<f32>,
    pub currencyId: Option<String>,
    pub available: i8,
    pub description: Option<String>,
}

#[derive(Queryable, Debug)]
pub struct Product {
    pub id: i32,
    pub offer_id: String,
    pub hub_stock_id: Option<String>,
    pub categoryId: i32,
    pub name: String,
    pub price: f32,
    pub oldprice: Option<f32>,
    pub currencyId: Option<String>,
    pub available: Option<i8>,
    pub description: Option<String>,
    pub renew_data: Option<chrono::NaiveDateTime>,
}

//#[derive(QueryableByName)]
//pub struct ProductHubStockIdOnly {
//    pub hub_stock_id: String,
//}

#[derive(AsChangeset, Default, Debug)]
#[table_name="products"]
pub struct ModProduct<'a> {
    pub available: Option<&'a i8>,
    pub price: Option<&'a f32>,
    pub oldprice: Option<Option<&'a f32>>,
    pub currencyId: Option<Option<&'a str>>,
    pub renew_date: Option<&'a chrono::NaiveDateTime>,
//    pub categoryId: Option<&'a i32>,
//    pub name: Option<&'a str>,
//    pub oldprice: Option<&'a Option<f32>>,
//    pub description: Option<&'a Option<String>>,
}
