use super::schema::products;

#[derive(Insertable)]
#[table_name="products"]
pub struct NewProduct {
    pub offer_id: String,
    pub categoryId: i32,
    pub name: String,
    pub price: f32,
    pub oldprice: Option<f32>,
    pub currencyId: Option<String>,
    pub available: Option<i8>,
    pub description: Option<String>,
}

#[derive(Queryable)]
pub struct Product {
    pub id: i32,
    pub offer_id: String,
    pub categoryId: i32,
    pub name: String,
    pub price: f32,
    pub oldprice: Option<f32>,
    pub currencyId: Option<String>,
    pub available: Option<i8>,
    pub description: Option<String>,
}

#[derive(AsChangeset, Default, Debug)]
#[table_name="products"]
pub struct ModProduct<'a> {
    pub price: Option<&'a f32>,
    pub available: Option<Option<&'a i8>>,
    pub currencyId: Option<Option<&'a str>>,
//    pub categoryId: Option<&'a i32>,
//    pub name: Option<&'a str>,
//    pub oldprice: Option<&'a Option<f32>>,
//    pub description: Option<&'a Option<String>>,
}
