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
