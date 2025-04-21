
use skytable::{
    query, response::RList, Config, Response
};

#[derive(Response)]
#[derive(Clone, PartialEq, Debug)] // we just do these for the assert (they are not needed)
pub struct Page{
    pub pageid: u64,
    pub title: Vec<u8>,
    pub connected: skytable::response::RList<u64>,
}

#[derive(Response)]
pub struct Metadata{
    page_ids: RList<u64>,
}

pub fn connect(host: &str, port: u16, username: &str, password: &str) -> Result<skytable::Connection, skytable::error::Error> {
    let config = Config::new(host, port, username, password);
    config.connect()
}

pub fn get_all_nodes(db: &mut skytable::Connection) -> Result<Vec<u64>, skytable::error::Error> {
    let query = query!("select page_ids from pages.metadata");
    let metadata: Metadata = db.query_parse(&query)?;
    Ok(metadata.page_ids.into_values())
}

pub fn get_page(db: &mut skytable::Connection, pageid: u64) -> Result<Page, skytable::error::Error> {
    let query = query!("select * from pages.page where pageid = ?", pageid);
    db.query_parse::<Page>(&query)
}