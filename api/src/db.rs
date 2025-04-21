
use skytable::{
    query, response::{RList, Rows}, Config, Response
};

#[derive(Response)]
#[derive(Clone, PartialEq, Debug)] // we just do these for the assert (they are not needed)
pub struct Page{
    pub id: u64,
    pub title: Vec<u8>,
    pub connected: skytable::response::RList<u64>,
}

#[derive(Response)]
pub struct Metadata{
    id: u8,
    pages: RList<u64>,
}

pub fn connect(host: &str, port: u16, username: &str, password: &str) -> Result<skytable::Connection, skytable::error::Error> {
    let config = Config::new(host, port, username, password);
    config.connect()
}

pub fn get_all_nodes(db: &mut skytable::Connection) -> Result<Vec<u64>, skytable::error::Error> {
    let query = query!("select all * from pages.metadata limit ?", 10u64);
    
    let metadata: Rows<Metadata> = db.query_parse(&query)?;
    // println!("{} {:?}", metadata[0].id, metadata[0].pages);
    // println!("{:?}", pages);
    
    Ok(metadata[0].pages.clone().into_values())
}

pub fn get_page(db: &mut skytable::Connection, pageid: u64) -> Result<Page, skytable::error::Error> {
    let query = query!("select * from pages.page where id = ?", pageid);
    db.query_parse::<Page>(&query)
}