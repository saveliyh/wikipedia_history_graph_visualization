use db::get_all_nodes;
use rocket::serde::json::Json;
use rocket::serde::Serialize;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::Header;
use rocket::Request;
use rocket::Response;

mod db;
#[macro_use] 
extern crate rocket;
extern crate serde_derive;
extern crate toml;

pub struct CORS;

#[rocket::async_trait]
impl Fairing for CORS {
    fn info(&self) -> Info {
        Info {
            name: "Add CORS headers to responses",
            kind: Kind::Response
        }
    }

    async fn on_response<'r>(&self, _request: &'r Request<'_>, response: &mut Response<'r>) {
        response.set_header(Header::new("Access-Control-Allow-Origin", "*"));
        response.set_header(Header::new("Access-Control-Allow-Methods", "POST, GET, PATCH, OPTIONS"));
        response.set_header(Header::new("Access-Control-Allow-Headers", "*"));
        response.set_header(Header::new("Access-Control-Allow-Credentials", "true"));
    }
}

#[derive(Serialize)]
struct PageResponse{
    pageid: u64,
    title: String,
    connected: Vec<u64>,
}

#[derive(Serialize)]
struct LinkResponse{
    source: u64,
    target: u64,
}

#[get("/all_nodes")]
fn all_nodes_handler() -> Json<Vec<u64>>{
    let mut db = db::connect("localhost", 2003, "root", "dev_password_will_be_changed");
    while db.is_err() {
        db = db::connect("localhost", 2003, "root", "dev_password_will_be_changed");
        println!("wait for db");
    }
    match get_all_nodes(&mut db.unwrap()){
        Ok(value) => {return Json(value);},
        Err(e) => {
            println!("Cannot get node list from db: {}", e);
            return Json(vec![]);
        }
    }
    
}

#[get("/page/<pageid>")]
fn get_page_handler(pageid: u64) -> Json<PageResponse>{
    let mut db = db::connect("localhost", 2003, "root", "dev_password_will_be_changed");
    while db.is_err() {
        db = db::connect("localhost", 2003, "root", "dev_password_will_be_changed");
        println!("wait for db");
    }

    match db::get_page(&mut db.unwrap(), pageid){
        Ok(value) => {return Json(PageResponse { pageid: value.id,
             title: String::from_utf8(value.title).unwrap(), 
             connected: value.connected.into_values()
        });},
        Err(e) => {
            println!("Cannot get page from db: {}", e);
            return Json(PageResponse{pageid: 0, title: String::new(), connected: vec![]});
        }
    }
}

#[get("/all_links")]
fn all_links_handler() -> Json<Vec<LinkResponse>>{
    let mut db = db::connect("localhost", 2003, "root", "dev_password_will_be_changed");
    while db.is_err() {
        db = db::connect("localhost", 2003, "root", "dev_password_will_be_changed");
        println!("wait for db");
    }
    let nodes; 
    let mut db = db.unwrap();
    match db::get_all_nodes(&mut db) {
        Ok(value) => {nodes = value;},
        Err(e) => {
            println!("Cannot get link list from db: {}", e);
            return Json(vec![]);
        }
    }
    let mut links    = vec![];
    for pageid in nodes.iter(){
        match db::get_page(&mut db, *pageid){
            Ok(value) => {
                links.extend(value.connected.iter().filter(|x| nodes.contains(*x)).map(|x| LinkResponse{source: *pageid, target: *x}));
            },
            Err(e) => {
                println!("Cannot get node {} from db: {}", *pageid, e);
            }
        }
    }
    Json(links)
}

#[launch]
fn rocket() -> _ {
    

    rocket::build().mount("/", routes![all_nodes_handler])
                    .mount("/", routes![get_page_handler])
                    .mount("/", routes![all_links_handler])
                    .attach(CORS)
}
