use db::get_all_nodes;
use rocket::serde::json::Json;
use rocket::serde::Serialize;

mod db;
#[macro_use] 
extern crate rocket;
extern crate serde_derive;
extern crate toml;

#[derive(Serialize)]
struct PageResponse{
    pageid: u64,
    title: String,
    connected: Vec<u64>,
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
        Ok(value) => {return Json(PageResponse { pageid: value.pageid,
             title: String::from_utf8(value.title).unwrap(), 
             connected: value.connected.into_values()
        });},
        Err(e) => {
            println!("Cannot get page from db: {}", e);
            return Json(PageResponse{pageid: 0, title: String::new(), connected: vec![]});
        }
    }
}


#[launch]
fn rocket() -> _ {
    
    rocket::build().mount("/all_nodes", routes![all_nodes_handler])
                    .mount("/page/<page_id>", routes![get_page_handler])
}
