#[macro_use] extern crate prepared_postgres_derive;

use dotenv::dotenv;
use postgres::{Client, NoTls};
use prepared_postgres::FromRow;

#[derive(FromRow)]
struct Post {
    pub id: i64,
    pub title: String,
    pub contents: Option<String>,
    pub published: bool,
}

#[derive(FromRow)]
struct Notification(i32, String, bool);

#[test]
fn derived_fromrow_for_named_struct() {
    dotenv().ok();
    let mut client = Client::connect(&std::env::var("TEST_DATABASE_URL").expect("Missing TEST_DATABASE_URL environment variable"), NoTls)
        .expect("Failed to open Postgres connection");
    
    let row = client.query("SELECT 10::int8, 'Hello, derive', NULL, TRUE", &[])
        .expect("Failed to execute a query")
        .into_iter()
        .nth(0)
        .expect("Failed to fetch first result row");
    
    let post = Post::from_row(&row)
        .expect("Failed to perform Post::from_row");
    
    assert_eq!(post.id, 10);
    assert_eq!(post.title, "Hello, derive");
    assert!(post.contents.is_none());
    assert_eq!(post.published, true);
}

#[test]
fn derived_fromrow_for_tuple_struct() {
    dotenv().ok();
    let mut client = Client::connect(&std::env::var("TEST_DATABASE_URL").expect("Missing TEST_DATABASE_URL environment variable"), NoTls)
        .expect("Failed to open Postgres connection");
    
    let row = client.query("SELECT 22::int4, 'URGENT', FALSE", &[])
        .expect("Failed to execute a query")
        .into_iter()
        .nth(0)
        .expect("Failed to fetch first result row");
    
    let notif = Notification::from_row(&row)
        .expect("Failed to perform Post::from_row");
    
    assert_eq!(notif.0, 22);
    assert_eq!(notif.1, "URGENT");
    assert_eq!(notif.2, false);
}
