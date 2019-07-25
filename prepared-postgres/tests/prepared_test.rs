use dotenv::dotenv;
use postgres::{Client, Row, NoTls};
use prepared_postgres::{FromRow, Result, AffectedRows, postgres_prepared_statements};

#[derive(Debug)]
struct RecordId(pub i64);

#[derive(Debug)]
struct PostInfo { pub id: RecordId, pub title: String }

#[derive(Debug)]
struct PostData { pub title: String, pub contents: Option<String> }

impl FromRow for RecordId {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(RecordId(row.try_get(0)?))
    }
}

impl FromRow for PostInfo {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(PostInfo{
            id: RecordId(row.try_get(0)?),
            title: row.try_get(1)?
        })
    }
}

impl FromRow for PostData {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(PostData {
            title: row.try_get(0)?,
            contents: row.try_get(1)?
        })
    }
}

postgres_prepared_statements!(TestSql {
    insert_post(title: &str, contents: Option<&str>) -> RecordId as "\
    INSERT INTO post_t (title, contents) VALUES ($1, $2) RETURNING id";
    
    list_posts() -> Vec<PostInfo> as "\
    SELECT id, title FROM post_t ORDER BY id";
    
    find_post(id: i64) -> PostData as "\
    SELECT title, contents FROM post_t WHERE id = $1";

    update_post_contents(id: i64, contents: Option<&str>) -> AffectedRows as "\
    UPDATE post_t SET contents = $2 WHERE id = $1";
});    

#[test]
fn it_works() {
    dotenv().ok();
    let mut client = Client::connect(&std::env::var("TEST_DATABASE_URL").expect("Missing TEST_DATABASE_URL environment variable"), NoTls).unwrap();
    let mut trans = client.transaction().unwrap();

    trans.simple_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap();
    trans.simple_query("CREATE TABLE post_t (id SERIAL8 PRIMARY KEY, title TEXT NOT NULL, contents TEXT)").unwrap();
    let sql = TestSql::setup(&mut trans).unwrap();
    
    assert!(sql.list_posts(&mut trans).unwrap().is_empty());
    
    let first_id = sql.insert_post(&mut trans, "First post", Some("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")).unwrap();
    assert_eq!(sql.list_posts(&mut trans).unwrap().len(), 1);
    
    let second_id = sql.insert_post(&mut trans, "Second post", Some("Ut enim ad minim veniam, quis nostrud exercitation ullamco.")).unwrap();
    assert_eq!(sql.list_posts(&mut trans).unwrap().len(), 2);
    
    let first_post = sql.find_post(&mut trans, first_id.0).unwrap();
    assert_eq!(first_post.title, "First post");
    assert!(first_post.contents.is_some());
    assert_eq!(first_post.contents.unwrap(), "Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
    
    let second_post = sql.find_post(&mut trans, second_id.0).unwrap();
    assert_eq!(second_post.title, "Second post");
    assert!(second_post.contents.is_some());
    assert_eq!(second_post.contents.unwrap(), "Ut enim ad minim veniam, quis nostrud exercitation ullamco.");

    let affected_rows = sql.update_post_contents(&mut trans, first_id.0, Some("Updated contents")).unwrap();
    assert_eq!(affected_rows.count(), 1);
    assert!(sql.find_post(&mut trans, first_id.0).unwrap().contents.is_some());
    assert_eq!(sql.find_post(&mut trans, first_id.0).unwrap().contents.unwrap(), "Updated contents");

    let affected_rows = sql.update_post_contents(&mut trans, first_id.0, None).unwrap();
    assert_eq!(affected_rows.count(), 1);
    assert!(sql.find_post(&mut trans, first_id.0).unwrap().contents.is_none());
}
