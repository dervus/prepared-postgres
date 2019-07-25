use std::result::Result as StdResult;

use log::warn;
use postgres::{Client, Transaction, Statement, ToStatement, Row, QueryIter, Error as PgError};
use postgres::types::ToSql;

#[derive(Debug)]
pub struct Error {
    inner: Option<PgError>,
}

pub type Result<T> = StdResult<T, Error>;

impl From<PgError> for Error {
    fn from(error: PgError) -> Self {
        Error { inner: Some(error) }
    }
}

pub trait QueryDispatcher {
    fn prepare(&mut self, sql: &str) -> Result<Statement>;
    fn execute<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<u64>;
    fn query<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<Vec<Row>>;
    fn query_iter<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<QueryIter>;
}

impl QueryDispatcher for Client {
    fn prepare(&mut self, sql: &str) -> Result<Statement> {
        self.prepare(sql).map_err(|e| e.into())
    }
    fn execute<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<u64> {
        self.execute(statement, params).map_err(|e| e.into())
    }
    fn query<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        self.query(statement, params).map_err(|e| e.into())
    }
    fn query_iter<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<QueryIter> {
        self.query_iter(statement, params).map_err(|e| e.into())
    }
}

impl QueryDispatcher for Transaction<'_> {
    fn prepare(&mut self, sql: &str) -> Result<Statement> {
        self.prepare(sql).map_err(|e| e.into())
    }
    fn execute<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<u64> {
        self.execute(statement, params).map_err(|e| e.into())
    }
    fn query<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        self.query(statement, params).map_err(|e| e.into())
    }
    fn query_iter<S: ToStatement>(&mut self, statement: &S, params: &[&dyn ToSql]) -> Result<QueryIter> {
        self.query_iter(statement, params).map_err(|e| e.into())
    }
}

pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self>;
}

pub trait QueryOutput: Sized {
    fn fetch<D: QueryDispatcher, S: ToStatement>(dispatcher: &mut D, statement: &S, params: &[&dyn ToSql]) -> Result<Self>;
}

impl QueryOutput for () {
    fn fetch<D: QueryDispatcher, S: ToStatement>(dispatcher: &mut D, statement: &S, params: &[&dyn ToSql]) -> Result<Self> {
        dispatcher.execute(statement, params).map(|_| ())
    }
}

impl QueryOutput for AffectedRows {
    fn fetch<D: QueryDispatcher, S: ToStatement>(dispatcher: &mut D, statement: &S, params: &[&dyn ToSql]) -> Result<Self> {
        dispatcher.execute(statement, params).map(AffectedRows)
    }
}

impl<T: FromRow> QueryOutput for T {
    fn fetch<D: QueryDispatcher, S: ToStatement>(dispatcher: &mut D, statement: &S, params: &[&dyn ToSql]) -> Result<Self> {
        let result = dispatcher.query(statement, params)?;
        if result.len() > 1 {
            warn!("QueryOutput implemetation expected single row response, but got {} rows", result.len());
        }
        if let Some(row) = result.iter().nth(0) {
            T::from_row(row)
        } else {
            Err(Error { inner: None })
        }
    }
}

impl<T: FromRow> QueryOutput for Vec<T> {
    fn fetch<D: QueryDispatcher, S: ToStatement>(dispatcher: &mut D, statement: &S, params: &[&dyn ToSql]) -> Result<Self> {
        let result = dispatcher.query(statement, params)?;
        let mut output = Vec::with_capacity(result.len());
        for row in result {
            output.push(T::from_row(&row)?);
        }
        Ok(output)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AffectedRows(u64);

impl AffectedRows {
    pub fn count(&self) -> u64 {
        self.0
    }
}

#[macro_export]
macro_rules! postgres_prepared_statements {
    ($vis:vis $struct_name:ident { $( $query:ident ( $( $param_name:ident : $param_type:ty ),* ) -> $output:ty as $sql:expr ; )* }) => {
        $vis struct $struct_name {
            $(pub $query: ::postgres::Statement),*
        }
        impl $struct_name {
            pub fn setup<D: $crate::QueryDispatcher>(client: &mut D) -> $crate::Result<Self> {
                $(let $query = client.prepare($sql)?;)*
                Ok(Self { $($query),* })
            }
            $(
                pub fn $query<D: $crate::QueryDispatcher>(&self, dispatcher: &mut D, $($param_name: $param_type),*) -> $crate::Result<$output> {
                    <$output as $crate::QueryOutput>::fetch(dispatcher, &self.$query, &[$(&$param_name),*])
                }
            )*
        }
    };
}
