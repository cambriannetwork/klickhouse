use std::fmt;

use crate::{KlickhouseError, Result, ToSql, Value};

mod select;
pub use select::*;

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    id:Option<String>,
    pub query:String
}

impl ParsedQuery {
    pub fn new(query: String) -> Self {
        ParsedQuery { id: None, query }
    }

    fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn id(&self) -> Option<&String> {
        self.id.as_ref()
    }
}

impl fmt::Display for ParsedQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.query)
    }
}

impl TryInto<ParsedQuery> for String {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery::new(self))
    }
}

impl<'a> TryInto<ParsedQuery> for &'a str {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery::new(self.to_string()))
    }
}

impl<'a> TryInto<ParsedQuery> for &'a String {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        Ok(ParsedQuery::new(self.clone()))
    }
}

impl<T1:AsRef<str>,T2:AsRef<str>> TryInto<ParsedQuery> for (T1,T2){
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        let query = self.1.as_ref().to_owned();
        let id = self.0.as_ref().to_owned();
        Ok(ParsedQuery::new(query).with_id(id))
    }
}

#[derive(Clone)]
pub struct QueryBuilder<'a> {
    base: &'a str,
    arguments: Vec<Result<Value>>,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(query: &'a str) -> Self {
        Self {
            base: query,
            arguments: vec![],
        }
    }

    pub fn arg(mut self, arg: impl ToSql) -> Self {
        self.arguments.push(arg.to_sql(None));
        self
    }

    pub fn args<A: ToSql>(mut self, args: impl IntoIterator<Item = A>) -> Self {
        self.arguments
            .extend(args.into_iter().map(|x| x.to_sql(None)));
        self
    }

    pub fn finalize(self) -> Result<ParsedQuery> {
        self.try_into()
    }
}

impl<'a> TryInto<ParsedQuery> for QueryBuilder<'a> {
    type Error = KlickhouseError;

    fn try_into(self) -> Result<ParsedQuery> {
        let arguments = self.arguments.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(
            ParsedQuery::new(
                crate::query_parser::parse_query_arguments(self.base,&arguments[..])
            )
        )
    }
}
