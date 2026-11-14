//! Self-contained HTTP endpoint descriptor.
//!
//! Encapsulates route path, HTTP method, command mapping, query parsing, and
//! path parameter transformation so each resource module declares its contract
//! in one place.
use super::error::Error;
use axum::{extract::Query, http::Uri, routing::MethodFilter};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verb {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl Verb {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }

    pub(crate) fn filter(self) -> MethodFilter {
        match self {
            Self::Get => MethodFilter::GET,
            Self::Post => MethodFilter::POST,
            Self::Put => MethodFilter::PUT,
            Self::Patch => MethodFilter::PATCH,
            Self::Delete => MethodFilter::DELETE,
        }
    }
}

pub(crate) type QueryParser = fn(&Uri) -> Result<Value, Error>;
pub(crate) type PathMapper = fn(&HashMap<String, String>, &mut Value);

pub(crate) struct Endpoint {
    pub(crate) path: &'static str,
    pub(crate) verb: Verb,
    pub(crate) command: &'static str,
    pub(crate) query_parser: Option<QueryParser>,
    pub(crate) path_mapper: Option<PathMapper>,
}

impl Endpoint {
    pub(crate) const fn get(path: &'static str, command: &'static str) -> Self {
        Self::new(Verb::Get, path, command)
    }

    pub(crate) const fn post(path: &'static str, command: &'static str) -> Self {
        Self::new(Verb::Post, path, command)
    }

    pub(crate) const fn put(path: &'static str, command: &'static str) -> Self {
        Self::new(Verb::Put, path, command)
    }

    pub(crate) const fn patch(path: &'static str, command: &'static str) -> Self {
        Self::new(Verb::Patch, path, command)
    }

    pub(crate) const fn delete(path: &'static str, command: &'static str) -> Self {
        Self::new(Verb::Delete, path, command)
    }

    pub(crate) const fn new(verb: Verb, path: &'static str, command: &'static str) -> Self {
        Self {
            path,
            verb,
            command,
            query_parser: None,
            path_mapper: None,
        }
    }

    pub(crate) const fn with_query(mut self, parser: QueryParser) -> Self {
        self.query_parser = Some(parser);
        self
    }

    pub(crate) const fn with_path_mapper(mut self, mapper: PathMapper) -> Self {
        self.path_mapper = Some(mapper);
        self
    }
}

pub(crate) fn parse_query<T: DeserializeOwned + Serialize>(uri: &Uri) -> Result<Value, Error> {
    let Query(query) = Query::<T>::try_from_uri(uri).map_err(Error::query)?;
    serde_json::to_value(query).map_err(|_| Error::internal("Parsed query serialization failed"))
}
