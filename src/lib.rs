//! A client implementation of the [SPARQL1.1 protocol] based on [Sophia].
//! It implements [`sophia::sparql::SparqlDataset`].
//!
//! Example:
//! ```
//! use sophia::sparql::{SparqlDataset, SparqlResult};
//! use sophia::term::TTerm;
//! use sophia_sparql_client::SparqlClient;
//!
//! # fn bla() -> Result<(), Box<dyn std::error::Error>> {
//! let cli = SparqlClient::new("https://query.wikidata.org/bigdata/namespace/wdq/sparql");
//! let query = r#"
//!     #All Dr. Who performers
//!     #added 2017-07-16, updated 2020-07-08
//!     SELECT ?doctor ?doctorLabel ?ordinal ?performer ?performerLabel
//!     WHERE {
//!       ?doctor wdt:P31 wd:Q47543030 .
//!       OPTIONAL { ?doctor wdt:P1545 ?ordinal }
//!       OPTIONAL { ?doctor p:P175 / ps:P175 ?performer }
//!       SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en" }
//!     }
//!     ORDER BY ASC(xsd:integer(?ordinal) )
//! "#;
//! if let SparqlResult::Bindings(bindings) = cli.query(query)? {
//!     for b in bindings {
//!         let b = b?;
//!         let doctor_label = b[1].as_ref().unwrap().value();
//!         let performer_label = b[4].as_ref().unwrap().value();
//!         println!("{}\t{}", doctor_label, performer_label);
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! [SPARQL1.1 protocol]: https://www.w3.org/TR/sparql11-protocol/
//! [Sophia]: https://docs.rs/sophia/
use sophia::parser::{nt, turtle, xml};
use sophia::sparql::{Query as SparqlQuery, SparqlBindings, SparqlDataset, SparqlResult, ToQuery};
use sophia::term::{BoxTerm, CopyTerm};
use sophia::triple::stream::TripleSource;
use sophia::triple::Triple;
use std::borrow::Borrow;
use std::io::BufReader;
use ureq::{Agent, Error as UreqError};

mod results;
pub use results::BindingsDocument as Bindings;
use results::ResultsDocument;

pub struct SparqlClient {
    endpoint: Box<str>,
    agent: Agent,
    accept: Option<String>,
}

impl SparqlClient {
    /// The default [Accept HTTP header](https://tools.ietf.org/html/rfc7231.html#section-5.3.2) used by clients.
    const DEFAULT_ACCEPT: &'static str = "application/sparql-results+json,application/sparql-results+xml;q=0.8,text/turtle,application/n-triples;q=0.9,application/rdf+xml;q=0.8";

    /// Create a [`SparqlClient`] on the given SPARQL-endpoint URL.
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: Box::from(endpoint),
            agent: Agent::new(),
            accept: None,
        }
    }

    /// Replace the underlying [`ureq::Agent`] of this client.
    pub fn with_agent(mut self, agent: Agent) -> Self {
        self.agent = agent;
        self
    }

    /// Replace the [Accept HTTP header](https://tools.ietf.org/html/rfc7231.html#section-5.3.2) used by this client.
    ///
    /// This might be useful if the endpoint implements content-negotation incorrectly.
    ///
    /// See also [`DEFAULT_ACCEPT`](Self::DEFAULT_ACCEPT)
    pub fn with_accept<T: ToString>(mut self, accept: T) -> Self {
        self.accept = Some(accept.to_string());
        self
    }

    /// The [Accept HTTP header](https://tools.ietf.org/html/rfc7231.html#section-5.3.2) used by this client.
    pub fn accept(&self) -> &str {
        self.accept.as_deref().unwrap_or(Self::DEFAULT_ACCEPT)
    }

    fn wrap_triple_source<T: TripleSource + 'static>(
        triples: T,
    ) -> Result<SparqlResult<Self>, Error>
    where
        Error: From<T::Error>,
    {
        let it: Box<dyn Iterator<Item = Result<[BoxTerm; 3], Error>>> = Box::new(
            triples
                .map_triples(|t| {
                    [
                        BoxTerm::copy(t.s()),
                        BoxTerm::copy(t.p()),
                        BoxTerm::copy(t.o()),
                    ]
                })
                .into_iter()
                .map(|r| r.map_err(Error::from)),
        );
        Ok(SparqlResult::Triples(it))
    }
}

impl SparqlDataset for SparqlClient {
    type BindingsTerm = BoxTerm;
    type BindingsResult = Bindings;
    type TriplesResult = Box<dyn Iterator<Item = Result<[BoxTerm; 3], Error>>>;
    type SparqlError = Error;
    type Query = Query;

    fn query<Q>(&self, query: Q) -> Result<SparqlResult<Self>, Error>
    where
        Q: ToQuery<Query>,
    {
        let query = query.to_query()?;
        let resp = self
            .agent
            .post(&self.endpoint)
            .set("Accept", self.accept())
            .set("Content-type", "application/sparql-query")
            .send_string(&query.borrow().0)?;
        use ResultsDocument::*;
        match resp.content_type() {
            "application/sparql-results+json" => match resp.into_json::<ResultsDocument>()? {
                Boolean { boolean, .. } => Ok(SparqlResult::Boolean(boolean)),
                Bindings { doc } => Ok(SparqlResult::Bindings(doc)),
            },
            "application/sparql-results+xml" => {
                todo!("XML bindings not supported yet")
            }
            "text/turtle" => {
                Self::wrap_triple_source(turtle::parse_bufread(BufReader::new(resp.into_reader())))
            }
            "application/n-triples" => {
                Self::wrap_triple_source(nt::parse_bufread(BufReader::new(resp.into_reader())))
            }
            "application/rdf+xml" => {
                Self::wrap_triple_source(xml::parse_bufread(BufReader::new(resp.into_reader())))
            }
            ctype => Err(Error::Unsupported(format!(
                "unsupported content-type: {0}",
                ctype
            ))),
        }
    }
}

impl SparqlBindings<SparqlClient> for Bindings {
    fn variables(&self) -> Vec<&str> {
        self.head
            .vars
            .iter()
            .map(|b| b.as_ref())
            .collect::<Vec<&str>>()
    }
}

impl Iterator for Bindings {
    type Item = Result<Vec<Option<BoxTerm>>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.results.bindings.is_empty() {
            None
        } else {
            Some(self.pop_binding())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(
        #[source]
        #[from]
        std::io::Error,
    ),
    #[error("http error: {0}")]
    Http(#[source] Box<UreqError>),
    #[error("{0}")]
    Unsupported(String),
    #[error("invalid term: {0}")]
    Term(
        #[source]
        #[from]
        sophia::term::TermError,
    ),
    #[error("turtle parsing error: {0}")]
    RioTurtle(
        #[source]
        #[from]
        rio_turtle::TurtleError,
    ),
    #[error("RDF/XML parsing error: {0}")]
    RioXml(
        #[source]
        #[from]
        rio_xml::RdfXmlError,
    ),
}

impl From<UreqError> for Error {
    fn from(other: UreqError) -> Error {
        Error::Http(Box::new(other))
    }
}

/// A SPARQL query prepared for a [`SparqlClient`].
///
/// NB: Actually, this type simply wraps the query as a `Box<str>`,
/// so [preparing](sophia::sparql::SparqlDataset::prepare_query)
/// it in advance has no benefit for this implementation.
pub struct Query(Box<str>);

impl SparqlQuery for Query {
    type Error = Error;

    fn parse(query_source: &str) -> Result<Self, Self::Error> {
        Ok(Query(Box::from(query_source)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sophia::graph::isomorphic_graphs;
    use sophia::ns::xsd;
    use sophia::term::{TTerm, TermKind};
    use SparqlResult::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn client() -> SparqlClient {
        let endpoint = match std::env::var("SOPHIA_SPARQL_ENDPOINT").ok() {
            None => "http://localhost:8080/sparql".to_string(),
            Some(ret) => ret,
        };
        SparqlClient::new(&endpoint)
    }

    #[test]
    fn select_simple() -> TestResult {
        match client().query("SELECT (42 as ?answer) {}")? {
            Bindings(b) => {
                assert_eq!(b.variables(), vec!["answer".to_string()]);
                let bindings = b.into_iter().collect::<Vec<_>>();
                assert_eq!(bindings.len(), 1);
                assert_eq!(
                    bindings[0].as_ref().unwrap()[0],
                    Some(BoxTerm::new_literal_dt("42", xsd::integer)?)
                );
            }
            _ => assert!(false),
        };
        Ok(())
    }

    #[test]
    fn select_complex() -> TestResult {
        match client().query(
            r#"
            PREFIX : <tag:>
            SELECT ?x ?y ?z {}
            VALUES (?x ?y ?z) {
                (:a "simple" 42)
                (UNDEF "lang"@en UNDEF)
                (UNDEF UNDEF UNDEF)
            }
        "#,
        )? {
            Bindings(b) => {
                assert_eq!(
                    b.variables(),
                    vec!["x".to_string(), "y".to_string(), "z".to_string()]
                );
                let bindings = b.into_iter().collect::<Vec<_>>();
                assert_eq!(bindings.len(), 3);
                assert_eq!(
                    bindings[0].as_ref().unwrap()[0],
                    Some(BoxTerm::new_iri("tag:a")?)
                );
                assert_eq!(
                    bindings[0].as_ref().unwrap()[1],
                    Some(BoxTerm::new_literal_dt("simple", xsd::string)?)
                );
                assert_eq!(
                    bindings[0].as_ref().unwrap()[2],
                    Some(BoxTerm::new_literal_dt("42", xsd::integer)?)
                );
                assert_eq!(bindings[1].as_ref().unwrap()[0], None);
                assert_eq!(
                    bindings[1].as_ref().unwrap()[1],
                    Some(BoxTerm::new_literal_lang("lang", "en")?)
                );
                assert_eq!(bindings[1].as_ref().unwrap()[2], None);
                assert_eq!(bindings[2].as_ref().unwrap()[0], None);
                assert_eq!(bindings[2].as_ref().unwrap()[1], None);
                assert_eq!(bindings[2].as_ref().unwrap()[2], None);
            }
            _ => assert!(false),
        };
        Ok(())
    }

    #[test]
    fn select_bnode() -> TestResult {
        match client().query(
            r#"
            PREFIX : <tag:>
            SELECT ?x {
                BIND(BNODE() as ?x)
            }
        "#,
        )? {
            Bindings(b) => {
                assert_eq!(b.variables(), vec!["x".to_string(),]);
                let bindings = b.into_iter().collect::<Vec<_>>();
                assert_eq!(bindings.len(), 1);
                assert_eq!(
                    bindings[0].as_ref().unwrap()[0].as_ref().unwrap().kind(),
                    TermKind::BlankNode
                );
            }
            _ => assert!(false),
        };
        Ok(())
    }

    #[test]
    fn ask_true() -> TestResult {
        match client().query("ASK {}")? {
            Boolean(true) => (),
            _ => assert!(false),
        };
        Ok(())
    }

    #[test]
    fn ask_false() -> TestResult {
        match client().query("PREFIX : <tag:> ASK {:abcdef :ghijkl :mnopqr}")? {
            Boolean(false) => (),
            _ => assert!(false),
        };
        Ok(())
    }

    #[test]
    fn construct_empty() -> TestResult {
        test_construct(client(), "")
    }

    #[test]
    fn construct_one_triple() -> TestResult {
        test_construct(client(), "[] a 42.")
    }

    #[test]
    fn construct_complex() -> TestResult {
        test_construct(client(), COMPLEX_TTL)
    }

    #[test]
    fn construct_ntriples() -> TestResult {
        test_construct(client().with_accept("application/n-triples"), COMPLEX_TTL)
    }

    #[test]
    fn construct_rdfxml() -> TestResult {
        test_construct(client().with_accept("application/rdf+xml"), COMPLEX_TTL)
    }

    const COMPLEX_TTL: &'static str = r#"
        :s :p1 :o1, :o2;
        :p2 :o1, :o3;
        :label "S".
    "#;

    fn test_construct(client: SparqlClient, ttl: &str) -> TestResult {
        let src = format!("@prefix : <tag:>. {}", ttl);
        let exp: Vec<[BoxTerm; 3]> = turtle::parse_str(&src).collect_triples()?;
        let q = format!("PREFIX : <tag:> CONSTRUCT {{ {} }} {{}}", ttl);

        match client.query(q.as_str())? {
            Triples(triples) => {
                let got: Vec<[BoxTerm; 3]> = triples.collect_triples()?;
                assert!(isomorphic_graphs(&got, &exp)?);
            }
            _ => assert!(false),
        };
        Ok(())
    }
}
