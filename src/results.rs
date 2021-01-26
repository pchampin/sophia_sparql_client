use super::Error;
use serde::{Deserialize, Serialize};
use sophia::ns::xsd;
use sophia::term::BoxTerm;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ResultsDocument {
    Boolean {
        head: BooleanHead,
        boolean: bool,
    },
    Bindings {
        #[serde(flatten)]
        doc: BindingsDocument,
    },
}

/// The result of a `SELECT` query as returned by [`SparqlClient`](super::SparqlClient).
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct BindingsDocument {
    pub(super) head: BindingsHead,
    pub(super) results: Results,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct BooleanHead {
    #[serde(default)]
    link: Vec<Box<str>>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct BindingsHead {
    pub(super) vars: Vec<Box<str>>,
    #[serde(default)]
    link: Vec<Box<str>>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct Results {
    pub(super) bindings: Vec<HashMap<Box<str>, Term>>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum Term {
    #[serde(rename = "bnode")]
    Bnode { value: Box<str> },
    #[serde(rename = "literal")]
    Literal(Literal),
    #[serde(rename = "uri")]
    Uri { value: Box<str> },
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Literal {
    Datatype {
        value: Box<str>,
        datatype: Box<str>,
    },
    Lang {
        value: Box<str>,
        #[serde(rename = "xml:lang")]
        lang: Box<str>,
    },
    Simple {
        value: Box<str>,
    },
}

impl TryFrom<Term> for BoxTerm {
    type Error = Error;
    fn try_from(other: Term) -> Result<BoxTerm, Error> {
        use self::Literal::*;
        use Term::*;
        match other {
            Bnode { value } => Ok(BoxTerm::new_bnode(value)?),
            Literal(Simple { value }) => Ok(BoxTerm::new_literal_dt(value, xsd::string)?),
            Literal(Datatype { value, datatype }) => {
                Ok(BoxTerm::new_literal_dt(value, BoxTerm::new_iri(datatype)?)?)
            }
            Literal(Lang { value, lang }) => Ok(BoxTerm::new_literal_lang(value, lang)?),
            Uri { value } => Ok(BoxTerm::new_iri(value)?),
        }
    }
}

impl BindingsDocument {
    pub(super) fn pop_binding(&mut self) -> Result<Vec<Option<BoxTerm>>, Error> {
        debug_assert!(!self.results.bindings.is_empty());
        let mut hm = self.results.bindings.drain(..1).next().unwrap();
        let mut v = Vec::<Option<BoxTerm>>::with_capacity(self.head.vars.len());
        for key in &self.head.vars {
            match hm.remove(&*key) {
                None => v.push(None),
                Some(term) => v.push(Some(term.try_into()?)),
            }
        }
        Ok(v)
    }
}

#[cfg(test)]
mod test_json {
    use super::*;
    use serde_json;

    #[test]
    fn uri() {
        let src = r#"{
            "type": "uri",
            "value": "tag:u"
        }"#;
        let got: Term = serde_json::from_str(src).unwrap();
        let exp = Term::Uri {
            value: "tag:u".into(),
        };
        assert_eq!(got, exp);
    }

    #[test]
    fn literal_simple() {
        let src = r#"{
            "type": "literal",
            "value": "simple"
        }"#;
        let got: Term = serde_json::from_str(src).unwrap();
        let exp = Term::Literal(Literal::Simple {
            value: "simple".into(),
        });
        assert_eq!(got, exp);
    }

    #[test]
    fn literal_datatype() {
        let src = r#"{
            "type": "literal",
            "value": "datatype",
            "datatype": "tag:d"
        }"#;
        let got: Term = serde_json::from_str(src).unwrap();
        let exp = Term::Literal(Literal::Datatype {
            value: "datatype".into(),
            datatype: "tag:d".into(),
        });
        assert_eq!(got, exp);
    }

    #[test]
    fn literal_lang() {
        let src = r#"{
            "type": "literal",
            "value": "lang",
            "xml:lang": "en"
        }"#;
        let got: Term = serde_json::from_str(src).unwrap();
        let exp = Term::Literal(Literal::Lang {
            value: "lang".into(),
            lang: "en".into(),
        });
        assert_eq!(got, exp);
    }

    #[test]
    fn bnode() {
        let src = r#"{
            "type": "bnode",
            "value": "bnode"
        }"#;
        let got: Term = serde_json::from_str(src).unwrap();
        let exp = Term::Bnode {
            value: "bnode".into(),
        };
        assert_eq!(got, exp);
    }

    #[test]
    fn empty_results() {
        let src = r#"{
            "bindings": []
        }"#;
        let got: Results = serde_json::from_str(src).unwrap();
        let exp = Results { bindings: vec![] };
        assert_eq!(got, exp);
    }

    #[test]
    fn len1_results() {
        let src = r#"{
            "bindings": [
                {
                    "a": {
                        "type": "uri",
                        "value": "tag:a0"
                    },
                    "b": {
                        "type": "literal",
                        "value": "simple"
                    },
                    "c": {
                        "type": "bnode",
                        "value": "bn0"
                    }
                }
            ]
        }"#;
        let got: Results = serde_json::from_str(src).unwrap();
        let exp = Results {
            bindings: vec![vec![
                (
                    "a".into(),
                    Term::Uri {
                        value: "tag:a0".into(),
                    },
                ),
                (
                    "b".into(),
                    Term::Literal(Literal::Simple {
                        value: "simple".into(),
                    }),
                ),
                (
                    "c".into(),
                    Term::Bnode {
                        value: "bn0".into(),
                    },
                ),
            ]
            .into_iter()
            .collect()],
        };
        assert_eq!(got, exp);
    }

    #[test]
    fn bindings_head() {
        let src = r#"{
            "vars": ["a", "b", "c"]
        }"#;
        let got: BindingsHead = serde_json::from_str(src).unwrap();
        let exp = BindingsHead {
            vars: vec!["a".into(), "b".into(), "c".into()],
            link: vec![],
        };
        assert_eq!(got, exp);
    }

    #[test]
    fn bindings_doc() {
        let src = r#"
        {
            "head": {
                "vars": ["a", "b", "c"]
            },
            "results": {
                "bindings": [
                    {
                        "a": {
                            "type": "uri",
                            "value": "tag:a0"
                        },
                        "b": {
                            "type": "literal",
                            "value": "simple"
                        },
                        "c": {
                            "type": "bnode",
                            "value": "bn0"
                        }
                    },
                    {
                        "c": {
                            "type": "literal",
                            "value": "datatype",
                            "datatype": "tag:d1"
                        },
                        "a": {
                            "type": "literal",
                            "value": "lang",
                            "xml:lang": "en"
                        }
                    }
                ]
            }
        }"#;
        let got: ResultsDocument = serde_json::from_str(src).unwrap();
        let exp = ResultsDocument::Bindings {
            doc: BindingsDocument {
                head: BindingsHead {
                    vars: vec!["a".into(), "b".into(), "c".into()],
                    link: vec![],
                },
                results: Results {
                    bindings: vec![
                        vec![
                            (
                                "a".into(),
                                Term::Uri {
                                    value: "tag:a0".into(),
                                },
                            ),
                            (
                                "b".into(),
                                Term::Literal(Literal::Simple {
                                    value: "simple".into(),
                                }),
                            ),
                            (
                                "c".into(),
                                Term::Bnode {
                                    value: "bn0".into(),
                                },
                            ),
                        ]
                        .into_iter()
                        .collect::<HashMap<Box<str>, Term>>(),
                        vec![
                            (
                                "c".into(),
                                Term::Literal(Literal::Datatype {
                                    value: "datatype".into(),
                                    datatype: "tag:d1".into(),
                                }),
                            ),
                            (
                                "a".into(),
                                Term::Literal(Literal::Lang {
                                    value: "lang".into(),
                                    lang: "en".into(),
                                }),
                            ),
                        ]
                        .into_iter()
                        .collect::<HashMap<Box<str>, Term>>(),
                    ],
                },
            },
        };
        assert_eq!(got, exp);
    }

    #[test]
    fn boolean_doc() {
        let src = r#"
        {
            "head": {},
            "boolean": true
        }"#;
        let got: ResultsDocument = serde_json::from_str(src).unwrap();
        let exp = ResultsDocument::Boolean {
            head: BooleanHead { link: vec![] },
            boolean: true,
        };
        assert_eq!(got, exp);
    }
}
