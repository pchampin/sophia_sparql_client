use sophia::api::sparql::{SparqlDataset, SparqlResult};
use sophia::api::term::Term;
use sophia_sparql_client::SparqlClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = SparqlClient::new("https://query.wikidata.org/bigdata/namespace/wdq/sparql");
    let query = r#"
        #All Dr. Who performers
        SELECT ?doctor ?doctorLabel ?ordinal ?performer ?performerLabel
        WHERE {
          ?doctor p:P31 ?type. ?type ps:P31 wd:Q47543030 .
          OPTIONAL { ?type pq:P1545 ?ordinal } OPTIONAL { ?doctor wdt:P1545 ?ordinal }
          OPTIONAL { ?doctor p:P175 / ps:P175 ?performer }
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en" }
        }
        ORDER BY ASC(xsd:integer(?ordinal) )
    "#;
    if let SparqlResult::Bindings(bindings) = cli.query(query)? {
        for b in bindings {
            let b = b?;
            let doctor_label = b[1].as_ref().and_then(|t| t.lexical_form()).unwrap();
            let performer_label = b[4]
                .as_ref()
                .and_then(|t| t.lexical_form())
                .unwrap_or("NULL".into());
            println!("{:?}\t{:?}", doctor_label, performer_label);
        }
    } else {
        panic!("Unexpected results for the query.");
    }
    Ok(())
}
