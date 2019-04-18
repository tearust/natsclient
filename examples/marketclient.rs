extern crate pretty_env_logger;
#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;
use natsclient::{self, AuthenticationStyle, Client, ClientOptions};
use serde_json;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let jwt = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJBNDNRN1NLT0tCT0tYUDc1WVhMWjcyVDZKNDVIVzJKR0ZRWUJFQ1I2VE1FWEZFN1RKSjVBIiwiaWF0IjoxNTU0ODk2OTQ1LCJpc3MiOiJBQU9KV0RRV1pPQkNFTUVWWUQ2VEhPTUVCSExYS0NBMzZGU0dJVUxINFBWRU1ORDVUMjNEUEM0VSIsIm5hbWUiOiJiZW1pc191c2VyIiwic3ViIjoiVUNMRkYzTFBLTTQ3WTZGNkQ3UExMVzU2MzZKU1JDUFhFUUFDTEdVWTZNT01BS1lXMkk2VUFFRUQiLCJ0eXBlIjoidXNlciIsIm5hdHMiOnsicHViIjp7fSwic3ViIjp7fX19.3aH-hCSTS8z8rg2km7Q_aat5VpwT-t9swSmh3bnVBY_9IV9wE9mjSOUgHE2sq-7pR4HTCpYa0RPrNcgNfaVuBg";
    let seed = "SUACGBWJZLVP4CHF7WTY65KT3I4QHAQ5DEZMFAJKTIUIRQPXE6DVMFQEUU";
    let opts = ClientOptions::builder()
        .cluster_uris(vec!["nats://localhost:4222".into()])
        .authentication(AuthenticationStyle::UserCredentials(
            jwt.to_string(),
            seed.to_string(),
        ))
        .build()?;

    let client = Client::from_options(opts)?;
    client.connect()?;

    client.subscribe("ticker", move |msg| {
        let symbol: SymbolReply = serde_json::from_slice(msg.payload.as_slice()).unwrap();
        info!("Received stock ticker: {:?}", symbol);
        Ok(())
    })?;

    let reply = client
        .request(
            "symbolquery",
            r#"{"symbol": "DOOM"}"#.as_bytes(),
            Duration::from_millis(100),
        )
        .unwrap();

    let symbol: SymbolReply = serde_json::from_slice(reply.payload.as_slice()).unwrap();
    info!("Stock symbol response: {:?}", symbol);

    std::thread::park();

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct SymbolQuery {
    symbol: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SymbolReply {
    symbol: String,
    price: u64,
    market_cap: u64,
    world_domination: bool,
}
