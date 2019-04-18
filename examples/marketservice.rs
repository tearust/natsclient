extern crate pretty_env_logger;
#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

use natsclient::{self, AuthenticationStyle, Client, ClientOptions};
use serde_json;
use std::{thread, time::Duration};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    info!("Starting market service...");
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

    let c = client.clone();
    client.subscribe("symbolquery", move |msg| {
        info!("Received stock symbol query: {}", msg);
        match &msg.reply_to {
            Some(r) => {
                info!("About to respond with reply...");
                c.publish(&r, generate_symbol_reply(&msg.payload).as_slice(), None)?;
            }
            None => info!("Nowhere to send symbol query response..."),
        };
        Ok(())
    })?;

    for i in 0..100 {
        let tick = SymbolReply {
            symbol: "NATS".to_string(),
            price: i * 2,
            market_cap: 1000000,
            world_domination: true,
        };
        trace!("Sending tick");
        let slice = serde_json::to_vec(&tick).unwrap();
        client.publish("ticker", &slice, None).unwrap();
        thread::sleep(Duration::from_millis(500));
    }

    std::thread::park();

    Ok(())
}

fn generate_symbol_reply(payload: &Vec<u8>) -> Vec<u8> {
    let query: SymbolQuery = serde_json::from_slice(payload.as_slice()).unwrap();

    let reply = SymbolReply {
        symbol: query.symbol,
        price: 10000,
        market_cap: 200000,
        world_domination: true,
    };

    serde_json::to_vec(&reply).unwrap()
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
