![travis](https://travis-ci.org/encabulators/natsclient.svg?branch=master)&nbsp;
![license](https://img.shields.io/github/license/encabulators/natsclient.svg)


# NATS Client
A simple, developer-friendly NATS client designed with an ergonomic API designed to allow you to use this client anywhere, whether you're using `tokio` or single-threaded apps or traditional multi-threaded.

## Usage
The following sample illustrates basic publish and subscribe features:

```rust
let jwt = "...";
let seed = "...";
    
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
    let symbol: SymbolReply = serde_json::from_slice(&msg.payload).unwrap();
    info!("Received stock ticker: {:?}", symbol);
    Ok(())
})?;
```

To publish a message:

```rust
c.publish(&r, payload_bytes, None)?;
```

And to utilize the request/response pattern:

```rust
 let reply = client.request(
    "symbolquery",
    r#"{"symbol": "NATS"}"#.as_bytes(),
    Duration::from_millis(100),
)?;

let symbol: SymbolReply = serde_json::from_slice(&reply.payload).unwrap();
info!("Stock symbol response: {:?}", symbol);
```

## Features
The following is a list of features currently supported and planned by this client:

* [X] - Request/Reply
* [X] - Subscribe
* [X] - Publish
* [X] - All authentication models, including NATS 2.0 JWT and seed keys
* [X] - Adherance to protocol v1, accepts new server information whenever it's sent from NATS
* [ ] - Automatic reconnect upon connection failure
* [ ] - TLS support
* [ ] - NATS Streaming (STAN)
