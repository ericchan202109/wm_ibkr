//! ```cargo
//! [dependencies]
//! yatws = { git = "https://github.com/drpngx/yatws", package = "yatws" }
//! postgres = { version = "0.19.7", features = ["with-serde_json-1"] }
//! anyhow = "1.0.86"
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! ```

use yatws::{IBKRClient, contract::Contract, data::MarketDataType};
use postgres::{Client, NoTls};
use std::time::Duration;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize, Debug)]
struct MarketQuote {
    symbol: String,
    bid: Option<f64>,
    ask: Option<f64>,
    last: Option<f64>,
}

#[derive(Serialize, Debug)]
struct MarketDataResult {
    quotes: Vec<MarketQuote>,
    db_contacts_count: i64,
}

fn main(port: Option<u16>) -> Result<MarketDataResult> {
    let ibkr_port = port.unwrap_or(4002);

    // 1. Connect to PostgreSQL Database
    let mut db_client = Client::connect("host=db user=postgres password=changeme dbname=windmill", NoTls)
        .context("Failed to connect to PostgreSQL database")?;
    
    // Create the tables if they don't exist
    db_client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS contracts (
            symbol TEXT PRIMARY KEY,
            conid INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS market_data (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL REFERENCES contracts(symbol),
            data JSONB NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        "
    ).context("Failed to create tables")?;

    // 2. Connect to IBKR TWS/Gateway
    println!("Connecting to IBKR on host.docker.internal:{}...", ibkr_port);
    let ibkr_client = IBKRClient::new("host.docker.internal", ibkr_port, 1, None)
        .context("Failed to connect to IBKR")?;

    let mut tsla = Contract::stock("TSLA");
    tsla.primary_exchange = Some("NASDAQ".to_string());
    let mut qqq = Contract::stock("QQQ");
    qqq.primary_exchange = Some("NASDAQ".to_string());

    let symbols = vec![tsla, qqq];

    let mut quotes = Vec::new();

    for contract in symbols {
        let symbol_str = contract.symbol.clone();
        println!("Fetching details for {}...", symbol_str);
        
        let details = ibkr_client.data_ref().get_contract_details(&contract)?;
        if let Some(detail) = details.first() {
            let conid = detail.contract.con_id;
            println!("Got conid for {}: {}", symbol_str, conid);
            
            // Store Metadata in DB
            db_client.execute(
                "INSERT INTO contracts (symbol, conid) VALUES ($1, $2) ON CONFLICT (symbol) DO UPDATE SET conid = EXCLUDED.conid",
                &[&symbol_str, &conid],
            )?;
            
            // 3. Fetch Real-Time Market Data Quote
            let mut bid_price = None;
            let mut ask_price = None;
            let mut last_price = None;
            
            println!("Fetching real-time quote for {}...", symbol_str);
            // Explicitly request RealTime market data to avoid simulated/delayed feeds
            let quote_result = ibkr_client.data_market().get_quote(
                &detail.contract, 
                Some(MarketDataType::RealTime), 
                Duration::from_secs(5)
            );
            
            match quote_result {
                Ok((bid, ask, last)) => {
                    bid_price = bid;
                    ask_price = ask;
                    last_price = last;
                    println!("Quote for {}: Bid: {:?}, Ask: {:?}, Last: {:?}", symbol_str, bid, ask, last);
                    
                    // Construct JSON object for the quote
                    let quote_json = json!({
                        "bid": bid,
                        "ask": ask,
                        "last": last
                    });
                    
                    // Insert JSONB array/object into market_data table
                    db_client.execute(
                        "INSERT INTO market_data (symbol, data) VALUES ($1, $2)",
                        &[&symbol_str, &quote_json],
                    )?;
                }
                Err(e) => {
                    println!("Warning: Could not fetch real-time quote for {}: {:?}", symbol_str, e);
                }
            }
            
            quotes.push(MarketQuote {
                symbol: symbol_str,
                bid: bid_price,
                ask: ask_price,
                last: last_price,
            });
        } else {
            println!("Could not find contract details for {}", symbol_str);
        }
    }

    // List DB contents for validation count
    let row = db_client.query_one("SELECT COUNT(*) FROM contracts", &[])?;
    let db_contacts_count: i64 = row.get(0);

    Ok(MarketDataResult {
        quotes,
        db_contacts_count,
    })
}
