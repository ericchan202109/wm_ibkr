---
name: yatws-ibkr-rust
description: Expert guide for using the yatws Rust crate to interact with Interactive Brokers TWS API. Covers connection, order management, market data, and options strategies.
---

# YATWS (Yet Another TWS API) Expert

## Goal
To effectively write, debug, and optimize Rust code using the `yatws` crate for high-performance interaction with Interactive Brokers' Trader Workstation (TWS) or IB Gateway.

## Architecture Overview
`yatws` utilizes a **Client-Manager** architecture. You do not interact with a single monolithic object for everything.
1.  **IBKRClient**: The entry point. Handles the connection lifecycle.
2.  **Managers**: Domain-specific accessors returned by the client (e.g., `.orders()`, `.account()`, `.data_market()`).
3.  **Builders**: Fluent interfaces for constructing complex objects like Orders and Option Strategies.

## Core Rules & Patterns
- **Blocking vs. Async**: The library supports both. Use blocking calls for simple scripts and the Observer pattern (Async) for high-frequency/event-driven systems.
- **Thread Safety**: The client and managers are thread-safe. You can share the `client` across threads.
- **Error Handling**: Functions return `Result<T, IBKRError>`. Always handle potential connection drops or API limits.

## API Usage Reference

### 1. Establishing a Connection
The client requires the host, port, client ID, and an optional session storage path (for recording/replay).

```rust
use yatws::IBKRClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to TWS (default port 7496) or Gateway (7497)
    // Args: Host, Port, ClientID, SessionFile (optional)
    let client = IBKRClient::new("127.0.0.1", 7497, 0, None)?;
    
    println!("Connected to IBKR!");
    Ok(())
}
2. Placing Orders (OrderBuilder)
Use OrderBuilder to construct orders safely. Access place_order through client.orders().

Rust
use yatws::{OrderBuilder, OrderSide, TimeInForce};

// Build a Limit Order for AAPL
let (contract, order_request) = OrderBuilder::new(OrderSide::Buy, 100.0)
    .for_stock("AAPL")
    .with_exchange("SMART")
    .with_currency("USD")
    .limit(150.0) // Set limit price
    .with_tif(TimeInForce::Day)
    .build()?;

// Place the order via the Order Manager
let order_id = client.orders().place_order(contract, order_request)?;
println!("Order placed with ID: {}", order_id);
3. Market Data (Synchronous)
Fetch a snapshot quote using data_market().

Rust
use std::time::Duration;

// Fetch a quote with a 5-second timeout
let (bid, ask, last) = client.data_market().get_quote(
    &contract, 
    None, // Generic tick list (optional)
    Duration::from_secs(5)
)?;

println!("Bid: {}, Ask: {}, Last: {}", bid, ask, last);
4. Historical Data
Retrieve historical bars. Note the use of DurationUnit and BarSize.

Rust
use yatws::contract::{BarSize, WhatToShow};
use yatws::data::DurationUnit;

let bars = client.data_market().get_historical_data(
    &contract,
    None,               // End DateTime (None = Now)
    DurationUnit::Day(1), // Duration string
    BarSize::Hour1,     // Bar Size
    WhatToShow::Trades, // Type of data
    true,               // Use Regular Trading Hours (RTH)
    1,                  // Date Format style
    false,              // Keep up to date (streaming)
    None,               // Chart options
    &[]
)?;

for bar in bars {
    println!("Time: {}, Close: {}", bar.date, bar.close);
}
5. Options Strategy Builder
yatws provides a helper to build multi-leg option strategies (e.g., Bull Call Spread).

Rust
use yatws::OptionsStrategyBuilder;
use yatws::contract::SecType;

let builder = OptionsStrategyBuilder::new(
    client.data_ref(), 
    "AAPL", 
    150.0, // Underlying price (reference)
    10.0,  // Quantity
    SecType::Stock
)?;

let (contract, order) = builder
    .bull_call_spread(
        "20251219", // Expiration YYYYMMDD
        150.0,      // Strike 1 (Buy)
        160.0       // Strike 2 (Sell)
    )?
    .with_limit_price(3.50) // Net debit limit
    .build()?;

client.orders().place_order(contract, order)?;
6. Account Data
Access portfolio and funds via client.account().

Rust
use yatws::account::AccountValueKey;

// Get Buying Power
let bp = client.account().get_account_value(AccountValueKey::BuyingPower)?;
if let Some(val) = bp {
    println!("Buying Power: {} {}", val.value, val.currency.unwrap_or_default());
}

// List Open Positions
let positions = client.account().list_open_positions()?;
for pos in positions {
    println!("{:?} : {}", pos.contract.symbol, pos.position);
}