//! ```cargo
//! [dependencies]
//! yatws = { git = "https://github.com/drpngx/yatws", package = "yatws" }
//! anyhow = "1.0.86"
//! serde = { version = "1.0", features = ["derive"] }
//! ```

use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use yatws::{IBKRClient, OrderBuilder, OrderSide, TimeInForce};

#[derive(Serialize, Debug)]
struct OrderExecutionResult {
    status: String,
    symbol: String,
    side: String,
    quantity: f64,
    order_type: String,
    limit_price: Option<f64>,
    time_in_force: String,
    host: String,
    port: u16,
    client_id: i32,
    exchange: String,
    primary_exchange: Option<String>,
    currency: String,
    outside_rth: bool,
    dry_run: bool,
    order_id: Option<i64>,
    message: String,
}

fn parse_side(value: &str) -> Result<OrderSide> {
    match value.trim().to_uppercase().as_str() {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        other => Err(anyhow!("unsupported side: {}", other)),
    }
}

fn parse_tif(value: &str) -> TimeInForce {
    match value.trim().to_uppercase().as_str() {
        "GTC" => TimeInForce::GoodTillCanceled,
        "IOC" => TimeInForce::ImmediateOrCancel,
        "OPG" => TimeInForce::AtTheOpen,
        _ => TimeInForce::Day,
    }
}

fn main(
    symbol: String,
    side: String,
    quantity: f64,
    order_type: Option<String>,
    limit_price: Option<f64>,
    time_in_force: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    client_id: Option<i32>,
    exchange: Option<String>,
    primary_exchange: Option<String>,
    currency: Option<String>,
    outside_rth: Option<bool>,
    dry_run: Option<bool>,
) -> Result<OrderExecutionResult> {
    if quantity <= 0.0 {
        return Err(anyhow!("quantity must be > 0"));
    }

    let resolved_symbol = symbol.trim().to_uppercase();
    if resolved_symbol.is_empty() {
        return Err(anyhow!("symbol is required"));
    }

    let resolved_side_text = side.trim().to_uppercase();
    let resolved_side = parse_side(&resolved_side_text)?;
    let resolved_order_type = order_type.unwrap_or_else(|| "MKT".to_string()).trim().to_uppercase();
    let resolved_tif_text = time_in_force.unwrap_or_else(|| "DAY".to_string()).trim().to_uppercase();
    let resolved_tif = parse_tif(&resolved_tif_text);
    let resolved_host = host.unwrap_or_else(|| "host.docker.internal".to_string());
    let resolved_port = port.unwrap_or(4002);
    let resolved_client_id = client_id.unwrap_or(7);
    let resolved_exchange = exchange.unwrap_or_else(|| "SMART".to_string());
    let resolved_primary_exchange = primary_exchange
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let resolved_currency = currency.unwrap_or_else(|| "USD".to_string());
    let resolved_outside_rth = outside_rth.unwrap_or(false);
    let resolved_dry_run = dry_run.unwrap_or(true);

    let mut builder = OrderBuilder::new(resolved_side, quantity)
        .for_stock(&resolved_symbol)
        .with_exchange(&resolved_exchange)
        .with_currency(&resolved_currency)
        .with_tif(resolved_tif);

    if let Some(primary) = resolved_primary_exchange.as_ref() {
        builder = builder.with_primary_exchange(primary);
    }

    let (contract, mut order_request) = match resolved_order_type.as_str() {
        "LMT" => {
            let price = limit_price.context("limit_price is required for LMT orders")?;
            builder.limit(price).build()?
        }
        "MKT" => builder.market().build()?,
        other => return Err(anyhow!("unsupported order_type: {}", other)),
    };

    order_request.outside_rth = Some(resolved_outside_rth);

    if resolved_dry_run {
        return Ok(OrderExecutionResult {
            status: "dry_run".to_string(),
            symbol: resolved_symbol,
            side: resolved_side_text,
            quantity,
            order_type: resolved_order_type,
            limit_price,
            time_in_force: resolved_tif_text,
            host: resolved_host,
            port: resolved_port,
            client_id: resolved_client_id,
            exchange: resolved_exchange,
            primary_exchange: resolved_primary_exchange,
            currency: resolved_currency,
            outside_rth: resolved_outside_rth,
            dry_run: true,
            order_id: None,
            message: "Dry run enabled. Order was validated but not sent to IBKR.".to_string(),
        });
    }

    let client = IBKRClient::new(
        &resolved_host,
        resolved_port,
        resolved_client_id,
        None,
    )
    .with_context(|| format!("failed to connect to IBKR at {}:{}", resolved_host, resolved_port))?;

    let order_id = client
        .orders()
        .place_order(contract, order_request)
        .context("failed to place order via yatws")?;

    Ok(OrderExecutionResult {
        status: "placed".to_string(),
        symbol: resolved_symbol,
        side: resolved_side_text,
        quantity,
        order_type: resolved_order_type,
        limit_price,
        time_in_force: resolved_tif_text,
        host: resolved_host,
        port: resolved_port,
        client_id: resolved_client_id,
        exchange: resolved_exchange,
        primary_exchange: resolved_primary_exchange,
        currency: resolved_currency,
        outside_rth: resolved_outside_rth,
        dry_run: false,
        order_id: Some(order_id.into()),
        message: "Order submitted to IBKR.".to_string(),
    })
}
