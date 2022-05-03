//! Submodule containing helper methods to pre-process auction data before passing it on to the solvers.

use crate::{liquidity::LimitOrder, settlement::external_prices::ExternalPrices};
use anyhow::{Context as _, Result};
use chrono::Utc;
use gas_estimation::GasPriceEstimating;
use model::order::Order;
use num::ToPrimitive as _;
use shared::conversions::u256_to_big_rational;
use std::time::Duration;

/// Filters orders whose full fee amount is below the suplied threshold and are older than a
/// minimum age.
pub async fn filter_orders_with_insufficient_fees(
    orders: &mut Vec<Order>,
    external_prices: &ExternalPrices,
    gas_price_estimator: &dyn GasPriceEstimating,
    max_gas_surcharge_factor: f64,
    min_age: Duration,
) -> Result<()> {
    let min_native_full_fee = gas_price_estimator
        .estimate()
        .await
        .context("failed to estimate gas price for solving")?
        .effective_gas_price()
        / max_gas_surcharge_factor;

    let now = Utc::now();
    let min_creation_time = now
        .checked_sub_signed(chrono::Duration::from_std(min_age)?)
        .with_context(|| {
            format!("overflowed min order surcharge filtering age {now:?}-{min_age:?}")
        })?;

    tracing::debug!(
        %min_native_full_fee,
        ?min_creation_time,
        "filtering orders with insufficient fees"
    );

    orders.retain(|order| {
        let native_full_fee = match order_native_full_fee_amount(order, external_prices) {
            Ok(amount) => amount,
            Err(err) => {
                // Should never happen as this indicates we are dealing with amounts that become
                // out of bound for `f64` or missing prices. Log an error and exclude the order.
                tracing::error!(
                    ?err,
                    ?order,
                    ?external_prices,
                    "error computing full fee amount for order"
                );
                return false;
            }
        };

        // TODO(nlordell): Waiting on new database row.
        //if order.metadata.is_liquidity_order {
        //    // Don't filter liquiidty orders, they already only get included if it is economically
        //    // viable to do so.
        //    return true;
        //}
        if order.metadata.creation_date >= min_creation_time {
            // Order was created recently so it is not subject to filtering.
            return true;
        }

        let is_sufficient_fee = native_full_fee >= min_native_full_fee;
        if !is_sufficient_fee {
            tracing::debug!(
                ?order, %native_full_fee, %min_native_full_fee,
                "filtered order because of insufficient fee",
            );
        }

        is_sufficient_fee
    });

    Ok(())
}

/// Computes an orders full fee amount in native token for the specified order and external prices.
fn order_native_full_fee_amount(order: &Order, external_prices: &ExternalPrices) -> Result<f64> {
    let fee_token = order.creation.sell_token;
    let amount = external_prices
        .try_get_native_amount(fee_token, u256_to_big_rational(&order.creation.fee_amount))
        .with_context(|| format!("missing external price for {fee_token:?}"))?;
    amount
        .to_f64()
        .with_context(|| format!("error converting rational amount {amount:?} to float"))
}

// vk: I would like to extend this to also check that the order has minimum age but for this we need
// access to the creation date which is a more involved change.
pub fn has_at_least_one_user_order(orders: &[LimitOrder]) -> bool {
    orders.iter().any(|order| !order.is_liquidity_order)
}
