pub mod buffers;
pub mod settlement;

use self::settlement::SettlementContext;
use crate::{
    interactions::allowances::AllowanceManaging,
    liquidity::{Exchange, LimitOrder, Liquidity},
    settlement::{external_prices::ExternalPrices, Settlement},
    solver::{Auction, Solver},
};
use anyhow::{anyhow, Context, Result};
use buffers::{BufferRetrievalError, BufferRetrieving};
use ethcontract::{errors::ExecutionError, Account, U256};
use futures::{join, lock::Mutex};
use maplit::{btreemap, hashset};
use model::order::OrderKind;
use num::{BigInt, BigRational};
use primitive_types::H160;
use shared::http_solver::{DefaultHttpSolverApi, HttpSolverApi};
use shared::{
    http_solver::{gas_model::GasModel, model::*},
    sources::balancer_v2::pools::common::compute_scaling_rate,
};
use shared::{
    measure_time,
    token_info::{TokenInfo, TokenInfoFetching},
};
use std::time::Instant;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter::FromIterator as _,
    sync::Arc,
};

/// Failure indicating the transaction reverted for some reason
pub fn is_transaction_failure(error: &ExecutionError) -> bool {
    matches!(error, ExecutionError::Failure(_))
        || matches!(error, ExecutionError::Revert(_))
        || matches!(error, ExecutionError::InvalidOpcode)
}

// TODO: special rounding for the prices we get from the solver?

/// Data shared between multiple instances of the http solver for the same solve id.
pub struct InstanceData {
    solve_id: u64,
    model: BatchAuctionModel,
    context: SettlementContext,
}

/// We keep a cache of per solve instance data because it is the same for all http solver
/// invocations. Without the cache we would duplicate most of the requests to the node.
pub type InstanceCache = Arc<Mutex<Option<InstanceData>>>;

pub struct HttpSolver {
    solver: DefaultHttpSolverApi,
    account: Account,
    native_token: H160,
    token_info_fetcher: Arc<dyn TokenInfoFetching>,
    buffer_retriever: Arc<dyn BufferRetrieving>,
    allowance_manager: Arc<dyn AllowanceManaging>,
    instance_cache: InstanceCache,
}

impl HttpSolver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        solver: DefaultHttpSolverApi,
        account: Account,
        native_token: H160,
        token_info_fetcher: Arc<dyn TokenInfoFetching>,
        buffer_retriever: Arc<dyn BufferRetrieving>,
        allowance_manager: Arc<dyn AllowanceManaging>,
        instance_cache: InstanceCache,
    ) -> Self {
        Self {
            solver,
            account,
            native_token,
            token_info_fetcher,
            buffer_retriever,
            allowance_manager,
            instance_cache,
        }
    }

    async fn prepare_model(
        &self,
        auction_id: u64,
        orders: Vec<LimitOrder>,
        liquidity: Vec<Liquidity>,
        gas_price: f64,
        external_prices: ExternalPrices,
    ) -> Result<(BatchAuctionModel, SettlementContext)> {
        let tokens = map_tokens_for_solver(&orders, &liquidity);
        let (token_infos, buffers_result) = join!(
            measure_time(
                self.token_info_fetcher.get_token_infos(tokens.as_slice()),
                |duration| tracing::debug!("get_token_infos took {} s", duration.as_secs_f32()),
            ),
            measure_time(
                self.buffer_retriever.get_buffers(tokens.as_slice()),
                |duration| tracing::debug!("get_buffers took {} s", duration.as_secs_f32()),
            ),
        );

        let buffers: HashMap<_, _> = buffers_result
            .into_iter()
            .filter_map(|(token, buffer)| match buffer {
                Err(BufferRetrievalError::Erc20(err)) if is_transaction_failure(&err.inner) => {
                    tracing::debug!(
                        "Failed to fetch buffers for token {} with transaction failure {}",
                        token,
                        err
                    );
                    None
                }
                Err(err) => {
                    tracing::error!(
                        "Failed to fetch buffers contract balance for token {} with error {:?}",
                        token,
                        err
                    );
                    None
                }
                Ok(b) => Some((token, b)),
            })
            .collect();

        // We are guaranteed to have price estimates for all tokens that are relevant to the
        // objective value by the driver. It is possible that we have AMM pools that contain tokens
        // that are not any order's tokens. We used to fetch these extra prices but it would often
        // slow down the solver and the solver can estimate them on its own.
        let price_estimates = external_prices.into_http_solver_prices();

        // For the solver to run correctly we need to be sure that there are no
        // isolated islands of tokens without connection between them.
        let fee_connected_tokens = compute_fee_connected_tokens(&liquidity, self.native_token);
        let gas_model = GasModel {
            native_token: self.native_token,
            gas_price,
        };

        let token_models = token_models(&token_infos, &price_estimates, &buffers, &gas_model);
        let order_models = order_models(&orders, &fee_connected_tokens, &gas_model);
        let amm_models = amm_models(&liquidity, &gas_model);
        let model = BatchAuctionModel {
            tokens: token_models,
            orders: order_models,
            amms: amm_models,
            metadata: Some(MetadataModel {
                environment: Some(self.solver.network_name.clone()),
                auction_id: Some(auction_id),
                gas_price: Some(gas_price),
                native_token: Some(self.native_token),
            }),
        };
        Ok((model, SettlementContext { orders, liquidity }))
    }
}

fn map_tokens_for_solver(orders: &[LimitOrder], liquidity: &[Liquidity]) -> Vec<H160> {
    let mut token_set = HashSet::new();
    token_set.extend(
        orders
            .iter()
            .flat_map(|order| [order.sell_token, order.buy_token]),
    );
    for liquidity in liquidity.iter() {
        match liquidity {
            Liquidity::ConstantProduct(amm) => token_set.extend(amm.tokens),
            Liquidity::BalancerWeighted(amm) => token_set.extend(amm.reserves.keys()),
            Liquidity::BalancerStable(amm) => token_set.extend(amm.reserves.keys()),
            Liquidity::LimitOrder(order) => token_set.extend([order.sell_token, order.buy_token]),
        }
    }

    Vec::from_iter(token_set)
}

fn order_fee(order: &LimitOrder) -> FeeModel {
    let amount = match order.is_liquidity_order {
        true => order.unscaled_subsidized_fee,
        false => order.scaled_unsubsidized_fee,
    };
    FeeModel {
        amount,
        token: order.sell_token,
    }
}

fn token_models(
    token_infos: &HashMap<H160, TokenInfo>,
    price_estimates: &HashMap<H160, f64>,
    buffers: &HashMap<H160, U256>,
    gas_model: &GasModel,
) -> BTreeMap<H160, TokenInfoModel> {
    token_infos
        .iter()
        .map(|(address, token_info)| {
            let external_price = match price_estimates.get(address).copied() {
                Some(price) if price.is_finite() => Some(price),
                _ => None,
            };
            (
                *address,
                TokenInfoModel {
                    decimals: token_info.decimals,
                    alias: token_info.symbol.clone(),
                    external_price,
                    normalize_priority: Some(if &gas_model.native_token == address {
                        1
                    } else {
                        0
                    }),
                    internal_buffer: buffers.get(address).copied(),
                },
            )
        })
        .collect()
}

fn order_models(
    orders: &[LimitOrder],
    fee_connected_tokens: &HashSet<H160>,
    gas_model: &GasModel,
) -> BTreeMap<usize, OrderModel> {
    orders
        .iter()
        .enumerate()
        .filter_map(|(index, order)| {
            if ![order.sell_token, order.buy_token]
                .iter()
                .any(|token| fee_connected_tokens.contains(token))
            {
                return None;
            }

            let cost = match order.exchange {
                Exchange::GnosisProtocol => gas_model.gp_order_cost(),
                Exchange::ZeroEx => gas_model.zeroex_order_cost(),
            };

            Some((
                index,
                OrderModel {
                    sell_token: order.sell_token,
                    buy_token: order.buy_token,
                    sell_amount: order.sell_amount,
                    buy_amount: order.buy_amount,
                    allow_partial_fill: order.partially_fillable,
                    is_sell_order: matches!(order.kind, OrderKind::Sell),
                    fee: order_fee(order),
                    cost,
                    is_liquidity_order: order.is_liquidity_order,
                    mandatory: false,
                    has_atomic_execution: !matches!(order.exchange, Exchange::GnosisProtocol),
                },
            ))
        })
        .collect()
}

fn amm_models(liquidity: &[Liquidity], gas_model: &GasModel) -> BTreeMap<usize, AmmModel> {
    liquidity
        .iter()
        .filter(|liquidity| !matches!(liquidity, Liquidity::LimitOrder(_)))
        .map(|liquidity| -> Result<_> {
            Ok(match liquidity {
                Liquidity::ConstantProduct(amm) => AmmModel {
                    parameters: AmmParameters::ConstantProduct(ConstantProductPoolParameters {
                        reserves: btreemap! {
                            amm.tokens.get().0 => amm.reserves.0.into(),
                            amm.tokens.get().1 => amm.reserves.1.into(),
                        },
                    }),
                    fee: BigRational::new(
                        BigInt::from(*amm.fee.numer()),
                        BigInt::from(*amm.fee.denom()),
                    ),
                    cost: gas_model.uniswap_cost(),
                    mandatory: false,
                },
                Liquidity::BalancerWeighted(amm) => AmmModel {
                    parameters: AmmParameters::WeightedProduct(WeightedProductPoolParameters {
                        reserves: amm
                            .reserves
                            .iter()
                            .map(|(token, state)| {
                                (
                                    *token,
                                    WeightedPoolTokenData {
                                        balance: state.common.balance,
                                        weight: BigRational::from(state.weight),
                                    },
                                )
                            })
                            .collect(),
                    }),
                    fee: amm.fee.into(),
                    cost: gas_model.balancer_cost(),
                    mandatory: false,
                },
                Liquidity::BalancerStable(amm) => AmmModel {
                    parameters: AmmParameters::Stable(StablePoolParameters {
                        reserves: amm
                            .reserves
                            .iter()
                            .map(|(token, state)| (*token, state.balance))
                            .collect(),
                        scaling_rates: amm
                            .reserves
                            .iter()
                            .map(|(token, state)| {
                                Ok((*token, compute_scaling_rate(state.scaling_exponent)?))
                            })
                            .collect::<Result<_>>()
                            .with_context(|| {
                                format!("error converting stable pool to solver model: {:?}", amm)
                            })?,
                        amplification_parameter: amm.amplification_parameter.as_big_rational(),
                    }),
                    fee: amm.fee.clone(),
                    cost: gas_model.balancer_cost(),
                    mandatory: false,
                },
                Liquidity::LimitOrder(_) => unreachable!("filtered out before"),
            })
        })
        .enumerate()
        .filter_map(|(index, result)| match result {
            Ok(value) => Some((index, value)),
            Err(err) => {
                tracing::error!(?err, "error converting liquidity to solver model");
                None
            }
        })
        .collect()
}

fn compute_fee_connected_tokens(liquidity: &[Liquidity], native_token: H160) -> HashSet<H160> {
    // Find all tokens that are connected through potentially multiple amm hops to the fee.
    // TODO: Replace with a more optimal graph algorithm.
    let mut pairs = liquidity
        .iter()
        .flat_map(|amm| amm.all_token_pairs())
        .collect::<HashSet<_>>();
    let mut fee_connected_tokens = hashset![native_token];
    loop {
        let mut added_token = false;
        pairs.retain(|token_pair| {
            let tokens = token_pair.get();
            if fee_connected_tokens.contains(&tokens.0) {
                fee_connected_tokens.insert(tokens.1);
                added_token = true;
                false
            } else if fee_connected_tokens.contains(&tokens.1) {
                fee_connected_tokens.insert(tokens.0);
                added_token = true;
                false
            } else {
                true
            }
        });
        if pairs.is_empty() || !added_token {
            break;
        }
    }

    fee_connected_tokens
}

#[async_trait::async_trait]
impl Solver for HttpSolver {
    async fn solve(
        &self,
        Auction {
            id,
            mut orders,
            liquidity,
            gas_price,
            deadline,
            external_prices,
        }: Auction,
    ) -> Result<Vec<Settlement>> {
        if orders.is_empty() {
            return Ok(Vec::new());
        };
        orders.extend(liquidity.iter().filter_map(|liquidity| match liquidity {
            Liquidity::LimitOrder(order) => Some(order.clone()),
            _ => None,
        }));

        let (model, context) = {
            let mut guard = self.instance_cache.lock().await;
            match guard.as_mut() {
                Some(data) if data.solve_id == id => (data.model.clone(), data.context.clone()),
                _ => {
                    let (model, context) = self
                        .prepare_model(id, orders, liquidity, gas_price, external_prices)
                        .await?;
                    *guard = Some(InstanceData {
                        solve_id: id,
                        model: model.clone(),
                        context: context.clone(),
                    });
                    (model, context)
                }
            }
        };
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("no time left to send request"))?;
        let settled = self.solver.solve(&model, timeout).await?;
        tracing::trace!(?settled);
        if !settled.has_execution_plan() {
            return Ok(Vec::new());
        }
        settlement::convert_settlement(settled, context, self.allowance_manager.clone())
            .await
            .map(|settlement| vec![settlement])
    }

    fn account(&self) -> &Account {
        &self.account
    }

    fn name(&self) -> &str {
        &self.solver.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interactions::allowances::MockAllowanceManaging;
    use crate::liquidity::{tests::CapturingSettlementHandler, ConstantProductOrder, LimitOrder};
    use crate::solver::http_solver::buffers::MockBufferRetrieving;
    use ::model::TokenPair;
    use ethcontract::Address;
    use maplit::hashmap;
    use num::rational::Ratio;
    use reqwest::Client;
    use shared::http_solver::SolverConfig;
    use shared::token_info::MockTokenInfoFetching;
    use shared::token_info::TokenInfo;
    use std::sync::Arc;
    use std::time::Duration;

    // cargo test real_solver -- --ignored --nocapture
    // set the env variable GP_V2_OPTIMIZER_URL to use a non localhost optimizer
    #[tokio::test]
    #[ignore]
    async fn real_solver() {
        tracing_subscriber::fmt::fmt()
            .with_env_filter("solver=trace")
            .init();
        let url = std::env::var("GP_V2_OPTIMIZER_URL")
            .unwrap_or_else(|_| "http://localhost:8000".to_string());

        let buy_token = H160::from_low_u64_be(1337);
        let sell_token = H160::from_low_u64_be(43110);

        let mut mock_token_info_fetcher = MockTokenInfoFetching::new();
        mock_token_info_fetcher
            .expect_get_token_infos()
            .return_once(move |_| {
                hashmap! {
                    buy_token => TokenInfo { decimals: Some(18), symbol: Some("CAT".to_string()) },
                    sell_token => TokenInfo { decimals: Some(18), symbol: Some("CAT".to_string()) },
                }
            });

        let mut mock_buffer_retriever = MockBufferRetrieving::new();
        mock_buffer_retriever
            .expect_get_buffers()
            .return_once(move |_| {
                hashmap! {
                    buy_token => Ok(U256::from(42)),
                    sell_token => Ok(U256::from(1337)),
                }
            });

        let gas_price = 100.;

        let solver = HttpSolver::new(
            DefaultHttpSolverApi {
                name: "Test Solver".to_string(),
                network_name: "mock_network_id".to_string(),
                chain_id: 0,
                base: url.parse().unwrap(),
                client: Client::new(),
                config: SolverConfig {
                    api_key: None,
                    max_nr_exec_orders: 0,
                    has_ucp_policy_parameter: false,
                    use_internal_buffers: None,
                },
            },
            Account::Local(Address::default(), None),
            H160::zero(),
            Arc::new(mock_token_info_fetcher),
            Arc::new(mock_buffer_retriever),
            Arc::new(MockAllowanceManaging::new()),
            Default::default(),
        );
        let base = |x: u128| x * 10u128.pow(18);
        let limit_orders = vec![LimitOrder {
            buy_token,
            sell_token,
            buy_amount: base(1).into(),
            sell_amount: base(2).into(),
            kind: OrderKind::Sell,
            id: "0".to_string(),
            ..Default::default()
        }];
        let liquidity = vec![Liquidity::ConstantProduct(ConstantProductOrder {
            tokens: TokenPair::new(buy_token, sell_token).unwrap(),
            reserves: (base(100), base(100)),
            fee: Ratio::new(0, 1),
            settlement_handling: CapturingSettlementHandler::arc(),
        })];
        let (model, _context) = solver
            .prepare_model(0u64, limit_orders, liquidity, gas_price, Default::default())
            .await
            .unwrap();
        let settled = solver
            .solver
            .solve(&model, Duration::from_secs(1000))
            .await
            .unwrap();
        dbg!(&settled);

        let exec_order = settled.orders.values().next().unwrap();
        assert_eq!(exec_order.exec_sell_amount.as_u128(), base(2));
        assert!(exec_order.exec_buy_amount.as_u128() > 0);

        let uniswap = settled.amms.values().next().unwrap();
        let execution = &uniswap.execution[0];
        assert!(execution.exec_buy_amount.gt(&U256::zero()));
        assert_eq!(execution.exec_sell_amount, U256::from(base(2)));
        assert!(execution.exec_plan.is_some());
        assert_eq!(execution.exec_plan.as_ref().unwrap().sequence, 0);
        assert_eq!(execution.exec_plan.as_ref().unwrap().position, 0);

        assert_eq!(settled.prices.len(), 2);
    }

    #[test]
    fn remove_orders_without_native_connection_() {
        let limit_handling = CapturingSettlementHandler::arc();
        let amm_handling = CapturingSettlementHandler::arc();

        let native_token = H160::from_low_u64_be(0);
        let tokens = [
            H160::from_low_u64_be(1),
            H160::from_low_u64_be(2),
            H160::from_low_u64_be(3),
            H160::from_low_u64_be(4),
        ];

        let gas_model = GasModel {
            gas_price: 1e9,
            native_token,
        };

        let amms = [(native_token, tokens[0]), (tokens[0], tokens[1])]
            .iter()
            .map(|tokens| {
                Liquidity::ConstantProduct(ConstantProductOrder {
                    tokens: TokenPair::new(tokens.0, tokens.1).unwrap(),
                    reserves: (0, 0),
                    fee: 0.into(),
                    settlement_handling: amm_handling.clone(),
                })
            })
            .collect::<Vec<_>>();

        let orders = [
            (native_token, tokens[0]),
            (native_token, tokens[1]),
            (tokens[0], tokens[1]),
            (tokens[1], tokens[0]),
            (tokens[1], tokens[2]),
            (tokens[2], tokens[1]),
            (tokens[2], tokens[3]),
            (tokens[3], tokens[2]),
        ]
        .iter()
        .map(|tokens| LimitOrder {
            sell_token: tokens.0,
            buy_token: tokens.1,
            kind: OrderKind::Sell,
            settlement_handling: limit_handling.clone(),
            ..Default::default()
        })
        .collect::<Vec<_>>();

        let fee_connected_tokens = compute_fee_connected_tokens(&amms, native_token);
        assert_eq!(
            fee_connected_tokens,
            hashset![native_token, tokens[0], tokens[1]],
        );

        let order_models = order_models(&orders, &fee_connected_tokens, &gas_model);
        assert_eq!(order_models.len(), 6);
    }

    #[test]
    fn decode_response() {
        let example_response = r#"
            {
              "extra_crap": ["Hello"],
              "orders": {
                "0": {
                  "sell_token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                  "buy_token": "0xba100000625a3754423978a60c9317c58a424e3d",
                  "sell_amount": "195160000000000000",
                  "buy_amount": "18529625032931383084",
                  "allow_partial_fill": false,
                  "is_sell_order": true,
                  "fee": {
                    "amount": "4840000000000000",
                    "token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                  },
                  "cost": {
                    "amount": "1604823000000000",
                    "token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                  },
                  "exec_buy_amount": "18689825362370811941",
                  "exec_sell_amount": "195160000000000000"
                },
                "1": {
                  "sell_token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                  "buy_token": "0xba100000625a3754423978a60c9317c58a424e3d",
                  "sell_amount": "395160000000000000",
                  "buy_amount": "37314737669229514851",
                  "allow_partial_fill": false,
                  "is_sell_order": true,
                  "fee": {
                    "amount": "4840000000000000",
                    "token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                  },
                  "cost": {
                    "amount": "1604823000000000",
                    "token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                  },
                  "exec_buy_amount": "37843161458262200293",
                  "exec_sell_amount": "395160000000000000"
                }
              },
              "ref_token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "prices": {
                "0xba100000625a3754423978a60c9317c58a424e3d": "10442045135045813",
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "1000000000000000000"
              },
              "amms": {
                "9": {
                  "kind": "WeightedProduct",
                  "reserves": {
                    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {
                      "balance": "99572200495363891220",
                      "weight": "0.5"
                    },
                    "0xba100000625a3754423978a60c9317c58a424e3d": {
                      "balance": "9605600791222732320384",
                      "weight": "0.5"
                    }
                  },
                  "fee": "0.0014",
                  "cost": {
                    "amount": "2904000000000000",
                    "token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                  },
                  "execution": [
                    {
                      "sell_token": "0xba100000625a3754423978a60c9317c58a424e3d",
                      "buy_token": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                      "exec_sell_amount": "56532986820633012234",
                      "exec_buy_amount": "590320000000000032",
                      "exec_plan": {
                        "sequence": 0,
                        "position": 0
                      }
                    }
                  ]
                }
              }
            }
        "#;
        let parsed_response = serde_json::from_str::<SettledBatchAuctionModel>(example_response);
        assert!(parsed_response.is_ok());
    }
}
