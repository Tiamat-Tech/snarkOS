// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![forbid(unsafe_code)]

#[macro_use]
extern crate tracing;

mod helpers;
// Imports custom `Path` type, to be used instead of `axum`'s.
pub use helpers::*;

mod routes;

mod version;

use snarkos_node_cdn::CdnBlockSync;
use snarkos_node_consensus::Consensus;
use snarkos_node_router::{
    Routing,
    messages::{Message, UnconfirmedTransaction},
};
use snarkos_node_sync::BlockSync;
use snarkvm::{
    console::{program::ProgramID, types::Field},
    ledger::narwhal::Data,
    prelude::{Ledger, Network, VM, cfg_into_iter, store::ConsensusStorage},
};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, DefaultBodyLimit, Query, State},
    http::{Method, Request, StatusCode, header::CONTENT_TYPE},
    middleware,
    response::Response,
    routing::{get, post},
};
use axum_extra::response::ErasedJson;
#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
use lru::LruCache;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinHandle};
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::Span;

/// The default port used for the REST API
pub const DEFAULT_REST_PORT: u16 = 3030;

/// The API version prefixes.
pub const API_VERSION_V1: &str = "v1";
pub const API_VERSION_V2: &str = "v2";

/// The capacity of the LRU holding recently requested blocks.
const BLOCK_CACHE_SIZE: usize = 128;

/// A REST API server for the ledger.
#[derive(Clone)]
pub struct Rest<N: Network, C: ConsensusStorage<N>, R: Routing<N>> {
    /// CDN sync (only if node is using the CDN to sync).
    cdn_sync: Option<Arc<CdnBlockSync>>,
    /// The consensus module.
    consensus: Option<Consensus<N>>,
    /// The ledger.
    ledger: Ledger<N, C>,
    /// The node (routing).
    routing: Arc<R>,
    /// The server handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// A reference to BlockSync,
    block_sync: Arc<BlockSync<N>>,
    /// The number of ongoing deploy transaction verifications via REST.
    num_verifying_deploys: Arc<Semaphore>,
    /// The number of ongoing execute transaction verifications via REST.
    num_verifying_executions: Arc<Semaphore>,
    /// The number of ongoing solution verifications via REST.
    num_verifying_solutions: Arc<Semaphore>,
    /// A cache containing recently requested blocks.
    block_cache: Arc<Mutex<LruCache<N::BlockHash, ErasedJson>>>,
}

impl<N: Network, C: 'static + ConsensusStorage<N>, R: Routing<N>> Rest<N, C, R> {
    /// Initializes a new instance of the server.
    pub async fn start(
        rest_ip: SocketAddr,
        rest_rps: u32,
        consensus: Option<Consensus<N>>,
        ledger: Ledger<N, C>,
        routing: Arc<R>,
        cdn_sync: Option<Arc<CdnBlockSync>>,
        block_sync: Arc<BlockSync<N>>,
    ) -> Result<Self> {
        // Initialize the server.
        let mut server = Self {
            consensus,
            ledger,
            routing,
            cdn_sync,
            block_sync,
            handles: Default::default(),
            num_verifying_deploys: Arc::new(Semaphore::new(VM::<N, C>::MAX_PARALLEL_DEPLOY_VERIFICATIONS)),
            num_verifying_executions: Arc::new(Semaphore::new(VM::<N, C>::MAX_PARALLEL_EXECUTE_VERIFICATIONS)),
            num_verifying_solutions: Arc::new(Semaphore::new(N::MAX_SOLUTIONS)),
            block_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(BLOCK_CACHE_SIZE).unwrap()))),
        };
        // Spawn the server.
        server.spawn_server(rest_ip, rest_rps).await?;
        // Return the server.
        Ok(server)
    }
}

impl<N: Network, C: ConsensusStorage<N>, R: Routing<N>> Rest<N, C, R> {
    /// Returns the ledger.
    pub const fn ledger(&self) -> &Ledger<N, C> {
        &self.ledger
    }

    /// Returns the handles.
    pub const fn handles(&self) -> &Arc<Mutex<Vec<JoinHandle<()>>>> {
        &self.handles
    }

    /// Shuts down the REST instance.
    pub fn shut_down(&self) {
        self.handles.lock().iter().for_each(|handle| handle.abort());
    }
}

impl<N: Network, C: ConsensusStorage<N>, R: Routing<N>> Rest<N, C, R> {
    fn build_routes(&self, rest_rps: u32) -> axum::Router {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
            .allow_headers([CONTENT_TYPE]);

        // Prepare the rate limiting setup.
        let governor_config = Box::new(
            GovernorConfigBuilder::default()
                .per_nanosecond((1_000_000_000 / rest_rps) as u64)
                .burst_size(rest_rps)
                .error_handler(|error| {
                    // Properly return a 429 Too Many Requests error
                    let error_message = error.to_string();
                    let mut response = Response::new(error_message.clone().into());
                    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    if error_message.contains("Too Many Requests") {
                        *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
                    }
                    response
                })
                .finish()
                .expect("Couldn't set up rate limiting for the REST server!"),
        );

        // Build the JWT auth-protected endpoints. #[cfg] cannot appear inside a method chain, so we
        // build this router as a named binding and conditionally extend it before applying the layer.
        let auth_routes = axum::Router::new()
            .route("/node/address", get(Self::get_node_address))
            .route("/program/{id}/mapping/{name}", get(Self::get_mapping_values))
            .route("/db_backup", post(Self::db_backup));

        // Slipstream plugin management endpoints require auth.
        #[cfg(feature = "slipstream-plugins")]
        let auth_routes = auth_routes
            .route("/slipstream/plugins", get(Self::slipstream_list_plugins).post(Self::slipstream_load_plugin))
            .route(
                "/slipstream/plugins/{name}",
                // TODO: PUT (reload) is not yet implemented.
                axum::routing::delete(Self::slipstream_unload_plugin),
            );

        let routes = axum::Router::new()
            .merge(auth_routes.route_layer(middleware::from_fn(auth_middleware)))

            // All endpoints declared after here are not protected

             // Get ../consensus_version
            .route("/consensus_version", get(Self::get_consensus_version))

            // GET ../block/..
            .route("/block/height/latest", get(Self::get_block_height_latest))
            .route("/block/hash/latest", get(Self::get_block_hash_latest))
            .route("/block/latest", get(Self::get_block_latest))
            .route("/block/{height_or_hash}", get(Self::get_block))
            // The path param here is actually only the height, but the name must match the route
            // above, otherwise there'll be a conflict at runtime.
            .route("/block/{height_or_hash}/header", get(Self::get_block_header))
            .route("/block/{height_or_hash}/transactions", get(Self::get_block_transactions))

            // GET and POST ../transaction/..
            .route("/transaction/{id}", get(Self::get_transaction))
            .route("/transaction/confirmed/{id}", get(Self::get_confirmed_transaction))
            .route("/transaction/unconfirmed/{id}", get(Self::get_unconfirmed_transaction))
            .route("/transaction/rejected/{id}/reason", get(Self::get_transaction_rejection_reason))
            .route("/transaction/broadcast", post(Self::transaction_broadcast))

            // GET and POST ../solution/..
            .route("/solution/limits/{prover_address}", get(Self::get_solution_limits_for_prover))
            .route("/solution/broadcast", post(Self::solution_broadcast))

            // GET ../find/..
            .route("/find/blockHash/{tx_id}", get(Self::find_block_hash))
            .route("/find/blockHeight/{state_root}", get(Self::find_block_height_from_state_root))
            .route("/find/transactionID/deployment/{program_id}", get(Self::find_latest_transaction_id_from_program_id))
            .route("/find/transactionID/deployment/{program_id}/{edition}", get(Self::find_latest_transaction_id_from_program_id_and_edition))
            .route("/find/transactionID/deployment/{program_id}/{edition}/original", get(Self::find_original_deployment_transaction_id))
            .route("/find/transactionID/deployment/{program_id}/{edition}/{amendment}", get(Self::find_transaction_id_from_program_id_edition_and_amendment))
            .route("/find/transactionID/{transition_id}", get(Self::find_transaction_id_from_transition_id))
            .route("/find/transitionID/{input_or_output_id}", get(Self::find_transition_id))

            // GET ../connections/p2p/.. (with ../peers/.. aliases)
            .route("/peers/count", get(Self::get_peers_count))
            .route("/peers/all", get(Self::get_peers_all))
            .route("/peers/all/metrics", get(Self::get_peers_all_metrics))
            .route("/connections/p2p/count", get(Self::get_peers_count))
            .route("/connections/p2p/all", get(Self::get_peers_all))
            .route("/connections/p2p/all/metrics", get(Self::get_peers_all_metrics))

            // GET ../program/..
            .route("/program/{id}", get(Self::get_program))
            .route("/program/{id}/latest_edition", get(Self::get_latest_program_edition))
            .route("/program/{id}/{edition}", get(Self::get_program_for_edition))
            .route("/program/{id}/mappings", get(Self::get_mapping_names))
            .route("/program/{id}/mapping/{name}/{key}", get(Self::get_mapping_value))
            .route("/program/{id}/amendment_count", get(Self::get_program_amendment_count))
            .route("/program/{id}/{edition}/amendment_count", get(Self::get_program_amendment_count_for_edition))

            // GET ../sync/..
            // Note: keeping ../sync_status for compatibility
            .route("/sync_status", get(Self::get_sync_status))
            .route("/sync/status", get(Self::get_sync_status))
            .route("/sync/peers", get(Self::get_sync_peers))
            .route("/sync/requests", get(Self::get_sync_requests_summary))
            .route("/sync/requests/list", get(Self::get_sync_requests_list))

            // GET misc endpoints.
            .route("/version", get(Self::get_version))
            .route("/blocks", get(Self::get_blocks))
            .route("/blocks/hashes", get(Self::get_block_hashes))
            .route("/blocks/headers", get(Self::get_block_headers))
            .route("/blocks/stateRoots", get(Self::get_block_state_roots))
            .route("/height/{hash}", get(Self::get_height))
            .route("/memoryPool/transmissions", get(Self::get_memory_pool_transmissions))
            .route("/memoryPool/solutions", get(Self::get_memory_pool_solutions))
            .route("/memoryPool/transactions", get(Self::get_memory_pool_transactions))
            .route("/statePath/{commitment}", get(Self::get_state_path_for_commitment))
            .route("/statePaths", get(Self::get_state_paths_for_commitments))
            .route("/stateRoot/latest", get(Self::get_state_root_latest))
            .route("/stateRoot/{height}", get(Self::get_state_root))
            .route("/committee/latest", get(Self::get_committee_latest))
            .route("/committee/{height}", get(Self::get_committee))
            .route("/delegators/{validator}", get(Self::get_delegators_for_validator));

        // If the node is a validator, enable the BFT connections endpoints.
        let routes = match self.consensus {
            Some(_) => routes
                .route("/connections/bft/count", get(Self::get_bft_connections_count))
                .route("/connections/bft/all", get(Self::get_bft_connections_all)),
            None => routes,
        };

        // If the node is a validator and `telemetry` features is enabled, enable the additional endpoint.
        #[cfg(feature = "metrics")]
        let routes = match self.consensus {
            Some(_) => routes.route("/validators/participation", get(Self::get_validator_participation_scores)),
            None => routes,
        };

        // Register the view-at-latest-height endpoint (always available, no history required).
        let routes = routes.route("/program/{id}/view/{function}", post(Self::evaluate_view_latest));

        // If the `history` feature is enabled, enable the additional endpoints.
        #[cfg(feature = "history")]
        let routes = routes
            .route("/program/{id}/mapping/{name}/{key}/history/{height}", get(Self::get_history))
            .route("/program/{id}/mapping/{name}/history/{height}", get(Self::get_history_batch))
            .route("/program/{id}/view/{function}/{height}", post(Self::evaluate_view));

        // If the `history-staking-rewards` feature is enabled, enable the additional endpoint.
        #[cfg(feature = "history-staking-rewards")]
        let routes = routes.route("/staking/rewards/{address}/{height}", get(Self::get_staking_reward));

        let trace_layer = TraceLayer::new_for_http()
            .make_span_with(|request: &Request<_>| {
                let addr = request
                    .extensions()
                    .get::<ConnectInfo<SocketAddr>>()
                    .map(|ConnectInfo(addr)| addr.to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                // Create a span that includes method, path, and our extracted IP
                tracing::info_span!(
                    "REST",
                    method = %request.method(),
                    uri = %request.uri().path(),
                    addr = %addr,
                )
            })
            .on_request(|_request: &Request<_>, _span: &Span| {
                info!("Received a request");
            })
            .on_response(|_response: &Response<_>, latency: Duration, _span: &Span| {
                info!("Finished request in {:?}", latency);
            });

        routes
            // Pass in `Rest` to make things convenient.
            .with_state(self.clone())
            // JSON encodings of transactions can exceed the binary size, so this is 2x
            // `LATEST_MAX_TRANSACTION_SIZE`.
            .layer(DefaultBodyLimit::max(2 * N::LATEST_MAX_TRANSACTION_SIZE()))
            .layer(GovernorLayer {
                config: governor_config.into(),
            })
            // Enable CORS.
            .layer(cors)
            // Enable tower-http tracing.
            .layer(trace_layer)
    }

    async fn spawn_server(&mut self, rest_ip: SocketAddr, rest_rps: u32) -> Result<()> {
        // Log the REST rate limit per IP.
        debug!("REST rate limit per IP - {rest_rps} RPS");

        // Add the v1 API as default and under "/v1".
        let default_router = axum::Router::new().nest(
            &format!("/{}", N::SHORT_NAME),
            self.build_routes(rest_rps).layer(middleware::map_response(v1_error_middleware)),
        );
        let v1_router = axum::Router::new().nest(
            &format!("/{API_VERSION_V1}/{}", N::SHORT_NAME),
            self.build_routes(rest_rps).layer(middleware::map_response(v1_error_middleware)),
        );

        // Add the v2 API under "/v2".
        let v2_router =
            axum::Router::new().nest(&format!("/{API_VERSION_V2}/{}", N::SHORT_NAME), self.build_routes(rest_rps));

        // Combine all routes.
        let router = default_router.merge(v1_router).merge(v2_router);

        let rest_listener =
            TcpListener::bind(rest_ip).await.with_context(|| "Failed to bind TCP port for REST endpoints")?;

        let handle = tokio::spawn(async move {
            axum::serve(rest_listener, router.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("couldn't start rest server");
        });

        self.handles.lock().push(handle);
        Ok(())
    }
}

/// Converts errors to the old style for the v1 API.
/// The error code will always be 500 and the content a simple string.
async fn v1_error_middleware(response: Response) -> Response {
    // The status code used by all v1 errors
    const V1_STATUS_CODE: StatusCode = StatusCode::INTERNAL_SERVER_ERROR;

    if response.status().is_success() {
        return response;
    }

    // Returns a opaque error instead of panicking.
    let fallback = || {
        let mut response = Response::new(Body::from("Failed to convert error"));
        *response.status_mut() = V1_STATUS_CODE;
        response
    };

    let Ok(bytes) = axum::body::to_bytes(response.into_body(), usize::MAX).await else {
        return fallback();
    };

    // Deserialize REST error so we can convert it to a string
    let Ok(json_err) = serde_json::from_slice::<SerializedRestError>(&bytes) else {
        return fallback();
    };

    let mut message = json_err.message;
    for next in json_err.chain.into_iter() {
        message = format!("{message} — {next}");
    }

    let mut response = Response::new(Body::from(message));

    *response.status_mut() = V1_STATUS_CODE;

    response
}

/// Formats an ID into a truncated identifier (for logging purposes).
pub fn fmt_id(id: impl ToString) -> String {
    let id = id.to_string();
    let mut formatted_id = id.chars().take(16).collect::<String>();
    if id.chars().count() > 16 {
        formatted_id.push_str("..");
    }
    formatted_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        middleware,
        routing::get,
    };
    use tower::ServiceExt; // for `oneshot`

    fn test_app() -> Router {
        let build_routes = || {
            Router::new()
                .route("/not_found", get(|| async { Err::<(), RestError>(RestError::not_found(anyhow!("missing"))) }))
                .route("/bad_request", get(|| async { Err::<(), RestError>(RestError::bad_request(anyhow!("bad"))) }))
                .route(
                    "/service_unavailable",
                    get(|| async { Err::<(), RestError>(RestError::service_unavailable(anyhow!("gone"))) }),
                )
        };
        let router_v1 = build_routes().route_layer(middleware::map_response(v1_error_middleware));
        let router_v2 = Router::new().nest(&format!("/{API_VERSION_V2}"), build_routes());
        router_v1.merge(router_v2)
    }

    #[tokio::test]
    async fn v1_routes_force_internal_server_error() {
        let app = test_app();

        let res = app.clone().oneshot(Request::builder().uri("/not_found").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let res =
            app.clone().oneshot(Request::builder().uri("/bad_request").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let res =
            app.oneshot(Request::builder().uri("/service_unavailable").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn v2_routes_return_specific_errors() {
        let app = test_app();

        let res =
            app.clone().oneshot(Request::builder().uri("/v2/not_found").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res =
            app.clone().oneshot(Request::builder().uri("/v2/bad_request").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        let res =
            app.oneshot(Request::builder().uri("/v2/service_unavailable").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[cfg(test)]
mod route_tests {
    use super::*;
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkos_node_network::ConnectionMode;
    use snarkos_node_router::test_helpers::{TestRouter, client, sample_genesis_block};
    use snarkvm::{
        ledger::{block::Header, committee::test_helpers::sample_committee, store::helpers::memory::ConsensusMemory},
        prelude::MainnetV0,
        utilities::TestRng,
    };

    use aleo_std::StorageMode;
    use axum::body::to_bytes;
    use tower::ServiceExt; // for `oneshot`

    type CurrentNetwork = MainnetV0;
    type CurrentRest = Rest<CurrentNetwork, ConsensusMemory<CurrentNetwork>, TestRouter<CurrentNetwork>>;

    /// The rate limit given to the router under test. The governor layer is applied by
    /// `build_routes`, so this is set high enough that a test making several requests in quick
    /// succession is never the thing that trips it.
    const TEST_RPS: u32 = 1_000;

    /// Builds a `Rest` over an in-memory ledger containing only the genesis block.
    ///
    /// This constructs the struct directly rather than calling `Rest::start`, which would bind a
    /// port and spawn a server. None of the routes exercised here touch `consensus`, `cdn_sync`,
    /// `routing` or `block_sync`; those fields exist only to satisfy the type.
    async fn sample_rest() -> CurrentRest {
        let rng = &mut TestRng::default();

        // `Ledger::load` reaches snarkVM's sequential-operation thread and blocks on the reply,
        // which panics if called from an async context. Production always drives these from a
        // blocking task, so do the same here.
        let ledger = tokio::task::spawn_blocking(|| {
            Ledger::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::load(
                sample_genesis_block::<CurrentNetwork>(),
                StorageMode::new_test(None),
            )
        })
        .await
        .expect("the ledger task panicked")
        .expect("couldn't load the test ledger");

        let ledger_service = Arc::new(MockLedgerService::new(sample_committee(rng)));

        Rest {
            cdn_sync: None,
            consensus: None,
            ledger,
            routing: Arc::new(client(0, 10, rng).await),
            handles: Default::default(),
            block_sync: Arc::new(BlockSync::new(ledger_service, ConnectionMode::Router)),
            num_verifying_deploys: Arc::new(Semaphore::new(1)),
            num_verifying_executions: Arc::new(Semaphore::new(1)),
            num_verifying_solutions: Arc::new(Semaphore::new(1)),
            block_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(BLOCK_CACHE_SIZE).unwrap()))),
        }
    }

    /// Issues a GET request against the routes, without the network prefix that `spawn_server`
    /// nests them under.
    ///
    /// The governor layer keys on the peer IP taken from `ConnectInfo`, which a request built by
    /// hand does not carry, so this attaches one; without it every request fails the rate limiter's
    /// key extractor rather than reaching a handler.
    async fn get(rest: &CurrentRest, uri: &str) -> (StatusCode, String) {
        let mut request = Request::builder().uri(uri).body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 4130))));

        let response = rest.build_routes(TEST_RPS).oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn block_hashes_returns_the_hashes_in_the_range() {
        let rest = sample_rest().await;

        // The test ledger holds only the genesis block, so this is the one height available.
        let (status, body) = get(&rest, "/blocks/hashes?start=0&end=1").await;
        assert_eq!(status, StatusCode::OK);

        let hashes: Vec<<CurrentNetwork as Network>::BlockHash> = serde_json::from_str(&body).unwrap();
        assert_eq!(hashes, vec![sample_genesis_block::<CurrentNetwork>().hash()]);
    }

    #[tokio::test]
    async fn block_headers_returns_the_headers_in_the_range() {
        let rest = sample_rest().await;

        let (status, body) = get(&rest, "/blocks/headers?start=0&end=1").await;
        assert_eq!(status, StatusCode::OK);

        let headers: Vec<Header<CurrentNetwork>> = serde_json::from_str(&body).unwrap();
        assert_eq!(headers, vec![*sample_genesis_block::<CurrentNetwork>().header()]);
    }

    #[tokio::test]
    async fn block_state_roots_returns_the_state_roots_in_the_range() {
        let rest = sample_rest().await;

        let (status, body) = get(&rest, "/blocks/stateRoots?start=0&end=1").await;
        assert_eq!(status, StatusCode::OK);

        // The state root at a height is the root *after* that block, so it is not the genesis
        // header's `previous_state_root`. Compare against what the ledger reports for the height.
        let state_roots: Vec<<CurrentNetwork as Network>::StateRoot> = serde_json::from_str(&body).unwrap();
        assert_eq!(state_roots, vec![rest.ledger.get_state_root(0).unwrap().unwrap()]);
    }

    #[tokio::test]
    async fn an_empty_range_returns_an_empty_array() {
        let rest = sample_rest().await;

        for route in ["hashes", "headers", "stateRoots"] {
            let (status, body) = get(&rest, &format!("/blocks/{route}?start=0&end=0")).await;
            assert_eq!(status, StatusCode::OK, "{route} rejected an empty range");
            assert_eq!(serde_json::from_str::<Vec<serde_json::Value>>(&body).unwrap(), Vec::<serde_json::Value>::new());
        }
    }

    #[tokio::test]
    async fn a_range_past_the_tip_is_not_found() {
        let rest = sample_rest().await;

        // The test ledger holds only the genesis block, so height 1 does not exist. The whole
        // request fails rather than returning a short array, matching `/blocks`.
        for route in ["hashes", "headers", "stateRoots"] {
            let (status, _) = get(&rest, &format!("/blocks/{route}?start=0&end=2")).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{route} did not report a missing height");
        }
    }

    #[tokio::test]
    async fn an_inverted_range_is_rejected() {
        let rest = sample_rest().await;

        for route in ["hashes", "headers", "stateRoots"] {
            let (status, _) = get(&rest, &format!("/blocks/{route}?start=10&end=0")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{route} accepted an inverted range");
        }
    }

    #[tokio::test]
    async fn a_range_over_the_maximum_is_rejected() {
        let rest = sample_rest().await;

        // One past each route's maximum. These are rejected before any lookup, so the fact that
        // the test ledger has a single block does not matter.
        for (route, over_max) in [("hashes", 5_001), ("headers", 321), ("stateRoots", 5_001)] {
            let (status, body) = get(&rest, &format!("/blocks/{route}?start=0&end={over_max}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{route} accepted a range over its maximum");
            assert!(body.contains("Cannot request more than"), "{route} gave an unexpected error: {body}");
        }
    }

    #[tokio::test]
    async fn a_range_at_the_maximum_is_accepted() {
        let rest = sample_rest().await;

        // Exactly each route's maximum passes the range check. The lookups then fail on the test
        // ledger's single block, so a 404 here still proves the maximum itself was not the reason.
        for (route, max) in [("hashes", 5_000), ("headers", 320), ("stateRoots", 5_000)] {
            let (status, body) = get(&rest, &format!("/blocks/{route}?start=0&end={max}")).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{route} rejected a range at its maximum");
            assert!(!body.contains("Cannot request more than"), "{route} rejected its own maximum: {body}");
        }
    }

    #[tokio::test]
    async fn missing_or_invalid_range_parameters_are_rejected() {
        let rest = sample_rest().await;

        for query in ["", "?start=0", "?end=1", "?start=abc&end=1", "?start=0&end=-1"] {
            let (status, _) = get(&rest, &format!("/blocks/hashes{query}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "the query '{query}' was not rejected");
        }
    }
}
