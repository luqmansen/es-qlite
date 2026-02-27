use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{any, get, post, put};
use axum::{Json, Router};
use std::sync::Arc;
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::trace::TraceLayer;

use crate::routes::{bulk, cluster, document, index, multi, search};
use crate::storage::registry::IndexRegistry;

pub fn build_router(registry: Arc<IndexRegistry>) -> Router {
    Router::new()
        // Root info endpoint
        .route("/", get(cluster::root_info))
        // Cluster endpoints
        .route("/_cluster/health", get(cluster::cluster_health))
        .route(
            "/_cluster/settings",
            get(cluster::cluster_settings).put(cluster::put_cluster_settings),
        )
        .route("/_cat/indices", get(cluster::cat_indices))
        // Bulk endpoint
        .route("/_bulk", post(bulk::bulk))
        // Multi-get (global)
        .route("/_mget", post(multi::mget).get(multi::mget))
        // Index templates (stubs)
        .route("/_index_template/{name}", any(cluster::stub_ok))
        .route("/_template/{name}", any(cluster::stub_ok))
        .route("/_component_template/{name}", any(cluster::stub_ok))
        // Aliases
        .route(
            "/_aliases",
            get(cluster::get_all_aliases).post(cluster::post_aliases),
        )
        .route("/_alias/{name}", get(cluster::get_alias))
        // Index management
        .route(
            "/{index}",
            put(index::create_index)
                .get(index::get_index)
                .delete(index::delete_index)
                .head(index::index_exists),
        )
        .route(
            "/{index}/_mapping",
            get(index::get_mapping).put(index::put_mapping),
        )
        .route(
            "/{index}/_settings",
            get(index::get_settings).put(index::put_settings),
        )
        .route(
            "/{index}/_alias/{alias}",
            put(cluster::put_alias).delete(cluster::delete_alias),
        )
        .route("/{index}/_aliases", get(index::get_aliases))
        .route("/{index}/_alias", get(index::get_aliases))
        .route("/{index}/_refresh", post(cluster::refresh))
        // Delete by query
        .route("/{index}/_delete_by_query", post(search::delete_by_query))
        // Document CRUD
        .route(
            "/{index}/_doc/{id}",
            put(document::index_doc)
                .post(document::index_doc)
                .get(document::get_doc)
                .delete(document::delete_doc),
        )
        .route("/{index}/_doc", post(document::index_doc_autoid))
        .route("/{index}/_update/{id}", post(document::update_doc))
        .route(
            "/{index}/_create/{id}",
            put(document::index_doc).post(document::index_doc),
        )
        // Search and count
        .route("/{index}/_search", post(search::search).get(search::search))
        .route("/{index}/_count", post(search::count).get(search::count))
        // Multi-get (index-scoped)
        .route(
            "/{index}/_mget",
            post(multi::mget_index).get(multi::mget_index),
        )
        .with_state(registry)
        .fallback(fallback_handler)
        .layer(TraceLayer::new_for_http())
        .layer(RequestDecompressionLayer::new())
}

async fn fallback_handler(req: axum::extract::Request) -> impl IntoResponse {
    let method = req.method().clone();
    let uri = req.uri().clone();
    tracing::warn!("Unmatched route: {} {}", method, uri);
    (
        StatusCode::OK,
        Json(serde_json::json!({"acknowledged": true})),
    )
}
