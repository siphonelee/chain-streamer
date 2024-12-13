use {
    anyhow::Result,
    axum::{
        extract::Query, response::{Response, IntoResponse}, body::Body, routing::{get, post}, Json, Router
    },
    serde::Deserialize,
    serde_json::Value,
    std::{net::SocketAddr, sync::Arc},
    streamhub::{
        define::{self, RelayType, StreamHubEventSender},
        stream::StreamIdentifier,
        utils::Uuid,
    },
    tokio::{self, net::TcpListener, sync::oneshot},
    axum::http::header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN},
    tower_http::cors::{Any, CorsLayer},
};

#[derive(serde::Serialize)]
struct ApiResponse<T> {
    error_code: i32,
    desp: String,
    data: T,
}

// the input to our `KickOffClient` handler
#[derive(Deserialize)]
struct KickOffClientParams {
    uuid: String,
}

#[derive(Deserialize, Debug)]
struct QueryWholeStreamsParams {
    // query top N by subscriber's count.
    top: Option<usize>,
}

#[derive(Deserialize)]
struct StreamNameParam {
    stream_name: String,
}

#[derive(Deserialize)]
struct StreamIndexParam {
    stream_index: u64,
}

#[derive(Deserialize)]
struct QueryStreamParams {
    identifier: StreamIdentifier,
    // if specify uuid, then query the stream by uuid and filter no used data.
    uuid: Option<String>,
}

#[derive(Deserialize)]
struct RelayStreamParams {
    //guaranteed by the user to be unique
    id: String,
    identifier: Option<StreamIdentifier>,
    server_address: Option<String>,
    relay_type: RelayType,
}

#[derive(Deserialize)]
struct CreateStreamParams {
    //guaranteed by the user to be unique
    url: String,
    name: String,
    description: String,
}

#[derive(Clone)]
struct ApiService {
    channel_event_producer: StreamHubEventSender,
}

impl ApiService {
    async fn root(&self) -> String {
        String::from(
            "Usage of chain-streamer http api:
                ./api/query_whole_streams(get) query whole streams' information or top streams' information.
                ./api/query_stream(post) query stream information by identifier and uuid.
                ./api/kick_off_client(post) kick off client by publish/subscribe id.\n",
        )
    }

    async fn query_whole_streams(
        &self,
        params: QueryWholeStreamsParams,
    ) -> Json<ApiResponse<Value>> {
        log::info!("query_whole_streams: {:?}", params);
        let (result_sender, result_receiver) = oneshot::channel();
        let hub_event = define::StreamHubEvent::ApiStatistic {
            top_n: params.top,
            identifier: None,
            uuid: None,
            result_sender,
        };
        if let Err(err) = self.channel_event_producer.send(hub_event) {
            log::error!("send api event error: {}", err);
        }

        match result_receiver.await {
            Ok(dat_val) => {
                let api_response = ApiResponse {
                    error_code: 0,
                    desp: String::from("succ"),
                    data: dat_val,
                };
                Json(api_response)
            }
            Err(err) => {
                let api_response = ApiResponse {
                    error_code: -1,
                    desp: String::from("failed"),
                    data: serde_json::json!(err.to_string()),
                };
                Json(api_response)
            }
        }
    }

    async fn query_live_m3u8(&self, param: StreamNameParam) -> Response<Body> {
        let (result_sender, result_receiver) = oneshot::channel();
        let hub_event = define::StreamHubEvent::ApiQueryLiveM3u8 {
            name: param.stream_name,
            result_sender
        };
        if let Err(err) = self.channel_event_producer.send(hub_event) {
            log::error!("send api event error: {}", err);
        }

        let body = match result_receiver.await {
            Ok(val) => {
                val.as_str().unwrap().to_owned()
            }
            Err(err) => {
                err.to_string()
            }
        };
        let builder = Response::builder().header("Content-Type", "application/x-mpegURL");
        return builder.body(Body::from(body)).unwrap();
    }

    async fn query_vod_m3u8(&self, param: StreamIndexParam) -> Response<Body> {
        let (result_sender, result_receiver) = oneshot::channel();
        let hub_event = define::StreamHubEvent::ApiQueryVodM3u8 {
            index: param.stream_index,
            result_sender
        };
        if let Err(err) = self.channel_event_producer.send(hub_event) {
            log::error!("send api event error: {}", err);
        }

        let body = match result_receiver.await {
            Ok(val) => {
                val.as_str().unwrap().to_owned()
            }
            Err(err) => {
                err.to_string()
            }
        };
        let builder = Response::builder().header("Content-Type", "application/x-mpegURL");
        return builder.body(Body::from(body)).unwrap();
    }

    async fn query_stream(&self, stream: QueryStreamParams) -> Json<ApiResponse<Value>> {
        let uuid = if let Some(uid) = stream.uuid {
            Uuid::from_str2(&uid)
        } else {
            None
        };

        let (result_sender, result_receiver) = oneshot::channel();
        let hub_event = define::StreamHubEvent::ApiStatistic {
            top_n: None,
            identifier: Some(stream.identifier),
            uuid,
            result_sender,
        };

        if let Err(err) = self.channel_event_producer.send(hub_event) {
            log::error!("send api event error: {}", err);
        }

        match result_receiver.await {
            Ok(dat_val) => {
                let api_response = ApiResponse {
                    error_code: 0,
                    desp: String::from("succ"),
                    data: dat_val,
                };
                Json(api_response)
            }
            Err(err) => {
                let api_response = ApiResponse {
                    error_code: -1,
                    desp: String::from("failed"),
                    data: serde_json::json!(err.to_string()),
                };
                Json(api_response)
            }
        }
    }

    async fn kick_off_client(&self, id: KickOffClientParams) -> Result<String> {
        let id_result = Uuid::from_str2(&id.uuid);

        if let Some(id) = id_result {
            let hub_event = define::StreamHubEvent::ApiKickClient { id };

            if let Err(err) = self.channel_event_producer.send(hub_event) {
                log::error!("send api kick_off_client event error: {}", err);
            }
        }

        Ok(String::from("ok"))
    }

    async fn start_relay_stream(&self, relay_info: RelayStreamParams) -> Json<ApiResponse<Value>> {
        if relay_info.identifier.is_none() || relay_info.server_address.is_none() {
            let api_response = ApiResponse {
                error_code: -1,
                desp: String::from("identifier or server_address is none"),
                data: Value::Null,
            };
            return Json(api_response);
        }

        let (result_sender, result_receiver) = oneshot::channel();

        let hub_event = define::StreamHubEvent::ApiStartRelayStream {
            id: relay_info.id,
            identifier: relay_info.identifier.unwrap(),
            server_address: relay_info.server_address.unwrap(),
            relay_type: relay_info.relay_type,
            result_sender,
        };

        if let Err(err) = self.channel_event_producer.send(hub_event) {
            log::error!("send api relay_stream event error: {}", err);
        }

        match result_receiver.await {
            Ok(val) => match val {
                Ok(()) => {
                    let api_response = ApiResponse {
                        error_code: 0,
                        desp: String::from("succ"),
                        data: Value::Null,
                    };
                    Json(api_response)
                }
                Err(err) => {
                    let api_response = ApiResponse {
                        error_code: -1,
                        desp: String::from("failed"),
                        data: serde_json::json!(err.to_string()),
                    };
                    Json(api_response)
                }
            },
            Err(err) => {
                let api_response = ApiResponse {
                    error_code: -1,
                    desp: String::from("failed"),
                    data: serde_json::json!(err.to_string()),
                };
                Json(api_response)
            }
        }
    }

    async fn create_live_stream(&self, stream_info: CreateStreamParams) -> Json<ApiResponse<Value>> {
        let (result_sender, result_receiver) = oneshot::channel();
        let hub_event = define::StreamHubEvent::ApiCreateStream {
            url: stream_info.url,
            name: stream_info.name,
            description: stream_info.description,
            result_sender
        };
        if let Err(err) = self.channel_event_producer.send(hub_event) {
            log::error!("send api event error: {}", err);
        }

        match result_receiver.await {
            Ok(val) => {
                let api_response = ApiResponse {
                    error_code: 0,
                    desp: String::from("success"),
                    data: serde_json::json!(val),
                };
                Json(api_response)
            }
            Err(err) => {
                let api_response = ApiResponse {
                    error_code: -1,
                    desp: String::from("failed"),
                    data: serde_json::json!(err.to_string()),
                };
                Json(api_response)
            }
        }
    }

    async fn stop_relay_stream(&self, relay_info: RelayStreamParams) -> Json<ApiResponse<Value>> {
        let (result_sender, result_receiver) = oneshot::channel();

        let hub_event = define::StreamHubEvent::ApiStopRelayStream {
            id: relay_info.id,
            relay_type: relay_info.relay_type,
            result_sender,
        };

        if let Err(err) = self.channel_event_producer.send(hub_event) {
            log::error!("send api relay_stream event error: {}", err);
        }

        match result_receiver.await {
            Ok(val) => match val {
                Ok(()) => {
                    let api_response = ApiResponse {
                        error_code: 0,
                        desp: String::from("succ"),
                        data: Value::Null,
                    };
                    Json(api_response)
                }
                Err(err) => {
                    let api_response = ApiResponse {
                        error_code: -1,
                        desp: String::from("failed"),
                        data: serde_json::json!(err.to_string()),
                    };
                    Json(api_response)
                }
            },
            Err(err) => {
                let api_response = ApiResponse {
                    error_code: -1,
                    desp: String::from("failed"),
                    data: serde_json::json!(err.to_string()),
                };
                Json(api_response)
            }
        }
    }
}

pub async fn run(producer: StreamHubEventSender, port: usize) {
    let api = Arc::new(ApiService {
        channel_event_producer: producer,
    });

    let api_root = api.clone();
    let root = move || async move { api_root.root().await };

    let api_query_streams = api.clone();
    let query_streams = move |Query(params): Query<QueryWholeStreamsParams>| async move {
        api_query_streams.query_whole_streams(params).await
    };

    let api_get_live_m3u8 = api.clone();
    let query_live_m3u8 = move |Query(params): Query<StreamNameParam>| async move {
        api_get_live_m3u8.query_live_m3u8(params).await
    };

    let api_get_vod_m3u8 = api.clone();
    let query_vod_m3u8 = move |Query(params): Query<StreamIndexParam>| async move {
        api_get_vod_m3u8.query_vod_m3u8(params).await
    };

    let api_query_stream = api.clone();
    let query_stream = move |Json(stream): Json<QueryStreamParams>| async move {
        api_query_stream.query_stream(stream).await
    };

    let api_kick_off = api.clone();
    let kick_off = move |Json(id): Json<KickOffClientParams>| async move {
        match api_kick_off.kick_off_client(id).await {
            Ok(response) => response,
            Err(_) => "error".to_owned(),
        }
    };

    let api_start_relay_stream = api.clone();
    let start_relay_stream = move |Json(params): Json<RelayStreamParams>| async move {
        api_start_relay_stream.start_relay_stream(params).await
    };

    let api_create_live_stream = api.clone();
    let create_live_stream = move |Json(params): Json<CreateStreamParams>| async move {
        api_create_live_stream.create_live_stream(params).await
    };

    let api_stop_relay_stream = api.clone();
    let stop_relay_stream = move |Json(params): Json<RelayStreamParams>| async move {
        api_stop_relay_stream.stop_relay_stream(params).await
    };

    let app = Router::new()
        .route("/", get(root))
        .route("/api/query_whole_streams", get(query_streams))
        .route("/api/query_live_m3u8", get(query_live_m3u8))
        .route("/api/query_vod_m3u8", get(query_vod_m3u8))
        .route("/api/create_live_stream", post(create_live_stream))
        .layer(CorsLayer::permissive())
        .route("/api/query_stream", post(query_stream))
        .route("/api/kick_off_client", post(kick_off))
        .route("/api/start_relay_stream", post(start_relay_stream))
        .route("/api/stop_relay_stream", post(stop_relay_stream));

    log::info!("Http api server listening on http://0.0.0.0:{}", port);

    let addr = SocketAddr::from(([0, 0, 0, 0], port as u16));
    let listener: TcpListener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            log::error!(target: "api", "Unable to create TCP listener: {}", e);
            std::process::exit(1);
        }
      };
          
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
