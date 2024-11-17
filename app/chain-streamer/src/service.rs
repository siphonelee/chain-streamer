use crate::config::{AuthConfig, AuthSecretConfig};
use commonlib::auth::AuthType;
use std::sync::Arc;

use {
    super::api,
    super::config::Config,
    //https://rustcc.cn/article?id=6dcbf032-0483-4980-8bfe-c64a7dfb33c7
    anyhow::Result,
    commonlib::auth::Auth,
    hls::remuxer::HlsRemuxer,
    hls::server as hls_server,
    rtmp::{
        relay::{pull_client::PullClient, push_client::PushClient},
        rtmp::RtmpServer,
    },
    streamhub::{notify::http::HttpNotifier, notify::Notifier, StreamsHub},
    tokio,
};

pub struct Service {
    cfg: Config,
}

impl Service {
    pub fn new(cfg: Config) -> Self {
        Service { cfg }
    }

    fn gen_auth(auth_config: &Option<AuthConfig>, authsecret: &AuthSecretConfig) -> Option<Auth> {
        if let Some(cfg) = auth_config {
            let auth_type = if let Some(push_enabled) = cfg.push_enabled {
                if push_enabled && cfg.pull_enabled {
                    AuthType::Both
                } else if !push_enabled && !cfg.pull_enabled {
                    AuthType::None
                } else if push_enabled && !cfg.pull_enabled {
                    AuthType::Push
                } else {
                    AuthType::Pull
                }
            } else {
                match cfg.pull_enabled {
                    true => AuthType::Pull,
                    false => AuthType::None,
                }
            };
            Some(Auth::new(
                authsecret.key.clone(),
                authsecret.password.clone(),
                authsecret.push_password.clone(),
                cfg.algorithm.clone(),
                auth_type,
            ))
        } else {
            None
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let notifier: Option<Arc<dyn Notifier>> = if let Some(httpnotifier) = &self.cfg.httpnotify {
            if !httpnotifier.enabled {
                None
            } else {
                Some(Arc::new(HttpNotifier::new(
                    httpnotifier.on_publish.clone(),
                    httpnotifier.on_unpublish.clone(),
                    httpnotifier.on_play.clone(),
                    httpnotifier.on_stop.clone(),
                )))
            }
        } else {
            None
        };

        let mut stream_hub = StreamsHub::new(notifier);

        self.start_hls(&mut stream_hub).await?;
        self.start_rtmp(&mut stream_hub).await?;
        self.start_http_api_server(&mut stream_hub).await?;

        tokio::spawn(async move {
            stream_hub.run().await;
            log::info!("stream hub end...");
        });
        Ok(())
    }

    async fn start_http_api_server(&mut self, stream_hub: &mut StreamsHub) -> Result<()> {
        let producer = stream_hub.get_hub_event_sender();

        let http_api_port = if let Some(httpapi) = &self.cfg.httpapi {
            httpapi.port
        } else {
            8000
        };

        tokio::spawn(async move {
            api::run(producer, http_api_port).await;
        });
        Ok(())
    }

    async fn start_rtmp(&mut self, stream_hub: &mut StreamsHub) -> Result<()> {
        let rtmp_cfg = &self.cfg.rtmp;

        if let Some(rtmp_cfg_value) = rtmp_cfg {
            if !rtmp_cfg_value.enabled {
                return Ok(());
            }

            let gop_num = if let Some(gop_num_val) = rtmp_cfg_value.gop_num {
                gop_num_val
            } else {
                1
            };

            let producer = stream_hub.get_hub_event_sender();

            /*static push */
            if let Some(push_cfg_values) = &rtmp_cfg_value.push {
                for push_value in push_cfg_values {
                    if !push_value.enabled {
                        continue;
                    }
                    log::info!("start rtmp push client..");
                    let address = format!(
                        "{ip}:{port}",
                        ip = push_value.address,
                        port = push_value.port
                    );

                    let mut push_client = PushClient::new(
                        address,
                        stream_hub.get_client_event_consumer(),
                        producer.clone(),
                    );
                    tokio::spawn(async move {
                        if let Err(err) = push_client.run().await {
                            log::error!("push client error {}", err);
                        }
                    });

                    stream_hub.set_rtmp_push_enabled(true);
                }
            }
            /*static pull*/
            if let Some(pull_cfg_value) = &rtmp_cfg_value.pull {
                if pull_cfg_value.enabled {
                    let address = format!(
                        "{ip}:{port}",
                        ip = pull_cfg_value.address,
                        port = pull_cfg_value.port
                    );
                    log::info!("start rtmp pull client from address: {}", address);
                    let mut pull_client = PullClient::new(
                        address,
                        stream_hub.get_client_event_consumer(),
                        producer.clone(),
                    );

                    tokio::spawn(async move {
                        if let Err(err) = pull_client.run().await {
                            log::error!("pull client error {}", err);
                        }
                    });

                    stream_hub.set_rtmp_pull_enabled(true);
                }
            }

            let listen_port = rtmp_cfg_value.port;
            let address = format!("0.0.0.0:{listen_port}");

            let auth = Self::gen_auth(&rtmp_cfg_value.auth, &self.cfg.authsecret);
            let mut rtmp_server = RtmpServer::new(address, producer, gop_num, auth);
            tokio::spawn(async move {
                if let Err(err) = rtmp_server.run().await {
                    log::error!("rtmp server error: {}", err);
                }
            });
        }

        Ok(())
    }

    async fn start_hls(&mut self, stream_hub: &mut StreamsHub) -> Result<()> {
        let hls_cfg = &self.cfg.hls;

        if let Some(hls_cfg_value) = hls_cfg {
            if !hls_cfg_value.enabled {
                return Ok(());
            }

            let event_producer = stream_hub.get_hub_event_sender();
            let cient_event_consumer = stream_hub.get_client_event_consumer();
            let mut hls_remuxer = HlsRemuxer::new(
                cient_event_consumer,
                event_producer,
                hls_cfg_value.need_record,
            );

            tokio::spawn(async move {
                if let Err(err) = hls_remuxer.run().await {
                    log::error!("rtmp event processor error: {}", err);
                }
            });

            let port = hls_cfg_value.port;
            let auth = Self::gen_auth(&hls_cfg_value.auth, &self.cfg.authsecret);
            tokio::spawn(async move {
                if let Err(err) = hls_server::run(port, auth).await {
                    log::error!("hls server error: {}", err);
                }
            });
            stream_hub.set_hls_enabled(true);
        }

        Ok(())
    }
}
