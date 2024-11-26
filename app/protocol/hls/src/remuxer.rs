use {
    super::{errors::HlsError, flv_data_receiver::FlvDataReceiver},
    streamhub::{
        define::{BroadcastEvent, BroadcastEventReceiver, StreamHubEventSender},
        stream::StreamIdentifier,
    },
};

pub struct HlsRemuxer {
    client_event_consumer: BroadcastEventReceiver,
    event_producer: StreamHubEventSender,
    need_record: bool,
}

impl HlsRemuxer {
    pub fn new(
        consumer: BroadcastEventReceiver,
        event_producer: StreamHubEventSender,
        need_record: bool,
    ) -> Self {
        Self {
            client_event_consumer: consumer,
            event_producer,
            need_record,
        }
    }

    pub async fn run(&mut self) -> Result<(), HlsError> {
        loop {
            let val = self.client_event_consumer.recv().await?;
            match val {
                BroadcastEvent::Publish { identifier } => {
                    if let StreamIdentifier::Rtmp {
                        app_name,
                        stream_name,
                    } = identifier
                    {
                        let mut rtmp_subscriber = FlvDataReceiver::new(
                            app_name,
                            stream_name,
                            self.event_producer.clone(),
                            25,   // calvin NOTE: the duration length depends on walrus confirmation speed
                            self.need_record,
                        );

                        tokio::spawn(async move {
                            if let Err(err) = rtmp_subscriber.run().await {
                                println!("hls handler run error {err}");
                            }
                        });
                    }
                }
                _ => {
                    log::trace!("other infos...");
                }
            }
        }
    }
}
