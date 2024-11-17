pub mod errors;
pub mod gop;
pub mod metadata;

use {
    self::gop::Gops,
    bytes::BytesMut,
    bytesio::bytes_reader::BytesReader,
    errors::CacheError,
    gop::Gop,
    std::collections::VecDeque,
    streamhub::define::{FrameData, StatisticData, StatisticDataSender},
    xflv::{
        define,
        flv_tag_header::{AudioTagHeader, VideoTagHeader},
        mpeg4_aac::Mpeg4AacProcessor,
        mpeg4_avc::Mpeg4AvcProcessor,
        Unmarshal,
    },
};

// #[derive(Clone)]
pub struct Cache {
    metadata: metadata::MetaData,
    metadata_timestamp: u32,
    video_seq: BytesMut,
    video_timestamp: u32,
    audio_seq: BytesMut,
    audio_timestamp: u32,
    gops: Gops,
    statistic_data_sender: Option<StatisticDataSender>,
}

impl Cache {
    pub fn new(gop_num: usize, statistic_data_sender: Option<StatisticDataSender>) -> Self {
        Cache {
            metadata: metadata::MetaData::new(),
            metadata_timestamp: 0,
            video_seq: BytesMut::new(),
            video_timestamp: 0,
            audio_seq: BytesMut::new(),
            audio_timestamp: 0,
            gops: Gops::new(gop_num),
            statistic_data_sender,
        }
    }

    //, values: Vec<Amf0ValueType>
    pub fn save_metadata(&mut self, chunk_body: &BytesMut, timestamp: u32) {
        self.metadata.save(chunk_body);
        self.metadata_timestamp = timestamp;
    }

    pub fn get_metadata(&self) -> Option<FrameData> {
        let data = self.metadata.get_chunk_body();
        if !data.is_empty() {
            Some(FrameData::MetaData {
                timestamp: self.metadata_timestamp,
                data,
            })
        } else {
            None
        }
    }
    //save audio gops and sequence header information
    pub async fn save_audio_data(
        &mut self,
        chunk_body: &BytesMut,
        timestamp: u32,
    ) -> Result<(), CacheError> {
        let channel_data = FrameData::Audio {
            timestamp,
            data: chunk_body.clone(),
        };
        self.gops.save_frame_data(channel_data, false);

        let mut reader = BytesReader::new(chunk_body.clone());
        let tag_header = AudioTagHeader::unmarshal(&mut reader)?;

        let remain_bytes = reader.extract_remaining_bytes();

        if remain_bytes.len() >= 2
            && tag_header.sound_format == define::SoundFormat::AAC as u8
            && tag_header.aac_packet_type == define::aac_packet_type::AAC_SEQHDR
        {
            self.audio_seq = chunk_body.clone();
            self.audio_timestamp = timestamp;

            if let Some(statistic_data_sender) = &self.statistic_data_sender {
                let mut aac_processor = Mpeg4AacProcessor::default();

                let aac = aac_processor
                    .extend_data(remain_bytes)
                    .audio_specific_config_load()?;

                let statistic_audio_codec = StatisticData::AudioCodec {
                    sound_format: define::SoundFormat::AAC,
                    profile: define::u8_2_aac_profile(aac.mpeg4_aac.object_type),
                    samplerate: aac.mpeg4_aac.sampling_frequency,
                    channels: aac.mpeg4_aac.channels,
                };
                if let Err(err) = statistic_data_sender.send(statistic_audio_codec) {
                    log::error!("send statistic_data err: {}", err);
                }
            }
        }

        if let Some(statistic_data_sender) = &self.statistic_data_sender {
            let statistic_audio_data = StatisticData::Audio {
                uuid: None,
                data_size: chunk_body.len(),
                aac_packet_type: tag_header.aac_packet_type,
                duration: 0,
            };
            if let Err(err) = statistic_data_sender.send(statistic_audio_data) {
                log::error!("send statistic_data err: {}", err);
            }
        }

        Ok(())
    }

    pub fn get_audio_seq(&self) -> Option<FrameData> {
        if !self.audio_seq.is_empty() {
            return Some(FrameData::Audio {
                timestamp: self.audio_timestamp,
                data: self.audio_seq.clone(),
            });
        }
        None
    }

    pub fn get_video_seq(&self) -> Option<FrameData> {
        if !self.video_seq.is_empty() {
            return Some(FrameData::Video {
                timestamp: self.video_timestamp,
                data: self.video_seq.clone(),
            });
        }
        None
    }
    //save video gops and sequence header information
    pub async fn save_video_data(
        &mut self,
        chunk_body: &BytesMut,
        timestamp: u32,
    ) -> Result<(), CacheError> {
        let channel_data = FrameData::Video {
            timestamp,
            data: chunk_body.clone(),
        };

        let mut reader = BytesReader::new(chunk_body.clone());
        let tag_header = VideoTagHeader::unmarshal(&mut reader)?;

        let is_key_frame = tag_header.frame_type == define::frame_type::KEY_FRAME;
        self.gops.save_frame_data(channel_data, is_key_frame);

        if is_key_frame && tag_header.avc_packet_type == define::avc_packet_type::AVC_SEQHDR {
            self.video_seq = chunk_body.clone();
            self.video_timestamp = timestamp;

            if let Some(statistic_data_sender) = &self.statistic_data_sender {
                let mut avc_processor = Mpeg4AvcProcessor::default();
                avc_processor.decoder_configuration_record_load(&mut reader)?;

                let statistic_video_codec = StatisticData::VideoCodec {
                    codec: define::AvcCodecId::H264,
                    profile: define::u8_2_avc_profile(avc_processor.mpeg4_avc.profile),
                    level: define::u8_2_avc_level(avc_processor.mpeg4_avc.level),
                    width: avc_processor.mpeg4_avc.width,
                    height: avc_processor.mpeg4_avc.height,
                };
                if let Err(err) = statistic_data_sender.send(statistic_video_codec) {
                    log::error!("send statistic_data err: {}", err);
                }
            }
        }

        if let Some(statistic_data_sender) = &self.statistic_data_sender {
            let statistic_video_data = StatisticData::Video {
                uuid: None,
                data_size: chunk_body.len(),
                frame_count: 1,
                is_key_frame: Some(is_key_frame),
                duration: 0,
            };

            if let Err(err) = statistic_data_sender.send(statistic_video_data) {
                log::error!("send statistic_data err: {}", err);
            }
        }
        Ok(())
    }

    pub fn get_gops_data(&self) -> Option<VecDeque<Gop>> {
        if self.gops.setted() {
            Some(self.gops.get_gops())
        } else {
            None
        }
    }
}
