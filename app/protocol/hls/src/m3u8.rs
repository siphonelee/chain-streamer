use {
    super::{errors::MediaError, errors::MediaErrorValue, ts::Ts}, bytes::BytesMut, futures::executor, rand::prelude::*, regex::Regex, std::{collections::VecDeque, error::Error, fs::{self, File}, io::{Cursor, Read, Write}, time::{Duration, SystemTime}}, tokio::{fs::File as TkFile, io::{self, AsyncReadExt}}

};

const PUBLIC_AGGREGATORS: [&str;2] = [
    // "https://publisher.walrus-testnet.walrus.space",   // fast? unstable
    "https://walrus-testnet-publisher.nodes.guru",  // fast
    // "https://walrus-testnet-publisher.nodeinfra.com",  // fast? unstable
    // "https://walrus-testnet.blockscope.net:11444",  // slow
    "http://walrus-publisher-testnet.overclock.run:9001",  // fast?
    // "http://walrus-testnet-publisher.everstake.one:9001",   // fast unstable
    // "http://walrus.testnet.pops.one:9001",  // kind slow
    // "http://ivory-dakar-e5812.walrus.bdnodes.net:9001", // fast unstable
    // "http://publisher.testnet.sui.rpcpool.com:9001",  // fast? unstable
    // "http://walrus.krates.ai:9001", // kind slow
    // "http://walrus-publisher-testnet.latitude-sui.com:9001",   // slow
    // "http://walrus-testnet.stakingdefenseleague.com:9001",   // fast? unstable
    // "http://walrus.sui.thepassivetrust.com:9001",   // kind slow
    // "http://walrus.globalstake.io:9001",   // fast? unstable
];

const BLOBID_REGEXP_STR: &str = "\"blobId\":\"(.*?)\",";

pub struct Segment {
    /*ts duration*/
    pub duration: i64,
    pub discontinuity: bool,
    /*ts name*/
    pub name: String,
    path: String,
    pub is_eof: bool,
    // calvin
    pub blob_id: String,
}

impl Segment {
    pub fn new(
        duration: i64,
        discontinuity: bool,
        name: String,
        path: String,
        is_eof: bool,
        blob_id: String,
    ) -> Self {
        Self {
            duration,
            discontinuity,
            name,
            path,
            is_eof,
            blob_id,
        }
    }
}

pub struct M3u8 {
    version: u16,
    sequence_no: u64,
    /*What duration should media files be?
    A duration of 10 seconds of media per file seems to strike a reasonable balance for most broadcast content.
    http://devimages.apple.com/iphone/samples/bipbop/bipbopall.m3u8*/
    duration: i64,
    /*How many files should be listed in the index file during a continuous, ongoing session?
    The normal recommendation is 3, but the optimum number may be larger.*/
    live_ts_count: usize,

    segments: VecDeque<Segment>,

    m3u8_folder: String,
    live_m3u8_name: String,

    ts_handler: Ts,

    need_record: bool,
    vod_m3u8_content: String,
    vod_m3u8_name: String,
}

impl M3u8 {
    pub fn new(
        duration: i64,
        live_ts_count: usize,
        app_name: String,
        stream_name: String,
        need_record: bool,
    ) -> Self {
        let m3u8_folder = format!("./{app_name}/{stream_name}");
        fs::create_dir_all(m3u8_folder.clone()).unwrap();

        let live_m3u8_name = format!("{stream_name}.m3u8");
        let vod_m3u8_name = if need_record {
            format!("vod_{stream_name}.m3u8")
        } else {
            String::default()
        };

        let mut m3u8 = Self {
            version: 3,
            sequence_no: 0,
            duration,
            live_ts_count,
            segments: VecDeque::new(),
            m3u8_folder,
            live_m3u8_name,
            ts_handler: Ts::new(app_name, stream_name),
            // record,
            need_record,
            vod_m3u8_content: String::default(),
            vod_m3u8_name,
        };

        if need_record {
            m3u8.vod_m3u8_content = m3u8.generate_m3u8_header(true);
        }
        m3u8
    }

    pub fn upload_walrus(&self, data: BytesMut) -> Result<String, MediaError> {
        let count = PUBLIC_AGGREGATORS.len();
        let mut rng = rand::thread_rng();
        let index = (rng.gen::<f64>() * count as f64).trunc() as usize;
        let aggr_url = PUBLIC_AGGREGATORS.get(index).unwrap();
        
        let publish_url = (*aggr_url).to_owned() + "/v1/store";
        println!("publish to: {}", publish_url);

        /*
        let client = reqwest::Client::new(); 
        let res = client.put(publish_url)
            // .header("content-type","application/octet-stream") 
            .body(data.freeze()) 
            .send()
            .await
            .unwrap();
        */

        let now = SystemTime::now();
        let res = ureq::put(publish_url.as_str())
            .set("Content-Length", &data.len().to_string())
            .send(Cursor::new(data.freeze().to_vec()));
        if res.is_err() {
            return Err(MediaError{value: MediaErrorValue::WalrusUploadError});
        }
        let res = res.unwrap();
        let span = SystemTime::now().duration_since(now).unwrap().as_secs();

        println!("{:?}", res);
        let text = res.into_string().unwrap(); 

        let regexp = Regex::new(BLOBID_REGEXP_STR).unwrap();
        let Some(caps) = regexp.captures(text.as_str()) else {
            println!("blobId not match: {}", text);
            return Err(MediaError{value: MediaErrorValue::BlobIdParseError});
        };

        let blob_id = &caps[1];
        println!("{}", blob_id);
        println!("seconds: {:?}", span);
        println!("{}", "--------------------");

        Ok(blob_id.to_owned())
    }

    pub fn add_segment(
        &mut self,
        duration: i64,
        discontinuity: bool,
        is_eof: bool,
        ts_data: BytesMut,
    ) -> Result<(), MediaError> {
        let segment_count = self.segments.len();

        if segment_count >= self.live_ts_count {
            let segment = self.segments.pop_front().unwrap();
            if !self.need_record {
                // calvin TODO: delete from walrus (or not, depending on epoch mechanism)
                self.ts_handler.delete(segment.path);
            }

            self.sequence_no += 1;
        }
        self.duration = std::cmp::max(duration, self.duration);
        let (ts_name, ts_path) = self.ts_handler.write(ts_data.clone())?;

        // calvin: TODO upload to walrus in the background
        let blob_id = self.upload_walrus(ts_data)?;

        let segment = Segment::new(duration, discontinuity, ts_name, ts_path, is_eof, blob_id);
        if self.need_record {
            self.update_vod_m3u8(&segment);
        }

        self.segments.push_back(segment);

        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), MediaError> {
        if self.need_record {
            let vod_m3u8_path = format!("{}/{}", self.m3u8_folder, self.vod_m3u8_name);
            let mut file_handler = File::create(vod_m3u8_path).unwrap();
            self.vod_m3u8_content += "#EXT-X-ENDLIST\n";
            file_handler.write_all(self.vod_m3u8_content.as_bytes())?;
        } else {
            for segment in &self.segments {
                self.ts_handler.delete(segment.path.clone());
            }
        }

        //clear live m3u8
        let live_m3u8_path = format!("{}/{}", self.m3u8_folder, self.live_m3u8_name);
        fs::remove_file(live_m3u8_path)?;

        Ok(())
    }

    pub fn generate_m3u8_header(&self, is_vod: bool) -> String {
        let mut m3u8_header = "#EXTM3U\n".to_string();
        m3u8_header += format!("#EXT-X-VERSION:{}\n", self.version).as_str();
        m3u8_header += format!("#EXT-X-TARGETDURATION:{}\n", (self.duration + 999) / 1000).as_str();

        if is_vod {
            m3u8_header += "#EXT-X-MEDIA-SEQUENCE:0\n";
            m3u8_header += "#EXT-X-PLAYLIST-TYPE:VOD\n";
            m3u8_header += "#EXT-X-ALLOW-CACHE:YES\n";
        } else {
            m3u8_header += format!("#EXT-X-MEDIA-SEQUENCE:{}\n", self.sequence_no).as_str();
        }

        m3u8_header
    }

    pub fn refresh_playlist(&mut self) -> Result<String, MediaError> {
        let mut m3u8_content = self.generate_m3u8_header(false);

        for segment in &self.segments {
            if segment.discontinuity {
                m3u8_content += "#EXT-X-DISCONTINUITY\n";
            }
            m3u8_content += format!(
                "#EXTINF:{:.3}\n{}\n",
                segment.duration as f64 / 1000.0,
                segment.name
            )
            .as_str();

            if segment.is_eof {
                m3u8_content += "#EXT-X-ENDLIST\n";
                break;
            }
        }

        let m3u8_path = format!("{}/{}", self.m3u8_folder, self.live_m3u8_name);

        let mut file_handler = File::create(m3u8_path).unwrap();
        file_handler.write_all(m3u8_content.as_bytes())?;

        // calvin TODO: upload m3u8 to contract

        Ok(m3u8_content)
    }

    // calvin TODO: change name to walrus blob url
    pub fn update_vod_m3u8(&mut self, segment: &Segment) {
        if segment.discontinuity {
            self.vod_m3u8_content += "#EXT-X-DISCONTINUITY\n";
        }
        self.vod_m3u8_content += format!(
            "#EXTINF:{:.3}\n{}\n",
            segment.duration as f64 / 1000.0,
            segment.name
        )
        .as_str();
    }
}
