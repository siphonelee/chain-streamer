module streamer::streamer {
    use sui::tx_context::sender;
    use sui::vec_map::{VecMap, Self};
    use sui::package;
    use std::string::{Self, String};
    use sui::clock::Clock;
    use sui::event;

    const ENoSuchLiveStream: u64 = 1;
    const ELiveStreamUrlAlreadyExists: u64 = 2;
    const ENoSuchVodStream: u64 = 1;

    public struct LiveStreamInfo has copy, store, drop {
        name: String,
        desc: String,
        start_at: u64,
        last_update_at: u64,
        m3u8_content: String,
    }

    public struct VodStreamInfo has copy, store, drop {
        name: String,
        desc: String,
        upload_at: u64,
        m3u8_content: String,
    }

    public struct AdminCap has key {
        id: UID,
    }

    public struct Streams has key, store {
        id: UID,
        live_streams: VecMap<String, LiveStreamInfo>,  // live stream url -> stream info
        vod_streams: vector<VodStreamInfo>,
    }

    public struct AllStreamsInfo has copy, drop {
        live_streams: VecMap<String, LiveStreamInfo>,  // live stream url -> stream info
        vod_streams: vector<VodStreamInfo>,        
    }

    public struct AllStreamsListEvent has copy, drop {
        data: AllStreamsInfo, 
    }

    public struct SingleLiveStreamsEvent has copy, drop {
        data: LiveStreamInfo, 
    }

    public struct SingleVodStreamsEvent has copy, drop {
        data: VodStreamInfo, 
    }

    public struct STREAMER has drop {}

    fun init(otw: STREAMER, ctx: &mut TxContext) {
        // Creating and sending the Publisher object to the sender.
        package::claim_and_keep(otw, ctx);

        // Creating and sending the HouseCap object to the sender.
        let streams = Streams {
            id: object::new(ctx),
            live_streams: vec_map::empty(),
            vod_streams: vector::empty(),
        };

        transfer::share_object(streams);

        let admin = AdminCap {
            id: object::new(ctx),
        };

        transfer::transfer(admin, tx_context::sender(ctx));
    }

    public fun create_live_stream(_: &AdminCap, streams: &mut Streams, clock: &Clock, 
                                url: String, name: String, desc: String, _ctx: &mut TxContext) {                
        let s = streams.live_streams.try_get(&url);
        assert!(s.is_none(), ELiveStreamUrlAlreadyExists);

        let stream = LiveStreamInfo {
            name,
            desc,
            start_at: clock.timestamp_ms(),
            last_update_at: 0u64,
            m3u8_content: string::utf8(b""),
        };
        streams.live_streams.insert(url, stream);
    } 

    public fun update_live_stream(_: &AdminCap, streams: &mut Streams, clock: &Clock, 
                            url: String, m3u8_content: String, _ctx: &mut TxContext) {        
        let s = streams.live_streams.try_get(&url);
        assert!(s.is_some(), ENoSuchLiveStream);

        let t = streams.live_streams.get_mut(&url);
        t.m3u8_content = m3u8_content;
        t.last_update_at = clock.timestamp_ms();
    } 

    public fun add_vod_stream(_: &AdminCap,  streams: &mut Streams, clock: &Clock, 
                            name: String, desc: String, m3u8_content: String, _ctx: &mut TxContext) {        
        let stream = VodStreamInfo {
            name,
            desc,
            upload_at: clock.timestamp_ms(),
            m3u8_content,
        };

        streams.vod_streams.push_back(stream);
    } 

    // move to VOD stream when live stream ends
    public fun move_live_stream_to_vod_stream(admin: &AdminCap, streams: &mut Streams, clock: &Clock, 
                            url: String, full_m3u8_content: String, _ctx: &mut TxContext) {        
        // will abort if the key not exists
        let (_, v) = streams.live_streams.remove(&url);
        add_vod_stream(admin, streams, clock, v.name, v.desc, full_m3u8_content, _ctx);
    } 

    public fun get_all_streams(streams: &mut Streams, _ctx: &mut TxContext): AllStreamsInfo {
        let info = AllStreamsInfo {
            live_streams: streams.live_streams,
            vod_streams: streams.vod_streams,
        };

        event::emit(AllStreamsListEvent {data: info});
        info
    }

    public fun get_live_stream(streams: &mut Streams, url: String, _ctx: &mut TxContext): LiveStreamInfo  {
        let s = streams.live_streams.try_get(&url);
        assert!(s.is_some(), ENoSuchLiveStream);

        let info = LiveStreamInfo {
            name: s.borrow().name,
            desc: s.borrow().desc,
            start_at: s.borrow().start_at,
            last_update_at: s.borrow().last_update_at,
            m3u8_content: s.borrow().m3u8_content,
        };

        event::emit(SingleLiveStreamsEvent {data: info });
        info
    } 

    public fun get_vod_stream(streams: &mut Streams, index: u64, _ctx: &mut TxContext): VodStreamInfo  {
        assert!(index < streams.vod_streams.length(), ENoSuchVodStream);
        
        let s = streams.vod_streams.borrow(index);

        let info = VodStreamInfo {
            name: s.name,
            desc: s.desc,
            upload_at: s.upload_at,
            m3u8_content: s.m3u8_content,
        };

        event::emit(SingleVodStreamsEvent {data: info });
        info
    } 
}

