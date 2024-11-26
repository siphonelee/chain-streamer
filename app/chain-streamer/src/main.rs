use {
    anyhow::Result,
    clap::{value_parser, Arg, Command},
    env_logger_extend::logger::{Logger, Rotate},
    std::{env, str::FromStr},
    tokio::signal,
    chain_streamer::{config, config::Config, service::Service},
};

// #[tokio::main(flavor = "current_thread")]
#[tokio::main]
async fn main() -> Result<()> {
    let log_levels = vec!["trace", "debug", "info", "warn", "error"];

    let mut cmd = Command::new("ChainStreamer")
        .bin_name("chain-streamer")
        .arg(
            Arg::new("config_file_path")
                .long("config")
                .short('c')
                .value_name("path")
                .help("Specify the chain-streamer server configuration file path.")
                .value_parser(value_parser!(String))
                .conflicts_with_all(["rtmp", "hls", "log"]),
        )
        .arg(
            Arg::new("rtmp")
                .long("rtmp")
                .short('r')
                .value_name("port")
                .help("Specify the rtmp listening port.(e.g.:1935)")
                .value_parser(value_parser!(usize))
                .conflicts_with("config_file_path"),
        )
        .arg(
            Arg::new("hls")
                .long("hls")
                .short('s')
                .value_name("port")
                .help("Specify the hls listening port.(e.g.:8081)")
                .value_parser(value_parser!(usize))
                .conflicts_with("config_file_path"),
        )
        .arg(
            Arg::new("log")
                .long("log")
                .short('l')
                .value_name("level")
                .help("Specify the log level.")
                .value_parser(log_levels)
                .conflicts_with("config_file_path"),
        );

    let args: Vec<String> = env::args().collect();
    if 1 == args.len() {
        cmd.print_help()?;
        return Ok(());
    }

    let matches = cmd.clone().get_matches();

    let config = if let Some(path) = matches.get_one::<String>("config_file_path") {
        let config = config::load(path);
        match config {
            Ok(val) => val,
            Err(err) => {
                println!("{path}: {err}");
                return Ok(());
            }
        }
    } else {
        let rtmp_port_o = matches.get_one::<usize>("rtmp");

        if rtmp_port_o.is_none() {
            println!("If you do not specify the config Options, you must enable at least one protocol from RTSP and RTMP.");
            return Ok(());
        }

        let rtmp_port = match rtmp_port_o {
            Some(val) => *val,
            None => 0,
        };

        let hls_port = match matches.get_one::<usize>("hls") {
            Some(val) => *val,
            None => 0,
        };
        let log_level = match matches.get_one::<String>("log") {
            Some(val) => val.clone(),
            None => String::from("info"),
        };

        Config::new(
            rtmp_port,
            hls_port,
            log_level,
        )
    };

    /*set log level*/
    let logger = if let Some(log_config_value) = &config.log {
        let (rotate, path) = if let Some(file_info) = &log_config_value.file {
            if file_info.enabled {
                (
                    Some(Rotate::from_str(&file_info.rotate).unwrap()),
                    Some(file_info.path.clone()),
                )
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };
        Logger::new(&log_config_value.level, rotate, path)?
    } else {
        Logger::new(&String::from("info"), None, None)?
    };

    /*run the service*/
    let mut service = Service::new(config);
    service.run().await?;
    
    signal::ctrl_c().await?;
    logger.stop();
    Ok(())
}
