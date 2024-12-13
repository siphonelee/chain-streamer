use {
    crate::{errors::{SuiError, SuiErrorValue}, sui_utils::setup_for_write}, serde::Deserialize, shared_crypto::intent::Intent, std::time::{Duration, SystemTime}, sui_config::{sui_config_dir, SUI_KEYSTORE_FILENAME}, sui_keys::keystore::{AccountKeystore, FileBasedKeystore}, sui_sdk::{
        rpc_types::{SuiObjectDataOptions, SuiObjectResponseQuery, SuiTransactionBlockResponseOptions, SuiTransactionBlockEffects, SuiExecutionStatus},
        types::{
            base_types::{ObjectID, SequenceNumber}, programmable_transaction_builder::ProgrammableTransactionBuilder, quorum_driver_types::ExecuteTransactionRequestType, transaction::{
            Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, Transaction, TransactionData
        }, Identifier
        },        
    }
};

const CONTRACT_PACKAGE: &str = "0x631274a289104633260905535e8a26903fd44026fe313ea1c96e55ff83cef5fc";
const ADMIN_CAP: &str = "0xeeff0b099111189d01fd5307548a8467488fdb68c7f30b0d4774e5f2d9f6eb7b";
const STREAMER_ADDR: &str = "0xfac88744d3c6b359d21fad3aa20f0aa81cca9fdaee25b10d2ffac62a989f8785";
const CLOCK_OBJ_ID: &str = "0x0000000000000000000000000000000000000000000000000000000000000006";
const AGGREGATOR_URL_PREFIX: &str = "https://aggregator.walrus-testnet.walrus.space/v1/";

#[derive(Deserialize, Debug)]
pub struct LiveM3u8Result {
    pub data: LiveM3u8Info,
}

#[derive(Deserialize, Debug)]
pub struct LiveM3u8Info {
    pub last_update_at: String,
    pub m3u8_content: String,
    pub name: String,
    pub desc: String,
    pub start_at: String,
}

#[derive(Deserialize, Debug)]
pub struct VodM3u8Result {
    pub data: VodM3u8Info,
}

#[derive(Deserialize, Debug)]
pub struct VodM3u8Info {
    pub name: String,
    pub desc: String,
    pub upload_at: String,
    pub m3u8_content: String,
}


pub async fn upload_playlist_to_contract(url_path: String, m3u8_content: &String) -> Result<(), SuiError> {
    let now = SystemTime::now();

    // 1) get the Sui client, the sender and recipient that we will use
    // for the transaction, and find the coin we use as gas       
    let (sui, sender, _recipient) = setup_for_write().await
                                .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;

    // we need to find the coin we will use as gas
    let coins = sui
        .coin_read_api()
        .get_coins(sender, None, None, None).await
        .map_err(|_| SuiError{value: SuiErrorValue::GetSuiCoinError})?;
    let coin = coins.data.into_iter().next().unwrap();

     // 2) create a programmable transaction builder to add commands and create a PTB
    let mut ptb = ProgrammableTransactionBuilder::new();

    // Create Argument::Input
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();

    let admin_cap_id: ObjectID = ADMIN_CAP.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let admin_cap_obj = sui_client.read_api().get_object_with_options(admin_cap_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let admin_cap_input = ptb.input(CallArg::Object(ObjectArg::ImmOrOwnedObject((admin_cap_obj.object_id, admin_cap_obj.version, admin_cap_obj.digest)))).unwrap();

    let streamer_id: ObjectID = STREAMER_ADDR.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let streamer_input = ptb.obj(ObjectArg::SharedObject {
        id: streamer_id,
        initial_shared_version: SequenceNumber::from(206208636),
        mutable: true,
    }).map_err(|_| SuiError{value: SuiErrorValue::PTBObjError})?;
    
    let clock_id: ObjectID = CLOCK_OBJ_ID.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let clock_input = ptb.obj(ObjectArg::SharedObject {
        id: clock_id,
        initial_shared_version: SequenceNumber::from(1),
        mutable: false,
    }).map_err(|_| SuiError{value: SuiErrorValue::PTBObjError})?;

    let mut path = url_path;
    if path.starts_with(".") {
        path = path.as_str()[1..].to_owned();
    }
    let live_url = ptb.input(CallArg::Pure(bcs::to_bytes(&path).unwrap())).unwrap();
    let m3u8 = ptb.input(CallArg::Pure(bcs::to_bytes(m3u8_content).unwrap())).unwrap();

    // 3) add a move call to the PTB
    // Replace the pkg_id with the package id you want to call
    let package = ObjectID::from_hex_literal(CONTRACT_PACKAGE).map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let module = Identifier::new("streamer").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let function = Identifier::new("update_live_stream").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    ptb.command(Command::move_call(
        package,
        module,
        function,
        vec![],
        vec![admin_cap_input, streamer_input, clock_input, live_url, m3u8],
    ));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();

    let gas_budget = 10_000_000;
    let gas_price = sui.read_api().get_reference_gas_price().await.map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?;

    // create the transaction data that will be sent to the network
    let tx_data = TransactionData::new_programmable(
        sender,
        vec![coin.object_ref()],
        builder,
        gas_budget,
        gas_price,
    );

    // 4) sign transaction
    let keystore = FileBasedKeystore::new(
        &sui_config_dir().map_err(|_| SuiError{value: SuiErrorValue::SuiConfigError})?.join(SUI_KEYSTORE_FILENAME))
            .map_err(|_| SuiError{value: SuiErrorValue::FileKeyStoreError})?;
    let signature = keystore.sign_secure(&sender, &tx_data, Intent::sui_transaction())
                            .map_err(|_| SuiError{value: SuiErrorValue::TransactionSignError})?;

    // 5) execute the transaction
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            Transaction::from_data(tx_data, vec![signature]),
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await.map_err(|_| SuiError{value: SuiErrorValue::TransactionBlockExecuteError})?;
    log::info!("{}", transaction_response);

    let res = match transaction_response.effects.unwrap() {
        SuiTransactionBlockEffects::V1(t) => {
            match t.status {
                SuiExecutionStatus::Success => Ok(()),
                SuiExecutionStatus::Failure {error: e} => {
                    log::error!("contract error: {}", e);
                    return Err(SuiError{value: SuiErrorValue::TransactionBlockExecuteError});
                }
            }
        }
    };
            
    let span = SystemTime::now().duration_since(now).unwrap().as_secs();
    log::info!("seconds: {}", span);
    log::info!("{}", "-------------------------------");

    res
}

pub async fn get_live_playlist(path_url: String) -> Result<String, SuiError> {
    let mut ptb = ProgrammableTransactionBuilder::new();
    
    let streamer_id: ObjectID = STREAMER_ADDR.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let arg0 = CallArg::Object(ObjectArg::SharedObject {
        id: streamer_id,
        initial_shared_version: SequenceNumber::from(206208636),
        mutable: true,
    });    
    ptb.input(arg0).map_err(|_| SuiError{value: SuiErrorValue::PTBInputError})?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&path_url).unwrap());
    ptb.input(arg1).map_err(|_| SuiError{value: SuiErrorValue::PTBInputError})?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(CONTRACT_PACKAGE).map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let module = Identifier::new("streamer").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let function = Identifier::new("get_live_stream").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    ptb.command(Command::move_call(
        package,
        module,
        function,
        vec![],
        vec![Argument::Input(0), Argument::Input(1)],
    ));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();

    // get the Sui client, the sender and recipient that we will use
    // for the transaction, and find the coin we use as gas       
    let (sui, sender, recipient) = setup_for_write().await
                                .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;

    // we need to find the coin we will use as gas
    let coins = sui
        .coin_read_api()
        .get_coins(recipient, None, None, None).await
        .map_err(|_| SuiError{value: SuiErrorValue::GetSuiCoinError})?;
    let coin = coins.data.into_iter().next().unwrap();

    let gas_budget = 10_000_000;
    let gas_price = sui.read_api().get_reference_gas_price().await.map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?;

    // create the transaction data that will be sent to the network
    let tx_data = TransactionData::new_programmable(
        recipient.clone(),
        vec![coin.object_ref()],
        builder,
        gas_budget,
        gas_price,
    );

    // sign transaction
    let keystore = FileBasedKeystore::new(&sui_config_dir().map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?
                                .join(SUI_KEYSTORE_FILENAME)).map_err(|_| SuiError{value: SuiErrorValue::FileKeyStoreError})?;
    let signature = keystore.sign_secure(&recipient, &tx_data, Intent::sui_transaction())
                                .map_err(|_| SuiError{value: SuiErrorValue::TransactionSignError})?;
    // execute the transaction
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            Transaction::from_data(tx_data, vec![signature]),
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await.map_err(|_| SuiError{value: SuiErrorValue::TransactionBlockExecuteError})?;

    let res = match transaction_response.effects.unwrap() {
        SuiTransactionBlockEffects::V1(t) => {
            match t.status {
                SuiExecutionStatus::Success => {
                    let v = &transaction_response.events.unwrap().data;    
                    let res: LiveM3u8Result = serde_json::from_str(v[0].parsed_json.to_string().as_str())
                            .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;
                    log::info!("{}", res.data.m3u8_content);
                
                    // concat with aggregator url
                    let mut ret: String = String::new();
                    for line in res.data.m3u8_content.split("\n") {
                        if !line.starts_with("#") && line.len() > 40 {
                            ret.push_str(AGGREGATOR_URL_PREFIX);
                        }
                        ret.push_str(line);
                        ret.push('\n');
                    }
                    Ok(ret)
                },
                SuiExecutionStatus::Failure {error: e} => {
                    log::error!("contract error: {}", e);
                    Err(SuiError{value: SuiErrorValue::TransactionBlockExecuteError})
                }
            }
        }
    };
    
    res
}

pub async fn live_to_vod(url_path: String, m3u8_full_content: &String) -> Result<(), SuiError> {
    let now = SystemTime::now();

    // 1) get the Sui client, the sender and recipient that we will use
    // for the transaction, and find the coin we use as gas       
    let (sui, sender, _recipient) = setup_for_write().await
                                .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;

    // we need to find the coin we will use as gas
    let coins = sui
        .coin_read_api()
        .get_coins(sender, None, None, None).await
        .map_err(|_| SuiError{value: SuiErrorValue::GetSuiCoinError})?;
    let coin = coins.data.into_iter().next().unwrap();

     // 2) create a programmable transaction builder to add commands and create a PTB
    let mut ptb = ProgrammableTransactionBuilder::new();

    // Create Argument::Input
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();

    let admin_cap_id: ObjectID = ADMIN_CAP.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let admin_cap_obj = sui_client.read_api().get_object_with_options(admin_cap_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let admin_cap_input = ptb.input(CallArg::Object(ObjectArg::ImmOrOwnedObject((admin_cap_obj.object_id, admin_cap_obj.version, admin_cap_obj.digest)))).unwrap();

    let streamer_id: ObjectID = STREAMER_ADDR.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let streamer_input = ptb.obj(ObjectArg::SharedObject {
        id: streamer_id,
        initial_shared_version: SequenceNumber::from(206208636),
        mutable: true,
    }).map_err(|_| SuiError{value: SuiErrorValue::PTBObjError})?;

    let clock_id: ObjectID = CLOCK_OBJ_ID.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let clock_input = ptb.obj(ObjectArg::SharedObject {
        id: clock_id,
        initial_shared_version: SequenceNumber::from(1),
        mutable: false,
    }).map_err(|_| SuiError{value: SuiErrorValue::PTBObjError})?;

    let mut path = url_path;
    if path.starts_with(".") {
        path = path.as_str()[1..].to_owned();
    }
    let live_url = ptb.input(CallArg::Pure(bcs::to_bytes(&path).unwrap())).unwrap();
    let full_m3u8 = ptb.input(CallArg::Pure(bcs::to_bytes(m3u8_full_content).unwrap())).unwrap();

    // 3) add a move call to the PTB
    // Replace the pkg_id with the package id you want to call
    let package = ObjectID::from_hex_literal(CONTRACT_PACKAGE).map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let module = Identifier::new("streamer").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let function = Identifier::new("move_live_stream_to_vod_stream").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    ptb.command(Command::move_call(
        package,
        module,
        function,
        vec![],
        vec![admin_cap_input, streamer_input, clock_input, live_url, full_m3u8],
    ));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();

    let gas_budget = 10_000_000;
    let gas_price = sui.read_api().get_reference_gas_price().await.map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?;

    // create the transaction data that will be sent to the network
    let tx_data = TransactionData::new_programmable(
        sender,
        vec![coin.object_ref()],
        builder,
        gas_budget,
        gas_price,
    );

    // 4) sign transaction
    let keystore = FileBasedKeystore::new(
        &sui_config_dir().map_err(|_| SuiError{value: SuiErrorValue::SuiConfigError})?.join(SUI_KEYSTORE_FILENAME))
            .map_err(|_| SuiError{value: SuiErrorValue::FileKeyStoreError})?;
    let signature = keystore.sign_secure(&sender, &tx_data, Intent::sui_transaction())
                            .map_err(|_| SuiError{value: SuiErrorValue::TransactionSignError})?;

    // 5) execute the transaction
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            Transaction::from_data(tx_data, vec![signature]),
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await.map_err(|_| SuiError{value: SuiErrorValue::TransactionBlockExecuteError})?;
    log::info!("{}", transaction_response);

    let res = match transaction_response.effects.unwrap() {
        SuiTransactionBlockEffects::V1(t) => {
            match t.status {
                SuiExecutionStatus::Success => Ok(()),
                SuiExecutionStatus::Failure {error: e} => {
                    log::error!("contract error: {}", e);
                    Err(SuiError{value: SuiErrorValue::TransactionBlockExecuteError})
                }
            }
        }
    };
    
    let span = SystemTime::now().duration_since(now).unwrap().as_secs();
    log::info!("seconds: {}", span);
    log::info!("{}", "-------------------------------");

    res
}

pub async fn create_live_stream(url: String, name: String, description: String) -> Result<(), SuiError> {
    let now = SystemTime::now();

    // 1) get the Sui client, the sender and recipient that we will use
    // for the transaction, and find the coin we use as gas       
    let (sui, sender, _recipient) = setup_for_write().await
                                .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;

    // we need to find the coin we will use as gas
    let coins = sui
        .coin_read_api()
        .get_coins(sender, None, None, None).await
        .map_err(|_| SuiError{value: SuiErrorValue::GetSuiCoinError})?;
    let coin = coins.data.into_iter().next().unwrap();

     // 2) create a programmable transaction builder to add commands and create a PTB
    let mut ptb = ProgrammableTransactionBuilder::new();

    // Create Argument::Input
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();

    let admin_cap_id: ObjectID = ADMIN_CAP.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let admin_cap_obj = sui_client.read_api().get_object_with_options(admin_cap_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let admin_cap_input = ptb.input(CallArg::Object(ObjectArg::ImmOrOwnedObject((admin_cap_obj.object_id, admin_cap_obj.version, admin_cap_obj.digest)))).unwrap();

    let streamer_id: ObjectID = STREAMER_ADDR.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let streamer_input = ptb.obj(ObjectArg::SharedObject {
        id: streamer_id,
        initial_shared_version: SequenceNumber::from(206208636),
        mutable: true,
    }).map_err(|_| SuiError{value: SuiErrorValue::PTBObjError})?;

    let clock_id: ObjectID = CLOCK_OBJ_ID.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let clock_input = ptb.obj(ObjectArg::SharedObject {
        id: clock_id,
        initial_shared_version: SequenceNumber::from(1),
        mutable: false,
    }).map_err(|_| SuiError{value: SuiErrorValue::PTBObjError})?;

    let mut path = url;
    if path.starts_with(".") {
        path = path.as_str()[1..].to_owned();
    }
    let live_url = ptb.input(CallArg::Pure(bcs::to_bytes(&path).unwrap())).unwrap();
    let name = ptb.input(CallArg::Pure(bcs::to_bytes(&name).unwrap())).unwrap();
    let description = ptb.input(CallArg::Pure(bcs::to_bytes(&description).unwrap())).unwrap();

    // 3) add a move call to the PTB
    // Replace the pkg_id with the package id you want to call
    let package = ObjectID::from_hex_literal(CONTRACT_PACKAGE).map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let module = Identifier::new("streamer").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let function = Identifier::new("create_live_stream").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    ptb.command(Command::move_call(
        package,
        module,
        function,
        vec![],
        vec![admin_cap_input, streamer_input, clock_input, live_url, name, description],
    ));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();

    let gas_budget = 10_000_000;
    let gas_price = sui.read_api().get_reference_gas_price().await.map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?;

    // create the transaction data that will be sent to the network
    let tx_data = TransactionData::new_programmable(
        sender,
        vec![coin.object_ref()],
        builder,
        gas_budget,
        gas_price,
    );

    // 4) sign transaction
    let keystore = FileBasedKeystore::new(
        &sui_config_dir().map_err(|_| SuiError{value: SuiErrorValue::SuiConfigError})?.join(SUI_KEYSTORE_FILENAME))
            .map_err(|_| SuiError{value: SuiErrorValue::FileKeyStoreError})?;
    let signature = keystore.sign_secure(&sender, &tx_data, Intent::sui_transaction())
                            .map_err(|_| SuiError{value: SuiErrorValue::TransactionSignError})?;

    // 5) execute the transaction
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            Transaction::from_data(tx_data, vec![signature]),
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await.map_err(|_| SuiError{value: SuiErrorValue::TransactionBlockExecuteError})?;

    let res = match transaction_response.effects.unwrap() {
        SuiTransactionBlockEffects::V1(t) => {
            match t.status {
                SuiExecutionStatus::Success => Ok(()),
                SuiExecutionStatus::Failure {error: e} => {
                    log::error!("contract error: {}", e);
                    return Err(SuiError{value: SuiErrorValue::TransactionBlockExecuteError});
                }
            }
        }
    };

    let span = SystemTime::now().duration_since(now).unwrap().as_secs();
    log::info!("seconds: {}", span);
    log::info!("{}", "-------------------------------");

    res
}

pub async fn get_vod_playlist(index: u64) -> Result<String, SuiError> {
    let mut ptb = ProgrammableTransactionBuilder::new();
    
    let streamer_id: ObjectID = STREAMER_ADDR.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let arg0 = CallArg::Object(ObjectArg::SharedObject {
        id: streamer_id,
        initial_shared_version: SequenceNumber::from(206208636),
        mutable: true,
    });
    ptb.input(arg0).map_err(|_| SuiError{value: SuiErrorValue::PTBInputError})?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&index).unwrap());
    ptb.input(arg1).map_err(|_| SuiError{value: SuiErrorValue::PTBInputError})?;

    // add a move call to the PTB
    let package = ObjectID::from_hex_literal(CONTRACT_PACKAGE).map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let module = Identifier::new("streamer").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    let function = Identifier::new("get_vod_stream").map_err(|_| SuiError{value: SuiErrorValue::IdentifierFormatError})?;
    ptb.command(Command::move_call(
        package,
        module,
        function,
        vec![],
        vec![Argument::Input(0), Argument::Input(1)],
    ));

    // build the transaction block by calling finish on the ptb
    let builder = ptb.finish();

    // get the Sui client, the sender and recipient that we will use
    // for the transaction, and find the coin we use as gas       
    let (sui, sender, recipient) = setup_for_write().await
                                .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;

    // we need to find the coin we will use as gas
    let coins = sui
        .coin_read_api()
        .get_coins(recipient, None, None, None).await
        .map_err(|_| SuiError{value: SuiErrorValue::GetSuiCoinError})?;
    let coin = coins.data.into_iter().next().unwrap();

    let gas_budget = 10_000_000;
    let gas_price = sui.read_api().get_reference_gas_price().await.map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?;

    // create the transaction data that will be sent to the network
    let tx_data = TransactionData::new_programmable(
        recipient.clone(),
        vec![coin.object_ref()],
        builder,
        gas_budget,
        gas_price,
    );

    // sign transaction
    let keystore = FileBasedKeystore::new(&sui_config_dir().map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?
                                .join(SUI_KEYSTORE_FILENAME)).map_err(|_| SuiError{value: SuiErrorValue::FileKeyStoreError})?;
    let signature = keystore.sign_secure(&recipient, &tx_data, Intent::sui_transaction())
                                .map_err(|_| SuiError{value: SuiErrorValue::TransactionSignError})?;
    // execute the transaction
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            Transaction::from_data(tx_data, vec![signature]),
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await.map_err(|e| {
            log::error!("{}", e);
            SuiError{value: SuiErrorValue::TransactionBlockExecuteError}
        })?;
    
    let res = match transaction_response.effects.unwrap() {
        SuiTransactionBlockEffects::V1(t) => {
            match t.status {
                SuiExecutionStatus::Success => {
                    let v = &transaction_response.events.unwrap().data;    
                    let res: VodM3u8Result = serde_json::from_str(v[0].parsed_json.to_string().as_str())
                            .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;
                    log::info!("{}", res.data.m3u8_content);

                    // concat with aggregator url
                    let mut ret: String = String::new();
                    for line in res.data.m3u8_content.split("\n") {
                        if !line.starts_with("#") && line.len() > 40 {
                            ret.push_str(AGGREGATOR_URL_PREFIX);
                        }
                        ret.push_str(line);
                        ret.push('\n');
                    }

                    Ok(ret)                    
                },
                SuiExecutionStatus::Failure {error: e} => {
                    log::error!("contract error: {}", e);
                    Err(SuiError{value: SuiErrorValue::TransactionBlockExecuteError})
                }
            }
        }
    };

    res
}