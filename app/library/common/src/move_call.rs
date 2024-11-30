use {
    crate::{errors::{SuiError, SuiErrorValue}, sui_utils::setup_for_write}, serde::Deserialize, shared_crypto::intent::Intent, std::time::{Duration, SystemTime}, sui_config::{sui_config_dir, SUI_KEYSTORE_FILENAME}, sui_keys::keystore::{AccountKeystore, FileBasedKeystore}, sui_sdk::{
        rpc_types::{SuiObjectDataOptions, SuiObjectResponseQuery, SuiTransactionBlockResponseOptions},
        types::{
            base_types::{ObjectID, SequenceNumber}, programmable_transaction_builder::ProgrammableTransactionBuilder, quorum_driver_types::ExecuteTransactionRequestType, transaction::{
            Argument, CallArg, Command, ObjectArg, ProgrammableMoveCall, Transaction, TransactionData
        }, Identifier
        },        
    }
};

const CONTRACT_PACKAGE: &str = "0x8f50dd1f7112da0d7b9260db347b79c6cd2bdb1da3737ff79601bdb958322e70";
const STREAMER_ADDR: &str = "0xf55e4d801568a13b69c699bdb31f1860737a4bfa0c8b7f4b4597764d4137c0a2";
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
    pub start_at: String,
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
    let streamer_id: ObjectID = STREAMER_ADDR.parse().map_err(|_| SuiError{value: SuiErrorValue::ParseError})?;
    let streamer_obj = sui_client.read_api().get_object_with_options(streamer_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let streamer_input = ptb.input(CallArg::Object(ObjectArg::ImmOrOwnedObject((streamer_obj.object_id, streamer_obj.version, streamer_obj.digest)))).unwrap();

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
        vec![streamer_input, clock_input, live_url, m3u8],
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
    print!("Executing the transaction...");
    let transaction_response = sui
        .quorum_driver_api()
        .execute_transaction_block(
            Transaction::from_data(tx_data, vec![signature]),
            SuiTransactionBlockResponseOptions::full_content(),
            Some(ExecuteTransactionRequestType::WaitForLocalExecution),
        )
        .await.map_err(|_| SuiError{value: SuiErrorValue::TransactionBlockExecuteError})?;

    let span = SystemTime::now().duration_since(now).unwrap().as_secs();
    log::info!("{}", transaction_response);
    log::info!("seconds: {}", span);
    log::info!("{}", "-------------------------------");

    Ok(())
}

pub async fn get_playlist(path_url: String) -> Result<String, SuiError> {
    let mut ptb = ProgrammableTransactionBuilder::new();

    // bucketsRoot
    let sui_client = sui_sdk::SuiClientBuilder::default().build_testnet().await.unwrap();
    let object_id: ObjectID = STREAMER_ADDR.parse().unwrap();
    let obj = sui_client.read_api().get_object_with_options(object_id, SuiObjectDataOptions::bcs_lossless()).await.unwrap().data.unwrap();
    let arg0 = CallArg::Object(ObjectArg::ImmOrOwnedObject((obj.object_id, obj.version, obj.digest)));
    ptb.input(arg0).map_err(|e| SuiError{value: SuiErrorValue::PTBInputError})?;

    let arg1 = CallArg::Pure(bcs::to_bytes(&path_url).unwrap());
    ptb.input(arg1).map_err(|e| SuiError{value: SuiErrorValue::PTBInputError})?;

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
    let (sui, sender, _recipient) = setup_for_write().await
                                .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;

    // we need to find the coin we will use as gas
    let coins = sui
        .coin_read_api()
        .get_coins(sender, None, None, None).await
        .map_err(|_| SuiError{value: SuiErrorValue::GetSuiCoinError})?;
    let coin = coins.data.into_iter().next().unwrap();

    let gas_budget = 10_000_000;
    let gas_price = sui.read_api().get_reference_gas_price().await.map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?;

    // create the transaction data that will be sent to the network
    let tx_data = TransactionData::new_programmable(
        sender.clone(),
        vec![coin.object_ref()],
        builder,
        gas_budget,
        gas_price,
    );

    // sign transaction
    let keystore = FileBasedKeystore::new(&sui_config_dir().map_err(|_| SuiError{value: SuiErrorValue::SuiRPCError})?
                                .join(SUI_KEYSTORE_FILENAME)).map_err(|_| SuiError{value: SuiErrorValue::FileKeyStoreError})?;
    let signature = keystore.sign_secure(&sender, &tx_data, Intent::sui_transaction())
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

    let v = &transaction_response.events.unwrap().data;
    let res: LiveM3u8Result = serde_json::from_str(v[0].parsed_json.to_string().as_str())
            .map_err(|_| SuiError{value: SuiErrorValue::SetupSuiClientError})?;

    println!("{}", res.data.m3u8_content);

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
}
