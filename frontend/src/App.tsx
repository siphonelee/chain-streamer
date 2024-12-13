import { useState, useEffect } from 'react'
import { Tab } from '@headlessui/react'
import { Plus } from 'lucide-react'
import CreateStreamModal from './components/CreateStreamModal'
import StreamList from './components/StreamList'
import { Stream } from './types'
import { ConnectButton, useCurrentAccount, useSuiClient } from '@mysten/dapp-kit'
import type { WalletAccount } from '@mysten/wallet-standard';
import { Transaction } from "@mysten/sui/transactions";
import { NAVISDKClient } from 'navi-sdk'
import {Sui} from 'navi-sdk/dist/address';
import {SignAndSubmitTXB } from 'navi-sdk/dist/libs/PTB'
import toast, { Toaster } from 'react-hot-toast';

import './index.css'

const GET_ALL_STREAMS_TARGET = '0x631274a289104633260905535e8a26903fd44026fe313ea1c96e55ff83cef5fc::streamer::get_all_streams';
const StreamerObjectId = '0xfac88744d3c6b359d21fad3aa20f0aa81cca9fdaee25b10d2ffac62a989f8785';
const BackendPrefix = 'http://127.0.0.1:8000';

function App() {
  const [lastFetch, setLastFetch] = useState<number>(0);
  const [writing, setWriting] = useState(false);
  const [isDisabled, setIsDisabled] = useState(false);

  const account = useCurrentAccount();

  const getAllStreams = () => {
    if (!account) {
      return;
    }
    let now = Date.now();
    if (writing || (Date.now() - lastFetch) < 3000) {
        return;
    }
    setLastFetch(now);

    const client = new NAVISDKClient({mnemonic: import.meta.env.VITE_MNEMONIC, networkType: "testnet", wordLength: 12});

    let acc = client.accounts[0];
    for (let i =0; i < client.accounts.length; i++) {
      if (client.accounts[i].address !== account.address) {
        acc = client.accounts[i];
        break;
      }
    }
    
    const sender = acc.address;
    const suiClient = useSuiClient();

    let txb = new Transaction();
    txb.setSender(sender);
    txb.setGasBudget(0.5 * 10**Sui.decimal);

    try {        
      txb.moveCall({
        target: GET_ALL_STREAMS_TARGET,
        arguments: [
          txb.object(StreamerObjectId),
        ],
        typeArguments: [],
      });

      SignAndSubmitTXB(txb, acc.client, acc.keypair).then(result => {  
        if (result.effects.status.status === "failure") {
          toast(result.effects.status.error);
        } else {
          suiClient.queryEvents({
              query:{Transaction: result.digest}
          }).then(ev => {
            const events = JSON.stringify(ev.data[0].parsedJson);
            const result = JSON.parse(events)['data'];

            let streams: Stream[] = [];
            let len = result.live_streams.contents?.length;
            for (let i = 0; i < len; ++i) {
              let ls = result.live_streams.contents[i];
              let s: Stream = {
                url: ls.key,
                name: ls.value['name'],
                description: ls.value['desc'],
                startAt: ls.value['start_at'],
                lastUpdateAt: ls.value['last_update_at'],
                m3u8_content: ls.value['m3u8_content'],
              };
              streams.push(s); 
            }

            console.log(result.vod_streams);

            len = result.vod_streams?.length;
            for (let i = 0; i < len; ++i) {
              let vs = result.vod_streams[i];
              let s: Stream = {
                url: "",
                name: vs['name'],

                description: vs['desc'],
                uploadAt: vs['upload_at'],
                m3u8_content: vs['m3u8_content'],
              };
              streams.push(s); 
            }
            setStreams(streams);
          })
        }
      });
    } catch (error) { 
      // Handle the error
      console.error(error);
    }
  }

  const [streams, setStreams] = useState<Stream[]>();
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)

  getAllStreams();  

  useEffect(() => {
    // localStorage.setItem('streams', JSON.stringify(streams))
  }, [streams])

  const handleCreateStream = async (streamData: Omit<Stream, 'startAt' | 'lastUpdateAt' | 'uploadAt' | 'm3u8_content'>, 
                            acc: WalletAccount | null) => {
    if (!acc) {
      alert("Please connect your wallet first");
      return;
    }
    setIsDisabled(true);

    const backend_url = import.meta.env.VITE_BACKEND_URL_PREFIX + "/api/create_live_stream";

    fetch(backend_url, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        url: streamData.url,
        name: streamData.name,
        description: streamData.description,
      })
    })
    .then(response => response.json())
    .then(response => {
        if (response['data'] === "OK") {
          toast.success("Success!", {
            style: {
              maxWidth: 300
            }
          });
        } else {
          toast.error(response['data']);
        }
        setWriting(false);
        setIsDisabled(false);
    })
    .catch(error =>{
      toast.error(error);
      setWriting(false);
      setIsDisabled(false);
    });

    
/*
    const client = new NAVISDKClient({mnemonic: import.meta.env.VITE_MNEMONIC, networkType: "testnet", wordLength: 12});
    for (let i =0; i < client.accounts.length; i++) {
      if (client.accounts[i].address === acc.address) {
        let account = client.accounts[i];
        const sender = account.address;

        let txb = new Transaction();
        txb.setSender(sender);
        txb.setGasBudget(0.5 * 10**Sui.decimal);
    
        try {        
          txb.moveCall({
            target: CREATE_STREAM_TARGET,
            arguments: [
              txb.object(StreamerObjectId),
              txb.object('0x6'),
              txb.pure.string(streamData.url), 
              txb.pure.string(streamData.name), 
              txb.pure.string(streamData.description), 
            ],
            typeArguments: [],
          });
    
          try {
            let result = await SignAndSubmitTXB(txb, account.client, account.keypair);
            // console.log(result.effects.status);
     
            if (result.effects.status.status === "failure") {              
              toast(result.effects.status.error);
            } else {
              toast.success("Success!", {
                style: {
                  maxWidth: 300
                }
              })
            }
            setWriting(false);
            setIsDisabled(false);
          } catch(err) {
            setWriting(false);
            toast.error((err as Error).message);
            setIsDisabled(false);
          };
        } catch (error) { 
          // Handle the error
          setWriting(false);
          setIsDisabled(false);
          console.error(error);
          toast((error as Error).message);
        }

        break;
      }
    }    
*/    
  }

  const watchStream = (url: string | null | undefined, idx: string | null | undefined, acc: WalletAccount | null) => {
    if (!acc) {
      alert("Please connect your wallet first");
      return;
    }

    let goto_url = '';
    if (!!idx) {
      goto_url = "/hls/index.html?view=" + encodeURIComponent(BackendPrefix + "/api/query_vod_m3u8?stream_index=" + idx);
    } else if (!!url) {
      goto_url = "/hls/index.html?view=" + encodeURIComponent(BackendPrefix + "/api/query_live_m3u8?stream_name=" + url);
    }

    const newWindow = window.open(goto_url, '_blank', 'noopener,noreferrer');
    if (newWindow) newWindow.opener = null;
  }

  const liveStreams = streams ? streams.filter((stream) => stream.url !== '') : []; //calvin .filter((stream) => stream.status === 'live')
  const pastStreams = streams ? streams.filter((stream) => stream.url === '') : []; // .filter((stream) => stream.status === 'ended')

  const sortedLiveStreams = liveStreams.sort((s1 , s2) => { return Number(s2.startAt) - Number(s1.startAt); });

  let idx = 0;
  pastStreams.forEach((item) => {
    item.index = idx.toString();
    ++idx;
  });

  const sortedPastStreams = pastStreams.reverse();

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-4xl mx-auto px-4 py-8">
        <div className="flex items-center justify-between mb-8">
          <h1 className="text-2xl font-bold text-gray-900">ChainStreamer Hub</h1>

          <button
            onClick={() => {
              setIsCreateModalOpen(true);
              setWriting(true);
            }}
            className="flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-500"
          >
            <Plus size={20} />
            New Stream
          </button>
          <ConnectButton connectText="Connect Wallet" />
        </div>

        <Tab.Group>
          <Tab.List className="flex space-x-1 rounded-lg bg-white p-1 shadow mb-6">
            <Tab
              className={({ selected }) =>
                `w-full rounded-lg py-2.5 text-sm font-medium leading-5
                ${
                  selected
                    ? 'bg-indigo-600 text-white shadow'
                    : 'text-gray-700 hover:bg-gray-100'
                }
              `}
            >
              Live Streams ({sortedLiveStreams?.length})
            </Tab>
            <Tab
              className={({ selected }) =>
                `w-full rounded-lg py-2.5 text-sm font-medium leading-5
                ${
                  selected
                    ? 'bg-indigo-600 text-white shadow'
                    : 'text-gray-700 hover:bg-gray-100'
                }
              `}
            >
              VOD Streams ({sortedPastStreams?.length})
            </Tab>
          </Tab.List>
          <Tab.Panels>
            <Tab.Panel>
              <StreamList
                streams={sortedLiveStreams}
                type="live"
                onWatchStream={watchStream}
                account={account}
              />
            </Tab.Panel>
            <Tab.Panel>
              <StreamList
                onWatchStream={watchStream}
                streams={sortedPastStreams}
                type="vod"
                account={account}
              />
            </Tab.Panel>
          </Tab.Panels>
        </Tab.Group>
      </div>

      <CreateStreamModal
        isOpen={isCreateModalOpen}
        onClose={() => {
          setIsCreateModalOpen(false);
          setWriting(false);
        }}
        onCreateStream={handleCreateStream}
        account={account}
        isDisabled={isDisabled}
      />

      <Toaster
        containerStyle={{
          position: 'absolute',
        }}
        toastOptions={{
          className: '',
          style: {
            maxWidth: 1000,
            border: '1px solid #713200',
            padding: '16px',
            color: '#713200',
          },
        }}
      />      
    </div>
  )
}

export default App