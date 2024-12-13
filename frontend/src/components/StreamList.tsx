import { Stream } from '../types'
import { formatDistanceToNow } from 'date-fns'
import type { WalletAccount } from '@mysten/wallet-standard';

interface Props {
  streams: Stream[]
  type: 'live' | 'vod'
  onWatchStream?: (streamId: string | null | undefined, idx: string | null | undefined, account: WalletAccount | null) => void
  account?: WalletAccount | null
}

export default function StreamList({ streams, type, onWatchStream: onWatchStream, account }: Props) {
  return (
    <div className="space-y-4">
      {streams.length === 0 ? (
        <div className="text-center py-8 text-gray-500">
          No {type === 'live' ? 'active streams' : 'VOD streams'} found
        </div>
      ) : (
        streams.map((stream) => (
          <div
            key={type === 'live' ? stream.url : stream.index}
            className="bg-white rounded-lg shadow p-4 flex items-center justify-between"
          >
            <div className="flex items-center space-x-4">
              <div>
                <h3 className="font-medium text-gray-900">{stream.name}</h3>
                <p className="text-sm text-gray-500">{stream.description}</p>
                {type === 'live' && (
                  <p className="text-sm text-gray-500">RTMP push: {import.meta.env.VITE_RTMP_URL_PREFIX}{stream.url}</p>
                )}
                {type === 'live' && (
                  <p className="text-sm text-gray-500">View URL: {import.meta.env.VITE_BACKEND_URL_PREFIX}/api/query_live_m3u8?stream_name={stream.url}</p>
                )}
                <p className="text-xs text-gray-400 mt-1">
                  Started {stream.startAt !== undefined ?
                    formatDistanceToNow(new Date(Number(stream.startAt))) : formatDistanceToNow(new Date(Number(stream.uploadAt)))
                  } ago
                </p>
              </div>
            </div>
            {onWatchStream && (
              <button
                onClick={() => onWatchStream(stream.url, stream.index, account!)}
                className="px-3 py-1 text-sm rounded-md bg-red-100 text-red-600 hover:bg-red-200"
              >
                Watch Stream
              </button>
            )}
          </div>
        ))
      )}
    </div>
  )
}