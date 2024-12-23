import pandas as pd
import numpy as np
import math
import asyncio
import websockets
import time
import json

def is_ws_connected(ws):
    return ws.recv() is not None

async def call_api(msg):
   async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
       await websocket.send(msg)
       if is_ws_connected(websocket):
           response = await websocket.recv()
           return response

def get_data(instrument = 'ETH-PERPETUAL', resolution = 60, length_of_data = 40000):
    
    lookback = 4999

    current_timestamp = math.floor(time.time()*1000)
    past_timestamp = current_timestamp - (lookback*60*resolution*1000)

    msg = {
        "jsonrpc" : "2.0",
        "id" : 1,
        "method" : "public/get_tradingview_chart_data",
        "params" : {
            "instrument_name" : instrument,
            "start_timestamp" : past_timestamp,
            "end_timestamp" : current_timestamp,
            "resolution" : str(resolution)
    }
    }
    response = json.loads(asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg))))
    df = pd.DataFrame(response['result'])

    while len(df) < length_of_data:
        current_timestamp = past_timestamp - (60*resolution*1000)
        past_timestamp = current_timestamp - (lookback*60*resolution*1000)
        msg = {
            "jsonrpc" : "2.0",
            "id" : 1,
            "method" : "public/get_tradingview_chart_data",
            "params" : {
                "instrument_name" : instrument,
                "start_timestamp" : past_timestamp,
                "end_timestamp" : current_timestamp,
                "resolution" : str(resolution)
        }
        }

        response = json.loads(asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg))))
        
        if 'result' in response.keys():
            df2 = pd.DataFrame(response['result'])
            df = pd.concat([df2,df], ignore_index=True)

    return df

def get_data_current(instrument, resolution = 60, lookback = 500):

    current_timestamp = math.floor(time.time()*1000)
    past_timestamp = current_timestamp - (lookback*3600*1000)

    msg = {
        "jsonrpc" : "2.0",
        "id" : 1,
        "method" : "public/get_tradingview_chart_data",
        "params" : {
            "instrument_name" : instrument,
            "start_timestamp" : past_timestamp,
            "end_timestamp" : current_timestamp,
            "resolution" : str(resolution)
        }
        }
    
    response = json.loads(asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg))))

    if 'result' in response.keys():
        df = pd.DataFrame(response['result'])

    return df

if __name__ == "__main__":

    df = get_data(instrument='BTC-PERPETUAL', resolution=60, length_of_data=50000)
    df.to_csv('BTC-PERP_60min.csv')
    print(df.head())
    print(df.tail())