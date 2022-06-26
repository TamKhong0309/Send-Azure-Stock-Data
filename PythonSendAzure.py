import pandas as pd 
import json
from yahoo_fin import stock_info as si 
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.eventhub.excecptions import EventHubError
import asyncio
from datetime import datetime

connection_str ='Endpoint=sb://stockfeedatademo.servicebus.windows.net/;SharedAccessKeyName=RootUserAccessPolicy;SharedAccessKey=Cu9a4pliXD7cwOkEh765XxNXtF5xSxXsu+O0xrQ2BIU=;EntityPath=pythonfeeddata'
eventhub_name = 'pythonfeeddata'

test = si.get_quote_data('msft')
test_df = pd.DataFrame([test], columns=test.keys())[['regularMarketTime', 'regularMarketPrice','marketCap', 'exchange', 'averageAnalystRating']]

datetime.fromtimestamp(test['regularMarketTime'])
test_df['averageAnalystRating'].str.split(' - ', 1,expand=True)
test_df.apply(lamda row: '$' +  str(round(test_df[marketCap]/1000000000,2)) + 'MM', axis=1)

def get_data_for_stock(stock):
    stock_pull = si.get_quote_data(stock)
    stock_dataframe = pd.DataFrame([stock_pull], columns=stock_pull.keys())[['regularMarketTime', 'regularMarketPrice','marketCap', 'exchange', 'averageAnalystRating']]

    stock_dataframe['regularMarketTime'] = datetime.fromtimestamp(stock_dataframe['regularMarketTime'])
    stock_dataframe['regularMarketTime'] = stock_dataframe['regularMarketTime'].astype(str)
    
    stock_dataframe['AnalystRating', 'AnalystBuySell'] = stock_dataframe['averageAnalystRating'].str.split(' - ', 1, expand=True)
    
    stock_dataframe.drop('averageAnalystRating', axis=1, inplace=True)

    stock_dataframe['MarketCapInTrill$$'] = stock_dataframe.apply(lamda row: '$' +  str(round(stock_dataframe[marketCap]/1000000000,2)) + 'MM', axis=1)

    return stock_dataframe.to_dict('record')

datetime.now()
get_data_for_stock('msft')

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    while True:
        await asyncio.sleep(5)
        producer = EventHubProducerClient.from_connection_string(conn_str="connection_string", eventhub_name="eventhub_name")
        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData('json.dumps(get_data_for_stock('msft'))))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            print('Success sent to Azure Event Hubs')

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(run())
    loop.run_forever()
except KeyboardInterrupt():
    pass
finally:
    print('ClosingLoopNow')
    loop.close()