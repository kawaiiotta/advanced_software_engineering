"""
This class will get data from ccxt live.
It will get the coins and markets as an input.
It will split the data accordingly and save it to the databases.
After it is done for the instance (1m) it will call the live updater.
"""

import ccxt
import time
import pandas as pd
import schedule
import gc
import datetime as datetime

from mongo_connection import get_database

class FetchTickerError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


class live_ccxt():

    def __init__(self, exchanges, markets):
        self.exchanges = exchanges
        self.markets = markets

        self.market_default = "BTC/USDT"
        self.exchange_default = "ftx"
        self.timeframe = "1m"
        self.empty_history = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

        # initialize database
        self.client = get_database()
        print(self.client)

        # will save the data to the database
        # if return is on then it will return a dataframe
        self.get_data()


    def get_data(self):

        # error check exchanges
        ccxt_exchanges = ccxt.exchanges                                                             # get the exchange list of all possible exchanges in ccxt
        self.exchanges = [x for x in self.exchanges if x in ccxt_exchanges]                         # remove all exchanges that are not in ccxt
        self.exchanges = [self.exchange_default] if len(self.exchanges) == 0 else self.exchanges    # set exchanges to exchange default if no exchange exists in ccxt list
        self.exchanges = self.exchanges[:4] if len(self.exchanges) >= 5 else self.exchanges         # make sure that only max 5 exchanges are in the list

        # for all exchanges, get all markets and find all values based on the times that need to be requested   
        for exchange_r in self.exchanges:

            # get exchange id
            exchange_id = exchange_r
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({})

            # wait for the rateLimit
            rate_limit = exchange.rateLimit / 1000
            time.sleep(rate_limit)

            # error check markets
            raw_markets = exchange.load_markets()
            marketList = [raw_markets[key]["symbol"] for key in raw_markets]                        # get the marketlist of all possible markets in this exchange 
            self.markets = [x for x in self.markets if x in marketList]                             # remove all markets that are not in this exchange
            if self.market_default in marketList:                                                   # make sure that the default exists in the exchange. Otherwise dont execute.
                self.markets = [self.market_default] if len(self.markets) == 0 else self.markets    # set markets to default if no of the markets exist in this exchange
            self.markets = self.markets[:4] if len(self.markets) >= 5 else self.markets             # make sure that only max 5 markets are in the list

            print("Fetching data from ", exchange_r)
            print("---------------------------------")


            if (exchange.has['fetchTickers']):  

                history = exchange.fetch_tickers(self.markets) # listed tickers indexed by their symbols
                # market = list(history.values())[0]
                # print(market)
                for market in list(history.values()):
                    content = [market["timestamp"], market["open"], market["high"], market["low"], market["close"], market["info"]["volume"]]
                    df = pd.DataFrame([content], columns=["time", "open", "high", "low", "close", "volume"])
                    df["updated_time"] = [datetime.datetime.fromtimestamp(float(time)/1000) for time in df["time"]]
                    df.insert(0, "market", market["symbol"], True)
                    df.insert(0, "exchange", str(exchange), True)

                    # save to database based on market["symbol"]
                    json_data = df.to_dict("records")
                    self.client["crypto_data"][f"raw {str(exchange)} {market['symbol']}"].insert_many(json_data)

                    # for debugging
                    print(df)

            else:           # return error
                raise FetchTickerError("No fetch tickers")


# huge issue with data input streams. Double data or even triple data while skipping minutes
if __name__ == "__main__":

    # call the class every minute
    def job():
        start = time.time()
        # ftx is very buggy
        print(live_ccxt(exchanges=["binance"],
                            markets=["ETH/USDT", "BTC/USDT", "XRP/USDT", "ADA/USDT"]))

        gc.collect()
        print(time.time() - start)

    while True:
        curDateTime = datetime.datetime.now()
        if curDateTime.minute % 1 == 0 and curDateTime.second < 3:
            job()
            time.sleep(3)
