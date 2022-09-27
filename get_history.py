import pandas as pd
import numpy as np
import ccxt
from pandas.core.algorithms import value_counts
from pandas.core.tools.datetimes import to_datetime
from pandas.io import excel
from requests import exceptions
import ccxt
from ccxt.base.errors import AuthenticationError, BadRequest, ExchangeError, ExchangeNotAvailable, NotSupported, RequestTimeout, NetworkError
from requests.exceptions import HTTPError, Timeout
import time
import pandas as pd
import numpy as np
from multiprocessing import Process, Queue
import datetime as datetime
import time
import calendar
import gc 

from mongo_connection import get_database


class ZeroLenError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class TimeoutError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class OHLCV_Error(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class Minute_Error(Exception):
    def __init__(self, msg):
        super().__init__(msg)


"""
This class will get the history of a certain market input in a certain timeframe 
As inputs it takes:
a list of markets of which coins to get the history from
a list of exchanges of which to try the markets and get the history from
a starttime in days that will be the amount of days from now it gets the start of the data
a length which will be the length of the dataset i.e: 1000 datapoints in minutes will be 0.69 days
a timeframe that is dependent whether the exchange allows the timeframe. "1m" is default and mostly works
a save file name to store the data in
a boolean add existing to add ontop of an existing dataset
a boolean local to get the data from local data instead of from ccxt (local will be faster, but also will have the issue of maybe not enough data)
"""
class get_history():
    def __init__(self, markets, exchanges, starttime, length, timeframe="1m", save_file_name="history.csv", add_existing="None", local=False):

        self.markets = markets
        # self.markets = ["BTC/USDT", "BTC/BUSD", "XRP/USDT", "ETH/USDT", "ETH/BUSD"]
        self.exchanges = exchanges
        # self.exchanges = ["binance", "ftx", "independentreserve", "coinbase", 'yobit']

        # initialize variables for check save file name
        self.currently_allowed_timeframe_formats = ["1m", "10m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "2d"]

        # do checks on starttime and length entry return unix variables
        self.check_start_end(starttime, length, timeframe)

        # initialize variables for check save file name
        self.currently_allowed_file_formats = [".csv", ""]
        self.default_save_file_name = "history"

        # do checks on save_file entry return self.save_file_name
        self.check_save_file_name(save_file_name)

        # initialize variables to check markets and exchanges, error checking will be conducted once called
        self.market_default = "BTC/USDT"
        self.exchange_default = "ftx"

        # initialize database
        self.client = get_database()


        self.add_existing = add_existing
        # check if "None" then dont add
        # check if empty then use same as "save_file"
        # check for name if it exists in location
        # if not search for it, if not found then dont add

        self.local = local
        # if True go through locally established database
        # if False take from file that maybe exists and or take from ccxt


    """
    check if filename is in right format
    check which file format data should be converted to
    """
    def check_save_file_name(self, save_file_name):

        has_point = "." in save_file_name
        has_content = "" != save_file_name 
        file_format = ""

        if has_point:
            file_format = save_file_name.split(".")[-1]

        match file_format:
            case "":
                file_format = "csv"
                csv = True
            case " ":
                file_format = "csv"
                csv = True
            case "csv":
                csv = True
            # from here on allow other file formats
            case "other":
                pass
            case _:
                print("Something went wrong in check_save_file_name in file format checker")
                csv = True

        if not has_content:
            save_file_name = self.default_save_file_name

        if not has_point:
            save_file_name += "."+file_format

        self.save_file_name = save_file_name


    """
    check if starttime is in days, should be between 1 and 500
    check that starttime is an interger and assumes days
    check that length is possible or cut it down to today
    default start date to now - 10 days to get at least 10 days worth of data
    convert timeframe to unixtime
    """
    def check_start_end(self, starttime, length, timeframe):
        # run checks on starttime
        starttime = int(starttime)                                              # make sure that starttime is a fullday 
        starttime = 500 if starttime > 500 or starttime < 0 else starttime      # make sure that starttime is between 0 and 500
        self.starttime = starttime
        # run checks on length  
        length = int(length)                                                    # make sure that length is a fulltime
        length = 10 if length < 0 else length                                   # make length default 10 long if its below 0

        # run checks on timeframe and convert it into minutes 
        # default to 1 minute
        match timeframe:
            case "":
                minute_timeframe = 1
                timeframe = "1m"
            case " ":
                minute_timeframe = 1
                timeframe = "1m"
            case "1m":
                minute_timeframe = 1
                timeframe = "1m"
            case "1h":
                minute_timeframe = 60
                timeframe = "1h"
            case "2h":
                minute_timeframe = 120
                timeframe = "2h"
            case "4h":
                minute_timeframe = 240
                timeframe = "4h"
            case "10m":
                minute_timeframe = 10
                timeframe = "10m"
            case "30m":
                minute_timeframe = 30
                timeframe = "30m"
            case "8h":
                minute_timeframe = 480
                timeframe = "8h"
            case "12h":
                minute_timeframe = 720
                timeframe = "12h"
            case "1d":
                minute_timeframe = 1440
                timeframe = "1d"
            case "2d":
                minute_timeframe = 2880
                timeframe = "2d"
            case _:
                minute_timeframe = 1
                timeframe = "1m"
                           
        self.timeframe = timeframe
        length_in_min = length * minute_timeframe                               # get time in minutes for enddate
        self.limit_changer = minute_timeframe                                   # get the value that will change the limit based on which timeframe has been chosen

        now = datetime.datetime.utcnow()                                        # time now
        unixtime_now_sec = calendar.timegm(now.utctimetuple())                  # time now in utc unix timestamp in seconds for better changeablitliy of time reference
        self.unixtime_now_mili = unixtime_now_sec * 1000                        # unix time in miliseconds
        self.unix_starttime = (unixtime_now_sec - 60*60*24*starttime) * 1000    # starttime in unix timestamp in ms assuming starttime is in days
      

        # run checks on possiblilty of length to calculate the end date
        unix_enddate = (unixtime_now_sec - 60*60*24*starttime + 60*length_in_min) * 1000                          # get unix enddate from starttime + length
        self.unix_enddate = unix_enddate if unix_enddate <= self.unixtime_now_mili else self.unixtime_now_mili    # make sure enddate is not further than now


    def save_file(self, dataframe):

        pass


    """
    Take an input of a list of markets. Up to 5 is okay. If more than 5 only first 5 will be taken.
    check if markets exist in ccxt
    remove markets from list if they do not exist or do not respond
    if list is empty afterwards default to FTX
    limit limit to how much the market allows to get
    limit type to what markets allow to get
    If one market in market list does not allow type fill variables accordingly
    """
    def ccxt_history(self):

        # create a parameterlist for the multiprocessing
        parameter_list = []                                                                         # parameters will be ["exchange", "market", "time", "limit", "timeframe"]

        # variables for multiprocessing
        
        # error check exchanges
        ccxt_exchanges = ccxt.exchanges                                                             # get the exchange list of all possible exchanges in ccxt
        self.exchanges = [x for x in self.exchanges if x in ccxt_exchanges]                         # remove all exchanges that are not in ccxt
        self.exchanges = [self.exchange_default] if len(self.exchanges) == 0 else self.exchanges    # set exchanges to exchange default if no exchange exists in ccxt list
        self.exchanges = self.exchanges[:4] if len(self.exchanges) >= 5 else self.exchanges         # make sure that only max 5 exchanges are in the list

        print("Fetching data from ", self.exchanges)
        
        # start timer for multiprocessing
        start_time = time.time()

        # for all exchanges, get all markets and find all values based on the times that need to be requested   
        for exchange_r in self.exchanges:

            # get exchange id
            exchange_id = exchange_r
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({})

            # wait for the rateLimit
            rate_limit = exchange.rateLimit / 1000

            # error check markets
            raw_markets = exchange.load_markets()
            marketList = [raw_markets[key]["symbol"] for key in raw_markets]                        # get the marketlist of all possible markets in this exchange 
            self.markets = [x for x in self.markets if x in marketList]                             # remove all markets that are not in this exchange
            if self.market_default in marketList:                                                   # make sure that the default exists in the exchange. Otherwise dont execute.
                self.markets = [self.market_default] if len(self.markets) == 0 else self.markets    # set markets to default if no of the markets exist in this exchange
            self.markets = self.markets[:4] if len(self.markets) >= 5 else self.markets             # make sure that only max 5 markets are in the list

            print("Fetching data from ", self.markets)
            print("---------------------------------")
            
            # for all markets in the exchange find all values based on the times that need to be requested
            for market in self.markets:
                
                # wait for the rate limit as every request_size will need to be figured out with a request
                time.sleep(rate_limit)
                # figure out the length of the returning dataframe to find the limit
                limit_out = exchange.fetch_ohlcv(symbol=market, timeframe=self.timeframe, since=self.unix_starttime, limit=None)
                limit_df = pd.DataFrame(limit_out, columns = ['time', 'open', 'high', 'low', 'close', 'volume'])
                time.sleep(rate_limit)
                self.limit = len(limit_df)                                                                  # the limit is the max that we can get from this market in this exchange
                if self.limit == 0:
                    raise ZeroLenError("Limit returned has been 0")                                         # make sure that limit is above 1
                step = self.limit * 60 * 1000 * self.limit_changer                                          # calculate for the case of hours and other variables
                times = [i for i in range(0, 1000*60*60*24*self.starttime, step)]                           # get the times cut into parts of the step
                times = [self.unixtime_now_mili-i for i in times]                                           # convert times back to unix time in miliseconds
                times.reverse()

                # for every time calculated add the parameters to the parameter_list
                count = 0
                # print(times, self.unix_enddate)
                for since in times:
                    # basically break whenever since goes above endtime apart from the very first time
                    if since >= self.unix_enddate and count != 0:
                        break
                    parameter_list.append((exchange_r, market, since, self.limit, self.timeframe))
                    count += 1
                
                print("Making", count ,"requests from ", exchange, "for", market)

        # esentially how many processes need to be calculated to fullfill the full request
        self.amount_processes = len(parameter_list)
        # print("LEN PROCESSES", self.amount_processes)

        # call multiprocessing ------>

        # create empty df with all variables that will be put
        df = pd.DataFrame(columns=["exchange", "market", "time", "open", "high", "low", "close", "volume"])

        # create queue
        queue = Queue()

        # for each timeslot in the parameter list a process will need to be started which will call a request and create a dataframe
        for i, para in enumerate(parameter_list):
            print("Number of Processess running: ", i+1, "/", self.amount_processes)    # Keep an update of how many processes have been loaded
            exchange, market, since, limit, timeframe = para                            # unravel parameters out of list
            p = Process(target=self.multiprocessing_ccxt_history,
                    args=(exchange, market, since, limit, timeframe, queue))            # create Process with parameters and queue
            p.start()                                                                   # start Process

            unsorted_df = queue.get()                                                   # get the dataframe that the multiprocessing has calculated
            df = pd.concat([df, unsorted_df])                                           # combine it with the previously created dataframe

            time.sleep(rate_limit)                                                      # wait for the rate limit each time a request is called

            # TODO here we can implement a count based on the amount of cores and thread the cpu has and how much memory the device has
            # allow only 36 processes each time and wait for them to be finished
            if i+1 % 36 == 0:
                print("---------------------------------")
                print("clearing CPU cores")
                # wait for 2 seconds for all Processes to end
                time.sleep(2)
                # terminate all current running Processes, clear all memorie and CPU Processes running
                p.terminate()

                # free up unused memorie
                gc.collect()


        # terminate all current running Processes, clear all memorie and CPU Processes running
        p.terminate()

        # free up unused memorie
        gc.collect()

        # saving processes
        # saving in this case will just dump the unsorted data into the files
        print("SAVING DATA")
        # self.save_to_pkl(df)
        # self.save_to_csv(df)
        self.save_to_database(df)

        # amount of time it took
        end_time = time.time() - start_time
        # print(end_time)

        # return dataframe it it wants to be used differently
        return df

    # mutliprocess the timeframes that are split up for easier use
    def multiprocessing_ccxt_history(self, exchange, market, since,
                                     limit, timeframe, queue):

        # create new exchange object
        exchange_id = exchange
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({})

        try:
            # initialize dataframe
            df = pd.DataFrame(columns = ["time", "open", "high", "low", "close", "volume"])
            
            # get history of market, with specified timeframe, given the start date, limit should be the limit possible for the market
            out = exchange.fetch_ohlcv(symbol=market, timeframe=timeframe, since=since, limit=limit)

            # change dataoutput stream
            df = pd.DataFrame(out, columns = ["time", "open", "high", "low", "close", "volume"])
            df["updated_time"] = [datetime.datetime.fromtimestamp(float(time)/1000) for time in df["time"]]
            df.insert(0, "market", market, True)
            df.insert(0, "exchange", exchange, True)

            if len(df) == 0:
                raise ZeroLenError("Multiprocessing returned an empty dataframe")

        # FILTERS
        # every exchange that needs an API to get data
        except AuthenticationError:
            print(f"NEEDS API_KEY AND SECRET FOR {exchange}")
        # every exchange where the connection failed
        except HTTPError:
            print(f"Bad gateway to {exchange}")
        # every exchange that maybe has been removed or stoped working all together
        except BadRequest:
            print(f"ONE OR MORE PAIRS GOT DELISTED, SKIPPED {exchange}")
        # When the exchange does not return any data
        except ZeroLenError:
            print(f"SOME ISSUE WITH {exchange}, 0 DATA RETRIEVED")
        # Exchanges that do not support ccxt or fetching all markets at the same time
        except NotSupported:
            print(f"Not Supported to fetch all markets in {exchange}")
        # Exchanges that are currently down
        except ExchangeNotAvailable:
            print(f"Exchange currently not available, skipped {exchange}")
        # Exchanges that do not support OHLCV
        except OHLCV_Error:
            print(f"{exchange} does not support OHLCV")
        # Exchanges that dont have minutes Data
        except Minute_Error:
            print(f"{exchange} does not support minute past data.")
        # Exchanges where the request timed out
        except RequestTimeout:
            print(f"Request timed out, skipped {exchange}")
        # Exchanges that took to long so that we had to stop them
        except TimeoutError:
            print(f"Time took to long with {exchange}")
        # Exchanges where the network had an error
        except NetworkError:
            print(f"Network error in {exchange}")
        except Exception as e:
            print("---------------------------------------------------------------------------")
            print(e)

        finally:
            # put the dataframe in the queue to be processed outside of the multiprocessing function
            queue.put(df)


    """ SAVING AS FILE IS NOT USED ANYMORE. DATABASE ONLY """
    # SAVE DATAFRAME TO PKL
    def save_to_pkl(self, dataframe):
        dataframe.to_pickle(self.psave)

    # SAVE DATAFRAME TO CSV
    def save_to_csv(self, dataframe):
        # for now ill just save it to "history.csv"
        dataframe.to_csv(self.save)
    """ ------------------------------------------------- """

    # SAVE DATAFRAME TO INTERNAL DATABASE
    def save_to_database(self, df):
        json_data = df.to_dict("records")
        exchange = str(df["exchange"].iloc[0])
        market = str(df["market"].iloc[0])
        self.client["crypto_data"][f"raw {exchange} {market}"].insert_many(json_data)



if __name__ == "__main__":
    # for one exchange and market:
    # dont make length above 1 million if you have less than 32gb ram (700 days of data)
    # dont make length above 500k if you have less than 24gb ram (350 days of data)
    # dont make length above 200k if you have less than 16gb ram (140 days of data)
    # for each subsequent exhange or market half the length 
    get_history_object = get_history(markets=["ETH/USDT"],
                                 exchanges=["binance"],
                                 starttime=700,
                                 length=1000000)
    get_history_object.ccxt_history()
    gc.collect()
    get_history_object = get_history(markets=["BTC/USDT"],
                                 exchanges=["binance"],
                                 starttime=700,
                                 length=1000000)
    get_history_object.ccxt_history()
    gc.collect()
    get_history_object = get_history(markets=["XRP/USDT"],
                                 exchanges=["binance"],
                                 starttime=700,
                                 length=1000000)
    get_history_object.ccxt_history()
    gc.collect()
    get_history_object = get_history(markets=["ADA/USDT"],
                                 exchanges=["binance"],
                                 starttime=700,
                                 length=1000000)
    get_history_object.ccxt_history()
