"""MC2-P1: Market simulator.  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
Copyright 2018, Georgia Institute of Technology (Georgia Tech)  		   	  			    		  		  		    	 		 		   		 		  
Atlanta, Georgia 30332  		   	  			    		  		  		    	 		 		   		 		  
All Rights Reserved  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
Template code for CS 4646/7646  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
Georgia Tech asserts copyright ownership of this template and all derivative  		   	  			    		  		  		    	 		 		   		 		  
works, including solutions to the projects assigned in this course. Students  		   	  			    		  		  		    	 		 		   		 		  
and other users of this template code are advised not to share it with others  		   	  			    		  		  		    	 		 		   		 		  
or to make it available on publicly viewable websites including repositories  		   	  			    		  		  		    	 		 		   		 		  
such as github and gitlab.  This copyright statement should not be removed  		   	  			    		  		  		    	 		 		   		 		  
or edited.  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
We do grant permission to share solutions privately with non-students such  		   	  			    		  		  		    	 		 		   		 		  
as potential employers. However, sharing with other current or future  		   	  			    		  		  		    	 		 		   		 		  
students of CS 7646 is prohibited and subject to being investigated as a  		   	  			    		  		  		    	 		 		   		 		  
GT honor code violation.  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
-----do not edit anything above this line---  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
Student Name: Havish Chennamraj
GT User ID: hchennamraj3
GT ID: 903201642
"""  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
import pandas as pd  		   	  			    		  		  		    	 		 		   		 		  
import numpy as np  		   	  			    		  		  		    	 		 		   		 		  
import datetime as dt  		   	  			    		  		  		    	 		 		   		 		  
import os  		   	  			    		  		  		    	 		 		   		 		  
from util import get_data, plot_data  	  	 		 		   		 		  
  		   	  	
def getUniqueSymbols(orders_df):
  symbols = list(orders_df["Symbol"])
  unique = {}
  for symbol in symbols:
    unique[symbol] = 1
  return unique.keys()

def getStockPrices(symbols, dates):
  temp = get_data(symbols, dates)
  temp.fillna(method = 'ffill',inplace = True)
  temp.fillna(method = 'backfill', inplace = True)
  return temp

def createSymbolOrderDF(symbol, prices, orders_df, commission, impact):
  symbolOrderData = orders_df[ orders_df['Symbol'] == symbol ]
  # Buy shares multiply by -1, because cash has to be reduced when BUY order is executed
  symbolOrderData.loc[ symbolOrderData.Order == 'BUY','Shares'] = -1 *  symbolOrderData.loc[ symbolOrderData.Order=="BUY",'Shares']
  symbolOrderData = symbolOrderData.rename(columns = {'Shares':symbol + ":Shares"})
  symbolOrderData[symbol+":Commission"] = commission
  symbolOrderData[symbol+":Impact"] = 0

  #get Prices for the Symbol
  symbolPrices = prices[symbol]

  # Attach Prices to SymbolOrderData 
  symbolOrderData = symbolOrderData.join(symbolPrices, how = 'left')

  #Calculate Impact of executing the order
  symbolOrderData[symbol+":Impact"] = impact * symbolOrderData[symbol+":Shares"].abs() * symbolOrderData[symbol]

  #Drop unnecesasry rows
  symbolOrderData.drop(['Order','Symbol', symbol],axis = 1,inplace = True)

  #Club multiple rows of same date into single row
  symbolOrderData = symbolOrderData.groupby(symbolOrderData.index).sum()
  return symbolOrderData

def executeOrders(ordersRecord, symbols):
  ordersRecord["cashVal"] = 0 
  for symbol in symbols:
      ordersRecord["cashVal"] += (ordersRecord[symbol] * ordersRecord[symbol + ":Shares"])
      # Has to be dropped to do a cascading sum of rows, if not dropped the symbol prices are also added up along rows
      ordersRecord.drop(symbol, inplace = True,axis = 1)
  # Cascade Sum: row[i] = row[i] + row[i-1]
  ordersRecord = ordersRecord.cumsum(axis = 0) 
  return ordersRecord

def calculatePortVal(ordersRecord, prices, symbols, start_val):
  #Get Symbol Prices again
  ordersRecord = prices.join(ordersRecord)
   
  ordersRecord.dropna(axis = 0)
  ordersRecord["portVal"] = start_val + ordersRecord["cashVal"]
  for symbol in symbols:
      ordersRecord["portVal"] -= ordersRecord[symbol] * ordersRecord[symbol+":Shares"] + ordersRecord[symbol+":Commission"] + ordersRecord[symbol+":Impact"]
  return(ordersRecord['portVal'])

def compute_portvals(orders_file = "./orders/orders.csv", start_val = 1000000, commission=9.95, impact=0.005):  		   	  			    		  		  		    	 		 		   		 		  
    # this is the function the autograder will call to test your code  		   	  			    		  		  		    	 		 		   		 		  
    # NOTE: orders_file may be a string, or it may be a file object. Your  		   	  			    		  		  		    	 		 		   		 		  
    # code should work correctly with either input  		   	  			    		  		  		    	 		 		   		 		  
    # TODO: Your code here  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
    # In the template, instead of computing the value of the portfolio, we just  		   	  			    		  		  		    	 		 		   		 		  
    # read in the value of IBM over 6 months  	

    orders_df = pd.read_csv(orders_file, index_col='Date', parse_dates=True, na_values=['nan'])
    orders_df = orders_df.sort_index()
    startDate = orders_df.index[0]
    endDate = orders_df.index[-1]
    symbols = getUniqueSymbols(orders_df)
    

    prices = getStockPrices(symbols, pd.date_range(startDate, endDate))
    prices.drop("SPY", axis = 1, inplace = True)	 

    ordersRecord = pd.DataFrame(index = pd.date_range(startDate, endDate))
    for symbol in symbols:
        symbolOrderData = createSymbolOrderDF(symbol, prices, orders_df, commission, impact)
        ordersRecord = ordersRecord.join(symbolOrderData)

    #removes dates on which trading is closed
    ordersRecord = prices.join(ordersRecord)
    ordersRecord = ordersRecord.fillna(value = 0)

    ordersRecord = executeOrders(ordersRecord, symbols)
    portVals = calculatePortVal(ordersRecord, prices, symbols, start_val)
    return portVals

def author():
  return "hchennamraj3" 		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
def test_code():  		   	  			    		  		  		    	 		 		   		 		  
    # this is a helper function you can use to test your code  		   	  			    		  		  		    	 		 		   		 		  
    # note that during autograding his function will not be called.  		   	  			    		  		  		    	 		 		   		 		  
    # Define input parameters  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
    of = "./orders/orders-02.csv"  		   	  			    		  		  		    	 		 		   		 		  
    sv = 1000000  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
    # Process orders  		   	  			    		  		  		    	 		 		   		 		  
    portvals = compute_portvals(orders_file = of, start_val = sv)  		   	  			    		  		  		    	 		 		   		 		  
    if isinstance(portvals, pd.DataFrame):  		   	  			    		  		  		    	 		 		   		 		  
        portvals = portvals[portvals.columns[0]] # just get the first column  		   	  			    		  		  		    	 		 		   		 		  
    else:  		   	  			    		  		  		    	 		 		   		 		  
        "warning, code did not return a DataFrame"  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
    # Get portfolio stats  		   	  			    		  		  		    	 		 		   		 		  
    # Here we just fake the data. you should use your code from previous assignments.  		   	  			    		  		  		    	 		 		   		 		  
    start_date = dt.datetime(2008,1,1)  		   	  			    		  		  		    	 		 		   		 		  
    end_date = dt.datetime(2008,6,1)  		   	  			    		  		  		    	 		 		   		 		  
    cum_ret, avg_daily_ret, std_daily_ret, sharpe_ratio = [0.2,0.01,0.02,1.5]  		   	  			    		  		  		    	 		 		   		 		  
    cum_ret_SPY, avg_daily_ret_SPY, std_daily_ret_SPY, sharpe_ratio_SPY = [0.2,0.01,0.02,1.5]  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
    # Compare portfolio against $SPX  		   	  			    		  		  		    	 		 		   		 		  
    print "Date Range: {} to {}".format(start_date, end_date)  		   	  			    		  		  		    	 		 		   		 		  
    print  		   	  			    		  		  		    	 		 		   		 		  
    print "Sharpe Ratio of Fund: {}".format(sharpe_ratio)  		   	  			    		  		  		    	 		 		   		 		  
    print "Sharpe Ratio of SPY : {}".format(sharpe_ratio_SPY)  		   	  			    		  		  		    	 		 		   		 		  
    print  		   	  			    		  		  		    	 		 		   		 		  
    print "Cumulative Return of Fund: {}".format(cum_ret)  		   	  			    		  		  		    	 		 		   		 		  
    print "Cumulative Return of SPY : {}".format(cum_ret_SPY)  		   	  			    		  		  		    	 		 		   		 		  
    print  		   	  			    		  		  		    	 		 		   		 		  
    print "Standard Deviation of Fund: {}".format(std_daily_ret)  		   	  			    		  		  		    	 		 		   		 		  
    print "Standard Deviation of SPY : {}".format(std_daily_ret_SPY)  		   	  			    		  		  		    	 		 		   		 		  
    print  		   	  			    		  		  		    	 		 		   		 		  
    print "Average Daily Return of Fund: {}".format(avg_daily_ret)  		   	  			    		  		  		    	 		 		   		 		  
    print "Average Daily Return of SPY : {}".format(avg_daily_ret_SPY)  		   	  			    		  		  		    	 		 		   		 		  
    print  		   	  			    		  		  		    	 		 		   		 		  
    print "Final Portfolio Value: {}".format(portvals[-1])  		   	  			    		  		  		    	 		 		   		 		  
  		   	  			    		  		  		    	 		 		   		 		  
if __name__ == "__main__":  		   	  			    		  		  		    	 		 		   		 		  
    test_code()  		   	  			    		  		  		    	 		 		   		 		  
