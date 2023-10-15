import json
import os
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import decimal
from requests import Session
from config import cred, cmcDataDir, controlFilesDir, initPortfolioDataDir, portfolioTradesDataDir, baseBTCPair, baseC

class getCMCData:
    def __init__(self):
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {
        'start':'1',
        'limit':'200',
        'convert':'USD'
        }
        headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': cred.cmcAPI,
        }

        session = Session()
        session.headers.update(headers)
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        self.rawData = data['data']
    
    def run(self):
        self.unpackData()
        self.getBinanceData()
        self.saveData()
    
    def unpackData(self):
        # aggregate CMC data
        quote_keys = list(self.rawData[0]['quote']['USD'].keys())
        self.cmc_df = []
        self.tag_list = []
        for i in range(len(self.rawData)):
            tmp_list = []
            tmp_data = self.rawData[i]
            tmp_list.append(tmp_data['symbol'])
            tmp_quote = tmp_data['quote']['USD']
            
            for j in range(len(quote_keys)):
                tmp_list.append(tmp_quote[quote_keys[j]])
                
            self.tag_list.append({tmp_data['symbol']:tmp_data['tags']})
            self.cmc_df.append(tmp_list)


        self.cmc_df = pd.DataFrame(self.cmc_df)
        self.cmc_df.columns = list(np.hstack([['symbol'],quote_keys]))

    def getBinanceData(self):
        binance_tickers = cred.client.get_exchange_info()
        # get all BTC tickers
        self.btc_tickers = binance_tickers['symbols']
        self.btc_tickers = [y for y in [self.btc_tickers[x]['symbol'] for x in range(len(self.btc_tickers))] if y[-3:]=='BTC']
        self.btc_tickers = pd.DataFrame(self.btc_tickers,columns=['ticker'])
        self.btc_tickers['symbol'] = [x.replace('BTC','') for x in self.btc_tickers['ticker']]
        self.btc_tickers = self.btc_tickers.drop_duplicates(subset='symbol',keep='first')

    def mergeData(self):
        self.cmc_df = self.cmc_df[self.cmc_df['symbol'].isin(np.hstack([['BTC'],self.btc_tickers['symbol']]))].reset_index(drop=True)
        self.cmc_df = self.cmc_df.merge(self.btc_tickers,how='left',left_on='symbol',right_on='symbol')

    def saveData(self):
        with open(os.path.join(cmcDataDir,'tag_list.json'), 'w') as fp:
            json.dump(self.tag_list, fp)
        
        self.cmc_df.to_csv(os.path.join(cmcDataDir,'CMC_data.csv'),index=False)


class weightedPortfolio:
    def __init__(self):
        self.cmcData = pd.read_csv(os.path.join(cmcDataDir,'CMC_data.csv'))
        buyList = pd.read_csv(os.path.join(controlFilesDir,'buyList.csv'))
        self.staticVar = pd.read_csv(os.path.join(controlFilesDir,'static_var_index_port.csv'))

        self.cmcData = self.cmcData[self.cmcData['symbol'].isin(buyList['symbol'])].sort_values(by=['market_cap'],ascending=False).reset_index(drop=True)

    def run(self):
        self.createPortfolio()
        
    def createPortfolio(self): 
        self.portfolioWeighted = []
        counter = 0
        for i in range(len(self.staticVar)):
            numAssets = self.staticVar.loc[i,'Value']
            allocType = self.staticVar.loc[i,'alloc_type']
            portName = self.staticVar.loc[i,'Variable']
            tmpList = []
            for j in range(numAssets):
                sym = self.cmcData.loc[counter,'symbol']
                mcap = self.cmcData.loc[counter,'fully_diluted_market_cap']
                price = self.cmcData.loc[counter,'price']
                tmpList.append([sym,mcap,price,portName,allocType])
                counter += 1

            tmpList =  pd.DataFrame(tmpList,columns=['symbol','marketCap','price','portfolio','allocType'])
            if allocType=='eq':
                tmpList['weight'] = 1/len(tmpList)
            else:
                tmpList['weight'] = tmpList['marketCap']/tmpList['marketCap'].sum()

            self.portfolioWeighted.append(tmpList)

                
        self.portfolioWeighted = pd.concat(self.portfolioWeighted).reset_index(drop=True)


class calculateNewPortfolio:
    def __init__(self):
        self.wp = weightedPortfolio()
        self.wp.run()
        
        self.client = cred.client
        self.rm_tickers = ['AUD','BTC','SUB','BUSD','NFT','SOLO','LUNC','LUNA','ETHW','USDT']
        #self.portfolioWeighted = pd.read_csv(os.path.join(cmcDataDir,'CMC_data.csv'))

    def joinBTCTicker(self,tickers):
        tickerU = []
        for t in tickers:
            if t==baseC:
                x = 'NAN@@'+t
            elif t=='BTC':
                x = t+'USDT'
            else:
                x = t+'BTC'
            tickerU.append(x)

        return tickerU
    
    def run(self):
        self.getBalanceData()
        self.getAssetInfo()
        self.mergeWeighting()
        self.calculateNewBtcBal()
        # fillna for assets that have been added to the portfolios
        self.btcPairsInfo[['balance','BTCvalue','currentWeight']] = self.btcPairsInfo[['balance','BTCvalue','currentWeight']].fillna(0)
        self.saveData()


    def getBalanceData(self):
        # get account balances
        acctInfo = self.client.get_account()

        self.balanceData = []
        for d in acctInfo['balances']:
            self.balanceData.append([d['asset'],d['free'],d['locked']])
        self.balanceData = pd.DataFrame(self.balanceData,columns=['ticker','free','locked'])
        self.balanceData['free'] = self.balanceData['free'].astype(float)
        self.balanceData['locked'] = self.balanceData['locked'].astype(float)
        self.balanceData['balance'] = self.balanceData['free'] + self.balanceData['locked']
        self.balanceData = self.balanceData[self.balanceData['balance']!=0].reset_index(drop=True)

        # remove BTC locked savings
        locked_btc = self.balanceData[self.balanceData['ticker']=='LDBTC'].reset_index(drop=True)
        self.balanceData = self.balanceData[self.balanceData['ticker']!='LDBTC'].reset_index(drop=True)
        if len(locked_btc)>0:
            self.balanceData.loc[self.balanceData['ticker']=='BTC','balance'] = self.balanceData.loc[self.balanceData['ticker']=='BTC','balance'] + locked_btc.loc[0,'balance']

    
    def getAssetInfo(self):
        tickers = self.wp.portfolioWeighted['symbol']
        # remove AUD, BTC and non-tradable tickers
        tickers = [x+'BTC' for x in tickers[~tickers.isin(self.rm_tickers)]]
        tickers.append(baseBTCPair)
        # make BTC pairs information dataframe
        btcPairsInfo_r = []
        for t in tickers:
            tmpLP = float(self.client.get_ticker(symbol=t)['lastPrice'])
            tmpMP = str(float(self.client.get_symbol_info(t)['filters'][1]['minQty']))
            btcPairsInfo_r.append([t,tmpLP,tmpMP])
        self.btcPairsInfo = pd.DataFrame(btcPairsInfo_r,columns=['ticker','lastPrice','minQty'])
        self.btcPairsInfo['minQty'] = [str(decimal.Decimal(x)) for x in self.btcPairsInfo['minQty']]
        self.btcPairsInfo['roundNum'] = [len(x.split('.')[1]) if x.split('.')[1]!='0' else 0 for x in self.btcPairsInfo['minQty']]

    def mergeWeighting(self):
        # merge to create final prepared portfolio
        tmpMerge = self.balanceData[['ticker','balance']]
        c = self.joinBTCTicker(tmpMerge['ticker'])
        tmpMerge.loc[:,'ticker'] = c
        self.btcPairsInfo = self.btcPairsInfo.merge(tmpMerge,how='left',on='ticker')
        self.btcPairsInfo['BTCvalue'] = [x*y if z!='BTCUSDT' else y for x,y,z in zip(self.btcPairsInfo['lastPrice'],self.btcPairsInfo['balance'],self.btcPairsInfo['ticker'])]

        tmpMerge = self.wp.portfolioWeighted[['symbol','marketCap','portfolio','allocType','weight']]
        tmpMerge.columns = ['ticker','marketCap','portfolio','allocType','weight']
        c = self.joinBTCTicker(tmpMerge['ticker'])
        tmpMerge.loc[:,'ticker'] = c
        tmpMerge['rank'] = tmpMerge.index
        self.btcPairsInfo = self.btcPairsInfo.merge(tmpMerge,how='left',on='ticker')

        self.btcPairsInfo = self.btcPairsInfo.sort_values(by=['rank'])
        self.btcPairsInfo = self.btcPairsInfo.reset_index(drop=True)
        del self.btcPairsInfo['rank']

        self.btcPairsInfo['currentWeight'] = 'null'
        for p in self.btcPairsInfo['portfolio']:
            tmp = self.btcPairsInfo[self.btcPairsInfo['portfolio']==p]
            tmpSum = tmp['BTCvalue'].sum()
            for i in tmp.index:
                self.btcPairsInfo.loc[i,'currentWeight'] = self.btcPairsInfo.loc[i,'BTCvalue']/tmpSum

    def calculateNewBtcBal(self):
        # calculate total portfolio value
        USDTBal ,= self.balanceData[self.balanceData['ticker']==baseC]['free']
        USDTBTCPrice ,= self.btcPairsInfo[self.btcPairsInfo['ticker']=='BTCUSDT']['lastPrice']
        USDTBTC_bal = USDTBal/USDTBTCPrice
        portfolioTotal = USDTBTC_bal + self.btcPairsInfo['BTCvalue'].sum()

        self.btcPairsInfo['newBTCBal'] = 'null'
        staticVar = pd.read_csv(os.path.join(controlFilesDir,'static_var_index_port.csv'))
        for i in range(len(staticVar)):
            tmp = staticVar.loc[i,['Variable','alloc_pct']]
            btcAlloc = portfolioTotal*tmp['alloc_pct']
            
            tmpIdx = self.btcPairsInfo[self.btcPairsInfo['portfolio']==tmp['Variable']].index
            for row in tmpIdx:
                self.btcPairsInfo.loc[row,'newBTCBal'] = btcAlloc*self.btcPairsInfo.loc[row,'weight']

    def saveData(self):
        self.btcPairsInfo.to_csv(os.path.join(initPortfolioDataDir,'portfolioDetails.csv'),index=False)


class calculateTrades:
    def __init__(self,data=None):
        '''
        import data and fillna - NaN values are an asset that has been dropped
        '''
        if data!=None:
            self.balanceData = data

        self.data = pd.read_csv(os.path.join(initPortfolioDataDir,'portfolioDetails.csv'))
        self.data = self.data.fillna(0)
        self.min_trade_valBTC = 0.001

    def run(self):
        self.getTradeAmounts()
        self.fixBTCTrade()
        self.remove_unused_assets()
        self.return_trade_dict()
        self.save()

    def getTradeAmounts(self):
        '''
        Calculate trade order quantity
        - round trade quantity to the minimum tradable amount
        - if trade value doesnt reach a certain threshhold do not execute the trade
        '''
        self.data['buyAmtBTC'] = self.data['newBTCBal'] - self.data['BTCvalue']

        self.data['tradeQty'] = 'null'
        self.data['tradeDirection'] = 'null'
        for i in range(len(self.data)):
            tmp_newBTC_bal = self.data.loc[i,'newBTCBal']
            round_num = self.data.loc[i,'roundNum']
            # sell total balance if new BTC value of asset is 0
            if tmp_newBTC_bal==0:
                self.data.loc[i,'tradeQty'] = round(self.data.loc[i,'balance'],round_num) 
                self.data.loc[i,'tradeDirection'] = 'SELL'
            else:
                tmp_trade_amt = self.data.loc[i,'buyAmtBTC']/self.data.loc[i,'lastPrice']
                self.data.loc[i,'tradeQty'] = round(tmp_trade_amt,round_num) 

                if abs(self.data.loc[i,'buyAmtBTC']) < self.min_trade_valBTC:
                    self.data.loc[i,'tradeDirection'] = 'NONE'
                elif tmp_trade_amt < 0:
                    self.data.loc[i,'tradeDirection'] = 'SELL'
                elif tmp_trade_amt > 0:
                    self.data.loc[i,'tradeDirection'] = 'BUY'

    def fixBTCTrade(self):
        # btcNet will be positive when more BTC is being sold then bought
        # btcNet will be negative when more BTC is being bought then sold
        btcNet = sum([x for x,y in zip(self.data['buyAmtBTC'],self.data['ticker']) if y.find(baseC)==-1])*-1

        baseBTCPairBool = self.data['ticker']==baseBTCPair
        round_num ,= self.data.loc[baseBTCPairBool,'roundNum'].to_list()
        tmpTrade = self.data.loc[baseBTCPairBool,'newBTCBal'].sum() - (btcNet + self.data.loc[baseBTCPairBool,'BTCvalue'].sum())
        tmpTrade = tmpTrade if tmpTrade > self.min_trade_valBTC else 0
        tmp_side = 'NONE' if tmpTrade == 0 else 'SELL' if tmpTrade < 0 else 'BUY'

        self.data.loc[baseBTCPairBool, 'buyAmtBTC'] = round(tmpTrade,round_num)
        self.data.loc[baseBTCPairBool, 'tradeQty'] = round(tmpTrade,round_num)
        self.data.loc[baseBTCPairBool, 'tradeDirection'] = tmp_side

    def remove_unused_assets(self):
        keep_bool = [not(x==0 and y==0) for x,y in self.data[['balance','tradeQty']].values]
        self.data = self.data[keep_bool].reset_index(drop=True)

    def return_trade_dict(self):
        self.trade_dict = {x:{'SIDE':z,'QTY':abs(y)} for x,y,z in self.data[['ticker','tradeQty','tradeDirection']].values if z!='NONE'}

    def save(self):
        self.data.to_csv(os.path.join(portfolioTradesDataDir,'portfolioTrades.csv'),index=False)
        with open(os.path.join(portfolioTradesDataDir,'portfolio_trades.json'), 'w') as fp:
            json.dump(self.trade_dict, fp)


