import os
import time
import json
from config import cred, portfolioTradesDataDir


class marketOrder:
    def __init__(self):
        self.client = cred.client

        with open(os.path.join(portfolioTradesDataDir,'portfolio_trades.json'), "r") as json_file:
            self.trade_dict = json.load(json_file)

    def send_trade(self,ticker,side,qty):
        if side == 'BUY':
            order = self.client.order_market_buy(
                symbol=ticker,
                quantity=qty
                )
            print(order['side'],order['executedQty'],order['symbol'])
        elif side == 'SELL':
            order = self.client.order_market_sell(
                symbol=ticker,
                quantity=qty
                )
            print(order['side'],order['executedQty'],order['symbol'])

    def execute_trades(self,test=True):
        for ticker,details in self.trade_dict.items():
            if test:
                print('TEST:',ticker,details)
            else:
                self.send_trade(
                    ticker=ticker,
                    side=details['SIDE'],
                    qty=details['QTY']
                    )

            time.sleep(0.5)