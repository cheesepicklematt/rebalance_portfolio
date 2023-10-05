import sys
sys.path.insert(0,'./')
from src.utility.pipelineMainUtils.prepareDataClasses import getCMCData, calculateNewPortfolio, calculateTrades
from src.utility.pipelineMainUtils.execute_trades import marketOrder

cmc = getCMCData()
cmc.run()

newp = calculateNewPortfolio()
newp.run()

t = calculateTrades()
t.run()


print(t.data)
print('TRADE ORDERS')
if len(t.trade_dict.items())==0:
    print('NO TRADES TO EXECUTE')
a = [print(x) for x in t.trade_dict.items()]


USER_INPUT = input("EXECUTE TRADES?")
if USER_INPUT.lower() == 'y' or USER_INPUT.lower() == 'yes':
    mo = marketOrder()
    mo.execute_trades(test=False)
