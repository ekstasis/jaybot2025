import datetime as dt

import pandas as pd
import matplotlib.pyplot as plt
import mongodb as db

#import sql_client as db

pd.options.display.max_columns = 20
pd.options.display.width = 180

start_date = '2024-11-24'
# end_date = start_date
# end_date = '2021-03-28'

start_time = '03:00:00.000000'
# end_time =   '12:54:00.000000'

start = f'{start_date} {start_time}'
# end = f'{end_date} {end_time}'
end = dt.datetime.utcnow()
print(start)
print(end)
# timedelta = dt.timedelta(minutes=10)
# start = end - timedelta

read_cache = True
if not read_cache:
    
    #qry = "SELECT * from matches_xlm "
    #qry += f"WHERE time BETWEEN '{start}' AND '{end}' "
    #qry += "ORDER BY trade_id asc"
    #print(qry)
    #conn = db.connection('jaybizserver')
    #result = db.do_query_with(conn, qry)
    conn = db.connection()
    result = conn['BTC'].find({})
    df = pd.DataFrame(result)
    df.to_csv('cache.csv')
    exit(0)
else:
    df = pd.read_csv('cache.csv')

df.index = pd.DatetimeIndex(df['time'])
df.drop('time', axis=1, inplace=True)
cols = ['trade_id', 'type', 'size', 'price', 'maker_order_id', 'taker_order_id']
df = df[cols]
df[["price", "size"]] = df[["price", "size"]].apply(pd.to_numeric)
df['maker_buys'] = df['size'].where(df.type == 'sell')
df['maker_sells'] = df['size'].where(df.type == 'buy')
df['cum_sum_maker_buy'] = df['maker_buys'].cumsum().fillna(method='ffill')
df['cum_sum_maker_sell'] = df['maker_sells'].cumsum().fillna(method='ffill')
print(df)
# df['pct'] = df.price.pct_change()

fig, axes = plt.subplots(nrows=2, ncols=1, gridspec_kw={'height_ratios': [4, 1]})
for ax in axes:
    ax.grid(True, which='both')
    #print(ax)
df_mins = df.resample('5T')
#print(df_mins)
sums = df_mins.sum()[['maker_buys', 'maker_sells']]
#print(sums)

df['price'].plot(grid=True, ax=axes[1])
#print(df_mins.mean()['price'])
#df_mins.mean()['price'].plot(grid=True, ax=axes[1])

(sums['maker_sells'] - sums['maker_buys']).plot(color='red', grid=True, ax=axes[0])
# sums['maker_buys'].plot(color='green', grid=True, style='.', ax=axes[1])
# sums.plot.bar(grid=True, ax=axes[0])

# diff = False
# if diff:
#     (df.cum_sum_maker_buy - df.cum_sum_maker_sell).plot(color='green', grid=True, style='.', ax=axes[0])
# else:
#     df['cum_sum_maker_sell'].plot(color='red', grid=True, style='.', ax=axes[0])
#     df['cum_sum_maker_buy'].plot(color='green', grid=True, style='.', ax=axes[0])

# df['maker_buys'].plot(color='green', grid=True, style='.', ax=axes[0])
# df['maker_sells'].plot(color='red', grid=True, style='.', ax=axes[1])
# df[['buys', 'sells']].plot(grid=True, style='.', ax=axes[0])
# count_diffs.plot(kind='bar', color='red', grid=True, label='Count difference', ax=axes[0], secondary_y=True)
# df_secs.count().buys.plot(color='green', grid=True, label='Buy Size', style='.', ax=axes[0])
# counts['price'].plot(color='black', grid=True, label='Price', ax=axes[0], secondary_y=True)
plt.show()

# takers_who_sold = df[df['type'] == 'sell'].groupby('taker_order_id')
# takers_who_bought = df[df['type'] == 'buy'].groupby('taker_order_id')
# makers_who_bought = df[df['type'] == 'sell'].groupby('maker_order_id')
# makers_who_sold = df[df['type'] == 'buy'].groupby('maker_order_id')

