-- Citigroup Tweets Data, This query gets all the tweets which mention Citigroup ticker symbol ordered by timestamp
SELECT * FROM `avian-force-216105.tweets.tweets_data` WHERE symbols="C" ORDER BY timestamp

-- Citigroup Stock Data, This query gets 1000 records of Citigroup stock prices ordered by date
SELECT * FROM `avian-force-216105.stock_data.5yr_stock_data` WHERE Name="C" ORDER BY date LIMIT 1000

-- Citigroup Company Name, This query looks up company name from stock symbol
SELECT * FROM `avian-force-216105.tweets.stock_companyname_lookup` WHERE ticker="C"