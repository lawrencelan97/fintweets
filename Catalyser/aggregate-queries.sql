--Counts all the tweets in the tweets_data table
select count (*) from `avian-force-216105.tweets.tweets_data`

--Counts all the tweets by the source in the tweets_data table and groups by its source
select source, count (text) from `avian-force-216105.tweets.tweets_data` group by source 

--Counts all the tweets by the source in the tweets_table, groups by its source that have tweets greater than or equal to 500
select source, count (text) from `avian-force-216105.tweets.tweets_data` group by source having count (text)>=500

--Counts all the tweets by the source in the tweets_table and groups by symbols
select symbols, count (text) from `avian-force-216105.tweets.tweets_data` group by symbols

--View ticker, timestamp when the tweet was made, source of the tweet, tweet text and stock closing price by performing a left outer join between company name lookup and tweet data table on symbol and ticker and right outer join between tweet table and stock data on symbol and name where tweet text and stock closing price is not null
select DISTINCT  a.ticker,b.timestamp, b.source, b.text, c.close  from `avian-force-216105.tweets.stock_companyname_lookup` 
as a left outer join `avian-force-216105.tweets.tweets_data` as b on a.ticker = b.symbols right outer join  `avian-force-216105.stock_data.5yr_stock_data`
as c on b.symbols = c.Name where b.text is not null or c.close is not null
