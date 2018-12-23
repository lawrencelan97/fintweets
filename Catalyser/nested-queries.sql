--Source with highest unverified tweet count (with a self join on the id)
select x.source, x.number_of_tweets from (select a.source, count(a.text) number_of_tweets from  `avian-force-216105.tweets.tweets_data` a join `avian-force-216105.tweets.tweets_data` b on a.id=b.id group by source) x where x.number_of_tweets=(select max(y.number_of_tweets) highest_tweet_count from (select source, count(text) number_of_tweets from `avian-force-216105.tweets.tweets_data` group by source) y)

--Source with highest verified tweet count
select x.source, x.number_of_tweets from (select source, verified, count(text) number_of_tweets from  `avian-force-216105.tweets.tweets_data` group by source,verified having verified=true) x where x.number_of_tweets=(select max(y.number_of_tweets) highest_tweet_count from (select source,verified, count(text) number_of_tweets from  `avian-force-216105.tweets.tweets_data` group by source,verified having verified=true) y)

--Stocks with highest unverified tweet count (with a self join on the id)
select x.symbols, x.number_of_tweets from (select a.symbols, count(a.text) number_of_tweets from `avian-force-216105.tweets.tweets_data` a join `avian-force-216105.tweets.tweets_data` b on a.id=b.id group by symbols) x where x.number_of_tweets=(select max(y.number_of_tweets) highest_tweet_count from (select symbols, count(text) number_of_tweets from `avian-force-216105.tweets.tweets_data` group by symbols) y)

--Stocks with highest verified tweet count
select x.symbols, x.number_of_tweets from (select symbols,verified, count(text) number_of_tweets from  `avian-force-216105.tweets.tweets_data` group by symbols,verified having verified=true) x where x.number_of_tweets=(select max(y.number_of_tweets) highest_tweet_count from (select symbols,verified, count(text) number_of_tweets from  `avian-force-216105.tweets.tweets_data` group by symbols,verified having verified=true) y)

--Tweet count composition of user bibeypost_stock grouped by Stocks
select source, symbols, count(text)number_of_tweets from `avian-force-216105.tweets.tweets_data` where source=(select x.source from (select source, count(text) number_of_tweets from  `avian-force-216105.tweets.tweets_data` group by source) x where x.number_of_tweets=(select max(y.number_of_tweets) highest_tweet_count from (select source, count(text) number_of_tweets from  `avian-force-216105.tweets.tweets_data` group by source) y)) group by source,symbols