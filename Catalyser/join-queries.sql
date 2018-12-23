--Perform left outer join to get all tweets data where company_names matches name in the lookup table
SELECT * FROM `avian-force-216105.tweets.tweets_data` as a LEFT OUTER JOIN `avian-force-216105.tweets.stock_companyname_lookup` as b on a.company_names = b.name
--Perform an inner join to get all tweets data where ticker symbol in tweet matches the lookup table symbol
SELECT * FROM `avian-force-216105.tweets.tweets_data` as a INNER JOIN `avian-force-216105.tweets.stock_companyname_lookup` as b on a.symbols = b.ticker
--Perform a left outer join to get all tweets data where company_name matches name in the lookup table and the tweet source matches "CityIndex"
SELECT * FROM `avian-force-216105.tweets.tweets_data` as a LEFT OUTER JOIN `avian-force-216105.tweets.stock_companyname_lookup` as b on a.company_names = b.name WHERE a.source="CityIndex"
--Perform a left outer join to get all tweets data where company_name matches name in the lookup table and tweet source starts with character "C"
SELECT * FROM `avian-force-216105.tweets.tweets_data` as a LEFT OUTER JOIN `avian-force-216105.tweets.stock_companyname_lookup` as b on a.company_names = b.name WHERE a.source LIKE "C%"
--Perform a left outer join to get all the tweets data where company_name matches name in the lookup table and tweet source starts with character "C" and are verified tweets
SELECT * FROM `avian-force-216105.tweets.tweets_data` as a LEFT OUTER JOIN `avian-force-216105.tweets.stock_companyname_lookup` as b on a.company_names = b.name WHERE a.source LIKE "C%" AND verified=true