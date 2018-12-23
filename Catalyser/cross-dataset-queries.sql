Query 1:
select *, cast(PARSE_DATETIME('%a %b %e %X +0000 %E4Y',  timestamp) as TIMESTAMP) as new_timestamp from `avian-force-216105.beam_dataset.Formatted_Tweets`

-- The query converts timestamp from string format to timestamp format. We can achieve this using Pardo.

Query 2:
SELECT * FROM `avian-force-216105.beam_dataset.Formatted_Tweets` WHERE symbols="FB" or symbols = "TWTR"

-- The query produces all the tweets of the company facebook or twitter. We can break it down by reading data from big query table using two separate queries in order to use flatten transformation which will produce the same result. 

Query 3:
SELECT * FROM `avian-force-216105.beam_dataset.Formatted_Tweets` as a LEFT OUTER JOIN `avian-force-216105.tweets.stock_companyname_lookup` as b on a.company_names = b.name WHERE a.source LIKE "C%"

-- The query produces the list of all the tweets made by sources whose handle start with character "C". It uses LIKE SQL operation. ParDo is required to parallelize the process of finding tweets by sources whose handle start with character "C".

Query 4:
SELECT * FROM `avian-force-216105.beam_dataset.Formatted_Tweets` as a LEFT OUTER JOIN `avian-force-216105.tweets.stock_companyname_lookup` as b on a.company_names = b.name WHERE a.source LIKE "C%" AND verified=true

-- The query produces the list of all the verified tweets made by sources whose handle start with character "C". It uses LIKE and comparison SQL operation. ParDo is required to parallelize the process of finding verified tweets by sources whose handle start with character "C".