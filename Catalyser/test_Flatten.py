import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'project': 'avian-force-216105'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:

    FBtweets_pcoll = p | 'Read FB Tweets' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM tweets.tweets_data WHERE symbols="FB" ORDER BY timestamp'))

    # write PCollection to a log file
    FBtweets_pcoll | 'Write to File 1' >> WriteToText('fb_tweets_query_results.txt')

    TWTRtweets_pcoll = p | 'Read TWTR Tweets' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM tweets.tweets_data WHERE symbols="TWTR" ORDER BY timestamp'))

    # write PCollection to a log file
    TWTRtweets_pcoll | 'Write to File 2' >> WriteToText('twtr_tweets_query_results.txt')

    # Flatten the two PCollections
    merged_pcoll = (FBtweets_pcoll, TWTRtweets_pcoll) | 'Merge Tweets Facebook and Twitter' >> beam.Flatten()

    # write PCollection to a file
    merged_pcoll | 'Write to File 3' >> WriteToText('output_merged_tweets_fb_twtr.txt')

    qualified_table_name = 'avian-force-216105:beam_dataset.Merged_Tweets_FB_TWTR'
    table_schema = 'id:INTEGER,text:STRING,timestamp:STRING,source:STRING,symbols:STRING,company_names:STRING,url:STRING,verified:BOOLEAN'

    # id	INTEGER	NULLABLE
    # text	STRING	NULLABLE
    # timestamp	STRING	NULLABLE
    # source	STRING	NULLABLE
    # symbols	STRING	NULLABLE
    # company_names	STRING	NULLABLE
    # url	STRING	NULLABLE
    # verified	BOOLEAN	NULLABLE

    merged_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                     schema=table_schema,
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
