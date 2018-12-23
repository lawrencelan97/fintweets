import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs processing on each element from the input PCollection.
class FilterSourceFn(beam.DoFn):
  def process(self, element):
    record = element
    input_source = record.get('a_source')
    input_ticker = record.get('b_ticker')

    if input_source.startswith("c"):
        if input_ticker != None:
            return [record]
    elif input_source.startswith("C"):
        if input_ticker != None:
            return [record]
    else:
        return

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'project': 'avian-force-216105'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='select * from tweets.tweets_data as a LEFT OUTER JOIN tweets.stock_companyname_lookup as b on a.company_names = b.name'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText('input_joined_table.txt')

    # apply a ParDo to the PCollection
    out_pcoll = query_results | 'Filter tweets by source handles starting with character C' >> beam.ParDo(FilterSourceFn())

    # write PCollection to a log file
    out_pcoll | 'Write to File 2' >> WriteToText('output_joined_table_source_like_c.txt')

    qualified_table_name = 'avian-force-216105:beam_dataset.LeftJoin_Milestone8_Single'
    table_schema = 'a_id:INTEGER,a_text:STRING,a_timestamp:STRING,a_source:STRING,a_symbols:STRING,a_company_names:STRING,a_url:STRING,a_verified:BOOLEAN,b_ticker:STRING,b_name:STRING'

    #Existing Schema
    # id	INTEGER	NULLABLE
    # text	STRING	NULLABLE
    # timestamp	STRING	NULLABLE
    # source	STRING	NULLABLE
    # symbols	STRING	NULLABLE
    # company_names	STRING	NULLABLE
    # url	STRING	NULLABLE
    # verified	BOOLEAN
    # ticker STRING	NULLABLE
    # name  STRING	NULLABLE

    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                     schema=table_schema,
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
print('job completed.')
