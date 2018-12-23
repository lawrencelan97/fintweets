import logging, os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs processing on each element from the input PCollection.
class FormatTimestampFn(beam.DoFn):
  def process(self, element):
    record = element
    input_timestamp = record.get('timestamp')

    # desired date format: YYYY-MM-DD hh:mm:ss Timezone (e.g. 2018-07-15 05:49:22 UTC)
    # input date formats: Day Month DD hh:mm:ss +0000 YYYY
    timestamp_split = input_timestamp.split(' ')
    if len(timestamp_split) > 1:
        # day = timestamp_split[0]
        month = timestamp_split[1]
        dd = timestamp_split[2]
        hhmmss = timestamp_split[3]
        timezone = timestamp_split[4]
        yyyy = timestamp_split[5]
        if(month=='Jan'):
            mm='01'
        elif(month=='Feb'):
            mm='02'
        elif(month=='Mar'):
            mm='03'
        elif(month=='Apr'):
            mm='04'
        elif(month=='May'):
            mm='05'
        elif(month=='Jun'):
            mm='06'
        elif(month=='Jul'):
            mm='07'
        elif(month=='Aug'):
            mm='08'
        elif(month=='Sep'):
            mm='09'
        elif(month=='Oct'):
            mm='10'
        elif(month=='Nov'):
            mm='11'
        elif(month=='Dec'):
            mm='12'
        else:
            mm='Unknown'

        if (timezone=='+0000'):
            tz='UTC'
        else:
            tz='Unknown'
        reformatted_timestamp = yyyy+'-'+mm+'-'+dd+' '+hhmmss+' '+tz
        record['timestamp_new'] = reformatted_timestamp
    return [record]

PROJECT_ID = 'avian-force-216105'
BUCKET = 'gs://catalyser-fall18-dataflow'
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-tweetsdata',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-8',
    'num_workers': 8
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:

    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='select * from tweets.tweets_data'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText(DIR_PATH + 'input.txt')

    # apply a ParDo to the PCollection
    out_pcoll = query_results | 'Format DOB' >> beam.ParDo(FormatTimestampFn())

    # write PCollection to a log file
    out_pcoll | 'Write to File 2' >> WriteToText(DIR_PATH + 'output.txt')

    qualified_table_name = 'avian-force-216105:beam_dataset.Formatted_Timestamp_Milestone8_Cluster'
    table_schema = 'id:INTEGER,text:STRING,timestamp:STRING,source:STRING,symbols:STRING,company_names:STRING,url:STRING,verified:BOOLEAN,timestamp_new:TIMESTAMP'

    #Existing Schema
    # id	INTEGER	NULLABLE
    # text	STRING	NULLABLE
    # timestamp	STRING	NULLABLE
    # source	STRING	NULLABLE
    # symbols	STRING	NULLABLE
    # company_names	STRING	NULLABLE
    # url	STRING	NULLABLE
    # verified	BOOLEAN

    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                     schema=table_schema,
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
print('job completed.')
