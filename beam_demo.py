import apache_beam as beam
import sys

def count_words(line):
    return len(line.split())

def lear_there(line):
    if 'Lear' in line:
        yield line

p = beam.Pipeline(argv=sys.argv)

lines = p | 'Read' >> beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
lines | 'lines_out' >> beam.io.WriteToText('out_1.txt')

word_counts = lines | 'CountWords' >> beam.Map(count_words)
word_counts | 'word_counts_out' >> beam.io.WriteToText('out_2.txt')

lear_there_flatmap = lines | 'FlatMapLear' >> beam.FlatMap(lear_there)
lear_there_flatmap | 'lear_there_flatmap_out' >> beam.io.WriteToText('out_3.txt')

lear_there_filter = lines | 'FilterLear' >> beam.Filter(lambda x: 'Lear' in x)
lear_there_filter | 'lear_there_filter_out' >> beam.io.WriteToText('out_4.txt')

p.run().wait_until_finish()