import apache_beam as beam

p1 = beam.Pipeline()

Collection = (
    p1
    |beam.io.ReadFromText('files/poema.txt')
    |beam.FlatMap(lambda record: record.split(' '))
    |beam.io.WriteToText('files/poema.txt')
)
p1.run()