import apache_beam as beam

palavras=['quatro','um']

def encontrarPalavras( i ):
 if i in palavras:
    return True

p1 = beam.Pipeline()

Collection = (
    p1
    |beam.io.ReadFromText('files/poema.txt')
    |beam.FlatMap(lambda record: record.split(' '))
    |beam.Filter(encontrarPalavras)
    |beam.io.WriteToText('files/results.txt')
)
p1.run()