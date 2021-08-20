import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
    | "Importar dados" >> beam.io.ReadFromText("files/voos_sample.csv", skip_header_lines=1)
    | "Separar por vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Mostrar resultados" >> beam.Map(print)
    | "Gravar resultado" >> beam.io.WriteToText("files/voos_sample.csv")
)

# Comando para executar
p1.run()