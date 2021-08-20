import apache_beam as beam

# definir pipeline
p1 = beam.Pipeline()

voos = (
p1
    # Ler arquivo, e excluir o cabeçalho
    # As pipes significam que um comando é usado como input do outro
    | "Importar dados" >> beam.io.ReadFromText("files/voos_sample.csv", skip_header_lines=1)
    | "Separar por vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Mostrar resultados" >> beam.Map(print)
)

# Comando para executar
p1.run()