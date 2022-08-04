#  Copyright 2021 Israel Herraiz
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
import apache_beam as beam
import argparse
from ast import parse

from apache_beam.options_pipeline_options import PipelineOptios

def main():
    #leer argumentos de entrada cli
    parser = argparse.ArgumentParser(description="Primer Pipeline")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)

def run_pipeline(custom_args,beam_args):
    entrada = custom_args.entrada
    salida = custom_args.salida

    opts = PipelineOptios(beam_args)

    with beam.Pipeline(opts) as p:
        lineas: PCollection[str] = p | beam.io.ReadFromText(entrada)
        palabras = lineas | beam.FlatMap(lambda  l : l.split())
        contadas: PCollection[Tuple[str,int]] = palabras | beam.combiners.Count.PerElement()
        palabras_top = contadas | beam.combiners.Top.Of(5, keys = lambda kv: kv[1])

        palabras_top | beam.Map(print)




if __name__ == '__main__':
    main()