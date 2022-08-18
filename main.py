#  Copyright 2021 Aldair Cortes
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
import codecs

import apache_beam as beam
import pandas as pd
import csv
import io
import argparse
import json
import requests
import logging

from apache_beam.options.pipeline_options import PipelineOptions

def create_dataframe(readable_file):

    # Open a channel to read the file from GCS
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)
    # Create the DataFrame
    dataFrame = pd.DataFrame(pd.read_csv(gcs_file, encoding = "latin-1", sep = "|"))
    json_list = dataFrame.to_json(orient="records")
    jsons = json.loads(json_list)
    return jsons

def make_request(data_payload):
    headers = {
        'apikey': 'iKG1eWAZFkQ3AOgw4bWp5WXggSBodNDYmxLWXTkY1P8AOYhn',
        'Content-Type': 'application/json'
    }
    url = "https://qatapigee.liverpool.com.mx/cdp-pos/v2/clientes/atg"
    payload = json.dumps(data_payload, indent=4)
    response = requests.request("PUT", url, headers=headers, data=payload, verify=False)
    print(response.content)

def main():
    #leer argumentos de entrada
    parser = argparse.ArgumentParser(description="Primer Pipeline")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)

def run_pipeline(custom_args,beam_args):
    entrada = custom_args.entrada
    salida = custom_args.salida
    # input_df = pd.read_csv(entrada,sep='|',encoding='latin-1')
    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p:
        (p | beam.Create([entrada])
           | beam.FlatMap(create_dataframe)
           | beam.Map(print)
           | beam.FlatMap(make_request)

           # | beam.Map(lambda element : print(element))

        )

        # data = p|beam.io.ReadFromText(entrada, coder=ISOCoder())|beam.Map(print)
        # json_list = data|beam.ParDo(ReadDataframe( sep='|', encoding='latin-1'))|beam.Map(print)
        #        # read_csv(entrada,encoding = "latin-1",sep="|")
        # # json_list = beam.dataframe.io.to_json(data,salida,orient="records")
        # json_list | beam.io.WriteToText(salida)
        # data: PCollection[str] = p | beam.parDo(csv_to_list_of_jsons)
        # palabras = lineas | beam.FlatMap(lambda  l : l.split())
        # limpiadas = palabras | beam.Map(sanitizar_palabra)
        # contadas: PCollection[Tuple[str,int]] = limpiadas | beam.combiners.Count.PerElement()
        # palabras_top_lista = contadas | beam.combiners.Top.Of(n_palabras, key=lambda kv: kv[1])
        # palabras_top = palabras_top_lista | beam.FlatMap(lambda x:x)
        # formateado: [PCollection[str]]  = palabras_top | beam.Map(lambda kv: "%s,%d" % (kv[0],kv[1]))
        # formateado| beam.io.WriteToText(salida)


if __name__ == '__main__':
    main()