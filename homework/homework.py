"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


import pandas as pd
import zipfile
import os
import numpy as np
import glob
import sys
from datetime import datetime


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    # Si existen archivos descomprimidos dentro de la carpeta descomprimidos, los elimina
    if os.path.exists("files/input/descompimidos"):
        for file in os.listdir("files/input/descompimidos"):
            os.remove(f"files/input/descompimidos/{file}")

    # crear un ciclo para leer los archivos zip
    for file in os.listdir("files/input/"):
        if file.endswith(".zip"):
            # Descomprimir archivos
            with zipfile.ZipFile(f"files/input/{file}", "r") as zip_ref:
                # si la carpeta descompimidos no existe, la crea
                if not os.path.exists("files/input/descompimidos"):
                    os.makedirs("files/input/descompimidos")
                zip_ref.extractall("files/input/descompimidos")

    # Leer archivos en la carpeta descomprimidos
    archivosDescomprimidos = glob.glob("files/input/descompimidos/*.csv")

    # Crear un ciclo para leer los archivos descomprimidos
    client = []
    campaign = []
    economics = []
    for file in archivosDescomprimidos:
        with open(file, "r") as f:
            line = pd.read_csv(f)

            # Crear un ciclo para leer las lineas de los archivos

            for i in range(len(line)):
                try:
                    # client
                    client.append(
                        {
                            "client_id": line["client_id"][i],
                            "age": line["age"][i],
                            "job": line["job"][i].replace(".", "").replace("-", "_"),
                            "marital": line["marital"][i],
                            "education": line["education"][i]
                            .replace(".", "_")
                            .replace("unknown", str(pd.NA)),
                            "credit_default": (
                                1 if line["credit_default"][i] == "yes" else 0
                            ),
                            "mortgage": 1 if line["mortgage"][i] == "yes" else 0,
                        }
                    )

                    month = str(datetime.strptime(line["month"][i], "%b").month).zfill(
                        2
                    )

                    # campaign
                    campaign.append(
                        {
                            "client_id": line["client_id"][i],
                            "number_contacts": line["number_contacts"][i],
                            "contact_duration": line["contact_duration"][i],
                            "previous_campaign_contacts": line[
                                "previous_campaign_contacts"
                            ][i],
                            "previous_outcome": (
                                1 if line["previous_outcome"][i] == "success" else 0
                            ),
                            "campaign_outcome": (
                                1 if line["campaign_outcome"][i] == "yes" else 0
                            ),
                            "last_contact_date": f"2022-{month}-{line['day'][i]}",
                        }
                    )

                    # economics
                    economics.append(
                        {
                            "client_id": line["client_id"][i],
                            "cons_price_idx": line["cons_price_idx"][i],
                            "euribor_three_months": line["euribor_three_months"][i],
                        }
                    )

                except:
                    error = sys.exc_info()
                    print(f"Tipo de error: {error[0]}")
                    print(f"Mensaje del error: {error[1]}")

    # Crear dataframes
    client = pd.DataFrame(client)
    campaign = pd.DataFrame(campaign)
    economics = pd.DataFrame(economics)

    # Crear carpeta output
    if not os.path.exists("files/output"):
        os.makedirs("files/output")

    # Guardar dataframes en archivos csv
    client.to_csv("files/output/client.csv", index=False)
    campaign.to_csv("files/output/campaign.csv", index=False)
    economics.to_csv("files/output/economics.csv", index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
