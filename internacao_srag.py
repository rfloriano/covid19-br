import io
import os
import argparse
import csv
from pathlib import Path

import ckanapi
import rows
from tqdm import tqdm

from covid19br.vacinacao import calculate_age_range


CKAN_URL = "https://opendatasus.saude.gov.br/"
SRAG_DATASETS = ("bd-srag-2020", "bd-srag-2021")
DOWNLOAD_PATH = Path(__file__).parent / "data" / "download"
OUTPUT_PATH = Path(__file__).parent / "data" / "output"
for path in (DOWNLOAD_PATH, OUTPUT_PATH):
    if not path.exists():
        path.mkdir(parents=True)


class PtBrDateField(rows.fields.DateField):
    INPUT_FORMAT = "%d/%m/%Y"

    @classmethod
    def deserialize(cls, value):
        if not (value or "").strip():
            return None
        elif value.count("/") == 2 and len(value.split("/")[-1]) == 2:
            parts = value.split("/")
            value = f"{parts[0]}/{parts[1]}/20{parts[2]}"
        return super().deserialize(value)


def get_csv_resources(dataset_name):
    api = ckanapi.RemoteCKAN(CKAN_URL)

    dataset = api.call_action("package_show", {"id": dataset_name})
    for resource in dataset["resources"]:
        if resource["format"] == "CSV":
            yield resource["url"]


def download_files(should_skip=False):
    for dataset in SRAG_DATASETS:
        csv_url = next(get_csv_resources(dataset))
        filename = DOWNLOAD_PATH / (Path(csv_url).name + ".gz")
        if should_skip and os.path.isfile(filename):
            yield filename
            return
        rows.utils.download_file(
            csv_url,
            filename=filename,
            progress_pattern="Downloading {filename.name}",
            progress=True,
        )
        yield filename


def convert_row(row):
    # new = {}
    # for key, value in row.items():
    #     key, value = key.lower(), value.strip()
    #     if len(value[value.rfind("/") + 1:]) == 3:
    #         # TODO: e se for 2021?
    #         value = value[: value.rfind("/")] + "/2020"
    #     if not value:
    #         value = None
    #     elif key.startswith("dt_"):
    #         value = PtBrDateField.deserialize(value)
    #     new[key] = value

    # TODO: refatorar código que calcula diferença em dias

    diff_days = None
    if row["DT_EVOLUCA"] and row["DT_INTERNA"]:
        dt_evoluca = PtBrDateField.deserialize(row["DT_EVOLUCA"])
        dt_interna = PtBrDateField.deserialize(row["DT_INTERNA"])
        diff_days = (dt_evoluca - dt_interna).days

    row["DIAS_INTERNACAO_A_OBITO_SRAG"] = None
    row["DIAS_INTERNACAO_A_OBITO_OUTRAS"] = None
    row["DIAS_INTERNACAO_A_ALTA"] = None
    row["CURA"] = False
    row["OBITO"] = False

    if row["EVOLUCAO"] == "1":
        row["CURA"] = True
        row["DIAS_INTERNACAO_A_ALTA"] = diff_days
    elif row["EVOLUCAO"] == "2":
        row["OBITO"] = True
        row["DIAS_INTERNACAO_A_OBITO_SRAG"] = diff_days
    elif row["EVOLUCAO"] == "3":
        row["OBITO"] = True
        row["DIAS_INTERNACAO_A_OBITO_OUTRAS"] = diff_days

    age = row["NU_IDADE_N"]
    if row["TP_IDADE"] in ['dia', 'mês']:
        age = 0
    if row["DT_NOTIFIC"] and row["DT_NASC"]:
        dt_notific = PtBrDateField.deserialize(row["DT_NOTIFIC"])
        dt_nasc = PtBrDateField.deserialize(row["DT_NASC"])
        age = int((dt_notific - dt_nasc).days/365.25)
    row["FAIXA_ETARIA"] = calculate_age_range(age)

    # TODO: adicionar coluna ano e semana epidemiológica
    # TODO: corrigir RuntimeError: ERROR:  invalid input syntax for integer: "20-1"
    #                CONTEXT:  COPY srag, line 151650, column cod_idade: "20-1"
    # TODO: data nascimento (censurar?)
    # TODO: dt_interna: corrigir valores de anos inexistentes

    return row


class CsvLazyDictWriter(rows.utils.CsvLazyDictWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fobj = io.StringIO()

    def writerows(self, rows):
        self.writerows = self.writer.writerows
        return self.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--skip", help="Skip downloaded files",  action="store_true")
    parser.add_argument("-p", "--processes", help="How many processes should be used to process file", type=int)
    parser.add_argument("-l", "--lines", help="How many lines should be writed at time", type=int, default=1000)
    args = parser.parse_args()

    filenames = download_files(args.skip)
    output_filename = OUTPUT_PATH / "internacao_srag.csv.gz"
    writer = CsvLazyDictWriter(output_filename)
    for filename in filenames:
        with rows.utils.open_compressed(filename, encoding="utf-8") as fobj:
            reader = csv.DictReader(fobj, delimiter=";")
            first = True
            lines = []
            for row in tqdm(reader, desc=f"Converting {filename.name}"):
                if first:
                    writer.writerow(convert_row(row))
                    first = False
                    continue
                lines.append(convert_row(row))
                if len(lines) > args.lines:
                    writer.writerows(lines)
                    lines = []


if __name__ == "__main__":
    main()
