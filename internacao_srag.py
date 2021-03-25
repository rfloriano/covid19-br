import shutil
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


EVOLUTION_CURE = 'cura'
EVOLUTION_DEATH = 'óbito'
EVOLUTION_DEATH_OTHER = 'óbito por outras causas'
EVOLUTION_DEATHS_TYPE = [EVOLUTION_DEATH, EVOLUTION_DEATH_OTHER]
IGNORED = 'ignorado'


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


FORMAT = {
    'EVOLUCAO': {'1': EVOLUTION_CURE, '2': EVOLUTION_DEATH, '3': EVOLUTION_DEATH_OTHER, '9': IGNORED, '': IGNORED},
    'CS_SEXO': {'M': 'masculino', 'F': 'feminino', 'I': IGNORED},
    'TP_IDADE': {'1': 'dia', '2': 'mês', '3': 'ano'},
    'CS_GESTANT': {'1': True, '2': True, '3': True, '4': True, '5': False, '6': False, '9': None, '0': None},  # TODO: opção 0 não está no manual, mas vêm nos dado,
    'CS_RACA': {'1': 'branca', '2': 'preta', '3': 'amarela', '4': 'parda', '5': 'indígena', '9': IGNORED, '': IGNORED},
    'CS_ESCOL_N': {'0': 'Sem escolaridade/Analfabeto', '1': 'Fundamental 1º ciclo (1ª a 5ª série)', '2': 'Fundamental 2º ciclo (6ª a 9ª série)', '3': ' Médio (1º ao 3º ano)', '4': 'Superior', '5': 'Não se aplica', '9': IGNORED, '': IGNORED},
    'CS_ZONA': {'1': 'Urbana', '2': 'Rural', '3': 'Periurbana', '9': IGNORED, '': IGNORED},
    'SURTO_SG': {'1': True, '2': False, '9': None, '': None},
    'NOSOCOMIAL': {'1': True, '2': False, '9': None, '': None},
    'AVE_SUINO': {'1': True, '2': False, '3': None, '9': None, '': None},   # TODO: opção 3 não está no manual, mas vêm nos dado,
    'FEBRE': {'1': True, '2': False, '9': None, '': None},
    'TOSSE': {'1': True, '2': False, '9': None, '': None},
    'GARGANTA': {'1': True, '2': False, '9': None, '': None},
    'DISPNEIA': {'1': True, '2': False, '9': None, '': None},
    'DESC_RESP': {'1': True, '2': False, '9': None, '': None},
    'SATURACAO': {'1': True, '2': False, '9': None, '': None},
    'DIARREIA': {'1': True, '2': False, '9': None, '': None},
    'VOMITO': {'1': True, '2': False, '9': None, '': None},
    'OUTRO_SIN': {'1': True, '2': False, '9': None, '': None},
    'PUERPERA': {'1': True, '2': False, '9': None, '': None},
    'FATOR_RISC': {'S': True, 'N': False},  # , 9: None},
    'CARDIOPATI': {'1': True, '2': False, '9': None, '': None},
    'HEMATOLOGI': {'1': True, '2': False, '9': None, '': None},
    'SIND_DOWN': {'1': True, '2': False, '9': None, '': None},
    'HEPATICA': {'1': True, '2': False, '9': None, '': None},
    'ASMA': {'1': True, '2': False, '9': None, '': None},
    'DIABETES': {'1': True, '2': False, '9': None, '': None},
    'NEUROLOGIC': {'1': True, '2': False, '9': None, '': None},
    'PNEUMOPATI': {'1': True, '2': False, '9': None, '': None},
    'IMUNODEPRE': {'1': True, '2': False, '9': None, '': None},
    'RENAL': {'1': True, '2': False, '9': None, '': None},
    'OBESIDADE': {'1': True, '2': False, '9': None, '': None},
    'OUT_MORBI': {'1': True, '2': False, '9': None, '': None},
    'VACINA': {'1': True, '2': False, '9': None, '': None},
    'MAE_VAC': {'1': True, '2': False, '9': None, '': None},
    'M_AMAMENTA': {'1': True, '2': False, '9': None, '': None},
    'ANTIVIRAL': {'1': True, '2': False, '9': None, '': None},
    'TP_ANTIVIR': {'1': 'Oseltamivir', '2': 'Zanamivir', '3': 'Outro', '': 'Outro'},
    'HOSPITAL': {'1': True, '2': False, '9': None, '': None},
    'UTI': {'1': True, '2': False, '9': None, '': None},
    'SUPORT_VEN': {'1': True, '2': True, '3': False, '9': None, '': None},
    'RAIOX_RES': {'1': 'Normal', '2': 'Infiltrado intersticial', '3': 'Consolidação', '4': 'Misto', '5': 'Outro', '6': 'Não realizado', '9': IGNORED, '': IGNORED},
    'AMOSTRA': {'1': True, '2': False, '9': None, '': None},
    'TP_AMOSTRA': {'1': 'Secreção de Nasoorofaringe', '2': 'Lavado Broco-alveolar', '3': 'Tecido post-mortem', '4': 'Outra, qual?', '5': 'LCR', '9': IGNORED, '': IGNORED},
    'PCR_RESUL': {'1': 'Detectável', '2': 'Não Detectável', '3': 'Inconclusivo', '4': 'Não Realizado', '5': 'Aguardando Resultado', '9': IGNORED, '': IGNORED},
    'POS_PCRFLU': {'1': True, '2': False, '9': None, '': None},
    'TP_FLU_PCR': {'1': 'Influenza A', '2': 'Influenza B', '': None},
    'PCR_FLUASU': {'1': 'Influenza A(H1N1)pdm09', '2': 'Influenza A (H3N2)', '3': 'Influenza A não subtipado', '4': 'Influenza A não subtipável', '5': 'Inconclusivo', '6': 'Outro, especifique:', '': ''},
    'PCR_FLUBLI': {'1': 'Victoria', '2': 'Yamagatha', '3': 'Não realizado', '4': 'Inconclusivo', '5': 'Outro, especifique:', '': ''},
    'POS_PCROUT': {'1': True, '2': False, '9': None, '': None},
    'PCR_VSR': {'1': True, '': False},
    'PCR_PARA1': {'1': True, '': False},
    'PCR_PARA2': {'1': True, '': False},
    'PCR_PARA3': {'1': True, '': False},
    'PCR_PARA4': {'1': True, '': False},
    'PCR_ADENO': {'1': True, '': False},
    'PCR_METAP': {'1': True, '': False},
    'PCR_BOCA': {'1': True, '': False},
    'PCR_RINO': {'1': True, '': False},
    'PCR_SARS2': {'1': True, '': False},
    'PCR_OUTRO': {'1': True, '': False},
    'CLASSI_FIN': {'1': True, '2': True, '3': True, '4': True, '5': True, '': False},
    'CRITERIO': {'1': 'Laboratorial', '2': 'Clínico Epidemiológico', '3': 'Clínico', '4': 'Clínico Imagem', '': None},
    'HISTO_VGM': {'1': 'Sim', '2': 'Não', '9': IGNORED, '0': ''},
    'DOR_ABD': {'1': True, '2': False, '9': None, '': None},
    'FADIGA': {'1': True, '2': False, '9': None, '': None},
    'PERD_OLFT': {'1': True, '2': False, '9': None, '': None},
    'PERD_PALA': {'1': True, '2': False, '9': None, '': None},
    'TOMO_RES': {'1': 'Tipico COVID-19', '2': 'Indeterminado COVID-19', '3': 'Atípico COVID-19', '4': 'Negativo para Pneumonia', '5': 'Outro', '6': 'Não realizado', '9': IGNORED, '': IGNORED},
    'TP_TES_AN': {'1': 'Imunofluorescência (IF)', '2': 'Teste rápido antigênico', '': None},
    'RES_AN': {'1': 'positivo', '2': 'Negativo', '3': 'Inconclusivo', '4': 'Não realizado', '5': 'Aguardando resultado', '9': IGNORED, '': IGNORED},
    'POS_AN_FLU': {'1': True, '2': False, '9': None, '': None},
    'TP_FLU_AN': {'1': 'Influenza A', '2': 'Influenza B', '': None},
    'POS_AN_OUT': {'1': True, '2': False, '9': None, '': None},
    'AN_SARS2': {'1': True, '': False},
    'AN_VSR': {'1': True, '': False},
    'AN_PARA1': {'1': True, '': False},
    'AN_PARA2': {'1': True, '': False},
    'AN_PARA3': {'1': True, '': False},
    'AN_ADENO': {'1': True, '': False},
    'AN_OUTRO': {'1': True, '': False},
    'TP_AM_SOR': {'1': 'Sangue/plasma/soro', '2': 'Outra, qual?', '9': IGNORED, '': IGNORED},
    'TP_SOR': {'1': 'Teste rápido', '2': 'Elisa', '3': 'Quimiluminescência', '4': 'Outro, qual', '': None},
}


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

    row['SUPORT_VEN_TYPE'] = {'1': 'invasivo', '2': 'não invasivo'}.get(row['SUPORT_VEN'])
    row['CS_GESTANT_TYPE'] = {'1': 'primeiro trimestre', '2': 'segundo trimestre', '3': 'terceiro trimestre', '4': 'idade gestacional ignorada', '5': 'não', '6': 'não aplicável', '9': IGNORED, '0': ''}.get(row['CS_GESTANT'])
    row['CLASSI_FIN_TYPE'] = {'1': 'influenza', '2': 'outro vírus respiratório', '3': row.get('CLASSI_OUT', ''), '4': 'não especificado', '5': 'COVID-19', '': ''}.get(row['CLASSI_FIN'])

    for key, choices in FORMAT.items():
        row[key] = choices[row[key]]

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

    if row["EVOLUCAO"] == EVOLUTION_CURE:
        row["CURA"] = True
        row["DIAS_INTERNACAO_A_ALTA"] = diff_days
    elif row["EVOLUCAO"] == EVOLUTION_DEATH:
        row["OBITO"] = True
        row["DIAS_INTERNACAO_A_OBITO_SRAG"] = diff_days
    elif row["EVOLUCAO"] == EVOLUTION_DEATH_OTHER:
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

    def dump(self):
        with open(self.filename_or_fobj, 'w') as fd:
            self._fobj.seek(0)
            shutil.copyfileobj(self._fobj, fd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--skip", help="Skip downloaded files",  action="store_true")
    parser.add_argument("-l", "--lines", help="How many lines should be writed at time", type=int, default=1000)
    args = parser.parse_args()

    filenames = download_files(args.skip)
    output_filename = OUTPUT_PATH / "internacao_srag.csv"
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

            if lines:
                writer.writerows(lines)
    print('-------> DUMPING')
    writer.dump()


if __name__ == "__main__":
    main()
