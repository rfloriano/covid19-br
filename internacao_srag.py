import io
import argparse
import csv
import os
from pathlib import Path
from multiprocessing import Process, Queue

import rows
import ckanapi
from tqdm import tqdm

import models
from covid19br.vacinacao import calculate_age_range


CKAN_URL = "https://opendatasus.saude.gov.br/"
SRAG_DATASETS = ("bd-srag-2020", "bd-srag-2021")
DOWNLOAD_PATH = Path(__file__).parent / "data" / "download"
OUTPUT_PATH = Path(__file__).parent / "data" / "output"
for path in (DOWNLOAD_PATH, OUTPUT_PATH):
    if not path.exists():
        path.mkdir(parents=True)

CURE = 'cura'
DEATH = 'óbito'
DEATH_OTHER_CAUSES = 'óbito por outras causas'
DEATHS_TYPE = [DEATH, DEATH_OTHER_CAUSES]
IGNORED = 'ignorado'

total_fixed = 0

# https://opendatasus.saude.gov.br/dataset/ae90fa8f-3e94-467e-a33f-94adbb66edf8/resource/8f571374-c555-4ec0-8e44-00b1e8b11c25/download/dicionario-de-dados-srag-hospitalizado-27.07.2020-final.pdf


class HospitalizationData(models.Model):
    dt_notific = models.BrDateField(rel='DT_NOTIFIC', help='data de preenchimento da ficha de notificação')
    dt_sin_pri = models.BrDateField(rel='DT_SIN_PRI', help='data de 1º sintomas do caso')
    dt_nasc = models.BrDateField(rel='DT_NASC', help='data de nascimento do paciente')
    dt_ut_dose = models.BrDateField(rel='DT_UT_DOSE', help='data da última dose de vacina contra gripe que o paciente tomou')
    dt_vac_mae = models.BrDateField(rel='DT_VAC_MAE', help='se a mãe recebeu vacina, qual a data?')
    dt_doseuni = models.BrDateField(rel='DT_DOSEUNI', help='se >= 6 meses e <= 8 anos, data da dose única para crianças vacinadas em campanhas de anos anteriores')
    dt_1_dose = models.BrDateField(rel='DT_1_DOSE', help='se >= 6 meses e <= 8 anos, data da 1ª dose para crianças vacinadas pela primeira vez')
    dt_2_dose = models.BrDateField(rel='DT_2_DOSE', help='se >= 6 meses e <= 8 anos data da 2ª dose para crianças vacinadas pela primeira vez')
    dt_antivir = models.BrDateField(rel='DT_ANTIVIR', help='data em que foi iniciado o tratamento com o antiviral.')
    dt_interna = models.BrDateField(rel='DT_INTERNA', help='data em que o paciente foi hospitalizado.')
    dt_entuti = models.BrDateField(rel='DT_ENTUTI', help='data de entrada do paciente na unidade de Terapia intensiva (UTI).')
    dt_saiduti = models.BrDateField(rel='DT_SAIDUTI', help='data em que o paciente saiu da Unidade de Terapia intensiva (UTI).')
    dt_raiox = models.BrDateField(rel='DT_RAIOX', help='se realizou RX de Tórax, especificar a data do exame.')
    dt_coleta = models.BrDateField(rel='DT_COLETA', help='data da coleta da amostra para realização do teste diagnóstico.')
    dt_pcr = models.BrDateField(rel='DT_PCR', help='data do Resultado RT-PCR/outro método por Biologia Molecular')
    dt_evoluca = models.BrDateField(rel='DT_EVOLUCA', help='data da alta ou óbito')
    dt_encerra = models.BrDateField(rel='DT_ENCERRA', help='data do encerramento do caso.')
    dt_digita = models.BrDateField(rel='DT_DIGITA', help='data de inclusão do registro no sistema.')
    dt_vgm = models.BrDateField(rel='DT_VGM', help='data em que foi realizada a viagem')
    dt_rt_vgm = models.BrDateField(rel='DT_RT_VGM', help='data em que retornou de viagem')
    dt_tomo = models.BrDateField(rel='DT_TOMO', help='se realizou tomografia, especificar a data do exame')
    dt_res_an = models.BrDateField(rel='DT_RES_AN', help='data do resultado do teste antigênico.')
    dt_co_sor = models.BrDateField(rel='DT_CO_SOR', help='data da coleta do material para diagnóstico por Sorologia.')
    dt_res = models.BrDateField(rel='DT_RES', help='data do Resultado do Teste Sorológico')
    evolucao = models.ChoiceField(rel='EVOLUCAO', help='Evolução do caso', choices={'1': CURE, '2': DEATH, '3': DEATH_OTHER_CAUSES, '9': IGNORED, '': IGNORED})
    sem_not = models.IntegerField(rel='SEM_NOT', help='Semana Epidemiológica do preenchimento da ficha de notificação')
    sem_pri = models.IntegerField(rel='SEM_PRI', help='Semana Epidemiológica do início dos sintomas')
    sg_uf_not = models.TextField(rel='SG_UF_NOT', help='Unidade Federativa onde está localizada a Unidade Sentinela que realizou a notificação.')
    id_regiona = models.TextField(rel='ID_REGIONA', help='Regional de Saúde onde está localizado o Município realizou a notificação.')
    co_regiona = models.IntegerField(rel='CO_REGIONA', help='Regional de Saúde onde está localizado o Município realizou a notificação.')
    id_municip = models.TextField(rel='ID_MUNICIP', help='Município onde está localizada a Unidade Sentinela que realizou a notificação.')
    co_mun_not = models.IntegerField(rel='CO_MUN_NOT', help='Município onde está localizada a Unidade Sentinela que realizou a notificação.')
    id_unidade = models.TextField(rel='ID_UNIDADE', help='Unidade Sentinela que realizou o atendimento, coleta de amostra e registro do caso')
    co_uni_not = models.IntegerField(rel='CO_UNI_NOT', help='Unidade Sentinela que realizou o atendimento, coleta de amostra e registro do caso')
    cs_sexo = models.ChoiceField(rel='CS_SEXO', help='Sexo do paciente.', choices={'M': 'masculino', 'F': 'feminino', 'I': IGNORED})
    nu_idade_n = models.IntegerField(rel='NU_IDADE_N', help='Idade informada pelo paciente quando não se sabe a data de nascimento. Na falta desse dado é registrada a idade aparente')
    tp_idade = models.ChoiceField(rel='TP_IDADE', help='tipo de dado informado no campo nu_idade_n .', choices={'1': 'dia', '2': 'mês', '3': 'ano'})
    cod_idade = models.TextField(rel='COD_IDADE', help='???')  # TODO: temos mais info para esse dado?
    cs_gestant = models.BoolChoiceField(rel='CS_GESTANT', nullable=True, help='Idade gestacional da paciente.',  choices={'1': True, '2': True, '3': True, '4': True, '5': False, '6': False, '9': None, '0': None})  # TODO: opção 0 não está no manual, mas vêm nos dados
    cs_gestant_type = models.TextField(help='Quando o cs_gestant for verdadeiro, qual foi o tipo?', default='')
    cs_raca = models.ChoiceField(rel='CS_RACA', help='Cor ou raça declarada pelo paciente: Branca; Preta; Amarela; Parda (pessoa que se declarou mulata, cabocla, cafuza, mameluca ou mestiça de preto com pessoa de outra cor ou raça); e, Indígena.', choices={'1': 'branca', '2': 'preta', '3': 'amarela', '4': 'parda', '5': 'indígena', '9': IGNORED, '': IGNORED})
    cs_etinia = models.TextField(rel='CS_ETINIA', help='Nome e código da etnia do paciente, quando indígena (cs_raca = 5-Indígena.)')
    cs_escol_n = models.ChoiceField(rel='CS_ESCOL_N', help='Nível de escolaridade do paciente. Para os níveis fundamental e médio deve ser considerada a última série ou ano concluído', choices={'0': 'Sem escolaridade/Analfabeto', '1': 'Fundamental 1º ciclo (1ª a 5ª série)', '2': 'Fundamental 2º ciclo (6ª a 9ª série)', '3': ' Médio (1º ao 3º ano)', '4': 'Superior', '5': 'Não se aplica', '9': IGNORED, '': IGNORED})
    id_pais = models.TextField(rel='ID_PAIS', help='País de residência do paciente.')
    co_pais = models.TextField(rel='CO_PAIS', help='País de residência do paciente.')
    sg_uf = models.TextField(rel='SG_UF', help='Unidade Federativa de residência do paciente')
    id_rg_resi = models.TextField(rel='ID_RG_RESI', help='Regional de Saúde onde está localizado o Município de residência do paciente.')
    co_rg_resi = models.TextField(rel='CO_RG_RESI', help='Regional de Saúde onde está localizado o Município de residência do paciente.')
    id_mn_resi = models.TextField(rel='ID_MN_RESI', help='Município de residência do paciente.')
    co_mun_res = models.IntegerField(rel='CO_MUN_RES', help='Município de residência do paciente.')
    cs_zona = models.ChoiceField(rel='CS_ZONA', help='Zona geográfica do endereço de residência do paciente.', choices={'1': 'Urbana', '2': 'Rural', '3': 'Periurbana', '9': IGNORED, '': IGNORED})
    surto_sg = models.BoolChoiceField(rel='SURTO_SG', help='Caso é proveniente de surto de SG?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    nosocomial = models.BoolChoiceField(rel='NOSOCOMIAL', nullable=True, help='Caso de SRAG com infecção adquirida após internação.', choices={'1': True, '2': False, '9': None, '': None})
    ave_suino = models.IntegerChoiceField(rel='AVE_SUINO', help='Caso com contato direto com aves ou suínos.', nullable=True, choices={'1': True, '2': False, '3': None, '9': None, '': None})   # TODO: opção 3 não está no manual, mas vêm nos dados
    febre = models.BoolChoiceField(rel='FEBRE', help='Paciente apresentou febre?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    tosse = models.BoolChoiceField(rel='TOSSE', help='Paciente apresentou tosse?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    garganta = models.BoolChoiceField(rel='GARGANTA', help='Paciente apresentou dor de garganta?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    dispneia = models.BoolChoiceField(rel='DISPNEIA', help='Paciente apresentou dispneia?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    desc_resp = models.BoolChoiceField(rel='DESC_RESP', help='Paciente apresentou desconforto respiratório?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    saturacao = models.BoolChoiceField(rel='SATURACAO', help='Paciente apresentou saturação O2<95%?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    diarreia = models.BoolChoiceField(rel='DIARREIA', help='Paciente apresentou diarreia?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    vomito = models.BoolChoiceField(rel='VOMITO', help='Paciente apresentou vômito?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    outro_sin = models.BoolChoiceField(rel='OUTRO_SIN', help='Paciente apresentou outro(s) sintoma(s)?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    outro_des = models.TextField(rel='OUTRO_DES', help='Listar outros sinais e sintomas')
    puerpera = models.BoolChoiceField(rel='PUERPERA', help='Paciente é puérpera ou parturiente (mulher que pariu recentemente – até 45 dias do parto)?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    fator_risc = models.BoolChoiceField(rel='FATOR_RISC', help='Paciente apresenta algum fator de risco', choices={'S': True, 'N': False})  # , 9: None})
    cardiopati = models.BoolChoiceField(rel='CARDIOPATI', help='Paciente possui Doença Cardiovascular Crônica?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    hematologi = models.BoolChoiceField(rel='HEMATOLOGI', help='Paciente possui Doença Hematológica Crônica?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    sind_down = models.BoolChoiceField(rel='SIND_DOWN', help='Paciente possui Síndrome de Down?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    hepatica = models.BoolChoiceField(rel='HEPATICA', help='Paciente possui Doença Hepática Crônica?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    asma = models.BoolChoiceField(rel='ASMA', help='Paciente possui Asma?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    diabetes = models.BoolChoiceField(rel='DIABETES', help='Paciente possui Diabetes mellitus?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    neurologic = models.BoolChoiceField(rel='NEUROLOGIC', help='Paciente possui Doença Neurológica?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    pneumopati = models.BoolChoiceField(rel='PNEUMOPATI', help='Paciente possui outra pneumopatia crônica?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    imunodepre = models.BoolChoiceField(rel='IMUNODEPRE', help='Paciente possui Imunodeficiência ou Imunodepressão (diminuição da função do sistema imunológico)?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    renal = models.BoolChoiceField(rel='RENAL', help='Paciente possui Doença Renal Crônica?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    obesidade = models.BoolChoiceField(rel='OBESIDADE', help='Paciente possui obesidade?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    obes_imc = models.TextField(rel='OBES_IMC', help='Valor do IMC (Índice de Massa Corporal) do paciente calculado pelo profissional de saúde (se sim no campo obesidade).')
    out_morbi = models.BoolChoiceField(rel='OUT_MORBI', help='Paciente possui outro(s) fator(es) de risco?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    morb_desc = models.TextField(rel='MORB_DESC', help='Listar outro(s) fator(es) de risco do paciente (se sim no campo out_morbi).')
    vacina = models.BoolChoiceField(rel='VACINA', help='Informar se o paciente foi vacinado contra gripe na última campanha, após verificar a documentação / caderneta. Caso o paciente não tenha a caderneta, direcionar a pergunta para ele ou responsável e preencher o campo com o código correspondente a resposta.', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    mae_vac = models.BoolChoiceField(rel='MAE_VAC', help='Se paciente < 6 meses, a mãe amamenta a criança?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    m_amamenta = models.BoolChoiceField(rel='M_AMAMENTA', help='Se paciente < 6 meses, a mãe amamenta a criança?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    antiviral = models.BoolChoiceField(rel='ANTIVIRAL', help='Fez uso de antiviral', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    tp_antivir = models.ChoiceField(rel='TP_ANTIVIR', help='Qual antiviral utilizado?', choices={'1': 'Oseltamivir', '2': 'Zanamivir', '3': 'Outro', '': 'Outro'})
    out_antiv = models.TextField(rel='OUT_ANTIV', help='Se o antiviral utilizado não foi Oseltamivir ou Zanamivir, informar qual antiviral foi utilizado (Habilitado se campo tp_antivir for igual a 3).')
    hospital = models.BoolChoiceField(rel='HOSPITAL', help='O paciente foi internado?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    sg_uf_inte = models.TextField(rel='SG_UF_INTE', help='Unidade Federativa de internação do paciente.')
    id_rg_inte = models.TextField(rel='ID_RG_INTE', help='Regional de Saúde onde está localizado o Município de internação do paciente.')
    co_rg_inte = models.TextField(rel='CO_RG_INTE', help='Regional de Saúde onde está localizado o Município de internação do paciente.')
    id_mn_inte = models.TextField(rel='ID_MN_INTE', help='Município onde está localizado a Unidade de Saúde onde o paciente internou.')
    co_mu_inte = models.TextField(rel='CO_MU_INTE', help='Município onde está localizado a Unidade de Saúde onde o paciente internou.')
    uti = models.BoolChoiceField(rel='UTI', help='O paciente foi internado em UTI?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    suport_ven = models.BoolChoiceField(rel='SUPORT_VEN', nullable=True, help='O paciente fez uso de suporte ventilatório?', choices={'1': True, '2': True, '3': False, '9': None, '': None})
    suport_ven_type = models.TextField(help='Quando suport_ven for verdadeiro, qual foi o tipo de suporte ventilatório usado?', nullable=True)
    raiox_res = models.ChoiceField(rel='RAIOX_RES', help='Informar resultado de Raio X de Tórax.', choices={'1': 'Normal', '2': 'Infiltrado intersticial', '3': 'Consolidação', '4': 'Misto', '5': 'Outro', '6': 'Não realizado', '9': IGNORED, '': IGNORED})
    raiox_out = models.TextField(rel='RAIOX_OUT', help='Informa o resultado do RX de tórax (se o campo raiox_res for 5).')
    amostra = models.BoolChoiceField(rel='AMOSTRA', help='Foi realizado coleta de amostra para realização de teste diagnóstico?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    tp_amostra = models.ChoiceField(rel='TP_AMOSTRA', help='Tipo da amostra clínica coletada para o teste diagnóstico.', choices={'1': 'Secreção de Nasoorofaringe', '2': 'Lavado Broco-alveolar', '3': 'Tecido post-mortem', '4': 'Outra, qual?', '5': 'LCR', '9': IGNORED, '': IGNORED})
    out_amost = models.TextField(rel='OUT_AMOST', help='Descrição do tipo da amostra clínica, caso diferente das listadas nas categorias do campo (habilitado se tp_amostra for 4).')
    pcr_resul = models.ChoiceField(rel='PCR_RESUL', help='Resultado do teste de RT-PCR/outro método por Biologia Molecular', choices={'1': 'Detectável', '2': 'Não Detectável', '3': 'Inconclusivo', '4': 'Não Realizado', '5': 'Aguardando Resultado', '9': IGNORED, '': IGNORED})
    pos_pcrflu = models.BoolChoiceField(rel='POS_PCRFLU', help='Resultado da RTPCR foi positivo para Influenza?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    tp_flu_pcr = models.ChoiceField(rel='TP_FLU_PCR', nullable=True, help='Resultado diagnóstico do RTPCR para o tipo de Influenza.', choices={'1': 'Influenza A', '2': 'Influenza B', '': None})
    pcr_fluasu = models.ChoiceField(rel='PCR_FLUASU', help='Subtipo para Influenza A.', choices={'1': 'Influenza A(H1N1)pdm09', '2': 'Influenza A (H3N2)', '3': 'Influenza A não subtipado', '4': 'Influenza A não subtipável', '5': 'Inconclusivo', '6': 'Outro, especifique:', '': ''})
    fluasu_out = models.TextField(rel='FLUASU_OUT', help='Outro subtipo para Influenza A (habilitado se tp_flu_pcr for 1).')
    pcr_flubli = models.ChoiceField(rel='PCR_FLUBLI', help='Linhagem para Influenza B.', choices={'1': 'Victoria', '2': 'Yamagatha', '3': 'Não realizado', '4': 'Inconclusivo', '5': 'Outro, especifique:', '': ''})
    flubli_out = models.TextField(rel='FLUBLI_OUT', help='Outra linhagem para Influenza B (habilitado se tp_flu_pcr for 2).')
    pos_pcrout = models.BoolChoiceField(rel='POS_PCROUT', help='Resultado da RTPCR foi positivo para outro vírus respiratório', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    pcr_vsr = models.BoolChoiceField(rel='PCR_VSR', help='Resultado diagnóstico do RTPCR para (VSR) (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_para1 = models.BoolChoiceField(rel='PCR_PARA1', help='Resultado diagnóstico do RTPCR para Parainfluenza 1 (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_para2 = models.BoolChoiceField(rel='PCR_PARA2', help='Resultado diagnóstico do RTPCR para Parainfluenza 2 (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_para3 = models.BoolChoiceField(rel='PCR_PARA3', help='Resultado diagnóstico do RTPCR para Parainfluenza 3 (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_para4 = models.BoolChoiceField(rel='PCR_PARA4', help='Resultado diagnóstico do RTPCR para Parainfluenza 4 (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_adeno = models.BoolChoiceField(rel='PCR_ADENO', help='Resultado diagnóstico do RTPCR para Adenovírus (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_metap = models.BoolChoiceField(rel='PCR_METAP', help='Resultado diagnóstico do RTPCR para Metapneumovírus (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_boca = models.BoolChoiceField(rel='PCR_BOCA', help='Resultado diagnóstico do RTPCR para Bocavírus (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_rino = models.BoolChoiceField(rel='PCR_RINO', help='Resultado diagnóstico do RTPCR para Rinovírus (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    pcr_sars2 = models.BoolChoiceField(rel='PCR_SARS2', help='Resultado diagnóstico do RTPCR para (SARS-CoV2).', choices={'1': True, '': False})
    pcr_outro = models.BoolChoiceField(rel='PCR_OUTRO', help='Resultado diagnóstico do RTPCR para Outro vírus respiratório (habilitado se pos_pcrout for 1).', choices={'1': True, '': False})
    ds_pcr_out = models.TextField(rel='DS_PCR_OUT', help='Nome do outro vírus respiratório identificado pelo RT-PCR')
    classi_fin = models.BoolChoiceField(rel='CLASSI_FIN', help='Diagnóstico final do caso. Se tiver resultados divergentes entre as metodologias laboratoriais, priorizar o resultado do RTPCR.', choices={'1': True, '2': True, '3': True, '4': True, '5': True, '': False})
    classi_fin_type = models.TextField(help='Quando o classi_fin for verdadeiro, qual foi o tipo?', default='')
    classi_out = models.TextField(rel='CLASSI_OUT', help='Descrição de qual outro agente etiológico foi identificado (Se campo classi_fin for 3).')
    criterio = models.ChoiceField(rel='CRITERIO', help='Indicar qual o critério de confirmação.', nullable=True, choices={'1': 'Laboratorial', '2': 'Clínico Epidemiológico', '3': 'Clínico', '4': 'Clínico Imagem', '': None})
    histo_vgm = models.ChoiceField(rel='HISTO_VGM', help='Paciente tem histórico de viagem internacional até 14 dias antes do início dos sintomas?', choices={'1': 'Sim', '2': 'Não', '9': IGNORED, '0': ''})
    pais_vgm = models.TextField(rel='PAIS_VGM', help='País onde foi realizada a viagem (se histo_vgm for sim).')
    co_ps_vgm = models.TextField(rel='CO_PS_VGM', help='???')  # TODO: temos mais informações sobre esse campo?
    lo_ps_vgm = models.TextField(rel='LO_PS_VGM', help='Local (cidade, estado, província e outros) onde foi realizada a viagem (habilitado se campo histo_vgm for sim)')
    pac_cocbo = models.TextField(rel='PAC_COCBO', help='Ocupação profissional do paciente')
    pac_dscbo = models.TextField(rel='PAC_DSCBO', help='Ocupação profissional do paciente')
    out_anim = models.TextField(rel='OUT_ANIM', help='Informar o animal que o paciente teve contato (se selecionado a opção 3).')
    dor_abd = models.BoolChoiceField(rel='DOR_ABD', help='Paciente apresentou dor abdominal?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    fadiga = models.BoolChoiceField(rel='FADIGA', help='Paciente apresentou fadiga?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    perd_olft = models.BoolChoiceField(rel='PERD_OLFT', help='Paciente apresentou perda do olfato?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    perd_pala = models.BoolChoiceField(rel='PERD_PALA', help='Paciente apresentou perda do paladar?', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    tomo_res = models.ChoiceField(rel='TOMO_RES', help='Informar o resultado da tomografia.', choices={'1': 'Tipico COVID-19', '2': 'Indeterminado COVID-19', '3': 'Atípico COVID-19', '4': 'Negativo para Pneumonia', '5': 'Outro', '6': 'Não realizado', '9': IGNORED, '': IGNORED})
    tomo_out = models.TextField(rel='TOMO_OUT', help='Informar o resultado da tomografia (se o campo tomo_res for 5')
    tp_tes_an = models.ChoiceField(rel='TP_TES_AN', help='Tipo do teste antigênico que foi realizado.', nullable=True, choices={'1': 'Imunofluorescência (IF)', '2': 'Teste rápido antigênico', '': None})
    res_an = models.ChoiceField(rel='RES_AN', help='Resultado do Teste Antigênico.', choices={'1': 'positivo', '2': 'Negativo', '3': 'Inconclusivo', '4': 'Não realizado', '5': 'Aguardando resultado', '9': IGNORED, '': IGNORED})
    pos_an_flu = models.BoolChoiceField(rel='POS_AN_FLU', help='Resultado do Teste Antigênico que foi positivo para Influenza', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    tp_flu_an = models.ChoiceField(rel='TP_FLU_AN', help='Resultado do Teste Antigênico, para o tipo de Influenza.', nullable=True, choices={'1': 'Influenza A', '2': 'Influenza B', '': None})
    pos_an_out = models.BoolChoiceField(rel='POS_AN_OUT', help='Resultado do Teste Antigênico, que foi positivo para outro vírus respiratório.', nullable=True, choices={'1': True, '2': False, '9': None, '': None})
    an_sars2 = models.BoolChoiceField(rel='AN_SARS2', help='Resultado do Teste Antigênico, para SARS-CoV-2 (habilitado se pos_an_out for 1).', choices={'1': True, '': False})
    an_vsr = models.BoolChoiceField(rel='AN_VSR', help='Resultado do Teste Antigênico, para VSR (habilitado se pos_an_out for 1).', choices={'1': True, '': False})
    an_para1 = models.BoolChoiceField(rel='AN_PARA1', help='Resultado do Teste Antigênico, para Parainfluenza 1 (habilitado se pos_an_out for 1).', choices={'1': True, '': False})
    an_para2 = models.BoolChoiceField(rel='AN_PARA2', help='Resultado do Teste Antigênico, para Parainfluenza 2 (habilitado se pos_an_out for 1).', choices={'1': True, '': False})
    an_para3 = models.BoolChoiceField(rel='AN_PARA3', help='Resultado do Teste Antigênico, para Parainfluenza 3 (habilitado se pos_an_out for 1).', choices={'1': True, '': False})
    an_adeno = models.BoolChoiceField(rel='AN_ADENO', help='Resultado do Teste Antigênico. Adenovírus. (habilitado se pos_an_out for 1).', choices={'1': True, '': False})
    an_outro = models.BoolChoiceField(rel='AN_OUTRO', help='Resultado do Teste Antigênico. Outro vírus respiratório (habilitado se pos_an_out for 1).', choices={'1': True, '': False})
    ds_an_out = models.TextField(rel='DS_AN_OUT', help='Nome do outro vírus respiratório identificado pelo Teste Antigênico (habilitado se pos_an_out for 1).')
    tp_am_sor = models.ChoiceField(rel='TP_AM_SOR', help='Tipo de amostra sorológica que foi coletada.', choices={'1': 'Sangue/plasma/soro', '2': 'Outra, qual?', '9': IGNORED, '': IGNORED})
    sor_out = models.TextField(rel='SOR_OUT', help='Descrição do tipo da amostra clínica, caso diferente das listadas na categoria um (1) do campo.')
    tp_sor = models.ChoiceField(rel='TP_SOR', help='Tipo do Teste Sorológico que foi realizado', nullable=True, choices={'1': 'Teste rápido', '2': 'Elisa', '3': 'Quimiluminescência', '4': 'Outro, qual', '': None})
    out_sor = models.TextField(rel='OUT_SOR', help='Descrição do tipo de Teste Sorológico (se campo tp_sor for 4).')
    res_igg = models.TextField(rel='RES_IGG', help='Resultado da Sorologia para SARS-CoV-2')
    res_igm = models.TextField(rel='RES_IGM', help='Resultado da Sorologia para SARS-CoV-2')
    res_iga = models.TextField(rel='RES_IGA', help='Resultado da Sorologia para SARS-CoV-2')

    # to hydrate fields
    dias_internacao_a_obito_srag = models.IntegerField(nullable=True)
    dias_internacao_a_obito_outras = models.IntegerField(nullable=True)
    dias_internacao_a_alta = models.IntegerField(nullable=True)
    faixa_etaria = models.TextField(nullable=True)
    idade = models.IntegerField(nullable=True)
    is_death = models.BoolField(default=False)
    is_cure = models.BoolField(default=False)

    def on_suport_ven(self, value, field, all_data):
        types = {'1': 'invasivo', '2': 'não invasivo'}
        self.suport_ven_type = types.get(value, None)
        return value

    def on_cs_gestant(self, value, field, all_data):
        types = {'1': 'primeiro trimestre', '2': 'segundo trimestre', '3': 'terceiro trimestre', '4': 'idade gestacional ignorada', '5': 'não', '6': 'não aplicável', '9': IGNORED, '0': ''}
        self.cs_gestant_type = types[value]
        return value

    def on_classi_fin(self, value, field, all_data):
        types = {'1': 'influenza', '2': 'outro vírus respiratório', '3': 'outro agente etiológico, qual:', '4': 'não especificado', '5': 'COVID-19', '': ''}
        self.classi_fin_type = types[value]
        if value in ['3']:
            self.classi_fin_type = all_data.get('CLASSI_OUT', '').lower()
        return value

    def on_receive_data(self, data):
        dates_fields = {}
        invalid_dates = {}
        fixed_dates = {}
        for key, value in data.items():
            if not key.startswith("DT_"):
                continue
            if len(value[value.rfind("/") + 1:]) == 3:  # looks like invalid date format
                invalid_dates[key] = value
                continue
            dates_fields[key] = value

        for key, value in invalid_dates.items():
            years = set()
            for other_key, other_value in dates_fields.items():
                if value != other_value and (other_value.startswith(value) or other_value.startswith(value[:value.rfind("/") + 1])):  # looks like we have a similar correct date
                    value = other_value
                    fixed_dates[key] = other_value
                    global total_fixed
                    total_fixed += 1
                    break
                elif other_key != key and len(other_value[other_value.rfind("/") + 1:]) == 4:
                    years.add(other_value[other_value.rfind("/") + 1:])
            if len(value[value.rfind("/") + 1:]) == 3 and len(years) == 0:  # looks like invalid date format yet
                fixed_dates[key] = value[: value.rfind("/")] + years[0]
                total_fixed += 1
            elif len(value[value.rfind("/") + 1:]):
                print('--------->', key, value, data)
                fixed_dates[key] = value[: value.rfind("/")] + "/2020"
                total_fixed += 1
        data.update(fixed_dates)
        return data

    def on_populate_finish(self, all_data):
        age = self.nu_idade_n.value
        if self.tp_idade in ['dia', 'mês']:
            age = 0
        if self.dt_notific.value and self.dt_nasc.value:
            age = int((self.dt_notific.value - self.dt_nasc.value).days/365.25)
        self.faixa_etaria = calculate_age_range(age)
        self.is_death = self.evolucao in [DEATH, DEATH_OTHER_CAUSES]
        self.is_cure = self.evolucao == CURE
        if self.dt_evoluca.value and self.dt_interna.value:
            diff_days = (self.dt_evoluca.value - self.dt_interna.value).days
            if self.is_cure:
                self.dias_internacao_a_alta = diff_days
            elif self.evolucao == DEATH:
                self.dias_internacao_a_obito_srag = diff_days
            elif self.evolucao == DEATH_OTHER_CAUSES:
                self.dias_internacao_a_obito_outras = diff_days


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


def worker(input, output):
    for args in iter(input.get, 'STOP'):
        result = convert_row(args)
        output.put(result)


def convert_row(row):
    return HospitalizationData(**row).serialize()

    # TODO: adicionar coluna ano e semana epidemiológica
    # TODO: corrigir RuntimeError: ERROR:  invalid input syntax for integer: "20-1"
    #                CONTEXT:  COPY srag, line 151650, column cod_idade: "20-1"
    # TODO: data nascimento (censurar?)
    # TODO: dt_interna: corrigir valores de anos inexistentes


class CsvLazyDictWriter(rows.utils.CsvLazyDictWriter):
    def __init__(self, fieldnames=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fobj = io.StringIO()
        if fieldnames is None:
            fieldnames = []
        self.fieldnames = fieldnames
        self.writer = csv.DictWriter(
            self.fobj,
            fieldnames=self.fieldnames,
            *self.writer_args,
            **self.writer_kwargs
        )
        self.writer.writeheader()

    def writerows(self, rows):
        self.writerows = self.writer.writerows
        return self.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--skip", help="Skip downloaded files",  action="store_true")
    parser.add_argument("-p", "--processes", help="How many processes should be used to process file", type=int, default=1)
    parser.add_argument("-l", "--lines", help="How many lines should be writed at time", type=int, default=1000)
    args = parser.parse_args()

    filenames = download_files(args.skip)
    output_filename = OUTPUT_PATH / "internacao_srag-batch.csv.gz"
    writer = CsvLazyDictWriter(HospitalizationData.fieldnames(), output_filename)

    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    # Start worker processes
    for i in range(args.processes):
        Process(target=worker, args=(task_queue, done_queue)).start()

    for filename in filenames:
        with rows.utils.open_compressed(filename, encoding="utf-8", buffering=100000) as fobj:
            reader = csv.DictReader(fobj, delimiter=";")
            count = 0
            # lines = []
            for row in tqdm(reader, desc=f"Converting {filename.name}"):
                count += 1
                task_queue.put(row)
                # if count >= args.lines:
                #     for i in range(count):
                #         line = done_queue.get()
                #         lines.append(line)
                #     writer.writerows(lines)
                #     lines = []
                #     count = 0
            # if lines:
            #     writer.writerows(lines)

    lines = []
    for i in tqdm(range(count), desc=f"Writing {output_filename}"):
        lines.append(done_queue.get())
        if len(lines) >= args.lines:
            writer.writerows(lines)
    if lines:
        writer.writerows(lines)

    for i in range(args.processes):
        task_queue.put('STOP')

        #     # parsed_data = convert_row(row)
        #     batch_rows.append(parsed_data)
        #     if len(batch_rows) >= args.lines:
        #         print('--->', len(batch_rows))
        # if batch_rows:
        #     writer.writerows(batch_rows)


if __name__ == "__main__":
    main()
