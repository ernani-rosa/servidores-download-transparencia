import pandas as pd
import numpy as np
import os
import joblib

def employee_parser(emp):

    flat_employee = dict()
    flat_employee['servidor_id'] = str(emp['servidor']['id'])
    flat_employee['servidor_idAposentadoPensionista'] = str(emp['servidor']['idServidorAposentadoPensionista'])
    flat_employee['servidor_pessoa_id'] = str(emp['servidor']['pessoa']['id'])
    flat_employee['nome'] = emp['servidor']['pessoa']['nome']
    flat_employee['servidor_pessoa_tipo'] = emp['servidor']['pessoa']['tipo']
    #flat_employee['situacao'] = emp['servidor']['situacao'] #All velues recovered were '1'
    flat_employee['servidor_orgaoLotacao_codigo'] = str(emp['servidor']['orgaoServidorLotacao']['codigo'])
    #flat_employee['cod_orgaoLotacaoVinculado'] = str(emp['servidor']['orgaoServidorLotacao']['codigoOrgaoVinculado'])
    flat_employee['servidor_orgaoExercicio_codigo'] = str(emp['servidor']['orgaoServidorExercicio']['codigo'])
    #flat_employee['cod_orgaoExercicioVinculado'] = str(emp['servidor']['orgaoServidorExercicio']['codigoOrgaoVinculado'])
    flat_employee['servidor_estadoExercicio'] = emp['servidor']['estadoExercicio']['nome']
    flat_employee['servidor_tipoServidor'] = emp['servidor']['tipoServidor']
    flat_employee['servidor_flagAfastado'] = emp['servidor']['flagAfastado']

    if len(emp['fichasCargoEfetivo']):        
        flat_employee['cargo_uorgLotacao'] = emp['fichasCargoEfetivo'][0]['uorgLotacao']
        flat_employee['cargo_dataIngressoOrgao'] = emp['fichasCargoEfetivo'][0]['dataIngressoOrgao']
        flat_employee['cargo_cargo'] = emp['fichasCargoEfetivo'][0]['cargo']
        flat_employee['cargo_dataIngressoCargo'] = emp['fichasCargoEfetivo'][0]['dataIngressoCargo']
        flat_employee['cargo_dataIngressoServicoPublico'] = emp['fichasCargoEfetivo'][0]['dataIngressoServicoPublico']

    else:
        flat_employee['cargo_uorgLotacao'] = np.nan
        flat_employee['cargo_dataIngressoOrgao'] = np.nan
        flat_employee['cargo_cargo'] = np.nan
        flat_employee['cargo_dataIngressoCargo'] = np.nan
        flat_employee['cargo_dataIngressoServicoPublico'] = np.nan
 

    if len(emp['fichasFuncao']):
        flat_employee['funcao'] = emp['fichasFuncao'][0]['funcao']
        flat_employee['funcao_atividade'] = emp['fichasFuncao'][0]['atividade']
        flat_employee['funcao_dataIngressoOrgao'] = emp['fichasFuncao'][0]['dataIngressoOrgao']
    else:
        flat_employee['funcao'] = np.nan
        flat_employee['funcao_atividade'] = np.nan
        flat_employee['funcao_dataIngressoOrgao'] = np.nan

    if len(emp['fichasMilitar']):
        pass

    if len(emp['fichasDemaisSituacoes']):
        flat_employee['demaisSituacoes'] = emp['fichasDemaisSituacoes'][0]['situacaoServidor']
        flat_employee['demaisSituacoes_dataIngressoServicoPublico'] = emp['fichasDemaisSituacoes'][0]['dataIngressoServicoPublico']
    else:
        flat_employee['demaisSituacoes'] = np.nan
        flat_employee['demaisSituacoes_dataIngressoServicoPublico'] = np.nan

    return flat_employee

def main() -> None:
    pickle_list = os.listdir("./pickles/")

    employee_list = []
    for progress,pickle in enumerate(pickle_list):
        pickled_employees = joblib.load(f"./pickles/{pickle}")
        print("Loading pickles... {:.2f}%".format(100*progress/len(pickle_list)),end="\r")
        for employee in pickled_employees:
            employee_list.append(employee_parser(employee))
    print("Done!")

    print("Saving .csv...",end='')
    df = pd.DataFrame(employee_list)
    df.to_csv("./parsed_employees.csv",index=False)
    print("done!")
    

if __name__=="__main__":
    main()


'''
servidor
{'id': 131647499,
 'idServidorAposentadoPensionista': 75424744,
 'pessoa': {'id': 5213607,'cpfFormatado': '***.198.123-**',  'cnpjFormatado': '',  'numeroInscricaoSocial': '',  'nome': 'LUCIANA DE AMORIM PARGA MARTINS ARAUJO',
            'razaoSocialReceita': '',  'nomeFantasiaReceita': '',  'tipo': 'Pessoa F??sica'},
 'situacao': 'Ativo',
 'orgaoServidorLotacao': {'codigo': '26272',  'nome': 'Funda????o Universidade Federal do Maranh??o',  'sigla': 'UFMA',  'codigoOrgaoVinculado': '15000',  'nomeOrgaoVinculado': 'Minist??rio da Educa????o'},
 'orgaoServidorExercicio': {'codigo': '12002',  'nome': 'Justi??a Federal - Amap??',  'sigla': 'JF/AP',  'codigoOrgaoVinculado': '00000',  'nomeOrgaoVinculado': 'Sem informa????o'},
 'estadoExercicio': {'sigla': '-1', 'nome': 'Sem informa????o'},
 'tipoServidor': 'Civil',
 'funcao': {'codigoFuncaoCargo': '-1',  'descricaoFuncaoCargo': 'Sem informa????o'},
 'servidorInativoInstuidorPensao': {'id': -2,  'cpfFormatado': '',  'nome': 'N??o se aplica'},
 'pensionistaRepresentante': {'id': -2,  'cpfFormatado': '',  'nome': 'N??o se aplica'},
 'codigoMatriculaFormatado': '109****',
 'flagAfastado': 1}

fichasCargoEfetivo
[{'nome': 'AMANDA LINS BRITO FANECO AMORIM',
  'cpfDescaracterizado': '***.617.744-**',
  'matriculaDescaracterizada': '134****',
  'dataPublicacaoDocumentoIngressoServicoPublico': '24/11/2016',
  'diplomaLegal': 'PORTARIA',
  'jornadaTrabalho': '40 HORAS SEMANAIS',
  'regimeJuridico': 'REGIME JURIDICO UNICO',
  'situacaoServidor': 'ATIVO EM OUTRO ORGAO',
  'afastamentos': ['Desde 14/10/2021'],
  'orgaoSuperiorLotacao': 'Advocacia-Geral da Uni??o',
  'orgaoLotacao': 'Advocacia-Geral da Uni??o - Unidades com v??nculo direto',
  'uorgLotacao': 'PROCURADORIA-GERAL DA UNIAO',
  'orgaoServidorLotacao': 'Advocacia-Geral da Uni??o',
  'dataIngressoOrgao': '24/11/2016',
  'dataIngressoServicoPublico': '05/12/2016',
  'orgaoSuperiorExercicio': 'Superior Tribunal de Justi??a',
  'orgaoExercicio': 'Superior Tribunal de Justi??a - Unidades com v??nculo direto',
  'orgaoServidorExercicio': 'Superior Tribunal de Justi??a',
  'uorgExercicio': 'Sem Informa????o',
  'cargo': 'ADVOGADO DA UNIAO',
  'classeCargo': '1',
  'padraoCargo': 'CAT',
  'nivelCargo': '',
  'dataIngressoCargo': '23/01/2017',
  'formaIngresso': 'NOMEACAO CARATER EFETIVO,ART.9,ITEM I ,LEI 8112/90',
  'ufExercicio': 'Sem informa????o'}]

fichasFuncao
[{'nome': 'PAULO SERGIO BARBOSA',
  'cpfDescaracterizado': '***.316.961-**',
  'matriculaDescaracterizada': '321****',
  'dataPublicacaoDocumentoIngressoServicoPublico': None,
  'diplomaLegal': 'PORTARIA',
  'jornadaTrabalho': '40 HORAS SEMANAIS',
  'regimeJuridico': 'REGIME JURIDICO UNICO',
  'situacaoServidor': 'NOMEADO CARGO COMIS.',
  'afastamentos': [],
  'orgaoSuperiorLotacao': 'Minist??rio da Agricultura, Pecu??ria e Abastecimento',
  'orgaoLotacao': 'Minist??rio da Agricultura, Pecu??ria e Abastecimento - Unidades com v??nculo direto',
  'uorgLotacao': 'Sem Informa????o',
  'orgaoServidorLotacao': 'Minist??rio da Agricultura, Pecu??ria e Abastecimento',
  'dataIngressoOrgao': '09/11/2020',
  'dataIngressoServicoPublico': 'Inv??lido',
  'orgaoSuperiorExercicio': 'Minist??rio da Agricultura, Pecu??ria e Abastecimento',
  'orgaoExercicio': 'Minist??rio da Agricultura, Pecu??ria e Abastecimento - Unidades com v??nculo direto',
  'uorgExercicio': 'DIV EXECUCAO FINANCEIRA',
  'orgaoServidorExercicio': 'Minist??rio da Agricultura, Pecu??ria e Abastecimento',
  'funcao': 'DAS 101.2 - DIRECAO E ASSESSORAMENTO SUPERIOR',
  'atividade': 'CHEFE DE DIVISAO',
  'opcaoFuncao': 'N??o',
  'dataIngressoFuncao': '09/11/2020',
  'ufExercicio': 'DISTRITO FEDERAL'}]


fichasDemaisSituacoes
[{'nome': 'ZAIRA ANISLEN FERREIRA MOUTINHO',
  'cpfDescaracterizado': '***.615.776-**',
  'matriculaDescaracterizada': '138****',
  'dataPublicacaoDocumentoIngressoServicoPublico': '16/03/2017',
  'diplomaLegal': 'PORTARIA',
  'jornadaTrabalho': '40 HORAS SEMANAIS',
  'regimeJuridico': 'REGIME JURIDICO UNICO',
  'situacaoServidor': 'EXERC. 7  ART93 8112',
  'afastamentos': [],
  'orgaoSuperiorLotacao': 'Minist??rio da Defesa',
  'orgaoLotacao': 'Comando do Ex??rcito',
  'uorgLotacao': 'Inv??lido',
  'orgaoServidorLotacao': 'Comando do Ex??rcito',
  'dataIngressoOrgao': '17/05/2021',
  'dataIngressoServicoPublico': '04/04/2017'}]
''' 