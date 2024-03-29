import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW
from Service import materiaPrimaService
## Nesse arquivo.py está a modelagem referente aos produtos projetados na PROJECAO DE CUSTOS

def ProdutosCsw(projecao, empresa):
    conn = ConexaoCSW.Conexao() # Abrindo a COnexao com o CSW

    ano = projecao[-2:]
    ano = "'%"+ano+"%'"

    if 'ALT' in projecao:

        produtos = 'SELECT e.codEngenharia as codengenharia , e.descricao  FROM tcp.Engenharia e ' \
    'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
    "WHERE e.codEmpresa = 1 and d.nomeColecao like '%ALTO VER%' AND  d.nomeColecao like " +ano+ " and e.status in (2,3) " \
    " and e.codEngenharia like '0%' "


        projecaoCSW = 'SELECT  t.codProduto as codengenharia, capa.dataCalculo as dataprojecao  FROM CusTex_Tpc.CProduto t ' \
                   'inner join CusTex_Tpc.CProdCapa capa on capa.codempresa = t.codEmpresa  and capa.numeroProj  = t.numeroProj ' \
                   "WHERE capa.descProjecao like " + ano + " and  capa.descProjecao like '%ALTO VER%' and capa.codempresa= 1"


        grade = 'SELECT d.codEngenharia as  codengenharia, g.codgrade as grade  from tcp.GradesEngenharia  g ' \
                   ' inner join tcp.DadosGeraisEng d on d.codEmpresa = g.Empresa  and d.codEngenharia = g.codEngenharia ' \
                " where d.codEmpresa = 1 and d.nomeColecao like '%ALTO VER%' AND  d.nomeColecao like " +ano+ ""

        precoCastrado = 'SELECT I.codProduto as codengenharia, I.codFaixa as grade, I.precoTabelaFloat as precoCSW  FROM ped.TabelaPreco p ' \
                        'INNER JOIN PED.TabelaPrecoItem I ON I.codEmpresa = p.codEmpresa and I.codTabela = p.codTabela ' \
                        "WHERE p.codEmpresa = 1 and p.descricao like '%ALTO VER%' AND p.descricao like " +ano+ " and codFaixa <> '0'"


    elif 'VER' in projecao:
        produtos = 'SELECT e.codEngenharia as codengenharia, e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like 'VER%' AND  d.nomeColecao like " + ano + " and e.status in (2,3) " \
                                                                                                           " and e.codEngenharia like '0%' "

        projecaoCSW = 'SELECT  t.codProduto as codengenharia, capa.dataCalculo as dataprojecao  FROM CusTex_Tpc.CProduto t ' \
                   'inner join CusTex_Tpc.CProdCapa capa on capa.codempresa = t.codEmpresa  and capa.numeroProj  = t.numeroProj ' \
                   "WHERE capa.descProjecao like " + ano + " and  capa.descProjecao like '%VER%' and capa.codempresa= 1"

        grade = 'SELECT d.codEngenharia as  codengenharia, g.codgrade as grade  from tcp.GradesEngenharia  g ' \
                   ' inner join tcp.DadosGeraisEng d on d.codEmpresa = g.Empresa  and d.codEngenharia = g.codEngenharia ' \
                " where d.codEmpresa = 1 and d.nomeColecao like 'VER%' AND  d.nomeColecao like " +ano+ ""

        precoCastrado = 'SELECT I.codProduto as codengenharia, I.codFaixa as grade, I.precoTabelaFloat  as precoCSW FROM ped.TabelaPreco p ' \
                        'INNER JOIN PED.TabelaPrecoItem I ON I.codEmpresa = p.codEmpresa and I.codTabela = p.codTabela ' \
                        "WHERE p.codEmpresa = 1 and p.descricao like '%VER%' AND p.descricao like " +ano+ " and codFaixa <> '0'"

    else:
        produtos = 'SELECT e.codEngenharia as codengenharia , e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like '%INVE%' AND  d.nomeColecao like " + ano + " and e.status in (2,3)"\
    " and e.codEngenharia like '0%' "

        projecaoCSW = 'SELECT  t.codProduto as codengenharia, capa.dataCalculo as dataprojecao   FROM CusTex_Tpc.CProduto t ' \
                   'inner join CusTex_Tpc.CProdCapa capa on capa.codempresa = t.codEmpresa  and capa.numeroProj  = t.numeroProj ' \
                   "WHERE capa.descProjecao like " + ano + " and  capa.descProjecao like '%INVE%' and capa.codempresa= 1"

        grade = 'SELECT d.codEngenharia as  codengenharia, g.codgrade as grade  from tcp.GradesEngenharia  g ' \
                   ' inner join tcp.DadosGeraisEng d on d.codEmpresa = g.Empresa  and d.codEngenharia = g.codEngenharia ' \
                " where d.codEmpresa = 1 and d.nomeColecao like 'INVE%' AND  d.nomeColecao like " +ano+ ""

        precoCastrado = 'SELECT I.codProduto as codengenharia, I.codFaixa as grade, I.precoTabelaFloat as precoCSW  FROM ped.TabelaPreco p ' \
                        'INNER JOIN PED.TabelaPrecoItem I ON I.codEmpresa = p.codEmpresa and I.codTabela = p.codTabela ' \
                        "WHERE p.codEmpresa = 1 and p.descricao like '%INVE%' AND p.descricao like " +ano+ " and codFaixa <> '0'"

    basico = 'SELECT e.codEngenharia as codengenharia , e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like '%BASIC%'  and e.status in (2,3)"\
    " and e.codEngenharia like '0%' AND e.codEngenharia like '%-0'  "

    gradebASICO = 'SELECT d.codEngenharia as  codengenharia, g.codgrade as grade  from tcp.GradesEngenharia  g ' \
            ' inner join tcp.DadosGeraisEng d on d.codEmpresa = g.Empresa  and d.codEngenharia = g.codEngenharia ' \
            " where d.codEmpresa = 1 and d.nomeColecao like 'BASIC%' "


    produtos_ = pd.read_sql(produtos,conn)
    grade = pd.read_sql(grade, conn)
    produtos_ = pd.merge(produtos_, grade, on='codengenharia', how='left')
    produtos_['origem'] = 'Lancamento'


    basico = pd.read_sql(basico,conn)
    gradebASICO = pd.read_sql(gradebASICO, conn)
    basico = pd.merge(basico,gradebASICO,on='codengenharia', how='left')
    basico['origem'] = 'Continuadas'

    produtos_ = pd.concat([produtos_, basico])



    projecaoCSW = pd.read_sql(projecaoCSW, conn)
    projecaoCSW['situacaocusto'] = 'Projetado'
    produtos_ = pd.merge(produtos_, projecaoCSW , on='codengenharia', how='left')

    precoCastrado = pd.read_sql(precoCastrado, conn)

    precoCastrado['codengenharia'] = precoCastrado['codengenharia'].astype(str)
    precoCastrado['codengenharia'] = '0'+precoCastrado['codengenharia'] +'-0'
    precoCastrado['grade'] = precoCastrado['grade'].str.extract(r'\/(.+)$')
    precoCastrado['grade'] = precoCastrado.apply(
        lambda row: obterGrade(row['grade']), axis=1)





    conn.close()
    produtos_['grade'] = produtos_.apply(
        lambda row: obterGrade(row['grade']), axis=1)
    produtos_ = pd.merge(produtos_, precoCastrado, on=['codengenharia', 'grade'], how='left')
    produtos_['categoria'] = '-'
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('BLAZER', row['descricao'], 'JAQUETA', row['categoria']), axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('TSHIRT', row['descricao'], 'CAMISETA', row['categoria']), axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('BATA', row['descricao'], 'CAMISA', row['categoria']), axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('CAMISA', row['descricao'], 'CAMISA', row['categoria']), axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('POLO', row['descricao'], 'POLO', row['categoria']), axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('CALCA JEANS', row['descricao'], 'CALCA JEANS', row['categoria']), axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('SHORT', row['descricao'], 'BOARDSHORT', row['categoria']),
                                     axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('CARTEIRA', row['descricao'], 'CARTEIRA', row['categoria']),
                                     axis=1)
    produtos_['categoria'] = produtos_.apply(lambda row: Categoria('MEIA', row['descricao'], 'MEIA', row['categoria']), axis=1)

    produtos_['projecao'] = projecao
    produtos_['marca'] = produtos_.apply(lambda  row: ObtendoMarca(row['codengenharia']),axis=1)
    produtos_['empresa'] = produtos_.apply(lambda row: ObetendoEmpresa(row['codengenharia']), axis=1)
    produtos_['grupo'] = produtos_.apply(lambda  row: obterGrupo(row['descricao']),axis=1)
    produtos_['estrategia'] = produtos_.apply(lambda  row: obterEstrategia(row['descricao']),axis=1)
    produtos_['criterio'] ='MaiorCor'


    return produtos_
def ObtendoMarca(coditempai):
    if coditempai[1:4] == '102':
        return 'M.POLLO'
    elif coditempai[1:4] == '202':
        return 'M.POLLO'
    elif coditempai[1:4] == '302':
        return 'M.POLLO'
    elif coditempai[1:4] == '104':
        return 'PACO'
    elif coditempai[1:4] == '204':
        return 'PACO'
    elif coditempai[1:4] == '304':
        return 'PACO'
    else:
        return '-'

def ObetendoEmpresa(coditempai):
    if coditempai[1:2] == '1':
        return 'MATRIZ'
    elif coditempai[1:2] == '2':
        return 'MATRIZ'

    else:
        return 'FILIAL'

def obterGrupo(descricao):
    if 'PACK' in descricao:
        return 'ESSENCIAL ALTO/GIRO'
    elif ' CB' in descricao:
        return 'ESSENCIAL ALTO/GIRO'
    else:
        return '-'

def obterEstrategia(descricao):
    if 'PACK' in descricao:
        return 'PACK'
    elif ' CB' in descricao:
        return 'COMBO'
    else:
        return '-'


def obterGrade(grade):
    if grade in [6,14,16,17,23,24,25,27,28,36,37,40,41,58]:
        return 'FILHO'
    else:
        return 'PAI'

def IncrementarProdutos(projecao, empresa):

    for p in projecao:
        conn = ConexaoPostgreMPL.conexao()
        consultaStatus = pd.read_sql('select situacao from "Reposicao"."ProjCustos".projecao '
                                     'where nome = %s ',conn, params=(p,))

        if consultaStatus['situacao'][0] == 'INICIADA':

            print(f'colecao {p} ja Iniciou vendas')
        else:

            delete = 'delete from "Reposicao"."ProjCustos".produtos ' \
                     ' where projecao = %s'


            cursor = conn.cursor()
            cursor.execute(delete,(p,))
            conn.commit()
            cursor.close()
            conn.close()




            ObeterProdutos = ProdutosCsw(p, empresa)
            ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutos,ObeterProdutos.size,'produtos','append')




def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia:
        return valorNovo
    else:
        return categoria



# Funcao utilizada Para Listar os Produtos com os filtros selecidonados
def ObterProdutosOficial(projecao, empresa, categoria, marca, grupo):
    produtos_concatenados = None  # Inicialize como None o data frame

    conn = ConexaoPostgreMPL.conexao()  # Abra a conexão fora do loop

    for p in projecao:
        ConsultaRestricoes(p)
        produtosPostgre_query = 'select * from "Reposicao"."ProjCustos".produtos p ' \
                                'where projecao = %s '

        produtosPostgre = pd.read_sql(produtosPostgre_query, conn, params=(p,))
        produtosPostgre['situacaocusto'].fillna('Não Calculado', inplace=True)
        produtosPostgre.fillna('-', inplace=True)


        if not produtosPostgre.empty:
            if produtos_concatenados is None:
                produtos_concatenados = produtosPostgre.copy()
            else:
                produtos_concatenados = pd.concat([produtos_concatenados, produtosPostgre], ignore_index=True)

    conn.close()# Fechar a conexao fora do loop

    if produtos_concatenados is None:
        print('Nenhum dado encontrado')
        return None
    else:
        produtos_concatenados = FuncaoFiltro(empresa,produtos_concatenados,'empresa')
        produtos_concatenados = FuncaoFiltro(categoria, produtos_concatenados, 'categoria')
        produtos_concatenados = FuncaoFiltro(marca, produtos_concatenados, 'marca')
        produtos_concatenados = FuncaoFiltro(grupo, produtos_concatenados, 'grupo')
        resumoCusto = materiaPrimaService.ResumirCustoSortimento(projecao)
        resumoCusto.drop('repeticao', axis=1, inplace=True)
        produtos_concatenados = pd.merge(produtos_concatenados,resumoCusto,on=['projecao','codengenharia','grade','criterio'],how='left')

        produtos_concatenados.fillna('-',inplace=True)
        return produtos_concatenados


def FuncaoFiltro(valores, dataframe, nomeColuna):
    if valores == '-':
        return dataframe
    elif valores == [] or valores == ['-']:
        return dataframe
    else:

        dataframeVetor = None
        for p in valores:

            dataframeFiltrado = dataframe[dataframe[nomeColuna] == p]
            dataframeVetor = pd.concat([dataframeVetor,dataframeFiltrado])
        return dataframeVetor

def RestricaoEngenharia(engenharia, obs, usuario, projecao):


    for p in projecao:


        validarPreco = ConsultaPrecoCSW(engenharia, p)

        if validarPreco == 'permite':
            conn = ConexaoPostgreMPL.conexao()

            inserir = 'insert into "Reposicao"."ProjCustos".restricaoengenharia ' \
                      '(codengenharia, obs, usuario, projecao) values (%s , %s , %s , %s )'

            cursor = conn.cursor()
            cursor.execute(inserir, (engenharia, obs, usuario, p))
            conn.commit()
            cursor.close()
            conn.close()

            conn.close()

            x , y = ConsultaCadastroItensCSW(engenharia)

            mensagem = []
            if y == 0:
                mensagem.append(f'Engenharia {engenharia} excluida da projecao {projecao}')
            else:
                mensagem.append(f'erro na {engenharia} projecao {projecao} existe itens com situacao normal no AFV, verifique com o PCP')


        else:
            mensagem.append(f'erro a Engenharia {engenharia} , projecao {projecao} ,possui preço de venda no CSW, '
                                              f'solicite a retira dela da tabela de preços')
    return pd.DataFrame([{'MENSAGEM':mensagem}])

def ConsultaCadastroItensCSW(engenharia):

    coditempai = engenharia[1:9]
    conn = ConexaoCSW.Conexao()

    consulta = "Select coditem as reduzido, codcor from cgi.item2 where empresa = 1 and coditempai = " +coditempai+""

    consultaAfvBloqueio = "select reduzido as reduzido, 'bloqueado' as situacao from Asgo_Afv.EngenhariasBloqueadas  b where b.codempresa = 1 and b.codengenharia = '" + engenharia+"'"

    consulta = pd.read_sql(consulta,conn)


    consultaAfvBloqueio = pd.read_sql(consultaAfvBloqueio, conn)
    conn.close()


    if consultaAfvBloqueio.empty:
        consulta['situacao'] = '-'

    else:
        consulta = pd.merge(consulta, consultaAfvBloqueio, on='reduzido', how='left')
    consulta.fillna('-', inplace=True)
    Normais = consulta[consulta['situacao'] == '-']
    qtItensNormais = Normais['situacao'].count()

    return consulta, qtItensNormais


def ConsultaPrecoCSW(engenharia , projecao):

    ano = projecao[-2:]
    ano = "'%"+ano+"%'"
    coditempai = engenharia[1:9]

    conn = ConexaoCSW.Conexao()
    if 'ALT' in projecao:
        precoCastrado = 'SELECT I.codProduto as codengenharia, I.codFaixa as grade, I.precoTabelaFloat as precoCSW  FROM ped.TabelaPreco p ' \
                        'INNER JOIN PED.TabelaPrecoItem I ON I.codEmpresa = p.codEmpresa and I.codTabela = p.codTabela ' \
                        "WHERE p.codEmpresa = 1 and p.descricao like '%ALTO%' AND p.descricao like " + ano + " and codFaixa <> '0' " \
                                                                                                             "And codProduto = '"+coditempai+"'"

    elif 'VER' in projecao:
        precoCastrado = 'SELECT I.codProduto as codengenharia, I.codFaixa as grade, I.precoTabelaFloat as precoCSW  FROM ped.TabelaPreco p ' \
                        'INNER JOIN PED.TabelaPrecoItem I ON I.codEmpresa = p.codEmpresa and I.codTabela = p.codTabela ' \
                        "WHERE p.codEmpresa = 1 and p.descricao like '%VER%' AND p.descricao like " + ano + " and codFaixa <> '0'" \
                                                                                                             "And codProduto = '"+coditempai+"'"
    else:
        precoCastrado = 'SELECT I.codProduto as codengenharia, I.codFaixa as grade, I.precoTabelaFloat as precoCSW  FROM ped.TabelaPreco p ' \
                        'INNER JOIN PED.TabelaPrecoItem I ON I.codEmpresa = p.codEmpresa and I.codTabela = p.codTabela ' \
                        "WHERE p.codEmpresa = 1 and p.descricao like '%INVER%' AND p.descricao like " + ano + " and codFaixa <> '0'" \
                                                                                                              "And codProduto = '" + coditempai + "'"


    precoCastrado = pd.read_sql(precoCastrado, conn)
    conn.close()

    if precoCastrado.empty:
        return 'permite'
    else:
        return 'bloqueia'

def ConsultaRestricoes(projecao):
    conn = ConexaoPostgreMPL.conexao()

    consulta = 'Select codengenharia from "Reposicao"."ProjCustos".restricaoengenharia ' \
               'where  projecao = %s '
    consulta = pd.read_sql(consulta,conn,params=(projecao,))


    delete = 'delete from "Reposicao"."ProjCustos".produtos ' \
             'where codengenharia in (' \
             'Select codengenharia from "Reposicao"."ProjCustos".restricaoengenharia  where projecao = %s ) ' \
             ' and  projecao = %s '

    cursor = conn.cursor()
    cursor.execute(delete,(projecao,projecao,))
    conn.commit()

    conn.close()


    return consulta
