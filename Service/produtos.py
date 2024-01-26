import pandas as pd
import ConexaoPostgreMPL
import ConexaoCSW



def ProdutosCsw(projecao, empresa):
    conn = ConexaoCSW.Conexao()

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

    elif 'VER' in projecao:
        produtos = 'SELECT e.codEngenharia as codengenharia, e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like 'VER%' AND  d.nomeColecao like " + ano + " and e.status in (2,3) " \
                                                                                                           " and e.codEngenharia like '0%' "

        projecaoCSW = 'SELECT  t.codProduto as codengenharia, capa.dataCalculo as dataprojecao  FROM CusTex_Tpc.CProduto t ' \
                   'inner join CusTex_Tpc.CProdCapa capa on capa.codempresa = t.codEmpresa  and capa.numeroProj  = t.numeroProj ' \
                   "WHERE capa.descProjecao like " + ano + " and  capa.descProjecao like 'VER%' and capa.codempresa= 1"

        grade = 'SELECT d.codEngenharia as  codengenharia, g.codgrade as grade  from tcp.GradesEngenharia  g ' \
                   ' inner join tcp.DadosGeraisEng d on d.codEmpresa = g.Empresa  and d.codEngenharia = g.codEngenharia ' \
                " where d.codEmpresa = 1 and d.nomeColecao like 'VER%' AND  d.nomeColecao like " +ano+ ""

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

    conn.close()
    produtos_['grade'] = produtos_.apply(
        lambda row: obterGrade(row['grade']), axis=1)
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

def ObterProdutosOficial(projecao, empresa, categoria, marca, grupo):
    produtos_concatenados = None  # Inicialize como None

    conn = ConexaoPostgreMPL.conexao()  # Abra a conexão fora do loop

    for p in projecao:
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

    conn.close()

    if produtos_concatenados is None:
        print('Nenhum dado encontrado')
        return None
    else:
        produtos_concatenados = FuncaoFiltro(empresa,produtos_concatenados,'empresa')
        produtos_concatenados = FuncaoFiltro(categoria, produtos_concatenados, 'categoria')
        produtos_concatenados = FuncaoFiltro(marca, produtos_concatenados, 'marca')
        produtos_concatenados = FuncaoFiltro(grupo, produtos_concatenados, 'grupo')

        return produtos_concatenados


def FuncaoFiltro(valores, dataframe, nomeColuna):
    if valores == '-':
        return dataframe
    elif valores == []:
        return dataframe
    else:

        dataframeVetor = None
        for p in valores:

            dataframeFiltrado = dataframe[dataframe[nomeColuna] == p]
            dataframeVetor = pd.concat([dataframeVetor,dataframeFiltrado])
        return dataframeVetor

def RestricaoEngenharia(engenharia, obs, usuario, projecao):
    conn = ConexaoPostgreMPL.conexao()

    inserir = 'insert into "Reposicao"."ProjCustos".restricaoengenharia ' \
              '(codengenharia, obs, usuario, projecao) values (%s , %s , %s , %s )'

    cursor = conn.cursor()
    cursor.execute(inserir, (engenharia,obs,usuario, projecao))
    conn.commit()
    cursor.close()
    conn.close()


    conn.close()

    return pd.DataFrame([{'MENSAGEM':f'Engenharia {engenharia} excluida da projecao'}])

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


