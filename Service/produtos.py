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


        projecaoCSW = 'SELECT  t.codProduto as codengenharia  FROM CusTex_Tpc.CProduto t ' \
                   'inner join CusTex_Tpc.CProdCapa capa on capa.codempresa = t.codEmpresa  and capa.numeroProj  = t.numeroProj ' \
                   "WHERE capa.descProjecao like " + ano + " and  capa.descProjecao like '%ALTO VER%' and capa.codempresa= 1"

    elif 'VER' in projecao:
        projecaoCSW = 'SELECT e.codEngenharia as codengenharia, e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like 'VER%' AND  d.nomeColecao like " + ano + " and e.status in (2,3) " \
                                                                                                           " and e.codEngenharia like '0%' "

        projecaoCSW = 'SELECT  t.codProduto as codengenharia FROM CusTex_Tpc.CProduto t ' \
                   'inner join CusTex_Tpc.CProdCapa capa on capa.codempresa = t.codEmpresa  and capa.numeroProj  = t.numeroProj ' \
                   "WHERE capa.descProjecao like " + ano + " and  capa.descProjecao like '%VER%' and capa.codempresa= 1"

    else:
        produtos = 'SELECT e.codEngenharia as codengenharia , e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like '%INVE%' AND  d.nomeColecao like " + ano + " and e.status in (2,3)"\
    " and e.codEngenharia like '0%' "

        projecaoCSW = 'SELECT  t.codProduto as codengenharia  FROM CusTex_Tpc.CProduto t ' \
                   'inner join CusTex_Tpc.CProdCapa capa on capa.codempresa = t.codEmpresa  and capa.numeroProj  = t.numeroProj ' \
                   "WHERE capa.descProjecao like " + ano + " and  capa.descProjecao like '%INVE%' and capa.codempresa= 1"

    basico = 'SELECT e.codEngenharia as codengenharia , e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like '%BASIC%'  and e.status in (2,3)"\
    " and e.codEngenharia like '0%' AND e.codEngenharia like '%-0'  "


    produtos_ = pd.read_sql(produtos,conn)
    produtos_['origem'] = 'Lancamento'
    basico = pd.read_sql(basico,conn)
    basico['origem'] = 'Continuadas'

    produtos_ = pd.concat([produtos_, basico])
    projecaoCSW = pd.read_sql(projecaoCSW, conn)
    projecaoCSW['situacaocusto'] = 'Projetado'
    produtos_ = pd.merge(produtos_, projecaoCSW , on='codengenharia', how='left')

    conn.close()
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
def obterGrupo(descricao):
    if 'PACK' in descricao:
        return 'ESSENCIAL ALTO GIRO'
    elif ' CB' in descricao:
        return 'ESSENCIAL ALTO GIRO'
    else:
        return '-'

def obterEstrategia(descricao):
    if 'PACK' in descricao:
        return 'PACK'
    elif ' CB' in descricao:
        return 'COMBO'
    else:
        return '-'

def IncrementarProdutos(projecao, empresa):

    delete = 'delete from "Reposicao"."ProjCustos".produtos ' \
             ' where projecao = %s'

    conn = ConexaoPostgreMPL.conexao()
    cursor = conn.cursor()
    cursor.execute(delete,(projecao,))
    conn.commit()
    cursor.close()
    conn.close()




    produtos = ProdutosCsw(projecao, empresa)
    ConexaoPostgreMPL.Funcao_Inserir(produtos,produtos.size,'produtos','append')




def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia:
        return valorNovo
    else:
        return categoria

def ObeterProdutosOficial(projecao, empresa):
    IncrementarProdutos(projecao, empresa)
    conn = ConexaoPostgreMPL.conexao()
    produtos = 'select * from "Reposicao"."ProjCustos".produtos p ' \
               'where projecao = %s '

    produtos = pd.read_sql(produtos,conn,params=(projecao,))
    conn.close()

    produtos.fillna('-', inplace=True)

    return produtos


