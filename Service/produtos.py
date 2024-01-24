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
        print(f'alto verao + {ano}')

    elif 'VER' in projecao:
        produtos = 'SELECT e.codEngenharia as codengenharia, e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like 'VER%' AND  d.nomeColecao like " + ano + " and e.status in (2,3) " \
                                                                                                           " and e.codEngenharia like '0%' "
    else:
        produtos = 'SELECT e.codEngenharia as codengenharia , e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like '%INVE%' AND  d.nomeColecao like " + ano + " and e.status in (2,3)"\
    " and e.codEngenharia like '0%' "

    basico = 'SELECT e.codEngenharia as codengenharia , e.descricao  FROM tcp.Engenharia e ' \
                   'inner join tcp.DadosGeraisEng d on d.codEmpresa = e.codEmpresa and d.codEngenharia = e.codEngenharia ' \
                   "WHERE e.codEmpresa = 1 and d.nomeColecao like '%BASIC%' AND  d.nomeColecao like " + ano + " and e.status in (2,3)"\
    " and e.codEngenharia like '0%' AND e.codEngenharia like '%-0'  "


    produtos = pd.read_sql(produtos,conn)
    basico = pd.read_sql(basico,conn)

    produtos = pd.concat([produtos, basico])
    conn.close()
    produtos['categoria'] = '-'
    produtos['categoria'] = produtos.apply(lambda row: Categoria('BLAZER', row['descricao'], 'JAQUETA', row['categoria']), axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('TSHIRT', row['descricao'], 'CAMISETA', row['categoria']), axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('BATA', row['descricao'], 'CAMISA', row['categoria']), axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('CAMISA', row['descricao'], 'CAMISA', row['categoria']), axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('POLO', row['descricao'], 'POLO', row['categoria']), axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('CALCA JEANS', row['descricao'], 'CALCA JEANS', row['categoria']), axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('SHORT', row['descricao'], 'BOARDSHORT', row['categoria']),
                                     axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('CARTEIRA', row['descricao'], 'CARTEIRA', row['categoria']),
                                     axis=1)
    produtos['categoria'] = produtos.apply(lambda row: Categoria('MEIA', row['descricao'], 'MEIA', row['categoria']), axis=1)

    produtos['Projecao'] = projecao


    return produtos


def IncrementarProdutos(projecao, empresa):
    produtos = ProdutosCsw(projecao, empresa)
    ConexaoPostgreMPL.Funcao_Inserir(produtos,produtos.size,'produtos','append')




def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia:
        return valorNovo
    else:
        return categoria
