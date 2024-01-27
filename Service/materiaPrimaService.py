import pandas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def ConsultaProjecaoMPCsw(projecao):

    conn = ConexaoCSW.Conexao()

    ano = projecao[-2:]
    ano = "'%"+ano+"%'"

    if 'ALT' in projecao:
        consulta = 'SELECT V.codProduto as codengenharia, codSortimento as codsortimento, codInsumo, '\
                '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" tc.descProjecao like '%ALTO VE%'"


    elif 'VER' in projecao:

        consulta = 'SELECT V.codProduto as codengenharia, codSortimento as codsortimento, codInsumo, '\
                '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" tc.descProjecao like '%VE%'"
    else:

        consulta = 'SELECT V.codProduto as codengenharia, codSortimento as codsortimento, codInsumo, '\
                '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" tc.descProjecao like '%INVER%'"

    ESTOQUE = 'SELECT codItem as codInsumo ,estoqueAtual ,precoMedio  FROM est.DadosEstoque d WHERE d.codNatureza in (1, 2) and estoqueAtual > 0'

    conn.close()

    consulta = pd.read_sql(consulta,conn)
    ESTOQUE = pd.read_sql(ESTOQUE,conn)
    consulta = pd.merge(consulta,ESTOQUE,on='codInsumo', how='left')
    consulta['projecao']=projecao

    return consulta

def IncrementarProdutosMateriaPrima(projecao, empresa):

    for p in projecao:
        conn = ConexaoPostgreMPL.conexao()
        consultaStatus = pd.read_sql('select situacao from "Reposicao"."ProjCustos"."projecao" '
                                     'where nome = %s ',conn, params=(p,))

        if consultaStatus['situacao'][0] == 'INICIADA':

            print(f'colecao {p} ja Iniciou vendas')
        else:

            delete = 'delete from "Reposicao"."ProjCustos"."custoMP" ' \
                     ' where projecao = %s'


            cursor = conn.cursor()
            cursor.execute(delete,(p,))
            conn.commit()
            cursor.close()
            conn.close()




            ObeterProdutos = ConsultaProjecaoMPCsw(p)
            ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutos,ObeterProdutos.size,'custoMP','append')


