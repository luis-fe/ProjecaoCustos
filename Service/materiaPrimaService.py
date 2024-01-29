import pandas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd


def ConsultaProjecaoMPCsw(projecao ,empresa = '-'):

    conn = ConexaoCSW.Conexao()

    ano = projecao[-2:]
    ano = "'%"+ano+"%'"



    if 'ALT' in projecao:
        if empresa == 'FILIAL':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%ALTO VE%' and v.codempresa = 4"
        elif empresa == 'MATRIZ':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%' and v.codempresa = 1"
        else:
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%'"



    elif 'INVER' in projecao:

        if empresa == 'FILIAL':
         consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                    "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                    ' codInsumo, '\
                '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%INVE%' and v.codempresa = 4"
        elif empresa == 'MATRIZ':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, ' \
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER%' and v.codempresa = 1"
        else:
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER%'"
    else:
        if empresa == 'FILIAL':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%VERA%' and v.codempresa = 4"
        elif empresa == 'MATRIZ':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%' and v.codempresa = 1"
        else:
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%'"

    ESTOQUE = 'SELECT codItem as codInsumo ,estoqueAtual ,precoMedio  FROM est.DadosEstoque d WHERE d.codNatureza in (1, 2) and estoqueAtual > 0'



    consulta = pd.read_sql(consulta,conn)
    ESTOQUE = pd.read_sql(ESTOQUE,conn)
    conn.close()
    consulta = pd.merge(consulta,ESTOQUE,on='codInsumo', how='left')
    consulta['projecao']=projecao
    consulta['empresa'] = consulta['empresa'].astype(str)
    consulta['empresa'] = consulta.apply(lambda row: 'MATRIZ' if row['empresa'] == '1' else 'FILIAL', axis=1)


    return consulta

def IncrementarProdutosMateriaPrima(projecao, empresa):

    for p in projecao:
        conn = ConexaoPostgreMPL.conexao()
        consultaStatus = pd.read_sql('select situacao from "Reposicao"."ProjCustos"."projecao" '
                                     'where nome = %s ',conn, params=(p,))

        if consultaStatus['situacao'][0] == 'INICIADA':

            print(f'colecao {p} ja Iniciou vendas')
        else:

            for e in empresa:
                if e == '-':

                    delete = 'delete from "Reposicao"."ProjCustos"."custoMP" ' \
                         ' where projecao = %s '
                    cursor = conn.cursor()
                    cursor.execute(delete, (p,))
                    conn.commit()
                    cursor.close()

                    ObeterProdutos = ConsultaProjecaoMPCsw(p, e)
                    ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutos, ObeterProdutos.size, 'custoMP', 'append')

                else:
                    delete = 'delete from "Reposicao"."ProjCustos"."custoMP" ' \
                         ' where projecao = %s and empresa = %s '
                    cursor = conn.cursor()
                    cursor.execute(delete, (p,e,))
                    conn.commit()
                    cursor.close()

                    ObeterProdutos = ConsultaProjecaoMPCsw(p, e)
                    if not ObeterProdutos.empty:
                        ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutos, ObeterProdutos.size, 'custoMP', 'append')
                    else:
                        print(f'vazio empresa {e}')
            conn.close()


def ResumirCustoSortimento(projecao):
    conn = ConexaoPostgreMPL.conexao()

    for p in projecao:

        consultaMP = pd.read_sql('select  codengenharia, sortimento, grade, custoTotal from "Reposicao"."ProjCustos"."custoMP" c '
                                 'where c.projecao = %s ', conn, params=(p))

        consultaMP['custoTotal'] = consultaMP['custoTotal'].astype(int)

        consultaMP['custoTotal'] = consultaMP.groupby(['codEngenharia, sortimento, grade']).aggregate({
            'custoTotal':'sum'
        })

    conn.close()

    return consultaMP