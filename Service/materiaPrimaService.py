### NESSE ARQUIVO ESTÁ PRESENTE AS REGRAS DE MODELAGEM PARA OBTER OS CUSTOS DETALHADOS DE MATÉRIA PRIMA DAS ENGENHARIAS PROJETADAS

# Bibliotecas:
import pandas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import locale

# Passo 1: Funcao para Consuntar no CSW os custos das materias primas vinculadas as engenharias da PROJECAO:
def ConsultaProjecaoMPCsw(projecao ,empresa = '-'):
    conn = ConexaoCSW.Conexao() # Abre a conexao com o Csw
    ano = projecao[-2:] # Obtem o ano da PROJECAO
    ano = "'%"+ano+"%'"# Transforma o ano para utilizar no LIKE do sql


    if 'ALT' in projecao: # Caso a Projecao contenha ALT de ALTO VERAO, realiza as consultas com clausuar "ALT VERAO"
        if empresa == 'FILIAL':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%ALTO VE%' and v.codempresa = 4 "\
                "union " \
                " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                " FROM CusTex_Tpc.CProdCompPad p " \
                                                   " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                                                   " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                                                   " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                                                   " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%ALTO VE%' and p.codempresa = 4 "
        elif empresa == 'MATRIZ':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%ALTO VE%' and v.codempresa = 1 "\
                "union " \
                " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                " FROM CusTex_Tpc.CProdCompPad p " \
                                                   " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                                                   " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                                                   " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                                                   " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%ALTO VE%' and p.codempresa = 1 "
        else:
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%'"



    elif 'INVER' in projecao:

        if empresa == 'FILIAL':
         consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                    "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                    ' codInsumo, '\
                '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%INVE%' and v.codempresa = 4" \
                                                   "union " \
                " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                " FROM CusTex_Tpc.CProdCompPad p " \
                                                   " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                                                   " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVE%' and p.codempresa = 4 "
        elif empresa == 'MATRIZ':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, ' \
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER%' and v.codempresa = 1 " \
                                                              "union " \
                       " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                       "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                       " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                       " FROM CusTex_Tpc.CProdCompPad p " \
                                                              " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                                                              " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                       " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVE%' and p.codempresa = 1"
        else:
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER%'"
    else:
        if empresa == 'FILIAL':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, '\
                'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal '\
                'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC '\
                'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj '\
                " WHERE tc.descProjecao like "+ano+" and tc.descProjecao like '%VERA%' and v.codempresa = 4 " \
                                                   "union " \
                                                   " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                                                   "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                    " FROM CusTex_Tpc.CProdCompPad p " \
                    " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                    " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                     " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                  " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%' and p.codempresa = 4 "
        elif empresa == 'MATRIZ':
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%' and v.codempresa = 1 "\
            "union " \
            " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
            "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
            " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
            " FROM CusTex_Tpc.CProdCompPad p " \
            " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
            " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
            " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
            " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%' and p.codempresa = 1 "

        else:
            consulta = 'SELECT V.codempresa as empresa, V.codProduto as codengenharia, codSortimento as codsortimento, ' \
                       "(select s.corbase ||'-'||s.descricao from tcp.sortimentosproduto s where s.codempresa = 1 and s.codProduto = v.codproduto and v.codSortimento = codSortimento ) as sortimento," \
                       ' codInsumo, '\
                       '(select i.nome from cgi.item i where i.codigo = V.codInsumo) as descricao_MP, ' \
                       'codGrade AS grade, qtdeGrade as consumo, v.custoUnit, v.custoTotal ' \
                       'FROM CusTex_Tpc.CProdInsVar V INNER JOIN CusTex_Tpc.CProdCapa TC ' \
                       'ON TC.codEmpresa = V.codEmpresa AND TC.numeroProj = V.numeroProj ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%'" \
                        "union " \
                        " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                        "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                        " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                        " FROM CusTex_Tpc.CProdCompPad p " \
                                                              " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                                                              " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                        " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                        " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%'  "


    ESTOQUE = 'SELECT codItem as codInsumo ,estoqueAtual ,precoMedio  FROM est.DadosEstoque d WHERE d.codNatureza in (1, 2) and estoqueAtual > 0'



    consulta = pd.read_sql(consulta,conn)
    ESTOQUE = pd.read_sql(ESTOQUE,conn)
    conn.close()
    consulta = pd.merge(consulta,ESTOQUE,on='codInsumo', how='left')
    consulta['projecao']=projecao
    consulta['empresa'] = consulta['empresa'].astype(str)
    consulta['empresa'] = consulta.apply(lambda row: 'MATRIZ' if row['empresa'] == '1' else 'FILIAL', axis=1)

    consulta['repeticao']=consulta.groupby(['codengenharia','codsortimento','codInsumo','grade','consumo']).cumcount() + 1

    consulta = consulta[consulta['repeticao'] != 2]
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


    result = None
    for p in projecao:

        consultaMP = pd.read_sql('select  projecao ,codengenharia, sortimento, grade, "custoTotal" from "Reposicao"."ProjCustos"."custoMP" c '
                                 'where c.projecao = %s ', conn, params=(p,))

        consultaMP['custoTotal'] = consultaMP['custoTotal'].astype(float)

        consultaMP= consultaMP.groupby(['codengenharia', 'projecao','sortimento', 'grade']).agg({
            'projecao':'first',
            'codengenharia':'first',
            'sortimento':'first',
            'grade':'first',
            'custoTotal':'sum'
        })
        result = pd.concat([result, consultaMP])
    def format_with_separator(value):
            return locale.format('%0.2f', value, grouping=True)

    result = result.sort_values(by='custoTotal', ascending=False,
                        ignore_index=True)  # escolher como deseja classificar

    result['custoTotal'] = result['custoTotal'].apply(format_with_separator)


    conn.close()

    return result