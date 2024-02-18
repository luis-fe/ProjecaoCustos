### NESSE ARQUIVO ESTÁ PRESENTE AS REGRAS DE MODELAGEM PARA OBTER OS CUSTOS DETALHADOS DE MATÉRIA PRIMA DAS ENGENHARIAS PROJETADAS

# Bibliotecas:
import pandas
import ConexaoCSW
import ConexaoPostgreMPL
import pandas as pd
import locale

######## Passo 1: Funcao para Consuntar no CSW os custos das materias primas vinculadas as engenharias da PROJECAO:
# No codigo optou-se por fazer a consulta em sql junto ao banco Intersystem Caché, devido a necessidade de utilizar clausuas especiais de sql.

#Funcao Consultar a Projecao de Custos de Materia Prima no CSW:
def ConsultaProjecaoMPCsw(projecao ,empresa = '-'):
    conn = ConexaoCSW.Conexao() # Abre a conexao com o Csw

    # Obtem o ano da PROJECAO do parametro "projecao"  e Transforma o ano para utilizar no LIKE do sql
    ano = projecao[-2:]
    ano = "'%"+ano+"%'"#


    #Inicia uma cadeia de IF para realizar as consultas de acordo com a projecao escolhida:

    if 'ALT' in projecao:
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
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%' " \
                                                              "union " \
                    " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                    "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                    " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                    " FROM CusTex_Tpc.CProdCompPad p " \
                    " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                    " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                    " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                    " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%' "



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
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER%' " \
                                                              "union " \
                                                              " select p.codempresa as empresa, p.codProduto as codengenharia, s.codSortimento as codsortimento, " \
                                                              "s.corbase ||'-'||s.descricao as sortimento, p.codCompPad as codInsumo, " \
                                                              " (select i.nome from cgi.item i where i.codigo = p.codCompPad) as descricao_MP, g.codGrade as grade, p.qtdeUnit as consumo, p.custoUnit , (p.custoUnit * p.qtdeUnit) as custoTotal " \
                                                              " FROM CusTex_Tpc.CProdCompPad p " \
                                                              " inner join tcp.GradesEngenharia g on g.Empresa = 1 and g.codEngenharia = p.codProduto  " \
                                                              " inner join tcp.sortimentosproduto s on s.codempresa = 1 and s.codProduto = p.codproduto " \
                                                              " inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj " \
                                                              " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%' "
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
    return consulta # O retorno dessa função é um dataframe com os dados relativos aos compontentes e processos dos produtos vinculados a projecao informada!


## Funcao utilizada para buscar os custos de processos dos produtos de acordo com a projecao escolhida.
## optou-se por utilizar a consulta sql ao banco para obter essas informacoes:
def ProcessosProdutos(projecao ,empresa = '-'):
    conn = ConexaoCSW.Conexao() # Abre a conexao com o Csw

    # Obtem o ano da PROJECAO do parametro "projecao"  e Transforma o ano para utilizar no LIKE do sql
    ano = projecao[-2:]
    ano = "'%"+ano+"%'"

    #Inicia uma cadeia de IF para realizar as consultas de acordo com a projecao escolhida:

    if 'ALT' in projecao:

        if empresa == 'FILIAL':


            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%' and p.codempresa = 4 "

        elif empresa == 'MATRIZ':

            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%' and p.codempresa = 1 "
        else:

            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%ALTO VE%'"

    elif 'INVER' in projecao:

        if empresa == 'FILIAL':


            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER%' and p.codempresa = 4 "

        elif empresa == 'MATRIZ':

            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER' and p.codempresa = 1 "
        else:

            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%INVER%'"

    else:
        if empresa == 'FILIAL':


            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%' and p.codempresa = 4 "

        elif empresa == 'MATRIZ':

            consulta = 'SELECT p.codempresa as empresa, codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA' and p.codempresa = 1 "
        else:
            print('x')
    consultaTeste = 'SELECT p.codempresa as empresa, p.codproduto as codengenharia, p.codGrade as codgrade , e.codFase as codfase , ' \
                    'e.nomeFase as nomefase  , p.tempoUnitCus as tempo , p.custoProc as custo, p.unidade, e.percEficiencia as eficiencia  from CusTex_Tpc.CProdOper p ' \
                       'inner join CusTex_Tpc.CProdCapa TC on TC.codEmpresa = p.codEmpresa and TC.numeroProj = P.numeroProj ' \
                       'inner join tcp.ProcessosEngenharia e on e.codEmpresa = p.codEmpresa and e.codEngenharia = p.codProduto and e.seqProcesso = p.codSeqOper ' \
                       " WHERE tc.descProjecao like " + ano + " and tc.descProjecao like '%VERA%'"

    consulta = pd.read_sql(consultaTeste, conn)

    conn.close()

    return consulta

X = ProcessosProdutos('INVERNO 2024', 'MATRIZ')
print(X)
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
                    #ObeterProdutosProcessos = ProcessosProdutos(p, e)

                    ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutos, ObeterProdutos.size, 'custoMP', 'append')
                    #ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutosProcessos, ObeterProdutosProcessos.size, 'custoprocesso', 'append')

                else:
                    delete = 'delete from "Reposicao"."ProjCustos"."custoMP" ' \
                         ' where projecao = %s and empresa = %s '
                    cursor = conn.cursor()
                    cursor.execute(delete, (p,e,))
                    conn.commit()
                    cursor.close()

                    ObeterProdutos = ConsultaProjecaoMPCsw(p, e)
                    #ObeterProdutosProcessos = ProcessosProdutos(p, e)
                    if not ObeterProdutos.empty:
                        ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutos, ObeterProdutos.size, 'custoMP', 'append')
                     #   ConexaoPostgreMPL.Funcao_Inserir(ObeterProdutosProcessos, ObeterProdutosProcessos.size, 'custoprocesso', 'append')
                    else:
                        print(f'vazio empresa {e}')
            conn.close()


def ResumirCustoSortimento(projecao):
    conn = ConexaoPostgreMPL.conexao()


    result = None
    for p in projecao:
        consultaMP = pd.read_sql('select  projecao ,codengenharia, sortimento, grade, "custoTotal" from "Reposicao"."ProjCustos"."custoMP" c '
                                 "where c.projecao = %s  ", conn, params=(p,))

        consultaMP['custoTotal'] = consultaMP['custoTotal'].astype(float)

        consultaMP= consultaMP.groupby(['codengenharia', 'projecao','sortimento', 'grade']).agg({
            'custoTotal':'sum'
        }).reset_index()
        result = pd.concat([result, consultaMP])
    def format_with_separator(value):
            return locale.format('%0.2f', value, grouping=True)

    result = result.sort_values(by=['codengenharia','grade','custoTotal'], ascending=False,
                        ignore_index=True)  # escolher como deseja classificar

    result['custoTotal'] = result['custoTotal'].apply(format_with_separator)



    conn.close()
    result['repeticao']=result.groupby(['codengenharia','grade']).cumcount() + 1

    result['criterio'] = result.apply(lambda row: 'MaiorCor' if row['repeticao']== 1 else '-',axis=1 )

    result['grade'] = result.apply(
        lambda row: obterGrade(row['grade']), axis=1)

    return result


def detalharCusto(codengenharia, projecao, grade):
    conn = ConexaoPostgreMPL.conexao()

    detalharCusto = ''

    conn.close()


def obterGrade(grade):
    if grade in [6,14,16,17,23,24,25,27,28,36,37,40,41,58]:
        return 'FILHO'
    else:
        return 'PAI'