import ConexaoPostgreMPL
import pandas as pd


def ObterProjecoes():
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('Select * from "Reposicao"."ProjCustos".projecao ',conn)
    conn.close()

    return consulta

def Marcas():
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('select distinct marca  from "Reposicao"."ProjCustos".projecao p ',conn)
    conn.close()

    return consulta