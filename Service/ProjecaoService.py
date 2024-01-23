import ConexaoPostgreMPL
import pandas as pd


def ObterProjecoes():
    conn = ConexaoPostgreMPL.conexao()
    consulta = pd.read_sql('Select * from "Reposicao"."ProjCusto".projecao ',conn)
    conn.close()

    return consulta