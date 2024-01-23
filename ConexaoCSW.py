import jaydebeapi
import pandas as pd
import Service.configuracoes.empresaConfigurada

def Conexao():
   # try:
        conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
    {'user': '_system', 'password': 'ccscache'},
    'CacheDB_root.jar'
    )
        return conn