import jaydebeapi
import pandas as pd

def Conexao():

    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
    {'user': '_system', 'password': 'ccscache'},
    'CacheDB.jar'
    )
    return conn


