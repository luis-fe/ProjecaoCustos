import Service.ProjecaoService
import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps

Projecao_routes = Blueprint('projecao', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'custoMPL123':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@Projecao_routes.route('/api/Projecoes', methods=['GET'])
@token_required
def get_Projecoes():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = Service.ProjecaoService.ObterProjecoes()
    Endereco_det = pd.DataFrame(Endereco_det)
    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)

@Projecao_routes.route('/api/Marcas', methods=['GET'])
@token_required
def get_Marcas():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = Service.ProjecaoService.Marcas()
    Endereco_det = pd.DataFrame(Endereco_det)
    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)