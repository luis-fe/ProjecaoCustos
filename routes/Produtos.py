import Service.materiaPrimaService
from Service import produtos, materiaPrimaService
import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps

Produtos_routes = Blueprint('produtos', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'custoMPL123':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@Produtos_routes.route('/api/Produtos', methods=['POST'])
@token_required
def get_Produtos():
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    projecao = data.get('projecao','sem dados')
    empresa = data.get('empresa', ['-'])
    categoria = data.get('categoria', '-')
    marca = data.get('marca', '-')
    grupo = data.get('grupo','-')
    Service.produtos.IncrementarProdutos(projecao, empresa)
    Service.materiaPrimaService.IncrementarProdutosMateriaPrima(projecao, empresa)
    Endereco_det = Service.produtos.ObterProdutosOficial(projecao, empresa, categoria,marca,grupo)
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



@Produtos_routes.route('/api/RetirarProduto', methods=['DELETE'])
@token_required
def get_RetirarProduto():
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    engenharia = data.get('engenharia')
    obs = data.get('obs', '-')
    usuario = data.get('usuario', '-')
    projecao = data.get('projecao', '-')

    Endereco_det = Service.produtos.RestricaoEngenharia(engenharia,obs,usuario, projecao)
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

@Produtos_routes.route('/api/ResumirSortimento', methods=['GET'])
@token_required
def ResumirSortimento():
    # Obtém os dados do corpo da requisição (JSON)

    projecao = request.args.get('projecao', '-')

    Endereco_det = Service.materiaPrimaService.ResumirCustoSortimento(projecao)
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


@Produtos_routes.route('/api/ConsultaCadastroItensCSW', methods=['GET'])
@token_required
def ConsultaCadastroItensCSW():
    # Obtém os dados do corpo da requisição (JSON)
    engenharia = request.args.get('engenharia')


    Endereco_det, qtItensNormais = Service.produtos.ConsultaCadastroItensCSW(engenharia)
    print(qtItensNormais)
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
