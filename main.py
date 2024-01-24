import Service.produtos
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import os
from routes import routes_blueprint
from functools import wraps

app = Flask(__name__)
port = int(os.environ.get('PORT', 8000))
app.register_blueprint(routes_blueprint)

CORS(app)

# Decorator para verificar o token fixo
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'custoMPL123':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


if __name__ == '__main__':
    produto = Service.produtos.ProdutosCsw('VERÃO 2024', '1')
    Service.produtos.IncrementarProdutos('VERÃO 2024', '1')
    print(produto)
    app.run(host='0.0.0.0', port=port)