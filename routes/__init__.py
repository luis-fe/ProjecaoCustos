from flask import Blueprint

# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)

# Importe as rotas dos arquivos individuais
from .Projecao import Projecao_routes
from .Produtos import Produtos_routes

# Registre as rotas nos blueprints
routes_blueprint.register_blueprint(Projecao_routes)
routes_blueprint.register_blueprint(Produtos_routes)

