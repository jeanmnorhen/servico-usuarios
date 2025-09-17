import os
import json
import uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import base64

# --- Importações de dependências ---
try:
    from firebase_admin import credentials, firestore, initialize_app, auth
    import firebase_admin
except ImportError:
    firebase_admin = None

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None

try:
    from sqlalchemy import create_engine, Column, String, MetaData, text
    from sqlalchemy.orm import sessionmaker, declarative_base
    from sqlalchemy.exc import SQLAlchemyError
    from geoalchemy2 import Geography
    from geoalchemy2.shape import to_shape
    from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
except ImportError:
    SQLAlchemyError = None
    declarative_base = None
    create_engine = None
    Column = String = MetaData = sessionmaker = Geography = to_shape = text = None
    urlparse = urlunparse = parse_qs = urlencode = None

from flask_cors import CORS

# --- Variáveis globais para erros de inicialização ---
firebase_init_error = None
postgres_init_error = None
kafka_producer_init_error = None # Renamed for clarity
db_init_error = None

# --- Configuração do Flask ---
app = Flask(__name__)
CORS(app)

# --- Configuração do Firebase ---
db = None
if firebase_admin:
    try:
        base64_sdk = os.environ.get('FIREBASE_ADMIN_SDK_BASE64')
        if base64_sdk:
            decoded_sdk = base64.b64decode(base64_sdk).decode('utf-8')
            cred_dict = json.loads(decoded_sdk)
            cred = credentials.Certificate(cred_dict)
            if not firebase_admin._apps:
                initialize_app(cred)
            db = firestore.client()
            print("Firebase inicializado com sucesso.")
        else:
            firebase_init_error = "Variável de ambiente FIREBASE_ADMIN_SDK_BASE64 não encontrada."
            print(firebase_init_error)
    except Exception as e:
        firebase_init_error = str(e)
        print(f"Erro ao inicializar Firebase: {e}")
else:
    firebase_init_error = "Biblioteca firebase_admin não encontrada."

# --- Configuração do PostgreSQL (PostGIS) ---
db_session = None
engine = None
if create_engine:
    try:
        db_url = os.environ.get('POSTGRES_POSTGRES_URL')
        if db_url:
            if db_url.startswith("postgres://"):
                db_url = db_url.replace("postgres://", "postgresql://", 1)

            cleaned_url = db_url
            if urlparse:
                try:
                    parsed_url = urlparse(db_url)
                    query_params = parse_qs(parsed_url.query)
                    query_params.pop('supa', None)
                    new_query = urlencode(query_params, doseq=True)
                    cleaned_url = urlunparse(parsed_url._replace(query=new_query))
                except Exception:
                    pass

            engine = create_engine(cleaned_url)
            Session = sessionmaker(bind=engine)
            db_session = Session()
            print("Conexão com PostgreSQL (PostGIS) estabelecida com sucesso.")
        else:
            postgres_init_error = "Variável de ambiente POSTGRES_POSTGRES_URL não encontrada."
            print(postgres_init_error)
    except Exception as e:
        postgres_init_error = str(e)
        print(f"Erro ao conectar com PostgreSQL: {e}")
else:
    postgres_init_error = "SQLAlchemy não encontrado."

# --- Definição do Modelo de Dados Geoespacial ---
Base = declarative_base() if declarative_base else object

if Base != object and Geography:
    class UserLocation(Base):
        __tablename__ = 'user_locations'
        user_id = Column(String, primary_key=True)
        location = Column(Geography(geometry_type='POINT', srid=4326), nullable=False)

def init_db():
    global db_init_error
    if not engine:
        db_init_error = "Engine do PostgreSQL não inicializada."
        print(db_init_error)
        return
    if not Base:
        db_init_error = "Declarative base do SQLAlchemy não pôde ser criada."
        print(db_init_error)
        return
    try:
        Base.metadata.create_all(engine)
        print("Tabela 'user_locations' verificada/criada com sucesso.")
    except Exception as e:
        db_init_error = str(e)
        print(f"Erro ao criar tabela 'user_locations': {e}")

# --- Configuração do Kafka Producer ---
producer = None
if Producer:
    try:
        kafka_conf = {
            'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.environ.get('KAFKA_API_KEY'),
            'sasl.password': os.environ.get('KAFKA_API_SECRET')
        }
        if kafka_conf['bootstrap.servers']:
            producer = Producer(kafka_conf)
            print("Produtor Kafka inicializado com sucesso.")
        else:
            kafka_producer_init_error = "Variáveis de ambiente do Kafka não encontradas."
            print(kafka_producer_init_error)
    except Exception as e:
        kafka_producer_init_error = str(e)
        print(f"Erro ao inicializar Produtor Kafka: {e}")
else:
    kafka_producer_init_error = "Biblioteca confluent_kafka não encontrada."

def delivery_report(err, msg):
    if err is not None:
        print(f'Falha ao entregar mensagem Kafka: {err}')
    else:
        print(f'Mensagem Kafka entregue em {msg.topic()} [{msg.partition()}]')

def publish_event(topic, event_type, user_id, data, changes=None):
    if not producer:
        print("Produtor Kafka não está inicializado. Evento não publicado.")
        return
    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "data": data,
        "source_service": "servico-usuarios"
    }
    if changes:
        event["changes"] = changes
    try:
        event_value = json.dumps(event, default=str)
        producer.produce(topic, key=user_id, value=event_value, callback=delivery_report)
        producer.poll(0)
        print(f"Evento '{event_type}' para o usuário {user_id} publicado no tópico {topic}.")
    except Exception as e:
        print(f"Erro ao publicar evento Kafka: {e}")

# --- Rotas da API ---

@app.route('/users', methods=['POST'])
def create_user():
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas.", "health": get_health_status()}), 503

    user_data = request.json
    if not user_data or 'email' not in user_data or 'name' not in user_data:
        return jsonify({"error": "Email e nome são obrigatórios."}), 400

    user_id = str(uuid.uuid4())
    
    firestore_data = user_data.copy()
    location_data = firestore_data.pop('location', None)
    firestore_data['created_at'] = firestore.SERVER_TIMESTAMP
    firestore_data['updated_at'] = firestore.SERVER_TIMESTAMP

    try:
        if location_data and 'latitude' in location_data and 'longitude' in location_data:
            lat = location_data['latitude']
            lon = location_data['longitude']
            wkt_point = f'POINT({lon} {lat})'
            new_location = UserLocation(user_id=user_id, location=wkt_point)
            db_session.add(new_location)

        db.collection('users').document(user_id).set(firestore_data)
        db_session.commit()

        publish_event('eventos_usuarios', 'UserCreated', user_id, user_data)
        return jsonify({"id": user_id, "message": "Usuário criado com sucesso."}), 201

    except SQLAlchemyError as e:
        db_session.rollback()
        return jsonify({"error": f"Erro no banco de dados geoespacial: {e}"}), 500
    except Exception as e:
        db_session.rollback()
        return jsonify({"error": f"Erro ao criar usuário: {e}"}), 500

@app.route('/users/<user_id>', methods=['GET'])
def get_user(user_id):
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    try:
        user_doc = db.collection('users').document(user_id).get()
        if not user_doc.exists:
            return jsonify({"error": "Usuário não encontrado."}), 404
        
        user_data = user_doc.to_dict()
        user_data['id'] = user_doc.id

        location_record = db_session.query(UserLocation).filter_by(user_id=user_id).first()
        if location_record and to_shape:
            point = to_shape(location_record.location)
            user_data['location'] = {'latitude': point.y, 'longitude': point.x}

        return jsonify(user_data), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao buscar usuário: {e}"}), 500

@app.route('/users/<user_id>', methods=['PUT'])
def update_user(user_id):
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    update_data = request.json
    if not update_data:
        return jsonify({"error": "Dados para atualização são obrigatórios."}), 400

    user_ref = db.collection('users').document(user_id)

    try:
        if not user_ref.get().exists:
            return jsonify({"error": "Usuário não encontrado."}), 404

        firestore_data = update_data.copy()
        location_data = firestore_data.pop('location', None)
        firestore_data['updated_at'] = firestore.SERVER_TIMESTAMP

        if location_data and 'latitude' in location_data and 'longitude' in location_data:
            lat = location_data['latitude']
            lon = location_data['longitude']
            wkt_point = f'POINT({lon} {lat})'
            
            location_record = db_session.query(UserLocation).filter_by(user_id=user_id).first()
            if location_record:
                location_record.location = wkt_point
            else:
                new_location = UserLocation(user_id=user_id, location=wkt_point)
                db_session.add(new_location)

        if firestore_data:
            user_ref.update(firestore_data)

        db_session.commit()

        publish_event('eventos_usuarios', 'UserUpdated', user_id, update_data)
        return jsonify({"message": "Usuário atualizado com sucesso.", "id": user_id}), 200

    except SQLAlchemyError as e:
        db_session.rollback()
        return jsonify({"error": f"Erro no banco de dados geoespacial: {e}"}), 500
    except Exception as e:
        db_session.rollback()
        return jsonify({"error": f"Erro ao atualizar usuário: {e}"}), 500

@app.route('/users/<user_id>', methods=['DELETE'])
def delete_user(user_id):
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    user_ref = db.collection('users').document(user_id)

    try:
        if not user_ref.get().exists:
            return jsonify({"error": "Usuário não encontrado."}), 404

        location_record = db_session.query(UserLocation).filter_by(user_id=user_id).first()
        if location_record:
            db_session.delete(location_record)

        user_ref.delete()
        db_session.commit()

        publish_event('eventos_usuarios', 'UserDeleted', user_id, {"user_id": user_id})
        return '', 204

    except SQLAlchemyError as e:
        db_session.rollback()
        return jsonify({"error": f"Erro no banco de dados geoespacial: {e}"}), 500
    except Exception as e:
        db_session.rollback()
        return jsonify({"error": f"Erro ao deletar usuário: {e}"}), 500

# --- Health Check (para Vercel) ---
def get_health_status():
    env_vars = {
        "FIREBASE_ADMIN_SDK_BASE64": "present" if os.environ.get('FIREBASE_ADMIN_SDK_BASE64') else "missing",
        "POSTGRES_POSTGRES_URL": "present" if os.environ.get('POSTGRES_POSTGRES_URL') else "missing",
        "KAFKA_BOOTSTRAP_SERVER": "present" if os.environ.get('KAFKA_BOOTSTRAP_SERVER') else "missing",
        "KAFKA_API_KEY": "present" if os.environ.get('KAFKA_API_KEY') else "missing",
        "KAFKA_API_SECRET": "present" if os.environ.get('KAFKA_API_SECRET') else "missing"
    }

    pg_status = "error"
    pg_query_error = None
    if db_session and text:
        try:
            db_session.execute(text('SELECT 1'))
            pg_status = "ok"
        except Exception as e:
            pg_query_error = str(e)
            pg_status = f"error during query: {pg_query_error}"

    status = {
        "environment_variables": env_vars,
        "dependencies": {
            "firestore": "ok" if db else "error",
            "kafka_producer": "ok" if producer else "error",
            "postgresql_connection": pg_status,
            "table_initialization": "ok" if not db_init_error else "error"
        },
        "initialization_errors": {
            "firestore": firebase_init_error,
            "postgresql_engine": postgres_init_error,
            "postgresql_table": db_init_error,
            "postgresql_query": pg_query_error,
            "kafka_producer": kafka_producer_init_error # Renamed
        }
    }
    return status

@app.route('/health', methods=['GET'])
def health_check():
    status = get_health_status()
    
    all_ok = (
        all(value == "present" for value in status["environment_variables"].values()) and
        status["dependencies"]["firestore"] == "ok" and
        status["dependencies"]["kafka_producer"] == "ok" and
        status["dependencies"]["postgresql_connection"] == "ok" and
        status["dependencies"]["table_initialization"] == "ok"
    )
    http_status = 200 if all_ok else 503
    
    return jsonify(status), http_status

# --- Inicialização ---
init_db()

if __name__ == '__main__':
    app.run(debug=True)