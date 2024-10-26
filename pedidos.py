from flask import Flask, request, jsonify
import sqlite3
import os
from flask_cors import CORS  # Importar CORS
import pika
import json

app = Flask(__name__)
CORS(app)  # Habilitar CORS para todas las rutas

# Conectar a la base de datos SQLite
def connect_db():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, 'db', 'pedidos.db')
    conn = sqlite3.connect(db_path)
    return conn

# Crear tabla de pedidos si no existe
def create_table():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS pedidos
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, productos TEXT, cantidad INTEGER)''')
    conn.commit()
    conn.close()

# Función para enviar un mensaje a RabbitMQ
def enviar_mensaje(producto_id, cantidad):
    conexion = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    canal = conexion.channel()
    canal.queue_declare(queue='pedidos_queue')  # Asegúrate de que la cola existe

    mensaje = {
        'producto_id': producto_id,
        'cantidad': cantidad
    }
    
    canal.basic_publish(exchange='', routing_key='pedidos_queue', body=json.dumps(mensaje))
    print(f"Pedido enviado: {mensaje}")

    conexion.close()

# Endpoint para crear un nuevo pedido
@app.route('/pedidos', methods=['POST'])
def crear_pedido():
    nuevo_pedido = request.json
    print(f"Recibido: {nuevo_pedido}")  # Agregar esta línea para depuración
    producto_id = nuevo_pedido['producto_id']
    cantidad = nuevo_pedido['cantidad']
    
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO pedidos (productos, cantidad) VALUES (?, ?)", (producto_id, cantidad))
    conn.commit()
    conn.close()
    
    # Enviar mensaje a RabbitMQ
    enviar_mensaje(producto_id, cantidad)
    
    return jsonify({'message': 'Pedido creado exitosamente'}), 201

# Endpoint para obtener todos los pedidos
@app.route('/pedidos', methods=['GET'])
def obtener_pedidos():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pedidos")
    pedidos = cursor.fetchall()
    conn.close()
    return jsonify(pedidos)

if __name__ == '__main__':
    create_table()
    app.run(port=7000, debug=True)
