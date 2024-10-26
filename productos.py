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
    db_path = os.path.join(base_dir, 'db', 'productos.db')
    conn = sqlite3.connect(db_path)
    return conn

# Crear tabla de productos si no existe
def create_table():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS productos
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT, precio REAL, stock INTEGER)''')
    conn.commit()
    conn.close()

# Endpoint para obtener todos los productos
@app.route('/productos', methods=['GET'])
def obtener_productos():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM productos")
    productos = cursor.fetchall()
    conn.close()
    return jsonify(productos)

# Endpoint para crear un nuevo producto
@app.route('/productos', methods=['POST'])
def crear_producto():
    nuevo_producto = request.json
    nombre = nuevo_producto['nombre']
    precio = nuevo_producto['precio']
    stock = nuevo_producto.get('stock', 0)  # Se puede especificar stock al crear un producto
    
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO productos (nombre, precio, stock) VALUES (?, ?, ?)", (nombre, precio, stock))
    conn.commit()
    conn.close()
    return jsonify({'message': 'Producto creado exitosamente'}), 201

# Función para actualizar el inventario
def actualizar_inventario(producto_id, cantidad):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE productos SET stock = stock - ? WHERE id = ?", (cantidad, producto_id))
    conn.commit()
    conn.close()

# Callback para procesar mensajes de RabbitMQ
def callback(ch, method, properties, body):
    mensaje = json.loads(body)
    producto_id = mensaje['producto_id']
    cantidad = mensaje['cantidad']
    actualizar_inventario(producto_id, cantidad)
    print(f"Inventario actualizado: Producto {producto_id}, Cantidad reducida {cantidad}")

# Consumir mensajes de RabbitMQ
def consumir_mensajes():
    conexion = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    canal = conexion.channel()
    canal.queue_declare(queue='pedidos_queue')  # Asegúrate de que la cola existe
    canal.basic_consume(queue='pedidos_queue', on_message_callback=callback, auto_ack=True)

    print('Esperando mensajes para actualizar inventario...')
    canal.start_consuming()

if __name__ == '__main__':
    create_table()
    # Iniciar la función de consumo en un hilo
    from threading import Thread
    thread = Thread(target=consumir_mensajes)
    thread.start()
    app.run(port=7001, debug=True)
