const apiUrlProductos = 'http://localhost:7001/productos';
const apiUrlPedidos = 'http://localhost:7000/pedidos';

function crearProducto() {
    const nombre = document.getElementById('nombreProducto').value;
    const precio = document.getElementById('precioProducto').value;
    const stock = document.getElementById('stockProducto').value;  // Obtener el stock

    fetch(apiUrlProductos, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ nombre: nombre, precio: parseFloat(precio), stock: parseInt(stock) })  // Enviar stock
    })
    .then(response => response.json())
    .then(data => {
        alert(data.message);
        listarProductos();
    })
    .catch(error => console.error('Error:', error));
}

function listarProductos() {
    fetch(apiUrlProductos)
    .then(response => response.json())
    .then(data => {
        const productosDiv = document.getElementById('productos');
        productosDiv.innerHTML = '<h2>Lista de Productos:</h2>';
        data.forEach(producto => {
            productosDiv.innerHTML += `<p>ID: ${producto[0]}, Nombre: ${producto[1]}, Precio: ${producto[2]}, Stock: ${producto[3]}</p>`;
        });
    })
    .catch(error => console.error('Error:', error));
}

function crearPedido() {
    const productos = document.getElementById('productosPedido').value;
    const cantidad = document.getElementById('cantidadPedido').value;

    fetch(apiUrlPedidos, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ producto_id: productos, cantidad: parseInt(cantidad) })
    })
    .then(response => response.json())
    .then(data => {
        alert(data.message);
        listarPedidos();
    })
    .catch(error => console.error('Error:', error));
}

function listarPedidos() {
    fetch(apiUrlPedidos)
    .then(response => response.json())
    .then(data => {
        const pedidosDiv = document.getElementById('pedidos');
        pedidosDiv.innerHTML = '<h2>Lista de Pedidos:</h2>';
        data.forEach(pedido => {
            pedidosDiv.innerHTML += `<p>ID: ${pedido[0]}, Productos: ${pedido[1]}, Cantidad: ${pedido[2]}</p>`;
        });
    })
    .catch(error => console.error('Error:', error));
}

// Llamar a listarProductos y listarPedidos al cargar la página
document.addEventListener('DOMContentLoaded', () => {
    listarProductos();
    listarPedidos();
});
