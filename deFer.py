import psycopg2

import config

# Establecer la conexión con la base de datos ( connection strig)
connection = psycopg2.connect(
    #configurar host
    host=config.host, 
    #configurar user
    user=config.user, 
    #configurar password
    password=config.password, 
    #configurar db
    database=config.database, 
    #configurar puerto
    port=config.port 
)

# Habilitar la confirmación automática de transacciones
connection.autocommit = True
#print(connection)

# Definimos la funcion para crear tabla en postgres:
def crear_tabla():
    cursor = connection.cursor()
#    query = "CREATE TABLE customers(customerid INT PRIMARY KEY, name VARCHAR(50), occupation VARCHAR(50), email VARCHAR(50), company VARCHAR(50), phonenumber VARCHAR(20), age INT)"
#    query2 = "CREATE TABLE agents(agentid INT primary key, name VARCHAR(50));"
#    query3 = "CREATE TABLE calls(callid INT primary key, agentid INT, customerid INT, pickedup SMALLINT, duration INT, productsold SMALLINT);"
    query = "CREATE TABLE IF NOT EXISTS categorias (categoria_id SERIAL PRIMARY KEY, nombre_categoria VARCHAR(50));"
    query2 = "CREATE TABLE IF NOT EXISTS productos (producto_id SERIAL PRIMARY KEY, nombre_producto VARCHAR(50), categoria_id INTEGER, FOREIGN KEY (categoria_id) REFERENCES categorias(categoria_id), precio DECIMAL);"
    query3 = "CREATE TABLE IF NOT EXISTS ventas (ventas_id SERIAL PRIMARY KEY, fecha_venta DATE, producto_id INTEGER, FOREIGN KEY (producto_id) REFERENCES productos(producto_id), tienda_id INTEGER, FOREIGN KEY (tienda_id) REFERENCES tienda(tienda_id), cliente_id INTEGER, FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id), cantidad_vendida INTEGER, monto_total DECIMAL);"
    query4 = "CREATE TABLE IF NOT EXISTS clientes (cliente_id SERIAL PRIMARY KEY, nombre_cliente VARCHAR(100), direccion VARCHAR(150), ciudad VARCHAR(50));"
    query5 = "CREATE TABLE IF NOT EXISTS tienda (tienda_id SERIAL PRIMARY KEY, nombre_tienda VARCHAR(100),ubicacion VARCHAR(150));"
    try:
        cursor.execute(query)
        cursor.execute(query2)
        cursor.execute(query5)
        cursor.execute(query4)
        cursor.execute(query3)
        print("La tabla ha sido creada exitosamente")
    except psycopg2.errors.DuplicateTable:
        print("La tabla ya existe")
    except psycopg2.Error as e:
        print(f"Ocurrió un error al crear la tabla: {e}")
    cursor.close()

crear_tabla()

def eliminar_tabla():
    cursor = connection.cursor()
    query = "DROP TABLE IF EXISTS customers"
    try:
        cursor.execute(query)
        print("La tabla ha sido eliminada exitosamente")
    except psycopg2.Error as e:
        print(f"Ocurrió un error al eliminar la tabla: {e}")
    cursor.close()

#eliminar_tabla()

def insertar_datos():
    cursor = connection.cursor()
    query = "INSERT INTO customers(customerid, name, occupation, email, company, phonenumber, age) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    try:
        cursor.execute(query, (1, 'Juan Perez', 'Desarrollador', 'juanperez@example.com', 'ABC Inc.', '555-1234', 30))
        cursor.execute(query, (2, 'Maria Gomez', 'Diseñadora', 'mariagomez@example.com', 'XYZ Ltda.', '555-5678', 25))
        cursor.execute(query, (3, 'Pedro Ramirez', 'Gerente', 'pedroramirez@example.com', 'MNO SA', '555-9012', 40))
        connection.commit()
        print("Los datos han sido insertados exitosamente")
    except psycopg2.Error as e:
        connection.rollback()
        print(f"{e}")
    cursor.close()

#insertar_datos()

def actualizar_datos():
    cursor = connection.cursor()
    query = "UPDATE customers SET age = %s WHERE customerid = %s"
    try:
        cursor.execute(query, (35, 1))
        connection.commit()
        print("El dato ha sido actualizado exitosamente")
    except psycopg2.Error as e:
        connection.rollback()
        print(f"Ocurrió un error al actualizar el dato: {e}")
    cursor.close()

#actualizar_datos()


# Create a cursor object to execute SQL commands
cur = connection.cursor()

# Execute the SQL command to create a new database
#cur.execute("CREATE DATABASE pythonventasdb;")

# Close the cursor and connection
cur.close()
connection.close()