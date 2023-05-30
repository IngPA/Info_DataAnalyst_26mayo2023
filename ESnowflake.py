"""
Ejemplo de esquema Snowflake o copo de nieve: en este caso hay relaciones entre tablas de dimensiones
Suponemos que tenemos una base de datos de una tienda en línea y queremos realizar consultas sobre las ventas. 
"""

import psycopg2

import config

#entorno virtual: 

# Establecer la conexión con la base de datos ( connection strig)
connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password=config.password,
    database="28mayo2023",
    port="5432"
)

# Habilitar la confirmación automática de transacciones
connection.autocommit = True
#print(connection)

# Definimos la funcion para crear tabla en postgres:
def crear_tabla():
    cursor = connection.cursor()
    #query = "CREATE TABLE customers(customerid INT PRIMARY KEY, name VARCHAR(50), occupation VARCHAR(50), email VARCHAR(50), company VARCHAR(50), phonenumber VARCHAR(20), age INT)"
    
    #Tabla de hechos (fact table): "Ventas"
    query1 = "CREATE TABLE IF NOT EXISTS ventas(venta_id INT primary key NOT NULL, fecha_venta DATE, producto_id INT, cliente_id INT, tienda_id INT, cantidad_vendida FLOAT, monto_total FLOAT, FOREIGN KEY (producto_id) REFERENCES productos(producto_id), FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id), FOREIGN KEY (tienda_id) REFERENCES tiendas(tienda_id));"
    
    #Tablas de dimensiones: Productos, Clientes, Tiendas. 
    #En Esquema de Estrella, se relacionan todas a la tabla de hechos (ventas). 
    
    #a) Tabla "Productos":
    querya = "CREATE TABLE IF NOT EXISTS productos(producto_id INT  primary key NOT NULL, nombre_producto VARCHAR(50), categoria_id INT, precio FLOAT, FOREIGN KEY (categoria_id) REFERENCES categorias(categoria_id));"

    #a1) Tabla "Categoria":
    querya1 = "CREATE TABLE IF NOT EXISTS categorias(categoria_id INT primary key NOT NULL, categoría VARCHAR(50));"

 
    #b) Tabla "Clientes":
    queryb = "CREATE TABLE IF NOT EXISTS clientes(cliente_id INT  primary key NOT NULL, nombre_cliente VARCHAR(30), dirección VARCHAR(50), ciudad VARCHAR(30));"

    #c) Tabla "Tiendas":
    queryc = "CREATE TABLE IF NOT EXISTS tiendas(tienda_id INT  primary key NOT NULL, nombre_tienda VARCHAR(30), ubicación VARCHAR(50));"

    for consulta in [querya1,querya,queryb,queryc,query1]:
        try:
            cursor.execute(consulta)
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
    #  cursor.execute(query, (1, 'Juan Perez', 'Desarrollador', 'juanperez@example.com', 'ABC Inc.', '555-1234', 30))
    #  cursor.execute(query, (2, 'Maria Gomez', 'Diseñadora', 'mariagomez@example.com', 'XYZ Ltda.', '555-5678', 25))
    #   cursor.execute(query, (3, 'Pedro Ramirez', 'Gerente', 'pedroramirez@example.com', 'MNO SA', '555-9012', 40))
        cursor.execute(query, (1000, 'Pedro RamirezX', 'GerenteX', 'Xpedroramirez@example.com', 'MNO SA', '555-9012', 40))
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
print("Se ejecuto el programa")