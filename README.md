# technical_test
Prueba Técnica de conocimientos de Ingeniería de Datos que mide: SQL, Python, PowerBI, Pipeline. Desarrollada por SantiGarzonD

# Informacion del Paquete para la prueba

## Conector MySQL

El archivo `conector.py` proporciona una clase para facilitar la conexión a una base de datos MySQL y realizar operaciones básicas como la creación de una base de datos si no existe y la subida de datos desde un DataFrame de Pandas a una tabla específica en la base de datos.

Nota: Debe ser usuario con permiso de creación de bases de datos.

### Características

- **Creación de Base de Datos**: Verifica y crea la base de datos especificada si no existe al momento de establecer la conexión.
- **Subida de DataFrames**: Permite subir un DataFrame de Pandas a una tabla específica en la base de datos, reemplazando los datos existentes.

### Uso

Para utilizar esta clase, necesitarás instalar las dependencias requeridas, como `pymysql` y `SQLAlchemy`. Al usar `pip intall `para instalar el paquete, se instalarán automáticamente.

### Creación de la Instancia

Para crear una instancia de la clase, necesitarás proporcionar los detalles de conexión a tu base de datos MySQL:

```python
from conector import ConectorMySQL

conector = ConectorMySQL(user='tu_usuario', password='tu_contraseña', db='nombre_de_tu_base_de_datos', host='localhost', port=3306)