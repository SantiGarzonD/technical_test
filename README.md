# technical_test
Prueba Técnica de conocimientos de Ingeniería de Datos que mide: SQL, Python, PowerBI, Pipeline. Desarrollada por SantiGarzonD

# ¿Qué encontrarás?

En este repositorio encontrarás lo siguiente:

- 6 Datasets que serán tu insumo para la prueba :)
    - Estos datasets están inspirados en el Dataset de Kaggle [Retail Banking-demo-data](https://www.kaggle.com/datasets/kabure/retail-bankingdemodata) pero no son los mismos.

- Un `src` con un paquete que servirá para el ejercicio.

Puedes instalarlo realizando `pip install .` en la raíz del directorio o `pip install conector-mysql-sgd`. El paquete fue creado especialmente para la prueba :)

El paquete te servirá para cargar DataFrames de Pandas al administrador de Base de Datos MySQL y realizar consultas de SQL devolviendo Dataframes de Pandas.

En la parte final está descrito el uso del paquete

# ¿Qué tecnología requieres tener instalada?

1. MySQL - Cualquier versión (Mínimo Server y Shell)
2. Python - Cualquier versión
3. PowerBI - Cualquier versión
4. El paquete para manipular mysql con python *conector-mysql-sgd* *
    * Lo puedes instalar con `pip install .` en la raíz del directorio o `pip install conector-mysql-sgd`

En la parte final está la descripción de como se usa el paquete

# Puntos

1. Cree un script de python que use la librería conector-mysql-sgd para subir las 6 bases de datos.

2. En un Jupyter Notebook usando la librería conector-mysql-sgd realice los siguientes cruces:

    1. ¿Cómo se distribuyen las tarjetas de crédito por sexo?
    2. ¿Cuál es la cantidad de préstamos, y montos de préstamos por Año y Mes?
    3. ¿Cuál es la cantidad de cuentas creadas por distrito y ciudad?

3. Haz un tablero de PowerBI cargando todas las tablas con una visual por producto (tarjetas, cuentas, prestamos) que permita hacer análisis visuales a partir de la segregación de la información por medio de amplia gama de filtros.



# Entregables

1. Script de python
2. Jupyter Notebook (Las salidas dejarlas en los prompts)
3. Tablero de PowerBI

Disfrutaaa :)


# Informacion del Paquete para la prueba

## Conector MySQL

La librería `conector_mysql_sgd` proporciona una clase para facilitar la conexión a una base de datos MySQL y realizar operaciones básicas como la creación de una base de datos si no existe y la subida de datos desde un DataFrame de Pandas a una tabla específica en la base de datos.

También permite realizar consultas SQL generando un DataFrame de Pandas como salida.

Nota: Debe ser usuario con permiso de creación de bases de datos.

### Características del paquete

- **Creación de Base de Datos**: Verifica y crea la base de datos especificada si no existe al momento de establecer la conexión.
- **Subida de DataFrames**: Permite subir un DataFrame de Pandas a una tabla específica en la base de datos, reemplazando los datos existentes.
- **Consultas**: Permite realizar consultas SQL devolviendo un dataframe.

### Uso

Para utilizar esta clase, necesitarás instalar las dependencias requeridas, como `pymysql` y `SQLAlchemy`. Al usar `pip intall conector-mysql-sgd` para instalar el paquete, se instalarán automáticamente.

### Creación de la Instancia

Para crear una instancia de la clase, necesitarás proporcionar los detalles de conexión a tu base de datos MySQL:

```python
from conector import ConectorMySQL

# Así creas la conexión y se creará
# la base de datos en caso de que no exista
conector = ConectorMySQL(user='tu_usuario',
    password='tu_contraseña',
    db='nombre_de_tu_base_de_datos',
    host='localhost',
    port=3306
    )
```

### Subida de Dataframe

La clase `conector` proporciona una manera sencilla de interactuar con bases de datos SQL desde Python. Una de sus funcionalidades clave es la capacidad de subir un DataFrame de pandas directamente a una tabla en la base de datos.

## Pasos para Subir un DataFrame

1. **Creación instancia de la Clase `conector`:** Asegúrate de tener una instancia de la clase `conector` que esté conectada a la base de datos donde deseas subir el DataFrame.

2. **Preparar el DataFrame:** Asegúrate de que el DataFrame que deseas subir esté listo. Esto incluye tener los nombres de columnas y tipos de datos correctos que coincidan con la estructura de la tabla de destino en la base de datos.

3. **Utilizar el Método `subir_df`:** Utiliza el método `subir_df` de la clase `conector` para subir el DataFrame a la base de datos.

### Ejemplo de Código

```python
# Suponiendo que `conector` es una instancia de la clase `Conector` ya configurada
df = # tu DataFrame preparado
tabla_destino = 'nombre_de_tu_tabla'

# Subir el DataFrame a la tabla especificada
conector.subir_df(df, tabla_destino)
```

## Método `consultar` de la clase `Conector`

El método `consultar` es responsable de ejecutar una consulta SQL en una base de datos y devolver el resultado como un DataFrame de pandas.

### Parámetros

- `query`: Una cadena de texto (`str`) que contiene la consulta SQL a ejecutar. Es importante que la consulta especifique la base de datos a utilizar.

```python
# Suponiendo que `conector` es una instancia de la clase `Conector` ya configurada
query = '''SELECT * FROM base_de_datos.nombre_de_tu_tabla
            WHERE columna = 'valor';'''

df = conector.consultar(query=query)

print(df.head())
```