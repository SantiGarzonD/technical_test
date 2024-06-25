from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Conector:
    """
    Una clase que representa un conector a una base de datos MySQL.

    Atributos:
        user (str): El nombre de usuario para la conexión a la base de datos.
        password (str): La contraseña para la conexión a la base de datos.
        db (str): El nombre de la base de datos.
        host (str): El nombre del host del servidor de la base de datos (por defecto es 'localhost').
        port (int): El número de puerto del servidor de la base de datos (por defecto es 3306).

    Métodos:
        subir_df: Sube un DataFrame de pandas a una tabla especificada en la base de datos.
    """

    def __init__(self, user, password, db, host='localhost', port=3306):
        """
        Inicializa una nueva instancia de la clase Conector.

        Args:
            user (str): El nombre de usuario para la conexión a la base de datos.
            password (str): La contraseña para la conexión a la base de datos.
            db (str): El nombre de la base de datos (Si no existe la crea).
            host (str, opcional): El nombre del host del servidor de la base de datos (por defecto es 'localhost').
            port (int, opcional): El número de puerto del servidor de la base de datos (por defecto es 3306).
        """

        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.port = port
        self.url = 'mysql+pymysql://{}:{}@{}:{}/information_schema'.format(user, password, host, port)

        self.engine = create_engine(self.url)

        conn = self.engine.connect()
        conn.execute('commit')
        conn.execute('create database if not exists {}'.format(self.db))
        conn.close()

        self.Session = sessionmaker(bind=self.engine)


    def subir_df(self, df, tabla):
        """
        Sube un DataFrame a una tabla específica en la base de datos.

        Parámetros:
        - df: DataFrame a subir.
        - tabla: Nombre de la tabla en la que se va a subir el DataFrame.

        """
        df.to_sql(tabla, self.engine, if_exists='replace', index=False)
