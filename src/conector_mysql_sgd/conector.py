from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import pandas as pd

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
        self.url = 'mysql+mysqlconnector://{}:{}@{}:{}/information_schema'.format(user, password, host, port)

        self.engine = create_engine(self.url)

        self.Session = sessionmaker(bind=self.engine)

        self.session = self.Session()

        exp = text("CREATE DATABASE IF NOT EXISTS {};".format(db))

        self.session.execute(exp)

        use_exp = text("USE {};".format(db))

        self.session.execute(use_exp)

        self.session.close()

        print('Proceso exitoso: Base de datos creada o ya existente.')

        self.url = 'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(user, password, host, port, db)

        self.engine = create_engine(self.url)


    def subir_df(self, df, tabla):
        """
        Sube un DataFrame a una tabla específica en la base de datos.

        Parámetros:
        - df: DataFrame a subir.
        - tabla: Nombre de la tabla en la que se va a subir el DataFrame.

        """
        df.to_sql(tabla, self.engine, if_exists='replace', index=False)

        print('Proceso exitoso: DataFrame subido a la tabla {}.'.format(tabla))

    
    def consultar(self, query):
        """
        Realiza una consulta a la base de datos.

        Parámetros:
        - query: Consulta a realizar. Importante declarar la base a usar
        """
        query = text(query)
        df = pd.read_sql(query, self.engine)

        return df