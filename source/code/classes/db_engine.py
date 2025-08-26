from sqlalchemy import create_engine

class DatabaseEngine:
    _instances = {}
    def __new__(cls, db, server, username, password, db_type='postgresql', port=None):
        key = (db, server, username, db_type, port)
        if key not in cls._instances:
            instance = super(DatabaseEngine, cls).__new__(cls)
            instance.db = db
            instance.server = server
            instance.username = username
            instance.password = password
            instance.db_type = db_type
            instance.port = port
            instance.engine = instance._create_engine()
            cls._instances[key] = instance
        return cls._instances[key]

    def _create_engine(self):
        if self.db_type == 'postgresql':
            port_part = f":{self.port}" if self.port else ""
            connection_string = f"postgresql+psycopg2://{self.username}:{self.password}@{self.server}{port_part}/{self.db}"
        elif self.db_type == 'mysql':
            port_part = f":{self.port}" if self.port else ""
            connection_string = f"mysql+pymysql://{self.username}:{self.password}@{self.server}{port_part}/{self.db}"
        elif self.db_type == 'sqlite':
            connection_string = f"sqlite:///{self.db}"  
        else:
            raise ValueError(f"Unsupported db_type: {self.db_type}")
        return create_engine(connection_string)

    def get_engine(self):
        return self.engine
