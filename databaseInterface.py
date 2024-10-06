class DatabaseInterface(ABC):
    def __init__(self):
        self.connection = None

    @abstractmethod
    def connect(self, **kwargs):
        """Connect to the database."""
        pass

    @abstractmethod
    def disconnect(self):
        """Disconnect from the database."""
        pass

    @abstractmethod
    def query(self, sql_query: str, params: tuple = ()):
        """Execute a query on the database."""
        pass
