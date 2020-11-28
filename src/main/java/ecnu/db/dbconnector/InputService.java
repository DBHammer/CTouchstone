package ecnu.db.dbconnector;

public class InputService {
    public static final InputService inputService = new InputService();
    private DbConnector databaseConnectorInterface;

    public DbConnector getDatabaseConnectorInterface() {
        return databaseConnectorInterface;
    }

    public void setDatabaseConnectorInterface(DbConnector databaseConnectorInterface) {
        this.databaseConnectorInterface = databaseConnectorInterface;
    }

    public static InputService getInputService(){
        return inputService;
    }

    private InputService() {
    }
}
