public class WorkerProperties {

    public String config_name;
    public InterpreterWorkers.WorkersTypes type_of_worker;
    public String class_name;


    /**
     * create class with name of config and type of worker
     * @param filename name of worker_config
     * @param type type of worker
     */
    WorkerProperties(String filename, InterpreterWorkers.WorkersTypes type, String class_name)
    {
        this.config_name = filename;
        this.type_of_worker = type;
        this.class_name = class_name;
    }
}
