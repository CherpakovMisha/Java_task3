import java.io.*;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class Manager {
    private Map<Enum, String> configMap;
    private Map<String, Executor> conveyorMap;
    private Executor first_worker;
    private boolean is_ready_to_run = false;

    /**
     * create manager with conveyor inside
     * @param config name of config file
     */
    Manager(String config)
    {
        configMap = new EnumMap(InterpreterConfig.Params.class);
        InterpreterConfig.Interpreted(config, configMap);

        Map<String, WorkerProperties> workersMap = new HashMap<>();
        Map<String, String[]> workers_relationsMap = new HashMap<>();
        int err1 = InterpreterWorkers.Interpreted(configMap.get(InterpreterConfig.Params.WORKERS_FILE), workersMap);
        int err2 = InterpreterRelations.Interpreted(configMap.get(InterpreterConfig.Params.CONNECTIONS_FILE), workers_relationsMap);

        conveyorMap = new HashMap<>();
        int err3 = CreateConveyorMap(workersMap, workers_relationsMap);

        if (err1 == 0 && err2 == 0 && err3 == 0)
            is_ready_to_run = true;
        else
        {
            Log.report("Can not create manager");
        }
    }

    /**
     * create conveyor map
     * @param workersMap map with interpreted workers file
     * @param workers_relationsMap map with interpreted relations file
     */
    private int CreateConveyorMap(Map<String, WorkerProperties> workersMap, Map<String, String[]> workers_relationsMap)
    {
            for (String key : workersMap.keySet()) {
                String input_file, output_file;
                Executor executor;
                try {
                    Class<?> Var = Class.forName(workersMap.get(key).class_name);
                    executor = (Executor)Var.newInstance();
                }
                catch (ClassNotFoundException e)
                {
                    Log.report("Class hadn't found");
                    return -1;
                }
                catch (InstantiationException e)
                {
                    Log.report("InstantiationException found");
                    return -1;
                }
                catch (IllegalAccessException b)
                {
                    Log.report("IllegalAccessException found");
                    return -1;
                }
                int err_conf = executor.setConfig(workersMap.get(key).config_name);
                if (err_conf != 0)
                    return -1;
                InterpreterWorkers.WorkersTypes type = workersMap.get(key).type_of_worker;
                if (type == InterpreterWorkers.WorkersTypes.WORKER_FIRST) {
                    input_file = configMap.get(InterpreterConfig.Params.INPUT_FILE);
                    try {
                        DataInputStream input = new DataInputStream(new FileInputStream(input_file));
                        executor.setInput(input);
                        first_worker = executor;
                    } catch (IOException ex) {

                        Log.report("Can't open input_file");
                        return -1;
                    }
                } else if (type == InterpreterWorkers.WorkersTypes.WORKER_LAST) {
                    output_file = configMap.get(InterpreterConfig.Params.OUTPUT_FILE);
                    try {
                        DataOutputStream output = new DataOutputStream(new FileOutputStream(output_file));
                        executor.setOutput(output);
                    } catch (IOException ex) {

                        Log.report("Can't open output_file");
                        return -1;
                    }
                }
                conveyorMap.put(key, executor);
            }
        for (String key : workers_relationsMap.keySet())
        {
            Executor Worker = conveyorMap.get(key);
            String[] cons_arr = workers_relationsMap.get(key);
            for (String consumer: cons_arr)
            {
                Executor Consumer = conveyorMap.get(consumer);
                if (Worker.setConsumer(Consumer) != 0)
                {
                    Log.report("Can't link workers");
                    return -1;
                }
            }

        }
        return 0;
    }

    /**
     * start conveyor work
     */
    public void Run()
    {
        if (is_ready_to_run)
            first_worker.run();
    }
}
