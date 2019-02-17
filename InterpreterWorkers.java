import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InterpreterWorkers {
    protected static final String COLON = ":";
    public static final Map<String, Enum> workers_lexemeMap;

    public enum WorkersTypes {WORKER_FIRST, WORKER_LAST, WORKER}

    static {
        workers_lexemeMap = new HashMap<>();
        workers_lexemeMap.put("WORKER_FIRST", WorkersTypes.WORKER_FIRST);
        workers_lexemeMap.put("WORKER_LAST", WorkersTypes.WORKER_LAST);
        workers_lexemeMap.put("WORKER", WorkersTypes.WORKER);
    }

    /**
     * interpret workers file
     *
     * @param config    name of file
     * @param resultMap map where we will put lexemes
     */
    public static int Interpreted(String config, Map<String, WorkerProperties> resultMap) {
        try
        {
            Reader reader = new Reader(config);
            String ParamString;


            while ((ParamString = reader.ReadLine()) != null) {
                String[] ParamPair = ParamString.split(COLON);
                if (!IsQuartetCorrect(ParamPair)) {
                    return -1;
                }
                WorkerProperties properties = new WorkerProperties(ParamPair[2], (WorkersTypes) workers_lexemeMap.get(ParamPair[0]), ParamPair[3]);/////&&&&&
                resultMap.put(ParamPair[1], properties);
            }

            if (!isAllWorkers(resultMap)) {
                Log.report("Incorrect types of workers");
                return -1;
            }

            reader.CloseStream();
         }
        catch (IOException e)
        {
            return -1;
        }
        return 0;
}

    /**
     *
     * @param lexeme current lexeme
     * @return true if have lexeme, false otherwise
     */
    public static boolean isLexeme(String lexeme)
    {
        for (String key : workers_lexemeMap.keySet())
        {
            if (lexeme.equals(key))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * is triplet correct or not
     * @param ParamPair checking pair
     * @return true if correct, false otherwise
     */
    public static boolean IsQuartetCorrect(String[] ParamPair)
    {
        if (ParamPair.length != 4|| ParamPair[0].isEmpty() || ParamPair[1].isEmpty()|| ParamPair[2].isEmpty()|| ParamPair[3].isEmpty())
        {
            Log.report("Invalid syntax in config file");
            return false;
        }
        if (!isLexeme(ParamPair[0]))
        {
            Log.report("Unknown lexeme " + ParamPair[0]);
            return false;
        }
        return true;
    }

    /**
     * check that we have all types of workers
     * @param resultMap workers, that we check
     * @return true if correct, false otherwise
     */
    public static boolean isAllWorkers(Map<String, WorkerProperties> resultMap)
    {
        if (isContainsFirst(resultMap) && isContainsLast(resultMap))
        {
            return  true;
        }
        return false;
    }

    /**
     * check that we have first worker
     * @param resultMap workers, that we check
     * @return true if correct, false otherwise
     */
    public static boolean isContainsFirst(Map<String, WorkerProperties> resultMap)
    {
        for (String worker_key: resultMap.keySet())
        {
            if(resultMap.get(worker_key).type_of_worker.equals(workers_lexemeMap.get("WORKER_FIRST")))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * check that we have last worker
     * @param resultMap workers, that we check
     * @return true if correct, false otherwise
     */
    public static boolean isContainsLast(Map<String, WorkerProperties> resultMap)
    {
        for (String worker_key: resultMap.keySet())
        {
            if(resultMap.get(worker_key).type_of_worker.equals(workers_lexemeMap.get("WORKER_FIRST")))
            {
                return true;
            }
        }
        return false;
    }
}
