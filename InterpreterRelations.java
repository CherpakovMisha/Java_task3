import java.io.IOException;
import java.util.Map;

public class InterpreterRelations {
    protected static final String COLON = ":";
    protected static final String RELATION_COLON = ",";

    /**
     * interpret relations file
     * @param config name of file
     * @param resultMap map where we will put lexemes
     */
    public static int Interpreted(String config, Map<String, String[]> resultMap) {
        try {
            Reader reader = new Reader(config);
            String ParamString;

            while ((ParamString = reader.ReadLine()) != null) {
                String[] ParamPair = ParamString.split(COLON);
                String[] Arr_executors = ParamPair[1].split(RELATION_COLON);
                if (!IsPairCorrect(ParamPair) && !IsExecutorsCorrect(Arr_executors)) {
                    return -1;
                }
                resultMap.put(ParamPair[0], Arr_executors);
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
     * is pair correct or not
     * @param ParamPair checking pair
     * @return true if correct, false otherwise
     */
    public static boolean IsPairCorrect(String[] ParamPair)
    {
        if (ParamPair.length != 2|| ParamPair[0].isEmpty() || ParamPair[1].isEmpty())
        {
            Log.report("Invalid syntax in config file");
            return false;
        }
        return true;
    }

    public static boolean IsExecutorsCorrect(String[] Arr_executors)
    {
        if (Arr_executors.length == 0)
        {
            Log.report("Invalid ex sequence");
            return false;
        }
        for (String str:Arr_executors) {
            if (str.isEmpty()) {
                Log.report("Invalid ex sequence");
                return false;
            }
        }
        return true;
    }
}
