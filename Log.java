import java.io.FileWriter;
import java.io.IOException;

public class Log {
    private static FileWriter logWriter;
    public static String lineSeparator = System.getProperty("line.separator");

    /**
     * init log file
     */
    private static void init()
    {
        try
        {
            logWriter = new FileWriter("log.log");
        }
        catch (IOException e)
        {
            System.out.println("Can't create log file");
            e.printStackTrace();
        }
        report("Program start");
    }

    /**
     * report to log
     * @param report string with message
     */
    public static void report(String report)
    {
        if (logWriter == null)
        {
            init();
        }
        try
        {
            logWriter.write(report);
            logWriter.write(lineSeparator);
            logWriter.flush();
        }
        catch (IOException e)
        {
            System.out.println("Can't write to log file");
            e.printStackTrace();
        }
    }

    /**
     * close log file
     */
    public static void close()
    {
        try
        {
            report("Program end");
            logWriter.flush();
            logWriter.close();
        }
        catch (IOException e)
        {
            System.out.println("Can't close log file");
            e.printStackTrace();
        }
    }
}
