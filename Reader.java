import java.io.*;

public class Reader {

    private BufferedReader inputBReader;

    /**
     * create reader
     * @param fileName name of file
     */
    Reader(String fileName) throws IOException
    {
        OpenStream(fileName);
    }

    /**
     * open stream
     * @param fileName name of file
     */
    public void OpenStream(String fileName) throws IOException
    {
        try
        {
            inputBReader = new BufferedReader( new InputStreamReader(new FileInputStream(fileName), "Cp1251"));
        }
        catch (FileNotFoundException e)
        {
            Log.report("Can't open the file " + fileName);
            throw new IOException();
        }
    }

    /**
     * read one line from file
     * @return read line
     */
    public String ReadLine() throws IOException
    {
        try
        {
            if (inputBReader != null)
            {
                return inputBReader.readLine();
            }
        }
        catch (IOException e)
        {
            Log.report("Can't read string");
            throw new IOException();
        }
        return null;
    }

    /**
     * close stream
     */
    public void CloseStream() throws IOException
    {
        try
        {
            if (inputBReader != null)
            {
                inputBReader.close();
            }
        }
        catch (IOException e)
        {
            Log.report("Input stream can't be closed");
            throw new IOException();
        }
    }
}
