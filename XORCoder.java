import java.io.*;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class XORCoder implements Executor {

    private DataInputStream input = null;
    private DataOutputStream output = null;
    private ArrayList<Executor> Consumers = new ArrayList<>();
    public byte[] block = null;
    private int counter = 0;

    private int CODE_MODE;
    private int BLOCK_LENGTH;
    private String KEY;

    public byte[] out_array;

    private APPROPRIATE_TYPES[] arr_types = {APPROPRIATE_TYPES.BYTE};
    private Map<Executor,Pair> adapters_map = new HashMap<>();


    public XORCoder(){
    }

    public int setConfig(String config)
    {
        Map<Enum, String> paramMap = new EnumMap(InterpretWorkerConfig.Params.class);
        int err = InterpretWorkerConfig.InterpretWorker(config, paramMap);
        if (err != 0)
            return -1;
        int code_mode = Integer.parseInt(paramMap.get(InterpretWorkerConfig.Params.CODE_MODE));
        int block_length = Integer.parseInt(paramMap.get(InterpretWorkerConfig.Params.BLOCK_LENGTH));
        String key = paramMap.get(InterpretWorkerConfig.Params.KEY);
        this.CODE_MODE = code_mode;
        this.BLOCK_LENGTH = block_length;
        this.KEY = key;

        return 0;
    }

    public void setInput (DataInputStream input)
    {
        this.input = input;
    }

    public void setOutput (DataOutputStream output)
    {
        this.output = output;
    }

    public int setConsumer(Executor consumer)
    {
        int is_no_equal = 1;
        APPROPRIATE_TYPES[] cons_types = consumer.getConsumedTypes();
        APPROPRIATE_TYPES equal_type = APPROPRIATE_TYPES.BYTE;

        for (APPROPRIATE_TYPES prov_type: arr_types)
        {
            for (APPROPRIATE_TYPES cons_type:cons_types)
            {
                if (prov_type == cons_type)
                {
                    equal_type = prov_type;
                    is_no_equal = 0;
                }
            }
        }
        if (is_no_equal == 1)
        {
            Log.report("No equal appropriate types found");
            return -1;
        }
        consumer.setAdapter(this, getterByte, equal_type);
        this.Consumers.add(consumer);
        return 0;
    }

    public APPROPRIATE_TYPES[] getConsumedTypes()
    {
        return arr_types;
    }

    public void setAdapter(Executor provider, Object adapter, APPROPRIATE_TYPES type)
    {
        Pair var = new Pair(adapter, type);
        adapters_map.put(provider, var);
    }

    class GetterByte implements InterfaceByteTransfer
    {
        public Byte getNextByte()
        {
            if (counter == out_array.length)
            {
                counter = 0;
                return null;
            }
            return out_array[counter++];
        }
    }
    GetterByte getterByte = new GetterByte();

    public int run()
    {
        int error = 0;
        while (error == 0)
        {
            error = ReadToArray();
            if (error == -1) {
                return -1;
            }
            out_array = block;
            for(Executor consumer: Consumers)
            {
                int err_put = consumer.put(this);
                if (err_put != 0) {
                    return err_put;
                }
            }
            block = null;
        }
        return 0;
    }

    public int put(Executor provider)
    {
        int err;
        if (CheckOutput() == -1)
            return -1;


        if (output != null)
        {
            err = PrintOutput(provider);
            block = null;
        }
        else
        {
            block = getData(provider);
            if (CODE_MODE == 0)
            {
                RunEncode();
            }
            else
            {
                RunDecode();
            }
            block = null;
            int err_put = 0;
            for(Executor consumer: Consumers)
            {
                err_put = consumer.put(this);
                if (err_put != 0) {
                    break;
                }
            }
            err = err_put;
        }
        out_array = null;
        return err;
    }

    private byte[] getData(Executor provider)
    {
        Byte var_byte = null;
        byte[] var_array = new byte[BLOCK_LENGTH];
        Pair adapter_pair = adapters_map.get(provider);
        if (adapter_pair.type == APPROPRIATE_TYPES.BYTE)
        {
            InterfaceByteTransfer byteTransfer = (InterfaceByteTransfer)adapter_pair.Adapter;

            for(int i = 0; i < BLOCK_LENGTH; ++i)
            {
                var_byte = byteTransfer.getNextByte();
                if (var_byte == null)
                    break;
                var_array[i] = var_byte;
                if (var_array[i] == -1)
                    break;
            }
            while (var_byte != null)
            {
                var_byte = byteTransfer.getNextByte();
            }
        }
        return var_array;
    }




    /**
     * run encode of data block
     */
    private void RunEncode()
    {
        byte[] key = KEY.getBytes();
        byte[] res = new byte[block.length];
        for (int i = 0; i < block.length; i++) {
            res[i] = (byte) (block[i] ^ key[i % key.length]);
        }
        out_array = new byte[block.length];
        out_array = res;
    }


    /**
     * run decode of data block
     */
    private void RunDecode()
    {
        byte[] res = new byte[block.length];
        byte[] key = KEY.getBytes();

        for (int i = 0; i < block.length; i++) {
            res[i] = (byte) (block[i] ^ key[i % key.length]);
        }
        out_array = res;
    }

    /**
     * check output and consumer do not exist together
     * @return 0 if correct, -1 otherwise
     */
    private int CheckOutput()
    {
        if (output != null && Consumers.size() != 0)
        {
            Log.report("Incorrect worker");
            return -1;
        }

        if (output == null && Consumers.size() == 0)
        {
            Log.report("Incorrect worker");
            return -1;
        }
        return 0;
    }

    /**
     * read block from input file
     * @return 0 if correct, -2 if get EOF, -1 otherwise
     */
    private int ReadToArray()
    {
        int k = 0;
        byte[] array = new byte[BLOCK_LENGTH];
        try
        {
            byte i = 0;

            while (i != -1 && k < BLOCK_LENGTH)
            {
                i = input.readByte();
                array[k] = i;
                k++;
            }
        }
        catch (EOFException e)
        {
            if (k < BLOCK_LENGTH)
                array[k] = -1;
            if (k == 0)
                return -1;
            block = array;
            return -2;
        }
        catch (IOException e)
        {
            Log.report("Can't encode file");
            return -1;
        }
        block = array;
        return 0;
    }

    /**
     * print block to output file
     * @param provider executor with data to print
     * @return 0 if correct, -2 if get EOF, -1 otherwise
     */
    private int PrintOutput(Executor provider)
    {
        Byte var_byte = 1;
        Pair adapter_pair = adapters_map.get(provider);
        if (adapter_pair.type == APPROPRIATE_TYPES.BYTE)
        {
            InterfaceByteTransfer byteTransfer = (InterfaceByteTransfer)adapter_pair.Adapter;
            while (true)
            {
                var_byte = byteTransfer.getNextByte();
                if (var_byte == null || var_byte == -1)
                    break;
                try {
                    output.writeByte(var_byte);
                    output.flush();
                }
                catch (IOException e)
                {
                    Log.report("Can't write to output file");
                    return -1;
                }
            }
        }
        return 0;
    }
}
