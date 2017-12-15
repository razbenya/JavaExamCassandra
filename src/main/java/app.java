
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.io.*;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by RazB on 12/11/2017.
 */
public class app {
    public static final String KEYSPACE_NAME = "razKeySpace";
    public static final String STRATEGY = "SimpleStrategy";
    public static final String TABLE_NAME = "slices";
    public static final int MAX_SLICE_SIZE = 10240; // 10kbs
    public static final int MAX_THREADS = 10;


    public static void main(String[] args){
        KeyspaceRepository schemaRepository;
        TableRepository tableRepository;
        Session session;
        URL urlObj;
        InputStream inputStream = null;
        ByteArrayOutputStream baos;
        ExecutorService es;
        int sliceCounter = 0;
        String url;
        String ip;
        int port;

        if(args.length >= 3) {
            url = args[0];
            ip = args[1];
            port = Integer.parseInt(args[2]);
        } else {
            System.out.println("the number of arguments is invalid");
            return;
        }

        //connect to cassandra
        CassandraConnector client = new CassandraConnector();
        client.connect(ip, port);
        session = client.getSession();

        //create keyspace if not exist
        schemaRepository = new KeyspaceRepository(session);
        schemaRepository.createKeyspace(KEYSPACE_NAME, STRATEGY, 1);


        //create table if not exist
        tableRepository = new TableRepository(session,KEYSPACE_NAME + "." + TABLE_NAME);
        tableRepository.createTable();

        es = Executors.newFixedThreadPool(MAX_THREADS);
        baos = new ByteArrayOutputStream();
        try {
            urlObj = new URL(url);
            inputStream = urlObj.openStream();
            byte[] byteChunk = new byte[MAX_SLICE_SIZE]; // read up to MAX_SLICE_SIZE at a time
            for (int n ;(n = inputStream.read(byteChunk, 0, MAX_SLICE_SIZE)) > 0 ; ) {
                baos.write(byteChunk, 0, n);
                baos.flush();

                //handle the current slice. (The thread inserts the slice to the db)
                String content = new String(baos.toByteArray());
                SliceHandler sliceHandler = new SliceHandler(tableRepository, url, sliceCounter,content);
                es.execute(sliceHandler);

                //increment the slice counter
                sliceCounter++;

                //reset the byte stream for the next slice
                baos.reset();

            }
            es.shutdown();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                if (inputStream != null)
                    inputStream.close();
            } catch (IOException ioe) { ioe.printStackTrace(); }

            try {
                es.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                client.close();
            }
        }
    }
}
