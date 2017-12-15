import com.datastax.driver.core.Session;
import org.junit.*;
import com.datastax.driver.core.ResultSet;


import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by RazB on 12/15/2017.
 */


public class CassandraTests {
    private static CassandraConnector client;
    private static Session session;

    @BeforeClass
    public static void connect() {
        client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        session = client.getSession();
    }

    @Test
    public void testKeySpaceCreate(){
        KeyspaceRepository schemaRepository = new KeyspaceRepository(session);
        String keyspaceName = "testKeySpace";
        schemaRepository.createKeyspace(keyspaceName,"SimpleStrategy", 1);
        ResultSet res = session.execute("SELECT * from system.schema_keyspaces;");

        List<String> matchedKeyspaces = res.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);
        assertTrue(matchedKeyspaces.get(0).equals(keyspaceName.toLowerCase()));
    }

    @Test
    public void testTableCreate(){
        String tableName = "testKeySpace.testTable";
        TableRepository tableRepository = new TableRepository(session, tableName);
        tableRepository.createTable();

        ResultSet res = session.execute("SELECT * FROM " + tableName+";");
        List<String> columnNames = res.getColumnDefinitions().asList()
                .stream()
                .map(cl -> cl.getName())
                .collect(Collectors.toList());

        assertEquals(columnNames.size(), 3);
        assertTrue(columnNames.contains("url"));
        assertTrue(columnNames.contains("slice"));
        assertTrue(columnNames.contains("content"));
    }

    @Test
    public void testTableInsert(){
        String tableName = "testKeySpace.testTable";
        TableRepository tableRepository = new TableRepository(session, tableName);

        tableRepository.insert(0, "http://testUrl.com" , "<html> <head> <title> this is a test page");
        tableRepository.insert(1, "http://testUrl.com" , "</title> </head> <body> test ' body");
        tableRepository.insert(0, "http://differentUrl.com" , "different page");
        tableRepository.insert(2, "http://testUrl.com" , "</body></html>");

        ResultSet res = session.execute("SELECT * FROM " + tableName+" WHERE url = 'http://testUrl.com';");
        List<String> urlsList = res.all()
                .stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());

        assertEquals(urlsList.size(), 3);
        assertTrue(urlsList.contains("http://testUrl.com"));
        assertFalse(urlsList.contains("http://differentUrl.com"));

        res = session.execute("SELECT content FROM " + tableName+" WHERE url = 'http://testUrl.com' AND slice = 2;");
        List<String> contents = res.all()
                .stream()
                .map(row-> row.getString(0).trim())
                .collect(Collectors.toList());

        assertEquals(contents.size(), 1);
        assertEquals(contents.get(0),"</body></html>");
    }

    @AfterClass
    public static void close() {
        client.close();
    }
}
