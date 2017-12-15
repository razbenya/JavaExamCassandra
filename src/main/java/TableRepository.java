import com.datastax.driver.core.Session;

/**
 * Created by RazB on 12/11/2017.
 */
public class TableRepository {
    private Session session;
    private String tableName;

    public TableRepository(Session session,String tableName){
        this.session = session;
        this.tableName = tableName;
    }

    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(tableName).append("(")
                .append("url text, ")
                .append("slice int,")
                .append("content text,")
                .append("PRIMARY KEY(url, slice));");
        String query = sb.toString();
        session.execute(query);
    }


    public void insert(int slice, String url,String content) {
        String fixcontent = content.replace("'","''");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(tableName).append("(slice, url, content) ")
                .append("VALUES (")
                .append(slice).append(", '")
                .append(url).append("', '")
                .append(fixcontent).append(" ' );");
        String query = sb.toString();
        session.execute(query);
    }

}
