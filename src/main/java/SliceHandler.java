/**
 * Created by RazB on 12/14/2017.
 */
public class SliceHandler implements Runnable {
    private String url;
    private int sliceIndex;
    private String sliceContent;
    private TableRepository table;

    public SliceHandler(TableRepository table, String url,int sliceIndex, String sliceContent){
        this.table = table;
        this.url = url;
        this.sliceIndex = sliceIndex;
        this.sliceContent = sliceContent;
    }

    public void run() {
        table.insert(sliceIndex, url, sliceContent);
    }
}
