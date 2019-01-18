package focusedCrawler.target.repository;

import focusedCrawler.target.model.Page;
import focusedCrawler.util.CloseableIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;

public interface TargetRepository {
    //public ConcurrentHashMap<String,String> storage_map = new ConcurrentHashMap<>();

    public HashMap<String,String> storage_map = new HashMap<>();

    public boolean insert(Page target);

    public void close();

    public CloseableIterator<Page> pagesIterator();

}
