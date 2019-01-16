package focusedCrawler.target.repository;

import focusedCrawler.target.model.Page;
import focusedCrawler.util.CloseableIterator;
import java.util.concurrent.ConcurrentHashMap;

public interface TargetRepository {
    public ConcurrentHashMap<String,String> storage_map = new ConcurrentHashMap<>();

    public boolean insert(Page target);

    public void close();

    public CloseableIterator<Page> pagesIterator();

}
