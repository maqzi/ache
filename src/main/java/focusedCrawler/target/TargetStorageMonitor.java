package focusedCrawler.target;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Scanner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

import focusedCrawler.target.model.Page;
import focusedCrawler.target.repository.TargetRepository;
import focusedCrawler.util.MetricsManager;

public class TargetStorageMonitor {
    
    private PrintWriter fCrawledPages;
    private PrintWriter fRelevantPages;
    private PrintWriter fNonRelevantPages;
    private PrintWriter fHarvestInfo;
    private PrintWriter fStorageMap;
    
    private Counter relevantUrlsDownloaded;
    private Counter totalNumberOfUrlsDownloaded;
    
    int totalOnTopicPages = 0;
    private int totalOfPages = 0;

    public TargetStorageMonitor(String dataPath) {
    	this(dataPath, new MetricsManager(dataPath));
    }
    
    public TargetStorageMonitor(String dataPath, MetricsManager metricsManager) {
        
        File file = new File(dataPath+"/data_monitor/");
        if(!file.exists()) {
            file.mkdirs();
        }
        
        String fileCrawledPages = dataPath + "/data_monitor/crawledpages.csv";
        String fileRelevantPages = dataPath + "/data_monitor/relevantpages.csv";
        String fileHarvestInfo = dataPath + "/data_monitor/harvestinfo.csv";
        String fileNonRelevantPages = dataPath + "/data_monitor/nonrelevantpages.csv";
        String fileStorageMap = dataPath + "/data_monitor/storagemap.csv";
        
        try {
            fCrawledPages = createBufferedWriter(fileCrawledPages);
            fRelevantPages = createBufferedWriter(fileRelevantPages);
            fHarvestInfo = createBufferedWriter(fileHarvestInfo);
            fNonRelevantPages = createBufferedWriter(fileNonRelevantPages);
            fStorageMap = createBufferedWriter(fileStorageMap);
            fStorageMap.printf("%s\t%s\n","original_url","location");
        } catch (Exception e) {
            throw new IllegalStateException("Problem while opening files to export target metrics", e);
        }
        setupMetrics(metricsManager);
    }

    private void setupMetrics(MetricsManager metrics) {
    	totalNumberOfUrlsDownloaded = metrics.getCounter("target.storage.pages.downloaded");
        relevantUrlsDownloaded = metrics.getCounter("target.storage.pages.relevant");
        
        Gauge<Double> harvestRateGauge = () -> ((double)totalOnTopicPages / (double)totalOfPages);
        metrics.register("target.storage.harvest.rate", harvestRateGauge);
    }
    
    private PrintWriter createBufferedWriter(String file) throws FileNotFoundException {
        boolean append = true;
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, append));
        boolean autoFlush = true;
        return new PrintWriter(bos, autoFlush);
    }
    
    public synchronized void countPage(Page page, boolean isRelevant, double prob) {
        long currentTime;
        if (page.getFetchTime() == 0) {
            currentTime = System.currentTimeMillis();
        } else {
            currentTime = page.getFetchTime();
        }
        totalOfPages++;
        totalNumberOfUrlsDownloaded.inc();
        fCrawledPages.printf("%s\t%d\n", page.getURL().toString(), currentTime);
        fHarvestInfo.printf("%d\t%d\t%d\n", totalOnTopicPages, totalOfPages, currentTime);
        if(isRelevant) {
            totalOnTopicPages++;
            relevantUrlsDownloaded.inc();
            fRelevantPages.printf("%s\t%.10f\t%d\n", page.getURL().toString(), prob, currentTime);
        } else {
            fNonRelevantPages.printf("%s\t%.10f\t%d\n", page.getURL().toString(), prob, currentTime);
        }

        String address = null;
//        try {
//            address = InetAddress.getByName(page.getURL().getHost()).getHostAddress();
//        }catch (UnknownHostException uhe){
//
//        }
        fStorageMap.printf("%s\t%s\n",page.getURL(),TargetRepository.storage_map.get(page.getURL().toString()));
    }

    public int getTotalOfPages() {
        return totalOfPages;
    }

    public static HashSet<String> readRelevantUrls(String dataPath) {
        String fileRelevantPages = dataPath + "/data_monitor/relevantpages.csv";
        HashSet<String> relevantUrls = new HashSet<>();
        try(Scanner scanner = new Scanner(new File(fileRelevantPages))) {
            while(scanner.hasNext()){
                String nextLine = scanner.nextLine();
                String[] splittedLine = nextLine.split("\t");
                if(splittedLine.length == 3) {
                    String url = splittedLine[0];
                    relevantUrls.add(url);
                }
            }
            return relevantUrls;
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to load relevant URL from target monitor file: "+fileRelevantPages);
        }
    }

    public void close() {
        fCrawledPages.close();
        fHarvestInfo.close();
        fNonRelevantPages.close();
        fRelevantPages.close();
        fStorageMap.close();
    }
    
}
