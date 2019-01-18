package focusedCrawler.target.repository;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import focusedCrawler.target.TargetStorageConfig;
import focusedCrawler.target.model.Page;
import focusedCrawler.util.CloseableIterator;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;


public class AmazonS3TargetRepository implements TargetRepository {
    private static final Logger logger = LoggerFactory.getLogger(AmazonS3TargetRepository.class);
    private final AmazonS3 awsClient;
    private final String bucketName;
    private final String region;
    private boolean bucketPresent;
    private boolean hashFilename;

    public AmazonS3TargetRepository(TargetStorageConfig config) {
        if(config.getBucketName()==null){
            throw new NullPointerException("Bucket Name cannot be null");
        }

        this.awsClient = AmazonS3ClientBuilder.defaultClient();
        this.bucketName = config.getBucketName();
        this.region = config.getAwsS3Region();
        this.hashFilename = config.getHashFileName();
    }

//    private String getBucketName(Path directory){
//        String crawlerId = directory.toString().replace('_','\0'); // other method could be passing crawlerId through constructors
//        Date date= new Date();
//        String bName = crawlerId.toLowerCase()+"-"+date.getTime();
//        if(bName.length()>63){
//            int extra = bName.length() - 63; // aws bucket names can only be 63 characters lowercased
//            bName = crawlerId.toLowerCase().substring(0,crawlerId.length()-extra)+"-"+date.getTime();
//        }
//        return bName;
//    }

    private synchronized void createBucket(){
        if(!bucketPresent) {
            try {
                List<Bucket> buckets = awsClient.listBuckets();
                for(Bucket b : buckets){
                    if (b.getName().equals(bucketName)){
                        bucketPresent = true;
                        return;
                    }
                }
                CreateBucketRequest cbr = new CreateBucketRequest(bucketName, region);
                awsClient.createBucket(cbr);
                bucketPresent = true;
            } catch (AmazonS3Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public boolean insert(Page target) {
        if(!bucketPresent){
            this.createBucket();
        }

        try {
            String id = target.getURL().toString();
            URL url = new URL(id);
            Path hostPath = getHostPath(url);
            Path filePath = getFilePath(id, hostPath);
            String filePathWithCrawlerId = Paths.get(target.getCrawlerId(), filePath.toString()).toString();
            storage_map.put(id, filePathWithCrawlerId);
            try{
//                synchronized (this){
                    awsClient.putObject(bucketName, filePathWithCrawlerId, target.getContentAsString());
//                }
            }catch(AmazonServiceException e){
                logger.error(e.getMessage());
            }

        }catch(MalformedURLException | UnsupportedEncodingException e){
            logger.error(e.getMessage());
        }

        return false;
    }


    @Override
    public void close() {    }

    @Override
    public CloseableIterator<Page> pagesIterator() {
        throw new UnsupportedOperationException("Iterator not supportted for AmazonS3TargetRepository yet");
    }

    private Path getHostPath(URL url) throws MalformedURLException, UnsupportedEncodingException {
        String host = url.getHost();
        return Paths.get(URLEncoder.encode(host, "UTF-8"));
    }

    private Path getHostPath(String url) throws MalformedURLException, UnsupportedEncodingException {
        return getHostPath(new URL(url));
    }

    private Path getFilePath(String url, Path hostPath) throws UnsupportedEncodingException {
        Path filePath;
        if (hashFilename) {
            String filenameEncoded = DigestUtils.sha256Hex(url);
            filePath = hostPath.resolve(filenameEncoded);
        } else {
            filePath = hostPath.resolve(URLEncoder.encode(url, "UTF-8"));
        }
        return filePath;
    }
}
