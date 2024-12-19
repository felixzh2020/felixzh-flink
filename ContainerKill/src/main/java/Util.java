import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;


public class Util {
    public static HttpHost[] getHostArray(String esIps, int port) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] hosts = esIps.split(",");
        for (String host : hosts) {
            httpHostList.add(new HttpHost(host, port));
        }
        HttpHost[] array = new HttpHost[httpHostList.size()];
        return httpHostList.toArray(array);
    }
}
