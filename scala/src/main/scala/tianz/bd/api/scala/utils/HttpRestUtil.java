package tianz.bd.api.scala.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class HttpRestUtil {

  private static Logger logger = LoggerFactory.getLogger(HttpRestUtil.class);

  public static final String ompApiKey = "79b647961ad9453d998d1540a6c17001";
  public static final String default_apiKey = "9cc4871e46094635a19d26557f9bb7f4";

  private static  RequestConfig requestConfig = RequestConfig.custom()
          .setConnectTimeout(5000).setConnectionRequestTimeout(5000)
          .setSocketTimeout(6000).build();

  public static String get(String url) throws Exception {
    return get(url, createHeader(), null, null);
  }

  public static String get(String url, String username, String password) throws Exception {
    return get(url, createHeader(), username, password);
  }

  public static String get(String url, Map<String, String> headers, String username, String password) throws Exception {
    return get(url, null, headers, username, password);
  }

  public static HttpResponse get(CloseableHttpClient httpClient, HttpUriRequest request) throws IOException {
    return httpClient.execute(request);
  }

  public static String get(String url, String cookie, Map<String, String> headers, String username, String password)
          throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    if (url.startsWith("https")) {
      httpClient = createSSLInsecureClient();
    }
    String result = null;
    url = url.replaceAll(" ", "%20");
    HttpGet request = new HttpGet(url);
    RequestConfig defaultRequestConfig = RequestConfig.custom()
            .setSocketTimeout(5000)
            .setConnectTimeout(5000)
            .setConnectionRequestTimeout(5000)
            .build();
    request.setConfig(defaultRequestConfig);
    if (headers != null) {
      for (Entry<String, String> entry : headers.entrySet()) {
        request.setHeader(entry.getKey(), entry.getValue());
      }
    }

    if (cookie != null) {
      request.setHeader("Cookie", cookie);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("executing request to " + url);
    }

    HttpClientContext context = null;
    if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
      context = new HttpClientContext();
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(username, password));
      context.setCredentialsProvider(credentialsProvider);
    }

    HttpResponse httpResponse = httpClient.execute(request, context);
    HttpEntity entity = httpResponse.getEntity();

    if (entity != null) {
      result = EntityUtils.toString(entity, "utf-8");
    }

    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (logger.isDebugEnabled()) {
      logger.debug("executing request result :{}", result);
    }
    if (200 != statusCode) {
      logger.warn("请求返回CODE：" + statusCode + "返回数据：" + result);
      throw new RuntimeException(result);
    }

    return result;
  }

  public static CloseableHttpClient createHttpClient(String url) {

    CloseableHttpClient httpClient = HttpClients.createDefault();
    if (httpClient == null) {
      httpClient = HttpClients.createDefault();
    }

    if (url.startsWith("https")) {
      logger.info("request post create SSL Insecure Client start");
      httpClient = createSSLInsecureClient();
      logger.info("request post create SSL Insecure Client start");
    }
    return httpClient;

  }

  public static String post(CloseableHttpClient httpClient, HttpUriRequest request) throws Exception {
    HttpResponse httpResponse = httpClient.execute(request);
    HttpEntity entity = httpResponse.getEntity();
    String result = null;
    if (entity != null) {
      result = EntityUtils.toString(entity);
    }
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (200 != statusCode) {
      logger.warn("请求返回CODE：" + statusCode + "返回数据：" + result);
      throw new RuntimeException(result);
    }

    return result;
  }

  public static String post(String url, Map<String, String> headers, String jsonString,
                            String username, String password) throws Exception {
    return post(url, null, headers, jsonString, username, password);
  }

  public static String post(String url, Map<String, String> headers, String jsonString) throws Exception {
    return post(url, null, headers, jsonString, null, null);
  }

  public static String post(String url, String jsonString) throws Exception {
    return post(url, createHeader(), jsonString, null, null);
  }

  public static String post(String url, String jsonString, String username, String password) throws Exception {
    return post(url, createHeader(), jsonString, username, password);
  }

  public static Map<String, String> createOmpHeader() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json;charset=utf-8");
    headers.put("Content-Type", "application/json;charset=utf-8");
    headers.put("apikey",ompApiKey);
    return headers;
  }

  private static Map<String, String> createHeader() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json;charset=utf-8");
    headers.put("Content-Type", "application/json;charset=utf-8");
//    headers.put("apikey", default_apiKey);
    return headers;
  }

  public static String post(String url, String cookie, Map<String, String> headers, String jsonString,
                            String username, String password) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    String result = null;
    try{
      if (url.startsWith("https")) {
        httpClient = createSSLInsecureClient();
      }
      HttpPost postRequest = new HttpPost(url);
      postRequest.setConfig(requestConfig);
      if (headers != null) {
        for (Entry<String, String> entry : headers.entrySet()) {
          postRequest.setHeader(entry.getKey(), entry.getValue());
        }
      }
      if (cookie != null) {
        postRequest.setHeader("Cookie", cookie);
      }
      if (!StringUtils.isBlank(jsonString)) {
        StringEntity entity = new StringEntity(jsonString, "utf-8");
        postRequest.setEntity(entity);
      }
      logger.debug("executing request to " + url);
      HttpClientContext context = null;
      if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
        context = new HttpClientContext();
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
        context.setCredentialsProvider(credentialsProvider);
      }
      HttpResponse httpResponse = httpClient.execute(postRequest, context);
      HttpEntity entity = httpResponse.getEntity();
      if (entity != null) {
        result = EntityUtils.toString(entity);
      }
      int statusCode = httpResponse.getStatusLine().getStatusCode();
      if (200 != statusCode) {
        logger.warn("请求返回CODE：" + statusCode + "返回数据：" + result);
        throw new RuntimeException(result);
      }
    }catch (Exception e){
      throw e;
    }finally {
      if(null!=httpClient){
        httpClient.close();
      }
    }
    return result;
  }

  public static String put(String url, String jsonString) throws Exception {
    return put(url, createHeader(), jsonString, null, null);
  }

  public static String put(String url, String jsonString, String username, String password)
          throws Exception {
    return put(url, createHeader(), jsonString, username, password);
  }

  public static String put(String url, Map<String, String> headers, String jsonString,
                           String username, String password) throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    String result = null;

    if (url.startsWith("https")) {
      httpClient = createSSLInsecureClient();
    }

    HttpPut putRequest = new HttpPut(url);
    if (headers != null) {
      for (Entry<String, String> entry : headers.entrySet()) {
        putRequest.setHeader(entry.getKey(), entry.getValue());
      }
    }

    if (!StringUtils.isBlank(jsonString)) {
      StringEntity entity = new StringEntity(jsonString, "utf-8");
      putRequest.setEntity(entity);
    }

    logger.debug("executing request to " + url);
    HttpClientContext context = null;
    if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
      context = new HttpClientContext();
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(username, password));
      context.setCredentialsProvider(credentialsProvider);
    }
    HttpResponse httpResponse = httpClient.execute(putRequest, context);
    HttpEntity entity = httpResponse.getEntity();

    if (entity != null) {
      result = EntityUtils.toString(entity);
    }

    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (200 != statusCode) {
      logger.warn("请求返回CODE：" + statusCode + "返回数据：" + result);
      throw new RuntimeException(result);
    }

    return result;
  }

  public static String delete(String url) throws Exception {
    return delete(url, createHeader(), null, null);
  }

  public static String delete(String url, String username, String password) throws Exception {
    return delete(url, createHeader(), username, password);
  }

  public static String delete(String url, Map<String, String> headers, String username, String password)
          throws Exception {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    String result = null;

    if (url.startsWith("https")) {
      httpClient = createSSLInsecureClient();
    }

    HttpDelete delRequest = new HttpDelete(url);
    if (headers != null) {
      for (Entry<String, String> entry : headers.entrySet()) {
        delRequest.setHeader(entry.getKey(), entry.getValue());
      }
    }

    logger.debug("executing request to " + url);
    HttpClientContext context = null;
    if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
      context = new HttpClientContext();
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(username, password));
      context.setCredentialsProvider(credentialsProvider);
    }
    HttpResponse httpResponse = httpClient.execute(delRequest, context);
    HttpEntity entity = httpResponse.getEntity();

    if (entity != null) {
      result = EntityUtils.toString(entity);
    }

    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (200 != statusCode) {
      logger.warn("请求返回CODE：" + statusCode + "返回数据：" + result);
      throw new RuntimeException(result);
    }

    return result;
  }

  private static CloseableHttpClient createSSLInsecureClient() {
    try {
      SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> {
        return true; //信任所有
      }).build();
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
      return HttpClients.custom().setSSLSocketFactory(sslsf).build();
    } catch (Exception e) {
      logger.warn("CertificateException", e);
    }
    return HttpClients.createDefault();
  }
}
