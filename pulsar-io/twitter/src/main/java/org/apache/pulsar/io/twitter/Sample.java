package org.apache.pulsar.io.twitter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pulsar.io.twitter.data.TweetData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;

/*
 * Sample code to demonstrate the use of the Labs Sample Stream endpoint
 * */
public class Sample {


    public static void main(String args[]) throws IOException, URISyntaxException {
        String accessToken = getAccessToken();
        connectStream(accessToken);
    }

    /*
     * This method calls the sample stream endpoint and streams Tweets from it
     * */
    private static void connectStream(String accessToken) throws IOException, URISyntaxException {

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/labs/1/tweets/stream/sample");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", accessToken));

        CloseableHttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            final ObjectMapper mapper = new ObjectMapper().configure(
                    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
            String line = reader.readLine();
            while (line != null) {
                JsonNode node = mapper.readTree(line);
                JsonNode subnode = node.get("data");
                TweetData tweet = mapper.treeToValue(subnode, TweetData.class);
                System.out.println(line);
                line = reader.readLine();
            }
        }

    }

    private static String getAccessToken() throws IOException, URISyntaxException {
        String accessToken = null;

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/oauth2/token");
        ArrayList<NameValuePair> postParameters;
        postParameters = new ArrayList<>();
        postParameters.add(new BasicNameValuePair("grant_type", "client_credentials"));
        uriBuilder.addParameters(postParameters);

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Basic %s", getBase64EncodedString()));
        httpPost.setHeader("Content-Type", "application/json");

        CloseableHttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();

        if (null != entity) {
            try (InputStream inputStream = entity.getContent()) {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> jsonMap = mapper.readValue(inputStream, Map.class);
                accessToken = jsonMap.get("access_token").toString();
            }
        }
        return accessToken;
    }

    private static String getBase64EncodedString() {
        String s = String.format("%s:%s", "bu1xaXDC6ftmWMG12CHveAPSl", "ZFhlX9sEchAminBMm92Xwv1mkza9WWEQVZijeOJ24gY6ohvU8G");
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }
}