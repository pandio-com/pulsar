/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.twitter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.ConnectionClosedException;
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
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.twitter.data.TweetData;
import org.apache.pulsar.io.twitter.data.TwitterRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Push based Twitter FireHose Source.
 */
@Connector(
    name = "twitter",
    type = IOType.SOURCE,
    help = "A simple connector moving tweets from Twitter to Pulsar",
    configClass = TwitterConnectorConfig.class
)
@Slf4j
public class TwitterConnector extends PushSource<TweetData> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterConnector.class);
    private static final String TWEET_API = "https://api.twitter.com/labs/1/tweets/stream/sample";
    private static final String TWITTER_OAUTH = "https://api.twitter.com/oauth2/token";

    private final ObjectMapper mapper = new ObjectMapper().configure(
            DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private volatile boolean running = false;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        TwitterConnectorConfig cfg = IOConfigUtils
                .loadWithSecrets(config, TwitterConnectorConfig.class, sourceContext);
        cfg.validate();
        Thread streamRunner = new Thread(() -> {
            try {
                running = true;
                stream(cfg);
            } catch (Exception e) {
                throw new RuntimeException("Exception thrown: ", e);
            }
        });
        streamRunner.start();
    }

    private void stream(TwitterConnectorConfig cfg) throws IOException, URISyntaxException {
        String accessToken = getAccessToken(cfg);
        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder(TWEET_API);

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", accessToken));

        CloseableHttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())),16384)) {
                String line = reader.readLine();
                while (line != null && running) {
                    if (Strings.isNullOrEmpty(line)) {
                        line = reader.readLine();
                        continue;
                    }
                    LOG.info(line);
                    JsonNode root = mapper.readTree(line);
                    TweetData tweet = mapper.treeToValue(root.get("data"), TweetData.class);
                    consume(new TwitterRecord(tweet));
                    line = reader.readLine();
                }
            } catch (ConnectionClosedException e) {
                LOG.info("Twitter closed the connection, reconnecting...");
                stream(cfg);
            }
        }

    }

    private String getAccessToken(TwitterConnectorConfig cfg) throws IOException, URISyntaxException {
        String accessToken = null;

        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder(TWITTER_OAUTH);
        ArrayList<NameValuePair> postParameters;
        postParameters = new ArrayList<>();
        postParameters.add(new BasicNameValuePair("grant_type", "client_credentials"));
        uriBuilder.addParameters(postParameters);

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization",
                String.format("Basic %s", getBase64EncodedString(cfg.getConsumerKey(), cfg.getConsumerSecret())));
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

    private String getBase64EncodedString(String key, String secret) {
        String s = String.format("%s:%s", key, secret);
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws Exception {
        running = false;
    }

}
