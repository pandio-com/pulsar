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

package org.apache.pulsar.io.twitter.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * POJO for Tweet object.
 */
@Data
public class TweetData {
    @JsonProperty("created_at")
    private String createdAt;
    private Long id;
    private String text;
    @JsonProperty("author_id")
    private String authorId;
    @JsonProperty("in_reply_to_user_id")
    private String inReplyToUserId;
    @JsonProperty("referenced_tweets")
    private List<ReferencedTweet> referencedTweets;
    private Attachment attachments;
    private String format;
    private Entity entities;

    /**
     * POJO for ReferencedTweet object.
     */
    @Data
    public static class ReferencedTweet {
        private String type;
        private String id;
    }

    /**
     * POJO for Attachment object.
     */
    @Data
    public static class Attachment {
        @JsonProperty("media_keys")
        private List<String> mediaKeys;

    }

    /**
     * POJO for Entity object.
     */
    @Data
    public static class Entity {
        private List<Url> urls;
        private List<Mention> mentions;
    }

    /**
     * POJO for Url object.
     */
    @Data
    public static class Url {
        private Integer start;
        private Integer end;
        private String url;
    }

    /**
     * POJO for Url object.
     */
    @Data
    public static class Mention {
        private Integer start;
        private Integer end;
        private String username;
    }

}
