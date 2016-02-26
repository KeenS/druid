/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.pubsub;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class PubsubFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{
  private static final Logger log = new Logger(PubsubFirehoseFactory.class);

  @JsonProperty
  private final String subscriptionName;// =
  // projects/myproject/subscriptions/mysubscription
  //    "projects/cyberagent-024/subscriptions/test_shimizu";
  @JsonProperty
  private final int maxBatchSize;

  @JsonCreator
  public PubsubFirehoseFactory(
                               @JsonProperty("subscription") String subscriptionName,
                               @JsonProperty("maxBatchSize") int maxBatchSize
                               )
  {
    this.subscriptionName = subscriptionName;
    this.maxBatchSize = maxBatchSize;
  }

  @Override
  public Firehose connect(final ByteBufferInputRowParser firehoseParser) throws IOException
  {
    Set<String> newDimExclus = Sets.union(
                                          firehoseParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
                                          Sets.newHashSet("subscription")
                                          );
    final ByteBufferInputRowParser theParser = firehoseParser.withParseSpec(
                                                                            firehoseParser.getParseSpec()
                                                                            .withDimensionsSpec(
                                                                                                firehoseParser.getParseSpec()
                                                                                                .getDimensionsSpec()
                                                                                                .withDimensionExclusions(
                                                                                                                         newDimExclus
                                                                                                                         )
                                                                                                )
                                                                            );


    final Pubsub pubsub = PortableConfiguration.createPubsubClient();
    // You can fetch multiple messages with a single API call.
    final PullRequest pullRequest = new PullRequest()
      .setReturnImmediately(false)
      .setMaxMessages(maxBatchSize);



    return new Firehose()
      {

        private PullResponse pullResponse;
        private List<String> ackIds;
        Iterator<ReceivedMessage> receivedMessages;

        private void loadData() throws IOException {
          this.pullResponse = pubsub.projects().subscriptions()
            .pull(subscriptionName, pullRequest).execute();
          this.ackIds = new ArrayList<>(maxBatchSize);
          this.receivedMessages =
            pullResponse.getReceivedMessages().iterator();
        }

        private boolean hasMoreInternal()
        {
          return receivedMessages == null || receivedMessages.hasNext();
        }


        @Override
        public boolean hasMore()
        {
          if (hasMoreInternal()) {
            try {
              loadData();
            } catch (IOException e) {
              log.warn("An IO Exception occured while fetching new data from Pub/Sub %s", e);
            }
          }
          return hasMoreInternal();
        }

        @Override
        public InputRow nextRow()
        {
          // FIXME: null checks
          final PubsubMessage pubsubMessage = receivedMessages.next().getMessage();
          final byte[] message = pubsubMessage.decodeData();//iter.next().message();

          return theParser.parse(ByteBuffer.wrap(message));
        }

        @Override
        public Runnable commit()
        {
          return new Runnable()
            {
              @Override
              public void run()
              {
                log.info("committing offsets");
                // TODO: retry
                try {
                  AcknowledgeRequest ackRequest =
                    new AcknowledgeRequest().setAckIds(ackIds);
                  pubsub.projects().subscriptions()
                    .acknowledge(subscriptionName, ackRequest).execute();
                } catch (IOException e) {
                  log.warn("An IO Exception occured while commiting to Pub/Sub %s", e);
                }
              }
          };
        }

        @Override
        public void close() throws IOException
        {
          //        connector.shutdown();
        }
    };
  }

}
