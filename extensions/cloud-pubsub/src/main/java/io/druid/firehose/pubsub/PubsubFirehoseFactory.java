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
        volatile private List<String> ackIds = null;
        Iterator<ReceivedMessage> receivedMessages;

        private void ack()
        {
          log.info("committing offsets");
          // TODO: retry
          if (ackIds != null) {
            try {
              AcknowledgeRequest ackRequest =
                new AcknowledgeRequest().setAckIds(ackIds);
              pubsub.projects().subscriptions()
                .acknowledge(subscriptionName, ackRequest).execute();
              ackIds = null;
            } catch (IOException e) {
              log.warn("An IO Exception occured while acking to Pub/Sub %s", e);
            }
          }
        }

        private void loadData() throws IOException {
          ack();

          log.info("loading data from Pub/Sub");
          pullResponse = pubsub.projects().subscriptions()
            .pull(subscriptionName, pullRequest).execute();
          ackIds = new ArrayList<>(maxBatchSize);
          List<ReceivedMessage> msgs = pullResponse.getReceivedMessages();
          if (msgs != null) {
            receivedMessages = msgs.iterator();
          } else {
            receivedMessages = null;
          }
        }

        private boolean isEmpty()
        {
          return receivedMessages == null || ! receivedMessages.hasNext();
        }


        @Override
        public boolean hasMore()
        {
          if (isEmpty()) {
            try {
              loadData();
            } catch (IOException e) {
              log.warn("An IO Exception occured while fetching new data from Pub/Sub %s", e);
            }
          }
          return ! isEmpty();
        }

        @Override
        public InputRow nextRow()
        {
          try{
            // FIXME: null checks
            ReceivedMessage receivedMessage = receivedMessages.next();
            PubsubMessage pubsubMessage = receivedMessage.getMessage();
            byte[] message = pubsubMessage.decodeData();
            log.info("data: %s", new String(message));
            InputRow row = theParser.parse(ByteBuffer.wrap(message));
            ackIds.add(receivedMessage.getAckId());

            log.info("raw: %s", row);
            return row;
          } catch (Exception e) {
            log.info("got a exception %s", e);
            return null;
          }
        }

        @Override
        public Runnable commit()
        {
          return new Runnable()
            {
              @Override
              public void run()
              {
                // the same as ack
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
