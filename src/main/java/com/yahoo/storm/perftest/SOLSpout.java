/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.perftest;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SOLSpout extends BaseRichSpout {
  private int _sizeInBytes;
  private long _messageCount;
  private SpoutOutputCollector _collector;
  private String [] _messages = null;
  private boolean _ackEnabled;
  private Random _rand = null;
  public SOLSpout(int sizeInBytes, boolean ackEnabled) {
    if(sizeInBytes < 0) {
      sizeInBytes = 0;
    }
    _sizeInBytes = sizeInBytes;
    _messageCount = 0;
    _ackEnabled = ackEnabled;
  }

  public boolean isDistributed() {
    return true;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _rand = new Random();
    _collector = collector;
    final int differentMessages = 100;
    _messages = new String[differentMessages];
    for(int i = 0; i < differentMessages; i++) {
      StringBuilder sb = new StringBuilder(_sizeInBytes);
      //This is a hack, and I don't know for sure the size of the messages
      //that are sent.  Just a rough guess, because it is UCS2, so 2 bytes
      // per character.
      for(int j = 0; j < (_sizeInBytes/2) + 1; j++) {
        sb.append(_rand.nextInt(9));
      }
      _messages[i] = sb.toString();
    }
  }

  @Override
  public void close() {
    //Empty
  }

  @Override
  public void nextTuple() {
    final String message = _messages[_rand.nextInt(_messages.length)];
    if(_ackEnabled) {
      _collector.emit(new Values(message), _messageCount);
    } else {
      _collector.emit(new Values(message));
    }
    _messageCount++;
  }


  @Override
  public void ack(Object msgId) {
    //Empty
  }

  @Override
  public void fail(Object msgId) {
    //Empty
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
  }
}
