package com.github.edgar615.message.core;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by edgar on 17-3-22.
 */
public class MessageTest {

  @Test
  public void testMessage() {
    Event event = Event.create("UserAdd", ImmutableMap.of("foo", "bar"));
    String id = UUID.randomUUID().toString();
    String to = UUID.randomUUID().toString();
    Message message = Message.create(id, to, event);

    Map<String, Object> map = message.toMap();
    Map<String, Object> headerMap = (Map<String, Object>) map.get("header");

    Assert.assertEquals(5, headerMap.size());
    Assert.assertEquals(id, headerMap.get("id"));
    Assert.assertEquals(to, headerMap.get("to"));
    Assert.assertEquals(event.name(), headerMap.get("body"));
    Assert.assertTrue(headerMap.containsKey("timestamp"));
    Assert.assertTrue(headerMap.containsKey("duration"));

    Map<String, Object> bodyMap = (Map<String, Object>) map.get("data");
    Assert.assertEquals("UserAdd", bodyMap.get("resource"));
    Map<String, Object> content = (Map<String, Object>) bodyMap.get("content");
    Assert.assertEquals("bar", content.get("foo"));

    System.out.println(map);
    Message message2 = Message.fromMap(map);
    Assert.assertEquals(id, message2.header().id());
    Assert.assertEquals(to, message2.header().to());
    Assert.assertEquals(event.name(), message2.header().action());

    Assert.assertTrue(message2.body() instanceof Event);

    Event event2 = (Event) message2.body();
    Assert.assertEquals("UserAdd", event2.resource());
    Assert.assertEquals("bar", event2.content().get("foo"));

  }

  @Test
  public void testRequest() {
    Request request = Request.create("UserAdd", "insert", ImmutableMap.of("foo", 1));
    String id = UUID.randomUUID().toString();
    String to = UUID.randomUUID().toString();
    Message message = Message.create(id, to, request);
    String from = UUID.randomUUID().toString();
    message.header().addExt("from", from);

    Map<String, Object> map = message.toMap();
    Map<String, Object> headerMap = (Map<String, Object>) map.get("header");

    Assert.assertEquals(6, headerMap.size());
    Assert.assertEquals(id, headerMap.get("id"));
    Assert.assertEquals(to, headerMap.get("to"));
    Assert.assertEquals(request.name(), headerMap.get("body"));
    Assert.assertTrue(headerMap.containsKey("timestamp"));
    Assert.assertTrue(headerMap.containsKey("duration"));
    Assert.assertTrue(headerMap.containsKey("from"));

    Map<String, Object> bodyMap = (Map<String, Object>) map.get("data");
    Assert.assertEquals("UserAdd", bodyMap.get("resource"));
    Assert.assertEquals("insert", bodyMap.get("operation"));
    Map<String, Object> content = (Map<String, Object>) bodyMap.get("content");
    Assert.assertEquals(1, content.get("foo"));

    System.out.println(map);
    Message message2 = Message.fromMap(map);
    Assert.assertEquals(id, message2.header().id());
    Assert.assertEquals(to, message2.header().to());
    Assert.assertEquals(request.name(), message2.header().action());
    Assert.assertEquals(from, message2.header().ext("from"));

    Assert.assertTrue(message2.body() instanceof Request);

    Request request2 = (Request) message2.body();
    Assert.assertEquals("UserAdd", request2.resource());
    Assert.assertEquals("insert", request2.operation());
    Assert.assertEquals(1, request2.content().get("foo"));

  }


  @Test
  public void testResponse() {
    String reply = UUID.randomUUID().toString();
    Response response = Response.create("test", 1, reply, ImmutableMap.of("foo", 1));
    String id = UUID.randomUUID().toString();
    String to = UUID.randomUUID().toString();
    Message message = Message.create(id, to, response);
    String from = UUID.randomUUID().toString();
    String group = UUID.randomUUID().toString();
    message.header().addExt("from", from)
    .addExt("group", group);

    Map<String, Object> map = message.toMap();
    Map<String, Object> headerMap = (Map<String, Object>) map.get("header");

    Assert.assertEquals(7, headerMap.size());
    Assert.assertEquals(id, headerMap.get("id"));
    Assert.assertEquals(to, headerMap.get("to"));
    Assert.assertEquals(response.name(), headerMap.get("body"));
    Assert.assertTrue(headerMap.containsKey("timestamp"));
    Assert.assertTrue(headerMap.containsKey("duration"));
    Assert.assertEquals(from, headerMap.get("from"));
    Assert.assertEquals(group, headerMap.get("group"));

    Map<String, Object> bodyMap = (Map<String, Object>) map.get("data");
    Assert.assertEquals(reply, bodyMap.get("reply"));
    Assert.assertEquals(1, bodyMap.get("result"));
    Map<String, Object> content = (Map<String, Object>) bodyMap.get("content");
    Assert.assertEquals(1, content.get("foo"));

    System.out.println(map);
    Message message2 = Message.fromMap(map);
    Assert.assertEquals(id, message2.header().id());
    Assert.assertEquals(to, message2.header().to());
    Assert.assertEquals(response.name(), message2.header().action());
    Assert.assertEquals(from, message2.header().ext("from"));
    Assert.assertEquals(group, message2.header().ext("group"));

    Assert.assertTrue(message2.body() instanceof Response);

    Response response2 = (Response) message2.body();
    Assert.assertEquals(reply, response2.reply());
    Assert.assertEquals(1, response2.result());
    Assert.assertEquals(1, response2.content().get("foo"));

  }


}
