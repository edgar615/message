package com.edgar.util.event;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

/**
 * Created by edgar on 17-3-22.
 */
public class EventTest {

  @Test
  public void testMessage() {
    Message message = Message.create("UserAdd", ImmutableMap.of("foo", "bar"));
    String id = UUID.randomUUID().toString();
    String to = UUID.randomUUID().toString();
    Event event = Event.create(id, to, message);

    Map<String, Object> map = event.toMap();
    Map<String, Object> headerMap = (Map<String, Object>) map.get("head");

    Assert.assertEquals(5, headerMap.size());
    Assert.assertEquals(id, headerMap.get("id"));
    Assert.assertEquals(to, headerMap.get("to"));
    Assert.assertEquals(message.name(), headerMap.get("action"));
    Assert.assertTrue(headerMap.containsKey("timestamp"));
    Assert.assertTrue(headerMap.containsKey("duration"));

    Map<String, Object> bodyMap = (Map<String, Object>) map.get("data");
    Assert.assertEquals("UserAdd", bodyMap.get("resource"));
    Map<String, Object> content = (Map<String, Object>) bodyMap.get("content");
    Assert.assertEquals("bar", content.get("foo"));

    System.out.println(map);
    Event event2 = Event.fromMap(map);
    Assert.assertEquals(id, event2.head().id());
    Assert.assertEquals(to, event2.head().to());
    Assert.assertEquals(message.name(), event2.head().action());

    Assert.assertTrue(event2.action() instanceof Message);

    Message message2 = (Message) event2.action();
    Assert.assertEquals("UserAdd", message2.resource());
    Assert.assertEquals("bar", message2.content().get("foo"));

  }

  @Test
  public void testRequest() {
    Request request = Request.create("UserAdd", "insert", ImmutableMap.of("foo", 1));
    String id = UUID.randomUUID().toString();
    String to = UUID.randomUUID().toString();
    Event event = Event.create(id, to, request);
    String from = UUID.randomUUID().toString();
    event.head().addExt("from", from);

    Map<String, Object> map = event.toMap();
    Map<String, Object> headerMap = (Map<String, Object>) map.get("head");

    Assert.assertEquals(6, headerMap.size());
    Assert.assertEquals(id, headerMap.get("id"));
    Assert.assertEquals(to, headerMap.get("to"));
    Assert.assertEquals(request.name(), headerMap.get("action"));
    Assert.assertTrue(headerMap.containsKey("timestamp"));
    Assert.assertTrue(headerMap.containsKey("duration"));
    Assert.assertTrue(headerMap.containsKey("from"));

    Map<String, Object> bodyMap = (Map<String, Object>) map.get("data");
    Assert.assertEquals("UserAdd", bodyMap.get("resource"));
    Assert.assertEquals("insert", bodyMap.get("operation"));
    Map<String, Object> content = (Map<String, Object>) bodyMap.get("content");
    Assert.assertEquals(1, content.get("foo"));

    System.out.println(map);
    Event event2 = Event.fromMap(map);
    Assert.assertEquals(id, event2.head().id());
    Assert.assertEquals(to, event2.head().to());
    Assert.assertEquals(request.name(), event2.head().action());
    Assert.assertEquals(from, event2.head().ext("from"));

    Assert.assertTrue(event2.action() instanceof Request);

    Request request2 = (Request) event2.action();
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
    Event event = Event.create(id, to, response);
    String from = UUID.randomUUID().toString();
    String group = UUID.randomUUID().toString();
    event.head().addExt("from", from)
    .addExt("group", group);

    Map<String, Object> map = event.toMap();
    Map<String, Object> headerMap = (Map<String, Object>) map.get("head");

    Assert.assertEquals(7, headerMap.size());
    Assert.assertEquals(id, headerMap.get("id"));
    Assert.assertEquals(to, headerMap.get("to"));
    Assert.assertEquals(response.name(), headerMap.get("action"));
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
    Event event2 = Event.fromMap(map);
    Assert.assertEquals(id, event2.head().id());
    Assert.assertEquals(to, event2.head().to());
    Assert.assertEquals(response.name(), event2.head().action());
    Assert.assertEquals(from, event2.head().ext("from"));
    Assert.assertEquals(group, event2.head().ext("group"));

    Assert.assertTrue(event2.action() instanceof Response);

    Response response2 = (Response) event2.action();
    Assert.assertEquals(reply, response2.reply());
    Assert.assertEquals(1, response2.result());
    Assert.assertEquals(1, response2.content().get("foo"));

  }


}
