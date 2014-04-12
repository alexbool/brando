package brando

import org.scalatest.FunSpecLike
import akka.testkit._

import akka.actor._
import akka.util.ByteString
import scala.concurrent.duration._
import java.util.UUID

import Requests._

class BrandoTest extends TestKit(ActorSystem("BrandoTest")) with FunSpecLike
    with ImplicitSender {

  describe("ping") {
    it("should respond with Pong") {
      val brando = system.actorOf(Brando())

      brando ! Ping

      expectMsg(Some(Pong))
    }
  }

  describe("flushdb") {
    it("should respond with OK") {
      val brando = system.actorOf(Brando())

      brando ! FlushDb

      expectMsg(Some(Ok))
    }
  }

  describe("set") {
    it("should respond with OK") {
      val brando = system.actorOf(Brando())

      brando ! Set("mykey", "somevalue")

      expectMsg(Some(Ok))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }
  }

  describe("get") {
    it("should respond with value option for existing key") {
      val brando = system.actorOf(Brando())

      brando ! Set("mykey", "somevalue")

      expectMsg(Some(Ok))

      brando ! Get("mykey")

      expectMsg(Some(ByteString("somevalue")))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }

    it("should respond with None for non-existent key") {
      val brando = system.actorOf(Brando())

      brando ! Get("mykey")

      expectMsg(None)
    }
  }

  describe("incr") {
    it("should increment and return value for existing key") {
      val brando = system.actorOf(Brando())

      brando ! Set("incr-test", "10")

      expectMsg(Some(Ok))

      brando ! Incr("incr-test")

      expectMsg(Some(11))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }

    it("should return 1 for non-existent key") {
      val brando = system.actorOf(Brando())

      brando ! Incr("incr-test")

      expectMsg(Some(1))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }
  }

  describe("sadd") {
    it("should return number of members added to set") {
      val brando = system.actorOf(Brando())

      brando ! SAdd("sadd-test", "one")

      expectMsg(Some(1))

      brando ! SAdd("sadd-test", "two", "three")

      expectMsg(Some(2))

      brando ! SAdd("sadd-test", "one", "four")

      expectMsg(Some(1))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }
  }

  describe("smembers") {
    it("should return all members in a set") {
      val brando = system.actorOf(Brando())

      brando ! SAdd("smembers-test", "one", "two", "three", "four")

      expectMsg(Some(4))

      brando ! SMembers("smembers-test")

      val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
      assert(resp.getOrElse(List()).toSet ===
        collection.immutable.Set(Some(ByteString("one")), Some(ByteString("two")),
          Some(ByteString("three")), Some(ByteString("four"))))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }

  }

  describe("piplining") {
    it("should respond to a Seq of multiple requests all at once") {
      val brando = system.actorOf(Brando())
      val ping = Ping

      brando ! ping
      brando ! ping
      brando ! ping

      expectMsg(Some(Pong))
      expectMsg(Some(Pong))
      expectMsg(Some(Pong))

    }

    it("should support pipelines of setex commands") {
      val brando = system.actorOf(Brando())
      val setex = SetEx("pipeline-setex-path", 10, "Some data")

      brando ! setex
      brando ! setex
      brando ! setex

      expectMsg(Some(Ok))
      expectMsg(Some(Ok))
      expectMsg(Some(Ok))
    }

    it("should receive responses in the right order") {
      val brando = system.actorOf(Brando())
      val ping = Ping
      val setex = SetEx("pipeline-setex-path", 10, "Some data")

      brando ! setex
      brando ! ping
      brando ! setex
      brando ! ping
      brando ! setex

      expectMsg(Some(Ok))
      expectMsg(Some(Pong))
      expectMsg(Some(Ok))
      expectMsg(Some(Pong))
      expectMsg(Some(Ok))
    }
  }

  describe("large data sets") {
    it("should read and write large files") {
      import java.io.{ File, FileInputStream }

      val file = new File("src/test/resources/crime_and_punishment.txt")
      val in = new FileInputStream(file)
      val bytes = new Array[Byte](file.length.toInt)
      in.read(bytes)
      in.close()

      val largeText = new String(bytes, "UTF-8")

      val brando = system.actorOf(Brando())

      brando ! Set("crime+and+punishment", largeText)

      expectMsg(Some(Ok))

      brando ! Get("crime+and+punishment")

      expectMsg(Some(ByteString(largeText)))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }
  }

  describe("error reply") {
    it("should receive a failure with the redis error message") {
      val brando = system.actorOf(Brando())

      brando ! Request("SET", "key")

      expectMsgPF(5.seconds) {
        case Status.Failure(e) ⇒
          assert(e.isInstanceOf[BrandoException])
          assert(e.getMessage === "ERR wrong number of arguments for 'set' command")
      }

      brando ! Request("EXPIRE", "1", "key")

      expectMsgPF(5.seconds) {
        case Status.Failure(e) ⇒
          assert(e.isInstanceOf[BrandoException])
          assert(e.getMessage === "ERR value is not an integer or out of range")
      }
    }
  }

  describe("select") {
    it("should execute commands on the selected database") {
      val brando = system.actorOf(Brando("localhost", 6379, Some(5)))

      brando ! Set("mykey", "somevalue")

      expectMsg(Some(Ok))

      brando ! Get("mykey")

      expectMsg(Some(ByteString("somevalue")))

      brando ! Select(0)

      expectMsg(Some(Ok))

      brando ! Get("mykey")

      expectMsg(None)

      brando ! Select(5)
      expectMsg(Some(Ok))

      brando ! FlushDb
      expectMsg(Some(Ok))
    }
  }

  describe("multi") {
    it("should return the multi responses after the exec command") {
      val brando = system.actorOf(Brando("localhost", 6379, Some(5)))

      brando ! Multi
      expectMsg(Some(Ok))

      brando ! Set("mykey", "somevalue")
      expectMsg(Some(Queued))

      brando ! Get("mykey")
      expectMsg(Some(Queued))

      brando ! Exec
      expectMsg(Some(List(Some(Ok), Some(ByteString("somevalue")))))
    }

    it("should pipeline multi requests") {
      val brando = system.actorOf(Brando("localhost", 6379, Some(5)))

      brando ! Multi
      brando ! Set("mykey", "somevalue")
      brando ! Get("mykey")
      brando ! Exec

      expectMsg(Some(Ok))
      expectMsg(Some(Queued))
      expectMsg(Some(Queued))
      expectMsg(Some(List(Some(Ok), Some(ByteString("somevalue")))))
    }

  }

  describe("subscriber") {

    it("should be able to subscribe to a pubsub channel") {
      val channel = UUID.randomUUID().toString
      val subscriber = system.actorOf(Brando())

      subscriber ! Subscribe(channel)

      expectMsg(Some(List(Some(
        ByteString("subscribe")),
        Some(ByteString(channel)),
        Some(1))))
    }

    it("should receive published messages from a pubsub channel") {
      val channel = UUID.randomUUID().toString
      val subscriber = system.actorOf(Brando())
      val publisher = system.actorOf(Brando())

      subscriber ! Subscribe(channel)

      expectMsg(Some(List(Some(
        ByteString("subscribe")),
        Some(ByteString(channel)),
        Some(1))))

      publisher ! Publish(channel, "test")
      expectMsg(Some(1)) //publisher gets back number of subscribers when publishing

      expectMsg(PubSubMessage(channel, "test"))
    }

    it("should be able to unsubscribe from a pubsub channel") {
      val channel = UUID.randomUUID().toString
      val subscriber = system.actorOf(Brando())
      val publisher = system.actorOf(Brando())

      subscriber ! Subscribe(channel)

      expectMsg(Some(List(Some(
        ByteString("subscribe")),
        Some(ByteString(channel)),
        Some(1))))

      subscriber ! Unsubscribe(channel)

      expectMsg(Some(List(Some(
        ByteString("unsubscribe")),
        Some(ByteString(channel)),
        Some(0))))

      publisher ! Publish(channel, "test")
      expectMsg(Some(0))

      expectNoMsg
    }
  }

  describe("State notifications") {

    it("should send an Authenticated event if connecting succeeds") {
      val probe = TestProbe()
      val brando = system.actorOf(Brando("localhost", 6379, listeners = collection.immutable.Set(probe.ref)))

      probe.expectMsg(Connected)
    }

    it("should send an ConnectionFailed event if connecting fails after the configured number of retries") {
      val probe = TestProbe()
      val brando = system.actorOf(Brando("localhost", 13579, listeners = collection.immutable.Set(probe.ref)))

      //3 retries * 2 seconds = 6 seconds
      probe.expectNoMsg(5900.milliseconds)
      probe.expectMsg(ConnectionFailed)
    }

    it("should send an AuthenticationFailed event if connecting succeeds but authentication fails") {
      val probe = TestProbe()
      val brando = system.actorOf(Brando("localhost", 6379, auth = Some("not-the-auth"), listeners = collection.immutable.Set(probe.ref)))

      probe.expectMsg(AuthenticationFailed)
    }
  }
}
