package brando

import akka.testkit.{ImplicitSender, TestKit}
import akka.actor.{PoisonPill, ActorSystem}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSpecLike}
import scala.concurrent.duration._
import Requests._

import language.postfixOps
import akka.util.ByteString

class RequestsTest extends TestKit(ActorSystem("RequestsTest")) with FunSpecLike with ImplicitSender with BeforeAndAfter
  with BeforeAndAfterAll {

  val brando = system.actorOf(Brando("localhost", 6379, Some(15)))

  before {
    brando ! FlushDb
    expectMsg(Some(Ok))
  }

  describe("Keys") {
    describe("DEL") {
      it("should remove single keys") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Del("key")
        expectMsg(Some(1))
      }

      it("should remove multiple keys") {
        brando ! Set("key-one", "value")
        expectMsg(Some(Ok))

        brando ! Set("key-two", "value")
        expectMsg(Some(Ok))

        brando ! Del("key-one", "key-two")
        expectMsg(Some(2))
      }
    }

    describe("DUMP") {
      it("should dump key") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Dump("key")
        expectMsgClass(Some().getClass)
      }
    }

    describe("EXISTS") {
      it("should return 1 for existing keys") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Exists("key")
        expectMsg(Some(1))
      }

      it("should return 0 for non-existent keys") {
        brando ! Exists("key")
        expectMsg(Some(0))
      }
    }

    describe("EXPIRE") {
      it("should set timeout on key") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Expire("key", 10)
        expectMsg(Some(1))
      }

      it("should return 0 for non-existent-keys") {
        brando ! Expire("non-existent-key", 10)
        expectMsg(Some(0))
      }
    }

    describe("EXPIREAT") {
      it("should set timeout on key") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! ExpireAt("key", System.currentTimeMillis() / 1000L)
        expectMsg(Some(1))
      }

      it("should return 0 for non-existent-keys") {
        brando ! ExpireAt("non-existent-key", 10)
        expectMsg(Some(0))
      }
    }

    describe("KEYS") {
      it("should list all keys matching pattern") {
        brando ! Set("key-one", "value")
        expectMsg(Some(Ok))

        brando ! Set("key-two", "value")
        expectMsg(Some(Ok))

        brando ! Set("blah-blah-key", "value")
        expectMsg(Some(Ok))

        brando ! Keys("key*")
        val resp = receiveOne(500.millis)
        resp match {
          case Response.AsStrings(List("key-one", "key-two")) =>
          case Response.AsStrings(List("key-two", "key-one")) =>
          case _ => fail("Unexpected response")
        }
      }
    }

    describe("MOVE") {
      it("should move a key to another DB") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Move("key", 14)
        expectMsg(Some(1))

        brando ! Get("key")
        expectMsg(None)

        brando ! Select(14)
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))

        brando ! FlushDb
        expectMsg(Some(Ok))

        brando ! Select(15)
        expectMsg(Some(Ok))
      }
    }

    describe("PERSIST") {
      it("should remove timeout from a key") {
        brando ! SetEx("key", 10, "value")
        expectMsg(Some(Ok))

        brando ! Persist("key")
        expectMsg(Some(1))

        brando ! Persist("key")
        expectMsg(Some(0))
      }

      it("should return 0 if the key does not exist") {
        brando ! Persist("non-existent-key")
        expectMsg(Some(0))
      }
    }

    describe("PEXPIRE") {
      it("should set timeout in millis on a key") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! PExpire("key", 1000)
        expectMsg(Some(1))
      }

      it("should return 0 if the key does not exist") {
        brando ! PExpire("key", 1000)
        expectMsg(Some(0))
      }
    }

    describe("PEXPIREAT") {
      it("should set millis timestamp to expire at on a key") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! PExpireAt("key", System.currentTimeMillis + 100500)
        expectMsg(Some(1))
      }

      it("should return 0 if the key does not exist") {
        brando ! PExpireAt("key", System.currentTimeMillis + 100500)
        expectMsg(Some(0))
      }
    }

    describe("PTTL") {
      it("should return TTL in millis for a key") {
        brando ! SetEx("key", 10, "value")
        expectMsg(Some(Ok))

        brando ! PTtl("key")
        val resp = receiveOne(500.millis).asInstanceOf[Option[Long]]
        assert(resp.get > 0)
        assert(resp.get <= 10000)
      }

      it("should return -1 if the key has no associated expire") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! PTtl("key")
        expectMsg(Some(-1))
      }

      it("should return -2 if the key does not exist") {
        brando ! PTtl("key")
        expectMsg(Some(-2))
      }
    }

    describe("RANDOMKEY") {
      it("should return random key") {
        brando ! Set("key-one", "value")
        expectMsg(Some(Ok))

        brando ! Set("key-two", "value")
        expectMsg(Some(Ok))

        brando ! RandomKey
        val resp = receiveOne(500.millis).asInstanceOf[Option[ByteString]]
        assert(resp.isDefined)
        assert(resp.get.utf8String == "key-one" || resp.get.utf8String == "key-two")
      }

      it("should return None when DB is empty") {
        brando ! RandomKey
        expectMsg(None)
      }
    }

    describe("RENAME") {
      it("should rename keys") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Rename("key", "new-key")
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(None)

        brando ! Get("new-key")
        expectMsg(Some(ByteString("value")))
      }
    }

    describe("RENAMENX") {
      it("should rename keys") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! RenameNx("key", "new-key")
        expectMsg(Some(1))

        brando ! Get("key")
        expectMsg(None)

        brando ! Get("new-key")
        expectMsg(Some(ByteString("value")))

      }

      it("should return 0 if the new key already exists") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Set("new-key", "value")
        expectMsg(Some(Ok))

        brando ! RenameNx("key", "new-key")
        expectMsg(Some(0))
      }
    }

    describe("RESTORE") {
      it("should restore dumped keys") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Dump("key")
        val dumped: ByteString = receiveOne(500.millis) match {
          case Some(string: ByteString) => string
          case _ => fail("Unexpected response")
        }

        brando ! Del("key")
        expectMsg(Some(1))

        brando ! Restore("key", dumped)
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))
      }
    }

    describe("SORT") {
      it("should sort sets") {
        brando ! SAdd("key", "1", "3", "2")
        expectMsg(Some(3))

        brando ! Sort("key")
        expectMsg(Some(List(Some(ByteString("1")), Some(ByteString("2")), Some(ByteString("3")))))
      }

      it("should sort with descending order") {
        brando ! SAdd("key", "1", "3", "2")
        expectMsg(Some(3))

        brando ! Sort("key", Order.Desc)
        expectMsg(Some(List(Some(ByteString("3")), Some(ByteString("2")), Some(ByteString("1")))))
      }

      it("should sort with limit") {
        brando ! SAdd("key", "1", "3", "2")
        expectMsg(Some(3))

        brando ! Sort("key", Limit(1, 1))
        expectMsg(Some(List(Some(ByteString("2")))))
      }

      it("should sort with ALPHA modifier") {
        brando ! SAdd("key", "01", "02", "003")
        expectMsg(Some(3))

        brando ! Sort("key", alpha = true)
        expectMsg(Some(List(Some(ByteString("003")), Some(ByteString("01")), Some(ByteString("02")))))
      }

      it("should sort and store result") {
        brando ! SAdd("key", "1", "3", "2")
        expectMsg(Some(3))

        brando ! Sort("key", "another-key")
        expectMsg(Some(3))

        brando ! LRange("another-key", 0 , -1)
        expectMsg(Some(List(Some(ByteString("1")), Some(ByteString("2")), Some(ByteString("3")))))
      }
    }

    describe("TTL") {
      it("should return TTL in seconds for a key") {
        brando ! SetEx("key", 10, "value")
        expectMsg(Some(Ok))

        brando ! Ttl("key")
        val resp = receiveOne(500.millis).asInstanceOf[Option[Long]]
        assert(resp.get > 0)
        assert(resp.get <= 10)
      }

      it("should return -1 if the key has no associated expire") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Ttl("key")
        expectMsg(Some(-1))
      }

      it("should return -2 if the key does not exist") {
        brando ! Ttl("key")
        expectMsg(Some(-2))
      }
    }

    describe("TYPE") {
      it("should get type of the key") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Type("key")
        expectMsg(Some(ValueType.String))
      }
    }

    describe("SCAN") {
      it("should scan keys") {
        brando ! Set("key-one", "value")
        expectMsg(Some(Ok))

        brando ! Set("key-two", "value")
        expectMsg(Some(Ok))

        brando ! Set("key-three", "value")
        expectMsg(Some(Ok))

        brando ! Scan()
        receiveOne(500.millis)
      }
    }
  }

  describe("Strings") {
    describe("APPEND") {
      it("should append a value to the key") {
        brando ! Append("key", "val")
        expectMsg(Some(3))

        brando ! Append("key", "ue")
        expectMsg(Some(5))

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))
      }
    }

    describe("BITCOUNT") {
      it("should return number of set bits") {
        brando ! Set("key", "\u0001\u0003")
        expectMsg(Some(Ok))

        brando ! BitCount("key")
        expectMsg(Some(3))

        brando ! BitCount("key", 0, 0)
        expectMsg(Some(1))
      }

      it("should return 0 for empty keys") {
        brando ! BitCount("key")
        expectMsg(Some(0))
      }
    }

    describe("BITOP") {
      it("should perform bitwise operations") {
        brando ! Set("key-one", new String(Array[Byte](0x01, 0x03)))
        expectMsg(Some(Ok))

        brando ! BitOp(Operation.Not, "key-two", "key-one")
        expectMsg(Some(2))

        brando ! Get("key-two")
        expectMsg(Some(ByteString(0xFE, 0xFC)))

        brando ! BitOp(Operation.Or, "key-three", "key-one", "key-two")
        expectMsg(Some(2))

        brando ! Get("key-three")
        expectMsg(Some(ByteString(0xFF, 0xFF)))
      }
    }

    describe("BITPOS") {
      it("should return the position of the first bit set to 1 or 0 in a string") {
        brando ! Set("key", new String(Array[Byte](0x01, 0x03)))
        expectMsg(Some(Ok))

        brando ! BitPos("key", Bit.Zero)
        expectMsg(Some(0))

        brando ! BitPos("key", Bit.One, 1)
        expectMsg(Some(14))
      }

      it("should return -1 when cannot find the bit") {
        brando ! BitPos("key", Bit.One)
        expectMsg(Some(-1))
      }
    }

    describe("DECR") {
      it("should decrement the key") {
        brando ! Set("key", "10")
        expectMsg(Some(Ok))

        brando ! Decr("key")
        expectMsg(Some(9))
      }

      it("should work on non-existent keys") {
        brando ! Decr("key")
        expectMsg(Some(-1))
      }
    }

    describe("DECRBY") {
      it("should decrement the key") {
        brando ! Set("key", "10")
        expectMsg(Some(Ok))

        brando ! DecrBy("key", 6)
        expectMsg(Some(4))
      }

      it("should work on non-existent keys") {
        brando ! DecrBy("key", 6)
        expectMsg(Some(-6))
      }
    }

    describe("GET") {
      it("should get string keys") {
        brando ! Set("key", "10")
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(Some(ByteString("10")))
      }

      it("should get None for non-existent keys") {
        brando ! Get("key")
        expectMsg(None)
      }
    }

    describe("GETBIT") {
      it("should return the bit value at offset in the string value stored at key") {
        brando ! Set("key", new String(Array[Byte](0x01, 0x03)))
        expectMsg(Some(Ok))

        brando ! GetBit("key", 7)
        expectMsg(Some(1))

        brando ! GetBit("key", 0)
        expectMsg(Some(0))
      }
    }

    describe("GETRANGE") {
      it("should return the substring of the string value stored at key, determined by the offsets start and end") {
        brando ! Set("key", "This is a string")
        expectMsg(Some(Ok))

        brando ! GetRange("key", 0, 3)
        expectMsg(Some(ByteString("This")))

        brando ! GetRange("key", -3, -1)
        expectMsg(Some(ByteString("ing")))
      }
    }

    describe("GETSET") {
      it("should atomically set key to value and returns the old value stored at key") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! GetSet("key", "new-value")
        expectMsg(Some(ByteString("value")))

        brando ! Get("key")
        expectMsg(Some(ByteString("new-value")))
      }
    }

    describe("INCR") {
      it("should increment the key") {
        brando ! Set("key", "10")
        expectMsg(Some(Ok))

        brando ! Incr("key")
        expectMsg(Some(11))
      }

      it("should work on non-existent keys") {
        brando ! Incr("key")
        expectMsg(Some(1))
      }
    }

    describe("INCRBY") {
      it("should increment the key") {
        brando ! Set("key", "10")
        expectMsg(Some(Ok))

        brando ! IncrBy("key", 6)
        expectMsg(Some(16))
      }

      it("should work on non-existent keys") {
        brando ! IncrBy("key", 6)
        expectMsg(Some(6))
      }
    }

    describe("INCRBYFLOAT") {
      it("should increment the key") {
        brando ! Set("key", "10")
        expectMsg(Some(Ok))

        brando ! IncrByFloat("key", 6.5)
        expectMsg(Some(ByteString("16.5")))
      }

      it("should work on non-existent keys") {
        brando ! IncrByFloat("key", 5e3)
        expectMsg(Some(ByteString("5000")))
      }
    }

    describe("MGET") {
      it("should return the values of all specified keys") {
        brando ! Set("key-one", "Hello")
        expectMsg(Some(Ok))

        brando ! Set("key-two", "World")
        expectMsg(Some(Ok))

        brando ! MGet("key-one", "key-two", "nonexisting")
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")), None)))
      }
    }

    describe("MSET") {
      it("should sets the given keys to their respective values") {
        brando ! MSet(("key-one", "Hello"), ("key-two", "World"))
        expectMsg(Some(Ok))

        brando ! Get("key-one")
        expectMsg(Some(ByteString("Hello")))

        brando ! Get("key-two")
        expectMsg(Some(ByteString("World")))
      }
    }

    describe("MSETNX") {
      it("should sets the given keys to their respective values") {
        brando ! MSetNx(("key-one", "Hello"), ("key-two", "World"))
        expectMsg(Some(1))

        brando ! MSetNx(("key-two", "Hello"), ("key-three", "World"))
        expectMsg(Some(0))

        brando ! MGet("key-one", "key-two")
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")))))
      }
    }

    describe("PSETEX") {
      it("should set key to hold the string value and set key to timeout after a given number of milliseconds") {
        brando ! PSetEx("key", 1000, "Hello")
        expectMsg(Some(Ok))

        brando ! PTtl("key")
        val resp = receiveOne(500.millis).asInstanceOf[Option[Long]]
        assert(resp.get > 0)
        assert(resp.get <= 1000)
      }
    }

    describe("SET") {
      it("should set key to hold the string value") {
        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))
      }

      it("should set key to hold the string value only when it exists") {
        brando ! Set("key", "value", xx = true)
        expectMsg(None)

        brando ! Get("key")
        expectMsg(None)

        brando ! Set("key", "value")
        expectMsg(Some(Ok))

        brando ! Set("key", "value-one", xx = true)
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(Some(ByteString("value-one")))
      }

      it("should set key to hold the string value only when it doesn't exist") {
        brando ! Set("key", "value", nx = true)
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))

        brando ! Set("key", "value-one", nx = true)
        expectMsg(None)

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))
      }
    }

    describe("SETBIT") {
      it("should set or clear the bit at offset in the string value stored at key") {
        brando ! SetBit("key", 7, Bit.One)
        expectMsg(Some(0))

        brando ! SetBit("key", 7, Bit.Zero)
        expectMsg(Some(1))

        brando ! Get("key")
        expectMsg(Some(ByteString(new String(Array[Byte](0x00)))))
      }
    }

    describe("SETEX") {
      it("should set key to hold the string value and set key to timeout after a given number of seconds") {
        brando ! SetEx("key", 10, "value")
        expectMsg(Some(Ok))

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))

        brando ! Ttl("key")
        val resp = receiveOne(500.millis).asInstanceOf[Option[Long]]
        assert(resp.get > 0)
        assert(resp.get <= 10)
      }
    }

    describe("SETNX") {
      it("should set key to hold string value if key does not exist") {
        brando ! SetNx("key", "value")
        expectMsg(Some(1))

        brando ! SetNx("key", "value")
        expectMsg(Some(0))

        brando ! Get("key")
        expectMsg(Some(ByteString("value")))
      }
    }

    describe("STRLEN") {
      it("should return the length of the string value stored at key") {
        brando ! Set("key", "Hello World")
        expectMsg(Some(Ok))

        brando ! StrLen("key")
        expectMsg(Some(11))

        brando ! StrLen("nonexisting")
        expectMsg(Some(0))
      }
    }
  }

  describe("Hashes") {
    describe("HDEL") {
      it("should remove the specified fields from the hash stored at key") {
        brando ! HSet("key", "field", "value")
        expectMsg(Some(1))

        brando ! HDel("key", "field")
        expectMsg(Some(1))

        brando ! HDel("key", "another-field")
        expectMsg(Some(0))
      }
    }

    describe("HEXISTS") {
      it("should return if field is an existing field in the hash stored at key") {
        brando ! HSet("key", "field", "value")
        expectMsg(Some(1))

        brando ! HExists("key", "field")
        expectMsg(Some(1))

        brando ! HExists("key", "another-field")
        expectMsg(Some(0))
      }
    }

    describe("HGET") {
      it("should return the value associated with field in the hash stored at key") {
        brando ! HSet("key", "field", "value")
        expectMsg(Some(1))

        brando ! HGet("key", "field")
        expectMsg(Some(ByteString("value")))

        brando ! HGet("key", "another-field")
        expectMsg(None)
      }
    }

    describe("HGETALL") {
      it("should return all fields and values of the hash stored at key") {
        brando ! HSet("key", "field-one", "Hello")
        expectMsg(Some(1))

        brando ! HSet("key", "field-two", "World")
        expectMsg(Some(1))

        brando ! HGetAll("key")
        expectMsg(Some(List(
          Some(ByteString("field-one")),
          Some(ByteString("Hello")),
          Some(ByteString("field-two")),
          Some(ByteString("World"))
        )))
      }
    }

    describe("HINCRBY") {
      it("should increment the number stored at field in the hash stored at key by increment") {
        brando ! HSet("key", "field", "5")
        expectMsg(Some(1))

        brando ! HIncrBy("key", "field", 1)
        expectMsg(Some(6))

        brando ! HIncrBy("key", "field", -1)
        expectMsg(Some(5))
      }
    }

    describe("HINCRBYFLOAT") {
      it("should increment the floating point number stored at field in the hash stored at key by increment") {
        brando ! HSet("key", "field", "5.0")
        expectMsg(Some(1))

        brando ! HIncrByFloat("key", "field", 9.5)
        expectMsg(Some(ByteString("14.5")))

        brando ! HIncrByFloat("key", "field", -1.5)
        expectMsg(Some(ByteString("13")))
      }
    }

    describe("HKEYS") {
      it("should return all field names in the hash stored at key") {
        brando ! HSet("key", "field-one", "value")
        expectMsg(Some(1))

        brando ! HSet("key", "field-two", "value")
        expectMsg(Some(1))

        brando ! HKeys("key")
        expectMsg(Some(List(Some(ByteString("field-one")), Some(ByteString("field-two")))))
      }
    }

    describe("HLEN") {
      it("should return the number of fields contained in the hash stored at key") {
        brando ! HSet("key", "field-one", "value")
        expectMsg(Some(1))

        brando ! HSet("key", "field-two", "value")
        expectMsg(Some(1))

        brando ! HLen("key")
        expectMsg(Some(2))
      }
    }

    describe("HMGET") {
      it("should return the values associated with the specified fields in the hash stored at key") {
        brando ! HSet("key", "field-one", "value-one")
        expectMsg(Some(1))

        brando ! HSet("key", "field-two", "value-two")
        expectMsg(Some(1))

        brando ! HMGet("key", "field-one", "field-two", "nofield")
        expectMsg(Some(List(Some(ByteString("value-one")), Some(ByteString("value-two")), None)))
      }
    }

    describe("HMSET") {
      it("should set the specified fields to their respective values in the hash stored at key") {
        brando ! HMSet("key", ("field-one", "value-one"), ("field-two", "value-two"))
        expectMsg(Some(Ok))

        brando ! HGet("key", "field-one")
        expectMsg(Some(ByteString("value-one")))

        brando ! HGet("key", "field-two")
        expectMsg(Some(ByteString("value-two")))
      }
    }

    describe("HSET") {
      it("should set field in the hash stored at key to value") {
        brando ! HSet("key", "field", "value")
        expectMsg(Some(1))

        brando ! HGet("key", "field")
        expectMsg(Some(ByteString("value")))
      }
    }

    describe("HSETNX") {
      it("should set field in the hash stored at key to value, only if field does not yet exist") {
        brando ! HSetNx("key", "field", "Hello")
        expectMsg(Some(1))

        brando ! HSetNx("key", "field", "World")
        expectMsg(Some(0))

        brando ! HGet("key", "field")
        expectMsg(Some(ByteString("Hello")))
      }
    }

    describe("HVALS") {
      it("should set field in the hash stored at key to value, only if field does not yet exist") {
        brando ! HSet("key", "field-one", "Hello")
        expectMsg(Some(1))

        brando ! HSet("key", "field-two", "World")
        expectMsg(Some(1))

        brando ! HVals("key")
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")))))
      }
    }

    describe("HSCAN") {
      it("should scan keys in hashes") {
        brando ! HMSet("key", ("field-one", "value-one"), ("field-two", "value-two"))
        expectMsg(Some(Ok))

        brando ! HScan("key")
        receiveOne(500.millis)
      }
    }
  }

  describe("Lists") {
    describe("BLPOP") {
      it("should pop first element from first non-empty list") {
        brando ! RPush("list", "a", "b", "c")
        expectMsg(Some(3))

        brando ! BLPop("list", "non-existent-list")
        expectMsg(Some(List(Some(ByteString("list")), Some(ByteString("a")))))
      }
    }

    describe("BRPOP") {
      it("should pop last element from first non-empty list") {
        brando ! RPush("list", "a", "b", "c")
        expectMsg(Some(3))

        brando ! BRPop("list", "non-existent-list")
        expectMsg(Some(List(Some(ByteString("list")), Some(ByteString("c")))))
      }
    }

    describe("BRPOPLPUSH") {
      it("should pop last element from source list and prepend it to the destination list") {
        brando ! RPush("list", "a", "b", "c")
        expectMsg(Some(3))

        brando ! BRPopLPush("list", "new-list")
        expectMsg(Some(ByteString("c")))

        brando ! LRange("new-list", 0, -1)
        expectMsg(Some(List(Some(ByteString("c")))))
      }
    }

    describe("LINDEX") {
      it("should return the element at index index in the list stored at key") {
        brando ! RPush("list", "a", "b", "c")
        expectMsg(Some(3))

        brando ! LIndex("list", 0)
        expectMsg(Some(ByteString("a")))

        brando ! LIndex("list", 1)
        expectMsg(Some(ByteString("b")))
      }
    }

    describe("LINSERT") {
      it("should insert value in the list stored at key either before or after the reference value pivot") {
        brando ! RPush("list", "Hello", "World")
        expectMsg(Some(2))

        brando ! LInsert("list", Before, "World", "There")
        expectMsg(Some(3))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("There")), Some(ByteString("World")))))
      }
    }

    describe("LLEN") {
      it("should return the length of the list stored at key") {
        brando ! RPush("list", "Hello", "World")
        expectMsg(Some(2))

        brando ! LLen("list")
        expectMsg(Some(2))
      }
    }

    describe("LPOP") {
      it("should remove and return the first element of the list stored at key") {
        brando ! RPush("list", "Hello", "World")
        expectMsg(Some(2))

        brando ! LPop("list")
        expectMsg(Some(ByteString("Hello")))

        brando ! LLen("list")
        expectMsg(Some(1))
      }
    }

    describe("LPUSH") {
      it("should insert all the specified values at the head of the list stored at key") {
        brando ! LPush("list", "World")
        expectMsg(Some(1))

        brando ! LPush("list", "Hello")
        expectMsg(Some(2))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")))))
      }
    }

    describe("LPUSHX") {
      it("should insert value at the head of the list stored at key, only if key already exists and holds a list") {
        brando ! LPush("list", "World")
        expectMsg(Some(1))

        brando ! LPushX("list", "Hello")
        expectMsg(Some(2))

        brando ! LPushX("other-list", "Hello")
        expectMsg(Some(0))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")))))

        brando ! LRange("other-list", 0, -1)
        expectMsg(Some(List()))
      }
    }

    describe("LRANGE") {
      it("should return the specified elements of the list stored at key") {
        brando ! RPush("list", "Hello")
        expectMsg(Some(1))

        brando ! RPush("list", "World")
        expectMsg(Some(2))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")))))
      }
    }

    describe("LREM") {
      it("should return the specified elements of the list stored at key") {
        brando ! RPush("list", "value")
        expectMsg(Some(1))

        brando ! RPush("list", "value")
        expectMsg(Some(2))

        brando ! RPush("list", "value")
        expectMsg(Some(3))

        brando ! LRem("list", 0, "value")
        expectMsg(Some(3))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List()))
      }
    }

    describe("LSET") {
      it("should set the list element at index to value") {
        brando ! RPush("list", "value")
        expectMsg(Some(1))

        brando ! RPush("list", "value")
        expectMsg(Some(2))

        brando ! LSet("list", 0, "new-value")
        expectMsg(Some(Ok))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("new-value")), Some(ByteString("value")))))
      }
    }

    describe("LTRIM") {
      it("should trim an existing list so that it will contain only the specified range of elements specified") {
        brando ! RPush("list", "one")
        expectMsg(Some(1))

        brando ! RPush("list", "two")
        expectMsg(Some(2))

        brando ! RPush("list", "three")
        expectMsg(Some(3))

        brando ! LTrim("list", 1, -1)
        expectMsg(Some(Ok))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("two")), Some(ByteString("three")))))
      }
    }

    describe("RPOP") {
      it("should remove and returns the last element of the list stored at key") {
        brando ! RPush("list", "one")
        expectMsg(Some(1))

        brando ! RPush("list", "two")
        expectMsg(Some(2))

        brando ! RPush("list", "three")
        expectMsg(Some(3))

        brando ! RPop("list")
        expectMsg(Some(ByteString("three")))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("one")), Some(ByteString("two")))))
      }
    }

    describe("RPOPLPUSH") {
      it("should pop last element from source list and prepend it to the destination list") {
        brando ! RPush("list", "a", "b", "c")
        expectMsg(Some(3))

        brando ! RPopLPush("list", "new-list")
        expectMsg(Some(ByteString("c")))

        brando ! LRange("new-list", 0, -1)
        expectMsg(Some(List(Some(ByteString("c")))))
      }
    }

    describe("RPUSH") {
      it("should insert all the specified values at the tail of the list stored at key") {
        brando ! RPush("list", "Hello")
        expectMsg(Some(1))

        brando ! RPush("list", "World")
        expectMsg(Some(2))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")))))
      }
    }

    describe("RPUSHX") {
      it("should insert value at the tail of the list stored at key, only if key already exists and holds a list") {
        brando ! RPush("list", "Hello")
        expectMsg(Some(1))

        brando ! RPushX("list", "World")
        expectMsg(Some(2))

        brando ! RPushX("other-list", "World")
        expectMsg(Some(0))

        brando ! LRange("list", 0, -1)
        expectMsg(Some(List(Some(ByteString("Hello")), Some(ByteString("World")))))

        brando ! LRange("other-list", 0, -1)
        expectMsg(Some(List()))
      }
    }
  }

  describe("Sets") {
    describe("SADD") {
      it("should add the specified members to the set stored at key") {
        brando ! SAdd("set", "Hello")
        expectMsg(Some(1))

        brando ! SAdd("set", "World")
        expectMsg(Some(1))

        brando ! SAdd("set", "World")
        expectMsg(Some(0))

        brando ! SMembers("set")
        val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
        assert(
          resp.get.toSet === collection.immutable.Set(Some(ByteString("Hello")), Some(ByteString("World")))
        )
      }
    }

    describe("SCARD") {
      it("should return the set cardinality (number of elements) of the set stored at key") {
        brando ! SAdd("set", "Hello")
        expectMsg(Some(1))

        brando ! SAdd("set", "World")
        expectMsg(Some(1))

        brando ! SCard("set")
        expectMsg(Some(2))
      }
    }

    describe("SDIFF") {
      it("should return members of the set resulting from the difference between the first set and all other sets") {
        brando ! SAdd("key1", "a", "b", "c", "d")
        expectMsg(Some(4))

        brando ! SAdd("key2", "c")
        expectMsg(Some(1))

        brando ! SAdd("key3", "a", "c", "e")
        expectMsg(Some(3))

        brando ! SDiff("key1", "key2", "key3")
        val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
        assert(
          resp.get.toSet === collection.immutable.Set(Some(ByteString("b")), Some(ByteString("d")))
        )
      }
    }

    describe("SDIFFSTORE") {
      it("should store members of the set resulting from the difference between the first set and all other sets") {
        brando ! SAdd("key1", "a", "b", "c", "d")
        expectMsg(Some(4))

        brando ! SAdd("key2", "c")
        expectMsg(Some(1))

        brando ! SAdd("key3", "a", "c", "e")
        expectMsg(Some(3))

        brando ! SDiffStore("key4", "key1", "key2", "key3")
        expectMsg(Some(2))

        brando ! SMembers("key4")
        val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
        assert(
          resp.get.toSet === collection.immutable.Set(Some(ByteString("b")), Some(ByteString("d")))
        )
      }
    }

    describe("SINTER") {
      it("should return the members of the set resulting from the intersection of all the given sets") {
        brando ! SAdd("key1", "a", "b", "c", "d")
        expectMsg(Some(4))

        brando ! SAdd("key2", "c")
        expectMsg(Some(1))

        brando ! SAdd("key3", "a", "c", "e")
        expectMsg(Some(3))

        brando ! SInter("key1", "key2", "key3")
        val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
        assert(
          resp.get.toSet === collection.immutable.Set(Some(ByteString("c")))
        )
      }
    }

    describe("SINTERSTORE") {
      it("should store the members of the set resulting from the intersection of all the given sets") {
        brando ! SAdd("key1", "a", "b", "c", "d")
        expectMsg(Some(4))

        brando ! SAdd("key2", "c")
        expectMsg(Some(1))

        brando ! SAdd("key3", "a", "c", "e")
        expectMsg(Some(3))

        brando ! SInterStore("key4", "key1", "key2", "key3")
        expectMsg(Some(1))

        brando ! SMembers("key4")
        val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
        assert(
          resp.get.toSet === collection.immutable.Set(Some(ByteString("c")))
        )
      }
    }

    describe("SMEMBERS") {
      it("should return all the members of the set value stored at key") {
        brando ! SAdd("set", "Hello")
        expectMsg(Some(1))

        brando ! SAdd("set", "World")
        expectMsg(Some(1))

        brando ! SMembers("set")
        val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
        assert(
          resp.get.toSet === collection.immutable.Set(Some(ByteString("Hello")), Some(ByteString("World")))
        )
      }
    }
  }

  override protected def afterAll() {
    brando ! PoisonPill
  }
}
