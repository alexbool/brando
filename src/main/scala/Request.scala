package brando

import akka.util.ByteString

object Request {
  def apply(command: String, params: String*) =
    new GenericRequest(ByteString(command), params.map(ByteString(_)).to[List])
}

//Helps creating a request like HMSET key k1 v1 k2 v2...
object HashRequest {
  def apply(cmd: String, key: String, map: Map[String, String]) = {
    val args = Seq(key) ++ map.map(e ⇒ Seq(e._1, e._2)).flatten
    Request(cmd, args: _*)
  }
}

trait Request {
  def command: ByteString
  def params: List[ByteString]

  def args = command :: params
  def toByteString = args.map(argLine).foldLeft(header)(_ ++ _)

  val CRLF = ByteString("\r\n")
  private def header = ByteString("*" + args.length) ++ CRLF
  private def argLine(bytes: ByteString) = ByteString("$" + bytes.length) ++ CRLF ++ bytes ++ CRLF
}

case class GenericRequest(command: ByteString, params: List[ByteString]) extends Request

object GenericRequest {
  def apply(command: ByteString, params: ByteString*): GenericRequest = new GenericRequest(command, params.to[List])
}

object Requests {

  private val toByteStringF: String => ByteString = ByteString.apply

  // Keys
  case class Del(keys: List[String]) extends Request {
    require(keys.nonEmpty, "Del command takes at least one key")

    val command = ByteString("DEL")
    val params = keys.map(toByteStringF)
  }

  object Del {
    def apply(firstKey: String, otherKeys: String*): Del = Del(firstKey :: otherKeys.to[List])
  }

  case class Dump(key: String) extends Request {
    val command = ByteString("DUMP")
    val params = List(ByteString(key))
  }

  case class Exists(key: String) extends Request {
    val command = ByteString("EXISTS")
    val params = List(ByteString(key))
  }

  case class Expire(key: String, seconds: Int) extends Request {
    val command = ByteString("EXPIRE")
    val params = List(ByteString(key), ByteString(seconds.toString))
  }

  case class ExpireAt(key: String, timestamp: Long) extends Request {
    val command = ByteString("EXPIREAT")
    val params = List(ByteString(key), ByteString(timestamp.toString))
  }

  case class Keys(pattern: String) extends Request {
    val command = ByteString("KEYS")
    val params = List(ByteString(pattern))
  }

  case class Move(key: String, db: Int) extends Request {
    val command = ByteString("MOVE")
    val params = List(ByteString(key), ByteString(db.toString))
  }

  case class Persist(key: String) extends Request {
    val command = ByteString("PERSIST")
    val params = List(ByteString(key))
  }

  case class PExpire(key: String, millis: Long) extends Request {
    val command = ByteString("PEXPIRE")
    val params = List(ByteString(key), ByteString(millis.toString))
  }

  case class PExpireAt(key: String, millisTimestamp: Long) extends Request {
    val command = ByteString("PEXPIREAT")
    val params = List(ByteString(key), ByteString(millisTimestamp.toString))
  }

  case class PTtl(key: String) extends Request {
    val command = ByteString("PTTL")
    val params = List(ByteString(key))
  }

  case object RandomKey extends Request {
    val command = ByteString("RANDOMKEY")
    val params = Nil
  }

  case class Rename(key: String, newKey: String) extends Request {
    val command = ByteString("RENAME")
    val params = List(ByteString(key), ByteString(newKey))
  }

  case class RenameNx(key: String, newKey: String) extends Request {
    val command = ByteString("RENAMENX")
    val params = List(ByteString(key), ByteString(newKey))
  }

  case class Restore(key: String, serializedValue: ByteString, ttlMillis: Long = 0) extends Request {
    val command = ByteString("RESTORE")
    val params = List(ByteString(key), ByteString(ttlMillis.toString), serializedValue)
  }

  case class Sort(
          key: String,
          pattern: Option[String] = None,
          limit: Option[Limit] = None,
          externalKeys: List[String] = Nil,
          order: Option[Order] = None,
          alpha: Boolean = false,
          storeDestination: Option[String] = None) extends Request {
    val command = ByteString("SORT")
    val params = (
      key ::
      pattern.to[List] :::
      limit.map(_.asStrings).getOrElse(Nil) :::
      externalKeys.map("GET " + _) :::
      order.map(_.code).to[List] :::
      (if (alpha) List("ALPHA") else Nil) :::
      storeDestination.map(List("STORE", _)).getOrElse(Nil)
    ).map(toByteStringF)
  }

  object Sort {
    def apply(key: String, store: String): Sort = Sort(key = key, storeDestination = Some(store))
    def apply(key: String, order: Order): Sort = Sort(key = key, order = Some(order))
    def apply(key: String, limit: Limit, order: Order): Sort = Sort(key = key, limit = Some(limit), order = Some(order))
    def apply(key: String, limit: Limit, order: Order, store: String): Sort =
      Sort(key = key, limit = Some(limit), order = Some(order), storeDestination = Some(store))
    def apply(key: String, limit: Limit): Sort = Sort(key = key, limit = Some(limit))
    def apply(key: String, limit: Limit, alpha: Boolean): Sort = Sort(key = key, limit = Some(limit), alpha = alpha)
    def apply(key: String, limit: Limit, store: String): Sort = Sort(key = key, limit = Some(limit), storeDestination = Some(store))
  }

  case class Limit(offset: Int, count: Int) {
    require(offset >= 0, "Offset must be >= 0")
    require(count > 0, "Count must be > 0")

    def asStrings = List("LIMIT", offset.toString, count.toString)
  }

  sealed abstract class Order(val code: String)
  object Order extends Enumeration {
    case object Asc  extends Order("ASC")
    case object Desc extends Order("DESC")
  }

  case class Ttl(key: String) extends Request {
    val command = ByteString("TTL")
    val params = List(ByteString(key))
  }

  case class Type(key: String) extends Request {
    val command = ByteString("TYPE")
    val params = List(ByteString(key))
  }

  case class Scan(cursor: Long = 0, pattern: Option[String] = None, count: Option[Int] = None) extends Request {
    val command = ByteString("SCAN")
    val params = (
      pattern.map(List("MATCH", _)).getOrElse(Nil) :::
      count.map(c => List("COUNT", c.toString)).getOrElse(Nil)
    ).map(toByteStringF)
  }

  object Scan {
    def apply(cursor: Long, pattern: String): Scan = Scan(cursor, Some(pattern))
    def apply(cursor: Long, count: Int): Scan = Scan(cursor, None, Some(count))
    def apply(cursor: Long, pattern: String, count: Int): Scan = Scan(cursor, Some(pattern), Some(count))
  }

  // Strings

  case class Append(key: String, value: String) extends Request {
    val command = ByteString("APPEND")
    val params = List(ByteString(key), ByteString(value))
  }

  case class BitCount(key: String, start: Option[Int] = None, stop: Option[Int] = None) extends Request {
    val command = ByteString("BITCOUNT")
    val params = (key :: start.map(_.toString).to[List] ::: stop.map(_.toString).to[List]).map(toByteStringF)
  }

  object BitCount {
    def apply(key: String, start: Int): BitCount = BitCount(key, Some(start))
    def apply(key: String, start: Int, stop: Int): BitCount = BitCount(key, Some(start), Some(stop))
  }

  case class BitOp(operation: Operation, destKey: String, srcKeys: List[String]) extends Request {
    require(srcKeys.nonEmpty, "Source keys must be not empty")
    if (operation == Operation.Not) require(srcKeys.size == 1, "NOT opertion requires strictly one source key")

    val command = ByteString("BITOP")
    val params = (operation.code :: destKey :: srcKeys).map(toByteStringF)
  }

  object BitOp {
    def apply(operation: Operation, destKey: String, firstSrcKey: String, otherSrcKeys: String*): BitOp =
      BitOp(operation, destKey, firstSrcKey :: otherSrcKeys.to[List])
  }

  sealed abstract class Operation(val code: String)
  object Operation {
    case object And extends Operation("AND")
    case object Or  extends Operation("OR")
    case object Xor extends Operation("XOR")
    case object Not extends Operation("NOT")
  }

  case class BitPos(key: String, bit: Bit, start: Option[Int] = None, stop: Option[Int] = None) extends Request {
    val command = ByteString("BITPOS")
    val params = (key :: bit.code :: (start ++ stop).map(_.toString).to[List]).map(toByteStringF)
  }

  object BitPos {
    def apply(key: String, bit: Bit, start: Int): BitPos = BitPos(key, bit, Some(start))
    def apply(key: String, bit: Bit, start: Int, stop: Int): BitPos = BitPos(key, bit, Some(start), Some(stop))
  }

  sealed abstract class Bit(val code: String)
  object Bit {
    case object Zero extends Bit("0")
    case object One  extends Bit("1")
  }

  case class Decr(key: String) extends Request {
    val command = ByteString("DECR")
    val params = List(ByteString(key))
  }

  case class DecrBy(key: String, decrement: Long) extends Request {
    val command = ByteString("DECRBY")
    val params = List(ByteString(key), ByteString(decrement.toString))
  }

  case class Get(key: String) extends Request {
    val command = ByteString("GET")
    val params = List(ByteString(key))
  }

  case class GetBit(key: String, offset: Int) extends Request {
    require(offset >= 0, "Offset must be >= 0")

    val command = ByteString("GETBIT")
    val params = List(ByteString(key), ByteString(offset.toString))
  }

  case class GetRange(key: String, start: Int, stop: Int) extends Request {
    val command = ByteString("GETRANGE")
    val params = List(key, start.toString, stop.toString).map(toByteStringF)
  }

  case class GetSet(key: String, value: String) extends Request {
    val command = ByteString("GETSET")
    val params = List(ByteString(key), ByteString(value))
  }

  case class Incr(key: String) extends Request {
    val command = ByteString("INCR")
    val params = List(ByteString(key))
  }

  case class IncrBy(key: String, increment: Long) extends Request {
    val command = ByteString("INCRBY")
    val params = List(ByteString(key), ByteString(increment.toString))
  }

  case class IncrByFloat(key: String, increment: Double) extends Request {
    val command = ByteString("INCRBYFLOAT")
    val params = List(ByteString(key), ByteString(increment.toString))
  }

  case class MGet(keys: List[String]) extends Request {
    require(keys.nonEmpty, "MGet command takes at least one key")

    val command = ByteString("MGET")
    val params = keys.map(toByteStringF)
  }

  object MGet {
    def apply(firstKey: String, otherKeys: String*): MGet = MGet(firstKey :: otherKeys.to[List])
  }

  case class MSet(pairs: List[(String, String)]) extends Request {
    require(pairs.nonEmpty, "MSet command takes at least one key-value pair")

    val command = ByteString("MSET")
    val params = pairs.flatMap(p => List(p._1, p._2)).map(toByteStringF)
  }

  object MSet {
    def apply(firstPair: (String, String), otherPairs: (String, String)*): MSet = MSet(firstPair :: otherPairs.to[List])
  }

  case class MSetNx(pairs: List[(String, String)]) extends Request {
    require(pairs.nonEmpty, "MSetNx command takes at least one key-value pair")

    val command = ByteString("MSETNX")
    val params = pairs.flatMap(p => List(p._1, p._2)).map(toByteStringF)
  }

  object MSetNx {
    def apply(firstPair: (String, String), otherPairs: (String, String)*): MSetNx = MSetNx(firstPair :: otherPairs.to[List])
  }

  case class PSetEx(key: String, millis: Int, value: String) extends Request {
    val command = ByteString("PSETEX")
    val params = List(key, millis.toString, value).map(toByteStringF)
  }

  case class Set(key: String, value: String, expireMillis: Option[Int] = None, nx: Boolean = false, xx: Boolean = false)
    extends Request {
    require(!(nx & xx), "Specify either nx, or xx, or none of them")

    val command = ByteString("SET")
    val params = (
      List(key, value) :::
        expireMillis.map("PX " + _).to[List] :::
        (if (nx) List("NX") else Nil) :::
        (if (xx) List("XX") else Nil)
      ).map(toByteStringF)
  }

  object Set {
    def apply(key: String, value: String, expireMillis: Int): Set =
      Set(
        key = key,
        value = value,
        expireMillis = Some(expireMillis)
      )

    def apply(key: String, value: String, expireMillis: Int, nx: Boolean, xx: Boolean): Set =
      Set(
        key = key,
        value = value,
        expireMillis = Some(expireMillis),
        nx = nx,
        xx = xx
      )
  }

  case class SetBit(key: String, offset: Int, value: Bit) extends Request {
    val command = ByteString("SETBIT")
    val params = List(key, offset.toString, value.code).map(toByteStringF)
  }

  case class SetEx(key: String, seconds: Int, value: String) extends Request {
    val command = ByteString("SETEX")
    val params = List(key, seconds.toString, value).map(toByteStringF)
  }

  case class SetNx(key: String, value: String) extends Request {
    val command = ByteString("SETNX")
    val params = List(ByteString(key), ByteString(value))
  }

  case class SetRange(key: String, offset: Int, value: String) extends Request {
    val command = ByteString("SETRANGE")
    val params = List(key, offset.toString, value).map(toByteStringF)
  }

  case class StrLen(key: String) extends Request {
    val command = ByteString("STRLEN")
    val params = List(ByteString(key))
  }

  // Hashes

  case class HDel(key: String, fields: List[String]) extends Request {
    require(fields.nonEmpty, "HDel command takes at least one field")

    val command = ByteString("HDEL")
    val params = (key :: fields).map(toByteStringF)
  }

  object HDel {
    def apply(key: String, firstField: String, otherFields: String*): HDel =
      HDel(key, firstField :: otherFields.to[List])
  }

  case class HExists(key: String, field: String) extends Request {
    val command = ByteString("HEXISTS")
    val params = List(ByteString(key), ByteString(field))
  }

  case class HGet(key: String, field: String) extends Request {
    val command = ByteString("HGET")
    val params = List(ByteString(key), ByteString(field))
  }

  case class HGetAll(key: String) extends Request {
    val command = ByteString("HGETALL")
    val params = List(ByteString(key))
  }

  case class HIncrBy(key: String, field: String, increment: Long) extends Request {
    val command = ByteString("HINCRBY")
    val params = List(key, field, increment.toString).map(toByteStringF)
  }

  case class HIncrByFloat(key: String, field: String, increment: Double) extends Request {
    val command = ByteString("HINCRBYFLOAT")
    val params = List(key, field, increment.toString).map(toByteStringF)
  }

  case class HKeys(key: String) extends Request {
    val command = ByteString("HKEYS")
    val params = List(ByteString(key))
  }

  case class HLen(key: String) extends Request {
    val command = ByteString("HLEN")
    val params = List(ByteString(key))
  }

  case class HMGet(key: String, fields: List[String]) extends Request {
    require(fields.nonEmpty, "HMGet command takes at least one field")

    val command = ByteString("HMGET")
    val params = (key :: fields).map(toByteStringF)
  }

  object HMGet {
    def apply(key: String, firstField: String, otherFields: String*): HMGet =
      HMGet(key, firstField :: otherFields.to[List])
  }

  case class HMSet(key: String, pairs: List[(String, String)]) extends Request {
    require(pairs.nonEmpty, "HMSet command takes at least one key-value pair")

    val command = ByteString("HMSET")
    val params = (key :: pairs.flatMap(p => List(p._1, p._2))).map(toByteStringF)
  }

  object HMSet {
    def apply(key: String, pairs: Map[String, String]): HMSet = HMSet(key, pairs.to[List])
    def apply(key: String, firstPair: (String, String), otherPairs: (String, String)*): HMSet =
      HMSet(key, firstPair :: otherPairs.to[List])
  }

  case class HSet(key: String, field: String, value: String) extends Request {
    val command = ByteString("HSET")
    val params = List(key, field, value).map(toByteStringF)
  }

  case class HSetNx(key: String, field: String, value: String) extends Request {
    val command = ByteString("HSETNX")
    val params = List(key, field, value).map(toByteStringF)
  }

  case class HVals(key: String) extends Request {
    val command = ByteString("HVALS")
    val params = List(ByteString(key))
  }

  case class HScan(key: String,
                   cursor: Long = 0,
                   pattern: Option[String] = None,
                   count: Option[Int] = None) extends Request {
    val command = ByteString("HSCAN")
    val params = (
      key ::
      pattern.map(List("MATCH", _)).getOrElse(Nil) :::
      count.map(c => List("COUNT", c.toString)).getOrElse(Nil)
    ).map(toByteStringF)
  }

  object HScan {
    def apply(key: String, cursor: Long, pattern: String): HScan = HScan(key, cursor, Some(pattern))
    def apply(key: String, cursor: Long, count: Int): HScan = HScan(key, cursor, None, Some(count))
    def apply(key: String, cursor: Long, pattern: String, count: Int): HScan =
      HScan(key, cursor, Some(pattern), Some(count))
  }

  // Lists

  case class BLPop(keys: List[String], timeoutSeconds: Int = 0) extends Request {
    require(keys.nonEmpty, "BLPop takes at least one key")

    val command = ByteString("BLPOP")
    val params = (keys :+ timeoutSeconds.toString).map(toByteStringF)
  }

  object BLPop {
    def apply(keys: String*): BLPop = BLPop(keys.to[List])
    def apply(timeoutSeconds: Int, keys: String*): BLPop = BLPop(keys.to[List], timeoutSeconds)
  }

  case class BRPop(keys: List[String], timeoutSeconds: Int = 0) extends Request {
    require(keys.nonEmpty, "BRPop takes at least one key")

    val command = ByteString("BRPOP")
    val params = (keys :+ timeoutSeconds.toString).map(toByteStringF)
  }

  object BRPop {
    def apply(keys: String*): BRPop = BRPop(keys.to[List])
    def apply(timeoutSeconds: Int, keys: String*): BRPop = BRPop(keys.to[List], timeoutSeconds)
  }

  case class BRPopLPush(source: String, destination: String, timeoutSeconds: Int = 0) extends Request {
    val command = ByteString("BRPOPLPUSH")
    val params = List(source, destination, timeoutSeconds.toString).map(toByteStringF)
  }

  case class LIndex(key: String, index : Int) extends Request {
    val command = ByteString("LINDEX")
    val params = List(ByteString(key), ByteString(index.toString))
  }

  case class LInsert(key: String, position: InsertPosition, pivot: String, value: String) extends Request {
    val command = ByteString("LINSERT")
    val params = List(key, position.code, pivot, value).map(toByteStringF)
  }

  sealed abstract class InsertPosition(val code: String)
  case object Before extends InsertPosition("BEFORE")
  case object After extends InsertPosition("AFTER")

  case class LLen(key: String) extends Request {
    val command = ByteString("LLEN")
    val params = List(ByteString(key))
  }

  case class LPop(key: String) extends Request {
    val command = ByteString("LPOP")
    val params = List(ByteString(key))
  }

  case class LPush(key: String, values: List[String]) extends Request {
    require(values.nonEmpty, "LPush command takes at least one value")

    val command = ByteString("LPUSH")
    val params = (key :: values).map(toByteStringF)
  }

  object LPush {
    def apply(key: String, firstValue: String, otherValues: String*): LPush =
      LPush(key, firstValue :: otherValues.to[List])
  }

  case class LPushX(key: String, value: String) extends Request {
    val command = ByteString("LPUSHX")
    val params = List(ByteString(key), ByteString(value))
  }

  case class LRange(key: String, start: Int, stop: Int) extends Request {
    val command = ByteString("LRANGE")
    val params = List(key, start.toString, stop.toString).map(toByteStringF)
  }

  case class LRem(key: String, count: Int, value: String) extends Request {
    val command = ByteString("LREM")
    val params = List(key, count.toString, value).map(toByteStringF)
  }

  case class LSet(key: String, index: Int, value: String) extends Request {
    val command = ByteString("LSET")
    val params = List(key, index.toString, value).map(toByteStringF)
  }

  case class LTrim(key: String, start: Int, stop: Int) extends Request {
    val command = ByteString("LTRIM")
    val params = List(key, start.toString, stop.toString).map(toByteStringF)
  }

  case class RPop(key: String) extends Request {
    val command = ByteString("RPOP")
    val params = List(ByteString(key))
  }

  case class RPopLPush(source: String, destination: String) extends Request {
    val command = ByteString("RPOPLPUSH")
    val params = List(ByteString(source), ByteString(destination))
  }

  case class RPush(key: String, values: List[String]) extends Request {
    require(values.nonEmpty, "RPush command takes at least one value")

    val command = ByteString("RPUSH")
    val params = (key :: values).map(toByteStringF)
  }

  object RPush {
    def apply(key: String, firstValue: String, otherValues: String*): RPush =
      RPush(key, firstValue :: otherValues.to[List])
  }

  case class RPushX(key: String, value: String) extends Request {
    val command = ByteString("RPUSHX")
    val params = List(ByteString(key), ByteString(value))
  }

  // Sets

  case class SAdd(key: String, members: List[String]) extends Request {
    require(members.nonEmpty, "Set command takes at least one member")

    val command = ByteString("SADD")
    val params = (key :: members).map(toByteStringF)
  }

  object SAdd {
    def apply(key: String, firstMember: String, otherMembers: String*): SAdd = SAdd(key, firstMember :: otherMembers.to[List])
  }

  case class SCard(key: String) extends Request {
    val command = ByteString("SCARD")
    val params = List(ByteString(key))
  }

  case class SDiff(keys: List[String]) extends Request {
    require(keys.nonEmpty, "SDIFF command takes at least one key")

    val command = ByteString("SDIFF")
    val params = keys.map(toByteStringF)
  }

  object SDiff {
    def apply(firstKey: String, otherKeys: String*): SDiff = SDiff(firstKey :: otherKeys.to[List])
  }

  case class SDiffStore(destination: String, keys: List[String]) extends Request {
    require(keys.nonEmpty, "SDIFFSTORE command takes at least one key")

    val command = ByteString("SDIFFSTORE")
    val params = (destination :: keys).map(toByteStringF)
  }

  object SDiffStore {
    def apply(destination: String, firstKey: String, otherKeys: String*): SDiffStore =
      SDiffStore(destination, firstKey :: otherKeys.to[List])
  }

  case class SInter(keys: List[String]) extends Request {
    require(keys.nonEmpty, "SINTER command takes at least one key")

    val command = ByteString("SINTER")
    val params = keys.map(toByteStringF)
  }

  object SInter {
    def apply(firstKey: String, otherKeys: String*): SInter = SInter(firstKey :: otherKeys.to[List])
  }

  case class SMembers(key: String) extends Request {
    val command = ByteString("SMEMBERS")
    val params = List(ByteString(key))
  }

  // Sorted Sets
  // HyperLogLog
  // Pub/Sub
  // Transactions
  // Scripting
  // Connection
  // Server

  case object Ping extends Request {
    val command = ByteString("PING")
    val params = Nil
  }

  case class Auth(auth: String) extends Request {
    val command = ByteString("AUTH")
    val params = List(ByteString(auth))
  }

  case class Select(database: Int) extends Request {
    val command = ByteString("SELECT")
    val params = List(ByteString(database.toString))
  }

  case object FlushDb extends Request {
    val command = ByteString("FLUSHDB")
    val params = Nil
  }

  case object Multi extends Request {
    val command = ByteString("MULTI")
    val params = Nil
  }

  case object Exec extends Request {
    val command = ByteString("EXEC")
    val params = Nil
  }

  case class Subscribe(channels: List[String]) extends Request {
    require(channels.nonEmpty, "Subscribe command takes at least one channel")

    val command = ByteString("SUBSCRIBE")
    val params = channels.map(toByteStringF)
  }

  object Subscribe {
    def apply(firstChannel: String, otherChannels: String*): Subscribe = Subscribe(firstChannel :: otherChannels.to[List])
  }

  case class Publish(channel: String, message: String) extends Request {
    val command = ByteString("PUBLISH")
    val params = List(ByteString(channel), ByteString(message))
  }

  case class Unsubscribe(channels: List[String]) extends Request {
    val command = ByteString("UNSUBSCRIBE")
    val params = channels.map(toByteStringF)
  }

  object Unsubscribe {
    def apply(channels: String*): Unsubscribe = Unsubscribe(channels.to[List])
  }
}

object ShardRequest {
  def apply(command: String, key: String, params: String*) = {
    new ShardRequest(ByteString(command), ByteString(key), params map (ByteString(_)): _*)
  }
}

case class ShardRequest(command: ByteString, key: ByteString, params: ByteString*)