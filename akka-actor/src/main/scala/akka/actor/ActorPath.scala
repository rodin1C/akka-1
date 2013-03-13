/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.actor
import scala.annotation.tailrec
import scala.collection.immutable
import akka.japi.Util.immutableSeq
import java.net.MalformedURLException

object ActorPath {
  /**
   * Parse string as actor path; throws java.net.MalformedURLException if unable to do so.
   */
  def fromString(s: String): ActorPath = s match {
    case ActorPathExtractor(addr, elems) ⇒ RootActorPath(addr) / elems
    case _                               ⇒ throw new MalformedURLException("cannot parse as ActorPath: " + s)
  }

  /**
   * This Regular Expression is used to validate a path element (Actor Name).
   * Since Actors form a tree, it is addressable using an URL, therefor an Actor Name has to conform to:
   * http://www.ietf.org/rfc/rfc2396.txt
   */
  val ElementRegex = """(?:[-\w:@&=+,.!~*'_;]|%\p{XDigit}{2})(?:[-\w:@&=+,.!~*'$_;]|%\p{XDigit}{2})*""".r

  private[akka] final val emptyActorPath: immutable.Iterable[String] = List("")
}

/**
 * Actor path is a unique path to an actor that shows the creation path
 * up through the actor tree to the root actor.
 *
 * ActorPath defines a natural ordering (so that ActorRefs can be put into
 * collections with this requirement); this ordering is intended to be as fast
 * as possible, which owing to the bottom-up recursive nature of ActorPath
 * is sorted by path elements FROM RIGHT TO LEFT, where RootActorPath >
 * ChildActorPath in case the number of elements is different.
 *
 * Two actor paths are compared equal when they have the same name and parent
 * elements, including the root address information. That doesn't necessarily
 * mean that they point to the same incarnation of the actor if the actor is
 * re-created with the same path. In other words, in contrast to how actor
 * references are compared the unique id of the actor is not taken into account
 * when comparing actor paths.
 */
@SerialVersionUID(1L)
sealed trait ActorPath extends Comparable[ActorPath] with Serializable {
  /**
   * The Address under which this path can be reached; walks up the tree to
   * the RootActorPath.
   */
  def address: Address

  /**
   * The name of the actor that this path refers to.
   */
  def name: String

  /**
   * The path for the parent actor.
   */
  def parent: ActorPath

  /**
   * Create a new child actor path.
   */
  def /(child: String): ActorPath

  /**
   * Java API: Create a new child actor path.
   */
  def child(child: String): ActorPath = /(child)

  /**
   * Recursively create a descendant’s path by appending all child names.
   */
  def /(child: Iterable[String]): ActorPath = (this /: child)((path, elem) ⇒ if (elem.isEmpty) path else path / elem)

  /**
   * Java API: Recursively create a descendant’s path by appending all child names.
   */
  def descendant(names: java.lang.Iterable[String]): ActorPath = /(immutableSeq(names))

  /**
   * Sequence of names for this path from root to this. Performance implication: has to allocate a list.
   */
  def elements: immutable.Iterable[String]

  /**
   * Java API: Sequence of names for this path from root to this. Performance implication: has to allocate a list.
   */
  def getElements: java.lang.Iterable[String] =
    scala.collection.JavaConverters.asJavaIterableConverter(elements).asJava

  /**
   * Walk up the tree to obtain and return the RootActorPath.
   */
  def root: RootActorPath

  /**
   * Generate String representation, replacing the Address in the RootActor
   * Path with the given one unless this path’s address includes host and port
   * information.
   */
  def toStringWithAddress(address: Address): String

  /**
   * Generate full String representation including the
   * uid for the actor cell instance as URI fragment.
   * This representation should be used as serialized
   * representation instead of `toString`.
   */
  def toRawString: String

  /**
   * Generate full String representation including the uid for the actor cell
   * instance as URI fragment, replacing the Address in the RootActor Path
   * with the given one unless this path’s address includes host and port
   * information. This representation should be used as serialized
   * representation instead of `toStringWithAddress`.
   */
  def toRawStringWithAddress(address: Address): String

  /**
   * INTERNAL API
   */
  private[akka] def uid: Int

  /**
   * INTERNAL API
   */
  private[akka] def withUid(uid: Int): ActorPath

}

/**
 * Root of the hierarchy of ActorPaths. There is exactly root per ActorSystem
 * and node (for remote-enabled or clustered systems).
 */
@SerialVersionUID(1L)
final case class RootActorPath(address: Address, name: String = "/") extends ActorPath {

  override def parent: ActorPath = this

  override def root: RootActorPath = this

  override def /(child: String): ActorPath = {
    val (childName, uid) = ActorCell.splitNameAndUid(child)
    new ChildActorPath(this, childName, uid)
  }

  override def elements: immutable.Iterable[String] = ActorPath.emptyActorPath

  override val toString: String = address + name

  override val toRawString: String = toString

  override def toStringWithAddress(addr: Address): String =
    if (address.host.isDefined) address + name
    else addr + name

  override def toRawStringWithAddress(addr: Address): String = toStringWithAddress(addr)

  override def compareTo(other: ActorPath): Int = other match {
    case r: RootActorPath  ⇒ toString compareTo r.toString // FIXME make this cheaper by comparing address and name in isolation
    case c: ChildActorPath ⇒ 1
  }

  /**
   * INTERNAL API
   */
  private[akka] def uid: Int = ActorCell.undefinedUid

  /**
   * INTERNAL API
   */
  override private[akka] def withUid(uid: Int): ActorPath =
    if (uid == ActorCell.undefinedUid) this
    else throw new IllegalStateException("RootActorPath must not have uid")

}

@SerialVersionUID(1L)
final class ChildActorPath private[akka] (val parent: ActorPath, val name: String, override private[akka] val uid: Int) extends ActorPath {
  if (name.indexOf('/') != -1) throw new IllegalArgumentException("/ is a path separator and is not legal in ActorPath names: [%s]" format name)
  if (name.indexOf('#') != -1) throw new IllegalArgumentException("# is a fragment separator and is not legal in ActorPath names: [%s]" format name)

  def this(parent: ActorPath, name: String) = this(parent, name, ActorCell.undefinedUid)

  override def address: Address = root.address

  override def /(child: String): ActorPath = {
    val (childName, uid) = ActorCell.splitNameAndUid(child)
    new ChildActorPath(this, childName, uid)
  }

  override def elements: immutable.Iterable[String] = {
    @tailrec
    def rec(p: ActorPath, acc: List[String]): immutable.Iterable[String] = p match {
      case r: RootActorPath ⇒ acc
      case _                ⇒ rec(p.parent, p.name :: acc)
    }
    rec(this, Nil)
  }

  override def root: RootActorPath = {
    @tailrec
    def rec(p: ActorPath): RootActorPath = p match {
      case r: RootActorPath ⇒ r
      case _                ⇒ rec(p.parent)
    }
    rec(this)
  }

  /**
   * INTERNAL API
   */
  override private[akka] def withUid(uid: Int): ActorPath =
    if (uid == this.uid) this
    else new ChildActorPath(parent, name, uid)

  // TODO research whether this should be cached somehow (might be fast enough, but creates GC pressure)
  /*
   * idea: add one field which holds the total length (because that is known)
   * so that only one String needs to be allocated before traversal; this is
   * cheaper than any cache
   */
  override def toString: String = buildToString(new StringBuilder(32)).toString

  override def toRawString: String = {
    val sb = buildToString(new StringBuilder(32))
    appendUidFragment(sb).toString
  }

  private def buildToString(sb: StringBuilder): StringBuilder = {
    @tailrec
    def rec(p: ActorPath, s: StringBuilder): StringBuilder = p match {
      case r: RootActorPath ⇒ s.insert(0, r.toString)
      case _                ⇒ rec(p.parent, s.insert(0, '/').insert(0, p.name))
    }
    rec(parent, sb.append(name))
  }

  override def toStringWithAddress(addr: Address): String =
    buildToStringWithAddress(addr, new StringBuilder(32)).toString

  override def toRawStringWithAddress(addr: Address): String = {
    val sb = buildToStringWithAddress(addr, new StringBuilder(32))
    appendUidFragment(sb).toString
  }

  private def buildToStringWithAddress(addr: Address, sb: StringBuilder): StringBuilder = {
    @tailrec
    def rec(p: ActorPath, s: StringBuilder): StringBuilder = p match {
      case r: RootActorPath ⇒ s.insert(0, r.toStringWithAddress(addr))
      case _                ⇒ rec(p.parent, s.insert(0, '/').insert(0, p.name))
    }
    rec(parent, sb.append(name))
  }

  private def appendUidFragment(sb: StringBuilder): StringBuilder = {
    if (uid == ActorCell.undefinedUid) sb
    else sb.append("#").append(uid)
  }

  override def equals(other: Any): Boolean = {
    @tailrec
    def rec(left: ActorPath, right: ActorPath): Boolean =
      if (left eq right) true
      else if (left.isInstanceOf[RootActorPath]) left equals right
      else if (right.isInstanceOf[RootActorPath]) right equals left
      else left.name == right.name && rec(left.parent, right.parent)

    other match {
      case p: ActorPath ⇒ rec(this, p)
      case _            ⇒ false
    }
  }

  // TODO RK investigate Phil’s hash from scala.collection.mutable.HashTable.improve
  override def hashCode: Int = {
    import akka.routing.MurmurHash._

    @tailrec
    def rec(p: ActorPath, h: Int, c: Int, k: Int): Int = p match {
      case r: RootActorPath ⇒ extendHash(h, r.##, c, k)
      case _                ⇒ rec(p.parent, extendHash(h, stringHash(name), c, k), nextMagicA(c), nextMagicB(k))
    }

    finalizeHash(rec(this, startHash(42), startMagicA, startMagicB))
  }

  override def compareTo(other: ActorPath): Int = {
    @tailrec
    def rec(left: ActorPath, right: ActorPath): Int =
      if (left eq right) 0
      else if (left.isInstanceOf[RootActorPath]) left compareTo right
      else if (right.isInstanceOf[RootActorPath]) -(right compareTo left)
      else {
        val x = left.name compareTo right.name
        if (x == 0) rec(left.parent, right.parent)
        else x
      }

    rec(this, other)
  }
}

