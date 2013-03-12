/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.io

import akka.actor._
import akka.io.Inet.SocketOption
import akka.io.Udp.UdpSettings
import akka.io.IO.SelectorBasedManager
import akka.io.SelectionHandler._
import akka.util.ByteString
import akka.japi.Util.immutableSeq
import java.net.InetSocketAddress
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectionKey.OP_READ
import java.nio.ByteBuffer
import java.lang.{ Iterable ⇒ JIterable }
import scala.collection.immutable
import scala.annotation.tailrec
import scala.util.control.NonFatal

object UdpFF extends ExtensionKey[UdpFFExt] {

  /**
   * Java API: retrieve the UdpFF extension for the given system.
   */
  override def get(system: ActorSystem): UdpFFExt = super.get(system)

  trait Command extends IO.HasFailureMessage {
    def failureMessage = CommandFailed(this)
  }

  case class NoAck(token: Any)
  object NoAck extends NoAck(null)

  case class Send(payload: ByteString, target: InetSocketAddress, ack: Any) extends Command {
    require(ack != null, "ack must be non-null. Use NoAck if you don't want acks.")

    def wantsAck: Boolean = !ack.isInstanceOf[NoAck]
  }
  object Send {
    def apply(data: ByteString, target: InetSocketAddress): Send = Send(data, target, NoAck)
  }

  case class Bind(handler: ActorRef,
                  endpoint: InetSocketAddress,
                  options: immutable.Traversable[SocketOption] = Nil) extends Command
  case object Unbind extends Command

  case class SimpleSender(options: immutable.Traversable[SocketOption] = Nil) extends Command
  object SimpleSender extends SimpleSender(Nil)

  case object StopReading extends Command
  case object ResumeReading extends Command

  trait Event

  case class Received(data: ByteString, sender: InetSocketAddress) extends Event
  case class CommandFailed(cmd: Command) extends Event

  sealed trait Bound extends Event
  case object Bound extends Bound

  sealed trait SimpleSendReady extends Event
  case object SimpleSendReady extends SimpleSendReady

  sealed trait Unbound
  case object Unbound extends Unbound

  case class SendFailed(cause: Throwable) extends Event
}

import UdpFF._

/**
 * Java API: factory methods for the message types used when communicating with the UdpConn service.
 */
object UdpFFMessage {
  def send(payload: ByteString, target: InetSocketAddress): Send = Send(payload, target)
  def send(payload: ByteString, target: InetSocketAddress, ack: Any): Send = Send(payload, target, ack)

  def bind(handler: ActorRef, endpoint: InetSocketAddress, options: JIterable[SocketOption]): Bind =
    Bind(handler, endpoint, immutableSeq(options))

  def bind(handler: ActorRef, endpoint: InetSocketAddress): Bind = Bind(handler, endpoint, Nil)

  def simpleSender(options: JIterable[SocketOption]): SimpleSender = SimpleSender(immutableSeq(options))
  def simpleSender: SimpleSender = SimpleSender

  def unbind: Unbind.type = Unbind

  def stopReading: StopReading.type = StopReading
  def resumeReading: ResumeReading.type = ResumeReading
}

class UdpFFExt(system: ExtendedActorSystem) extends IO.Extension {

  val settings: UdpSettings = new UdpSettings(system.settings.config.getConfig("akka.io.udp-fire-and-forget"))

  val manager: ActorRef = {
    system.asInstanceOf[ActorSystemImpl].systemActorOf(
      props = Props(new UdpFFManager(this)),
      name = "IO-UDP-FF")
  }

  val bufferPool: BufferPool = new DirectByteBufferPool(settings.DirectBufferSize, settings.MaxDirectBufferPoolSize)
}

/**
 * INTERNAL API
 */
private[io] class UdpFFListener(val udpFF: UdpFFExt,
                                val bindCommander: ActorRef,
                                val bind: Bind)
  extends Actor with ActorLogging with WithUdpFFSend {

  import bind._
  import udpFF.bufferPool
  import udpFF.settings._
  import akka.io.UdpFF._

  def selector: ActorRef = context.parent

  context.watch(handler) // sign death pact
  val channel = {
    val datagramChannel = DatagramChannel.open
    datagramChannel.configureBlocking(false)
    val socket = datagramChannel.socket
    options.foreach(_.beforeDatagramBind(socket))
    try socket.bind(endpoint)
    catch {
      case NonFatal(e) ⇒
        bindCommander ! CommandFailed(bind)
        log.error(e, "Failed to bind UDP channel to endpoint [{}]", endpoint)
        context.stop(self)
    }
    datagramChannel
  }
  context.parent ! RegisterChannel(channel, OP_READ)
  log.debug("Successfully bound to [{}]", endpoint)

  def receive: Receive = {
    case ChannelRegistered ⇒
      bindCommander ! Bound
      context.become(readHandlers orElse sendHandlers, discardOld = true)
  }

  def readHandlers: Receive = {
    case StopReading     ⇒ selector ! DisableReadInterest
    case ResumeReading   ⇒ selector ! ReadInterest
    case ChannelReadable ⇒ doReceive(handler)

    case Unbind ⇒
      log.debug("Unbinding endpoint [{}]", endpoint)
      try {
        channel.close()
        sender ! Unbound
        log.debug("Unbound endpoint [{}], stopping listener", endpoint)
      } finally context.stop(self)
  }

  def doReceive(handler: ActorRef): Unit = {
    @tailrec def innerReceive(readsLeft: Int, buffer: ByteBuffer) {
      buffer.clear()
      buffer.limit(DirectBufferSize)

      channel.receive(buffer) match {
        case sender: InetSocketAddress ⇒
          buffer.flip()
          handler ! Received(ByteString(buffer), sender)
          if (readsLeft > 0) innerReceive(readsLeft - 1, buffer)
        case null ⇒ // null means no data was available
      }
    }

    val buffer = bufferPool.acquire()
    try innerReceive(BatchReceiveLimit, buffer) finally {
      bufferPool.release(buffer)
      selector ! ReadInterest
    }
  }

  override def postStop() {
    if (channel.isOpen) {
      log.debug("Closing DatagramChannel after being stopped")
      try channel.close()
      catch {
        case NonFatal(e) ⇒ log.error(e, "Error closing DatagramChannel")
      }
    }
  }
}

/**
 * INTERNAL API
 *
 * UdpFFManager is a facade for simple fire-and-forget style UDP operations
 *
 * UdpFFManager is obtainable by calling {{{ IO(UdpFF) }}} (see [[akka.io.IO]] and [[akka.io.UdpFF]])
 *
 * *Warning!* UdpFF uses [[java.nio.channels.DatagramChannel#send]] to deliver datagrams, and as a consequence if a
 * security manager  has been installed then for each datagram it will verify if the target address and port number are
 * permitted. If this performance overhead is undesirable use the connection style Udp extension.
 *
 * == Bind and send ==
 *
 * To bind and listen to a local address, a [[akka.io.UdpFF..Bind]] command must be sent to this actor. If the binding
 * was successful, the sender of the [[akka.io.UdpFF.Bind]] will be notified with a [[akka.io.UdpFF.Bound]]
 * message. The sender of the [[akka.io.UdpFF.Bound]] message is the Listener actor (an internal actor responsible for
 * listening to server events). To unbind the port an [[akka.io.Tcp.Unbind]] message must be sent to the Listener actor.
 *
 * If the bind request is rejected because the Udp system is not able to register more channels (see the nr-of-selectors
 * and max-channels configuration options in the akka.io.udpFF section of the configuration) the sender will be notified
 * with a [[akka.io.UdpFF.CommandFailed]] message. This message contains the original command for reference.
 *
 * The handler provided in the [[akka.io.UdpFF.Bind]] message will receive inbound datagrams to the bound port
 * wrapped in [[akka.io.UdpFF.Received]] messages which contain the payload of the datagram and the sender address.
 *
 * UDP datagrams can be sent by sending [[akka.io.UdpFF.Send]] messages to the Listener actor. The sender port of the
 * outbound datagram will be the port to which the Listener is bound.
 *
 * == Simple send ==
 *
 * UdpFF provides a simple method of sending UDP datagrams if no reply is expected. To acquire the Sender actor
 * a SimpleSend message has to be sent to the manager. The sender of the command will be notified by a SimpleSendReady
 * message that the service is available. UDP datagrams can be sent by sending [[akka.io.UdpFF.Send]] messages to the
 * sender of SimpleSendReady. All the datagrams will contain an ephemeral local port as sender and answers will be
 * discarded.
 *
 */
private[io] class UdpFFManager(udpFF: UdpFFExt) extends SelectorBasedManager(udpFF.settings, udpFF.settings.NrOfSelectors) {
  def receive = workerForCommandHandler {
    case b: Bind ⇒
      val commander = sender
      Props(new UdpFFListener(udpFF, commander, b))
    case SimpleSender(options) ⇒
      val commander = sender
      Props(new UdpFFSender(udpFF, options, commander))
  }

}

/**
 * Base class for TcpIncomingConnection and TcpOutgoingConnection.
 *
 * INTERNAL API
 */
private[io] class UdpFFSender(val udpFF: UdpFFExt, options: immutable.Traversable[SocketOption], val commander: ActorRef)
  extends Actor with ActorLogging with WithUdpFFSend {

  def selector: ActorRef = context.parent

  val channel = {
    val datagramChannel = DatagramChannel.open
    datagramChannel.configureBlocking(false)
    val socket = datagramChannel.socket

    options foreach { _.beforeDatagramBind(socket) }

    datagramChannel
  }
  selector ! RegisterChannel(channel, 0)

  def receive: Receive = {
    case ChannelRegistered ⇒
      context.become(sendHandlers, discardOld = true)
      commander ! SimpleSendReady
  }

  override def postStop(): Unit = if (channel.isOpen) {
    log.debug("Closing DatagramChannel after being stopped")
    try channel.close()
    catch {
      case NonFatal(e) ⇒ log.error(e, "Error closing DatagramChannel")
    }
  }

  override def postRestart(reason: Throwable): Unit =
    throw new IllegalStateException("Restarting not supported for connection actors.")
}

/**
 * INTERNAL API
 */
private[io] trait WithUdpFFSend {
  me: Actor with ActorLogging ⇒

  var pendingSend: Send = null
  var pendingCommander: ActorRef = null
  // If send fails first, we allow a second go after selected writable, but no more. This flag signals that
  // pending send was already tried once.
  var retriedSend = false
  def hasWritePending = pendingSend ne null

  def selector: ActorRef
  def channel: DatagramChannel
  def udpFF: UdpFFExt
  val settings = udpFF.settings

  import settings._

  def sendHandlers: Receive = {

    case send: Send if hasWritePending ⇒
      if (TraceLogging) log.debug("Dropping write because queue is full")
      sender ! CommandFailed(send)

    case send: Send if send.payload.isEmpty ⇒
      if (send.wantsAck)
        sender ! send.ack

    case send: Send ⇒
      pendingSend = send
      pendingCommander = sender
      doSend()

    case ChannelWritable ⇒ if (hasWritePending) doSend()

  }

  final def doSend(): Unit = {

    val buffer = udpFF.bufferPool.acquire()
    try {
      buffer.clear()
      pendingSend.payload.copyToBuffer(buffer)
      buffer.flip()
      val writtenBytes = channel.send(buffer, pendingSend.target)
      if (TraceLogging) log.debug("Wrote [{}] bytes to channel", writtenBytes)

      // Datagram channel either sends the whole message, or nothing
      if (writtenBytes == 0) {
        if (retriedSend) {
          pendingCommander ! CommandFailed(pendingSend)
          retriedSend = false
          pendingSend = null
          pendingCommander = null
        } else {
          selector ! WriteInterest
          retriedSend = true
        }
      } else {
        if (pendingSend.wantsAck) pendingCommander ! pendingSend.ack
        retriedSend = false
        pendingSend = null
        pendingCommander = null
      }

    } finally {
      udpFF.bufferPool.release(buffer)
    }
  }
}
