/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.io

import java.net.DatagramSocket
import akka.util.Helpers.Requiring
import akka.io.Inet.{ SoJavaFactories, SocketOption }
import com.typesafe.config.Config

object Udp {

  object SO extends Inet.SoForwarders {

    /**
     * [[akka.io.Inet.SocketOption]] to set the SO_BROADCAST option
     *
     * For more information see [[java.net.DatagramSocket#setBroadcast]]
     */
    case class Broadcast(on: Boolean) extends SocketOption {
      override def beforeDatagramBind(s: DatagramSocket): Unit = s.setBroadcast(on)
    }

  }

  private[io] class UdpSettings(_config: Config) extends SelectionHandlerSettings(_config) {
    import _config._

    val NrOfSelectors = getInt("nr-of-selectors").
      requiring(_ > 0, "nr-of-selectors must be > 0")
    val DirectBufferSize = getBytes("direct-buffer-size").
      requiring(_ < Int.MaxValue, "direct-buffer-size must be < 2 GiB").toInt
    val MaxDirectBufferPoolSize = getInt("direct-buffer-pool-limit")
    val BatchReceiveLimit = getInt("receive-throughput")
    val ManagementDispatcher = getString("management-dispatcher")

    override val MaxChannelsPerSelector = if (MaxChannels == -1) -1 else math.max(MaxChannels / NrOfSelectors, 1)
  }
}

object UdpSO extends SoJavaFactories {
  import Udp.SO._
  def broadcast(on: Boolean) = Broadcast(on)
}
