/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.actor.dungeon

import akka.actor.{ Terminated, InternalActorRef, ActorPath, ActorRef, ActorRefScope, ActorCell, Actor, Address, AddressTerminated }
import akka.dispatch.{ ChildTerminated, Watch, Unwatch }
import akka.event.Logging.{ Warning, Error, Debug }
import scala.util.control.NonFatal

private[akka] trait DeathWatch { this: ActorCell ⇒

  private var watching: Map[ActorPath, ActorRef] = ActorCell.emptyActorRefMap
  private var watchedBy: Map[ActorPath, ActorRef] = ActorCell.emptyActorRefMap

  override final def watch(subject: ActorRef): ActorRef = subject match {
    case a: InternalActorRef ⇒
      if (a.path != self.path && !watching.contains(a.path)) {
        maintainAddressTerminatedSubscription(a) {
          a.sendSystemMessage(Watch(a, self)) // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
          watching += (a.path -> a)
        }
      }
      a
  }

  override final def unwatch(subject: ActorRef): ActorRef = subject match {
    case a: InternalActorRef ⇒
      if (a.path != self.path && watching.contains(a.path)) {
        maintainAddressTerminatedSubscription(a) {
          a.sendSystemMessage(Unwatch(a, self)) // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
          watching -= a.path
        }
      }
      a
  }

  /**
   * When this actor is watching the subject of [[akka.actor.Terminated]] message
   * it will be propagated to user's receive.
   */
  protected def watchedActorTerminated(t: Terminated): Unit =
    if (watching.contains(t.actor.path)) {
      maintainAddressTerminatedSubscription(t.actor) {
        watching -= t.actor.path
      }
      receiveMessage(t)
    }

  protected def tellWatchersWeDied(actor: Actor): Unit = {
    if (!watchedBy.isEmpty) {
      val terminated = Terminated(self)(existenceConfirmed = true, addressTerminated = false)
      try {
        def sendTerminated(ifLocal: Boolean)(watcher: ActorRef): Unit =
          if (watcher.asInstanceOf[ActorRefScope].isLocal == ifLocal) watcher.tell(terminated, self)

        /*
         * It is important to notify the remote watchers first, otherwise RemoteDaemon might shut down, causing
         * the remoting to shut down as well. At this point Terminated messages to remote watchers are no longer
         * deliverable.
         *
         * The problematic case is:
         *  1. Terminated is sent to RemoteDaemon
         *   1a. RemoteDaemon is fast enough to notify the terminator actor in RemoteActorRefProvider
         *   1b. The terminator is fast enough to enqueue the shutdown command in the remoting
         *  2. Only at this point is the Terminated (to be sent remotely) enqueued in the mailbox of remoting
         *
         * If the remote watchers are notified first, then the mailbox of the Remoting will guarantee the correct order.
         */
        watchedBy foreach { case (_, a) ⇒ sendTerminated(ifLocal = false)(a) }
        watchedBy foreach { case (_, a) ⇒ sendTerminated(ifLocal = true)(a) }
      } finally watchedBy = ActorCell.emptyActorRefMap
    }
  }

  protected def unwatchWatchedActors(actor: Actor): Unit = {
    if (!watching.isEmpty) {
      try {
        watching foreach { // ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
          case (_, watchee: InternalActorRef) ⇒ watchee.sendSystemMessage(Unwatch(watchee, self))
        }
      } finally {
        watching = ActorCell.emptyActorRefMap
        unsubscribeAddressTerminated()
      }
    }
  }

  protected def addWatcher(watchee: ActorRef, watcher: ActorRef): Unit = {
    val watcheeSelf = watchee.path == self.path
    val watcherSelf = watcher.path == self.path

    if (watcheeSelf && !watcherSelf) {
      if (!watchedBy.contains(watcher.path)) maintainAddressTerminatedSubscription(watcher) {
        watchedBy += (watcher.path -> watcher)
        if (system.settings.DebugLifecycle) publish(Debug(self.path.toString, clazz(actor), "now monitoring " + watcher))
      }
    } else if (!watcheeSelf && watcherSelf) {
      watch(watchee)
    } else {
      publish(Warning(self.path.toString, clazz(actor), "BUG: illegal Watch(%s,%s) for %s".format(watchee, watcher, self)))
    }
  }

  protected def remWatcher(watchee: ActorRef, watcher: ActorRef): Unit = {
    val watcheeSelf = watchee.path == self.path
    val watcherSelf = watcher.path == self.path

    if (watcheeSelf && !watcherSelf) {
      if (watchedBy.contains(watcher.path)) maintainAddressTerminatedSubscription(watcher) {
        watchedBy -= watcher.path
        if (system.settings.DebugLifecycle) publish(Debug(self.path.toString, clazz(actor), "stopped monitoring " + watcher))
      }
    } else if (!watcheeSelf && watcherSelf) {
      unwatch(watchee)
    } else {
      publish(Warning(self.path.toString, clazz(actor), "BUG: illegal Unwatch(%s,%s) for %s".format(watchee, watcher, self)))
    }
  }

  protected def addressTerminated(address: Address): Unit = {
    // cleanup watchedBy since we know they are dead
    maintainAddressTerminatedSubscription() {
      for ((p, _) ← watchedBy; if p.address == address) watchedBy -= p
    }

    // send Terminated to self for all matching subjects
    // existenceConfirmed = false because we could have been watching a
    // non-local ActorRef that had never resolved before the other node went down
    // When a parent is watching a child and it terminates due to AddressTerminated
    // it is removed by sending ChildTerminated to support immediate creation of child
    // with same name.
    for ((p, a) ← watching; if p.address == address) {
      childrenRefs.getByRef(a) foreach { _ ⇒ self.sendSystemMessage(ChildTerminated(a)) }
      self ! Terminated(a)(existenceConfirmed = false, addressTerminated = true)
    }
  }

  /**
   * Starts subscription to AddressTerminated if not already subscribing and the
   * block adds a non-local ref to watching or watchedBy.
   * Ends subscription to AddressTerminated if subscribing and the
   * block removes the last non-local ref from watching and watchedBy.
   */
  private def maintainAddressTerminatedSubscription[T](change: ActorRef = null)(block: ⇒ T): T = {
    def isNonLocal(ref: ActorRef) = ref match {
      case null                              ⇒ true
      case a: InternalActorRef if !a.isLocal ⇒ true
      case _                                 ⇒ false
    }

    if (isNonLocal(change)) {
      def hasNonLocalAddress: Boolean =
        ((watching.exists { case (_, a) ⇒ isNonLocal(a) }) || (watchedBy.exists { case (_, a) ⇒ isNonLocal(a) }))
      val had = hasNonLocalAddress
      val result = block
      val has = hasNonLocalAddress
      if (had && !has) unsubscribeAddressTerminated()
      else if (!had && has) subscribeAddressTerminated()
      result
    } else {
      block
    }
  }

  private def unsubscribeAddressTerminated(): Unit = system.eventStream.unsubscribe(self, classOf[AddressTerminated])

  private def subscribeAddressTerminated(): Unit = system.eventStream.subscribe(self, classOf[AddressTerminated])

}
