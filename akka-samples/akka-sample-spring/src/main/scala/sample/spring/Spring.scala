/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.spring

import akka.actor.{ Actor, ActorRef, ActorRefFactory, Props }
import akka.japi.Creator
import akka.pattern.ask
import akka.util.Timeout
import org.springframework.beans.factory.{ BeanFactory, ObjectFactory }
import org.springframework.beans.factory.config.{ ConfigurableBeanFactory, Scope }
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.context.{ ApplicationContext, ApplicationContextAware, ConfigurableApplicationContext }
import scala.beans.BeanProperty
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * A very rough interface to Spring. Currently provides support for creating actors from
 * some actor configuration.
 */
object SpringHelper {
  def createSimpleSpringConfiguredActor(
    arf: ActorRefFactory,
    applicationContext: ApplicationContext,
    beanName: String): ActorRef = {
    createSpringConfiguredActor(arf, applicationContext, (bf: BeanFactory) ⇒ bf.getBean(beanName, classOf[Actor]), identity[Props])
  }

  def createSpringConfiguredActor(
    arf: ActorRefFactory,
    applicationContext: ApplicationContext,
    getBean: BeanFactory ⇒ Actor,
    propsTransform: Props ⇒ Props): ActorRef = {
    val scopeAttrs = new ActorScopeAttributes
    val springProps = Props({
      scopeAttrs.within {
        // We can only guarantee destruction callbacks will be called for
        // actors that implement ScopedActor. Therefore we do not support
        // destruction callbacks in the 'actor' scope for other types of
        // actor.
        //
        // Before proceeding we need to examine the actor we've created and
        // check if it's a ScopedActor. If it is, then we proceed normally.
        // If not, then we need to make sure that destruction callbacks
        // cannot happen.
        //
        // To prevent these callbacks we call `forbidDestructionCallbacks`
        // on the scopeAttrs object. This will cause the object to throw
        // an UnsupportedOperationException if there is even an attempt in the
        // future to register a destruction callback.
        //
        // Unfortunately some destruction callbacks may have *already*
        // been registered with the scopeAttrs object (by Spring, during construction).
        // If this has happened then we immediately destroy the scope and throw an
        // UnsupportedOperationException.
        val actor = getBean(applicationContext)
        actor match {
          case scopedActor: ScopedActor ⇒ scopedActor.scopeAttrs = scopeAttrs
          case _ ⇒ {
            if (scopeAttrs.hasDestructionCallbacks) {
              scopeAttrs.destroy()
              throw new UnsupportedOperationException("Only ScopedActors support destruction of objects in the Spring 'actor' scope.")
            } else {
              scopeAttrs.forbidDestructionCallbacks()
            }
          }
        }
        actor
      }
    })
    arf.actorOf(propsTransform(springProps))
  }
}

/**
 * A Spring scope that lasts for the lifetime of an actor, from construction through to `postStop`.
 * Needs to be registered with Spring.
 */
class ActorScope extends Scope {
  private[akka] def attrs: ActorScopeAttributes =
    Option(ActorScopeAttributes.local.get()).getOrElse(throw new IllegalStateException("Not in Spring 'actor' scope"))
  private[akka] def destroy() { attrs.destroy() }

  override def get(name: String, objectFactory: ObjectFactory[_]) = attrs.get(name, objectFactory)
  override def getConversationId() = attrs.getConversationId()
  override def registerDestructionCallback(name: String, callback: Runnable) {
    attrs.registerDestructionCallback(name, callback)
  }
  override def remove(name: String) = attrs.remove(name)
  override def resolveContextualObject(name: String): Object = attrs.resolveContextualObject(name)
}

/**
 * An actor that knows about the Spring 'actor' scope. In particular, it
 * guarantees that the scope is properly destroyed when the actor is stopped.
 */
trait ScopedActor extends Actor {
  private[akka] var scopeAttrs: ActorScopeAttributes = _

  def withinScope[A](body: ⇒ A): A = scopeAttrs.within(body)

  final override def preStart() = {
    withinScope(scopedPreStart())
  }

  final override def postStop() = {
    try withinScope(scopedPostStop()) finally scopeAttrs.destroy()
  }

  def scopedPreStart() {}

  def scopedPostStop() {}
}

/**
 * Used to implement the ActorScope; holds the thread-local information.
 */
private[akka] object ActorScopeAttributes {
  private[akka] val local = new ThreadLocal[ActorScopeAttributes]()
}

/**
 * Used to implement the ActorScope; stores the scope information.
 */
private[akka] class ActorScopeAttributes {

  private[akka] def within[A](body: ⇒ A): A = {
    import ActorScopeAttributes.local
    val old = local.get()
    local.set(this)
    try body finally local.set(old)
  }

  // TODO: Think about thread-safety
  private var beans: Map[String, AnyRef] = Map.empty
  private var destructionCallbacks: Option[Map[String, Runnable]] = Some(Map.empty) // None = not in ScopedActor

  private[akka] def get(name: String, objectFactory: ObjectFactory[_]): AnyRef = {
    beans.getOrElse(name, {
      val bean: Object = objectFactory.getObject().asInstanceOf[AnyRef] // FIXME: Think more about this cast
      beans = beans.updated(name, bean)
      bean
    })
  }
  private[akka] def getConversationId() = null
  private[akka] def registerDestructionCallback(name: String, callback: Runnable) {
    destructionCallbacks.fold(throw new UnsupportedOperationException("Only ScopeActors support 'actor' scope objects with destructors")) {
      map ⇒ destructionCallbacks = Some(map.updated(name, callback))
    }
  }
  private[akka] def hasDestructionCallbacks = !destructionCallbacks.get.isEmpty
  private[akka] def forbidDestructionCallbacks() {
    assert(destructionCallbacks.forall(map ⇒ map.isEmpty))
    destructionCallbacks = None
  }
  private[akka] def remove(name: String) = ??? // FIXME
  private[akka] def resolveContextualObject(name: String): Object = null
  private[akka] def destroy() {
    for (map ← destructionCallbacks; (_, callback) ← map) { callback.run() }
  }
}