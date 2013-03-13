/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package sample.spring

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.japi.Creator
import akka.pattern.ask
import akka.util.Timeout
import org.springframework.beans.factory.ObjectFactory
import org.springframework.beans.factory.config.{ ConfigurableBeanFactory, Scope }
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.context.{ ApplicationContext, ApplicationContextAware, ConfigurableApplicationContext }
import scala.beans.BeanProperty
import scala.concurrent.Await
import scala.concurrent.duration._

import javax.inject._

import akka.spring.{ ScopedActor, SpringHelper }

object Main {
  def main(args: Array[String]): Unit = {
    val context: ConfigurableApplicationContext = new ClassPathXmlApplicationContext("spring.xml")
    // TODO: Programmatically register ActorScope
    // TODO: Maybe ensure that Actors can only have prototype scope, since refs should not be held
    try {
      val system = ActorSystem()
      try {
        implicit val timeout = Timeout(2.seconds)

        val hi = SpringHelper.createSimpleSpringConfiguredActor(system, context, "hi-actor")
        val hello = SpringHelper.createSimpleSpringConfiguredActor(system, context, "hello-actor")
        Await.result(hi ? Greet, 1.second)
        Await.result(hello ? Greet, 1.second)

        val resourceCtor = SpringHelper.createSimpleSpringConfiguredActor(system, context, classOf[ResourceHolderCtor])
        val resourceProp = SpringHelper.createSimpleSpringConfiguredActor(system, context, classOf[ResourceHolderProp])
        Await.result(resourceCtor ? ResourcePing, 1.second)
        Await.result(resourceProp ? ResourcePing, 1.second)
      } finally system.shutdown()
    } finally context.close()
  }
}

// Testing simple constructor dependency injection

case object Greet
case object GreetDone

class GreetingActor(greeting: String) extends Actor {
  def receive = {
    case Greet ⇒ {
      println(s"$greeting world!")
      sender ! GreetDone
    }
  }
}

// Testing injecting resources into an actor using the 'actor' scope for cleanup

class Resource {
  def destroy() { println("Destroying resource") }
}

case object ResourcePing
case class ResourcePong(resource: Resource)

// Create with constructor-based dependency injection
@Named
@org.springframework.context.annotation.Scope("prototype")
class ResourceHolderCtor @Inject() (resource: Resource) extends ScopedActor {
  def receive = {
    case _ ⇒ sender ! ResourcePong(resource)
  }
}

// Create with property-based dependency injection
@Named
@org.springframework.context.annotation.Scope("prototype")
class ResourceHolderProp extends ScopedActor {
  @Inject @BeanProperty
  var resource: Resource = _
  def receive = {
    case ResourcePing ⇒ sender ! ResourcePong(resource)
  }
}
