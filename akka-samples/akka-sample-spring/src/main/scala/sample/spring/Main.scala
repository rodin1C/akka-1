/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package sample.spring

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.japi.Creator
import akka.pattern.ask
import akka.util.Timeout
import org.springframework.context.support.{ AbstractApplicationContext, ClassPathXmlApplicationContext }
import org.springframework.context.{ ApplicationContext, ApplicationContextAware }
import scala.beans.BeanProperty
import scala.concurrent.Await
import scala.concurrent.duration._

class SpringBridge(system: ActorSystem) extends ApplicationContextAware {
  @BeanProperty
  var applicationContext: ApplicationContext = _

  private def actorBeanCreator(name: String) = new Creator[Actor] {
    def create() = applicationContext.getBean(name, classOf[Actor])
  }

  def actorOf(actorBeanRef: String) = {
    system.actorOf(Props(actorBeanCreator(actorBeanRef)))
  }

  def actorOf(actorBeanRef: String, name: String) = {
    system.actorOf(Props(actorBeanCreator(actorBeanRef)), name)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val context: AbstractApplicationContext = new ClassPathXmlApplicationContext("spring.xml")

    val hi = context.getBean("hi-ref", classOf[ActorRef])
    val hello = context.getBean("hello-ref", classOf[ActorRef])

    implicit val timeout = Timeout(5.seconds)
    Await.result(hi ? Greet, 5.seconds)
    Await.result(hello ? Greet, 5.seconds)

    context.close()
  }
}

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
