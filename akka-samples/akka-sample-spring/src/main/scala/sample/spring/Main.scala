/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package sample.spring

import akka.actor._
import akka.pattern.gracefulStop
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

import javax.annotation.PreDestroy
import javax.inject._

import akka.spring.{ ScopedActor, SpringHelper }

object Main {
  def main(args: Array[String]): Unit = {
    val context: ConfigurableApplicationContext = new ClassPathXmlApplicationContext("spring.xml")
    // TODO: Programmatically register ActorScope
    // TODO: Maybe ensure that Actors can only have prototype scope, since refs should not be held
    try {
      implicit val timeout = Timeout(5.seconds)
      import IncrementService._
      val incrementService = context.getBean(classOf[IncrementService])
      val result = Await.result(incrementService.incrementer ? Increment(5), 6.seconds)
      println(s"Result is: $result")
    } finally context.close()
  }
}

object AdditionService {
  case class Add(a: Int, b: Int)
  case class AddResult(x: Int)
}

@Named
@Singleton
class AdditionService @Inject() (arf: ActorSystem) {
  import AdditionService._

  val adder = arf.actorOf(Props(new Actor {
    def receive = {
      case Add(a, b) ⇒ sender ! AddResult(a + b)
    }
  }))

  @PreDestroy
  def shutdown() {
    val stopped = gracefulStop(adder, 4.seconds)(arf)
    Await.result(stopped, 5.seconds)
  }
}

object IncrementService {
  case class Increment(x: Int)
  case class IncrementResult(x: Int)
}

@Named
@Singleton
class IncrementService @Inject() (arf: ActorSystem, additionService: AdditionService) {
  import IncrementService._

  val incrementer = arf.actorOf(Props(new Actor {
    override def receive = {
      case Increment(x) ⇒ {
        val incrementSender = sender
        // Start a little worker to handle the conversation with the adder
        context.actorOf(Props(new Actor {
          import AdditionService._
          override def preStart() {
            additionService.adder ! Add(x, 1)
          }
          def receive = {
            case AddResult(y) ⇒ {
              incrementSender ! IncrementResult(y) // TODO send from incrementer?
              context.stop(self)
            }
          }
        }))
      }
    }
  }))

  @PreDestroy
  def shutdown() {
    val stopped = gracefulStop(incrementer, 4.seconds)(arf)
    Await.result(stopped, 5.seconds)
  }
}