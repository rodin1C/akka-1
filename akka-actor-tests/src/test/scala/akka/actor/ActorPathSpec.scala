/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.actor

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class ActorPathSpec extends WordSpec with MustMatchers {

  "ActorPath" must {

    "create correct toString" in {
      val a = Address("akka.tcp", "mysys")
      RootActorPath(a).toString must be("akka.tcp://mysys/")
      (RootActorPath(a) / "user").toString must be("akka.tcp://mysys/user")
      (RootActorPath(a) / "user" / "foo").toString must be("akka.tcp://mysys/user/foo")
      (RootActorPath(a) / "user" / "foo" / "bar").toString must be("akka.tcp://mysys/user/foo/bar")
    }
  }
}
