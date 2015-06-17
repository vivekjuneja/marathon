package mesosphere.marathon.core.matcher

import akka.actor.ActorSystem
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.impl.DefaultOfferMatcherModule
import mesosphere.marathon.core.task.bus.{ TaskStatusObservables, TaskStatusEmitter }

import scala.util.Random

trait OfferMatcherModule {
  /** The offer matcher which forwards match requests to all registered sub offer matchers. */
  def globalOfferMatcher: OfferMatcher
  def subOfferMatcherManager: OfferMatcherManager
}

object OfferMatcherModule {
  def apply(
    clock: Clock,
    random: Random,
    actorSystem: ActorSystem,
    taskStatusEmitter: TaskStatusEmitter,
    taskStatusObservables: TaskStatusObservables): OfferMatcherModule =
    new DefaultOfferMatcherModule(clock, random, actorSystem, taskStatusEmitter, taskStatusObservables)
}

