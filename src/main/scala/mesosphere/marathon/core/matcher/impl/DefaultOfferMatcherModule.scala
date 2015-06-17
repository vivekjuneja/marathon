package mesosphere.marathon.core.matcher.impl

import akka.actor.{ ActorRef, ActorSystem }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.util.ActorOfferMatcher
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherManager, OfferMatcherModule }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusObservables, TaskStatusEmitter }

import scala.concurrent.duration._
import scala.util.Random

private[matcher] class DefaultOfferMatcherModule(
  clock: Clock,
  random: Random,
  actorSystem: ActorSystem,
  taskStatusEmitter: TaskStatusEmitter,
  taskStatusObservables: TaskStatusObservables)
    extends OfferMatcherModule {

  val launchTokenInterval: FiniteDuration = 30.seconds
  val launchTokensPerInterval: Int = 1000

  private[this] lazy val offerMatcherMultiplexer: ActorRef = {
    val props = OfferMatcherMultiplexerActor.props(
      random,
      clock,
      taskStatusEmitter)
    val actorRef = actorSystem.actorOf(props, "OfferMatcherMultiplexer")
    implicit val dispatcher = actorSystem.dispatcher
    actorSystem.scheduler.schedule(
      0.seconds, launchTokenInterval, actorRef,
      OfferMatcherMultiplexerActor.SetTaskLaunchTokens(launchTokensPerInterval))

    taskStatusObservables.forAll.foreach {
      case TaskStatusUpdate(_, _, MarathonTaskStatus.Staging(_)) =>
        actorRef ! OfferMatcherMultiplexerActor.AddTaskLaunchTokens(1)
      case _ => // ignore
    }
    actorRef
  }

  override val globalOfferMatcher: OfferMatcher = new ActorOfferMatcher(clock, offerMatcherMultiplexer)

  override val subOfferMatcherManager: OfferMatcherManager = new ActorOfferMatcherManager(offerMatcherMultiplexer)
}
