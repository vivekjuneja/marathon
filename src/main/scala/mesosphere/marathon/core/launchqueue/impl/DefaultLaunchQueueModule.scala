package mesosphere.marathon.core.launchqueue.impl

import akka.actor.{ ActorRef, ActorSystem, Props }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.OfferMatcherManager
import mesosphere.marathon.core.launchqueue.{ LaunchQueue, LaunchQueueModule }
import mesosphere.marathon.core.task.bus.TaskStatusObservables
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker }

private[core] class DefaultLaunchQueueModule(
    actorSystem: ActorSystem,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    taskStatusObservables: TaskStatusObservables,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory) extends LaunchQueueModule {

  override lazy val taskQueue: LaunchQueue = new ActorLaunchQueue(taskQueueActorRef)

  private[this] def appActorProps(app: AppDefinition, count: Int): Props =
    AppTaskLauncherActor.props(
      subOfferMatcherManager,
      clock,
      taskFactory,
      taskStatusObservables,
      taskTracker)(app, count)

  private[impl] lazy val taskQueueActorRef: ActorRef = {
    val props = TaskQueueActor.props(appActorProps)
    actorSystem.actorOf(props, "taskQueue")
  }

}
