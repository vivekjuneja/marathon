package mesosphere.marathon.core

import com.google.inject.Inject
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.actors.ActorsModule
import mesosphere.marathon.core.base.{ Clock, ShutdownHooks }
import mesosphere.marathon.core.launcher.LauncherModule
import mesosphere.marathon.core.matcher.OfferMatcherModule
import mesosphere.marathon.core.launchqueue.LaunchQueueModule
import mesosphere.marathon.core.task.bus.TaskBusModule
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker }

import scala.util.Random

/**
  * Provides the wiring for the core module.
  *
  * Its parameters represent guice wired dependencies.
  * [[CoreGuiceModule]] exports some dependencies back to guice.
  */
class DefaultCoreModule @Inject() (
    // external dependencies still wired by guice
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory) extends CoreModule {

  // INFRASTRUCTURE LAYER

  override lazy val clock = Clock()
  private[this] lazy val random = Random
  private[this] lazy val shutdownHookModule = ShutdownHooks()
  private[this] lazy val actorsModule = ActorsModule(shutdownHookModule)

  // CORE

  override lazy val taskBusModule = TaskBusModule()

  private[this] lazy val offerMatcherModule = OfferMatcherModule(
    // infrastructure
    clock, random,
    actorsModule.actorSystem,

    // core references
    taskBusModule.taskStatusEmitter,
    taskBusModule.taskStatusObservables
  )

  override lazy val launcherModule = LauncherModule(
    // infrastructure
    clock,

    // external guicedependencies
    marathonSchedulerDriverHolder,

    // internal core dependencies
    taskBusModule.taskStatusEmitter,
    offerMatcherModule.globalOfferMatcher)

  override lazy val appOfferMatcherModule = LaunchQueueModule(
    actorsModule.actorSystem, clock,

    // internal core dependencies
    offerMatcherModule.subOfferMatcherManager,
    taskBusModule.taskStatusObservables,

    // external guice dependencies
    taskTracker,
    taskFactory
  )
}
