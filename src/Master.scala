import scala.math
import akka.actor.Props
import akka.actor._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;

case class Converged(Index: Int)

//============= Since this is a singleton Master, we are placing the instance variables in the static block
object Master {

  val GOSSIP_CODE = "GOSSIP"
  val PUSH_SUM_CODE = "PUSHSUM"
  var startTime = System.currentTimeMillis()
  var terminatedWorkers = new ArrayBuffer[Int]
  var jobCompletionRatio: Double = 0
  var totalExchangesMssgs: Double = 0
  var checkPointMssgs: Double = 0

}

class Master(numActors: Int, algoTopoCode: String, numStartNodes: Int, msgCount: Int) extends Actor {

  var completedWorkersCount: Int = 0
  var ImplementationObj = new Implementation(numActors: Int, algoTopoCode: String, numStartNodes: Int, msgCount: Int, context: ActorContext, self: ActorRef)
  var minPropagationLimit:Double = 0

  import context.dispatcher

  val startSetFireTime = Duration(2000, "millis")
  val nextSetFireTime = Duration(10, "millis")
  val cancellable =
    context.system.scheduler.schedule(startSetFireTime, nextSetFireTime) {
      if (Master.totalExchangesMssgs == Master.checkPointMssgs) {
        println(" CONVERGANCE CANNOT BE REACHED AS THERE IS A DEADLOCK :"+Master.totalExchangesMssgs)
        context.system.shutdown()
        exit()
      }
      Master.checkPointMssgs = Master.totalExchangesMssgs
    }

  println(" ========== STARTING THE MASTER NODE ===============")

  def receive = {

    case "GOSSIP-FULL" => {ImplementationObj.handleGossipFull(Master.GOSSIP_CODE);minPropagationLimit = 0.9}

    case "GOSSIP-LINE" => {ImplementationObj.handleGossipLine(Master.GOSSIP_CODE);minPropagationLimit = 0.9}

    case "GOSSIP-2D" => {ImplementationObj.handleGossip2D(Master.GOSSIP_CODE);minPropagationLimit = 0.9}

    case "GOSSIP-IMP2D" => {ImplementationObj.handleGossip2DImp(Master.GOSSIP_CODE);minPropagationLimit = 0.9}

    case "PUSH-SUM-FULL" => {ImplementationObj.handleGossipFull(Master.PUSH_SUM_CODE);minPropagationLimit = 0.7}

    case "PUSH-SUM-2D" => {ImplementationObj.handleGossip2D(Master.PUSH_SUM_CODE);minPropagationLimit = 0.7}

    case "PUSH-SUM-LINE" => {ImplementationObj.handleGossipLine(Master.PUSH_SUM_CODE);minPropagationLimit = 0.7}

    case "PUSH-SUM-IMP2D" => {ImplementationObj.handleGossip2DImp(Master.PUSH_SUM_CODE);minPropagationLimit = 0.7}

    case Converged(index: Int) =>

      completedWorkersCount += 1

      var newJobCompletionratio: Double = completedWorkersCount.toDouble / numActors.toDouble
      var jobCompletionRatioDiff: Double = newJobCompletionratio - Master.jobCompletionRatio

      Master.terminatedWorkers.append(index)

      if (newJobCompletionratio >= minPropagationLimit) {
        println(" ========== GOSSIP PROPAGATION % : " + (newJobCompletionratio * 100).toInt)
        println(" ========== PROPAGATION GREATER THAN "+(minPropagationLimit*100).toInt+"% and HENCE SHUTTING DOWN")
        println(" ======================================================================")
        println(" ========== TOTAL TIME TAKEN : " + (System.currentTimeMillis() - Master.startTime) + " MILLISECONDS")
        println(" ======================================================================")
        context.system.shutdown()
        exit()
      }

      if (jobCompletionRatioDiff >= 0.1) {
        Master.jobCompletionRatio = newJobCompletionratio
        println(" ========== GOSSIP PROPAGATION % : " + (newJobCompletionratio * 100).toInt)
      }
    /*if (completedWorkersCount == numActors) {
        println("====================JOB FINISHED ===============\n")
        context.system.shutdown()
      }*/

  }

}
