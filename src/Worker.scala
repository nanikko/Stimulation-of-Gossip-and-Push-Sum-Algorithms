import akka.actor._
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;

case class pushSumRumour(Sum: Double, Weight: Double)

class Worker(numActors: Int, msgcount: Int, adjacentWorkersIndex: ArrayBuffer[Int], workers: Array[ActorRef], master: ActorRef, actorSumInit: Int, context: ActorContext) extends Actor {

  import context.dispatcher

  var count: Int = 0 //This variable maintains the numbers of rumours received from other Workers.
  var sentMaxReport = false
  var actorSum: Double = actorSumInit
  var actorWeight: Double = 1
  var DIVERGENCE: Double = 2 //This variale is need in the push sum algorithm where the sum and weights are split among self and adjacent actors.
  var PUSH_SUM_TERMINATION_VALUE: Double = math.pow(10, -10)
  var pushSumCounter = 0 //Variable to check the number of times the s/w diff gets less than 10^-10. actor stops forwarding mssgs once this var equals 3.
  var PUSH_SUM_MAX_RUMOURS: Int = 3

  def receive = {

    case "rumour" => //GOSSIP CASE
      {

        Master.totalExchangesMssgs += 1
        if (count < msgcount) {
          count = count + 1
          var i: Int = 0;
          val adjacenctWorkersLength = adjacentWorkersIndex.length
          while (i < 6) { // sending to five other nodes.
            var rand = new Random()
            var randIndex: Int = rand.nextInt(adjacenctWorkersLength)
            workers(adjacentWorkersIndex(randIndex)) ! "rumour"
            i += 1
          }
        } else {

          if (sentMaxReport == false) {
            master ! Converged(actorSumInit)
            sentMaxReport = true
          }

        }

      }
    case pushSumRumour(incomingsum, incomingweight) => //PUSH-SUM CASE
      {
        count = count + 1
        Master.totalExchangesMssgs += 1

        var OldSByW = (actorSum / actorWeight).toDouble

        actorSum += incomingsum
        actorWeight += incomingweight

        //=============== DIVIDING THE SUM AND WEIGHT AMONG SELF AND THE NEIGHBOUR ACTORS :USUALLY SELF AND ONE OTHER ========================
        actorSum = actorSum / DIVERGENCE
        actorWeight = actorWeight / DIVERGENCE

        var newSByW = actorSum / actorWeight

        //println(" ========= "+self.path.name+" RUMOURS COUNT: " + count + " S/W DIFF: " + Math.abs(newSByW - OldSByW))

        val adjacenctWorkersLength = adjacentWorkersIndex.length

        if (sentMaxReport == false) {

          if (count == 1 || Math.abs(newSByW - OldSByW) > PUSH_SUM_TERMINATION_VALUE) {

            pushSumCounter = 0
            forwardPushSumRumour(actorSum, actorWeight)

          } else { // When the difference less than termination count which is 3 for us
            if (pushSumCounter < PUSH_SUM_MAX_RUMOURS) {

              pushSumCounter += 1
              for (i <- 0 until 6) {
                forwardPushSumRumour(actorSum, actorWeight)
              }

            } else if (pushSumCounter == PUSH_SUM_MAX_RUMOURS && sentMaxReport == false) {
              master ! Converged(actorSumInit)
              sentMaxReport = true
              for (i <- 0 until 6) {
                forwardPushSumRumour(actorSum, actorWeight)
              }
            }
          }
        }
      }

  }

  /*THIS FUNCTION IS APPLICABLE ONLY TO PUSH-SUM ALGORITHM AS THE ALGO SPECIFIES 
   * THAT WE DO NOT SEND THE SEND THE RUMOUR TO THE ADJACENT ACTORS THAT ARE TERMINATED 
   * WHEREAS FOR GOSSIP ALGO, WE SEND TO THOSE THAT REACH THEIR THRESHOLD*/

  def forwardPushSumRumour(actorSum: Double, actorWeight: Double) {

    val adjacenctWorkersLength = adjacentWorkersIndex.length
    var randIndex: Int = 0

    var i: Int = 0
    var forwarded = false
    while (forwarded == false && i < adjacenctWorkersLength) {
      var rand = new Random()
      randIndex = rand.nextInt(adjacenctWorkersLength)
      if (!Master.terminatedWorkers.contains(adjacentWorkersIndex(randIndex))) {
        workers(adjacentWorkersIndex(randIndex)) ! pushSumRumour(actorSum, actorWeight)
        forwarded = true
      }
      i += 1
    }

  }
}  