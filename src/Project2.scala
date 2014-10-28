import scala.math
import akka.actor._
import com.typesafe.config.ConfigFactory

// TO-DO We are importing older version of Akka actors. Actor selection syntax is a little modified in the newer version.

object project2 extends App {

  if (args.length > 0) {
    val numActors = args(0).toInt
    val topology = args(1)
    val algo = args(2)
    var numStartNodes = 1

    if (args.length > 3) {
      numStartNodes = args(3).toInt
    }

    var msgcount = 10
    val masterSystem = ActorSystem("mastersystem")
    val algoTopoCode = getAlgoGossipCode(topology, algo) //For better code visibility

    //For our ease, we are rounding off the number of actors to a perfect square for matric topologies
    val revisedNumActors = getRevisedActorsNum(topology, numActors)

    val master = masterSystem.actorOf(Props(new Master(revisedNumActors, algoTopoCode, numStartNodes, msgcount)), name = "master")

    // starting the rumour
    master ! algoTopoCode
  }

  def getAlgoGossipCode(topology: String, algo: String): String = {

    var algoGossipCode = ""

    if ((algo.equalsIgnoreCase("gossip")) && (topology.equalsIgnoreCase("full"))) algoGossipCode = "GOSSIP-FULL"
    else if ((algo.equalsIgnoreCase("gossip")) && (topology.equalsIgnoreCase("2D"))) algoGossipCode = "GOSSIP-2D"
    else if ((algo.equalsIgnoreCase("gossip")) && (topology.equalsIgnoreCase("imp2D"))) algoGossipCode = "GOSSIP-IMP2D"
    else if ((algo.equalsIgnoreCase("gossip")) && (topology.equalsIgnoreCase("line"))) algoGossipCode = "GOSSIP-LINE"
    else if ((algo.equalsIgnoreCase("push-sum")) && (topology.equalsIgnoreCase("full"))) algoGossipCode = "PUSH-SUM-FULL"
    else if ((algo.equalsIgnoreCase("push-sum")) && (topology.equalsIgnoreCase("2D"))) algoGossipCode = "PUSH-SUM-2D"
    else if ((algo.equalsIgnoreCase("push-sum")) && (topology.equalsIgnoreCase("imp2D"))) algoGossipCode = "PUSH-SUM-IMP2D"
    else if ((algo.equalsIgnoreCase("push-sum")) && (topology.equalsIgnoreCase("line"))) algoGossipCode = "PUSH-SUM-LINE"

    algoGossipCode

  }

  def getRevisedActorsNum(topology: String, actualNumActors: Int): Int = {

    var revisedNumActors = actualNumActors
    var sqRootValue: Double = 0;

    if ((topology.equalsIgnoreCase("2D")) || (topology.equalsIgnoreCase("imp2D"))) {
      while ({ sqRootValue = Math.sqrt(revisedNumActors); sqRootValue } != Math.ceil(sqRootValue)) {
        revisedNumActors = revisedNumActors + 1
      }
      println(" ========== ROUNDING TO THE NEAREST PERF. SQ. : " + revisedNumActors)
    }
    revisedNumActors
  }

}