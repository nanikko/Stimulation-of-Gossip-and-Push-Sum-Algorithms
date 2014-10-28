import scala.math
import akka.actor.Props
import akka.actor._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;

class Implementation(numActors: Int, algoTopoCode: String, numStartNodes: Int, msgCount: Int, context: ActorContext, masterActor: ActorRef) {

  import context.dispatcher

  var workers = new Array[ActorRef](numActors)
  //var adjacentWorkers: ArrayBuffer[Int] = null //adjacent actor refs for each of the workers. This varies with topology.

  def handleGossipLine(algo: String) = {

    println(" ========== IN THE GOSSIP LINE HANDLER ================ " + numActors)
    var leftWorkerIndex: Int = 0
    var rightWorkerIndex: Int = 0

    for (i <- 0 until numActors) {

      var adjacentWorkers = new ArrayBuffer[Int]()

      leftWorkerIndex = i - 1
      rightWorkerIndex = i + 1

      if ((leftWorkerIndex >= 0)) {
        adjacentWorkers.append(leftWorkerIndex) //Appending to the adjancency list of this worker 
      }

      if (rightWorkerIndex < workers.length) {
        adjacentWorkers.append(rightWorkerIndex)
      }

      workers(i) = context.actorOf(Props(new Worker(numActors, msgCount, adjacentWorkers, workers, masterActor, i, context)), name = "worker" + i) // last param in the constructer is the base sum in push sum algo

    }

    spreadRumour(algo, numStartNodes) // FINALLY IMPLEMENTING THE ALGO ON THE ADJANCENCY MATRIX.

  }

  def handleGossip2D(algo: String) = {

    println(" ========== IN THE GOSSIP 2D HANDLER ================ " + numActors)

    var workersPerRow: Int = math.sqrt(numActors).toInt
    var leftColumnIndex: Int = 0
    var rightColumnIndex: Int = 0
    var topRowIndex: Int = 0
    var bottomRowIndex: Int = 0
    var currentWorkerPosition: Int = 0
    var leftWorkerPosition: Int = 0 //Left worker position shows the index of the worker when represented as a one dimensional array
    var rightWorkerPosition: Int = 0
    var topWorkerPosition: Int = 0
    var bottomWorkerPosition: Int = 0

    for (rowIndex <- 0 until workersPerRow) {
      for (columnIndex <- 0 until workersPerRow) {
        var adjacentWorkers = new ArrayBuffer[Int]()

        currentWorkerPosition = rowIndex * workersPerRow + columnIndex
        leftColumnIndex = columnIndex - 1
        rightColumnIndex = columnIndex + 1
        topRowIndex = rowIndex - 1
        bottomRowIndex = rowIndex + 1

        if ((leftColumnIndex >= 0)) {
          leftWorkerPosition = workersPerRow * rowIndex + leftColumnIndex
          adjacentWorkers.append(leftWorkerPosition) //Appending to the adjancency list of this worker.
        }

        if (rightColumnIndex < workersPerRow) {
          rightWorkerPosition = workersPerRow * rowIndex + rightColumnIndex
          adjacentWorkers.append(rightWorkerPosition)
        }

        if ((topRowIndex >= 0)) {
          topWorkerPosition = columnIndex + workersPerRow * topRowIndex
          adjacentWorkers.append(topWorkerPosition)
        }

        if (bottomRowIndex < workersPerRow) {
          bottomWorkerPosition = columnIndex + workersPerRow * bottomRowIndex
          adjacentWorkers.append(bottomWorkerPosition)
        }

        workers(currentWorkerPosition) = context.actorOf(Props(new Worker(numActors, msgCount, adjacentWorkers, workers, masterActor, currentWorkerPosition, context)), name = "worker" + (currentWorkerPosition))
      }
    }

    spreadRumour(algo, numStartNodes) // FINALLY IMPLEMENTING THE ALGO ON THE ADJANCENCY MATRIX.

  }

  def handleGossip2DImp(algo: String) = {

    println(" ========== IN THE GOSSIP 2D IMP HANDLER ================ " + numActors)

    var workersPerRow: Int = math.sqrt(numActors).toInt
    var leftColumnIndex: Int = 0
    var rightColumnIndex: Int = 0
    var topRowIndex: Int = 0
    var bottomRowIndex: Int = 0
    var currentWorkerPosition: Int = 0
    var leftWorkerPosition: Int = 0
    var rightWorkerPosition: Int = 0
    var topWorkerPosition: Int = 0
    var bottomWorkerPosition: Int = 0

    for (rowIndex <- 0 until workersPerRow) {
      for (columnIndex <- 0 until workersPerRow) {
        var adjacentWorkers = new ArrayBuffer[Int]()

        currentWorkerPosition = rowIndex * workersPerRow + columnIndex
        leftColumnIndex = columnIndex - 1
        rightColumnIndex = columnIndex + 1
        topRowIndex = rowIndex - 1
        bottomRowIndex = rowIndex + 1

        if ((leftColumnIndex >= 0)) {
          leftWorkerPosition = workersPerRow * rowIndex + leftColumnIndex
          adjacentWorkers.append(leftWorkerPosition) //Appending to the adjancency list of this worker.
        }

        if (rightColumnIndex < workersPerRow) {
          rightWorkerPosition = workersPerRow * rowIndex + rightColumnIndex
          adjacentWorkers.append(rightWorkerPosition)
        }

        if ((topRowIndex >= 0)) {
          topWorkerPosition = columnIndex + workersPerRow * topRowIndex
          adjacentWorkers.append(topWorkerPosition)
        }

        if (bottomRowIndex < workersPerRow) {
          bottomWorkerPosition = columnIndex + workersPerRow * bottomRowIndex
          adjacentWorkers.append(bottomWorkerPosition)
        }

        var randomIndex: Int = new Random().nextInt(numActors - 1) // -1 is added as we are working on indexes.

        while (randomIndex == leftWorkerPosition || randomIndex == rightWorkerPosition || randomIndex == topWorkerPosition || randomIndex == bottomWorkerPosition || randomIndex == currentWorkerPosition) {
          randomIndex = new Random().nextInt(numActors - 1)
        }

        adjacentWorkers.append(randomIndex) //appending the random node.

        workers(currentWorkerPosition) = context.actorOf(Props(new Worker(numActors, msgCount, adjacentWorkers, workers, masterActor, currentWorkerPosition, context)), name = "worker" + (currentWorkerPosition))
      }
    }

    spreadRumour(algo, numStartNodes) // FINALLY IMPLEMENTING THE ALGO ON THE ADJANCENCY MATRIX.

  }

  def handleGossipFull(algo: String) = {

    println(" ========== IN THE GOSSIP FULL HANDLER ================ " + numActors)

    for (i <- 0 until numActors) {

      //For each actors, we are adding the other 'n-1' into the adjacency list.

      var adjacentWorkers = new ArrayBuffer[Int]()
      var workersIndex: Int = 0

      while (workersIndex < numActors) {
        if (i != workersIndex) { //since it cannot be the same actor as self
          adjacentWorkers.append(workersIndex) //Appending the index of the worker to the List.
        }
        workersIndex = workersIndex + 1
      }

      workers(i) = context.actorOf(Props(new Worker(numActors, msgCount, adjacentWorkers, workers, masterActor, i, context)), name = "worker" + i)

    }

    spreadRumour(algo, numStartNodes) // FINALLY IMPLEMENTING THE ALGO ON THE ADJANCENCY MATRIX.

  }

  /*FUNCTION TO SELECT RANDOM NODES IN CASE WE WISH TO START 
   * THE RUMOUR WITH MORE THAN ONE NODE.*/

  def getRandomStartNodes(numOfStartingNodes: Int): ArrayBuffer[Int] = {
    var randIndexesToStart = new ArrayBuffer[Int]
    var i: Int = 0
    while (i < numStartNodes) {
      var rand = new Random()
      var randIndex: Int = rand.nextInt(numActors)
      if (!randIndexesToStart.contains(randIndex)) {
        //println(" ========== ADDING TO THE STARTING NODES: " + randIndex)
        randIndexesToStart.append(randIndex)
        i += 1
      }
    }

    println(" ========== RANDOM NODES SELECTED ======= " + randIndexesToStart)
    randIndexesToStart

  }

  /*DEPENDING ON THE ALGO CHOSEN, WE CHOOSE APPROPRIATE
   *  IMPLEMENTATION ON THE ADJACENT MATRIX*/

  def spreadRumour(algo: String, numStartNodes: Int) {
    var randIndexesToStart = getRandomStartNodes(numStartNodes)

    Master.startTime = System.currentTimeMillis()

    if (algo == Master.GOSSIP_CODE) { // Gossip algo case
      for (i <- 0 until randIndexesToStart.length) {
        workers(i) ! "rumour"
      }
    } else {
      for (i <- 0 until randIndexesToStart.length) {
        workers(i) ! pushSumRumour(0, 1)
      }

    }
  }

}