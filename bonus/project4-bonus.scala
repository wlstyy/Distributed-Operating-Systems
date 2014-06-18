import akka.actor.Actor
import akka.actor.Cancellable
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorRef
import scala.math._
import scala.concurrent.duration._
import java.io._
import java.util._

case class NoSuchTopologyException() extends Exception
case object READY
case object LOOP
case object ROUMOR  
case object START
case object BEGIN
case object END
case class WORKERFINISHLOG(actor: String, average: Double)
case object BOSSFINISHLOG

sealed trait LOGACTION
case class BOSSPARA(size: Long, topology: String, algorithm: String) extends LOGACTION
case class BOSSSTART(starttime: Long) extends LOGACTION
case class PBOSSTERMINATE(endtime: Long, average: Double, time: Long, actor: String) extends LOGACTION
case object BOSSBUILDTOPO extends LOGACTION
case class BOSSREADY(readyactor: String) extends LOGACTION
case class BOSSROUMOR(starter: String) extends LOGACTION

case class PWORKERINIT(id: Long, topology: String, size: Long, neighbors: Array[Long], sum: Double) extends LOGACTION
case class PWORKERCONT(counter: Int, x: Double, w: Double, sum: Double, weight: Double, neighbor: AnyVal, sender: String) extends LOGACTION
case class PWORKERTERM(counter: Int, average: Double) extends LOGACTION
case class PWORKERTERMLOG(acotr: String, average: Double) extends LOGACTION

object project4bonus {
  def main(args: Array[String]): Unit = {
    val numOfNodes = args(0).toLong
    val topology = args(1)
    val algorithm = "push-sum"
    var size = numOfNodes
    if (topology.equals("2D") || topology.equals("im2D")) {
      size=pow(ceil(sqrt(size)),2).toLong
    }
    val system = ActorSystem("simulator")
    var boss = system.actorOf(Props[Boss],"Boss")
    boss ! (size,topology,algorithm)
    boss ! START
  }
}

trait Log {
  var date: Date = new Date()	// the suffix of the filename
  
  def logging(filename: String, action: LOGACTION, timestamp: Long): Unit = {
    val writer = new PrintWriter(new FileOutputStream(new File(filename + "." + date.toString), true))
    
    action match {
      case BOSSPARA(size, topology, algorithm) => {
        writer.println("Date: " + date + "\n")
        writer.println("Problem definition ...    [timestamp: " + timestamp + "]") 
        writer.println("size: " + size + "    " + "topology: " + topology + "    " + "algorithm: " + algorithm + "\n")
        writer.close()
      }
      case BOSSBUILDTOPO => {
        writer.println("The boss actor is building the topology and creating the work actors now ...    [timestamp: "+timestamp + "]"+"\n")
        writer.close()
      }
      case BOSSREADY(readyworker) => {
        writer.println("The actor " + readyworker + " has finished initializing and ready to work ...    [timestamp: "+timestamp+"]")
        writer.close()
      }
      case BOSSSTART(starttime) => { 
        writer.println("\n" + "All actor finished intializing and program starts running ...    [timestamp: "+timestamp+"]")
        writer.println("The started system time is " + starttime + "\n")
        writer.close()
      }
      case BOSSROUMOR(starter) => {
        writer.println("The roumor is sent from the actor named " + starter + " ...    [timestamp: " + timestamp + "] \n")
        writer.close()
      }
      case PBOSSTERMINATE(endtime, average, time, actor) => {
        writer.println("Program terminates gracefully and the algorithm convergent ...    [timestamp: " + timestamp + "]")
        writer.println("The terminated actor is " + actor)
        writer.println("The average is " + average)
        writer.println("The ended system time is " + endtime)
        writer.println("The elpsed time is " + time + "\n")
        writer.println("Finished logging for boss actor ...")
        writer.close()
      }
      case PWORKERINIT(id, topology, size, neighbors, sum) => {
        writer.println("Date: " + date + "\n")
        writer.println("The actor " + id + " in the system has been initialized with the parameters listed below ...    [timestamp: " + timestamp + "]")
        writer.println("topology: " + topology + "    size: " + size + "    sum: " + sum)
        writer.println("neighbors: " + neighbors.deep.mkString(" ") + "\n")
        writer.println("This actor is ready to work ..." + "\n")
        writer.close()
      }
      case PWORKERCONT(counter, x, w, sum, weight, neighbor, sender) => {
        writer.println("The " + counter + " time received a roumor, the sender is " + sender + "...    [timestamp: " + timestamp + "]")
        writer.println("received sum: " + x + "    received weight: " + w)
        writer.println("current sum: " + sum + "    current weight: " + weight)
        writer.println("send the roumor to the neighbor actor " + neighbor + "\n")
        writer.close()
      }
      case PWORKERTERM(counter, average) => {
        writer.println("The algorithm convergent in the " + counter + " time received the roumor for this actor ...    [timestamp: " + timestamp + "]")
        writer.println("The average is " + average + "\n")
        writer.println("Finished logging for worker actor ...")
        writer.close()
      }
      case PWORKERTERMLOG(actor, average) => {
        writer.println("The algorithm terminated gracefully ...    [timestamp: " + timestamp + "]")
        writer.println("The terminated actor is " + actor)
        writer.println("The average is " + average + "\n")
        writer.println("Finished logging for worker actor ...")
        writer.close()
      }
    }
  }
}

trait Topology {
  var id: Long
  var topo: String
  var size: Long
  def getNeighbors(): Array[Long] = {
    if (topo.equals("full")) {
      (1l to size).view.filter(_ != id).toArray[Long]
    } else if (topo.equals("line")) {
      Array(id - 1l, id + 1l).filter(x => (x > 0 && x <= size))
    } else if (topo.equals("2D")) {
      var row = Math.sqrt(size).toLong
      var index = id % row
      var ca = Array(id - row, id + row).filter(x => (x > 0 && x <= size))
      var ra = Array[Long]()
      if (index == 0l) { ra = Array(id - 1l) }
      else if (index == 1l) { ra = Array(id + 1l) }
      else { ra = Array(id + 1l, id - 1l) }
      ca ++ ra

    } else if (topo.equals("im2D")) {
      var row = Math.sqrt(size).toLong
      var index = id % row
      var ca = Array(id - row, id + row).filter(x => (x > 0 && x <= size))
      var ra = Array[Long]()
      if (index == 0l) { ra = Array(id - 1l) }
      else if (index == 1l) { ra = Array(id + 1l) }
      else { ra = Array(id + 1l, id - 1l) }
      var nbs = ca ++ ra
      var another = (Math.random() * size).toLong + 1l
      while (nbs.contains(another) || another == id) {
        another = (Math.random() * size).toLong + 1
      }
      nbs ++ Array(another)
    } else {
      throw new NoSuchTopologyException
    }
  }
  
  def getRandomNeighbor(neighbors:Array[Long]) = {
    var random = (Math.random() * neighbors.length).toInt
    try{
      neighbors(random)
    }catch{
      case e:Exception=>{}
    }
  }
}

class PushSumWorker extends Actor with Topology with Log {
  var id = 0l
  var topo = ""
  var size = 0l
  var neighbors = Array[Long]()
  var pushsumCount=0
  var sum = 0.0
  val converging:Array[Boolean]=Array.fill(3)(false)
  var weight = 1.0
  def receive = {
    case (xid: Long, xtopo: String, xsize: Long) =>{	// parent is the boss
      id=xid
      topo=xtopo
      size=xsize
      neighbors=getNeighbors
      sum=id.toDouble
      logging("worker" + self.path.name + ".log", PWORKERINIT(id, topo, size, neighbors, sum), System.currentTimeMillis)
      context.parent ! READY
    }
    case (x: Double, w: Double) => {
      pushsumCount+=1
      converging(pushsumCount%3)=Math.abs(sum/weight-(sum+x)/(weight+w)) < 1e-10
      sum += x
      weight += w
      if(converging forall(_==true)) {
        logging("worker" + self.path.name + ".log", PWORKERTERM(pushsumCount, sum/weight), System.currentTimeMillis)
        context.parent ! (END,sum/weight)
      }
      else {
        var neighbor=getRandomNeighbor(neighbors)
        context.actorSelection("../"+neighbor) ! (sum/2.0,weight/2.0)
        sum /= 2.0
        weight /= 2.0
        logging("worker" + self.path.name + ".log", PWORKERCONT(pushsumCount, x, w, sum, weight, neighbor, sender.path.name), System.currentTimeMillis)
      }
    }
    case WORKERFINISHLOG(actor, average) => {
      logging("worker" + self.path.name + ".log", PWORKERTERMLOG(actor, average), System.currentTimeMillis)
      context.parent ! BOSSFINISHLOG
    }

  }
}

class Boss extends Actor with Log{
  var start = 0l
  var end = 0l
  var count = 0l
  var size=0l
  var topo=""
  var algorithm=""
  var readyCount=0l
  var logcounter = 0

  def receive = {
    case (xsize:Long,xtopo:String,xalgorithm:String) =>{
      size=xsize
      topo=xtopo
      algorithm=xalgorithm
      logging("boss.log", BOSSPARA(xsize, xtopo, xalgorithm), System.currentTimeMillis)
    }
    case START => {
      buildTopo
    }
    case READY =>{
      readyCount+=1
      logging("boss.log", BOSSREADY(sender.path.toString), System.currentTimeMillis)
      if(readyCount==size){
        start = System.currentTimeMillis
        logging("boss.log", BOSSSTART(start), System.currentTimeMillis)
        startProto
      }
    }
    case (END,avg:Double) => {
      end = System.currentTimeMillis
      println("TIME: "+(end-start))
      println("Average value: "+avg)
      logging("boss.log", PBOSSTERMINATE(end, avg, end-start, sender.path.name), System.currentTimeMillis)

      var sendter: Array[Long] = (1l until size+1l).toArray.filter(x => x!=sender.path.name.toLong)
      for(i <- sendter) {
        context.actorSelection(i.toString) ! WORKERFINISHLOG(sender.path.name, avg)
      }
    }
    case BOSSFINISHLOG => {
      logcounter += 1
      if(logcounter == (size-1)) {
        System.exit(0)
      }
    }
  }
  def buildTopo = {
    logging("boss.log", BOSSBUILDTOPO, System.currentTimeMillis)
    var i = 1l
    if(algorithm.equals("push-sum")){
        for (i <- 1l to size) {
    		var worker = context.actorOf(Props[PushSumWorker], i.toString)
    		worker ! (i,topo,size)
    	}
    }
  }
  def startProto() = {
    var starter = (Math.random*size+1l).toLong.toString
    if (algorithm.equals("gossip"))
      context.child(1l.toString).get ! ROUMOR
    else if (algorithm.equals("push-sum")) {
      context.child(starter).get ! (0.0, 0.0)
    }
    logging("boss.log", BOSSROUMOR(starter), System.currentTimeMillis)
  }
}
