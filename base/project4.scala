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
case object START
case object BEGIN
case object END
case class WORKERFINISHLOG(actor: String, average: Double)
case object BOSSFINISHLOG


object project4 {
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


class PushSumWorker extends Actor with Topology {
  var id = 0l
  var topo = ""
  var size = 0l
  var neighbors = Array[Long]()
  var pushsumCount=0
  var sum = 0.0
  val converging:Array[Boolean]=Array.fill(3)(false)
  var weight = 1.0
  val logger=new actorlog("worker" + self.path.name + ".log");
  def receive = {
    case (xid: Long, xtopo: String, xsize: Long) =>{	// parent is the boss
      id=xid
      topo=xtopo
      size=xsize
      neighbors=getNeighbors
      sum=id.toDouble
      logger.log(PWORKERINIT(id, topo, size, neighbors, sum), System.currentTimeMillis)
      context.parent ! READY
    }
    case (x: Double, w: Double) => {
      pushsumCount+=1
      converging(pushsumCount%3)=Math.abs(sum/weight-(sum+x)/(weight+w)) < 1e-10
      sum += x
      weight += w
      if(converging forall(_==true)) {
        logger.log(PWORKERTERM(pushsumCount, sum/weight), System.currentTimeMillis)
        context.parent ! (END,sum/weight)
      }
      else {
        var neighbor=getRandomNeighbor(neighbors)
        context.actorSelection("../"+neighbor) ! (sum/2.0,weight/2.0)
        sum /= 2.0
        weight /= 2.0
        logger.log(PWORKERCONT(pushsumCount, x, w, sum, weight, neighbor, sender.path.name), System.currentTimeMillis)
      }
    }
    case WORKERFINISHLOG(actor, average) => {
      logger.log(PWORKERTERMLOG(actor, average), System.currentTimeMillis)
      logger.exit
      context.parent ! BOSSFINISHLOG
    }

  }
}

class Boss extends Actor{
  var start = 0l
  var end = 0l
  var count = 0l
  var size=0l
  var topo=""
  var algorithm=""
  var readyCount=0l
  var logcounter = 0
  val logger=new actorlog("boss.log")
  

  def receive = {
    case (xsize:Long,xtopo:String,xalgorithm:String) =>{
      size=xsize
      topo=xtopo
      algorithm=xalgorithm
      logger.log(BOSSPARA(xsize, xtopo, xalgorithm), System.currentTimeMillis)
    }
    case START => {
      buildTopo
    }
    case READY =>{
      readyCount+=1
      logger.log(BOSSREADY(sender.path.toString), System.currentTimeMillis)
      if(readyCount==size){
        start = System.currentTimeMillis
        logger.log(BOSSSTART(start), System.currentTimeMillis)
        startProto
      }
    }
    case (END,avg:Double) => {
      end = System.currentTimeMillis
      println("TIME: "+(end-start))
      println("Average value: "+avg)
      logger.log(PBOSSTERMINATE(end, avg, end-start, sender.path.name), System.currentTimeMillis)

      var sendter: Array[Long] = (1l until size+1l).toArray.filter(x => x!=sender.path.name.toLong)
      for(i <- sendter) {
        context.actorSelection(i.toString) ! WORKERFINISHLOG(sender.path.name, avg)
      }
    }
    case END => {
      count += 1
     
      if (count == size) {
        end = System.currentTimeMillis
        println("TIME: " + (end - start))
        logger.log(GBOSSTERMINATE(end, end-start, sender.path.name), System.currentTimeMillis)
	System.exit(0)
      }
    }
    case BOSSFINISHLOG => {
      logcounter += 1
      if(logcounter == (size-1)) {
        logger.exit
        System.exit(0)
      }
    }
  }
  def buildTopo = {
    logger.log(BOSSBUILDTOPO, System.currentTimeMillis)
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
    if (algorithm.equals("push-sum")) {
      context.child(starter).get ! (0.0, 0.0)
    }
    logger.log(BOSSROUMOR(starter), System.currentTimeMillis)
  }
}
