import java.io.File
import java.io.PrintWriter
import java.util._
sealed trait LOGACTION
case class BOSSPARA(size: Long, topology: String, algorithm: String) extends LOGACTION
case class BOSSSTART(starttime: Long) extends LOGACTION
case class PBOSSTERMINATE(endtime: Long, average: Double, time: Long, actor: String) extends LOGACTION
case class GBOSSTERMINATE(endtime: Long, time: Long, actor: String) extends LOGACTION
case object BOSSBUILDTOPO extends LOGACTION
case class BOSSREADY(readyactor: String) extends LOGACTION
case class BOSSROUMOR(starter: String) extends LOGACTION

case class PWORKERINIT(id: Long, topology: String, size: Long, neighbors: Array[Long], sum: Double) extends LOGACTION
case class PWORKERCONT(counter: Int, x: Double, w: Double, sum: Double, weight: Double, neighbor: AnyVal, sender: String) extends LOGACTION
case class PWORKERTERM(counter: Int, average: Double) extends LOGACTION
case class PWORKERTERMLOG(acotr: String, average: Double) extends LOGACTION
case class GWORKERINIT(id: Long, topology: String, size: Long, neighbors: Array[Long]) extends LOGACTION

class actorlog(filename:String) {
  var date: Date = new Date()
  val writer= new PrintWriter(new File(filename+"."+date.toString))
	def log(action: LOGACTION, timestamp: Long)={
		action match {
      case BOSSPARA(size, topology, algorithm) => {
        writer.println("Date: " + date + "\n")
        writer.println("Problem definition ...    [timestamp: " + timestamp + "]") 
        writer.println("size: " + size + "    " + "topology: " + topology + "    " + "algorithm: " + algorithm + "\n")
      }
      case BOSSBUILDTOPO => {
        writer.println("The boss actor is building the topology and creating the work actors now ...    [timestamp: "+timestamp + "]"+"\n")
      }
      case BOSSREADY(readyworker) => {
        writer.println("The actor " + readyworker + " has finished initializing and ready to work ...    [timestamp: "+timestamp+"]")
      }
      case BOSSSTART(starttime) => { 
        writer.println("\n" + "All actor finished intializing and program starts running ...    [timestamp: "+timestamp+"]")
        writer.println("The started system time is " + starttime + "\n")
      }
      case BOSSROUMOR(starter) => {
        writer.println("The roumor is sent from the actor named " + starter + " ...    [timestamp: " + timestamp + "] \n")
      }
      case PBOSSTERMINATE(endtime, average, time, actor) => {
        writer.println("Program terminates gracefully and the algorithm convergent ...    [timestamp: " + timestamp + "]")
        writer.println("The terminated actor is " + actor)
        writer.println("The average is " + average)
        writer.println("The ended system time is " + endtime)
        writer.println("The elpsed time is " + time + "\n")
        writer.println("Finished logging for boss actor ...")
      }
      case GBOSSTERMINATE(endtime, time, actor) => {
        writer.println("Program terminates gracefully and the algorithm convergent ...    [timestamp: " + timestamp + "]")
        writer.println("The terminated actor is " + actor)
        writer.println("The ended system time is " + endtime)
        writer.println("The elpsed time is " + time + "\n")
        writer.println("Finished logging for boss actor ...")
      }

      case PWORKERINIT(id, topology, size, neighbors, sum) => {
        writer.println("Date: " + date + "\n")
        writer.println("The actor " + id + " in the system has been initialized with the parameters listed below ...    [timestamp: " + timestamp + "]")
        writer.println("topology: " + topology + "    size: " + size + "    sum: " + sum)
        writer.println("neighbors: " + neighbors.deep.mkString(" ") + "\n")
        writer.println("This actor is ready to work ..." + "\n")
      }
      case PWORKERCONT(counter, x, w, sum, weight, neighbor, sender) => {
        writer.println("The " + counter + " time received a roumor, the sender is " + sender + "...    [timestamp: " + timestamp + "]")
        writer.println("received sum: " + x + "    received weight: " + w)
        writer.println("current sum: " + sum + "    current weight: " + weight)
        writer.println("send the roumor to the neighbor actor " + neighbor + "\n")
      }
      case PWORKERTERM(counter, average) => {
        writer.println("The algorithm convergent in the " + counter + " time received the roumor for this actor ...    [timestamp: " + timestamp + "]")
        writer.println("The average is " + average + "\n")
        writer.println("Finished logging for worker actor ...")
      }
      case PWORKERTERMLOG(actor, average) => {
        writer.println("The algorithm terminated gracefully ...    [timestamp: " + timestamp + "]")
        writer.println("The terminated actor is " + actor)
        writer.println("The average is " + average + "\n")
        writer.println("Finished logging for worker actor ...")
      }

      case GWORKERINIT(id, topology, size, neighbors) => {
        writer.println("Date: " + date + "\n")
        writer.println("The actor " + id + " in the system has been initialized with the parameters listed below ...    [timestamp: " + timestamp + "]")
        writer.println("topology: " + topology + "    size: " + size)
        writer.println("neighbors: " + neighbors.deep.mkString(" ") + "\n")
        writer.println("This actor is ready to work ..." + "\n")
      }

    }
	}
	def exit={
	  writer.close()
	}
}