
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import java.security.MessageDigest
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random

object Project3 {

  sealed trait ChordSimulation1

  class finger_table_entry(var start: Int, var node: ActorRef)

  case class Start_Network_formation(numNodes: Int, numRequests: Int) extends ChordSimulation1

  case class initialize() extends ChordSimulation1

  case class join_chord() extends ChordSimulation1

  case class find_successor_complete(s: ActorRef, finger_table_entry_no: Int) extends ChordSimulation1

  case class set_my_predecessor() extends ChordSimulation1

  case class closest_preceding_finger(id:Int,node_id:Int,finger_table_entry_no:Int,type_req:String) extends ChordSimulation1

  //case class prepare_successor(node_id:Int,finger_table_entry_no:Int)  extends ChordSimulation1
  case class found_predecessor_for_updating(s:ActorRef,i:Int) extends ChordSimulation1

  case class set_predecessor(s:ActorRef)  extends ChordSimulation1

  case class find_successor_for(id: Int, node_id:Int, finger_table_entry_no: Int,type_req:String) extends ChordSimulation1

  case class update_finger_table(s:ActorRef,i:Int) extends ChordSimulation1

  case class Initialization_done() extends ChordSimulation1

  case class process_complete() extends ChordSimulation1

  case object StartRouting extends ChordSimulation1

  case class Route(key:Int, hop: Int) extends ChordSimulation1

  case class Request_received(hop: Int) extends ChordSimulation1

  var start_node: ActorRef = null
  var m:Int = 1



  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("ERROR: No input given, Please give input as <numNodes> <numRequests>" + "\n" + "Exiting the program")
      System.exit(1)
    }
    else if (args.length < 2) {
      println("ERROR: No of arguments less than 2, Please give input as <numNodes> <numRequests>" + "\n" + "Exiting the program")
      System.exit(1)
    }
    val numNodes = args(0).toInt
    val numRequests = args(1).toInt
    val system = ActorSystem("Project3")
    val master = system.actorOf(Props[Master], name = "master")
    master ! Start_Network_formation(numNodes, numRequests)
  }

  class Master extends Actor {
    var network_num_nodes: Int = 0
    var network_num_requests: Int = 0
    var sha_input: Int = 1
    //var m: Int = 1
    var flag: Boolean = true
    var node_list = new ListBuffer[ActorRef]
    var decimal_list = new ListBuffer[Int]
    //var key_list = new ListBuffer[Int]
    var i: Int = 0
    var count:Int = 0
    var request_till_now = 0
    var total_number_hops = 0

    def receive = {
      case Start_Network_formation(numNodes, numRequests) =>
        network_num_nodes = numNodes
        network_num_requests = numRequests
        while (flag == true) {
          if (math.pow(2, m) >= numNodes) {
            flag = false
          }
          else {
            m += 1
          }
        }

        println("Initialization of network started")
        //network_num_nodes = math.pow(2,m-1).toInt
        //("Value of m:"+m)
        /*
        for (i <- 0 until numRequests) {
          //println(i)
          var key_returned_string = hex_decimal(m)
          var key_returned_int = key_returned_string.toInt

          while (key_list.contains(key_returned_int)) {
            key_returned_string = hex_decimal(m)
            key_returned_int = key_returned_string.toInt
          }

          key_list += key_returned_int
        }
        */

        //Generate SHA-1 for each actor
        for (i <- 0 until network_num_nodes) {
          //println(i)
          var decimal_returned_string = hex_decimal(m)
          var decimal_returned_int = decimal_returned_string.toInt

          while (decimal_list.contains(decimal_returned_int)) {
            decimal_returned_string = hex_decimal(m)
            decimal_returned_int = decimal_returned_string.toInt
          }

          decimal_list += decimal_returned_int


          context.actorOf(Props(new ChordNode(network_num_nodes, network_num_requests, m)), decimal_returned_string)
          //println("Initialization of network started")
          context.actorSelection(decimal_returned_string) ! join_chord()


          //Thread.sleep(10000)
        }

      //println("node list length:" + node_list.length)
      //decimal_list.sorted
      //println("decimal list length" + decimal_list.length)
      //Sorted list to be kept for reference
      //decimal_list = decimal_list.sortWith(_<_)
      //println("decimal list length" + decimal_list)

      //val string_name: String = decimal_list(0).toString
      //context.actorSelection(string_name) ! join_chord()
      //Thread.sleep(10000)

      case Initialization_done() => {
        count += 1
        if (count == network_num_nodes - 1) {
          println("Initialization of network complete")
          println("Routing started")
          for (i <- 0 until network_num_nodes) {
            context.actorSelection(decimal_list(i).toString()) ! StartRouting
          }
        }
      }
      case Request_received(h) =>
       // println("request_till_now"+ request_till_now)
        request_till_now += 1
        total_number_hops += h
        if (request_till_now >= (network_num_nodes * network_num_requests)) {
          println("Routing complete")
          var average_hop = total_number_hops.toDouble / (network_num_nodes * network_num_requests)
          println("Total no of hops = " + total_number_hops)
          println("Total no of requests = " + request_till_now)
          println("Average no of hops = " + average_hop)
          println("Shutting system down...Exit")
          context.system.shutdown
          System.exit(0)
        }
    }
  }

  def hex_Digest(s: String): String = {
    val sha = MessageDigest.getInstance("SHA-1")
    sha.digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  def hex_decimal(m: Int): String = {
    val temp = Random.nextInt(Integer.MAX_VALUE).toString
    val temp1 = hex_Digest(temp)
    val s: String = BigInt(temp1, 16).toString(2)
    val string1: String = s.substring(1, m + 1)
    //println("output :" + string1)
    val string2 = BigInt(string1, 2).toString(10)
    return string2
  }

  class finger_table_row(var start: Int, var node: ActorRef)


  class ChordNode(num_Nodes: Int, num_Requests: Int, m: Int) extends Actor {

    var my_finger_table = new ListBuffer[finger_table_row]
    var node_id: Int = self.path.name.toInt
    var predecessor: ActorRef = null
    var count:Int = 0
    var count_num_req:Int = 0
    var key:Int = self.path.name.toInt


    override def preStart(): Unit = {
      //Initialize all finger table entries with self and set the predecessor as well
      for (i <- 1 to m) {
        val finger_entry_start: Int = (((this.node_id.toInt) + math.pow(2, i - 1)) % math.pow(2, m)).toInt
        var finger_entry = new finger_table_row(finger_entry_start, self)
        this.my_finger_table += finger_entry
        //println((i-1)+". start: "+my_finger_table(i-1).start)
      }
      this.predecessor = self
      //Part to be commented later
      /*
      println("My node_id is" +node_id)
      println("My intialization started")
      for (i<- 0 to m-1){
        println(i+ "finger table entry start is: "+ this.my_finger_table(i).start)
        println(i+ "My node is " + this.my_finger_table(i).node.path.name)
      }
      println("My predecessor should be same as me :" + this.predecessor.path.name)
      println("My intialization ended")
      */

    }

    def check_interval(id:Int,lower_bound:Int,upper_bound:Int,i_left:Int,i_right:Int): Boolean = {
      if (lower_bound > upper_bound) {
        if (i_left == 1 && i_right == 0) {
          if (id >= lower_bound && id <= math.pow(2, m) || id >= 0 && id < upper_bound) {
            return true
          }
          else {
            return false
          }
        }
        else if (i_left == 0 && i_right == 1) {
          if (id > lower_bound && id <= math.pow(2, m) || id >= 0 && id <= upper_bound) {
            return true
          }
          else {
            return false
          }
        }
        else if (i_left == 0 && i_right == 0) {
          if (id > lower_bound && id <= math.pow(2, m) || id >= 0 && id < upper_bound) {
            return true
          }
          else {
            return false
          }
        }
        else {
          if (id >= lower_bound && id <= math.pow(2, m) || id >= 0 && id <= upper_bound) {
            return true
          }
          else {
            return false
          }
        }
      } else if (upper_bound > lower_bound){
        if (i_left == 1 && i_right == 0) {
          if (id >= lower_bound && id < upper_bound) {
            return true
          }
          else {
            return false
          }
        }
        else if (i_left == 0 && i_right == 1) {
          if (id > lower_bound && id <= upper_bound) {
            return true
          }
          else {
            return false
          }
        }
        else if (i_left == 0 && i_right == 0) {
          if (id > lower_bound && id < upper_bound) {
            return true
          }
          else {
            return false
          }
        }
        else {
          if (id >= lower_bound && id <= upper_bound) {
            return true
          }
          else {
            return false
          }
        }
      }
      else {
        return true
      }
    }

    def find_my_successor(): ActorRef = {
      return this.my_finger_table(0).node
    }

    def find_my_node_id(): Int = {
      return this.node_id
    }

    def update_others() = {
      for (i<-1 to m){
        var temp5 = (this.node_id - math.pow(2,i-1).toInt)
        //println("Temp is"+temp5)
        if (temp5 < 0){
          temp5 = math.pow(2,m).toInt + temp5
          //println("Temp is"+temp5)
        }
        self ! find_successor_for(temp5,this.node_id,i, "update")

      }
      self ! process_complete()
    }

    def update_finger_table(n:ActorRef,i:Int) = {
      val temp1 = n.path.name.toInt
      val temp = this.my_finger_table(i-1).node.path.name.toInt
      //check_interval(temp1,this.node_id,temp,1,0)
      if(check_interval(temp1,this.node_id,temp,1,0)) {
        this.my_finger_table(i-1).node = n
        //context.actorSelection("../"+ this.predecessor.path.name) ! found_predecessor_for_updating(n:ActorRef,i:Int)
      }
      context.actorSelection("../"+ this.predecessor.path.name) ! found_predecessor_for_updating(n:ActorRef,i:Int)
    }

    def count_complete() = {
      if (count == m) {
        //println("Entered here")
        //println("Hello")
        //System.exit(0)
        update_others()
      }
    }


    def init_finger_table (finger_table_entry_no:Int): Unit = {
      context.actorSelection("../" +this.my_finger_table(finger_table_entry_no).node.path.name) ! set_my_predecessor()
      //println(this.node_id+" has predecessor as"+this.predecessor.path.name)
      for (i <- 1 to m - 1) {
        val temp = this.my_finger_table(i-1).node.path.name.toInt
        //check_interval(temp,this.node_id,temp,1,0)
        val temp2 = this.my_finger_table(i).start
        if (check_interval(temp2,this.node_id,temp,1,0)) {
          this.my_finger_table(i).node = this.my_finger_table(i-1).node
          count += 1
        }
        else {
          start_node ! find_successor_for(this.my_finger_table(i).start, find_my_node_id(),i,"successor")
        }
        count_complete()
      }
      //count_complete()
    }


    def receive = {
      case join_chord() => {
        if (start_node != null) {
          //println("Join chord called should be 2")
          //println("Join chord is called on start_node by node" + this.node_id)
          //System.exit(0)
          start_node ! find_successor_for(this.my_finger_table(0).start, find_my_node_id(), 0, "successor")

        }
        else {
          start_node = self
          //println("Start node is" + start_node.path.name)
          //println("start node should be 1")
        }
      }

      case find_successor_complete(s: ActorRef, finger_table_entry_no: Int) => {
        this.my_finger_table(finger_table_entry_no).node = s
        //println("Finger table entry is " + finger_table_entry_no)
        //println(this.node_id + "Successor for me is" + this.my_finger_table(finger_table_entry_no).node.path.name)
        //System.exit(0)
        count += 1
        //println("Count is " + count)

        if (finger_table_entry_no == 0) {
          init_finger_table(0)
        }
        count_complete()
      }

      /*
      println(this.node_id+ "has successor set as " +this.my_finger_table(finger_table_entry_no).node.path.name)
      context.actorSelection("../" +this.my_finger_table(finger_table_entry_no).node.path.name) ! set_my_predecessor()
      //update_my_other_tuples
      for (i <- 1 to m - 1) {
        val temp = this.my_finger_table(i-1).node.path.name.toInt
        //check_interval(temp,this.node_id,temp,1,0)
        val temp2 = this.my_finger_table(i).start
        if (check_interval(temp2,this.node_id,temp,1,0)) {
          this.my_finger_table(i).node = this.my_finger_table(i-1).node
        }
        else {
          start_node ! find_successor_for(this.my_finger_table(i).start, find_my_node_id(),i,"successor")
        }
      }
      */


      case set_my_predecessor() => {
        //println("S is" + sender.path.name)
        //println("Myself" + self.path.name)
        sender ! set_predecessor(this.predecessor)
        this.predecessor = sender
        //println(this.node_id + " has predecessor as" + this.predecessor.path.name)
        //System.exit(0)
      }


      case set_predecessor(s: ActorRef) => {
        this.predecessor = s
        //println(this.node_id + " has predecessor as" + this.predecessor.path.name)
      }

      case find_successor_for(id: Int, node_id: Int, finger_table_entry_no: Int, type_req: String) => {
        //find predecessor
        val successor_id = this.find_my_successor().path.name.toInt
        //val flag1: Boolean = (id >= this.node_id && id < successor_id)
        //println("My successor is" + successor_id)

        val flag1: Boolean = check_interval(id, this.node_id, successor_id, 0, 1)
        //("flag value"+flag1)
        //System.exit(0)
        if (!flag1) {
          //println("Entered closest preceding loop")
          //System.exit(0)
          self ! closest_preceding_finger(id, node_id, finger_table_entry_no, type_req)
        } else {
          if (type_req == "successor") {
            //println("Found success for that finger table entry is completed")
            //println("My successor is" + find_my_successor().path.name)
            context.actorSelection("../" + node_id.toString) ! find_successor_complete(find_my_successor(), finger_table_entry_no)
          }
          else {
            //println("I am in update")
            context.actorSelection("../" + node_id.toString) ! found_predecessor_for_updating(self, finger_table_entry_no)
          }
        }
      }
      /*
      case prepare_successor(node_id:Int,finger_table_entry_no:Int) => {
        context.actorSelection("../" + node_id) ! find_successor_complete(find_my_successor(),finger_table_entry_no)
      }
      */
      case found_predecessor_for_updating(s: ActorRef, i: Int) => {
        context.actorSelection("../" + s.path.name) ! update_finger_table(self, i)
      }

      case update_finger_table(s: ActorRef, i: Int) => {
        update_finger_table(s, i)
      }

      case closest_preceding_finger(id: Int, node_id: Int, finger_table_entry_no: Int, type_req: String) => {
        for (i <- m - 1 to 0) {
          val temp = this.my_finger_table(i).node.path.name.toInt
          //check_interval(temp,this.node_id,id,0,0)

          if (check_interval(temp, this.node_id, id, 0, 0)) {

            context.actorSelection("../" + this.my_finger_table(i).node.path.name) ! find_successor_for(id, node_id, finger_table_entry_no, type_req)
          }
        }
        //self ! find_successor_for(id,node_id,finger_table_entry_no,type_req)
      }

      case process_complete() =>
        /*
        println("My node_id is" + this.node_id)
        for (i <- 0 to m - 1) {
          println(i + "finger table entry start is: " + this.my_finger_table(i).start)
          println(i + "My node is " + this.my_finger_table(i).node.path.name)
        }
        println("My predecessor should be same as me :" + this.predecessor.path.name)

        //println("My predecessor should be same as me :" + this.predecessor.path.name)
        */
        context.actorSelection("../") ! Initialization_done()

      case StartRouting =>
        //println("HeLLLLLLLLLLLLLLLLLLLLLLLLLLLo")
        //println("Started Routing")
        val rand = Random.nextInt(num_Nodes)
        import context.dispatcher
        context.system.scheduler.schedule(0 milliseconds,1000 milliseconds,self, Route(rand,0))

      case Route(key_hash, hop) =>

        if (this.key == key_hash) {
          //println("this key" + this.key )
          //println("key is" + key_hash )
          //println("equal")
          context.actorSelection("../") ! Request_received(hop)
        }
        else {
          for (i <- m - 1 until 0) {
            if (key_hash == this.my_finger_table(i).node.path.name.toInt) {
              //println("equal in ft")
              context.actorSelection("../" + (this.my_finger_table(i).node.path.name)) ! Route(key, hop + 1) //node to which it matches
            }
          }
          if (key_hash > this.my_finger_table(m - 1).node.path.name.toInt) {
            //println("greater" + this.my_finger_table(m - 1).node.path.name.toInt)
            //println("key" + key.toInt)
            context.actorSelection("../" + (this.my_finger_table(m - 1).node.path.name)) ! Route(key, hop + 1)
          }

          if (key_hash < (this.my_finger_table(0).node.path.name.toInt)) {
            //println("less")
            context.actorSelection("../" + this.my_finger_table(0).node.path.name) ! Route(key, hop + 1)

          }

        }
    }
  }
}
  
