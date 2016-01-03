import scala.collection.mutable.ArrayBuffer
import scala.math._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala

import scala.util.Random
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

/**
 * Created by canara on 10/3/2015.
 */
object project2 extends App{

  var numOfNodes:Int =args(0).toInt
  var topology:String =args(1)
  var algorithm:String =args(2)
  val system=ActorSystem("NodeActor")
  val protcolFunc=system.actorOf(Props[protcolFunctions],name="protcolFunc")
  if(algorithm.equalsIgnoreCase("gossip")){
    var message = "I started the fire"
    protcolFunc ! initiateGossip(numOfNodes, topology, message, protcolFunc)
  }
  else if(algorithm.equalsIgnoreCase("push sum")){
    protcolFunc ! initiatePushSum(numOfNodes,topology,protcolFunc)
  }
}
class protcolFunctions extends Actor{
  val system=ActorSystem("Node")
  var nodeList:List[ActorRef]=Nil
  var nodeCount:Int=0
  var convergenceCount:Int=0
  var startTime= System.currentTimeMillis
  var nodePattern:ArrayBuffer[String] = ArrayBuffer[String]()

  def receive={

    case initiatePushSum(numOfNodes,topology,protcolFunc) =>{
      initializeNodes(topology,numOfNodes)
      println("Protocol started...")
      nodeList(0) ! pushSumProtocol(numOfNodes,topology,nodeList,protcolFunc,nodePattern)
    }
    case initiateGossip(numOfNodes, topology, message, protcolFunc)=>{
      initializeNodes(topology,numOfNodes)
      println("Protocol started...")
      nodeList(0) ! gossipZero(numOfNodes,message,topology,nodeList,protcolFunc,nodePattern)

    }
    case updatePushSumStatus(nodeNum,numOfNodes)=> {
      println("Node " + nodeNum+" has terminated")
      convergenceCount = convergenceCount + 1
      if (convergenceCount == numOfNodes) {
        var endTime = System.currentTimeMillis
        var convergenceTime = endTime - startTime
        println("Convergence time: " + convergenceTime)
        context.system.shutdown()
        system.shutdown()

      }
    }
    case updateGossipStatus(nodeNo, numOfNodes) =>{
      convergenceCount+=1
      println("Node "+nodeNo+" has terminated")
      if(convergenceCount==numOfNodes) {
        var endTime = System.currentTimeMillis
        var convergenceTime = endTime - startTime
        println("Convergence time: " + convergenceTime)
        context.system.shutdown()
      }
    }
  }
  def initializeNodes(topology:String,numOfNodes:Int) = {
    for (nodeCount <- 0 to numOfNodes-1) {
      nodeList ::= system.actorOf(Props[Node])
    }
    if(topology == "3d grid" || topology == "imperfect 3d grid")
    {
      nodePattern = Topology.create3DGrid(topology,numOfNodes)
    }
    println("Topology has been built...")
    startTime = System.currentTimeMillis
  }
}


class Node extends Actor{
  val system = ActorSystem("Node")
  var neighborList:ArrayBuffer[Int]=ArrayBuffer[Int]()
  var sum:Double=0.0
  var weight:Double=0.0
  var flag=true
  var ratio:Double=0
  var neighborFlag=true
  var received_count = 0
  var ratioList:ArrayBuffer[Double]=ArrayBuffer[Double]()

  def receive={
    case pushSumProtocol(numOfNodes,topology,nodeList,protocolFunc,nodePattern) => {
      var message="";
    system.scheduler.schedule(0 seconds,0.005 seconds)(findNeighbour(0,nodeList,numOfNodes,topology,"push sum",message,protocolFunc,nodePattern))
    }

    case calculateSumWeight(receivingNodeNum,transferSum,transferWeight,nodeList,numOfNodes,topology,protocolFunc,nodePattern)=> {
      var j:Int=0;
      if(flag){
        sum=receivingNodeNum
        weight=1
        flag=false
      }
      //receiving
      sum=sum+transferSum
      weight=weight+transferWeight
      ratio=sum/weight
      ratioList += ratio

      if(ratioList.length==4)
      {

          var change1=abs(ratioList(1)-ratioList(0))
          var change2=abs(ratioList(2)-ratioList(1))
          var change3=abs(ratioList(3)-ratioList(2))
          if(change1<pow(10,-10)&&change2<pow(10,-10)&&change3<pow(10,-10)){
            protocolFunc ! updatePushSumStatus(receivingNodeNum,numOfNodes)
            context.stop(self)
          }
        ratioList(0)=ratioList(1)
        ratioList(1)=ratioList(2)
        ratioList(2)=ratioList(3)
        ratioList.remove(3);
      }
      //sending
    system.scheduler.schedule(0 seconds, 0.005 seconds)(findNeighbour(receivingNodeNum, nodeList, numOfNodes, topology, "push sum", (sum / 2).toString + "," + (weight / 2).toString(), protocolFunc, nodePattern))
  }
    case gossipZero(numOfNodes, message,topology,nodeList,protocolFunc,nodePattern)=>{
      received_count += 1
      system.scheduler.schedule(0 seconds,0.005 seconds)(findNeighbour(0,nodeList,numOfNodes,topology,"gossip",message,protocolFunc,nodePattern))
    }
    case gossipReceive(numOfNodes,message,nodeNo,topology,nodeList,protocolFunc,nodePattern) =>{
      received_count += 1
      if(received_count == 10)
      {
        protocolFunc ! updateGossipStatus(nodeNo,numOfNodes)
        context.stop(self)
      }
      system.scheduler.schedule(0 seconds,1 seconds)(findNeighbour(nodeNo,nodeList,numOfNodes,topology,"gossip",message,protocolFunc,nodePattern))
    }
  }
  def findNeighbour(nodeNum:Int,nodeList:List[ActorRef],numOfNodes:Int,topology:String,algorithm:String,message:String,protocolFunc:ActorRef,nodePattern:ArrayBuffer[String]) = {
    if(neighborFlag) {
      neighborList = Topology.createTopology(topology, numOfNodes, nodeNum, nodePattern)
      neighborFlag=false
    }
    var randomNum=Random.nextInt(neighborList.length)
    var neighborNode:Int=neighborList(randomNum)
    if(algorithm.equalsIgnoreCase("push sum")){
      var values=message.split(",");

      if(values.length>1) {
        sum = values(0).toDouble
        weight = values(1).toDouble
      }
      else{
        sum=0
        weight=1/2.0
        flag=false
      }
      nodeList(neighborNode) ! calculateSumWeight(neighborNode,sum,weight,nodeList,numOfNodes,topology,protocolFunc,nodePattern)
    }
    else
    {
      nodeList(neighborNode) ! gossipReceive(numOfNodes,message,neighborNode,topology,nodeList,protocolFunc,nodePattern)
    }

  }
}
object Topology{
  def create3DGrid(topology:String, noOfNodes:Int):ArrayBuffer[String]={
    var cuberoot:Int = ceil(cbrt(noOfNodes)).toInt
    var nodePattern:ArrayBuffer[String]=ArrayBuffer[String]()
    var i = 0

    for(z<-0 to cuberoot-1)
    {
      for(y<-0 to cuberoot-1)
      {
        for(x<-0 to cuberoot-1)
        {
          var point = x.toString()
          point += ","
          point += y.toString()
          point += ","
          point += z.toString()
          nodePattern += point
          i += 1
          if(i == noOfNodes) return nodePattern
        }

      }

    }
    return nodePattern
  }
  def createTopology(topology : String,noOfNodes: Int,nodeNumber: Int,nodePattern:ArrayBuffer[String]): ArrayBuffer[Int]={
    var neighbors:ArrayBuffer[Int]=ArrayBuffer[Int]()
    if(topology.equalsIgnoreCase("full")){
      for(i<-0 to noOfNodes-1){
        if(i!=nodeNumber){
          neighbors+=i;
        }
      }
    }
    else if(topology=="line"){
      if(nodeNumber==0){
        neighbors+=nodeNumber+1
      }
      else if(nodeNumber==noOfNodes-1){
        neighbors+=nodeNumber-1
      }
      else{
        neighbors+=nodeNumber-1
        neighbors+=nodeNumber+1
      }
    }
    else if(topology == "3d grid" || topology == "imperfect 3d grid")
    {
      var currentNode = nodePattern(nodeNumber)
      var point = currentNode.split(",")

      var pointx = point(0).toInt
      var pointx2 = pointx
      pointx = pointx-1
      var index = nodePattern.indexOf(pointx.toString() + "," + point(1) + "," + point(2))
      if(index != -1) neighbors += index
      pointx2 = pointx2+1
      index = nodePattern.indexOf(pointx2.toString() + "," + point(1) + "," + point(2))
      if(index != -1) neighbors += index

      var pointy = point(1).toInt
      var pointy2 = pointy
      pointy = pointy-1
      index = nodePattern.indexOf(point(0)+","+pointy.toString()+","+point(2))
      if(index != -1) neighbors += index
      pointy2 = pointy2+1
      index = nodePattern.indexOf(point(0)+","+pointy2.toString()+","+point(2))
      if(index != -1) neighbors += index

      var pointz1 = point(2).toInt
      var pointz2 = pointz1
      pointz1 = pointz1-1
      index = nodePattern.indexOf(point(0)+","+point(1)+","+pointz1.toString())
      if(index != -1) neighbors += index
      pointz2 = pointz2+1
      index = nodePattern.indexOf(point(0)+","+point(1)+","+pointz2.toString())
      if(index != -1) neighbors += index

      if(topology == "imperfect 3d grid")
      {
        var rand:Int=Random.nextInt(noOfNodes)
        while(neighbors.contains(rand))
          rand=Random.nextInt(noOfNodes)
        neighbors+=rand
      }
    }
    return neighbors
  }
}


case class initiatePushSum(numNodes:Int,topology:String,protcolFunc:ActorRef)
case class pushSumProtocol(numNodes:Int,topology:String,nodeList:List[ActorRef],protcolFunc:ActorRef,nodePattern:ArrayBuffer[String])
case class calculateSumWeight(receivingNodeNum:Int,sum:Double,weight:Double,nodeList:List[ActorRef],numOfNodes:Int,topology:String,protocolFunc:ActorRef,nodePattern:ArrayBuffer[String])
case class updatePushSumStatus(nodeNum:Int,numOfNodes:Int)
case class initiateGossip(numNodes:Int, topology:String,message:String,protcolFunc:ActorRef)
case class updateGossipStatus(nodeNo:Int, numOfNodes:Int)
case class gossipZero(numOfNodes:Int, message:String,topology:String,nodeList:List[ActorRef],protocolFunc:ActorRef,nodePattern:ArrayBuffer[String])
case class gossipReceive(numOfNodes:Int,message:String,workernumber:Int,topology:String,nodeList:List[ActorRef],protocolFunc:ActorRef,nodePattern:ArrayBuffer[String])
