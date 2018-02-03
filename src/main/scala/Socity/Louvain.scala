package Socity

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class VertexData(val vId:Long,var cId:Long) extends Serializable{
  var innerDegree = 0.0  //内部节点权重
  var innerVertices = new mutable.HashSet[Long]() //内部节点
  var degree = 0.0   //节点度
  var commVertices = new mutable.HashSet[Long]()  //社区中节点
}

object Louvain {

  //汇聚邻居节点信息
  def get_NeighCommInfo(G:Graph[VertexData,Double]):RDD[(VertexId,Iterable[(Long,Double,Double)])]={
    //计算每个社区tot
    val commTot = G.vertices.map(v=>(v._2.cId,v._2.degree+v._2.innerDegree)).groupByKey().map(x=>{
      val cid = x._1
      val tot = x._2.sum
      (cid,tot)
    })

    //求每个节点的邻居社区的k_in
    val commKIn = G.triplets.flatMap(trip=>{
      val weight = trip.attr
      Array((trip.srcAttr.cId,(trip.dstId->weight)),(trip.dstAttr.cId,(trip.srcId,trip.attr)))
    }).groupByKey().map(t=>{
      val cid = t._1
      val m = new mutable.HashMap[VertexId,Double]()
      for (x <- t._2){
        if(m.contains(x._1)){
          m(x._1) += x._2
        }
        else{
          m(x._1) = x._2
        }
      }
      (cid,m)
    })

    //节点邻居社区tot以及该节点k_i_in
    val neighCommInfo = commTot.join(commKIn).flatMap(x=>{
      val cid = x._1
      val tot = x._2._1
      x._2._2.map(t=>{
        val vid = t._1
        val k_in = t._2
        (vid,(cid,k_in,tot))
      })
    }).groupByKey()

    neighCommInfo
  }

  // 计算每个结点的最大modularity增加
  def getChangeInfo(G:Graph[VertexData,Double],neighCommInfo:RDD[(VertexId,Iterable[(Long,Double,Double)])], m:Double):RDD[(VertexId,Long,Double)] = {
    val changeInfo = G.vertices.join(neighCommInfo).map(x=>{
      val vid = x._1
      val data = x._2._1
      val commIter = x._2._2
      val vCid = data.cId
      val k_v = data.degree+data.innerDegree
      val maxQ = commIter.map(t=>{
        val nCid = t._1
        val k_v_in = t._2
        var tot = t._3
        if(vCid == nCid){
          tot -= k_v
        }
        val q = (k_v_in-tot*k_v/m)
        (vid,nCid,q)
      }).max(Ordering.by[(VertexId,Long,Double),Double](_._3))

      if (maxQ._3 > 0.0)
        maxQ
      else
        (vid,vCid,0.0)
    })
    changeInfo
  }
  

}
