/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = { 
    case op: Operation => root ! op
    case GC => {
      val newRoot = createRoot
      pendingQueue = Queue.empty[Operation]
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => {
      pendingQueue.enqueue(op)
      ()
    }

    case GC => ()
    case CopyFinished => { 
      root = newRoot
      context.become(normal)
      pendingQueue foreach { root ! _ }
    }
      
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case req @ Insert(requester, id, value) => if (value == elem) {
      if (removed) removed = false
      requester ! OperationFinished(id)
    } else {
      val pos = if (value < elem) Left else Right
      subtrees.get(pos) match {
        case Some(node) => node ! req
        case None => {
          subtrees = subtrees.updated(pos, context.actorOf(BinaryTreeNode.props(value, false)))
          requester ! OperationFinished(id)
        }
      }
    }

    case req @ Contains(requester, id, value) => if (value == elem) {
      requester ! ContainsResult(id, !removed)
    } else {
      val pos = if (value < elem) Left else Right
      subtrees.get(pos) match {
        case Some(node) => node ! req
        case None => requester ! ContainsResult(id, false)
      }
    }

    case req @ Remove(requester, id, value) => if (value == elem) {
      removed = true
      requester ! OperationFinished(id)
    } else {
      val pos = if (value < elem) Left else Right
      subtrees.get(pos) match {
        case Some(node) => node ! req
        case None => requester ! OperationFinished(id)
      }
    }

    case req @ CopyTo(node) => {
      val children: Set[ActorRef] = Set(subtrees.get(Left), subtrees.get(Right)).flatten
      if (children.isEmpty && removed) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(children, removed))
        if (!removed) {
          node ! Insert(self, 0, elem)
        }
        children.foreach(_ ! req)
      }
    }

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) => if (expected.isEmpty) {
      context.parent ! CopyFinished
      context.stop(self)
    } else {
      context.become(copying(expected, true))
    }

    case CopyFinished => {
      val newExpected = expected - sender
      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(newExpected, insertConfirmed))
      }
    }
  }
}
