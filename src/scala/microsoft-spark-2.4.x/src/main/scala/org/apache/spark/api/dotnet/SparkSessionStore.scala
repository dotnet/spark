package org.apache.spark.api.dotnet

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.SparkSession

object SparkSessionStore {

    var sessions: ConcurrentHashMap[Int, SparkSession] = new ConcurrentHashMap()

    def saveSession(possibleSession: SparkSession): Int = {
        val session = SparkSession.getActiveSession
        if(session == None && possibleSession == None) {
            0
        }else{
            val hashCode = session.getOrElse(possibleSession).hashCode()
            if(!sessions.contains(hashCode)){
                sessions.put(hashCode, session.getOrElse(possibleSession))
            }
            hashCode
        }
    }

    def saveSession(): Int = {
        val session = SparkSession.getActiveSession
        if(session != None ){
            val hashCode = session.get.hashCode()
            if(!sessions.contains(hashCode)){
                sessions.put(hashCode, session.get)
            }
            hashCode
        }else{
            0
        }
    }

    def setSession(sessionId : Int) = {
        if(sessionId != 0){
            val session = sessions.get(sessionId)
            session.range(1).show()
            SparkSession.setActiveSession(session)
        }
    }

    def removeSession(session: SparkSession): Unit = {
        sessions.remove(session.hashCode())
    }
}
