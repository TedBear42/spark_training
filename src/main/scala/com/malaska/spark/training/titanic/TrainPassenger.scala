package com.malaska.spark.training.titanic

/**
  * Created by tmalaska on 6/24/17.
  */
case class TrainPassenger(passengerId:String,
                          pclass:String,
                          name:String,
                          sex:String,
                          age:String,
                          sibSp:String,
                          parch:String,
                          ticket:String,
                          fare:String,
                          cabin:String,
                          embarked:String,
                          survived:String) {
  def getTrainPassenger() :TestPassenger = {
    TestPassenger(passengerId,
    pclass,
    name,
    sex,
    age,
    sibSp,
    parch,
    ticket,
    fare,
    cabin,
    embarked)
  }
}
