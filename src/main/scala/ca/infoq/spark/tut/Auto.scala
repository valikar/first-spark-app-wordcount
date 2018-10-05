package ca.infoq.spark.tut

import java.sql.Timestamp

case class Auto(dateCrawled:String, name:String, seller:String, offerType:String, price:Integer, abtest:String,
               vehicleType:String, yearOfRegistration:Integer, gearbox:String, powerPS:Integer, model:String, kilometer:Integer,
               monthOfRegistration:Integer, fuelType:String, brand:String, notRepairedDamage:String, dateCreated:Timestamp, nrOfPictures:Integer,
               postalCode:Integer, lastSeen:Timestamp)
