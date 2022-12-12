// Here is the imports for this exercises.

import scala.util.Try
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

// (a)

// First we created a class, called Collision which stores the some specific fields. Then we created a function, called parseCollisions such that it parses the lines of the file. The output of this function is in an RDD[Collision]

case class Collision (date:String, time:String, borough:String, latitude:String, longitude:String, location:String, onStreetName:String, crossStreetName:String, offStreetName:String, numberInjured:Int, numberKilled:Int, contributingFactorVehicle1:String, contributingFactorVehicle:String, vehicleTypeCode1:String, vehicleTypeCode2:String)

def parseCollisions(rdd: RDD[String]): RDD[Collision] = {rdd.map(data => {
val line = data.split(",")
val date = Try(line(0).trim).getOrElse("")
val time = Try(line(1).trim).getOrElse("")
val borough = Try(line(2).trim).getOrElse("")
val latitude = Try(line(4).trim).getOrElse("")
val longitude = Try(line(5).trim).getOrElse("")
val location = Try(((line(6).trim.split("\\\"\\(")(1).trim, line(7).trim.split("\\)\\\"")(0).trim)).toString).getOrElse(null)
val onStreetName = Try(line(8).trim).getOrElse("")
val crossStreetName = Try(line(9).trim).getOrElse("")
val offStreetName = Try(line(10).trim).getOrElse("")
val numberInjured = Try(line(11).trim.toInt).getOrElse(0)
val numberKilled = Try(line(12).trim.toInt).getOrElse(0)
val contributingFactorVehicle1 = Try(line(19).trim).getOrElse("")
val contributingFactorVehicle2 = Try(line(20).trim).getOrElse("")
val vehicleTypeCode1 = Try(line(25).trim).getOrElse("")
val vehicleTypeCode2 = Try(line(26).trim).getOrElse("")
	
Collision(date, time, borough, latitude, longitude, location, onStreetName, crossStreetName, offStreetName, numberInjured, numberKilled, contributingFactorVehicle1, contributingFactorVehicle2, vehicleTypeCode1, vehicleTypeCode2)
})}

// We saved the path of the csv and load it as a sc.textFile. From this RDD, we drop the header of the csv and we pass you through the parseCollision. At the end, we drop each row such that the following fields are empty. Note that OFF STREET NAME and CROSS STREET NAME are complementary to each other and since we don't need the OFF STREET NAME in this exercise, i will allow empty value for this field.

val inputPath = "/Users/carolina/Documents/Semester_8/Big_Data_Analytics/Exercise_4/NYPD_Motor_Vehicle_Collisions.csv"

val collisionData = sc.textFile(inputPath)

val filteredCollisionData = parseCollisions(collisionData.filter(line => line!=header))

val nonEmptyCollisionData = filteredCollisionData.filter{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => a.length() > 0 && b.length() > 0 && c.length() > 0 && d.length() > 0 && e.length() > 0 && f.length() > 0 && g.length() > 0 && h.length() > 0 && k.length() > 0 && l.length() > 0 && m.length() > 0 && n.length() > 0 && o.length() > 0 && p.length() > 0}



// (b)

// First we select from each row only the following field BOROUGH, ON STREET NAME and CROSS STREET NAME as key and we add the NUMBER OF PERSONS INJURED and NUMBER OF PERSONS KILLED as put it as value. We reduced it by key. Then separately we select the following field BOROUGH, ON STREET NAME, CROSS STREET NAME and CONTRIBUTING FACTOR VEHICLE 1, resp. CONTRIBUTING FACTOR VEHICLE 2. All of those fields are keys and we put a 1 as value. We save the union of both rdd. This new union will be reduced by key and group by the combination of BOROUGH, ON STREET NAME and CROSS STREET NAME. We order the argument in the group in descending order and take the top 5 arguments. Then we go again to the first RDD and do a leftOuterJoin with the RDD which contain the factors. At the end we take the top 25

val collisionWithInjuriesAndKilled = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h), k + l)}.reduceByKey((x, y) => x + y)

val collisionWithFactor1 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h, m), 1)}
val collisionWithFactor2 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h, n), 1)}

val collisionWithBothFactor = collisionWithFactor1.union(collisionWithFactor2).reduceByKey((x, y) => x+y).map{case ((a, b, c, d), e) => ((a, b, c), (e, d))}.groupByKey().mapValues(_.toSeq.sortWith(_._1 > _._1)).mapValues(x => x.take(5))

val collision = collisionWithInjuriesAndKilled.leftOuterJoin(collisionWithBothFactor).map{case (a, (b, c)) => (b, (a, c))}.sortByKey(false).take(25)



// (c)

// We create a RDD with all of the name of the week of each day of january. We go to the non empty fielded RDD  from question (a) and split the field date into month, day and year and time into hour only. We join this RDD with the january RDD and now we know the day of the week for each date for each we have data. Then from the joined RDD we will select the DAY OF THE WEEK and the HOUR OF THE DAY and we add the NUMBER OF PERSONS INJURED and NUMBER OF PERSONS KILLED. For the next steps, we did everything as in point (b). We create 2 RDD's, one with the previous fields and CONTRIBUTING FACTOR VEHICLE 1, the other with the previous fields and CONTRIBUTING FACTOR VEHICLE 2. We create an union between both RDDs. The fields are the key and we put 1 ass value. We do an union of the RDD of both factors. We reduce by key, group them by DAY OF WEEK and HOUR OF DAY. We take the top 5 of the arguments. We join it with the RDD with the day of the week and we take the top 25.

val january = spark.sparkContext.parallelize(Seq(("01", "01", "2013", "Tuesday"), 
("01", "02", "2013", "Wednesday"),
("01", "03", "2013", "Thursday"),
("01", "04", "2013", "Friday"),
("01", "05", "2013", "Saturday"),
("01", "06", "2013", "Sunday"),
("01", "07", "2013", "Monday"),
("01", "08", "2013", "Tuesday"),
("01", "09", "2013", "Wednesday"),
("01", "10", "2013", "Thursday"),
("01", "11", "2013", "Friday"),
("01", "12", "2013", "Saturday"),
("01", "13", "2013", "Sunday"),
("01", "14", "2013", "Monday"),
("01", "15", "2013", "Tuesday"),
("01", "16", "2013", "Wednesday"),
("01", "17", "2013", "Thursday"),
("01", "18", "2013", "Friday"),
("01", "19", "2013", "Saturday"),
("01", "20", "2013", "Sunday"),
("01", "21", "2013", "Monday"),
("01", "22", "2013", "Tuesday"),
("01", "23", "2013", "Wednesday"),
("01", "24", "2013", "Thursday"),
("01", "25", "2013", "Friday"),
("01", "26", "2013", "Saturday"),
("01", "27", "2013", "Sunday"),
("01", "28", "2013", "Monday"),
("01", "29", "2013", "Tuesday"),
("01", "30", "2013", "Wednesday"),
("01", "31", "2013", "Thursday"))).map{case (a, b, c, d) => ((a, b, c), d)}

val collisionFilteredHour = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => {
val date = a.split("\\/")
val month = date(0).trim
val day = date(1).trim
val year = date(2).trim
	
val number = b.split(":")
val hour = number(0).trim.toInt
	
((month, day, year), (b, hour, c, d, e, f, g, h, j, k, l, m, n, o, p))
}}

val collisionWithDayOfWeek = collisionFilteredHour.join(january).map{case ((a, b, c), ((d, e, f, g, h, j, k, l, m, n, o, p, q, r, s), t)) => (a, b, c, t, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s)}

val collisionWithDayOfWeekAndInjuriesAndKilled = collisionWithDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f), o + p)}.reduceByKey((x, y) => x + y)

val collisionWithDayOfWeekAndFactor1 = collisionWithDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f, q), 1)}

val collisionWithDayOfWeekAndFactor2 = collisionWithDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f, r), 1)}

val collisionWithDayOfWeekAndBothFactor = collisionWithDayOfWeekAndFactor1.union(collisionWithDayOfWeekAndFactor2).reduceByKey((x, y) => x + y).map{case ((a, b, c), d) => ((a, b), (d, c))}.groupByKey().mapValues(_.toSeq.sortWith(_._1 > _._1)).mapValues(x => x.take(5))

val collisionWithDayOfWeekJoin = collisionWithDayOfWeekAndInjuriesAndKilled.leftOuterJoin(collisionWithDayOfWeekAndBothFactor).map{case ((a, b), (c, d)) => (c, (a, b, d))}.sortBy(_._1, false).take(25)


// (d)

// Separately we select the VEHICLE TYPE CODE 1 or 2 with the added number of NUMBER OF PERSONS INJURED and NUMBER OF PERSONS KILLED. At the end, we do an union of both, reduce it by key and take the top 5 of vehicle with the most NUMBER OF PERSONS INJURED and NUMBER OF PERSONS KILLED.

val collisionOfVehicleType1 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => (o, k + l)}

val collisionOfVehicleType2 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => (p, k + l)}

val collisionOfVehicleType = collisionOfVehicleType1.union(collisionOfVehicleType2).reduceByKey((x, y) => x + y).sortBy(_._2, false).map{case (a, b) => (b, a)}.take(5)



// (e)

// First we do the same steps as in point (b) but with the number of persons injured and the number of persons killed separately. At the end we join each of them with the RDD which contains the factors, so we have a RDD with the factor of the collision and either with the number of persons injured or number of persons killed. Now we join both RDDs and calculate the difference between the number of persons injured and number of persons killed. Then we take the top 5 of the highest difference.

// (b) Injuries

val collisionWithInjuries = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h), k)}.reduceByKey((x, y) => x + y)

val collisionWithInjuriesAndFactor1 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h, m), 1)}
val collisionWithInjuriesAndFactor2 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h, n), 1)}

val collisionWithInjuriesAndBothFactor = collisionWithInjuriesAndFactor1.union(collisionWithInjuriesAndFactor2).reduceByKey((x, y) => x+y).map{case ((a, b, c, d), e) => ((a, b, c), (e, d))}.groupByKey().mapValues(_.toSeq.sortWith(_._1 > _._1)).mapValues(x => x.take(5))

val collisionInjuries = collisionWithInjuries.leftOuterJoin(collisionWithInjuriesAndBothFactor).map{case (a, (b, c)) => (b, (a, c))}.sortByKey(false).take(25)

// (b) Killed

val collisionWithKilled = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h), l)}.reduceByKey((x, y) => x + y)
val collisionWithKilledAndFactor1 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h, m), 1)}
val collisionWithKilledAndFactor2 = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => ((c, g, h, n), 1)}

val collisionWithKilledAndBothFactor = collisionWithKilledAndFactor1.union(collisionWithKilledAndFactor2).reduceByKey((x, y) => x+y).map{case ((a, b, c, d), e) => ((a, b, c), (e, d))}.groupByKey().mapValues(_.toSeq.sortWith(_._1 > _._1)).mapValues(x => x.take(5))

val collisionKilled = collisionWithKilled.leftOuterJoin(collisionWithKilledAndBothFactor).map{case (a, (b, c)) => (b, (a, c))}.sortByKey(false).take(25)

// (b) Conclusion

val collisionInjuries = collisionWithInjuries.leftOuterJoin(collisionWithInjuriesAndBothFactor).map{case (a, (b, c)) => ((a, c), b)}
val collisionKilled = collisionWithKilled.leftOuterJoin(collisionWithKilledAndBothFactor).map{case (a, (b, c)) => ((a, c), b)}

val collisionDifference_b = collisionInjuries.join(collisionKilled).map{case (a, (b, c)) => ((b-c).abs, a)}.sortByKey(false).take(5)


// First we do the same steps as in point (c) but with the number of persons injured and the number of persons killed separately. At the end we join each of them with the RDD which contains the factors, so we have a RDD with the factor of the collision and either with the number of persons injured or number of persons killed. Now we join both RDDs and calculate the difference between the number of persons injured and number of persons killed. Then we take the top 5 of the highest difference.

// (c) Injuries

val collisionWithInjuriesAndDayOfWeek = collisionFilteredHour.join(january).map{case ((a, b, c), ((d, e, f, g, h, j, k, l, m, n, o, p, q, r, s), t)) => (a, b, c, t, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s)}

val collisionWithDayOfWeekAndInjuries = collisionWithInjuriesAndDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f), o)}.reduceByKey((x, y) => x + y)

val collisionWithDayOfWeekAndInjuriesAndFactor1 = collisionWithInjuriesAndDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f, q), 1)}
val collisionWithDayOfWeekAndInjuriesAndFactor2 = collisionWithInjuriesAndDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f, r), 1)}

val collisionWithDayOfWeekAndInjuriesAndBothFactor = collisionWithDayOfWeekAndInjuriesAndFactor1.union(collisionWithDayOfWeekAndInjuriesAndFactor2).reduceByKey((x, y) => x + y).map{case ((a, b, c), d) => ((a, b), (d, c))}.groupByKey().mapValues(_.toSeq.sortWith(_._1 > _._1)).mapValues(x => x.take(5))

val collisionWithDayOfWeekJoinInjuries = collisionWithDayOfWeekAndInjuries.leftOuterJoin(collisionWithDayOfWeekAndInjuriesAndBothFactor).map{case ((a, b), (c, d)) => (c, (a, b, d))}.sortBy(_._1, false).take(25)

// (c) Killed

val collisionWithKilledAndDayOfWeek = collisionFilteredHour.join(january).map{case ((a, b, c), ((d, e, f, g, h, j, k, l, m, n, o, p, q, r, s), t)) => (a, b, c, t, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s)}

val collisionWithDayOfWeekAndKilled = collisionWithKilledAndDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f), p)}.reduceByKey((x, y) => x + y)

val collisionWithDayOfWeekAndKilledAndFactor1 = collisionWithKilledAndDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f, q), 1)}
val collisionWithDayOfWeekAndKilledAndFactor2 = collisionWithKilledAndDayOfWeek.map{case (a, b, c, d, e, f, g, h, j, k, l, m, n, o, p, q, r, s, t) => ((d, f, r), 1)}

val collisionWithDayOfWeekAndKilledAndBothFactor = collisionWithDayOfWeekAndKilledAndFactor1.union(collisionWithDayOfWeekAndKilledAndFactor2).reduceByKey((x, y) => x + y).map{case ((a, b, c), d) => ((a, b), (d, c))}.groupByKey().mapValues(_.toSeq.sortWith(_._1 > _._1)).mapValues(x => x.take(5))

val collisionWithDayOfWeekJoinKilled = collisionWithDayOfWeekAndKilled.leftOuterJoin(collisionWithDayOfWeekAndKilledAndBothFactor).map{case ((a, b), (c, d)) => (c, (a, b, d))}.sortBy(_._1, false).take(25)

// (c) Conclusion

val collisionWithDayOfWeekJoinInjuries = collisionWithDayOfWeekAndInjuries.leftOuterJoin(collisionWithDayOfWeekAndInjuriesAndBothFactor).map{case ((a, b), (c, d)) => ((a, b, d), c)}

val collisionWithDayOfWeekJoinKilled = collisionWithDayOfWeekAndKilled.leftOuterJoin(collisionWithDayOfWeekAndKilledAndBothFactor).map{case ((a, b), (c, d)) => ((a, b, d), c)}

val collisionDifference_c = collisionWithDayOfWeekJoinInjuries.join(collisionWithDayOfWeekJoinKilled).map{case (a, (b, c)) => ((b-c).abs, a)}.sortByKey(false).take(5)


// First we do the same steps as in point (d) but with the number of persons injured and the number of persons killed separately. At the end we join each of them with the RDD which contains the vehicle type, so we have a RDD with the vehicle type of the collision and either with the number of persons injured or number of persons killed. Now we join both RDDs and calculate the difference between the number of persons injured and number of persons killed. Then we take the top 5 of the highest difference.

// (d) Injuries

val collisionOfVehicleType1WithInjuries = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => (o, k)}
val collisionOfVehicleType2WithInjuries = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => (p, k)}

val collisionOfVehicleTypeWithInjuries = collisionOfVehicleType1WithInjuries.union(collisionOfVehicleType2WithInjuries).reduceByKey((x, y) => x + y).sortBy(_._2, false).map{case (a, b) => (b, a)}.take(5)

// (d) Killed

val collisionOfVehicleType1WithKilled = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => (o, l)}
val collisionOfVehicleType2WithKilled = nonEmptyCollisionData.map{case Collision(a, b, c, d, e, f, g, h, j, k, l, m, n, o, p) => (p, l)}

val collisionOfVehicleTypeWithKilled = collisionOfVehicleType1WithKilled.union(collisionOfVehicleType2WithKilled).reduceByKey((x, y) => x + y).sortBy(_._2, false).map{case (a, b) => (b, a)}.take(5)

// (d) Conclusion

val collisionOfVehicleTypeWithInjuries = collisionOfVehicleType1WithInjuries.union(collisionOfVehicleType2WithInjuries).reduceByKey((x, y) => x + y)

val collisionOfVehicleTypeWithKilled = collisionOfVehicleType1WithKilled.union(collisionOfVehicleType2WithKilled).reduceByKey((x, y) => x + y)

val collisionDifference_d = collisionOfVehicleTypeWithInjuries.join(collisionOfVehicleTypeWithKilled).map{case (a, (b, c)) => ((b-c).abs, a)}.sortByKey(false).take(5)










