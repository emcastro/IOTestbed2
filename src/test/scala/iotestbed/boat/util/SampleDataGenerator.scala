package iotestbed.boat.util

import scala.collection.immutable.TreeMap
import scala.util.Random

object SampleDataGenerator extends App {

  var rc = 0

  def nextRadioCode() = {
    rc += 1
    rc
  }

  case class Boat(name: String, length: Int, width: Int, hullCode: Int)

  val boats = Seq(
    Boat("AARDAL", 94, 17, 91536),
    Boat("ABRO", 15, 4, 19378),
    Boat("AGIOS PANTELEIMON", 58, 14, 19214),
    Boat("ALMA LIBRE TOO", 17, 5, 37506),
    Boat("ALPHECCA", 23, 6, 50159),
    Boat("ALREK", 100, 17, 50469),
    Boat("ALTISIGMA", 14, 4, 29891),
    Boat("ANGY R", 187, 28, 14433),
    Boat("ASTRO VERMELHO", 66, 12, 32223),
    Boat("ATLOY VIKING", 35, 8, 87701),
    Boat("AURORA BOREAL", 26, 7, 82992),
    Boat("AYSE NAZ BAYRAKTAR", 156, 25, 99369),
    Boat("BAO TUO", 48, 6, 82240),
    Boat("BARA SI-10", 12, 4, 83459),
    Boat("BELMAR", 249, 44, 41463),
    Boat("BI YANG 6 HAO", 85, 13, 36845),
    Boat("BIG ARON", 47, 11, 11124),
    Boat("BIMA MULU", 34, 8, 75345),
    Boat("BRIDGET CAULLEY", 32, 11, 82728),
    Boat("CAPE FEAR", 14, 4, 72306),
    Boat("CEST SI BON I", 16, 4, 15452),
    Boat("CHANG TIAN HAI", 178, 27, 36219),
    Boat("CHENG GONG YOU 8", 37, 9, 78481),
    Boat("CORSAIR", 177, 30, 16752),
    Boat("CRYSTAL GALAXY", 111, 17, 61150),
    Boat("DANPILOT LODSFARTOJ4", 10, 4, 18867),
    Boat("DON SANTIAGO", 26, 8, 36593),
    Boat("EDDA FONN", 85, 19, 54761),
    Boat("EDDA FRIGG", 84, 19, 71784),
    Boat("FAIR FALCON", 105, 16, 54885),
    Boat("FAXI", 67, 12, 12202),
    Boat("FORTE", 91, 16, 79284),
    Boat("FRANCISCO", 52, 11, 60303),
    Boat("FRI SEA", 92, 14, 62677),
    Boat("FUTURE PROSPERITY", 180, 33, 36564),
    Boat("GEMINI", 190, 32, 12257),
    Boat("GEO EXPLORER SURVEY", 17, 5, 15727),
    Boat("GUAN HAI ZHAO YANG", 187, 28, 63801),
    Boat("GUO TOU 102", 190, 32, 16260),
    Boat("HARMONIE", 26, 5, 81625),
    Boat("HD27 ZUIDERHAAKS", 42, 8, 43169),
    Boat("HMSTC KUKRI", 17, 5, 31522),
    Boat("HOEGH AMERICA", 200, 33, 23087),
    Boat("HOLMA", 40, 9, 17573),
    Boat("HOLTENAU", 27, 9, 36574),
    Boat("HUGIN EXPLORER", 86, 20, 61583),
    Boat("ICM NAUTICAL 1", 32, 8, 59478),
    Boat("ISLA-S DS1", 36, 7, 76173),
    Boat("IZBORSK", 81, 12, 18012),
    Boat("JIANGHAITONG19", 83, 16, 66203),
    Boat("JOHNNY K", 20, 6, 48753),
    Boat("K.DADAYLI", 87, 14, 86828),
    Boat("KARAGOZLER-1", 23, 8, 59647),
    Boat("KMP.MARINA PRATAMA", 54, 14, 74300),
    Boat("KONGSNES", 45, 8, 10517),
    Boat("KPV KAPAS", 60, 14, 66079),
    Boat("KRISTIN SCHEPERS", 333, 22, 56324),
    Boat("KUEMOH-T-1", 60, 10, 48163),
    Boat("LADY MOURA", 105, 19, 18793),
    Boat("LEHMANN BELT", 91, 14, 55140),
    Boat("LIAHOLM", 33, 8, 34211),
    Boat("LINGE", 28, 9, 11165),
    Boat("LIVA GRETA", 65, 11, 43764),
    Boat("LOS LLANITOS", 224, 32, 62639),
    Boat("LOVUND", 72, 16, 25819),
    Boat("LU JIANG 6", 32, 7, 63111),
    Boat("LUANDA TIDE", 67, 16, 79324),
    Boat("MACABI", 14, 4, 35414),
    Boat("MAERSK CHICAGO", 300, 40, 54643),
    Boat("MAI LEHMANN", 91, 14, 64131),
    Boat("MARIA STAR", 25, 7, 45212),
    Boat("MARSJA", 110, 12, 75646),
    Boat("MELBOURNE", 110, 14, 88058),
    Boat("MISENO", 183, 33, 16706),
    Boat("MISS JACQUELINE IV", 20, 8, 80683),
    Boat("NARVIK", 135, 11, 89266),
    Boat("NAUTIC", 27, 7, 14429),
    Boat("NAVE TITAN", 184, 32, 48511),
    Boat("NEVERLAND DREAM", 250, 44, 75338),
    Boat("NIBULON 3", 38, 12, 87444),
    Boat("O DAEYANG 12", 100, 18, 44668),
    Boat("OCEAN PIERRE JULIEN", 25, 10, 80539),
    Boat("OLYMPUS", 190, 32, 40904),
    Boat("PAGANELLA", 183, 32, 73952),
    Boat("PAM LEOPARDO", 26, 8, 25837),
    Boat("PAVINO", 106, 17, 41143),
    Boat("PEARL SEAWAYS", 179, 34, 35358),
    Boat("PENG CHENG 66", 49, 9, 35859),
    Boat("PHILIPPOS A", 292, 45, 26394),
    Boat("PINE GALAXY", 148, 25, 77854),
    Boat("POURQUOIPAS", 108, 20, 60118),
    Boat("PUERTO ROSARIO", 200, 33, 18884),
    Boat("RASTA", 172, 11, 85269),
    Boat("RESCUE ELSA JOHANSSO", 16, 5, 91111),
    Boat("RESCUE KJOPSTAD", 40, 10, 17538),
    Boat("RIVO", 27, 9, 72533),
    Boat("RV DISCOVERY", 20, 6, 94365),
    Boat("SAN MARCO AT", 33, 12, 88561),
    Boat("SEA LYNX", 333, 61, 89775),
    Boat("SEBASTIAN", 45, 8, 75292),
    Boat("SERTANEJO SPIRIT", 282, 49, 51200),
    Boat("SHENG XIN 9", 94, 14, 93150),
    Boat("SINOKOR ULSAN", 120, 19, 92924),
    Boat("SIYA", 80, 16, 49303),
    Boat("SOELOEVEN", 54, 9, 51108),
    Boat("STELLA", 15, 4, 72575),
    Boat("STENSTRAUM", 146, 21, 82000),
    Boat("STIG HARRY", 12, 5, 88566),
    Boat("SUN BRG APP BUOY C", 88, 17, 91394),
    Boat("SUNG-JIN HOA", 28, 10, 86465),
    Boat("SVITZER ELLERBY", 30, 10, 40828),
    Boat("TANG MA BO MEI", 30, 9, 59004),
    Boat("TAURUS J", 155, 25, 47350),
    Boat("TAYMA", 367, 49, 60168),
    Boat("TRUFFALDINO", 89, 13, 98309),
    Boat("UK237 GRIETJE BOS", 43, 8, 73872),
    Boat("URZELA", 24, 10, 33116),
    Boat("VAALS", 110, 12, 94205),
    Boat("VALO", 27, 9, 16882),
    Boat("VENTUROUS", 24, 7, 36905),
    Boat("VINCENZA GIACALONE", 28, 6, 93996),
    Boat("VIVIEN A", 211, 30, 34159),
    Boat("WADI MAI", 23, 9, 41655),
    Boat("WEI LONG 68", 93, 13, 58259),
    Boat("WHATEVER", 12, 5, 61507),
    Boat("WILSON CARDIFF", 100, 13, 70904),
    Boat("XIN HUA 803", 112, 16, 63743),
    Boat("XIN LU SHENG 3", 149, 21, 80620),
    Boat("YOUNG GLORY", 206, 32, 91108),
    Boat("ZENITH SPIRIT", 275, 49, 30851),
    Boat("ZHEGANGHANGXUN1103", 97, 16, 10933),
    Boat("ZWAANTJE", 40, 6, 82508)
  )

  val ports = IndexedSeq(
    "Shanghai",
    "Singapore",
    "Shenzhen",
    "Hong Kong",
    "Busan",
    "Ningbo",
    "Qingdao",
    "Guangzhou",
    "Dubai",
    "Tianjin",
    "Rotterdam",
    "Dalian",
    "Kuala Lumpur",
    "Kaohsiung",
    "Hamburg",
    "Antwerp",
    "Yokohama",
    "Xiamen",
    "Los Angeles",
    "Tanjung Pelepas",
    "Long Beach",
    "Jakarta",
    "Bangkok",
    "Ho Chi Minh",
    "Bremen",
    "Lianyungang",
    "New York",
    "Osaka",
    "Yingkou",
    "Jeddah",
    "Algeciras",
    "Valencia",
    "Columbo",
    "Mumbai",
    "Sharjah",
    "Manila",
    "Felixstowe",
    "São Paulo",
    "Istanbul",
    "Panama"
  )

  val companyProbDensity = Seq(
    "Maersk" -> 20,
    "MSC" -> 17,
    "CMA-CGM" -> 11,
    "Evergreen" -> 6,
    "COSCO" -> 6,
    "Hapag-Lloyd" -> 5,
    "American President Lines" -> 5,
    "Hanjin Shipping" -> 4,
    "China Shipping" -> 4,
    "Mitsui OSK Line" -> 4,
    "OOCL" -> 4,
    "Hamburg Süd" -> 3,
    "NYK Line" -> 3,
    "Yang Ming Marine" -> 3,
    "K Line" -> 3,
    "Hyundai Marine" -> 2
  )

  val companyProbDistribution = {
    var p = 0
    TreeMap.empty[Int, String] ++ (for ((name, prob) <- companyProbDensity) yield {
      p += prob
      p -> name
    })
  }

  var boatOwner: Map[Boat, String] = boats.map(_ -> randomCompany()).toMap
  var boatRadioCode: Map[Boat, Int] = boats.map(_ -> nextRadioCode()).toMap

  def randomCompany() = companyProbDistribution.from(Random.nextInt(100)).head._2

  println("date,hullCode,radioCode,name,owner,length,width,position")

  def sieve(a: Any, probability: Double) = if (Random.nextDouble() < probability) a.toString else ""

  for (date <- Range(20150100, 20150110)) {
    val shuffled = Random.shuffle(boats)
    val boatsToReport = shuffled.take(boats.size * 50 / 100)
    val changingOwnerBoats = shuffled.take(boats.size * 10 / 100)
    boatOwner ++= changingOwnerBoats.map(_ -> randomCompany())
    boatRadioCode ++= changingOwnerBoats.map(_ -> nextRadioCode())

    for (b <- boatsToReport) {
      val position = ports(Random.nextInt(ports.size))

      val hullCode = sieve(b.hullCode, .7)
      val radioCode = sieve(boatRadioCode(b), .7)
      val name = sieve(b.name, .7)
      val owner = sieve(boatOwner(b), .7)
      val length = sieve(b.length, .5)
      val width = sieve(b.width, .1)

      println(s"$date,$hullCode,$radioCode,$name,$owner,$length,$width,$position")
    }
  }

}
