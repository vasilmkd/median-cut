import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object SparkMedianCut {

  def readPixels(pixels: RDD[String]): RDD[Color] =
    pixels.map { line =>
      val Array(red, green, blue) = line.split(",")
      Color(red.toInt, green.toInt, blue.toInt)
    }

  def range(colors: RDD[Color], f: Color => Int): (Int, Int) =
    colors.map(color => (f(color), f(color))).fold((255, 0)) { case ((min, max), (v1, v2)) =>
      val newMin = scala.math.min(min, v1)
      val newMax = scala.math.max(max, v2)
      (newMin, newMax)
    }

  def redRange(colors: RDD[Color]): (Int, Int) =
    range(colors, _.red)

  def greenRange(colors: RDD[Color]): (Int, Int) =
    range(colors, _.green)

  def blueRange(colors: RDD[Color]): (Int, Int) =
    range(colors, _.blue)

  def rangeValue(r: (Int, Int)): Int = r._2 - r._1

  def sortByBiggestRange(colors: RDD[Color]): RDD[Color] = {
    val red = (rangeValue(redRange(colors)), (c: Color) => c.red)
    val green = (rangeValue(greenRange(colors)), (c: Color) => c.green)
    val blue = (rangeValue(blueRange(colors)), (c: Color) => c.blue)
    val sortFn = Vector(red, green, blue).maxBy(_._1)._2
    colors.sortBy(sortFn)
  }

  def cut(colors: RDD[Color]): (RDD[Color], RDD[Color]) = {
    val half = colors.count() / 2
    val zipped = colors.zipWithIndex
    val lower = zipped.filter(_._2 < half).map(_._1)
    val higher = zipped.filter(_._2 >= half).map(_._1)
    (lower, higher)
  }

  def average(colors: RDD[Color]): Color = {
    val sum = colors.map { color =>
      (color.red.toLong, color.green.toLong, color.blue.toLong)
    }.fold((0L, 0L, 0L)) { case ((red, green, blue), (r, g, b)) =>
      (red + r, green + g, blue + b)
    }
    val len = colors.count()
    Color((sum._1 / len).toInt, (sum._2 / len).toInt, (sum._3 / len).toInt)
  }

  def medianCut(colors: RDD[Color], stop: Int): Vector[Color] = {

    def loop(colors: RDD[Color], size: Int, stop: Int): Vector[Color] = {
      if (size == stop) {
        Vector(average(colors))
      } else {
        val sorted = sortByBiggestRange(colors).cache()
        val (lower, higher) = cut(sorted)
        loop(lower, size * 2, stop) ++ loop(higher, size * 2, stop)
      }
    }

    loop(colors, 1, stop)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("median-cut"))
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
    val pixels: RDD[String] = sc.textFile(args.head)
    val start = System.nanoTime()
    val result = medianCut(readPixels(pixels), 256)
    println(result.length)
    println(result)
    val end = System.nanoTime()
    println(s"Elapsed time: ${end - start}ns")
    Thread.sleep(600000L)
    sc.stop()
  }
}
