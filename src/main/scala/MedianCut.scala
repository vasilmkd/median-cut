import java.awt
import java.io.{File, FileWriter, PrintWriter}

import javax.imageio.ImageIO

import scala.io.Source

object MedianCut {

  def readPixels(filename: String): Vector[Color] =
    Source.fromFile(filename).getLines().map { line =>
      val Array(red, green, blue) = line.split(",")
      Color(red.toInt, green.toInt, blue.toInt)
    }.toVector

  def range(colors: Vector[Color], f: Color => Int): (Int, Int) =
    colors.foldLeft((255, 0)) { case ((min, max), color) =>
      val newMin = scala.math.min(min, f(color))
      val newMax = scala.math.max(max, f(color))
      (newMin, newMax)
    }

  def redRange(colors: Vector[Color]): (Int, Int) =
    range(colors, _.red)

  def greenRange(colors: Vector[Color]): (Int, Int) =
    range(colors, _.green)

  def blueRange(colors: Vector[Color]): (Int, Int) =
    range(colors, _.blue)

  def rangeValue(r: (Int, Int)): Int = r._2 - r._1

  def sortByBiggestRange(colors: Vector[Color]): Vector[Color] = {
    val red = (rangeValue(redRange(colors)), (c: Color) => c.red)
    val green = (rangeValue(greenRange(colors)), (c: Color) => c.green)
    val blue = (rangeValue(blueRange(colors)), (c: Color) => c.blue)
    val sortFn = Vector(red, green, blue).maxBy(_._1)._2
    colors.sortBy(sortFn)
  }

  def cut(colors: Vector[Color]): (Vector[Color], Vector[Color]) = {
    val half = colors.length / 2
    val zipped = colors.zipWithIndex
    val lower = zipped.filter(_._2 < half).map(_._1)
    val higher = zipped.filter(_._2 >= half).map(_._1)
    (lower, higher)
  }

  def average(colors: Vector[Color]): Color = {
    val sum = colors.map { color =>
      (color.red.toLong, color.green.toLong, color.blue.toLong)
    }.fold((0L, 0L, 0L)) { case ((red, green, blue), (r, g, b)) =>
      (red + r, green + g, blue + b)
    }
    val len = colors.length
    Color((sum._1 / len).toInt, (sum._2 / len).toInt, (sum._3 / len).toInt)
  }

  def medianCut(colors: Vector[Color], stop: Int): Vector[Color] = {

    def loop(colors: Vector[Color], size: Int, stop: Int): Vector[Color] = {
      if (size == stop) {
        Vector(average(colors))
      } else {
        val sorted = sortByBiggestRange(colors)
        val (lower, higher) = cut(sorted)
        loop(lower, size * 2, stop) ++ loop(higher, size * 2, stop)
      }
    }

    loop(colors, 1, stop)
  }

  def dumpPixels(filename: String, output: String): Unit = {
    val image = ImageIO.read(new File(filename))
    val writer = new PrintWriter(new FileWriter(output), true)
    for (y <- 0 until image.getHeight) {
      for (x <- 0 until image.getWidth) {
        val c = new awt.Color(image.getRGB(x, y))
        writer.println(s"${c.getRed},${c.getGreen},${c.getBlue}")
      }
    }
    writer.close()
  }
}
