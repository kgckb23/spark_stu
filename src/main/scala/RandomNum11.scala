import scala.util.Random

object RandomNum11 {

  def main(args: Array[String]): Unit = {
    val i = new Random().nextInt(19)  // [0,19)
    println(i + 1)  // [1,20)
  }
}
