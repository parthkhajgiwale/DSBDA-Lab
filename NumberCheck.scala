object NumberCheck {
  def main(args: Array[String]): Unit = {
    // Prompting user to enter a number
    println("Enter a number:")
    // Reading input from the user
    val num = scala.io.StdIn.readDouble()
    
    // Checking if the number is positive, negative, or zero
    if (num > 0) {
      println("The number is positive.")
    } else if (num < 0) {
      println("The number is negative.")
    } else {
      println("The number is zero.")
    }
  }
}
