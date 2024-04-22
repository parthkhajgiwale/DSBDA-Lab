object LargerOfTwo {
  def main(args: Array[String]): Unit = {
    // Prompting user to enter the first number
    println("Enter the first number:")
    // Reading input from the user
    val num1 = scala.io.StdIn.readDouble()
    
    // Prompting user to enter the second number
    println("Enter the second number:")
    // Reading input from the user
    val num2 = scala.io.StdIn.readDouble()
    
    // Finding the larger of the two numbers
    val largerNumber = if (num1 > num2) num1 else num2
    
    // Printing the larger number
    println(s"The larger number is: $largerNumber")
  }
}
