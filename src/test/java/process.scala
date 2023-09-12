package io.glutenproject.ldps


import scala.sys.process._

//object ExecuteShellScript {
//  def start():Unit={
//    val processBuilder = new ProcessBuilder
//    processBuilder.command("sh /Users/caoduanxin/tmp/test.sh")
//    val process = processBuilder.start
//
//    //    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
//    //    var line: String = null
//    //    while ((line = reader.readLine) != null) {
//    //      logInfo(s"start clickhouse : ${line}")
//    //    }
//
//    val exitCode = process.waitFor
//    if (exitCode == 0) {
//      println("Shell script executed successfully.")
//    } else {
//      throw new IllegalArgumentException("start clickhouse failed")
//    }
//
//    val command = "sh /Users/caoduanxin/tmp/test.sh"
//    val exitCode = command.!
//
//    if (exitCode == 0) {
//    } else {
//      println("Error executing shell script. Exit code: " + exitCode)
//    }
//  }
//  def test():Unit={
//    println("test.")
//  }
//  def main(args: Array[String]): Unit = {
//    start
//    test
//  }
//}