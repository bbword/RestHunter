package RestHunter

import streaming.core.StreamingApp

object LocalRestHunter {
  def main(args: Array[String]): Unit = {
    val newArgs = Array(
      "-streaming.duration", "10",
      "-streaming.name", "Sync",
      "-streaming.jobs", "VODDictSync,AlbumDictSync",
      "-streaming.rest", "true"
    ) ++ args
    StreamingApp.main(newArgs)
  }
}
