package brgroup

import org.apache.log4j.{Level, LogManager}

trait Logger {
  private val logger = LogManager.getLogger(this.getClass)

  def debug(message: String): Unit = logger.debug(message)

  def info(message: String): Unit = logger.info(message)

  def warn(message: String): Unit = logger.warn(message)

  def error(message: String, throwable: Throwable): Unit = logger.error(message, throwable)
}
