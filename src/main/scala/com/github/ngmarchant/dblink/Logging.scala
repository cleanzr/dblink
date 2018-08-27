// Copyright (C) 2018  Neil Marchant
//
// Author: Neil Marchant
//
// This file is part of dblink.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package com.github.ngmarchant.dblink

import org.apache.log4j.Logger
import org.apache.log4j.Level

trait Logging {
  private[this] lazy val logger = Logger.getLogger(logName)

  private[this] lazy val logName: String = this.getClass.getName.stripSuffix("$")

  def info(message: => String): Unit = if (logger.isEnabledFor(Level.INFO)) logger.info(message)

  def warn(message: => String): Unit = if (logger.isEnabledFor(Level.WARN)) logger.warn(message)

  def warn(message: => String, t: Throwable): Unit = if (logger.isEnabledFor(Level.WARN)) logger.warn(message, t)

  def error(message: => String): Unit = if (logger.isEnabledFor(Level.ERROR)) logger.error(message)

  def error(message: => String, t: Throwable): Unit = if (logger.isEnabledFor(Level.ERROR)) logger.error(message, t)

  def fatal(message: => String): Unit = if (logger.isEnabledFor(Level.FATAL)) logger.fatal(message)

  def fatal(message: => String, t: Throwable): Unit = if (logger.isEnabledFor(Level.FATAL)) logger.fatal(message, t)

  def debug(message: => String): Unit = if (logger.isEnabledFor(Level.DEBUG)) logger.debug(message)
}
