/**
/// MemCached C# Client Logger
/// Copyright (c) 2005
///
/// This module is Copyright (c) 2005 Tim Gebhardt
/// 
/// Based on code written originally by Greg Whalin
/// http://www.whalin.com/memcached/
/// 
/// All rights reserved.
///
/// This library is free software; you can redistribute it and/or
/// modify it under the terms of the GNU Lesser General Public
/// License as published by the Free Software Foundation; either
/// version 2.1 of the License, or (at your option) any later
/// version.
///
/// This library is distributed in the hope that it will be
/// useful, but WITHOUT ANY WARRANTY; without even the implied
/// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
/// PURPOSE.  See the GNU Lesser General Public License for more
/// details.
///
/// You should have received a copy of the GNU Lesser General Public
/// License along with this library; if not, write to the Free Software
/// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
///
/// @author: Tim Gebhardt<tim@gebhardtcomputing.com> 
/// @version 1.0
**/
namespace MemCached.clientlib
{
	using System;
	using System.Collections;

	using log4net;

	/// <summary>
	/// This is a generic logger class for use in logging.
	///
	/// This can easily be swapped out for any other logging package in the main code.
	/// For now, this is just a quick and dirty logger which will allow you to specify
	/// log levels, but only wraps system.out.println.
	/// </summary>
	public class Logger
	{
		public static readonly int LEVEL_DEBUG   = 0;
		public static readonly int LEVEL_INFO    = 1;
		public static readonly int LEVEL_WARN    = 2;
		public static readonly int LEVEL_ERROR   = 3;
		public static readonly int LEVEL_FATAL   = 4;

		private string name;
		private bool initialized = false;

		private ILog Log;

		protected Logger(string name, int level) 
		{
			this.name  = name;
			this.initialized = true;
			Log = LogManager.GetLogger(name);
		}

		protected Logger(string name) 
		{
			this.name  = name;
			this.initialized = true;
			Log = LogManager.GetLogger(name);
		}

		/// <summary>
		/// Gets a Logger obj for given name and level. 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="level"></param>
		/// <returns></returns>
		public static Logger getLogger(string name, int level) 
		{
			Logger log = new Logger(name, level);

			return log;
		}

		/// <summary>
		/// Gets a Logger obj for given name
		/// and sets default level. 
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public static Logger getLogger(string name) 
		{
			Logger log = new Logger(name);

			return log;
		}

		/// <summary>
		/// logs a debug mesg 
		/// </summary>
		/// <param name="mesg"></param>
		/// <param name="ex"></param>
		public void Debug(string mesg, Exception ex) 
		{
			Log.Debug(mesg, ex);
		}

		/// <summary>
		/// Logs a debug mesg
		/// </summary>
		/// <param name="mesg"></param>
		public void Debug(string mesg) 
		{
			Debug(mesg, null);
		}

		/// <summary>
		/// Logs an info mesg
		/// </summary>
		/// <param name="mesg"></param>
		/// <param name="ex"></param>
		public void Info(string mesg, Exception ex) 
		{
			Log.Info(mesg, ex);
		}

		/// <summary>
		/// Logs an info mesg
		/// </summary>
		/// <param name="mesg"></param>
		public void Info(string mesg) 
		{
			Info(mesg, null);
		}

		/// <summary>
		/// Logs a warn mesg
		/// </summary>
		/// <param name="mesg"></param>
		/// <param name="ex"></param>
		public void Warn(string mesg, Exception ex) 
		{
			Log.Warn(mesg, ex);
		}

		/// <summary>
		/// Logs a warn mesg
		/// </summary>
		/// <param name="mesg"></param>
		public void Warn(string mesg) 
		{
			Warn(mesg, null);
		}

		/// <summary>
		/// Logs an error mesg
		/// </summary>
		/// <param name="mesg"></param>
		/// <param name="ex"></param>
		public void Error(string mesg, Exception ex) 
		{
			Log.Error(mesg, ex);
		}

		/// <summary>
		/// Logs an error mesg
		/// </summary>
		/// <param name="mesg"></param>
		public void Error(string mesg) 
		{
			Error(mesg, null);
		}

		/// <summary>
		/// Logs a fatal mesg
		/// </summary>
		/// <param name="mesg"></param>
		/// <param name="ex"></param>
		public void Fatal(string mesg, Exception ex) 
		{
			Log.Fatal(mesg, ex);
		}

		/// <summary>
		/// logs a fatal mesg
		/// </summary>
		/// <param name="mesg"></param>
		public void Fatal(string mesg) 
		{
			Fatal(mesg, null);
		}
	}
}