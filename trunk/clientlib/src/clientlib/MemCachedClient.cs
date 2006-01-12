/**
/// MemCached C# client
/// Copyright (c) 2005
///
/// This module is Copyright (c) 2005 Tim Gebhardt
/// All rights reserved.
/// Based on code written by Greg Whalin and Richard Russo
/// for a Java MemCached client which can be found here:
/// http://www.whalin.com/memcached/
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
/// @author Tim Gebhardt <tim@gebhardtcomputing.com>
/// @version 1.0
**/
namespace MemCached.clientlib
{
	using System;
	using System.Collections;
	using System.IO;
	using System.Runtime.Serialization;
	using System.Runtime.Serialization.Formatters.Binary;
	using System.Text;
	using System.Text.RegularExpressions;

	using log4net;
	using ICSharpCode.SharpZipLib.GZip;

	/// <summary>
	/// This is a C# client for the memcached server available from
	/// <a href="http:/www.danga.com/memcached/">http://www.danga.com/memcached/</a>.
	///
	/// Supports setting, adding, replacing, deleting compressed/uncompressed and
	/// serialized (can be stored as string if object is native class) objects to memcached.
	///
	/// Now pulls SockIO objects from SockIOPool, which is a connection pool.  The server failover
	/// has also been moved into the SockIOPool class.
	/// This pool needs to be initialized prior to the client working.  See javadocs from SockIOPool.
	/// (This will have to be fixed for our C# version.  Most of this code is straight ported over from Java.)
	/// </summary>
	/// <example>
	/// //***To create cache client object and set params:***
	/// MemCachedClient mc = new MemCachedClient();
	/// 
	/// // compression is enabled by default	
	/// mc.setCompressEnable(true);
	///
	///	// set compression threshhold to 4 KB (default: 15 KB)	
	///	mc.setCompressThreshold(4096);
	///
	///	// turn on storing primitive types as a string representation
	///	// Should not do this in most cases.	
	///	mc.setPrimitiveAsString(true);
	/// 
	/// 
	/// //***To store an object:***
	/// MemCachedClient mc = new MemCachedClient();
	/// string key   = "cacheKey1";	
	/// object value = SomeClass.getObject();	
	/// mc.set(key, value);
	/// 
	/// 
	/// //***To store an object using a custom server hashCode:***
	/// //The set method shown here will always set the object in the cache.
	/// //The add and replace methods do the same, but with a slight difference.
	/// //  add -- will store the object only if the server does not have an entry for this key
	/// //  replace -- will store the object only if the server already has an entry for this key
	///	MemCachedClient mc = new MemCachedClient();
	///	string key   = "cacheKey1";	
	///	object value = SomeClass.getObject();	
	///	int hash = 45;
	///	mc.set(key, value, hash);
	/// 
	/// 
	/// //***To delete a cache entry:***
	/// MemCachedClient mc = new MemCachedClient();
	/// string key   = "cacheKey1";	
	/// mc.delete(key);
	/// 
	/// 
	/// //***To delete a cache entry using a custom hash code:***
	/// MemCachedClient mc = new MemCachedClient();
	/// string key   = "cacheKey1";	
	/// int hash = 45;
	/// mc.delete(key, hashCode);
	/// 
	/// 
	/// //***To store a counter and then increment or decrement that counter:***
	/// MemCachedClient mc = new MemCachedClient();
	/// string key   = "counterKey";	
	/// mc.storeCounter(key, 100);
	/// Console.WriteLine("counter after adding      1: " mc.incr(key));	
	/// Console.WriteLine("counter after adding      5: " mc.incr(key, 5));	
	/// Console.WriteLine("counter after subtracting 4: " mc.decr(key, 4));	
	/// Console.WriteLine("counter after subtracting 1: " mc.decr(key));	
	/// 
	/// 
	/// //***To store a counter and then increment or decrement that counter with custom hash:***
	/// MemCachedClient mc = new MemCachedClient();
	/// string key   = "counterKey";	
	/// int hash = 45;	
	/// mc.storeCounter(key, 100, hash);
	/// Console.WriteLine("counter after adding      1: " mc.incr(key, 1, hash));	
	/// Console.WriteLine("counter after adding      5: " mc.incr(key, 5, hash));	
	/// Console.WriteLine("counter after subtracting 4: " mc.decr(key, 4, hash));	
	/// Console.WriteLine("counter after subtracting 1: " mc.decr(key, 1, hash));	
	/// 
	/// 
	/// //***To retrieve an object from the cache:***
	/// MemCachedClient mc = new MemCachedClient();
	/// string key   = "key";	
	/// object value = mc.get(key);	
	///
	///
	/// //***To retrieve an object from the cache with custom hash:***
	/// MemCachedClient mc = new MemCachedClient();
	/// string key   = "key";	
	/// int hash = 45;	
	/// object value = mc.get(key, hash);
	/// 
	/// 
	/// //***To retrieve an multiple objects from the cache***
	/// MemCachedClient mc = new MemCachedClient();
	/// string[] keys   = { "key", "key1", "key2" };
	/// object value = mc.getMulti(keys);
	/// 
	///
	/// //***To retrieve an multiple objects from the cache with custom hashing***
	/// MemCachedClient mc = new MemCachedClient();
	/// string[] keys    = { "key", "key1", "key2" };
	/// int[] hashes = { 45, 32, 44 };
	/// object value = mc.getMulti(keys, hashes);
	/// 
	///
	/// //***To flush all items in server(s)***
	/// MemCachedClient mc = new MemCachedClient();
	/// mc.FlushAll();
	/// 
	///
	/// //***To get stats from server(s)***
	/// MemCachedClient mc = new MemCachedClient();
	/// Hashtable stats = mc.stats();
	/// </example>
	public class MemCachedClient 
	{

		// logger
		private static Logger log =
			Logger.getLogger(typeof(MemCachedClient).Name);

		// return codes
		private static readonly string VALUE        = "VALUE";			// start of value line from server
		private static readonly string STATS        = "STAT";			// start of stats line from server
		private static readonly string DELETED      = "DELETED";		// successful deletion
		private static readonly string NOTFOUND     = "NOT_FOUND";		// record not found for delete or incr/decr
		private static readonly string STORED       = "STORED";		// successful store of data
		private static readonly string NOTSTORED    = "NOT_STORED";	// data not stored
		private static readonly string OK           = "OK";			// success
		private static readonly string END          = "END";			// end of data from server
		private static readonly string ERROR        = "ERROR";			// invalid command name from client
		private static readonly string CLIENT_ERROR = "CLIENT_ERROR";	// client error in input line - invalid protocol
		private static readonly string SERVER_ERROR = "SERVER_ERROR";	// server error

		// default compression threshold
		private static readonly int COMPRESS_THRESH = 30720;
    
		// values for cache flags 
		//
		// using 8 (1 << 3) so other clients don't try to unpickle/unstore/whatever
		// things that are serialized... I don't think they'd like it. :)
		private static readonly int F_COMPRESSED = 2;
		private static readonly int F_SERIALIZED = 8;
	
		// flags
		private bool primitiveAsString;
		private bool compressEnable;
		private long compressThreshold;
		private string defaultEncoding;

		// which pool to use
		private string poolName;

		/// <summary>
		/// Creates a new instance of MemCachedClient.
		/// </summary>
		public MemCachedClient() 
		{
			Init();
		}

		/// <summary>
		/// Initializes client object to defaults.
		/// 
		/// This enables compression and sets compression threshhold to 15 KB.
		/// </summary>
		private void Init() 
		{
			this.primitiveAsString   = false;
			this.compressEnable      = true;
			this.compressThreshold   = COMPRESS_THRESH;
			this.defaultEncoding     = "UTF-8";
			this.poolName            = "default";
		}

		/// <summary>
		/// Sets the pool that this instance of the client will use.
		/// The pool must already be initialized or none of this will work.
		/// </summary>
		/// <param name="poolName">name of the pool to use</param>
		public void SetPoolName( string poolName ) 
		{
			this.poolName = poolName;
		}

		/// <summary>
		/// Enables storing primitive types as their string values. 
		/// </summary>
		public bool PrimitiveAsString
		{
			get { return this.primitiveAsString; }
			set { this.primitiveAsString = value; }
		}

		/// <summary>
		/// Sets default string encoding when storing primitives as strings. 
		/// Default is UTF-8.
		/// </summary>
		public string DefaultEncoding
		{
			get { return this.defaultEncoding; }
			set { this.defaultEncoding = value; }
		}

		/// <summary>
		/// Enable storing compressed data, provided it meets the threshold requirements.
		/// 
		/// If enabled, data will be stored in compressed form if it is
		/// longer than the threshold length set with setCompressThreshold(int)
		/// 
		/// The default is that compression is enabled.
		/// 
		/// Even if compression is disabled, compressed data will be automatically
		/// decompressed.
		/// </summary>
		/// <value><c>true</c> to enable compuression, <c>false</c> to disable compression</value>
		public bool EnableCompression
		{
			get { return this.compressEnable; }
			set { this.compressEnable = value; }
		}

		/// <summary>
		/// Sets the required length for data to be considered for compression.
		/// 
		/// If the length of the data to be stored is not equal or larger than this value, it will
		/// not be compressed.
		/// 
		/// This defaults to 15 KB.
		/// </summary>
		/// <value>required length of data to consider compression</value>
		public long CompressionThreshold
		{
			get { return this.compressThreshold; }
			set { this.compressThreshold = value; }
		}

		/// <summary>
		/// Checks to see if key exists in cache. 
		/// </summary>
		/// <param name="key">the key to look for</param>
		/// <returns><c>true</c> if key found in cache, <c>false</c> if not (or if cache is down)</returns>
		public bool KeyExists( string key ) 
		{
			return ( this.Get( key, null, true ) != null );
		}
	
		/// <summary>
		/// Deletes an object from cache given cache key.
		/// </summary>
		/// <param name="key">the key to be removed</param>
		/// <returns><c>true</c>, if the data was deleted successfully</returns>
		public bool Delete( string key ) 
		{
			return Delete( key, null, DateTime.MaxValue );
		}

		/// <summary>
		/// Deletes an object from cache given cache key and expiration date. 
		/// </summary>
		/// <param name="key">the key to be removed</param>
		/// <param name="expiry">when to expire the record.</param>
		/// <returns><c>true</c>, if the data was deleted successfully</returns>
		public bool Delete( string key, DateTime expiry ) 
		{
			return Delete( key, null, expiry );
		}

		/// <summary>
		/// Deletes an object from cache given cache key, a delete time, and an optional hashcode.
		/// 
		/// The item is immediately made non retrievable.<br/>
		/// Keep in mind: 
		/// <see cref="add">add(string, object)</see> and <see cref="replace">replace(string, object)</see>
		///	will fail when used with the same key will fail, until the server reaches the
		///	specified time. However, <see cref="set">set(string, object)</see> will succeed
		/// and the new value will not be deleted.
		/// </summary>
		/// <param name="key">the key to be removed</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <param name="expiry">when to expire the record.</param>
		/// <returns><c>true</c>, if the data was deleted successfully</returns>
		public bool Delete( string key, object hashCode, DateTime expiry ) 
		{

			if ( key == null ) 
			{
				log.Error( "null value for key passed to delete()" );
				return false;
			}

			// get SockIO obj from hash or from key
			SockIOPool.SockIO sock = SockIOPool.getInstance( poolName ).getSock( key, hashCode );

			// return false if unable to get SockIO obj
			if ( sock == null )
				return false;

			// build command
			StringBuilder command = new StringBuilder( "delete " ).Append( key );
			if ( expiry != DateTime.MaxValue )
				command.Append( " " + GetExpirationTime(expiry) / 1000 );

			command.Append( "\r\n" );
		
			try 
			{
				sock.Write( UTF8Encoding.UTF8.GetBytes(command.ToString()) );
				sock.Flush();
			
				// if we get appropriate response back, then we return true
				string line = sock.ReadLine();
				if ( DELETED == line ) 
				{
					log.Info( "++++ deletion of key: " + key + " from cache was a success" );

					// return sock to pool and bail here
					sock.Close();
					sock = null;
					return true;
				}
				else if ( NOTFOUND == line ) 
				{
					log.Info( "++++ deletion of key: " + key + " from cache failed as the key was not found" );
				}
				else 
				{
					log.Error( "++++ error deleting key: " + key );
					log.Error( line );
				}
			}
			catch ( IOException e) 
			{
				// exception thrown
				log.Error( "++++ exception thrown while writing bytes to server on delete" );
				log.Error( e.Message, e );

				try 
				{
					sock.TrueClose();
				}
				catch ( IOException ) 
				{
					log.Error( "++++ failed to close socket : " + sock.ToString() );
				}

				sock = null;
			}

			if ( sock != null )
				sock.Close();

			return false;
		}

		/// <summary>
		/// Converts a .NET date time to a UNIX timestamp
		/// </summary>
		/// <param name="ticks"></param>
		/// <returns></returns>
		private int GetExpirationTime(DateTime expiration)
		{
			if(expiration <= new DateTime(1970, 1, 1))
				return 0;

			TimeSpan thirtyDays = new TimeSpan(29, 23, 59, 59);
			if(expiration.Subtract(DateTime.Now) > thirtyDays)
				return (int)thirtyDays.TotalSeconds;
			
			return (int)expiration.Subtract(DateTime.Now).TotalSeconds;
		}
    
		/// <summary>
		/// Stores data on the server; only the key and the value are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Set( string key, object value ) 
		{
			return Set( "set", key, value, DateTime.MaxValue, null, primitiveAsString );
		}

		/// <summary>
		/// Stores data on the server; only the key and the value are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Set( string key, object value, int hashCode ) 
		{
			return Set( "set", key, value, DateTime.MaxValue, hashCode, primitiveAsString );
		}

		/// <summary>
		/// Stores data on the server; the key, value, and an expiration time are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="expiry">when to expire the record</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Set( string key, object value, DateTime expiry ) 
		{
			return Set( "set", key, value, expiry, null, primitiveAsString );
		}

		/// <summary>
		/// Stores data on the server; the key, value, and an expiration time are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="expiry">when to expire the record</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Set( string key, object value, DateTime expiry, int hashCode ) 
		{
			return Set( "set", key, value, expiry, hashCode, primitiveAsString );
		}

		/// <summary>
		/// Adds data to the server; only the key and the value are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Add( string key, object value ) 
		{
			return Set( "add", key, value, DateTime.MaxValue, null, primitiveAsString );
		}

		/// <summary>
		/// Adds data to the server; the key, value, and an optional hashcode are passed in.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Add( string key, object value, int hashCode ) 
		{
			return Set( "add", key, value, DateTime.MaxValue, hashCode, primitiveAsString );
		}

		/// <summary>
		/// Adds data to the server; the key, value, and an expiration time are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="expiry">when to expire the record</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Add( string key, object value, DateTime expiry ) 
		{
			return Set( "add", key, value, expiry, null, primitiveAsString );
		}

		/// <summary>
		/// Adds data to the server; the key, value, and an expiration time are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="expiry">when to expire the record</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Add( string key, object value, DateTime expiry, int hashCode ) 
		{
			return Set( "add", key, value, expiry, hashCode, primitiveAsString );
		}

		/// <summary>
		/// Updates data on the server; only the key and the value are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Replace( string key, object value ) 
		{
			return Set( "replace", key, value, DateTime.MaxValue, null, primitiveAsString );
		}

		/// <summary>
		/// Updates data on the server; only the key and the value and an optional hash are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Replace( string key, object value, int hashCode ) 
		{
			return Set( "replace", key, value, DateTime.MaxValue, hashCode, primitiveAsString );
		}

		/// <summary>
		/// Updates data on the server; the key, value, and an expiration time are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="expiry">when to expire the record</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Replace( string key, object value, DateTime expiry ) 
		{
			return Set( "replace", key, value, expiry, null, primitiveAsString );
		}

		/// <summary>
		/// Updates data on the server; the key, value, and an expiration time are specified.
		/// </summary>
		/// <param name="key">key to store data under</param>
		/// <param name="value">value to store</param>
		/// <param name="expiry">when to expire the record</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>true, if the data was successfully stored</returns>
		public bool Replace( string key, object value, DateTime expiry, int hashCode ) 
		{
			return Set( "replace", key, value, expiry, hashCode, primitiveAsString );
		}

		/// <summary>
		/// Stores data to cache.
		/// 
		/// If data does not already exist for this key on the server, or if the key is being
		/// deleted, the specified value will not be stored.
		/// The server will automatically delete the value when the expiration time has been reached.
		/// 
		/// If compression is enabled, and the data is longer than the compression threshold
		/// the data will be stored in compressed form.
		/// 
		/// As of the current release, all objects stored will use .NET serialization.
		/// </summary>
		/// <param name="cmdname">action to take (set, add, replace)</param>
		/// <param name="key">key to store cache under</param>
		/// <param name="value">object to cache</param>
		/// <param name="expiry">expiration</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <param name="asString">store this object as a string?</param>
		/// <returns>true/false indicating success</returns>
		private bool Set( string cmdname, string key, object obj, DateTime expiry, object hashCode, bool asString ) 
		{

			if ( cmdname == null || cmdname.Trim() == "" || key == null ) 
			{
				log.Error( "key is null or cmd is null/empty for set()" );
				return false;
			}

			// get SockIO obj
			SockIOPool.SockIO sock = SockIOPool.getInstance( poolName ).getSock( key, hashCode );
		
			if ( sock == null )
				return false;
		
			if ( expiry == DateTime.MaxValue )
				expiry = new DateTime(0);

			// store flags
			int flags = 0;
		
			// byte array to hold data
			byte[] val;

			if ( NativeHandler.isHandled( obj ) ) 
			{
			
				if ( asString ) 
				{
					// useful for sharing data between java and non-java
					// and also for storing ints for the increment method
					log.Info( "++++ storing data as a string for key: " + key + " for class: " + obj.GetType().Name );
					try 
					{
						val = UTF8Encoding.UTF8.GetBytes(obj.ToString());
					}
					catch ( Exception ) 
					{
						log.Error( "invalid encoding type used: " + defaultEncoding );
						sock.Close();
						sock = null;
						return false;
					}
				}
				else 
				{
					log.Info( "Storing with native handler..." );

					try 
					{
						val = NativeHandler.encode( obj );
					}
					catch ( Exception e ) 
					{
						log.Error( "Failed to native handle obj", e );

						sock.Close();
						sock = null;
						return false;
					}
				}
			}
			else 
			{
				// always serialize for non-primitive types
				log.Info( "++++ serializing for key: " + key + " for class: " + obj.GetType().Name );
				try 
				{
					MemoryStream memStream = new MemoryStream();
					new BinaryFormatter().Serialize(memStream, obj);
					val = memStream.GetBuffer();
					flags |= F_SERIALIZED;
				}
				catch ( IOException e ) 
				{
					// if we fail to serialize, then
					// we bail
					log.Error( "failed to serialize obj", e );
					log.Error( obj.ToString() );

					// return socket to pool and bail
					sock.Close();
					sock = null;
					return false;
				}
			}
		
			// now try to compress if we want to
			// and if the length is over the threshold 
			if ( compressEnable && val.Length > compressThreshold ) 
			{
				log.Info( "++++ trying to compress data" );
				log.Info( "++++ size prior to compression: " + val.Length );

				try 
				{
					MemoryStream memoryStream = new MemoryStream();
					GZipOutputStream gos = new GZipOutputStream( memoryStream );
					gos.Write( val, 0, val.Length );
					gos.Finish();
				
					// store it and set compression flag
					val = memoryStream.GetBuffer();
					flags |= F_COMPRESSED;

					log.Info( "++++ compression succeeded, size after: " + val.Length );
				}
				catch (IOException e) 
				{
					log.Error( "IOException while compressing stream: " + e.ToString() );
					log.Error( "storing data uncompressed" );
				}
			}

			// now write the data to the cache server
			try 
			{
				string cmd = cmdname + " " + key + " " + flags + " "
					+ GetExpirationTime(expiry) + " " + val.Length + "\r\n";
				sock.Write( UTF8Encoding.UTF8.GetBytes( cmd ) );
				sock.Write( val );
				sock.Write( UTF8Encoding.UTF8.GetBytes( "\r\n" ) );
				sock.Flush();

				// get result code
				string line = sock.ReadLine();
				log.Info( "++++ memcache cmd (result code): " + cmd + " (" + line + ")" );

				if ( STORED == line ) 
				{
					log.Info("++++ data successfully stored for key: " + key );
					sock.Close();
					sock = null;
					return true;
				}
				else if ( NOTSTORED == line ) 
				{
					log.Info( "++++ data not stored in cache for key: " + key );
				}
				else 
				{
					log.Error( "++++ error storing data in cache for key: " + key + " -- length: " + val.Length);
					log.Error( line );
				}
			}
			catch ( IOException e ) 
			{
				// exception thrown
				log.Error( "++++ exception thrown while writing bytes to server on delete" );
				log.Error( e.ToString(), e );

				try 
				{
					sock.TrueClose();
				}
				catch ( IOException ) 
				{
					log.Error( "++++ failed to close socket : " + sock.ToString() );
				}

				sock = null;
			}

			if ( sock != null )
				sock.Close();

			return false;
		}

		/// <summary>
		/// Store a counter to memcached given a key
		/// </summary>
		/// <param name="key">cache key</param>
		/// <param name="counter">number to store</param>
		/// <returns>true/false indicating success</returns>
		public bool StoreCounter( string key, long counter ) 
		{
			return Set( "set", key, counter, DateTime.MaxValue, null, true );
		}
    
		/// <summary>
		/// Store a counter to memcached given a key
		/// </summary>
		/// <param name="key">cache key</param>
		/// <param name="counter">number to store</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>true/false indicating success</returns>
		public bool StoreCounter( string key, long counter, int hashCode ) 
		{
			return Set( "set", key, counter, DateTime.MaxValue, hashCode, true );
		}

		/// <summary>
		/// Returns value in counter at given key as long. 
		/// </summary>
		/// <param name="key">cache ket</param>
		/// <returns>counter value or -1 if not found</returns>
		public long GetCounter( string key ) 
		{
			return GetCounter( key, null );
		}

		/// <summary>
		/// Returns value in counter at given key as long. 
		/// </summary>
		/// <param name="key">cache ket</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>counter value or -1 if not found</returns>
		public long GetCounter( string key, object hashCode ) 
		{

			if ( key == null ) 
			{
				log.Error( "null key for getCounter()" );
				return -1;
			}

			long counter = -1;
			try 
			{
				counter = long.Parse( (string)Get( key, hashCode, true ) );
			}
			catch ( Exception ) 
			{
				// not found or error getting out
				log.Error( "counter not found at key: " + key );
			}
		
			return counter;
		}

		/// <summary>
		/// Increment the value at the specified key by 1, and then return it.
		/// </summary>
		/// <param name="key">key where the data is stored</param>
		/// <returns>-1, if the key is not found, the value after incrementing otherwise</returns>
		public long Increment( string key ) 
		{
			return IncrementOrDecrement( "incr", key, 1, null );
		}

		/// <summary>
		/// Increment the value at the specified key by passed in val. 
		/// </summary>
		/// <param name="key">key where the data is stored</param>
		/// <param name="inc">how much to increment by</param>
		/// <returns>-1, if the key is not found, the value after incrementing otherwise</returns>
		public long Increment( string key, long inc ) 
		{
			return IncrementOrDecrement( "incr", key, inc, null );
		}

		/// <summary>
		/// Increment the value at the specified key by the specified increment, and then return it.
		/// </summary>
		/// <param name="key">key where the data is stored</param>
		/// <param name="inc">how much to increment by</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>-1, if the key is not found, the value after incrementing otherwise</returns>
		public long Increment( string key, long inc, int hashCode ) 
		{
			return IncrementOrDecrement( "incr", key, inc, hashCode );
		}
	
		/// <summary>
		/// Decrement the value at the specified key by 1, and then return it.
		/// </summary>
		/// <param name="key">key where the data is stored</param>
		/// <returns>-1, if the key is not found, the value after incrementing otherwise</returns>
		public long Decrement( string key ) 
		{
			return IncrementOrDecrement( "decr", key, 1, null );
		}

		/// <summary>
		/// Decrement the value at the specified key by passed in value, and then return it.
		/// </summary>
		/// <param name="key">key where the data is stored</param>
		/// <param name="inc">how much to increment by</param>
		/// <returns>-1, if the key is not found, the value after incrementing otherwise</returns>
		public long Decrement( string key, long inc ) 
		{
			return IncrementOrDecrement( "decr", key, inc, null );
		}

		/// <summary>
		/// Decrement the value at the specified key by the specified increment, and then return it.
		/// </summary>
		/// <param name="key">key where the data is stored</param>
		/// <param name="inc">how much to increment by</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>-1, if the key is not found, the value after incrementing otherwise</returns>
		public long Decrement( string key, long inc, int hashCode ) 
		{
			return IncrementOrDecrement( "decr", key, inc, hashCode );
		}

		/// <summary>
		/// Increments/decrements the value at the specified key by inc.
		/// 
		/// Note that the server uses a 32-bit unsigned integer, and checks for
		/// underflow. In the event of underflow, the result will be zero.  Because
		/// Java lacks unsigned types, the value is returned as a 64-bit integer.
		/// The server will only decrement a value if it already exists;
		/// if a value is not found, -1 will be returned.
		/// 
		/// TODO: C# has unsigned types.  We can fix this.
		/// </summary>
		/// <param name="cmdname">increment/decrement</param>
		/// <param name="key">cache key</param>
		/// <param name="inc">amount to incr or decr</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>new value or -1 if not exist</returns>
		private long IncrementOrDecrement(string cmdname, string key, long inc, object hashCode) 
		{

			// get SockIO obj for given cache key
			SockIOPool.SockIO sock = SockIOPool.getInstance( poolName ).getSock(key, hashCode);

			if (sock == null)
				return -1;
		
			try 
			{
				string cmd = cmdname + " " + key + " " + inc + "\r\n";
				log.Debug("++++ memcache incr/decr command: " + cmd);

				sock.Write(UTF8Encoding.UTF8.GetBytes( cmd ));
				sock.Flush();

				// get result back
				string line = sock.ReadLine();

				if (new Regex("\\d+").Match(line).Success) 
				{

					// return sock to pool and return result
					sock.Close();
					return long.Parse(line);

				} 
				else if (NOTFOUND == line) 
				{
					log.Info("++++ key not found to incr/decr for key: " + key);

				} 
				else 
				{
					log.Error("error incr/decr key: " + key);
				}
			}
			catch (IOException e) 
			{
				// exception thrown
				log.Error("++++ exception thrown while writing bytes to server on incr/decr");
				log.Error(e.ToString(), e);

				try 
				{
					sock.TrueClose();
				}
				catch(IOException) 
				{
					log.Error("++++ failed to close socket : " + sock.ToString());
				}

				sock = null;
			}
		
			if (sock != null)
				sock.Close();
			return -1;
		}

		/// <summary>
		/// Retrieve a key from the server, using a specific hash.
		/// 
		/// If the data was compressed or serialized when compressed, it will automatically
		/// be decompressed or serialized, as appropriate. (Inclusive or)
		/// 
		/// Non-serialized data will be returned as a string, so explicit conversion to
		/// numeric types will be necessary, if desired
		/// </summary>
		/// <param name="key">key where data is stored</param>
		/// <returns>the object that was previously stored, or null if it was not previously stored</returns>
		public object Get(string key) 
		{
			return Get( key, null, false );
		}

		/// <summary>
		/// Retrieve a key from the server, using a specific hash.
		/// 
		/// If the data was compressed or serialized when compressed, it will automatically
		/// be decompressed or serialized, as appropriate. (Inclusive or)
		/// 
		/// Non-serialized data will be returned as a string, so explicit conversion to
		/// numeric types will be necessary, if desired
		/// </summary>
		/// <param name="key">key where data is stored</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>the object that was previously stored, or null if it was not previously stored</returns>
		public object Get( string key, int hashCode ) 
		{
			return Get( key, hashCode, false );
		}

		/// <summary>
		/// Retrieve a key from the server, using a specific hash.
		/// 
		/// If the data was compressed or serialized when compressed, it will automatically
		/// be decompressed or serialized, as appropriate. (Inclusive or)
		/// 
		/// Non-serialized data will be returned as a string, so explicit conversion to
		/// numeric types will be necessary, if desired
		/// </summary>
		/// <param name="key">key where data is stored</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <param name="asString">if true, then return string val</param>
		/// <returns>the object that was previously stored, or null if it was not previously stored</returns>
		public object Get( string key, object hashCode, bool asString ) 
		{

			// get SockIO obj using cache key
			SockIOPool.SockIO sock = SockIOPool.getInstance( poolName ).getSock(key, hashCode);
	    
			if (sock == null)
				return null;

			try 
			{
				string cmd = "get " + key + "\r\n";
				log.Debug("++++ memcache get command: " + cmd);

				sock.Write( UTF8Encoding.UTF8.GetBytes(cmd) );
				sock.Flush();

				// build empty map
				// and fill it from server
				Hashtable hm = new Hashtable();
				LoadItems( sock, hm, asString );

				// debug code
				log.Debug("++++ memcache: got back " + hm.Count + " results");

				// return the value for this key if we found it
				// else return null 
				sock.Close();
				return hm[key];

			}
			catch (IOException e) 
			{
				// exception thrown
				log.Error("++++ exception thrown while trying to get object from cache for key: " + key);
				log.Error(e.ToString(), e);

				try 
				{
					sock.TrueClose();
				}
				catch(IOException) 
				{
					log.Error("++++ failed to close socket : " + sock.ToString());
				}
				sock = null;
			}

			if (sock != null)
				sock.Close();

			return null;
		}

		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to <see cref="get">get(string)</see>, since it
		/// is more efficient.
		/// </summary>
		/// <param name="keys">string array of keys to retrieve</param>
		/// <returns>object array ordered in same order as key array containing results</returns>
		public object[] GetMultiArray( string[] keys ) 
		{
			return GetMultiArray( keys, null, false );
		}

		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to <see cref="get">get(string)</see>, since it
		/// is more efficient.
		/// </summary>
		/// <param name="keys">string array of keys to retrieve</param>
		/// <param name="hashCodes">if not null, then the int array of hashCodes</param>
		/// <returns>object array ordered in same order as key array containing results</returns>
		public object[] GetMultiArray( string[] keys, int[] hashCodes ) 
		{
			return GetMultiArray( keys, hashCodes, false );
		}

		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to <see cref="get">get(string)</see>, since it
		/// is more efficient.
		/// </summary>
		/// <param name="keys">string array of keys to retrieve</param>
		/// <param name="hashCodes">if not null, then the int array of hashCodes</param>
		/// <param name="asString">asString if true, retrieve string vals</param>
		/// <returns>object array ordered in same order as key array containing results</returns>
		public object[] GetMultiArray( string[] keys, int[] hashCodes, bool asString ) 
		{

			Hashtable data = GetMulti( keys, hashCodes, asString );

			object[] res = new object[keys.Length];
			for (int i = 0; i < keys.Length; i++) 
			{
				res[i] = data[ keys[i] ];
			}

			return res;
		}

		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to <see cref="get">get(string)</see>, since it
		/// is more efficient.
		/// </summary>
		/// <param name="keys">string array of keys to retrieve</param>
		/// <returns>
		/// a hashmap with entries for each key is found by the server,
		/// keys that are not found are not entered into the hashmap, but attempting to
		/// retrieve them from the hashmap gives you null.
		/// </returns>
		public Hashtable GetMulti( string[] keys ) 
		{
			return GetMulti( keys, null, false );
		}
    
		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to <see cref="get">get(string)</see>, since it
		/// is more efficient.
		/// </summary>
		/// <param name="keys">string array of keys to retrieve</param>
		/// <param name="hashCodes">hashCodes if not null, then the int array of hashCodes</param>
		/// <returns>
		/// a hashmap with entries for each key is found by the server,
		/// keys that are not found are not entered into the hashmap, but attempting to
		/// retrieve them from the hashmap gives you null.
		/// </returns>
		public Hashtable GetMulti( string[] keys, int[] hashCodes ) 
		{
			return GetMulti( keys, hashCodes, false );
		}

		/// <summary>
		/// Retrieve multiple objects from the memcache.
		/// 
		/// This is recommended over repeated calls to <see cref="get">get(string)</see>, since it
		/// is more efficient.
		/// </summary>
		/// <param name="keys">string array of keys to retrieve</param>
		/// <param name="hashCodes">hashCodes if not null, then the int array of hashCodes</param>
		/// <param name="asString">if true then retrieve using string val</param>
		/// <returns>
		/// a hashmap with entries for each key is found by the server,
		/// keys that are not found are not entered into the hashmap, but attempting to
		/// retrieve them from the hashmap gives you null.
		/// </returns>
		public Hashtable GetMulti( string[] keys, int[] hashCodes, bool asString ) 
		{
			Hashtable sockKeys = new Hashtable();

			for (int i = 0; i < keys.Length; ++i) 
			{

				object hash = null;
				if ( hashCodes != null && hashCodes.Length > i )
					hash = hashCodes[i];

				// get SockIO obj from cache key
				SockIOPool.SockIO sock = SockIOPool.getInstance( poolName ).getSock(keys[i], hash);

				if (sock == null)
					continue;

				// store in map and list if not already
				if ( !sockKeys.ContainsKey( sock.Host ) )
					sockKeys[ sock.Host ] = new StringBuilder();

				((StringBuilder)sockKeys[ sock.Host ]).Append( " " + keys[i] );

				// return to pool
				sock.Close();
			}
		
			log.Info( "multi get socket count : " + sockKeys.Count );

			// now query memcache
			Hashtable ret = new Hashtable();
			ArrayList toRemove = new ArrayList();
			foreach (string host in sockKeys.Keys) 
			{
				// get SockIO obj from hostname
				SockIOPool.SockIO sock = SockIOPool.getInstance( poolName ).getConnection(host);

				try 
				{
					string cmd = "get" + (StringBuilder) sockKeys[ host ] + "\r\n";
					log.Debug( "++++ memcache getMulti cmd: " + cmd );
					sock.Write( UTF8Encoding.UTF8.GetBytes( cmd ) );
					sock.Flush();
					LoadItems( sock, ret, asString );
				}
				catch (IOException e) 
				{
					// exception thrown
					log.Error("++++ exception thrown while getting from cache on getMulti");
					log.Error(e.ToString(), e);

					// clear this sockIO obj from the list
					// and from the map containing keys
					toRemove.Add(host);
					try 
					{
						sock.TrueClose();
					}
					catch(IOException) 
					{
						log.Error("++++ failed to close socket : " + sock.ToString());
					}
					sock = null;
				}

				// Return socket to pool
				if (sock != null)
					sock.Close();
			}

			foreach(string host in toRemove)
			{
				sockKeys.Remove(host);
			}
		
			log.Debug("++++ memcache: got back " + ret.Count + " results");
			return ret;
		}
    
		/// <summary>
		/// This method loads the data from cache into a Hashtable.
		/// 
		/// Pass a SockIO object which is ready to receive data and a Hashtable
		/// to store the results.
		/// </summary>
		/// <param name="sock">socket waiting to pass back data</param>
		/// <param name="hm">hashmap to store data into</param>
		/// <param name="asString">if true, and if we are using NativehHandler, return string val</param>
		private void LoadItems( SockIOPool.SockIO sock, Hashtable hm, bool asString ) 
		{

			while ( true ) 
			{
				string line = sock.ReadLine();
				log.Debug( "++++ line: " + line );

				if (line.StartsWith(VALUE)) 
				{
					string[] info = line.Split(' ');
					string key    = info[1];
					int flag      = int.Parse(info[2]);
					int length    = int.Parse(info[3]);

					log.Debug("++++ key: " + key);
					log.Debug("++++ flags: " + flag);
					log.Debug("++++ length: " + length);
				
					// read obj into buffer
					byte[] buf = new byte[length];
					sock.Read(buf);
					sock.ClearEOL();

					// ready object
					object o;
				
					// check for compression
					if ((flag & F_COMPRESSED) != 0) 
					{
						try 
						{
							// read the input stream, and write to a byte array output stream since
							// we have to read into a byte array, but we don't know how large it
							// will need to be, and we don't want to resize it a bunch
							GZipInputStream gzi = new GZipInputStream(new MemoryStream(buf));
							MemoryStream bos = new MemoryStream(buf.Length);
							
							int count;
							byte[] tmp = new byte[2048];
							while ((count = gzi.Read(tmp, 0, tmp.Length)) > 0)
							{
								bos.Write(tmp, 0, count);
							}
							
							// store uncompressed back to buffer
							buf = bos.ToArray();
							gzi.Close();
						}
						catch (IOException e) 
						{
							log.Error("++++ IOException thrown while trying to uncompress input stream for key: " + key);
							log.Error(e.ToString(), e);
							throw new NestedIOException("++++ IOException thrown while trying to uncompress input stream for key: " + key, e);
						}
					}

					// we can only take out serialized objects
					if ((flag & F_SERIALIZED) == 0) 
					{
						if ( primitiveAsString || asString ) 
						{
							// pulling out string value
							log.Info("++++ retrieving object and stuffing into a string.");
							o = Encoding.GetEncoding(defaultEncoding).GetString(buf);
						}
						else 
						{
							// decoding object
							try 
							{
								o = NativeHandler.decode( buf );    
							}
							catch ( Exception e ) 
							{
								log.Error( "++++ Exception thrown while trying to deserialize for key: " + key, e );
								throw new NestedIOException( e );
							}
						}
					}
					else 
					{
						// deserialize if the data is serialized
						try 
						{
							MemoryStream memStream = new MemoryStream(buf);
							o = new BinaryFormatter().Deserialize(memStream);
							log.Info("++++ deserializing " + o.GetType().Name);
						}
						catch (SerializationException e) 
						{
							log.Error("++++ SerializationException thrown while trying to deserialize for key: " + key, e);
							throw new NestedIOException("+++ failed while trying to deserialize for key: " + key, e);
						}
					}

					// store the object into the cache
					hm[ key ] =  o ;
				}
				else if ( END == line ) 
				{
					log.Debug("++++ finished reading from cache server");
					break;
				}
			}
		}

		/// <summary>
		/// Invalidates the entire cache.
		/// 
		/// Will return true only if succeeds in clearing all servers.
		/// </summary>
		/// <returns>success true/false</returns>
		public bool FlushAll() 
		{
			return FlushAll(null);
		}

		/// <summary>
		/// Invalidates the entire cache.
		/// 
		/// Will return true only if succeeds in clearing all servers.
		/// If pass in null, then will try to flush all servers.
		/// </summary>
		/// <param name="servers">optional array of host(s) to flush (host:port)</param>
		/// <returns>success true/false</returns>
		public bool FlushAll(string[] servers) 
		{

			// get SockIOPool instance
			SockIOPool pool = SockIOPool.getInstance( poolName );

			// return false if unable to get SockIO obj
			if (pool == null) 
			{
				log.Error("++++ unable to get SockIOPool instance");
				return false;
			}

			// get all servers and iterate over them
			servers = (servers == null)
				? pool.Servers
				: servers;

			// if no servers, then return early
			if (servers == null || servers.Length <= 0) 
			{
				log.Error("++++ no servers to flush");
				return false;
			}

			bool success = true;

			for (int i = 0; i < servers.Length; i++) 
			{

				SockIOPool.SockIO sock = pool.getConnection(servers[i]);
				if (sock == null) 
				{
					log.Error("++++ unable to get connection to : " + servers[i]);
					success = false;
					continue;
				}

				// build command
				string command = "flush_all\r\n";

				try 
				{
					sock.Write(UTF8Encoding.UTF8.GetBytes( command ));
					sock.Flush();

					// if we get appropriate response back, then we return true
					string line = sock.ReadLine();
					success = (OK == line)
						? success && true
						: false;
				}
				catch (IOException e) 
				{
					// exception thrown
					log.Error("++++ exception thrown while writing bytes to server on delete");
					log.Error(e.ToString(), e);

					try 
					{
						sock.TrueClose();
					}
					catch (IOException) 
					{
						log.Error("++++ failed to close socket : " + sock.ToString());
					}

					success = false;
					sock = null;
				}

				if (sock != null)
					sock.Close();
			}

			return success;
		}

		/// <summary>
		/// Retrieves stats for all servers.
		/// 
		/// Returns a map keyed on the servername.
		/// The value is another map which contains stats
		/// with stat name as key and value as value.
		/// </summary>
		/// <returns></returns>
		public Hashtable Stats() 
		{
			return Stats(null);
		}

		/// <summary>
		/// Retrieves stats for passed in servers (or all servers).
		/// 
		/// Returns a map keyed on the servername.
		/// The value is another map which contains stats
		/// with stat name as key and value as value.
		/// </summary>
		/// <param name="servers">string array of servers to retrieve stats from, or all if this is null</param>
		/// <returns>Stats map</returns>
		public Hashtable Stats(string[] servers) 
		{

			// get SockIOPool instance
			SockIOPool pool = SockIOPool.getInstance( poolName );

			// return false if unable to get SockIO obj
			if (pool == null) 
			{
				log.Error("++++ unable to get SockIOPool instance");
				return null;
			}

			// get all servers and iterate over them
			servers = (servers == null)
				? pool.Servers
				: servers;

			// if no servers, then return early
			if (servers == null || servers.Length <= 0) 
			{
				log.Error("++++ no servers to check stats");
				return null;
			}

			// array of stats Hashtables
			Hashtable statsMaps = new Hashtable();

			for (int i = 0; i < servers.Length; i++) 
			{

				SockIOPool.SockIO sock = pool.getConnection(servers[i]);
				if (sock == null) 
				{
					log.Error("++++ unable to get connection to : " + servers[i]);
					continue;
				}

				// build command
				string command = "stats\r\n";

				try 
				{
					sock.Write(UTF8Encoding.UTF8.GetBytes( command ));
					sock.Flush();

					// map to hold key value pairs
					Hashtable stats = new Hashtable();

					// loop over results
					while (true) 
					{
						string line = sock.ReadLine();
						log.Debug("++++ line: " + line);

						if (line.StartsWith(STATS)) 
						{
							string[] info = line.Split(' ');
							string key    = info[1];
							string val  = info[2];

							log.Debug("++++ key  : " + key);
							log.Debug("++++ value: " + val);

							stats[ key ] = val;

						}
						else if (END == line) 
						{
							// finish when we get end from server
							log.Debug("++++ finished reading from cache server");
							break;
						}

						statsMaps[ servers[i] ] = stats;
					}
				}
				catch (IOException e) 
				{
					// exception thrown
					log.Error("++++ exception thrown while writing bytes to server on delete");
					log.Error(e.ToString(), e);

					try 
					{
						sock.TrueClose();
					}
					catch (IOException) 
					{
						log.Error("++++ failed to close socket : " + sock.ToString());
					}

					sock = null;
				}

				if (sock != null)
					sock.Close();
			}

			return statsMaps;
		}
	}
}