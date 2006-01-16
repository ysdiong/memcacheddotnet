/**
 * MemCached C# client, connection pool for Socket IO
 * Copyright (c) 2005
 *
 * This module is Copyright (c) 2005 Tim Gebhardt
 * Based on code from Greg Whalin and Richard Russo from the 
 * Java memcached client api:
 * http://www.whalin.com/memcached/
 * All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later
 * version.
 *
 * This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 * @author Tim Gebhardt <tim@gebhardtcomputing.com>
 *
 * @version 1.0
 */
namespace MemCached.clientlib
{
	using System;
	using System.Collections;
	using System.IO;
	using System.Net;
	using System.Net.Sockets;
	using System.Runtime.CompilerServices;
	using System.Security.Cryptography;
	using System.Text;
	using System.Threading;

	using Communication.IO.Tools;
	using log4net;

	/// <summary>
	/// This class is a connection pool for maintaning a pool of persistent connections
	/// to memcached servers.
	/// 
	/// The pool must be initialized prior to use. This should typically be early on
	/// in the lifecycle of the application instance.
	/// </summary>
	/// <example>
	/// //Using defaults
	///	String[] serverlist = { "cache0.server.com:12345", "cache1.server.com:12345" };
	///
	///	SockIOPool pool = SockIOPool.getInstance();
	///	pool.setServers(serverlist);
	///	pool.initialize();	
	///		
	///	
	///	//An example of initializing using defaults and providing weights for servers:
	///	String[] serverlist = { "cache0.server.com:12345", "cache1.server.com:12345" };
	///	Integer[] weights   = { new Integer(5), new Integer(2) };
	/// 
	/// SockIOPool pool = SockIOPool.getInstance();
	///	pool.setServers(serverlist);
	///	pool.setWeights(weights);	
	///	pool.initialize();	
	///	
	///	
	/// //An example of initializing overriding defaults:
	/// String[] serverlist     = { "cache0.server.com:12345", "cache1.server.com:12345" };
	/// Integer[] weights       = { new Integer(5), new Integer(2) };	
	/// int initialConnections  = 10;
	/// int minSpareConnections = 5;
	/// int maxSpareConnections = 50;	
	/// long maxIdleTime        = 1000 * 60 * 30;	// 30 minutes
	/// long maxBusyTime        = 1000 * 60 * 5;	// 5 minutes
	/// long maintThreadSleep   = 1000 * 5;			// 5 seconds
	/// int	socketTimeOut       = 1000 * 3;			// 3 seconds to block on reads
	/// int	socketConnectTO     = 1000 * 3;			// 3 seconds to block on initial connections.  If 0, then will use blocking connect (default)
	/// boolean failover        = false;			// turn off auto-failover in event of server down	
	/// boolean nagleAlg        = false;			// turn off Nagle's algorithm on all sockets in pool	
	/// 
	/// SockIOPool pool = SockIOPool.getInstance();
	/// pool.setServers( serverlist );
	/// pool.setWeights( weights );	
	/// pool.setInitConn( initialConnections );
	/// pool.setMinConn( minSpareConnections );
	/// pool.setMaxConn( maxSpareConnections );
	/// pool.setMaxIdle( maxIdleTime );
	/// pool.setMaxBusyTime( maxBusyTime );
	/// pool.setMaintSleep( maintThreadSleep );
	/// pool.setSocketTO( socketTimeOut );
	/// pool.setSocketConnectTO( socketConnectTO );	
	/// pool.setNagle( nagleAlg );	
	/// pool.setHashingAlg( SockIOPool.NEW_COMPAT_HASH );	
	/// pool.initialize();	
	/// 
	/// 
	/// //The easiest manner in which to initialize the pool is to set the servers and rely on defaults as in the first example.
	/// //After pool is initialized, a client will request a SockIO object by calling getSock with the cache key
	/// //The client must always close the SockIO object when finished, which will return the connection back to the pool.
	/// //An example of retrieving a SockIO object:
	/// SockIOPool.SockIO sock = SockIOPool.getInstance().getSock( key );
	/// try 
	/// {
	///		sock.write( "version\r\n" );	
	///		sock.flush();	
	///		System.out.println( "Version: " + sock.readLine() );	
	/// }
	///	catch (IOException ioe) { System.out.println( "io exception thrown" ) }
	/// finally { sock.close();	}
	/// 
	///	</example>
	public class SockIOPool 
	{
		// logger
		private static Logger log =
			Logger.getLogger( typeof(SockIOPool).Name );

		// store instances of pools
		private static Hashtable pools = new Hashtable();

		// Pool data
		private MaintThread maintThread;
		private bool initialized        = false;
		private int maxCreate              = 1;					// this will be initialized by pool when the pool is initialized
		private Hashtable createShift;

		// initial, min and max pool sizes
		private int poolMultiplier				= 4;
		private int initConn					= 3;
		private int minConn						= 3;
		private int maxConn						= 10;
		private long maxIdle					= 1000 * 60 * 3;		// max idle time for avail sockets
		private long maxBusyTime				= 1000 * 60 * 5;		// max idle time for avail sockets
		private long maintSleep					= 1000 * 5; 			// maintenance thread sleep time
		private int socketTO					= 1000 * 10;			// default timeout of socket reads
		private int socketConnectTO			    = 0;			        // default timeout of socket connections
		private bool failover			    = true;				// default to failover in event of cache server dead
		private bool nagle					= true;				// enable/disable Nagle's algorithm
		private HashingAlgorithms hashingAlg	= HashingAlgorithms.NATIVE_HASH ;		// default to using the native hash as it is the fastest

		// list of all servers
		private string[] servers;
		private int[] weights;
		private ArrayList buckets;

		// dead server map
		private Hashtable hostDead;
		private Hashtable hostDeadDur;
	
		// map to hold all available sockets
		private Hashtable availPool;

		// map to hold busy sockets
		private Hashtable busyPool;
	
		// empty constructor
		protected SockIOPool() { }

		/// <summary>
		/// Factory to create/retrieve new pools given a unique poolName.
		/// </summary>
		/// <param name="poolName">unique name of the pool</param>
		/// <returns>instance of SockIOPool</returns>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public static SockIOPool getInstance( String poolName ) 
		{
			if ( pools.ContainsKey( poolName ) )
				return (SockIOPool)pools[poolName];

			SockIOPool pool = new SockIOPool();
			pools[poolName] = pool ;

			return pool;
		}

		/// <summary>
		/// Single argument version of factory used for back compat.
		/// Simply creates a pool named "default". 
		/// </summary>
		/// <returns>instance of SockIOPool</returns>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public static SockIOPool getInstance() 
		{
			return getInstance( "default" );
		}

		/// <summary>
		/// Gets or sets the list of all cache servers
		/// </summary>
		/// <value>string array of servers [host:port]</value>
		public string[] Servers
		{
			get { return this.servers; }
			set { this.servers = value; }
		}

		/// <summary>
		/// Gets or sets the list of weights to apply to the server list
		/// 
		/// This is an int array with each element corresponding to an element
		/// in the same position in the server string array <see>Servers</see>.
		/// </summary>
		/// <value>int array of weights</value>
		public int[] Weights
		{
			get { return this.weights; }
			set { this.weights = value; }
		}

		/// <summary>
		/// Gets or sets the initial number of connections per server setting in the available pool.
		/// </summary>
		/// <value>int number of connections</value>
		public int InitConnections
		{
			get { return this.initConn; }
			set { this.initConn = value; }
		}

		/// <summary>
		/// Gets or sets the minimum number of spare connections to maintain in our available pool
		/// </summary>
		public int MinConnections
		{
			get { return this.minConn; }
			set { this.minConn = value; }
		}

		/// <summary>
		/// Gets or sets the maximum number of spare connections allowed in our available pool.
		/// </summary>
		public int MaxConnections
		{
			get { return this.maxConn; }
			set { this.maxConn = value; }
		}

		/// <summary>
		/// Gets or sets the maximum idle time for threads in the avaiable pool.
		/// </summary>
		public long MaxIdle
		{
			get { return this.maxIdle; }
			set { this.maxIdle = value; }
		}

		/// <summary>
		/// Gets or sets the maximum busy time for threads in the busy pool
		/// </summary>
		/// <value>idle time in milliseconds</value>
		public long MaxBusy
		{
			get { return this.maxBusyTime; }
			set { this.maxBusyTime = value; }
		}

		/// <summary>
		/// Gets or sets the sleep time between runs of the pool maintenance thread.
		/// If set to 0, then the maintenance thread will not be started;
		/// </summary>
		/// <value>sleep time in milliseconds</value>
		public long MaintenanceSleep
		{
			get { return this.maintSleep; }
			set { this.maintSleep = value; }
		}

		/// <summary>
		/// Gets or sets the socket timeout for reads
		/// </summary>
		/// <value>timeout time in milliseconds</value>
		public int SocketTimeout
		{
			get { return this.socketTO; }
			set { this.socketTO = value; }
		}

		/// <summary>
		/// Gets or sets the socket timeout for connects.
		/// </summary>
		/// <value>timeout time in milliseconds</value>
		public int SocketConnectTimeout
		{
			get { return this.socketConnectTO; }
			set { this.socketConnectTO = value; }
		}

		/// <summary>
		/// Gets or sets the failover flag for the pool.
		/// 
		/// If this flag is set to true and a socket fails to connect,
		/// the pool will attempt to return a socket from another server
		/// if one exists.  If set to false, then getting a socket
		/// will return null if it fails to connect to the requested server.
		/// </summary>
		public bool Failover
		{
			get { return this.failover; }
			set { this.failover = value; }
		}

		/// <summary>
		/// Gets or sets the Nagle algorithm flag for the pool.
		/// 
		/// If false, will turn off Nagle's algorithm on all sockets created.
		/// </summary>
		public bool Nagle
		{
			get { return this.nagle; }
			set { this.nagle = value; }
		}

		/// <summary>
		/// Gets or sets the hashing algorithm we will use.
		/// </summary>
		public HashingAlgorithms HashingAlg
		{
			get { return this.hashingAlg; }
			set { this.hashingAlg = value; } 
		}

		/// <summary>
		/// Internal private hashing method.
		/// 
		/// This is the original hashing algorithm from other clients.
		/// Found to be slow and have poor distribution.
		/// </summary>
		/// <param name="key">string to hash</param>
		/// <returns>hashcode for this string using memcached's hashing algorithm</returns>
		private static int origCompatHashingAlg( string key ) 
		{
			int hash = 0;
			char[] cArr = key.ToCharArray();

			for ( int i = 0; i < cArr.Length; ++i ) 
			{
				hash = (hash * 33) + cArr[i];
			}

			return hash;
		}
		
		/// <summary>
		/// Internal private hashing method.
		/// 
		/// This is the new hashing algorithm from other clients.
		/// Found to be fast and have very good distribution.
		/// 
		/// UPDATE: this is dog slow under java.  Maybe under .NET? 
		/// </summary>
		/// <param name="key">string to hash</param>
		/// <returns>hashcode for this string using memcached's hashing algorithm</returns>
		private static int newCompatHashingAlg( string key ) 
		{
			CRCTool checksum = new CRCTool();
			checksum.Init(CRCTool.CRCCode.CRC32);
			int crc = (int) checksum.crctablefast(UTF8Encoding.UTF8.GetBytes(key));

			return (crc >> 16) & 0x7fff;
		}

		/// <summary>
		/// Initializes the pool
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public void initialize() 
		{
			// check to see if already initialized
			if ( initialized
				&& ( buckets != null )
				&& ( availPool != null )
				&& ( busyPool != null ) ) 
			{
				log.Error( "++++ trying to initialize an already initialized pool" );
				return;
			}

			// initialize empty maps
			buckets     = new ArrayList();
			availPool   = new Hashtable( servers.Length * initConn );
			busyPool    = new Hashtable( servers.Length * initConn );
			hostDeadDur = new Hashtable();
			hostDead    = new Hashtable();
			createShift = new Hashtable();
			maxCreate   = (poolMultiplier > minConn) ? minConn : minConn / poolMultiplier;		// only create up to maxCreate connections at once

			log.Debug( "++++ initializing pool with following settings:" );
			log.Debug( "++++ initial size: " + initConn );
			log.Debug( "++++ min spare   : " + minConn );
			log.Debug( "++++ max spare   : " + maxConn );

			// if servers is not set, or it empty, then
			// throw a runtime exception
			if ( servers == null || servers.Length <= 0 ) 
			{
				log.Error( "++++ trying to initialize with no servers" );
				throw new ArgumentNullException( "++++ trying to initialize with no servers" );
			}

			for ( int i = 0; i < servers.Length; i++ ) 
			{
				// add to bucket
				// with weights if we have them 
				if ( weights != null && weights.Length > i ) 
				{
					for ( int k = 0; k < weights[i]; k++ ) 
					{
						buckets.Add( servers[i] );
						log.Debug( "++++ added " + servers[i] + " to server bucket" );
					}
				}
				else 
				{
					buckets.Add( servers[i] );
					log.Debug( "++++ added " + servers[i] + " to server bucket" );
				}

				// create initial connections
				log.Debug( "+++ creating initial connections (" + initConn + ") for host: " + servers[i] );

				for ( int j = 0; j < initConn; j++ ) 
				{
					SockIO socket = createSocket( servers[i] );
					if ( socket == null ) 
					{
						log.Error( "++++ failed to create connection to: " + servers[i] + " -- only " + j + " created." );
						break;
					}

					addSocketToPool( availPool, servers[i], socket );
					log.Debug( "++++ created and added socket: " + socket.ToString() + " for host " + servers[i] );
				}
			}

			// mark pool as initialized
			this.initialized = true;

			// start maint thread TODO: re-enable
			if (this.maintSleep > 0)
				this.startMaintThread();
		}

		/// <summary>
		/// Returns the state of the pool
		/// </summary>
		/// <returns>returns <c>true</c> if initialized</returns>
		public bool isInitialized() 
		{
			return initialized;
		}

		/// <summary>
		/// Creates a new SockIO obj for the given server.
		///
		///If server fails to connect, then return null and do not try
		///again until a duration has passed.  This duration will grow
		///by doubling after each failed attempt to connect.
		/// </summary>
		/// <param name="host">host:port to connect to</param>
		/// <returns>SockIO obj or null if failed to create</returns>
		protected SockIO createSocket( string host ) 
		{

			SockIO socket = null;

			// if host is dead, then we don't need to try again
			// until the dead status has expired
			// we do not try to put back in if failover is off
			if ( failover && hostDead.ContainsKey( host ) && hostDeadDur.ContainsKey( host ) ) 
			{

				DateTime store  = (DateTime)hostDead[ host ];
				long expire = ((long)hostDeadDur[ host ]);

				if ( (store.AddMilliseconds(expire) ) > DateTime.Now )
					return null;
			}

			try 
			{
				socket = new SockIO( this, host, this.socketTO, this.socketConnectTO, this.nagle );

				if ( !socket.IsConnected ) 
				{
					log.Error( "++++ failed to get SockIO obj for: " + host + " -- new socket is not connected" );
					try 
					{
						socket.TrueClose();
					}
					catch ( Exception ex ) 
					{
						log.Error( "++++ failed to close SockIO obj for server: " + host );
						log.Error( ex.Message, ex );
						socket = null;
					}
				}
			}
			catch ( Exception ex ) 
			{
				log.Error( "++++ failed to get SockIO obj for: " + host );
				log.Error( ex.Message, ex );
				socket = null;
			}

			// if we failed to get socket, then mark
			// host dead for a duration which falls off
			if ( socket == null ) 
			{
				DateTime now = DateTime.Now;
				hostDead[ host ] = now ;
				long expire = ( hostDeadDur.ContainsKey( host ) ) ? (((long)hostDeadDur[ host ]) * 2) : 1000;
				hostDeadDur[ host ] = expire;
				log.Debug( "++++ ignoring dead host: " + host + " for " + expire + " ms" );

				// also clear all entries for this host from availPool
				clearHostFromPool( availPool, host );
			}
			else 
			{
				log.Debug( "++++ created socket (" + socket.ToString() + ") for host: " + host );
				hostDead.Remove( host );
				hostDeadDur.Remove( host );
				if(buckets.BinarySearch(host) < 0)
					buckets.Add(host);
			}

			return socket;
		}

		/// <summary>
		/// Returns appropriate SockIO object given
		/// string cache key.
		/// </summary>
		/// <param name="key">hashcode for cache key</param>
		/// <returns>SockIO obj connected to server</returns>
		public SockIO getSock( string key ) 
		{
			return getSock( key, null );
		}

		/// <summary>
		/// Returns appropriate SockIO object given
		/// string cache key and optional hashcode.
		/// 
		/// Trys to get SockIO from pool.  Fails over
		/// to additional pools in event of server failure.
		/// </summary>
		/// <param name="key">hashcode for cache key</param>
		/// <param name="hashCode">if not null, then the int hashcode to use</param>
		/// <returns>SockIO obj connected to server</returns>
		public SockIO getSock( string key, object hashCode ) 
		{

			log.Debug( "cache socket pick " + key + " " + hashCode );

			if ( !this.initialized ) 
			{
				log.Error( "attempting to get SockIO from uninitialized pool!" );
				return null;
			}

			// if no servers return null
			if ( buckets.Count == 0 )
				return null;

			// if only one server, return it
			if ( buckets.Count == 1 )
				return getConnection( (string) buckets[ 0 ] );
		
			int tries = 0;

			// generate hashcode
			int hv;
			if ( hashCode != null ) 
			{
				hv = (int)hashCode;
			}
			else 
			{

				// NATIVE_HASH = 0
				// OLD_COMPAT_HASH = 1
				// NEW_COMPAT_HASH = 2
				switch ( hashingAlg ) 
				{
					case HashingAlgorithms.NATIVE_HASH:
						hv = key.GetHashCode();
						break;

					case HashingAlgorithms.OLD_COMPAT_HASH:
						hv = origCompatHashingAlg( key );
						break;

					case HashingAlgorithms.NEW_COMPAT_HASH:
						hv = newCompatHashingAlg( key );
						break;

					default:
						// use the native hash as a default
						hv = key.GetHashCode();
						hashingAlg = HashingAlgorithms.NATIVE_HASH;
						break;
				}
			}

			// keep trying different servers until we find one
			while ( tries++ <= buckets.Count) 
			{
				// get bucket using hashcode 
				// get one from factory
				int bucket = hv % buckets.Count;
				if ( bucket < 0 )
					bucket += buckets.Count;

				SockIO sock = getConnection( (string)buckets[ bucket ] );

				log.Debug( "cache choose " + buckets[ bucket ] + " for " + key );

				if ( sock != null )
					return sock;

				// if we do not want to failover, then bail here
				if ( !failover )
					return null;

				// if we failed to get a socket from this server
				// then we try again by adding an incrementer to the
				// current key and then rehashing 
				switch ( hashingAlg ) 
				{
					case HashingAlgorithms.NATIVE_HASH:
						hv += ((string)("" + tries + key)).GetHashCode();
						break;

					case HashingAlgorithms.OLD_COMPAT_HASH:
						hv += origCompatHashingAlg( "" + tries + key );
						break;

					case HashingAlgorithms.NEW_COMPAT_HASH:
						hv += newCompatHashingAlg( "" + tries + key );
						break;

					default:
						// use the native hash as a default
						hv += ((string)("" + tries + key)).GetHashCode();
						hashingAlg = HashingAlgorithms.NATIVE_HASH;
						break;
				}
			}

			return null;
		}

		/// <summary>
		/// Returns a SockIO object from the pool for the passed in host.
		/// 
		/// Meant to be called from a more intelligent method
		/// which handles choosing appropriate server
		/// and failover. 
		/// </summary>
		/// <param name="host">host from which to retrieve object</param>
		/// <returns>SockIO object or null if fail to retrieve one</returns>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public SockIO getConnection( string host ) 
		{

			if ( !this.initialized ) 
			{
				log.Error( "attempting to get SockIO from uninitialized pool!" );
				return null;
			}

			if ( host == null )
				return null;

			// if we have items in the pool
			// then we can return it
			if ( availPool != null && !(availPool.Count == 0) ) 
			{

				// take first connected socket
				Hashtable aSockets = (Hashtable)availPool[ host ];

				if ( aSockets != null && !(aSockets.Count == 0) ) 
				{

					foreach (SockIO socket in new IterIsolate(aSockets.Keys) ) 
					{
						if ( socket.IsConnected ) 
						{
							log.Debug( "++++ moving socket for host (" + host + ") to busy pool ... socket: " + socket );

							// remove from avail pool
							aSockets.Remove(socket);

							// add to busy pool
							addSocketToPool( busyPool, host, socket );

							// return socket
							return socket;
						}
						else 
						{
							// not connected, so we need to remove it
							log.Error( "++++ socket in avail pool is not connected: " + socket.ToString() + " for host: " + host );

							// remove from avail pool
							aSockets.Remove(socket);
						}

					}
				}
			}
		
			// if here, then we found no sockets in the pool
			// try to create on a sliding scale up to maxCreate
			object cShift = createShift[ host ];
			int shift = (cShift != null) ? (int)cShift : 0;

			int create = 1 << shift;
			if ( create >= maxCreate ) 
			{
				create = maxCreate;
			}
			else 
			{
				shift++;
			}

			// store the shift value for this host
			createShift[ host ] = shift;

			log.Debug( "++++ creating " + create + " new SockIO objects" );

			for ( int i = create; i > 0; i-- ) 
			{
				SockIO socket = createSocket( host );
				if ( socket == null )
					break;

				if ( i == 1 ) 
				{
					// last iteration, add to busy pool and return sockio
					addSocketToPool( busyPool, host, socket );
					return socket;
				}
				else 
				{
					// add to avail pool
					addSocketToPool( availPool, host, socket );
				}
			}

			// should never get here
			return null;
		}

		/// <summary>
		/// Adds a socket to a given pool for the given host.
		/// 
		/// Internal utility method. 
		/// </summary>
		/// <param name="pool">pool to add to</param>
		/// <param name="host">host this socket is connected to</param>
		/// <param name="socket">socket to add</param>
		[MethodImpl(MethodImplOptions.Synchronized)]
		protected void addSocketToPool( Hashtable pool, string host, SockIO socket ) 
		{

			Hashtable sockets = null;
			if ( pool.ContainsKey( host ) ) 
			{
				sockets = (Hashtable)pool[ host ];
				if ( sockets != null ) 
				{
					sockets[ socket ] = new TimeSpan(DateTime.Now.Ticks).TotalMilliseconds;
					return;
				}
			}

			sockets = new Hashtable();
			sockets[ socket ] = new TimeSpan(DateTime.Now.Ticks).TotalMilliseconds;
			pool[ host ] = sockets ;
		}

		/// <summary>
		/// Removes a socket from specified pool for host.
		/// 
		/// Internal utility method. 
		/// </summary>
		/// <param name="pool">pool to remove from</param>
		/// <param name="host">host pool</param>
		/// <param name="socket">socket to remove</param>
		[MethodImpl(MethodImplOptions.Synchronized)]
		protected void removeSocketFromPool( Hashtable pool, string host, SockIO socket ) 
		{
			if ( pool.ContainsKey( host ) ) 
			{
				Hashtable sockets = (Hashtable)pool[ host ];
				if ( sockets != null ) 
				{
					sockets.Remove( socket );
				}
			}
		}

		/// <summary>
		/// Closes and removes all sockets from specified pool for host. 
		/// 
		/// Internal utility method. 
		/// </summary>
		/// <param name="pool">pool to clear</param>
		/// <param name="host">host to clear</param>
		[MethodImpl(MethodImplOptions.Synchronized)]
		protected void clearHostFromPool( Hashtable pool, string host ) 
		{
			if ( pool.ContainsKey( host ) ) 
			{
				Hashtable sockets = (Hashtable)pool[ host ];

				if ( sockets != null && sockets.Count > 0 ) 
				{
					foreach (SockIO socket in new IterIsolate(sockets.Keys) ) 
					{
						try 
						{
							socket.TrueClose();
						}
						catch ( IOException ioe ) 
						{
							log.Error( "++++ failed to close socket: " + ioe.Message );
						}

						sockets.Remove(socket);
					}
				}
			}
		}

		/// <summary>
		/// Checks a SockIO object in with the pool.
		/// 
		/// This will remove SocketIO from busy pool, and optionally
		/// add to avail pool.
		/// </summary>
		/// <param name="socket">socket to return</param>
		/// <param name="addToAvail">add to avail pool if true</param>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public void checkIn( SockIO socket, bool addToAvail ) 
		{
			string host = socket.Host;
			log.Debug( "++++ calling check-in on socket: " + socket.ToString() + " for host: " + host );

			// remove from the busy pool
			log.Debug( "++++ removing socket (" + socket.ToString() + ") from busy pool for host: " + host );
			removeSocketFromPool( busyPool, host, socket );

			// add to avail pool
			if ( addToAvail && socket.IsConnected ) 
			{
				log.Debug( "++++ returning socket (" + socket.ToString() + " to avail pool for host: " + host );
				addSocketToPool( availPool, host, socket );
			}
		}

		/// <summary>
		/// Returns a socket to the avail pool.
		/// 
		/// This is called from SockIO.close().  Calling this method
		/// directly without closing the SockIO object first
		/// will cause an IOException to be thrown.
		/// </summary>
		/// <param name="socket">socket to return</param>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public void checkIn(SockIO socket) 
		{
			checkIn( socket, true );
		}

		/// <summary>
		/// Closes all sockets in the passed in pool.
		/// 
		/// Internal utility method. 
		/// </summary>
		/// <param name="pool">pool to close</param>
		protected void closePool( Hashtable pool ) 
		{

			foreach (string host in pool.Keys ) 
			{
				Hashtable sockets = (Hashtable)pool[ host ];

				foreach (SockIO socket in sockets.Keys ) 
				{
					try 
					{
						socket.TrueClose();
					}
					catch ( IOException ) 
					{
						log.Error( "++++ failed to TrueClose socket: " + socket.ToString() + " for host: " + host );
					}

					sockets.Remove(socket);
				}
			}
		}

		/// <summary>
		/// Shuts down the pool.
		/// 
		/// Cleanly closes all sockets.
		/// Stops the maint thread.
		/// Nulls out all internal maps
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public void shutDown() 
		{
			log.Debug( "++++ SockIOPool shutting down..." );
			if ( maintThread != null && maintThread.IsRunning )
				stopMaintThread();

			log.Debug( "++++ closing all internal pools." );
			closePool( availPool );
			closePool( busyPool );
			availPool   = null;
			busyPool    = null;
			buckets     = null;
			hostDeadDur = null;
			hostDead    = null;
			initialized = false;
			log.Debug( "++++ SockIOPool finished shutting down." );
		}

		/// <summary>
		/// Starts the maintenance thread.
		/// 
		/// This thread will manage the size of the active pool
		/// as well as move any closed, but not checked in sockets
		/// back to the available pool.
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		protected void startMaintThread() 
		{

			if ( maintThread != null ) 
			{

				if ( maintThread.IsRunning ) 
				{
					log.Error( "main thread already running" );
				}
				else 
				{
					maintThread.Start();
				}
			}
			else 
			{
				maintThread = new MaintThread( this );
				maintThread.Interval = this.maintSleep ;
				maintThread.Start();
			}
		}

		/// <summary>
		/// Stops the maintenance thread.
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		protected void stopMaintThread() 
		{
			if ( maintThread != null && maintThread.IsRunning )
				maintThread.StopThread();
		}

		/// <summary>
		/// Runs self maintenance on all internal pools.
		/// 
		/// This is typically called by the maintenance thread to manage pool size. 
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		protected void selfMaint() 
		{
			log.Debug( "++++ Starting self maintenance...." );

			// go through avail sockets and create/destroy sockets
			// as needed to maintain pool settings
			foreach (string host in availPool.Keys) 
			{
				Hashtable sockets  = (Hashtable)availPool[ host ];
				log.Debug( "++++ Size of avail pool for host (" + host + ") = " + sockets.Count );

				// if pool is too small (n < minSpare)
				if ( sockets.Count < minConn ) 
				{
					// need to create new sockets
					int need = minConn - sockets.Count;
					log.Debug( "++++ Need to create " + need + " new sockets for pool for host: " + host );

					for ( int j = 0; j < need; j++ ) 
					{
						SockIO socket = createSocket( host );

						if ( socket == null )
							break;

						addSocketToPool( availPool, host, socket );
					}
				}
				else if ( sockets.Count > maxConn ) 
				{
					// need to close down some sockets
					int diff        = sockets.Count - maxConn;
					int needToClose = (diff <= poolMultiplier)
						? diff
						: (diff) / poolMultiplier;

					log.Debug( "++++ need to remove " + needToClose + " spare sockets for pool for host: " + host );
					foreach (SockIO socket in new IterIsolate(sockets.Keys) ) 
					{
						if ( needToClose <= 0 )
							break;

						// remove stale entries
						long expire   = (long)sockets[ socket ];

						// if past idle time
						// then close socket
						// and remove from pool
						if ( (expire + maxIdle) < new TimeSpan(DateTime.Now.Ticks).TotalMilliseconds ) 
						{
							log.Debug( "+++ removing stale entry from pool as it is past its idle timeout and pool is over max spare" );
							try 
							{
								socket.TrueClose();
							}
							catch ( IOException ioe ) 
							{
								log.Error( "failed to close socket" );
								log.Error( ioe.Message, ioe );
							}

							sockets.Remove(socket);
							needToClose--;
						}
					}
				}

				// reset the shift value for creating new SockIO objects
				createShift[ host ] = 0;
			}

			// go through busy sockets and destroy sockets
			// as needed to maintain pool settings
			foreach (string host in busyPool.Keys ) 
			{
				Hashtable sockets = (Hashtable)busyPool[ host ];
				log.Debug( "++++ Size of busy pool for host (" + host + ")  = " + sockets.Count );

				// loop through all connections and check to see if we have any hung connections
				foreach (SockIO socket in new IterIsolate(sockets.Keys) ) 
				{
					// remove stale entries
					long hungTime = (long)sockets[ socket ];

					// if past max busy time
					// then close socket
					// and remove from pool
					if ( (hungTime + maxBusyTime) < new TimeSpan(DateTime.Now.Ticks).TotalMilliseconds ) 
					{
						log.Error( "+++ removing potentially hung connection from busy pool ... socket in pool for " + (new TimeSpan(DateTime.Now.Ticks).TotalMilliseconds - hungTime) + "ms" );
						try 
						{
							socket.TrueClose();
						}
						catch ( IOException ioe ) 
						{
							log.Error( "failed to close socket" );
							log.Error( ioe.Message, ioe );
						}

						sockets.Remove(socket);
					}
				}
			}

			log.Debug( "+++ ending self maintenance." );
		}

		/// <summary>
		/// Class which extends thread and handles maintenance of the pool.
		/// </summary>
		protected class MaintThread
		{
			private MaintThread() { }

			private Thread thread;

			private SockIOPool pool;
			private long interval      = 1000 * 3; // every 3 seconds
			private bool stopThread = false;

			public MaintThread( SockIOPool pool ) 
			{
				thread = new Thread(new ThreadStart(Maintain));
				this.pool = pool;
			}

			public long Interval
			{
				get { return this.interval; }
				set { this.interval = value; }
			}

			public bool IsRunning
			{
				get { return thread.IsAlive; }
			}

			/// <summary>
			/// Sets stop variable and interups and wait
			/// </summary>
			public void StopThread() 
			{
				this.stopThread = true;
				thread.Interrupt();
			}

			/// <summary>
			/// The logic of the thread.
			/// </summary>
			private void Maintain()
			{
				while ( !this.stopThread ) 
				{
					try 
					{
						Thread.Sleep( (int)interval );

						// if pool is initialized, then
						// run the maintenance method on itself
						if ( pool.isInitialized() )
							pool.selfMaint();

					}
					catch ( Exception ex ) 
					{
						log.Error("maintenance thread choked.", ex);
					}
				}
			}

			/// <summary>
			/// Start the thread
			/// </summary>
			public void Start() 
			{
				this.stopThread = false;
				thread.Start();
			}
		}
		
		/// <summary>
		/// MemCached C# client, utility class for Socket IO.
		/// 
		/// This class is a wrapper around a Socket and its streams.
		/// </summary>
		public class SockIO 
		{
			// logger
			private static Logger log =
				Logger.getLogger( typeof(SockIO).Name );

			// pool
			private SockIOPool pool;

			// data
			private String host;
			private Socket sock;
			private Stream inStream;
			private Stream outStream;

			/// <summary>
			/// creates a new SockIO object wrapping a socket
			/// connection to host:port, and its input and output streams
			/// </summary>
			/// <param name="pool">Pool this object is tied to</param>
			/// <param name="host">host to connect to</param>
			/// <param name="port">port to connect to</param>
			/// <param name="timeout">int ms to block on data for read</param>
			/// <param name="connectTimeout">timeout (in ms) for initial connection</param>
			/// <param name="noDelay">TCP NODELAY option?</param>
			public SockIO( SockIOPool pool, String host, int port, int timeout, int connectTimeout, bool noDelay )  
			{
				this.pool = pool;

				if(connectTimeout > 0)
				{
					sock = GetSocket(host, port, connectTimeout);
				}
				else
				{
					sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
					sock.Connect(new IPEndPoint(IPAddress.Parse(host), port));
				}

				
				if (timeout >= 0)
					sock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, timeout);

				// testing only
				sock.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, noDelay ? 1 : 0);

				NetworkStream netStream = new NetworkStream(sock);
				inStream = new BufferedStream(netStream);
				outStream = new BufferedStream(netStream);

				this.host = host + ":" + port;
			}

			/// <summary>
			/// creates a new SockIO object wrapping a socket
			/// connection to host:port, and its input and output streams
			/// </summary>
			/// <param name="pool">Pool this object is tied to</param>
			/// <param name="host">hostname:port</param>
			/// <param name="timeout">read timeout value for connected socket</param>
			/// <param name="connectTimeout">timeout for initial connections</param>
			/// <param name="noDelay">TCP NODELAY option?</param>
			public SockIO( SockIOPool pool, String host, int timeout, int connectTimeout, bool noDelay )
			{

				this.pool = pool;

				String[] ip = host.Split(':');

				// get socket: default is to use non-blocking connect
				if(connectTimeout > 0)
				{
					sock = GetSocket(ip[ 0 ], int.Parse( ip[ 1 ] ), connectTimeout);
				}
				else
				{
					sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
					sock.Connect(new IPEndPoint(IPAddress.Parse(ip[ 0 ]), int.Parse( ip[ 1 ])));
				}

				if ( timeout >= 0 )
					sock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, timeout);

				// testing only
				sock.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, noDelay ? 1 : 0);

				NetworkStream netStream = new NetworkStream(sock);
				inStream = new BufferedStream(netStream);
				outStream = new BufferedStream(netStream);

				this.host = host;
			}

			/// <summary>
			/// Method which spawns thread to get a connection and then enforces a timeout on the initial
			/// connection.
			/// 
			/// This should be backed by a thread pool.  Any volunteers?
			/// </summary>
			/// <param name="host">host to establish connection to</param>
			/// <param name="port">port on that host</param>
			/// <param name="timeout">connection timeout in ms</param>
			/// <returns>connected socket</returns>
			protected static Socket GetSocket( String host, int port, int timeout )
			{
				// Create a new thread which will attempt to connect to host:port, and start it running
				ConnectThread thread = new ConnectThread( host, port );
				thread.Start();

				int timer     = 0;
				int sleep     = 25;

				while ( timer < timeout ) 
				{

					// if the thread has a connected socket
					// then return it
					if ( thread.IsConnected )
						return thread.Socket;

					// if the thread had an error
					// then throw a new IOException
					if ( thread.IsError )
						throw new IOException();

					try 
					{
						// sleep for short time before polling again
						Thread.Sleep( sleep );
					}
					catch ( ThreadInterruptedException ) { }

					// Increment timer
					timer += sleep;
				}

				// made it through loop without getting connection
				// the connection thread will timeout on its own at OS timeout
				throw new IOException( "Could not connect for " + timeout + " milliseconds" );
			}

			/// <summary>
			/// returns the host this socket is connected to 
			/// 
			/// String representation of host (hostname:port)
			/// </summary>
			public string Host
			{
				get { return this.host; }
			}

			/// <summary>
			/// closes socket and all streams connected to it 
			/// </summary>
			public void TrueClose()
			{
				log.Debug( "++++ Closing socket for real: " + ToString() );

				bool err = false;
				StringBuilder errMsg = new StringBuilder();

				if ( inStream == null || outStream == null || sock == null ) 
				{
					err = true;
					errMsg.Append( "++++ socket or its streams already null in trueClose call" );
				}

				if ( inStream != null ) 
				{
					try 
					{
						inStream.Close();
					}
					catch( IOException ioe ) 
					{
						log.Error( "++++ error closing input stream for socket: " + ToString() + " for host: " + Host );
						log.Error( ioe.Message, ioe );
						errMsg.Append( "++++ error closing input stream for socket: " + ToString() + " for host: " + Host + "\n" );
						errMsg.Append( ioe.Message );
						err = true;
					}
				}

				if ( outStream != null ) 
				{
					try 
					{
						outStream.Close();
					}
					catch ( IOException ioe ) 
					{
						log.Error( "++++ error closing output stream for socket: " + ToString() + " for host: " + Host );
						log.Error( ioe.Message, ioe );
						errMsg.Append( "++++ error closing output stream for socket: " + ToString() + " for host: " + Host + "\n" );
						errMsg.Append( ioe.Message );
						err = true;
					}
				}

				if ( sock != null ) 
				{
					try 
					{
						sock.Close();
					}
					catch ( IOException ioe ) 
					{
						log.Error( "++++ error closing socket: " + ToString() + " for host: " + Host );
						log.Error( ioe.Message, ioe );
						errMsg.Append( "++++ error closing socket: " + ToString() + " for host: " + Host + "\n" );
						errMsg.Append( ioe.Message );
						err = true;
					}
				}

				// check in to pool
				if ( sock != null )
					pool.checkIn( this, false );

				inStream = null;
				outStream = null;
				sock = null;

				if ( err )
					throw new IOException( errMsg.ToString() );
			}

			/// <summary>
			/// sets closed flag and checks in to connection pool
			/// but does not close connections
			/// </summary>
			public void Close() 
			{
				// check in to pool
				log.Debug("++++ marking socket (" + this.ToString() + ") as closed and available to return to avail pool");
				pool.checkIn( this );
			}
		
			/// <summary>
			/// Gets whether or not the socket is connected.  Returns <c>true</c> if it is.
			/// </summary>
			public bool IsConnected
			{
				get { return (sock != null && sock.Connected); } 
			}

			/// <summary>
			/// reads a line
			/// intentionally not using the deprecated readLine method from DataInputStream 
			/// </summary>
			/// <returns>String that was read in</returns>
			public string ReadLine() 
			{
				if (sock == null || !sock.Connected) 
				{
					log.Error("++++ attempting to read from closed socket");
					throw new IOException("++++ attempting to read from closed socket");
				}

				byte[] b = new byte[1];
				MemoryStream memoryStream = new MemoryStream();
				bool eol = false;

				while (inStream.Read(b, 0, 1) != -1) 
				{

					if (b[0] == 13) 
					{
						eol = true;

					} 
					else 
					{
						if (eol) 
						{
							if (b[0] == 10)
								break;

							eol = false;
						}
					}

					// cast byte into char array
					memoryStream.Write(b, 0, 1);
				}

				if (memoryStream == null || memoryStream.Length <= 0) 
				{
					throw new IOException("++++ Stream appears to be dead, so closing it down");
				}

				// else return the string
				string temp = UTF8Encoding.UTF8.GetString(memoryStream.GetBuffer()).TrimEnd('\0', '\r', '\n');
				return temp;
			}

			/// <summary>
			/// reads up to end of line and returns nothing 
			/// </summary>
			public void ClearEOL() 
			{
				if (sock == null || !sock.Connected) 
				{
					log.Error("++++ attempting to read from closed socket");
					throw new IOException("++++ attempting to read from closed socket");
				}

				byte[] b = new byte[1];
				bool eol = false;
				while (inStream.Read(b, 0, 1) != -1) 
				{

					// only stop when we see
					// \r (13) followed by \n (10)
					if (b[0] == 13) 
					{
						eol = true;
						continue;
					}

					if (eol) 
					{
						if (b[0] == 10)
							break;

						eol = false;
					}
				}
			}

			/// <summary>
			/// reads length bytes into the passed in byte array from stream
			/// </summary>
			/// <param name="b">byte array</param>
			public void Read(byte[] b) 
			{
				if (sock == null || !sock.Connected) 
				{
					log.Error("++++ attempting to read from closed socket");
					throw new IOException("++++ attempting to read from closed socket");
				}

				int count = 0;
				while (count < b.Length) 
				{
					int cnt = inStream.Read(b, count, (b.Length - count));
					count += cnt;
				}
			}

			/// <summary>
			/// flushes output stream 
			/// </summary>
			public void Flush()
			{
				if (sock == null || !sock.Connected) 
				{
					log.Error("++++ attempting to write to closed socket");
					throw new IOException("++++ attempting to write to closed socket");
				}
				outStream.Flush();
			}
		
			/// <summary>
			/// writes a byte array to the output stream
			/// </summary>
			/// <param name="b">byte array to write</param>
			public void Write(byte[] b) 
			{
				if (sock == null || !sock.Connected) 
				{
					log.Error("++++ attempting to write to closed socket");
					throw new IOException("++++ attempting to write to closed socket");
				}
				outStream.Write(b, 0, b.Length);
			}

			/// <summary>
			/// use the sockets hashcode for this object
			/// so we can key off of SockIOs 
			/// </summary>
			/// <returns>hashcode</returns>
			public override int GetHashCode() 
			{
				return (sock == null) ? 0 : sock.GetHashCode();
			}

			/// <summary>
			/// returns the string representation of this socket 
			/// </summary>
			/// <returns></returns>
			public override string ToString() 
			{
				return (sock == null) ? "" : sock.ToString();
			}
		}

		///<summary>
		///Hashing algorithms we can use
		///</summary>
		public enum HashingAlgorithms 
		{ 
			///<summary>native String.hashCode() - fast (cached) but not compatible with other clients</summary>
			NATIVE_HASH = 0,								
			///<summary>original compatibility hashing alg (works with other clients)</summary>
			OLD_COMPAT_HASH = 1,							
			///<summary>new CRC32 based compatibility hashing algorithm (fast and works with other clients)</summary>
			NEW_COMPAT_HASH = 2 
		}

		/// <summary>
		/// Thread to attempt connection. 
		/// This will be polled by the main thread. We run the risk of filling up w/
		/// threads attempting connections if network is down.  However, the falling off
		/// mech in the main code should limit this.
		/// </summary>
		internal class ConnectThread
		{
			//thread
			Thread thread;

			// logger
			private static Logger log =
				Logger.getLogger(typeof(ConnectThread).Name);

			private Socket socket;
			private String host;
			private int port;
			bool error;

			/// <summary>
			/// Constructor 
			/// </summary>
			/// <param name="host"></param>
			/// <param name="port"></param>
			public ConnectThread(string host, int port) 
			{
				this.host    = host;
				this.port    = port;
				this.socket  = null;
				this.error   = false;

				thread = new Thread(new ThreadStart(Connect));
				thread.IsBackground = true;
			}

			/// <summary>
			/// The logic of the thread.
			/// </summary>
			private void Connect()
			{
				try 
				{
					socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
					socket.Connect(new IPEndPoint(IPAddress.Parse(host), port));
				}
				catch (IOException) 
				{
					error = true;
				}

				log.Debug("socket creation thread leaving for host: " + host);
			}

			/// <summary>
			/// start thread running.
			/// This attempts to establish a connection. 
			/// </summary>
			public void Start() 
			{
				thread.Start();
			}

			/// <summary>
			/// Is the new socket connected yet 
			/// </summary>
			public bool IsConnected
			{
				get { return (socket != null && socket.Connected) ? true : false; }
			}

			/// <summary>
			/// Did we have an exception while connecting? 
			/// </summary>
			/// <returns></returns>
			public bool IsError
			{
				get { return error; }
			}

			/// <summary>
			/// Return the socket. 
			/// </summary>
			public Socket Socket
			{
				get { return socket; }
			}
		}
	}
}