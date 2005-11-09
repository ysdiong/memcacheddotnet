/**
/// MemCached C# client
/// Copyright (c) 2005
/// 
/// Based on code written originally by Greg Whalin
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
	using System.IO;

	/// <summary>
	/// Bridge class to provide nested Exceptions with IOException which has
	/// constructors that don't take Throwables.
	/// </summary>
	public class NestedIOException:IOException 
	{
		Exception _innerException;

		/// <summary>
		/// Create a new <c>NestedIOException</c> instance.
		/// </summary>
		/// <param name="cause">The inner exception</param>
		public NestedIOException( Exception inner )
		{
			_innerException = inner;
		}

		public NestedIOException( string message, Exception inner ) 
			:base(message)
		{
			_innerException = inner;
		}

		public override System.Exception GetBaseException()
		{
			return _innerException;
		}

	}
}