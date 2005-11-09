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
	using System.Text;
/**
/// COMMENT FROM ORIGINAL JAVA CLIENT LIBRARY.  NOT SURE HOW MUCH IT 
/// APPLIES TO THIS LIBRARY.
///
///
/// Handle encoding standard Java types directly which can result in significant
/// memory savings:
/// 
/// Currently the Memcached driver for Java supports the setSerialize() option.
/// This can increase performance in some situations but has a few issues:
/// 
/// Code that performs class casting will throw ClassCastExceptions when
/// setSerialize is enabled. For example:
/// 
///     mc.set( "foo", new Integer( 1 ) ); Integer output = (Integer)mc.get("foo");
/// 
/// Will work just file when setSerialize is true but when its false will just throw
/// a ClassCastException.
/// 
/// Also internally it doesn't support bool and since toString is called wastes a
/// lot of memory and causes additional performance issue.  For example an Integer
/// can take anywhere from 1 byte to 10 bytes.
/// 
/// Due to the way the memcached slab allocator works it seems like a LOT of wasted
/// memory to store primitive types as serialized objects (from a performance and
/// memory perspective).  In our applications we have millions of small objects and
/// wasted memory would become a big problem.
/// 
/// For example a Serialized bool takes 47 bytes which means it will fit into the
/// 64byte LRU.  Using 1 byte means it will fit into the 8 byte LRU thus saving 8x
/// the memory.  This also saves the CPU performance since we don't have to
/// serialize bytes back and forth and we can compute the byte[] value directly.
/// 
/// One problem would be when the user calls get() because doing so would require
/// the app to know the type of the object stored as a bytearray inside memcached
/// (since the user will probably cast).
/// 
/// If we assume the basic types are interned we could use the first byte as the
/// type with the remaining bytes as the value.  Then on get() we could read the
/// first byte to determine the type and then construct the correct object for it.
/// This would prevent the ClassCastException I talked about above.
/// 
/// We could remove the setSerialize() option and just assume that standard VM types
/// are always internd in this manner.
/// 
/// mc.set( "foo", new bool.TRUE ); bool b = (bool)mc.get( "foo" );
/// 
/// And the type casts would work because internally we would create a new bool
/// to return back to the client.
/// 
/// This would reduce memory footprint and allow for a virtual implementation of the
/// Externalizable interface which is much faster than Serialzation.
/// 
/// Currently the memory improvements would be:
/// 
/// java.lang.bool - 8x performance improvement (now just two bytes)
/// java.lang.Integer - 16x performance improvement (now just 5 bytes)
/// 
/// Most of the other primitive types would benefit from this optimization.
/// java.lang.Character being another obvious example.
/// 
/// I know it seems like I'm being really picky here but for our application I'd
/// save 1G of memory right off the bat.  We'd go down from 1.152G of memory used
/// down to 144M of memory used which is much better IMO.
**/
public class NativeHandler 
{

    //FIXME: what about other common types?  Also what about
    //Collections of native types?  I could reconstruct these on the remote end
    //if necessary.  Though I'm not sure of the performance advantage here.
    
    public static readonly byte MARKER_BYTE				= 1;
    public static readonly byte MARKER_BOOL				= 2;
    public static readonly byte MARKER_INTEGER			= 3;
    public static readonly byte MARKER_LONG				= 4;
    public static readonly byte MARKER_CHARACTER		= 5;
    public static readonly byte MARKER_STRING			= 6;
    public static readonly byte MARKER_STRINGBUFFER		= 7;
    public static readonly byte MARKER_FLOAT			= 8;
    public static readonly byte MARKER_SHORT			= 9;
    public static readonly byte MARKER_DOUBLE			= 10;
    public static readonly byte MARKER_DATE				= 11;

    public static bool isHandled( object val ) {

        if ( val is bool				||
             val is byte				||
             val is string				||
             val is char				||
             val is StringBuilder		||
             val is short				||
             val is long				||
             val is double				||
             val is float				||
             val is DateTime			||
             val is Int32 ) 
		{
            return true;
        }

        return false;
        
    }

    // **** encode methods ******************************************************

    public static byte[] encode( object val ){

        if ( val is bool )
            return encode( (bool)val );

        if ( val is Int32 ) 
            return encode((Int32)val);

        if ( val is string )
            return encode( (string)val );

        if ( val is char )
            return encode( (char)val );

        if ( val is byte )
            return encode( (byte)val );

        if ( val is StringBuilder )
            return encode( (StringBuilder)val );

        if ( val is short )
            return encode( (short)val );

        if ( val is long ) 
            return encode( (long)val );

        if ( val is double) 
            return encode( (double)val );

        if ( val is float ) 
            return encode( (float)val );

		if (val is DateTime) 
			return encode((DateTime) val);

        return null;
        
    }

    public static byte[] encode(DateTime val) {
		byte[] b = getBytes(val.Ticks);
		b[0] = MARKER_DATE;
		return b;
	}

    public static byte[] encode( bool val ) {

        byte[] b = new byte[2];

        b[0] = MARKER_BOOL;
        
        if ( val ) {
            b[1] = 1;
        } else {
            b[1] = 0;
        }

        return b;
        
    }

    public static byte[] encode( int val ) {

        byte[] b = getBytes( val );
        b[0] = MARKER_INTEGER;

        return b;
        
    }

    public static byte[] encode( char val ) {

        byte[] result = encode( (short) val );

        result[0] = MARKER_CHARACTER;
        
        return result;

    }
    
    public static byte[] encode( string val ) {

		byte[] asBytes = UTF8Encoding.UTF8.GetBytes(val);
		
		byte[] result = new byte[asBytes.Length + 1];
		
		result[0] = MARKER_STRING;
		
		Array.Copy(asBytes, 0, result, 1, asBytes.Length);

		return result;
        
    }

    public static byte[] encode( byte val ) {

        byte[] b = new byte[2];

        b[0] = MARKER_BYTE;

        b[1] = val;
        
        return b;
        
    }

    public static byte[] encode( StringBuilder val ){

        byte[] b = encode( val.ToString() );
        b[0] = MARKER_STRINGBUFFER;
        
        return b;
        
    }

    public static byte[] encode( short val ){

        byte[] b = encode( (int)val );
        b[0] = MARKER_SHORT;
        
        return b;
        
    }

    public static byte[] encode( long val ) {

        byte[] b = getBytes( val );
        b[0] = MARKER_LONG;

        return b;
        
    }

    public static byte[] encode( double val ) {

		byte[] temp = BitConverter.GetBytes(val);
        byte[] b = new byte[temp.Length + 1];
        b[0] = MARKER_DOUBLE;
		Array.Copy(temp, 0, b, 1, temp.Length);
        
        return b;
        
    }

    public static byte[] encode( float val ) {

		byte[] temp = BitConverter.GetBytes(val);
        byte[] b = new byte[temp.Length + 1];
        b[0] = MARKER_FLOAT;
        Array.Copy(temp, 0, b, 1, temp.Length);
		
        return b;
        
    }

    public static byte[] getBytes( long val ) {

        byte b0 = (byte)((val >> 56) & 0xFF);
        byte b1 = (byte)((val >> 48) & 0xFF);
        byte b2 = (byte)((val >> 40) & 0xFF);
        byte b3 = (byte)((val >> 32) & 0xFF);
        byte b4 = (byte)((val >> 24) & 0xFF);
        byte b5 = (byte)((val >> 16) & 0xFF);
        byte b6 = (byte)((val >> 8) & 0xFF);
        byte b7 = (byte)((val >> 0) & 0xFF);

        byte[] b = new byte[9];
        b[1] = b0;
        b[2] = b1;
        b[3] = b2;
        b[4] = b3;
        b[5] = b4;
        b[6] = b5;
        b[7] = b6;
        b[8] = b7;

        return b;
        
    }

    public static byte[] getBytes( int val ) {

        byte b0 = (byte)((val >> 24) & 0xFF);
        byte b1 = (byte)((val >> 16) & 0xFF);
        byte b2 = (byte)((val >> 8) & 0xFF);
        byte b3 = (byte)((val >> 0) & 0xFF);

        byte[] b = new byte[5];
        b[1] = b0;
        b[2] = b1;
        b[3] = b2;
        b[4] = b3;

        return b;
        
    }

    // **** decode methods ******************************************************

    public static Object decode( byte[] b) {

        //something strange is going on.
        if ( b.Length < 1 )
            return null;


		//determine what type this is:
		
        if ( b[0] == MARKER_BOOL )
            return decodebool( b );

        if ( b[0] == MARKER_INTEGER )
            return decodeInteger( b );

        if ( b[0] == MARKER_STRING )
            return decodeString( b );

        if ( b[0] == MARKER_CHARACTER )
            return decodeCharacter( b );

        if ( b[0] == MARKER_BYTE )
            return decodeByte( b );

        if ( b[0] == MARKER_STRINGBUFFER )
            return decodeStringBuilder( b );

        if ( b[0] == MARKER_SHORT )
            return decodeShort( b );

        if ( b[0] == MARKER_LONG )
            return decodeLong( b );

        if ( b[0] == MARKER_DOUBLE )
            return decodeDouble( b );

        if ( b[0] == MARKER_FLOAT )
            return decodeFloat( b );

        if ( b[0] == MARKER_DATE )
            return decodeDate( b );
			

        return null;

    }

	public static DateTime decodeDate(byte[] b) {
		return new DateTime(toLong(b));
	}

    public static bool decodebool( byte[] b ) {

        bool val = b[1] == 1;

        if ( val )
            return true;

        return false;
        
    }

    public static Int32 decodeInteger( byte[] b ) {

        return toInt( b ) ;
        
    }

    public static string decodeString( byte[] b ) {
		return UTF8Encoding.UTF8.GetString(b, 1, b.Length -1);
    }

    public static char decodeCharacter( byte[] b ) {

        return (char)decodeInteger( b );
        
    }

    public static byte decodeByte( byte[] b ) {

        byte val = b[1];

        return val;
        
    }

    public static StringBuilder decodeStringBuilder( byte[] b ) {

        return new StringBuilder( decodeString( b ) );
        
    }

    public static short decodeShort( byte[] b ) {

        return (short)decodeInteger( b );
        
    }

    public static long decodeLong( byte[] b ) {

        return toLong( b );
    }

    public static double decodeDouble( byte[] b )  {

		return BitConverter.ToDouble(b, 1);
        
    }

    public static float decodeFloat( byte[] b ) {
        
		return BitConverter.ToSingle(b, 1);        
    }

    public static int toInt( byte[] b ) {

        //This works by taking each of the bit patterns and converting them to
        //ints taking into account 2s complement and then adding them..
        
        return (((((int) b[4]) & 0xFF) << 32) +
                ((((int) b[3]) & 0xFF) << 40) +
                ((((int) b[2]) & 0xFF) << 48) +
                ((((int) b[1]) & 0xFF) << 56));
    }    

    public static long toLong( byte[] b ) {

        //FIXME: this is sad in that it takes up 16 bytes instead of JUST 8
        //bytes and wastes memory.  We could use a memcached flag to enable
        //special treatment for 64bit types
        
        //This works by taking each of the bit patterns and converting them to
        //ints taking into account 2s complement and then adding them..

        return ((((long) b[8]) & 0xFF) +
                ((((long) b[7]) & 0xFF) << 8) +
                ((((long) b[6]) & 0xFF) << 16) +
                ((((long) b[5]) & 0xFF) << 24) +
                ((((long) b[4]) & 0xFF) << 32) +
                ((((long) b[3]) & 0xFF) << 40) +
                ((((long) b[2]) & 0xFF) << 48) +
                ((((long) b[1]) & 0xFF) << 56));
    }    
}
}