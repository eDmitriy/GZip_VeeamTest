using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;


namespace VeeamTest
{
    public static class CommonUtils
    {

        public static void CopyTo ( this Stream input, Stream output )
        {
            if( input==null || !input.CanRead) return;
            if ( output==null || !output.CanWrite) return;


            byte[] buffer = new byte[1024 * 1024]; // Fairly arbitrary size
            int bytesRead;

            while ( ( bytesRead = input.Read( buffer, 0, buffer.Length ) ) > 0 )
            {
                output.Write( buffer, 0, bytesRead );
            }
/*            int bytesRead = input.Read( buffer, 0, buffer.Length ) ;
            
            if(bytesRead>0) output.Write( buffer, 0, bytesRead );*/
            
        }


        public static void CopyStreamByBlocks ( this Stream input, Stream output, int bufferSize = 1024 )
        {
            if ( input == null || !input.CanRead ) return;
            if ( output == null || !output.CanWrite ) return;


            byte[] buffer = new byte[bufferSize * bufferSize]; // Fairly arbitrary size
/*            int bytesRead;

            while ( ( bytesRead = input.Read( buffer, 0, buffer.Length ) ) > 0 )
            {
                output.Write( buffer, 0, bytesRead );
            }*/

            Console.Write( '-' );

            int bytesRead = input.Read( buffer, 0, buffer.Length ) ;
            if(bytesRead>0) output.Write( buffer, 0, bytesRead );

        }





        public static byte [] CompressDataChunk ( byte[] bytes )
        {
            return bytes;

            using ( MemoryStream mStream = new MemoryStream() )
            {
                using ( GZipStream compressionStream =
                    new GZipStream( mStream, CompressionMode.Compress, false ) )
                {
                    //compressionStream.Seek( seekIndex, SeekOrigin.Begin );
                    compressionStream.Write( bytes, 0, bytes.Length );
                    //return compressionStream.GetBuffer();

                }
                return mStream.GetBuffer();
            }

        }



    }
}
