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
            Console.WriteLine(bytesRead);
        }




        public static byte [] CompressDataBlock ( byte[] bytes, CompressionMode compressionMode )
        {
            //return bytes;

            using ( MemoryStream mStream = new MemoryStream() )
            {
                using ( GZipStream gZipStream = new GZipStream( mStream, compressionMode, false ) )
                {
                    gZipStream.Write( bytes, 0, bytes.Length );
                }
                return mStream.ToArray(); 
            }
        }

    }
}
