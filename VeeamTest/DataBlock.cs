using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.Compression;


namespace VeeamTest
{
    public class DataBlock
    {
        public long startIndex;
        //public long endIndex;

        public byte[] ByteData { get; set; }



        #region Public Methods

        public void DeCompressDataBlock (byte[] bytes,  long startIndex)
        {
            try
            {
                using ( MemoryStream mStreamOrigFile = new MemoryStream( bytes ) )
                {
                    mStreamOrigFile.Position = startIndex;

                    using ( MemoryStream mStream = new MemoryStream() )
                    {
                        using ( GZipStream gZipStream = new GZipStream( mStreamOrigFile, CompressionMode.Decompress ) )
                        {
                            gZipStream.CopyTo( mStream );
                        }
                        ByteData = mStream.ToArray();
                    }
                }
            }
            catch( Exception e )
            {
                Console.WriteLine("\n Decompression of block at "+ startIndex +" Failed!: " +e.Message);
            }

        }



        public void CompressDataBlock ( byte [] bytes )
        {
            try
            {
                using ( MemoryStream mStream = new MemoryStream() )
                {
                    using ( GZipStream gZipStream = new GZipStream( mStream, CompressionMode.Compress ) )
                    {
                        gZipStream.Write( bytes, 0, bytes.Length );
                    }
                    ByteData = mStream.ToArray();
                }
            }
            catch( Exception e )
            {
                Console.WriteLine( "\n Compression of block at " + startIndex + " Failed!: " + e.Message );
                //throw;
            }


        }


        public bool Equals ( DataBlock dataBlockB )
        {
            if ( this == null || dataBlockB == null ) return false;

            return this.startIndex == dataBlockB.startIndex
                   /*&& this.endIndex == dataBlockB.endIndex*/;
        }

        #endregion


    }
}
