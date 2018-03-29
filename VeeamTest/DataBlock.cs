﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.Compression;


namespace VeeamTest
{
    public class DataBlock
    {
        public long startIndex;
        public long endIndex;

        public byte[] ByteData { get; set; }



        #region Public Methods

        public void DeCompressDataBlock ( byte[] bytes )
        {
            using ( MemoryStream mStreamOrigFile = new MemoryStream( bytes ) )
            {
                mStreamOrigFile.Position = 0;

                using ( MemoryStream mStream = new MemoryStream() )
                {
                    using ( GZipStream gZipStream = new GZipStream( mStreamOrigFile, CompressionMode.Decompress ) )
                    {
                        gZipStream.CopyTo( mStream );
                    }
                    ByteData = mStream.ToArray();

                    //return (ulong)ByteData.Length;
                }
            }
        }

        public void CompressDataBlock ( byte [] bytes )
        {
            using ( MemoryStream mStream = new MemoryStream() )
            {
                using ( GZipStream gZipStream = new GZipStream( mStream, CompressionMode.Compress ) )
                {
                    gZipStream.Write( bytes, 0, bytes.Length );
                }
                ByteData = mStream.ToArray();

                //return ( ulong )ByteData.Length;
            }
        }

        #endregion


        public bool Equals( /*DataBlock dataBlockA, */DataBlock dataBlockB )
        {
            if( this == null || dataBlockB == null ) return false;

            return this.startIndex == dataBlockB.startIndex 
                && this.endIndex == dataBlockB.endIndex ;
        }
    }
}
