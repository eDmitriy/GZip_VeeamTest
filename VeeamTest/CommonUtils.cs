using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using ThreadState = System.Diagnostics.ThreadState;

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


        public static void WriteBlock (string newFileName,/*this Stream inStream,*/ int count, byte [] buffer, long seekIndex)
        {
            Console.Write( " -" +Program.threadCounter+" ");
            Program.threadCounter += 1;
            using ( FileStream compressedFileStream = File.Create( newFileName ) )
            {
                using ( GZipStream compressionStream = new GZipStream( compressedFileStream, CompressionMode.Compress, false ) )
                {
                    compressionStream.Seek( seekIndex, SeekOrigin.Begin );
                    compressionStream.Write( buffer, 0, count );
                }
            }
            //buffer = new byte[0];
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


/*        public static void Compress ( string inFileName )
        {
            int dataPortionSize = 1024 * 1024;//Environment.SystemPageSize / threadNumber;
            try
            {
                FileStream inFile = new FileStream(inFileName, FileMode.Open);
                FileStream outFile = new FileStream(inFileName + ".gz", FileMode.Append);

                int _dataPortionSize;
                Thread[] tPool;

                Console.Write( "Compressing..." );

                while ( inFile.Position < inFile.Length )
                {
                    Console.Write( "." );
                    tPool = new Thread [ threadNumber ];
                    for ( int portionCount = 0;
                         ( portionCount < threadNumber ) && ( inFile.Position < inFile.Length );
                         portionCount++ )
                    {
                        if ( inFile.Length - inFile.Position <= dataPortionSize )
                        {
                            _dataPortionSize = ( int )( inFile.Length - inFile.Position );
                        }
                        else
                        {
                            _dataPortionSize = dataPortionSize;
                        }
                        dataArray [ portionCount ] = new byte [ _dataPortionSize ];
                        inFile.Read( dataArray [ portionCount ], 0, _dataPortionSize );

                        tPool [ portionCount ] = new Thread( CompressBlock );
                        tPool [ portionCount ].Start( portionCount );
                    }

                    for ( int portionCount = 0; ( portionCount < threadNumber ) && ( tPool [ portionCount ] != null ); )
                    {
                        if ( tPool [ portionCount ].ThreadState == ThreadState.Stopped )
                        {
                            BitConverter.GetBytes( compressedDataArray [ portionCount ].Length + 1 )
                                        .CopyTo( compressedDataArray [ portionCount ], 4 );
                            outFile.Write( compressedDataArray [ portionCount ], 0, compressedDataArray [ portionCount ].Length );
                            portionCount++;
                        }
                    }

                }

                outFile.Close();
                inFile.Close();
            }
            catch ( Exception ex )
            {
                Console.WriteLine( "ERROR:" + ex.Message );
            }
        }

        public static void CompressBlock ( object i )
        {
            using ( MemoryStream output = new MemoryStream( dataArray [ ( int )i ].Length ) )
            {
                using ( GZipStream cs = new GZipStream( output, CompressionMode.Compress ) )
                {
                    cs.Write( dataArray [ ( int )i ], 0, dataArray [ ( int )i ].Length );
                }
                compressedDataArray [ ( int )i ] = output.ToArray();
            }
        }

        public static void Decompress ( string inFileName )
        {
            try
            {
                FileStream inFile = new FileStream(inFileName, FileMode.Open);
                FileStream outFile = new FileStream(inFileName.Remove(inFileName.Length - 3), FileMode.Append);
                int _dataPortionSize;
                int compressedBlockLength;
                Thread[] tPool;
                Console.Write( "Decompressing..." );
                byte[] buffer = new byte[8];


                while ( inFile.Position < inFile.Length )
                {
                    Console.Write( "." );
                    tPool = new Thread [ threadNumber ];
                    for ( int portionCount = 0;
                         ( portionCount < threadNumber ) && ( inFile.Position < inFile.Length );
                         portionCount++ )
                    {
                        inFile.Read( buffer, 0, 8 );
                        compressedBlockLength = BitConverter.ToInt32( buffer, 4 );
                        compressedDataArray [ portionCount ] = new byte [ compressedBlockLength + 1 ];
                        buffer.CopyTo( compressedDataArray [ portionCount ], 0 );

                        inFile.Read( compressedDataArray [ portionCount ], 8, compressedBlockLength - 8 );
                        _dataPortionSize = BitConverter.ToInt32( compressedDataArray [ portionCount ], compressedBlockLength - 4 );
                        dataArray [ portionCount ] = new byte [ _dataPortionSize ];

                        tPool [ portionCount ] = new Thread( DecompressBlock );
                        tPool [ portionCount ].Start( portionCount );
                    }

                    for ( int portionCount = 0; ( portionCount < threadNumber ) && ( tPool [ portionCount ] != null ); )
                    {
                        if ( tPool [ portionCount ].ThreadState == ThreadState.Stopped )
                        {
                            outFile.Write( dataArray [ portionCount ], 0, dataArray [ portionCount ].Length );
                            portionCount++;
                        }
                    }
                }

                outFile.Close();
                inFile.Close();
            }
            catch ( Exception ex )
            {
                Console.WriteLine( "ERROR:" + ex.Message );
            }
        }

        public static void DecompressBlock ( object i )
        {
            using ( MemoryStream input = new MemoryStream( compressedDataArray [ ( int )i ] ) )
            {

                using ( GZipStream ds = new GZipStream( input, CompressionMode.Decompress ) )
                {
                    ds.Read( dataArray [ ( int )i ], 0, dataArray [ ( int )i ].Length );
                }

            }
        }*/

    }
}
