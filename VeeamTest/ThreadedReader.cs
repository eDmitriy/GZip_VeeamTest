using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;

namespace VeeamTest
{
    public class ThreadedReader
    {
        #region Vars

        private Thread thread;
        private FileInfo inputFileInfo;
        private int threadsCount;
        private int threadIndex;
        private Func< FileStream, ulong, int > funcWork;

        public bool Finished { get; set; }

        static object threadLock = new object();

        #endregion
        

        #region Constructors

        public ThreadedReader ( FileInfo inputFileInfo, int threadsCount,
            int threadIndex, Func<FileStream, ulong, int> funcWork )
        {
            this.inputFileInfo = inputFileInfo;
            this.threadsCount = threadsCount;
            this.threadIndex = threadIndex;
            this.funcWork = funcWork;

            thread = new Thread( DoWork );
            thread.IsBackground = true;
            thread.Name = "DeCompressionThread " + threadIndex;
            thread.Start();
        }

        #endregion
        

        void DoWork ()
        {
            using ( var readFileStream = new FileStream( inputFileInfo.FullName, 
                FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                for ( ulong i = ( ulong )threadIndex; i < ( ulong )Program.dataBlocks.Length; i += ( ulong )threadsCount )
                {
                    #region MemoryLimit

                    while( Program.dataBlocksBufferedMemoryAmount>= Program.maxMemoryForDataBlocksBuffer )
                    {
                        Thread.Sleep( 1 ); 
                    }

                    #endregion

                    
                    lock ( threadLock )
                    {
                        if( funcWork != null ) funcWork.Invoke( readFileStream, i );
                        //Console.Write( " -R " + i );
                    }
                }

                Finished = true;
                //thread.Abort();
            }
        }
        


        #region Read bytes block

        public static int ReadBytesBlockForDecompression( FileStream readFileStream, ulong index )
        {
            var newDataChunk = Program.dataBlocks[ index ];
            byte[] buffer = new byte [ (newDataChunk.endIndex - newDataChunk.startIndex) ];

            readFileStream.Position = newDataChunk.startIndex;
            int bytesRead = readFileStream.Read( buffer, 0, buffer.Length );

            //check for wrong header
/*            if ( buffer [ 0 ] != ( 0x1F ) )
            {
                Console.WriteLine( "\n\n Wrong stream. " + " ThreadIndex " + threadIndex
                                   + " loop index " + index + " \n\n" );

                readFileStream.Position = newDataChunk.startIndex - 100;
                buffer = new byte [ 200 ];
                readFileStream.Read( buffer, 0, buffer.Length );
                readFileStream.Flush();

                newDataChunk.byteData = new byte [ 1 ];
                return 1;
            }*/


            //if reach end file and buffer filled with nulls
            if ( bytesRead < buffer.Length )
            {
                buffer = buffer.Take( bytesRead ).ToArray();
            }

            newDataChunk.byteData = DeCompressDataBlock( buffer );
            Program.dataBlocksBufferedMemoryAmount += ( ulong )newDataChunk.byteData.Length;

            return 0;
        }

        public static int ReadBytesBlockForCompression( FileStream readFileStream, ulong index )
        {
            byte[] buffer = new byte [ Program.BufferSize ]; 


            ulong startReadPosition = index * ( ulong )buffer.Length;
            readFileStream.Position = ( long )startReadPosition;
            int bytesRead = readFileStream.Read( buffer, 0, buffer.Length );


            //if reach end file and buffer filled with nulls
            if ( bytesRead < buffer.Length )
            {
                buffer = buffer.Take( bytesRead ).ToArray();
            }

            var newDataChunk = Program.dataBlocks[ index ];
            newDataChunk.byteData = CompressDataBlock( buffer, CompressionMode.Compress );
            Program.dataBlocksBufferedMemoryAmount += ( ulong )newDataChunk.byteData.Length;


            return 0;
        }

        #endregion



        #region DataBlock Compression And Decompression

        static byte [] DeCompressDataBlock ( byte [] bytes )
        {
            //return bytes;
            using ( MemoryStream mStreamOrigFile = new MemoryStream( bytes ) )
            {
                mStreamOrigFile.Position = 0;
/*                var buffer = new byte[ 1024 * 1024 ];
                int bytesRead = 0;*/

                using ( MemoryStream mStream = new MemoryStream() )
                {
                    using ( GZipStream gZipStream = new GZipStream( mStreamOrigFile, CompressionMode.Decompress ) )
                    {
/*                        while ( ( bytesRead = gZipStream.Read( buffer,
                                    0, buffer.Length ) ) > 0 )
                        {
                            mStream.Write( buffer, 0, bytesRead );
                        }*/
                        gZipStream.CopyTo( mStream );
                    }
                    return mStream.ToArray();
                }
            }

        }

        static byte [] CompressDataBlock ( byte [] bytes, CompressionMode compressionMode )
        {
            //return bytes;

            using ( MemoryStream mStream = new MemoryStream() )
            {
                using ( GZipStream gZipStream = new GZipStream( mStream, compressionMode ) )
                {
                    gZipStream.Write( bytes, 0, bytes.Length );
                }
                return mStream.ToArray();
            }
        }

        #endregion

    }

}
