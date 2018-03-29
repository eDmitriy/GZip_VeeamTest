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
        private static int threadsCount;
        private int threadIndex;

        private Func< FileStream, ulong, int > funcWork;
        public enum WorkType
        {
            readCompressed,
            readHeaders,
            readNotCompressed
        }


        public bool Finished { get; set; }



        private object threadLock_headersRead = new object();
        private object threadLock_writerQueue = new object();

        private object threadLock_loop = new object();



        private static int lastWriteThread;
        public static int LastWriteThread
        {
            get { return lastWriteThread; }
            set
            {
                lastWriteThread = value;
                if( lastWriteThread >= threadsCount )
                {
                    lastWriteThread = 0;
                }
            }
        }

        #endregion


        #region Constructors

        public ThreadedReader ( FileInfo inputFileInfo, int threadsCountToSet,
            int threadIndex, /*Func<FileStream, ulong, int, int> funcWork*/WorkType workType )
        {
            this.inputFileInfo = inputFileInfo;
            threadsCount = threadsCountToSet;
            this.threadIndex = threadIndex;

            switch ( workType )
            {
                case WorkType.readCompressed:
                    this.funcWork = ReadBytesBlockForDecompression;
                    break;
                case WorkType.readNotCompressed:
                    this.funcWork = ReadBytesBlockForCompression;
                    break;
                case WorkType.readHeaders:
                    this.funcWork = ReadHeaders;
                    break;

            }
            //this.funcWork = funcWork;

            thread = new Thread( DoWork );
            //thread.IsBackground = true;
            thread.Name = "ThreadedReader_" + threadIndex;
            thread.Start();
        }

        #endregion


        /// <summary>
        /// This method will loop dataBlocks array and send them for a further processing
        /// </summary>
        void DoWork ()
        {
            using ( var readFileStream = new FileStream( inputFileInfo.FullName,
                FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {

                for ( ulong i = ( ulong ) threadIndex; i < ( ulong ) Program.dataBlocks.Length; i += ( ulong ) threadsCount )
                {
                    #region MemoryLimit

/*                        while( Program.dataBlocksBufferedMemoryAmount >= Program.maxMemoryForDataBlocksBuffer )
                    {
                        if( Program.lastWritedDataBlockIndex + 1 == i )
                            break; //if writer need this block then dont sleep
                        Thread.Sleep( 1 );
                    }*/

                    #endregion

                    if( funcWork != null ) funcWork.Invoke( readFileStream, i);

                    //Console.Write( " -R " + i );
                }

                Finished = true;
                //thread.Abort();
            }
            
        }
        


        #region Block work

        public /*static*/ int ReadBytesBlockForDecompression( FileStream readFileStream, ulong index )
        {
            var newDataBlock = Program.dataBlocks[ index ];
            byte[] buffer = new byte [ (newDataBlock.endIndex - newDataBlock.startIndex) ];

            readFileStream.Position = newDataBlock.startIndex;
            int bytesRead = readFileStream.Read( buffer, 0, buffer.Length );

            //if reach end file and buffer filled with nulls
            if ( bytesRead < buffer.Length )
            {
                buffer = buffer.Take( bytesRead ).ToArray();
            }

            newDataBlock.DeCompressDataBlock( buffer );


            lock ( Program._outFileWriter )
            {
                while ( LastWriteThread != threadIndex )
                {
                    //Console.Write( " -Wait " + threadIndex );
                    Monitor.Wait( Program._outFileWriter );
                }

                
                if ( Program._outFileWriter != null ) Program._outFileWriter.EnqueueDataBlocks( newDataBlock );
                //Program.dataBlocksBufferedMemoryAmount += ( ulong )newDataBlock.ByteData.Length;

                Console.Write( " -R " + index );



                LastWriteThread++;
                Monitor.PulseAll( Program._outFileWriter );
            }

            return 0;
        }

        public /*static*/ int ReadBytesBlockForCompression( FileStream readFileStream, ulong index )
        {
            var newDataBlock = Program.dataBlocks[ index ];
            byte[] buffer = new byte [ Program.BufferSize ]; 


            ulong startReadPosition = index * ( ulong )buffer.Length;
            readFileStream.Position = ( long )startReadPosition;
            int bytesRead = readFileStream.Read( buffer, 0, buffer.Length );


            //if reach end file and buffer filled with nulls
            if ( bytesRead < buffer.Length )
            {
                buffer = buffer.Take( bytesRead ).ToArray();
            }

            newDataBlock.CompressDataBlock( buffer );
            //if( Program._outFileWriter != null ) Program._outFileWriter.EnqueueDataBlocks( newDataBlock );
            Program.dataBlocksBufferedMemoryAmount += ( ulong )newDataBlock.ByteData.Length;


            return 0;
        }


        /// <summary>
        /// This method will read GZip headers per byte
        /// keys based on http://www.zlib.org/rfc-gzip.html
        /// </summary>
        /// <param name="readFileStream"></param>
        /// <param name="index"></param>
        /// <param name="threadIndex"></param>
        /// <returns></returns>
        public /*static*/ int ReadHeaders( FileStream readFileStream, ulong index )
        {
            byte[] bufferGZipHeader = new byte[6];
            var buffer = new byte[Program.BufferSize + bufferGZipHeader.Length*2 ]; // read offset for byte mask size for start/end.  6b+data+6b
            List<long> headers = new List< long >();

            ulong startReadPosition = index * ( ulong )buffer.Length;

            //if read position not at 0 => offset position
            if( startReadPosition > ( ulong )bufferGZipHeader.Length ) startReadPosition -= (ulong)bufferGZipHeader.Length;

            //read from file
            readFileStream.Position = ( long )startReadPosition;
            readFileStream.Read( buffer, 0, buffer.Length );



            //Read headers
            for ( ulong i = 0; i < (ulong)buffer.Length; i++ )
            {
                #region Read Header

                for ( ulong j = 0; j < (ulong)bufferGZipHeader.Length; j++ )
                {
                    if ( (ulong)buffer.Length > i + j ) //check for i+j < bufferAllFile.Lenght
                    {
                        bufferGZipHeader [ j ] = buffer [ i + j ];
                    }
                }

                #endregion


                //Check header and if true => decompress block
                if (    bufferGZipHeader [ 0 ] == 0x1F
                     && bufferGZipHeader [ 1 ] == 0x8B
                     && bufferGZipHeader [ 2 ] == 0x08
                     && bufferGZipHeader [ 3 ] == 0x00
                     && bufferGZipHeader [ 4 ] == 0x00
                     && bufferGZipHeader [ 5 ] == 0x00
                )
                {
                    var newHeader = startReadPosition + i;
                    headers.Add( (long)newHeader );

                    //Console.Write( " H "+ Program.headersFound.Count  );
                }
            }

            //write headers sync
            lock ( Program.headersFound )
            {
                Program.headersFound.AddRange( headers );
            }
            return 0;
        }


        #endregion
        

    }

}
