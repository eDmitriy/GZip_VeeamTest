using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace VeeamTest
{
    public class ThreadedReader
    {
        #region Vars

        protected Writer writer;

        protected Thread thread;
        protected FileInfo inputFileInfo;
        protected static int threadsCount;
        protected int threadIndex;


        public bool Finished { get; set; }

        public static List<ThreadedReader> registeredReaders = new List< ThreadedReader >();

        
        #region NextWriteThread

        protected static int NextWriteThreadIndex { get; set; }

        protected int IncreaseNextWriteThreadIndex()
        {
            int startValue = NextWriteThreadIndex;

            NextWriteThreadIndex++;
            if ( NextWriteThreadIndex == threadsCount )
            {
                NextWriteThreadIndex = 0;
            }
            while( registeredReaders[ NextWriteThreadIndex ].Finished && NextWriteThreadIndex != startValue )
            {
                NextWriteThreadIndex++;
                if ( NextWriteThreadIndex == threadsCount )
                {
                    NextWriteThreadIndex = 0;
                }
            }

            return NextWriteThreadIndex;
        }

/*        protected static int LastWriteThread
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
        }*/

        #endregion

        static object progreesLogLocker = new object();

        #endregion


        #region Constructors

        public ThreadedReader ( FileInfo inputFileInfo, int threadsCountToSet,
            int threadIndex )
        {
            this.inputFileInfo = inputFileInfo;
            threadsCount = threadsCountToSet;
            this.threadIndex = threadIndex;


            registeredReaders.Add( this );
            this.writer = Program._outFileWriter;

            thread = new Thread( Loop );
            thread.IsBackground = true;
            thread.Name = "ThreadedReader_" + threadIndex;
            thread.Start();
        }

        #endregion


        /// <summary>
        /// This method will loop dataBlocks array and send them for a further processing
        /// </summary>
        void Loop ()
        {
            using ( var readFileStream = new FileStream( inputFileInfo.FullName,
                FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {

                for ( ulong i = ( ulong ) threadIndex; i < ( ulong ) Program.dataBlocks.Length; i += ( ulong ) threadsCount )
                {
                    AddDataBlocksToWriterQueue( ReadDataBlocks( readFileStream, i ) );
                    //Console.Write( " -R " + i );


                    lock ( progreesLogLocker )
                    {
                        Program.ClearCurrentConsoleLine();
                        Console.Write( "-%" 
                            + ( ( ( float )i / ( float )Program.dataBlocks.Length ) * 100 ).ToString( "F" ) 
                            + ".  index = " + i );
                    }
                }

                Finished = true;
                //Console.WriteLine( "\n\n ThrReader #-"+threadIndex + " finished! \n\n");
                //thread.Abort();
            }
            
        }




        protected virtual List<DataBlock> ReadDataBlocks ( FileStream readFileStream, ulong index )
        {
            DataBlock newDataBlock = new DataBlock();
            newDataBlock.ByteData = ReadFileBlockFromStream( readFileStream, index );
            newDataBlock.startIndex = (long)index * newDataBlock.ByteData.Length;

            return new List< DataBlock >(){newDataBlock};
        }


        protected virtual byte[] ReadFileBlockFromStream ( FileStream readFileStream, ulong index )
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
            return buffer;
        }


        protected void AddDataBlocksToWriterQueue( List< DataBlock > dataBlocks )
        {
            lock ( writer )
            {
                //wait for right threadIndex
                while ( NextWriteThreadIndex != threadIndex )
                {
                    //Console.Write( " -Wait " + threadIndex );
                    Monitor.Wait( writer );
                }

                //add blocks to write queue
                foreach ( DataBlock dataBlock in dataBlocks )
                {
                    //DataBlockToWriteQueue( dataBlock );
                    if ( writer != null ) writer.EnqueueDataBlocks( dataBlock );
                }

                IncreaseNextWriteThreadIndex();
                Monitor.PulseAll( writer );
            }
        }

    }


    public class DecompressReader : ThreadedReader
    {
        #region Vars

        public static List<long> headersFound = new List< long >();

        byte[] bufferGZipHeader = new byte[6];

        #endregion


        #region Constructor

        public DecompressReader ( FileInfo inputFileInfo, int threadsCountToSet, int threadIndex )
            : base( inputFileInfo, threadsCountToSet, threadIndex )
        {
        }

        #endregion



        #region Overrides of ThreadedReader

        protected override List<DataBlock> ReadDataBlocks ( FileStream readFileStream, ulong index )
        {
            //return base.Work( readFileStream, index );
            return DecompressSequence( readFileStream, index );
        }

        #endregion




        #region Decompression

        public List<DataBlock> DecompressSequence ( FileStream readFileStream, ulong index )
        {
            List<DataBlock> dataBlocks = new List< DataBlock >();


            #region ReadFileToBuffer

            var buffer = new byte[Program.BufferSize*2 ]; // memory buffer offset 2X block size
            long headersReadBufferEndIndex = Program.BufferSize + bufferGZipHeader.Length * 2;
            //byte[] buffer = new byte [ Program.BufferSize ];


            long startReadPositionIndex = (long)index * Program.BufferSize;
            if ( startReadPositionIndex >= bufferGZipHeader.Length ) startReadPositionIndex -= bufferGZipHeader.Length;

            readFileStream.Position = ( long )startReadPositionIndex;
            readFileStream.Read( buffer, 0, buffer.Length );

            #endregion


            #region ReadHeadersFromBuffer

            List<long> headers = ReadHeaders( buffer, headersReadBufferEndIndex, startReadPositionIndex );
            
            #endregion

            
            #region foreach of headers create datablock and decompress. Then add block to write queue

            long startIndex, endIndex = 0;

            //create and decompress blocks 
            for ( int i = 0; i < headers.Count; i++ )
            {
                #region Create new dataBlock

                startIndex = headers [ i ];
                endIndex = i + 1 < headers.Count ? headers [ i + 1 ] : buffer.Length + 1;

                DataBlock newDataBlock = new DataBlock
                {
                    startIndex = startIndex,
                    endIndex = endIndex
                };

                #endregion

                #region Decompress

                //newDataBlock.ByteData = TakeBytesBetweenIndexes( buffer, startIndex - ( long )startReadPosition ,  endIndex - ( long )startReadPosition  );
                newDataBlock.DeCompressDataBlock( buffer, startIndex - startReadPositionIndex );
                dataBlocks.Add( newDataBlock );

                #endregion

                //Console.Write( " -R +" + index + "_" + i + "+ thrInd= " + threadIndex );
            }

            #endregion

            return dataBlocks;
        }


        #endregion


        #region Headers
        

        /// <summary>
        /// This method will read GZip headers per byte
        /// keys based on http://www.zlib.org/rfc-gzip.html
        /// </summary>
        /// <param name="readFileStream"></param>
        /// <param name="index"></param>
        /// <param name="threadIndex"></param>
        /// <returns></returns>
        public List<long> ReadHeaders ( byte [] buffer, long endIndexInBuffer, long startReadPositionIndex )
        {
            List<long> headers = new List< long >();


            //Read headers
            for ( long i = 0; i < endIndexInBuffer; i++ )
            {
                #region Read Header

                for ( long j = 0; j < bufferGZipHeader.Length; j++ )
                {
                    if ( endIndexInBuffer > i + j ) //check for i+j < endIndexInBuffer
                    {
                        bufferGZipHeader [ j ] = buffer [ i + j ];
                    }
                }

                #endregion


                //Check header and add it to list
                if ( bufferGZipHeader [ 0 ] == 0x1F
                     && bufferGZipHeader [ 1 ] == 0x8B
                     && bufferGZipHeader [ 2 ] == 0x08
                     && bufferGZipHeader [ 3 ] == 0x00
                     && bufferGZipHeader [ 4 ] == 0x00
                     && bufferGZipHeader [ 5 ] == 0x00
                )
                {
                    headers.Add( i + startReadPositionIndex );
                    //Console.Write( " H "+ Program.headersFound.Count  );
                }
            }
            lock ( headersFound )
            {
                headersFound.AddRange( headers );
            }
            return headers;
        }

        #endregion
        
    }




    public class CompressReader : ThreadedReader
    {
        public CompressReader ( FileInfo inputFileInfo, int threadsCountToSet, int threadIndex ) : base( inputFileInfo, threadsCountToSet, threadIndex )
        {
        }



        #region Overrides of ThreadedReader

        protected override List<DataBlock> ReadDataBlocks ( FileStream readFileStream, ulong index )
        {
            //return base.Work( readFileStream, index );

            return ReadBytesBlockForCompression( readFileStream, index );
        }

        #endregion


        public List<DataBlock> ReadBytesBlockForCompression ( FileStream readFileStream, ulong index )
        {
            var newDataBlock = new DataBlock();
            newDataBlock.CompressDataBlock( ReadFileBlockFromStream( readFileStream, index ) );
            newDataBlock.startIndex = ( long )index * newDataBlock.ByteData.Length;


            return new List<DataBlock>() { newDataBlock };
        }

    }
}