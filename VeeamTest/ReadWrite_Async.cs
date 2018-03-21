using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;

namespace VeeamTest
{
    public class CompressionThread
    {
        private Thread thread;
        private FileInfo inputFileInfo;
        private int threadsCount;
        private int threadIndex;

        public bool Finished { get; set; }



        public CompressionThread ( FileInfo inputFileInfoToSet, int threadsCountToSet, int threadIndexToSet )
        {
            inputFileInfo = inputFileInfoToSet;
            this.threadsCount = threadsCountToSet;
            this.threadIndex = threadIndexToSet;

            thread = new Thread( DoWork );
            thread.Start();
        }



        void DoWork ()
        {
            using ( var readFileStream = new FileStream( inputFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                int bytesRead = 0;
                long startReadPosition = 0;
                byte[] buffer = new byte [ Program.BufferSize ]; ;


                #region ForLoop

                for ( int i = threadIndex; i < Program.dataBlocks.Length; i += threadsCount )
                {
                    startReadPosition = i * buffer.Length;
                    readFileStream.Position = startReadPosition;
                    bytesRead = readFileStream.Read( buffer, 0, buffer.Length ) ;

                    //if reach end file and buffer filled with nulls
                    if ( bytesRead < buffer.Length )
                    {
                        buffer = buffer.Take( bytesRead ).ToArray();
                    }

                    var newDataChunk = Program.dataBlocks[ i ];
                    newDataChunk.byteData = CommonUtils.CompressDataBlock( buffer , CompressionMode.Compress);

                    //Console.Write( " -R " + i );
                }
                #endregion


                Finished = true;
                thread.Abort();
            }
        }

    }


    public class ReadCompressedFileHeadersThread
    {
        private Thread thread;
        private FileInfo fileToDecompress;
        private int threadsCount;
        private int threadIndex;

        //public  Queue<long> compresedBlocksQueue = new Queue< long >();
        public Queue<DataBlock> dataBlocksQueue = new Queue< DataBlock >();

        public bool Finished { get; set; }



        public ReadCompressedFileHeadersThread( FileInfo fileToDecompress, int threadsCount, int threadIndex )
        {
            this.fileToDecompress = fileToDecompress;
            this.threadIndex = threadIndex;
            this.threadsCount = threadsCount;

            thread = new Thread( DoWork );
            thread.Start();
        }


        void DoWork()
        {
            using ( FileStream originalFileStream = new FileStream( fileToDecompress.FullName, 
                FileMode.Open, FileAccess.Read, FileShare.Read ) )
            {
                long iTemp = 0;
                long iteartionCount = 0;
                int totalBlocksFound = 0;

                long blockSize = originalFileStream.Length / threadsCount;
                long initialPos = blockSize * threadIndex;

                byte[] bufferGZipHeader = new byte[4];
                byte[] bufferCompressedFilePart = new byte[blockSize];
                byte[] bufferGZipDecompression = new byte[Program.BufferSize];

                var bytesRead = 0;


                //read compressed file part
                originalFileStream.Position = initialPos;
                originalFileStream.Read( bufferCompressedFilePart, 0, bufferCompressedFilePart.Length );
                

                for ( long i = 0; i < blockSize; i ++ )
                {
                    iTemp = i;
                    iteartionCount++;

                    //read header bytes from file bytes array. Continously
                    #region Read Header

                    for ( int j = 0; j < 4; j++ )
                    {
                        if( blockSize > i + j ) //check for i+j < bufferAllFile.Lenght
                        {
                            bufferGZipHeader[ j ] = bufferCompressedFilePart[ i + j ];
                        }
                    }

                    #endregion
                    

                    //Check header and if true => decompress block
                    if (    bufferGZipHeader [ 0 ] == 0x1F
                         && bufferGZipHeader [ 1 ] == 0x8B
                         && bufferGZipHeader [ 2 ] == 0x08
                         && bufferGZipHeader [ 3 ] == 0x00 )
                    {
                        DataBlock newDataBlock = new DataBlock();

                        originalFileStream.Position = i + initialPos;

                        #region Decompress

                        using ( MemoryStream mStream = new MemoryStream() )
                        {
                            using ( GZipStream gZipStream = new GZipStream(
                                originalFileStream, CompressionMode.Decompress, true) )
                            {
                                while( ( bytesRead = gZipStream.Read( bufferGZipDecompression, 0,
                                           bufferGZipDecompression.Length ) ) > 0 )
                                {
                                    mStream.Write( bufferGZipDecompression, 0, bytesRead );
                                }
                                gZipStream.Close();
                                //i += ( bytesRead - 1 );
                            }
                            newDataBlock.byteData = mStream.ToArray();
                            mStream.Close();
                        }

                        #endregion

                        //compresedBlocksQueue.Enqueue( i+ initialPos );
                        dataBlocksQueue.Enqueue( newDataBlock);
                        totalBlocksFound++;

                        Console.WriteLine( " Thread " + threadIndex +"   "  + iTemp + " R " + totalBlocksFound );
                    }
                }
                Console.WriteLine(" Thread "+threadIndex + " Finished at " + iTemp + " BLOCKS Found = "+ totalBlocksFound );
                Finished = true;

            }


        }
    }

}
