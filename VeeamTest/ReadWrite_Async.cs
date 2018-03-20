using System;
using System.Collections.Generic;
using System.IO;
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
                //long index = 0;
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
                    newDataChunk.byteData = CommonUtils.CompressDataChunk( buffer );

/*                    List< byte > bytes = newDataChunk.byteData.ToList();
                    bytes.InsertRange( 0, new byte [ 800 ] );
                    newDataChunk.byteData = bytes.ToArray();*/

                    newDataChunk.currState = DataBlock.State.dataReaded;

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
                byte[] buffer2 = new byte[4];
                long iTemp = 0;
                long iteartionCount = 0;

                long blockSize = originalFileStream.Length / threadsCount;
                long initialPos = blockSize * threadIndex;
                //long stepForward = threadsCount * 4;


                for ( long i = initialPos; i < initialPos+blockSize; i ++ )
                {
                    iTemp = i;
                    iteartionCount++;
                    originalFileStream.Position = i;

                    //read compressed file per byte fro gzip headers
                    
                    originalFileStream.Read( buffer2, 0, buffer2.Length );
                    if (    buffer2 [ 0 ] == 0x1F
                         && buffer2 [ 1 ] == 0x8B
                         && buffer2 [ 2 ] == 0x08
                         && buffer2 [ 3 ] == 0x00 )
                    {
                        //Program.headersFound++;
                        //Program.comprssedFileHeadersQueue.Enqueue( i );
                        Program.compressedBlocksheadersIndexList.Add( i );
                        Program.compressedBlocksheadersIndexList = 
                            Program.compressedBlocksheadersIndexList.OrderBy( v => v ).ToList();

                        Console.WriteLine( " Thread " + threadIndex +"   "  + iTemp + " R " 
                            + Program.compressedBlocksheadersIndexList .Count);
                    }
                }
                Console.WriteLine(" Thread "+threadIndex + " Finished at " + iTemp + " IterationsCount = "+iteartionCount );
                Finished = true;

            }
        }
    }

}
