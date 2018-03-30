using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace VeeamTest
{
    public class Writer : BackgroundWorker
    {
        #region Vars

        private Queue<DataBlock> dataBlocksQueue = new Queue< DataBlock >();
        private string newFileName;
        private string inputFileName;

        ulong dataBlocksBufferedMemoryAmount = 0;
        ulong maxMemoryForDataBlocksBuffer = 1024 * 1024 * 100; //100 mb


        #endregion



        #region Constructor

        public Writer ( string newFileName, string inputFileName )
        {
            this.newFileName = newFileName;
            this.inputFileName = inputFileName;

            DoWork += WriteDataBlocksToOutputFile;
        }

        #endregion


        #region Public methods

        public void EnqueueDataBlocks ( DataBlock dataBlock )
        {
            lock ( dataBlocksQueue )
            {
                while ( dataBlocksBufferedMemoryAmount >= maxMemoryForDataBlocksBuffer )
                {
                    Monitor.Wait( dataBlocksQueue );
                }

                if ( /*dataBlocksQueue.Count( v => v != null && v.Equals( dataBlock ) ) == 0 */true)
                {
                    dataBlocksQueue.Enqueue( dataBlock );
                    dataBlocksBufferedMemoryAmount += ( ulong )dataBlock.ByteData.Length;
                }
            }
        }

        public int GetQueueCount()
        {
            return dataBlocksQueue.Count;
        }

        #endregion




        void WriteDataBlocksToOutputFile ( object sender, DoWorkEventArgs e )//( string newFileName, string inputFileName )
        {
            FileInfo outFileInfo = new FileInfo( newFileName );
            FileInfo inputFileInfo = new FileInfo( inputFileName );
            ulong counter = 0;


            //write every dataBlock continuously or wait for dataBlock become ready
            using ( FileStream outFileStream = File.Create( outFileInfo.FullName ) )
            {
                //outFileStream.Lock( 0, dataBlocks.Length * BufferSize );
                DataBlock dataBlock = null;

                while ( true )
                {
                    //wait dataBlock for write 
                    while ( dataBlocksQueue.Count==0 )
                    {
                        Thread.Sleep( 10 );
                    }


                    lock ( dataBlocksQueue )
                    {
                        dataBlock = dataBlocksQueue.Dequeue();
                        Monitor.PulseAll( dataBlocksQueue );
                    }
                    if ( dataBlock==null || dataBlock.ByteData==null || dataBlock.ByteData.Length==0) continue;

                    
                    outFileStream.Write( dataBlock.ByteData, 0, dataBlock.ByteData.Length );



                    dataBlocksBufferedMemoryAmount -= (ulong)dataBlock.ByteData.Length;
                    dataBlock.ByteData = new byte [ 0 ];
                    dataBlock = null;

                    //Console.Write( " -W " /*+ ++counter*/+ dataBlock.startIndex.ToString("C0") );
                }

            }
        }



    }
}
