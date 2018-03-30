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

        public static List<long> writedIndexes = new List< long >();

        #endregion



        #region Constructor

        public Writer ( string newFileName )
        {
            this.newFileName = newFileName;

            WorkerSupportsCancellation = true;
            DoWork += WriteDataBlocksToOutputFile;
        }

        #endregion


        #region Public methods

        public void EnqueueDataBlocks ( DataBlock dataBlock )
        {
            lock ( dataBlocksQueue )
            {
                #region Check for duplicates

                lock ( writedIndexes )
                {
                    if( writedIndexes.Any( v => v == dataBlock.startIndex )
                        || dataBlocksQueue.Any( v => v.startIndex == dataBlock.startIndex )
                    )
                    {
                        //Console.WriteLine("  !!! DUPLICATE !!! "+ dataBlock.startIndex );
                        return;
                    }
                }

                #endregion

                //wait for queue writing
                while ( GetQueueCount() >4  )
                {
                    Monitor.Wait( dataBlocksQueue );
                }

                dataBlocksQueue.Enqueue( dataBlock );
            }
        }

        public int GetQueueCount()
        {
            return dataBlocksQueue.Count;
        }

        #endregion




        void WriteDataBlocksToOutputFile ( object sender, DoWorkEventArgs e )
        {
            if ( Thread.CurrentThread.Name == null ) Thread.CurrentThread.Name = "ThreadedWriter";
            FileInfo outFileInfo = new FileInfo( newFileName );

            //write every dataBlock continuously or wait for dataBlock become ready
            using ( FileStream outFileStream = File.Create( outFileInfo.FullName ) )
            {
                //outFileStream.Lock( 0, dataBlocks.Length * BufferSize );
                DataBlock dataBlock = null;

                while ( true )
                {
                    //wait dataBlock for write 
                    while ( GetQueueCount()==0 )
                    {
                        Thread.Sleep( 10 );
                    }


                    #region Dequeue

                    lock ( dataBlocksQueue )
                    {
                        dataBlock = dataBlocksQueue.Dequeue();
                        Monitor.PulseAll( dataBlocksQueue );
                    }
                    if ( dataBlock==null || dataBlock.ByteData==null || dataBlock.ByteData.Length==0) continue;

                    #endregion


                    //write
                    outFileStream.Write( dataBlock.ByteData, 0, dataBlock.ByteData.Length );


                    dataBlock.ByteData = new byte [ 0 ];
                    lock ( writedIndexes ) writedIndexes.Add( dataBlock.startIndex );
                    //Console.Write( " -W " /*+ ++counter*/+ dataBlock.startIndex.ToString("C0") );
                }

            }
        }



    }
}
