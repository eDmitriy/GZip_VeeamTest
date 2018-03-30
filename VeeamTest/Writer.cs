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

        public static List<long> writedIndexes = new List< long >();

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


                    lock ( dataBlocksQueue )
                    {
                        dataBlock = dataBlocksQueue.Dequeue();
                        Monitor.PulseAll( dataBlocksQueue );
                    }

                    lock ( writedIndexes ) writedIndexes.Add( dataBlock.startIndex );

                    if ( dataBlock==null || dataBlock.ByteData==null || dataBlock.ByteData.Length==0) continue;

                    
                    outFileStream.Write( dataBlock.ByteData, 0, dataBlock.ByteData.Length );

                    
                    dataBlock.ByteData = new byte [ 0 ];
                    //Console.Write( " -W " /*+ ++counter*/+ dataBlock.startIndex.ToString("C0") );
                }

            }
        }



    }
}
